//! HTTP 接口：路由、鉴权、各 handler 与 OpenAPI 文档。
use crate::{
    db::{
        earliest_summary_overlapping, ensure_meeting, insert_segment, meeting_ended,
        segment_payload, SegmentRow, SegmentView, SEGMENT_SELECT,
    },
    events::MeetingEvent,
    jobs::{enqueue_classification, enqueue_rebuild, enqueue_summary, lane_of},
    livekit_ingest::{self, spawn_ingest, stop_ingest, LivekitIngest},
    providers::SEMANTIC_TYPES,
    publish_event,
    summary::{empty_board, ActionItem, SummaryDocument},
    AppError, AppState,
};
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, Query, Request, State},
    http::{header, HeaderMap, HeaderValue, StatusCode},
    middleware::{self, Next},
    response::{
        sse::{Event, KeepAlive, Sse},
        Html, IntoResponse, Response,
    },
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::types::Json as DbJson;
use std::{convert::Infallible, path::PathBuf};
use tokio::{fs, io::AsyncWriteExt, time::Duration};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tower::ServiceExt;
use tower_http::services::ServeFile;
use tracing::{error, info};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

pub(crate) const DEFAULT_SUMMARY_WINDOW_MS: i64 = 120_000;
const MIN_SUMMARY_WINDOW_MS: i64 = 10_000;
const MAX_SUMMARY_WINDOW_MS: i64 = 3_600_000;
/// 内置前端写入的鉴权 cookie 名。
const TOKEN_COOKIE: &str = "diting_token";

#[derive(OpenApi)]
#[openapi(
    info(title = "Diting Meeting API", version = "0.1.0", description = "SQLite-backed meeting processing service. 配置 DITING_API_TOKEN 后，/api/ 下的接口需要 `Authorization: Bearer <token>`（或 diting_token cookie）。"),
    paths(
        health, create_meeting, list_meetings, get_meeting, delete_meeting, end_meeting,
        create_speaker, list_speakers, upload_segment, list_segments, get_segment_audio,
        list_summaries, get_board, list_board_versions, list_jobs, retry_job,
        meeting_events, update_segment
    ),
    components(schemas(
        CreateMeeting, CreateSpeaker, IdResponse, SummaryDocument, ActionItem, LivekitIngest,
        UpdateSegment, SegmentView, MeetingListItem, MeetingDetail, SpeakerView, JobView,
        SummaryView, BoardVersionView
    )),
    tags((name = "meetings", description = "会议生命周期与处理"), (name = "jobs", description = "后台处理任务"), (name = "system", description = "服务运行状态"))
)]
struct ApiDoc;

pub(crate) fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/styles.css", get(styles_css))
        .route("/health", get(health))
        .route("/api/v1/jobs", get(list_jobs))
        .route("/api/v1/jobs/{id}/retry", post(retry_job))
        .route("/api/v1/meetings", post(create_meeting).get(list_meetings))
        .route(
            "/api/v1/meetings/{id}",
            get(get_meeting).delete(delete_meeting),
        )
        .route("/api/v1/meetings/{id}/end", post(end_meeting))
        .route(
            "/api/v1/meetings/{id}/speakers",
            post(create_speaker).get(list_speakers),
        )
        .route(
            "/api/v1/meetings/{id}/segments",
            post(upload_segment).get(list_segments),
        )
        .route(
            "/api/v1/meetings/{id}/segments/{segment_id}",
            axum::routing::patch(update_segment),
        )
        .route(
            "/api/v1/meetings/{id}/segments/{segment_id}/audio",
            get(get_segment_audio),
        )
        .route("/api/v1/meetings/{id}/summaries", get(list_summaries))
        .route("/api/v1/meetings/{id}/events", get(meeting_events))
        .route("/api/v1/meetings/{id}/board", get(get_board))
        .route(
            "/api/v1/meetings/{id}/board/versions",
            get(list_board_versions),
        )
        .layer(middleware::from_fn_with_state(state.clone(), require_token))
        .layer(DefaultBodyLimit::max(state.max_upload_bytes))
        .with_state(state)
}

/// 配置了 DITING_API_TOKEN 时，/api/ 下的接口需要携带令牌；页面、文档与 /health 不受限。
/// EventSource 和 `<audio>` 无法自定义请求头，内置前端改用同源 cookie 携带令牌。
async fn require_token(State(s): State<AppState>, request: Request, next: Next) -> Response {
    let Some(expected) = s.api_token.as_deref() else {
        return next.run(request).await;
    };
    if !request.uri().path().starts_with("/api/")
        || request_token_matches(request.headers(), expected)
    {
        return next.run(request).await;
    }
    (
        StatusCode::UNAUTHORIZED,
        [(header::WWW_AUTHENTICATE, "Bearer")],
        Json(json!({"error": "missing or invalid API token"})),
    )
        .into_response()
}

fn request_token_matches(headers: &HeaderMap, expected: &str) -> bool {
    let bearer = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .map(str::trim);
    let cookie = headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(';'))
        .filter_map(|pair| pair.trim().strip_prefix(TOKEN_COOKIE)?.strip_prefix('='))
        .map(percent_decode)
        .find(|token| constant_time_eq(token, expected));
    bearer.is_some_and(|token| constant_time_eq(token, expected)) || cookie.is_some()
}

fn constant_time_eq(a: &str, b: &str) -> bool {
    a.len() == b.len()
        && a.bytes()
            .zip(b.bytes())
            .fold(0u8, |acc, (x, y)| acc | (x ^ y))
            == 0
}

/// 解码前端 `encodeURIComponent` 写入的 cookie 值。
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        let hex = || {
            std::str::from_utf8(bytes.get(i + 1..i + 3)?)
                .ok()
                .and_then(|h| u8::from_str_radix(h, 16).ok())
        };
        match (bytes[i], hex()) {
            (b'%', Some(byte)) => {
                out.push(byte);
                i += 3;
            }
            (byte, _) => {
                out.push(byte);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[derive(Deserialize, utoipa::ToSchema)]
pub(crate) struct CreateMeeting {
    /// 会议标题
    title: String,
    /// 滚动摘要窗口（毫秒），默认 120000（2 分钟）；实时场景可调小，范围 10000-3600000
    #[serde(default)]
    summary_window_ms: Option<i64>,
    /// 可选：携带 LiveKit 连接信息时，服务会以 bot 身份进房订阅音频并自行切窗转写
    #[serde(default)]
    livekit: Option<LivekitIngest>,
}
#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct IdResponse {
    id: String,
}
#[derive(Deserialize, utoipa::ToSchema)]
pub(crate) struct CreateSpeaker {
    /// 说话人显示名称
    name: String,
}

#[derive(Deserialize, utoipa::ToSchema)]
pub(crate) struct UpdateSegment {
    /// 新的转写文本（trim 后为空则忽略该字段）
    pub transcript: Option<String>,
    /// 修改该分段所属说话人的显示名称：同一说话人（同一 LiveKit 参会者）的全部分段一起改名，
    /// 不会把不同参会者合并。分段尚无说话人时新建一个。
    pub speaker_name: Option<String>,
    /// 人工指定语义类型：decision/report/question/action/other；设置后不再被 LLM 分类覆盖
    pub semantic_type: Option<String>,
}

#[derive(Deserialize)]
pub(crate) struct MeetingListFilter {
    status: Option<String>,
    limit: Option<i64>,
    offset: Option<i64>,
}

#[derive(Deserialize)]
pub(crate) struct JobFilter {
    meeting_id: Option<String>,
    status: Option<String>,
}

#[derive(Default, Deserialize)]
pub(crate) struct SegmentPage {
    pub after_sequence: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub(crate) struct MeetingListItem {
    id: String,
    title: String,
    status: String,
    started_at: Option<String>,
    ended_at: Option<String>,
    created_at: String,
    summary_window_ms: i64,
    board_version: i64,
    segment_count: i64,
    transcribed_count: i64,
    speaker_count: i64,
    summary_count: i64,
}

#[derive(Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub(crate) struct MeetingDetail {
    pub id: String,
    pub title: String,
    /// running、ending 或 ended
    pub status: String,
    pub started_at: Option<String>,
    pub ended_at: Option<String>,
    pub board_version: i64,
    pub next_summary_end_ms: i64,
    pub summary_window_ms: i64,
    /// idle、running、stopped 或 failed
    pub ingest_status: String,
    pub ingest_error: Option<String>,
    pub pending_jobs: i64,
    pub failed_jobs: i64,
    #[serde(skip)]
    unsummarized: bool,
    /// 会议已结束且所有转写、摘要都已处理完
    #[sqlx(skip)]
    pub processing_complete: bool,
}

#[derive(Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub(crate) struct SpeakerView {
    id: String,
    name: String,
}

#[derive(Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub(crate) struct JobView {
    id: String,
    job_type: String,
    meeting_id: String,
    target_id: Option<String>,
    status: String,
    retry_count: i64,
    available_at: String,
    error_message: Option<String>,
}

#[derive(Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub(crate) struct SummaryView {
    id: String,
    window_start_ms: i64,
    window_end_ms: i64,
    #[sqlx(rename = "content_json")]
    #[schema(value_type = SummaryDocument)]
    content: DbJson<Value>,
    created_at: String,
}

#[derive(Serialize, sqlx::FromRow, utoipa::ToSchema)]
pub(crate) struct BoardVersionView {
    version: i64,
    source_summary_id: String,
    #[sqlx(rename = "content_json")]
    #[schema(value_type = Object)]
    content: DbJson<Value>,
    created_at: String,
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/events", tag = "meetings", summary = "订阅会议实时事件", description = "以 SSE 实时推送该会议的事件：segment.partial（流式 ASR 临时字幕）、segment.uploaded、segment.transcribed、segment.updated（含人工修订与语义分类结果）、segment.failed、summary.created、board.updated、meeting.ended、ingest.*。订阅建立后和每次重连时请通过 segments/summaries/board 接口补拉快照；收到 resync.required 时也应补拉。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "事件流（text/event-stream）", content_type = "text/event-stream"), (status = 404, description = "会议不存在")))]
async fn meeting_events(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let receiver = s.events.subscribe(&meeting_id);
    let stream = BroadcastStream::new(receiver).map(|message| {
        Ok(match message {
            Ok(MeetingEvent { kind, data }) => Event::default().event(kind).data(data.to_string()),
            Err(_) => Event::default().event("resync.required").data("{}"),
        })
    });
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

#[utoipa::path(post, path = "/api/v1/meetings", tag = "meetings", summary = "创建会议", description = "创建一个进行中的会议并返回会议 ID。后续说话人和音频分段都通过该 ID 关联。可通过 summary_window_ms 调整滚动摘要窗口（默认 2 分钟）。", request_body = CreateMeeting, responses((status = 201, description = "会议已创建", body = IdResponse), (status = 400, description = "标题为空或摘要窗口越界")))]
async fn create_meeting(
    State(s): State<AppState>,
    Json(input): Json<CreateMeeting>,
) -> Result<(StatusCode, Json<IdResponse>), AppError> {
    let title = input.title.trim();
    if title.is_empty() {
        return Err(AppError::BadRequest("title is required".into()));
    }
    let window = input.summary_window_ms.unwrap_or(DEFAULT_SUMMARY_WINDOW_MS);
    if !(MIN_SUMMARY_WINDOW_MS..=MAX_SUMMARY_WINDOW_MS).contains(&window) {
        return Err(AppError::BadRequest(format!(
            "summary_window_ms must be between {MIN_SUMMARY_WINDOW_MS} and {MAX_SUMMARY_WINDOW_MS}"
        )));
    }
    let id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO meetings(id,title,status,started_at,next_summary_end_ms,summary_window_ms) VALUES(?,?, 'running', CURRENT_TIMESTAMP, ?, ?)")
        .bind(&id)
        .bind(title)
        .bind(window)
        .bind(window)
        .execute(&s.db)
        .await?;
    match input.livekit {
        Some(cfg) => {
            spawn_ingest(&s, &id, cfg);
            info!(meeting_id = %id, title, "meeting created, livekit ingest requested");
        }
        None => {
            info!(meeting_id = %id, title, "meeting created without livekit config, transcription only via segment uploads")
        }
    }
    Ok((StatusCode::CREATED, Json(IdResponse { id })))
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../frontend/index.html"))
}

async fn app_js() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/javascript; charset=utf-8")],
        include_str!("../frontend/app.js"),
    )
}

async fn styles_css() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../frontend/styles.css"),
    )
}

#[utoipa::path(get, path = "/health", tag = "system", summary = "检查服务健康状态", description = "检查 SQLite 连接，并返回待处理和失败任务数量。此接口不依赖具体会议，也不需要鉴权。", responses((status = 200, description = "服务健康", body = Value)))]
async fn health(State(s): State<AppState>) -> Result<Json<Value>, AppError> {
    let (pending, failed): (i64, i64) = sqlx::query_as(
        "SELECT COALESCE(SUM(status='pending'),0), COALESCE(SUM(status='failed'),0) FROM jobs WHERE status IN ('pending','failed')",
    )
    .fetch_one(&s.db)
    .await?;
    Ok(Json(
        json!({"status":"ok","database":"ok","jobs":{"pending":pending,"failed":failed}}),
    ))
}

#[utoipa::path(get, path = "/api/v1/jobs", tag = "jobs", summary = "查询后台任务", description = "按会议和任务状态查询最近 100 条后台任务。任务包括转写、分类、校对、Summary 和 rebuild；已结束会议中过期的已完成任务会被定期清理。", params(("meeting_id" = Option<String>, Query, description = "按会议 ID 过滤"), ("status" = Option<String>, Query, description = "按状态过滤：pending、running、completed 或 failed")), responses((status = 200, description = "任务列表", body = [JobView]), (status = 400, description = "状态参数无效")))]
async fn list_jobs(
    State(s): State<AppState>,
    Query(filter): Query<JobFilter>,
) -> Result<Json<Vec<JobView>>, AppError> {
    if let Some(ref status) = filter.status {
        if !matches!(
            status.as_str(),
            "pending" | "running" | "completed" | "failed"
        ) {
            return Err(AppError::BadRequest("invalid job status".into()));
        }
    }
    let rows = sqlx::query_as(
        "SELECT id,job_type,meeting_id,target_id,status,retry_count,available_at,error_message
         FROM jobs WHERE (?1 IS NULL OR meeting_id=?1) AND (?2 IS NULL OR status=?2)
         ORDER BY available_at DESC LIMIT 100",
    )
    .bind(&filter.meeting_id)
    .bind(&filter.status)
    .fetch_all(&s.db)
    .await?;
    Ok(Json(rows))
}

#[utoipa::path(post, path = "/api/v1/jobs/{id}/retry", tag = "jobs", summary = "重试失败任务", description = "将 failed 任务重置为 pending，并立即唤醒对应的 Worker。", params(("id" = String, Path, description = "任务 ID")), responses((status = 200, description = "任务已重新排队", body = Value), (status = 400, description = "任务不存在或当前不可重试")))]
async fn retry_job(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let job_type: String = sqlx::query_scalar(
        "UPDATE jobs SET status='pending',retry_count=0,available_at=CURRENT_TIMESTAMP,error_message=NULL
         WHERE id=? AND status='failed' RETURNING job_type",
    )
    .bind(&id)
    .fetch_optional(&s.db)
    .await?
    .ok_or_else(|| AppError::BadRequest("job does not exist or is not failed".into()))?;
    s.job_notify.wake(lane_of(&job_type));
    Ok(Json(json!({"id":id,"status":"pending"})))
}

#[utoipa::path(get, path = "/api/v1/meetings", tag = "meetings", summary = "会议列表", description = "按创建时间倒序返回会议，并附带每个会议的音频分段数、已完成转写数、说话人数与 Summary 数，用于会议列表页展示。", params(("status" = Option<String>, Query, description = "按状态过滤：running 或 ended"), ("limit" = Option<i64>, Query, description = "返回条数，默认 50，最大 200"), ("offset" = Option<i64>, Query, description = "分页偏移量，默认 0")), responses((status = 200, description = "会议列表", body = [MeetingListItem]), (status = 400, description = "状态参数无效")))]
async fn list_meetings(
    State(s): State<AppState>,
    Query(filter): Query<MeetingListFilter>,
) -> Result<Json<Vec<MeetingListItem>>, AppError> {
    if let Some(ref status) = filter.status {
        if status != "running" && status != "ended" {
            return Err(AppError::BadRequest(
                "status must be running or ended".into(),
            ));
        }
    }
    let limit = filter.limit.unwrap_or(50).clamp(1, 200);
    let offset = filter.offset.unwrap_or(0).max(0);
    let rows = sqlx::query_as(
        "SELECT m.id,m.title,m.status,m.started_at,m.ended_at,m.created_at,
                m.summary_window_ms,m.board_version,
         (SELECT COUNT(*) FROM audio_segments a WHERE a.meeting_id=m.id) AS segment_count,
         (SELECT COUNT(*) FROM audio_segments a WHERE a.meeting_id=m.id AND a.status='completed') AS transcribed_count,
         (SELECT COUNT(*) FROM speakers sp WHERE sp.meeting_id=m.id) AS speaker_count,
         (SELECT COUNT(*) FROM rolling_summaries rs WHERE rs.meeting_id=m.id) AS summary_count
         FROM meetings m WHERE (?1 IS NULL OR m.status=?1)
         ORDER BY m.created_at DESC, m.id LIMIT ?2 OFFSET ?3",
    )
    .bind(&filter.status)
    .bind(limit)
    .bind(offset)
    .fetch_all(&s.db)
    .await?;
    Ok(Json(rows))
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}", tag = "meetings", summary = "获取会议详情", description = "返回会议状态、开始/结束时间、Board 版本、下一个 Summary 窗口、进房采集状态与任务积压情况。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "会议详情", body = MeetingDetail), (status = 404, description = "会议不存在")))]
pub(crate) async fn get_meeting(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<MeetingDetail>, AppError> {
    let mut meeting: MeetingDetail = sqlx::query_as("SELECT id,title,status,started_at,ended_at,board_version,next_summary_end_ms,summary_window_ms,ingest_status,ingest_error,
        (SELECT COUNT(*) FROM jobs WHERE meeting_id=meetings.id AND status IN ('pending','running')) pending_jobs,
        (SELECT COUNT(*) FROM jobs WHERE meeting_id=meetings.id AND status='failed') failed_jobs,
        (SELECT COALESCE(MAX(end_ms),0) FROM audio_segments WHERE meeting_id=meetings.id AND status='completed') >
        (SELECT COALESCE(MAX(window_end_ms),0) FROM rolling_summaries WHERE meeting_id=meetings.id) unsummarized
        FROM meetings WHERE id=?").bind(&id).fetch_optional(&s.db).await?.ok_or(AppError::NotFound)?;
    meeting.processing_complete = meeting.status == "ended"
        && meeting.pending_jobs == 0
        && (!meeting.unsummarized || meeting.failed_jobs > 0)
        && !s.ingest_stop.lock().unwrap().contains_key(&id);
    Ok(Json(meeting))
}

#[utoipa::path(delete, path = "/api/v1/meetings/{id}", tag = "meetings", summary = "删除会议", description = "事务删除会议及全部关联记录，并在成功后删除本地音频目录。该操作不可恢复。", params(("id" = String, Path, description = "会议 ID")), responses((status = 204, description = "删除成功"), (status = 404, description = "会议不存在")))]
pub(crate) async fn delete_meeting(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    ensure_meeting(&s.db, &id).await?;
    sqlx::query("UPDATE meetings SET status='ending' WHERE id=? AND status='running'")
        .bind(&id)
        .execute(&s.db)
        .await?;
    stop_ingest(&s, &id).await.map_err(AppError::Processing)?;
    let mut tx = s.db.begin().await?;
    for statement in [
        "DELETE FROM jobs WHERE meeting_id=?",
        "DELETE FROM meeting_board_versions WHERE meeting_id=?",
        "DELETE FROM meeting_boards WHERE meeting_id=?",
        "DELETE FROM rolling_summaries WHERE meeting_id=?",
        "DELETE FROM audio_segments WHERE meeting_id=?",
        "DELETE FROM speakers WHERE meeting_id=?",
        "DELETE FROM meetings WHERE id=?",
    ] {
        sqlx::query(statement).bind(&id).execute(&mut *tx).await?;
    }
    tx.commit().await?;
    s.events.close(&id);
    let audio_dir = s.audio_dir.join(&id);
    if let Err(error) = fs::remove_dir_all(&audio_dir).await {
        if error.kind() != std::io::ErrorKind::NotFound {
            error!(%error, path = %audio_dir.display(), meeting_id = %id, "meeting records deleted but audio cleanup failed");
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post, path = "/api/v1/meetings/{id}/end", tag = "meetings", summary = "结束会议", description = "将会议标记为 ended，并为最后不足一个窗口的音频安排最终 Summary。进房采集尚未保存完尾包时返回 status=ending，由采集任务完成收尾。接口幂等。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "会议已结束或正在收尾", body = Value), (status = 404, description = "会议不存在")))]
pub(crate) async fn end_meeting(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    ensure_meeting(&s.db, &id).await?;
    sqlx::query("UPDATE meetings SET status='ending' WHERE id=? AND status='running'")
        .bind(&id)
        .execute(&s.db)
        .await?;
    match stop_ingest(&s, &id).await {
        Ok(()) => complete_meeting_end(&s, &id).await?,
        Err(error) if error.starts_with("timed out") => {
            // The ingest task owns finalization once all queued audio is saved.
            return Ok(Json(json!({"status":"ending"})));
        }
        Err(error) => return Err(AppError::Processing(error)),
    }
    Ok(Json(json!({"status":"ended"})))
}

pub(crate) async fn complete_meeting_end(s: &AppState, id: &str) -> Result<(), AppError> {
    sqlx::query("UPDATE meetings SET status='ended', ended_at=COALESCE(ended_at,CURRENT_TIMESTAMP) WHERE id=?")
        .bind(id).execute(&s.db).await?;
    enqueue_summary(&s.db, id, true).await?;
    s.job_notify.wake("summary");
    publish_event(s, id, "meeting.ended", json!({"meeting_id":id}));
    Ok(())
}

#[utoipa::path(post, path = "/api/v1/meetings/{id}/speakers", tag = "meetings", summary = "添加说话人", description = "为会议登记一个说话人。创建后可在上传音频分段时通过 speaker_id 关联。", params(("id" = String, Path, description = "会议 ID")), request_body = CreateSpeaker, responses((status = 201, description = "说话人已创建", body = IdResponse), (status = 404, description = "会议不存在")))]
async fn create_speaker(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
    Json(input): Json<CreateSpeaker>,
) -> Result<(StatusCode, Json<IdResponse>), AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let name = input.name.trim();
    if name.is_empty() {
        return Err(AppError::BadRequest("name is required".into()));
    }
    let id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO speakers(id,meeting_id,name) VALUES(?,?,?)")
        .bind(&id)
        .bind(&meeting_id)
        .bind(name)
        .execute(&s.db)
        .await?;
    Ok((StatusCode::CREATED, Json(IdResponse { id })))
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/speakers", tag = "meetings", summary = "列出说话人", description = "按创建时间返回会议中的全部说话人。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "说话人列表", body = [SpeakerView]), (status = 404, description = "会议不存在")))]
async fn list_speakers(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Vec<SpeakerView>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let rows =
        sqlx::query_as("SELECT id,name FROM speakers WHERE meeting_id=? ORDER BY created_at")
            .bind(meeting_id)
            .fetch_all(&s.db)
            .await?;
    Ok(Json(rows))
}

/// 上传过程中写入的音频文件；未 `keep()` 就被丢弃时（校验失败、入库失败）自动删除。
struct UploadedAudio {
    path: Option<PathBuf>,
}

impl UploadedAudio {
    fn keep(mut self) -> String {
        self.path
            .take()
            .map(|p| p.to_string_lossy().into_owned())
            .unwrap_or_default()
    }
}

impl Drop for UploadedAudio {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            if let Err(error) = std::fs::remove_file(&path) {
                if error.kind() != std::io::ErrorKind::NotFound {
                    error!(%error, path = %path.display(), "failed to clean up uploaded audio");
                }
            }
        }
    }
}

/// 把 multipart 音频字段流式写入磁盘，不在内存中缓冲整个文件。空文件视为未提供。
async fn save_audio_field(
    mut field: axum::extract::multipart::Field<'_>,
    path: PathBuf,
    max_bytes: usize,
) -> Result<Option<UploadedAudio>, AppError> {
    if let Some(dir) = path.parent() {
        fs::create_dir_all(dir)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
    }
    let mut file = fs::File::create(&path)
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    let guard = UploadedAudio { path: Some(path) };
    let mut written = 0usize;
    while let Some(chunk) = field
        .chunk()
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?
    {
        written += chunk.len();
        if written > max_bytes {
            return Err(AppError::BadRequest(format!(
                "audio exceeds {max_bytes} byte upload limit"
            )));
        }
        file.write_all(&chunk)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
    }
    file.flush()
        .await
        .map_err(|e| AppError::Internal(e.to_string()))?;
    Ok((written > 0).then_some(guard))
}

#[utoipa::path(post, path = "/api/v1/meetings/{id}/segments", tag = "meetings", summary = "上传音频分段", description = "上传一个会议音频分段并加入转写队列。multipart 字段：audio、speaker_id 或 speaker_name、sequence_no、start_ms、end_ms 和 transcript；时间单位为毫秒。audio 与 transcript 至少提供一个——实时场景下上游已有 ASR 结果时可只传 transcript，跳过音频存储与转写。", params(("id" = String, Path, description = "会议 ID")), responses((status = 201, description = "音频分段已创建", body = IdResponse), (status = 400, description = "字段缺失、时间范围无效、文件超限或会议已结束"), (status = 404, description = "会议不存在")))]
async fn upload_segment(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<IdResponse>), AppError> {
    let status: String = sqlx::query_scalar("SELECT status FROM meetings WHERE id=?")
        .bind(&meeting_id)
        .fetch_optional(&s.db)
        .await?
        .ok_or(AppError::NotFound)?;
    if status != "running" {
        return Err(AppError::BadRequest(
            "audio can only be uploaded to a running meeting".into(),
        ));
    }
    let id = Uuid::new_v4().to_string();
    let mut speaker_id = None;
    let mut speaker_name: Option<String> = None;
    let mut sequence_no: Option<i64> = None;
    let mut start_ms: Option<i64> = None;
    let mut end_ms: Option<i64> = None;
    let mut audio: Option<UploadedAudio> = None;
    let mut audio_seen = false;
    let mut transcript = None;
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?
    {
        let name = field.name().unwrap_or("").to_string();
        if name == "audio" {
            if audio_seen {
                return Err(AppError::BadRequest(
                    "only one audio file is allowed".into(),
                ));
            }
            audio_seen = true;
            let filename = sanitize_filename(field.file_name().unwrap_or(""));
            let filename = if filename.is_empty() {
                "audio.bin".to_string()
            } else {
                filename
            };
            let path = s
                .audio_dir
                .join(&meeting_id)
                .join(format!("{id}-{filename}"));
            audio = save_audio_field(field, path, s.max_upload_bytes).await?;
        } else {
            let value = field
                .text()
                .await
                .map_err(|e| AppError::BadRequest(e.to_string()))?;
            match name.as_str() {
                "speaker_id" => speaker_id = Some(value),
                "speaker_name" => speaker_name = Some(value),
                "sequence_no" => sequence_no = value.parse().ok(),
                "start_ms" => start_ms = value.parse().ok(),
                "end_ms" => end_ms = value.parse().ok(),
                "transcript" => transcript = Some(value),
                _ => {}
            }
        }
    }
    let (seq, start, end) = (
        sequence_no.ok_or_else(|| AppError::BadRequest("sequence_no is required".into()))?,
        start_ms.ok_or_else(|| AppError::BadRequest("start_ms is required".into()))?,
        end_ms.ok_or_else(|| AppError::BadRequest("end_ms is required".into()))?,
    );
    let transcript = transcript
        .map(|text| text.trim().to_string())
        .filter(|text| !text.is_empty());
    if audio.is_none() && transcript.is_none() {
        return Err(AppError::BadRequest(
            "audio or transcript is required".into(),
        ));
    }
    let transcript_was_provided = transcript.is_some();
    if end <= start {
        return Err(AppError::BadRequest(
            "end_ms must be greater than start_ms".into(),
        ));
    }
    if seq < 0 || start < 0 {
        return Err(AppError::BadRequest(
            "sequence_no, start_ms and end_ms must be non-negative".into(),
        ));
    }
    // 未指定 speaker_id 时按名字自动建档/复用
    if speaker_id.is_none() {
        if let Some(name) = speaker_name
            .map(|n| n.trim().to_string())
            .filter(|n| !n.is_empty())
        {
            speaker_id =
                Some(livekit_ingest::ensure_speaker_by_name(&s.db, &meeting_id, &name).await?);
        }
    }
    if let Some(ref speaker) = speaker_id {
        let belongs_to_meeting = sqlx::query("SELECT 1 FROM speakers WHERE id=? AND meeting_id=?")
            .bind(speaker)
            .bind(&meeting_id)
            .fetch_optional(&s.db)
            .await?
            .is_some();
        if !belongs_to_meeting {
            return Err(AppError::BadRequest(
                "speaker_id does not belong to this meeting".into(),
            ));
        }
    }
    let sequence_taken =
        sqlx::query("SELECT 1 FROM audio_segments WHERE meeting_id=? AND sequence_no=?")
            .bind(&meeting_id)
            .bind(seq)
            .fetch_optional(&s.db)
            .await?
            .is_some();
    if sequence_taken {
        return Err(AppError::BadRequest(
            "sequence_no already exists for this meeting".into(),
        ));
    }
    let file_path = audio
        .as_ref()
        .and_then(|a| a.path.as_ref())
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_default();
    insert_segment(
        &s.db,
        &id,
        &meeting_id,
        speaker_id.as_deref(),
        seq,
        start,
        end,
        &file_path,
        transcript,
    )
    .await?;
    // 入库成功后才保留音频文件；此前任何错误返回都会删除它。
    let has_audio = audio.map(UploadedAudio::keep).is_some();
    info!(
        meeting_id = %meeting_id,
        segment_id = %id,
        sequence_no = seq,
        start_ms = start,
        end_ms = end,
        has_audio,
        transcript_provided = transcript_was_provided,
        "segment uploaded"
    );
    s.job_notify.wake("asr");
    publish_event(
        &s,
        &meeting_id,
        "segment.uploaded",
        segment_payload(&s.db, &meeting_id, &id).await?,
    );
    Ok((StatusCode::CREATED, Json(IdResponse { id })))
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/segments", tag = "meetings", summary = "列出音频分段", description = "按会议时间线返回音频分段、转写状态和转写文本。可用 after_sequence（上一页最大 sequence_no）+ limit 分页；返回结果按 start_ms 排序。", params(("id" = String, Path, description = "会议 ID"), ("after_sequence" = Option<i64>, Query, description = "只返回 sequence_no 大于该值的分段"), ("limit" = Option<i64>, Query, description = "返回条数，1-1000，默认不限")), responses((status = 200, description = "音频分段列表", body = [SegmentView]), (status = 404, description = "会议不存在")))]
pub(crate) async fn list_segments(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
    Query(page): Query<SegmentPage>,
) -> Result<Json<Vec<SegmentView>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let rows: Vec<SegmentRow> = sqlx::query_as(&format!(
        "{SEGMENT_SELECT} WHERE a.meeting_id=?1 AND (?2 IS NULL OR a.sequence_no > ?2) ORDER BY a.sequence_no LIMIT ?3"
    ))
    .bind(&meeting_id)
    .bind(page.after_sequence)
    .bind(page.limit.map(|n| n.clamp(1, 1000)).unwrap_or(-1))
    .fetch_all(&s.db)
    .await?;
    let mut items: Vec<SegmentView> = rows.into_iter().map(SegmentView::from).collect();
    // Preserve chronological order for existing clients; cursor is sequence_no.
    items.sort_by_key(|v| (v.start_ms, v.sequence_no));
    Ok(Json(items))
}

#[utoipa::path(patch, path = "/api/v1/meetings/{id}/segments/{segment_id}", tag = "meetings", summary = "编辑音频分段", description = "人工修订转写文本、修改分段所属说话人的名称，或指定语义类型（decision/report/question/action/other）。人工修订的文本和语义类型不会被后台 ASR/LLM 结果覆盖；修改说话人名称会作用于该说话人的全部分段。文本或说话人变化会从最早受影响的窗口重建滚动摘要与 Board。成功后广播 segment.updated 事件。", params(("id" = String, Path, description = "会议 ID"), ("segment_id" = String, Path, description = "分段 ID")), request_body = UpdateSegment, responses((status = 200, description = "分段已更新", body = SegmentView), (status = 400, description = "没有可更新的字段或语义类型非法"), (status = 404, description = "会议或分段不存在")))]
pub(crate) async fn update_segment(
    State(s): State<AppState>,
    Path((meeting_id, segment_id)): Path<(String, String)>,
    Json(body): Json<UpdateSegment>,
) -> Result<Json<Value>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let transcript = body
        .transcript
        .map(|t| t.trim().to_string())
        .filter(|t| !t.is_empty());
    let speaker_name = body
        .speaker_name
        .map(|n| n.trim().to_string())
        .filter(|n| !n.is_empty());
    let semantic_type = body
        .semantic_type
        .map(|t| t.trim().to_lowercase())
        .filter(|t| !t.is_empty());
    if let Some(value) = semantic_type.as_deref() {
        if !SEMANTIC_TYPES.contains(&value) {
            return Err(AppError::BadRequest(format!(
                "semantic_type must be one of: {}",
                SEMANTIC_TYPES.join("/")
            )));
        }
    }
    if transcript.is_none() && speaker_name.is_none() && semantic_type.is_none() {
        return Err(AppError::BadRequest(
            "transcript, speaker_name or semantic_type is required".into(),
        ));
    }
    let segment: Option<(Option<String>, i64, i64)> = sqlx::query_as(
        "SELECT speaker_id,start_ms,end_ms FROM audio_segments WHERE id=? AND meeting_id=?",
    )
    .bind(&segment_id)
    .bind(&meeting_id)
    .fetch_optional(&s.db)
    .await?;
    let Some((current_speaker, start_ms, end_ms)) = segment else {
        return Err(AppError::NotFound);
    };
    let text_changed = transcript.is_some();
    let mut renamed_speaker = None;
    let mut tx = s.db.begin().await?;
    if let Some(text) = transcript {
        sqlx::query("UPDATE audio_segments SET transcript=?, status='completed', transcript_manual_override=1 WHERE id=? AND meeting_id=?")
            .bind(text).bind(&segment_id).bind(&meeting_id).execute(&mut *tx).await?;
    }
    if let Some(name) = speaker_name {
        // Editing a label must not merge two distinct LiveKit identities: rename in place.
        let id = current_speaker.unwrap_or_else(|| Uuid::new_v4().to_string());
        sqlx::query("INSERT INTO speakers(id,meeting_id,name) VALUES(?,?,?) ON CONFLICT(id) DO UPDATE SET name=excluded.name")
            .bind(&id).bind(&meeting_id).bind(name).execute(&mut *tx).await?;
        sqlx::query("UPDATE audio_segments SET speaker_id=? WHERE id=?")
            .bind(&id)
            .bind(&segment_id)
            .execute(&mut *tx)
            .await?;
        renamed_speaker = Some(id);
    }
    if let Some(value) = semantic_type {
        sqlx::query(
            "UPDATE audio_segments SET semantic_type=?, semantic_manual_override=1 WHERE id=?",
        )
        .bind(value)
        .bind(&segment_id)
        .execute(&mut *tx)
        .await?;
    }
    sqlx::query("UPDATE audio_segments SET revision=revision+1 WHERE id=?")
        .bind(&segment_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query("UPDATE meetings SET transcript_revision=transcript_revision+1 WHERE id=?")
        .bind(&meeting_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    if text_changed || renamed_speaker.is_some() {
        // 改名影响该说话人的全部分段；只改文本时只影响本分段所在窗口。
        let (from, to) = match &renamed_speaker {
            Some(speaker) => sqlx::query_as::<_, (i64, i64)>(
                "SELECT MIN(start_ms), MAX(end_ms) FROM audio_segments WHERE meeting_id=? AND speaker_id=?",
            )
            .bind(&meeting_id)
            .bind(speaker)
            .fetch_one(&s.db)
            .await?,
            None => (start_ms, end_ms),
        };
        match earliest_summary_overlapping(&s.db, &meeting_id, from, to).await? {
            Some(window_start) => enqueue_rebuild(&s.db, &meeting_id, window_start).await?,
            // 人工补录的文本可能让一个等待中的窗口变得可以生成
            None => {
                let ended = meeting_ended(&s.db, &meeting_id).await?;
                enqueue_summary(&s.db, &meeting_id, ended).await?;
            }
        }
        if text_changed {
            enqueue_classification(&s.db, &meeting_id, &segment_id).await?;
            s.job_notify.wake("llm");
        }
        s.job_notify.wake("summary");
    }
    let payload = segment_payload(&s.db, &meeting_id, &segment_id).await?;
    publish_event(&s, &meeting_id, "segment.updated", payload.clone());
    Ok(Json(payload))
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/segments/{segment_id}/audio", tag = "meetings", summary = "下载分段音频", description = "返回分段关联的音频文件，支持 HTTP Range 请求，可直接用于浏览器播放和拖动；分段不存在、纯文本分段或音频文件缺失时返回 404。", params(("id" = String, Path, description = "会议 ID"), ("segment_id" = String, Path, description = "分段 ID")), responses((status = 200, description = "音频文件", body = Vec<u8>), (status = 206, description = "音频文件片段（Range 请求）"), (status = 404, description = "会议、分段或音频文件不存在")))]
async fn get_segment_audio(
    State(s): State<AppState>,
    Path((meeting_id, segment_id)): Path<(String, String)>,
    request: Request,
) -> Result<Response, AppError> {
    let file_path: String =
        sqlx::query_scalar("SELECT file_path FROM audio_segments WHERE id=? AND meeting_id=?")
            .bind(&segment_id)
            .bind(&meeting_id)
            .fetch_optional(&s.db)
            .await?
            .ok_or(AppError::NotFound)?;
    if file_path.is_empty() {
        return Err(AppError::NotFound);
    }
    let path = PathBuf::from(&file_path);
    let mut response = ServeFile::new(&path)
        .oneshot(request)
        .await
        .unwrap_or_else(|e: Infallible| match e {})
        .map(axum::body::Body::new);
    if response.status() == StatusCode::NOT_FOUND {
        error!(path = %file_path, "segment audio file is missing");
        return Err(AppError::NotFound);
    }
    let filename = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("audio.bin")
        .to_string();
    let headers = response.headers_mut();
    headers.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(audio_content_type(&filename)),
    );
    if let Ok(value) = HeaderValue::from_str(&format!("inline; filename=\"{filename}\"")) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }
    Ok(response)
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/summaries", tag = "meetings", summary = "获取滚动摘要", description = "返回会议按滚动窗口（默认 2 分钟）生成的 Summary，迟到音频或人工修订重建后会更新受影响窗口。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "Summary 列表", body = [SummaryView]), (status = 404, description = "会议不存在")))]
async fn list_summaries(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Vec<SummaryView>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let rows = sqlx::query_as("SELECT id,window_start_ms,window_end_ms,content_json,created_at FROM rolling_summaries WHERE meeting_id=? ORDER BY window_end_ms")
        .bind(meeting_id)
        .fetch_all(&s.db)
        .await?;
    Ok(Json(rows))
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/board", tag = "meetings", summary = "获取当前会议板", description = "返回最新 Meeting Board 及其版本号。Board 会随着每次 Summary 更新而增量合并。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "当前会议板", body = Value), (status = 404, description = "会议不存在")))]
async fn get_board(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let row: Option<(i64, DbJson<Value>, String)> = sqlx::query_as(
        "SELECT version,content_json,updated_at FROM meeting_boards WHERE meeting_id=?",
    )
    .bind(meeting_id)
    .fetch_optional(&s.db)
    .await?;
    Ok(Json(match row {
        Some((version, content, updated_at)) => {
            json!({"version": version, "content": content.0, "updated_at": updated_at})
        }
        None => json!({"version": 0, "content": empty_board()}),
    }))
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/board/versions", tag = "meetings", summary = "获取会议板历史版本", description = "按版本号返回 Meeting Board 的历史快照及来源 Summary。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "会议板版本列表", body = [BoardVersionView]), (status = 404, description = "会议不存在")))]
async fn list_board_versions(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Vec<BoardVersionView>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let rows = sqlx::query_as(
        "SELECT version,source_summary_id,content_json,created_at FROM meeting_board_versions
         WHERE meeting_id=? ORDER BY version",
    )
    .bind(meeting_id)
    .fetch_all(&s.db)
    .await?;
    Ok(Json(rows))
}

/// 根据文件名推断音频 Content-Type，未知扩展名回退到 octet-stream。
fn audio_content_type(filename: &str) -> &'static str {
    match filename
        .rsplit('.')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase()
        .as_str()
    {
        "wav" => "audio/wav",
        "mp3" => "audio/mpeg",
        "m4a" | "mp4" => "audio/mp4",
        "aac" => "audio/aac",
        "ogg" | "oga" | "opus" => "audio/ogg",
        "webm" => "audio/webm",
        "flac" => "audio/flac",
        "amr" => "audio/amr",
        _ => "application/octet-stream",
    }
}

pub(crate) fn sanitize_filename(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_is_accepted_from_bearer_header_or_cookie() {
        let mut headers = HeaderMap::new();
        assert!(!request_token_matches(&headers, "s3cr3t"));
        headers.insert(
            header::AUTHORIZATION,
            HeaderValue::from_static("Bearer s3cr3t"),
        );
        assert!(request_token_matches(&headers, "s3cr3t"));
        let mut headers = HeaderMap::new();
        headers.insert(
            header::COOKIE,
            HeaderValue::from_static("other=1; diting_token=a%2Bb%3D"),
        );
        assert!(request_token_matches(&headers, "a+b="));
        assert!(!request_token_matches(&headers, "a+b"));
    }
}
