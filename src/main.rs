use async_trait::async_trait;
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::StatusCode,
    response::{sse::{Event, KeepAlive, Sse}, Html, IntoResponse, Response},
    routing::{delete as delete_route, get, patch, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Row, SqlitePool,
};
use std::{convert::Infallible, env, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};
use tokio::{
    fs,
    sync::{broadcast, Notify},
    time::{sleep, Duration},
};
use tokio_stream::{wrappers::BroadcastStream, StreamExt};
use tracing::{error, info, warn};

mod livekit_ingest;
use livekit_ingest::{spawn_ingest, stop_ingest, IngestStopMap, LivekitIngest};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;
use uuid::Uuid;

#[derive(OpenApi)]
#[openapi(
    info(title = "Diting Meeting API", version = "0.1.0", description = "SQLite-backed meeting processing service"),
    paths(
        health, create_meeting, get_meeting, delete_meeting, end_meeting,
        create_speaker, list_speakers, upload_segment, list_segments,
        list_summaries, get_board, list_board_versions, list_jobs, retry_job,
        meeting_events, update_segment
    ),
    components(schemas(CreateMeeting, CreateSpeaker, IdResponse, SummaryDocument, ActionItem, LivekitIngest, UpdateSegment)),
    tags((name = "meetings", description = "会议生命周期与处理"), (name = "jobs", description = "后台处理任务"), (name = "system", description = "服务运行状态"))
)]
struct ApiDoc;

#[derive(Clone)]
struct OpenAiTranscriber {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
}

#[async_trait]
impl Transcriber for OpenAiTranscriber {
    async fn transcribe(&self, file_path: &str, existing: Option<&str>) -> Result<String, String> {
        if let Some(text) = existing.map(str::trim).filter(|text| !text.is_empty()) {
            return Ok(text.to_string());
        }
        let bytes = tokio::fs::read(file_path)
            .await
            .map_err(|e| e.to_string())?;
        let filename = std::path::Path::new(file_path)
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("audio.bin")
            .to_string();
        let part = reqwest::multipart::Part::bytes(bytes).file_name(filename);
        let form = reqwest::multipart::Form::new()
            .text("model", self.model.clone())
            .part("file", part);
        let response = self
            .client
            .post(format!(
                "{}/audio/transcriptions",
                self.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.api_key)
            .multipart(form)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = response.status();
        let body: Value = response.json().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("ASR provider returned {}: {}", status, body));
        }
        body.get("text")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| "ASR response does not contain text".into())
    }
}

#[derive(Clone)]
struct OpenAiSummarizer {
    client: reqwest::Client,
    base_url: String,
    api_key: String,
    model: String,
}

#[async_trait]
impl Summarizer for OpenAiSummarizer {
    async fn summarize(
        &self,
        start_ms: i64,
        end_ms: i64,
        transcript: &str,
    ) -> Result<SummaryDocument, String> {
        let system = "Return only valid JSON matching this schema: {\"topics\":[],\"decisions\":[],\"action_items\":[{\"content\":\"\",\"owner\":null,\"due_date\":null,\"status\":\"open\"}],\"open_questions\":[],\"risks\":[],\"key_points\":[]}. Extract facts from the meeting transcript. Do not invent details.";
        let user = format!("Meeting window {start_ms}-{end_ms} ms.\nTranscript:\n{transcript}");
        let request = json!({
            "model": self.model,
            "temperature": 0,
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user}
            ]
        });
        let response = self
            .client
            .post(format!(
                "{}/chat/completions",
                self.base_url.trim_end_matches('/')
            ))
            .bearer_auth(&self.api_key)
            .json(&request)
            .send()
            .await
            .map_err(|e| e.to_string())?;
        let status = response.status();
        let body: Value = response.json().await.map_err(|e| e.to_string())?;
        if !status.is_success() {
            return Err(format!("LLM provider returned {}: {}", status, body));
        }
        let content = body
            .pointer("/choices/0/message/content")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                "LLM response does not contain choices[0].message.content".to_string()
            })?;
        let json_text = content
            .trim()
            .strip_prefix("```json")
            .and_then(|value| value.strip_suffix("```"))
            .unwrap_or(content)
            .trim();
        serde_json::from_str(json_text).map_err(|e| format!("invalid SummaryDocument JSON: {e}"))
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, utoipa::ToSchema)]
struct SummaryDocument {
    /// 讨论主题
    #[serde(default)]
    topics: Vec<String>,
    /// 已确认的决策
    #[serde(default)]
    decisions: Vec<String>,
    /// 待执行的行动项
    #[serde(default)]
    action_items: Vec<ActionItem>,
    /// 尚未解决的问题
    #[serde(default)]
    open_questions: Vec<String>,
    /// 会议风险
    #[serde(default)]
    risks: Vec<String>,
    /// 关键点
    #[serde(default)]
    key_points: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default, utoipa::ToSchema)]
struct ActionItem {
    /// 行动项内容
    content: String,
    /// 负责人
    #[serde(default)]
    owner: Option<String>,
    /// 截止日期，通常为 ISO 日期
    #[serde(default)]
    due_date: Option<String>,
    /// open、in_progress、done 或 blocked
    #[serde(default = "default_action_status")]
    status: String,
}

fn default_action_status() -> String {
    "open".into()
}

#[async_trait]
trait Transcriber: Send + Sync {
    async fn transcribe(&self, file_path: &str, existing: Option<&str>) -> Result<String, String>;
}

#[async_trait]
trait Summarizer: Send + Sync {
    async fn summarize(
        &self,
        start_ms: i64,
        end_ms: i64,
        transcript: &str,
    ) -> Result<SummaryDocument, String>;
}

struct LocalTranscriber;

#[async_trait]
impl Transcriber for LocalTranscriber {
    async fn transcribe(&self, _file_path: &str, existing: Option<&str>) -> Result<String, String> {
        match existing.map(str::trim).filter(|text| !text.is_empty()) {
            Some(text) => Ok(text.to_string()),
            None => Ok("[transcript provider not configured]".to_string()),
        }
    }
}

struct LocalSummarizer;

#[async_trait]
impl Summarizer for LocalSummarizer {
    async fn summarize(
        &self,
        _start_ms: i64,
        _end_ms: i64,
        transcript: &str,
    ) -> Result<SummaryDocument, String> {
        let points: Vec<String> = transcript
            .split(['。', '.', '\n'])
            .map(str::trim)
            .filter(|text| !text.is_empty())
            .map(ToOwned::to_owned)
            .collect();
        Ok(SummaryDocument {
            key_points: points,
            ..SummaryDocument::default()
        })
    }
}

const DEFAULT_SUMMARY_WINDOW_MS: i64 = 300_000;
const MIN_SUMMARY_WINDOW_MS: i64 = 10_000;
const MAX_SUMMARY_WINDOW_MS: i64 = 3_600_000;

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
CREATE TABLE IF NOT EXISTS meetings (
  id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL,
  started_at TEXT, ended_at TEXT, next_summary_end_ms INTEGER NOT NULL DEFAULT 300000,
  summary_window_ms INTEGER NOT NULL DEFAULT 300000,
  board_version INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS speakers (
  id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id), name TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS audio_segments (
  id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id), speaker_id TEXT REFERENCES speakers(id),
  sequence_no INTEGER NOT NULL, start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL,
  file_path TEXT NOT NULL, transcript TEXT, status TEXT NOT NULL DEFAULT 'uploaded',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(meeting_id, sequence_no)
);
CREATE TABLE IF NOT EXISTS rolling_summaries (
  id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id), window_start_ms INTEGER NOT NULL,
  window_end_ms INTEGER NOT NULL, content_json TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'completed',
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(meeting_id, window_end_ms)
);
CREATE TABLE IF NOT EXISTS meeting_boards (
  meeting_id TEXT PRIMARY KEY REFERENCES meetings(id), version INTEGER NOT NULL, content_json TEXT NOT NULL,
  updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS meeting_board_versions (
  id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id), version INTEGER NOT NULL,
  source_summary_id TEXT NOT NULL REFERENCES rolling_summaries(id), content_json TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE(meeting_id, version)
);
CREATE TABLE IF NOT EXISTS jobs (
  id TEXT PRIMARY KEY, job_type TEXT NOT NULL, meeting_id TEXT NOT NULL REFERENCES meetings(id),
  target_id TEXT, status TEXT NOT NULL DEFAULT 'pending', retry_count INTEGER NOT NULL DEFAULT 0,
  available_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, error_message TEXT,
  UNIQUE(job_type, meeting_id, target_id)
);
"#;

#[derive(Clone, Debug)]
struct MeetingEvent {
    meeting_id: String,
    kind: &'static str,
    data: Value,
}

#[derive(Clone)]
struct AppState {
    db: SqlitePool,
    audio_dir: Arc<PathBuf>,
    transcriber: Arc<dyn Transcriber>,
    summarizer: Arc<dyn Summarizer>,
    max_upload_bytes: usize,
    job_notify: Arc<Notify>,
    events: broadcast::Sender<MeetingEvent>,
    /// meeting_id -> 停止信号，结束/删除会议时通知 LiveKit 进房任务退出
    ingest_stop: IngestStopMap,
}

/// 向 SSE 订阅者广播会议事件；没有订阅者时直接丢弃。
fn publish_event(s: &AppState, meeting_id: &str, kind: &'static str, data: Value) {
    let _ = s.events.send(MeetingEvent {
        meeting_id: meeting_id.to_string(),
        kind,
        data,
    });
}

/// 写入音频分段并入队转写任务（HTTP 上传与 LiveKit 进房共用）。
pub(crate) async fn insert_segment(
    db: &SqlitePool,
    id: &str,
    meeting_id: &str,
    speaker_id: Option<&str>,
    seq: i64,
    start: i64,
    end: i64,
    file_path: &str,
    transcript: Option<String>,
) -> Result<(), sqlx::Error> {
    let mut tx = db.begin().await?;
    sqlx::query("INSERT INTO audio_segments(id,meeting_id,speaker_id,sequence_no,start_ms,end_ms,file_path,transcript) VALUES(?,?,?,?,?,?,?,?)")
        .bind(id).bind(meeting_id).bind(speaker_id).bind(seq).bind(start).bind(end).bind(file_path).bind(transcript)
        .execute(&mut *tx).await?;
    sqlx::query("INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES(?, 'transcribe', ?, ?)")
        .bind(Uuid::new_v4().to_string()).bind(meeting_id).bind(id)
        .execute(&mut *tx).await?;
    tx.commit().await
}

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("database operation failed: {0}")]
    Db(sqlx::Error),
    #[error("{0}")]
    BadRequest(String),
    #[error("resource not found")]
    NotFound,
    #[error("processing failed: {0}")]
    Processing(String),
    #[error("internal error: {0}")]
    Internal(String),
}
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            Self::Db(e) => {
                error!(error = %e, "database error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal database error".to_string(),
                )
            }
            Self::BadRequest(m) => (StatusCode::BAD_REQUEST, m),
            Self::NotFound => (StatusCode::NOT_FOUND, "not found".to_string()),
            Self::Processing(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
            Self::Internal(m) => {
                error!(error = %m, "internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            }
        };
        (status, Json(json!({"error": message}))).into_response()
    }
}
impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

#[derive(Deserialize, utoipa::ToSchema)]
struct CreateMeeting {
    /// 会议标题
    title: String,
    /// 滚动摘要窗口（毫秒），默认 300000（5 分钟）；实时场景可调小，范围 10000-3600000
    #[serde(default)]
    summary_window_ms: Option<i64>,
    /// 可选：携带 LiveKit 连接信息时，服务会以 bot 身份进房订阅音频并自行切窗转写
    #[serde(default)]
    livekit: Option<LivekitIngest>,
}
#[derive(Serialize, utoipa::ToSchema)]
struct IdResponse {
    id: String,
}
#[derive(Deserialize, utoipa::ToSchema)]
struct CreateSpeaker {
    /// 说话人显示名称
    name: String,
}

#[derive(Deserialize, utoipa::ToSchema)]
struct UpdateSegment {
    /// 新的转写文本（trim 后为空则忽略该字段）
    transcript: Option<String>,
    /// 重新指派说话人（按名字自动建档/复用）
    speaker_name: Option<String>,
}

#[derive(Deserialize)]
struct JobFilter {
    meeting_id: Option<String>,
    status: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    fs::create_dir_all("data/audio").await?;
    let options = match env::var("DITING_DATABASE_URL") {
        Ok(url) => SqliteConnectOptions::from_str(&url)?,
        Err(_) => SqliteConnectOptions::new().filename("data/meeting.db"),
    }
    .create_if_missing(true)
    .journal_mode(SqliteJournalMode::Wal)
    .foreign_keys(true)
    .busy_timeout(Duration::from_secs(5));
    let db = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    for statement in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
        sqlx::query(statement).execute(&db).await?;
    }
    migrate_jobs_unique_constraint(&db).await?;
    migrate_summary_window_column(&db).await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_jobs_dispatch ON jobs(status, available_at)")
        .execute(&db)
        .await?;
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_segments_timeline ON audio_segments(meeting_id, start_ms, end_ms)",
    )
    .execute(&db)
    .await?;
    let recovered = sqlx::query(
        "UPDATE jobs SET status='pending', available_at=CURRENT_TIMESTAMP,
         error_message='recovered after service restart' WHERE status='running'",
    )
    .execute(&db)
    .await?
    .rows_affected();
    if recovered > 0 {
        info!(recovered, "recovered interrupted jobs");
    }
    let http_client = reqwest::Client::builder()
        .timeout(Duration::from_secs(120))
        .build()?;
    let (transcriber, asr_provider_name): (Arc<dyn Transcriber>, &str) = match (
        env::var("DITING_ASR_BASE_URL").ok(),
        env::var("DITING_ASR_API_KEY").ok(),
        env::var("DITING_ASR_MODEL").ok(),
    ) {
        (Some(base_url), Some(api_key), Some(model)) => (
            Arc::new(OpenAiTranscriber {
                client: http_client.clone(),
                base_url,
                api_key,
                model,
            }),
            "openai-compatible",
        ),
        _ => {
            warn!("DITING_ASR_BASE_URL/DITING_ASR_API_KEY/DITING_ASR_MODEL not fully set: transcription will output placeholder text '[transcript provider not configured]'");
            (Arc::new(LocalTranscriber), "local")
        }
    };
    let (summarizer, summarizer_provider_name): (Arc<dyn Summarizer>, &str) = match (
        env::var("DITING_LLM_BASE_URL").ok(),
        env::var("DITING_LLM_API_KEY").ok(),
        env::var("DITING_LLM_MODEL").ok(),
    ) {
        (Some(base_url), Some(api_key), Some(model)) => (
            Arc::new(OpenAiSummarizer {
                client: http_client,
                base_url,
                api_key,
                model,
            }),
            "openai-compatible",
        ),
        _ => {
            warn!("DITING_LLM_BASE_URL/DITING_LLM_API_KEY/DITING_LLM_MODEL not fully set: summaries will use local placeholder");
            (Arc::new(LocalSummarizer), "local")
        }
    };
    info!(
        asr_provider = asr_provider_name,
        summarizer_provider = summarizer_provider_name,
        "processing providers configured"
    );
    let max_upload_bytes = env::var("DITING_MAX_UPLOAD_BYTES")
        .ok()
        .and_then(|value| value.parse().ok())
        .filter(|value| *value > 0)
        .unwrap_or(100 * 1024 * 1024);
    let (events, _) = broadcast::channel(512);
    let state = AppState {
        db,
        audio_dir: Arc::new(PathBuf::from("data/audio")),
        transcriber,
        summarizer,
        max_upload_bytes,
        job_notify: Arc::new(Notify::new()),
        events,
        ingest_stop: IngestStopMap::default(),
    };
    tokio::spawn(worker(state.clone()));
    let app = build_router(state);
    let addr: SocketAddr = env::var("DITING_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3000".into())
        .parse()?;
    info!(%addr, "meeting service started");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .merge(SwaggerUi::new("/docs").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/", get(index))
        .route("/app.js", get(app_js))
        .route("/styles.css", get(styles_css))
        .route("/health", get(health))
        .route("/api/v1/jobs", get(list_jobs))
        .route("/api/v1/jobs/{id}/retry", post(retry_job))
        .route("/api/v1/meetings", post(create_meeting))
        .route("/api/v1/meetings/{id}", get(get_meeting))
        .route("/api/v1/meetings/{id}", delete_route(delete_meeting))
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
            patch(update_segment),
        )
        .route("/api/v1/meetings/{id}/summaries", get(list_summaries))
        .route("/api/v1/meetings/{id}/events", get(meeting_events))
        .route("/api/v1/meetings/{id}/board", get(get_board))
        .route(
            "/api/v1/meetings/{id}/board/versions",
            get(list_board_versions),
        )
        .layer(DefaultBodyLimit::max(state.max_upload_bytes))
        .with_state(state)
}

async fn migrate_summary_window_column(db: &SqlitePool) -> Result<(), sqlx::Error> {
    let columns = sqlx::query("PRAGMA table_info(meetings)")
        .fetch_all(db)
        .await?;
    let exists = columns
        .iter()
        .any(|row| row.get::<String, _>("name") == "summary_window_ms");
    if !exists {
        sqlx::query(
            "ALTER TABLE meetings ADD COLUMN summary_window_ms INTEGER NOT NULL DEFAULT 300000",
        )
        .execute(db)
        .await?;
        info!("migrated meetings.summary_window_ms");
    }
    Ok(())
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/events", tag = "meetings", summary = "订阅会议实时事件", description = "以 SSE 实时推送该会议的事件：segment.uploaded、segment.transcribed、segment.failed、summary.created、board.updated、meeting.ended。历史状态请通过 segments/summaries/board 接口补拉。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "事件流（text/event-stream）", content_type = "text/event-stream"), (status = 404, description = "会议不存在")))]
async fn meeting_events(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let receiver = s.events.subscribe();
    let stream = BroadcastStream::new(receiver).filter_map(move |message| match message {
        Ok(event) if event.meeting_id == meeting_id => Some(Ok(Event::default()
            .event(event.kind)
            .data(event.data.to_string()))),
        // 其它会议的事件或订阅方滞后（lagged）都直接跳过
        _ => None,
    });
    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    ))
}

async fn migrate_jobs_unique_constraint(db: &SqlitePool) -> Result<(), sqlx::Error> {
    let definition =
        sqlx::query("SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'")
            .fetch_one(db)
            .await?
            .get::<String, _>("sql");
    if !definition.contains("UNIQUE(job_type, target_id)") {
        return Ok(());
    }
    let mut tx = db.begin().await?;
    sqlx::query(
        "CREATE TABLE jobs_migrated (
          id TEXT PRIMARY KEY, job_type TEXT NOT NULL, meeting_id TEXT NOT NULL REFERENCES meetings(id),
          target_id TEXT, status TEXT NOT NULL DEFAULT 'pending', retry_count INTEGER NOT NULL DEFAULT 0,
          available_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, error_message TEXT,
          UNIQUE(job_type, meeting_id, target_id)
        )",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "INSERT INTO jobs_migrated(id,job_type,meeting_id,target_id,status,retry_count,available_at,error_message)
         SELECT id,job_type,meeting_id,target_id,status,retry_count,available_at,error_message FROM jobs",
    )
    .execute(&mut *tx)
    .await?;
    sqlx::query("DROP TABLE jobs").execute(&mut *tx).await?;
    sqlx::query("ALTER TABLE jobs_migrated RENAME TO jobs")
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    info!("migrated jobs uniqueness constraint");
    Ok(())
}

#[utoipa::path(post, path = "/api/v1/meetings", tag = "meetings", summary = "创建会议", description = "创建一个进行中的会议并返回会议 ID。后续说话人和音频分段都通过该 ID 关联。可通过 summary_window_ms 调整滚动摘要窗口（默认 5 分钟）。", request_body = CreateMeeting, responses((status = 201, description = "会议已创建", body = IdResponse), (status = 400, description = "标题为空或摘要窗口越界")))]
async fn create_meeting(
    State(s): State<AppState>,
    Json(input): Json<CreateMeeting>,
) -> Result<(StatusCode, Json<IdResponse>), AppError> {
    if input.title.trim().is_empty() {
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
        .bind(input.title.trim())
        .bind(window)
        .bind(window)
        .execute(&s.db)
        .await?;
    let livekit_enabled = input.livekit.is_some();
    if let Some(cfg) = input.livekit {
        spawn_ingest(&s, &id, cfg);
    }
    if livekit_enabled {
        info!(meeting_id = %id, title = %input.title, "meeting created, livekit ingest requested");
    } else {
        info!(meeting_id = %id, title = %input.title, "meeting created without livekit config, transcription only via segment uploads");
    }
    Ok((StatusCode::CREATED, Json(IdResponse { id })))
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../frontend/index.html"))
}

async fn app_js() -> impl IntoResponse {
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/javascript; charset=utf-8",
        )],
        include_str!("../frontend/app.js"),
    )
}

async fn styles_css() -> impl IntoResponse {
    (
        [(axum::http::header::CONTENT_TYPE, "text/css; charset=utf-8")],
        include_str!("../frontend/styles.css"),
    )
}

#[utoipa::path(get, path = "/health", tag = "system", summary = "检查服务健康状态", description = "检查 SQLite 连接，并返回待处理和失败任务数量。此接口不依赖具体会议。", responses((status = 200, description = "服务健康", body = Value)))]
async fn health(State(s): State<AppState>) -> Result<Json<Value>, AppError> {
    sqlx::query("SELECT 1").execute(&s.db).await?;
    let pending = sqlx::query("SELECT COUNT(*) value FROM jobs WHERE status='pending'")
        .fetch_one(&s.db)
        .await?
        .get::<i64, _>("value");
    let failed = sqlx::query("SELECT COUNT(*) value FROM jobs WHERE status='failed'")
        .fetch_one(&s.db)
        .await?
        .get::<i64, _>("value");
    Ok(Json(
        json!({"status":"ok","database":"ok","jobs":{"pending":pending,"failed":failed}}),
    ))
}

#[utoipa::path(get, path = "/api/v1/jobs", tag = "jobs", summary = "查询后台任务", description = "按会议和任务状态查询最近 100 条后台任务。任务包括转写、Summary 和 rebuild。", params(("meeting_id" = Option<String>, Query, description = "按会议 ID 过滤"), ("status" = Option<String>, Query, description = "按状态过滤：pending、running、completed 或 failed")), responses((status = 200, description = "任务列表", body = [Value]), (status = 400, description = "状态参数无效")))]
async fn list_jobs(
    State(s): State<AppState>,
    Query(filter): Query<JobFilter>,
) -> Result<Json<Vec<Value>>, AppError> {
    if let Some(ref status) = filter.status {
        if !matches!(
            status.as_str(),
            "pending" | "running" | "completed" | "failed"
        ) {
            return Err(AppError::BadRequest("invalid job status".into()));
        }
    }
    let rows = sqlx::query(
        "SELECT id,job_type,meeting_id,target_id,status,retry_count,available_at,error_message
         FROM jobs WHERE (? IS NULL OR meeting_id=?) AND (? IS NULL OR status=?)
         ORDER BY available_at DESC LIMIT 100",
    )
    .bind(&filter.meeting_id)
    .bind(&filter.meeting_id)
    .bind(&filter.status)
    .bind(&filter.status)
    .fetch_all(&s.db)
    .await?;
    Ok(Json(rows.into_iter().map(|r| json!({
        "id":r.get::<String,_>("id"), "job_type":r.get::<String,_>("job_type"),
        "meeting_id":r.get::<String,_>("meeting_id"), "target_id":r.get::<Option<String>,_>("target_id"),
        "status":r.get::<String,_>("status"), "retry_count":r.get::<i64,_>("retry_count"),
        "available_at":r.get::<String,_>("available_at"), "error_message":r.get::<Option<String>,_>("error_message")
    })).collect()))
}

#[utoipa::path(post, path = "/api/v1/jobs/{id}/retry", tag = "jobs", summary = "重试失败任务", description = "将 failed 任务重置为 pending，Worker 会在下一轮调度中重新执行。", params(("id" = String, Path, description = "任务 ID")), responses((status = 200, description = "任务已重新排队", body = Value), (status = 400, description = "任务不存在或当前不可重试")))]
async fn retry_job(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let result = sqlx::query(
        "UPDATE jobs SET status='pending',retry_count=0,available_at=CURRENT_TIMESTAMP,error_message=NULL
         WHERE id=? AND status='failed'",
    )
    .bind(&id)
    .execute(&s.db)
    .await?;
    if result.rows_affected() == 0 {
        return Err(AppError::BadRequest(
            "job does not exist or is not failed".into(),
        ));
    }
    s.job_notify.notify_one();
    Ok(Json(json!({"id":id,"status":"pending"})))
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}", tag = "meetings", summary = "获取会议详情", description = "返回会议状态、开始/结束时间、Board 版本和下一个 Summary 窗口。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "会议详情", body = Value), (status = 404, description = "会议不存在")))]
async fn get_meeting(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let row = sqlx::query("SELECT id,title,status,started_at,ended_at,board_version,next_summary_end_ms,summary_window_ms FROM meetings WHERE id=?").bind(&id).fetch_optional(&s.db).await?.ok_or(AppError::NotFound)?;
    Ok(Json(
        json!({"id":row.get::<String,_>("id"),"title":row.get::<String,_>("title"),"status":row.get::<String,_>("status"),"started_at":row.get::<Option<String>,_>("started_at"),"ended_at":row.get::<Option<String>,_>("ended_at"),"board_version":row.get::<i64,_>("board_version"),"next_summary_end_ms":row.get::<i64,_>("next_summary_end_ms"),"summary_window_ms":row.get::<i64,_>("summary_window_ms")}),
    ))
}

#[utoipa::path(delete, path = "/api/v1/meetings/{id}", tag = "meetings", summary = "删除会议", description = "事务删除会议及全部关联记录，并在成功后删除本地音频目录。该操作不可恢复。", params(("id" = String, Path, description = "会议 ID")), responses((status = 204, description = "删除成功"), (status = 404, description = "会议不存在")))]
async fn delete_meeting(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    ensure_meeting(&s.db, &id).await?;
    stop_ingest(&s, &id);
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
    let audio_dir = s.audio_dir.join(&id);
    if let Err(error) = fs::remove_dir_all(&audio_dir).await {
        if error.kind() != std::io::ErrorKind::NotFound {
            error!(%error, path = %audio_dir.display(), meeting_id = %id, "meeting records deleted but audio cleanup failed");
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

#[utoipa::path(post, path = "/api/v1/meetings/{id}/end", tag = "meetings", summary = "结束会议", description = "将会议标记为 ended，并为最后不足 5 分钟的音频窗口安排最终 Summary。接口幂等。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "会议已结束", body = Value), (status = 404, description = "会议不存在")))]
async fn end_meeting(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    ensure_meeting(&s.db, &id).await?;
    stop_ingest(&s, &id);
    sqlx::query("UPDATE meetings SET status='ended', ended_at=COALESCE(ended_at,CURRENT_TIMESTAMP) WHERE id=?")
        .bind(&id)
        .execute(&s.db)
        .await?;
    enqueue_summary(&s.db, &id, true).await?;
    s.job_notify.notify_one();
    publish_event(&s, &id, "meeting.ended", json!({"meeting_id": id}));
    Ok(Json(json!({"status":"ended"})))
}

#[utoipa::path(post, path = "/api/v1/meetings/{id}/speakers", tag = "meetings", summary = "添加说话人", description = "为会议登记一个说话人。创建后可在上传音频分段时通过 speaker_id 关联。", params(("id" = String, Path, description = "会议 ID")), request_body = CreateSpeaker, responses((status = 201, description = "说话人已创建", body = IdResponse), (status = 404, description = "会议不存在")))]
async fn create_speaker(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
    Json(input): Json<CreateSpeaker>,
) -> Result<(StatusCode, Json<IdResponse>), AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    if input.name.trim().is_empty() {
        return Err(AppError::BadRequest("name is required".into()));
    }
    let id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO speakers(id,meeting_id,name) VALUES(?,?,?)")
        .bind(&id)
        .bind(&meeting_id)
        .bind(input.name.trim())
        .execute(&s.db)
        .await?;
    Ok((StatusCode::CREATED, Json(IdResponse { id })))
}
#[utoipa::path(get, path = "/api/v1/meetings/{id}/speakers", tag = "meetings", summary = "列出说话人", description = "按创建时间返回会议中的全部说话人。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "说话人列表", body = [Value]), (status = 404, description = "会议不存在")))]
async fn list_speakers(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Vec<Value>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let rows = sqlx::query("SELECT id,name FROM speakers WHERE meeting_id=? ORDER BY created_at")
        .bind(meeting_id)
        .fetch_all(&s.db)
        .await?;
    Ok(Json(
        rows.into_iter()
            .map(|r| json!({"id":r.get::<String,_>("id"),"name":r.get::<String,_>("name")}))
            .collect(),
    ))
}

#[utoipa::path(post, path = "/api/v1/meetings/{id}/segments", tag = "meetings", summary = "上传音频分段", description = "上传一个会议音频分段并加入转写队列。multipart 字段：audio、speaker_id、sequence_no、start_ms、end_ms 和 transcript；时间单位为毫秒。audio 与 transcript 至少提供一个——实时场景下上游已有 ASR 结果时可只传 transcript，跳过音频存储与转写。", params(("id" = String, Path, description = "会议 ID")), responses((status = 201, description = "音频分段已创建", body = IdResponse), (status = 400, description = "字段缺失、时间范围无效、文件超限或会议已结束"), (status = 404, description = "会议不存在")))]
async fn upload_segment(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
    mut multipart: Multipart,
) -> Result<(StatusCode, Json<IdResponse>), AppError> {
    let meeting = sqlx::query("SELECT status FROM meetings WHERE id=?")
        .bind(&meeting_id)
        .fetch_optional(&s.db)
        .await?
        .ok_or(AppError::NotFound)?;
    if meeting.get::<String, _>("status") != "running" {
        return Err(AppError::BadRequest(
            "audio can only be uploaded to a running meeting".into(),
        ));
    }
    let mut speaker_id = None;
    let mut speaker_name: Option<String> = None;
    let mut sequence_no: Option<i64> = None;
    let mut start_ms: Option<i64> = None;
    let mut end_ms: Option<i64> = None;
    let mut bytes = None;
    let mut transcript = None;
    let mut filename = "audio.bin".to_string();
    while let Some(field) = multipart
        .next_field()
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?
    {
        let name = field.name().unwrap_or("").to_string();
        if name == "audio" {
            filename = field.file_name().unwrap_or("audio.bin").to_string();
            bytes = Some(
                field
                    .bytes()
                    .await
                    .map_err(|e| AppError::BadRequest(e.to_string()))?,
            );
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
    let data = bytes.filter(|data| !data.is_empty());
    if data.is_none() && transcript.is_none() {
        return Err(AppError::BadRequest("audio or transcript is required".into()));
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
    if let Some(ref data) = data {
        if data.len() > s.max_upload_bytes {
            return Err(AppError::BadRequest(format!(
                "audio exceeds {} byte upload limit",
                s.max_upload_bytes
            )));
        }
    }
    // 未指定 speaker_id 时按名字自动建档/复用
    if speaker_id.is_none() {
        if let Some(name) = speaker_name.map(|n| n.trim().to_string()).filter(|n| !n.is_empty()) {
            speaker_id = Some(
                livekit_ingest::ensure_speaker_by_name(&s.db, &meeting_id, &name).await?,
            );
        }
    }
    if let Some(ref id) = speaker_id {
        let belongs_to_meeting = sqlx::query("SELECT 1 FROM speakers WHERE id=? AND meeting_id=?")
            .bind(id)
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
    let id = Uuid::new_v4().to_string();
    // 仅当携带音频时才落盘；纯文本分段 file_path 为空串
    let file_path = if let Some(data) = data {
        let dir = s.audio_dir.join(&meeting_id);
        fs::create_dir_all(&dir)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        let safe_filename = sanitize_filename(&filename);
        let safe_filename = if safe_filename.is_empty() {
            "audio.bin".to_string()
        } else {
            safe_filename
        };
        let path = dir.join(format!("{}-{}", id, safe_filename));
        fs::write(&path, &data)
            .await
            .map_err(|e| AppError::Internal(e.to_string()))?;
        path.to_string_lossy().to_string()
    } else {
        String::new()
    };
    let db_result =
        insert_segment(&s.db, &id, &meeting_id, speaker_id.as_deref(), seq, start, end, &file_path, transcript).await;
    if let Err(error) = db_result {
        if !file_path.is_empty() {
            if let Err(cleanup_error) = fs::remove_file(&file_path).await {
                error!(%cleanup_error, path = %file_path, "failed to clean up audio after database error");
            }
        }
        return Err(AppError::Db(error));
    }
    info!(
        meeting_id = %meeting_id,
        segment_id = %id,
        sequence_no = seq,
        start_ms = start,
        end_ms = end,
        has_audio = !file_path.is_empty(),
        transcript_provided = transcript_was_provided,
        "segment uploaded"
    );
    s.job_notify.notify_one();
    publish_event(
        &s,
        &meeting_id,
        "segment.uploaded",
        json!({"segment_id": id, "sequence_no": seq, "start_ms": start, "end_ms": end}),
    );
    Ok((StatusCode::CREATED, Json(IdResponse { id })))
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/segments", tag = "meetings", summary = "列出音频分段", description = "按会议时间线返回音频分段、转写状态和转写文本。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "音频分段列表", body = [Value]), (status = 404, description = "会议不存在")))]
async fn list_segments(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Vec<Value>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let rows=sqlx::query("SELECT a.id,a.speaker_id,sp.name AS speaker_name,a.sequence_no,a.start_ms,a.end_ms,a.status,a.transcript FROM audio_segments a LEFT JOIN speakers sp ON sp.id=a.speaker_id WHERE a.meeting_id=? ORDER BY a.start_ms").bind(meeting_id).fetch_all(&s.db).await?;
    Ok(Json(rows.into_iter().map(|r|json!({"id":r.get::<String,_>("id"),"speaker_id":r.get::<Option<String>,_>("speaker_id"),"speaker_name":r.get::<Option<String>,_>("speaker_name"),"sequence_no":r.get::<i64,_>("sequence_no"),"start_ms":r.get::<i64,_>("start_ms"),"end_ms":r.get::<i64,_>("end_ms"),"status":r.get::<String,_>("status"),"transcript":r.get::<Option<String>,_>("transcript")})).collect()))
}
#[utoipa::path(patch, path = "/api/v1/meetings/{id}/segments/{segment_id}", tag = "meetings", summary = "编辑音频分段", description = "人工修订转写文本或重新指派说话人；只更新分段记录，不触发重新转写，历史滚动摘要不回溯重建。成功后广播 segment.updated 事件。", params(("id" = String, Path, description = "会议 ID"), ("segment_id" = String, Path, description = "分段 ID")), request_body = UpdateSegment, responses((status = 200, description = "分段已更新", body = Value), (status = 400, description = "没有可更新的字段"), (status = 404, description = "会议或分段不存在")))]
async fn update_segment(
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
    if transcript.is_none() && speaker_name.is_none() {
        return Err(AppError::BadRequest(
            "transcript or speaker_name is required".into(),
        ));
    }
    let exists =
        sqlx::query("SELECT 1 FROM audio_segments WHERE id=? AND meeting_id=?")
            .bind(&segment_id)
            .bind(&meeting_id)
            .fetch_optional(&s.db)
            .await?
            .is_some();
    if !exists {
        return Err(AppError::NotFound);
    }
    if let Some(text) = transcript {
        sqlx::query("UPDATE audio_segments SET transcript=?, status='completed' WHERE id=? AND meeting_id=?")
            .bind(text)
            .bind(&segment_id)
            .bind(&meeting_id)
            .execute(&s.db)
            .await?;
    }
    if let Some(name) = speaker_name {
        let speaker_id =
            livekit_ingest::ensure_speaker_by_name(&s.db, &meeting_id, &name).await?;
        sqlx::query("UPDATE audio_segments SET speaker_id=? WHERE id=? AND meeting_id=?")
            .bind(speaker_id)
            .bind(&segment_id)
            .bind(&meeting_id)
            .execute(&s.db)
            .await?;
    }
    let row = sqlx::query(
        "SELECT a.id,a.speaker_id,sp.name AS speaker_name,a.sequence_no,a.start_ms,a.end_ms,a.status,a.transcript FROM audio_segments a LEFT JOIN speakers sp ON sp.id=a.speaker_id WHERE a.id=? AND a.meeting_id=?",
    )
    .bind(&segment_id)
    .bind(&meeting_id)
    .fetch_one(&s.db)
    .await?;
    let payload = json!({
        "id": row.get::<String, _>("id"),
        "segment_id": row.get::<String, _>("id"),
        "speaker_id": row.get::<Option<String>, _>("speaker_id"),
        "speaker_name": row.get::<Option<String>, _>("speaker_name"),
        "sequence_no": row.get::<i64, _>("sequence_no"),
        "start_ms": row.get::<i64, _>("start_ms"),
        "end_ms": row.get::<i64, _>("end_ms"),
        "status": row.get::<String, _>("status"),
        "transcript": row.get::<Option<String>, _>("transcript"),
    });
    publish_event(&s, &meeting_id, "segment.updated", payload.clone());
    Ok(Json(payload))
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/summaries", tag = "meetings", summary = "获取滚动摘要", description = "返回会议按 5 分钟窗口生成的 Summary，迟到音频重建后会更新受影响窗口。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "Summary 列表", body = [Value]), (status = 404, description = "会议不存在")))]
async fn list_summaries(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Vec<Value>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let rows=sqlx::query("SELECT id,window_start_ms,window_end_ms,content_json,created_at FROM rolling_summaries WHERE meeting_id=? ORDER BY window_end_ms").bind(meeting_id).fetch_all(&s.db).await?;
    Ok(Json(rows.into_iter().map(|r|json!({"id":r.get::<String,_>("id"),"window_start_ms":r.get::<i64,_>("window_start_ms"),"window_end_ms":r.get::<i64,_>("window_end_ms"),"content":serde_json::from_str::<Value>(&r.get::<String,_>("content_json")).unwrap_or(json!({})),"created_at":r.get::<String,_>("created_at")})).collect()))
}
#[utoipa::path(get, path = "/api/v1/meetings/{id}/board", tag = "meetings", summary = "获取当前会议板", description = "返回最新 Meeting Board 及其版本号。Board 会随着每次 Summary 更新而增量合并。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "当前会议板", body = Value), (status = 404, description = "会议不存在")))]
async fn get_board(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Value>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let row = sqlx::query(
        "SELECT version,content_json,updated_at FROM meeting_boards WHERE meeting_id=?",
    )
    .bind(meeting_id)
    .fetch_optional(&s.db)
    .await?;
    match row {
        Some(r) => Ok(Json(
            json!({"version":r.get::<i64,_>("version"),"content":serde_json::from_str::<Value>(&r.get::<String,_>("content_json")).unwrap_or(json!({})),"updated_at":r.get::<String,_>("updated_at")}),
        )),
        None => Ok(Json(json!({"version":0,"content":empty_board()}))),
    }
}

#[utoipa::path(get, path = "/api/v1/meetings/{id}/board/versions", tag = "meetings", summary = "获取会议板历史版本", description = "按版本号返回 Meeting Board 的历史快照及来源 Summary。", params(("id" = String, Path, description = "会议 ID")), responses((status = 200, description = "会议板版本列表", body = [Value]), (status = 404, description = "会议不存在")))]
async fn list_board_versions(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Vec<Value>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let rows = sqlx::query(
        "SELECT version,source_summary_id,content_json,created_at FROM meeting_board_versions
         WHERE meeting_id=? ORDER BY version",
    )
    .bind(meeting_id)
    .fetch_all(&s.db)
    .await?;
    Ok(Json(
        rows.into_iter()
            .map(|r| {
                json!({
                    "version": r.get::<i64, _>("version"),
                    "source_summary_id": r.get::<String, _>("source_summary_id"),
                    "content": serde_json::from_str::<Value>(&r.get::<String, _>("content_json")).unwrap_or(json!({})),
                    "created_at": r.get::<String, _>("created_at")
                })
            })
            .collect(),
    ))
}

async fn ensure_meeting(db: &SqlitePool, id: &str) -> Result<(), AppError> {
    if sqlx::query("SELECT 1 FROM meetings WHERE id=?")
        .bind(id)
        .fetch_optional(db)
        .await?
        .is_none()
    {
        Err(AppError::NotFound)
    } else {
        Ok(())
    }
}
fn sanitize_filename(name: &str) -> String {
    name.chars()
        .filter(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '_' | '-'))
        .collect()
}
fn empty_board() -> Value {
    json!({"topics":[],"decisions":[],"action_items":[],"open_questions":[],"risks":[],"key_points":[]})
}

fn merge_board(board: &mut Value, summary: &SummaryDocument) {
    for (field, values) in [
        ("topics", &summary.topics),
        ("decisions", &summary.decisions),
        ("open_questions", &summary.open_questions),
        ("risks", &summary.risks),
        ("key_points", &summary.key_points),
    ] {
        if !board.get(field).is_some_and(Value::is_array) {
            board[field] = json!([]);
        }
        let target = board
            .get_mut(field)
            .and_then(Value::as_array_mut)
            .expect("field was normalized to an array");
        for value in values {
            if !target.iter().any(|item| item.as_str() == Some(value)) {
                target.push(Value::String(value.clone()));
            }
        }
    }
    if !board.get("action_items").is_some_and(Value::is_array) {
        board["action_items"] = json!([]);
    }
    let action_items = board
        .get_mut("action_items")
        .and_then(Value::as_array_mut)
        .expect("action_items was normalized to an array");
    for item in &summary.action_items {
        let exists = action_items.iter().any(|existing| {
            existing.get("content").and_then(Value::as_str) == Some(item.content.as_str())
        });
        if !exists {
            if let Ok(value) = serde_json::to_value(item) {
                action_items.push(value);
            }
        }
    }
}

fn normalize_summary(mut summary: SummaryDocument) -> SummaryDocument {
    fn normalize_list(values: &mut Vec<String>) {
        let mut normalized = Vec::new();
        for value in values.drain(..) {
            let value = value.trim();
            if !value.is_empty()
                && !normalized
                    .iter()
                    .any(|existing: &String| existing.eq_ignore_ascii_case(value))
            {
                normalized.push(value.to_string());
            }
            if normalized.len() == 100 {
                break;
            }
        }
        *values = normalized;
    }
    for values in [
        &mut summary.topics,
        &mut summary.decisions,
        &mut summary.open_questions,
        &mut summary.risks,
        &mut summary.key_points,
    ] {
        normalize_list(values);
    }
    let mut actions = Vec::new();
    for mut item in summary.action_items.drain(..) {
        item.content = item.content.trim().to_string();
        if item.content.is_empty()
            || actions
                .iter()
                .any(|existing: &ActionItem| existing.content.eq_ignore_ascii_case(&item.content))
        {
            continue;
        }
        item.owner = item.owner.and_then(trimmed_option);
        item.due_date = item.due_date.and_then(trimmed_option);
        item.status = item.status.trim().to_string();
        if !matches!(
            item.status.as_str(),
            "open" | "in_progress" | "done" | "blocked"
        ) {
            item.status = default_action_status();
        }
        actions.push(item);
        if actions.len() == 100 {
            break;
        }
    }
    summary.action_items = actions;
    summary
}

fn trimmed_option(value: String) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

async fn enqueue_rebuild(
    db: &SqlitePool,
    meeting_id: &str,
    window_start: i64,
) -> Result<(), AppError> {
    sqlx::query(
        "INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES(?, 'rebuild', ?, ?)
         ON CONFLICT(job_type,meeting_id,target_id) DO UPDATE SET
           status='pending',retry_count=0,available_at=CURRENT_TIMESTAMP,error_message=NULL
         WHERE jobs.status IN ('completed','failed')",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(meeting_id)
    .bind(window_start.to_string())
    .execute(db)
    .await?;
    Ok(())
}

async fn enqueue_summary(
    db: &SqlitePool,
    meeting_id: &str,
    final_window: bool,
) -> Result<(), AppError> {
    let row = sqlx::query("SELECT next_summary_end_ms, summary_window_ms FROM meetings WHERE id=?")
        .bind(meeting_id)
        .fetch_one(db)
        .await?;
    let end = row.get::<i64, _>("next_summary_end_ms");
    let window = row.get::<i64, _>("summary_window_ms");
    let start = end.saturating_sub(window);
    let unfinished = sqlx::query(
        "SELECT COUNT(*) value FROM audio_segments
         WHERE meeting_id=? AND status NOT IN ('completed','failed') AND start_ms < ? AND end_ms > ?",
    )
    .bind(meeting_id)
    .bind(end)
    .bind(start)
    .fetch_one(db)
    .await?
    .get::<i64, _>("value");
    if unfinished > 0 {
        return Ok(());
    }
    let max_end=sqlx::query("SELECT COALESCE(MAX(end_ms),0) max_end FROM audio_segments WHERE meeting_id=? AND status='completed'").bind(meeting_id).fetch_one(db).await?.get::<i64,_>("max_end");
    if max_end >= end {
        sqlx::query("INSERT OR IGNORE INTO jobs(id,job_type,meeting_id,target_id) VALUES(?, 'summary', ?, ?)").bind(Uuid::new_v4().to_string()).bind(meeting_id).bind(end.to_string()).execute(db).await?;
    } else if final_window && max_end > 0 {
        let summarized_end = sqlx::query(
            "SELECT COALESCE(MAX(window_end_ms),0) value FROM rolling_summaries WHERE meeting_id=?",
        )
        .bind(meeting_id)
        .fetch_one(db)
        .await?
        .get::<i64, _>("value");
        if max_end > summarized_end {
            sqlx::query("INSERT OR IGNORE INTO jobs(id,job_type,meeting_id,target_id) VALUES(?, 'summary', ?, ?)").bind(Uuid::new_v4().to_string()).bind(meeting_id).bind(format!("final:{}",max_end)).execute(db).await?;
        }
    }
    Ok(())
}

async fn worker(state: AppState) {
    loop {
        match process_jobs(&state).await {
            // 本轮领到了任务：立即进入下一轮，避免链式任务（转写→摘要）等待 tick
            Ok(claimed) if claimed > 0 => continue,
            Ok(_) => {}
            Err(e) => error!(error=?e,"worker cycle failed"),
        }
        tokio::select! {
            // 入队点（上传分段、结束会议、重试）即时唤醒
            _ = state.job_notify.notified() => {}
            // 兜底 tick：处理带 5s 退避的重试任务及漏通知场景
            _ = sleep(Duration::from_secs(3)) => {}
        }
    }
}
async fn process_jobs(s: &AppState) -> Result<usize, AppError> {
    let mut claimed_count = 0;
    let jobs = sqlx::query(
        "SELECT id,job_type,meeting_id,target_id FROM jobs
         WHERE status='pending' AND available_at <= CURRENT_TIMESTAMP
         ORDER BY available_at LIMIT 10",
    )
    .fetch_all(&s.db)
    .await?;
    for j in jobs {
        let id = j.get::<String, _>("id");
        let typ = j.get::<String, _>("job_type");
        let meeting = j.get::<String, _>("meeting_id");
        let target = j.get::<Option<String>, _>("target_id");
        let claimed = sqlx::query(
            "UPDATE jobs SET status='running',error_message=NULL
             WHERE id=? AND status='pending' AND available_at <= CURRENT_TIMESTAMP",
        )
        .bind(&id)
        .execute(&s.db)
        .await?
        .rows_affected();
        if claimed == 0 {
            continue;
        }
        claimed_count += 1;
        let result = match typ.as_str() {
            "transcribe" => {
                process_transcription(s, target.as_deref().unwrap_or(""), &meeting).await
            }
            "summary" => process_summary(s, &meeting, target.as_deref().unwrap_or("0")).await,
            "rebuild" => process_rebuild(s, &meeting, target.as_deref().unwrap_or("")).await,
            _ => Err(AppError::Processing(format!("unknown job type: {typ}"))),
        };
        match result {
            Ok(_) => {
                sqlx::query("UPDATE jobs SET status='completed' WHERE id=?")
                    .bind(id)
                    .execute(&s.db)
                    .await?;
            }
            Err(e) => {
                warn!(
                    job_id = id,
                    job_type = %typ,
                    meeting_id = %meeting,
                    error = %e,
                    "job processing failed, scheduling retry"
                );
                let failed = sqlx::query(
                    "UPDATE jobs SET status=CASE WHEN retry_count < 2 THEN 'pending' ELSE 'failed' END,
                     retry_count=retry_count+1, available_at=datetime('now','+5 seconds'), error_message=? WHERE id=?",
                )
                .bind(e.to_string())
                .bind(&id)
                .execute(&s.db)
                .await?;
                if failed.rows_affected() > 0 && typ == "transcribe" {
                    sqlx::query(
                        "UPDATE audio_segments SET status=CASE
                         WHEN (SELECT status FROM jobs WHERE id=?)='failed' THEN 'failed'
                         ELSE 'transcribing' END WHERE id=?",
                    )
                    .bind(&id)
                    .bind(target.as_deref().unwrap_or(""))
                    .execute(&s.db)
                    .await?;
                    let permanently_failed =
                        sqlx::query("SELECT status='failed' value FROM jobs WHERE id=?")
                            .bind(&id)
                            .fetch_one(&s.db)
                            .await?
                            .get::<bool, _>("value");
                    if permanently_failed {
                        let failed_segment = sqlx::query(
                            "SELECT sequence_no FROM audio_segments WHERE id=?",
                        )
                        .bind(target.as_deref().unwrap_or(""))
                        .fetch_optional(&s.db)
                        .await?;
                        publish_event(
                            s,
                            &meeting,
                            "segment.failed",
                            json!({
                                "segment_id": target.as_deref().unwrap_or(""),
                                "sequence_no": failed_segment.map(|r| r.get::<i64, _>("sequence_no")),
                            }),
                        );
                        let ended =
                            sqlx::query("SELECT status='ended' value FROM meetings WHERE id=?")
                                .bind(&meeting)
                                .fetch_one(&s.db)
                                .await?
                                .get::<bool, _>("value");
                        if let Err(error) = enqueue_summary(&s.db, &meeting, ended).await {
                            error!(%error, meeting_id = %meeting, "failed to enqueue summary after transcription failure");
                        }
                    }
                }
            }
        }
    }
    Ok(claimed_count)
}
async fn process_transcription(
    s: &AppState,
    segment_id: &str,
    meeting_id: &str,
) -> Result<(), AppError> {
    sqlx::query("UPDATE audio_segments SET status='transcribing' WHERE id=? AND meeting_id=?")
        .bind(segment_id)
        .bind(meeting_id)
        .execute(&s.db)
        .await?;
    let row = sqlx::query(
        "SELECT a.file_path,a.transcript,a.start_ms,a.end_ms,a.sequence_no,a.speaker_id,\
         (SELECT name FROM speakers WHERE id=a.speaker_id) AS speaker_name \
         FROM audio_segments a WHERE a.id=? AND a.meeting_id=?",
    )
            .bind(segment_id)
            .bind(meeting_id)
            .fetch_optional(&s.db)
            .await?
            .ok_or(AppError::NotFound)?;
    let file_path = row.get::<String, _>("file_path");
    let existing = row.get::<Option<String>, _>("transcript");
    let segment_start = row.get::<i64, _>("start_ms");
    let segment_end = row.get::<i64, _>("end_ms");
    let sequence_no = row.get::<i64, _>("sequence_no");
    let speaker_id = row.get::<Option<String>, _>("speaker_id");
    let speaker_name = row.get::<Option<String>, _>("speaker_name");
    let transcript = s
        .transcriber
        .transcribe(&file_path, existing.as_deref())
        .await
        .map_err(|e| {
            warn!(
                meeting_id = %meeting_id,
                segment_id = %segment_id,
                sequence_no,
                error = %e,
                "ASR provider call failed"
            );
            AppError::Processing(e)
        })?;
    info!(
        meeting_id = %meeting_id,
        segment_id = %segment_id,
        sequence_no,
        chars = transcript.chars().count(),
        reused_existing = existing.as_deref().map(str::trim).map_or(false, |t| !t.is_empty()),
        "segment transcribed"
    );
    sqlx::query(
        "UPDATE audio_segments SET status='completed', transcript=? WHERE id=? AND meeting_id=?",
    )
    .bind(&transcript)
    .bind(segment_id)
    .bind(meeting_id)
    .execute(&s.db)
    .await?;
    publish_event(
        s,
        meeting_id,
        "segment.transcribed",
        json!({
            "segment_id": segment_id,
            "sequence_no": sequence_no,
            "speaker_id": speaker_id,
            "speaker_name": speaker_name,
            "start_ms": segment_start,
            "end_ms": segment_end,
            "transcript": transcript,
        }),
    );
    let affected = sqlx::query(
        "SELECT MIN(window_start_ms) value FROM rolling_summaries
         WHERE meeting_id=? AND window_start_ms < ? AND window_end_ms > ?",
    )
    .bind(meeting_id)
    .bind(segment_end)
    .bind(segment_start)
    .fetch_one(&s.db)
    .await?
    .get::<Option<i64>, _>("value");
    if let Some(window_start) = affected {
        enqueue_rebuild(&s.db, meeting_id, window_start).await?;
        return Ok(());
    }
    let ended = sqlx::query("SELECT status='ended' value FROM meetings WHERE id=?")
        .bind(meeting_id)
        .fetch_one(&s.db)
        .await?
        .get::<bool, _>("value");
    enqueue_summary(&s.db, meeting_id, ended).await
}

async fn process_rebuild(s: &AppState, meeting_id: &str, target: &str) -> Result<(), AppError> {
    let affected_start = target
        .parse::<i64>()
        .map_err(|_| AppError::Processing("invalid rebuild target".into()))?;
    let mut tx = s.db.begin().await?;
    sqlx::query("DELETE FROM jobs WHERE meeting_id=? AND job_type='summary' AND status!='running'")
        .bind(meeting_id)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "DELETE FROM meeting_board_versions WHERE source_summary_id IN
         (SELECT id FROM rolling_summaries WHERE meeting_id=? AND window_end_ms > ?)",
    )
    .bind(meeting_id)
    .bind(affected_start)
    .execute(&mut *tx)
    .await?;
    sqlx::query("DELETE FROM rolling_summaries WHERE meeting_id=? AND window_end_ms > ?")
        .bind(meeting_id)
        .bind(affected_start)
        .execute(&mut *tx)
        .await?;
    let previous = sqlx::query(
        "SELECT version,content_json FROM meeting_board_versions
         WHERE meeting_id=? ORDER BY version DESC LIMIT 1",
    )
    .bind(meeting_id)
    .fetch_optional(&mut *tx)
    .await?;
    let board_version = if let Some(row) = previous {
        let version = row.get::<i64, _>("version");
        sqlx::query(
            "INSERT INTO meeting_boards(meeting_id,version,content_json) VALUES(?,?,?)
             ON CONFLICT(meeting_id) DO UPDATE SET version=excluded.version,
             content_json=excluded.content_json,updated_at=CURRENT_TIMESTAMP",
        )
        .bind(meeting_id)
        .bind(version)
        .bind(row.get::<String, _>("content_json"))
        .execute(&mut *tx)
        .await?;
        version
    } else {
        sqlx::query("DELETE FROM meeting_boards WHERE meeting_id=?")
            .bind(meeting_id)
            .execute(&mut *tx)
            .await?;
        0
    };
    let last_summary_end = sqlx::query(
        "SELECT COALESCE(MAX(window_end_ms),0) value FROM rolling_summaries WHERE meeting_id=?",
    )
    .bind(meeting_id)
    .fetch_one(&mut *tx)
    .await?
    .get::<i64, _>("value");
    let window = sqlx::query("SELECT summary_window_ms FROM meetings WHERE id=?")
        .bind(meeting_id)
        .fetch_one(&mut *tx)
        .await?
        .get::<i64, _>("summary_window_ms");
    let next_summary_end = last_summary_end + window;
    sqlx::query("UPDATE meetings SET board_version=?,next_summary_end_ms=? WHERE id=?")
        .bind(board_version)
        .bind(next_summary_end)
        .bind(meeting_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    publish_event(
        s,
        meeting_id,
        "board.updated",
        json!({"version": board_version, "reason": "rebuild"}),
    );
    let ended = sqlx::query("SELECT status='ended' value FROM meetings WHERE id=?")
        .bind(meeting_id)
        .fetch_one(&s.db)
        .await?
        .get::<bool, _>("value");
    enqueue_summary(&s.db, meeting_id, ended).await
}
async fn process_summary(s: &AppState, meeting_id: &str, target: &str) -> Result<(), AppError> {
    let end = target
        .strip_prefix("final:")
        .unwrap_or(target)
        .parse::<i64>()
        .map_err(|_| AppError::Processing(format!("invalid summary target: {target}")))?;
    let window = sqlx::query("SELECT summary_window_ms FROM meetings WHERE id=?")
        .bind(meeting_id)
        .fetch_one(&s.db)
        .await?
        .get::<i64, _>("summary_window_ms");
    let start = if target.starts_with("final:") {
        sqlx::query(
            "SELECT COALESCE(MAX(window_end_ms),0) value FROM rolling_summaries WHERE meeting_id=?",
        )
        .bind(meeting_id)
        .fetch_one(&s.db)
        .await?
        .get::<i64, _>("value")
    } else {
        end - window
    };
    let rows=sqlx::query("SELECT COALESCE(s.name,'Unknown') speaker_name,transcript FROM audio_segments a LEFT JOIN speakers s ON s.id=a.speaker_id WHERE a.meeting_id=? AND a.status='completed' AND a.start_ms < ? AND a.end_ms > ? ORDER BY a.start_ms").bind(meeting_id).bind(end).bind(start).fetch_all(&s.db).await?;
    let transcript = rows
        .into_iter()
        .filter_map(|r| {
            r.get::<Option<String>, _>("transcript")
                .map(|text| format!("{}: {}", r.get::<String, _>("speaker_name"), text))
        })
        .collect::<Vec<_>>()
        .join("\n");
    let document = s
        .summarizer
        .summarize(start, end, &transcript)
        .await
        .map_err(AppError::Processing)?;
    let document = normalize_summary(document);
    let content =
        serde_json::to_value(&document).map_err(|e| AppError::Processing(e.to_string()))?;
    let summary_id = Uuid::new_v4().to_string();
    let mut tx = s.db.begin().await?;
    let inserted=sqlx::query("INSERT OR IGNORE INTO rolling_summaries(id,meeting_id,window_start_ms,window_end_ms,content_json) VALUES(?,?,?,?,?)").bind(&summary_id).bind(meeting_id).bind(start).bind(end).bind(content.to_string()).execute(&mut *tx).await?;
    if inserted.rows_affected() == 0 {
        tx.commit().await?;
        return Ok(());
    }
    let current = sqlx::query(
        "SELECT COALESCE(MAX(version),0) version FROM meeting_board_versions WHERE meeting_id=?",
    )
    .bind(meeting_id)
    .fetch_one(&mut *tx)
    .await?
    .get::<i64, _>("version");
    let version = current + 1;
    let mut board = match sqlx::query("SELECT content_json FROM meeting_boards WHERE meeting_id=?")
        .bind(meeting_id)
        .fetch_optional(&mut *tx)
        .await?
    {
        Some(row) => serde_json::from_str::<Value>(&row.get::<String, _>("content_json"))
            .unwrap_or_else(|_| empty_board()),
        None => empty_board(),
    };
    merge_board(&mut board, &document);
    sqlx::query("INSERT INTO meeting_boards(meeting_id,version,content_json) VALUES(?,?,?) ON CONFLICT(meeting_id) DO UPDATE SET version=excluded.version,content_json=excluded.content_json,updated_at=CURRENT_TIMESTAMP").bind(meeting_id).bind(version).bind(board.to_string()).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO meeting_board_versions(id,meeting_id,version,source_summary_id,content_json) VALUES(?,?,?,?,?)").bind(Uuid::new_v4().to_string()).bind(meeting_id).bind(version).bind(&summary_id).bind(board.to_string()).execute(&mut *tx).await?;
    sqlx::query("UPDATE meetings SET board_version=?,next_summary_end_ms=MAX(next_summary_end_ms,?) WHERE id=?").bind(version).bind(end+window).bind(meeting_id).execute(&mut *tx).await?;
    tx.commit().await?;
    publish_event(
        s,
        meeting_id,
        "summary.created",
        json!({
            "summary_id": summary_id,
            "window_start_ms": start,
            "window_end_ms": end,
            "content": content,
        }),
    );
    publish_event(
        s,
        meeting_id,
        "board.updated",
        json!({"version": version, "content": board, "reason": "summary"}),
    );
    let ended = sqlx::query("SELECT status='ended' value FROM meetings WHERE id=?")
        .bind(meeting_id)
        .fetch_one(&s.db)
        .await?
        .get::<bool, _>("value");
    enqueue_summary(&s.db, meeting_id, ended).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{body::Body, http::Request};
    use sqlx::sqlite::SqlitePoolOptions;
    use tower::ServiceExt;

    /// 固定返回的转写桩：已有转写时透传，否则返回固定文本。
    struct FixedTranscriber(String);

    #[async_trait]
    impl Transcriber for FixedTranscriber {
        async fn transcribe(
            &self,
            _file_path: &str,
            existing: Option<&str>,
        ) -> Result<String, String> {
            if let Some(text) = existing.map(str::trim).filter(|text| !text.is_empty()) {
                return Ok(text.to_string());
            }
            Ok(self.0.clone())
        }
    }

    /// 永远失败的转写桩，用于验证失败路径。
    struct FailingTranscriber;

    #[async_trait]
    impl Transcriber for FailingTranscriber {
        async fn transcribe(
            &self,
            _file_path: &str,
            _existing: Option<&str>,
        ) -> Result<String, String> {
            Err("asr provider unavailable".to_string())
        }
    }

    /// 固定返回的摘要桩。
    struct FixedSummarizer(SummaryDocument);

    #[async_trait]
    impl Summarizer for FixedSummarizer {
        async fn summarize(
            &self,
            _start_ms: i64,
            _end_ms: i64,
            _transcript: &str,
        ) -> Result<SummaryDocument, String> {
            Ok(self.0.clone())
        }
    }

    fn fixed_document() -> SummaryDocument {
        SummaryDocument {
            topics: vec!["发布计划".into()],
            decisions: vec!["下周三上线".into()],
            action_items: vec![ActionItem {
                content: "补充回归测试".into(),
                owner: Some("Alice".into()),
                due_date: None,
                status: "open".into(),
            }],
            key_points: vec!["接口联调完成".into()],
            ..SummaryDocument::default()
        }
    }

    pub(crate) async fn test_db() -> SqlitePool {
        let db = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        for statement in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(statement).execute(&db).await.unwrap();
        }
        db
    }

    fn test_state(db: &SqlitePool) -> AppState {
        let (events, _) = broadcast::channel(16);
        AppState {
            db: db.clone(),
            audio_dir: Arc::new(PathBuf::from("data/audio")),
            transcriber: Arc::new(FixedTranscriber("固定转写文本".into())),
            summarizer: Arc::new(FixedSummarizer(fixed_document())),
            max_upload_bytes: 64 * 1024,
            job_notify: Arc::new(Notify::new()),
            events,
            ingest_stop: IngestStopMap::default(),
        }
    }

    /// 启动一个返回固定响应的本地 HTTP 服务，模拟 OpenAI 兼容的 ASR/LLM provider。
    async fn spawn_fixed_provider(routes: Vec<(&'static str, StatusCode, Value)>) -> String {
        let mut app = Router::new();
        for (path, status, body) in routes {
            app = app.route(
                path,
                post(move || {
                    let body = body.clone();
                    async move { (status, Json(body)) }
                }),
            );
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        format!("http://{addr}")
    }

    fn multipart_request(
        uri: &str,
        fields: &[(&str, &str)],
        audio: Option<(&str, &[u8])>,
    ) -> Request<Body> {
        let boundary = "DITINGTESTBOUNDARY";
        let mut body = Vec::new();
        for (name, value) in fields {
            body.extend(
                format!("--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n")
                    .into_bytes(),
            );
        }
        if let Some((filename, bytes)) = audio {
            body.extend(
                format!("--{boundary}\r\nContent-Disposition: form-data; name=\"audio\"; filename=\"{filename}\"\r\nContent-Type: application/octet-stream\r\n\r\n")
                    .into_bytes(),
            );
            body.extend_from_slice(bytes);
            body.extend(b"\r\n");
        }
        body.extend(format!("--{boundary}--\r\n").into_bytes());
        Request::builder()
            .method("POST")
            .uri(uri)
            .header(
                "content-type",
                format!("multipart/form-data; boundary={boundary}"),
            )
            .body(Body::from(body))
            .unwrap()
    }

    #[test]
    fn local_summarizer_splits_key_points() {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let document = runtime
            .block_on(LocalSummarizer.summarize(0, 300_000, "讨论需求。确认接口\n安排测试"))
            .unwrap();
        assert_eq!(document.key_points, ["讨论需求", "确认接口", "安排测试"]);
    }

    #[test]
    fn board_merge_deduplicates_and_keeps_actions() {
        let mut board = empty_board();
        let summary = SummaryDocument {
            topics: vec!["需求".into()],
            key_points: vec!["接口完成".into()],
            action_items: vec![ActionItem {
                content: "补测试".into(),
                owner: Some("Alice".into()),
                ..ActionItem::default()
            }],
            ..SummaryDocument::default()
        };
        merge_board(&mut board, &summary);
        merge_board(&mut board, &summary);
        assert_eq!(board["topics"].as_array().unwrap().len(), 1);
        assert_eq!(board["key_points"].as_array().unwrap().len(), 1);
        assert_eq!(board["action_items"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn rebuild_removes_affected_state_and_requeues_summary() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status,next_summary_end_ms,board_version) VALUES('m','test','running',600000,1)").execute(&db).await.unwrap();
        sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,transcript,status) VALUES('a','m',1,0,300000,'audio.wav','text','completed')").execute(&db).await.unwrap();
        sqlx::query("INSERT INTO rolling_summaries(id,meeting_id,window_start_ms,window_end_ms,content_json) VALUES('s','m',0,300000,'{}')").execute(&db).await.unwrap();
        sqlx::query(
            "INSERT INTO meeting_boards(meeting_id,version,content_json) VALUES('m',1,'{}')",
        )
        .execute(&db)
        .await
        .unwrap();
        sqlx::query("INSERT INTO meeting_board_versions(id,meeting_id,version,source_summary_id,content_json) VALUES('v','m',1,'s','{}')").execute(&db).await.unwrap();
        sqlx::query("INSERT INTO jobs(id,job_type,meeting_id,target_id,status) VALUES('j','summary','m','300000','completed')").execute(&db).await.unwrap();
        let state = AppState {
            db: db.clone(),
            audio_dir: Arc::new(PathBuf::from("data/audio")),
            transcriber: Arc::new(LocalTranscriber),
            summarizer: Arc::new(LocalSummarizer),
            max_upload_bytes: 1024,
            ..test_state(&db)
        };

        process_rebuild(&state, "m", "0").await.unwrap();

        let summary_count = sqlx::query("SELECT COUNT(*) value FROM rolling_summaries")
            .fetch_one(&db)
            .await
            .unwrap()
            .get::<i64, _>("value");
        let meeting =
            sqlx::query("SELECT board_version,next_summary_end_ms FROM meetings WHERE id='m'")
                .fetch_one(&db)
                .await
                .unwrap();
        let queued = sqlx::query(
            "SELECT COUNT(*) value FROM jobs WHERE job_type='summary' AND status='pending' AND target_id='300000'",
        )
        .fetch_one(&db)
        .await
        .unwrap()
        .get::<i64, _>("value");
        assert_eq!(summary_count, 0);
        assert_eq!(meeting.get::<i64, _>("board_version"), 0);
        assert_eq!(meeting.get::<i64, _>("next_summary_end_ms"), 300_000);
        assert_eq!(queued, 1);
    }

    #[tokio::test]
    async fn rebuild_jobs_are_idempotent_per_window() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
            .execute(&db)
            .await
            .unwrap();
        enqueue_rebuild(&db, "m", 0).await.unwrap();
        enqueue_rebuild(&db, "m", 0).await.unwrap();
        let count = sqlx::query(
            "SELECT COUNT(*) value FROM jobs WHERE meeting_id='m' AND job_type='rebuild'",
        )
        .fetch_one(&db)
        .await
        .unwrap()
        .get::<i64, _>("value");
        assert_eq!(count, 1);
    }

    #[tokio::test]
    async fn deleting_meeting_removes_records_and_audio() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','ended')")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO speakers(id,meeting_id,name) VALUES('sp','m','Alice')")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO audio_segments(id,meeting_id,speaker_id,sequence_no,start_ms,end_ms,file_path,status) VALUES('a','m','sp',1,0,1,'audio.wav','completed')").execute(&db).await.unwrap();
        sqlx::query("INSERT INTO rolling_summaries(id,meeting_id,window_start_ms,window_end_ms,content_json) VALUES('s','m',0,1,'{}')").execute(&db).await.unwrap();
        sqlx::query(
            "INSERT INTO meeting_boards(meeting_id,version,content_json) VALUES('m',1,'{}')",
        )
        .execute(&db)
        .await
        .unwrap();
        sqlx::query("INSERT INTO meeting_board_versions(id,meeting_id,version,source_summary_id,content_json) VALUES('v','m',1,'s','{}')").execute(&db).await.unwrap();
        sqlx::query(
            "INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES('j','transcribe','m','a')",
        )
        .execute(&db)
        .await
        .unwrap();
        let audio_root = std::env::temp_dir().join(format!("diting-delete-{}", Uuid::new_v4()));
        fs::create_dir_all(audio_root.join("m")).await.unwrap();
        fs::write(audio_root.join("m/audio.wav"), b"audio")
            .await
            .unwrap();
        let state = AppState {
            db: db.clone(),
            audio_dir: Arc::new(audio_root.clone()),
            transcriber: Arc::new(LocalTranscriber),
            summarizer: Arc::new(LocalSummarizer),
            max_upload_bytes: 1024,
            ..test_state(&db)
        };

        let status = delete_meeting(State(state), Path("m".into()))
            .await
            .unwrap();

        let records = sqlx::query(
            "SELECT
              (SELECT COUNT(*) FROM meetings) + (SELECT COUNT(*) FROM speakers) +
              (SELECT COUNT(*) FROM audio_segments) + (SELECT COUNT(*) FROM rolling_summaries) +
              (SELECT COUNT(*) FROM meeting_boards) + (SELECT COUNT(*) FROM meeting_board_versions) +
              (SELECT COUNT(*) FROM jobs) value",
        )
        .fetch_one(&db)
        .await
        .unwrap()
        .get::<i64, _>("value");
        assert_eq!(status, StatusCode::NO_CONTENT);
        assert_eq!(records, 0);
        assert!(!audio_root.join("m").exists());
        fs::remove_dir(audio_root).await.unwrap();
    }

    #[test]
    fn normalize_summary_trims_dedups_and_fixes_status() {
        let summary = SummaryDocument {
            topics: vec![" 发布 ".into(), "发布".into(), "".into()],
            action_items: vec![
                ActionItem {
                    content: "补测试".into(),
                    owner: Some("   ".into()),
                    status: "unsupported".into(),
                    ..ActionItem::default()
                },
                ActionItem {
                    content: "补测试".into(),
                    status: "done".into(),
                    ..ActionItem::default()
                },
            ],
            ..SummaryDocument::default()
        };
        let normalized = normalize_summary(summary);
        assert_eq!(normalized.topics, ["发布"]);
        assert_eq!(normalized.action_items.len(), 1);
        assert_eq!(normalized.action_items[0].owner, None);
        assert_eq!(normalized.action_items[0].status, "open");
    }

    #[test]
    fn sanitize_filename_removes_path_separators_and_non_ascii() {
        assert_eq!(sanitize_filename("../etc/录音-1.wav"), "..etc-1.wav");
        assert!(sanitize_filename("中文").is_empty());
    }

    #[tokio::test]
    async fn openai_transcriber_returns_fixed_provider_text() {
        let base_url = spawn_fixed_provider(vec![(
            "/audio/transcriptions",
            StatusCode::OK,
            json!({"text": "固定的转写结果"}),
        )])
        .await;
        let transcriber = OpenAiTranscriber {
            client: reqwest::Client::new(),
            base_url,
            api_key: "test-key".into(),
            model: "whisper-1".into(),
        };
        let path = std::env::temp_dir().join(format!("diting-asr-{}.wav", Uuid::new_v4()));
        fs::write(&path, b"audio").await.unwrap();
        let text = transcriber
            .transcribe(path.to_str().unwrap(), None)
            .await
            .unwrap();
        fs::remove_file(&path).await.unwrap();
        assert_eq!(text, "固定的转写结果");
    }

    #[tokio::test]
    async fn openai_transcriber_prefers_existing_transcript() {
        // base_url 指向不可达地址：若真的发起 HTTP 请求，测试会失败
        let transcriber = OpenAiTranscriber {
            client: reqwest::Client::new(),
            base_url: "http://127.0.0.1:1".into(),
            api_key: "test-key".into(),
            model: "whisper-1".into(),
        };
        let text = transcriber
            .transcribe("unused.wav", Some(" 已提供的转写 "))
            .await
            .unwrap();
        assert_eq!(text, "已提供的转写");
    }

    #[tokio::test]
    async fn openai_transcriber_surfaces_provider_error() {
        let base_url = spawn_fixed_provider(vec![(
            "/audio/transcriptions",
            StatusCode::INTERNAL_SERVER_ERROR,
            json!({"error": "provider down"}),
        )])
        .await;
        let transcriber = OpenAiTranscriber {
            client: reqwest::Client::new(),
            base_url,
            api_key: "test-key".into(),
            model: "whisper-1".into(),
        };
        let path = std::env::temp_dir().join(format!("diting-asr-{}.wav", Uuid::new_v4()));
        fs::write(&path, b"audio").await.unwrap();
        let error = transcriber
            .transcribe(path.to_str().unwrap(), None)
            .await
            .unwrap_err();
        fs::remove_file(&path).await.unwrap();
        assert!(error.contains("500"), "unexpected error: {error}");
    }

    #[tokio::test]
    async fn openai_summarizer_parses_fixed_provider_json() {
        let document = json!({
            "topics": ["发布计划"],
            "decisions": ["下周三上线"],
            "action_items": [{"content": "补充回归测试", "owner": "Alice", "due_date": null, "status": "open"}],
            "key_points": ["接口联调完成"]
        });
        let base_url = spawn_fixed_provider(vec![(
            "/chat/completions",
            StatusCode::OK,
            json!({"choices": [{"message": {"content": document.to_string()}}]}),
        )])
        .await;
        let summarizer = OpenAiSummarizer {
            client: reqwest::Client::new(),
            base_url,
            api_key: "test-key".into(),
            model: "test-model".into(),
        };
        let parsed = summarizer
            .summarize(0, 300_000, "Alice: 讨论发布计划")
            .await
            .unwrap();
        assert_eq!(parsed.topics, ["发布计划"]);
        assert_eq!(parsed.decisions, ["下周三上线"]);
        assert_eq!(parsed.key_points, ["接口联调完成"]);
        assert_eq!(parsed.action_items.len(), 1);
        assert_eq!(parsed.action_items[0].content, "补充回归测试");
        assert_eq!(parsed.action_items[0].owner.as_deref(), Some("Alice"));
    }

    #[tokio::test]
    async fn openai_summarizer_strips_markdown_code_fence() {
        let content = format!("```json\n{}\n```", json!({"topics": ["发布"]}));
        let base_url = spawn_fixed_provider(vec![(
            "/chat/completions",
            StatusCode::OK,
            json!({"choices": [{"message": {"content": content}}]}),
        )])
        .await;
        let summarizer = OpenAiSummarizer {
            client: reqwest::Client::new(),
            base_url,
            api_key: "test-key".into(),
            model: "test-model".into(),
        };
        let parsed = summarizer
            .summarize(0, 300_000, "transcript")
            .await
            .unwrap();
        assert_eq!(parsed.topics, ["发布"]);
    }

    #[tokio::test]
    async fn openai_summarizer_rejects_invalid_json_content() {
        let base_url = spawn_fixed_provider(vec![(
            "/chat/completions",
            StatusCode::OK,
            json!({"choices": [{"message": {"content": "not a json document"}}]}),
        )])
        .await;
        let summarizer = OpenAiSummarizer {
            client: reqwest::Client::new(),
            base_url,
            api_key: "test-key".into(),
            model: "test-model".into(),
        };
        let error = summarizer
            .summarize(0, 300_000, "transcript")
            .await
            .unwrap_err();
        assert!(
            error.contains("invalid SummaryDocument JSON"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn transcription_with_fixed_provider_completes_and_enqueues_summary() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,status) VALUES('a','m',1,0,300000,'audio.wav','uploaded')").execute(&db).await.unwrap();
        let state = test_state(&db);

        process_transcription(&state, "a", "m").await.unwrap();

        let segment = sqlx::query("SELECT status,transcript FROM audio_segments WHERE id='a'")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(segment.get::<String, _>("status"), "completed");
        assert_eq!(
            segment.get::<Option<String>, _>("transcript").as_deref(),
            Some("固定转写文本")
        );
        let queued = sqlx::query(
            "SELECT COUNT(*) value FROM jobs WHERE job_type='summary' AND status='pending' AND target_id='300000'",
        )
        .fetch_one(&db)
        .await
        .unwrap()
        .get::<i64, _>("value");
        assert_eq!(queued, 1);
    }

    #[tokio::test]
    async fn summary_with_fixed_provider_updates_board_once_per_window() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,transcript,status) VALUES('a','m',1,0,300000,'audio.wav','讨论发布计划','completed')").execute(&db).await.unwrap();
        let state = test_state(&db);

        process_summary(&state, "m", "300000").await.unwrap();
        process_summary(&state, "m", "300000").await.unwrap(); // 同窗口幂等

        let summary = sqlx::query(
            "SELECT COUNT(*) count, MAX(window_start_ms) start, MAX(window_end_ms) end, MAX(content_json) content FROM rolling_summaries WHERE meeting_id='m'",
        )
        .fetch_one(&db)
        .await
        .unwrap();
        assert_eq!(summary.get::<i64, _>("count"), 1);
        assert_eq!(summary.get::<i64, _>("start"), 0);
        assert_eq!(summary.get::<i64, _>("end"), 300_000);
        let content: Value = serde_json::from_str(&summary.get::<String, _>("content")).unwrap();
        assert_eq!(content["topics"], json!(["发布计划"]));

        let board =
            sqlx::query("SELECT version,content_json FROM meeting_boards WHERE meeting_id='m'")
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(board.get::<i64, _>("version"), 1);
        let board_content: Value =
            serde_json::from_str(&board.get::<String, _>("content_json")).unwrap();
        assert_eq!(board_content["decisions"], json!(["下周三上线"]));
        assert_eq!(board_content["action_items"][0]["content"], "补充回归测试");
        assert_eq!(board_content["action_items"][0]["owner"], "Alice");

        let meeting =
            sqlx::query("SELECT board_version,next_summary_end_ms FROM meetings WHERE id='m'")
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(meeting.get::<i64, _>("board_version"), 1);
        assert_eq!(meeting.get::<i64, _>("next_summary_end_ms"), 600_000);
    }

    #[tokio::test]
    async fn summary_rejects_malformed_target() {
        let db = test_db().await;
        let state = test_state(&db);
        let result = process_summary(&state, "m", "not-a-window").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn worker_pipeline_runs_transcription_and_summary_with_fixed_providers() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,status) VALUES('a','m',1,0,300000,'audio.wav','uploaded')").execute(&db).await.unwrap();
        sqlx::query(
            "INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES('j1','transcribe','m','a')",
        )
        .execute(&db)
        .await
        .unwrap();
        let state = test_state(&db);

        for _ in 0..5 {
            process_jobs(&state).await.unwrap();
        }

        let segment = sqlx::query("SELECT status,transcript FROM audio_segments WHERE id='a'")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(segment.get::<String, _>("status"), "completed");
        assert_eq!(
            segment.get::<Option<String>, _>("transcript").as_deref(),
            Some("固定转写文本")
        );
        let summaries =
            sqlx::query("SELECT COUNT(*) value FROM rolling_summaries WHERE meeting_id='m'")
                .fetch_one(&db)
                .await
                .unwrap()
                .get::<i64, _>("value");
        assert_eq!(summaries, 1);
        let board = sqlx::query("SELECT content_json FROM meeting_boards WHERE meeting_id='m'")
            .fetch_one(&db)
            .await
            .unwrap();
        let board_content: Value =
            serde_json::from_str(&board.get::<String, _>("content_json")).unwrap();
        assert_eq!(board_content["topics"], json!(["发布计划"]));
        let open_jobs = sqlx::query(
            "SELECT COUNT(*) value FROM jobs WHERE status IN ('pending','running','failed')",
        )
        .fetch_one(&db)
        .await
        .unwrap()
        .get::<i64, _>("value");
        assert_eq!(open_jobs, 0);
    }

    #[tokio::test]
    async fn permanently_failed_transcription_does_not_block_final_summary() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','ended')")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,transcript,status) VALUES('ok','m',1,0,1000,'a.wav','已完成','completed')").execute(&db).await.unwrap();
        sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,status) VALUES('bad','m',2,1000,2000,'b.wav','uploaded')").execute(&db).await.unwrap();
        // retry_count 已达上限，下一次失败即为永久失败
        sqlx::query("INSERT INTO jobs(id,job_type,meeting_id,target_id,retry_count) VALUES('j1','transcribe','m','bad',2)")
            .execute(&db)
            .await
            .unwrap();
        let state = AppState {
            transcriber: Arc::new(FailingTranscriber),
            ..test_state(&db)
        };

        process_jobs(&state).await.unwrap();

        let job = sqlx::query("SELECT status FROM jobs WHERE id='j1'")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(job.get::<String, _>("status"), "failed");
        let segment = sqlx::query("SELECT status FROM audio_segments WHERE id='bad'")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(segment.get::<String, _>("status"), "failed");
        // 失败分段不再阻塞：应为已完成部分排入最终摘要
        let queued = sqlx::query(
            "SELECT COUNT(*) value FROM jobs WHERE job_type='summary' AND status='pending' AND target_id='final:1000'",
        )
        .fetch_one(&db)
        .await
        .unwrap()
        .get::<i64, _>("value");
        assert_eq!(queued, 1);

        process_jobs(&state).await.unwrap();
        let summary = sqlx::query(
            "SELECT window_start_ms,window_end_ms FROM rolling_summaries WHERE meeting_id='m'",
        )
        .fetch_one(&db)
        .await
        .unwrap();
        assert_eq!(summary.get::<i64, _>("window_start_ms"), 0);
        assert_eq!(summary.get::<i64, _>("window_end_ms"), 1000);
    }

    #[tokio::test]
    async fn create_meeting_rejects_blank_title() {
        let db = test_db().await;
        let app = build_router(test_state(&db));
        let response = ServiceExt::oneshot(
            app,
            Request::builder()
                .method("POST")
                .uri("/api/v1/meetings")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"title":"   "}"#))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn meeting_lifecycle_over_http() {
        let db = test_db().await;
        let audio_root = std::env::temp_dir().join(format!("diting-http-{}", Uuid::new_v4()));
        let state = AppState {
            audio_dir: Arc::new(audio_root.clone()),
            ..test_state(&db)
        };
        let app = build_router(state);

        let response = ServiceExt::oneshot(
            app.clone(),
            Request::builder()
                .method("POST")
                .uri("/api/v1/meetings")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"title":"产品周会"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let id = serde_json::from_slice::<Value>(&bytes).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        let get = |app: Router| {
            let uri = format!("/api/v1/meetings/{id}");
            ServiceExt::oneshot(
                app,
                Request::builder().uri(uri).body(Body::empty()).unwrap(),
            )
        };
        let response = get(app.clone()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["status"],
            "running"
        );

        let response = ServiceExt::oneshot(
            app.clone(),
            Request::builder()
                .method("POST")
                .uri(format!("/api/v1/meetings/{id}/end"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let response = get(app.clone()).await.unwrap();
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["status"],
            "ended"
        );

        let response = ServiceExt::oneshot(
            app.clone(),
            Request::builder()
                .method("DELETE")
                .uri(format!("/api/v1/meetings/{id}"))
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::NO_CONTENT);

        let response = get(app).await.unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let _ = fs::remove_dir_all(&audio_root).await;
    }

    #[tokio::test]
    async fn upload_segment_rejects_invalid_window() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
            .execute(&db)
            .await
            .unwrap();
        let app = build_router(test_state(&db));
        let response = ServiceExt::oneshot(
            app,
            multipart_request(
                "/api/v1/meetings/m/segments",
                &[
                    ("sequence_no", "1"),
                    ("start_ms", "1000"),
                    ("end_ms", "1000"),
                ],
                Some(("a.wav", b"audio")),
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn upload_segment_persists_audio_and_rejects_duplicate_sequence() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
            .execute(&db)
            .await
            .unwrap();
        let audio_root = std::env::temp_dir().join(format!("diting-upload-{}", Uuid::new_v4()));
        let state = AppState {
            audio_dir: Arc::new(audio_root.clone()),
            ..test_state(&db)
        };
        let app = build_router(state.clone());

        let mut receiver = state.events.subscribe();
        let request = || {
            multipart_request(
                "/api/v1/meetings/m/segments",
                &[
                    ("sequence_no", "1"),
                    ("start_ms", "0"),
                    ("end_ms", "300000"),
                ],
                Some(("sample.wav", b"audio")),
            )
        };

        let response = ServiceExt::oneshot(app.clone(), request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let segment_id = serde_json::from_slice::<Value>(&bytes).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();

        // 上传后应立即通知 worker 并向 SSE 订阅者广播事件
        let event = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(event.meeting_id, "m");
        assert_eq!(event.kind, "segment.uploaded");
        assert_eq!(event.data["segment_id"], Value::String(segment_id.clone()));

        let segment = sqlx::query("SELECT file_path,status FROM audio_segments WHERE id=?")
            .bind(&segment_id)
            .fetch_one(&db)
            .await
            .unwrap();
        let file_path = segment.get::<String, _>("file_path");
        assert!(std::path::Path::new(&file_path).exists());
        assert!(file_path.starts_with(audio_root.to_str().unwrap()));
        assert_eq!(segment.get::<String, _>("status"), "uploaded");
        let job = sqlx::query(
            "SELECT COUNT(*) value FROM jobs WHERE job_type='transcribe' AND target_id=?",
        )
        .bind(&segment_id)
        .fetch_one(&db)
        .await
        .unwrap()
        .get::<i64, _>("value");
        assert_eq!(job, 1);

        // 相同 sequence_no 重复上传应返回 400 而不是 500
        let response = ServiceExt::oneshot(app, request()).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        assert!(serde_json::from_slice::<Value>(&bytes).unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("sequence_no"));
        let orphans = sqlx::query("SELECT COUNT(*) value FROM audio_segments WHERE meeting_id='m'")
            .fetch_one(&db)
            .await
            .unwrap()
            .get::<i64, _>("value");
        assert_eq!(orphans, 1);
        fs::remove_dir_all(&audio_root).await.unwrap();
    }

    #[tokio::test]
    async fn upload_segment_accepts_transcript_only_and_skips_asr() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
            .execute(&db)
            .await
            .unwrap();
        let state = test_state(&db);
        let app = build_router(state.clone());

        let response = ServiceExt::oneshot(
            app,
            multipart_request(
                "/api/v1/meetings/m/segments",
                &[
                    ("sequence_no", "1"),
                    ("start_ms", "0"),
                    ("end_ms", "5000"),
                    ("transcript", " 实时 ASR 文本 "),
                ],
                None,
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        process_transcription(&state, &sqlx::query("SELECT id FROM audio_segments WHERE meeting_id='m'")
            .fetch_one(&db)
            .await
            .unwrap()
            .get::<String, _>("id"), "m")
        .await
        .unwrap();

        let segment = sqlx::query("SELECT file_path,status,transcript FROM audio_segments WHERE meeting_id='m'")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(segment.get::<String, _>("file_path"), "");
        assert_eq!(segment.get::<String, _>("status"), "completed");
        assert_eq!(
            segment.get::<Option<String>, _>("transcript").as_deref(),
            Some("实时 ASR 文本")
        );
    }

    #[tokio::test]
    async fn update_segment_edits_transcript_and_speaker() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','ended')")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,transcript,status) VALUES('seg1','m',1,0,5000,'','原文','completed')")
            .execute(&db)
            .await
            .unwrap();
        let state = test_state(&db);
        let app = build_router(state);

        let response = ServiceExt::oneshot(
            app,
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/meetings/m/segments/seg1")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"transcript":"修订文本","speaker_name":"王五"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::OK);

        let row = sqlx::query("SELECT a.transcript,sp.name AS speaker_name FROM audio_segments a LEFT JOIN speakers sp ON sp.id=a.speaker_id WHERE a.id='seg1'")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(row.get::<String, _>("transcript"), "修订文本");
        assert_eq!(row.get::<Option<String>, _>("speaker_name").as_deref(), Some("王五"));

        // 空 body 报 400；不存在的分段报 404
        let app = build_router(test_state(&db));
        let response = ServiceExt::oneshot(
            app,
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/meetings/m/segments/seg1")
                .header("content-type", "application/json")
                .body(Body::from("{}"))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let app = build_router(test_state(&db));
        let response = ServiceExt::oneshot(
            app,
            Request::builder()
                .method("PATCH")
                .uri("/api/v1/meetings/m/segments/nope")
                .header("content-type", "application/json")
                .body(Body::from(r#"{"transcript":"x"}"#))
                .unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn upload_segment_requires_audio_or_transcript() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
            .execute(&db)
            .await
            .unwrap();
        let app = build_router(test_state(&db));
        let response = ServiceExt::oneshot(
            app,
            multipart_request(
                "/api/v1/meetings/m/segments",
                &[("sequence_no", "1"), ("start_ms", "0"), ("end_ms", "5000")],
                None,
            ),
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn create_meeting_validates_and_stores_summary_window() {
        let db = test_db().await;
        let app = build_router(test_state(&db));
        let create = |app: Router, body: &'static str| {
            ServiceExt::oneshot(
                app,
                Request::builder()
                    .method("POST")
                    .uri("/api/v1/meetings")
                    .header("content-type", "application/json")
                    .body(Body::from(body))
                    .unwrap(),
            )
        };
        let response = create(
            app.clone(),
            r#"{"title":"周会","summary_window_ms":5000}"#,
        )
        .await
        .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);

        let response = create(app, r#"{"title":"周会","summary_window_ms":30000}"#)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);
        let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
            .await
            .unwrap();
        let id = serde_json::from_slice::<Value>(&bytes).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string();
        let meeting = sqlx::query("SELECT summary_window_ms,next_summary_end_ms FROM meetings WHERE id=?")
            .bind(&id)
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(meeting.get::<i64, _>("summary_window_ms"), 30_000);
        assert_eq!(meeting.get::<i64, _>("next_summary_end_ms"), 30_000);
    }

    #[tokio::test]
    async fn custom_summary_window_drives_summary_scheduling() {
        let db = test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status,next_summary_end_ms,summary_window_ms) VALUES('m','test','running',30000,30000)")
            .execute(&db)
            .await
            .unwrap();
        sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,transcript,status) VALUES('a','m',1,0,30000,'','已完成','completed')")
            .execute(&db)
            .await
            .unwrap();
        let state = test_state(&db);
        let mut receiver = state.events.subscribe();

        process_summary(&state, "m", "30000").await.unwrap();

        let summary = sqlx::query(
            "SELECT window_start_ms,window_end_ms FROM rolling_summaries WHERE meeting_id='m'",
        )
        .fetch_one(&db)
        .await
        .unwrap();
        assert_eq!(summary.get::<i64, _>("window_start_ms"), 0);
        assert_eq!(summary.get::<i64, _>("window_end_ms"), 30_000);
        let meeting = sqlx::query("SELECT next_summary_end_ms FROM meetings WHERE id='m'")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(meeting.get::<i64, _>("next_summary_end_ms"), 60_000);

        // 摘要与会议板事件实时下发
        let first = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(first.kind, "summary.created");
        assert_eq!(first.data["window_end_ms"], json!(30_000));
        let second = tokio::time::timeout(Duration::from_secs(1), receiver.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.kind, "board.updated");
        assert_eq!(second.data["version"], json!(1));
    }
}
