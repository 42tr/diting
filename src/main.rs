use async_trait::async_trait;
use axum::{
    extract::{DefaultBodyLimit, Multipart, Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete as delete_route, get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    Row, SqlitePool,
};
use std::{env, net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};
use tokio::{
    fs,
    time::{interval, Duration},
};
use tracing::{error, info};
use uuid::Uuid;

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

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct SummaryDocument {
    #[serde(default)]
    topics: Vec<String>,
    #[serde(default)]
    decisions: Vec<String>,
    #[serde(default)]
    action_items: Vec<ActionItem>,
    #[serde(default)]
    open_questions: Vec<String>,
    #[serde(default)]
    risks: Vec<String>,
    #[serde(default)]
    key_points: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone, Default)]
struct ActionItem {
    content: String,
    #[serde(default)]
    owner: Option<String>,
    #[serde(default)]
    due_date: Option<String>,
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

const SCHEMA: &str = r#"
PRAGMA journal_mode = WAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
CREATE TABLE IF NOT EXISTS meetings (
  id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL,
  started_at TEXT, ended_at TEXT, next_summary_end_ms INTEGER NOT NULL DEFAULT 300000,
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

#[derive(Clone)]
struct AppState {
    db: SqlitePool,
    audio_dir: Arc<PathBuf>,
    transcriber: Arc<dyn Transcriber>,
    summarizer: Arc<dyn Summarizer>,
    max_upload_bytes: usize,
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
        };
        (status, Json(json!({"error": message}))).into_response()
    }
}
impl From<sqlx::Error> for AppError {
    fn from(e: sqlx::Error) -> Self {
        Self::Db(e)
    }
}

#[derive(Deserialize)]
struct CreateMeeting {
    title: String,
}
#[derive(Serialize)]
struct IdResponse {
    id: String,
}
#[derive(Deserialize)]
struct CreateSpeaker {
    name: String,
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
        _ => (Arc::new(LocalTranscriber), "local"),
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
        _ => (Arc::new(LocalSummarizer), "local"),
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
    let state = AppState {
        db,
        audio_dir: Arc::new(PathBuf::from("data/audio")),
        transcriber,
        summarizer,
        max_upload_bytes,
    };
    tokio::spawn(worker(state.clone()));
    let app = Router::new()
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
        .route("/api/v1/meetings/{id}/summaries", get(list_summaries))
        .route("/api/v1/meetings/{id}/board", get(get_board))
        .route(
            "/api/v1/meetings/{id}/board/versions",
            get(list_board_versions),
        )
        .layer(DefaultBodyLimit::max(max_upload_bytes))
        .with_state(state);
    let addr: SocketAddr = env::var("DITING_ADDR")
        .unwrap_or_else(|_| "0.0.0.0:3000".into())
        .parse()?;
    info!(%addr, "meeting service started");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
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

async fn create_meeting(
    State(s): State<AppState>,
    Json(input): Json<CreateMeeting>,
) -> Result<(StatusCode, Json<IdResponse>), AppError> {
    if input.title.trim().is_empty() {
        return Err(AppError::BadRequest("title is required".into()));
    }
    let id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO meetings(id,title,status,started_at) VALUES(?,?, 'running', CURRENT_TIMESTAMP)").bind(&id).bind(input.title.trim()).execute(&s.db).await?;
    Ok((StatusCode::CREATED, Json(IdResponse { id })))
}

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
    Ok(Json(json!({"id":id,"status":"pending"})))
}

async fn get_meeting(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    let row = sqlx::query("SELECT id,title,status,started_at,ended_at,board_version,next_summary_end_ms FROM meetings WHERE id=?").bind(&id).fetch_optional(&s.db).await?.ok_or(AppError::NotFound)?;
    Ok(Json(
        json!({"id":row.get::<String,_>("id"),"title":row.get::<String,_>("title"),"status":row.get::<String,_>("status"),"started_at":row.get::<Option<String>,_>("started_at"),"ended_at":row.get::<Option<String>,_>("ended_at"),"board_version":row.get::<i64,_>("board_version"),"next_summary_end_ms":row.get::<i64,_>("next_summary_end_ms")}),
    ))
}

async fn delete_meeting(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, AppError> {
    ensure_meeting(&s.db, &id).await?;
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

async fn end_meeting(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<Json<Value>, AppError> {
    ensure_meeting(&s.db, &id).await?;
    sqlx::query("UPDATE meetings SET status='ended', ended_at=COALESCE(ended_at,CURRENT_TIMESTAMP) WHERE id=?")
        .bind(&id)
        .execute(&s.db)
        .await?;
    enqueue_summary(&s.db, &id, true).await?;
    Ok(Json(json!({"status":"ended"})))
}

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
                "sequence_no" => sequence_no = value.parse().ok(),
                "start_ms" => start_ms = value.parse().ok(),
                "end_ms" => end_ms = value.parse().ok(),
                "transcript" => transcript = Some(value),
                _ => {}
            }
        }
    }
    let (seq, start, end, data) = (
        sequence_no.ok_or_else(|| AppError::BadRequest("sequence_no is required".into()))?,
        start_ms.ok_or_else(|| AppError::BadRequest("start_ms is required".into()))?,
        end_ms.ok_or_else(|| AppError::BadRequest("end_ms is required".into()))?,
        bytes.ok_or_else(|| AppError::BadRequest("audio is required".into()))?,
    );
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
    if data.is_empty() {
        return Err(AppError::BadRequest("audio must not be empty".into()));
    }
    if data.len() > s.max_upload_bytes {
        return Err(AppError::BadRequest(format!(
            "audio exceeds {} byte upload limit",
            s.max_upload_bytes
        )));
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
    let id = Uuid::new_v4().to_string();
    let dir = s.audio_dir.join(&meeting_id);
    fs::create_dir_all(&dir)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    let safe_filename = sanitize_filename(&filename);
    let safe_filename = if safe_filename.is_empty() {
        "audio.bin".to_string()
    } else {
        safe_filename
    };
    let path = dir.join(format!("{}-{}", id, safe_filename));
    fs::write(&path, &data)
        .await
        .map_err(|e| AppError::BadRequest(e.to_string()))?;
    let db_result = async {
        let mut tx = s.db.begin().await?;
        sqlx::query("INSERT INTO audio_segments(id,meeting_id,speaker_id,sequence_no,start_ms,end_ms,file_path,transcript) VALUES(?,?,?,?,?,?,?,?)").bind(&id).bind(&meeting_id).bind(speaker_id).bind(seq).bind(start).bind(end).bind(path.to_string_lossy().to_string()).bind(transcript).execute(&mut *tx).await?;
        sqlx::query("INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES(?, 'transcribe', ?, ?)")
            .bind(Uuid::new_v4().to_string())
            .bind(&meeting_id)
            .bind(&id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }
    .await;
    if let Err(error) = db_result {
        if let Err(cleanup_error) = fs::remove_file(&path).await {
            error!(%cleanup_error, path = %path.display(), "failed to clean up audio after database error");
        }
        return Err(AppError::Db(error));
    }
    Ok((StatusCode::CREATED, Json(IdResponse { id })))
}

async fn list_segments(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Vec<Value>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let rows=sqlx::query("SELECT id,speaker_id,sequence_no,start_ms,end_ms,status,transcript FROM audio_segments WHERE meeting_id=? ORDER BY start_ms").bind(meeting_id).fetch_all(&s.db).await?;
    Ok(Json(rows.into_iter().map(|r|json!({"id":r.get::<String,_>("id"),"speaker_id":r.get::<Option<String>,_>("speaker_id"),"sequence_no":r.get::<i64,_>("sequence_no"),"start_ms":r.get::<i64,_>("start_ms"),"end_ms":r.get::<i64,_>("end_ms"),"status":r.get::<String,_>("status"),"transcript":r.get::<Option<String>,_>("transcript")})).collect()))
}
async fn list_summaries(
    State(s): State<AppState>,
    Path(meeting_id): Path<String>,
) -> Result<Json<Vec<Value>>, AppError> {
    ensure_meeting(&s.db, &meeting_id).await?;
    let rows=sqlx::query("SELECT id,window_start_ms,window_end_ms,content_json,created_at FROM rolling_summaries WHERE meeting_id=? ORDER BY window_end_ms").bind(meeting_id).fetch_all(&s.db).await?;
    Ok(Json(rows.into_iter().map(|r|json!({"id":r.get::<String,_>("id"),"window_start_ms":r.get::<i64,_>("window_start_ms"),"window_end_ms":r.get::<i64,_>("window_end_ms"),"content":serde_json::from_str::<Value>(&r.get::<String,_>("content_json")).unwrap_or(json!({})),"created_at":r.get::<String,_>("created_at")})).collect()))
}
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
    let row = sqlx::query("SELECT next_summary_end_ms FROM meetings WHERE id=?")
        .bind(meeting_id)
        .fetch_one(db)
        .await?;
    let end = row.get::<i64, _>("next_summary_end_ms");
    let start = end.saturating_sub(300_000);
    let unfinished = sqlx::query(
        "SELECT COUNT(*) value FROM audio_segments
         WHERE meeting_id=? AND status!='completed' AND start_ms < ? AND end_ms > ?",
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
    let mut ticker = interval(Duration::from_secs(3));
    loop {
        ticker.tick().await;
        if let Err(e) = process_jobs(&state).await {
            error!(error=?e,"worker cycle failed");
        }
    }
}
async fn process_jobs(s: &AppState) -> Result<(), AppError> {
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
                }
            }
        }
    }
    Ok(())
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
        "SELECT file_path,transcript,start_ms,end_ms FROM audio_segments WHERE id=? AND meeting_id=?",
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
    let transcript = s
        .transcriber
        .transcribe(&file_path, existing.as_deref())
        .await
        .map_err(AppError::Processing)?;
    sqlx::query(
        "UPDATE audio_segments SET status='completed', transcript=? WHERE id=? AND meeting_id=?",
    )
    .bind(transcript)
    .bind(segment_id)
    .bind(meeting_id)
    .execute(&s.db)
    .await?;
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
    let next_summary_end = last_summary_end + 300_000;
    sqlx::query("UPDATE meetings SET board_version=?,next_summary_end_ms=? WHERE id=?")
        .bind(board_version)
        .bind(next_summary_end)
        .bind(meeting_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
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
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| target.parse().unwrap_or(300000));
    let start = if target.starts_with("final:") {
        sqlx::query(
            "SELECT COALESCE(MAX(window_end_ms),0) value FROM rolling_summaries WHERE meeting_id=?",
        )
        .bind(meeting_id)
        .fetch_one(&s.db)
        .await?
        .get::<i64, _>("value")
    } else {
        end - 300000
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
    sqlx::query("INSERT INTO meeting_board_versions(id,meeting_id,version,source_summary_id,content_json) VALUES(?,?,?,?,?)").bind(Uuid::new_v4().to_string()).bind(meeting_id).bind(version).bind(summary_id).bind(board.to_string()).execute(&mut *tx).await?;
    sqlx::query("UPDATE meetings SET board_version=?,next_summary_end_ms=MAX(next_summary_end_ms,?) WHERE id=?").bind(version).bind(end+300000).bind(meeting_id).execute(&mut *tx).await?;
    tx.commit().await?;
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
    use sqlx::sqlite::SqlitePoolOptions;

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
        let db = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        for statement in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(statement).execute(&db).await.unwrap();
        }
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
        let db = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        for statement in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(statement).execute(&db).await.unwrap();
        }
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
        let db = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        for statement in SCHEMA.split(';').map(str::trim).filter(|s| !s.is_empty()) {
            sqlx::query(statement).execute(&db).await.unwrap();
        }
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
}
