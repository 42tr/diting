use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde_json::{json, Value};
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
    SqlitePool,
};
use std::{net::SocketAddr, path::PathBuf, str::FromStr, sync::Arc};
use tokio::{fs, time::Duration};
use tracing::{error, info, warn};

mod api;
mod db;
mod events;
mod jobs;
mod livekit_ingest;
mod providers;
mod realtime_asr;
#[cfg(test)]
mod reliability_tests;
mod summary;
#[cfg(test)]
mod tests;

#[cfg(test)]
use api::*;
#[cfg(test)]
use db::*;
#[cfg(test)]
use jobs::*;
#[cfg(test)]
use providers::*;
#[cfg(test)]
use summary::*;

use events::EventHub;
use jobs::{SummaryLocks, WorkerSignals};
use livekit_ingest::IngestStopMap;
use providers::{env_nonempty, Classifier, Corrector, Providers, Summarizer, Transcriber};

#[derive(Clone)]
struct AppState {
    db: SqlitePool,
    audio_dir: Arc<PathBuf>,
    transcriber: Arc<dyn Transcriber>,
    summarizer: Arc<dyn Summarizer>,
    classifier: Arc<dyn Classifier>,
    /// 转写校对（DITING_CORRECTION_ENABLED），未启用时为 None
    corrector: Option<Arc<dyn Corrector>>,
    max_upload_bytes: usize,
    /// DITING_API_TOKEN：设置后 /api/ 下的接口需要鉴权
    api_token: Option<Arc<str>>,
    job_notify: Arc<WorkerSignals>,
    events: EventHub,
    /// meeting_id -> 停止信号，结束/删除会议时通知 LiveKit 进房任务退出
    ingest_stop: IngestStopMap,
    summary_locks: Arc<SummaryLocks>,
}

/// 向该会议的 SSE 订阅者广播事件；没有订阅者时直接丢弃。
fn publish_event(s: &AppState, meeting_id: &str, kind: &'static str, data: Value) {
    s.events.publish(meeting_id, kind, data);
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
    #[error("processing deferred: {0}")]
    Deferred(String),
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
            Self::Deferred(m) => (StatusCode::SERVICE_UNAVAILABLE, m),
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

fn env_parse<T: FromStr>(key: &str) -> Option<T> {
    env_nonempty(key).and_then(|value| value.trim().parse().ok())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    fs::create_dir_all("data/audio").await?;
    let options = match env_nonempty("DITING_DATABASE_URL") {
        Some(url) => SqliteConnectOptions::from_str(&url)?,
        None => SqliteConnectOptions::new().filename("data/meeting.db"),
    }
    .create_if_missing(true)
    .journal_mode(SqliteJournalMode::Wal)
    .foreign_keys(true)
    .busy_timeout(Duration::from_secs(5));
    let db = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    db::migrate(&db).await?;
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
    let providers = Providers::from_env(http_client);
    let api_token = env_nonempty("DITING_API_TOKEN").map(|t| Arc::<str>::from(t.trim()));
    let addr: SocketAddr = env_nonempty("DITING_ADDR")
        .unwrap_or_else(|| "127.0.0.1:3000".into())
        .parse()?;
    if api_token.is_none() && !addr.ip().is_loopback() {
        warn!(%addr, "listening on a non-loopback address without DITING_API_TOKEN: the API is unauthenticated");
    }
    let state = AppState {
        db,
        audio_dir: Arc::new(PathBuf::from("data/audio")),
        transcriber: providers.transcriber,
        summarizer: providers.summarizer,
        classifier: providers.classifier,
        corrector: providers.corrector,
        max_upload_bytes: env_parse::<usize>("DITING_MAX_UPLOAD_BYTES")
            .filter(|value| *value > 0)
            .unwrap_or(100 * 1024 * 1024),
        api_token,
        job_notify: Default::default(),
        events: EventHub::new(512),
        ingest_stop: IngestStopMap::default(),
        summary_locks: Default::default(),
    };
    for (lane, default) in jobs::LANES {
        let count = env_parse::<usize>(&format!("DITING_{}_WORKERS", lane.to_uppercase()))
            .unwrap_or(default)
            .clamp(1, 16);
        for _ in 0..count {
            tokio::spawn(jobs::worker(state.clone(), lane));
        }
    }
    let retention_hours = env_parse::<i64>("DITING_JOB_RETENTION_HOURS")
        .filter(|h| *h > 0)
        .unwrap_or(24 * 7);
    let db = state.db.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(3600));
        loop {
            tick.tick().await;
            match jobs::purge_finished_jobs(&db, retention_hours).await {
                Ok(0) => {}
                Ok(purged) => info!(purged, "purged finished jobs"),
                Err(error) => warn!(%error, "failed to purge finished jobs"),
            }
        }
    });
    let app = api::build_router(state);
    info!(%addr, "meeting service started");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
