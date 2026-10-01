//! SQLite schema、版本化迁移与各模块共用的查询。
use crate::AppError;
use serde::Serialize;
use sqlx::{Row, SqliteExecutor, SqlitePool};
use tracing::info;
use uuid::Uuid;

/// 新库的完整建表语句。老库由 `migrate` 按版本补齐列和索引。
pub(crate) const SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS meetings (
  id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL,
  started_at TEXT, ended_at TEXT, next_summary_end_ms INTEGER NOT NULL DEFAULT 120000,
  summary_window_ms INTEGER NOT NULL DEFAULT 120000,
  board_version INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  ingest_status TEXT NOT NULL DEFAULT 'idle', ingest_error TEXT,
  transcript_revision INTEGER NOT NULL DEFAULT 0,
  ingest_watermark_ms INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS speakers (
  id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id), name TEXT NOT NULL,
  participant_identity TEXT,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS audio_segments (
  id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL REFERENCES meetings(id), speaker_id TEXT REFERENCES speakers(id),
  sequence_no INTEGER NOT NULL, start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL,
  file_path TEXT NOT NULL, transcript TEXT, status TEXT NOT NULL DEFAULT 'uploaded',
  semantic_type TEXT, semantic_manual_override INTEGER NOT NULL DEFAULT 0,
  revision INTEGER NOT NULL DEFAULT 0, transcript_manual_override INTEGER NOT NULL DEFAULT 0,
  raw_transcript TEXT, corrected_transcript TEXT,
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
  rerun_requested INTEGER NOT NULL DEFAULT 0,
  UNIQUE(job_type, meeting_id, target_id)
);
"#;

/// 当前 schema 版本，记录在 `PRAGMA user_version`。
const SCHEMA_VERSION: i64 = 2;

/// 建表并按 `PRAGMA user_version` 依次执行未应用的迁移；可重复调用。
pub(crate) async fn migrate(db: &SqlitePool) -> Result<(), sqlx::Error> {
    sqlx::query("PRAGMA foreign_keys = ON").execute(db).await?;
    let version: i64 = sqlx::query_scalar("PRAGMA user_version")
        .fetch_one(db)
        .await?;
    if version < 1 {
        // v1：引入版本号之前的所有库（含新库）统一对齐到同一基线。
        migrate_jobs_unique_constraint(db).await?;
        sqlx::raw_sql(SCHEMA).execute(db).await?;
        ensure_columns(
            db,
            &[
                (
                    "meetings",
                    "summary_window_ms",
                    "INTEGER NOT NULL DEFAULT 120000",
                ),
                ("meetings", "ingest_status", "TEXT NOT NULL DEFAULT 'idle'"),
                ("meetings", "ingest_error", "TEXT"),
                (
                    "meetings",
                    "transcript_revision",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                ("audio_segments", "semantic_type", "TEXT"),
                (
                    "audio_segments",
                    "semantic_manual_override",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                ("audio_segments", "revision", "INTEGER NOT NULL DEFAULT 0"),
                (
                    "audio_segments",
                    "transcript_manual_override",
                    "INTEGER NOT NULL DEFAULT 0",
                ),
                ("audio_segments", "raw_transcript", "TEXT"),
                ("audio_segments", "corrected_transcript", "TEXT"),
                ("speakers", "participant_identity", "TEXT"),
                ("jobs", "rerun_requested", "INTEGER NOT NULL DEFAULT 0"),
            ],
        )
        .await?;
        sqlx::raw_sql(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_speaker_identity ON speakers(meeting_id,participant_identity) WHERE participant_identity IS NOT NULL;
             CREATE INDEX IF NOT EXISTS idx_jobs_dispatch ON jobs(status, available_at);
             CREATE INDEX IF NOT EXISTS idx_segments_timeline ON audio_segments(meeting_id, start_ms, end_ms);",
        )
        .execute(db)
        .await?;
    }
    if version < 2 {
        // v2：静音水位线（静音窗口不再入库）+ 按会议/目标查询任务、说话人的索引。
        ensure_columns(
            db,
            &[(
                "meetings",
                "ingest_watermark_ms",
                "INTEGER NOT NULL DEFAULT 0",
            )],
        )
        .await?;
        sqlx::raw_sql(
            "CREATE INDEX IF NOT EXISTS idx_jobs_meeting ON jobs(meeting_id, status);
             CREATE INDEX IF NOT EXISTS idx_jobs_target ON jobs(target_id, job_type, status);
             CREATE INDEX IF NOT EXISTS idx_speakers_meeting ON speakers(meeting_id);",
        )
        .execute(db)
        .await?;
    }
    if version < SCHEMA_VERSION {
        sqlx::query(&format!("PRAGMA user_version = {SCHEMA_VERSION}"))
            .execute(db)
            .await?;
        info!(
            from = version,
            to = SCHEMA_VERSION,
            "database schema migrated"
        );
    }
    Ok(())
}

async fn ensure_columns(
    db: &SqlitePool,
    columns: &[(&str, &str, &str)],
) -> Result<(), sqlx::Error> {
    for (table, column, spec) in columns {
        let existing = sqlx::query(&format!("PRAGMA table_info({table})"))
            .fetch_all(db)
            .await?;
        if !existing
            .iter()
            .any(|r| r.get::<String, _>("name") == *column)
        {
            sqlx::query(&format!("ALTER TABLE {table} ADD COLUMN {column} {spec}"))
                .execute(db)
                .await?;
            info!(table, column, "added missing column");
        }
    }
    Ok(())
}

/// 早期 jobs 表的唯一约束是 (job_type, target_id)，会让不同会议的同名窗口互相冲突。
async fn migrate_jobs_unique_constraint(db: &SqlitePool) -> Result<(), sqlx::Error> {
    let definition: Option<String> =
        sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='table' AND name='jobs'")
            .fetch_optional(db)
            .await?;
    if !definition.is_some_and(|sql| sql.contains("UNIQUE(job_type, target_id)")) {
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

pub(crate) async fn ensure_meeting(db: &SqlitePool, id: &str) -> Result<(), AppError> {
    sqlx::query("SELECT 1 FROM meetings WHERE id=?")
        .bind(id)
        .fetch_optional(db)
        .await?
        .map(|_| ())
        .ok_or(AppError::NotFound)
}

/// 会议是否已结束；会议不存在时返回 false。
pub(crate) async fn meeting_ended(db: &SqlitePool, id: &str) -> Result<bool, sqlx::Error> {
    Ok(
        sqlx::query_scalar("SELECT status='ended' FROM meetings WHERE id=?")
            .bind(id)
            .fetch_optional(db)
            .await?
            .unwrap_or(false),
    )
}

pub(crate) async fn summary_window<'e>(
    db: impl SqliteExecutor<'e>,
    meeting_id: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar("SELECT summary_window_ms FROM meetings WHERE id=?")
        .bind(meeting_id)
        .fetch_one(db)
        .await
}

/// 已生成 Summary 覆盖到的最大窗口结束时间。
pub(crate) async fn summarized_end<'e>(
    db: impl SqliteExecutor<'e>,
    meeting_id: &str,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COALESCE(MAX(window_end_ms),0) FROM rolling_summaries WHERE meeting_id=?",
    )
    .bind(meeting_id)
    .fetch_one(db)
    .await
}

/// 与 [start, end) 重叠的最早 Summary 窗口起点；没有重叠的 Summary 时返回 None。
pub(crate) async fn earliest_summary_overlapping(
    db: &SqlitePool,
    meeting_id: &str,
    start: i64,
    end: i64,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT MIN(window_start_ms) FROM rolling_summaries
         WHERE meeting_id=? AND window_start_ms < ? AND window_end_ms > ?",
    )
    .bind(meeting_id)
    .bind(end)
    .bind(start)
    .fetch_one(db)
    .await
}

/// 写入音频分段并入队转写任务（HTTP 上传与 LiveKit 进房共用）。
#[allow(clippy::too_many_arguments)]
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
        .bind(Uuid::new_v4().to_string())
        .bind(meeting_id)
        .bind(id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await
}

#[derive(sqlx::FromRow)]
pub(crate) struct SegmentRow {
    pub id: String,
    pub meeting_id: String,
    pub sequence_no: i64,
    pub start_ms: i64,
    pub end_ms: i64,
    pub speaker_id: Option<String>,
    pub speaker_name: Option<String>,
    pub participant_identity: Option<String>,
    pub status: String,
    pub transcript: Option<String>,
    pub raw_transcript: Option<String>,
    pub corrected_transcript: Option<String>,
    pub revision: i64,
    pub transcript_manual_override: bool,
    pub semantic_type: Option<String>,
    pub semantic_manual_override: bool,
    pub file_path: String,
}

/// 分段的对外表示（列表接口、SSE 事件共用）。
#[derive(Serialize, utoipa::ToSchema)]
pub(crate) struct SegmentView {
    pub id: String,
    /// 与 id 相同，兼容旧客户端
    pub segment_id: String,
    pub sequence_no: i64,
    pub start_ms: i64,
    pub end_ms: i64,
    pub speaker_id: Option<String>,
    pub speaker_name: Option<String>,
    pub participant_identity: Option<String>,
    /// uploaded、transcribing、completed 或 failed
    pub status: String,
    pub transcript: Option<String>,
    pub raw_transcript: Option<String>,
    pub corrected_transcript: Option<String>,
    pub revision: i64,
    /// 转写文本经过人工修订
    pub is_edited: bool,
    pub semantic_type: Option<String>,
    pub semantic_manual_override: bool,
    pub has_audio: bool,
    pub audio_url: String,
}

impl From<SegmentRow> for SegmentView {
    fn from(r: SegmentRow) -> Self {
        Self {
            audio_url: format!("/api/v1/meetings/{}/segments/{}/audio", r.meeting_id, r.id),
            has_audio: !r.file_path.is_empty(),
            segment_id: r.id.clone(),
            id: r.id,
            sequence_no: r.sequence_no,
            start_ms: r.start_ms,
            end_ms: r.end_ms,
            speaker_id: r.speaker_id,
            speaker_name: r.speaker_name,
            participant_identity: r.participant_identity,
            status: r.status,
            transcript: r.transcript,
            raw_transcript: r.raw_transcript,
            corrected_transcript: r.corrected_transcript,
            revision: r.revision,
            is_edited: r.transcript_manual_override,
            semantic_type: r.semantic_type,
            semantic_manual_override: r.semantic_manual_override,
        }
    }
}

pub(crate) const SEGMENT_SELECT: &str = "SELECT a.*,sp.name speaker_name,sp.participant_identity FROM audio_segments a LEFT JOIN speakers sp ON sp.id=a.speaker_id";

pub(crate) async fn fetch_segment(
    db: &SqlitePool,
    meeting_id: &str,
    id: &str,
) -> Result<SegmentRow, AppError> {
    sqlx::query_as::<_, SegmentRow>(&format!("{SEGMENT_SELECT} WHERE a.meeting_id=? AND a.id=?"))
        .bind(meeting_id)
        .bind(id)
        .fetch_optional(db)
        .await?
        .ok_or(AppError::NotFound)
}

/// 分段的 JSON 表示，用于 SSE 事件与接口响应。
pub(crate) async fn segment_payload(
    db: &SqlitePool,
    meeting_id: &str,
    id: &str,
) -> Result<serde_json::Value, AppError> {
    let view = SegmentView::from(fetch_segment(db, meeting_id, id).await?);
    serde_json::to_value(view).map_err(|e| AppError::Internal(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlx::sqlite::SqlitePoolOptions;

    #[tokio::test]
    async fn migrating_a_pre_versioned_database_adds_columns_and_is_idempotent() {
        let db = SqlitePoolOptions::new()
            .max_connections(1)
            .connect("sqlite::memory:")
            .await
            .unwrap();
        // 最早版本的表结构：没有 realtime/semantic 列，jobs 唯一约束为旧形式。
        sqlx::raw_sql(
            "CREATE TABLE meetings (id TEXT PRIMARY KEY, title TEXT NOT NULL, status TEXT NOT NULL,
               started_at TEXT, ended_at TEXT, next_summary_end_ms INTEGER NOT NULL DEFAULT 120000,
               board_version INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);
             CREATE TABLE audio_segments (id TEXT PRIMARY KEY, meeting_id TEXT NOT NULL, speaker_id TEXT,
               sequence_no INTEGER NOT NULL, start_ms INTEGER NOT NULL, end_ms INTEGER NOT NULL,
               file_path TEXT NOT NULL, transcript TEXT, status TEXT NOT NULL DEFAULT 'uploaded',
               created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, UNIQUE(meeting_id, sequence_no));
             CREATE TABLE jobs (id TEXT PRIMARY KEY, job_type TEXT NOT NULL, meeting_id TEXT NOT NULL,
               target_id TEXT, status TEXT NOT NULL DEFAULT 'pending', retry_count INTEGER NOT NULL DEFAULT 0,
               available_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP, error_message TEXT,
               UNIQUE(job_type, target_id));
             INSERT INTO meetings(id,title,status) VALUES('m','old','ended');
             INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES('j','summary','m','120000');",
        )
        .execute(&db)
        .await
        .unwrap();

        migrate(&db).await.unwrap();
        migrate(&db).await.unwrap();

        let version: i64 = sqlx::query_scalar("PRAGMA user_version")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(version, SCHEMA_VERSION);
        let row: (i64, i64) = sqlx::query_as(
            "SELECT summary_window_ms, ingest_watermark_ms FROM meetings WHERE id='m'",
        )
        .fetch_one(&db)
        .await
        .unwrap();
        assert_eq!(row, (120_000, 0));
        let jobs_sql: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE name='jobs'")
                .fetch_one(&db)
                .await
                .unwrap();
        assert!(jobs_sql.contains("UNIQUE(job_type, meeting_id, target_id)"));
        let rerun: i64 = sqlx::query_scalar("SELECT rerun_requested FROM jobs WHERE id='j'")
            .fetch_one(&db)
            .await
            .unwrap();
        assert_eq!(rerun, 0);
    }
}
