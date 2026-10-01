//! SQLite 任务队列：按 lane 分组的 worker、入队与各类任务处理。
use crate::{
    db::{
        earliest_summary_overlapping, fetch_segment, meeting_ended, segment_payload,
        summarized_end, summary_window,
    },
    publish_event,
    summary::{empty_board, merge_board, normalize_summary, SummaryDocument},
    AppError, AppState,
};
use serde_json::{json, Value};
use sqlx::{Row, SqlitePool};
use std::sync::Arc;
use tokio::{
    sync::Notify,
    time::{sleep, Duration},
};
use tracing::{error, info, warn};
use uuid::Uuid;

/// worker lane 及默认并发数：ASR、LLM（分类/校对）、Summary 互不阻塞。
pub(crate) const LANES: [(&str, usize); 3] = [("asr", 2), ("llm", 2), ("summary", 1)];

pub(crate) fn lane_of(job_type: &str) -> &'static str {
    match job_type {
        "transcribe" => "asr",
        "classify" | "correct" => "llm",
        _ => "summary",
    }
}

/// 每个 lane 一个 Notify。唤醒时既通知正在等待的 worker，也留下一个 permit，
/// 避免 worker 正在处理（尚未进入等待）时的通知丢失。
#[derive(Default)]
pub(crate) struct WorkerSignals {
    asr: Notify,
    llm: Notify,
    summary: Notify,
}

impl WorkerSignals {
    fn select(&self, lane: &str) -> Vec<&Notify> {
        match lane {
            "asr" => vec![&self.asr],
            "llm" => vec![&self.llm],
            "summary" => vec![&self.summary],
            _ => vec![&self.asr, &self.llm, &self.summary],
        }
    }

    pub fn wake(&self, lane: &str) {
        for notify in self.select(lane) {
            notify.notify_waiters();
            notify.notify_one();
        }
    }

    async fn notified(&self, lane: &str) {
        match lane {
            "asr" => self.asr.notified().await,
            "llm" => self.llm.notified().await,
            _ => self.summary.notified().await,
        }
    }
}

pub(crate) async fn worker(state: AppState, lane: &'static str) {
    loop {
        match process_jobs_for_lane(&state, lane).await {
            // 本轮领到了任务：立即进入下一轮，避免链式任务（转写→摘要）等待 tick
            Ok(claimed) if claimed > 0 => continue,
            Ok(_) => {}
            Err(e) => error!(error=?e, lane, "worker cycle failed"),
        }
        tokio::select! {
            _ = state.job_notify.notified(lane) => {}
            // 兜底 tick：处理带退避的重试/延后任务
            _ = sleep(Duration::from_secs(3)) => {}
        }
    }
}

/// 定期清理已结束会议中早于保留期的已完成任务，避免 jobs 表无限增长。
pub(crate) async fn purge_finished_jobs(
    db: &SqlitePool,
    retention_hours: i64,
) -> Result<u64, sqlx::Error> {
    Ok(sqlx::query(
        "DELETE FROM jobs WHERE status='completed' AND available_at < datetime('now', ?)
         AND meeting_id IN (SELECT id FROM meetings WHERE status='ended')",
    )
    .bind(format!("-{retention_hours} hours"))
    .execute(db)
    .await?
    .rows_affected())
}

#[cfg(test)]
pub(crate) async fn process_jobs(s: &AppState) -> Result<usize, AppError> {
    process_jobs_for_lane(s, "all").await
}

pub(crate) async fn process_jobs_for_lane(s: &AppState, lane: &str) -> Result<usize, AppError> {
    let mut claimed_count = 0;
    let jobs = sqlx::query(
        "SELECT id,job_type,meeting_id,target_id FROM jobs
         WHERE status='pending' AND available_at <= CURRENT_TIMESTAMP
           AND (?='all' OR (?='asr' AND job_type='transcribe')
             OR (?='llm' AND job_type IN ('classify','correct'))
             OR (?='summary' AND job_type IN ('summary','rebuild')))
         ORDER BY available_at, rowid LIMIT 10",
    )
    .bind(lane)
    .bind(lane)
    .bind(lane)
    .bind(lane)
    .fetch_all(&s.db)
    .await?;
    for j in jobs {
        let id = j.get::<String, _>("id");
        let typ = j.get::<String, _>("job_type");
        let meeting = j.get::<String, _>("meeting_id");
        let target = j.get::<Option<String>, _>("target_id").unwrap_or_default();
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
            "transcribe" => process_transcription(s, &target, &meeting).await,
            "classify" => process_classification(s, &target, &meeting).await,
            "correct" => process_correction(s, &target, &meeting).await,
            "summary" | "rebuild" => {
                let lock = s.summary_locks.get(&meeting);
                let result = {
                    let _guard = lock.lock().await;
                    if typ == "summary" {
                        process_summary(s, &meeting, &target).await
                    } else {
                        process_rebuild(s, &meeting, &target).await
                    }
                };
                s.summary_locks.release(&meeting, lock);
                result
            }
            _ => Err(AppError::Processing(format!("unknown job type: {typ}"))),
        };
        match result {
            Ok(_) => {
                let rerun: bool = sqlx::query_scalar(
                    "UPDATE jobs SET status=CASE WHEN rerun_requested=1 THEN 'pending' ELSE 'completed' END,
                     rerun_requested=0 WHERE id=? RETURNING status='pending'",
                )
                .bind(&id)
                .fetch_optional(&s.db)
                .await?
                .unwrap_or(false);
                if rerun {
                    s.job_notify.wake(lane_of(&typ));
                }
            }
            Err(AppError::Deferred(reason)) => {
                sqlx::query("UPDATE jobs SET status='pending',available_at=datetime('now','+1 seconds'),error_message=? WHERE id=?")
                    .bind(reason).bind(&id).execute(&s.db).await?;
            }
            Err(e) => {
                warn!(
                    job_id = id,
                    job_type = %typ,
                    meeting_id = %meeting,
                    error = %e,
                    "job processing failed, scheduling retry"
                );
                let permanently_failed: Option<bool> = sqlx::query_scalar(
                    "UPDATE jobs SET status=CASE WHEN retry_count < 2 THEN 'pending' ELSE 'failed' END,
                     retry_count=retry_count+1, available_at=datetime('now','+5 seconds'), error_message=?
                     WHERE id=? RETURNING status='failed'",
                )
                .bind(e.to_string())
                .bind(&id)
                .fetch_optional(&s.db)
                .await?;
                if typ == "transcribe" {
                    if let Some(permanently_failed) = permanently_failed {
                        on_transcription_failure(s, &meeting, &target, permanently_failed).await?;
                    }
                }
            }
        }
        if typ == "correct" {
            // Failed correction leaves raw ASR text intact, and must not block summaries.
            let terminal: bool =
                sqlx::query_scalar("SELECT status IN ('completed','failed') FROM jobs WHERE id=?")
                    .bind(&id)
                    .fetch_optional(&s.db)
                    .await?
                    .unwrap_or(false);
            if terminal {
                enqueue_classification(&s.db, &meeting, &target).await?;
                let ended = meeting_ended(&s.db, &meeting).await?;
                enqueue_summary(&s.db, &meeting, ended).await?;
                s.job_notify.wake("llm");
                s.job_notify.wake("summary");
            }
        }
    }
    Ok(claimed_count)
}

async fn on_transcription_failure(
    s: &AppState,
    meeting: &str,
    segment: &str,
    permanently_failed: bool,
) -> Result<(), AppError> {
    sqlx::query("UPDATE audio_segments SET status=? WHERE id=? AND transcript_manual_override=0")
        .bind(if permanently_failed {
            "failed"
        } else {
            "transcribing"
        })
        .bind(segment)
        .execute(&s.db)
        .await?;
    if !permanently_failed {
        return Ok(());
    }
    let sequence_no: Option<i64> =
        sqlx::query_scalar("SELECT sequence_no FROM audio_segments WHERE id=?")
            .bind(segment)
            .fetch_optional(&s.db)
            .await?;
    publish_event(
        s,
        meeting,
        "segment.failed",
        json!({"segment_id": segment, "sequence_no": sequence_no}),
    );
    // 失败分段不再阻塞摘要：为已完成的部分继续排入后续 Summary。
    let ended = meeting_ended(&s.db, meeting).await?;
    if let Err(error) = enqueue_summary(&s.db, meeting, ended).await {
        error!(%error, meeting_id = %meeting, "failed to enqueue summary after transcription failure");
    }
    s.job_notify.wake("summary");
    Ok(())
}

pub(crate) async fn enqueue_rebuild(
    db: &SqlitePool,
    meeting_id: &str,
    window_start: i64,
) -> Result<(), AppError> {
    sqlx::query(
        "INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES(?, 'rebuild', ?, ?)
         ON CONFLICT(job_type,meeting_id,target_id) DO UPDATE SET
           status=CASE WHEN jobs.status='running' THEN 'running' ELSE 'pending' END,
           rerun_requested=CASE WHEN jobs.status='running' THEN 1 ELSE 0 END,
           retry_count=0,available_at=CURRENT_TIMESTAMP,error_message=NULL",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(meeting_id)
    .bind(window_start.to_string())
    .execute(db)
    .await?;
    Ok(())
}

pub(crate) async fn enqueue_classification(
    db: &SqlitePool,
    meeting: &str,
    segment: &str,
) -> Result<(), AppError> {
    sqlx::query(
        "INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES(?,'classify',?,?)
         ON CONFLICT(job_type,meeting_id,target_id) DO UPDATE SET
           status=CASE WHEN jobs.status='running' THEN 'running' ELSE 'pending' END,
           rerun_requested=CASE WHEN jobs.status='running' THEN 1 ELSE 0 END,
           retry_count=0,available_at=CURRENT_TIMESTAMP",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(meeting)
    .bind(segment)
    .execute(db)
    .await?;
    Ok(())
}

/// 窗口 [start, end) 内仍在转写或校对中的分段数量。
async fn unfinished_segments(
    db: &SqlitePool,
    meeting_id: &str,
    start: i64,
    end: i64,
) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "SELECT COUNT(*) FROM audio_segments a
         WHERE a.meeting_id=? AND a.start_ms < ? AND a.end_ms > ?
           AND (a.status NOT IN ('completed','failed') OR EXISTS (
             SELECT 1 FROM jobs j WHERE j.target_id=a.id AND j.job_type='correct'
               AND j.status IN ('pending','running') AND j.meeting_id=a.meeting_id))",
    )
    .bind(meeting_id)
    .bind(end)
    .bind(start)
    .fetch_one(db)
    .await
}

pub(crate) async fn enqueue_summary(
    db: &SqlitePool,
    meeting_id: &str,
    final_window: bool,
) -> Result<(), AppError> {
    let Some(row) = sqlx::query(
        "SELECT next_summary_end_ms, summary_window_ms, ingest_watermark_ms FROM meetings WHERE id=?",
    )
    .bind(meeting_id)
    .fetch_optional(db)
    .await?
    else {
        return Ok(());
    };
    let end = row.get::<i64, _>("next_summary_end_ms");
    let window = row.get::<i64, _>("summary_window_ms");
    let start = end.saturating_sub(window);
    if unfinished_segments(db, meeting_id, start, end).await? > 0 {
        return Ok(());
    }
    // 静音窗口不入库，由 ingest_watermark_ms 记录采集已推进到的时间。
    let completed_end: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(end_ms),0) FROM audio_segments WHERE meeting_id=? AND status='completed'",
    )
    .bind(meeting_id)
    .fetch_one(db)
    .await?;
    let max_end = completed_end.max(row.get::<i64, _>("ingest_watermark_ms"));
    let target = if max_end >= end {
        end.to_string()
    } else if final_window && max_end > 0 && max_end > summarized_end(db, meeting_id).await? {
        format!("final:{max_end}")
    } else {
        return Ok(());
    };
    sqlx::query(
        "INSERT OR IGNORE INTO jobs(id,job_type,meeting_id,target_id) VALUES(?, 'summary', ?, ?)",
    )
    .bind(Uuid::new_v4().to_string())
    .bind(meeting_id)
    .bind(target)
    .execute(db)
    .await?;
    Ok(())
}

async fn process_correction(
    s: &AppState,
    segment_id: &str,
    meeting_id: &str,
) -> Result<(), AppError> {
    // 校对开关在入队后被关闭（例如重启后）时直接跳过，保留原始转写。
    let Some(corrector) = s.corrector.clone() else {
        return Ok(());
    };
    let row = fetch_segment(&s.db, meeting_id, segment_id).await?;
    if row.transcript_manual_override {
        return Ok(());
    }
    let text = row.transcript.clone().unwrap_or_default();
    let corrected = corrector
        .correct(&text)
        .await
        .map_err(AppError::Processing)?;
    let mut tx = s.db.begin().await?;
    let changed = sqlx::query("UPDATE audio_segments SET transcript=?,corrected_transcript=?,revision=revision+1 WHERE id=? AND revision=? AND transcript_manual_override=0")
        .bind(&corrected).bind(&corrected).bind(segment_id).bind(row.revision)
        .execute(&mut *tx).await?.rows_affected();
    if changed == 0 {
        tx.rollback().await?;
        return Ok(());
    }
    sqlx::query("UPDATE meetings SET transcript_revision=transcript_revision+1 WHERE id=?")
        .bind(meeting_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    publish_event(
        s,
        meeting_id,
        "segment.updated",
        segment_payload(&s.db, meeting_id, segment_id).await?,
    );
    enqueue_classification(&s.db, meeting_id, segment_id).await?;
    if let Some(start) =
        earliest_summary_overlapping(&s.db, meeting_id, row.start_ms, row.end_ms).await?
    {
        enqueue_rebuild(&s.db, meeting_id, start).await?;
    }
    s.job_notify.wake("llm");
    s.job_notify.wake("summary");
    Ok(())
}

/// LLM 判定分段语义类型（decision/report/question/action/other），完成后广播 segment.updated。
/// 人工指定过（semantic_manual_override=1）或文本为空的分段直接跳过。
pub(crate) async fn process_classification(
    s: &AppState,
    segment_id: &str,
    meeting_id: &str,
) -> Result<(), AppError> {
    let row = fetch_segment(&s.db, meeting_id, segment_id).await?;
    if row.semantic_manual_override {
        return Ok(());
    }
    let transcript = row.transcript.unwrap_or_default();
    if transcript.trim().is_empty() {
        return Ok(());
    }
    let semantic_type = s
        .classifier
        .classify(&transcript)
        .await
        .map_err(AppError::Processing)?;
    let changed = sqlx::query("UPDATE audio_segments SET semantic_type=?,revision=revision+1 WHERE id=? AND meeting_id=? AND revision=? AND semantic_manual_override=0")
        .bind(&semantic_type).bind(segment_id).bind(meeting_id)
        .bind(row.revision).execute(&s.db).await?.rows_affected();
    if changed > 0 {
        publish_event(
            s,
            meeting_id,
            "segment.updated",
            segment_payload(&s.db, meeting_id, segment_id).await?,
        );
    }
    Ok(())
}

pub(crate) async fn process_transcription(
    s: &AppState,
    segment_id: &str,
    meeting_id: &str,
) -> Result<(), AppError> {
    sqlx::query("UPDATE audio_segments SET status='transcribing' WHERE id=? AND meeting_id=? AND transcript_manual_override=0")
        .bind(segment_id)
        .bind(meeting_id)
        .execute(&s.db)
        .await?;
    let row = fetch_segment(&s.db, meeting_id, segment_id).await?;
    let existing = row.transcript.as_deref();
    let transcript = s
        .transcriber
        .transcribe(&row.file_path, existing)
        .await
        .map_err(|e| {
            warn!(
                meeting_id = %meeting_id,
                segment_id = %segment_id,
                sequence_no = row.sequence_no,
                error = %e,
                "ASR provider call failed"
            );
            AppError::Processing(e)
        })?;
    info!(
        meeting_id = %meeting_id,
        segment_id = %segment_id,
        sequence_no = row.sequence_no,
        chars = transcript.chars().count(),
        reused_existing = existing.map(str::trim).is_some_and(|t| !t.is_empty()),
        "segment transcribed"
    );
    let mut tx = s.db.begin().await?;
    let changed = sqlx::query("UPDATE audio_segments SET status='completed',transcript=?,raw_transcript=COALESCE(raw_transcript,?),revision=revision+1 WHERE id=? AND meeting_id=? AND revision=? AND transcript_manual_override=0")
        .bind(&transcript).bind(&transcript).bind(segment_id).bind(meeting_id)
        .bind(row.revision).execute(&mut *tx).await?.rows_affected();
    if changed == 0 {
        tx.rollback().await?;
        return Ok(());
    }
    sqlx::query("UPDATE meetings SET transcript_revision=transcript_revision+1 WHERE id=?")
        .bind(meeting_id)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    publish_event(
        s,
        meeting_id,
        "segment.transcribed",
        segment_payload(&s.db, meeting_id, segment_id).await?,
    );
    if s.corrector.is_some() && !transcript.trim().is_empty() {
        sqlx::query(
            "INSERT OR IGNORE INTO jobs(id,job_type,meeting_id,target_id) VALUES(?,'correct',?,?)",
        )
        .bind(Uuid::new_v4().to_string())
        .bind(meeting_id)
        .bind(segment_id)
        .execute(&s.db)
        .await?;
    } else {
        enqueue_classification(&s.db, meeting_id, segment_id).await?;
    }
    s.job_notify.wake("llm");
    schedule_summaries_after_change(s, meeting_id, row.start_ms, row.end_ms).await
}

/// 分段内容变化后：已有 Summary 覆盖该时间段则从最早受影响窗口重建，否则尝试推进下一个窗口。
pub(crate) async fn schedule_summaries_after_change(
    s: &AppState,
    meeting_id: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<(), AppError> {
    match earliest_summary_overlapping(&s.db, meeting_id, start_ms, end_ms).await? {
        Some(window_start) => enqueue_rebuild(&s.db, meeting_id, window_start).await?,
        None => {
            let ended = meeting_ended(&s.db, meeting_id).await?;
            enqueue_summary(&s.db, meeting_id, ended).await?;
        }
    }
    s.job_notify.wake("summary");
    Ok(())
}

pub(crate) async fn process_rebuild(
    s: &AppState,
    meeting_id: &str,
    target: &str,
) -> Result<(), AppError> {
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
    let next_summary_end =
        summarized_end(&mut *tx, meeting_id).await? + summary_window(&mut *tx, meeting_id).await?;
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
    let ended = meeting_ended(&s.db, meeting_id).await?;
    enqueue_summary(&s.db, meeting_id, ended).await
}

type SourceVersions = Vec<(String, i64, String)>;

async fn summary_sources(
    conn: &mut sqlx::SqliteConnection,
    meeting_id: &str,
    start: i64,
    end: i64,
) -> Result<(SourceVersions, String), sqlx::Error> {
    let rows = sqlx::query("SELECT a.id,a.revision,COALESCE(s.name,'Unknown') speaker_name,transcript FROM audio_segments a LEFT JOIN speakers s ON s.id=a.speaker_id WHERE a.meeting_id=? AND a.status='completed' AND a.start_ms < ? AND a.end_ms > ? ORDER BY a.start_ms,a.id")
        .bind(meeting_id).bind(end).bind(start).fetch_all(conn).await?;
    let versions = rows
        .iter()
        .map(|r| (r.get("id"), r.get("revision"), r.get("speaker_name")))
        .collect();
    let transcript = rows
        .iter()
        .filter_map(|r| {
            let text = r.get::<Option<String>, _>("transcript")?;
            let text = text.trim();
            (!text.is_empty()).then(|| format!("{}: {}", r.get::<String, _>("speaker_name"), text))
        })
        .collect::<Vec<_>>()
        .join("\n");
    Ok((versions, transcript))
}

pub(crate) async fn process_summary(
    s: &AppState,
    meeting_id: &str,
    target: &str,
) -> Result<(), AppError> {
    let is_final = target.starts_with("final:");
    let end = target
        .strip_prefix("final:")
        .unwrap_or(target)
        .parse::<i64>()
        .map_err(|_| AppError::Processing(format!("invalid summary target: {target}")))?;
    let window = summary_window(&s.db, meeting_id).await?;
    let summarized = summarized_end(&s.db, meeting_id).await?;
    if end <= summarized {
        return Ok(());
    }
    if !is_final {
        let expected: i64 =
            sqlx::query_scalar("SELECT next_summary_end_ms FROM meetings WHERE id=?")
                .bind(meeting_id)
                .fetch_one(&s.db)
                .await?;
        if end > expected {
            return Err(AppError::Deferred(
                "earlier summary window is still pending".into(),
            ));
        }
    }
    let start = if is_final { summarized } else { end - window };
    if unfinished_segments(&s.db, meeting_id, start, end).await? > 0 {
        return Err(AppError::Deferred("waiting for source transcripts".into()));
    }
    let (source_versions, transcript) = {
        let mut conn = s.db.acquire().await?;
        summary_sources(&mut conn, meeting_id, start, end).await?
    };
    let document = if transcript.is_empty() {
        SummaryDocument::default()
    } else {
        s.summarizer
            .summarize(start, end, &transcript)
            .await
            .map_err(AppError::Processing)?
    };
    let document = normalize_summary(document);
    let content =
        serde_json::to_value(&document).map_err(|e| AppError::Processing(e.to_string()))?;
    let summary_id = Uuid::new_v4().to_string();
    let mut tx = s.db.begin().await?;
    let (current_versions, _) = summary_sources(&mut tx, meeting_id, start, end).await?;
    if current_versions != source_versions {
        return Err(AppError::Deferred(
            "summary source changed; retrying".into(),
        ));
    }
    let inserted = sqlx::query("INSERT OR IGNORE INTO rolling_summaries(id,meeting_id,window_start_ms,window_end_ms,content_json) VALUES(?,?,?,?,?)")
        .bind(&summary_id).bind(meeting_id).bind(start).bind(end).bind(content.to_string())
        .execute(&mut *tx).await?;
    if inserted.rows_affected() == 0 {
        tx.commit().await?;
        return Ok(());
    }
    let version: i64 = sqlx::query_scalar(
        "SELECT COALESCE(MAX(version),0)+1 FROM meeting_board_versions WHERE meeting_id=?",
    )
    .bind(meeting_id)
    .fetch_one(&mut *tx)
    .await?;
    let mut board: Value = sqlx::query_scalar::<_, String>(
        "SELECT content_json FROM meeting_boards WHERE meeting_id=?",
    )
    .bind(meeting_id)
    .fetch_optional(&mut *tx)
    .await?
    .and_then(|json| serde_json::from_str(&json).ok())
    .unwrap_or_else(empty_board);
    merge_board(&mut board, &document);
    let board_json = board.to_string();
    sqlx::query("INSERT INTO meeting_boards(meeting_id,version,content_json) VALUES(?,?,?) ON CONFLICT(meeting_id) DO UPDATE SET version=excluded.version,content_json=excluded.content_json,updated_at=CURRENT_TIMESTAMP")
        .bind(meeting_id).bind(version).bind(&board_json).execute(&mut *tx).await?;
    sqlx::query("INSERT INTO meeting_board_versions(id,meeting_id,version,source_summary_id,content_json) VALUES(?,?,?,?,?)")
        .bind(Uuid::new_v4().to_string()).bind(meeting_id).bind(version).bind(&summary_id).bind(&board_json)
        .execute(&mut *tx).await?;
    sqlx::query("UPDATE meetings SET board_version=?,next_summary_end_ms=MAX(next_summary_end_ms,?) WHERE id=?")
        .bind(version).bind(end + window).bind(meeting_id).execute(&mut *tx).await?;
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
    let ended = meeting_ended(&s.db, meeting_id).await?;
    enqueue_summary(&s.db, meeting_id, ended).await
}

/// 每个会议一把 Summary/rebuild 锁，保证同一会议的窗口串行生成。
#[derive(Default)]
pub(crate) struct SummaryLocks(
    std::sync::Mutex<std::collections::HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
);

impl SummaryLocks {
    pub fn get(&self, meeting_id: &str) -> Arc<tokio::sync::Mutex<()>> {
        self.0
            .lock()
            .unwrap()
            .entry(meeting_id.into())
            .or_default()
            .clone()
    }

    /// 没有其他任务持有或等待时回收该会议的锁，避免 map 随会议数无限增长。
    pub fn release(&self, meeting_id: &str, lock: Arc<tokio::sync::Mutex<()>>) {
        let mut locks = self.0.lock().unwrap();
        // map 自身 + 调用方各持有一个引用
        if Arc::strong_count(&lock) == 2 {
            locks.remove(meeting_id);
        }
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0.lock().unwrap().len()
    }
}
