use super::*;
use crate::tests::{test_db, test_state};

async fn seed(db: &SqlitePool) {
    sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
        .execute(db)
        .await
        .unwrap();
    insert_segment(db, "s", "m", None, 0, 0, 1000, "audio.wav", None)
        .await
        .unwrap();
}

struct DelayedTranscriber {
    started: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl Transcriber for DelayedTranscriber {
    async fn transcribe(&self, _: &str, _: Option<&str>) -> Result<String, String> {
        self.started.notify_one();
        self.release.notified().await;
        Ok("late ASR".into())
    }
}
struct DelayedClassifier {
    started: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl Classifier for DelayedClassifier {
    async fn classify(&self, _: &str) -> Result<String, String> {
        self.started.notify_one();
        self.release.notified().await;
        Ok("report".into())
    }
}
fn edit(transcript: Option<&str>, semantic: Option<&str>) -> UpdateSegment {
    UpdateSegment {
        transcript: transcript.map(str::to_owned),
        speaker_name: None,
        semantic_type: semantic.map(str::to_owned),
        custom_tags: None,
    }
}

#[tokio::test]
async fn late_asr_does_not_overwrite_manual_text() {
    let db = test_db().await;
    seed(&db).await;
    let mut s = test_state(&db);
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    s.transcriber = Arc::new(DelayedTranscriber {
        started: started.clone(),
        release: release.clone(),
    });
    let state = s.clone();
    let task = tokio::spawn(async move { process_transcription(&state, "s", "m").await });
    started.notified().await;
    let _ = update_segment(
        State(s.clone()),
        Path(("m".into(), "s".into())),
        Json(edit(Some("人工确认"), None)),
    )
    .await
    .unwrap();
    release.notify_one();
    task.await.unwrap().unwrap();
    let row = segment_payload(&db, "m", "s").await.unwrap();
    assert_eq!(row["transcript"], "人工确认");
    assert_eq!(row["is_edited"], true);
    assert_eq!(row["status"], "completed");
}

#[tokio::test]
async fn late_classification_does_not_overwrite_manual_category() {
    let db = test_db().await;
    seed(&db).await;
    let mut s = test_state(&db);
    sqlx::query("UPDATE audio_segments SET status='completed',transcript='text'")
        .execute(&db)
        .await
        .unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    s.classifier = Arc::new(DelayedClassifier {
        started: started.clone(),
        release: release.clone(),
    });
    let state = s.clone();
    let task = tokio::spawn(async move { process_classification(&state, "s", "m").await });
    started.notified().await;
    let _ = update_segment(
        State(s.clone()),
        Path(("m".into(), "s".into())),
        Json(edit(None, Some("decision"))),
    )
    .await
    .unwrap();
    release.notify_one();
    task.await.unwrap().unwrap();
    let row = segment_payload(&db, "m", "s").await.unwrap();
    assert_eq!(row["semantic_type"], "decision");
    assert_eq!(row["semantic_manual_override"], true);
}

#[tokio::test]
async fn blocked_llm_does_not_block_asr_lane() {
    let db = test_db().await;
    seed(&db).await;
    let mut s = test_state(&db);
    insert_segment(
        &db,
        "other",
        "m",
        None,
        1,
        1000,
        2000,
        "",
        Some("ready".into()),
    )
    .await
    .unwrap();
    sqlx::query("DELETE FROM jobs WHERE target_id='other'")
        .execute(&db)
        .await
        .unwrap();
    enqueue_classification(&db, "m", "other").await.unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    s.classifier = Arc::new(DelayedClassifier {
        started: started.clone(),
        release: release.clone(),
    });
    let state = s.clone();
    let task = tokio::spawn(async move { process_jobs_for_lane(&state, "llm").await });
    started.notified().await;
    tokio::time::timeout(Duration::from_secs(2), process_jobs_for_lane(&s, "asr"))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        segment_payload(&db, "m", "s").await.unwrap()["status"],
        "completed"
    );
    release.notify_one();
    task.await.unwrap().unwrap();
}

#[tokio::test]
async fn manual_edit_rebuilds_existing_summary() {
    let db = test_db().await;
    seed(&db).await;
    let s = test_state(&db);
    sqlx::query("UPDATE audio_segments SET status='completed',transcript='before'")
        .execute(&db)
        .await
        .unwrap();
    process_summary(&s, "m", "final:1000").await.unwrap();
    let _ = update_segment(
        State(s.clone()),
        Path(("m".into(), "s".into())),
        Json(edit(Some("after"), None)),
    )
    .await
    .unwrap();
    let n: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM jobs WHERE job_type='rebuild' AND status='pending'",
    )
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(n, 1);
}

#[tokio::test]
async fn lagged_sse_requests_resync() {
    use tower::ServiceExt;
    let db = test_db().await;
    seed(&db).await;
    let s = test_state(&db);
    let response = build_router(s.clone())
        .oneshot(
            axum::http::Request::builder()
                .uri("/api/v1/meetings/m/events")
                .body(axum::body::Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    for _ in 0..30 {
        publish_event(&s, "m", "segment.updated", json!({}));
    }
    let mut stream = response.into_body().into_data_stream();
    let bytes = tokio::time::timeout(Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&bytes).contains("resync.required"));
}

#[tokio::test]
async fn ended_meeting_reports_pending_work() {
    let db = test_db().await;
    seed(&db).await;
    let s = test_state(&db);
    let _ = end_meeting(State(s.clone()), Path("m".into()))
        .await
        .unwrap();
    let Json(meeting) = get_meeting(State(s), Path("m".into())).await.unwrap();
    assert_eq!(meeting["status"], "ended");
    assert_eq!(meeting["processing_complete"], false);
}

#[tokio::test]
async fn segment_cursor_is_stable_across_overlapping_tracks() {
    let db = test_db().await;
    seed(&db).await;
    insert_segment(&db, "s2", "m", None, 1, 0, 900, "", Some("two".into()))
        .await
        .unwrap();
    let Json(rows) = list_segments(
        State(test_state(&db)),
        Path("m".into()),
        Query(SegmentPage {
            after_sequence: Some(0),
            limit: Some(1),
        }),
    )
    .await
    .unwrap();
    assert_eq!(rows.as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["id"], "s2");
}

struct DelayedSummarizer {
    started: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl Summarizer for DelayedSummarizer {
    async fn summarize(&self, _: i64, _: i64, _: &str) -> Result<SummaryDocument, String> {
        self.started.notify_one();
        self.release.notified().await;
        Ok(SummaryDocument {
            key_points: vec!["summary".into()],
            ..Default::default()
        })
    }
}

#[tokio::test]
async fn edit_during_summary_discards_stale_model_output() {
    let db = test_db().await;
    seed(&db).await;
    let mut s = test_state(&db);
    sqlx::query("UPDATE audio_segments SET status='completed',transcript='before'")
        .execute(&db)
        .await
        .unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    s.summarizer = Arc::new(DelayedSummarizer {
        started: started.clone(),
        release: release.clone(),
    });
    let state = s.clone();
    let task = tokio::spawn(async move { process_summary(&state, "m", "final:1000").await });
    started.notified().await;
    let _ = update_segment(
        State(s.clone()),
        Path(("m".into(), "s".into())),
        Json(edit(Some("after"), None)),
    )
    .await
    .unwrap();
    release.notify_one();
    assert!(matches!(task.await.unwrap(), Err(AppError::Deferred(_))));
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rolling_summaries")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(count, 0);
}

#[tokio::test]
async fn later_audio_does_not_starve_an_earlier_summary() {
    let db = test_db().await;
    seed(&db).await;
    let mut s = test_state(&db);
    sqlx::query("UPDATE audio_segments SET status='completed',transcript='before'")
        .execute(&db)
        .await
        .unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    s.summarizer = Arc::new(DelayedSummarizer {
        started: started.clone(),
        release: release.clone(),
    });
    let state = s.clone();
    let task = tokio::spawn(async move { process_summary(&state, "m", "final:1000").await });
    started.notified().await;
    insert_segment(
        &db,
        "later",
        "m",
        None,
        2,
        2000,
        3000,
        "",
        Some("later text".into()),
    )
    .await
    .unwrap();
    process_transcription(&s, "later", "m").await.unwrap();
    release.notify_one();
    task.await.unwrap().unwrap();
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM rolling_summaries")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(count, 1);
}
