use super::*;
use async_trait::async_trait;
use axum::{
    body::Body,
    extract::{Path, State},
    http::Request,
    routing::post,
    Router,
};
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::Row;
use tower::ServiceExt;
use uuid::Uuid;

/// 固定返回的转写桩：已有转写时透传，否则返回固定文本。
struct FixedTranscriber(String);

#[async_trait]
impl Transcriber for FixedTranscriber {
    async fn transcribe(&self, _file_path: &str, existing: Option<&str>) -> Result<String, String> {
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

/// 固定返回的分类桩。
struct FixedClassifier(&'static str);

#[async_trait]
impl Classifier for FixedClassifier {
    async fn classify(&self, _transcript: &str) -> Result<String, String> {
        Ok(self.0.to_string())
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
    db::migrate(&db).await.unwrap();
    db
}

pub(crate) fn test_state(db: &SqlitePool) -> AppState {
    AppState {
        db: db.clone(),
        // 上传会先流式写盘再校验，测试不能写进仓库的 data/audio。
        audio_dir: Arc::new(std::env::temp_dir().join(format!("diting-test-{}", Uuid::new_v4()))),
        transcriber: Arc::new(FixedTranscriber("固定转写文本".into())),
        summarizer: Arc::new(FixedSummarizer(fixed_document())),
        classifier: Arc::new(LocalClassifier),
        corrector: None,
        max_upload_bytes: 64 * 1024,
        api_token: None,
        job_notify: Default::default(),
        events: EventHub::new(16),
        ingest_stop: IngestStopMap::default(),
        summary_locks: Default::default(),
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
    sqlx::query("INSERT INTO meetings(id,title,status,next_summary_end_ms,summary_window_ms,board_version) VALUES('m','test','running',600000,300000,1)").execute(&db).await.unwrap();
    sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,transcript,status) VALUES('a','m',1,0,300000,'audio.wav','text','completed')").execute(&db).await.unwrap();
    sqlx::query("INSERT INTO rolling_summaries(id,meeting_id,window_start_ms,window_end_ms,content_json) VALUES('s','m',0,300000,'{}')").execute(&db).await.unwrap();
    sqlx::query("INSERT INTO meeting_boards(meeting_id,version,content_json) VALUES('m',1,'{}')")
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
    let count =
        sqlx::query("SELECT COUNT(*) value FROM jobs WHERE meeting_id='m' AND job_type='rebuild'")
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
    sqlx::query("INSERT INTO meeting_boards(meeting_id,version,content_json) VALUES('m',1,'{}')")
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
    let summarizer = OpenAiSummarizer(ChatClient {
        client: reqwest::Client::new(),
        base_url,
        api_key: "test-key".into(),
        model: "test-model".into(),
        timeout: Duration::from_secs(5),
    });
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
    let summarizer = OpenAiSummarizer(ChatClient {
        client: reqwest::Client::new(),
        base_url,
        api_key: "test-key".into(),
        model: "test-model".into(),
        timeout: Duration::from_secs(5),
    });
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
    let summarizer = OpenAiSummarizer(ChatClient {
        client: reqwest::Client::new(),
        base_url,
        api_key: "test-key".into(),
        model: "test-model".into(),
        timeout: Duration::from_secs(5),
    });
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
    sqlx::query("INSERT INTO meetings(id,title,status,next_summary_end_ms,summary_window_ms) VALUES('m','test','running',300000,300000)")
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
    sqlx::query("INSERT INTO meetings(id,title,status,next_summary_end_ms,summary_window_ms) VALUES('m','test','running',300000,300000)")
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

    let board = sqlx::query("SELECT version,content_json FROM meeting_boards WHERE meeting_id='m'")
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
    sqlx::query("INSERT INTO meetings(id,title,status,next_summary_end_ms,summary_window_ms) VALUES('m','test','running',300000,300000)")
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
async fn list_meetings_returns_newest_first_with_counts() {
    let db = test_db().await;
    sqlx::query(
        "INSERT INTO meetings(id,title,status,created_at) VALUES('m1','较早会议','ended','2026-01-01 00:00:00')",
    )
    .execute(&db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO meetings(id,title,status,created_at) VALUES('m2','最新会议','running','2026-01-02 00:00:00')",
    )
    .execute(&db)
    .await
    .unwrap();
    sqlx::query("INSERT INTO speakers(id,meeting_id,name) VALUES('s1','m2','Alice')")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO audio_segments(id,meeting_id,speaker_id,sequence_no,start_ms,end_ms,file_path,transcript,status) VALUES('a1','m2','s1',1,0,1000,'x.wav','文本','completed')")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path) VALUES('a2','m2',2,1000,2000,'')")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO rolling_summaries(id,meeting_id,window_start_ms,window_end_ms,content_json) VALUES('r1','m2',0,2000,'{}')")
        .execute(&db)
        .await
        .unwrap();
    let app = build_router(test_state(&db));

    let response = ServiceExt::oneshot(
        app.clone(),
        Request::builder()
            .uri("/api/v1/meetings")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let list = serde_json::from_slice::<Value>(&bytes).unwrap();
    assert_eq!(list[0]["id"], "m2");
    assert_eq!(list[0]["segment_count"], 2);
    assert_eq!(list[0]["transcribed_count"], 1);
    assert_eq!(list[0]["speaker_count"], 1);
    assert_eq!(list[0]["summary_count"], 1);
    assert_eq!(list[1]["id"], "m1");

    let response = ServiceExt::oneshot(
        app.clone(),
        Request::builder()
            .uri("/api/v1/meetings?status=ended")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let list = serde_json::from_slice::<Value>(&bytes).unwrap();
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["id"], "m1");

    let response = ServiceExt::oneshot(
        app,
        Request::builder()
            .uri("/api/v1/meetings?status=bogus")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn segment_audio_endpoint_serves_stored_file() {
    let db = test_db().await;
    sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
        .execute(&db)
        .await
        .unwrap();
    let audio_root = std::env::temp_dir().join(format!("diting-audio-{}", Uuid::new_v4()));
    let state = AppState {
        audio_dir: Arc::new(audio_root.clone()),
        ..test_state(&db)
    };
    let app = build_router(state);

    let response = ServiceExt::oneshot(
        app.clone(),
        multipart_request(
            "/api/v1/meetings/m/segments",
            &[("sequence_no", "1"), ("start_ms", "0"), ("end_ms", "1000")],
            Some(("sample.wav", b"RIFFfake")),
        ),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let segment_id = serde_json::from_slice::<Value>(&bytes).unwrap()["id"]
        .as_str()
        .unwrap()
        .to_string();

    // 分段列表暴露音频播放地址
    let response = ServiceExt::oneshot(
        app.clone(),
        Request::builder()
            .uri("/api/v1/meetings/m/segments")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    let segments = serde_json::from_slice::<Value>(&bytes).unwrap();
    assert_eq!(segments[0]["has_audio"], true);
    let audio_url = segments[0]["audio_url"].as_str().unwrap().to_string();
    assert_eq!(
        audio_url,
        format!("/api/v1/meetings/m/segments/{segment_id}/audio")
    );

    // 音频接口返回原始字节与正确的 Content-Type
    let response = ServiceExt::oneshot(
        app.clone(),
        Request::builder()
            .uri(&audio_url)
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        axum::http::HeaderValue::from_static("audio/wav")
    );
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .unwrap();
    assert_eq!(&bytes[..], b"RIFFfake");

    // 纯文本分段没有音频，返回 404
    sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,transcript,status) VALUES('txt','m',2,1000,2000,'','文本','completed')")
        .execute(&db)
        .await
        .unwrap();
    let response = ServiceExt::oneshot(
        app,
        Request::builder()
            .uri("/api/v1/meetings/m/segments/txt/audio")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();
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

    let mut receiver = state.events.subscribe("m");
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
    let job =
        sqlx::query("SELECT COUNT(*) value FROM jobs WHERE job_type='transcribe' AND target_id=?")
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

    process_transcription(
        &state,
        &sqlx::query("SELECT id FROM audio_segments WHERE meeting_id='m'")
            .fetch_one(&db)
            .await
            .unwrap()
            .get::<String, _>("id"),
        "m",
    )
    .await
    .unwrap();

    let segment =
        sqlx::query("SELECT file_path,status,transcript FROM audio_segments WHERE meeting_id='m'")
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
            .body(Body::from(
                r#"{"transcript":"修订文本","speaker_name":"王五"}"#,
            ))
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
    assert_eq!(
        row.get::<Option<String>, _>("speaker_name").as_deref(),
        Some("王五")
    );
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
async fn update_segment_sets_semantic_fields() {
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
    let mut receiver = state.events.subscribe("m");
    let app = build_router(state);

    let response = ServiceExt::oneshot(
        app,
        Request::builder()
            .method("PATCH")
            .uri("/api/v1/meetings/m/segments/seg1")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"semantic_type":"Decision"}"#))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let row = sqlx::query(
        "SELECT semantic_type,semantic_manual_override FROM audio_segments WHERE id='seg1'",
    )
    .fetch_one(&db)
    .await
    .unwrap();
    assert_eq!(row.get::<String, _>("semantic_type"), "decision");
    assert_eq!(row.get::<i64, _>("semantic_manual_override"), 1);

    // segment.updated 事件携带语义字段
    let event = receiver.try_recv().unwrap();
    assert_eq!(event.kind, "segment.updated");
    assert_eq!(event.data["semantic_type"], json!("decision"));
    assert_eq!(event.data["semantic_manual_override"], json!(true));

    // 非法语义类型报 400
    let app = build_router(test_state(&db));
    let response = ServiceExt::oneshot(
        app,
        Request::builder()
            .method("PATCH")
            .uri("/api/v1/meetings/m/segments/seg1")
            .header("content-type", "application/json")
            .body(Body::from(r#"{"semantic_type":"nonsense"}"#))
            .unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn classification_job_classifies_segment() {
    let db = test_db().await;
    sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','test','running')")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,transcript,status) VALUES('seg1','m',1,0,5000,'','我们决定下周三上线','completed')")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,transcript,status,semantic_type,semantic_manual_override) VALUES('seg2','m',2,5000,9000,'','人工指定','completed','question',1)")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES('jc1','classify','m','seg1')",
    )
    .execute(&db)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES('jc2','classify','m','seg2')",
    )
    .execute(&db)
    .await
    .unwrap();
    let state = AppState {
        classifier: Arc::new(FixedClassifier("decision")),
        ..test_state(&db)
    };
    let mut receiver = state.events.subscribe("m");

    process_jobs(&state).await.unwrap();

    // 普通分段被分类并广播；人工指定过的分段不被覆盖
    let row = sqlx::query("SELECT semantic_type FROM audio_segments WHERE id='seg1'")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("semantic_type"), "decision");
    let row = sqlx::query("SELECT semantic_type FROM audio_segments WHERE id='seg2'")
        .fetch_one(&db)
        .await
        .unwrap();
    assert_eq!(row.get::<String, _>("semantic_type"), "question");

    let event = receiver.try_recv().unwrap();
    assert_eq!(event.kind, "segment.updated");
    assert_eq!(event.data["segment_id"], json!("seg1"));
    assert_eq!(event.data["semantic_type"], json!("decision"));
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
    let response = create(app.clone(), r#"{"title":"周会","summary_window_ms":5000}"#)
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
    let meeting =
        sqlx::query("SELECT summary_window_ms,next_summary_end_ms FROM meetings WHERE id=?")
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
    let mut receiver = state.events.subscribe("m");

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

#[tokio::test]
async fn summary_lock_is_released_after_job() {
    let db = test_db().await;
    sqlx::query("INSERT INTO meetings(id,title,status,next_summary_end_ms,summary_window_ms) VALUES('m','test','running',30000,30000)")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query("INSERT INTO audio_segments(id,meeting_id,sequence_no,start_ms,end_ms,file_path,transcript,status) VALUES('a','m',1,0,30000,'','文本','completed')")
        .execute(&db)
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO jobs(id,job_type,meeting_id,target_id) VALUES('j','summary','m','30000')",
    )
    .execute(&db)
    .await
    .unwrap();
    let state = test_state(&db);
    process_jobs(&state).await.unwrap();
    assert_eq!(state.summary_locks.len(), 0);
}
