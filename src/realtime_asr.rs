//! Optional FunASR 2pass preview transport. Audio is still archived; transport
//! failure falls back to the existing durable HTTP transcription job.
//!
//! 连接、发送与读取全部在后台任务里完成：`send` 只做非阻塞入队，慢速或故障的
//! ASR 服务不会拖住音频落盘循环，队列满时本句降级为文件转写。
use crate::{providers::env_nonempty, publish_event, AppState};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::mpsc,
    task::JoinHandle,
    time::{sleep_until, timeout, Instant},
};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{protocol::frame::coding::CloseCode, Message},
    MaybeTlsStream, WebSocketStream,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Sink = futures_util::stream::SplitSink<Socket, Message>;
type Stream = futures_util::stream::SplitStream<Socket>;

/// 每个 WebSocket 音频帧的采样数（16kHz 下 60ms）。
const FRAME_SAMPLES: usize = 960;
/// 待发送音频队列，约 10 秒的 10ms 帧。
const AUDIO_QUEUE_FRAMES: usize = 1000;
/// 连接失败后暂停尝试的时长，避免每句话都重连并刷屏 ingest.degraded。
const RETRY_AFTER_FAILURE: Duration = Duration::from_secs(30);
/// 句尾发出 is_speaking=false 后等待最终结果的时长。
const FINAL_RESULT_TIMEOUT: Duration = Duration::from_secs(5);
const SEND_TIMEOUT: Duration = Duration::from_secs(2);

/// 预览字幕所属的分段信息，随 segment.partial 事件下发。
#[derive(Clone)]
pub struct PreviewTarget {
    pub meeting_id: String,
    pub speaker_id: String,
    pub speaker_name: Option<String>,
    pub segment_id: String,
    pub sequence_no: i64,
    pub start_ms: i64,
}

/// 同一音轨共享：连接失败后一段时间内不再尝试流式 ASR。
#[derive(Clone, Default)]
pub struct PreviewBackoff(Arc<Mutex<Option<Instant>>>);

impl PreviewBackoff {
    fn available(&self) -> bool {
        self.0
            .lock()
            .unwrap()
            .is_none_or(|until| Instant::now() >= until)
    }

    fn trip(&self) {
        *self.0.lock().unwrap() = Some(Instant::now() + RETRY_AFTER_FAILURE);
    }
}

struct AbortOnDrop(JoinHandle<Option<String>>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub struct RealtimeSession {
    audio: mpsc::Sender<Vec<i16>>,
    task: AbortOnDrop,
    overflowed: bool,
}

impl RealtimeSession {
    /// 未配置 DITING_ASR_WS_URL 或处于失败退避期时返回 None。
    pub fn start(s: &AppState, target: PreviewTarget, backoff: &PreviewBackoff) -> Option<Self> {
        let url = env_nonempty("DITING_ASR_WS_URL")?;
        if !backoff.available() {
            return None;
        }
        Some(Self::spawn(s, target, url, backoff.clone()))
    }

    fn spawn(s: &AppState, target: PreviewTarget, url: String, backoff: PreviewBackoff) -> Self {
        let (audio, rx) = mpsc::channel(AUDIO_QUEUE_FRAMES);
        let task = tokio::spawn(run(s.clone(), target, url, rx, backoff));
        Self {
            audio,
            task: AbortOnDrop(task),
            overflowed: false,
        }
    }

    /// 非阻塞：队列满说明 ASR 跟不上，本句放弃预览结果，由文件转写兜底。
    pub fn send(&mut self, samples: &[i16]) {
        if !self.overflowed && self.audio.try_send(samples.to_vec()).is_err() {
            self.overflowed = true;
        }
    }

    /// 结束本句并等待最终结果；任何异常都返回 None（不把临时字幕当成最终结果）。
    pub async fn finish(self) -> Option<String> {
        let Self {
            audio,
            mut task,
            overflowed,
        } = self;
        drop(audio);
        if overflowed {
            return None;
        }
        let budget = FINAL_RESULT_TIMEOUT + SEND_TIMEOUT * 2;
        match timeout(budget, &mut task.0).await {
            Ok(Ok(text)) => text,
            _ => None,
        }
    }
}

async fn run(
    s: AppState,
    target: PreviewTarget,
    url: String,
    mut audio: mpsc::Receiver<Vec<i16>>,
    backoff: PreviewBackoff,
) -> Option<String> {
    let degrade = || {
        backoff.trip();
        publish_event(
            &s,
            &target.meeting_id,
            "ingest.degraded",
            json!({"reason":"streaming ASR unavailable; using file transcription"}),
        );
    };
    let Ok(Ok((ws, _))) = timeout(Duration::from_secs(3), connect_async(&url)).await else {
        degrade();
        return None;
    };
    let (mut sink, mut stream) = ws.split();
    let init = json!({"mode":"2pass","chunk_size":[5,10,5],"chunk_interval":5,"wav_name":target.segment_id,
        "wav_format":"pcm","audio_fs":16000,"is_speaking":true,"itn":true,
        "hotwords":std::env::var("DITING_ASR_HOTWORDS").unwrap_or_else(|_|"{}".into())});
    if !send_text(&mut sink, init).await {
        degrade();
        return None;
    }
    let result = stream_session(&s, &target, &mut sink, &mut stream, &mut audio).await;
    let _ = timeout(Duration::from_millis(200), sink.close()).await;
    result.filter(|text| !text.trim().is_empty())
}

async fn send_text(sink: &mut Sink, value: Value) -> bool {
    matches!(
        timeout(
            SEND_TIMEOUT,
            sink.send(Message::Text(value.to_string().into()))
        )
        .await,
        Ok(Ok(()))
    )
}

async fn send_audio(sink: &mut Sink, samples: &[i16]) -> bool {
    let bytes: Vec<u8> = samples.iter().flat_map(|v| v.to_le_bytes()).collect();
    matches!(
        timeout(SEND_TIMEOUT, sink.send(Message::Binary(bytes.into()))).await,
        Ok(Ok(()))
    )
}

async fn stream_session(
    s: &AppState,
    t: &PreviewTarget,
    sink: &mut Sink,
    stream: &mut Stream,
    audio: &mut mpsc::Receiver<Vec<i16>>,
) -> Option<String> {
    let mut pending: Vec<i16> = Vec::new();
    let mut text = String::new();
    let mut preview = String::new();
    let mut deadline: Option<Instant> = None;
    loop {
        let stopping = deadline.is_some();
        tokio::select! {
            samples = audio.recv(), if !stopping => match samples {
                Some(samples) => {
                    pending.extend_from_slice(&samples);
                    while pending.len() >= FRAME_SAMPLES {
                        let chunk: Vec<i16> = pending.drain(..FRAME_SAMPLES).collect();
                        if !send_audio(sink, &chunk).await {
                            return None;
                        }
                    }
                }
                None => {
                    // 本句结束：发出尾包和 is_speaking=false，等待最终结果。
                    if !pending.is_empty() && !send_audio(sink, &pending).await {
                        return None;
                    }
                    pending.clear();
                    if !send_text(sink, json!({"is_speaking":false})).await {
                        return None;
                    }
                    deadline = Some(Instant::now() + FINAL_RESULT_TIMEOUT);
                }
            },
            message = stream.next() => {
                // A broken transport is never treated as a complete transcript.
                let Some(Ok(message)) = message else { return None };
                if let Message::Close(frame) = &message {
                    let normal = frame.as_ref().is_none_or(|f| f.code == CloseCode::Normal);
                    return (normal && stopping && preview.is_empty() && !text.is_empty()).then_some(text);
                }
                let Message::Text(raw) = message else { continue };
                let Ok(data) = serde_json::from_str::<Value>(&raw) else { continue };
                if data["type"] == "state" && data["state"] == "closed" && stopping {
                    return (preview.is_empty() && !text.is_empty()).then_some(text);
                }
                let fragment = data["text"].as_str().unwrap_or_default();
                let offline = match data["mode"].as_str().unwrap_or_default() {
                    "2pass-online" | "online" => {
                        preview.push_str(fragment);
                        false
                    }
                    "2pass-offline" | "offline" => {
                        text.push_str(fragment);
                        preview.clear();
                        true
                    }
                    _ => continue,
                };
                publish_event(
                    s,
                    &t.meeting_id,
                    "segment.partial",
                    json!({"segment_id":t.segment_id,"id":t.segment_id,
                        "sequence_no":t.sequence_no,"speaker_id":t.speaker_id,"speaker_name":t.speaker_name,
                        "start_ms":t.start_ms,"transcript":format!("{text}{preview}"),"status":"partial","revision":0}),
                );
                // FunASR uses is_final for sentence boundaries too. A nonempty
                // sentence alone cannot acknowledge all audio sent before stop.
                if stopping && offline && data["is_final"] == true && fragment.is_empty() {
                    return Some(text);
                }
            }
            _ = sleep_until(deadline.unwrap_or_else(Instant::now)), if stopping => return None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target() -> PreviewTarget {
        PreviewTarget {
            meeting_id: "m".into(),
            speaker_id: "speaker".into(),
            speaker_name: None,
            segment_id: "segment".into(),
            sequence_no: 7,
            start_ms: 12000,
        }
    }

    #[tokio::test]
    async fn streaming_preview_and_tail_share_segment_identity() {
        let db = crate::tests::test_db().await;
        let s = crate::tests::test_state(&db);
        let mut events = s.events.subscribe("m");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let mut received = 0;
            while let Some(Ok(msg)) = ws.next().await {
                match msg {
                    Message::Binary(bytes) => {
                        received += bytes.len();
                        ws.send(Message::Text(
                            json!({"mode":"2pass-online","text":"临时"})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                    }
                    Message::Text(raw) => {
                        let data: Value = serde_json::from_str(&raw).unwrap();
                        if data["is_speaking"] == false {
                            ws.send(Message::Text(
                                json!({"mode":"2pass-offline","text":"最终文字","is_final":true})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .unwrap();
                            ws.close(None).await.unwrap();
                            return received;
                        }
                    }
                    _ => {}
                }
            }
            received
        });
        let mut session = RealtimeSession::spawn(&s, target(), url, PreviewBackoff::default());
        session.send(&[1000; 1000]);
        assert_eq!(session.finish().await.as_deref(), Some("最终文字"));
        assert_eq!(server.await.unwrap(), 2000); // includes the final 40 samples
        let event = events.recv().await.unwrap();
        assert_eq!(event.kind, "segment.partial");
        assert_eq!(event.data["segment_id"], "segment");
        assert_eq!(event.data["start_ms"], 12000);
    }

    #[tokio::test]
    async fn broken_stream_does_not_promote_partial_text_to_final() {
        let db = crate::tests::test_db().await;
        let s = crate::tests::test_state(&db);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            ws.send(Message::Text(
                json!({"mode":"2pass-online","text":"不完整"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            ws.close(None).await.unwrap();
        });
        let session = RealtimeSession::spawn(&s, target(), url, PreviewBackoff::default());
        server.await.unwrap();
        assert!(session.finish().await.is_none());
    }

    #[tokio::test]
    async fn unreachable_server_trips_backoff_and_degrades() {
        let db = crate::tests::test_db().await;
        let s = crate::tests::test_state(&db);
        let mut events = s.events.subscribe("m");
        // 绑定后立即释放端口，确保连接被拒绝。
        let url = {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            format!("ws://{}", listener.local_addr().unwrap())
        };
        let backoff = PreviewBackoff::default();
        let session = RealtimeSession::spawn(&s, target(), url, backoff.clone());
        assert!(session.finish().await.is_none());
        assert!(!backoff.available());
        assert_eq!(events.recv().await.unwrap().kind, "ingest.degraded");
    }
}
