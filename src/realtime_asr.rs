//! Optional FunASR 2pass preview transport. Audio is still archived; transport
//! failure falls back to the existing durable HTTP transcription job.
use crate::{publish_event, AppState};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpStream, task::JoinHandle};
use tokio_tungstenite::{connect_async, tungstenite::Message, MaybeTlsStream, WebSocketStream};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
pub struct RealtimeSession {
    sender: futures_util::stream::SplitSink<Socket, Message>,
    reader: JoinHandle<Option<String>>,
    stopping: Arc<AtomicBool>,
    failed: bool,
    pending: Vec<i16>,
}
impl RealtimeSession {
    pub async fn start(
        s: &AppState,
        meeting: &str,
        speaker: &str,
        id: &str,
        seq: i64,
        start: i64,
    ) -> Option<Self> {
        let url = std::env::var("DITING_ASR_WS_URL")
            .ok()
            .filter(|v| !v.trim().is_empty())?;
        Self::connect(s, meeting, speaker, id, seq, start, &url).await
    }

    async fn connect(
        s: &AppState,
        meeting: &str,
        speaker: &str,
        id: &str,
        seq: i64,
        start: i64,
        url: &str,
    ) -> Option<Self> {
        let (mut ws, _) =
            match tokio::time::timeout(Duration::from_secs(3), connect_async(url)).await {
                Ok(Ok(value)) => value,
                _ => {
                    publish_event(
                        s,
                        meeting,
                        "ingest.degraded",
                        json!({"reason":"streaming ASR unavailable; using file transcription"}),
                    );
                    return None;
                }
            };
        let init = json!({"mode":"2pass","chunk_size":[5,10,5],"chunk_interval":5,"wav_name":id,
            "wav_format":"pcm","audio_fs":16000,"is_speaking":true,"itn":true,
            "hotwords":std::env::var("DITING_ASR_HOTWORDS").unwrap_or_else(|_|"{}".into())});
        if !matches!(
            tokio::time::timeout(
                Duration::from_secs(2),
                ws.send(Message::Text(init.to_string().into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            return None;
        }
        let (sender, mut receiver) = ws.split();
        let stopping = Arc::new(AtomicBool::new(false));
        let stopped = stopping.clone();
        let state = s.clone();
        let meeting = meeting.to_owned();
        let id = id.to_owned();
        let speaker = speaker.to_owned();
        let name: Option<String> = sqlx::query_scalar("SELECT name FROM speakers WHERE id=?")
            .bind(&speaker)
            .fetch_optional(&s.db)
            .await
            .ok()
            .flatten();
        let reader = tokio::spawn(async move {
            let mut text = String::new();
            let mut preview = String::new();
            while let Some(message) = receiver.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(_) => return None,
                };
                if let Message::Close(frame) = &message {
                    let normal = frame.as_ref().is_none_or(|f| f.code == tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal);
                    return (normal
                        && stopped.load(Ordering::Acquire)
                        && preview.is_empty()
                        && !text.is_empty())
                    .then_some(text);
                }
                let Message::Text(raw) = message else {
                    continue;
                };
                let Ok(data) = serde_json::from_str::<Value>(&raw) else {
                    continue;
                };
                if data["type"] == "state"
                    && data["state"] == "closed"
                    && stopped.load(Ordering::Acquire)
                {
                    return (preview.is_empty() && !text.is_empty()).then_some(text);
                }
                let fragment = data["text"].as_str().unwrap_or_default();
                match data["mode"].as_str().unwrap_or_default() {
                    "2pass-online" | "online" => preview.push_str(fragment),
                    "2pass-offline" | "offline" => {
                        text.push_str(fragment);
                        preview.clear();
                    }
                    _ => continue,
                }
                publish_event(
                    &state,
                    &meeting,
                    "segment.partial",
                    json!({"segment_id":id,"id":id,
                    "sequence_no":seq,"speaker_id":speaker,"speaker_name":name,"start_ms":start,
                    "transcript":format!("{text}{preview}"),"status":"partial","revision":0}),
                );
                if stopped.load(Ordering::Acquire)
                    && data["is_final"] == true
                    // FunASR uses is_final for sentence boundaries too. A nonempty
                    // sentence alone cannot acknowledge all audio sent before stop.
                    && fragment.is_empty()
                    && matches!(data["mode"].as_str(), Some("2pass-offline" | "offline"))
                {
                    return Some(text);
                }
            }
            // A broken transport is never treated as a complete transcript.
            None
        });
        Some(Self {
            sender,
            reader,
            stopping,
            failed: false,
            pending: Vec::new(),
        })
    }
    pub async fn send(&mut self, samples: &[i16]) {
        if self.failed {
            return;
        }
        self.pending.extend_from_slice(samples);
        while self.pending.len() >= 960 && !self.failed {
            let chunk: Vec<i16> = self.pending.drain(..960).collect();
            self.send_frame(&chunk).await;
        }
    }
    async fn send_frame(&mut self, samples: &[i16]) {
        let bytes: Vec<u8> = samples.iter().flat_map(|v| v.to_le_bytes()).collect();
        if !matches!(
            tokio::time::timeout(
                Duration::from_secs(2),
                self.sender.send(Message::Binary(bytes.into()))
            )
            .await,
            Ok(Ok(()))
        ) {
            self.failed = true;
        }
    }
    pub async fn finish(mut self) -> Option<String> {
        if !self.pending.is_empty() && !self.failed {
            let tail = std::mem::take(&mut self.pending);
            self.send_frame(&tail).await;
        }
        let result = if self.failed {
            None
        } else {
            self.stopping.store(true, Ordering::Release);
            let sent = tokio::time::timeout(
                Duration::from_secs(2),
                self.sender.send(Message::Text(
                    json!({"is_speaking":false}).to_string().into(),
                )),
            )
            .await;
            if matches!(sent, Ok(Ok(()))) {
                match tokio::time::timeout(Duration::from_secs(5), &mut self.reader).await {
                    Ok(Ok(text)) => text.filter(|t| !t.trim().is_empty()),
                    _ => None,
                }
            } else {
                None
            }
        };
        self.reader.abort();
        let _ = tokio::time::timeout(Duration::from_millis(200), self.sender.close()).await;
        result
    }
}
impl Drop for RealtimeSession {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

pub async fn correct_text(text: &str) -> Result<String, String> {
    let base = std::env::var("DITING_LLM_BASE_URL").map_err(|e| e.to_string())?;
    let model = std::env::var("DITING_LLM_MODEL").map_err(|e| e.to_string())?;
    let key = std::env::var("DITING_LLM_API_KEY").unwrap_or_default();
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| e.to_string())?;
    let response=client.post(format!("{}/chat/completions",base.trim_end_matches('/'))).bearer_auth(key)
        .json(&json!({"model":model,"temperature":0,"messages":[
            {"role":"system","content":"你是会议转写校对器。只修正明确的同音错字、重复词和标点；保持原意、数字、专有名词与事实。不得总结、增补、改写或添加说话人。只输出校对后的正文。"},
            {"role":"user","content":text}]})).send().await.map_err(|e|e.to_string())?
        .error_for_status().map_err(|e|e.to_string())?;
    let data: Value = response.json().await.map_err(|e| e.to_string())?;
    data.pointer("/choices/0/message/content")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_owned)
        .ok_or_else(|| "empty correction".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn streaming_preview_and_tail_share_segment_identity() {
        let db = crate::tests::test_db().await;
        let s = crate::tests::test_state(&db);
        let mut events = s.events.subscribe();
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
        let mut session = RealtimeSession::connect(&s, "m", "speaker", "segment", 7, 12000, &url)
            .await
            .unwrap();
        session.send(&vec![1000; 1000]).await;
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
        let session = RealtimeSession::connect(&s, "m", "speaker", "segment", 0, 0, &url)
            .await
            .unwrap();
        server.await.unwrap();
        assert!(session.finish().await.is_none());
    }
}
