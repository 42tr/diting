//! LiveKit 进房订阅：以 bot 身份加入房间，订阅全部远端音频轨道，
//! 按固定窗口（默认 5s）切分 PCM 落盘为 WAV，复用既有转写/摘要流水线。
//!
//! 由创建会议时携带的 `livekit` 配置触发；`end_meeting`/`delete_meeting`
//! 通过 stop 信号通知退出，退出前会把各轨道未满一个窗口的尾包 flush 掉。

use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use livekit::prelude::*;
use livekit::webrtc::audio_stream::native::NativeAudioStream;
use serde::Deserialize;
use serde_json::json;
use sqlx::{Row, SqlitePool};
use tokio::{sync::watch, task::JoinSet};
use tokio_stream::StreamExt;
use tracing::{error, info, warn};
use uuid::Uuid;

use crate::{publish_event, AppState};

pub const INGEST_SAMPLE_RATE: u32 = 16_000;
const INGEST_CHANNELS: i32 = 1;
const MAX_CONNECT_ATTEMPTS: u32 = 5;
/// 尾包短于该时长（毫秒）直接丢弃——太短没有转写价值
const MIN_FLUSH_MS: i64 = 500;

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct LivekitIngest {
    /// LiveKit 服务地址（ws:// 或 wss://）
    pub url: String,
    /// 房间名（仅用于日志）
    pub room_name: String,
    /// 进房 token（由调用方用 LiveKit API Key 签发，需带 room join 权限，建议长 TTL）
    pub token: String,
}

pub type IngestStopMap = Arc<Mutex<HashMap<String, watch::Sender<bool>>>>;

/// 会议创建后启动进房订阅；同会议重复调用幂等。
pub fn spawn_ingest(s: &AppState, meeting_id: &str, cfg: LivekitIngest) {
    let room_name = cfg.room_name.clone();
    {
        let mut stops = s.ingest_stop.lock().unwrap();
        if stops.contains_key(meeting_id) {
            return;
        }
        let (tx, rx) = watch::channel(false);
        stops.insert(meeting_id.to_string(), tx);
        let state = s.clone();
        let id = meeting_id.to_string();
        tokio::spawn(async move {
            ingest_loop(state.clone(), id.clone(), cfg, rx).await;
            state.ingest_stop.lock().unwrap().remove(&id);
        });
    }
    info!(meeting_id, room = %room_name, "livekit ingest scheduled");
}

/// 会议结束/删除时通知进房任务退出（会先 flush 未完成的音频窗口）。
pub fn stop_ingest(s: &AppState, meeting_id: &str) {
    if let Some(tx) = s.ingest_stop.lock().unwrap().get(meeting_id) {
        let _ = tx.send(true);
    }
}

fn ingest_window_ms() -> i64 {
    std::env::var("DITING_INGEST_WINDOW_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &i64| (1000..=60_000).contains(v))
        .unwrap_or(5_000)
}

async fn meeting_ended(db: &SqlitePool, meeting_id: &str) -> bool {
    sqlx::query("SELECT status='ended' AS v FROM meetings WHERE id=?")
        .bind(meeting_id)
        .fetch_optional(db)
        .await
        .map(|row| row.map(|r| r.get::<bool, _>("v")).unwrap_or(true))
        .unwrap_or(true)
}

async fn ingest_loop(
    s: AppState,
    meeting_id: String,
    cfg: LivekitIngest,
    mut stop: watch::Receiver<bool>,
) {
    let mut attempt = 0u32;
    loop {
        if *stop.borrow() || meeting_ended(&s.db, &meeting_id).await {
            break;
        }
        match Room::connect(&cfg.url, &cfg.token, RoomOptions::default()).await {
            Ok((room, events)) => {
                attempt = 0;
                info!(meeting_id, room = %cfg.room_name, "livekit room connected");
                run_session(&s, &meeting_id, room, events, &mut stop).await;
            }
            Err(e) => {
                attempt += 1;
                if attempt >= MAX_CONNECT_ATTEMPTS {
                    error!(meeting_id, error = %e.to_string(), attempt, "livekit ingest gave up");
                    publish_event(
                        &s,
                        &meeting_id,
                        "ingest.failed",
                        json!({"error": e.to_string(), "attempts": attempt}),
                    );
                    break;
                }
                warn!(meeting_id, error = %e.to_string(), attempt, "livekit connect failed, retrying");
            }
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs((attempt.max(1)) as u64 * 2)) => {}
            _ = stop.changed() => {}
        }
    }
    info!(meeting_id, "livekit ingest stopped");
}

async fn run_session(
    s: &AppState,
    meeting_id: &str,
    room: Room,
    mut events: tokio::sync::mpsc::UnboundedReceiver<RoomEvent>,
    stop: &mut watch::Receiver<bool>,
) {
    // 进房时刻作为该次会议音频时间轴零点
    let t0 = Instant::now();
    let next_seq = Arc::new(AtomicI64::new(max_sequence_no(&s.db, meeting_id).await + 1));
    let window_ms = ingest_window_ms();
    // identity -> speaker_id，同会议内复用，避免并发建重复 speaker
    let speakers = Arc::new(tokio::sync::Mutex::new(HashMap::<String, String>::new()));
    let mut tracks = JoinSet::new();
    loop {
        tokio::select! {
            event = events.recv() => match event {
                Some(RoomEvent::TrackSubscribed { track, participant, .. }) => {
                    let RemoteTrack::Audio(audio) = track else { continue };
                    let identity = participant.identity().as_str().to_string();
                    let display = participant.name();
                    let display = if display.is_empty() { identity.clone() } else { display };
                    info!(meeting_id, identity, "subscribed remote audio track");
                    tracks.spawn(track_ingest(
                        s.clone(),
                        meeting_id.to_string(),
                        audio,
                        identity,
                        display,
                        t0,
                        window_ms,
                        next_seq.clone(),
                        speakers.clone(),
                    ));
                }
                Some(RoomEvent::Disconnected { reason }) => {
                    info!(meeting_id, ?reason, "livekit room disconnected");
                    break;
                }
                None => break,
                _ => {}
            },
            _ = stop.changed() => {
                break;
            }
        }
    }
    if let Err(e) = room.close().await {
        warn!(meeting_id, error = %e.to_string(), "failed to close livekit room cleanly");
    }
    // 等所有轨道把剩余音频 flush 完再退出
    while let Some(result) = tracks.join_next().await {
        if let Err(e) = result {
            warn!(meeting_id, error = %e, "track ingest task join failed");
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn track_ingest(
    s: AppState,
    meeting_id: String,
    track: RemoteAudioTrack,
    identity: String,
    display_name: String,
    t0: Instant,
    window_ms: i64,
    next_seq: Arc<AtomicI64>,
    speakers: Arc<tokio::sync::Mutex<HashMap<String, String>>>,
) {
    let speaker_id = match cached_speaker(&s.db, &speakers, &meeting_id, &identity, &display_name)
        .await
    {
        Ok(id) => Some(id),
        Err(e) => {
            error!(meeting_id, identity, error = %e, "failed to register speaker");
            None
        }
    };
    let mut stream = NativeAudioStream::new(
        track.rtc_track(),
        INGEST_SAMPLE_RATE as i32,
        INGEST_CHANNELS,
    );
    let window_samples = (INGEST_SAMPLE_RATE as i64 * window_ms / 1000) as usize;
    let mut buf: Vec<i16> = Vec::with_capacity(window_samples * 2);
    while let Some(frame) = stream.next().await {
        buf.extend_from_slice(&frame.data);
        while buf.len() >= window_samples {
            let chunk: Vec<i16> = buf.drain(..window_samples).collect();
            if let Err(e) = flush_chunk(&s, &meeting_id, speaker_id.as_deref(), &chunk, t0, &next_seq).await {
                error!(meeting_id, error = %e, "failed to store audio window");
            }
        }
    }
    // 轨道结束：剩余不足一个窗口的尾包落盘（太短则丢弃）
    let tail_ms = buf.len() as i64 * 1000 / INGEST_SAMPLE_RATE as i64;
    if tail_ms >= MIN_FLUSH_MS {
        if let Err(e) = flush_chunk(&s, &meeting_id, speaker_id.as_deref(), &buf, t0, &next_seq).await {
            error!(meeting_id, error = %e, "failed to flush audio tail");
        }
    }
    info!(meeting_id, identity, "audio track ingest ended");
}

/// 把一段 PCM 写成 WAV 并入队转写；start/end 取进房时刻的相对毫秒。
async fn flush_chunk(
    s: &AppState,
    meeting_id: &str,
    speaker_id: Option<&str>,
    samples: &[i16],
    t0: Instant,
    next_seq: &AtomicI64,
) -> Result<String, String> {
    let end_ms = t0.elapsed().as_millis() as i64;
    let duration_ms = samples.len() as i64 * 1000 / INGEST_SAMPLE_RATE as i64;
    let start_ms = (end_ms - duration_ms).max(0);
    let id = Uuid::new_v4().to_string();
    let dir = s.audio_dir.join(meeting_id);
    tokio::fs::create_dir_all(&dir).await.map_err(|e| e.to_string())?;
    let path = dir.join(format!("{id}-live.wav"));
    tokio::fs::write(&path, wav_bytes(samples, INGEST_SAMPLE_RATE))
        .await
        .map_err(|e| e.to_string())?;
    let file_path = path.to_string_lossy().to_string();
    let seq = next_seq.fetch_add(1, Ordering::Relaxed);
    if let Err(e) = crate::insert_segment(
        &s.db,
        &id,
        meeting_id,
        speaker_id,
        seq,
        start_ms,
        end_ms,
        &file_path,
        None,
    )
    .await
    {
        let _ = tokio::fs::remove_file(&path).await;
        return Err(e.to_string());
    }
    s.job_notify.notify_one();
    publish_event(
        s,
        meeting_id,
        "segment.uploaded",
        json!({"segment_id": id, "sequence_no": seq, "start_ms": start_ms, "end_ms": end_ms, "source": "livekit"}),
    );
    Ok(id)
}

/// 同一 identity 只建一个 speaker（按显示名复用既有记录）。
async fn cached_speaker(
    db: &SqlitePool,
    speakers: &tokio::sync::Mutex<HashMap<String, String>>,
    meeting_id: &str,
    identity: &str,
    display_name: &str,
) -> Result<String, sqlx::Error> {
    let mut cache = speakers.lock().await;
    if let Some(id) = cache.get(identity) {
        return Ok(id.clone());
    }
    let id = ensure_speaker_by_name(db, meeting_id, display_name).await?;
    cache.insert(identity.to_string(), id.clone());
    Ok(id)
}

/// 按 (meeting_id, name) 复用或创建说话人。
pub(crate) async fn ensure_speaker_by_name(
    db: &SqlitePool,
    meeting_id: &str,
    name: &str,
) -> Result<String, sqlx::Error> {
    let existing = sqlx::query(
        "SELECT id FROM speakers WHERE meeting_id=? AND name=? ORDER BY created_at LIMIT 1",
    )
    .bind(meeting_id)
    .bind(name)
    .fetch_optional(db)
    .await?;
    if let Some(row) = existing {
        return Ok(row.get("id"));
    }
    let id = Uuid::new_v4().to_string();
    sqlx::query("INSERT INTO speakers(id,meeting_id,name) VALUES(?,?,?)")
        .bind(&id)
        .bind(meeting_id)
        .bind(name)
        .execute(db)
        .await?;
    Ok(id)
}

async fn max_sequence_no(db: &SqlitePool, meeting_id: &str) -> i64 {
    sqlx::query("SELECT COALESCE(MAX(sequence_no), -1) AS v FROM audio_segments WHERE meeting_id=?")
        .bind(meeting_id)
        .fetch_one(db)
        .await
        .map(|r| r.get::<i64, _>("v"))
        .unwrap_or(-1)
}

/// 16-bit 单声道 PCM 裸数据加 WAV 头。
pub(crate) fn wav_bytes(samples: &[i16], sample_rate: u32) -> Vec<u8> {
    let data_len = (samples.len() * 2) as u32;
    let mut out = Vec::with_capacity(44 + data_len as usize);
    out.extend_from_slice(b"RIFF");
    out.extend_from_slice(&(36 + data_len).to_le_bytes());
    out.extend_from_slice(b"WAVEfmt ");
    out.extend_from_slice(&16u32.to_le_bytes()); // fmt chunk size
    out.extend_from_slice(&1u16.to_le_bytes()); // PCM
    out.extend_from_slice(&1u16.to_le_bytes()); // mono
    out.extend_from_slice(&sample_rate.to_le_bytes());
    out.extend_from_slice(&(sample_rate * 2).to_le_bytes()); // byte rate
    out.extend_from_slice(&2u16.to_le_bytes()); // block align
    out.extend_from_slice(&16u16.to_le_bytes()); // bits per sample
    out.extend_from_slice(b"data");
    out.extend_from_slice(&data_len.to_le_bytes());
    for sample in samples {
        out.extend_from_slice(&sample.to_le_bytes());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wav_header_is_well_formed() {
        let samples = [0i16, 1000, -1000];
        let wav = wav_bytes(&samples, 16_000);
        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(u32::from_le_bytes(wav[4..8].try_into().unwrap()), 36 + 6);
        assert_eq!(u32::from_le_bytes(wav[24..28].try_into().unwrap()), 16_000);
        assert_eq!(&wav[36..40], b"data");
        assert_eq!(u32::from_le_bytes(wav[40..44].try_into().unwrap()), 6);
        assert_eq!(wav.len(), 44 + 6);
    }

    #[tokio::test]
    async fn ensure_speaker_reuses_existing_name() {
        let db = crate::tests::test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m1','t','running')")
            .execute(&db)
            .await
            .unwrap();
        let first = ensure_speaker_by_name(&db, "m1", "张三").await.unwrap();
        let second = ensure_speaker_by_name(&db, "m1", "张三").await.unwrap();
        let other = ensure_speaker_by_name(&db, "m1", "李四").await.unwrap();
        assert_eq!(first, second);
        assert_ne!(first, other);
    }
}
