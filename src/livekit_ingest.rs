//! LiveKit audio ingestion: cancellable readers, bounded persistence queue and a
//! meeting-wide clock. Each participant identity retains its own speaker record.
use crate::{publish_event, realtime_asr::RealtimeSession, AppState};
use futures_util::FutureExt;
use livekit::prelude::*;
use livekit::webrtc::audio_stream::native::{NativeAudioStream, NativeAudioStreamOptions};
use serde::Deserialize;
use serde_json::json;
use sqlx::SqlitePool;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    sync::{mpsc, watch},
    task::JoinSet,
};
use tokio_stream::StreamExt;
use tracing::{info, warn};
use uuid::Uuid;

pub const INGEST_SAMPLE_RATE: u32 = 16_000;
const MAX_CONNECT_ATTEMPTS: u32 = 5;

#[derive(Debug, Clone, Deserialize, utoipa::ToSchema)]
pub struct LivekitIngest {
    pub url: String,
    pub room_name: String,
    pub token: String,
}

#[derive(Clone)]
pub struct IngestControl {
    stop: watch::Sender<bool>,
    done: watch::Receiver<Option<Result<(), String>>>,
}
pub type IngestStopMap = Arc<Mutex<HashMap<String, IngestControl>>>;

pub fn spawn_ingest(s: &AppState, meeting_id: &str, cfg: LivekitIngest) {
    let mut stops = s.ingest_stop.lock().unwrap();
    if stops.contains_key(meeting_id) {
        return;
    }
    let (stop, rx) = watch::channel(false);
    let (done_tx, done) = watch::channel(None);
    stops.insert(meeting_id.to_owned(), IngestControl { stop, done });
    let state = s.clone();
    let id = meeting_id.to_owned();
    tokio::spawn(async move {
        let result = ingest_loop(&state, &id, cfg, rx).await;
        if let Err(error) = &result {
            publish_event(&state, &id, "ingest.failed", json!({"error":error}));
        }
        let ingest_status = if result.is_ok() { "stopped" } else { "failed" };
        let _ = sqlx::query("UPDATE meetings SET ingest_status=?,ingest_error=? WHERE id=?")
            .bind(ingest_status)
            .bind(result.as_ref().err())
            .bind(&id)
            .execute(&state.db)
            .await;
        if result.is_ok() {
            let ending: bool =
                sqlx::query_scalar("SELECT status='ending' FROM meetings WHERE id=?")
                    .bind(&id)
                    .fetch_optional(&state.db)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or(false);
            if ending {
                let _ = crate::complete_meeting_end(&state, &id).await;
            }
        }
        done_tx.send_replace(Some(result));
        state.ingest_stop.lock().unwrap().remove(&id);
    });
}

/// Returns only after readers and persistence tasks have finished, or an error.
pub async fn stop_ingest(s: &AppState, meeting_id: &str) -> Result<(), String> {
    let control = s.ingest_stop.lock().unwrap().get(meeting_id).cloned();
    if let Some(mut control) = control {
        control.stop.send_replace(true);
        tokio::time::timeout(Duration::from_secs(20), async {
            loop {
                if let Some(result) = control.done.borrow().clone() {
                    return result;
                }
                control
                    .done
                    .changed()
                    .await
                    .map_err(|_| "ingest task exited without acknowledgement".to_string())?;
            }
        })
        .await
        .map_err(|_| {
            "timed out saving the last audio segments; retry ending the meeting".to_string()
        })??;
    }
    Ok(())
}

fn ingest_window_ms() -> i64 {
    std::env::var("DITING_INGEST_WINDOW_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|v: &i64| (1000..=60_000).contains(v))
        .unwrap_or(5000)
}

#[derive(Clone, Copy)]
struct MeetingClock {
    origin: Instant,
    offset_ms: i64,
}
impl MeetingClock {
    fn now_ms(&self) -> i64 {
        self.offset_ms + self.origin.elapsed().as_millis() as i64
    }
}

async fn ingest_loop(
    s: &AppState,
    meeting_id: &str,
    cfg: LivekitIngest,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    // The offset includes time since meeting creation. Never reset it on reconnect.
    let offset: i64 = sqlx::query_scalar("SELECT MAX(CAST((julianday('now')-julianday(started_at))*86400000 AS INTEGER),COALESCE((SELECT MAX(end_ms) FROM audio_segments WHERE meeting_id=meetings.id),0),0) FROM meetings WHERE id=?")
        .bind(meeting_id).fetch_one(&s.db).await.map_err(|e|e.to_string())?;
    let clock = MeetingClock {
        origin: Instant::now(),
        offset_ms: offset,
    };
    let next_seq = Arc::new(AtomicI64::new(max_sequence_no(&s.db, meeting_id).await + 1));
    let mut attempt = 0;
    loop {
        if *stop.borrow() {
            return Ok(());
        }
        let connected = tokio::select! {
            _ = stop.changed() => return Ok(()),
            result = tokio::time::timeout(Duration::from_secs(15), Room::connect(&cfg.url, &cfg.token, RoomOptions::default())) => result,
        };
        match connected {
            Ok(Ok((room, events))) => {
                attempt = 0;
                let _ = sqlx::query(
                    "UPDATE meetings SET ingest_status='running',ingest_error=NULL WHERE id=?",
                )
                .bind(meeting_id)
                .execute(&s.db)
                .await;
                publish_event(
                    s,
                    meeting_id,
                    "ingest.connected",
                    json!({"room_name":cfg.room_name}),
                );
                run_session(
                    s,
                    meeting_id,
                    room,
                    events,
                    &mut stop,
                    clock,
                    next_seq.clone(),
                )
                .await?;
                if *stop.borrow() {
                    return Ok(());
                }
                publish_event(s, meeting_id, "ingest.reconnecting", json!({}));
            }
            result => {
                attempt += 1;
                warn!(meeting_id, attempt, error=?result.err(), "LiveKit connection failed");
                if attempt >= MAX_CONNECT_ATTEMPTS {
                    return Err("LiveKit connection retries exhausted".into());
                }
            }
        }
        tokio::select! {
            _ = stop.changed() => return Ok(()),
            _ = tokio::time::sleep(Duration::from_secs(attempt.max(1) as u64 * 2)) => {},
        }
    }
}

async fn run_session(
    s: &AppState,
    meeting_id: &str,
    room: Room,
    mut events: tokio::sync::mpsc::UnboundedReceiver<RoomEvent>,
    stop: &mut watch::Receiver<bool>,
    clock: MeetingClock,
    next_seq: Arc<AtomicI64>,
) -> Result<(), String> {
    let mut tracks = JoinSet::new();
    let mut track_stops: HashMap<String, watch::Sender<bool>> = HashMap::new();
    let mut failure = None;
    loop {
        if *stop.borrow() {
            break;
        }
        tokio::select! {
            _ = stop.changed() => break,
            result = tracks.join_next(), if !tracks.is_empty() => {
                match result {
                    Some(Ok((sid, result))) => {
                        track_stops.remove(&sid);
                        if let Err(error) = result { failure = Some(error); break; }
                    }
                    Some(Err(error)) => { failure = Some(error.to_string()); break; }
                    _ => {}
                }
            }
            event = events.recv() => match event {
                Some(RoomEvent::TrackSubscribed { track: RemoteTrack::Audio(audio), participant, .. }) => {
                    let sid = audio.sid().to_string();
                    if track_stops.contains_key(&sid) { continue; }
                    let identity = participant.identity().as_str().to_owned();
                    let name = if participant.name().is_empty() { identity.clone() } else { participant.name() };
                    let (tx, rx) = watch::channel(false);
                    track_stops.insert(sid.clone(),tx);
                    let state=s.clone(); let id=meeting_id.to_owned(); let seq=next_seq.clone();
                    tracks.spawn(async move {
                        let result = track_ingest(state,id,audio,identity,name,clock,seq,rx).await;
                        (sid,result)
                    });
                }
                Some(RoomEvent::TrackUnsubscribed { track, .. }) => {
                    if let Some(tx) = track_stops.get(&track.sid().to_string()) { tx.send_replace(true); }
                }
                Some(RoomEvent::Disconnected { .. }) | None => break,
                _ => {}
            }
        }
    }
    for tx in track_stops.values() {
        tx.send_replace(true);
    }
    let _ = tokio::time::timeout(Duration::from_secs(3), room.close()).await;
    while let Some(result) = tracks.join_next().await {
        match result {
            Ok((_, Err(error))) => {
                failure.get_or_insert(error);
            }
            Err(error) => {
                failure.get_or_insert(error.to_string());
            }
            _ => {}
        }
    }
    failure.map_or(Ok(()), Err)
}

struct CapturedFrame {
    samples: Vec<i16>,
    start_ms: i64,
}

#[allow(clippy::too_many_arguments)]
async fn track_ingest(
    s: AppState,
    meeting_id: String,
    track: RemoteAudioTrack,
    identity: String,
    display_name: String,
    clock: MeetingClock,
    next_seq: Arc<AtomicI64>,
    mut stop: watch::Receiver<bool>,
) -> Result<(), String> {
    let speaker_id = ensure_speaker_identity(&s.db, &meeting_id, &identity, &display_name)
        .await
        .map_err(|e| e.to_string())?;
    let mut stream = NativeAudioStream::with_options(
        track.rtc_track(),
        16000,
        1,
        NativeAudioStreamOptions {
            queue_size_frames: Some(200),
        },
    );
    // Capture never waits for filesystem/SQLite/ASR. About 30 seconds at 10ms/frame.
    let (tx, rx) = mpsc::channel(3000);
    let state = s.clone();
    let id = meeting_id.clone();
    let writer =
        tokio::spawn(
            async move { persist_track(state, id, speaker_id, clock, next_seq, rx).await },
        );
    let mut cursor = None;
    let mut failure = None;
    loop {
        if *stop.borrow() {
            break;
        }
        let frame = tokio::select! {
            biased;
            _ = stop.changed() => break,
            frame=stream.next() => match frame { Some(f)=>f,None=>break },
        };
        if *stop.borrow() {
            break;
        }
        let duration = frame.data.len() as i64 * 1000 / 16000;
        let now = clock.now_ms();
        let mut start = cursor.unwrap_or((now - duration).max(0));
        // Muted tracks / actual dropped frames are gaps, not compressed meeting time.
        if now - start - duration > 250 {
            start = (now - duration).max(0);
        }
        cursor = Some(start + duration);
        if tx
            .try_send(CapturedFrame {
                samples: frame.data.to_vec(),
                start_ms: start,
            })
            .is_err()
        {
            failure=Some("audio persistence queue overflow or writer unavailable; capture stopped to avoid silent data loss".to_string());
            break;
        }
    }
    // Drain already-decoded frames before close(), which clears the SDK queue.
    for _ in 0..200 {
        let Some(Some(frame)) = stream.next().now_or_never() else {
            break;
        };
        let duration = frame.data.len() as i64 * 1000 / INGEST_SAMPLE_RATE as i64;
        let start = cursor.unwrap_or((clock.now_ms() - duration).max(0));
        cursor = Some(start + duration);
        if tx
            .try_send(CapturedFrame {
                samples: frame.data.to_vec(),
                start_ms: start,
            })
            .is_err()
        {
            failure = Some("audio queue overflow while saving tail".into());
            break;
        }
    }
    stream.close();
    drop(tx);
    writer.await.map_err(|e| e.to_string())??;
    info!(meeting_id, identity, "audio track flushed");
    failure.map_or(Ok(()), Err)
}

async fn persist_track(
    s: AppState,
    meeting_id: String,
    speaker_id: String,
    _clock: MeetingClock,
    next_seq: Arc<AtomicI64>,
    mut rx: mpsc::Receiver<CapturedFrame>,
) -> Result<(), String> {
    let mut samples = Vec::new();
    let mut start = 0;
    let mut silence_ms = 0;
    let mut voiced = false;
    let mut session = None;
    let mut id = String::new();
    let mut seq = 0;
    let mut writes = JoinSet::new();
    while let Some(frame) = rx.recv().await {
        // Muting/disconnection must not compress a real gap into one utterance.
        if !samples.is_empty() && frame.start_ms > start + samples.len() as i64 * 1000 / 16000 + 250
        {
            let state = s.clone();
            let meeting = meeting_id.clone();
            let speaker = speaker_id.clone();
            let current = id.clone();
            let chunk = std::mem::take(&mut samples);
            let asr = session.take();
            writes.spawn(async move {
                flush_chunk(
                    &state, &meeting, &speaker, &current, seq, start, chunk, asr, voiced,
                )
                .await
            });
            silence_ms = 0;
            voiced = false;
            if writes.len() >= 4 {
                writes
                    .join_next()
                    .await
                    .unwrap()
                    .map_err(|e| e.to_string())??;
            }
        }
        if samples.is_empty() {
            start = frame.start_ms;
            id = Uuid::new_v4().to_string();
            seq = next_seq.fetch_add(1, Ordering::Relaxed);
        }
        let voice = frame
            .samples
            .iter()
            .map(|v| (*v as f64).powi(2))
            .sum::<f64>()
            / frame.samples.len().max(1) as f64
            > 10000.0;
        if voice && !voiced {
            session = RealtimeSession::start(&s, &meeting_id, &speaker_id, &id, seq, start).await;
            if let Some(asr) = session.as_mut() {
                asr.send(&samples).await;
            }
        }
        voiced |= voice;
        silence_ms = if voice {
            0
        } else {
            silence_ms + frame.samples.len() as i64 * 1000 / 16000
        };
        if let Some(asr) = session.as_mut() {
            asr.send(&frame.samples).await;
        }
        samples.extend(frame.samples);
        let duration = samples.len() as i64 * 1000 / 16000;
        if duration >= ingest_window_ms() || (voiced && silence_ms >= 500 && duration >= 800) {
            let state = s.clone();
            let meeting = meeting_id.clone();
            let speaker = speaker_id.clone();
            let chunk = std::mem::take(&mut samples);
            let current = id.clone();
            let asr = session.take();
            writes.spawn(async move {
                flush_chunk(
                    &state, &meeting, &speaker, &current, seq, start, chunk, asr, voiced,
                )
                .await
            });
            silence_ms = 0;
            voiced = false;
            // Bound final-result waits and outstanding writes without blocking capture.
            if writes.len() >= 4 {
                writes
                    .join_next()
                    .await
                    .unwrap()
                    .map_err(|e| e.to_string())??;
            }
            while let Some(result) = writes.try_join_next() {
                result.map_err(|e| e.to_string())??;
            }
        }
    }
    // Even a short voiced tail is meaningful (e.g. “好”). Never drop it by duration.
    if !samples.is_empty() {
        flush_chunk(
            &s,
            &meeting_id,
            &speaker_id,
            &id,
            seq,
            start,
            samples,
            session,
            voiced,
        )
        .await?;
    }
    while let Some(result) = writes.join_next().await {
        result.map_err(|e| e.to_string())??;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn flush_chunk(
    s: &AppState,
    meeting_id: &str,
    speaker_id: &str,
    id: &str,
    seq: i64,
    start_ms: i64,
    samples: Vec<i16>,
    session: Option<RealtimeSession>,
    voiced: bool,
) -> Result<(), String> {
    let end_ms = start_ms + samples.len() as i64 * 1000 / 16000;
    let dir = s.audio_dir.join(meeting_id);
    tokio::fs::create_dir_all(&dir)
        .await
        .map_err(|e| e.to_string())?;
    let path = dir.join(format!("{id}-live.wav"));
    tokio::fs::write(&path, wav_bytes(&samples, INGEST_SAMPLE_RATE))
        .await
        .map_err(|e| e.to_string())?;
    let transcript = if !voiced {
        Some(String::new())
    } else {
        match session {
            Some(asr) => asr.finish().await,
            None => None,
        }
    };
    // A silent window is archived, but does not spend an ASR or LLM request.
    let result = crate::insert_segment(
        &s.db,
        id,
        meeting_id,
        Some(speaker_id),
        seq,
        start_ms,
        end_ms,
        &path.to_string_lossy(),
        transcript,
    )
    .await;
    if let Err(error) = result {
        let _ = tokio::fs::remove_file(&path).await;
        return Err(error.to_string());
    }
    s.job_notify.notify_waiters();
    let payload = crate::segment_payload(&s.db, meeting_id, id)
        .await
        .map_err(|e| e.to_string())?;
    publish_event(s, meeting_id, "segment.uploaded", payload);
    Ok(())
}

async fn ensure_speaker_identity(
    db: &SqlitePool,
    meeting: &str,
    identity: &str,
    name: &str,
) -> Result<String, sqlx::Error> {
    sqlx::query("INSERT INTO speakers(id,meeting_id,name,participant_identity) VALUES(?,?,?,?) ON CONFLICT(meeting_id,participant_identity) WHERE participant_identity IS NOT NULL DO NOTHING")
        .bind(Uuid::new_v4().to_string()).bind(meeting).bind(name).bind(identity).execute(db).await?;
    sqlx::query_scalar("SELECT id FROM speakers WHERE meeting_id=? AND participant_identity=?")
        .bind(meeting)
        .bind(identity)
        .fetch_one(db)
        .await
}

pub(crate) async fn ensure_speaker_by_name(
    db: &SqlitePool,
    meeting_id: &str,
    name: &str,
) -> Result<String, sqlx::Error> {
    let mut tx = db.begin().await?;
    let existing:Option<String>=sqlx::query_scalar("SELECT id FROM speakers WHERE meeting_id=? AND name=? AND participant_identity IS NULL ORDER BY created_at LIMIT 1")
        .bind(meeting_id).bind(name).fetch_optional(&mut *tx).await?;
    let id = existing.unwrap_or_else(|| Uuid::new_v4().to_string());
    sqlx::query("INSERT OR IGNORE INTO speakers(id,meeting_id,name) VALUES(?,?,?)")
        .bind(&id)
        .bind(meeting_id)
        .bind(name)
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(id)
}
async fn max_sequence_no(db: &SqlitePool, meeting_id: &str) -> i64 {
    sqlx::query_scalar(
        "SELECT COALESCE(MAX(sequence_no),-1) FROM audio_segments WHERE meeting_id=?",
    )
    .bind(meeting_id)
    .fetch_one(db)
    .await
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
    #[tokio::test]
    async fn same_display_name_keeps_distinct_livekit_identities() {
        let db = crate::tests::test_db().await;
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','t','running')")
            .execute(&db)
            .await
            .unwrap();
        let first = ensure_speaker_identity(&db, "m", "user-1", "张三")
            .await
            .unwrap();
        let other = ensure_speaker_identity(&db, "m", "user-2", "张三")
            .await
            .unwrap();
        let reconnect = ensure_speaker_identity(&db, "m", "user-1", "新名字")
            .await
            .unwrap();
        assert_ne!(first, other);
        assert_eq!(first, reconnect);
    }

    #[tokio::test]
    async fn stop_waits_for_persistence_acknowledgement() {
        let db = crate::tests::test_db().await;
        let s = crate::tests::test_state(&db);
        let (stop, mut rx) = watch::channel(false);
        let (done_tx, done) = watch::channel(None);
        s.ingest_stop
            .lock()
            .unwrap()
            .insert("m".into(), IngestControl { stop, done });
        let state = s.clone();
        let mut task = tokio::spawn(async move { stop_ingest(&state, "m").await });
        rx.changed().await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(20), &mut task)
            .await
            .is_err());
        done_tx.send_replace(Some(Ok(())));
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }

    #[tokio::test]
    async fn short_voiced_tail_is_archived_with_its_meeting_timestamp() {
        let db = crate::tests::test_db().await;
        let mut s = crate::tests::test_state(&db);
        sqlx::query("INSERT INTO meetings(id,title,status) VALUES('m','t','running')")
            .execute(&db)
            .await
            .unwrap();
        let speaker = ensure_speaker_identity(&db, "m", "u", "用户")
            .await
            .unwrap();
        let dir = std::env::temp_dir().join(Uuid::new_v4().to_string());
        s.audio_dir = Arc::new(dir.clone());
        let (tx, rx) = mpsc::channel(2);
        tx.send(CapturedFrame {
            samples: vec![1000; 160],
            start_ms: 120000,
        })
        .await
        .unwrap();
        drop(tx);
        persist_track(
            s.clone(),
            "m".into(),
            speaker,
            MeetingClock {
                origin: Instant::now(),
                offset_ms: 120000,
            },
            Arc::new(AtomicI64::new(4)),
            rx,
        )
        .await
        .unwrap();
        let row: (i64, i64, i64) =
            sqlx::query_as("SELECT start_ms,end_ms,sequence_no FROM audio_segments")
                .fetch_one(&db)
                .await
                .unwrap();
        assert_eq!(row, (120000, 120010, 4));
        tokio::fs::remove_dir_all(dir).await.unwrap();
    }
}
