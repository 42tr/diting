//! E2E helper: join a LiveKit room and publish a sine-wave audio track.
//! Usage: cargo run --example lk_pub -- <ws_url> <token> [seconds]

use std::borrow::Cow;
use std::time::Duration;

use livekit::options::TrackPublishOptions;
use livekit::prelude::*;
use livekit::webrtc::audio_source::native::NativeAudioSource;
use livekit::webrtc::audio_source::{AudioSourceOptions, RtcAudioSource};
use livekit::webrtc::prelude::AudioFrame;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::args().nth(1).expect("ws url");
    let token = std::env::args().nth(2).expect("token");
    let secs: u64 = std::env::args().nth(3).unwrap_or_else(|| "25".into()).parse()?;

    let (room, mut events) = Room::connect(&url, &token, RoomOptions::default()).await?;
    println!("connected: {}", room.name());

    let source = NativeAudioSource::new(AudioSourceOptions::default(), 16000, 1, 100);
    let track = LocalAudioTrack::create_audio_track("mic", RtcAudioSource::Native(source.clone()));
    room.local_participant()
        .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
        .await?;
    println!("published audio track");

    // 10ms frames @16kHz mono = 160 samples; alternating tones to look like speech energy
    let frame_samples = 160usize;
    let total_frames = (secs * 100) as usize;
    for i in 0..total_frames {
        let freq = if (i / 100) % 2 == 0 { 440.0 } else { 550.0 };
        let data: Vec<i16> = (0..frame_samples)
            .map(|n| {
                let t = (i * frame_samples + n) as f32 / 16000.0;
                (f32::sin(2.0 * std::f32::consts::PI * freq * t) * 12000.0) as i16
            })
            .collect();
        source
            .capture_frame(&AudioFrame {
                data: Cow::Borrowed(&data),
                sample_rate: 16000,
                num_channels: 1,
                samples_per_channel: frame_samples as u32,
            })
            .await?;
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    println!("done publishing");
    // drain events briefly so tracks unpublish cleanly
    room.close().await?;
    let _ = events.recv().await;
    Ok(())
}
