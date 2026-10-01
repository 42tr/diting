//! 会议事件分发：每个会议一个 broadcast 通道，避免一场高频会议（segment.partial）
//! 让其他会议的 SSE 订阅者 lag 而被迫全量补拉。
use serde_json::Value;
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};
use tokio::sync::broadcast;

#[derive(Clone, Debug)]
pub(crate) struct MeetingEvent {
    pub kind: &'static str,
    pub data: Value,
}

#[derive(Clone)]
pub(crate) struct EventHub {
    capacity: usize,
    channels: Arc<Mutex<HashMap<String, broadcast::Sender<MeetingEvent>>>>,
}

impl EventHub {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            channels: Default::default(),
        }
    }

    pub fn subscribe(&self, meeting_id: &str) -> broadcast::Receiver<MeetingEvent> {
        self.channels
            .lock()
            .unwrap()
            .entry(meeting_id.to_owned())
            .or_insert_with(|| broadcast::channel(self.capacity).0)
            .subscribe()
    }

    /// 没有订阅者时直接丢弃，并回收该会议的通道。
    pub fn publish(&self, meeting_id: &str, kind: &'static str, data: Value) {
        let mut channels = self.channels.lock().unwrap();
        let Some(sender) = channels.get(meeting_id) else {
            return;
        };
        let event = MeetingEvent { kind, data };
        if sender.send(event).is_err() {
            channels.remove(meeting_id);
        }
    }

    /// 会议删除后关闭通道，已连接的 SSE 流随之结束。
    pub fn close(&self, meeting_id: &str) {
        self.channels.lock().unwrap().remove(meeting_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn events_are_isolated_per_meeting() {
        let hub = EventHub::new(4);
        let mut a = hub.subscribe("a");
        let mut b = hub.subscribe("b");
        for _ in 0..10 {
            hub.publish("a", "segment.partial", json!({}));
        }
        hub.publish("b", "segment.uploaded", json!({}));
        assert!(matches!(
            a.try_recv(),
            Err(broadcast::error::TryRecvError::Lagged(_))
        ));
        assert_eq!(b.try_recv().unwrap().kind, "segment.uploaded");
    }

    #[test]
    fn channel_is_dropped_without_subscribers() {
        let hub = EventHub::new(4);
        drop(hub.subscribe("a"));
        hub.publish("a", "x", json!({}));
        assert!(hub.channels.lock().unwrap().is_empty());
    }
}
