#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use tokio::sync::{Mutex, Notify, Semaphore};
use tokio::time::timeout;

use supabase_realtime_rs::{
    protocol_objects::{Broadcast, BroadcastConfig, JoinConfig, Payload, PresenceConfig},
    types::{Result, SubscribeState},
};

mod common;
use common::create_client;

#[cfg(test)]
mod tests {
    use super::*;

    const BROADCAST_JOIN_CONFIG: JoinConfig = JoinConfig {
        broadcast: Some(BroadcastConfig {
            self_item: true,
            ack: false,
        }),
        presence: Some(PresenceConfig { key: String::new() }),
        postgres_changes: None,
        private: false,
    };

    #[tokio::test]
    async fn test_broadcast_events() {
        let mut client = create_client().expect("Error while creating client.");
        client
            .connect()
            .await
            .expect("Error while connecting to client.");
        assert!(client.is_connected());

        let topic = String::from("test-broadcast");
        let mut channel = client
            .create_channel(&topic, Some(BROADCAST_JOIN_CONFIG))
            .await;

        let received_events = Arc::new(Mutex::new(Vec::<Payload>::new()));
        let semaphore = Arc::new(Semaphore::new(0));
        let subscribe_notify = Arc::new(Notify::new());

        let events_clone = Arc::clone(&received_events);
        let semaphore_clone = Arc::clone(&semaphore);

        let broadcast_callback = move |payload: Payload| {
            events_clone
                .try_lock()
                .expect("Failed to get lock on events clone.")
                .push(payload);
            semaphore_clone.add_permits(1);
        };

        let notify_clone = Arc::clone(&subscribe_notify);
        let subscribe_callback = move |state_result: Result<SubscribeState>| {
            if let Ok(state) = state_result
                && state == SubscribeState::Subscribed
            {
                notify_clone.notify_one();
            }
        };

        channel
            .on_broadcast("test-event", Box::new(broadcast_callback))
            .await;

        channel
            .subscribe(&mut client, Some(Box::new(subscribe_callback)))
            .await
            .expect("Error while subscribing to channel.");

        timeout(Duration::from_secs(5), subscribe_notify.notified())
            .await
            .expect("Timeout elapsed while waiting for subscribe response.");

        for i in 1..4 {
            let payload = Payload::Broadcast(Broadcast {
                event: String::from("test-event"),
                payload: json!({"message": format!("Event {i}")}),
            });

            channel
                .send_broadcast(&mut client, "test-event", payload)
                .await
                .expect("Error while sending broadcast.");

            let permit = timeout(Duration::from_secs(5), semaphore.acquire())
                .await
                .expect("Timeout elapsed while waiting for broadcast response.")
                .unwrap();
            permit.forget();
        }

        let broadcast_events: Vec<Broadcast> = received_events
            .lock()
            .await
            .iter()
            .flat_map(|e| match e {
                Payload::Broadcast(broadcast) => Some(broadcast.clone()),
                _ => None,
            })
            .collect();

        assert_eq!(broadcast_events.len(), 3);

        assert_eq!(broadcast_events[0].payload["message"], "Event 1");
        assert_eq!(broadcast_events[1].payload["message"], "Event 2");
        assert_eq!(broadcast_events[2].payload["message"], "Event 3");

        client.close().await.expect("Error disconnecting client.");
    }

    #[tokio::test]
    async fn test_delayed_single_broadcast() {
        let mut client = create_client().expect("Error while creating client.");
        client.connect().await.unwrap();
        assert!(client.is_connected());

        let topic = String::from("test-delayed-broadcast");
        let mut channel = client
            .create_channel(&topic, Some(BROADCAST_JOIN_CONFIG))
            .await;

        let received_events = Arc::new(Mutex::new(Vec::<Payload>::new()));
        let semaphore = Arc::new(Semaphore::new(0));
        let subscribe_notify = Arc::new(Notify::new());

        let events_clone = Arc::clone(&received_events);
        let semaphore_clone = Arc::clone(&semaphore);

        let broadcast_callback = move |payload: Payload| {
            events_clone
                .try_lock()
                .expect("Failed to get lock on events clone.")
                .push(payload);
            semaphore_clone.add_permits(1);
        };

        let notify_clone = Arc::clone(&subscribe_notify);
        let subscribe_callback = move |state_result: Result<SubscribeState>| {
            if let Ok(state) = state_result
                && state == SubscribeState::Subscribed
            {
                notify_clone.notify_one();
            }
        };

        channel
            .on_broadcast("test-event", Box::new(broadcast_callback))
            .await;

        channel
            .subscribe(&mut client, Some(Box::new(subscribe_callback)))
            .await
            .expect("Error while subscribing to channel.");

        timeout(Duration::from_secs(5), subscribe_notify.notified())
            .await
            .expect("Timeout elapsed while waiting for subscribe response.");

        // Wait 30 seconds after connecting before sending the broadcast
        tokio::time::sleep(Duration::from_secs(30)).await;

        let payload = Payload::Broadcast(Broadcast {
            event: String::from("test-event"),
            payload: json!({"message": "Delayed broadcast message"}),
        });

        channel
            .send_broadcast(&mut client, "test-event", payload)
            .await
            .expect("Error while sending broadcast.");

        let permit = timeout(Duration::from_secs(5), semaphore.acquire())
            .await
            .expect("Timeout elapsed while waiting for broadcast response.")
            .unwrap();
        permit.forget();

        let broadcast_events: Vec<Broadcast> = received_events
            .lock()
            .await
            .iter()
            .flat_map(|e| match e {
                Payload::Broadcast(broadcast) => Some(broadcast.clone()),
                _ => None,
            })
            .collect();

        assert_eq!(broadcast_events.len(), 1);
        assert_eq!(
            broadcast_events[0].payload["message"],
            "Delayed broadcast message"
        );

        client.close().await.expect("Error disconnecting client.");
    }
}
