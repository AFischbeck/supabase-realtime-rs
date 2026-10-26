#![cfg(not(target_arch = "wasm32"))]

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{Mutex, Notify};
use tokio::time::timeout;

use supabase_realtime_rs::{
    channel::RealtimeChannel,
    client::RealtimeClient,
    protocol_objects::{BroadcastConfig, JoinConfig, Presence, PresenceConfig, PresenceState},
    types::{Result, SubscribeState},
};

mod common;
use common::create_client;

#[cfg(test)]
mod tests {
    use super::*;

    const PRESENCE_TIMEOUT: Duration = Duration::from_secs(10);

    type UserPresenceRef = (String, String);

    #[derive(Debug, PartialEq, Eq)]
    struct PresenceEvent {
        key: String,
        affected: Vec<UserPresenceRef>,
        current: Vec<UserPresenceRef>,
    }

    fn unique_topic(base: &str) -> String {
        static TOPIC_COUNTER: AtomicUsize = AtomicUsize::new(0);

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();

        format!(
            "{base}-{}-{nanos}-{}",
            std::process::id(),
            TOPIC_COUNTER.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn presence_join_config(key: &str) -> JoinConfig {
        JoinConfig {
            broadcast: Some(BroadcastConfig {
                self_item: true,
                ack: false,
            }),
            presence: Some(PresenceConfig {
                key: String::from(key),
                enabled: true,
            }),
            postgres_changes: None,
            private: false,
        }
    }

    fn presence_user_ids(state: &PresenceState) -> Vec<String> {
        let mut user_ids: Vec<String> = state
            .0
            .values()
            .flat_map(|presence| presence.metas.iter())
            .filter_map(|meta| {
                meta.extra
                    .get("user_id")
                    .and_then(|value| value.as_str())
                    .map(String::from)
            })
            .collect();

        user_ids.sort();
        user_ids
    }

    fn user_presence_refs(presences: &[Presence]) -> Vec<UserPresenceRef> {
        let mut refs: Vec<UserPresenceRef> = presences
            .iter()
            .flat_map(|presence| presence.metas.iter())
            .map(|meta| {
                let user_id = meta
                    .extra
                    .get("user_id")
                    .and_then(|value| value.as_str())
                    .expect("Presence meta must contain a string user_id.")
                    .to_owned();

                assert!(
                    !meta.phx_ref.is_empty(),
                    "Presence meta must contain a phx_ref."
                );

                (user_id, meta.phx_ref.clone())
            })
            .collect();

        refs.sort();
        refs
    }

    fn state_user_presence_refs(state: &PresenceState) -> Vec<UserPresenceRef> {
        let presences: Vec<Presence> = state.0.values().cloned().collect();
        user_presence_refs(&presences)
    }

    fn presence_key_for_user(state: &PresenceState, user_id: &str) -> Option<String> {
        state.0.iter().find_map(|(key, presence)| {
            presence
                .metas
                .iter()
                .any(|meta| {
                    meta.extra.get("user_id").and_then(|value| value.as_str()) == Some(user_id)
                })
                .then(|| key.clone())
        })
    }

    async fn wait_for_syncs(
        sync_count: &AtomicUsize,
        target: usize,
        sync_notify: &Notify,
        label: &str,
    ) {
        timeout(PRESENCE_TIMEOUT, async {
            loop {
                if sync_count.load(Ordering::SeqCst) >= target {
                    break;
                }

                sync_notify.notified().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("Timeout elapsed while waiting for {label}."));
    }

    async fn wait_for_presence_state<F>(
        channel: &RealtimeChannel,
        sync_notify: &Notify,
        label: &str,
        predicate: F,
    ) -> PresenceState
    where
        F: Fn(&PresenceState) -> bool,
    {
        timeout(PRESENCE_TIMEOUT, async {
            loop {
                let state = channel.presence_state().await;

                if predicate(&state) {
                    return state;
                }

                sync_notify.notified().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("Timeout elapsed while waiting for {label}."))
    }

    async fn subscribe_and_wait(client: &mut RealtimeClient, channel: &mut RealtimeChannel) {
        let subscribe_notify = Arc::new(Notify::new());
        let subscribe_failure = Arc::new(Mutex::new(None::<String>));

        let notify_clone = Arc::clone(&subscribe_notify);
        let failure_clone = Arc::clone(&subscribe_failure);
        let subscribe_callback = move |state_result: Result<SubscribeState>| {
            let failure = match state_result {
                Ok(SubscribeState::Subscribed) => None,
                Ok(state) => Some(format!("Unexpected subscribe state: {state:?}.")),
                Err(error) => Some(format!("Subscribe error: {error}.")),
            };

            if let Some(failure) = failure {
                *failure_clone
                    .try_lock()
                    .expect("Failed to lock subscribe failure state.") = Some(failure);
            }

            notify_clone.notify_one();
        };

        channel
            .subscribe(client, Some(Box::new(subscribe_callback)))
            .await
            .expect("Error while subscribing to channel.");

        timeout(PRESENCE_TIMEOUT, subscribe_notify.notified())
            .await
            .expect("Timeout elapsed while waiting for subscribe response.");

        if let Some(failure) = subscribe_failure.lock().await.take() {
            panic!("{failure}");
        }
    }

    #[tokio::test]
    async fn test_presence_track_and_sync() {
        let mut client = create_client().expect("Error while creating client.");
        client
            .connect()
            .await
            .expect("Error while connecting to client.");
        assert!(client.is_connected());

        let topic = unique_topic("test-presence-track");
        let mut channel = client
            .create_channel(&topic, Some(presence_join_config("alice")))
            .await;

        let sync_count = Arc::new(AtomicUsize::new(0));
        let sync_notify = Arc::new(Notify::new());

        let sync_count_clone = Arc::clone(&sync_count);
        let sync_notify_clone = Arc::clone(&sync_notify);
        channel
            .on_presence_sync(move || {
                sync_count_clone.fetch_add(1, Ordering::SeqCst);
                sync_notify_clone.notify_one();
            })
            .await;

        subscribe_and_wait(&mut client, &mut channel).await;
        wait_for_syncs(&sync_count, 1, &sync_notify, "initial presence sync").await;
        assert!(channel.presence_state().await.0.is_empty());

        channel
            .track(&client, "123".into())
            .await
            .expect("Error while tracking presence.");

        let presence_state =
            wait_for_presence_state(&channel, &sync_notify, "presence for user 123", |state| {
                presence_user_ids(state) == vec![String::from("123")]
            })
            .await;

        assert_eq!(presence_state.0.len(), 1);

        let (key, presence) = presence_state
            .0
            .iter()
            .next()
            .expect("Expected one presence key.");
        assert_eq!(key, "alice");
        assert_eq!(presence.metas.len(), 1);
        assert_eq!(
            presence.metas[0]
                .extra
                .get("user_id")
                .and_then(|value| value.as_str()),
            Some("123")
        );
        assert!(!presence.metas[0].phx_ref.is_empty());

        channel
            .untrack(&client)
            .await
            .expect("Error while untracking presence.");

        let presence_state = wait_for_presence_state(
            &channel,
            &sync_notify,
            "empty presence state after untrack",
            |state| state.0.is_empty(),
        )
        .await;
        assert!(presence_state.0.is_empty());

        client.close().await.expect("Error disconnecting client.");
    }

    #[tokio::test]
    async fn test_presence_track_update() {
        let mut client = create_client().expect("Error while creating client.");
        client
            .connect()
            .await
            .expect("Error while connecting to client.");
        assert!(client.is_connected());

        let topic = unique_topic("test-presence-track-update");
        let mut channel = client
            .create_channel(&topic, Some(presence_join_config("carol")))
            .await;

        let sync_count = Arc::new(AtomicUsize::new(0));
        let sync_notify = Arc::new(Notify::new());

        let sync_count_clone = Arc::clone(&sync_count);
        let sync_notify_clone = Arc::clone(&sync_notify);
        channel
            .on_presence_sync(move || {
                sync_count_clone.fetch_add(1, Ordering::SeqCst);
                sync_notify_clone.notify_one();
            })
            .await;

        subscribe_and_wait(&mut client, &mut channel).await;
        wait_for_syncs(&sync_count, 1, &sync_notify, "initial presence sync").await;
        assert!(channel.presence_state().await.0.is_empty());

        channel
            .track(&client, "123".into())
            .await
            .expect("Error while tracking presence.");

        let presence_state =
            wait_for_presence_state(&channel, &sync_notify, "presence for user 123", |state| {
                presence_user_ids(state) == vec![String::from("123")]
            })
            .await;

        assert_eq!(presence_state.0.len(), 1);
        assert_eq!(
            presence_key_for_user(&presence_state, "123").as_deref(),
            Some("carol")
        );

        channel
            .track(&client, "456".into())
            .await
            .expect("Error while updating presence.");

        let presence_state = wait_for_presence_state(
            &channel,
            &sync_notify,
            "updated presence for user 456 while keeping its channel key",
            |state| presence_user_ids(state) == vec![String::from("456")],
        )
        .await;

        assert_eq!(
            presence_state.0.len(),
            1,
            "An updated presence must keep its channel key."
        );
        assert_eq!(
            presence_key_for_user(&presence_state, "456").as_deref(),
            Some("carol")
        );
        assert_eq!(
            presence_user_ids(&presence_state),
            vec![String::from("456")]
        );

        channel
            .untrack(&client)
            .await
            .expect("Error while untracking presence.");

        let presence_state = wait_for_presence_state(
            &channel,
            &sync_notify,
            "empty presence state after untrack",
            |state| state.0.is_empty(),
        )
        .await;
        assert!(presence_state.0.is_empty());

        client.close().await.expect("Error disconnecting client.");
    }

    #[tokio::test]
    async fn test_presence_update_callbacks() {
        let mut client = create_client().expect("Error while creating client.");
        client
            .connect()
            .await
            .expect("Error while connecting to client.");
        assert!(client.is_connected());

        let topic = unique_topic("test-presence-update-callbacks");
        let mut channel = client
            .create_channel(&topic, Some(presence_join_config("carol")))
            .await;

        let join_events = Arc::new(Mutex::new(Vec::<PresenceEvent>::new()));
        let leave_events = Arc::new(Mutex::new(Vec::<PresenceEvent>::new()));
        let join_count = Arc::new(AtomicUsize::new(0));
        let leave_count = Arc::new(AtomicUsize::new(0));
        let callback_notify = Arc::new(Notify::new());

        let join_events_clone = Arc::clone(&join_events);
        let join_count_clone = Arc::clone(&join_count);
        let callback_notify_clone = Arc::clone(&callback_notify);
        channel
            .on_presence_join(
                move |key: String, current: Vec<Presence>, new: Vec<Presence>| {
                    join_events_clone
                        .try_lock()
                        .expect("Failed to lock join events.")
                        .push(PresenceEvent {
                            key,
                            affected: user_presence_refs(&new),
                            current: user_presence_refs(&current),
                        });
                    join_count_clone.fetch_add(1, Ordering::SeqCst);
                    callback_notify_clone.notify_one();
                },
            )
            .await;

        let leave_events_clone = Arc::clone(&leave_events);
        let leave_count_clone = Arc::clone(&leave_count);
        let callback_notify_clone = Arc::clone(&callback_notify);
        channel
            .on_presence_leave(
                move |key: String, current: Vec<Presence>, left: Vec<Presence>| {
                    leave_events_clone
                        .try_lock()
                        .expect("Failed to lock leave events.")
                        .push(PresenceEvent {
                            key,
                            affected: user_presence_refs(&left),
                            current: user_presence_refs(&current),
                        });
                    leave_count_clone.fetch_add(1, Ordering::SeqCst);
                    callback_notify_clone.notify_one();
                },
            )
            .await;

        subscribe_and_wait(&mut client, &mut channel).await;

        channel
            .track(&client, "123".into())
            .await
            .expect("Error while tracking presence.");

        timeout(PRESENCE_TIMEOUT, async {
            loop {
                if join_count.load(Ordering::SeqCst) >= 1 {
                    break;
                }

                callback_notify.notified().await;
            }
        })
        .await
        .expect("Timeout elapsed while waiting for the initial presence join callback.");

        let old_user_presence_ref = {
            let join_events = join_events.lock().await;
            assert_eq!(join_events.len(), 1);
            assert_eq!(join_events[0].key, "carol");
            assert_eq!(join_events[0].affected.len(), 1);
            assert_eq!(join_events[0].affected[0].0, "123");
            assert!(!join_events[0].affected[0].1.is_empty());
            join_events[0].affected[0].clone()
        };

        channel
            .track(&client, "456".into())
            .await
            .expect("Error while updating presence.");

        timeout(PRESENCE_TIMEOUT, async {
            loop {
                if join_count.load(Ordering::SeqCst) >= 2 && leave_count.load(Ordering::SeqCst) >= 1
                {
                    break;
                }

                callback_notify.notified().await;
            }
        })
        .await
        .expect("Timeout elapsed while waiting for update presence callbacks.");

        // `current` is not asserted for either update callback because the
        // known update-state failure also empties it.
        {
            let join_events = join_events.lock().await;
            assert_eq!(join_events.len(), 2);
            assert_eq!(join_events[1].key, "carol");
            assert_eq!(join_events[1].affected.len(), 1);
            assert_eq!(join_events[1].affected[0].0, "456");
            assert!(!join_events[1].affected[0].1.is_empty());
        }

        {
            let leave_events = leave_events.lock().await;
            assert_eq!(leave_events.len(), 1);
            assert_eq!(leave_events[0].key, "carol");
            assert_eq!(
                leave_events[0].affected,
                vec![old_user_presence_ref.clone()]
            );
        }

        client.close().await.expect("Error disconnecting client.");
    }

    #[tokio::test]
    async fn test_presence_join_and_leave() {
        let mut client = create_client().expect("Error while creating client.");
        client
            .connect()
            .await
            .expect("Error while connecting to client.");
        assert!(client.is_connected());

        let topic = unique_topic("test-presence-join-leave");
        let mut channel = client
            .create_channel(&topic, Some(presence_join_config("bob")))
            .await;

        let join_events = Arc::new(Mutex::new(Vec::<PresenceEvent>::new()));
        let leave_events = Arc::new(Mutex::new(Vec::<PresenceEvent>::new()));
        let sync_count = Arc::new(AtomicUsize::new(0));
        let sync_notify = Arc::new(Notify::new());

        let join_events_clone = Arc::clone(&join_events);
        channel
            .on_presence_join(
                move |key: String, current: Vec<Presence>, new: Vec<Presence>| {
                    join_events_clone
                        .try_lock()
                        .expect("Failed to lock join events.")
                        .push(PresenceEvent {
                            key,
                            affected: user_presence_refs(&new),
                            current: user_presence_refs(&current),
                        });
                },
            )
            .await;

        let leave_events_clone = Arc::clone(&leave_events);
        channel
            .on_presence_leave(
                move |key: String, current: Vec<Presence>, left: Vec<Presence>| {
                    leave_events_clone
                        .try_lock()
                        .expect("Failed to lock leave events.")
                        .push(PresenceEvent {
                            key,
                            affected: user_presence_refs(&left),
                            current: user_presence_refs(&current),
                        });
                },
            )
            .await;

        let sync_count_clone = Arc::clone(&sync_count);
        let sync_notify_clone = Arc::clone(&sync_notify);
        channel
            .on_presence_sync(move || {
                sync_count_clone.fetch_add(1, Ordering::SeqCst);
                sync_notify_clone.notify_one();
            })
            .await;

        subscribe_and_wait(&mut client, &mut channel).await;
        wait_for_syncs(&sync_count, 1, &sync_notify, "initial presence sync").await;
        assert!(channel.presence_state().await.0.is_empty());

        channel
            .track(&client, "456".into())
            .await
            .expect("Error while tracking presence.");

        let presence_state =
            wait_for_presence_state(&channel, &sync_notify, "presence for user 456", |state| {
                presence_user_ids(state) == vec![String::from("456")]
            })
            .await;

        assert_eq!(presence_state.0.len(), 1);
        assert_eq!(
            presence_key_for_user(&presence_state, "456").as_deref(),
            Some("bob")
        );

        let presence = presence_state
            .0
            .get("bob")
            .expect("Expected presence for key bob.");
        assert_eq!(presence.metas.len(), 1);
        let user_presence_ref = (String::from("456"), presence.metas[0].phx_ref.clone());

        {
            let join_events = join_events.lock().await;
            assert_eq!(join_events.len(), 1);
            assert_eq!(
                join_events[0],
                PresenceEvent {
                    key: String::from("bob"),
                    affected: vec![user_presence_ref.clone()],
                    current: vec![user_presence_ref.clone()],
                }
            );
        }

        channel
            .untrack(&client)
            .await
            .expect("Error while untracking presence.");

        let presence_state = wait_for_presence_state(
            &channel,
            &sync_notify,
            "empty presence state after untrack",
            |state| state.0.is_empty(),
        )
        .await;
        assert!(presence_state.0.is_empty());

        {
            let leave_events = leave_events.lock().await;
            assert_eq!(leave_events.len(), 1);
            assert_eq!(
                leave_events[0],
                PresenceEvent {
                    key: String::from("bob"),
                    affected: vec![user_presence_ref.clone()],
                    current: Vec::new(),
                }
            );
        }

        client.close().await.expect("Error disconnecting client.");
    }

    #[tokio::test]
    async fn test_presence_untrack() {
        let mut client = create_client().expect("Error while creating client.");
        client
            .connect()
            .await
            .expect("Error while connecting to client.");
        assert!(client.is_connected());

        let topic = unique_topic("test-presence-untrack");
        let mut channel = client
            .create_channel(&topic, Some(presence_join_config("")))
            .await;

        let sync_count = Arc::new(AtomicUsize::new(0));
        let sync_notify = Arc::new(Notify::new());

        let sync_count_clone = Arc::clone(&sync_count);
        let sync_notify_clone = Arc::clone(&sync_notify);
        channel
            .on_presence_sync(move || {
                sync_count_clone.fetch_add(1, Ordering::SeqCst);
                sync_notify_clone.notify_one();
            })
            .await;

        subscribe_and_wait(&mut client, &mut channel).await;
        wait_for_syncs(&sync_count, 1, &sync_notify, "initial presence sync").await;
        assert!(channel.presence_state().await.0.is_empty());

        channel
            .track(&client, "789".into())
            .await
            .expect("Error while tracking presence.");

        let presence_state =
            wait_for_presence_state(&channel, &sync_notify, "presence for user 789", |state| {
                presence_user_ids(state) == vec![String::from("789")]
            })
            .await;

        assert_eq!(presence_state.0.len(), 1);

        let (key, presence) = presence_state
            .0
            .iter()
            .next()
            .expect("Expected one presence entry.");
        assert!(!key.is_empty());
        assert_eq!(presence.metas.len(), 1);
        assert!(!presence.metas[0].phx_ref.is_empty());

        channel
            .untrack(&client)
            .await
            .expect("Error while untracking presence.");

        let presence_state = wait_for_presence_state(
            &channel,
            &sync_notify,
            "empty presence state after untrack",
            |state| state.0.is_empty(),
        )
        .await;
        assert!(presence_state.0.is_empty());

        channel
            .track(&client, "790".into())
            .await
            .expect("Error while tracking presence again.");

        let presence_state = wait_for_presence_state(
            &channel,
            &sync_notify,
            "presence for user 790 after retracking",
            |state| presence_user_ids(state) == vec![String::from("790")],
        )
        .await;

        assert_eq!(presence_state.0.len(), 1);

        let (key_after_retrack, _) = presence_state
            .0
            .iter()
            .next()
            .expect("Expected one presence entry after retracking.");
        assert!(!key_after_retrack.is_empty());

        channel
            .untrack(&client)
            .await
            .expect("Error while untracking presence again.");

        let presence_state = wait_for_presence_state(
            &channel,
            &sync_notify,
            "empty presence state after second untrack",
            |state| state.0.is_empty(),
        )
        .await;
        assert!(presence_state.0.is_empty());

        client.close().await.expect("Error disconnecting client.");
    }

    #[tokio::test]
    async fn test_presence_multiple_clients() {
        let mut client_a = create_client().expect("Error while creating client A.");
        client_a
            .connect()
            .await
            .expect("Error while connecting client A.");
        assert!(client_a.is_connected());

        let mut client_b = create_client().expect("Error while creating client B.");
        client_b
            .connect()
            .await
            .expect("Error while connecting client B.");
        assert!(client_b.is_connected());

        let topic = unique_topic("test-presence-multiple-clients");
        let mut channel_a = client_a
            .create_channel(&topic, Some(presence_join_config("")))
            .await;
        let mut channel_b = client_b
            .create_channel(&topic, Some(presence_join_config("")))
            .await;

        let join_events_a = Arc::new(Mutex::new(Vec::<PresenceEvent>::new()));
        let leave_events_a = Arc::new(Mutex::new(Vec::<PresenceEvent>::new()));
        let sync_count_a = Arc::new(AtomicUsize::new(0));
        let sync_notify_a = Arc::new(Notify::new());

        let join_events_a_clone = Arc::clone(&join_events_a);
        channel_a
            .on_presence_join(
                move |key: String, current: Vec<Presence>, new: Vec<Presence>| {
                    join_events_a_clone
                        .try_lock()
                        .expect("Failed to lock client A join events.")
                        .push(PresenceEvent {
                            key,
                            affected: user_presence_refs(&new),
                            current: user_presence_refs(&current),
                        });
                },
            )
            .await;

        let leave_events_a_clone = Arc::clone(&leave_events_a);
        channel_a
            .on_presence_leave(
                move |key: String, current: Vec<Presence>, left: Vec<Presence>| {
                    leave_events_a_clone
                        .try_lock()
                        .expect("Failed to lock client A leave events.")
                        .push(PresenceEvent {
                            key,
                            affected: user_presence_refs(&left),
                            current: user_presence_refs(&current),
                        });
                },
            )
            .await;

        let sync_count_a_clone = Arc::clone(&sync_count_a);
        let sync_notify_a_clone = Arc::clone(&sync_notify_a);
        channel_a
            .on_presence_sync(move || {
                sync_count_a_clone.fetch_add(1, Ordering::SeqCst);
                sync_notify_a_clone.notify_one();
            })
            .await;

        let sync_count_b = Arc::new(AtomicUsize::new(0));
        let sync_notify_b = Arc::new(Notify::new());

        let sync_count_b_clone = Arc::clone(&sync_count_b);
        let sync_notify_b_clone = Arc::clone(&sync_notify_b);
        channel_b
            .on_presence_sync(move || {
                sync_count_b_clone.fetch_add(1, Ordering::SeqCst);
                sync_notify_b_clone.notify_one();
            })
            .await;

        subscribe_and_wait(&mut client_a, &mut channel_a).await;
        subscribe_and_wait(&mut client_b, &mut channel_b).await;

        wait_for_syncs(
            &sync_count_a,
            1,
            &sync_notify_a,
            "client A initial presence sync",
        )
        .await;
        wait_for_syncs(
            &sync_count_b,
            1,
            &sync_notify_b,
            "client B initial presence sync",
        )
        .await;

        assert!(channel_a.presence_state().await.0.is_empty());
        assert!(channel_b.presence_state().await.0.is_empty());

        channel_a
            .track(&client_a, "1".into())
            .await
            .expect("Error while tracking presence from client A.");

        let presence_state_a = wait_for_presence_state(
            &channel_a,
            &sync_notify_a,
            "client A presence for user 1",
            |state| presence_user_ids(state) == vec![String::from("1")],
        )
        .await;
        let presence_state_b = wait_for_presence_state(
            &channel_b,
            &sync_notify_b,
            "client B presence for user 1",
            |state| presence_user_ids(state) == vec![String::from("1")],
        )
        .await;

        assert_eq!(presence_state_a.0.len(), 1);
        assert_eq!(presence_state_b.0.len(), 1);

        let user_1_key = presence_key_for_user(&presence_state_a, "1")
            .expect("Expected presence key for user 1.");
        assert!(!user_1_key.is_empty());
        assert_eq!(
            presence_key_for_user(&presence_state_b, "1").as_deref(),
            Some(user_1_key.as_str())
        );

        let refs_after_user_1 = state_user_presence_refs(&presence_state_a);
        assert_eq!(
            refs_after_user_1,
            state_user_presence_refs(&presence_state_b)
        );
        assert_eq!(refs_after_user_1.len(), 1);
        assert_eq!(refs_after_user_1[0].0, "1");
        let user_1_ref = refs_after_user_1[0].1.clone();

        {
            let join_events = join_events_a.lock().await;
            assert_eq!(join_events.len(), 1);
            assert_eq!(
                join_events[0],
                PresenceEvent {
                    key: user_1_key.clone(),
                    affected: refs_after_user_1.clone(),
                    current: refs_after_user_1.clone(),
                }
            );
        }

        channel_b
            .track(&client_b, "2".into())
            .await
            .expect("Error while tracking presence from client B.");

        let presence_state_a = wait_for_presence_state(
            &channel_a,
            &sync_notify_a,
            "client A presence for users 1 and 2",
            |state| presence_user_ids(state) == vec![String::from("1"), String::from("2")],
        )
        .await;
        let presence_state_b = wait_for_presence_state(
            &channel_b,
            &sync_notify_b,
            "client B presence for users 1 and 2",
            |state| presence_user_ids(state) == vec![String::from("1"), String::from("2")],
        )
        .await;

        assert_eq!(presence_state_a.0.len(), 2);
        assert_eq!(presence_state_b.0.len(), 2);

        let user_2_key = presence_key_for_user(&presence_state_a, "2")
            .expect("Expected presence key for user 2.");
        assert!(!user_2_key.is_empty());
        assert_ne!(user_2_key, user_1_key);
        assert_eq!(
            presence_key_for_user(&presence_state_b, "2").as_deref(),
            Some(user_2_key.as_str())
        );

        let mut keys_a: Vec<String> = presence_state_a.0.keys().cloned().collect();
        let mut keys_b: Vec<String> = presence_state_b.0.keys().cloned().collect();
        keys_a.sort();
        keys_b.sort();
        assert_eq!(keys_a, keys_b);

        let refs_after_user_2 = state_user_presence_refs(&presence_state_a);
        assert_eq!(
            refs_after_user_2,
            state_user_presence_refs(&presence_state_b)
        );
        assert_eq!(refs_after_user_2.len(), 2);
        assert_eq!(refs_after_user_2[0], refs_after_user_1[0]);
        let user_2_ref = refs_after_user_2
            .iter()
            .find(|(user_id, _)| user_id == "2")
            .map(|(_, presence_ref)| presence_ref.clone())
            .expect("Expected presence ref for user 2.");

        {
            let join_events = join_events_a.lock().await;
            assert_eq!(join_events.len(), 2);
            assert_eq!(
                join_events[1],
                PresenceEvent {
                    key: user_2_key.clone(),
                    affected: vec![(String::from("2"), user_2_ref.clone())],
                    current: refs_after_user_2.clone(),
                }
            );
        }

        channel_b
            .untrack(&client_b)
            .await
            .expect("Error while untracking presence from client B.");

        let presence_state_a = wait_for_presence_state(
            &channel_a,
            &sync_notify_a,
            "client A presence for user 1 after client B untracked",
            |state| presence_user_ids(state) == vec![String::from("1")],
        )
        .await;
        let presence_state_b = wait_for_presence_state(
            &channel_b,
            &sync_notify_b,
            "client B presence for user 1 after untracking",
            |state| presence_user_ids(state) == vec![String::from("1")],
        )
        .await;

        assert_eq!(presence_state_a.0.len(), 1);
        assert_eq!(presence_state_b.0.len(), 1);

        let refs_after_user_2_untrack = state_user_presence_refs(&presence_state_a);
        assert_eq!(
            refs_after_user_2_untrack,
            state_user_presence_refs(&presence_state_b)
        );
        assert_eq!(refs_after_user_2_untrack, refs_after_user_1);

        {
            let leave_events = leave_events_a.lock().await;
            assert_eq!(leave_events.len(), 1);
            assert_eq!(
                leave_events[0],
                PresenceEvent {
                    key: user_2_key.clone(),
                    affected: vec![(String::from("2"), user_2_ref.clone())],
                    current: refs_after_user_2_untrack.clone(),
                }
            );
        }

        channel_a
            .untrack(&client_a)
            .await
            .expect("Error while untracking presence from client A.");

        let presence_state_a = wait_for_presence_state(
            &channel_a,
            &sync_notify_a,
            "client A empty presence state after untrack",
            |state| state.0.is_empty(),
        )
        .await;
        let presence_state_b = wait_for_presence_state(
            &channel_b,
            &sync_notify_b,
            "client B empty presence state after client A untracked",
            |state| state.0.is_empty(),
        )
        .await;

        assert!(presence_state_a.0.is_empty());
        assert!(presence_state_b.0.is_empty());

        {
            let leave_events = leave_events_a.lock().await;
            assert_eq!(leave_events.len(), 2);
            assert_eq!(
                leave_events[1],
                PresenceEvent {
                    key: user_1_key.clone(),
                    affected: vec![(String::from("1"), user_1_ref.clone())],
                    current: Vec::new(),
                }
            );
        }

        client_b
            .close()
            .await
            .expect("Error disconnecting client B.");
        client_a
            .close()
            .await
            .expect("Error disconnecting client A.");
    }
}
