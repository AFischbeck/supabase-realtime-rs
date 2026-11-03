use std::{
    mem::discriminant,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::{
    Notify,
    oneshot::{Receiver, channel},
};

use crate::{
    channel::RealtimeChannel,
    channel_event::ChannelEvent,
    client::RealtimeClient,
    error::RealtimeError,
    protocol_objects::{Payload, PhxClose, PhxError, PhxJoin, System},
    push::Push,
    types::{
        ChannelState, DEFAULT_TIMEOUT, PayloadResponse, PushReplyStatus, Result, SubscribeCallback,
        SubscribeState,
    },
    utils::get_reply_event_name,
};

#[derive(Clone)]
struct SubscribeNotifier {
    callback: Arc<Mutex<Option<SubscribeCallback>>>,
    resolution: Arc<Notify>,
}

impl SubscribeNotifier {
    fn new(callback: Option<SubscribeCallback>) -> Self {
        Self {
            callback: Arc::new(Mutex::new(callback)),
            resolution: Arc::new(Notify::new()),
        }
    }

    fn resolve(&self, result: Result<SubscribeState>) {
        let callback = self
            .callback
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take();

        self.resolution.notify_one();

        if let Some(callback) = callback {
            callback(result);
        }
    }

    async fn wait_for_resolution(&self) {
        self.resolution.notified().await;
    }
}

impl RealtimeChannel {
    pub async fn subscribe(
        &mut self,
        client: &mut RealtimeClient,
        callback: Option<SubscribeCallback>,
    ) -> Result<()> {
        if !client.is_connected() {
            client.connect().await?;
        }

        let mut state = self.mutable_state.lock().await;
        if state.joined_once {
            return Err(RealtimeError::MultipleSubscriptionError);
        }
        if state.state == ChannelState::Leaving {
            return Err(RealtimeError::ChannelLeaving);
        }
        state.joined_once = true;

        let has_postgres_changes = !state.postgres_bindings.is_empty();
        self.config.postgres_changes = has_postgres_changes.then(|| {
            state
                .postgres_bindings
                .iter()
                .map(|b| b.config.clone())
                .collect()
        });
        drop(state);

        self.join_push = Push::new(
            "phx_join",
            Payload::PhxJoin(PhxJoin {
                config: self.config.clone(),
                access_token: String::from(client.get_access_token()),
            }),
            None,
        );

        let notifier = SubscribeNotifier::new(callback);

        self.register_join_push_callbacks(notifier.clone(), has_postgres_changes)
            .await;
        self.register_subscribe_lifecycle(notifier.clone()).await;

        if has_postgres_changes {
            self.register_postgres_readiness(notifier.clone()).await;
        }

        spawn_subscribe_timeout(notifier.clone());

        let (sender, receiver) = channel();
        let reference = client.make_ref().await;
        let reply_event_name = get_reply_event_name(&reference);
        self.mutable_state
            .lock()
            .await
            .push_senders
            .insert(reply_event_name.clone(), sender);

        if let Err(error) = self.join(client, &reference, receiver).await {
            let mut state = self.mutable_state.lock().await;
            state.push_senders.remove(&reply_event_name);
            state.joined_once = false;
            drop(state);

            notifier.resolve(Err(RealtimeError::SubscribeError {
                payload: error.to_string(),
            }));

            return Err(error);
        }

        Ok(())
    }

    async fn join(
        &mut self,
        client: &mut RealtimeClient,
        reference: &str,
        receiver: Receiver<PayloadResponse>,
    ) -> Result<()> {
        {
            let mut state = self.mutable_state.lock().await;
            if state.state == ChannelState::Leaving {
                return Err(RealtimeError::ChannelLeaving);
            }

            // TODO: client leave open topic
            state.state = ChannelState::Joining;
        }

        self.join_push
            .resend(
                client,
                &self.topic,
                reference,
                &get_reply_event_name(reference),
                receiver,
            )
            .await
    }

    async fn register_join_push_callbacks(
        &mut self,
        notifier: SubscribeNotifier,
        has_postgres_changes: bool,
    ) {
        let on_ok = {
            let notifier = notifier.clone();
            move |_: &Payload| {
                if !has_postgres_changes {
                    notifier.resolve(Ok(SubscribeState::Subscribed));
                }
            }
        };

        let on_error = {
            let notifier = notifier.clone();
            move |payload: &Payload| {
                let payload_raw = serde_json::to_string(payload).unwrap_or(String::from(
                    "Subscription error payload failed to deserialize.",
                ));
                notifier.resolve(Err(RealtimeError::SubscribeError {
                    payload: payload_raw,
                }));
            }
        };

        self.join_push
            .register_receive_callback(PushReplyStatus::Ok, Box::new(on_ok))
            .await;
        self.join_push
            .register_receive_callback(PushReplyStatus::Error, Box::new(on_error))
            .await;
    }

    async fn register_subscribe_lifecycle(&self, notifier: SubscribeNotifier) {
        let close_notifier = notifier.clone();
        self.register_event(
            discriminant(&Payload::PhxClose(PhxClose)),
            ChannelEvent::Close(Box::new(move |_, _| {
                close_notifier.resolve(Ok(SubscribeState::Closed))
            })),
        )
        .await;

        self.register_event(
            discriminant(&Payload::PhxError(PhxError)),
            ChannelEvent::Error(Box::new(move |_| {
                notifier.resolve(Ok(SubscribeState::Closed))
            })),
        )
        .await;
    }

    async fn register_postgres_readiness(&self, notifier: SubscribeNotifier) {
        self.register_event(
            discriminant(&Payload::System(System::default())),
            ChannelEvent::System(Box::new(move |system: &System| {
                if system.extension != "postgres_changes" {
                    return;
                }

                match system.status.as_str() {
                    "ok" => notifier.resolve(Ok(SubscribeState::Subscribed)),
                    "timeout" => notifier.resolve(Ok(SubscribeState::TimedOut)),
                    _ => {
                        let payload = serde_json::to_string(system)
                            .unwrap_or_else(|_| system.message.clone());
                        notifier.resolve(Err(RealtimeError::SubscribeError { payload }));
                    }
                }
            })),
        )
        .await;
    }
}

fn spawn_subscribe_timeout(notifier: SubscribeNotifier) {
    spawn_subscribe_timeout_after(notifier, DEFAULT_TIMEOUT);
}

fn spawn_subscribe_timeout_after(notifier: SubscribeNotifier, timeout: Duration) {
    tokio::spawn(async move {
        tokio::select! {
            _ = tokio::time::sleep(timeout) => {
                notifier.resolve(Ok(SubscribeState::TimedOut));
            }
            _ = notifier.wait_for_resolution() => {}
        }
    });
}
