use std::{
    collections::HashMap,
    mem::{Discriminant, discriminant},
    sync::Arc,
};

use tokio::sync::{
    Mutex,
    oneshot::{Receiver, Sender, channel},
};

use crate::{
    channel_event::ChannelEvent,
    client::RealtimeClient,
    error::RealtimeError,
    protocol_objects::{
        Broadcast, JoinConfig, JoinPostgresChangedEvent, JoinPostgresChanges, Payload, PhxClose,
        PhxError, PhxJoin, PhxLeave, PhxReply, PhxResponse, PostgresChangesData,
        PostgresChangesPayload, PostgresDataChangeEvent, Presence, PresenceDiff, PresencePush,
        PresenceState,
    },
    push::Push,
    types::{
        Binding, ChannelState, PayloadResponse, PostgresChangeBinding, PostgresChangesParams,
        PushReplyStatus, Result, SubscribeCallback, SubscribeState,
    },
    utils::get_reply_event_name,
};

#[derive(Clone)]
pub struct RealtimeChannel {
    topic: String,
    config: JoinConfig,
    join_push: Push,
    mutable_state: Arc<Mutex<RealtimeChannelMutableState>>,
}

#[derive(Default)]
pub(crate) struct RealtimeChannelMutableState {
    state: ChannelState,
    joined_once: bool,
    push_buffer: Vec<Push>,
    bindings: HashMap<Discriminant<Payload>, Vec<Binding>>,
    push_senders: HashMap<String, Sender<PayloadResponse>>,
    presence_state: PresenceState,
    postgres_bindings: Vec<PostgresChangeBinding>,
}

impl RealtimeChannel {
    pub(crate) fn new(client: &RealtimeClient, topic: &str, config: Option<JoinConfig>) -> Self {
        let join_push = Push::new(
            "phx_join",
            Payload::PhxJoin(PhxJoin {
                config: config.clone().unwrap_or_default(),
                access_token: String::from(client.get_access_token()),
            }),
            None,
        );

        Self {
            topic: String::from(topic),
            config: config.unwrap_or_default(),
            join_push,
            mutable_state: Arc::new(Mutex::new(RealtimeChannelMutableState::default())),
        }
    }

    pub(crate) async fn register_default_events(&self) {
        self.register_event(
            discriminant(&Payload::PhxClose(PhxClose)),
            ChannelEvent::Close(Box::new(Self::on_close)),
        )
        .await;
        self.register_event(
            discriminant(&Payload::PhxError(PhxError)),
            ChannelEvent::Error(Box::new(Self::on_error)),
        )
        .await;
        self.register_event(
            discriminant(&Payload::PhxReply(PhxReply::Ok(PhxResponse::default()))),
            ChannelEvent::Reply(Box::new(Self::on_reply)),
        )
        .await;

        self.register_event(
            discriminant(&Payload::PresenceState(PresenceState(HashMap::new()))),
            ChannelEvent::PresenceState(Box::new(Self::on_presence_payload)),
        )
        .await;

        self.register_event(
            discriminant(&Payload::PresenceDiff(PresenceDiff {
                joins: HashMap::new(),
                leaves: HashMap::new(),
            })),
            ChannelEvent::PresenceDiff(Box::new(Self::on_presence_diff)),
        )
        .await;

        self.register_event(
            discriminant(&Payload::PostgresChanges(PostgresChangesPayload {
                data: PostgresChangesData {
                    columns: vec![],
                    commit_timestamp: String::new(),
                    errors: None,
                    old_record: None,
                    record: None,
                    schema: String::new(),
                    table: String::new(),
                    type_: PostgresDataChangeEvent::Insert,
                },
                ids: vec![],
            })),
            ChannelEvent::PostgresChanges(Box::new(Self::on_postgres_changes_payload)),
        )
        .await;
    }

    fn on_close(state: &mut RealtimeChannelMutableState, should_remove_channel: &mut bool) {
        // reset the rejoin timer
        state.state = ChannelState::Closed;

        *should_remove_channel = true;
    }

    fn on_error(state: &mut RealtimeChannelMutableState) {
        if state.state == ChannelState::Closed || state.state == ChannelState::Leaving {
            return;
        }

        state.state = ChannelState::Errored;
        // rejoin
    }

    fn on_reply(state: &mut RealtimeChannelMutableState, payload: Payload, _ref: Option<&str>) {
        if let Payload::PhxReply(ref reply) = payload {
            match reply {
                PhxReply::Ok(response) => {
                    if let Some(ref postgres_changes) = response.postgres_changes {
                        for (i, confirmed) in postgres_changes.iter().enumerate() {
                            if i < state.postgres_bindings.len() {
                                state.postgres_bindings[i].id = Some(confirmed.id);
                            }
                        }
                    }
                    if let Some(_ref) = _ref {
                        let event_name = get_reply_event_name(_ref);
                        if let Some(sender) = state.push_senders.remove(&event_name)
                            && let Err(failed_to_send_payload) =
                                sender.send(PayloadResponse::new(PushReplyStatus::Ok, payload))
                        {
                            // TODO: error handling
                            println!("Reply received for a push that was already dropped.");
                        }
                    }
                }
                PhxReply::Error(_) => {
                    if let Some(_ref) = _ref {
                        let event_name = get_reply_event_name(_ref);
                        if let Some(sender) = state.push_senders.remove(&event_name)
                            && let Err(failed_to_send_payload) =
                                sender.send(PayloadResponse::new(PushReplyStatus::Error, payload))
                        {
                            // TODO: error handling
                            println!("Reply received for a push that was already dropped.");
                        }
                    }
                }
            }
        }
    }

    fn on_presence_payload(state: &mut RealtimeChannelMutableState, payload: Payload) {
        if let Payload::PresenceState(presence_state) = payload {
            state.presence_state = presence_state;
        }
    }

    fn on_presence_diff(state: &mut RealtimeChannelMutableState, payload: Payload) {
        if let Payload::PresenceDiff(diff) = payload {
            merge_presence_joins(&mut state.presence_state.0, diff.joins);
            remove_presence_leaves(&mut state.presence_state.0, diff.leaves);
        }
    }

    fn on_postgres_changes_payload(state: &mut RealtimeChannelMutableState, payload: Payload) {
        if let Payload::PostgresChanges(pg_payload) = payload {
            let incoming = JoinPostgresChangedEvent::from(&pg_payload.data.type_);

            state.postgres_bindings.iter().for_each(|postgres_binding| {
                if postgres_binding.config.event == JoinPostgresChangedEvent::All
                    || postgres_binding.config.event == incoming
                {
                    (postgres_binding.callback)(pg_payload.clone());
                }
            });
        }
    }

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
        state.joined_once = true;

        self.config.postgres_changes = (!state.postgres_bindings.is_empty()).then(|| {
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

        let callback = Arc::new(callback);
        let on_join_push_ok = {
            let callback = Arc::clone(&callback);
            move |_: &Payload| {
                if let Some(ref callback) = *callback {
                    callback(Ok(SubscribeState::Subscribed));
                }
            }
        };

        let on_join_push_error = {
            let callback = Arc::clone(&callback);
            move |payload: &Payload| {
                println!("Calling 'on_join_push_error' PushEvent.");
                if let Some(ref callback) = *callback {
                    let payload_raw = serde_json::to_string(payload).unwrap_or(String::from(
                        "Subscription error payload failed to deserialize.",
                    ));
                    callback(Err(RealtimeError::SubscribeError {
                        payload: payload_raw,
                    }))
                }
            }
        };

        let on_join_push_timeout = move |_: &Payload| {
            println!("Calling 'on_join_push_timeout' PushEvent.");
            if let Some(ref callback) = *callback {
                callback(Ok(SubscribeState::TimedOut));
            }
        };

        self.join_push
            .register_receive_callback(PushReplyStatus::Ok, Box::new(on_join_push_ok))
            .await;
        self.join_push
            .register_receive_callback(PushReplyStatus::Error, Box::new(on_join_push_error))
            .await;
        self.join_push
            .register_receive_callback(PushReplyStatus::TimedOut, Box::new(on_join_push_timeout))
            .await;

        let (sender, receiver) = channel();
        let _ref = client.make_ref().await;
        let reply_event_name = get_reply_event_name(&_ref);

        {
            let mut mutable_state = self.mutable_state.lock().await;
            mutable_state.push_senders.insert(reply_event_name, sender);
        }

        self.rejoin(client, &_ref, receiver).await?;

        Ok(())
    }

    pub async fn track(&self, client: &RealtimeClient, user_id: String) -> Result<()> {
        let payload =
            Payload::Presence(PresencePush::Track(serde_json::json!({"user_id": user_id})));
        self.push(client, "presence", payload).await
    }

    pub async fn untrack(&self, client: &RealtimeClient) -> Result<()> {
        let payload = Payload::Presence(PresencePush::Untrack(serde_json::json!({})));
        self.push(client, "presence", payload).await
    }

    pub async fn presence_state(&self) -> PresenceState {
        self.mutable_state.lock().await.presence_state.clone()
    }

    pub async fn on_presence_sync<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let callback = Arc::new(callback);
        let on_state = Arc::clone(&callback);
        self.register_event(
            discriminant(&Payload::PresenceState(PresenceState(HashMap::new()))),
            ChannelEvent::Broadcast(Box::new(move |_| on_state())),
        )
        .await;

        self.register_event(
            discriminant(&Payload::PresenceDiff(PresenceDiff {
                joins: HashMap::new(),
                leaves: HashMap::new(),
            })),
            ChannelEvent::Broadcast(Box::new(move |_| callback())),
        )
        .await;
    }

    pub async fn on_presence_join<F>(&self, callback: F)
    where
        F: Fn(String, Vec<Presence>, Vec<Presence>) + Send + Sync + 'static,
    {
        self.register_event(
            discriminant(&Payload::PresenceDiff(PresenceDiff {
                joins: HashMap::new(),
                leaves: HashMap::new(),
            })),
            ChannelEvent::PresenceDiff(Box::new(
                move |state: &mut RealtimeChannelMutableState, payload: Payload| {
                    if let Payload::PresenceDiff(diff) = payload {
                        let current_presences: Vec<Presence> =
                            state.presence_state.0.values().cloned().collect();
                        for (key, presence) in diff.joins {
                            let new_presences = presence
                                .metas
                                .into_iter()
                                .map(|meta| Presence { metas: vec![meta] })
                                .collect();
                            callback(key, current_presences.clone(), new_presences);
                        }
                    }
                },
            )),
        )
        .await;
    }

    pub async fn on_postgres_changes<F>(
        &self,
        event: JoinPostgresChangedEvent,
        params: PostgresChangesParams,
        callback: F,
    ) -> Result<()>
    where
        F: Fn(PostgresChangesPayload) + Send + Sync + 'static,
    {
        let mut state = self.mutable_state.lock().await;
        if state.joined_once {
            return Err(RealtimeError::MultipleSubscriptionError);
        }

        state.postgres_bindings.push(PostgresChangeBinding {
            callback: Box::new(callback),
            id: None,
            config: JoinPostgresChanges {
                event,
                schema: params.schema,
                table: params.table,
                filter: params.filter,
                select: params.select,
            },
        });

        Ok(())
    }

    pub async fn on_presence_leave<F>(&self, callback: F)
    where
        F: Fn(String, Vec<Presence>, Vec<Presence>) + Send + Sync + 'static,
    {
        self.register_event(
            discriminant(&Payload::PresenceDiff(PresenceDiff {
                joins: HashMap::new(),
                leaves: HashMap::new(),
            })),
            ChannelEvent::PresenceDiff(Box::new(
                move |state: &mut RealtimeChannelMutableState, payload: Payload| {
                    if let Payload::PresenceDiff(diff) = payload {
                        let current_presences: Vec<Presence> =
                            state.presence_state.0.values().cloned().collect();
                        for (key, presence) in diff.leaves {
                            let left_presences = presence
                                .metas
                                .into_iter()
                                .map(|meta| Presence { metas: vec![meta] })
                                .collect();
                            callback(key, current_presences.clone(), left_presences);
                        }
                    }
                },
            )),
        )
        .await;
    }

    pub(crate) async fn unsubscribe(&self, client: &RealtimeClient) -> Result<()> {
        let mut state = self.mutable_state.lock().await;
        state.state = ChannelState::Leaving;
        // reset rejoin timer

        let payload = Payload::PhxLeave(PhxLeave);
        let mut leave_push = Push::new("phx_leave", payload, None);
        let topic_clone = self.topic.clone();
        leave_push
            .register_receive_callback(
                PushReplyStatus::Ok,
                Box::new(move |_| println!("Leaving channel {topic_clone}")),
            )
            .await;

        let (sender, receiver) = channel();
        let _ref = client.make_ref().await;
        let reply_event_name = get_reply_event_name(&_ref);
        state.push_senders.insert(reply_event_name, sender);

        leave_push
            .send(client, &self.topic, &_ref, "phx_leave", receiver)
            .await
    }

    async fn push(
        &self,
        client: &RealtimeClient,
        event: &str,
        payload: Payload,
        // timeout
    ) -> Result<()> {
        if !self.mutable_state.lock().await.joined_once {
            return Err(RealtimeError::PushWhileUnsubscribedError {
                event: String::from(event),
                topic: self.topic.clone(),
            });
        }

        let mut push = Push::new(event, payload, None);

        let (sender, receiver) = channel();
        let _ref = client.make_ref().await;
        let reply_event_name = get_reply_event_name(&_ref);

        {
            let mut mutable_state = self.mutable_state.lock().await;
            mutable_state
                .push_senders
                .insert(reply_event_name.clone(), sender);
        }

        if self.can_push(client).await {
            return push
                .send(client, &self.topic, &_ref, &reply_event_name, receiver)
                .await;
        } else {
            println!("Didn't push event {reply_event_name} because client was not connected.");
            // push.start_timeout(self, client, receiver);
            // self.push_buffer.push(push);
        }

        Ok(())
    }

    pub(crate) async fn register_event(
        &self,
        discriminant: Discriminant<Payload>,
        event: ChannelEvent,
    ) {
        let mut mutable_state = self.mutable_state.lock().await;

        let binding = Binding::new(event, None);
        mutable_state
            .bindings
            .entry(discriminant)
            .or_default()
            .push(binding);
    }

    pub async fn on_broadcast<F>(&self, event: &str, f: F)
    where
        F: Fn(Payload) + Send + Sync + 'static,
    {
        self.register_event(
            discriminant(&Payload::Broadcast(Broadcast::default())),
            ChannelEvent::Broadcast(Box::new(f)),
        )
        .await
    }

    pub async fn send_broadcast(
        &self,
        client: &RealtimeClient,
        event: &str,
        payload: Payload,
    ) -> Result<()> {
        self.push(client, event, payload).await
    }

    pub fn get_topic(&self) -> &str {
        &self.topic
    }

    async fn rejoin(
        &mut self,
        client: &mut RealtimeClient,
        _ref: &str,
        receiver: Receiver<PayloadResponse>,
    ) -> Result<()> {
        let mut mutable_state = self.mutable_state.lock().await;
        if mutable_state.state == ChannelState::Leaving {
            return Ok(());
        }

        // TODO: client leave open topic
        mutable_state.state = ChannelState::Joining;
        self.join_push
            .resend(
                client,
                &self.topic,
                _ref,
                &get_reply_event_name(_ref),
                receiver,
            )
            .await
    }

    async fn can_push(&self, client: &RealtimeClient) -> bool {
        client.is_connected() && self.mutable_state.lock().await.joined_once
    }

    pub(crate) async fn trigger(
        &self,
        payload: Payload,
        _ref: Option<&str>,
        should_remove_channel: &mut bool,
    ) {
        let maybe_ignore = [
            discriminant(&Payload::PhxClose(PhxClose)),
            discriminant(&Payload::PhxError(PhxError)),
            discriminant(&Payload::PhxLeave(PhxLeave)),
            discriminant(&Payload::PhxJoin(PhxJoin::default())),
        ];

        if let Some(_ref) = _ref
            && let Some(join_push_ref) = self.join_push.get_ref()
            && maybe_ignore.contains(&discriminant(&payload))
            && _ref != join_push_ref
        {
            println!("Ignoring trigger for {payload} payload with ref {_ref}.");
            return;
        }

        let mut mutable_state = self.mutable_state.lock().await;

        if let Some(bindings) = mutable_state.bindings.remove(&discriminant(&payload)) {
            for binding in &bindings {
                binding.invoke(
                    &mut mutable_state,
                    payload.clone(),
                    _ref,
                    should_remove_channel,
                );
            }
            mutable_state
                .bindings
                .insert(discriminant(&payload), bindings);
        }
    }
}

fn merge_presence_joins(
    presence_state: &mut HashMap<String, Presence>,
    joins: HashMap<String, Presence>,
) {
    for (key, presence) in joins {
        match presence_state.get_mut(&key) {
            Some(existing) => {
                existing.metas.retain(|meta| {
                    !presence
                        .metas
                        .iter()
                        .any(|joined| joined.phx_ref == meta.phx_ref)
                });
                existing.metas.extend(presence.metas);
            }
            None => {
                presence_state.insert(key, presence);
            }
        }
    }
}

fn remove_presence_leaves(
    presence_state: &mut HashMap<String, Presence>,
    leaves: HashMap<String, Presence>,
) {
    for (key, presence) in leaves {
        let Some(existing) = presence_state.get_mut(&key) else {
            continue;
        };

        existing.metas.retain(|meta| {
            !presence
                .metas
                .iter()
                .any(|left| left.phx_ref == meta.phx_ref)
        });

        if existing.metas.is_empty() {
            presence_state.remove(&key);
        }
    }
}
