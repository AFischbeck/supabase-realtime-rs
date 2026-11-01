use std::{pin::Pin, time::Duration};

use crate::{
    channel::RealtimeChannelMutableState,
    channel_event::ChannelEvent,
    error::RealtimeError,
    protocol_objects::{JoinPostgresChanges, Message, Payload, PostgresChangesPayload},
};

// Constants
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(25);

pub type Result<Type> = std::result::Result<Type, RealtimeError>;

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChannelState {
    Joined,
    #[default]
    Closed,
    Errored,
    Joining,
    Leaving,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubscribeState {
    Subscribed,
    TimedOut,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PushReplyStatus {
    Ok,
    TimedOut,
    Error,
}

pub(crate) type ConnectionMessageReceivedEvent =
    Box<dyn Fn(Message) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;
pub(crate) type SubscribeCallback = Box<dyn Fn(Result<SubscribeState>) + Send + Sync>;
pub(crate) type PushCallback = Box<dyn Fn(&Payload) + Send + Sync>;
pub(crate) type PostgresChangesCallback = Box<dyn Fn(PostgresChangesPayload) + Send + Sync>;

pub(crate) struct Binding {
    callback: ChannelEvent,
    id: Option<i64>,
}

impl Binding {
    pub(crate) fn new(callback: ChannelEvent, id: Option<i64>) -> Self {
        Self { callback, id }
    }

    pub(crate) fn invoke(
        &self,
        channel_state: &mut RealtimeChannelMutableState,
        payload: Payload,
        _ref: Option<&str>,
        should_remove_channel: &mut bool,
    ) {
        self.callback
            .invoke(channel_state, payload, _ref, should_remove_channel)
    }
}

pub(crate) struct PostgresChangeBinding {
    pub config: JoinPostgresChanges,
    pub id: Option<i64>,
    pub callback: PostgresChangesCallback,
}

pub(crate) struct Hook {
    status: PushReplyStatus,
    callback: PushCallback,
}

impl Hook {
    pub(crate) fn new(status: PushReplyStatus, callback: PushCallback) -> Self {
        Self { status, callback }
    }

    pub(crate) fn get_status(&self) -> &PushReplyStatus {
        &self.status
    }

    pub(crate) fn invoke(&self, payload: &Payload) {
        (self.callback)(payload);
    }
}

#[derive(Clone)]
pub(crate) struct PayloadResponse {
    status: PushReplyStatus,
    payload: Payload,
}

impl PayloadResponse {
    pub(crate) fn new(status: PushReplyStatus, payload: Payload) -> Self {
        Self { status, payload }
    }

    pub(crate) fn get_status(&self) -> &PushReplyStatus {
        &self.status
    }

    pub(crate) fn get_payload(&self) -> &Payload {
        &self.payload
    }
}

#[derive(Default, Clone)]
pub struct PostgresChangesParams {
    pub schema: Option<String>,
    pub table: Option<String>,
    pub filter: Option<String>,
    pub select: Option<Vec<String>>,
}

impl PostgresChangesParams {
    pub fn schema(mut self, schema: impl Into<String>) -> Self {
        self.schema = Some(schema.into());
        self
    }

    pub fn table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }

    pub fn filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    pub fn select<I, S>(mut self, select: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.select = Some(select.into_iter().map(Into::into).collect());
        self
    }
}
