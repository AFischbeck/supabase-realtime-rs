use crate::{channel::RealtimeChannelMutableState, protocol_objects::Payload};

pub(crate) enum ChannelEvent {
    Reply(Box<dyn Fn(&mut RealtimeChannelMutableState, Payload, Option<&str>) + Send + Sync>),
    Broadcast(Box<dyn Fn(Payload) + Send + Sync>),
    Error(Box<dyn Fn(&mut RealtimeChannelMutableState) + Send + Sync>),
    Close(Box<dyn Fn(&mut RealtimeChannelMutableState, &mut bool) + Send + Sync>),
    PresenceState(Box<dyn Fn(&mut RealtimeChannelMutableState, Payload) + Send + Sync>),
    PresenceDiff(Box<dyn Fn(&mut RealtimeChannelMutableState, Payload) + Send + Sync>),
    PostgresChanges(Box<dyn Fn(&mut RealtimeChannelMutableState, Payload) + Send + Sync>),
}

impl ChannelEvent {
    pub(crate) fn invoke(
        &self,
        channel_state: &mut RealtimeChannelMutableState,
        payload: Payload,
        _ref: Option<&str>,
        should_remove_channel: &mut bool,
    ) {
        match self {
            ChannelEvent::Reply(event) => event(channel_state, payload, _ref),
            ChannelEvent::Broadcast(event) => event(payload),
            ChannelEvent::Error(event) => event(channel_state),
            ChannelEvent::Close(event) => event(channel_state, should_remove_channel),
            ChannelEvent::PresenceState(event) => event(channel_state, payload),
            ChannelEvent::PresenceDiff(event) => event(channel_state, payload),
            ChannelEvent::PostgresChanges(event) => event(channel_state, payload),
        }
    }
}