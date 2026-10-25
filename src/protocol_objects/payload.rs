use std::fmt::Display;

use serde::{Deserialize, Serialize};

use super::{
    access_token::AccessToken,
    broadcast::Broadcast,
    heartbeat::Heartbeat,
    phx_close::PhxClose,
    phx_error::PhxError,
    phx_join::PhxJoin,
    phx_leave::PhxLeave,
    phx_reply::PhxReply,
    postgres_changes::PostgresChangesPayload,
    presence::{PresenceDiff, PresencePush, PresenceState},
    system::System,
};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", content = "payload", rename_all = "snake_case")]
pub enum Payload {
    #[serde(rename = "phx_join")]
    PhxJoin(PhxJoin),
    #[serde(rename = "phx_leave")]
    PhxLeave(PhxLeave),
    #[serde(rename = "phx_reply")]
    PhxReply(PhxReply),
    #[serde(rename = "phx_close")]
    PhxClose(PhxClose),
    #[serde(rename = "phx_error")]
    PhxError(PhxError),
    #[serde(rename = "heartbeat")]
    Heartbeat(Heartbeat),
    #[serde(rename = "access_token")]
    AccessToken(AccessToken),
    #[serde(rename = "broadcast")]
    Broadcast(Broadcast),
    #[serde(rename = "presence")]
    Presence(PresencePush),
    #[serde(rename = "presence_state")]
    PresenceState(PresenceState),
    #[serde(rename = "presence_diff")]
    PresenceDiff(PresenceDiff),
    #[serde(rename = "system")]
    System(System),
    #[serde(rename = "postgres_changes")]
    PostgresChanges(PostgresChangesPayload),
}

impl Display for Payload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Payload::PhxJoin(_) => write!(f, "phx_join"),
            Payload::PhxLeave(_) => write!(f, "phx_leave"),
            Payload::PhxReply(_) => write!(f, "phx_reply"),
            Payload::PhxClose(_) => write!(f, "phx_close"),
            Payload::PhxError(_) => write!(f, "phx_error"),
            Payload::Heartbeat(_) => write!(f, "heartbeat"),
            Payload::AccessToken(_) => write!(f, "access_token"),
            Payload::Broadcast(_) => write!(f, "broadcast"),
            Payload::Presence(_) => write!(f, "presence"),
            Payload::PresenceState(_) => write!(f, "presence_state"),
            Payload::PresenceDiff(_) => write!(f, "presence_diff"),
            Payload::System(_) => write!(f, "system"),
            Payload::PostgresChanges(_) => write!(f, "postgres_changes"),
        }
    }
}
