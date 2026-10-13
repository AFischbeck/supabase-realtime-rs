use thiserror::Error;
use tokio::task::JoinError;

use crate::protocol_objects::Message;

#[cfg(not(target_arch = "wasm32"))]
type TungsteniteError = tokio_tungstenite::tungstenite::Error;
#[cfg(target_arch = "wasm32")]
type TungsteniteError = tokio_tungstenite_wasm::Error;

#[derive(Error, Debug)]
pub enum RealtimeError {
    #[error("Failed to connect to WebSocket: {0}")]
    ConnectionError(#[from] Box<TungsteniteError>),

    #[error("Failed to serialize message to JSON: {0}")]
    SerializationError(#[from] serde_json::Error),

    #[error("WebSocket connection closed unexpectedly")]
    ConnectionClosed,

    #[error("Failed to send message through channel")]
    ChannelSendError,

    #[error("Client is not connected")]
    NotConnected,

    // #[error("Failed to send message through MPSC channel: {0}")]
    // MpscSendError(#[from] futures::channel::mpsc::SendError),

    // #[error("Failed to send message through MPSC channel (channel full): {0}")]
    // MpscTrySendError(#[from] Box<futures::channel::mpsc::TrySendError<Message>>),

    // #[error("Failed to send message through broadcast channel: {0}")]
    // BroadcastSendError(#[from] Box<tokio::sync::broadcast::error::SendError<Message>>),
    #[error("Failed to send message through MPSC channel: {0}")]
    MpscSendError(#[from] Box<tokio::sync::mpsc::error::SendError<Message>>),

    #[error("Heartbeat failed")]
    HeartbeatError,

    #[error("Url must be a valid WebSocket URL or HTTP URL string")]
    InvalidUrlError,

    #[error(
        "Tried to subscribe multiple times. 'subscribe' can only be called a single time per channel instance"
    )]
    MultipleSubscriptionError,

    #[error(
        "Tried to push {event} to {topic} before joining. Use channel.subscribe() before pushing events."
    )]
    PushWhileUnsubscribedError { event: String, topic: String },

    #[error("Subscribe error: {payload}")]
    SubscribeError { payload: String },

    #[error("Task panicked or was cancelled: {0}")]
    TaskPanic(#[from] JoinError),

    #[error("Multiple tasks failed ({} errors): {}", errors.len(), .errors.iter().map(|e| e.to_string()).collect::<Vec<_>>().join("; "))]
    MultipleTaskErrors { errors: Vec<RealtimeError> },
}

impl From<TungsteniteError> for RealtimeError {
    fn from(err: TungsteniteError) -> Self {
        RealtimeError::ConnectionError(Box::new(err))
    }
}

// impl From<futures::channel::mpsc::TrySendError<Message>> for RealtimeError {
//     fn from(err: futures::channel::mpsc::TrySendError<Message>) -> Self {
//         RealtimeError::MpscTrySendError(Box::new(err))
//     }
// }

// impl From<tokio::sync::broadcast::error::SendError<Message>> for RealtimeError {
//     fn from(err: tokio::sync::broadcast::error::SendError<Message>) -> Self {
//         RealtimeError::BroadcastSendError(Box::new(err))
//     }
// }

impl From<tokio::sync::mpsc::error::SendError<Message>> for RealtimeError {
    fn from(err: tokio::sync::mpsc::error::SendError<Message>) -> Self {
        RealtimeError::MpscSendError(Box::new(err))
    }
}
