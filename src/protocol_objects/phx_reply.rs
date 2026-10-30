use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "status", content = "response")]
pub enum PhxReply {
    #[serde(rename = "error")]
    Error(ErrorReply),
    #[serde(rename = "ok")]
    Ok(PhxResponse),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ErrorReply {
    pub reason: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PhxResponse {
    #[serde(default)]
    #[serde(rename = "postgres_changes")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres_changes: Option<Vec<ReplyPostgresChanges>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplyPostgresChanges {
    pub event: ReplyPostgresChangedEvent,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub schema: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub table: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub select: Option<Vec<String>>,
    pub id: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReplyPostgresChangedEvent {
    #[serde(rename = "*")]
    All,
    #[serde(rename = "INSERT")]
    Insert,
    #[serde(rename = "UPDATE")]
    Update,
    #[serde(rename = "DELETE")]
    Delete,
}
