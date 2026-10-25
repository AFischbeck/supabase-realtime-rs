use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PhxJoin {
    pub config: JoinConfig,
    #[serde(default)]
    pub access_token: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JoinConfig {
    #[serde(rename = "broadcast")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub broadcast: Option<BroadcastConfig>,
    #[serde(rename = "presence")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub presence: Option<PresenceConfig>,
    #[serde(rename = "postgres_changes")]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub postgres_changes: Option<Vec<JoinPostgresChanges>>,
    pub private: bool,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BroadcastConfig {
    #[serde(rename = "self")]
    pub self_item: bool,
    pub ack: bool,
}

// Workaround for serde bool default https://github.com/serde-rs/serde/issues/368
fn default_true() -> bool {
    true
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PresenceConfig {
    #[serde(default)]
    pub key: String,
    #[serde(default = "default_true")]
    pub enabled: bool,
}

impl Default for PresenceConfig {
    fn default() -> Self {
        Self {
            key: String::new(),
            enabled: true,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct JoinPostgresChanges {
    pub event: JoinPostgresChangedEvent,
    pub schema: String,
    pub table: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub filter: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub enum JoinPostgresChangedEvent {
    #[default]
    #[serde(rename = "*")]
    All,
    #[serde(rename = "INSERT")]
    Insert,
    #[serde(rename = "UPDATE")]
    Update,
    #[serde(rename = "DELETE")]
    Delete,
}
