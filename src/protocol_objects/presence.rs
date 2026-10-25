use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PresenceState(pub HashMap<String, Presence>);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Presence {
    pub metas: Vec<PresenceMeta>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PresenceMeta {
    pub phx_ref: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub phx_ref_prev: Option<String>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PresenceDiff {
    pub joins: HashMap<String, Presence>,
    pub leaves: HashMap<String, Presence>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "event", content = "payload", rename_all = "snake_case")]
pub enum PresencePush {
    Track(Value),
    Untrack(#[serde(default)] Value),
}
