use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct System {
    #[serde(rename = "channel")]
    pub channel: String,
    #[serde(rename = "extension")]
    pub extension: String,
    #[serde(rename = "message")]
    pub message: String,
    #[serde(rename = "status")]
    pub status: String,
}
