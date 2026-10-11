use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use tokio::sync::Mutex;

use crate::{
    channel::RealtimeChannel,
    connection::RealtimeConnection,
    error::RealtimeError,
    protocol_objects::{JoinConfig, Message},
    types::Result,
    utils::{http_to_ws, is_ws_url},
};

#[derive(Clone)]
pub struct RealtimeClient {
    url: String,
    api_key: String,
    access_token: String,
    auto_reconnect: bool,
    max_retries: u32,
    initial_backoff: f32,
    connection: Option<RealtimeConnection>,
    mutable_state: Arc<Mutex<RealtimeClientMutableState>>,
}

#[derive(Clone, Default)]
pub(crate) struct RealtimeClientMutableState {
    _ref: u32,
    channels: HashMap<String, RealtimeChannel>,
}

impl RealtimeClient {
    pub fn new(
        project_url: &str,
        api_key: &str,
        auto_reconnect: Option<bool>,
        max_retries: Option<u32>,
        initial_backoff: Option<f32>,
    ) -> Result<Self> {
        if !is_ws_url(project_url) {
            return Err(RealtimeError::InvalidUrlError);
        }
        let url = http_to_ws(project_url);
        // let http_endpoint = http_endpoint_url(project_url);

        Ok(Self {
            url,
            api_key: String::from(api_key),
            access_token: String::from(api_key),
            auto_reconnect: auto_reconnect.unwrap_or(true),
            max_retries: max_retries.unwrap_or(5),
            initial_backoff: initial_backoff.unwrap_or(1.0),
            connection: None,
            mutable_state: Arc::new(Mutex::new(RealtimeClientMutableState::default())),
        })
    }

    pub async fn create_channel(
        &self,
        topic: &str,
        options: Option<JoinConfig>,
    ) -> RealtimeChannel {
        let topic = format!("realtime:{topic}");

        let channel = RealtimeChannel::new(self, &topic, options);
        channel.register_default_events().await;

        self.mutable_state
            .lock()
            .await
            .channels
            .insert(topic.clone(), channel.clone());

        channel
    }

    pub async fn remove_channel(&mut self, channel: &RealtimeChannel) {
        let topic = format!("realtime:{}", channel.get_topic());

        let channels = &self.mutable_state.lock().await.channels;
        let channel = channels.get(&topic);

        if let Some(channel) = channel {
            channel.unsubscribe(self).await;
        }
    }

    pub async fn remove_all_channels(&mut self) {
        let channels = &self.mutable_state.lock().await.channels;

        for channel in channels.values() {
            channel.unsubscribe(self).await;
        }
    }

    pub fn is_connected(&self) -> bool {
        self.connection.is_some()
    }

    pub(crate) fn get_access_token(&self) -> &str {
        &self.access_token
    }

    pub async fn connect(&mut self) -> Result<()> {
        let url = format!("{}?apikey={}&vsn=1.0.0", self.url, self.api_key);

        if self.is_connected() {
            println!("Websocket client already connected.");
            return Ok(());
        }

        let mut backoff = self.initial_backoff;
        let mut last_error = None;

        println!("Attempting to connect to websocket at {url}.");

        for attempt in 0..self.max_retries {
            let mutable_state = self.mutable_state.clone();
            let message_received_callback = move |message: Message| {
                let mutable_state = mutable_state.clone();
                Box::pin(async move {
                    let channels = &mut mutable_state.lock().await.channels;
                    Self::on_receive(channels, message).await;
                }) as Pin<Box<dyn Future<Output = ()> + Send>>
            };

            // TODO: pass heartbeat interval option
            let result =
                RealtimeConnection::new(&url, Box::new(message_received_callback), None).await;
            match result {
                Ok(connection) => {
                    self.connection = Some(connection);
                    println!("Websockets connection established successfully.");
                    return Ok(());
                }
                Err(error) => {
                    println!("Connection attempt failed: {error}");
                    last_error = Some(error);

                    if !self.auto_reconnect {
                        println!("{}", self.auto_reconnect);
                        break;
                    }

                    let wait_time = backoff * (2.0 * attempt as f32);
                    println!(
                        "Retry {}/{}: Next attempt in {wait_time}s (backoff={backoff}s)",
                        attempt + 1,
                        self.max_retries
                    );
                    tokio::time::sleep(Duration::from_secs_f32(wait_time)).await;
                    backoff = f32::min(backoff * 2.0, 60.0);
                }
            }
        }

        let error = last_error.unwrap();
        println!(
            "Connection failed after {} attempts. Error {error}",
            self.max_retries
        );
        Err(error)
    }

    pub async fn close(self) -> Result<()> {
        if let Some(connection) = self.connection {
            return connection.close().await;
        }

        Ok(())
    }

    async fn on_receive(channels: &mut HashMap<String, RealtimeChannel>, message: Message) {
        let channel = channels.remove(&message.topic);

        if let Some(channel) = channel {
            let mut should_remove_channel = false;
            channel
                .trigger(
                    message.payload,
                    message.ref_field.as_deref(),
                    &mut should_remove_channel,
                )
                .await;

            if !should_remove_channel {
                channels.insert(message.topic, channel);
            }
        }
    }

    pub(crate) async fn make_ref(&self) -> String {
        let mut state = self.mutable_state.lock().await;
        state._ref += 1;
        state._ref.to_string()
    }

    pub(crate) fn send(&self, message: Message) -> Result<()> {
        if let Some(connection) = &self.connection {
            connection.send(message)
        } else {
            Err(RealtimeError::ConnectionClosed)
        }
    }
}
