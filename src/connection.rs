use futures::{
    SinkExt, StreamExt,
    stream::{SplitSink, SplitStream},
};
use tokio::{
    net::TcpStream,
    sync::mpsc::{UnboundedReceiver, UnboundedSender},
    task::AbortHandle,
    time::{Interval, interval},
};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async, tungstenite::Message as TMessage,
};
use tokio_util::sync::CancellationToken;

use crate::{
    protocol_objects::{Heartbeat, Message, Payload},
    types::{ConnectionMessageReceivedEvent, DEFAULT_HEARTBEAT_INTERVAL, Result},
};

#[derive(Clone)]
pub struct RealtimeConnection {
    sender: UnboundedSender<Message>,
    listen_join_handle: AbortHandle,
    send_join_handle: AbortHandle,
    heartbeat_join_handle: AbortHandle,
    cancellation_token: CancellationToken,
}

impl RealtimeConnection {
    pub async fn new(
        url: &str,
        message_received_callback: ConnectionMessageReceivedEvent,
        heartbeat_interval: Option<Interval>,
    ) -> Result<Self> {
        let heartbeat_interval = heartbeat_interval.unwrap_or(interval(DEFAULT_HEARTBEAT_INTERVAL));

        let (ws_stream, _) = connect_async(url).await?;
        let (ws_sender, ws_receiver) = ws_stream.split();
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();

        let cancellation_token = CancellationToken::new();

        // TODO: need to handle errors for join handles
        let listen_join_handle = tokio::spawn(Self::listen(
            ws_receiver,
            message_received_callback,
            cancellation_token.clone(),
        ));
        let send_join_handle = tokio::spawn(Self::ws_send_loop(
            receiver,
            ws_sender,
            cancellation_token.clone(),
        ));

        let heartbeat_join_handle = tokio::spawn(Self::heartbeat(
            sender.clone(),
            cancellation_token.clone(),
            heartbeat_interval,
        ));

        Ok(Self {
            sender,
            listen_join_handle: listen_join_handle.abort_handle(),
            send_join_handle: send_join_handle.abort_handle(),
            heartbeat_join_handle: heartbeat_join_handle.abort_handle(),
            cancellation_token,
        })
    }

    // TODO: use join handles and return result with MultipleTaskErrors
    pub async fn close(self) -> Result<()> {
        self.cancellation_token.cancel();

        // let results = tokio::try_join!(self.listen_join_handle, self.heartbeat_join_handle)?;

        self.listen_join_handle.abort();
        self.send_join_handle.abort();
        self.heartbeat_join_handle.abort();

        // let mut task_errors = vec![];

        // if let Err(error) = results.0 {
        //     task_errors.push(error);
        // }
        // if let Err(error) = results.1 {
        //     task_errors.push(error);
        // }

        // if !task_errors.is_empty() {
        //     return Err(RealtimeError::MultipleTaskErrors {
        //         errors: task_errors,
        //     });
        // }

        Ok(())
    }

    async fn listen(
        mut ws_receiver: SplitStream<WebSocketStream<MaybeTlsStream<TcpStream>>>,
        message_received_callback: ConnectionMessageReceivedEvent,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                received = ws_receiver.next() => {
                    Self::handle_receive(received, &message_received_callback).await?;
                }

                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
        }

        Ok(())
    }

    async fn ws_send_loop(
        mut receiver: UnboundedReceiver<Message>,
        mut ws_sender: SplitSink<WebSocketStream<MaybeTlsStream<TcpStream>>, TMessage>,
        cancellation_token: CancellationToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                message = receiver.recv() => {
                    if let Some(message) = message {
                        let tmessage = Self::message_to_tmessage(&message)?;
                        ws_sender.send(tmessage).await?;
                    }else {
                        println!("connection: canceling due to None received from receiver.");
                        cancellation_token.cancel();
                        break;
                    }
                },

                _ = cancellation_token.cancelled() => {
                    break;
                }
            }
        }

        let _ = ws_sender.close().await;
        Ok(())
    }

    async fn heartbeat(
        sender: UnboundedSender<Message>,
        cancellation_token: CancellationToken,
        mut interval: Interval,
    ) -> Result<()> {
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let heartbeat_message = Message {
                        topic: String::from("phoenix"),
                        payload: Payload::Heartbeat(Heartbeat {}),
                        ref_field: None,
                    };
                    sender.send(heartbeat_message).unwrap(); // TODO: error handling?
                },

                _ = cancellation_token.cancelled() => {
                    return Ok(())
                }
            }
        }
    }

    pub(crate) fn send(&self, message: Message) -> Result<()> {
        Ok(self.sender.send(message)?)
    }

    async fn handle_receive(
        received: Option<std::result::Result<TMessage, tokio_tungstenite::tungstenite::Error>>,
        message_received_callback: &ConnectionMessageReceivedEvent,
    ) -> Result<()> {
        match received {
            Some(received) => match received {
                Ok(message) => match message {
                    TMessage::Text(text) => {
                        let message = Self::tmessage_text_to_message(&text)?;
                        // if matches!(
                        //     message.payload,
                        //     Payload::PhxReply(PhxReply::Ok(PhxResponse {
                        //         postgres_changes: _
                        //     }))
                        // ) {
                        //     println!("Skipping postgres changes reply");
                        //     Ok(())
                        // } else {
                        message_received_callback(message).await;
                        Ok(())
                        // }
                    }
                    TMessage::Close(_) => Ok(()),
                    _ => Ok(()),
                },
                Err(error) => Err(error.into()),
            },
            None => Ok(()),
        }
    }

    fn message_to_tmessage(message: &Message) -> Result<TMessage> {
        Ok(TMessage::Text(serde_json::to_string(message)?))
    }

    fn tmessage_text_to_message(tmessage: &str) -> Result<Message> {
        Ok(serde_json::from_str(tmessage)?)
    }
}
