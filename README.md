# supabase-realtime-rs

[![Native Tests](https://github.com/AFischbeck/supabase-realtime-rs/actions/workflows/native-tests.yml/badge.svg?branch=master)](https://github.com/AFischbeck/supabase-realtime-rs/actions/workflows/native-tests.yml)
[![Wasm Tests](https://github.com/AFischbeck/supabase-realtime-rs/actions/workflows/wasm-tests.yml/badge.svg?branch=master)](https://github.com/AFischbeck/supabase-realtime-rs/actions/workflows/wasm-tests.yml)

# Overview

This client enables you to use the following Supabase Realtime's features:

- **Broadcast**: send ephemeral messages from client to clients with minimal latency. Use cases include sharing cursor positions between users.
- **Presence**: track and synchronize shared ephemeral state across clients, such as who is currently online.
- WebAssembly target (wasm32-unknown-unknown) supported

# Usage

## Creating a Channel

```rust
async fn main():
use supabase_realtime_rs::{
    client::RealtimeClient,
    types::{Result, SubscribeState},
};

const REALTIME_URL: &str = "http://127.0.0.1:54321";
const API_KEY: &str = "12345";

async fn main() -> Result<()> {
    let mut client = RealtimeClient::new(REALTIME_URL, API_KEY, None, None, None)?;
    let mut channel = client.create_channel("test-channel", None).await;

    let on_subscribe = move |state_result: Result<SubscribeState>| match state_result {
        Ok(state) => match state {
            SubscribeState::Subscribed => {
                println!("Conencted!");
            }
            SubscribeState::TimedOut => {
                println!("Realtime server did not respond in time.");
            }
            SubscribeState::Closed => {
                println!("Realtime channel was unexpectedly closed.");
            }
        },
        Err(error) => {
            eprintln!("There was an error subscribing to channel: {error}");
        }
    };

    channel
        .subscribe(&mut client, Some(Box::new(on_subscribe)))
        .await?;

    Ok(())
}

```

### Notes:

- `REALTIME_URL` is `http://127.0.0.1:54321` when developing locally and `wss://<project_ref>.supabase.co/realtime/v1` when connecting to your Supabase project.
- `API_KEY` is a JWT whose claims must contain `exp` and `role` (existing database role).
- Channel name can be any `string`.


## Broadcast

Your client can send and receive messages based on the `event`.

```rust
    // Setup...

    channel.on_broadcast("some-event", |payload| println!("{}", payload)).await;

    let payload = Payload::Broadcast(Broadcast {
        event: String::from("some-event"),
        payload: json!({"message": format!("Hello world")}),
    });

    channel.send_broadcast(&client, "some-event", payload).await?;

```

## Cleanup

It is highly recommended that you clean up your channels after you're done with them.

- Remove a single channel

```rust
    // Setup...

    client.remove_channel(&channel).await?;
```
- Remove all channels
```rust
    // Setup...
    
    client.remove_all_channels().await?;
```


## Credits

This repo is modeled after [realtime-py](https://https://github.com/supabase/realtime-py) but with some differences due to the constraints of rust.
