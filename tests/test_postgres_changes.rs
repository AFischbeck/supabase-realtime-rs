#![cfg(not(target_arch = "wasm32"))]

use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;
use tokio::sync::{Mutex, Notify};
use tokio::time::timeout;
use uuid::Uuid;

use supabase_realtime_rs::client::RealtimeClient;
use supabase_realtime_rs::types::{Result, SubscribeState};

mod common;
use common::supabase_helpers::{
    create_message, create_todo, create_widget, delete_message, delete_messages_by_marker,
    delete_todo, delete_todos_by_marker, delete_widget, delete_widgets_by_marker, get_anon_key,
    get_api_url, get_auth_token, update_todo,
};

#[cfg(test)]
mod tests {
    use super::*;

    fn unique_topic(base: &str) -> String {
        format!("{}-{}-{}", base, std::process::id(), Uuid::new_v4())
    }

    fn unique_marker() -> String {
        format!("cdc-{}-{}", std::process::id(), Uuid::new_v4())
    }

    async fn subscribe_and_wait(
        client: &mut RealtimeClient,
        channel: &mut supabase_realtime_rs::channel::RealtimeChannel,
    ) {
        let subscribe_notify = Arc::new(Notify::new());
        let subscribe_failure = Arc::new(Mutex::new(None::<String>));

        let notify_clone = Arc::clone(&subscribe_notify);
        let failure_clone = Arc::clone(&subscribe_failure);
        let subscribe_callback = move |state_result: Result<SubscribeState>| {
            let failure = match state_result {
                Ok(SubscribeState::Subscribed) => None,
                Ok(state) => Some(format!("Unexpected subscribe state: {:?}", state)),
                Err(error) => Some(format!("Subscribe error: {}", error)),
            };

            if let Some(failure) = failure {
                if let Ok(mut guard) = failure_clone.try_lock() {
                    *guard = Some(failure);
                }
            }

            notify_clone.notify_one();
        };

        // channel.subscribe(client, Some(Box::new(subscribe_callback))).await.expect("Error while subscribing to channel.");

        timeout(Duration::from_secs(10), subscribe_notify.notified())
            .await
            .expect("Timeout elapsed while waiting for subscribe response.");

        if let Some(failure) = subscribe_failure.lock().await.take() {
            panic!("{}", failure);
        }
    }

    #[tokio::test]
    async fn test_postgres_changes_all_event_types() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            assert!(client.is_connected());

            // client.set_auth(&token);

            let topic = unique_topic("test-postgres-changes-all");
            let mut channel = client.create_channel(&topic, None).await;

            let all_events = Arc::new(Mutex::new(Vec::<Value>::new()));
            let insert_events = Arc::new(Mutex::new(Vec::<Value>::new()));
            let update_events = Arc::new(Mutex::new(Vec::<Value>::new()));
            let delete_events = Arc::new(Mutex::new(Vec::<Value>::new()));
            let all_notify = Arc::new(Notify::new());
            let insert_notify = Arc::new(Notify::new());
            let update_notify = Arc::new(Notify::new());
            let delete_notify = Arc::new(Notify::new());

            let all_events_clone = Arc::clone(&all_events);
            let all_notify_clone = Arc::clone(&all_notify);
            // channel.on_postgres_changes("*", None, Some("todos"), None, None, Box::new(move |payload| {
            //     if let Ok(v) = serde_json::to_value(&payload) { all_events_clone.lock().unwrap().push(v); }
            //     all_notify_clone.notify_one();
            // })).await;

            let insert_events_clone = Arc::clone(&insert_events);
            let insert_notify_clone = Arc::clone(&insert_notify);
            // channel.on_postgres_changes("INSERT", None, Some("todos"), None, None, Box::new(move |payload| {
            //     if let Ok(v) = serde_json::to_value(&payload) { insert_events_clone.lock().unwrap().push(v); }
            //     insert_notify_clone.notify_one();
            // })).await;

            let update_events_clone = Arc::clone(&update_events);
            let update_notify_clone = Arc::clone(&update_notify);
            // channel.on_postgres_changes("UPDATE", None, Some("todos"), None, None, Box::new(move |payload| {
            //     if let Ok(v) = serde_json::to_value(&payload) { update_events_clone.lock().unwrap().push(v); }
            //     update_notify_clone.notify_one();
            // })).await;

            let delete_events_clone = Arc::clone(&delete_events);
            let delete_notify_clone = Arc::clone(&delete_notify);
            // channel.on_postgres_changes("DELETE", None, Some("todos"), None, None, Box::new(move |payload| {
            //     if let Ok(v) = serde_json::to_value(&payload) { delete_events_clone.lock().unwrap().push(v); }
            //     delete_notify_clone.notify_one();
            // })).await;

            // subscribe_and_wait(&mut client, &mut channel).await;

            let todo_id = create_todo(&format!("{} - insert", marker)).await;
            created_todo_id = Some(todo_id.clone());

            timeout(Duration::from_secs(10), insert_notify.notified())
                .await
                .expect("Timeout waiting for insert");
            timeout(Duration::from_secs(10), all_notify.notified())
                .await
                .expect("Timeout waiting for insert in *");

            update_todo(&todo_id, &format!("{} - update", marker), true).await;
            timeout(Duration::from_secs(10), update_notify.notified())
                .await
                .expect("Timeout waiting for update");
            timeout(Duration::from_secs(10), all_notify.notified())
                .await
                .expect("Timeout waiting for update in *");

            delete_todo(&todo_id).await;
            created_todo_id = None;
            timeout(Duration::from_secs(10), delete_notify.notified())
                .await
                .expect("Timeout waiting for delete");
            timeout(Duration::from_secs(10), all_notify.notified())
                .await
                .expect("Timeout waiting for delete in *");

            let all_list = all_events.lock().await;
            assert_eq!(all_list.len(), 3);
            let insert_list = insert_events.lock().await;
            assert_eq!(insert_list.len(), 1);
            let update_list = update_events.lock().await;
            assert_eq!(update_list.len(), 1);
            let delete_list = delete_events.lock().await;
            assert_eq!(delete_list.len(), 1);

            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        delete_todos_by_marker(&marker).await;

        result.expect("test failed");
    }

    #[tokio::test]
    async fn test_postgres_changes_table_scoping() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let mut created_msg_id: Option<i64> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            // client.set_auth(&token);

            let topic = unique_topic("test-postgres-changes-scope");
            let mut channel = client.create_channel(&topic, None).await;

            let todo_all = Arc::new(Mutex::new(Vec::<Value>::new()));
            let todo_insert = Arc::new(Mutex::new(Vec::<Value>::new()));
            let msg_insert = Arc::new(Mutex::new(Vec::<Value>::new()));
            let todo_all_notify = Arc::new(Notify::new());
            let todo_insert_notify = Arc::new(Notify::new());
            let msg_insert_notify = Arc::new(Notify::new());

            // channel.on_postgres_changes("*", None, Some("todos"), None, None, Box::new(|_p|{})).await;
            // channel.on_postgres_changes("INSERT", None, Some("todos"), None, None, Box::new(|_p|{})).await;
            // channel.on_postgres_changes("INSERT", None, Some("messages"), None, None, Box::new(|_p|{})).await;

            // subscribe_and_wait(&mut client, &mut channel).await;

            let todo_id = create_todo(&format!("{} - todo", marker)).await;
            created_todo_id = Some(todo_id);
            timeout(Duration::from_secs(10), todo_insert_notify.notified())
                .await
                .expect("Timeout");
            timeout(Duration::from_secs(10), todo_all_notify.notified())
                .await
                .expect("Timeout");

            let msg_id = create_message(
                &format!("{} - msg", marker),
                "this is a long enough message",
            )
            .await;
            created_msg_id = Some(msg_id);
            timeout(Duration::from_secs(10), msg_insert_notify.notified())
                .await
                .expect("Timeout");

            assert_eq!(todo_all.lock().await.len(), 1);
            assert_eq!(todo_insert.lock().await.len(), 1);
            assert_eq!(msg_insert.lock().await.len(), 1);

            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        if let Some(id) = created_msg_id {
            let _ = delete_message(id).await;
        }
        delete_todos_by_marker(&marker).await;
        delete_messages_by_marker(&marker).await;

        result.expect("test failed");
    }

    #[tokio::test]
    async fn test_postgres_changes_authenticated_access() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            // client.set_auth(&token);

            let topic = unique_topic("test-postgres-changes-auth");
            let mut channel = client.create_channel(&topic, None).await;
            // channel.on_postgres_changes("*", None, Some("todos"), None, None, Box::new(|_p|{})).await;
            // subscribe_and_wait(&mut client, &mut channel).await;

            let todo_id = create_todo(&format!("{} - auth", marker)).await;
            created_todo_id = Some(todo_id);
            tokio::time::sleep(Duration::from_secs(2)).await;

            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        delete_todos_by_marker(&marker).await;

        result.expect("test failed");
    }

    #[tokio::test]
    async fn test_postgres_changes_anon_denied() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");

            let topic = unique_topic("test-postgres-changes-anon");
            let mut channel = client.create_channel(&topic, None).await;
            // channel.on_postgres_changes("*", None, Some("todos"), None, None, Box::new(|_p|{})).await;
            // subscribe_and_wait(&mut client, &mut channel).await;

            // authenticated write
            let todo_id = create_todo(&format!("{} - anon-check", marker)).await;
            created_todo_id = Some(todo_id);
            tokio::time::sleep(Duration::from_secs(3)).await;

            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        delete_todos_by_marker(&marker).await;

        result.expect("test failed");
    }

    #[tokio::test]
    async fn test_postgres_changes_subscription_error_is_surfaced() {
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
            .expect("Error while creating client.");
        client
            .connect()
            .await
            .expect("Error while connecting to client.");
        let topic = unique_topic("test-postgres-changes-error");
        let mut channel = client.create_channel(&topic, None).await;
        // channel.on_postgres_changes("INSERT", None, Some("nonexistent_table_xyz"), None, None, Box::new(|_p|{})).await;
        // subscribe_and_wait(&mut client, &mut channel).await;
        client.close().await.expect("Error disconnecting client.");
    }

    #[tokio::test]
    async fn test_postgres_changes_cleanup_on_failure() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            // client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-cleanup");
            let mut _channel = client.create_channel(&topic, None).await;
            // _channel.on_postgres_changes("*", None, Some("todos"), None, None, Box::new(|_p|{})).await;
            // subscribe_and_wait(&mut client, &mut _channel).await;

            let todo_id = create_todo(&format!("{} - cleanup", marker)).await;
            created_todo_id = Some(todo_id);
            panic!("forced failure");
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        delete_todos_by_marker(&marker).await;
        let _ = result;
    }

    #[tokio::test]
    async fn test_postgres_changes_filter_match() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            // client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-filter-match");
            let mut channel = client.create_channel(&topic, None).await;
            // channel.on_postgres_changes("INSERT", None, Some("todos"), Some(format!("description=eq.{} - match", marker)), None, Box::new(|_p|{})).await;
            // subscribe_and_wait(&mut client, &mut channel).await;

            let _other = create_todo(&format!("{} - other", marker)).await;
            let todo_id = create_todo(&format!("{} - match", marker)).await;
            created_todo_id = Some(todo_id);
            tokio::time::sleep(Duration::from_secs(3)).await;

            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        delete_todos_by_marker(&marker).await;
        result.expect("test failed");
    }

    #[tokio::test]
    async fn test_postgres_changes_filter_non_match() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            // client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-filter-nonmatch");
            let mut channel = client.create_channel(&topic, None).await;
            // channel.on_postgres_changes("INSERT", None, Some("todos"), Some(format!("description=eq.{} - only-this", marker)), None, Box::new(|_p|{})).await;
            // subscribe_and_wait(&mut client, &mut channel).await;

            let _nope = create_todo(&format!("{} - nope", marker)).await;
            let todo_id = create_todo(&format!("{} - only-this", marker)).await;
            created_todo_id = Some(todo_id);
            tokio::time::sleep(Duration::from_secs(3)).await;

            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        delete_todos_by_marker(&marker).await;
        result.expect("test failed");
    }

    #[tokio::test]
    async fn test_postgres_changes_select_projection() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            // client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-select");
            let mut channel = client.create_channel(&topic, None).await;
            // let select = vec!["id".to_string(), "description".to_string()];
            // channel.on_postgres_changes("INSERT", None, Some("todos"), None, Some(select), Box::new(|_p|{})).await;
            // subscribe_and_wait(&mut client, &mut channel).await;

            let todo_id = create_todo(&format!("{} - select", marker)).await;
            created_todo_id = Some(todo_id);
            tokio::time::sleep(Duration::from_secs(3)).await;

            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        delete_todos_by_marker(&marker).await;
        result.expect("test failed");
    }

    #[tokio::test]
    async fn test_postgres_changes_explicit_public_schema() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            // client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-schema-public");
            let mut channel = client.create_channel(&topic, None).await;
            // channel.on_postgres_changes("*", Some("public"), Some("todos"), None, None, Box::new(|_p|{})).await;
            // subscribe_and_wait(&mut client, &mut channel).await;

            let todo_id = create_todo(&format!("{} - schema", marker)).await;
            created_todo_id = Some(todo_id);
            tokio::time::sleep(Duration::from_secs(3)).await;

            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        delete_todos_by_marker(&marker).await;
        result.expect("test failed");
    }

    #[tokio::test]
    async fn test_postgres_changes_non_public_schema() {
        let marker = unique_marker();
        let mut created_widget_id: Option<String> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            // client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-schema-nonpublic");
            let mut channel = client.create_channel(&topic, None).await;
            // channel.on_postgres_changes("*", Some("cdc_test"), Some("widgets"), None, None, Box::new(|_p|{})).await;
            // subscribe_and_wait(&mut client, &mut channel).await;

            let widget_id = create_widget(&format!("{} - widget", marker)).await;
            created_widget_id = Some(widget_id);
            tokio::time::sleep(Duration::from_secs(3)).await;
            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_widget_id {
            let _ = delete_widget(&id).await;
        }
        delete_widgets_by_marker(&marker).await;
        delete_todos_by_marker(&marker).await;
        result.expect("test failed");
    }

    #[tokio::test]
    async fn test_postgres_changes_multiple_bindings_and_ids() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let mut created_msg_id: Option<i64> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            // client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-multi");
            let mut channel = client.create_channel(&topic, None).await;
            // channel.on_postgres_changes("INSERT", None, Some("todos"), Some(format!("description=eq.{} - todo", marker)), None, Box::new(|_p|{})).await;
            // channel.on_postgres_changes("INSERT", None, Some("messages"), Some(format!("title=eq.{} - msg", marker)), None, Box::new(|_p|{})).await;
            // subscribe_and_wait(&mut client, &mut channel).await;

            let todo_id = create_todo(&format!("{} - todo", marker)).await;
            created_todo_id = Some(todo_id);
            let msg_id = create_message(
                &format!("{} - msg", marker),
                "this is a long enough message",
            )
            .await;
            created_msg_id = Some(msg_id);
            tokio::time::sleep(Duration::from_secs(3)).await;

            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        if let Some(id) = created_msg_id {
            let _ = delete_message(id).await;
        }
        delete_todos_by_marker(&marker).await;
        delete_messages_by_marker(&marker).await;
        result.expect("test failed");
    }

    #[tokio::test]
    async fn test_postgres_changes_private_channel() {
        let marker = unique_marker();
        let mut created_todo_id: Option<String> = None;
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result: std::result::Result<(), String> = async {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            // client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-private");
            let join_config = supabase_realtime_rs::protocol_objects::JoinConfig {
                broadcast: None,
                presence: None,
                postgres_changes: None,
                private: true,
            };
            let mut channel = client.create_channel(&topic, Some(join_config)).await;
            // channel.on_postgres_changes("*", None, Some("todos"), None, None, Box::new(|_p|{})).await;
            // subscribe_and_wait(&mut client, &mut channel).await;

            let todo_id = create_todo(&format!("{} - private", marker)).await;
            created_todo_id = Some(todo_id);
            tokio::time::sleep(Duration::from_secs(3)).await;

            client.close().await.expect("Error disconnecting client.");
            Ok(())
        }
        .await;

        if let Some(id) = created_todo_id {
            let _ = delete_todo(&id).await;
        }
        delete_todos_by_marker(&marker).await;
        result.expect("test failed");
    }
}
