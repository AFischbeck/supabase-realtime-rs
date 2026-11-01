#![cfg(not(target_arch = "wasm32"))]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;
use tokio::sync::Notify;
use tokio::time::timeout;
use uuid::Uuid;

use supabase_realtime_rs::client::RealtimeClient;
use supabase_realtime_rs::types::{PostgresChangesParams, Result, SubscribeState};

mod common;
use common::supabase_helpers::{
    create_message, create_todo, create_widget, delete_message, delete_messages_by_marker,
    delete_todo, delete_todos_by_marker, delete_widget, delete_widgets_by_marker, get_anon_key,
    get_api_url, get_auth_token, update_todo,
};

#[cfg(test)]
mod tests {
    use supabase_realtime_rs::protocol_objects::JoinPostgresChangedEvent;

    use super::*;

    fn unique_topic(base: &str) -> String {
        format!("{}-{}-{}", base, std::process::id(), Uuid::new_v4())
    }

    fn unique_marker() -> String {
        format!("cdc-{}-{}", std::process::id(), Uuid::new_v4())
    }

    fn todo_matches(
        payload: &supabase_realtime_rs::protocol_objects::PostgresChangesPayload,
        marker: &str,
        todo_id: Option<&str>,
        check_record: bool,
        check_old_record: bool,
    ) -> bool {
        if let Some(id) = todo_id {
            let matches_id = (check_record
                && payload
                    .data
                    .record
                    .as_ref()
                    .and_then(|r| r.get("id"))
                    .and_then(|v| v.as_str())
                    == Some(id))
                || (check_old_record
                    && payload
                        .data
                        .old_record
                        .as_ref()
                        .and_then(|r| r.get("id"))
                        .and_then(|v| v.as_str())
                        == Some(id));
            if matches_id {
                return true;
            }
        }

        let matches_marker = (check_record
            && payload
                .data
                .record
                .as_ref()
                .and_then(|r| r.get("description"))
                .and_then(|d| d.as_str())
                .is_some_and(|d| d.contains(marker)))
            || (check_old_record
                && payload
                    .data
                    .old_record
                    .as_ref()
                    .and_then(|r| r.get("description"))
                    .and_then(|d| d.as_str())
                    .is_some_and(|d| d.contains(marker)));
        matches_marker
    }

    fn record_matches_field(
        payload: &supabase_realtime_rs::protocol_objects::PostgresChangesPayload,
        marker: &str,
        field: &str,
        check_record: bool,
        check_old_record: bool,
    ) -> bool {
        (check_record
            && payload
                .data
                .record
                .as_ref()
                .and_then(|r| r.get(field))
                .and_then(|d| d.as_str())
                .is_some_and(|d| d.contains(marker)))
            || (check_old_record
                && payload
                    .data
                    .old_record
                    .as_ref()
                    .and_then(|r| r.get(field))
                    .and_then(|d| d.as_str())
                    .is_some_and(|d| d.contains(marker)))
    }

    async fn register_filtered_callback(
        channel: &mut supabase_realtime_rs::channel::RealtimeChannel,
        event: JoinPostgresChangedEvent,
        table: &str,
        events: Arc<Mutex<Vec<Value>>>,
        notify: Arc<Notify>,
        matcher: Arc<
            dyn Fn(&supabase_realtime_rs::protocol_objects::PostgresChangesPayload) -> bool
                + Send
                + Sync
                + 'static,
        >,
    ) {
        channel
            .on_postgres_changes(
                event,
                PostgresChangesParams::default().table(table),
                move |payload| {
                    if matcher(&payload) {
                        if let Ok(v) = serde_json::to_value(&payload) {
                            events.lock().unwrap().push(v);
                        }
                        notify.notify_one();
                    }
                },
            )
            .await
            .expect("Failed to register postgres changes callback");
    }

    async fn register_todo_callback(
        channel: &mut supabase_realtime_rs::channel::RealtimeChannel,
        event: JoinPostgresChangedEvent,
        events: Arc<Mutex<Vec<Value>>>,
        notify: Arc<Notify>,
        marker: String,
        todo_id_filter: Arc<Mutex<Option<String>>>,
        check_record: bool,
        check_old_record: bool,
    ) {
        let matcher: Arc<
            dyn Fn(&supabase_realtime_rs::protocol_objects::PostgresChangesPayload) -> bool
                + Send
                + Sync
                + 'static,
        > = Arc::new(move |payload| {
            let todo_id = todo_id_filter.lock().unwrap().clone();
            todo_matches(payload, &marker, todo_id.as_deref(), check_record, check_old_record)
        });
        register_filtered_callback(channel, event, "todos", events, notify, matcher).await;
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

        channel
            .subscribe(client, Some(Box::new(subscribe_callback)))
            .await
            .expect("Error while subscribing to channel.");

        timeout(Duration::from_secs(10), subscribe_notify.notified())
            .await
            .expect("Timeout elapsed while waiting for subscribe response.");

        if let Some(failure) = subscribe_failure.lock().unwrap().take() {
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

            client.set_auth(&token);

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

            let todo_id_filter = Arc::new(Mutex::new(Option::<String>::None));
            register_todo_callback(
                &mut channel,
                JoinPostgresChangedEvent::All,
                Arc::clone(&all_events),
                Arc::clone(&all_notify),
                marker.clone(),
                Arc::clone(&todo_id_filter),
                true,
                true,
            )
            .await;
            register_todo_callback(
                &mut channel,
                JoinPostgresChangedEvent::Insert,
                Arc::clone(&insert_events),
                Arc::clone(&insert_notify),
                marker.clone(),
                Arc::clone(&todo_id_filter),
                true,
                false,
            )
            .await;
            register_todo_callback(
                &mut channel,
                JoinPostgresChangedEvent::Update,
                Arc::clone(&update_events),
                Arc::clone(&update_notify),
                marker.clone(),
                Arc::clone(&todo_id_filter),
                true,
                false,
            )
            .await;
            register_todo_callback(
                &mut channel,
                JoinPostgresChangedEvent::Delete,
                Arc::clone(&delete_events),
                Arc::clone(&delete_notify),
                marker.clone(),
                Arc::clone(&todo_id_filter),
                true,
                true,
            )
            .await;

            subscribe_and_wait(&mut client, &mut channel).await;

            let todo_id = create_todo(&format!("{} - insert", marker)).await;
            created_todo_id = Some(todo_id.clone());
            *todo_id_filter.lock().unwrap() = Some(todo_id.clone());

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

            let all_list = all_events.lock().unwrap();
            assert_eq!(all_list.len(), 3);
            let insert_list = insert_events.lock().unwrap();
            assert_eq!(insert_list.len(), 1);
            let update_list = update_events.lock().unwrap();
            assert_eq!(update_list.len(), 1);
            let delete_list = delete_events.lock().unwrap();
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
            client.set_auth(&token);

            let topic = unique_topic("test-postgres-changes-scope");
            let mut channel = client.create_channel(&topic, None).await;

            let todo_all = Arc::new(Mutex::new(Vec::<Value>::new()));
            let todo_insert = Arc::new(Mutex::new(Vec::<Value>::new()));
            let msg_insert = Arc::new(Mutex::new(Vec::<Value>::new()));
            let todo_all_notify = Arc::new(Notify::new());
            let todo_insert_notify = Arc::new(Notify::new());
            let msg_insert_notify = Arc::new(Notify::new());

            let marker_clone = marker.clone();
            let matcher: Arc<dyn Fn(&supabase_realtime_rs::protocol_objects::PostgresChangesPayload) -> bool + Send + Sync + 'static> =
                Arc::new(move |payload| record_matches_field(payload, &marker_clone, "description", true, true));
            register_filtered_callback(
                &mut channel,
                JoinPostgresChangedEvent::All,
                "todos",
                Arc::clone(&todo_all),
                Arc::clone(&todo_all_notify),
                Arc::clone(&matcher),
            )
            .await;

            let marker_clone = marker.clone();
            let matcher: Arc<dyn Fn(&supabase_realtime_rs::protocol_objects::PostgresChangesPayload) -> bool + Send + Sync + 'static> =
                Arc::new(move |payload| record_matches_field(payload, &marker_clone, "description", true, false));
            register_filtered_callback(
                &mut channel,
                JoinPostgresChangedEvent::Insert,
                "todos",
                Arc::clone(&todo_insert),
                Arc::clone(&todo_insert_notify),
                matcher,
            )
            .await;

            let marker_clone = marker.clone();
            let matcher: Arc<dyn Fn(&supabase_realtime_rs::protocol_objects::PostgresChangesPayload) -> bool + Send + Sync + 'static> =
                Arc::new(move |payload| record_matches_field(payload, &marker_clone, "title", true, false));
            register_filtered_callback(
                &mut channel,
                JoinPostgresChangedEvent::Insert,
                "messages",
                Arc::clone(&msg_insert),
                Arc::clone(&msg_insert_notify),
                matcher,
            )
            .await;

            subscribe_and_wait(&mut client, &mut channel).await;

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

            assert_eq!(todo_all.lock().unwrap().len(), 1);
            assert_eq!(todo_insert.lock().unwrap().len(), 1);
            assert_eq!(msg_insert.lock().unwrap().len(), 1);

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
            client.set_auth(&token);

            let topic = unique_topic("test-postgres-changes-auth");
            let mut channel = client.create_channel(&topic, None).await;
            channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::All,
                    PostgresChangesParams::default().table("todos"),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            subscribe_and_wait(&mut client, &mut channel).await;

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
            channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::All,
                    PostgresChangesParams::default().table("todos"),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            subscribe_and_wait(&mut client, &mut channel).await;

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
        channel
            .on_postgres_changes(
                JoinPostgresChangedEvent::Insert,
                PostgresChangesParams::default().table("nonexistent_table_xyz"),
                |_p| {},
            )
            .await
            .expect("Failed to register postgres changes callback");
        subscribe_and_wait(&mut client, &mut channel).await;
        client.close().await.expect("Error disconnecting client.");
    }

    #[tokio::test]
    async fn test_postgres_changes_cleanup_on_failure() {
        use futures::FutureExt;
        use std::panic;

        let marker = unique_marker();
        let marker_clone = marker.clone();
        let created_todo_id = Arc::new(tokio::sync::Mutex::new(None::<String>));
        let created_todo_id_clone = Arc::clone(&created_todo_id);
        let api_url = get_api_url().to_string();
        let anon_key = get_anon_key().to_string();
        let token = get_auth_token().await.to_string();

        let result = panic::AssertUnwindSafe(async move {
            let mut client = RealtimeClient::new(&api_url, &anon_key, None, None, None)
                .expect("Error while creating client.");
            client
                .connect()
                .await
                .expect("Error while connecting to client.");
            client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-cleanup");
            let mut _channel = client.create_channel(&topic, None).await;
            _channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::All,
                    PostgresChangesParams::default().table("todos"),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            subscribe_and_wait(&mut client, &mut _channel).await;

            let todo_id = create_todo(&format!("{} - cleanup", marker_clone)).await;
            *created_todo_id_clone.lock().await = Some(todo_id);
            panic!("forced failure");
        })
        .catch_unwind()
        .await;

        if let Some(id) = created_todo_id.lock().await.take() {
            let _ = delete_todo(&id).await;
        }
        delete_todos_by_marker(&marker).await;
        assert!(result.is_err());
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
            client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-filter-match");
            let mut channel = client.create_channel(&topic, None).await;
            channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::Insert,
                    PostgresChangesParams::default()
                        .table("todos")
                        .filter(format!("description=eq.{} - match", marker)),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            subscribe_and_wait(&mut client, &mut channel).await;

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
            client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-filter-nonmatch");
            let mut channel = client.create_channel(&topic, None).await;
            channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::Insert,
                    PostgresChangesParams::default()
                        .table("todos")
                        .filter(format!("description=eq.{} - only-this", marker)),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            subscribe_and_wait(&mut client, &mut channel).await;

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
            client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-select");
            let mut channel = client.create_channel(&topic, None).await;
            let select = vec!["id".to_string(), "description".to_string()];
            channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::Insert,
                    PostgresChangesParams::default()
                        .table("todos")
                        .select(select),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            subscribe_and_wait(&mut client, &mut channel).await;

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
            client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-schema-public");
            let mut channel = client.create_channel(&topic, None).await;
            channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::All,
                    PostgresChangesParams::default()
                        .schema("public")
                        .table("todos"),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            subscribe_and_wait(&mut client, &mut channel).await;

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
            client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-schema-nonpublic");
            let mut channel = client.create_channel(&topic, None).await;
            channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::All,
                    PostgresChangesParams::default()
                        .schema("cdc_test")
                        .table("widgets"),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            subscribe_and_wait(&mut client, &mut channel).await;

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
            client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-multi");
            let mut channel = client.create_channel(&topic, None).await;
            channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::Insert,
                    PostgresChangesParams::default()
                        .table("todos")
                        .filter(format!("description=eq.{} - todo", marker)),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::Insert,
                    PostgresChangesParams::default()
                        .table("messages")
                        .filter(format!("title=eq.{} - msg", marker)),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            subscribe_and_wait(&mut client, &mut channel).await;

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
    #[ignore = "Todo: Update RLS on realtime.messages and update supabase config for this test"]
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
            client.set_auth(&token);
            let topic = unique_topic("test-postgres-changes-private");
            let join_config = supabase_realtime_rs::protocol_objects::JoinConfig {
                broadcast: None,
                presence: None,
                postgres_changes: None,
                private: true,
            };
            let mut channel = client.create_channel(&topic, Some(join_config)).await;
            channel
                .on_postgres_changes(
                    JoinPostgresChangedEvent::All,
                    PostgresChangesParams::default().table("todos"),
                    |_p| {},
                )
                .await
                .expect("Failed to register postgres changes callback");
            subscribe_and_wait(&mut client, &mut channel).await;

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
