#![cfg(not(target_arch = "wasm32"))]

use std::time::{SystemTime, UNIX_EPOCH};

use dotenv::dotenv;
use once_cell::sync::Lazy;
use postgrest::Postgrest;
use supabase_auth::models::{AuthClient, EmailSignUpResult};
use tokio::sync::OnceCell;
use url::Url;

use super::defaults::{DEFAULT_KEY, DEFAULT_URL};

#[derive(Debug, Clone)]
pub struct TestConfig {
    pub api_url: String,
    pub anon_key: String,
    pub password: String,
}

static TEST_CONFIG: Lazy<TestConfig> = Lazy::new(|| {
    dotenv().ok();
    let api_url = std::env::var("REALTIME_TEST_URL")
        .or_else(|_| std::env::var("SUPABASE_URL"))
        .unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let anon_key = std::env::var("REALTIME_TEST_ANON_KEY")
        .or_else(|_| std::env::var("SUPABASE_ANON_KEY"))
        .unwrap_or_else(|_| DEFAULT_KEY.to_owned());
    let password =
        std::env::var("REALTIME_TEST_PASSWORD").unwrap_or_else(|_| "test.123".to_owned());

    if std::env::var("REALTIME_TEST_ALLOW_REMOTE").as_deref() != Ok("1") {
        if let Ok(parsed) = Url::parse(&api_url) {
            if let Some(host) = parsed.host_str() {
                let is_loopback = host == "127.0.0.1" || host == "localhost" || host == "::1";
                if !is_loopback {
                    panic!(
                        "Refusing to run Postgres CDC tests against non-loopback host '{}'. Set REALTIME_TEST_ALLOW_REMOTE=1 to override.",
                        host
                    );
                }
            }
        }
    }

    TestConfig {
        api_url,
        anon_key,
        password,
    }
});

static AUTH_TOKEN: OnceCell<String> = OnceCell::const_new();

fn build_postgrest() -> Postgrest {
    let config = &*TEST_CONFIG;
    Postgrest::new(format!("{}/rest/v1", config.api_url)).insert_header("apikey", &config.anon_key)
}

pub async fn get_auth_token() -> &'static str {
    AUTH_TOKEN
        .get_or_init(|| async {
            let config = &*TEST_CONFIG;
            let jwt_secret = std::env::var("SUPABASE_JWT_SECRET").unwrap_or_else(|_| {
                "super-secret-jwt-token-with-at-least-32-characters-long".to_string()
            });
            let auth_client = AuthClient::new(&config.api_url, &config.anon_key, jwt_secret);
            let email = format!(
                "test_{}_{}@example.com",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs()
            );
            match auth_client
                .sign_up_with_email_and_password(&email, &config.password, None)
                .await
            {
                Ok(EmailSignUpResult::SessionResult(session)) => session.access_token,
                Ok(EmailSignUpResult::ConfirmationResult(_)) => {
                    panic!("Email confirmation required but tests expect direct access token");
                }
                Err(e) => {
                    panic!("Failed to signup for Postgres CDC tests: {:?}", e);
                }
            }
        })
        .await
}

pub fn get_anon_key() -> &'static str {
    &TEST_CONFIG.anon_key
}

pub fn get_api_url() -> &'static str {
    &TEST_CONFIG.api_url
}

pub async fn create_todo(description: &str) -> String {
    let token = get_auth_token().await;
    let client = build_postgrest();
    let body = serde_json::json!({
        "description": description,
        "is_completed": false,
    });
    let resp = client
        .from("todos")
        .auth(token)
        .insert(body.to_string())
        .select("id")
        .execute()
        .await
        .expect("Failed to create todo");
    let text = resp.text().await.expect("Failed to read todo response");
    let v: serde_json::Value = serde_json::from_str(&text).expect("Failed to parse todo response");
    if let Some(arr) = v.as_array() {
        if let Some(first) = arr.first() {
            if let Some(id) = first.get("id").and_then(|x| x.as_str()) {
                return id.to_string();
            }
        }
    }
    if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
        return id.to_string();
    }
    panic!("Failed to parse todo id from response: {}", text);
}

pub async fn update_todo(id: &str, description: &str, is_completed: bool) {
    let token = get_auth_token().await;
    let client = build_postgrest();
    let body = serde_json::json!({
        "description": description,
        "is_completed": is_completed,
    });
    let resp = client
        .from("todos")
        .auth(token)
        .eq("id", id)
        .update(body.to_string())
        .execute()
        .await
        .expect("Failed to update todo");
    let _ = resp;
}

pub async fn delete_todo(id: &str) {
    let token = get_auth_token().await;
    let client = build_postgrest();
    let resp = client
        .from("todos")
        .auth(token)
        .eq("id", id)
        .delete()
        .execute()
        .await
        .expect("Failed to delete todo");
    let _ = resp;
}

pub async fn create_message(title: &str, message: &str) -> i64 {
    let token = get_auth_token().await;
    let client = build_postgrest();
    let body = serde_json::json!({
        "title": title,
        "message": message,
    });
    let resp = client
        .from("messages")
        .auth(token)
        .insert(body.to_string())
        .select("id")
        .execute()
        .await
        .expect("Failed to create message");
    let text = resp.text().await.expect("Failed to read message response");
    let v: serde_json::Value =
        serde_json::from_str(&text).expect("Failed to parse message response");
    if let Some(arr) = v.as_array() {
        if let Some(first) = arr.first() {
            if let Some(id) = first.get("id").and_then(|x| x.as_i64()) {
                return id;
            }
            if let Some(id) = first.get("id").and_then(|x| x.as_u64()) {
                return id as i64;
            }
        }
    }
    if let Some(id) = v.get("id").and_then(|x| x.as_i64()) {
        return id;
    }
    if let Some(id) = v.get("id").and_then(|x| x.as_u64()) {
        return id as i64;
    }
    panic!("Failed to parse message id from response: {}", text);
}

pub async fn delete_message(id: i64) {
    let token = get_auth_token().await;
    let client = build_postgrest();
    let resp = client
        .from("messages")
        .auth(token)
        .eq("id", id.to_string())
        .delete()
        .execute()
        .await
        .expect("Failed to delete message");
    let _ = resp;
}

pub async fn delete_todos_by_marker(marker: &str) {
    let token = get_auth_token().await;
    let client = build_postgrest();
    let resp = client
        .from("todos")
        .auth(token)
        .ilike("description", format!("%{}%", marker))
        .delete()
        .execute()
        .await;
    let _ = resp;
}

pub async fn delete_messages_by_marker(marker: &str) {
    let token = get_auth_token().await;
    let client = build_postgrest();
    let resp = client
        .from("messages")
        .auth(token)
        .ilike("title", format!("%{}%", marker))
        .delete()
        .execute()
        .await;
    let _ = resp;
}

pub async fn create_widget(name: &str) -> String {
    let token = get_auth_token().await;
    let client = Postgrest::new(format!("{}/rest/v1", TEST_CONFIG.api_url))
        .insert_header("apikey", &TEST_CONFIG.anon_key);
    let body = serde_json::json!({
        "name": name,
    });
    let resp = client
        .from("cdc_test.widgets")
        .auth(token)
        .insert(body.to_string())
        .select("id")
        .execute()
        .await
        .expect("Failed to create widget");
    let text = resp.text().await.expect("Failed to read widget response");
    let v: serde_json::Value =
        serde_json::from_str(&text).expect("Failed to parse widget response");
    if let Some(arr) = v.as_array() {
        if let Some(first) = arr.first() {
            if let Some(id) = first.get("id").and_then(|x| x.as_str()) {
                return id.to_string();
            }
        }
    }
    if let Some(id) = v.get("id").and_then(|x| x.as_str()) {
        return id.to_string();
    }
    panic!("Failed to parse widget id from response: {}", text);
}

pub async fn delete_widget(id: &str) {
    let token = get_auth_token().await;
    let client = Postgrest::new(format!("{}/rest/v1", TEST_CONFIG.api_url))
        .insert_header("apikey", &TEST_CONFIG.anon_key);
    let resp = client
        .from("cdc_test.widgets")
        .auth(token)
        .eq("id", id)
        .delete()
        .execute()
        .await
        .expect("Failed to delete widget");
    let _ = resp;
}

pub async fn delete_widgets_by_marker(marker: &str) {
    let token = get_auth_token().await;
    let client = Postgrest::new(format!("{}/rest/v1", TEST_CONFIG.api_url))
        .insert_header("apikey", &TEST_CONFIG.anon_key);
    let resp = client
        .from("cdc_test.widgets")
        .auth(token)
        .ilike("name", format!("%{}%", marker))
        .delete()
        .execute()
        .await;
    let _ = resp;
}
