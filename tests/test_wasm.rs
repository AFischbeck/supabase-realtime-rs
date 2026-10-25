#![cfg(target_arch = "wasm32")]

use supabase_realtime_rs::client::RealtimeClient;
use wasm_bindgen_test::wasm_bindgen_test;

mod common;
use common::{KEY, URL};

#[wasm_bindgen_test]
fn constructs_client_without_connecting() {
    let client = RealtimeClient::new(URL, KEY, None, None, None)
        .expect("a valid http url should construct a client");
    assert!(!client.is_connected());
}

#[wasm_bindgen_test]
fn rejects_unsupported_scheme() {
    let result = RealtimeClient::new("not-a-url", KEY, None, None, None);
    assert!(result.is_err());
}
