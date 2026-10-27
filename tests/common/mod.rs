#[cfg(not(target_arch = "wasm32"))]
mod defaults;
#[cfg(not(target_arch = "wasm32"))]
pub mod supabase_helpers;

#[cfg(not(target_arch = "wasm32"))]
mod native {
    use super::defaults::{DEFAULT_KEY, DEFAULT_URL};
    use dotenv::dotenv;

    pub fn create_client()
    -> supabase_realtime_rs::types::Result<supabase_realtime_rs::client::RealtimeClient> {
        dotenv().ok();
        let url = dotenv::var("SUPABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
        let key = dotenv::var("SUPABASE_ANON_KEY").unwrap_or_else(|_| DEFAULT_KEY.to_owned());
        supabase_realtime_rs::client::RealtimeClient::new(&url, &key, None, None, None)
    }
}

#[cfg(not(target_arch = "wasm32"))]
pub use native::create_client;

#[cfg(target_arch = "wasm32")]
include!(concat!(env!("OUT_DIR"), "/wasm_env.rs"));
