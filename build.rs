use std::{env, fs, path::PathBuf};

include!("tests/common/defaults.rs");

fn main() {
    println!("cargo:rerun-if-changed=.env");
    println!("cargo:rerun-if-changed=tests/common/defaults.rs");
    println!("cargo:rerun-if-env-changed=SUPABASE_URL");
    println!("cargo:rerun-if-env-changed=SUPABASE_ANON_KEY");

    dotenv::dotenv().ok();

    let url = env::var("SUPABASE_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let key = env::var("SUPABASE_ANON_KEY").unwrap_or_else(|_| DEFAULT_KEY.to_owned());

    let contents = format!("pub const URL: &str = {url:?};\npub const KEY: &str = {key:?};\n");

    let out = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR")).join("wasm_env.rs");
    fs::write(&out, contents).expect("failed to write wasm_env.rs");
}
