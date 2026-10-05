//! Records the compiler release and the target of this build.
//!
//! `GET /nodes` reports them in the `otp_release` field. The reference API
//! uses that field for the Erlang release. This node has no Erlang runtime,
//! so the field carries the Rust compiler release and the target triple.
//! The script runs only at build time and adds no dependency.

use std::process::Command;

fn main() {
    let rustc = std::env::var("RUSTC").unwrap_or_else(|_| "rustc".to_string());
    // `rustc --version` prints `rustc 1.88.0 (hash date)`. Keep the release.
    let release = Command::new(rustc)
        .arg("--version")
        .output()
        .ok()
        .and_then(|out| String::from_utf8(out.stdout).ok())
        .and_then(|text| text.split_whitespace().nth(1).map(str::to_string))
        .unwrap_or_default();
    let target = std::env::var("TARGET").unwrap_or_default();
    println!("cargo:rustc-env=INDRA_BUILD_RUSTC={release}");
    println!("cargo:rustc-env=INDRA_BUILD_TARGET={target}");
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=RUSTC");
    println!("cargo:rerun-if-env-changed=TARGET");
}
