//! Conformance test binary for the kernel MQTT listener.
//!
//! `support` holds the shared kernel boot, socket and packet helpers.
//! Every later TA ticket adds one `mod ta_NNN;` line here.

mod support;
mod ta_000;
