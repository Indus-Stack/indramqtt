//! Conformance test binary for the kernel MQTT listener.
//!
//! `support` holds the shared kernel boot, socket and packet helpers.
//! Every later TA ticket adds one `mod ta_NNN;` line here.

mod support;
mod ta_000;
mod ta_001;
mod ta_002;
mod ta_003;
mod ta_004;
mod ta_005;
mod ta_006;
mod ta_007;
mod ta_008;
mod ta_009;
mod ta_010;
mod ta_011;
