//! MQTT 5 re-authentication over real sockets.
//!
//! Each test boots the kernel on ephemeral ports, connects with a real
//! TCP socket and sends AUTH packets. The suite covers the protocol
//! errors, JWT re-authentication through a live JWKS endpoint and a
//! SCRAM-SHA-256 round. The JWT test reads the authorization-cache route
//! before and after the exchange.

#[allow(dead_code)]
#[path = "../../broker-auth/src/jwks_test_support.rs"]
pub(crate) mod jwks_test_support;

#[path = "reauth/reauth_harness.rs"]
pub(crate) mod reauth_harness;

#[path = "reauth/reauth_jwt.rs"]
mod reauth_jwt;
#[path = "reauth/reauth_protocol.rs"]
mod reauth_protocol;
#[path = "reauth/reauth_scram.rs"]
mod reauth_scram;
