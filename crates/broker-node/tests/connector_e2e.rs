//! End-to-end tests for the connectors that need no external server.
//!
//! One test target shares one harness build.
//! Each test boots one real broker.
//! It creates the connector and rule.
//! It publishes on a real socket.
//! It reads the result where a user sees it.

#[path = "connector_e2e/e2e_support/mod.rs"]
mod e2e_support;
#[path = "connector_e2e/http_connector_e2e.rs"]
mod http_connector_e2e;
#[path = "connector_e2e/mqtt_bridge_e2e.rs"]
mod mqtt_bridge_e2e;
