use super::*;
use broker_auth::Authenticator;
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[path = "mn08_tests.rs"]
mod mn08;

pub(crate) mod support;
pub(crate) use support::*;

mod action_options;
mod actions;
mod api_key;
mod authn_chain;
mod authz;
mod backup;
mod capabilities;
mod clients;
mod cloud;
mod connector_create;
mod connector_operation;
mod connectors;
mod data;
mod document;
mod industrial;
mod kafka;
mod licensing;
mod login;
mod mcp;
mod messaging;
mod observability;
mod probe;
mod publish;
mod pulsar;
mod rules;
mod schema_registry;
mod sources;
mod sso;
mod storage;
mod subscriptions;
mod timeseries;
mod users;
mod websocket;
