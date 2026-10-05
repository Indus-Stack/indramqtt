//! Single-node scope check for `/nodes/{node}/...` routes.
//!
//! Node-scoped routes must 404 `NOT_FOUND` for unknown nodes. This broker
//! is single-node first: the helper compares the requested `{node}` segment
//! against the one local node name with an exact, case-sensitive match.
//!
//! Single-node contract: every endpoint that renders the name of the local
//! node uses [`local_node_name`]. The name has the form `name@host`. The
//! `name` part is the kernel's `--node-id` (default `indra-node-1`). The
//! `host` part is the host name of the machine. A node id that has an `@`
//! already is used as it is. The routes under `/nodes/{node}` accept the
//! full name and the bare node id.

use std::sync::OnceLock;

use crate::errors::ApiError;

/// The node id that the kernel uses when no `--node-id` is set.
const DEFAULT_NODE_ID: &str = "indra-node-1";

/// Host part of the name when the machine has no usable host name.
const FALLBACK_HOST: &str = "localhost";

static LOCAL_NODE_NAME: OnceLock<String> = OnceLock::new();

/// Host name of this machine. Reads the kernel host name on Linux, then
/// the `HOSTNAME` and `COMPUTERNAME` variables. Falls back to
/// `localhost` when no name is usable.
fn host_name() -> String {
    let from_file = ["/proc/sys/kernel/hostname", "/etc/hostname"]
        .iter()
        .find_map(|path| std::fs::read_to_string(path).ok());
    let raw = from_file
        .or_else(|| std::env::var("HOSTNAME").ok())
        .or_else(|| std::env::var("COMPUTERNAME").ok())
        .unwrap_or_default();
    let host = raw.trim();
    let usable = !host.is_empty()
        && host.len() <= 253
        && host
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '_'));
    if usable {
        host.to_string()
    } else {
        FALLBACK_HOST.to_string()
    }
}

/// Build the `name@host` form from a node id and a host name. A node id
/// that has an `@` already is returned unchanged.
pub fn compose_node_name(node_id: &str, host: &str) -> String {
    if node_id.contains('@') {
        node_id.to_string()
    } else {
        format!("{node_id}@{host}")
    }
}

/// Fix the name of the local node from the kernel's node id. The first
/// call wins; later calls change nothing, so every endpoint shows one name
/// for the life of the process.
pub fn init_local_node_name(node_id: &str) {
    let _ = LOCAL_NODE_NAME.set(compose_node_name(node_id, &host_name()));
}

/// The name of the local node in the `name@host` form. Before the kernel
/// calls [`init_local_node_name`], the name uses the default node id.
pub fn local_node_name() -> &'static str {
    LOCAL_NODE_NAME
        .get_or_init(|| compose_node_name(DEFAULT_NODE_ID, &host_name()))
        .as_str()
}

/// Resolve a requested node name against the local node name.
///
/// Returns `Ok(())` only on an exact, case-sensitive match. Anything else
/// (unknown names, empty strings, differently-cased names, prefix matches)
/// returns 404 `NOT_FOUND` naming the unknown node.
pub fn resolve_node(local: &str, requested: &str) -> Result<(), ApiError> {
    if requested == local && !requested.is_empty() {
        Ok(())
    } else {
        Err(ApiError::NotFound(format!("node not found: {requested}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compose_adds_the_host_unless_the_id_has_one() {
        assert_eq!(compose_node_name("n1", "h1"), "n1@h1");
        assert_eq!(compose_node_name("n1@10.0.0.2", "h1"), "n1@10.0.0.2");
    }

    #[test]
    fn local_name_has_the_name_at_host_form() {
        let name = local_node_name();
        let (id, host) = name.split_once('@').expect("name has an @");
        assert!(!id.is_empty());
        assert!(!host.is_empty());
        assert_eq!(name, local_node_name(), "the name is stable");
    }

    #[test]
    fn exact_local_name_resolves() {
        assert!(resolve_node("indra-node-1", "indra-node-1").is_ok());
    }

    #[test]
    fn unknown_name_yields_not_found() {
        let err = resolve_node("indra-node-1", "other-node").unwrap_err();
        assert_eq!(err.code(), "NOT_FOUND");
        assert_eq!(err.status_code(), axum::http::StatusCode::NOT_FOUND);
        assert!(
            err.message().contains("other-node"),
            "message names the unknown node: {}",
            err.message()
        );
    }

    #[test]
    fn empty_and_differently_cased_names_yield_not_found() {
        for requested in [
            "",
            "INDRA-NODE-1",
            "Indra-Node-1",
            "indra-node-1-extra",
            "indra-node-",
        ] {
            let err = resolve_node("indra-node-1", requested).unwrap_err();
            assert_eq!(err.code(), "NOT_FOUND", "requested: {requested:?}");
            assert_eq!(
                err.status_code(),
                axum::http::StatusCode::NOT_FOUND,
                "requested: {requested:?}"
            );
        }
    }
}
