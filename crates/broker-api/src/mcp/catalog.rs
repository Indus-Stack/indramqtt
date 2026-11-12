//! MCP tool catalog: one tool per management operation.
//!
//! The route table comes from `build.rs`, which scans the router source
//! at build time (see `route_scan`). This module pairs each row with a
//! tool definition or with one row of the exclusion table below. A new
//! route without either fails the coverage test. Schemas are mechanical:
//! path segments become required string fields, `query` carries query
//! keys, `body` carries the JSON body. No hand-written per-operation
//! schema exists anywhere here.

use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    OnceLock,
};

use super::route_scan::path_params;

include!(concat!(env!("OUT_DIR"), "/mcp_tool_table.rs"));

/// One excluded operation: method, path template, reason.
///
/// The groups below follow the approved scope: every management
/// function is a tool except the API key group (agents authenticate
/// with a key; key management stays operator-only) and the
/// interactive sign-in flows (login, logout, MFA setup, single
/// sign-on). Transport and console routes are not operations.
pub struct Exclusion {
    /// Uppercase HTTP method.
    pub method: &'static str,
    /// Path template as in the router.
    pub path: &'static str,
    /// Why no tool exposes this operation.
    pub reason: &'static str,
}

/// Operations with no tool. The coverage test proves every router row
/// is either a tool or one entry here, and every entry here matches a
/// real router row, so stale entries fail loudly.
pub const EXCLUSIONS: &[Exclusion] = &[
    // The MCP transport itself: a tool cannot call it.
    Exclusion {
        method: "POST",
        path: "/mcp",
        reason: "the MCP transport itself; tools call operations, not the transport",
    },
    // Console, docs and probes: not management operations.
    Exclusion {
        method: "GET",
        path: "/",
        reason: "dashboard redirect; not a management operation",
    },
    Exclusion {
        method: "GET",
        path: "/dashboard",
        reason: "console page; not a management operation",
    },
    Exclusion {
        method: "GET",
        path: "/ui",
        reason: "console page; not a management operation",
    },
    Exclusion {
        method: "GET",
        path: "/swagger",
        reason: "API explorer page; not a management operation",
    },
    Exclusion {
        method: "GET",
        path: "/api-docs/openapi.json",
        reason: "API document; not a management operation",
    },
    Exclusion {
        method: "GET",
        path: "/assets/*path",
        reason: "console asset; not a management operation",
    },
    Exclusion {
        method: "GET",
        path: "/static/*path",
        reason: "console asset; not a management operation",
    },
    Exclusion {
        method: "GET",
        path: "/favicon.ico",
        reason: "console asset; not a management operation",
    },
    Exclusion {
        method: "GET",
        path: "/version",
        reason: "dashboard version file; not a management operation",
    },
    Exclusion {
        method: "GET",
        path: "/healthz",
        reason: "load-balancer probe with a plain-text answer; not a management operation",
    },
    Exclusion {
        method: "GET",
        path: "/ws/mqtt",
        reason: "client message socket; not a management operation",
    },
    // API keys: agents authenticate with a key, so key management
    // stays operator-only.
    Exclusion {
        method: "GET",
        path: "/api/v5/api_key",
        reason: "API key management stays operator-only; agents authenticate with a key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/api_key",
        reason: "API key management stays operator-only; agents authenticate with a key",
    },
    Exclusion {
        method: "GET",
        path: "/api/v5/api_key/:name",
        reason: "API key management stays operator-only; agents authenticate with a key",
    },
    Exclusion {
        method: "PUT",
        path: "/api/v5/api_key/:name",
        reason: "API key management stays operator-only; agents authenticate with a key",
    },
    Exclusion {
        method: "DELETE",
        path: "/api/v5/api_key/:name",
        reason: "API key management stays operator-only; agents authenticate with a key",
    },
    Exclusion {
        method: "GET",
        path: "/api/v5/api_key_scopes",
        reason: "API key management stays operator-only; agents authenticate with a key",
    },
    // Installation licence: agents authenticate with a key, so
    // licence installation stays operator-only.
    Exclusion {
        method: "GET",
        path: "/api/v1/licence/request",
        reason: "licence installation stays operator-only; agents authenticate with a key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v1/licence/install",
        reason: "licence installation stays operator-only; agents authenticate with a key",
    },
    Exclusion {
        method: "GET",
        path: "/api/v1/licence/status",
        reason: "licence installation stays operator-only; agents authenticate with a key",
    },
    // Interactive sign-in: agents authenticate with a key.
    Exclusion {
        method: "POST",
        path: "/api/v5/login",
        reason: "interactive sign-in; agents authenticate with an API key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/login/challenge",
        reason: "interactive sign-in; agents authenticate with an API key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/login/verify",
        reason: "interactive sign-in; agents authenticate with an API key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/logout",
        reason: "interactive sign-in; agents authenticate with an API key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/sso/login/:backend",
        reason: "interactive sign-in flow; agents authenticate with an API key",
    },
    Exclusion {
        method: "GET",
        path: "/api/v5/sso/oidc/callback",
        reason: "interactive sign-in flow; agents authenticate with an API key",
    },
    Exclusion {
        method: "GET",
        path: "/api/v5/sso/running",
        reason: "interactive sign-in flow; agents authenticate with an API key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/sso/saml/acs",
        reason: "interactive sign-in flow; agents authenticate with an API key",
    },
    Exclusion {
        method: "GET",
        path: "/api/v5/sso/saml/metadata",
        reason: "interactive sign-in flow; agents authenticate with an API key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/sso/token_exchange",
        reason: "interactive sign-in flow; agents authenticate with an API key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/sso/mfa/setup",
        reason: "interactive MFA setup; agents authenticate with an API key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/sso/mfa/setup_info",
        reason: "interactive MFA setup; agents authenticate with an API key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/sso/mfa/verify",
        reason: "interactive MFA setup; agents authenticate with an API key",
    },
    Exclusion {
        method: "POST",
        path: "/api/v5/users/:username/mfa",
        reason: "interactive MFA setup; agents authenticate with an API key",
    },
    Exclusion {
        method: "DELETE",
        path: "/api/v5/users/:username/mfa",
        reason: "interactive MFA setup; agents authenticate with an API key",
    },
];

/// One MCP tool: a management operation an agent can call.
#[derive(Debug, Clone)]
pub struct ToolDef {
    /// Stable snake-case name.
    pub name: String,
    /// Uppercase HTTP method of the operation.
    pub method: &'static str,
    /// Path template of the operation.
    pub path: &'static str,
    /// Short description: the handler doc line plus the route
    /// (`METHOD path` in parentheses), else `METHOD path` alone.
    pub summary: &'static str,
    /// True for GET operations.
    pub read_only: bool,
    /// True for DELETE and for stop, kick, purge and reset operations.
    pub destructive: bool,
    /// Toolset (group) name for `toolsets=` selection.
    pub toolset: &'static str,
}

/// Path segments that mark a non-DELETE operation destructive.
/// Reason: these verbs stop, remove or reset live broker state, so an
/// agent must treat them like a delete.
const DESTRUCTIVE_MARKERS: [&str; 7] = [
    "stop",
    "kick",
    "purge",
    "reset",
    "restart",
    "deactivate",
    "evacuation",
];

/// True when the operation destroys or stops live state.
pub fn is_destructive(method: &str, path: &str) -> bool {
    if method == "DELETE" {
        return true;
    }
    DESTRUCTIVE_MARKERS.iter().any(|mark| path.contains(mark))
}

/// Toolset (group) of one operation path.
///
/// Groups follow the operation tags (see `TOOLSETS`). Every
/// non-excluded path maps to exactly one group. Unknown paths map to
/// `configuration`, so a new operation stays callable under
/// `toolsets=all` while the mapping waits for its own group.
pub fn toolset_for(path: &str) -> &'static str {
    if path == "/api/v5/status" || path == "/status" {
        return "status";
    }
    if let Some(rest) = path.strip_prefix("/api/v5/") {
        let head = rest.split('/').next().unwrap_or_default();
        return match head {
            "clients" | "clients_v2" | "sessions_count" | "banned" => "clients",
            "subscriptions" => "subscriptions",
            "topics" => "topics",
            "publish" => "publish",
            "metrics" | "monitor" | "monitor_current" | "stats" | "alarms" | "trace"
            | "tracing" | "slow_subscriptions" | "telemetry" | "prometheus" => "metrics",
            "rules"
            | "rule_engine"
            | "rule_events"
            | "rule_test"
            | "actions"
            | "actions_probe"
            | "actions_summary"
            | "action_types"
            | "sources"
            | "sources_probe"
            | "sources_summary"
            | "source_types"
            | "schemas"
            | "schema_registry"
            | "message_transformations"
            | "ai" => "rules",
            "connectors" | "connectors_probe" => "connectors",
            "authentication" | "users" | "user_scopes" | "sso" | "current_user" => "authentication",
            "authorization" => "authorization",
            "mt" => "tenants",
            "listeners" | "listeners_status" => "listeners",
            "configs" | "configs_reset" | "mqtt" | "data" | "certs" | "queues" | "queue"
            | "streams" | "stream" | "features" | "error_codes" => "configuration",
            "nodes" | "cluster" | "load_rebalance" | "node_eviction" => "cluster",
            "gateways" => "gateways",
            "license" => "license",
            "audit" => "audit",
            _ => "configuration",
        };
    }
    if let Some(rest) = path.strip_prefix("/api/v1/") {
        let head = rest.split('/').next().unwrap_or_default();
        return match head {
            "nodes" => "cluster",
            "clients" => "clients",
            "metrics" => "metrics",
            "connectors" | "rules" => "rules",
            "auth" => "authentication",
            "config" => "configuration",
            "licence" => "license",
            _ => "configuration",
        };
    }
    "configuration"
}

/// One toolset: name plus short description for `list_toolsets`.
pub struct ToolsetDef {
    /// Group name used in `toolsets=a,b`.
    pub name: &'static str,
    /// Short description of the group.
    pub description: &'static str,
}

/// Every toolset with its description.
pub const TOOLSETS: &[ToolsetDef] = &[
    ToolsetDef {
        name: "status",
        description: "Node status and version.",
    },
    ToolsetDef {
        name: "clients",
        description: "Connected clients, bans and session counts.",
    },
    ToolsetDef {
        name: "subscriptions",
        description: "Subscription listing.",
    },
    ToolsetDef {
        name: "topics",
        description: "Topic listing and retained topics.",
    },
    ToolsetDef {
        name: "publish",
        description: "Publish messages through the broker.",
    },
    ToolsetDef {
        name: "metrics",
        description: "Metrics, alarms, traces and monitor history.",
    },
    ToolsetDef {
        name: "rules",
        description: "Rules, actions, sources, schemas and transformations.",
    },
    ToolsetDef {
        name: "connectors",
        description: "Connector instances and probes.",
    },
    ToolsetDef {
        name: "authentication",
        description: "Users, authenticators and single sign-on.",
    },
    ToolsetDef {
        name: "authorization",
        description: "Authorization sources, rules and caches.",
    },
    ToolsetDef {
        name: "tenants",
        description: "Managed tenant namespaces.",
    },
    ToolsetDef {
        name: "listeners",
        description: "Network listeners and their status.",
    },
    ToolsetDef {
        name: "configuration",
        description: "Broker configuration, certificates and data files.",
    },
    ToolsetDef {
        name: "cluster",
        description: "Cluster topology and nodes.",
    },
    ToolsetDef {
        name: "gateways",
        description: "Protocol gateways.",
    },
    // Decided (MV-15): the router exposes the gateway honest markers
    // (list, detail, enable), so this set holds their tools. Gateways
    // stay out of the MVP: the tools answer the same empty or refused
    // states as the routes.
    ToolsetDef {
        name: "license",
        description: "Licence document and settings.",
    },
    ToolsetDef {
        name: "audit",
        description: "Management audit log.",
    },
];

/// Toolsets selected with no `toolsets` parameter.
///
/// Reason: the task names this default set: status, clients,
/// subscriptions, topics, publish and metrics.
pub const CORE_TOOLSETS: &[&str] = &[
    "status",
    "clients",
    "subscriptions",
    "topics",
    "publish",
    "metrics",
];

/// Name of the built-in toolset listing tool. It is always present:
/// every key sees it and every key may call it.
pub const LIST_TOOLSETS_TOOL: &str = "list_toolsets";

/// All tools: the built-in status tool first, then one tool per
/// non-excluded router row, in route-table order.
pub fn build_catalog() -> Vec<ToolDef> {
    let mut tools = Vec::new();
    tools.push(ToolDef {
        name: BROKER_STATUS_TOOL.to_string(),
        method: "GET",
        path: "/api/v5/status",
        summary: "Read the version, uptime and node name of this node. (GET /api/v5/status)",
        read_only: true,
        destructive: false,
        toolset: "status",
    });
    for row in MCP_ROUTE_TABLE {
        if find_exclusion(row.method, row.path).is_some() {
            continue;
        }
        tools.push(ToolDef {
            name: row.tool.to_string(),
            method: row.method,
            path: row.path,
            summary: row.summary,
            read_only: row.method == "GET",
            destructive: is_destructive(row.method, row.path),
            toolset: toolset_for(row.path),
        });
    }
    tools
}

/// Shared tool catalog, built one time. The table is static, so every
/// request and every concurrent call shares it. No state lives here.
static CATALOG: OnceLock<Vec<ToolDef>> = OnceLock::new();

/// Count of catalog builds. Tests read this; production ignores it.
static CATALOG_BUILDS: AtomicUsize = AtomicUsize::new(0);

/// The shared catalog. The first call builds it; later calls share it.
pub fn catalog() -> &'static [ToolDef] {
    CATALOG.get_or_init(|| {
        CATALOG_BUILDS.fetch_add(1, Ordering::Relaxed);
        build_catalog()
    })
}

/// Count of catalog builds. Tests read this; production ignores it.
#[cfg(test)]
pub(crate) fn catalog_builds() -> usize {
    CATALOG_BUILDS.load(Ordering::Relaxed)
}

/// The built-in status tool (proves the path end to end).
pub const BROKER_STATUS_TOOL: &str = "broker_status";

/// Find the exclusion for `(method, path)`, if any.
pub fn find_exclusion(method: &str, path: &str) -> Option<&'static Exclusion> {
    EXCLUSIONS
        .iter()
        .find(|item| item.method == method && item.path == path)
}

/// Find a tool by name in a catalog.
pub fn tool_by_name<'a>(catalog: &'a [ToolDef], name: &str) -> Option<&'a ToolDef> {
    catalog.iter().find(|tool| tool.name == name)
}

/// JSON Schema for a tool: required path fields plus `query` and,
/// for writes, `body` and `body_base64`. The shape is mechanical: it
/// derives from the path template and the method, never from a
/// hand-written per-operation copy.
pub fn input_schema_for(tool: &ToolDef) -> Value {
    let mut properties = serde_json::Map::new();
    let mut required: Vec<Value> = Vec::new();
    for param in path_params(tool.path) {
        properties.insert(
            param.clone(),
            json!({
                "type": "string",
                "description": format!("Value for the :{param} path segment."),
            }),
        );
        required.push(Value::String(param));
    }
    properties.insert(
        "query".to_string(),
        json!({
            "type": "object",
            "description": "Query keys of the operation. Omit keys the call does not need.",
            "additionalProperties": true,
        }),
    );
    if tool.method != "GET" && tool.method != "DELETE" {
        properties.insert(
            "body".to_string(),
            json!({
                "description": "JSON body of the operation, exactly as the HTTP route takes it.",
            }),
        );
        properties.insert(
            "body_base64".to_string(),
            json!({
                "type": "string",
                "description": "Raw body bytes in base64 for file operations. Use either body or body_base64, never both.",
            }),
        );
    }
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}

/// A tool as a `tools/list` entry.
pub fn list_entry_for(tool: &ToolDef) -> Value {
    json!({
        "name": tool.name,
        "description": tool.summary,
        "inputSchema": input_schema_for(tool),
        "annotations": {
            "readOnlyHint": tool.read_only,
            "destructiveHint": tool.destructive,
        },
    })
}

/// One toolset row for `list_toolsets`: name, description and count.
pub fn toolset_entry_for(name: &str, description: &str, tools: &[&ToolDef]) -> Value {
    let names: Vec<Value> = tools
        .iter()
        .map(|tool| Value::String(tool.name.clone()))
        .collect();
    json!({
        "name": name,
        "description": description,
        "tool_count": tools.len(),
        "tools": names,
    })
}
