//! Cluster and node status endpoints for the v5 REST API.
//!
//! Covers `GET /nodes` (single-node membership list) and
//! `GET /nodes/{node}` (one node's record scoped with the shared
//! [`resolve_node`] helper). Single-node, management-plane only: nothing
//! here runs on the per-message path, so reads never take a delivery lock
//! and no new buffering is added to fan-out or fan-in.
//!
//! Every value in a record comes from the running process or from the
//! kernel state. A field with no true source is left out of the record.
//! The schema of the reference API has no required field, so a missing
//! field is valid.
//!
//! Store bounds (both stated here and enforced below):
//! - membership is exactly one record (the local node); listing copies
//!   one small JSON object per request and never grows with connections,
//!   sessions or subscriptions;
//! - per-request work is a few small reads of `/proc`, one metrics read
//!   and one session count, so handler cost stays constant no matter how
//!   many clients are connected.

use std::sync::OnceLock;
use std::time::Instant;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::node_scope::resolve_node;
use crate::ApiState;

/// Single-node name rendered by [`list_nodes`] and the other v5 rows
/// (`alarms`, `monitoring`, `clients`). Accepted alongside
/// [`ApiState::node_id`] until every caller migrates to the configured id
/// (see `node_scope` docs); both name the same local node.
const LEGACY_NODE_NAME: &str = "indramqtt@127.0.0.1";

/// Name of the local node in the `name@host` form, as `GET /nodes`
/// renders it. Other endpoints that name the local node use this value.
pub(crate) fn local_node_name() -> &'static str {
    LEGACY_NODE_NAME
}

/// Compiler release and target triple of this build, written by `build.rs`.
const BUILD_RUSTC: &str = env!("INDRA_BUILD_RUSTC");
const BUILD_TARGET: &str = env!("INDRA_BUILD_TARGET");

/// Text of the `log_path` field while the node writes no log file. The
/// reference API documents this sentence for the same case.
const LOG_PATH_NOT_SET: &str = "The log path is not yet set";

/// Start of this process, as seen by the management plane.
static PROCESS_START: OnceLock<Instant> = OnceLock::new();

/// Start the uptime clock. The `ApiState` constructors call this at
/// kernel boot, so the clock starts when the node starts and not at the
/// first request. Later calls change nothing.
pub(crate) fn mark_process_start() {
    PROCESS_START.get_or_init(Instant::now);
}

/// Milliseconds since [`mark_process_start`] first ran.
fn uptime_ms() -> u64 {
    let start = *PROCESS_START.get_or_init(Instant::now);
    u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// Format a byte count as the reference API does: two decimals and one
/// unit letter (`K`, `M`, `G`, `T`, `P`), base 1024, for example
/// `512.00M`. The management console reads this form back into bytes.
fn format_size(bytes: u64) -> String {
    const UNITS: [char; 5] = ['K', 'M', 'G', 'T', 'P'];
    if bytes < 1024 {
        return format!("{bytes}B");
    }
    let mut value = bytes as f64 / 1024.0;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    format!("{value:.2}{}", UNITS[unit])
}

/// Read the kibibyte count of a `/proc` line such as
/// `MemTotal:   16318420 kB`, and return bytes.
fn parse_kib_line(text: &str, key: &str) -> Option<u64> {
    let line = text.lines().find(|l| l.starts_with(key))?;
    let kib: u64 = line.split_whitespace().nth(1)?.parse().ok()?;
    kib.checked_mul(1024)
}

/// Read the three load averages from the text of `/proc/loadavg`.
fn parse_loadavg(text: &str) -> Option<[f64; 3]> {
    let mut parts = text.split_whitespace();
    let load1 = parts.next()?.parse().ok()?;
    let load5 = parts.next()?.parse().ok()?;
    let load15 = parts.next()?.parse().ok()?;
    Some([load1, load5, load15])
}

/// Read the soft limit of open files from the text of `/proc/self/limits`.
/// Returns `None` when the limit is `unlimited`.
fn parse_nofile_soft_limit(text: &str) -> Option<u64> {
    let line = text.lines().find(|l| l.starts_with("Max open files"))?;
    line.split_whitespace().nth(3)?.parse().ok()
}

/// Memory the process may use, in bytes: the physical memory of the host,
/// lowered to the cgroup limit when the process runs under one.
fn memory_limit_bytes() -> Option<u64> {
    let host = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|text| parse_kib_line(&text, "MemTotal:"));
    // cgroup v2 first, then cgroup v1. A value that is not a number
    // (`max`) means that no limit is set.
    let cgroup = [
        "/sys/fs/cgroup/memory.max",
        "/sys/fs/cgroup/memory/memory.limit_in_bytes",
    ]
    .iter()
    .find_map(|path| {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|text| text.trim().parse::<u64>().ok())
    });
    match (host, cgroup) {
        (Some(host), Some(limit)) => Some(host.min(limit)),
        (host, limit) => host.or(limit),
    }
}

/// Resident memory of this process, in bytes.
fn process_rss_bytes() -> Option<u64> {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|text| parse_kib_line(&text, "VmRSS:"))
}

/// Number of live tasks on the async runtime that serves this request.
///
/// Mapping of `process_used`: the reference API reports the number of
/// Erlang processes. The closest fact in a Rust process is the number of
/// live runtime tasks.
fn live_task_count() -> Option<u64> {
    let handle = tokio::runtime::Handle::try_current().ok()?;
    u64::try_from(handle.metrics().num_alive_tasks()).ok()
}

/// The `otp_release` text of this build.
///
/// The reference API reports the Erlang release in this field. This node
/// has no Erlang runtime. The field carries the Rust compiler release and
/// the target triple of the build, as `<release>/<target>`.
fn build_release() -> Option<String> {
    if BUILD_RUSTC.is_empty() {
        None
    } else if BUILD_TARGET.is_empty() {
        Some(BUILD_RUSTC.to_string())
    } else {
        Some(format!("{BUILD_RUSTC}/{BUILD_TARGET}"))
    }
}

/// Build one node's record from live process and kernel state.
///
/// `node_name` is the name rendered in the `node` field (the canonical
/// [`LEGACY_NODE_NAME`] for the list, the requested name for a known
/// detail read).
///
/// Source of each field:
/// - `node_status`: the readiness flag of the node (`running` while ready).
/// - `role`: `core`. This broker has one node kind and no replicants.
/// - `version`: the crate version of this build.
/// - `otp_release`: see [`build_release`].
/// - `uptime`: milliseconds since the kernel started.
/// - `connections`, `live_connections`: the active connection gauge.
/// - `cluster_sessions`: sessions held in memory, connected or detached.
/// - `memory_total`: host memory, or the cgroup limit when lower.
/// - `memory_used`: resident memory of this process.
/// - `load1`, `load5`, `load15`: load averages from the operating system.
/// - `max_fds`: soft limit of open files of this process.
/// - `process_used`: live runtime tasks (see [`live_task_count`]).
/// - `sys_path`: the data directory of the node.
/// - `log_path`: the node writes no log file, so the documented
///   "not yet set" text.
///
/// A value that the operating system does not give (for example the
/// `/proc` files on a system without them) leaves its field out.
/// `process_available`, `edition`, `feature_preset` and `security_profile`
/// are always left out: this node has no true value for them.
/// Management-plane only: no delivery locks, no new buffering.
fn node_record(state: &ApiState, node_name: &str) -> serde_json::Value {
    let conns = state.metrics.connections_active().max(0);
    let mut rec = serde_json::Map::with_capacity(18);
    rec.insert("node".into(), node_name.into());
    let status = if state.readiness.is_ready() {
        "running"
    } else {
        "stopped"
    };
    rec.insert("node_status".into(), status.into());
    rec.insert("role".into(), "core".into());
    rec.insert("version".into(), env!("CARGO_PKG_VERSION").into());
    if let Some(release) = build_release() {
        rec.insert("otp_release".into(), release.into());
    }
    rec.insert("uptime".into(), uptime_ms().into());
    rec.insert("connections".into(), conns.into());
    rec.insert("live_connections".into(), conns.into());
    rec.insert(
        "cluster_sessions".into(),
        (state.sessions.session_count() as u64).into(),
    );
    if let Some(total) = memory_limit_bytes() {
        rec.insert("memory_total".into(), format_size(total).into());
    }
    if let Some(used) = process_rss_bytes() {
        rec.insert("memory_used".into(), format_size(used).into());
    }
    if let Some(loads) = std::fs::read_to_string("/proc/loadavg")
        .ok()
        .and_then(|text| parse_loadavg(&text))
    {
        for (key, load) in ["load1", "load5", "load15"].into_iter().zip(loads) {
            if let Some(number) = serde_json::Number::from_f64(load) {
                rec.insert(key.into(), number.into());
            }
        }
    }
    if let Some(limit) = std::fs::read_to_string("/proc/self/limits")
        .ok()
        .and_then(|text| parse_nofile_soft_limit(&text))
    {
        rec.insert("max_fds".into(), limit.into());
    }
    if let Some(tasks) = live_task_count() {
        rec.insert("process_used".into(), tasks.into());
    }
    rec.insert(
        "sys_path".into(),
        state.config.dir().display().to_string().into(),
    );
    rec.insert("log_path".into(), LOG_PATH_NOT_SET.into());
    serde_json::Value::Object(rec)
}

/// `GET /nodes`: every known node (exactly one on this single node).
///
/// Returns an array with the local node's record from [`node_record`].
/// Bounded: one record per request regardless of cluster or
/// client scale. Management-plane only.
pub async fn list_nodes(State(state): State<ApiState>) -> Response {
    let node = node_record(&state, LEGACY_NODE_NAME);
    (StatusCode::OK, Json(vec![node])).into_response()
}

/// `GET /nodes/{node}`: one node's record.
///
/// Single node, management-plane only: the named node is checked with the
/// shared [`resolve_node`] helper against the configured [`ApiState::node_id`]
/// (falling back to the [`LEGACY_NODE_NAME`] literal the v5 rows render
/// today); an unknown name returns the helper's 404 `NOT_FOUND` shape
/// naming the node. A known name returns the same record shape as one
/// [`list_nodes`] entry, with no work on the per-message path.
pub async fn get_node(State(state): State<ApiState>, Path(node_name): Path<String>) -> Response {
    if resolve_node(&state.node_id, &node_name).is_err()
        && resolve_node(LEGACY_NODE_NAME, &node_name).is_err()
    {
        // Reuse the helper's error so the unknown-node shape stays exactly
        // the documented `{code: NOT_FOUND, message}` body.
        return resolve_node(&state.node_id, &node_name)
            .expect_err("node already checked as unknown")
            .into_response();
    }
    (StatusCode::OK, Json(node_record(&state, &node_name))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn standalone_state() -> ApiState {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    async fn response_body(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("node body is small and readable");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("node body is JSON");
        (status, body)
    }

    /// Fields that every node has a true source for on every platform.
    fn always_present_fields() -> [&'static str; 8] {
        [
            "node",
            "node_status",
            "role",
            "version",
            "uptime",
            "connections",
            "live_connections",
            "cluster_sessions",
        ]
    }

    /// Fields that come from `/proc`. They must exist on Linux.
    #[cfg(target_os = "linux")]
    fn proc_fields() -> [&'static str; 8] {
        [
            "memory_total",
            "memory_used",
            "load1",
            "load5",
            "load15",
            "max_fds",
            "process_used",
            "otp_release",
        ]
    }

    #[tokio::test]
    async fn list_shows_local_node_with_true_values() {
        let state = standalone_state();
        state.metrics.set_active_connections(3);
        let (status, body) = response_body(list_nodes(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        let rows = body.as_array().expect("nodes list is an array");
        assert_eq!(
            rows.len(),
            1,
            "single-node membership is exactly one record"
        );
        let first = &rows[0];
        assert_eq!(first["node"], serde_json::json!(LEGACY_NODE_NAME));
        assert_eq!(first["node_status"], serde_json::json!("running"));
        assert_eq!(first["role"], serde_json::json!("core"));
        for field in always_present_fields() {
            assert!(first.get(field).is_some(), "missing field {field}: {first}");
        }
        #[cfg(target_os = "linux")]
        for field in proc_fields() {
            assert!(first.get(field).is_some(), "missing field {field}: {first}");
        }
        // Live connection counts track the metrics snapshot.
        assert_eq!(first["connections"], serde_json::json!(3));
        assert_eq!(first["live_connections"], serde_json::json!(3));
        // Load figures must be JSON numbers, never strings.
        for field in ["load1", "load5", "load15"] {
            if let Some(value) = first.get(field) {
                assert!(
                    value.is_number(),
                    "field {field} must be numeric, got: {first}"
                );
            }
        }
        // The record holds only fields of the documented schema.
        let documented = [
            "cluster_sessions",
            "connections",
            "edition",
            "feature_preset",
            "live_connections",
            "load1",
            "load15",
            "load5",
            "log_path",
            "max_fds",
            "memory_total",
            "memory_used",
            "node",
            "node_status",
            "otp_release",
            "process_available",
            "process_used",
            "role",
            "security_profile",
            "sys_path",
            "uptime",
            "version",
        ];
        for key in first.as_object().expect("record is an object").keys() {
            assert!(
                documented.contains(&key.as_str()),
                "field {key} is not in the documented schema"
            );
        }
    }

    #[tokio::test]
    async fn fields_without_a_true_source_are_left_out() {
        let state = standalone_state();
        let (_, body) = response_body(list_nodes(State(state)).await).await;
        for field in [
            "process_available",
            "edition",
            "feature_preset",
            "security_profile",
        ] {
            assert!(
                body[0].get(field).is_none(),
                "{field} has no true source and must not be set: {}",
                body[0]
            );
        }
    }

    #[tokio::test]
    async fn version_and_release_are_the_build_values() {
        let state = standalone_state();
        let (_, body) = response_body(list_nodes(State(state)).await).await;
        let row = &body[0];
        assert_eq!(row["version"], serde_json::json!(env!("CARGO_PKG_VERSION")));
        // A fixed release or version string from the reference API must
        // never return.
        assert_ne!(row["version"], serde_json::json!("5.8.0"));
        assert_ne!(row["otp_release"], serde_json::json!("26.2"));
        if let Some(release) = row.get("otp_release").and_then(|v| v.as_str()) {
            assert_eq!(Some(release.to_string()), build_release());
            assert!(
                release.starts_with(BUILD_RUSTC),
                "release {release} must start with the compiler release {BUILD_RUSTC}"
            );
        }
        // The compiler release has the shape `major.minor.patch`.
        assert!(
            BUILD_RUSTC.split('.').count() >= 3
                && BUILD_RUSTC
                    .split('.')
                    .next()
                    .is_some_and(|major| major.parse::<u32>().is_ok()),
            "build script must record the compiler release, got {BUILD_RUSTC:?}"
        );
        assert!(
            !BUILD_TARGET.is_empty(),
            "build script must record the target"
        );
    }

    #[tokio::test]
    async fn uptime_grows_with_real_time() {
        let state = standalone_state();
        let (_, first) = response_body(list_nodes(State(state.clone())).await).await;
        tokio::time::sleep(std::time::Duration::from_millis(60)).await;
        let (_, second) = response_body(list_nodes(State(state)).await).await;
        let first = first[0]["uptime"].as_u64().expect("uptime is an integer");
        let second = second[0]["uptime"].as_u64().expect("uptime is an integer");
        assert!(
            second >= first + 60,
            "uptime must follow the clock: {first} then {second}"
        );
        assert_ne!(second, 86_400_000, "uptime must not be a constant");
    }

    #[tokio::test]
    async fn status_follows_the_readiness_flag() {
        let state = standalone_state();
        state.readiness.mark_not_ready();
        let (_, body) = response_body(list_nodes(State(state.clone())).await).await;
        assert_eq!(body[0]["node_status"], serde_json::json!("stopped"));
        state.readiness.mark_ready();
        let (_, body) = response_body(list_nodes(State(state)).await).await;
        assert_eq!(body[0]["node_status"], serde_json::json!("running"));
    }

    #[tokio::test]
    async fn cluster_sessions_counts_sessions_in_memory() {
        let state = standalone_state();
        let (_, before) = response_body(list_nodes(State(state.clone())).await).await;
        assert_eq!(before[0]["cluster_sessions"], serde_json::json!(0));
        let _a = state.sessions.get_or_create("node-test-a", true);
        let _b = state.sessions.get_or_create("node-test-b", true);
        let (_, after) = response_body(list_nodes(State(state)).await).await;
        assert_eq!(after[0]["cluster_sessions"], serde_json::json!(2));
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn linux_values_match_the_kernel_files() {
        let state = standalone_state();
        let (_, body) = response_body(list_nodes(State(state)).await).await;
        let row = &body[0];
        // Load averages: same first figure as the kernel file, within the
        // drift of a few seconds.
        let text = std::fs::read_to_string("/proc/loadavg").expect("loadavg exists on Linux");
        let kernel = parse_loadavg(&text).expect("loadavg parses");
        let reported = row["load1"].as_f64().expect("load1 is a number");
        assert!(
            (reported - kernel[0]).abs() < 5.0,
            "load1 {reported} must follow /proc/loadavg {}",
            kernel[0]
        );
        // Memory: the used figure is the resident memory of this process.
        // A test process uses well under the host memory.
        let used = row["memory_used"].as_str().expect("memory_used is text");
        let total = row["memory_total"].as_str().expect("memory_total is text");
        assert!(used.ends_with(|c: char| "KMGTPB".contains(c)), "{used}");
        assert!(total.ends_with(|c: char| "KMGTPB".contains(c)), "{total}");
        assert_ne!(used, "128MB");
        assert_ne!(total, "16GB");
        // Process count: the count of live runtime tasks. A test future
        // runs on the test thread and not as a task, so zero is true here.
        assert!(row["process_used"].as_u64().is_some());
        // The open-file limit of a normal process is above zero.
        assert!(row["max_fds"].as_u64().is_some_and(|n| n > 0));
    }

    #[test]
    fn format_size_uses_two_decimals_and_one_unit_letter() {
        assert_eq!(format_size(0), "0B");
        assert_eq!(format_size(1023), "1023B");
        assert_eq!(format_size(1024), "1.00K");
        assert_eq!(format_size(512 * 1024 * 1024), "512.00M");
        assert_eq!(format_size(3 * 1024 * 1024 * 1024 / 2), "1.50G");
        assert_eq!(format_size(u64::MAX), "16384.00P");
    }

    #[test]
    fn proc_text_parsers_read_kernel_formats() {
        let meminfo = "MemTotal:       16318420 kB\nMemFree:         1000 kB\n";
        assert_eq!(
            parse_kib_line(meminfo, "MemTotal:"),
            Some(16_318_420 * 1024)
        );
        assert_eq!(parse_kib_line(meminfo, "VmRSS:"), None);
        let status = "Name:\tindra\nVmRSS:\t   20480 kB\n";
        assert_eq!(parse_kib_line(status, "VmRSS:"), Some(20_480 * 1024));
        assert_eq!(
            parse_loadavg("0.52 0.41 0.30 1/200 12345\n"),
            Some([0.52, 0.41, 0.30])
        );
        assert_eq!(parse_loadavg("garbage"), None);
        let limits = "Limit                     Soft Limit           Hard Limit           Units\n\
                      Max open files            1048576              1048576              files\n";
        assert_eq!(parse_nofile_soft_limit(limits), Some(1_048_576));
        let unlimited =
            "Max open files            unlimited            unlimited            files\n";
        assert_eq!(parse_nofile_soft_limit(unlimited), None);
    }

    #[tokio::test]
    async fn detail_for_known_node_returns_same_shape_as_list() {
        let state = standalone_state();
        state.metrics.set_active_connections(2);
        let (_, list_body) = response_body(list_nodes(State(state.clone())).await).await;
        let listed = list_body[0].clone();
        for known in [LEGACY_NODE_NAME, "indra-node-1"] {
            let response = get_node(State(state.clone()), Path(known.to_string())).await;
            let (status, body) = response_body(response).await;
            assert_eq!(status, StatusCode::OK, "known node {known}");
            assert_eq!(body["node"], serde_json::json!(known));
            for field in always_present_fields() {
                assert!(body.get(field).is_some(), "missing field {field}: {body}");
            }
            // Same set of fields as the list record. Values that move with
            // time (uptime, load, memory, tasks) may differ.
            let list_keys: Vec<_> = listed.as_object().expect("object").keys().collect();
            let body_keys: Vec<_> = body.as_object().expect("object").keys().collect();
            assert_eq!(list_keys, body_keys, "detail must carry the list fields");
            for field in ["version", "role", "node_status", "log_path", "sys_path"] {
                assert_eq!(
                    body[field], listed[field],
                    "field {field} must match the list record"
                );
            }
            assert_eq!(body["connections"], serde_json::json!(2));
        }
    }

    #[tokio::test]
    async fn detail_for_unknown_node_is_not_found() {
        let state = standalone_state();
        let response = get_node(State(state), Path("no-such-node".to_string())).await;
        let (status, body) = response_body(response).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        let message = body["message"].as_str().expect("message is a string");
        assert!(
            message.contains("no-such-node"),
            "message names the unknown node: {body}"
        );
    }
}
