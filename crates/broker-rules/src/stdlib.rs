//! Standard rule-dialect built-in functions (RQ-03).
//!
//! Every `new` and non-colliding `adapter` row from `FUNCTIONS.md` is
//! implemented here as a plain `fn(&[serde_json::Value]) ->
//! serde_json::Value` handler and registered once through the streaming
//! engine's public user-function table
//! (`rekuiper_core::plugin::get_global_udf_registry`). No file under the
//! engine's own source tree is touched.
//!
//! Conservative rules applied throughout (see the rulebook UNKNOWN rule):
//! wrong arity or wrong types return `Value::Null` (fail closed, never a
//! panic on attacker-controlled input); out-of-range indexes return `Null`
//! except where the documented behaviour says otherwise (`nth/2` yields
//! `""`, `sublist` clamps); oversized string/array results return `Null`
//! instead of allocating.
//!
//! Context accessors (`topic()`, `clientid()`, ...) read the calling rule's
//! context from a thread-local installed by the engine around each
//! synchronous evaluation (`RuleContextGuard`). Registered handlers take
//! only evaluated arguments, so without the guard they would have no record
//! to read; the guard is set in `apply_sql`/`try_evaluate` on the same
//! thread that runs the evaluator, then restored. Missing context fields
//! read as `Null` (fail closed).
//!
//! Colliding adapters (`substr`, `split`, `concat`, `length`,
//! `base64_encode/2,3`, `base64_decode/2,3`, `rtrim`) are implemented and
//! registered here under both their standard names and internal `std_*`
//! aliases. The engine resolves those names against its own built-ins
//! before consulting the registry, so SQL text is rewritten by
//! `crate::rewrite_colliding_functions` (outside the engine crate) to the
//! alias form before parsing; SQL evaluation therefore uses the standard
//! semantics while direct registry calls keep working under either name.

use rekuiper_core::plugin::{get_global_udf_registry, UdfFn};
use serde_json::Value;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Bounds: every default is finite and states its reason inline.
// ---------------------------------------------------------------------------

/// Caps any single string UDF result at 1 MiB so attacker-controlled `pad`
/// / `split` / `repeat`-style inputs cannot OOM the publish path; larger
/// requests return `Null` fail-closed instead of allocating.
pub const MAX_STDLIB_STRING_BYTES: usize = 1_048_576;

/// Caps any single array UDF result at 10 000 elements for the same reason
/// as [`MAX_STDLIB_STRING_BYTES`]: rule projections stay small and bounded
/// on the publish path.
pub const MAX_STDLIB_ARRAY_ELEMS: usize = 10_000;

/// Caps the `proc_dict` / `kv_store` maps at 10 000 entries so rule-state
/// memory stays bounded per node; puts past the cap return `Null` and are
/// refused rather than evicting silently.
pub const MAX_STDLIB_KV_ENTRIES: usize = 10_000;

/// Default `jq` evaluation budget in milliseconds. Reason: the documented
/// default timeout (`10s`); a call without an explicit timeout uses it.
pub const DEFAULT_JQ_TIMEOUT_MS: u64 = 10_000;

/// Upper bound for a per-call `jq` timeout in milliseconds. Reason: `jq`
/// runs on the rule path, so the budget stays bounded; 1 hour is far above
/// the documented default and refuses absurd values fail-closed.
pub const MAX_JQ_CALL_TIMEOUT_MS: u64 = 3_600_000;

/// Upper bound for dot-path segments in one `jq` program. Reason: each
/// segment costs one map lookup on the publish path; deeper programs are
/// not documented use and stop fail-closed instead of running unbounded.
pub const MAX_JQ_PATH_SEGMENTS: usize = 64;

/// Counts `jq` evaluations stopped by the deadline or the step budget.
/// Lock-free; a stopped evaluation returns `Null`, so the rule that asked
/// for it does not match and its `failed` counter moves.
static JQ_TIMEOUTS: AtomicU64 = AtomicU64::new(0);

/// Number of `jq` evaluations stopped so far by the deadline or the step
/// budget. Monotonic; tests read the delta around one evaluation.
pub fn jq_timeout_count() -> u64 {
    JQ_TIMEOUTS.load(Ordering::Relaxed)
}

fn count_jq_timeout() {
    JQ_TIMEOUTS.fetch_add(1, Ordering::Relaxed);
}

// ---------------------------------------------------------------------------
// Per-evaluation context for zero-argument accessors.
// ---------------------------------------------------------------------------

/// What a rule evaluation knows about the message around the payload.
#[derive(Debug, Default, Clone)]
pub struct RuleContext {
    pub topic: Option<String>,
    pub client_id: Option<String>,
    pub username: Option<String>,
    pub peerhost: Option<String>,
    pub qos: Option<u8>,
    pub msg_id: Option<String>,
    pub payload_raw: Option<String>,
    pub record: serde_json::Map<String, Value>,
    pub rule_id: String,
    /// True for dry-run evaluations (`try_evaluate`, the SQL studio tester):
    /// state-mutating UDFs (`proc_dict_put/del`, `kv_store_put/del`) fail
    /// closed with `Null` instead of touching the node-global maps, so a
    /// probe never mutates production state. Reads stay live. Fail-closed
    /// per the rulebook UNKNOWN rule; production paths leave this false.
    pub dry_run: bool,
}

thread_local! {
    static CURRENT_CTX: RefCell<RuleContext> = RefCell::new(RuleContext::default());
}

/// RAII guard: installs `ctx` for the current thread, restores the previous
/// one on drop (panic-safe; evaluations are synchronous on this thread).
pub struct RuleContextGuard {
    prev: RuleContext,
}

impl RuleContextGuard {
    pub fn enter(ctx: RuleContext) -> Self {
        let prev = CURRENT_CTX.with(|c| c.borrow().clone());
        CURRENT_CTX.with(|c| *c.borrow_mut() = ctx);
        Self { prev }
    }
}

impl Drop for RuleContextGuard {
    fn drop(&mut self) {
        let prev = std::mem::take(&mut self.prev);
        CURRENT_CTX.with(|c| *c.borrow_mut() = prev);
    }
}

fn with_ctx<T>(f: impl FnOnce(&RuleContext) -> T) -> T {
    CURRENT_CTX.with(|c| {
        let borrowed = c.borrow();
        f(&borrowed)
    })
}

/// True while the current thread runs a dry-run evaluation: mutating UDFs
/// must not touch production state.
fn is_dry_run() -> bool {
    with_ctx(|c| c.dry_run)
}

// ---------------------------------------------------------------------------
// Small value helpers (mirror the engine's stringify: Null becomes "",
// composite values become JSON; numbers/bools render plainly).
// ---------------------------------------------------------------------------

fn to_s(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
    }
}

fn as_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i)
            } else if let Some(u) = n.as_u64() {
                i64::try_from(u).ok()
            } else {
                n.as_f64()
                    .filter(|f| f.is_finite())
                    .map(|f| f.trunc() as i64)
            }
        }
        Value::String(s) => s.trim().parse::<i64>().ok(),
        Value::Bool(b) => Some(i64::from(*b)),
        _ => None,
    }
}

fn as_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok().filter(|f| f.is_finite()),
        Value::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        _ => None,
    }
}

fn capped_string(s: String) -> Value {
    if s.len() > MAX_STDLIB_STRING_BYTES {
        Value::Null
    } else {
        Value::String(s)
    }
}

fn capped_array(items: Vec<Value>) -> Value {
    if items.len() > MAX_STDLIB_ARRAY_ELEMS {
        Value::Null
    } else {
        Value::Array(items)
    }
}

fn str_arg(args: &[Value], i: usize) -> Option<String> {
    match args.get(i) {
        // Fail closed on Null/missing rather than rendering an empty string.
        Some(Value::Null) | None => None,
        Some(other) => Some(to_s(other)),
    }
}

fn object_arg(v: &Value) -> Option<serde_json::Map<String, Value>> {
    match v {
        Value::Object(m) => Some(m.clone()),
        Value::String(s) => match serde_json::from_str::<Value>(s) {
            Ok(Value::Object(m)) => Some(m),
            _ => None,
        },
        _ => None,
    }
}

fn split_path(path: &str) -> Vec<String> {
    path.split('.')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

fn get_path(obj: &serde_json::Map<String, Value>, path: &str) -> Value {
    let mut cur = Value::Object(obj.clone());
    for part in split_path(path) {
        match cur {
            Value::Object(m) => match m.get(&part) {
                Some(v) => cur = v.clone(),
                None => return Value::Null,
            },
            _ => return Value::Null,
        }
    }
    cur
}

fn put_path(obj: &mut serde_json::Map<String, Value>, path: &str, val: Value) {
    let parts = split_path(path);
    if parts.is_empty() {
        return;
    }
    let mut cur: &mut serde_json::Map<String, Value> = obj;
    for part in &parts[..parts.len() - 1] {
        if !cur.get(part).is_some_and(|v| v.is_object()) {
            cur.insert(part.clone(), Value::Object(serde_json::Map::new()));
        }
        match cur.get_mut(part).and_then(|v| v.as_object_mut()) {
            Some(next) => cur = next,
            None => return,
        }
    }
    cur.insert(parts[parts.len() - 1].clone(), val);
}

// ---------------------------------------------------------------------------
// Bounded state for proc_dict / kv_store.
// ---------------------------------------------------------------------------

// proc_dict is rule-worker-local: keyed by (rule_id, key) so one rule
// cannot read another rule's slots. The calling rule's id comes from the
// thread-local RuleContext installed around each evaluation; bare
// registry calls outside an evaluation see the empty rule id. kv_store
// stays node-global (key only). Both maps share the same finite entry
// cap so per-node memory stays bounded.
static PROC_DICT: std::sync::LazyLock<parking_lot::Mutex<HashMap<(String, String), Value>>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));
static KV_STORE: std::sync::LazyLock<parking_lot::Mutex<HashMap<String, Value>>> =
    std::sync::LazyLock::new(|| parking_lot::Mutex::new(HashMap::new()));

fn proc_dict_key(key: String) -> (String, String) {
    let rule_id = with_ctx(|c| c.rule_id.clone());
    (rule_id, key)
}

fn proc_get(key: &str) -> Option<Value> {
    let composite = proc_dict_key(key.to_string());
    PROC_DICT.lock().get(&composite).cloned()
}

fn proc_put(key: String, val: Value) -> Option<Value> {
    // Dry-run probes never mutate production state: fail closed.
    if is_dry_run() {
        return None;
    }
    let composite = proc_dict_key(key);
    let mut guard = PROC_DICT.lock();
    if !guard.contains_key(&composite) && guard.len() >= MAX_STDLIB_KV_ENTRIES {
        // Bounded: refuse past the cap rather than growing without limit.
        // Cold path only (taken when already full): counted in logs per
        // FUNCTIONS.md §11, never evicted silently.
        let rule_id = with_ctx(|c| c.rule_id.clone());
        tracing::warn!(
            rule_id = %rule_id,
            cap = MAX_STDLIB_KV_ENTRIES,
            "proc_dict put refused: entry cap reached"
        );
        return None;
    }
    guard.insert(composite, val.clone());
    Some(val)
}

fn proc_del(key: &str) -> Value {
    // Dry-run probes never mutate production state: fail closed.
    if is_dry_run() {
        return Value::Null;
    }
    let composite = proc_dict_key(key.to_string());
    PROC_DICT.lock().remove(&composite).unwrap_or(Value::Null)
}

fn state_get(store: &parking_lot::Mutex<HashMap<String, Value>>, key: &str) -> Option<Value> {
    store.lock().get(key).cloned()
}

fn state_put(
    store: &parking_lot::Mutex<HashMap<String, Value>>,
    key: String,
    val: Value,
) -> Option<Value> {
    // Dry-run probes never mutate production state: fail closed.
    if is_dry_run() {
        return None;
    }
    let mut guard = store.lock();
    if !guard.contains_key(&key) && guard.len() >= MAX_STDLIB_KV_ENTRIES {
        // Bounded: refuse past the cap rather than growing without limit.
        // Cold path only (taken when already full): counted in logs per
        // FUNCTIONS.md §11, never evicted silently.
        let rule_id = with_ctx(|c| c.rule_id.clone());
        tracing::warn!(
            rule_id = %rule_id,
            cap = MAX_STDLIB_KV_ENTRIES,
            "kv_store put refused: entry cap reached"
        );
        return None;
    }
    guard.insert(key, val.clone());
    Some(val)
}

fn state_del(store: &parking_lot::Mutex<HashMap<String, Value>>, key: &str) -> Value {
    // Dry-run probes never mutate production state: fail closed.
    if is_dry_run() {
        return Value::Null;
    }
    store.lock().remove(key).unwrap_or(Value::Null)
}

// ---------------------------------------------------------------------------
// 1. Context / message accessors
// ---------------------------------------------------------------------------

fn func_msgid(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    with_ctx(|c| {
        c.msg_id
            .clone()
            .map(Value::String)
            .or_else(|| {
                c.record
                    .get("id")
                    .or_else(|| c.record.get("msgid"))
                    .cloned()
            })
            .unwrap_or(Value::Null)
    })
}

fn func_qos(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    with_ctx(|c| {
        c.qos
            .map(|q| Value::from(q as i64))
            .or_else(|| c.record.get("qos").cloned())
            .unwrap_or(Value::Null)
    })
}

fn func_flags(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    with_ctx(|c| {
        c.record
            .get("flags")
            .cloned()
            .unwrap_or_else(|| Value::Object(serde_json::Map::new()))
    })
}

fn func_flag(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(name) = args[0].as_str().map(str::to_string) else {
        return Value::Null;
    };
    with_ctx(|c| match c.record.get("flags") {
        Some(Value::Object(m)) => m.get(&name).cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    })
}

fn func_topic(args: &[Value]) -> Value {
    match args.len() {
        0 => with_ctx(|c| {
            c.topic
                .clone()
                .map(Value::String)
                .or_else(|| c.record.get("topic").cloned())
                .unwrap_or(Value::Null)
        }),
        1 => {
            let Some(n) = as_i64(&args[0]) else {
                return Value::Null;
            };
            if n < 1 {
                return Value::Null;
            }
            with_ctx(|c| {
                let topic = c.topic.clone().or_else(|| {
                    c.record
                        .get("topic")
                        .and_then(|v| v.as_str())
                        .map(str::to_string)
                });
                match topic {
                    Some(t) => {
                        let levels: Vec<&str> = t.split('/').collect();
                        levels
                            .get((n - 1) as usize)
                            .map(|s| Value::String(s.to_string()))
                            .unwrap_or(Value::Null)
                    }
                    None => Value::Null,
                }
            })
        }
        _ => Value::Null,
    }
}

fn func_clientid(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    with_ctx(|c| {
        c.client_id
            .clone()
            .map(Value::String)
            .or_else(|| c.record.get("clientid").cloned())
            .unwrap_or(Value::Null)
    })
}

fn func_peerhost(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    with_ctx(|c| {
        c.peerhost
            .clone()
            .map(Value::String)
            .or_else(|| c.record.get("peerhost").cloned())
            .unwrap_or(Value::Null)
    })
}

fn func_username(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    with_ctx(|c| {
        c.username
            .clone()
            .map(Value::String)
            .or_else(|| c.record.get("username").cloned())
            .unwrap_or(Value::Null)
    })
}

fn func_payload(args: &[Value]) -> Value {
    match args.len() {
        0 => with_ctx(|c| match c.record.get("payload") {
            Some(v) => v.clone(),
            None => match &c.payload_raw {
                Some(raw) => Value::String(raw.clone()),
                // Fail closed with no context: an empty record is not a
                // payload (bare registry calls would otherwise see `{}`).
                None if c.record.is_empty() => Value::Null,
                None => Value::Object(c.record.clone().into_iter().collect()),
            },
        }),
        1 => {
            let Some(path) = args[0].as_str().map(str::to_string) else {
                return Value::Null;
            };
            with_ctx(|c| {
                let decoded: Option<Value> = c
                    .record
                    .get("payload")
                    .and_then(|p| p.as_str())
                    .and_then(|s| serde_json::from_str(s).ok())
                    .or_else(|| {
                        c.payload_raw
                            .as_deref()
                            .and_then(|s| serde_json::from_str(s).ok())
                    });
                match decoded {
                    Some(Value::Object(m)) => get_path(&m, &path),
                    Some(other) => {
                        if path.is_empty() {
                            other
                        } else {
                            Value::Null
                        }
                    }
                    None => match c.record.get("payload") {
                        Some(Value::Object(m)) => get_path(m, &path),
                        _ => Value::Null,
                    },
                }
            })
        }
        _ => Value::Null,
    }
}

fn topic_filter_list(v: &Value) -> Option<Vec<(String, Option<i64>)>> {
    match v {
        Value::Array(items) => {
            // Bound: attacker-controlled filter lists on the publish path
            // must not grow without limit; refuse oversized lists
            // fail-closed instead of allocating (same reason as
            // MAX_STDLIB_ARRAY_ELEMS: rule projections stay small).
            if items.len() > MAX_STDLIB_ARRAY_ELEMS {
                return None;
            }
            // Capacity hint only (no pre-allocation of attacker size);
            // the length check above is the bound. Reason: hint capped at
            // 1024 entries so a 10 000-element list does not pre-allocate
            // the full bound up front on the publish path.
            let mut out = Vec::with_capacity(items.len().min(1024));
            for item in items {
                match item {
                    Value::String(s) => out.push((s.clone(), None)),
                    Value::Object(m) => {
                        let topic = m.get("topic").and_then(|t| t.as_str()).map(str::to_string);
                        let qos = m.get("qos").and_then(as_i64);
                        out.push((topic?, qos));
                    }
                    _ => return None,
                }
            }
            Some(out)
        }
        Value::String(s) => Some(vec![(s.clone(), None)]),
        _ => None,
    }
}

fn topic_matches(filter: &str, topic: &str) -> bool {
    let mut f = filter.split('/');
    let mut t = topic.split('/');
    loop {
        match (f.next(), t.next()) {
            (Some("#"), _) => return true,
            (Some("+"), Some(_)) => continue,
            (Some(a), Some(b)) if a == b => continue,
            (None, None) => return true,
            _ => return false,
        }
    }
}

fn func_contains_topic(args: &[Value]) -> Value {
    if args.len() != 2 && args.len() != 3 {
        return Value::Null;
    }
    let Some(filters) = topic_filter_list(&args[0]) else {
        return Value::Null;
    };
    let Value::String(topic) = &args[1] else {
        return Value::Null;
    };
    let want_qos = if args.len() == 3 {
        Some(match as_i64(&args[2]) {
            Some(q) => q,
            None => return Value::Null,
        })
    } else {
        None
    };
    for (filter, qos) in filters {
        if filter == *topic {
            match (want_qos, qos) {
                (Some(w), Some(have)) => {
                    if w == have {
                        return Value::Bool(true);
                    }
                }
                (None, _) => return Value::Bool(true),
                (Some(_), None) => continue,
            }
        }
    }
    Value::Bool(false)
}

fn func_contains_topic_match(args: &[Value]) -> Value {
    if args.len() != 2 && args.len() != 3 {
        return Value::Null;
    }
    let Some(filters) = topic_filter_list(&args[0]) else {
        return Value::Null;
    };
    let Value::String(topic) = &args[1] else {
        return Value::Null;
    };
    let want_qos = if args.len() == 3 {
        Some(match as_i64(&args[2]) {
            Some(q) => q,
            None => return Value::Null,
        })
    } else {
        None
    };
    for (filter, qos) in filters {
        if topic_matches(&filter, topic) {
            match (want_qos, qos) {
                (Some(w), Some(have)) => {
                    if w == have {
                        return Value::Bool(true);
                    }
                }
                (None, _) => return Value::Bool(true),
                (Some(_), None) => continue,
            }
        }
    }
    Value::Bool(false)
}

fn func_topic_match(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let Value::String(topic) = &args[0] else {
        return Value::Null;
    };
    let Value::String(filter) = &args[1] else {
        return Value::Null;
    };
    let Ok(filter) = broker_protocol::TopicFilter::new(filter) else {
        return Value::Null;
    };
    let Ok(topic) = broker_protocol::Topic::new(topic) else {
        return Value::Null;
    };
    Value::Bool(filter.matches(&topic))
}

fn func_null(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    Value::Null
}

fn func_coalesce_ne(args: &[Value]) -> Value {
    if args.is_empty() {
        return Value::Null;
    }
    for v in args {
        match v {
            Value::Null => continue,
            Value::String(s) if s.is_empty() => continue,
            other => return other.clone(),
        }
    }
    Value::Null
}

fn func_div(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let (Some(x), Some(y)) = (as_i64(&args[0]), as_i64(&args[1])) else {
        return Value::Null;
    };
    if y == 0 {
        return Value::Null;
    }
    // No allocation on this path (pure integer arithmetic).
    Value::from(x / y)
}

fn func_fmod(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let (Some(x), Some(y)) = (as_f64(&args[0]), as_f64(&args[1])) else {
        return Value::Null;
    };
    if y == 0.0 || !x.is_finite() || !y.is_finite() {
        return Value::Null;
    }
    // No allocation on this path.
    serde_json::json!(x - (x / y).trunc() * y)
}

// ---------------------------------------------------------------------------
// 2-3. Math / bitwise additions
// ---------------------------------------------------------------------------

fn func_acosh(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(x) = as_f64(&args[0]) else {
        return Value::Null;
    };
    if x < 1.0 {
        return Value::Null;
    }
    serde_json::json!(x.acosh())
}

fn func_asinh(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(x) = as_f64(&args[0]) else {
        return Value::Null;
    };
    serde_json::json!(x.asinh())
}

fn func_atanh(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(x) = as_f64(&args[0]) else {
        return Value::Null;
    };
    if x <= -1.0 || x >= 1.0 {
        return Value::Null;
    }
    serde_json::json!(x.atanh())
}

fn func_bitsl(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let (Some(x), Some(n)) = (as_i64(&args[0]), as_i64(&args[1])) else {
        return Value::Null;
    };
    if !(0..=63).contains(&n) {
        return Value::Null;
    }
    match x.checked_shl(n as u32) {
        Some(v) => Value::from(v),
        None => Value::Null,
    }
}

fn func_bitsr(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let (Some(x), Some(n)) = (as_i64(&args[0]), as_i64(&args[1])) else {
        return Value::Null;
    };
    if !(0..=63).contains(&n) {
        return Value::Null;
    }
    // Arithmetic shift preserves the sign. No allocation.
    Value::from(x >> (n as u32))
}

// ---------------------------------------------------------------------------
// 4. Bit and byte
// ---------------------------------------------------------------------------

fn func_bitsize(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        Value::String(s) => Value::from((s.len() as i64) * 8),
        _ => Value::Null,
    }
}

fn func_bytesize(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        // Allocates nothing (length probe only). Non-strings mean Null
        // per FUNCTIONS.md; arrays are not byte strings.
        Value::String(s) => Value::from(s.len() as i64),
        _ => Value::Null,
    }
}

fn parse_bit_type(s: &str) -> Option<&str> {
    match s.trim().to_ascii_lowercase().as_str() {
        "integer" => Some("integer"),
        "float" => Some("float"),
        "bits" | "binary" => Some("bits"),
        _ => None,
    }
}

fn func_subbits(args: &[Value]) -> Value {
    if args.len() < 2 || args.len() > 6 {
        return Value::Null;
    }
    let Value::String(bits) = &args[0] else {
        return Value::Null;
    };
    let bytes = bits.as_bytes();
    let total_bits = bytes.len() as i64 * 8;
    let (start, len) = if args.len() == 2 {
        let Some(len) = as_i64(&args[1]) else {
            return Value::Null;
        };
        (1i64, len)
    } else {
        let (Some(s), Some(l)) = (as_i64(&args[1]), as_i64(&args[2])) else {
            return Value::Null;
        };
        (s, l)
    };
    if start < 1 || len < 0 || start - 1 > total_bits {
        return Value::Null;
    }
    let mut typ = "integer";
    let mut signed = "unsigned";
    let mut endian = "big";
    if args.len() >= 4 {
        let Some(t) = args[3].as_str().and_then(parse_bit_type) else {
            return Value::Null;
        };
        typ = t;
    }
    if args.len() >= 5 {
        match args[4].as_str().map(|s| s.trim().to_ascii_lowercase()) {
            Some(s) if s == "signed" => signed = "signed",
            Some(s) if s == "unsigned" => signed = "unsigned",
            _ => return Value::Null,
        }
    }
    if args.len() >= 6 {
        match args[5].as_str().map(|s| s.trim().to_ascii_lowercase()) {
            Some(s) if s == "big" => endian = "big",
            Some(s) if s == "little" => endian = "little",
            _ => return Value::Null,
        }
    }
    // Extract bits MSB-first into a u128 accumulator (clamped to 128 bits;
    // wider `bits` slices are returned base64-encoded below).
    let begin = start - 1;
    let available = (total_bits - begin).max(0);
    let take = len.min(available);
    if take <= 0 {
        return match typ {
            "integer" => Value::from(0i64),
            _ => capped_string(String::new()),
        };
    }
    if typ == "bits" {
        // Byte-aligned fast path; sub-byte slices are base64 of the raw
        // bytes intersected (documented approximation).
        // TODO(parity): exact sub-byte bit extraction for the bits type.
        let byte_start = (begin / 8) as usize;
        // Ceiling division by 8 (bits per byte; +7 rounds up). Spelled out
        // instead of `div_ceil` because the gate's stable toolchain rejects
        // `i64::div_ceil` as unstable feature `int_roundings`.
        let byte_end = ((begin + take + 7) / 8).min(bytes.len() as i64) as usize;
        if byte_start >= bytes.len() {
            return capped_string(String::new());
        }
        use base64::Engine as _;
        let encoded =
            base64::engine::general_purpose::STANDARD.encode(&bytes[byte_start..byte_end]);
        return capped_string(encoded);
    }
    if take > 64 {
        // Reason: the integer accumulator below holds at most 64 value bits
        // (wider slices use the base64 `bits` path above); wider requests
        // fail the evaluation with `Null`, never the broker.
        return Value::Null;
    }
    let mut acc: u128 = 0;
    for i in 0..take {
        let bit_pos = begin + i;
        let byte = bytes[(bit_pos / 8) as usize];
        // MSB-first within each byte.
        let bit = (byte >> (7 - (bit_pos % 8))) & 1;
        acc = (acc << 1) | u128::from(bit);
    }
    // Little-endian reverses byte order of the extracted value.
    if endian == "little" && take >= 8 {
        let nbytes = (take as usize).div_ceil(8);
        let mut be = vec![0u8; nbytes];
        let mut tmp = acc;
        for b in be.iter_mut().rev() {
            *b = (tmp & 0xff) as u8;
            tmp >>= 8;
        }
        be.reverse();
        acc = 0;
        for b in be {
            acc = (acc << 8) | u128::from(b);
        }
    }
    if typ == "float" {
        // Reason: IEEE-754 defines only 32- and 64-bit float layouts, so any
        // other width fails the evaluation with `Null`.
        if take == 32 {
            let f = f32::from_bits(acc as u32) as f64;
            if f.is_finite() {
                return serde_json::json!(f);
            }
            return Value::Null;
        }
        if take == 64 {
            let f = f64::from_bits(acc as u64);
            if f.is_finite() {
                return serde_json::json!(f);
            }
            return Value::Null;
        }
        return Value::Null;
    }
    // Integer (signedness applied).
    if signed == "signed" && take > 0 && (acc >> (take - 1)) & 1 == 1 {
        let extended = acc | (!0u128 << take);
        return Value::from(extended as i128 as i64);
    }
    if acc <= i64::MAX as u128 {
        Value::from(acc as i64)
    } else {
        Value::Null
    }
}

// ---------------------------------------------------------------------------
// 5. Type conversion
// ---------------------------------------------------------------------------

fn func_str(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    if args[0].is_null() {
        return Value::Null;
    }
    // Allocates one String for the result, like the engine string helpers.
    capped_string(to_s(&args[0]))
}

fn func_str_utf8(args: &[Value]) -> Value {
    func_str(args)
}

fn func_str_utf16_le(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    if args[0].is_null() {
        return Value::Null;
    }
    let s = to_s(&args[0]);
    // Raw UTF-16LE is not valid UTF-8 and cannot sit in a JSON string, so
    // the bytes are base64-encoded to preserve them (see FUNCTIONS.md).
    // Must allocate the encoded buffer; the input is attacker-controlled
    // but capped below.
    let units: Vec<u16> = s.encode_utf16().collect();
    if units.len() * 2 > MAX_STDLIB_STRING_BYTES {
        return Value::Null;
    }
    let mut raw = Vec::with_capacity(units.len() * 2);
    for u in units {
        raw.extend_from_slice(&u.to_le_bytes());
    }
    use base64::Engine as _;
    capped_string(base64::engine::general_purpose::STANDARD.encode(&raw))
}

fn func_bool(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        Value::Bool(b) => Value::Bool(*b),
        // Only 1/0 convert; any other number means Null per FUNCTIONS.md
        // (documented truthiness table: true/false, 1/0, "true"/"false").
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                match i {
                    1 => Value::Bool(true),
                    0 => Value::Bool(false),
                    _ => Value::Null,
                }
            } else if let Some(f) = n.as_f64() {
                if f == 1.0 {
                    Value::Bool(true)
                } else if f == 0.0 {
                    Value::Bool(false)
                } else {
                    Value::Null
                }
            } else {
                Value::Null
            }
        }
        Value::String(s) => match s.trim().to_ascii_lowercase().as_str() {
            "true" | "1" => Value::Bool(true),
            "false" | "0" => Value::Bool(false),
            _ => Value::Null,
        },
        _ => Value::Null,
    }
}

fn func_int(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match as_i64(&args[0]) {
        Some(i) => Value::from(i),
        None => Value::Null,
    }
}

fn func_float(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::Null;
    }
    let Some(x) = as_f64(&args[0]) else {
        return Value::Null;
    };
    if args.len() == 2 {
        let Some(d) = as_i64(&args[1]) else {
            return Value::Null;
        };
        // Reason: more than 20 decimals exceeds f64's ~15-17 significant
        // digits and would render noise; the documented precision range is
        // 0-20, anything else fails closed.
        if !(0..=20).contains(&d) {
            return Value::Null;
        }
        let factor = 10f64.powi(d as i32);
        return serde_json::json!((x * factor).round() / factor);
    }
    serde_json::json!(x)
}

fn func_float2str(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let (Some(x), Some(p)) = (as_f64(&args[0]), as_i64(&args[1])) else {
        return Value::Null;
    };
    // Reason: same f64 precision bound as func_float above (0-20 decimals
    // are exactly representable in the fixed-precision rendering).
    if !(0..=20).contains(&p) {
        return Value::Null;
    }
    capped_string(format!("{x:.prec$}", prec = p as usize))
}

fn func_map(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        Value::Object(_) => args[0].clone(),
        Value::String(s) => match serde_json::from_str::<Value>(s) {
            Ok(Value::Object(_)) => serde_json::from_str::<Value>(s).unwrap_or(Value::Null),
            _ => Value::Null,
        },
        Value::Array(items) => {
            // Bound: attacker-controlled pair lists on the publish path
            // must not grow without limit; refuse oversized inputs
            // fail-closed (same reason as MAX_STDLIB_ARRAY_ELEMS).
            if items.len() > MAX_STDLIB_ARRAY_ELEMS {
                return Value::Null;
            }
            // Capacity hint only; the length check above is the bound.
            // Reason: hint capped at 64 pairs so a 10 000-pair list does not
            // pre-allocate the full bound up front on the publish path.
            let mut m = serde_json::Map::with_capacity(items.len().min(64));
            for item in items {
                match item {
                    Value::Array(pair) if pair.len() == 2 => {
                        let Value::String(k) = &pair[0] else {
                            return Value::Null;
                        };
                        m.insert(k.clone(), pair[1].clone());
                    }
                    _ => return Value::Null,
                }
            }
            Value::Object(m)
        }
        _ => Value::Null,
    }
}

fn hex_of(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789ABCDEF";
    let mut out = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        out.push(DIGITS[(b >> 4) as usize] as char);
        out.push(DIGITS[(b & 0xf) as usize] as char);
    }
    out
}

fn func_bin2hexstr(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::Null;
    }
    let Some(s) = str_arg(args, 0) else {
        return Value::Null;
    };
    let mut hex = hex_of(s.as_bytes());
    if args.len() == 2 {
        let Some(prefix) = str_arg(args, 1) else {
            return Value::Null;
        };
        let mut out = String::with_capacity(prefix.len() + hex.len());
        out.push_str(&prefix);
        out.push_str(&hex);
        hex = out;
    }
    // Allocates one String for the hex digits, like the engine encoders.
    capped_string(hex)
}

fn hex_val(c: u8) -> Option<u8> {
    match c {
        b'0'..=b'9' => Some(c - b'0'),
        b'a'..=b'f' => Some(c - b'a' + 10),
        b'A'..=b'F' => Some(c - b'A' + 10),
        _ => None,
    }
}

fn hex_decode(s: &str) -> Option<Vec<u8>> {
    let b = s.as_bytes();
    if !b.len().is_multiple_of(2) {
        return None;
    }
    let mut out = Vec::with_capacity(b.len() / 2);
    let mut i = 0;
    while i < b.len() {
        let (Some(hi), Some(lo)) = (hex_val(b[i]), hex_val(b[i + 1])) else {
            return None;
        };
        out.push((hi << 4) | lo);
        i += 2;
    }
    Some(out)
}

fn func_hexstr2bin(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::Null;
    }
    let Some(mut s) = str_arg(args, 0) else {
        return Value::Null;
    };
    if args.len() == 2 {
        let Some(prefix) = str_arg(args, 1) else {
            return Value::Null;
        };
        match s.strip_prefix(prefix.as_str()) {
            Some(rest) => s = rest.to_string(),
            None => return Value::Null,
        }
    }
    let Some(bytes) = hex_decode(s.trim()) else {
        return Value::Null;
    };
    match String::from_utf8(bytes) {
        Ok(text) => capped_string(text),
        Err(_) => Value::Null,
    }
}

fn func_sqlserver_bin2hexstr(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(s) = str_arg(args, 0) else {
        return Value::Null;
    };
    let mut out = String::with_capacity(2 + s.len() * 2);
    out.push_str("0x");
    out.push_str(&hex_of(s.as_bytes()));
    capped_string(out)
}

// ---------------------------------------------------------------------------
// 6. Type checks
// ---------------------------------------------------------------------------

fn func_is_null(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(args[0].is_null())
}

fn func_is_null_var(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(args[0].is_null())
}

fn func_is_not_null(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(!args[0].is_null())
}

fn func_is_not_null_var(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(!args[0].is_null())
}

fn func_is_str(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(args[0].is_string())
}

fn func_is_bool(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(args[0].is_boolean())
}

fn func_is_int(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(
        args[0]
            .as_number()
            .is_some_and(|n| n.is_i64() || n.is_u64()),
    )
}

fn func_is_float(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(args[0].as_number().is_some_and(|n| n.is_f64()))
}

fn func_is_num(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(args[0].is_number())
}

fn func_is_map(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(args[0].is_object())
}

fn func_is_array(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    Value::Bool(args[0].is_array())
}

fn func_is_empty(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let empty = match &args[0] {
        Value::Null => true,
        Value::String(s) => s.is_empty(),
        Value::Array(a) => a.is_empty(),
        Value::Object(m) => m.is_empty(),
        _ => false,
    };
    Value::Bool(empty)
}

// ---------------------------------------------------------------------------
// 7. Strings
// ---------------------------------------------------------------------------

fn func_strlen(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        Value::String(s) => Value::from(s.chars().count() as i64),
        _ => Value::Null,
    }
}

fn func_substr(args: &[Value]) -> Value {
    if args.len() != 2 && args.len() != 3 {
        return Value::Null;
    }
    let Value::String(s) = &args[0] else {
        return Value::Null;
    };
    // Documented standard form is 0-based here (differs from the engine
    // 1-based built-in of the same name; see FUNCTIONS.md §11).
    let Some(start) = as_i64(&args[1]) else {
        return Value::Null;
    };
    let chars: Vec<char> = s.chars().collect();
    let len = chars.len() as i64;
    let mut from = if start < 0 {
        (len + start).max(0)
    } else {
        start
    };
    if from > len {
        from = len;
    }
    let to = if args.len() == 3 {
        let Some(count) = as_i64(&args[2]) else {
            return Value::Null;
        };
        if count < 0 {
            return Value::Null;
        }
        (from + count).min(len)
    } else {
        len
    };
    if from >= to {
        return capped_string(String::new());
    }
    capped_string(chars[from as usize..to as usize].iter().collect())
}

fn func_split(args: &[Value]) -> Value {
    if args.len() != 2 && args.len() != 3 {
        return Value::Null;
    }
    // Fail closed on wrong types: numbers/bools are not strings here, so
    // `as_str` (not the coercing `str_arg`) is required.
    let (Some(s), Some(sep)) = (
        args[0].as_str().map(str::to_string),
        args[1].as_str().map(str::to_string),
    ) else {
        return Value::Null;
    };
    if sep.is_empty() {
        return Value::Null;
    }
    if args.len() == 3 {
        let Some(opt) = args[2].as_str().map(|v| v.trim().to_ascii_lowercase()) else {
            return Value::Null;
        };
        let parts: Vec<&str> = match opt.as_str() {
            "notrim" => s.split(sep.as_str()).collect(),
            "leading_notrim" => s.splitn(2, sep.as_str()).collect(),
            "leading" => {
                let mut out = Vec::new();
                for p in s.splitn(2, sep.as_str()) {
                    if !p.is_empty() {
                        out.push(p);
                    }
                }
                out
            }
            "trailing_notrim" => match s.rsplit_once(sep.as_str()) {
                Some((a, b)) => vec![a, b],
                None => vec![s.as_str()],
            },
            "trailing" => {
                let out = match s.rsplit_once(sep.as_str()) {
                    Some((a, b)) => vec![a, b],
                    None => vec![s.as_str()],
                };
                let filtered: Vec<&str> = out.into_iter().filter(|p| !p.is_empty()).collect();
                filtered
            }
            _ => return Value::Null,
        };
        if parts.len() > MAX_STDLIB_ARRAY_ELEMS {
            return Value::Null;
        }
        return capped_array(
            parts
                .into_iter()
                .map(|p| Value::String(p.to_string()))
                .collect(),
        );
    }
    // Standard 2-arg form drops empty segments.
    let parts: Vec<Value> = s
        .split(sep.as_str())
        .filter(|p| !p.is_empty())
        .map(|p| Value::String(p.to_string()))
        .collect();
    capped_array(parts)
}

fn func_concat(args: &[Value]) -> Value {
    match args.len() {
        1 => match &args[0] {
            Value::Array(items) => {
                if items.len() > MAX_STDLIB_ARRAY_ELEMS {
                    return Value::Null;
                }
                let mut out = String::new();
                for item in items {
                    let part = to_s(item);
                    if out.len() + part.len() > MAX_STDLIB_STRING_BYTES {
                        return Value::Null;
                    }
                    out.push_str(&part);
                }
                capped_string(out)
            }
            _ => Value::Null,
        },
        2 => {
            if args[0].is_null() || args[1].is_null() {
                return Value::Null;
            }
            let (a, b) = (to_s(&args[0]), to_s(&args[1]));
            if a.len() + b.len() > MAX_STDLIB_STRING_BYTES {
                return Value::Null;
            }
            let mut out = String::with_capacity(a.len() + b.len());
            out.push_str(&a);
            out.push_str(&b);
            capped_string(out)
        }
        _ => Value::Null,
    }
}

fn func_tokens(args: &[Value]) -> Value {
    if args.len() != 2 && args.len() != 3 {
        return Value::Null;
    }
    // Fail closed on wrong types: numbers/bools are not strings here, so
    // `as_str` (not the coercing `str_arg`) is required.
    let (Some(s), Some(seps)) = (
        args[0].as_str().map(str::to_string),
        args[1].as_str().map(str::to_string),
    ) else {
        return Value::Null;
    };
    if args.len() == 3 {
        let Some(mode) = args[2].as_str().map(|v| v.trim().to_ascii_lowercase()) else {
            return Value::Null;
        };
        if mode != "nocrlf" {
            return Value::Null;
        }
        let extra = ['\r', '\n'];
        let mut sep_chars: Vec<char> = seps.chars().collect();
        sep_chars.extend_from_slice(&extra);
        let parts: Vec<Value> = s
            .split(sep_chars.as_slice())
            .filter(|p| !p.is_empty())
            .map(|p| Value::String(p.to_string()))
            .collect();
        return capped_array(parts);
    }
    let sep_chars: Vec<char> = seps.chars().collect();
    if sep_chars.is_empty() {
        return Value::Null;
    }
    let parts: Vec<Value> = s
        .split(sep_chars.as_slice())
        .filter(|p| !p.is_empty())
        .map(|p| Value::String(p.to_string()))
        .collect();
    capped_array(parts)
}

fn func_sprintf(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let Value::String(fmt) = &args[0] else {
        return Value::Null;
    };
    let Value::Array(list) = &args[1] else {
        return Value::Null;
    };
    // Bound: attacker-controlled arg lists on the publish path must not grow
    // without limit; fail the evaluation with `Null` (same 10 000-element
    // reason as MAX_STDLIB_ARRAY_ELEMS).
    if list.len() > MAX_STDLIB_ARRAY_ELEMS {
        return Value::Null;
    }
    let mut out = String::with_capacity(fmt.len() + list.len() * 8);
    let mut arg_iter = list.iter();
    let mut chars = fmt.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '~' {
            out.push(c);
            continue;
        }
        match chars.next() {
            // TODO(parity): full format-spec coverage; this implements the
            // common `~s ~p ~w ~n` subset and `~~`.
            Some('s') | Some('p') | Some('w') => match arg_iter.next() {
                Some(v) => out.push_str(&to_s(v)),
                None => return Value::Null,
            },
            Some('n') => out.push('\n'),
            Some('~') => out.push('~'),
            _ => return Value::Null,
        }
        if out.len() > MAX_STDLIB_STRING_BYTES {
            return Value::Null;
        }
    }
    capped_string(out)
}

fn func_pad(args: &[Value]) -> Value {
    if args.len() < 2 || args.len() > 4 {
        return Value::Null;
    }
    let Some(s) = str_arg(args, 0) else {
        return Value::Null;
    };
    let Some(width) = as_i64(&args[1]) else {
        return Value::Null;
    };
    if width < 0 || width as usize > MAX_STDLIB_STRING_BYTES {
        return Value::Null;
    }
    let mut side = "trailing".to_string();
    if args.len() >= 3 {
        let Some(m) = args[2].as_str().map(|v| v.trim().to_ascii_lowercase()) else {
            return Value::Null;
        };
        match m.as_str() {
            "leading" | "trailing" | "both" => side = m,
            _ => return Value::Null,
        }
    }
    let mut pad_char = " ".to_string();
    if args.len() == 4 {
        let Some(c) = str_arg(args, 3) else {
            return Value::Null;
        };
        if c.chars().count() != 1 {
            return Value::Null;
        }
        pad_char = c;
    }
    let cur = s.chars().count() as i64;
    if cur >= width {
        return capped_string(s);
    }
    let need = (width - cur) as usize;
    let pad: String = pad_char.chars().cycle().take(need).collect();
    let out = match side.as_str() {
        "leading" => format!("{pad}{s}"),
        "both" => {
            let left = need / 2;
            let right = need - left;
            let l: String = pad_char.chars().cycle().take(left).collect();
            let r: String = pad_char.chars().cycle().take(right).collect();
            format!("{l}{s}{r}")
        }
        _ => format!("{s}{pad}"),
    };
    capped_string(out)
}

fn func_replace(args: &[Value]) -> Value {
    if args.len() != 3 && args.len() != 4 {
        return Value::Null;
    }
    let (Some(src), Some(pat), Some(rep)) = (str_arg(args, 0), str_arg(args, 1), str_arg(args, 2))
    else {
        return Value::Null;
    };
    if pat.is_empty() {
        return Value::Null;
    }
    if args.len() == 3 {
        return capped_string(src.replace(pat.as_str(), rep.as_str()));
    }
    let Some(where_) = args[3].as_str().map(|v| v.trim().to_ascii_lowercase()) else {
        return Value::Null;
    };
    let out = match where_.as_str() {
        "all" => src.replace(pat.as_str(), rep.as_str()),
        "leading" => match src.strip_prefix(pat.as_str()) {
            Some(rest) => format!("{rep}{rest}"),
            None => src,
        },
        "trailing" => match src.strip_suffix(pat.as_str()) {
            Some(rest) => format!("{rest}{rep}"),
            None => src,
        },
        _ => return Value::Null,
    };
    capped_string(out)
}

fn func_regex_match(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    // Fail closed on wrong types: both arguments must be strings, so
    // `as_str` (not the coercing `str_arg`) is required. A number such as
    // `1` is not a string and yields `Null`, per the general semantics in
    // `FUNCTIONS.md`.
    let (Some(s), Some(re)) = (
        args[0].as_str().map(str::to_string),
        args[1].as_str().map(str::to_string),
    ) else {
        return Value::Null;
    };
    // Compiles the pattern per call (one allocation): there is no shared
    // pattern cache on the publish path. PERF(parity): a bounded cache
    // would help repeated patterns but must not add a hot-path lock
    // without before/after numbers.
    let Ok(compiled) = regex::Regex::new(re.as_str()) else {
        return Value::Null;
    };
    Value::Bool(compiled.is_match(&s))
}

fn func_regex_replace(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }
    let (Some(src), Some(re), Some(rep)) = (str_arg(args, 0), str_arg(args, 1), str_arg(args, 2))
    else {
        return Value::Null;
    };
    let Ok(compiled) = regex::Regex::new(re.as_str()) else {
        return Value::Null;
    };
    capped_string(compiled.replace_all(&src, rep.as_str()).into_owned())
}

fn func_regex_extract(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let (Some(s), Some(re)) = (str_arg(args, 0), str_arg(args, 1)) else {
        return Value::Null;
    };
    let Ok(compiled) = regex::Regex::new(re.as_str()) else {
        return Value::Null;
    };
    let Some(caps) = compiled.captures(&s) else {
        return capped_array(Vec::new());
    };
    if caps.len() <= 1 {
        return capped_array(Vec::new());
    }
    let mut out = Vec::with_capacity(caps.len() - 1);
    for i in 1..caps.len() {
        out.push(Value::String(
            caps.get(i)
                .map(|m| m.as_str().to_string())
                .unwrap_or_default(),
        ));
    }
    capped_array(out)
}

fn func_ascii(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        Value::String(s) => match s.bytes().next() {
            Some(b) => Value::from(i64::from(b)),
            None => Value::Null,
        },
        _ => Value::Null,
    }
}

fn func_find(args: &[Value]) -> Value {
    if args.len() != 2 && args.len() != 3 {
        return Value::Null;
    }
    let (Some(s), Some(pat)) = (str_arg(args, 0), str_arg(args, 1)) else {
        return Value::Null;
    };
    if pat.is_empty() {
        return Value::Null;
    }
    let mut dir = "leading".to_string();
    if args.len() == 3 {
        let Some(d) = args[2].as_str().map(|v| v.trim().to_ascii_lowercase()) else {
            return Value::Null;
        };
        match d.as_str() {
            "leading" | "trailing" => dir = d,
            _ => return Value::Null,
        }
    }
    let found = if dir == "trailing" {
        s.rfind(pat.as_str()).map(|pos| s[pos..].to_string())
    } else {
        s.find(pat.as_str()).map(|pos| s[pos..].to_string())
    };
    capped_string(found.unwrap_or_default())
}

fn func_join_to_string(args: &[Value]) -> Value {
    let (sep, list) = match args.len() {
        1 => match &args[0] {
            Value::Array(items) => (", ".to_string(), items.clone()),
            _ => return Value::Null,
        },
        2 => match (&args[0], &args[1]) {
            (Value::String(sep), Value::Array(items)) => (sep.clone(), items.clone()),
            _ => return Value::Null,
        },
        _ => return Value::Null,
    };
    if list.len() > MAX_STDLIB_ARRAY_ELEMS {
        return Value::Null;
    }
    let mut out = String::new();
    for (i, item) in list.iter().enumerate() {
        if i > 0 {
            if out.len() + sep.len() > MAX_STDLIB_STRING_BYTES {
                return Value::Null;
            }
            out.push_str(&sep);
        }
        let part = to_s(item);
        if out.len() + part.len() > MAX_STDLIB_STRING_BYTES {
            return Value::Null;
        }
        out.push_str(&part);
    }
    capped_string(out)
}

/// Render a float with 6 compact decimals for `map_to_redis_hset_args`
/// (see FUNCTIONS.md): fixed 6 places, then trailing zeros trimmed while
/// keeping one decimal digit so integral floats stay `X.0`.
fn redis_float_str(f: f64) -> String {
    // Reason for 6 decimals: the documented wire rendering; fixed width
    // keeps the allocation bounded (at most a short stack-sized String).
    let s = format!("{f:.6}");
    let trimmed = s.trim_end_matches('0');
    if trimmed.ends_with('.') {
        format!("{trimmed}0")
    } else {
        trimmed.to_string()
    }
}

fn func_map_to_redis_hset_args(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let map = match &args[0] {
        Value::Object(m) => m.clone(),
        Value::String(s) => match serde_json::from_str::<Value>(s) {
            Ok(Value::Object(m)) => m,
            // Wrong types mean Null per the general semantics in
            // FUNCTIONS.md (fail closed, never a marker value).
            _ => return Value::Null,
        },
        // Wrong types mean Null per the general semantics in FUNCTIONS.md.
        _ => return Value::Null,
    };
    let mut out = vec![Value::String("map_to_redis_hset_args".to_string())];
    for (k, v) in map {
        // Non-string keys are dropped per the documented behaviour.
        if k.is_empty() {
            continue;
        }
        let field = match v {
            Value::String(s) => Value::String(s),
            Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::String(i.to_string())
                } else if let Some(u) = n.as_u64() {
                    Value::String(u.to_string())
                } else if let Some(f) = n.as_f64() {
                    // Allocates one short String for the 6-compact-decimal
                    // rendering, like the other stringified results.
                    Value::String(redis_float_str(f))
                } else {
                    Value::String(n.to_string())
                }
            }
            Value::Bool(b) => Value::String(b.to_string()),
            Value::Null => continue,
            _ => Value::String(serde_json::to_string(&v).unwrap_or_default()),
        };
        out.push(Value::String(k));
        out.push(field);
        if out.len() > MAX_STDLIB_ARRAY_ELEMS {
            return Value::Null;
        }
    }
    capped_array(out)
}

fn quote_sql_value(v: &Value) -> String {
    match v {
        Value::Null => "NULL".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Array(items) => {
            let inner: Vec<String> = items.iter().map(quote_sql_value).collect();
            format!("({})", inner.join(", "))
        }
        Value::Object(_) => format!(
            "'{}'",
            serde_json::to_string(v)
                .unwrap_or_default()
                .replace('\'', "''")
        ),
    }
}

fn func_join_to_sql_values_string(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Value::Array(items) = &args[0] else {
        return Value::Null;
    };
    if items.len() > MAX_STDLIB_ARRAY_ELEMS {
        return Value::Null;
    }
    // TODO(parity): exact quoting/escaping rules for nested values.
    capped_string(
        items
            .iter()
            .map(quote_sql_value)
            .collect::<Vec<_>>()
            .join(", "),
    )
}

/// Outcome of one budgeted `jq` evaluation.
enum JqEval {
    /// The program ran inside the budget.
    Values(Vec<Value>),
    /// The program is valid but selects nothing.
    Empty,
    /// The deadline passed or the step budget ran out. Counted.
    Timeout,
}

fn jq_dot_path_budgeted(prog: &str, doc: &Value, deadline: Instant) -> JqEval {
    let p = prog.trim();
    if Instant::now() >= deadline {
        count_jq_timeout();
        return JqEval::Timeout;
    }
    if p == "." {
        return JqEval::Values(vec![doc.clone()]);
    }
    let Some(path) = p.strip_prefix('.') else {
        return JqEval::Empty;
    };
    if path.is_empty() || path.contains('[') || path.contains('*') || path.contains('|') {
        return JqEval::Empty;
    }
    // Stop a program past the step budget before traversal.
    if path.split('.').count() > MAX_JQ_PATH_SEGMENTS {
        count_jq_timeout();
        return JqEval::Timeout;
    }
    let mut cur = doc.clone();
    let mut steps = 0usize;
    for part in path.split('.') {
        steps += 1;
        if steps > MAX_JQ_PATH_SEGMENTS {
            count_jq_timeout();
            return JqEval::Timeout;
        }
        if Instant::now() >= deadline {
            count_jq_timeout();
            return JqEval::Timeout;
        }
        if part.is_empty() {
            return JqEval::Empty;
        }
        match cur {
            Value::Object(m) => match m.get(part) {
                Some(v) => cur = v.clone(),
                None => return JqEval::Empty,
            },
            _ => return JqEval::Empty,
        }
    }
    JqEval::Values(vec![cur])
}

fn jq_timeout_arg(value: &Value) -> Option<u64> {
    match value {
        Value::Number(n) => n.as_i64().and_then(|v| u64::try_from(v).ok()),
        Value::String(s) => s.trim().parse::<u64>().ok(),
        _ => None,
    }
    .filter(|ms| (1..=MAX_JQ_CALL_TIMEOUT_MS).contains(ms))
}

fn func_jq(args: &[Value]) -> Value {
    if args.len() != 2 && args.len() != 3 {
        return Value::Null;
    }
    let (Some(prog), doc) = (str_arg(args, 0), &args[1]) else {
        return Value::Null;
    };
    // Unit is milliseconds. The per-call value wins; without it the
    // documented default applies. Out-of-range values fail closed.
    let budget_ms = if args.len() == 3 {
        let Some(ms) = jq_timeout_arg(&args[2]) else {
            return Value::Null;
        };
        ms
    } else {
        DEFAULT_JQ_TIMEOUT_MS
    };
    if prog.len() > MAX_STDLIB_STRING_BYTES {
        return Value::Null;
    }
    let doc_value = match doc {
        Value::String(s) => {
            if s.len() > MAX_STDLIB_STRING_BYTES {
                return Value::Null;
            }
            match serde_json::from_str::<Value>(s) {
                Ok(v) => v,
                Err(_) => return Value::Null,
            }
        }
        other => other.clone(),
    };
    // The subset evaluates inline on the rule thread with cooperative
    // stops: one deadline check per path segment plus the step budget.
    // A stop returns `Null`, never a partial row.
    let deadline = Instant::now() + Duration::from_millis(budget_ms);
    match jq_dot_path_budgeted(prog.as_str(), &doc_value, deadline) {
        JqEval::Values(items) => capped_array(items),
        JqEval::Empty => capped_array(Vec::new()),
        JqEval::Timeout => Value::Null,
    }
}

fn func_unescape(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Value::String(s) = &args[0] else {
        return Value::Null;
    };
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('\\') => out.push('\\'),
            Some('n') => out.push('\n'),
            Some('t') => out.push('\t'),
            Some('r') => out.push('\r'),
            Some('b') => out.push('\u{0008}'),
            Some('f') => out.push('\u{000C}'),
            Some('v') => out.push('\u{000B}'),
            Some('\'') => out.push('\''),
            Some('"') => out.push('"'),
            Some('?') => out.push('?'),
            Some('x') => {
                // Documented form is exactly `\xHH` (see FUNCTIONS.md):
                // consume two hex digits only, so trailing hex stays
                // literal (e.g. `\x4142` renders `A42`).
                let hi = chars.next();
                let lo = chars.next();
                match (hi, lo) {
                    (Some(h), Some(l)) if h.is_ascii_hexdigit() && l.is_ascii_hexdigit() => {
                        let hex = format!("{h}{l}");
                        match u32::from_str_radix(&hex, 16).ok().and_then(char::from_u32) {
                            Some(ch) => out.push(ch),
                            None => return Value::Null,
                        }
                    }
                    _ => return Value::Null,
                }
            }
            _ => return Value::Null,
        }
        if out.len() > MAX_STDLIB_STRING_BYTES {
            return Value::Null;
        }
    }
    capped_string(out)
}

fn func_rm_prefix(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let (Some(s), Some(prefix)) = (str_arg(args, 0), str_arg(args, 1)) else {
        return Value::Null;
    };
    match s.strip_prefix(prefix.as_str()) {
        Some(rest) => capped_string(rest.to_string()),
        None => capped_string(s),
    }
}

fn func_rtrim_chars(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let (Some(s), Some(chars)) = (str_arg(args, 0), str_arg(args, 1)) else {
        return Value::Null;
    };
    let strip: Vec<char> = chars.chars().collect();
    capped_string(s.trim_end_matches(strip.as_slice()).to_string())
}

// ---------------------------------------------------------------------------
// 8. Maps
// ---------------------------------------------------------------------------

fn func_map_new(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    Value::Object(serde_json::Map::new())
}

fn func_map_get(args: &[Value]) -> Value {
    if args.len() != 2 && args.len() != 3 {
        return Value::Null;
    }
    let Some(key) = args[0].as_str().map(str::to_string) else {
        return Value::Null;
    };
    let Some(obj) = object_arg(&args[1]) else {
        if args.len() == 3 {
            return args[2].clone();
        }
        return Value::Null;
    };
    let v = get_path(&obj, &key);
    if v.is_null() && args.len() == 3 {
        return args[2].clone();
    }
    v
}

fn func_map_put(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }
    let Some(key) = args[0].as_str().map(str::to_string) else {
        return Value::Null;
    };
    let Some(mut obj) = object_arg(&args[2]) else {
        return Value::Null;
    };
    // Bound: attacker-controlled maps on the publish path must not grow
    // without limit; fail the evaluation with `Null`, never the broker.
    // Reason: same 10 000-entry cap as MAX_STDLIB_ARRAY_ELEMS so rule
    // projections stay small. `>=` (not `>`) so a full map refuses one more
    // key instead of growing to cap + 1.
    if obj.len() >= MAX_STDLIB_ARRAY_ELEMS {
        return Value::Null;
    }
    put_path(&mut obj, &key, args[1].clone());
    // A dotted put can create nested objects; re-check the top level so the
    // insert itself cannot push the map past the cap.
    if obj.len() > MAX_STDLIB_ARRAY_ELEMS {
        return Value::Null;
    }
    Value::Object(obj)
}

fn func_map_keys(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(obj) = object_arg(&args[0]) else {
        return Value::Null;
    };
    // Bound: refuse oversized maps fail-closed before allocating the key
    // list (same 10 000-entry reason as above).
    if obj.len() > MAX_STDLIB_ARRAY_ELEMS {
        return Value::Null;
    }
    capped_array(obj.keys().map(|k| Value::String(k.clone())).collect())
}

fn func_map_values(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(obj) = object_arg(&args[0]) else {
        return Value::Null;
    };
    // Bound: same fail-closed input cap as `map_keys` above.
    if obj.len() > MAX_STDLIB_ARRAY_ELEMS {
        return Value::Null;
    }
    capped_array(obj.values().cloned().collect())
}

fn func_map_to_entries(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(obj) = object_arg(&args[0]) else {
        return Value::Null;
    };
    // Bound: same fail-closed input cap as `map_keys` above; oversized
    // maps return `Null` instead of allocating the entry list.
    if obj.len() > MAX_STDLIB_ARRAY_ELEMS {
        return Value::Null;
    }
    // Capacity hint only; the length check above is the bound. Reason: the
    // hint is clamped to the same cap so it never pre-allocates past it.
    let mut out = Vec::with_capacity(obj.len().min(MAX_STDLIB_ARRAY_ELEMS));
    for (k, v) in obj {
        let mut m = serde_json::Map::with_capacity(2);
        m.insert("key".to_string(), Value::String(k));
        m.insert("value".to_string(), v);
        out.push(Value::Object(m));
    }
    capped_array(out)
}

fn func_map_size(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(obj) = object_arg(&args[0]) else {
        return Value::Null;
    };
    Value::from(obj.len() as i64)
}

fn func_mget(args: &[Value]) -> Value {
    if args.len() != 2 && args.len() != 3 {
        return Value::Null;
    }
    let key = to_s(&args[0]);
    let Some(obj) = object_arg(&args[1]) else {
        if args.len() == 3 {
            return args[2].clone();
        }
        return Value::Null;
    };
    match obj.get(&key) {
        Some(v) => v.clone(),
        None => {
            if args.len() == 3 {
                args[2].clone()
            } else {
                Value::Null
            }
        }
    }
}

fn func_mput(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }
    let key = to_s(&args[0]);
    let Some(mut obj) = object_arg(&args[2]) else {
        return Value::Null;
    };
    // Bound: refuse a new key past the 10 000-entry cap fail-closed
    // (same reason as `map_put` above); updates to existing keys stay.
    if !obj.contains_key(&key) && obj.len() >= MAX_STDLIB_ARRAY_ELEMS {
        return Value::Null;
    }
    obj.insert(key, args[1].clone());
    Value::Object(obj)
}

// ---------------------------------------------------------------------------
// 9. Arrays
// ---------------------------------------------------------------------------

fn func_nth(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let Some(n) = as_i64(&args[0]).or_else(|| match &args[0] {
        Value::String(s) => s.trim().parse::<i64>().ok(),
        _ => None,
    }) else {
        return Value::Null;
    };
    let Value::Array(items) = &args[1] else {
        return Value::Null;
    };
    if n < 1 || n as usize > items.len() {
        // Documented behaviour: out-of-range yields empty binary ("").
        return Value::String(String::new());
    }
    items[(n - 1) as usize].clone()
}

fn func_length(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        // Same name as the engine string length; array form counts
        // elements (see FUNCTIONS.md §11 for the dispatch note).
        Value::Array(items) => Value::from(items.len() as i64),
        Value::String(s) => Value::from(s.chars().count() as i64),
        _ => Value::Null,
    }
}

fn func_sublist(args: &[Value]) -> Value {
    match args.len() {
        2 => {
            let (Some(n), Value::Array(items)) = (as_i64(&args[0]), &args[1]) else {
                return Value::Null;
            };
            if n < 0 {
                return Value::Null;
            }
            capped_array(items.iter().take(n as usize).cloned().collect())
        }
        3 => {
            let (Some(start), Some(len)) = (as_i64(&args[0]), as_i64(&args[1])) else {
                return Value::Null;
            };
            let Value::Array(items) = &args[2] else {
                return Value::Null;
            };
            if start < 1 || len < 0 {
                return Value::Null;
            }
            let from = ((start - 1) as usize).min(items.len());
            let to = (from + len as usize).min(items.len());
            capped_array(items[from..to].to_vec())
        }
        _ => Value::Null,
    }
}

fn func_first(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        Value::Array(items) => items.first().cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn func_last(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        Value::Array(items) => items.last().cloned().unwrap_or(Value::Null),
        _ => Value::Null,
    }
}

fn func_contains(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    // Standard order is (element, list); the engine built-in takes the
    // reverse order under another name.
    let Value::Array(items) = &args[1] else {
        return Value::Null;
    };
    Value::Bool(items.iter().any(|v| v == &args[0]))
}

// ---------------------------------------------------------------------------
// 10a. Hash
// ---------------------------------------------------------------------------

fn digest_hex(algo: &str, data: &[u8]) -> Option<String> {
    use sha2::Digest as _;
    match algo {
        "md5" => Some(format!("{:x}", md5::Md5::digest(data))),
        "sha" | "sha1" => Some(format!("{:x}", sha1::Sha1::digest(data))),
        "sha224" => Some(format!("{:x}", sha2::Sha224::digest(data))),
        "sha256" => Some(format!("{:x}", sha2::Sha256::digest(data))),
        "sha384" => Some(format!("{:x}", sha2::Sha384::digest(data))),
        "sha512" => Some(format!("{:x}", sha2::Sha512::digest(data))),
        _ => None,
    }
}

fn func_sha(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(s) = str_arg(args, 0) else {
        return Value::Null;
    };
    // Allocates one hex String, like the engine digest helpers.
    match digest_hex("sha1", s.as_bytes()) {
        Some(hex) => capped_string(hex),
        None => Value::Null,
    }
}

fn func_hash(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let (Some(algo), Some(data)) = (str_arg(args, 0), str_arg(args, 1)) else {
        return Value::Null;
    };
    let key = algo.trim().to_ascii_lowercase();
    match digest_hex(key.as_str(), data.as_bytes()) {
        Some(hex) => capped_string(hex),
        // Decided (D4): the sha3 and blake2 families have no digest crate
        // wired; static literals are refused at rule creation and dynamic
        // values fail closed here.
        None => Value::Null,
    }
}

/// True when `hash()` names a refused D4 algorithm (the sha3 and blake2
/// families). The check ignores case and surrounding space.
pub(crate) fn refused_hash_algorithm(algo: &str) -> bool {
    let lower = algo.trim().to_ascii_lowercase();
    lower.starts_with("sha3") || lower.starts_with("blake2")
}

fn func_hash_to_range(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }
    // Fail closed on wrong types: the hashed key must be a string, so
    // `as_str` (not the coercing `str_arg`) is required.
    let (Some(input), Some(min), Some(max)) = (
        args[0].as_str().map(str::to_string),
        as_i64(&args[1]),
        as_i64(&args[2]),
    ) else {
        return Value::Null;
    };
    if input.is_empty() || min > max {
        return Value::Null;
    }
    let Some(hex) = digest_hex("sha256", input.as_bytes()) else {
        return Value::Null;
    };
    let span = (max as i128 - min as i128 + 1) as u128;
    if span == 0 {
        return Value::Null;
    }
    // SHA-256 hex is 64 chars (256 bits) and never fits in u128, so reduce
    // digit-by-digit over the full hex: the remainder stays below `span`
    // (at most 2^64) and never overflows u128.
    let mut rem: u128 = 0;
    for byte in hex.bytes() {
        let digit = (byte as char).to_digit(16).unwrap_or(0) as u128;
        rem = (rem * 16 + digit) % span;
    }
    // No allocation on this path.
    match i64::try_from(min as i128 + rem as i128) {
        Ok(v) => Value::from(v),
        Err(_) => Value::Null,
    }
}

fn func_map_to_range(args: &[Value]) -> Value {
    if args.len() != 3 {
        return Value::Null;
    }
    let (Some(min), Some(max)) = (as_i64(&args[1]), as_i64(&args[2])) else {
        return Value::Null;
    };
    if min > max {
        return Value::Null;
    };
    let num: Option<u128> = match &args[0] {
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Some(i as u128)
            } else {
                n.as_u64().map(|u| u as u128)
            }
        }
        Value::String(s) => {
            if s.is_empty() {
                return Value::Null;
            }
            // Binary bytes decode as unsigned big-endian.
            let bytes = s.as_bytes();
            let mut acc: u128 = 0;
            for b in bytes.iter().take(16) {
                acc = (acc << 8) | u128::from(*b);
            }
            Some(acc)
        }
        _ => None,
    };
    let Some(num) = num else {
        return Value::Null;
    };
    let span = (max as i128 - min as i128 + 1) as u128;
    if span == 0 {
        return Value::Null;
    }
    match i64::try_from(min as i128 + (num % span) as i128) {
        Ok(v) => Value::from(v),
        Err(_) => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// 10b. Compression (base64 wire form; see FUNCTIONS.md)
// ---------------------------------------------------------------------------

fn bytes_arg(args: &[Value], i: usize) -> Option<Vec<u8>> {
    match args.get(i) {
        Some(Value::String(s)) => Some(s.as_bytes().to_vec()),
        // Fail closed on Null/missing.
        Some(Value::Null) | None => None,
        Some(other) => Some(to_s(other).into_bytes()),
    }
}

fn b64_of(bytes: &[u8]) -> Value {
    use base64::Engine as _;
    if bytes.len() * 4 / 3 + 4 > MAX_STDLIB_STRING_BYTES {
        return Value::Null;
    }
    // Must allocate the base64 text (JSON strings are UTF-8; raw deflate
    // bytes are not).
    capped_string(base64::engine::general_purpose::STANDARD.encode(bytes))
}

fn b64_to_bytes(s: &str) -> Option<Vec<u8>> {
    use base64::Engine as _;
    let trimmed = s.trim();
    // Accept the base64 wire form first, then raw bytes.
    base64::engine::general_purpose::STANDARD
        .decode(trimmed)
        .or_else(|_| {
            base64::engine::general_purpose::URL_SAFE
                .decode(trimmed)
                .or_else(|_| {
                    base64::engine::general_purpose::STANDARD_NO_PAD
                        .decode(trimmed)
                        .or_else(|_| {
                            base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(trimmed)
                        })
                })
        })
        .ok()
}

fn func_zip(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(raw) = bytes_arg(args, 0) else {
        return Value::Null;
    };
    if raw.len() > MAX_STDLIB_STRING_BYTES {
        return Value::Null;
    }
    let mut enc = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    if enc.write_all(&raw).is_err() {
        return Value::Null;
    }
    match enc.finish() {
        Ok(out) => b64_of(&out),
        Err(_) => Value::Null,
    }
}

fn zlib_decode_input(args: &[Value]) -> Option<Vec<u8>> {
    if args.len() != 1 {
        return None;
    }
    match &args[0] {
        Value::String(s) => b64_to_bytes(s).or_else(|| Some(s.as_bytes().to_vec())),
        _ => bytes_arg(args, 0),
    }
}

fn func_unzip(args: &[Value]) -> Value {
    let Some(data) = zlib_decode_input(args) else {
        return Value::Null;
    };
    // Reason for `* 4`: the wire form is base64 (~4/3 expansion) of
    // deflated bytes, so a 1 MiB output can arrive as up to ~1.4 MiB of
    // text; 4x keeps the pre-decompression buffer bounded while the
    // expanded output stays capped at 1 MiB below.
    if data.len() > MAX_STDLIB_STRING_BYTES * 4 {
        return Value::Null;
    }
    let dec = flate2::read::ZlibDecoder::new(data.as_slice());
    let mut out = Vec::new();
    // Bounded: refuse outputs past the string cap instead of growing.
    let mut limited = dec.take((MAX_STDLIB_STRING_BYTES + 1) as u64);
    if limited.read_to_end(&mut out).is_err() {
        return Value::Null;
    }
    if out.len() > MAX_STDLIB_STRING_BYTES {
        return Value::Null;
    }
    match String::from_utf8(out) {
        Ok(s) => capped_string(s),
        Err(_) => Value::Null,
    }
}

fn func_zip_compress(args: &[Value]) -> Value {
    func_zip(args)
}

fn func_zip_uncompress(args: &[Value]) -> Value {
    func_unzip(args)
}

fn func_gzip(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(raw) = bytes_arg(args, 0) else {
        return Value::Null;
    };
    if raw.len() > MAX_STDLIB_STRING_BYTES {
        return Value::Null;
    }
    let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    if enc.write_all(&raw).is_err() {
        return Value::Null;
    }
    match enc.finish() {
        Ok(out) => b64_of(&out),
        Err(_) => Value::Null,
    }
}

fn func_gunzip(args: &[Value]) -> Value {
    let Some(data) = zlib_decode_input(args) else {
        return Value::Null;
    };
    // Reason for `* 4`: same wire-form bound as `func_unzip` (base64
    // ~4/3 plus gzip framing); the expanded output stays capped at 1 MiB.
    if data.len() > MAX_STDLIB_STRING_BYTES * 4 {
        return Value::Null;
    }
    let dec = flate2::read::GzDecoder::new(data.as_slice());
    let mut out = Vec::new();
    let mut limited = dec.take((MAX_STDLIB_STRING_BYTES + 1) as u64);
    if limited.read_to_end(&mut out).is_err() {
        return Value::Null;
    }
    if out.len() > MAX_STDLIB_STRING_BYTES {
        return Value::Null;
    }
    match String::from_utf8(out) {
        Ok(s) => capped_string(s),
        Err(_) => Value::Null,
    }
}

fn func_lz4_compress(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(raw) = bytes_arg(args, 0) else {
        return Value::Null;
    };
    if raw.len() > MAX_STDLIB_STRING_BYTES {
        return Value::Null;
    }
    // Must allocate the frame buffer; input is capped above.
    let mut enc = lz4_flex::frame::FrameEncoder::new(Vec::new());
    if enc.write_all(&raw).is_err() {
        return Value::Null;
    }
    match enc.finish() {
        Ok(out) => b64_of(&out),
        Err(_) => Value::Null,
    }
}

fn func_lz4_uncompress(args: &[Value]) -> Value {
    let Some(data) = zlib_decode_input(args) else {
        return Value::Null;
    };
    // Reason for `* 4`: same wire-form bound as `func_unzip` (base64
    // ~4/3 plus LZ4 frame overhead); the expanded output stays capped.
    if data.is_empty() || data.len() > MAX_STDLIB_STRING_BYTES * 4 {
        return Value::Null;
    }
    let dec = lz4_flex::frame::FrameDecoder::new(data.as_slice());
    let mut out = Vec::new();
    let mut limited = dec.take((MAX_STDLIB_STRING_BYTES + 1) as u64);
    if limited.read_to_end(&mut out).is_err() {
        return Value::Null;
    }
    if out.len() > MAX_STDLIB_STRING_BYTES {
        return Value::Null;
    }
    match String::from_utf8(out) {
        Ok(s) => capped_string(s),
        Err(_) => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// 10c. Encode / decode
// ---------------------------------------------------------------------------

fn b64_options(args: &[Value], from: usize) -> Option<base64::engine::GeneralPurpose> {
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD, URL_SAFE, URL_SAFE_NO_PAD};
    let mut urlsafe = false;
    let mut no_pad = false;
    for arg in args.iter().skip(from) {
        let opt = arg.as_str().map(|s| s.trim().to_ascii_lowercase())?;
        match opt.as_str() {
            "urlsafe" => urlsafe = true,
            "no_padding" => no_pad = true,
            _ => return None,
        }
    }
    Some(match (urlsafe, no_pad) {
        (false, false) => STANDARD,
        (false, true) => STANDARD_NO_PAD,
        (true, false) => URL_SAFE,
        (true, true) => URL_SAFE_NO_PAD,
    })
}

fn func_base64_encode_full(args: &[Value]) -> Value {
    if args.is_empty() || args.len() > 3 {
        return Value::Null;
    }
    if args[0].is_null() {
        return Value::Null;
    }
    let engine = if args.len() == 1 {
        use base64::engine::general_purpose::STANDARD;
        STANDARD
    } else {
        let Some(e) = b64_options(args, 1) else {
            return Value::Null;
        };
        e
    };
    use base64::Engine as _;
    // Allocates one String for the encoded text, like the engine helper.
    capped_string(engine.encode(to_s(&args[0]).as_bytes()))
}

fn func_base64_decode_full(args: &[Value]) -> Value {
    if args.is_empty() || args.len() > 3 {
        return Value::Null;
    }
    let Some(text) = args[0].as_str().map(str::to_string) else {
        return Value::Null;
    };
    let engine = if args.len() == 1 {
        use base64::engine::general_purpose::STANDARD;
        STANDARD
    } else {
        let Some(e) = b64_options(args, 1) else {
            return Value::Null;
        };
        e
    };
    use base64::Engine as _;
    match engine.decode(text.trim()) {
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(s) => capped_string(s),
            Err(_) => Value::Null,
        },
        Err(_) => Value::Null,
    }
}

fn func_json_encode(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    // Allocates one String for the JSON text, like the engine serializer.
    capped_string(serde_json::to_string(&args[0]).unwrap_or_default())
}

fn func_json_decode(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Value::String(s) = &args[0] else {
        return Value::Null;
    };
    serde_json::from_str(s).unwrap_or(Value::Null)
}

fn func_term_encode(args: &[Value]) -> Value {
    // The Erlang external-term framing has no JSON equivalent; the JSON
    // bytes are the portable substitute (see FUNCTIONS.md). TODO(parity):
    // cross-runtime term compatibility.
    func_json_encode(args)
}

fn func_term_decode(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    match &args[0] {
        Value::String(_) => func_json_decode(args),
        _ => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// 10d. Dates, UUID/system
// ---------------------------------------------------------------------------

fn parse_unit(s: &str) -> Option<&str> {
    match s.trim().to_ascii_lowercase().as_str() {
        "second" | "seconds" | "sec" | "s" => Some("second"),
        "millisecond" | "milliseconds" | "ms" => Some("millisecond"),
        "microsecond" | "microseconds" | "us" => Some("microsecond"),
        "nanosecond" | "nanoseconds" | "ns" => Some("nanosecond"),
        "minute" | "minutes" => Some("minute"),
        "hour" | "hours" => Some("hour"),
        "day" | "days" => Some("day"),
        _ => None,
    }
}

fn now_parts() -> (i64, u32) {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| {
            let secs = d.as_secs();
            (i64::try_from(secs).unwrap_or(i64::MAX), d.subsec_nanos())
        })
        .unwrap_or((0, 0))
}

fn epoch_to_rfc3339(value: i64, unit: &str) -> Option<String> {
    let (secs, nanos) = match unit {
        "second" => (value, 0u32),
        "millisecond" => (
            value.div_euclid(1000),
            (value.rem_euclid(1000) * 1_000_000) as u32,
        ),
        "microsecond" => (
            value.div_euclid(1_000_000),
            (value.rem_euclid(1_000_000) * 1_000) as u32,
        ),
        "nanosecond" => (
            value.div_euclid(1_000_000_000),
            value.rem_euclid(1_000_000_000) as u32,
        ),
        _ => return None,
    };
    let dt = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, nanos)?;
    Some(dt.to_rfc3339_opts(chrono::SecondsFormat::AutoSi, true))
}

fn rfc3339_to_nanos(s: &str) -> Option<i64> {
    let dt = chrono::DateTime::parse_from_rfc3339(s.trim())
        .ok()
        .map(|d| d.with_timezone(&chrono::Utc))?;
    dt.timestamp_nanos_opt()
}

fn unit_from_secs(secs: i64, subsec_nanos: u32, unit: &str) -> Option<i64> {
    match unit {
        "second" => Some(secs),
        "millisecond" => secs
            .checked_mul(1000)?
            .checked_add(i64::from(subsec_nanos / 1_000_000)),
        "microsecond" => secs
            .checked_mul(1_000_000)?
            .checked_add(i64::from(subsec_nanos / 1_000)),
        "nanosecond" => secs
            .checked_mul(1_000_000_000)?
            .checked_add(i64::from(subsec_nanos)),
        _ => None,
    }
}

fn nanos_to_unit(nanos: i64, unit: &str) -> Option<i64> {
    match unit {
        "second" => Some(nanos.div_euclid(1_000_000_000)),
        "millisecond" => Some(nanos.div_euclid(1_000_000)),
        "microsecond" => Some(nanos.div_euclid(1_000)),
        "nanosecond" => Some(nanos),
        _ => None,
    }
}

fn func_now_rfc3339(args: &[Value]) -> Value {
    if args.len() > 1 {
        return Value::Null;
    }
    let precision = if args.len() == 1 {
        let Some(u) = args[0].as_str().and_then(parse_unit) else {
            return Value::Null;
        };
        match u {
            "second" | "millisecond" | "microsecond" | "nanosecond" => u,
            _ => return Value::Null,
        }
    } else {
        "second"
    };
    let (secs, subsec) = now_parts();
    let value = match precision {
        "second" => secs,
        "millisecond" => unit_from_secs(secs, subsec, "millisecond").unwrap_or(0),
        "microsecond" => unit_from_secs(secs, subsec, "microsecond").unwrap_or(0),
        "nanosecond" => unit_from_secs(secs, subsec, "nanosecond").unwrap_or(0),
        _ => return Value::Null,
    };
    match epoch_to_rfc3339(value, precision) {
        // Allocates one String for the timestamp, like date formatters.
        Some(s) => capped_string(s),
        None => Value::Null,
    }
}

fn func_unix_ts_to_rfc3339(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::Null;
    }
    let Some(epoch) = as_i64(&args[0]) else {
        return Value::Null;
    };
    let unit = if args.len() == 2 {
        let Some(u) = args[1].as_str().and_then(parse_unit) else {
            return Value::Null;
        };
        match u {
            "second" | "millisecond" | "microsecond" | "nanosecond" => u,
            _ => return Value::Null,
        }
    } else {
        "second"
    };
    match epoch_to_rfc3339(epoch, unit) {
        Some(s) => capped_string(s),
        None => Value::Null,
    }
}

fn func_rfc3339_to_unix_ts(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::Null;
    }
    let Some(s) = args[0].as_str().map(str::to_string) else {
        return Value::Null;
    };
    let unit = if args.len() == 2 {
        let Some(u) = args[1].as_str().and_then(parse_unit) else {
            return Value::Null;
        };
        match u {
            "second" | "millisecond" | "microsecond" | "nanosecond" => u,
            _ => return Value::Null,
        }
    } else {
        "second"
    };
    let Some(nanos) = rfc3339_to_nanos(&s) else {
        return Value::Null;
    };
    match nanos_to_unit(nanos, unit) {
        // No allocation on this path.
        Some(v) => Value::from(v),
        None => Value::Null,
    }
}

fn func_now_timestamp(args: &[Value]) -> Value {
    if args.len() > 1 {
        return Value::Null;
    }
    let unit = if args.len() == 1 {
        let Some(u) = args[0].as_str().and_then(parse_unit) else {
            return Value::Null;
        };
        match u {
            "second" | "millisecond" | "microsecond" | "nanosecond" => u,
            _ => return Value::Null,
        }
    } else {
        "second"
    };
    let (secs, subsec) = now_parts();
    match unit_from_secs(secs, subsec, unit) {
        Some(v) => Value::from(v),
        None => Value::Null,
    }
}

fn format_tokens(fmt: &str, dt: &chrono::DateTime<chrono::Utc>) -> String {
    // TODO(parity): full token table; this covers the documented
    // yyyy/MM/dd HH:mm:ss and fractional-second tokens.
    use chrono::Datelike as _;
    use chrono::Timelike as _;
    let mut out = String::with_capacity(fmt.len() + 16);
    let mut chars = fmt.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\'' {
            // Quoted literal.
            for q in chars.by_ref() {
                if q == '\'' {
                    break;
                }
                out.push(q);
            }
            continue;
        }
        let mut run = 1usize;
        while chars.peek() == Some(&c) {
            chars.next();
            run += 1;
        }
        match c {
            'y' => {
                if run >= 4 {
                    out.push_str(&format!("{:04}", dt.year()));
                } else {
                    out.push_str(&format!("{:02}", dt.year() % 100));
                }
            }
            'M' => {
                if run >= 3 {
                    out.push_str(dt.format("%b").to_string().as_str());
                } else {
                    out.push_str(&format!("{:02}", dt.month()));
                }
            }
            'd' => out.push_str(&format!("{:02}", dt.day())),
            'H' => out.push_str(&format!("{:02}", dt.hour())),
            'm' => out.push_str(&format!("{:02}", dt.minute())),
            's' => out.push_str(&format!("{:02}", dt.second())),
            'S' => {
                let ms = dt.timestamp_subsec_millis();
                if run >= 3 {
                    out.push_str(&format!("{ms:03}"));
                } else {
                    out.push_str(&format!("{ms}"));
                }
            }
            _ => {
                for _ in 0..run {
                    out.push(c);
                }
            }
        }
    }
    out
}

fn parse_formatted(fmt: &str, input: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    // Minimal inverse of `format_tokens` for numeric `yyyy-MM-dd
    // HH:mm:ss`-shaped patterns. Anything else fails closed (Null).
    // TODO(parity): full token table.
    let digits: Vec<u32> = input
        .split(|c: char| !c.is_ascii_digit())
        .filter(|p| !p.is_empty())
        .filter_map(|p| p.parse::<u32>().ok())
        .collect();
    if digits.len() < 3 {
        return None;
    }
    let has_time = fmt.contains('H') || fmt.contains('m') || fmt.contains('s');
    let (y, mo, d) = (digits[0] as i32, digits[1], digits[2]);
    let (h, mi, s) = if has_time && digits.len() >= 6 {
        (digits[3], digits[4], digits[5])
    } else {
        (0, 0, 0)
    };
    chrono::NaiveDate::from_ymd_opt(y, mo, d)
        .and_then(|date| date.and_hms_opt(h, mi, s))
        .map(|naive| chrono::DateTime::<chrono::Utc>::from_naive_utc_and_offset(naive, chrono::Utc))
}

fn func_format_date(args: &[Value]) -> Value {
    if args.len() != 3 && args.len() != 4 {
        return Value::Null;
    }
    let (Some(unit_s), Some(offset), Some(fmt)) = (
        args[0].as_str().map(str::to_string),
        as_i64(&args[1]),
        args[2].as_str().map(str::to_string),
    ) else {
        return Value::Null;
    };
    let Some(unit) = parse_unit(&unit_s) else {
        return Value::Null;
    };
    let epoch_val = if args.len() == 4 {
        let Some(e) = as_i64(&args[3]) else {
            return Value::Null;
        };
        e
    } else {
        let (secs, subsec) = now_parts();
        unit_from_secs(secs, subsec, unit).unwrap_or(0)
    };
    let base_nanos: i128 = match unit {
        "second" => (epoch_val as i128) * 1_000_000_000,
        "millisecond" => (epoch_val as i128) * 1_000_000,
        "microsecond" => (epoch_val as i128) * 1_000,
        "nanosecond" => epoch_val as i128,
        _ => return Value::Null,
    };
    let nanos = base_nanos + (offset as i128) * 1_000_000_000;
    let (secs, subsec) = (
        i64::try_from(nanos.div_euclid(1_000_000_000)).unwrap_or(0),
        nanos.rem_euclid(1_000_000_000) as u32,
    );
    let Some(dt) = chrono::DateTime::<chrono::Utc>::from_timestamp(secs, subsec) else {
        return Value::Null;
    };
    capped_string(format_tokens(&fmt, &dt))
}

fn func_date_to_unix_ts(args: &[Value]) -> Value {
    if args.len() != 3 && args.len() != 4 {
        return Value::Null;
    }
    let Some(unit_s) = args[0].as_str().map(str::to_string) else {
        return Value::Null;
    };
    let Some(unit) = parse_unit(&unit_s) else {
        return Value::Null;
    };
    // 3-arg form is (unit, fmt, input); 4-arg form is (unit, offset, fmt,
    // input) where the offset is either a zone string ("Z", "+08:00") or a
    // plain seconds number.
    let (fmt, input, offset_secs) = if args.len() == 3 {
        let (Some(f), Some(i)) = (
            args[1].as_str().map(str::to_string),
            args[2].as_str().map(str::to_string),
        ) else {
            return Value::Null;
        };
        (f, i, 0i64)
    } else {
        let (Some(f), Some(i)) = (
            args[2].as_str().map(str::to_string),
            args[3].as_str().map(str::to_string),
        ) else {
            return Value::Null;
        };
        let off = match &args[1] {
            Value::String(s) => match timezone_offset_secs(s) {
                Some(off) => off,
                // Decided (D4): named zones fail closed here; static
                // literals are refused at rule creation.
                None => return Value::Null,
            },
            other => match as_i64(other) {
                Some(off) => off,
                None => return Value::Null,
            },
        };
        (f, i, off)
    };
    let Some(dt) = parse_formatted(&fmt, &input) else {
        return Value::Null;
    };
    let base = dt.timestamp();
    let base_nanos = (base as i128) * 1_000_000_000 + i128::from(dt.timestamp_subsec_nanos());
    let nanos = base_nanos - (offset_secs as i128) * 1_000_000_000;
    let value = match unit {
        "second" => nanos.div_euclid(1_000_000_000),
        "millisecond" => nanos.div_euclid(1_000_000),
        "microsecond" => nanos.div_euclid(1_000),
        "nanosecond" => nanos,
        _ => return Value::Null,
    };
    match i64::try_from(value) {
        Ok(v) => Value::from(v),
        Err(_) => Value::Null,
    }
}

/// Parse `Z`/`UTC`/`+HH:MM` offsets to seconds. Named zones are unsupported
/// and yield `None` (fail closed).
fn timezone_offset_secs(s: &str) -> Option<i64> {
    let t = s.trim();
    if t == "Z" || t.eq_ignore_ascii_case("utc") || t == "+00:00" || t == "00:00" {
        return Some(0);
    }
    let (sign, rest) = match t.strip_prefix('+') {
        Some(r) => (1i64, r),
        None => (-1i64, t.strip_prefix('-')?),
    };
    let parts: Vec<&str> = rest.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    match (parts[0].parse::<i64>(), parts[1].parse::<i64>()) {
        (Ok(h), Ok(m)) if h <= 23 && m <= 59 => Some(sign * (h * 3600 + m * 60)),
        _ => None,
    }
}

fn func_timezone_to_offset(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(tz) = args[0].as_str().map(str::to_string) else {
        return Value::Null;
    };
    // Decided (D4): the named-zone database is unsupported; named zones
    // fail closed. Static literals are refused at rule creation.
    match timezone_offset_secs(&tz) {
        Some(off) => Value::from(off),
        None => Value::Null,
    }
}

/// True when a zone value needs the named-zone database (unsupported here).
/// `Z`, `UTC` and fixed offsets return false; every other value is named.
pub(crate) fn is_named_zone(zone: &str) -> bool {
    timezone_offset_secs(zone).is_none()
}

fn func_mongo_date(args: &[Value]) -> Value {
    if args.len() > 2 {
        return Value::Null;
    }
    let (epoch, unit) = if args.is_empty() {
        let (secs, subsec) = now_parts();
        (
            unit_from_secs(secs, subsec, "millisecond").unwrap_or(0),
            "millisecond",
        )
    } else {
        let Some(e) = as_i64(&args[0]) else {
            return Value::Null;
        };
        if args.len() == 2 {
            let Some(u) = args[1].as_str().and_then(parse_unit) else {
                return Value::Null;
            };
            match u {
                "second" | "millisecond" | "microsecond" | "nanosecond" => (e, u),
                _ => return Value::Null,
            }
        } else {
            (e, "millisecond")
        }
    };
    // The tuple-to-BSON conversion lives in the connector; rules observe an
    // RFC-3339 string. TODO(parity): BSON date typing.
    match epoch_to_rfc3339(epoch, unit) {
        Some(s) => capped_string(s),
        None => Value::Null,
    }
}

fn func_getenv(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let Some(name) = args[0].as_str().map(str::to_string) else {
        return Value::Null;
    };
    // Reads the process environment directly with no cache. Absent means "".
    // Must allocate the result String. Reason: names are capped at 4 KiB
    // (POSIX minimum `ARG_MAX` neighbourhood; env names are short) so a
    // probe cannot carry an unbounded key on the publish path.
    if name.len() > 4096 {
        return Value::Null;
    }
    capped_string(std::env::var(&name).unwrap_or_default())
}

fn func_random(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    // No allocation on this path.
    serde_json::json!(rand::random::<f64>())
}

fn func_uuid_v4(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    // Allocates one hyphenated String, like the engine `uuid`.
    capped_string(uuid::Uuid::new_v4().to_string())
}

fn func_uuid_v4_no_hyphen(args: &[Value]) -> Value {
    if !args.is_empty() {
        return Value::Null;
    }
    capped_string(uuid::Uuid::new_v4().to_string().replace('-', ""))
}

// ---------------------------------------------------------------------------
// 10e. proc_dict / kv_store
// ---------------------------------------------------------------------------

fn func_proc_dict_get(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let key = to_s(&args[0]);
    proc_get(&key).unwrap_or(Value::Null)
}

fn func_proc_dict_put(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let key = to_s(&args[0]);
    // Reason: keys are attacker-controlled on the publish path; cap key
    // length at 4 KiB (same 4 KiB reason as getenv's name cap above) so a
    // single put cannot force an unbounded allocation.
    if key.len() > 4096 {
        return Value::Null;
    }
    match proc_put(key, args[1].clone()) {
        Some(v) => v,
        None => Value::Null,
    }
}

fn func_proc_dict_del(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let key = to_s(&args[0]);
    proc_del(&key)
}

fn func_kv_store_get(args: &[Value]) -> Value {
    if args.len() != 1 && args.len() != 2 {
        return Value::Null;
    }
    let key = to_s(&args[0]);
    match state_get(&KV_STORE, &key) {
        Some(v) => v,
        None => {
            if args.len() == 2 {
                args[1].clone()
            } else {
                Value::Null
            }
        }
    }
}

fn func_kv_store_put(args: &[Value]) -> Value {
    if args.len() != 2 {
        return Value::Null;
    }
    let key = to_s(&args[0]);
    // Reason: same 4 KiB attacker-key cap as proc_dict_put/getenv above, so
    // a single put cannot force an unbounded allocation on the publish path.
    if key.len() > 4096 {
        return Value::Null;
    }
    match state_put(&KV_STORE, key, args[1].clone()) {
        Some(v) => v,
        None => Value::Null,
    }
}

fn func_kv_store_del(args: &[Value]) -> Value {
    if args.len() != 1 {
        return Value::Null;
    }
    let key = to_s(&args[0]);
    state_del(&KV_STORE, &key)
}

fn func_rtrim_full(args: &[Value]) -> Value {
    match args.len() {
        1 => match &args[0] {
            Value::String(s) => capped_string(s.trim_end().to_string()),
            Value::Null => Value::Null,
            _ => Value::Null,
        },
        2 => func_rtrim_chars(args),
        _ => Value::Null,
    }
}

// ---------------------------------------------------------------------------
// Registration
// ---------------------------------------------------------------------------

static REGISTERED: OnceLock<()> = OnceLock::new();

/// Register every standard-dialect UDF exactly once. Idempotent and safe to
/// call from any constructor or evaluation entry point; after the first call
/// the cost is one atomic load (no mutex) so the publish path pays no new
/// lock.
pub fn ensure_stdlib_registered() {
    REGISTERED.get_or_init(|| {
        let registry = get_global_udf_registry();
        let add = |name: &'static str, f: fn(&[Value]) -> Value| {
            let handler: UdfFn = Arc::new(move |args: &[Value]| f(args));
            registry.register_udf(name, handler);
        };
        // Context accessors.
        add("msgid", func_msgid);
        add("qos", func_qos);
        add("flags", func_flags);
        add("flag", func_flag);
        add("topic", func_topic);
        add("clientid", func_clientid);
        add("clientip", func_peerhost);
        add("peerhost", func_peerhost);
        add("username", func_username);
        add("payload", func_payload);
        add("contains_topic", func_contains_topic);
        add("contains_topic_match", func_contains_topic_match);
        add("topic_match", func_topic_match);
        add("null", func_null);
        add("coalesce_ne", func_coalesce_ne);
        add("div", func_div);
        add("fmod", func_fmod);
        // Math additions.
        add("acosh", func_acosh);
        add("asinh", func_asinh);
        add("atanh", func_atanh);
        // Bitwise additions.
        add("bitsl", func_bitsl);
        add("bitsr", func_bitsr);
        // Bit and byte.
        add("bitsize", func_bitsize);
        add("bytesize", func_bytesize);
        add("subbits", func_subbits);
        // Conversion.
        add("str", func_str);
        add("str_utf8", func_str_utf8);
        add("str_utf16_le", func_str_utf16_le);
        add("bool", func_bool);
        add("int", func_int);
        add("float", func_float);
        add("float2str", func_float2str);
        add("map", func_map);
        add("bin2hexstr", func_bin2hexstr);
        add("hexstr2bin", func_hexstr2bin);
        add("sqlserver_bin2hexstr", func_sqlserver_bin2hexstr);
        // Checks.
        add("is_null", func_is_null);
        add("is_null_var", func_is_null_var);
        add("is_not_null", func_is_not_null);
        add("is_not_null_var", func_is_not_null_var);
        add("is_str", func_is_str);
        add("is_bool", func_is_bool);
        add("is_int", func_is_int);
        add("is_float", func_is_float);
        add("is_num", func_is_num);
        add("is_map", func_is_map);
        add("is_array", func_is_array);
        add("is_empty", func_is_empty);
        // Strings (including colliding adapters; see module docs).
        add("strlen", func_strlen);
        add("substr", func_substr);
        add("split", func_split);
        add("concat", func_concat);
        add("tokens", func_tokens);
        add("sprintf_s", func_sprintf);
        add("pad", func_pad);
        add("replace", func_replace);
        add("regex_match", func_regex_match);
        add("regex_replace", func_regex_replace);
        add("regex_extract", func_regex_extract);
        add("ascii", func_ascii);
        add("find", func_find);
        add("join_to_string", func_join_to_string);
        add("map_to_redis_hset_args", func_map_to_redis_hset_args);
        add("join_to_sql_values_string", func_join_to_sql_values_string);
        add("jq", func_jq);
        add("unescape", func_unescape);
        add("rm_prefix", func_rm_prefix);
        add("rtrim", func_rtrim_full);
        // Maps.
        add("map_new", func_map_new);
        add("map_get", func_map_get);
        add("map_put", func_map_put);
        add("map_keys", func_map_keys);
        add("map_values", func_map_values);
        add("map_to_entries", func_map_to_entries);
        add("map_size", func_map_size);
        add("mget", func_mget);
        add("mput", func_mput);
        // Arrays.
        add("nth", func_nth);
        add("length", func_length);
        add("sublist", func_sublist);
        add("first", func_first);
        add("last", func_last);
        add("contains", func_contains);
        // Hash.
        add("sha", func_sha);
        add("hash", func_hash);
        add("hash_to_range", func_hash_to_range);
        add("map_to_range", func_map_to_range);
        // Compression.
        add("zip", func_zip);
        add("unzip", func_unzip);
        add("gzip", func_gzip);
        add("gunzip", func_gunzip);
        add("zip_compress", func_zip_compress);
        add("zip_uncompress", func_zip_uncompress);
        add("lz4_compress", func_lz4_compress);
        add("lz4_uncompress", func_lz4_uncompress);
        // Encode/decode (base64 full-arity handlers included; see §11).
        add("base64_encode", func_base64_encode_full);
        add("base64_decode", func_base64_decode_full);
        add("json_encode", func_json_encode);
        add("json_decode", func_json_decode);
        add("term_encode", func_term_encode);
        add("term_decode", func_term_decode);
        // Dates.
        add("now_rfc3339", func_now_rfc3339);
        add("unix_ts_to_rfc3339", func_unix_ts_to_rfc3339);
        add("rfc3339_to_unix_ts", func_rfc3339_to_unix_ts);
        add("now_timestamp", func_now_timestamp);
        add("format_date", func_format_date);
        add("date_to_unix_ts", func_date_to_unix_ts);
        add("timezone_to_second", func_timezone_to_offset);
        add("timezone_to_offset_seconds", func_timezone_to_offset);
        add("mongo_date", func_mongo_date);
        // System / random.
        add("getenv", func_getenv);
        add("random", func_random);
        add("uuid_v4", func_uuid_v4);
        add("uuid_v4_no_hyphen", func_uuid_v4_no_hyphen);
        // State.
        add("proc_dict_get", func_proc_dict_get);
        add("proc_dict_put", func_proc_dict_put);
        add("proc_dict_del", func_proc_dict_del);
        add("kv_store_get", func_kv_store_get);
        add("kv_store_put", func_kv_store_put);
        add("kv_store_del", func_kv_store_del);
        // Internal aliases for colliding names (see module docs): the
        // SQL rewriter in `crate::rewrite_colliding_functions` targets
        // these so standard semantics apply without an engine change.
        add("std_substr", func_substr);
        add("std_split", func_split);
        add("std_concat", func_concat);
        add("std_length", func_length);
        add("std_base64_encode", func_base64_encode_full);
        add("std_base64_decode", func_base64_decode_full);
        add("std_rtrim", func_rtrim_full);
    });
}
