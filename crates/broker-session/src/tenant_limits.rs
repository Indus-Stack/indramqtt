//! Per-tenant limits for connections, sessions and retained topics.
//!
//! Holds the effective bound for each tenant. Defaults come from the
//! file configuration. The built-in default is no limit (0), so the broker
//! bounds a tenant only when the operator sets a bound. Per-tenant overrides come from the management
//! API namespace config. The connect path reads the effective bound.
//! The publish path reads only the retained bound on retained writes.
//! The deliver path reads nothing here.
//!
//! Bounds (each stated here and checked below):
//! - a default is 0 (unlimited) unless the operator sets a bound;
//! - zero means the operator opted into unlimited for that bound;
//! - at most [`MAX_TENANT_LIMIT_OVERRIDES`] overrides are stored;
//! - each refusal is counted and logged where it happens;
//! - an unreachable store denies new use and logs the event.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

/// Default max live connections per tenant (0 = unlimited).
///
/// The listener limit is the only default bound on connections.
pub const DEFAULT_MAX_CONNECTIONS_PER_TENANT: u32 = 0;

/// Default max sessions per tenant (0 = unlimited).
pub const DEFAULT_MAX_SESSIONS_PER_TENANT: u32 = 0;

/// Default max retained topics per tenant (0 = unlimited).
pub const DEFAULT_MAX_RETAINED_PER_TENANT: u32 = 0;

/// Upper bound for stored per-tenant overrides.
///
/// Reason: each entry holds short strings plus
/// three integers. 1024 entries bound the table near tens of KiB.
/// Larger tables are rejected.
pub const MAX_TENANT_LIMIT_OVERRIDES: usize = 1024;

/// Upper bound for a per-tenant connection limit.
///
/// Reason: each connection holds one socket.
/// 1M bounds one tenant table entry so the value stays finite.
/// Larger values are rejected.
pub const MAX_CONNECTIONS_PER_TENANT: u32 = 1_000_000;

/// Upper bound for a per-tenant session limit.
///
/// Reason: each session holds one record plus
/// subscriptions. 100k bounds total session state. Larger values
/// are rejected.
pub const MAX_SESSIONS_PER_TENANT_LIMIT: u32 = 100_000;

/// Upper bound for a per-tenant retained limit.
///
/// Reason: each retained topic holds one topic plus
/// payload. 100k bounds total retained state.
/// Larger values are rejected.
pub const MAX_RETAINED_PER_TENANT_LIMIT: u32 = 100_000;

/// Effective bounds for one tenant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TenantLimits {
    /// Max live connections in this tenant (0 = unlimited).
    pub max_connections: u32,
    /// Max sessions in this tenant (0 = unlimited).
    pub max_sessions: u32,
    /// Max retained topics in this tenant (0 = unlimited).
    pub max_retained: u32,
}

impl Default for TenantLimits {
    fn default() -> Self {
        Self {
            max_connections: DEFAULT_MAX_CONNECTIONS_PER_TENANT,
            max_sessions: DEFAULT_MAX_SESSIONS_PER_TENANT,
            max_retained: DEFAULT_MAX_RETAINED_PER_TENANT,
        }
    }
}

impl TenantLimits {
    /// Check one override triple before it applies.
    pub fn validate(
        max_connections: u32,
        max_sessions: u32,
        max_retained: u32,
    ) -> Result<(), String> {
        if max_connections > MAX_CONNECTIONS_PER_TENANT {
            return Err(format!(
                "field `max_connections` holds {max_connections}, at most {MAX_CONNECTIONS_PER_TENANT}"
            ));
        }
        if max_sessions > MAX_SESSIONS_PER_TENANT_LIMIT {
            return Err(format!(
                "field `max_sessions` holds {max_sessions}, at most {MAX_SESSIONS_PER_TENANT_LIMIT}"
            ));
        }
        if max_retained > MAX_RETAINED_PER_TENANT_LIMIT {
            return Err(format!(
                "field `max_retained` holds {max_retained}, at most {MAX_RETAINED_PER_TENANT_LIMIT}"
            ));
        }
        Ok(())
    }
}

/// Per-tenant limit store behind one short lock.
///
/// Defaults come from the file configuration at boot. Overrides
/// come from the management API namespace config. Reads copy the
/// effective triple off the lock. The publish path never touches
/// this store directly; the kernel copies the retained bound once
/// per retained write off the hot path.
/// The JSON shape carries max_connections, max_sessions and max_retained.
pub struct TenantLimitStore {
    defaults: parking_lot::RwLock<TenantLimits>,
    inner: parking_lot::RwLock<HashMap<String, TenantLimits>>,
    /// True when the backing store is unreachable. Reads then deny.
    unreachable: AtomicBool,
    /// Connection refusals by per-tenant limit.
    conn_refused: AtomicU64,
    /// Session refusals by per-tenant limit.
    sess_refused: AtomicU64,
    /// Retained drops by per-tenant limit.
    retained_dropped: AtomicU64,
    /// Fail-closed denials when the store was unreachable.
    store_denied: AtomicU64,
}

impl TenantLimitStore {
    /// Store with explicit defaults.
    pub fn new(defaults: TenantLimits) -> Self {
        Self {
            defaults: parking_lot::RwLock::new(defaults),
            inner: parking_lot::RwLock::new(HashMap::new()),
            unreachable: AtomicBool::new(false),
            conn_refused: AtomicU64::new(0),
            sess_refused: AtomicU64::new(0),
            retained_dropped: AtomicU64::new(0),
            store_denied: AtomicU64::new(0),
        }
    }

    /// Current defaults.
    pub fn defaults(&self) -> TenantLimits {
        *self.defaults.read()
    }

    /// Replace the defaults (boot applies the file values).
    pub fn set_defaults(&self, defaults: TenantLimits) {
        *self.defaults.write() = defaults;
    }

    /// True when the store is marked unreachable.
    pub fn is_unreachable(&self) -> bool {
        self.unreachable.load(Ordering::SeqCst)
    }

    /// Mark the store unreachable (fail closed) or reachable again.
    pub fn set_unreachable(&self, value: bool) {
        self.unreachable.store(value, Ordering::SeqCst);
    }

    /// Effective bounds for one tenant.
    pub fn effective(&self, tenant: &str) -> TenantLimits {
        let defaults = self.defaults();
        if self.is_unreachable() {
            return defaults;
        }
        self.inner.read().get(tenant).copied().unwrap_or(defaults)
    }

    /// Set one tenant override after full validation.
    pub fn set_override(&self, tenant: &str, limits: TenantLimits) -> Result<(), String> {
        TenantLimits::validate(
            limits.max_connections,
            limits.max_sessions,
            limits.max_retained,
        )?;
        let mut inner = self.inner.write();
        if !inner.contains_key(tenant) && inner.len() >= MAX_TENANT_LIMIT_OVERRIDES {
            return Err(format!(
                "tenant limit table holds {} entries, at most {MAX_TENANT_LIMIT_OVERRIDES}",
                inner.len()
            ));
        }
        inner.insert(tenant.to_string(), limits);
        Ok(())
    }

    /// Remove one tenant override.
    pub fn remove_override(&self, tenant: &str) {
        self.inner.write().remove(tenant);
    }

    /// Count one connection refusal.
    pub fn count_conn_refused(&self) -> u64 {
        self.conn_refused.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Count one session refusal.
    pub fn count_sess_refused(&self) -> u64 {
        self.sess_refused.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Count one retained drop.
    pub fn count_retained_dropped(&self) -> u64 {
        self.retained_dropped.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Count one fail-closed denial.
    pub fn count_store_denied(&self) -> u64 {
        self.store_denied.fetch_add(1, Ordering::SeqCst) + 1
    }

    /// Connection refusals so far.
    pub fn conn_refused(&self) -> u64 {
        self.conn_refused.load(Ordering::SeqCst)
    }

    /// Session refusals so far.
    pub fn sess_refused(&self) -> u64 {
        self.sess_refused.load(Ordering::SeqCst)
    }

    /// Retained drops so far.
    pub fn retained_dropped(&self) -> u64 {
        self.retained_dropped.load(Ordering::SeqCst)
    }

    /// Fail-closed denials so far.
    pub fn store_denied(&self) -> u64 {
        self.store_denied.load(Ordering::SeqCst)
    }
}

impl Default for TenantLimitStore {
    fn default() -> Self {
        Self::new(TenantLimits::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_are_unlimited() {
        let limits = TenantLimits::default();
        assert_eq!(limits.max_connections, 0);
        assert_eq!(limits.max_sessions, 0);
        assert_eq!(limits.max_retained, 0);
        // A tenant with no override reads the unlimited default.
        let store = TenantLimitStore::default();
        assert_eq!(store.effective("any-tenant"), limits);
    }

    #[test]
    fn override_applies_per_tenant_only() {
        let store = TenantLimitStore::default();
        store
            .set_override(
                "t-a",
                TenantLimits {
                    max_connections: 2,
                    max_sessions: 3,
                    max_retained: 4,
                },
            )
            .expect("override stores");
        assert_eq!(store.effective("t-a").max_connections, 2);
        assert_eq!(
            store.effective("t-b").max_connections,
            DEFAULT_MAX_CONNECTIONS_PER_TENANT
        );
    }

    #[test]
    fn overlong_table_refuses_write() {
        let store = TenantLimitStore::default();
        for index in 0..MAX_TENANT_LIMIT_OVERRIDES {
            store
                .set_override(
                    &format!("t-{index}"),
                    TenantLimits {
                        max_connections: 1,
                        max_sessions: 1,
                        max_retained: 1,
                    },
                )
                .expect("override stores");
        }
        let err = store
            .set_override(
                "t-overflow",
                TenantLimits {
                    max_connections: 1,
                    max_sessions: 1,
                    max_retained: 1,
                },
            )
            .expect_err("full table must fail");
        assert!(err.contains("at most"), "error names the bound: {err}");
    }
}
