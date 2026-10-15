pub mod authz_node_cache;
pub mod authz_settings;
pub mod authz_sources;
pub mod chain;
pub mod db;
pub mod jwks;
/// Shared JWKS test fixtures for B5-02 (T-95): the RSA signing-key minter
/// and the loopback HTTPS JWKS server, single-sourced here and reused by
/// the `broker-auth` authenticator tests and (via `#[path]` include) the
/// `broker-node` CONNECT tests. Test-only: compiled into the test harness
/// only, never into production binaries.
#[cfg(test)]
pub mod jwks_test_support;
pub mod kerberos;
pub mod ldap;
pub mod node_cache;
pub mod psk;
pub mod settings;
#[cfg(test)]
pub mod test_directory_server;
pub mod webhook;

pub use authz_node_cache::{AuthzCacheUpdateError, AuthzNodeCache};
pub use authz_settings::{AuthzSettingsStore, AuthzSettingsUpdateError};
pub use authz_sources::{
    AuthzSourceCounterSnapshot, AuthzSourceEntry, AuthzSourceRegistry, SourceInsertError,
    SourceMoveError, SourceRemoveError, SourceReorderError, SourceUpdateError,
};
pub use chain::{
    AuthnChain, AuthnCounterSnapshot, AuthnEntry, ChainInsertError, ChainMoveError,
    ChainRemoveError, ChainReorderError, ChainUpdateError,
};
pub use db::{
    DbAuthSet, DbAuthSetConfig, DbSourceSettings, MongoAuth, MongoAuthConfig, MysqlAuth,
    MysqlAuthConfig, PostgresAuth, PostgresAuthConfig, RedisAuth, RedisAuthConfig,
    DEFAULT_CACHE_MAX_ENTRIES, DEFAULT_CACHE_TTL_SECS, DEFAULT_CONNECT_TIMEOUT_MS,
    DEFAULT_POOL_SIZE, DEFAULT_READ_TIMEOUT_MS,
};
pub use jwks::{
    JwksAuthenticator, JwksConfig, VerifiedJwt, JWKS_CACHE_MAX_KEYS, JWKS_DEFAULT_DOCUMENT_CAP,
};
pub use kerberos::{KerberosAuthenticator, KerberosConfig, REPLAY_MAX_ENTRIES};
pub use ldap::{LdapAuthenticator, LdapConfig};
pub use node_cache::NodeAuthCache;
pub use psk::{PskInsertError, PskStore};
pub use psk::{MAX_PSK_ENTRIES, MAX_PSK_IDENTITY_BYTES, MAX_PSK_KEY_BYTES, MIN_PSK_KEY_BYTES};
pub use settings::{AuthnSettingsStore, SettingsSubscriber, SettingsUpdateError};
pub use webhook::{
    WebhookAuth, WebhookConfig, WEBHOOK_BREAKER_RESET_MS, WEBHOOK_BREAKER_THRESHOLD,
    WEBHOOK_CACHE_MAX_ENTRIES, WEBHOOK_CACHE_TTL_SECS, WEBHOOK_POOL_SIZE,
    WEBHOOK_REQUEST_TIMEOUT_MS,
};

use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier as _, SaltString};
use argon2::{Algorithm, Argon2, Params, Version};
use async_trait::async_trait;
use base64::Engine as _;
use broker_config::{AclConf, ConfigRegistry, MqttUser, MqttUsersConf};
use broker_protocol::{Topic, TopicFilter};
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::Arc;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AuthError {
    #[error("Authentication failed for client {0}")]
    AuthenticationFailed(String),

    #[error("Permission denied: cannot publish to {0}")]
    PublishDenied(String),

    #[error("Permission denied: cannot subscribe to {0}")]
    SubscribeDenied(String),
}

pub type Result<T> = std::result::Result<T, AuthError>;

#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(
        &self,
        client_id: &str,
        username: Option<&str>,
        password: Option<&[u8]>,
    ) -> Result<()>;
}

#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn authorize_publish(&self, client_id: &str, topic: &Topic) -> Result<()>;
    async fn authorize_subscribe(&self, client_id: &str, filter: &TopicFilter) -> Result<()>;
}

#[derive(Default)]
pub struct AllowAllAuth;

#[async_trait]
impl Authenticator for AllowAllAuth {
    async fn authenticate(
        &self,
        _client_id: &str,
        _username: Option<&str>,
        _password: Option<&[u8]>,
    ) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl Authorizer for AllowAllAuth {
    async fn authorize_publish(&self, _client_id: &str, _topic: &Topic) -> Result<()> {
        Ok(())
    }

    async fn authorize_subscribe(&self, _client_id: &str, _filter: &TopicFilter) -> Result<()> {
        Ok(())
    }
}

/// MQTT action gated by an ACL rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AclAction {
    Publish,
    Subscribe,
    All,
}

impl AclAction {
    /// Parse `publish` | `subscribe` | `all` (case-insensitive).
    pub fn parse(action: &str) -> Option<Self> {
        match action.to_ascii_lowercase().as_str() {
            "publish" => Some(AclAction::Publish),
            "subscribe" => Some(AclAction::Subscribe),
            "all" => Some(AclAction::All),
            _ => None,
        }
    }

    fn covers(self, action: AclAction) -> bool {
        self == AclAction::All || self == action
    }
}

/// One ordered ACL entry: first match wins.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AclRule {
    pub client_pattern: String,
    pub action: AclAction,
    pub topic_pattern: String,
    pub allow: bool,
}

impl AclRule {
    pub fn new(
        client_pattern: impl Into<String>,
        action: AclAction,
        topic_pattern: impl Into<String>,
        allow: bool,
    ) -> Self {
        Self {
            client_pattern: client_pattern.into(),
            action,
            topic_pattern: topic_pattern.into(),
            allow,
        }
    }

    fn client_matches(&self, client_id: &str) -> bool {
        if self.client_pattern == "*" || self.client_pattern == client_id {
            return true;
        }
        // Otherwise the pattern may use MQTT wildcards over the id.
        if !self.client_pattern.contains(['+', '#']) {
            return false;
        }
        TopicFilter::new(self.client_pattern.as_str())
            .and_then(|filter| Topic::new(client_id).map(|id| (filter, id)))
            .map(|(filter, id)| filter.matches(&id))
            .unwrap_or(false)
    }

    fn topic_matches(&self, topic: &Topic) -> bool {
        TopicFilter::new(self.topic_pattern.as_str())
            .map(|filter| filter.matches(topic))
            .unwrap_or(false)
    }

    /// Whether `filter` (a subscription request) is covered by this rule's
    /// topic pattern: every topic the request could match must also match
    /// the rule pattern. Level-wise: `#` (final) covers anything below,
    /// `+` covers exactly one level, anything else must equal.
    fn filter_covered(&self, filter: &TopicFilter) -> bool {
        let pattern_levels: Vec<&str> = self.topic_pattern.split('/').collect();
        let request_levels: Vec<&str> = filter.as_str().split('/').collect();
        let mut pi = 0;
        for &req in request_levels.iter() {
            match pattern_levels.get(pi) {
                Some(&"#") => return true,
                Some(&"+") => {
                    pi += 1;
                }
                Some(&pat) if pat == req => {
                    pi += 1;
                }
                _ => return false,
            }
        }
        pi == pattern_levels.len() || (pi == pattern_levels.len() - 1 && pattern_levels[pi] == "#")
    }
}

/// In-memory credentials + ACL authorizer.
///
/// Passwords are stored as versioned verifiers: legacy SHA-256 hex plus
/// bcrypt, PBKDF2-SHA256 and Argon2id (B5-01). With zero users configured
/// the broker is open; once users exist, unknown names and bad passwords
/// fail. With zero ACL rules everything is allowed; once rules exist the
/// first matching rule decides and anything unmatched follows the stored
/// `no_match` setting (`deny` when no settings store is linked).
///
/// There is no network credential store behind this type, so every
/// failure mode denies access and never grants it: unknown users, locked
/// or undecodable verifiers, oversized passwords, exhausted verification
/// permits and blocking-task failures all return `AuthenticationFailed`
/// and are logged. A registry commit or save failure is surfaced to the
/// caller (HTTP 500 on the management plane) and never silent.
///
/// Expensive verifiers run in `tokio::task::spawn_blocking` off the
/// accept path while CONNECT still awaits the verdict, gated by a
/// semaphore with [`MAX_CONCURRENT_VERIFICATIONS`] permits; exhaustion
/// fails closed instead of queueing unboundedly. Concurrent CONNECTs
/// with the same credential share one check through
/// [`MAX_VERIFY_DEDUP_INFLIGHT`]; past the cap they verify directly.
/// The sharing absorbs a reconnect burst: one hash serves many waiters.
/// [`MemoryAuth::verify_run_count`] reports the hashes run, so a burst
/// test proves the cost stays within the credential count.
/// Legacy SHA-256 verifies
/// inline: a single hash over at most [`MAX_PASSWORD_BYTES`] bytes with no
/// tunable cost, so off-thread dispatch would add overhead without benefit.
/// A successful legacy login migrates to the configured default off the
/// connect task in `spawn_blocking`, gated by a semaphore with
/// [`MAX_CONCURRENT_MIGRATIONS`] permits; exhaustion keeps the legacy
/// verifier (still verifiable) and retries on the next login instead of
/// queueing unboundedly.
///
/// When built with [`MemoryAuth::from_registry`] (or seeded in place
/// with [`MemoryAuth::seed_from_registry`]) every user/ACL/quota
/// mutation commits the exported [`MqttUsersConf`] root and atomically
/// saves it, so credentials, ACLs and per-user quotas ([`UserQuotas`])
/// survive kernel restarts.
pub struct MemoryAuth {
    /// Per-tenant credential and ACL tables keyed by tenant id (MT-06):
    /// the same username in two tenants is two independent identities
    /// with independent password hashes and rule sets. The default tenant
    /// is seeded from today's persisted tables; new tenants start closed
    /// (no users, no rules, deny). One short lock; the publish/deliver
    /// authorisation reads state the check already holds (no new lock:
    /// one guard covers the tenant map plus the tenant's rule scan).
    tenants: RwLock<HashMap<String, TenantState>>,
    /// True while the default tenant holds no ACL rule. The publish check
    /// reads it with one atomic load and skips the tenant lock then: an
    /// empty default tenant allows every publish. Every change of the
    /// tenant tables goes through [`MemoryAuth::tenants_mut`] (or the
    /// default rule guard), which refreshes this flag before it releases
    /// the write lock.
    default_rules_empty: std::sync::atomic::AtomicBool,
    /// Closed-tenant denials (unknown/invalid tenant, or a non-default
    /// tenant with no users/rules): counted exactly where the denial
    /// happens, fail closed and logged there.
    tenant_denials: std::sync::atomic::AtomicU64,
    /// Kernel config registry receiving every user/ACL mutation (`None`
    /// in unit tests and standalone state, which stay memory-only).
    /// Every tenant persists through it: each mutation commits the
    /// exported root (all tenants) and saves it atomically, so a restart
    /// and a reload keep all tenants.
    registry: RwLock<Option<Arc<ConfigRegistry>>>,
    /// Serialises export-commit-save so concurrent mutations cannot
    /// interleave into a lost update on disk.
    save_lock: parking_lot::Mutex<()>,
    /// Hashing policy for new credentials and migrations (see
    /// [`PasswordHashPolicy`]). Replaced atomically; reads take a short
    /// read lock on the management plane only, never on delivery.
    policy: RwLock<PasswordHashPolicy>,
    /// Ordered authorization source registry driving the stored
    /// evaluation order and the live allow/deny counters
    /// (`AuthzSourceRegistry` in `crate::authz_sources`, shared with the
    /// management API). `None` in unit tests and standalone state, which
    /// stay memory-only with today's open behaviour. `Some` on the kernel
    /// and the management plane after boot links the same instance the
    /// API serves, so publish/subscribe verdicts attribute to the
    /// built-in source and honour its stored enable flag and order.
    /// Bound: at most [`broker_config::MAX_AUTHZ_SOURCES`] counter
    /// records (8: one per source type, each exactly three atomics, no
    /// list or history ever accumulates); linking is one `Arc` clone at
    /// boot, verdicts take one short read lock to clone the counter
    /// `Arc` plus lock-free atomic adds, and order consults load one
    /// lock-free snapshot of at most the capped entries.
    authz_sources: arc_swap::ArcSwapOption<AuthzSourceRegistry>,
    /// Global authorization settings served by the management API
    /// (`AuthzSettingsStore` in `crate::authz_settings`, shared with the
    /// management API). `None` in unit tests and standalone state, which
    /// keep today's behaviour (unmatched denied, denied ignored). `Some`
    /// on the kernel after boot links the same instance the API serves,
    /// so publish/subscribe verdicts honour the stored `no_match` and
    /// `deny_action` values. Linking is one `Arc` clone at boot. The
    /// authorize path reads the two bools with one lock-free snapshot
    /// load each. It loads only on the unmatched or denied path, never
    /// a snapshot clone per message.
    authz_settings: arc_swap::ArcSwapOption<AuthzSettingsStore>,
    /// Bounds concurrent expensive verifications (bcrypt/PBKDF2/Argon2).
    /// `Arc` so the async verifier can hold an owned permit across the
    /// blocking task without borrowing `self`.
    verify_gate: Arc<tokio::sync::Semaphore>,
    /// In-flight dedup for concurrent same-credential expensive checks.
    /// `Arc` so CONNECT tasks share entries without borrowing `self`.
    /// Bound: at most [`MAX_VERIFY_DEDUP_INFLIGHT`] keys; past the cap
    /// new CONNECTs verify directly without sharing.
    verify_inflight: Arc<tokio::sync::Mutex<HashMap<VerifyDedupKey, Arc<VerifyInflight>>>>,
    /// Waiters that joined a leader check instead of hashing again. One
    /// atomic; the storm test reads it to prove sharing happened.
    verify_dedup_joins: std::sync::atomic::AtomicU64,
    /// Expensive verifications that ran the blocking hash. One atomic;
    /// the storm test reads it to prove the burst cost stays bounded.
    verify_runs: std::sync::atomic::AtomicU64,
    /// Bounds concurrent legacy-hash migrations (re-hashes at production
    /// cost). `Arc` so the async migration can hold an owned permit
    /// across the blocking task without borrowing `self`.
    migrate_gate: Arc<tokio::sync::Semaphore>,
}

impl std::fmt::Debug for MemoryAuth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MemoryAuth")
            .field("tenants", &self.tenants)
            .field(
                "tenant_denials",
                &self
                    .tenant_denials
                    .load(std::sync::atomic::Ordering::SeqCst),
            )
            .field("policy", &self.policy)
            .field("verify_permits", &self.verify_gate.available_permits())
            .field("migrate_permits", &self.migrate_gate.available_permits())
            .finish_non_exhaustive()
    }
}

impl Default for MemoryAuth {
    fn default() -> Self {
        Self {
            tenants: RwLock::new(HashMap::new()),
            tenant_denials: std::sync::atomic::AtomicU64::new(0),
            registry: RwLock::new(None),
            save_lock: parking_lot::Mutex::new(()),
            policy: RwLock::new(PasswordHashPolicy::default()),
            authz_sources: arc_swap::ArcSwapOption::empty(),
            authz_settings: arc_swap::ArcSwapOption::empty(),
            default_rules_empty: std::sync::atomic::AtomicBool::new(true),
            verify_gate: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_VERIFICATIONS)),
            verify_inflight: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            verify_dedup_joins: std::sync::atomic::AtomicU64::new(0),
            verify_runs: std::sync::atomic::AtomicU64::new(0),
            migrate_gate: Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT_MIGRATIONS)),
        }
    }
}

/// Supported password-hashing algorithms. `Sha256Legacy` is the
/// pre-B5-01 unsalted hex digest: still verified during the migration
/// window, never used for new credentials under the default policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PasswordAlgorithm {
    Sha256Legacy,
    Bcrypt,
    Pbkdf2Sha256,
    Argon2id,
}

/// Default bcrypt cost (2^cost Blowfish rounds). Reason: the long-standing
/// bcrypt default and the lowest cost that still resists offline cracking
/// on modern GPUs; CONNECT is infrequent so the cost is acceptable, and
/// operators with high-churn fleets may lower it to 10.
pub const DEFAULT_BCRYPT_COST: u32 = 12;
/// Default PBKDF2-HMAC-SHA256 iteration count. Reason: the OWASP 2023
/// recommendation for PBKDF2-HMAC-SHA256 (600,000); CPU-hard where
/// Argon2id's memory hardness is unavailable.
pub const DEFAULT_PBKDF2_ITERATIONS: u32 = 600_000;
/// Default PBKDF2 salt length in bytes. Reason: 128-bit salts make
/// rainbow tables infeasible while keeping the verifier string short.
pub const DEFAULT_PBKDF2_SALT_LEN: usize = 16;
/// Default PBKDF2 derived-key length in bytes. Reason: 256-bit output
/// matches HMAC-SHA-256's native width with no truncation.
pub const DEFAULT_PBKDF2_KEY_LEN: usize = 32;
/// Default Argon2id memory cost in KiB. Reason: the OWASP first
/// recommendation / RFC 9106 (19 MiB) for interactive logins.
pub const DEFAULT_ARGON2_M_KIB: u32 = 19_456;
/// Default Argon2id time cost (passes). Reason: OWASP first
/// recommendation (2) paired with the memory cost above.
pub const DEFAULT_ARGON2_T_COST: u32 = 2;
/// Default Argon2id parallelism (lanes). Reason: 1 lane keeps per-attempt
/// memory strictly at `m` KiB on the shared 8-core test host; higher
/// values multiply memory per attempt with no CONNECT-latency benefit.
pub const DEFAULT_ARGON2_P_COST: u32 = 1;
/// Default Argon2 salt length in bytes. Reason: 128-bit salts, the
/// password-hashing competition minimum.
pub const DEFAULT_ARGON2_SALT_LEN: usize = 16;
/// Default Argon2 output length in bytes. Reason: 256-bit tags match the
/// other schemes and fit the PHC string in one line.
pub const DEFAULT_ARGON2_KEY_LEN: usize = 32;
/// Bound on concurrent expensive verifications. Reason: verifications
/// use memory-hard hashes on the accept path;
/// excess CONNECTs fail closed instead of queueing unboundedly there.
pub const MAX_CONCURRENT_VERIFICATIONS: usize = 32;
/// Bound on concurrent same-credential dedup entries. Reason: one entry
/// per verification permit in [`MAX_CONCURRENT_VERIFICATIONS`]; a burst
/// holds at most one shared check per permit. Past the cap new CONNECTs
/// verify directly.
pub const MAX_VERIFY_DEDUP_INFLIGHT: usize = MAX_CONCURRENT_VERIFICATIONS;
/// Key for concurrent same-credential verification sharing.
/// The password hash is SHA-256 of the presented password.
/// The verifier is the stored encoding the lookup found.
/// Same inputs share one expensive check with the same result.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct VerifyDedupKey {
    tenant: String,
    username: String,
    pwd_hash: [u8; 32],
    verifier: String,
}
/// In-flight expensive verification shared by concurrent CONNECTs.
/// One leader runs the blocking check; waiters await the result.
/// The result is atomic so the cancel path sets it without a lock.
/// Values: 0 means pending, 1 means success, 2 means refusal.
struct VerifyInflight {
    notify: tokio::sync::Notify,
    result: std::sync::atomic::AtomicU8,
}
impl VerifyInflight {
    /// Pending leader check.
    const PENDING: u8 = 0;
    /// Successful leader check.
    const OK: u8 = 1;
    /// Failed or cancelled leader check.
    const REFUSED: u8 = 2;
}
/// Guard that fails waiters closed and removes the entry on early exit.
/// The leader sets `completed` on the normal path; early drop (cancel)
/// records refusal, wakes waiters and removes the key when it still
/// points at this entry.
struct VerifyLeaderGuard {
    map: Arc<tokio::sync::Mutex<HashMap<VerifyDedupKey, Arc<VerifyInflight>>>>,
    key: Option<VerifyDedupKey>,
    entry: Arc<VerifyInflight>,
    completed: bool,
}
impl Drop for VerifyLeaderGuard {
    fn drop(&mut self) {
        if self.completed {
            return;
        }
        // Fail waiters closed. A stored outcome stays.
        let _ = self.entry.result.compare_exchange(
            VerifyInflight::PENDING,
            VerifyInflight::REFUSED,
            std::sync::atomic::Ordering::SeqCst,
            std::sync::atomic::Ordering::SeqCst,
        );
        self.entry.notify.notify_waiters();
        if let Some(key) = self.key.take() {
            if let Ok(mut table) = self.map.try_lock() {
                let same = table
                    .get(&key)
                    .map(|current| Arc::ptr_eq(current, &self.entry))
                    .unwrap_or(false);
                if same {
                    table.remove(&key);
                }
            } else {
                // The lock is busy and Drop cannot await. The entry
                // keeps its stored refusal and the next lookup for
                // this key reclaims it. Log the miss, never stay silent.
                tracing::warn!("mqtt authentication dedup entry reclaimed on next lookup");
            }
        }
    }
}
/// Bound on concurrent legacy-hash migrations. Reason: a burst of legacy
/// logins must not re-hash unboundedly; at most this many
/// migrations hash concurrently, excess CONNECTs keep the legacy verifier
/// and retry on the next login instead of queueing
/// unbounded blocking work behind the connect path.
pub const MAX_CONCURRENT_MIGRATIONS: usize = 4;
/// Bound on password bytes copied per attempt. Reason: 8 KiB covers human
/// passwords and service tokens while staying far below the 64 KiB MQTT
/// wire cap, so one CONNECT can cost at most one 8 KiB clone plus fixed
/// hash state; longer passwords fail closed rather than allocating
/// unboundedly.
pub const MAX_PASSWORD_BYTES: usize = 8 * 1024;
/// Bound on bcrypt password bytes. Reason: the maintained bcrypt primitive
/// truncates input to 72 bytes, so longer inputs must fail closed rather
/// than verifying by prefix.
pub const MAX_BCRYPT_PASSWORD_BYTES: usize = 72;
/// Bound on the stored verifier string. Reason: the longest PHC encoding
/// (Argon2id) is well under 300 bytes; 2 KiB rejects a corrupt state file
/// without unbounded allocation while leaving headroom for future params.
pub const MAX_STORED_VERIFIER_BYTES: usize = 2048;
/// Bound on users per authenticator (the built-in user map). Reason: each
/// entry holds a verifier plus map overhead, so an unbounded map lets one
/// management batch exhaust the kernel; 100,000 entries cover fleet
/// provisioning while keeping list reads paged and per-CONNECT lookups
/// cheap. Default is the empty map (open broker until users exist).
pub const MAX_AUTHN_USERS: usize = 100_000;
/// Bound on one import batch. Reason: the request body plus the validated
/// batch already hold every entry once each; 10,000 entries keep one
/// management request's hashing and memory bounded while large fleets
/// import in repeated batches.
pub const MAX_IMPORT_BATCH: usize = 10_000;
/// Longest accepted authenticator username (`user_id`). Reason: usernames
/// ride the per-CONNECT lookup and the persisted snapshot, so one entry
/// cannot balloon either; 256 bytes cover service names with headroom
/// while bounding map and file growth.
pub const MAX_AUTHN_USERNAME_LEN: usize = 256;
/// Bound on catch-all authorization rules (the built-in rule table behind
/// `GET/POST/DELETE /authorization/sources/built_in_database/rules/all`).
/// Reason: the management plane snapshots the whole list per request and
/// the publish/subscribe decision scans it linearly under a short read
/// lock, so the list must stay small and bounded.
/// Writes past the cap are rejected instead of growing without
/// limit. Default is the empty list (deny unless a rule allows, per
/// documented semantics: with zero rules everything is allowed until the
/// first rule is written, then first-match-wins with unmatched denied).
// The table holds at most 128 rules.
// Each rule carries topic, permission and action.
pub const MAX_AUTHZ_CATCH_ALL_RULES: usize = 128;
/// Bound on per-client authorization rules (the built-in rule table behind
/// `GET/POST /authorization/sources/built_in_database/rules/clients`,
/// `GET/PUT/DELETE .../rules/clients/{clientid}` and
/// `GET .../rules/users`, filtered to client-scoped
/// entries). Reason: the management plane snapshots the whole rule list
/// per request and the publish/subscribe decision scans it linearly under
/// a short read lock, so the list must stay bounded.
/// Writes past the cap are rejected instead of
/// growing without limit. Default is the empty list (with zero rules
/// everything is allowed until the first rule is written, then
/// first-match-wins with unmatched denied).
// The table holds at most 100000 rules.
// Each rule carries clientid, topic, permission and action.
pub const MAX_AUTHZ_CLIENT_RULES: usize = 100_000;
/// Longest accepted per-client rule `clientid`. Reason: ids ride the
/// per-decision scan and the persisted snapshot, so one entry cannot
/// balloon either; 256 bytes cover service names with headroom while
/// bounding map and file growth.
pub const MAX_AUTHZ_CLIENTID_LEN: usize = 256;
/// Bound on per-user authorization rules (the built-in rule table behind
/// `POST /authorization/sources/built_in_database/rules/users` and
/// `GET/DELETE .../rules/users/{username}`, read through the same shared
/// table as the per-client index). Reason: the management plane snapshots
/// the whole rule list per request and the publish/subscribe decision
/// scans it linearly under a short read lock, so the list must stay
/// bounded. Writes past the
/// cap are rejected instead of growing without limit. Default is the empty
/// index.
// The shared table holds at most 100000 user rules.
// Every stored rule stays live on the publish decision path.
pub const MAX_AUTHZ_USER_RULES: usize = 100_000;
/// Longest accepted per-user rule `username`. Reason: ids ride the
/// per-decision scan and the persisted snapshot, so one entry cannot
/// balloon either; 256 bytes cover service names with headroom while
/// bounding map and file growth.
pub const MAX_AUTHZ_USERNAME_LEN: usize = 256;

/// Why [`MemoryAuth::add_client_rule`] refused an entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ClientRuleError {
    /// A rule for the same client id and topic scope is already stored.
    Duplicate,
    /// The table holds [`MAX_AUTHZ_CLIENT_RULES`] entries.
    Full,
    /// The entry failed validation (message names the field).
    Invalid(String),
    /// The registry commit or atomic save failed after validation.
    Persist(String),
}
/// Tenant id for clients with no tenant attribute (MT-06). Reason: mirrors
/// the session tenant default (`broker-session::tenant::DEFAULT_TENANT_ID`)
/// without adding a dependency; the same literal keeps the default-tenant
/// seed identical to today's single global table.
pub const DEFAULT_TENANT_ID: &str = "default";
/// Longest accepted tenant id (MT-06). Reason: mirrors the MT-01 session
/// bound (`MAX_TENANT_ID_LEN` 128): the tenant id joins session keys,
/// credential keys and log lines, so 128 chars bound key memory while
/// covering hierarchical names.
pub const MAX_TENANT_ID_LEN: usize = 128;
/// Upper bound for tenants held in the built-in table (MT-06). Reason: one
/// outer entry is one map header plus short strings, so 1024 entries bound
/// the table near tens of KiB while covering multi-tenant installs far
/// past real deployments; past the cap a new tenant is rejected instead
/// of growing without limit. Mirrors the session `MAX_TENANTS` backstop.
pub const MAX_AUTH_TENANTS: usize = 1024;
/// Upper bound for ACL rules per tenant (MT-06). Reason: authorisation
/// scans first-match-wins linearly on the publish/subscribe path, so
/// 10,000 entries bound per-check work to a small constant while covering
/// large fleets; writes past the cap are rejected instead of growing the
/// scan without limit.
pub const MAX_ACL_RULES_PER_TENANT: usize = 10_000;

/// Bulk-import result: atomic counts for the documented result shape.
/// A successful import applies every entry, so `total == success` and the
/// remaining buckets are zero; any failure applies nothing and surfaces
/// as [`ImportUsersError`] instead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImportSummary {
    pub total: usize,
    pub success: usize,
}

/// Why [`MemoryAuth::import_users`] refused a batch without applying it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ImportUsersError {
    /// An entry failed shape validation (message names the field).
    Malformed(String),
    /// A `user_id` appears twice in the batch or already exists.
    Duplicate(String),
    /// The batch is too large or would exceed [`MAX_AUTHN_USERS`].
    TooMany(String),
    /// The registry commit or atomic save failed after the in-memory
    /// apply (the import stays in memory; the next restart reloads disk).
    Persist(String),
}

/// Hashing policy for new credentials and migrations. Every field is
/// operator-configurable via [`MemoryAuth::set_policy`]; the defaults
/// below are the secure starting point (see each constant's reason).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PasswordHashPolicy {
    /// Algorithm for new credentials, password changes and SHA-256
    /// migrations. Default Argon2id: memory-hard, so a stolen credential
    /// table cannot be tested at billions of candidates per second.
    pub default_algorithm: PasswordAlgorithm,
    /// bcrypt cost used when the default or explicit algorithm is bcrypt.
    pub bcrypt_cost: u32,
    /// PBKDF2-HMAC-SHA256 iterations for new PBKDF2 verifiers.
    pub pbkdf2_iterations: u32,
    /// Argon2id memory cost (KiB) for new Argon2 verifiers.
    pub argon2_m_kib: u32,
    /// Argon2id time cost for new Argon2 verifiers.
    pub argon2_t_cost: u32,
    /// Argon2id parallelism for new Argon2 verifiers.
    pub argon2_p_cost: u32,
}

impl Default for PasswordHashPolicy {
    fn default() -> Self {
        Self {
            default_algorithm: PasswordAlgorithm::Argon2id,
            bcrypt_cost: DEFAULT_BCRYPT_COST,
            pbkdf2_iterations: DEFAULT_PBKDF2_ITERATIONS,
            argon2_m_kib: DEFAULT_ARGON2_M_KIB,
            argon2_t_cost: DEFAULT_ARGON2_T_COST,
            argon2_p_cost: DEFAULT_ARGON2_P_COST,
        }
    }
}

impl PasswordHashPolicy {
    /// Fast, insecure policy for tests only: bcrypt cost 4, PBKDF2 1,000
    /// iterations, Argon2id 8 MiB x 1 x 1. Keeps suites interactive while
    /// exercising every code path. Never use in production (production
    /// uses [`PasswordHashPolicy::default`]).
    pub fn for_tests() -> Self {
        Self {
            default_algorithm: PasswordAlgorithm::Argon2id,
            bcrypt_cost: 4,
            pbkdf2_iterations: 1_000,
            argon2_m_kib: 8 * 1024,
            argon2_t_cost: 1,
            argon2_p_cost: 1,
        }
    }
}

/// True when `tenant` names the default tenant: empty (absent) or the
/// default literal. Empty rides as default so callers passing an
/// unassigned connection land in the pre-tenancy path, never in another
/// tenant's space.
fn is_default_tenant(tenant: &str) -> bool {
    tenant.is_empty() || tenant == DEFAULT_TENANT_ID
}

/// True when the tenant id may key credential/ACL state: non-empty, at
/// most [`MAX_TENANT_ID_LEN`] chars and free of control characters.
/// Mirrors the MT-01 session bound; invalid ids fail closed (deny, count,
/// log), never route into another tenant's space.
// Overlong and control ids deny outright.
// The broker never routes them into another tenant.
pub fn is_valid_tenant_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= MAX_TENANT_ID_LEN && !id.chars().any(|c| c.is_control())
}

/// Canonical tenant key for credential/ACL lookups: empty rides as the
/// default tenant. Callers validate non-default ids with
/// [`is_valid_tenant_id`] before consulting state.
fn tenant_key(tenant: &str) -> &str {
    if tenant.is_empty() {
        DEFAULT_TENANT_ID
    } else {
        tenant
    }
}

/// Write guard of the tenant tables. On release it refreshes the flag
/// that tells the publish check whether the default tenant holds rules.
struct TenantsGuard<'a> {
    map: parking_lot::RwLockWriteGuard<'a, HashMap<String, TenantState>>,
    flag: &'a std::sync::atomic::AtomicBool,
}

impl std::ops::Deref for TenantsGuard<'_> {
    type Target = HashMap<String, TenantState>;
    fn deref(&self) -> &Self::Target {
        &self.map
    }
}

impl std::ops::DerefMut for TenantsGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.map
    }
}

impl Drop for TenantsGuard<'_> {
    fn drop(&mut self) {
        let empty = self
            .map
            .get(DEFAULT_TENANT_ID)
            .is_none_or(|state| state.rules.is_empty());
        self.flag.store(empty, std::sync::atomic::Ordering::Release);
    }
}

/// Write guard of the rule list of the default tenant. On release it
/// refreshes the flag that tells the publish check whether that list is
/// empty.
struct DefaultRulesGuard<'a> {
    rules: parking_lot::MappedRwLockWriteGuard<'a, Vec<AclRule>>,
    flag: &'a std::sync::atomic::AtomicBool,
}

impl std::ops::Deref for DefaultRulesGuard<'_> {
    type Target = Vec<AclRule>;
    fn deref(&self) -> &Self::Target {
        &self.rules
    }
}

impl std::ops::DerefMut for DefaultRulesGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.rules
    }
}

impl Drop for DefaultRulesGuard<'_> {
    fn drop(&mut self) {
        self.flag
            .store(self.rules.is_empty(), std::sync::atomic::Ordering::Release);
    }
}

/// Per-tenant credential and ACL state: one user map plus one ordered
/// rule list. The default tenant is seeded from today's persisted tables;
/// every other tenant starts empty (closed: deny, counted, logged).
#[derive(Debug, Default)]
struct TenantState {
    users: HashMap<String, UserEntry>,
    rules: Vec<AclRule>,
}

/// Stored per-user password verifier: the algorithm plus its parameters
/// alongside the hash, so entries created under different settings each
/// verify correctly. `Locked` never verifies (fail closed).
#[derive(Debug, Clone, PartialEq, Eq)]
enum PasswordVerifier {
    Sha256([u8; 32]),
    Bcrypt(String),
    Pbkdf2 {
        iterations: u32,
        salt: Vec<u8>,
        hash: Vec<u8>,
    },
    Argon2(String),
    Locked,
}

/// Per-user multi-tenant quotas. Every bound is optional (`None` =
/// unlimited); unset quotas preserve the pre-quota open behavior.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct UserQuotas {
    pub max_connections: Option<u32>,
    pub max_publish_rate: Option<u32>,
    pub max_publish_burst: Option<u32>,
}

#[derive(Debug, Clone)]
struct UserEntry {
    verifier: PasswordVerifier,
    quotas: UserQuotas,
}

impl MemoryAuth {
    pub fn new() -> Self {
        Self::default()
    }

    /// Memory-only store with an explicit hashing policy (tests and
    /// operators selecting non-default parameters).
    pub fn with_policy(policy: PasswordHashPolicy) -> Self {
        let auth = Self::new();
        *auth.policy.write() = policy;
        auth
    }

    /// Replace the hashing policy for subsequently created credentials
    /// and migrations. Existing verifiers keep verifying (their
    /// parameters travel with the hash); they migrate lazily on next
    /// successful login.
    pub fn set_policy(&self, policy: PasswordHashPolicy) {
        *self.policy.write() = policy;
    }

    /// Current hashing policy.
    pub fn policy(&self) -> PasswordHashPolicy {
        *self.policy.read()
    }

    /// Link the shared authorization source registry serving the
    /// management API. Boot and management plane only: one `Arc` clone.
    /// After linking, every publish/subscribe verdict attributes to the
    /// built-in source counters and honours its stored enable flag and
    /// order. Unlinked state keeps today's behaviour (open when empty).
    pub fn set_authz_sources(&self, registry: Arc<AuthzSourceRegistry>) {
        self.authz_sources.store(Some(registry));
    }

    /// Link the shared authorization settings store serving the
    /// management API. Boot and management plane only: one `Arc` clone.
    /// After linking, an unmatched rule scan follows the stored
    /// `no_match` value and denials report the stored `deny_action`
    /// value. Unlinked state keeps today's behaviour (unmatched denied,
    /// denied ignored).
    pub fn set_authz_settings(&self, store: Arc<AuthzSettingsStore>) {
        self.authz_settings.store(Some(store));
    }

    /// True when an unmatched rule scan allows. Unlinked state denies.
    /// One lock-free snapshot load with no clone; called only when no
    /// rule matches, never per matched message.
    fn no_match_allows(&self) -> bool {
        self.authz_settings
            .load()
            .as_deref()
            .is_some_and(|store| store.no_match_is_allow())
    }

    /// True when a denied publish or subscribe disconnects. Unlinked
    /// state ignores. One lock-free snapshot load with no clone; called
    /// only on the denial path, never per allowed message.
    pub fn authz_deny_disconnects(&self) -> bool {
        self.authz_settings
            .load()
            .as_deref()
            .is_some_and(|store| store.deny_action_is_disconnect())
    }

    /// True when the built-in database source may execute: unlinked
    /// state keeps today's behaviour (true), linked state follows the
    /// stored registry order and enable flags via
    /// [`AuthzSourceRegistry::built_in_available`]. One lock-free
    /// snapshot load of at most [`broker_config::MAX_AUTHZ_SOURCES`]
    /// entries; stored-only sources never execute and are skipped in
    /// stored order, so the check consults both order and enable.
    fn built_in_live(&self) -> bool {
        self.authz_sources
            .load()
            .as_deref()
            .is_none_or(|registry| registry.built_in_available())
    }

    /// Attribute one built-in verdict to the shared registry counters.
    /// Unlinked state applies nothing. One short read lock to clone the
    /// counter `Arc` plus lock-free atomic adds; unknown types apply
    /// nothing and never accumulate.
    fn record_authz_verdict(&self, allowed: bool) {
        if let Some(registry) = self.authz_sources.load().as_deref() {
            registry.record_built_in(allowed);
        }
    }

    /// Attributes one built-in verdict through an already loaded source
    /// link. Lock-free: see [`AuthzSourceRegistry::record_built_in`].
    fn record_verdict(sources: Option<&AuthzSourceRegistry>, allowed: bool) {
        if let Some(registry) = sources {
            registry.record_built_in(allowed);
        }
    }

    fn digest(password: &[u8]) -> [u8; 32] {
        Sha256::digest(password).into()
    }

    /// Seed from a validated snapshot root. An empty snapshot yields
    /// today's empty behaviour (open broker, everything allowed).
    /// Memory-only: mutations are not persisted. A missing/unreadable
    /// secret fails with an error naming the reference, never the value.
    /// An invalid snapshot fails closed: the call logs the error and
    /// returns a closed store. The closed store denies every
    /// authentication and every publish or subscribe.
    pub fn from_snapshot(conf: &MqttUsersConf) -> Self {
        let auth = Self::new();
        if let Err(err) = auth.seed_from_snapshot(conf) {
            tracing::error!("invalid stored mqtt_users snapshot: {err}");
            return Self::closed();
        }
        auth
    }

    /// Seed from the registry's current snapshot and persist every later
    /// user/ACL mutation back through it. This is the kernel boot path:
    /// an empty snapshot yields today's empty behaviour. A
    /// missing/unreadable secret fails loudly naming the reference.
    /// An invalid snapshot fails closed: the call logs the error and
    /// returns a closed store. The closed store denies every
    /// authentication and every publish or subscribe.
    pub fn from_registry(registry: &Arc<ConfigRegistry>) -> Self {
        let auth = Self::new();
        if let Err(err) = auth.seed_from_snapshot(&registry.snapshot().mqtt_users) {
            tracing::error!("invalid stored mqtt_users snapshot: {err}");
            return Self::closed();
        }
        *auth.registry.write() = Some(Arc::clone(registry));
        auth
    }

    /// Closed store for a failed load. It holds one locked account and
    /// one deny rule. The open-when-empty path stays off, so every
    /// authentication and every publish or subscribe denies. The caller
    /// logs the load error.
    fn closed() -> Self {
        let auth = Self::new();
        {
            let mut tenants = auth.tenants_mut();
            let state = tenants.entry(DEFAULT_TENANT_ID.to_string()).or_default();
            state.users.insert(
                "__closed__".to_string(),
                UserEntry {
                    verifier: PasswordVerifier::Locked,
                    quotas: UserQuotas::default(),
                },
            );
            state
                .rules
                .push(AclRule::new("*", AclAction::All, "#", false));
        }
        auth
    }

    /// Attach the registry and replace the current contents with its
    /// snapshot, in place on the same instance.
    ///
    /// The kernel boot path calls this on the single shared `MemoryAuth`
    /// (serving both the BrokerLink plane and `ApiState.auth`) so no
    /// second copy can diverge. Replacing is idempotent: seeding twice
    /// from the same snapshot yields identical decisions. A
    /// missing/unreadable secret fails closed with an error naming the
    /// reference, never the value.
    pub fn seed_from_registry(
        &self,
        registry: &Arc<ConfigRegistry>,
    ) -> std::result::Result<(), broker_config::ConfigError> {
        self.seed_from_snapshot(&registry.snapshot().mqtt_users)?;
        *self.registry.write() = Some(Arc::clone(registry));
        Ok(())
    }

    /// Replace users and rules with the snapshot contents. Password
    /// verifiers decode from their stored form without re-hashing:
    /// legacy SHA-256 hex plus bcrypt, PBKDF2 and Argon2id encodings.
    /// A `file:`/`env:` secret reference resolves at use time through the
    /// secret store and the resolved password is hashed under the default
    /// policy (rotation takes effect on the next reload); a
    /// missing/unreadable secret fails closed with an error naming the
    /// reference, never the value. An undecodable verifier or unknown
    /// algorithm identifier locks that account, fail closed, instead of
    /// silently dropping the user, so the outage stays visible in
    /// `usernames()`. Quota bounds load from the record (missing bounds
    /// mean unlimited).
    /// Deny for a closed tenant (unknown, invalid, or a non-default
    /// tenant with no state): count exactly here, log once, fail closed.
    /// CONNECT/publish/subscribe paths only; never on delivery fan-out.
    fn deny_closed_tenant(&self, tenant: &str, client_id: &str, reason: &str) {
        self.tenant_denials
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tracing::warn!(
            client_id,
            tenant,
            reason,
            "mqtt tenant access refused: closed tenant, failing closed"
        );
    }

    /// Closed-tenant denials observed so far (MT-06 counter).
    pub fn tenant_denial_count(&self) -> u64 {
        self.tenant_denials
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Same-credential verifications shared so far. Each count is one
    /// CONNECT that awaited a leader check instead of hashing again.
    pub fn verify_dedup_join_count(&self) -> u64 {
        self.verify_dedup_joins
            .load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Expensive verifications run so far. Each count is one blocking
    /// hash for a non-legacy verifier. Shared bursts add one run per
    /// different credential; waiters add none.
    pub fn verify_run_count(&self) -> u64 {
        self.verify_runs.load(std::sync::atomic::Ordering::SeqCst)
    }

    /// Total users across all tenants (drives the global open-mode gate).
    /// The rule list of the default tenant, for a change. The rule
    /// tables of the management API are the rules of the default tenant.
    /// Management writes only: the publish path does not call this.
    fn default_rules_mut(&self) -> DefaultRulesGuard<'_> {
        let rules = parking_lot::RwLockWriteGuard::map(self.tenants.write(), |tenants| {
            &mut tenants
                .entry(DEFAULT_TENANT_ID.to_string())
                .or_default()
                .rules
        });
        DefaultRulesGuard {
            rules,
            flag: &self.default_rules_empty,
        }
    }

    /// Write access to the tenant tables. The guard refreshes the
    /// `default_rules_empty` flag before it releases the lock.
    fn tenants_mut(&self) -> TenantsGuard<'_> {
        TenantsGuard {
            map: self.tenants.write(),
            flag: &self.default_rules_empty,
        }
    }

    /// A copy of the rule list of the default tenant. Management reads
    /// only: the publish path does not call this.
    fn default_rules(&self) -> Vec<AclRule> {
        self.tenants
            .read()
            .get(DEFAULT_TENANT_ID)
            .map(|state| state.rules.clone())
            .unwrap_or_default()
    }

    fn total_user_count(&self) -> usize {
        self.tenants.read().values().map(|s| s.users.len()).sum()
    }

    /// Users in one tenant (0 for unknown tenants).
    pub fn user_count_in_tenant(&self, tenant: &str) -> usize {
        let key = tenant_key(tenant);
        self.tenants
            .read()
            .get(key)
            .map(|s| s.users.len())
            .unwrap_or(0)
    }

    /// Rules in one tenant (0 for unknown tenants). Management-plane only.
    pub fn rule_count_in_tenant(&self, tenant: &str) -> usize {
        let key = tenant_key(tenant);
        self.tenants
            .read()
            .get(key)
            .map(|s| s.rules.len())
            .unwrap_or(0)
    }

    fn seed_from_snapshot(
        &self,
        conf: &MqttUsersConf,
    ) -> std::result::Result<(), broker_config::ConfigError> {
        // Build every table before swapping, so a bad write leaves the
        // running state unchanged. The default tenant comes from the root
        // fields (pre-change files carry only these and load as the
        // default tenant with the same passwords and rules); each
        // `tenants` entry becomes one non-default table.
        let default_state = self.build_tenant_state(&conf.users, &conf.acls)?;
        if conf.tenants.len() + 1 > MAX_AUTH_TENANTS {
            return Err(broker_config::ConfigError::Invalid(format!(
                "mqtt_users holds {} tenants, at most {MAX_AUTH_TENANTS} (field `tenants`)",
                conf.tenants.len() + 1,
            )));
        }
        let mut total_users = default_state.users.len();
        let mut next: HashMap<String, TenantState> = HashMap::with_capacity(conf.tenants.len() + 1);
        for (index, table) in conf.tenants.iter().enumerate() {
            if !is_valid_tenant_id(&table.tenant) {
                return Err(broker_config::ConfigError::Invalid(format!(
                    "mqtt_users.tenants[{index}].tenant {:?} is not a valid tenant id (field `tenant`)",
                    table.tenant
                )));
            }
            if is_default_tenant(&table.tenant) {
                return Err(broker_config::ConfigError::Invalid(format!(
                    "mqtt_users.tenants[{index}].tenant {:?} must live in the root users/acls fields (field `tenant`)",
                    table.tenant
                )));
            }
            if next.contains_key(&table.tenant) {
                return Err(broker_config::ConfigError::Invalid(format!(
                    "mqtt_users.tenants[{index}].tenant {:?} is duplicated (field `tenant`)",
                    table.tenant
                )));
            }
            let state = self.build_tenant_state(&table.users, &table.acls)?;
            if state.rules.len() > MAX_ACL_RULES_PER_TENANT {
                return Err(broker_config::ConfigError::Invalid(format!(
                    "mqtt_users.tenants[{index}].acls holds {} entries, at most {MAX_ACL_RULES_PER_TENANT} (field `acls`)",
                    state.rules.len(),
                )));
            }
            total_users += state.users.len();
            next.insert(table.tenant.clone(), state);
        }
        if default_state.rules.len() > MAX_ACL_RULES_PER_TENANT {
            return Err(broker_config::ConfigError::Invalid(format!(
                "mqtt_users.acls holds {} entries, at most {MAX_ACL_RULES_PER_TENANT} (field `acls`)",
                default_state.rules.len(),
            )));
        }
        if total_users > MAX_AUTHN_USERS {
            return Err(broker_config::ConfigError::Invalid(format!(
                "mqtt_users holds {total_users} users, at most {MAX_AUTHN_USERS} (field `users`)"
            )));
        }
        // Replace every tenant at once: the snapshot is the full truth,
        // so a reload keeps exactly the tenants it carries (a restart
        // reads the same file and lands in the same state). A table that
        // is empty on both sides stays absent: unknown and empty tenants
        // both deny (closed, counted, logged).
        // A reload prunes tenants absent from disk.
        // The snapshot is the full truth.
        next.insert(DEFAULT_TENANT_ID.to_string(), default_state);
        *self.tenants_mut() = next;
        Ok(())
    }

    /// Build one tenant table from its snapshot rows. Secret references
    /// resolve at use time (a missing secret fails closed with an error
    /// naming the reference, never the value); undecodable verifiers lock
    /// the account (fail closed, still listed). Unknown ACL actions are
    /// skipped (deny-safe: no rule means denied) rather than widened.
    fn build_tenant_state(
        &self,
        users: &[broker_config::MqttUser],
        acls: &[broker_config::AclConf],
    ) -> std::result::Result<TenantState, broker_config::ConfigError> {
        let mut state = TenantState {
            users: HashMap::with_capacity(users.len()),
            rules: Vec::with_capacity(acls.len()),
        };
        for entry in users {
            let trimmed = entry.password_hash.trim();
            let verifier = if broker_config::secrets::is_secret_ref(trimmed) {
                // Fail closed: propagate the resolve error (it names the
                // reference, never the value) so reload answers with an
                // error instead of 200 with a locked account.
                let secret = broker_config::secrets::resolve_secret(trimmed)?;
                let mut secret = secret;
                let policy = self.policy();
                let hashed = hash_password_for_policy(&policy, &secret, policy.default_algorithm)
                    .unwrap_or(PasswordVerifier::Locked);
                zero_bytes(&mut secret);
                hashed
            } else {
                match parse_stored_verifier(&entry.password_hash) {
                    Some(verifier) => verifier,
                    None => {
                        // Fail closed and stay visible: log once per account
                        // so a hand-edited state file cannot silently open or
                        // vanish an account.
                        tracing::warn!(
                            username = entry.username.as_str(),
                            "mqtt snapshot entry has an undecodable password verifier; account locked until its password is changed"
                        );
                        PasswordVerifier::Locked
                    }
                }
            };
            state.users.insert(
                entry.username.clone(),
                UserEntry {
                    verifier,
                    quotas: UserQuotas {
                        max_connections: entry.max_connections,
                        max_publish_rate: entry.max_publish_rate,
                        max_publish_burst: entry.max_publish_burst,
                    },
                },
            );
        }
        // Every entry reaching here through the registry is validated
        // (`MqttUsersConf::validate` rejects unknown actions at commit
        // and load), and `export_conf` only writes canonical spellings,
        // so `parse` succeeds on every round-trip. An unvalidated caller
        // passing an unknown action gets that entry skipped (deny-safe:
        // no rule means denied) rather than widened into `All`.
        state.rules = acls
            .iter()
            .filter_map(|entry| {
                AclAction::parse(&entry.action).map(|action| {
                    AclRule::new(
                        entry.username.clone(),
                        action,
                        entry.topic.clone(),
                        entry.allow,
                    )
                })
            })
            .collect();
        Ok(state)
    }

    /// Export the current contents as a validated config root: the
    /// default tenant in the root fields plus one table per non-default
    /// tenant, users sorted by username so the persisted file is
    /// deterministic, tables sorted by tenant id, rules in evaluation order
    /// (first match wins, so order is significant). Passwords export in
    /// their stored verifier encoding (legacy SHA-256 hex round-trips
    /// until migration, then the new PHC-style encoding; never plaintext);
    /// quota bounds export alongside the user record they belong to. Every
    /// table persists, so a restart and a reload keep all tenants.
    fn export_conf(&self) -> MqttUsersConf {
        let tenants = self.tenants.read();
        let state = tenants.get(DEFAULT_TENANT_ID);
        let mut entries: Vec<MqttUser> = state
            .map(|s| {
                s.users
                    .iter()
                    .map(|(username, entry)| MqttUser {
                        username: username.clone(),
                        password_hash: encode_stored_verifier(&entry.verifier),
                        max_connections: entry.quotas.max_connections,
                        max_publish_rate: entry.quotas.max_publish_rate,
                        max_publish_burst: entry.quotas.max_publish_burst,
                    })
                    .collect()
            })
            .unwrap_or_default();
        entries.sort_by(|a, b| a.username.cmp(&b.username));
        let acls = state
            .map(|s| {
                s.rules
                    .iter()
                    .map(|rule| AclConf {
                        username: rule.client_pattern.clone(),
                        topic: rule.topic_pattern.clone(),
                        action: action_to_conf(rule.action).to_string(),
                        allow: rule.allow,
                    })
                    .collect()
            })
            .unwrap_or_default();
        // One table per non-default tenant with users or rules. Tables
        // that are empty on both sides stay absent: unknown and empty
        // tenants both deny (closed, counted, logged).
        let mut tables: Vec<broker_config::TenantMqttConf> = tenants
            .iter()
            .filter(|(id, state)| {
                *id != DEFAULT_TENANT_ID && (!state.users.is_empty() || !state.rules.is_empty())
            })
            .map(|(id, state)| {
                let mut users: Vec<MqttUser> = state
                    .users
                    .iter()
                    .map(|(username, entry)| MqttUser {
                        username: username.clone(),
                        password_hash: encode_stored_verifier(&entry.verifier),
                        max_connections: entry.quotas.max_connections,
                        max_publish_rate: entry.quotas.max_publish_rate,
                        max_publish_burst: entry.quotas.max_publish_burst,
                    })
                    .collect();
                users.sort_by(|a, b| a.username.cmp(&b.username));
                let acls: Vec<AclConf> = state
                    .rules
                    .iter()
                    .map(|rule| AclConf {
                        username: rule.client_pattern.clone(),
                        topic: rule.topic_pattern.clone(),
                        action: action_to_conf(rule.action).to_string(),
                        allow: rule.allow,
                    })
                    .collect();
                broker_config::TenantMqttConf {
                    tenant: id.clone(),
                    users,
                    acls,
                }
            })
            .collect();
        tables.sort_by(|a, b| a.tenant.cmp(&b.tenant));
        MqttUsersConf {
            users: entries,
            acls,
            tenants: tables,
        }
    }

    /// Commit the exported root and atomically save it. A no-op without
    /// a registry; any commit or save failure surfaces as
    /// [`broker_config::ConfigError`] so callers answer 500 and the loss
    /// is never silent.
    fn persist(&self) -> std::result::Result<(), broker_config::ConfigError> {
        let Some(registry) = self.registry.read().clone() else {
            return Ok(());
        };
        let _guard = self.save_lock.lock();
        let conf = self.export_conf();
        registry.commit_mqtt_users(conf)?;
        registry.save()?;
        Ok(())
    }

    /// Store (or replace) a username with its password verifier under the
    /// configured default algorithm ([`PasswordHashPolicy::default`] is
    /// Argon2id). Existing quotas survive a password change. Hashing runs
    /// inline: creation is management-plane (infrequent), while
    /// verification runs off the accept path. Persists through the
    /// registry when one is attached; a save failure is returned and the
    /// in-memory update stays (commit precedes the atomic save). An
    /// oversized password or a hashing failure fails closed without
    /// storing anything.
    ///
    /// The password may be a secret reference (`file:/path`, `env:NAME`)
    /// resolved at use time; resolution failures fail closed with an error
    /// naming the reference, never the value. Plaintext passwords never
    /// reach the persisted snapshot (only the digest does) and never
    /// appear in logs or API reads.
    pub fn add_user(
        &self,
        username: impl Into<String>,
        password: &[u8],
    ) -> std::result::Result<(), broker_config::ConfigError> {
        let policy = self.policy();
        self.add_user_with_algorithm(username, password, policy.default_algorithm)
    }

    /// Store (or replace) a username in one tenant (MT-06). The default
    /// tenant behaves exactly like [`MemoryAuth::add_user`]; every tenant
    /// persists (TM-01) through the registry. Creating a new tenant past
    /// [`MAX_AUTH_TENANTS`] is rejected instead of growing without limit.
    pub fn add_user_in_tenant(
        &self,
        tenant: &str,
        username: impl Into<String>,
        password: &[u8],
    ) -> std::result::Result<(), broker_config::ConfigError> {
        let policy = self.policy();
        self.add_user_with_algorithm_in_tenant(tenant, username, password, policy.default_algorithm)
    }

    /// Store (or replace) a username in one tenant with an explicit
    /// algorithm (MT-06). Same per-tenant persistence as
    /// [`MemoryAuth::add_user_in_tenant`].
    pub fn add_user_with_algorithm_in_tenant(
        &self,
        tenant: &str,
        username: impl Into<String>,
        password: &[u8],
        algorithm: PasswordAlgorithm,
    ) -> std::result::Result<(), broker_config::ConfigError> {
        let key = tenant_key(tenant);
        if !is_default_tenant(key) && !is_valid_tenant_id(key) {
            self.deny_closed_tenant(key, "", "invalid tenant id at user creation");
            return Err(broker_config::ConfigError::Invalid(format!(
                "mqtt_users tenant {key:?} is not a valid tenant id (field `tenant`)"
            )));
        }
        let mut password = resolve_password_bytes(password)?;
        let username = username.into();
        if password.len() > MAX_PASSWORD_BYTES {
            tracing::warn!(
                username = username.as_str(),
                "mqtt user creation refused: password exceeds bound"
            );
            return Err(broker_config::ConfigError::Invalid(format!(
                "mqtt_users.users password for {username:?} exceeds {MAX_PASSWORD_BYTES} bytes (field `password_hash`)"
            )));
        }
        let policy = self.policy();
        let verifier = hash_password_for_policy(&policy, &password, algorithm).ok_or_else(|| {
            broker_config::ConfigError::Invalid(format!(
                "mqtt_users.users password for {username:?} cannot be hashed with {algorithm:?} (field `password_hash`)"
            ))
        })?;
        {
            let mut tenants = self.tenants_mut();
            if !tenants.contains_key(key) {
                if tenants.len() >= MAX_AUTH_TENANTS {
                    return Err(broker_config::ConfigError::Invalid(format!(
                        "mqtt_users holds {} tenants, at most {MAX_AUTH_TENANTS} (field `tenant`)",
                        tenants.len()
                    )));
                }
                if self.total_user_count_locked(&tenants) + 1 > MAX_AUTHN_USERS {
                    return Err(broker_config::ConfigError::Invalid(format!(
                        "mqtt_users holds too many users for another tenant (at most {MAX_AUTHN_USERS} total)"
                    )));
                }
                tenants.insert(key.to_string(), TenantState::default());
            }
            if self.total_user_count_locked(&tenants)
                + usize::from(
                    !tenants
                        .get(key)
                        .is_some_and(|state| state.users.contains_key(&username)),
                )
                > MAX_AUTHN_USERS
            {
                return Err(broker_config::ConfigError::Invalid(format!(
                    "mqtt_users holds too many users (at most {MAX_AUTHN_USERS} total)"
                )));
            }
            let state = tenants.get_mut(key).expect("tenant just inserted");
            let quotas = state
                .users
                .get(&username)
                .map(|entry| entry.quotas.clone())
                .unwrap_or_default();
            state.users.insert(username, UserEntry { verifier, quotas });
        }
        zero_bytes(&mut password);
        // Every tenant persists (TM-01): the exported root carries all
        // tenants, so each mutation commits and saves.
        self.persist()?;
        Ok(())
    }

    /// Total users across all tenants under an already-held tenant lock.
    fn total_user_count_locked(&self, tenants: &HashMap<String, TenantState>) -> usize {
        tenants.values().map(|s| s.users.len()).sum()
    }

    /// Store (or replace) a username with an explicit algorithm, using the
    /// current policy's parameters. Used by tests covering each algorithm
    /// and by operators pinning an entry during migration. Same
    /// persistence and fail-closed behaviour as [`MemoryAuth::add_user`].
    pub fn add_user_with_algorithm(
        &self,
        username: impl Into<String>,
        password: &[u8],
        algorithm: PasswordAlgorithm,
    ) -> std::result::Result<(), broker_config::ConfigError> {
        // Default-tenant shim: single-tenant installs behave exactly as
        // before (same passwords, same persistence).
        self.add_user_with_algorithm_in_tenant(DEFAULT_TENANT_ID, username, password, algorithm)
    }

    /// Bulk import: validate-all-before-apply over the live user map.
    ///
    /// Every entry is a `(username, password)` pair with a plaintext
    /// password that is hashed here under the configured default
    /// algorithm (same policy as [`MemoryAuth::add_user`]).
    ///
    /// Validate-all-before-apply: usernames must be non-empty (at most
    /// [`MAX_AUTHN_USERNAME_LEN`] bytes) and unique within the batch,
    /// passwords must be non-empty and at most [`MAX_PASSWORD_BYTES`]
    /// bytes, the batch must be non-empty and at most
    /// [`MAX_IMPORT_BATCH`] entries, no entry may collide with the
    /// existing map, and the batch must fit under [`MAX_AUTHN_USERS`].
    /// Any violation rejects the whole batch with [`ImportUsersError`]
    /// and applies nothing, so a bad batch can never partially corrupt
    /// the store. A duplicate within the batch or against the existing
    /// map is an error (never a silent overwrite or merge).
    // TODO(parity): is a duplicate an error or an overwrite/merge, and
    // does the documented result count overrides versus fail the batch?
    // The rulebook does not decide the exact duplicate semantics; the
    // current choice rejects the whole batch as the conservative
    // fail-closed answer until the checker pins it.
    ///
    /// On success every user is inserted under a single write lock and
    /// persisted once through the registry, so the import survives a
    /// kernel restart. Management-plane only: takes only the user-map
    /// write lock (plus the registry save lock inside [`MemoryAuth::persist`]);
    /// the CONNECT path keeps reading under short read locks and
    /// publish/deliver never touch this store.
    ///
    /// The caller holds at most the validated batch (one `(username,
    /// verifier)` pair per entry) plus the request body itself; no
    /// second full copy of the batch is built.
    pub fn import_users(
        &self,
        users: Vec<(String, Vec<u8>)>,
    ) -> std::result::Result<ImportSummary, ImportUsersError> {
        if users.is_empty() {
            return Err(ImportUsersError::Malformed(
                "user batch must not be empty".to_string(),
            ));
        }
        if users.len() > MAX_IMPORT_BATCH {
            return Err(ImportUsersError::TooMany(format!(
                "user batch of {} exceeds {MAX_IMPORT_BATCH} entries",
                users.len()
            )));
        }
        // Validate shape before any hashing: non-empty usernames and
        // passwords within bounds, no duplicate within the batch.
        {
            let mut seen = std::collections::HashSet::with_capacity(users.len());
            for (username, password) in &users {
                if username.trim().is_empty() {
                    return Err(ImportUsersError::Malformed(
                        "user batch entry `user_id` must not be empty".to_string(),
                    ));
                }
                if username.len() > MAX_AUTHN_USERNAME_LEN {
                    return Err(ImportUsersError::Malformed(format!(
                        "user batch entry `user_id` {username:?} exceeds {MAX_AUTHN_USERNAME_LEN} bytes"
                    )));
                }
                if password.is_empty() {
                    return Err(ImportUsersError::Malformed(format!(
                        "user batch entry for {username:?} has an empty password (field `password`)"
                    )));
                }
                if password.len() > MAX_PASSWORD_BYTES {
                    return Err(ImportUsersError::Malformed(format!(
                        "user batch entry for {username:?} exceeds {MAX_PASSWORD_BYTES} bytes (field `password`)"
                    )));
                }
                if !seen.insert(username.clone()) {
                    return Err(ImportUsersError::Duplicate(format!(
                        "user batch entry `user_id` {username:?} is duplicated"
                    )));
                }
            }
        }
        // Fail fast against the default tenant's map under a short read
        // lock before paying for hashing; the write-lock section below
        // re-checks so a concurrent import cannot slip in between.
        // Imports land in the default tenant (per-tenant import is MT-07).
        {
            let tenants = self.tenants.read();
            if let Some(state) = tenants.get(DEFAULT_TENANT_ID) {
                for (username, _) in &users {
                    if state.users.contains_key(username) {
                        return Err(ImportUsersError::Duplicate(format!(
                            "user {username:?} already exists"
                        )));
                    }
                }
            }
            if self.total_user_count_locked(&tenants) + users.len() > MAX_AUTHN_USERS {
                return Err(ImportUsersError::TooMany(format!(
                    "import of {} users would exceed the per-authenticator cap of {MAX_AUTHN_USERS}",
                    users.len()
                )));
            }
        }
        // Hash outside the write lock (management-plane cost only; the
        // CONNECT path never waits on it), one verifier per entry.
        let policy = self.policy();
        let algorithm = policy.default_algorithm;
        let mut hashed: Vec<(String, PasswordVerifier)> = Vec::with_capacity(users.len());
        for (username, password) in &users {
            match hash_password_for_policy(&policy, password, algorithm) {
                Some(verifier) => hashed.push((username.clone(), verifier)),
                None => {
                    return Err(ImportUsersError::Malformed(format!(
                        "user batch entry for {username:?} cannot be hashed with {algorithm:?} (field `password`)"
                    )));
                }
            }
        }
        // Single atomic apply under one write lock: re-check duplicates
        // and the cap so concurrent imports cannot interleave into a
        // partial apply, then insert all and persist once.
        {
            let mut tenants = self.tenants_mut();
            let total = self.total_user_count_locked(&tenants);
            let state = tenants.entry(DEFAULT_TENANT_ID.to_string()).or_default();
            for (username, _) in &hashed {
                if state.users.contains_key(username) {
                    return Err(ImportUsersError::Duplicate(format!(
                        "user {username:?} already exists"
                    )));
                }
            }
            if total + hashed.len() > MAX_AUTHN_USERS {
                return Err(ImportUsersError::TooMany(format!(
                    "import of {} users would exceed the per-authenticator cap of {MAX_AUTHN_USERS}",
                    hashed.len()
                )));
            }
            for (username, verifier) in hashed {
                state.users.insert(
                    username,
                    UserEntry {
                        verifier,
                        quotas: UserQuotas::default(),
                    },
                );
            }
        }
        if let Err(error) = self.persist() {
            return Err(ImportUsersError::Persist(error.to_string()));
        }
        let total = users.len();
        Ok(ImportSummary {
            total,
            success: total,
        })
    }

    /// Stored verifier encoding for a user (`None` when unknown). Test and
    /// operator visibility only: proves migration changed the scheme
    /// without exposing password material beyond the hash itself.
    pub fn verifier_string(&self, username: &str) -> Option<String> {
        self.verifier_string_in_tenant(DEFAULT_TENANT_ID, username)
    }

    /// Stored verifier encoding for a user in one tenant (MT-06).
    pub fn verifier_string_in_tenant(&self, tenant: &str, username: &str) -> Option<String> {
        let key = tenant_key(tenant);
        self.tenants
            .read()
            .get(key)
            .and_then(|state| state.users.get(username))
            .map(|entry| encode_stored_verifier(&entry.verifier))
    }

    /// Replace users and ACLs with one validated snapshot root (M1-05
    /// runtime apply path for the `mqtt_users` root).
    ///
    /// Called by the management reload path after whole-config validation
    /// and by boot; driven by CONNECT-time authentication afterwards (each
    /// CONNECT consults this store). Memory-only when no registry is
    /// attached.
    pub fn apply_snapshot_conf(
        &self,
        conf: &MqttUsersConf,
    ) -> std::result::Result<(), broker_config::ConfigError> {
        conf.validate()?;
        self.seed_from_snapshot(conf)?;
        Ok(())
    }

    /// Remove a user (false when unknown; unknown names persist nothing).
    /// Persists through the registry when one is attached.
    pub fn remove_user(
        &self,
        username: &str,
    ) -> std::result::Result<bool, broker_config::ConfigError> {
        self.remove_user_in_tenant(DEFAULT_TENANT_ID, username)
    }

    /// Remove a user in one tenant (MT-06). Every tenant persists
    /// (TM-01) through the registry.
    pub fn remove_user_in_tenant(
        &self,
        tenant: &str,
        username: &str,
    ) -> std::result::Result<bool, broker_config::ConfigError> {
        let key = tenant_key(tenant);
        let removed = self
            .tenants
            .write()
            .get_mut(key)
            .is_some_and(|state| state.users.remove(username).is_some());
        if removed {
            self.persist()?;
        }
        Ok(removed)
    }

    /// Total users across all tenants (drives the global open-mode gate:
    /// anonymous CONNECT is refused while any tenant holds users).
    pub fn user_count(&self) -> usize {
        self.total_user_count()
    }

    /// Whether `username` has a local record (one read lock, no
    /// allocation). The publish path uses this to decide between the
    /// local ACL and the database ACL without cloning the user list.
    pub fn has_user(&self, username: &str) -> bool {
        self.has_user_in_tenant(DEFAULT_TENANT_ID, username)
    }

    /// Whether `username` has a local record in one tenant (MT-06): one
    /// read lock, no allocation. The publish path calls this with the
    /// publisher's own tenant so a name in another tenant never selects
    /// the local ACL for this tenant.
    pub fn has_user_in_tenant(&self, tenant: &str, username: &str) -> bool {
        let key = tenant_key(tenant);
        self.tenants
            .read()
            .get(key)
            .is_some_and(|state| state.users.contains_key(username))
    }

    /// Attach quota bounds to an existing user (`Ok(false)` when
    /// unknown; unknown names persist nothing). Persists through the
    /// registry when one is attached, so a configured quota survives a
    /// restart.
    pub fn set_quotas(
        &self,
        username: &str,
        quotas: UserQuotas,
    ) -> std::result::Result<bool, broker_config::ConfigError> {
        self.set_quotas_in_tenant(DEFAULT_TENANT_ID, username, quotas)
    }

    /// Attach quota bounds to an existing user in one tenant (MT-06).
    /// Every tenant persists (TM-01) through the registry.
    pub fn set_quotas_in_tenant(
        &self,
        tenant: &str,
        username: &str,
        quotas: UserQuotas,
    ) -> std::result::Result<bool, broker_config::ConfigError> {
        let key = tenant_key(tenant);
        match self
            .tenants
            .write()
            .get_mut(key)
            .and_then(|state| state.users.get_mut(username))
        {
            Some(entry) => {
                entry.quotas = quotas;
            }
            None => return Ok(false),
        }
        self.persist()?;
        Ok(true)
    }

    /// Quota bounds for a user (`None` when unknown).
    pub fn get_quotas(&self, username: &str) -> Option<UserQuotas> {
        self.get_quotas_in_tenant(DEFAULT_TENANT_ID, username)
    }

    /// Quota bounds for a user in one tenant (MT-06; `None` when unknown
    /// or in a closed tenant). The CONNECT quota check calls this with
    /// the bind's own tenant so one tenant's limits never gate another.
    pub fn get_quotas_in_tenant(&self, tenant: &str, username: &str) -> Option<UserQuotas> {
        let key = tenant_key(tenant);
        self.tenants
            .read()
            .get(key)
            .and_then(|state| state.users.get(username))
            .map(|entry| entry.quotas.clone())
    }

    /// Sorted usernames (passwords are write-only, never listed).
    pub fn usernames(&self) -> Vec<String> {
        self.usernames_in_tenant(DEFAULT_TENANT_ID)
    }

    /// Sorted usernames in one tenant (MT-06).
    pub fn usernames_in_tenant(&self, tenant: &str) -> Vec<String> {
        let key = tenant_key(tenant);
        let mut names: Vec<String> = self
            .tenants
            .read()
            .get(key)
            .map(|state| state.users.keys().cloned().collect())
            .unwrap_or_default();
        names.sort();
        names
    }

    /// Every live user as `(tenant, username)` (TM-01): tenants sorted,
    /// then names sorted. Management-plane export only: clones at most
    /// the capped user set under one short read lock; the CONNECT path
    /// keeps reading under its own short locks and publish/deliver never
    /// touch this store.
    pub fn all_users_with_tenants(&self) -> Vec<(String, String)> {
        let tenants = self.tenants.read();
        let mut ids: Vec<&String> = tenants.keys().collect();
        ids.sort();
        let mut out = Vec::new();
        for id in ids {
            let mut names: Vec<&String> = tenants
                .get(id)
                .map(|state| state.users.keys().collect())
                .unwrap_or_default();
            names.sort();
            for name in names {
                out.push((id.clone(), name.clone()));
            }
        }
        out
    }

    /// Every live ACL rule as `(tenant, rule)` (TM-01): tenants sorted,
    /// rules in evaluation order (first match wins, so order is
    /// significant). Management-plane export only: same lock discipline
    /// as [`MemoryAuth::all_users_with_tenants`].
    pub fn all_rules_with_tenants(&self) -> Vec<(String, AclRule)> {
        let tenants = self.tenants.read();
        let mut ids: Vec<&String> = tenants.keys().collect();
        ids.sort();
        let mut out = Vec::new();
        for id in ids {
            if let Some(state) = tenants.get(id) {
                for rule in state.rules.iter() {
                    out.push((id.clone(), rule.clone()));
                }
            }
        }
        out
    }

    /// Append an ACL rule (first match wins). Persists through the
    /// registry when one is attached.
    pub fn add_rule(&self, rule: AclRule) -> std::result::Result<(), broker_config::ConfigError> {
        self.add_rule_in_tenant(DEFAULT_TENANT_ID, rule)
    }

    /// Append an ACL rule in one tenant (MT-06): first match wins within
    /// the tenant; a rule in one tenant never grants in another. Every
    /// tenant persists (TM-01) through the registry. Writes past
    /// [`MAX_ACL_RULES_PER_TENANT`] are rejected instead of growing the
    /// per-check scan without limit.
    pub fn add_rule_in_tenant(
        &self,
        tenant: &str,
        rule: AclRule,
    ) -> std::result::Result<(), broker_config::ConfigError> {
        let key = tenant_key(tenant);
        if !is_default_tenant(key) && !is_valid_tenant_id(key) {
            self.deny_closed_tenant(
                key,
                &rule.client_pattern,
                "invalid tenant id at rule creation",
            );
            return Err(broker_config::ConfigError::Invalid(format!(
                "mqtt_users tenant {key:?} is not a valid tenant id (field `tenant`)"
            )));
        }
        {
            let mut tenants = self.tenants_mut();
            if !tenants.contains_key(key) {
                if tenants.len() >= MAX_AUTH_TENANTS {
                    return Err(broker_config::ConfigError::Invalid(format!(
                        "mqtt_users holds {} tenants, at most {MAX_AUTH_TENANTS} (field `tenant`)",
                        tenants.len()
                    )));
                }
                tenants.insert(key.to_string(), TenantState::default());
            }
            let state = tenants.get_mut(key).expect("tenant just inserted");
            if state.rules.len() >= MAX_ACL_RULES_PER_TENANT {
                return Err(broker_config::ConfigError::Invalid(format!(
                    "mqtt_users tenant {key:?} holds {} rules, at most {MAX_ACL_RULES_PER_TENANT} (field `acls`)",
                    state.rules.len()
                )));
            }
            state.rules.push(rule);
        }
        // Every tenant persists (TM-01): the exported root carries all
        // tenants, so each mutation commits and saves.
        self.persist()?;
        Ok(())
    }

    /// Ordered snapshot of the ACL for management display.
    pub fn acl_rules(&self) -> Vec<AclRule> {
        self.acl_rules_in_tenant(DEFAULT_TENANT_ID)
    }

    /// Ordered snapshot of one tenant's ACL (MT-06).
    pub fn acl_rules_in_tenant(&self, tenant: &str) -> Vec<AclRule> {
        let key = tenant_key(tenant);
        self.tenants
            .read()
            .get(key)
            .map(|state| state.rules.clone())
            .unwrap_or_default()
    }

    /// Replace the whole catch-all rule table (first match wins).
    ///
    /// Validate-all-before-apply: the length cap and every entry are
    /// checked before anything becomes visible, so a bad batch applies
    /// nothing. Entries must carry a non-empty client pattern and a
    /// non-empty topic filter that parses as an MQTT filter; unknown
    /// shapes fail closed instead of widening into an allow. Persists
    /// through the registry when one is attached, so the list survives a
    /// kernel restart.
    ///
    /// Management-plane only: one short write lock per request (plus the
    /// registry save lock on persist); the publish and subscribe
    /// authorization consults keep reading under short read locks and
    /// never take this lock beyond their check.
    pub fn replace_rules(
        &self,
        rules: Vec<AclRule>,
    ) -> std::result::Result<(), broker_config::ConfigError> {
        if rules.len() > MAX_AUTHZ_CATCH_ALL_RULES {
            return Err(broker_config::ConfigError::Invalid(format!(
                "mqtt_users.acls holds {} entries, at most {} (field `rules`)",
                rules.len(),
                MAX_AUTHZ_CATCH_ALL_RULES
            )));
        }
        for (index, rule) in rules.iter().enumerate() {
            if rule.client_pattern.trim().is_empty() {
                return Err(broker_config::ConfigError::Invalid(format!(
                    "mqtt_users.acls[{index}].username must not be empty (field `username`)"
                )));
            }
            if rule.topic_pattern.trim().is_empty() {
                return Err(broker_config::ConfigError::Invalid(format!(
                    "mqtt_users.acls[{index}].topic must not be empty (field `topic`)"
                )));
            }
            if TopicFilter::new(rule.topic_pattern.clone()).is_err() {
                return Err(broker_config::ConfigError::Invalid(format!(
                    "mqtt_users.acls[{index}].topic {:?} is not a valid filter (field `topic`)",
                    rule.topic_pattern
                )));
            }
        }
        *self.default_rules_mut() = rules;
        self.persist()
    }

    /// Drop every ACL rule. Persists through the registry when one is
    /// attached (a no-op on an already-empty list persists nothing).
    pub fn clear_rules(&self) -> std::result::Result<(), broker_config::ConfigError> {
        self.clear_rules_in_tenant(DEFAULT_TENANT_ID)
    }

    /// Drop every ACL rule in one tenant (MT-06). Every tenant persists
    /// (TM-01) through the registry.
    pub fn clear_rules_in_tenant(
        &self,
        tenant: &str,
    ) -> std::result::Result<(), broker_config::ConfigError> {
        let key = tenant_key(tenant);
        let was_nonempty = {
            let mut tenants = self.tenants_mut();
            let Some(state) = tenants.get_mut(key) else {
                return Ok(());
            };
            if state.rules.is_empty() {
                return Ok(());
            }
            state.rules.clear();
            true
        };
        if was_nonempty {
            self.persist()?;
        }
        Ok(())
    }

    /// Remove the rule at `index` (false when out of bounds; out of
    /// bounds persists nothing). Persists through the registry when one
    /// is attached.
    pub fn remove_rule(
        &self,
        index: usize,
    ) -> std::result::Result<bool, broker_config::ConfigError> {
        self.remove_rule_in_tenant(DEFAULT_TENANT_ID, index)
    }

    /// Remove the rule at `index` in one tenant (MT-06). Every tenant
    /// persists (TM-01) through the registry.
    pub fn remove_rule_in_tenant(
        &self,
        tenant: &str,
        index: usize,
    ) -> std::result::Result<bool, broker_config::ConfigError> {
        let key = tenant_key(tenant);
        let removed = {
            let mut tenants = self.tenants_mut();
            let Some(state) = tenants.get_mut(key) else {
                return Ok(false);
            };
            if index < state.rules.len() {
                state.rules.remove(index);
                true
            } else {
                return Ok(false);
            }
        };
        if removed {
            self.persist()?;
        }
        Ok(removed)
    }

    /// Per-tenant first-match-wins check (MT-06): the default tenant with
    /// no rules stays open (today's behaviour); a non-default tenant with
    /// no rules denies (closed, counted, logged); an invalid tenant
    /// denies the same way. When rules exist and none matches, the stored
    /// `no_match` setting decides (deny when unlinked). Reads run under
    /// the single tenant-map guard the caller already holds, plus one
    /// lock-free settings load only on the unmatched path. The work is
    /// one map lookup plus a bounded linear scan of at most
    /// [`MAX_ACL_RULES_PER_TENANT`] entries, so matched messages take no
    /// new lock and no allocation beyond the caller's clones.
    fn check_in_tenant(
        &self,
        tenants: &HashMap<String, TenantState>,
        tenant: &str,
        client_id: &str,
        action: AclAction,
        matches_rule: impl Fn(&AclRule) -> bool,
    ) -> bool {
        let key = tenant_key(tenant);
        // Global open broker (MT-06): while no tenant holds users or rules
        // the broker behaves exactly as today (open) in every tenant, so
        // pre-tenancy isolation tests (MT-02..MT-05) keep working. Once any
        // tenant is provisioned, a non-default tenant with no state is
        // closed (deny). Reason: an empty store cannot distinguish tenants;
        // closing only after provisioning preserves today's open-when-empty
        // while new tenants still start closed.
        let globally_empty = || {
            tenants
                .values()
                .all(|s| s.users.is_empty() && s.rules.is_empty())
        };
        let Some(state) = tenants.get(key) else {
            // No state for this tenant yet: the default tenant keeps
            // today's open-when-no-rules behaviour, every other tenant is
            // closed (deny) unless the whole store is still empty (open
            // broker). Unknown non-default tenants are counted by
            // the caller, not here (this helper takes no atomic itself;
            // the settings load below runs only on the unmatched path).
            return is_default_tenant(key) || globally_empty();
        };
        if state.rules.is_empty() {
            return is_default_tenant(key) || globally_empty();
        }
        for rule in state.rules.iter() {
            if rule.client_matches(client_id) && rule.action.covers(action) && matches_rule(rule) {
                return rule.allow;
            }
        }
        // Rules exist and none matches: the stored setting decides.
        self.no_match_allows()
    }

    /// Ordered snapshot of the per-client rule index: every stored rule
    /// scoped to a concrete client id (the catch-all `*` entries belong
    /// to the `/rules/all` table and are excluded here), in stored
    /// evaluation order. Management-plane read only: clones at most the
    /// capped list under one short read lock; the publish and subscribe
    /// authorization consults keep reading under their own short locks.
    // TODO(parity): does the documented per-client listing include the
    // catch-all `*` entries or only client-scoped ones? The rulebook does
    // not decide the exact membership; the current choice lists only
    // client-scoped entries until the checker pins it.
    pub fn all_client_rules(&self) -> Vec<AclRule> {
        self.default_rules()
            .iter()
            .filter(|rule| rule.client_pattern != "*")
            .cloned()
            .collect()
    }

    /// Ordered snapshot of one client's rules (`None` never: an empty
    /// client reads as an empty list; callers map empty to 404 where the
    /// documented shape requires it). Exact id match only: wildcard
    /// patterns belong to their literal spelling, never to the clients
    /// they could match. Management-plane read only.
    pub fn client_rules(&self, clientid: &str) -> Vec<AclRule> {
        self.default_rules()
            .iter()
            .filter(|rule| rule.client_pattern == clientid)
            .cloned()
            .collect()
    }

    /// Append one client-scoped rule (first match wins).
    ///
    /// Validate-before-apply: the client id must be non-empty (at most
    /// [`MAX_AUTHZ_CLIENTID_LEN`] bytes) and must name a concrete client
    /// (the catch-all `*` belongs to the `/rules/all` table, never here);
    /// the topic must be non-empty and parse as an MQTT filter. A rule
    /// for the same client id and topic scope (same client id, topic and
    /// action; permission is not part of the scope) is rejected as
    /// [`ClientRuleError::Duplicate`] instead of merging, and a table
    /// that already holds [`MAX_AUTHZ_CLIENT_RULES`] entries fails with
    /// [`ClientRuleError::Full`]. Persists through the registry when one
    /// is attached, so entries survive a kernel restart.
    ///
    /// Management-plane only: one short rule-table write lock per request
    /// (plus the registry save lock on persist); the publish and
    /// subscribe authorization consults keep reading under short read
    /// locks and never take this lock beyond their check.
    // TODO(parity): is the duplicate scope (clientid, topic, action), or
    // does permission participate, or is topic alone the scope? The
    // rulebook does not decide the exact scope; the current choice treats
    // the same client, topic and action as the same scope (a second
    // permission for that scope would silently shadow, never merge).
    pub fn add_client_rule(&self, rule: AclRule) -> std::result::Result<(), ClientRuleError> {
        if rule.client_pattern.trim().is_empty() {
            return Err(ClientRuleError::Invalid(
                "field `clientid` must not be empty".to_string(),
            ));
        }
        if rule.client_pattern.len() > MAX_AUTHZ_CLIENTID_LEN {
            return Err(ClientRuleError::Invalid(format!(
                "field `clientid` {:?} exceeds {MAX_AUTHZ_CLIENTID_LEN} bytes",
                rule.client_pattern
            )));
        }
        if rule.client_pattern == "*" {
            return Err(ClientRuleError::Invalid(
                "field `clientid` must name a concrete client, not the catch-all (field `clientid`)".to_string(),
            ));
        }
        if rule.topic_pattern.trim().is_empty() {
            return Err(ClientRuleError::Invalid(
                "field `topic` must not be empty".to_string(),
            ));
        }
        if TopicFilter::new(rule.topic_pattern.clone()).is_err() {
            return Err(ClientRuleError::Invalid(format!(
                "field `topic` {:?} is not a valid filter",
                rule.topic_pattern
            )));
        }
        {
            let mut rules = self.default_rules_mut();
            if rules.iter().any(|stored| {
                stored.client_pattern == rule.client_pattern
                    && stored.topic_pattern == rule.topic_pattern
                    && stored.action == rule.action
            }) {
                return Err(ClientRuleError::Duplicate);
            }
            if rules.len() >= MAX_AUTHZ_CLIENT_RULES {
                return Err(ClientRuleError::Full);
            }
            rules.push(rule);
        }
        self.persist()
            .map_err(|e| ClientRuleError::Persist(e.to_string()))
    }

    /// Remove every rule scoped to `clientid` (true when at least one
    /// entry was removed; false when none exist and nothing was
    /// persisted). The catch-all `*` belongs to the `/rules/all` table
    /// and is never removed here: asking for it reports false so the
    /// caller answers 404 instead of emptying another table. Persists
    /// through the registry when one is attached.
    /// Management-plane only: one short rule-table write lock per request
    /// (plus the registry save lock on persist).
    pub fn remove_client_rules(
        &self,
        clientid: &str,
    ) -> std::result::Result<bool, broker_config::ConfigError> {
        if clientid == "*" {
            return Ok(false);
        }
        let mut rules = self.default_rules_mut();
        let before = rules.len();
        rules.retain(|rule| rule.client_pattern != clientid);
        let removed = rules.len() != before;
        drop(rules);
        if removed {
            self.persist()?;
        }
        Ok(removed)
    }

    /// Replace every rule scoped to `clientid` with `rules` (first match
    /// wins within the client's set, in the given order).
    ///
    /// Validate-all-before-apply: the client id must be non-empty (at most
    /// [`MAX_AUTHZ_CLIENTID_LEN`] bytes) and must name a concrete client
    /// (the catch-all `*` belongs to the `/rules/all` table, never here);
    /// every entry must carry that same client id, a non-empty topic that
    /// parses as an MQTT filter, and a known action. A batch with two
    /// entries for the same topic scope (same topic and action) is
    /// rejected as [`ClientRuleError::Duplicate`] instead of merging, and
    /// a replacement that would push the table past
    /// [`MAX_AUTHZ_CLIENT_RULES`] entries fails with
    /// [`ClientRuleError::Full`]. Any violation rejects the whole write
    /// and the stored set is unchanged. Writing an unknown client id
    /// creates its set; writing an empty list clears it. Persists through
    /// the registry when one is attached, so the set survives a kernel
    /// restart.
    ///
    /// Management-plane only: topic validation runs before the lock, then
    /// one short rule-table write lock per request (plus the registry
    /// save lock on persist); the publish and subscribe authorization
    /// consults keep reading under short read locks and never take this
    /// lock beyond their check.
    // TODO(parity): does the documented replace accept a bare array body
    // alongside the `{"rules": [...]}` object, and must per-entry key
    // fields match the path id or are they ignored? The rulebook does not
    // decide either; the current choice requires the object shape and
    // rejects key mismatches fail-closed until the checker pins it.
    pub fn replace_client_rules(
        &self,
        clientid: &str,
        rules: Vec<AclRule>,
    ) -> std::result::Result<(), ClientRuleError> {
        if clientid.trim().is_empty() {
            return Err(ClientRuleError::Invalid(
                "field `clientid` must not be empty".to_string(),
            ));
        }
        if clientid.len() > MAX_AUTHZ_CLIENTID_LEN {
            return Err(ClientRuleError::Invalid(format!(
                "field `clientid` {clientid:?} exceeds {MAX_AUTHZ_CLIENTID_LEN} bytes",
            )));
        }
        if clientid == "*" {
            return Err(ClientRuleError::Invalid(
                "field `clientid` must name a concrete client, not the catch-all (field `clientid`)".to_string(),
            ));
        }
        for (index, rule) in rules.iter().enumerate() {
            if rule.client_pattern != clientid {
                return Err(ClientRuleError::Invalid(format!(
                    "rules[{index}].clientid {:?} does not match {clientid:?} (field `clientid`)",
                    rule.client_pattern
                )));
            }
            if rule.topic_pattern.trim().is_empty() {
                return Err(ClientRuleError::Invalid(format!(
                    "rules[{index}].topic must not be empty (field `topic`)"
                )));
            }
            if TopicFilter::new(rule.topic_pattern.clone()).is_err() {
                return Err(ClientRuleError::Invalid(format!(
                    "rules[{index}].topic {:?} is not a valid filter (field `topic`)",
                    rule.topic_pattern
                )));
            }
        }
        {
            let mut seen: std::collections::HashSet<(&str, &str)> =
                std::collections::HashSet::with_capacity(rules.len());
            for rule in &rules {
                let scope = (
                    rule.topic_pattern.as_str(),
                    match rule.action {
                        AclAction::Publish => "publish",
                        AclAction::Subscribe => "subscribe",
                        AclAction::All => "all",
                    },
                );
                if !seen.insert(scope) {
                    return Err(ClientRuleError::Duplicate);
                }
            }
        }
        {
            let mut table = self.default_rules_mut();
            let others = table
                .iter()
                .filter(|stored| stored.client_pattern != clientid)
                .count();
            if others + rules.len() > MAX_AUTHZ_CLIENT_RULES {
                return Err(ClientRuleError::Full);
            }
            table.retain(|stored| stored.client_pattern != clientid);
            table.extend(rules);
        }
        self.persist()
            .map_err(|e| ClientRuleError::Persist(e.to_string()))
    }

    /// Ordered snapshot of the per-user rule index: every stored rule
    /// scoped to a concrete id (the catch-all `*` entries belong to the
    /// `/rules/all` table and are excluded here), in stored evaluation
    /// order. This is the same shared built-in table the per-client index
    /// reads: client and user entries share one first-match-wins list, so
    /// a rule written for id `X` reads back under both the client key
    /// `X` and the username `X`. Management-plane read only: clones at
    /// most the capped list under one short read lock; the publish and
    /// subscribe authorization consults keep reading under their own
    /// short locks.
    // TODO(parity): are the per-client and per-user tables separate
    // stores (a client rule invisible to the user listing and vice
    // versa), or one shared table keyed by id as implemented here? The
    // rulebook does not decide the namespace split; the current choice
    // shares the W2-19 table so every stored rule stays live on the
    // publish/subscribe decision path until the checker pins it.
    pub fn all_user_rules(&self) -> Vec<AclRule> {
        self.all_client_rules()
    }

    /// Ordered snapshot of one username's rules. Exact id match only,
    /// like [`MemoryAuth::client_rules`]. Management-plane read only.
    pub fn user_rules(&self, username: &str) -> Vec<AclRule> {
        self.client_rules(username)
    }

    /// Append one user-scoped rule (first match wins).
    ///
    /// Validate-before-apply: the username must be non-empty (at most
    /// [`MAX_AUTHZ_USERNAME_LEN`] bytes) and must name a concrete user
    /// (the catch-all `*` belongs to the `/rules/all` table, never here);
    /// the topic must be non-empty and parse as an MQTT filter. A rule
    /// for the same username and topic scope (same username, topic and
    /// action; permission is not part of the scope) is rejected as
    /// [`ClientRuleError::Duplicate`] instead of merging, and a table
    /// that already holds [`MAX_AUTHZ_USER_RULES`] entries fails with
    /// [`ClientRuleError::Full`]. Persists through the registry when one
    /// is attached, so entries survive a kernel restart. The write lands
    /// in the same shared built-in table the per-client index reads, so
    /// it is live on the publish/subscribe decision path.
    ///
    /// Management-plane only: one short rule-table write lock per request
    /// (plus the registry save lock on persist); the publish and
    /// subscribe authorization consults keep reading under short read
    /// locks and never take this lock beyond their check.
    // TODO(parity): is the duplicate scope (username, topic, action), or
    // does permission participate, or is topic alone the scope? The
    // rulebook does not decide the exact scope; the current choice treats
    // the same username, topic and action as the same scope (a second
    // permission for that scope would silently shadow, never merge).
    pub fn add_user_rule(&self, rule: AclRule) -> std::result::Result<(), ClientRuleError> {
        if rule.client_pattern.trim().is_empty() {
            return Err(ClientRuleError::Invalid(
                "field `username` must not be empty".to_string(),
            ));
        }
        if rule.client_pattern.len() > MAX_AUTHZ_USERNAME_LEN {
            return Err(ClientRuleError::Invalid(format!(
                "field `username` {:?} exceeds {MAX_AUTHZ_USERNAME_LEN} bytes",
                rule.client_pattern
            )));
        }
        if rule.client_pattern == "*" {
            return Err(ClientRuleError::Invalid(
                "field `username` must name a concrete user, not the catch-all (field `username`)"
                    .to_string(),
            ));
        }
        if rule.topic_pattern.trim().is_empty() {
            return Err(ClientRuleError::Invalid(
                "field `topic` must not be empty".to_string(),
            ));
        }
        if TopicFilter::new(rule.topic_pattern.clone()).is_err() {
            return Err(ClientRuleError::Invalid(format!(
                "field `topic` {:?} is not a valid filter",
                rule.topic_pattern
            )));
        }
        {
            let mut rules = self.default_rules_mut();
            if rules.iter().any(|stored| {
                stored.client_pattern == rule.client_pattern
                    && stored.topic_pattern == rule.topic_pattern
                    && stored.action == rule.action
            }) {
                return Err(ClientRuleError::Duplicate);
            }
            if rules.len() >= MAX_AUTHZ_USER_RULES {
                return Err(ClientRuleError::Full);
            }
            rules.push(rule);
        }
        self.persist()
            .map_err(|e| ClientRuleError::Persist(e.to_string()))
    }

    /// Remove every rule scoped to `username` (true when at least one
    /// entry was removed; false when none exist and nothing was
    /// persisted). The catch-all `*` belongs to the `/rules/all` table
    /// and is never removed here: asking for it reports false so the
    /// caller answers 404 instead of emptying another table. Persists
    /// through the registry when one is attached.
    /// Management-plane only: one short rule-table write lock per request
    /// (plus the registry save lock on persist).
    pub fn remove_user_rules(
        &self,
        username: &str,
    ) -> std::result::Result<bool, broker_config::ConfigError> {
        if username == "*" {
            return Ok(false);
        }
        let mut rules = self.default_rules_mut();
        let before = rules.len();
        rules.retain(|rule| rule.client_pattern != username);
        let removed = rules.len() != before;
        drop(rules);
        if removed {
            self.persist()?;
        }
        Ok(removed)
    }

    /// Replace every rule scoped to `username` with `rules` (first match
    /// wins within the user's set, in the given order).
    ///
    /// Validate-all-before-apply: the username must be non-empty (at most
    /// [`MAX_AUTHZ_USERNAME_LEN`] bytes) and must name a concrete user
    /// (the catch-all `*` belongs to the `/rules/all` table, never here);
    /// every entry must carry that same username, a non-empty topic that
    /// parses as an MQTT filter, and a known action. A batch with two
    /// entries for the same topic scope (same topic and action) is
    /// rejected as [`ClientRuleError::Duplicate`] instead of merging, and
    /// a replacement that would push the table past
    /// [`MAX_AUTHZ_USER_RULES`] entries fails with
    /// [`ClientRuleError::Full`]. Any violation rejects the whole write
    /// and the stored set is unchanged. Writing an unknown username
    /// creates its set (create-on-write); writing an empty list clears it.
    /// Persists through the registry when one is attached, so the set
    /// survives a kernel restart. The write lands in the same shared
    /// built-in table the per-client index reads, so it is live on the
    /// publish/subscribe decision path.
    ///
    /// Management-plane only: topic validation runs before the lock, then
    /// one short rule-table write lock per request (plus the registry
    /// save lock on persist); the publish and subscribe authorization
    /// consults keep reading under short read locks and never take this
    /// lock beyond their check.
    // TODO(parity): does the documented replace accept a bare array body
    // alongside the `{"rules": [...]}` object, and must per-entry key
    // fields match the path id or are they ignored? The rulebook does not
    // decide either; the current choice requires the object shape and
    // rejects key mismatches fail-closed until the checker pins it.
    pub fn replace_user_rules(
        &self,
        username: &str,
        rules: Vec<AclRule>,
    ) -> std::result::Result<(), ClientRuleError> {
        if username.trim().is_empty() {
            return Err(ClientRuleError::Invalid(
                "field `username` must not be empty".to_string(),
            ));
        }
        if username.len() > MAX_AUTHZ_USERNAME_LEN {
            return Err(ClientRuleError::Invalid(format!(
                "field `username` {username:?} exceeds {MAX_AUTHZ_USERNAME_LEN} bytes",
            )));
        }
        if username == "*" {
            return Err(ClientRuleError::Invalid(
                "field `username` must name a concrete user, not the catch-all (field `username`)"
                    .to_string(),
            ));
        }
        for (index, rule) in rules.iter().enumerate() {
            if rule.client_pattern != username {
                return Err(ClientRuleError::Invalid(format!(
                    "rules[{index}].username {:?} does not match {username:?} (field `username`)",
                    rule.client_pattern
                )));
            }
            if rule.topic_pattern.trim().is_empty() {
                return Err(ClientRuleError::Invalid(format!(
                    "rules[{index}].topic must not be empty (field `topic`)"
                )));
            }
            if TopicFilter::new(rule.topic_pattern.clone()).is_err() {
                return Err(ClientRuleError::Invalid(format!(
                    "rules[{index}].topic {:?} is not a valid filter (field `topic`)",
                    rule.topic_pattern
                )));
            }
        }
        {
            let mut seen: std::collections::HashSet<(&str, &str)> =
                std::collections::HashSet::with_capacity(rules.len());
            for rule in &rules {
                let scope = (
                    rule.topic_pattern.as_str(),
                    match rule.action {
                        AclAction::Publish => "publish",
                        AclAction::Subscribe => "subscribe",
                        AclAction::All => "all",
                    },
                );
                if !seen.insert(scope) {
                    return Err(ClientRuleError::Duplicate);
                }
            }
        }
        {
            let mut table = self.default_rules_mut();
            let others = table
                .iter()
                .filter(|stored| stored.client_pattern != username)
                .count();
            if others + rules.len() > MAX_AUTHZ_USER_RULES {
                return Err(ClientRuleError::Full);
            }
            table.retain(|stored| stored.client_pattern != username);
            table.extend(rules);
        }
        self.persist()
            .map_err(|e| ClientRuleError::Persist(e.to_string()))
    }

    fn check(
        &self,
        client_id: &str,
        action: AclAction,
        matches_rule: impl Fn(&AclRule) -> bool,
    ) -> bool {
        let tenants = self.tenants.read();
        self.check_in_tenant(&tenants, DEFAULT_TENANT_ID, client_id, action, matches_rule)
    }
}

#[async_trait]
impl Authenticator for MemoryAuth {
    async fn authenticate(
        &self,
        client_id: &str,
        username: Option<&str>,
        password: Option<&[u8]>,
    ) -> Result<()> {
        // Default-tenant shim: single-tenant installs behave exactly as
        // before (same passwords, same open-when-empty behaviour).
        self.authenticate_in_tenant(DEFAULT_TENANT_ID, client_id, username, password)
            .await
    }
}

impl MemoryAuth {
    /// Authenticate one credential pair in one tenant (MT-06): the same
    /// username in two tenants is two independent identities with
    /// independent password hashes. The default tenant keeps today's
    /// open-when-empty behaviour (only while no tenant holds users, so a
    /// provisioned non-default tenant closes the default too); every
    /// other tenant starts closed (no users means deny, counted, logged).
    /// An invalid tenant id denies the same way. An unreachable
    /// credential backend denies (there is no network store behind this
    /// type: unknown names, locked verifiers, oversized passwords,
    /// exhausted permits and blocking-task failures all deny).
    pub async fn authenticate_in_tenant(
        &self,
        tenant: &str,
        client_id: &str,
        username: Option<&str>,
        password: Option<&[u8]>,
    ) -> Result<()> {
        let key = tenant_key(tenant);
        if !is_default_tenant(key) && !is_valid_tenant_id(key) {
            self.deny_closed_tenant(key, client_id, "invalid tenant id at authenticate");
            return Err(AuthError::AuthenticationFailed(format!(
                "{client_id} presented bad credentials"
            )));
        }
        // Snapshot the verifier under a short read lock; the expensive
        // check runs after the guard drops so accepts never block on a
        // hashing peer. No lock is held across `.await`.
        let (owned_tenant, owned_username, owned_password, verifier, needs_migration) = {
            let tenants = self.tenants.read();
            if self.total_user_count_locked(&tenants) == 0 {
                // Single-tenant open mode: no users anywhere yet.
                return Ok(());
            }
            let Some(state) = tenants.get(key) else {
                // Unknown tenant: closed, counted, logged; never granted.
                drop(tenants);
                self.deny_closed_tenant(key, client_id, "unknown tenant at authenticate");
                return Err(AuthError::AuthenticationFailed(format!(
                    "{client_id} presented bad credentials"
                )));
            };
            if !is_default_tenant(key) && state.users.is_empty() {
                // New tenant with no users yet: closed, counted, logged.
                drop(tenants);
                self.deny_closed_tenant(key, client_id, "empty tenant at authenticate");
                return Err(AuthError::AuthenticationFailed(format!(
                    "{client_id} presented bad credentials"
                )));
            }
            if is_default_tenant(key) && state.users.is_empty() {
                // Default tenant with no users while another tenant is
                // provisioned: fail closed (the global open mode above
                // already returned when nobody holds users).
                drop(tenants);
                self.deny_closed_tenant(key, client_id, "empty default tenant at authenticate");
                return Err(AuthError::AuthenticationFailed(format!(
                    "{client_id} presented bad credentials"
                )));
            }
            let (username, password) = match (username, password) {
                (Some(username), Some(password)) => (username, password),
                _ => {
                    return Err(AuthError::AuthenticationFailed(format!(
                        "{client_id} presented no credentials"
                    )));
                }
            };
            let Some(entry) = state.users.get(username) else {
                // Fail closed: unknown names never authenticate. No
                // timing oracle beyond the map lookup itself.
                return Err(AuthError::AuthenticationFailed(format!(
                    "{client_id} presented bad credentials"
                )));
            };
            if password.len() > MAX_PASSWORD_BYTES {
                // Fail closed: bound the per-connection copy instead of
                // hashing unbounded input on the connect path.
                tracing::warn!(
                    client_id,
                    username,
                    "mqtt authentication refused: password exceeds bound"
                );
                return Err(AuthError::AuthenticationFailed(format!(
                    "{client_id} presented bad credentials"
                )));
            }
            let verifier = entry.verifier.clone();
            if matches!(verifier, PasswordVerifier::Locked) {
                tracing::warn!(
                    client_id,
                    username,
                    "mqtt authentication refused: account locked (undecodable verifier)"
                );
                return Err(AuthError::AuthenticationFailed(format!(
                    "{client_id} presented bad credentials"
                )));
            }
            // Migration window: a legacy SHA-256 entry re-hashes to the
            // configured default on next successful login (unless the
            // operator pinned the default back to legacy).
            let needs_migration = matches!(verifier, PasswordVerifier::Sha256(_))
                && self.policy().default_algorithm != PasswordAlgorithm::Sha256Legacy;
            (
                key.to_string(),
                username.to_string(),
                password.to_vec(),
                verifier,
                needs_migration,
            )
        };

        // Legacy SHA-256 verifies inline without touching
        // the semaphore or the blocking pool: one hash over a bounded
        // input with no tunable cost. On success the entry
        // migrates to the configured default (unless the operator pinned
        // the default back to legacy); a hashing failure keeps the legacy
        // entry so the user is not locked out.
        if let PasswordVerifier::Sha256(expected) = &verifier {
            let candidate = MemoryAuth::digest(&owned_password);
            if constant_time_eq_32(&candidate, expected) {
                if needs_migration {
                    // Default-tenant logins migrate through the shared
                    // default helper; other tenants migrate in place so one
                    // tenant never rewrites another's entry.
                    if is_default_tenant(&owned_tenant) {
                        migrate_legacy_hash_gated(self, &owned_username, &owned_password).await;
                    } else {
                        migrate_legacy_hash_gated_in_tenant(
                            self,
                            &owned_tenant,
                            &owned_username,
                            &owned_password,
                        )
                        .await;
                    }
                }
                return Ok(());
            }
            return Err(AuthError::AuthenticationFailed(format!(
                "{client_id} presented bad credentials"
            )));
        }

        // Expensive verifiers share one check for the same credential.
        // Concurrent CONNECTs with the same tenant, username, password
        // and verifier await the leader result instead of hashing again.
        // The sharing stays bounded by [`MAX_VERIFY_DEDUP_INFLIGHT`];
        // past the cap new CONNECTs verify directly.
        let pwd_hash = Self::digest(&owned_password);
        let verifier_encoded = encode_stored_verifier(&verifier);
        let dedup_key = VerifyDedupKey {
            tenant: owned_tenant.clone(),
            username: owned_username.clone(),
            pwd_hash,
            verifier: verifier_encoded,
        };
        // One lock covers lookup and insert together. Reason: two tasks
        // must never both lead for one key and overwrite each other.
        let (joined, leader): (Option<Arc<VerifyInflight>>, Option<Arc<VerifyInflight>>) = {
            let mut table = self.verify_inflight.lock().await;
            if let Some(entry) = table.get(&dedup_key).cloned() {
                // Reclaim a stored entry. Reason: a cancelled leader
                // leaves a refusal; a new burst verifies fresh.
                if entry.result.load(std::sync::atomic::Ordering::SeqCst) == VerifyInflight::PENDING
                {
                    (Some(entry), None)
                } else {
                    let fresh = Arc::new(VerifyInflight {
                        notify: tokio::sync::Notify::new(),
                        result: std::sync::atomic::AtomicU8::new(VerifyInflight::PENDING),
                    });
                    table.insert(dedup_key.clone(), fresh.clone());
                    (None, Some(fresh))
                }
            } else if table.len() >= MAX_VERIFY_DEDUP_INFLIGHT {
                (None, None)
            } else {
                let entry = Arc::new(VerifyInflight {
                    notify: tokio::sync::Notify::new(),
                    result: std::sync::atomic::AtomicU8::new(VerifyInflight::PENDING),
                });
                table.insert(dedup_key.clone(), entry.clone());
                (None, Some(entry))
            }
        };
        if let Some(entry) = joined {
            // One waiter joined the leader check instead of hashing
            // again. Count it so a burst test observes the sharing.
            self.verify_dedup_joins
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            // Register for the wakeup before reading the result.
            // Reason: the leader stores the result before it wakes
            // waiters, so a stored result needs no wait and a later
            // wakeup still arrives.
            let notified = entry.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let outcome = entry.result.load(std::sync::atomic::Ordering::SeqCst);
            if outcome == VerifyInflight::PENDING {
                notified.await;
            }
            let ok = entry.result.load(std::sync::atomic::Ordering::SeqCst) == VerifyInflight::OK;
            if ok {
                return Ok(());
            }
            return Err(AuthError::AuthenticationFailed(format!(
                "{client_id} presented bad credentials"
            )));
        }
        let Some(leader) = leader else {
            // Past the cap: verify directly without sharing.
            // One owned semaphore permit bounds concurrency; the blocking
            // hash runs off the accept path while CONNECT awaits the verdict.
            let permit = self.verify_gate.clone().try_acquire_owned().map_err(|_| {
                // Fail closed: an unbounded queue of pending verifications
                // would let a CONNECT flood grow memory without limit.
                tracing::warn!(
                    client_id,
                    username = owned_username.as_str(),
                    "mqtt authentication refused: verification permits exhausted"
                );
                AuthError::AuthenticationFailed(format!("{client_id} presented bad credentials"))
            })?;
            // Count the run. Reason: the storm test asserts real hashes
            // stay within the credential count.
            self.verify_runs
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let verified = tokio::task::spawn_blocking(move || {
                let ok = verify_blocking(&verifier, &owned_password);
                drop(permit);
                ok
            })
            .await
            .unwrap_or(false);
            if verified {
                return Ok(());
            }
            return Err(AuthError::AuthenticationFailed(format!(
                "{client_id} presented bad credentials"
            )));
        };
        let mut guard = VerifyLeaderGuard {
            map: self.verify_inflight.clone(),
            key: Some(dedup_key),
            entry: leader.clone(),
            completed: false,
        };
        // One owned semaphore permit bounds concurrency; the blocking
        // hash runs off the accept path while CONNECT awaits the verdict.
        // Exhaustion fails closed. The guard fails joined waiters closed
        // too when this return runs.
        let permit = self.verify_gate.clone().try_acquire_owned().map_err(|_| {
            // Fail closed: an unbounded queue of pending verifications
            // would let a CONNECT flood grow memory without limit.
            tracing::warn!(
                client_id,
                username = owned_username.as_str(),
                "mqtt authentication refused: verification permits exhausted"
            );
            AuthError::AuthenticationFailed(format!("{client_id} presented bad credentials"))
        })?;
        // Count the run. Reason: the storm test asserts real hashes stay
        // within the credential count.
        self.verify_runs
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let verified = tokio::task::spawn_blocking(move || {
            let ok = verify_blocking(&verifier, &owned_password);
            drop(permit);
            ok
        })
        .await
        .unwrap_or(false);
        // Store the result before waking waiters. Reason: waiters
        // register before they read, so no wakeup is lost.
        leader.result.store(
            if verified {
                VerifyInflight::OK
            } else {
                VerifyInflight::REFUSED
            },
            std::sync::atomic::Ordering::SeqCst,
        );
        leader.notify.notify_waiters();
        {
            let key = guard.key.take().expect("leader holds dedup key");
            let mut table = self.verify_inflight.lock().await;
            let same = table
                .get(&key)
                .map(|current| Arc::ptr_eq(current, &leader))
                .unwrap_or(false);
            if same {
                table.remove(&key);
            }
        }
        guard.completed = true;
        if verified {
            Ok(())
        } else {
            Err(AuthError::AuthenticationFailed(format!(
                "{client_id} presented bad credentials"
            )))
        }
    }
}

/// Re-hash a legacy SHA-256 password to the configured default after a
/// successful login (migration window). The password already proved
/// correct, so every failure mode keeps the legacy entry (still
/// verifiable) instead of locking the user out, and the next login
/// retries. The production-cost hash runs in `spawn_blocking` off the
/// connect task, gated by [`MAX_CONCURRENT_MIGRATIONS`] permits: a burst
/// of legacy logins cannot grow unbounded blocking work, and exhaustion
/// defers the migration. A persist failure is logged and keeps the
/// in-memory migration; the next login retries it.
async fn migrate_legacy_hash_gated(auth: &MemoryAuth, username: &str, password: &[u8]) {
    migrate_legacy_hash_gated_in_tenant(auth, DEFAULT_TENANT_ID, username, password).await;
}

/// Per-tenant legacy-hash migration (MT-06): re-hashes in the connection's
/// own tenant so one tenant's login never rewrites another tenant's
/// entry. Every tenant persists (TM-01); a persist failure keeps the
/// in-memory migration and the next login retries it.
async fn migrate_legacy_hash_gated_in_tenant(
    auth: &MemoryAuth,
    tenant: &str,
    username: &str,
    password: &[u8],
) {
    let Ok(permit) = auth.migrate_gate.clone().try_acquire_owned() else {
        // Bounded burst: defer instead of queueing unboundedly.
        tracing::warn!(
            username,
            "mqtt password migration deferred: migration permits exhausted; legacy verifier retained"
        );
        return;
    };
    let policy = auth.policy();
    let algorithm = policy.default_algorithm;
    let password = password.to_vec();
    let next = tokio::task::spawn_blocking(move || {
        let _permit = permit;
        hash_password_for_policy(&policy, &password, algorithm)
    })
    .await
    .unwrap_or(None);
    let Some(next) = next else {
        tracing::warn!(
            username,
            "mqtt password migration skipped: default hash failed; legacy verifier retained"
        );
        return;
    };
    {
        let key = tenant_key(tenant);
        let mut tenants = auth.tenants_mut();
        let Some(state) = tenants.get_mut(key) else {
            return;
        };
        let Some(entry) = state.users.get_mut(username) else {
            return;
        };
        if !matches!(entry.verifier, PasswordVerifier::Sha256(_)) {
            return;
        }
        entry.verifier = next;
    }
    if let Err(error) = auth.persist() {
        tracing::warn!(
            username,
            reason = error.to_string(),
            "mqtt password migration kept in memory only: persist failed"
        );
    }
}

#[async_trait]
impl Authorizer for MemoryAuth {
    async fn authorize_publish(&self, client_id: &str, topic: &Topic) -> Result<()> {
        // Stored order and enable flags gate the built-in table: a
        // disabled or removed built-in source denies fail closed and
        // counts the denial on the shared registry. Otherwise the
        // verdict attributes to the built-in counters.
        if !self.built_in_live() {
            self.record_authz_verdict(false);
            return Err(AuthError::PublishDenied(format!(
                "{client_id} cannot publish"
            )));
        }
        // Default-tenant shim through the shared first-match-wins helper
        // so single-tenant installs behave exactly as before (same
        // open-when-empty; unmatched follows the stored `no_match`).
        if self.check(client_id, AclAction::Publish, |rule| {
            rule.topic_matches(topic)
        }) {
            self.record_authz_verdict(true);
            Ok(())
        } else {
            self.record_authz_verdict(false);
            Err(AuthError::PublishDenied(format!(
                "{client_id} cannot publish"
            )))
        }
    }

    async fn authorize_subscribe(&self, client_id: &str, filter: &TopicFilter) -> Result<()> {
        // Same stored order and enable gate as publish (see above).
        if !self.built_in_live() {
            self.record_authz_verdict(false);
            return Err(AuthError::SubscribeDenied(format!(
                "{client_id} cannot subscribe"
            )));
        }
        // Same default-tenant shim as publish (see above).
        if self.check(client_id, AclAction::Subscribe, |rule| {
            rule.filter_covered(filter)
        }) {
            self.record_authz_verdict(true);
            Ok(())
        } else {
            self.record_authz_verdict(false);
            Err(AuthError::SubscribeDenied(format!(
                "{client_id} cannot subscribe"
            )))
        }
    }
}

impl MemoryAuth {
    /// Authorize one publish in one tenant (MT-06): a rule in one tenant
    /// never grants publish in another. The default tenant with no rules
    /// stays open; a non-default tenant with no rules (or an unknown or
    /// invalid tenant) denies, counted and logged. Reads under one short
    /// guard with no new lock and no allocation on the publish path.
    /// Operation count for one allowed publish in the empty default tenant.
    /// Before: four read locks and two Arc clones per publish.
    /// After: four lock-free loads and one atomic add, no lock, no clone,
    /// no allocation. A non-empty tenant adds one short tenant read lock.
    /// An unmatched scan adds one lock-free settings load.
    pub async fn authorize_publish_in_tenant(
        &self,
        tenant: &str,
        client_id: &str,
        topic: &Topic,
    ) -> Result<()> {
        let key = tenant_key(tenant);
        // The allow path avoids locks. It reads the source link
        // and the settings with lock-free loads. It counts verdicts
        // with atomic adds. With no rule in the default tenant it
        // skips the tenant lock as well.
        let sources = self.authz_sources.load();
        let sources = sources.as_deref();
        if !is_default_tenant(key) && !is_valid_tenant_id(key) {
            self.deny_closed_tenant(key, client_id, "invalid tenant id at authorize_publish");
            Self::record_verdict(sources, false);
            return Err(AuthError::PublishDenied(format!(
                "{client_id} cannot publish"
            )));
        }
        // Stored order and enable gate first: a disabled or removed
        // built-in source denies fail closed in every tenant and counts
        // the denial on the shared registry.
        if !sources.is_none_or(|registry| registry.built_in_available()) {
            Self::record_verdict(sources, false);
            return Err(AuthError::PublishDenied(format!(
                "{client_id} cannot publish"
            )));
        }
        // An empty default tenant allows the publish: no lock, no scan.
        // Otherwise one short guard covers the tenant lookup plus the rule
        // scan; closed-ness is derived from the same guard so the deny
        // path takes no second tenant lock. Only an unmatched scan takes
        // one lock-free settings load for the `no_match` value.
        let (allowed, closed) = if is_default_tenant(key)
            && self
                .default_rules_empty
                .load(std::sync::atomic::Ordering::Acquire)
        {
            (true, false)
        } else {
            let tenants = self.tenants.read();
            let closed = match tenants.get(key) {
                None => !is_default_tenant(key),
                Some(state) => !is_default_tenant(key) && state.rules.is_empty(),
            };
            let allowed =
                self.check_in_tenant(&tenants, key, client_id, AclAction::Publish, |rule| {
                    rule.topic_matches(topic)
                });
            (allowed, closed)
        };
        if allowed {
            Self::record_verdict(sources, true);
            Ok(())
        } else {
            // Count only genuinely closed tenants (unknown or a
            // non-default tenant with no rules); a default-tenant
            // deny-if-unmatched is today's behaviour, not a
            // closed-tenant event.
            if closed {
                self.deny_closed_tenant(key, client_id, "closed tenant at authorize_publish");
            }
            Self::record_verdict(sources, false);
            Err(AuthError::PublishDenied(format!(
                "{client_id} cannot publish"
            )))
        }
    }

    /// Authorize one subscribe in one tenant (MT-06): same per-tenant
    /// isolation as publish. Same lock/allocation discipline.
    pub async fn authorize_subscribe_in_tenant(
        &self,
        tenant: &str,
        client_id: &str,
        filter: &TopicFilter,
    ) -> Result<()> {
        let key = tenant_key(tenant);
        let filter = filter.clone();
        if !is_default_tenant(key) && !is_valid_tenant_id(key) {
            self.deny_closed_tenant(key, client_id, "invalid tenant id at authorize_subscribe");
            self.record_authz_verdict(false);
            return Err(AuthError::SubscribeDenied(format!(
                "{client_id} cannot subscribe"
            )));
        }
        // Same stored order and enable gate as the publish path.
        if !self.built_in_live() {
            self.record_authz_verdict(false);
            return Err(AuthError::SubscribeDenied(format!(
                "{client_id} cannot subscribe"
            )));
        }
        // Same single-guard discipline as the publish path: only an
        // unmatched scan takes the lock-free settings load.
        let (allowed, closed) = {
            let tenants = self.tenants.read();
            let closed = match tenants.get(key) {
                None => !is_default_tenant(key),
                Some(state) => !is_default_tenant(key) && state.rules.is_empty(),
            };
            let allowed =
                self.check_in_tenant(&tenants, key, client_id, AclAction::Subscribe, |rule| {
                    rule.filter_covered(&filter)
                });
            (allowed, closed)
        };
        if allowed {
            self.record_authz_verdict(true);
            Ok(())
        } else {
            if closed {
                self.deny_closed_tenant(key, client_id, "closed tenant at authorize_subscribe");
            }
            self.record_authz_verdict(false);
            Err(AuthError::SubscribeDenied(format!(
                "{client_id} cannot subscribe"
            )))
        }
    }
}

/// Prefix of the PBKDF2-SHA256 verifier encoding:
/// `$pbkdf2-sha256$<iterations>$<salt-b64>$<key-b64>` (standard base64).
const PBKDF2_VERIFIER_PREFIX: &str = "$pbkdf2-sha256$";
/// Encoding of a locked account. Starts with `$` so it never collides
/// with legacy hex, and names no known algorithm so it re-locks on
/// reload (fail closed, stable across restarts).
const LOCKED_VERIFIER_ENCODING: &str = "$locked$undecodable-verifier";

/// Hash `password` under `algorithm` with `policy` parameters (`None`
/// when the password cannot be hashed: oversized input or a backend
/// failure such as bcrypt's 72-byte limit). Uses only maintained
/// primitives (bcrypt, RustCrypto PBKDF2, Argon2); no hand-rolled crypto.
fn hash_password_for_policy(
    policy: &PasswordHashPolicy,
    password: &[u8],
    algorithm: PasswordAlgorithm,
) -> Option<PasswordVerifier> {
    if password.len() > MAX_PASSWORD_BYTES {
        return None;
    }
    match algorithm {
        PasswordAlgorithm::Sha256Legacy => {
            Some(PasswordVerifier::Sha256(MemoryAuth::digest(password)))
        }
        PasswordAlgorithm::Bcrypt => {
            let cost = policy.bcrypt_cost.clamp(4, 31);
            // Maintained bcrypt primitive. The password crosses as `&str`
            // so this compiles against both byte- and string-taking crate
            // generations; non-UTF8 and overlong (>72 byte) passwords fail
            // closed here (`None`) instead of truncating (the primitive
            // silently keeps only the first 72 bytes).
            if password.len() > MAX_BCRYPT_PASSWORD_BYTES {
                return None;
            }
            let password_str = std::str::from_utf8(password).ok()?;
            bcrypt::hash(password_str, cost)
                .ok()
                .map(PasswordVerifier::Bcrypt)
        }
        PasswordAlgorithm::Pbkdf2Sha256 => {
            let iterations = policy.pbkdf2_iterations.max(1);
            let mut salt = vec![0u8; DEFAULT_PBKDF2_SALT_LEN];
            rand::Rng::fill_bytes(&mut rand::rng(), &mut salt[..]);
            let mut hash = vec![0u8; DEFAULT_PBKDF2_KEY_LEN];
            pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password, &salt, iterations, &mut hash);
            Some(PasswordVerifier::Pbkdf2 {
                iterations,
                salt,
                hash,
            })
        }
        PasswordAlgorithm::Argon2id => hash_argon2id(
            password,
            policy.argon2_m_kib,
            policy.argon2_t_cost,
            policy.argon2_p_cost,
        )
        .map(PasswordVerifier::Argon2),
    }
}

/// Argon2id hash returning the PHC encoding (`None` on bad params).
/// Salt is [`DEFAULT_ARGON2_SALT_LEN`] bytes from the OS RNG; output
/// [`DEFAULT_ARGON2_KEY_LEN`] bytes.
fn hash_argon2id(password: &[u8], m_kib: u32, t_cost: u32, p_cost: u32) -> Option<String> {
    let m_kib = m_kib.clamp(8, 256 * 1024);
    let t_cost = t_cost.clamp(1, 10);
    let p_cost = p_cost.clamp(1, 8);
    let params = Params::new(m_kib, t_cost, p_cost, Some(DEFAULT_ARGON2_KEY_LEN)).ok()?;
    let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
    let mut salt_bytes = vec![0u8; DEFAULT_ARGON2_SALT_LEN];
    rand::Rng::fill_bytes(&mut rand::rng(), &mut salt_bytes[..]);
    let salt = SaltString::encode_b64(&salt_bytes).ok()?;
    argon2
        .hash_password(password, &salt)
        .ok()
        .map(|hash| hash.to_string())
}

/// Blocking verification for one stored verifier (runs in
/// `spawn_blocking`). Unknown shapes never verify; every error fails
/// closed (`false`).
fn verify_blocking(verifier: &PasswordVerifier, password: &[u8]) -> bool {
    match verifier {
        PasswordVerifier::Sha256(expected) => {
            constant_time_eq_32(&MemoryAuth::digest(password), expected)
        }
        PasswordVerifier::Bcrypt(encoded) => {
            // See hashing: bcrypt sees the UTF-8 form; non-UTF8 and
            // overlong (>72 byte) inputs fail closed (deny) rather than
            // truncating or guessing, so passwords sharing a 72-byte
            // prefix never verify identically.
            if password.len() > MAX_BCRYPT_PASSWORD_BYTES {
                return false;
            }
            let Ok(password_str) = std::str::from_utf8(password) else {
                return false;
            };
            bcrypt::verify(password_str, encoded.as_str()).unwrap_or(false)
        }
        PasswordVerifier::Pbkdf2 {
            iterations,
            salt,
            hash,
        } => {
            if *iterations == 0 || salt.is_empty() || hash.is_empty() {
                return false;
            }
            let mut candidate = vec![0u8; hash.len()];
            pbkdf2::pbkdf2_hmac::<sha2::Sha256>(password, salt, *iterations, &mut candidate);
            constant_time_eq_bytes(&candidate, hash)
        }
        PasswordVerifier::Argon2(encoded) => {
            let Ok(parsed) = PasswordHash::new(encoded) else {
                return false;
            };
            // The cost parameters travel with the PHC string, so the
            // default instance verifies hashes created under any settings.
            Argon2::default().verify_password(password, &parsed).is_ok()
        }
        PasswordVerifier::Locked => false,
    }
}

/// Encode a verifier for `MqttUser.password_hash` (never plaintext).
/// Legacy entries stay 64-char lowercase hex until migration; bcrypt
/// keeps its `$2*` PHC string; PBKDF2 uses `$pbkdf2-sha256$...`; Argon2
/// keeps its `$argon2id$...` PHC string; locked accounts use the locked
/// marker (fail closed on reload).
fn encode_stored_verifier(verifier: &PasswordVerifier) -> String {
    match verifier {
        PasswordVerifier::Sha256(hash) => encode_hex(hash),
        PasswordVerifier::Bcrypt(encoded) | PasswordVerifier::Argon2(encoded) => encoded.clone(),
        PasswordVerifier::Pbkdf2 {
            iterations,
            salt,
            hash,
        } => {
            let engine = base64::engine::general_purpose::STANDARD;
            format!(
                "{PBKDF2_VERIFIER_PREFIX}{iterations}${}${}",
                engine.encode(salt),
                engine.encode(hash),
            )
        }
        PasswordVerifier::Locked => LOCKED_VERIFIER_ENCODING.to_string(),
    }
}

/// Parse a stored verifier (`None` when malformed or naming an unknown
/// algorithm; the caller locks the account). Overlong inputs are rejected
/// before allocation beyond the bound. Legacy 64-hex decodes to SHA-256;
/// `$2a$`/`$2b$`/`$2y$` to bcrypt; `$pbkdf2-sha256$` to PBKDF2;
/// `$argon2id$` to Argon2id (validated with the maintained parser).
fn parse_stored_verifier(raw: &str) -> Option<PasswordVerifier> {
    if raw.len() > MAX_STORED_VERIFIER_BYTES || raw.is_empty() {
        return None;
    }
    if let Some(rest) = raw.strip_prefix(PBKDF2_VERIFIER_PREFIX) {
        return parse_pbkdf2_verifier(rest);
    }
    if raw.starts_with("$argon2id$") {
        // Validate with the maintained PHC parser; the exact string is
        // kept so parameters round-trip byte-identically.
        if PasswordHash::new(raw).is_ok() {
            return Some(PasswordVerifier::Argon2(raw.to_string()));
        }
        return None;
    }
    if raw.starts_with("$2a$") || raw.starts_with("$2b$") || raw.starts_with("$2y$") {
        if raw.len() == 60 && raw.is_ascii() {
            return Some(PasswordVerifier::Bcrypt(raw.to_string()));
        }
        return None;
    }
    if raw.starts_with('$') {
        // Unknown algorithm identifier: fail closed, never guess.
        // TODO(parity): no judge scenario covers unknown-scheme migration
        // policy (re-hash vs. hold); current choice is hold-locked.
        return None;
    }
    decode_hex_sha256(raw).map(PasswordVerifier::Sha256)
}

/// Parse the body after `$pbkdf2-sha256$` (`<iter>$<salt-b64>$<key-b64>`).
fn parse_pbkdf2_verifier(rest: &str) -> Option<PasswordVerifier> {
    let mut parts = rest.split('$');
    let iterations: u32 = parts.next()?.parse().ok()?;
    if iterations == 0 {
        return None;
    }
    let engine = base64::engine::general_purpose::STANDARD;
    let salt = engine.decode(parts.next()?).ok()?;
    let hash = engine.decode(parts.next()?).ok()?;
    if parts.next().is_some() {
        return None;
    }
    if salt.is_empty() || salt.len() > 64 || hash.is_empty() || hash.len() > 128 || hash.len() < 16
    {
        return None;
    }
    Some(PasswordVerifier::Pbkdf2 {
        iterations,
        salt,
        hash,
    })
}

/// Lowercase hex of a SHA-256 digest (legacy persisted form).
fn encode_hex(hash: &[u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(64);
    for byte in hash {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

/// Decode the legacy 64-char hex verifier (`None` when malformed).
fn decode_hex_sha256(raw: &str) -> Option<[u8; 32]> {
    if raw.len() != 64 || !raw.is_ascii() {
        return None;
    }
    let bytes = raw.as_bytes();
    let mut hash = [0u8; 32];
    for (index, slot) in hash.iter_mut().enumerate() {
        let hi = hex_value(bytes[2 * index])?;
        let lo = hex_value(bytes[2 * index + 1])?;
        *slot = (hi << 4) | lo;
    }
    Some(hash)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

fn constant_time_eq_32(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

fn constant_time_eq_bytes(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Canonical config spelling of an [`AclAction`] (matches
/// [`AclAction::parse`]).
fn action_to_conf(action: AclAction) -> &'static str {
    match action {
        AclAction::Publish => "publish",
        AclAction::Subscribe => "subscribe",
        AclAction::All => "all",
    }
}

/// Resolve a password argument that may be a secret reference.
///
/// UTF-8 values starting with `file:`/`env:` resolve through the secret
/// store at use time; anything else (including non-UTF-8 bytes) is used
/// verbatim. Failures fail closed naming the reference, never the value.
fn resolve_password_bytes(
    password: &[u8],
) -> std::result::Result<Vec<u8>, broker_config::ConfigError> {
    match std::str::from_utf8(password) {
        // Secret references resolve at use time via the shared helper;
        // anything else (including non-UTF-8 bytes) is used verbatim.
        // Failures fail closed naming the reference, never the value.
        Ok(text) if broker_config::secrets::is_secret_ref(text.trim()) => {
            broker_config::secrets::resolve_maybe_secret(text.trim())
        }
        _ => Ok(password.to_vec()),
    }
}

/// Best-effort zeroing of a resolved secret copy.
fn zero_bytes(bytes: &mut [u8]) {
    for byte in bytes.iter_mut() {
        *byte = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn topic(s: &str) -> Topic {
        Topic::new(s).unwrap()
    }

    fn filter(s: &str) -> TopicFilter {
        TopicFilter::new(s).unwrap()
    }

    #[tokio::test]
    async fn test_authenticate_open_mode_without_users() {
        let auth = MemoryAuth::new();
        // No users configured: everything (even credentialed) passes.
        assert!(auth.authenticate("anon", None, None).await.is_ok());
        assert!(auth
            .authenticate("someone", Some("u"), Some(b"p"))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn invalid_snapshot_fails_closed_without_panic() {
        // A duplicated tenant table is invalid. The load fails closed:
        // no panic, and every authentication and publish denies.
        let conf = MqttUsersConf {
            users: Vec::new(),
            acls: Vec::new(),
            tenants: vec![
                broker_config::TenantMqttConf {
                    tenant: "dup".to_string(),
                    users: Vec::new(),
                    acls: Vec::new(),
                },
                broker_config::TenantMqttConf {
                    tenant: "dup".to_string(),
                    users: Vec::new(),
                    acls: Vec::new(),
                },
            ],
        };
        let auth = MemoryAuth::from_snapshot(&conf);
        assert!(auth
            .authenticate("any", Some("u"), Some(b"p"))
            .await
            .is_err());
        assert!(auth.authorize_publish("any", &topic("a/b")).await.is_err());
        assert!(auth
            .authorize_subscribe("any", &filter("a/b"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn empty_default_tenant_flag_follows_rules_and_verdicts_are_counted() {
        // An empty default tenant allows without the tenant lock. The flag
        // that drives this follows every rule change, and each verdict is
        // counted on the shared source registry.
        let auth = MemoryAuth::new();
        let registry = Arc::new(AuthzSourceRegistry::new());
        auth.set_authz_sources(Arc::clone(&registry));
        auth.authorize_publish("client-a", &topic("own/data"))
            .await
            .expect("an empty default tenant allows");
        auth.add_rule(AclRule::new(
            "client-a",
            AclAction::Publish,
            "other/#",
            true,
        ))
        .expect("memory-only persist cannot fail");
        assert!(auth
            .authorize_publish("client-a", &topic("own/data"))
            .await
            .is_err());
        auth.authorize_publish("client-a", &topic("other/data"))
            .await
            .expect("the rule allows");
        auth.clear_rules().expect("memory-only persist cannot fail");
        auth.authorize_publish("client-a", &topic("own/data"))
            .await
            .expect("no rule allows again");
        auth.add_rule(AclRule::new(
            "client-a",
            AclAction::Publish,
            "other/#",
            true,
        ))
        .expect("memory-only persist cannot fail");
        auth.replace_rules(Vec::new())
            .expect("memory-only persist cannot fail");
        auth.authorize_publish("client-a", &topic("own/data"))
            .await
            .expect("replaced with no rule allows");
        let counts = registry
            .counter_snapshot("built_in_database")
            .expect("built-in counters exist");
        assert_eq!((counts.allow, counts.deny, counts.total), (4, 1, 5));
    }

    #[tokio::test]
    async fn unmatched_scan_follows_no_match_setting() {
        // One rule that never matches the probe topic. Unlinked state
        // keeps today's behaviour (unmatched denied).
        let auth = MemoryAuth::new();
        auth.add_rule(AclRule::new(
            "client-a",
            AclAction::Publish,
            "other/#",
            true,
        ))
        .expect("memory-only persist cannot fail");
        assert!(auth
            .authorize_publish("client-a", &topic("own/data"))
            .await
            .is_err());
        assert!(!auth.authz_deny_disconnects());

        // Linked `no_match: allow` admits the unmatched publish and the
        // unmatched subscribe while a matched deny still denies.
        let store = Arc::new(AuthzSettingsStore::from_conf(
            &broker_config::AuthzSettingsConf {
                no_match: "allow".to_string(),
                ..broker_config::AuthzSettingsConf::default()
            },
        ));
        auth.set_authz_settings(Arc::clone(&store));
        assert!(auth
            .authorize_publish("client-a", &topic("own/data"))
            .await
            .is_ok());
        assert!(auth
            .authorize_subscribe("client-a", &filter("own/#"))
            .await
            .is_ok());
        auth.add_rule(AclRule::new("client-a", AclAction::Publish, "own/#", false))
            .expect("memory-only persist cannot fail");
        assert!(auth
            .authorize_publish("client-a", &topic("own/data"))
            .await
            .is_err());

        // Linked `no_match: deny` with `deny_action: disconnect` denies
        // the unmatched probe and reports the disconnect.
        store
            .replace(broker_config::AuthzSettingsConf {
                no_match: "deny".to_string(),
                deny_action: "disconnect".to_string(),
                ..broker_config::AuthzSettingsConf::default()
            })
            .expect("valid replace");
        assert!(auth
            .authorize_publish("client-b", &topic("own/data"))
            .await
            .is_err());
        assert!(auth.authz_deny_disconnects());
    }

    #[tokio::test]
    async fn open_broker_without_rules_stays_open() {
        // No rules at all: the broker stays open even with an explicit
        // `no_match: deny` linked.
        let auth = MemoryAuth::new();
        let store = Arc::new(AuthzSettingsStore::from_conf(
            &broker_config::AuthzSettingsConf::default(),
        ));
        auth.set_authz_settings(store);
        assert!(auth.authorize_publish("any", &topic("a/b")).await.is_ok());
        assert!(auth
            .authorize_subscribe("any", &filter("a/#"))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_authenticate_valid_and_invalid_passwords() {
        let auth = MemoryAuth::new();
        auth.add_user("alice", b"s3cret")
            .expect("memory-only persist cannot fail");
        assert_eq!(auth.user_count(), 1);

        assert!(auth
            .authenticate("device-1", Some("alice"), Some(b"s3cret"))
            .await
            .is_ok());
        assert!(auth
            .authenticate("device-1", Some("alice"), Some(b"wrong"))
            .await
            .is_err());
        assert!(auth
            .authenticate("device-1", Some("mallory"), Some(b"s3cret"))
            .await
            .is_err());
        assert!(auth.authenticate("device-1", None, None).await.is_err());

        assert!(auth
            .remove_user("alice")
            .expect("memory-only persist cannot fail"));
        assert!(!auth
            .remove_user("alice")
            .expect("memory-only persist cannot fail"));
        // Back to open mode once the last user is gone.
        assert!(auth.authenticate("anon", None, None).await.is_ok());
    }

    #[tokio::test]
    async fn password_hashing_each_algorithm_verifies() {
        // B5-01: one user per supported algorithm verifies, and a wrong
        // password fails for each. Fast test policy keeps the suite
        // interactive; hashing itself is local computation (no server).
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        for (name, algorithm) in [
            ("u-bcrypt", PasswordAlgorithm::Bcrypt),
            ("u-pbkdf2", PasswordAlgorithm::Pbkdf2Sha256),
            ("u-argon2", PasswordAlgorithm::Argon2id),
            ("u-legacy", PasswordAlgorithm::Sha256Legacy),
        ] {
            auth.add_user_with_algorithm(name, b"correct-horse", algorithm)
                .expect("memory-only persist cannot fail");
        }
        for name in ["u-bcrypt", "u-pbkdf2", "u-argon2", "u-legacy"] {
            assert!(
                auth.authenticate("c", Some(name), Some(b"correct-horse"))
                    .await
                    .is_ok(),
                "{name} must verify"
            );
            assert!(
                auth.authenticate("c", Some(name), Some(b"wrong-horse"))
                    .await
                    .is_err(),
                "{name} must refuse a wrong password"
            );
        }
    }

    #[tokio::test]
    async fn password_hashing_default_is_argon2id() {
        // B5-01: new credentials use the configured default (Argon2id).
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        assert_eq!(auth.policy().default_algorithm, PasswordAlgorithm::Argon2id);
        auth.add_user("fresh", b"fresh-secret")
            .expect("memory-only persist cannot fail");
        let stored = auth.verifier_string("fresh").expect("user stored");
        assert!(
            stored.starts_with("$argon2id$"),
            "default verifier must be Argon2id PHC, got {stored}"
        );
        assert!(auth
            .authenticate("c", Some("fresh"), Some(b"fresh-secret"))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn password_hashing_legacy_sha256_migrates_on_login() {
        // B5-01: a legacy SHA-256 entry still verifies, then migrates to
        // the default on successful login; the old password keeps working
        // and a wrong one still fails.
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        auth.add_user_with_algorithm("legacy", b"old-secret", PasswordAlgorithm::Sha256Legacy)
            .expect("memory-only persist cannot fail");
        let before = auth.verifier_string("legacy").expect("user stored");
        assert_eq!(before.len(), 64, "legacy form is 64 hex chars");
        assert!(auth
            .authenticate("c", Some("legacy"), Some(b"old-secret"))
            .await
            .is_ok());
        let after = auth.verifier_string("legacy").expect("user stored");
        assert!(
            after.starts_with("$argon2id$"),
            "migrated verifier must be Argon2id PHC, got {after}"
        );
        assert!(auth
            .authenticate("c", Some("legacy"), Some(b"old-secret"))
            .await
            .is_ok());
        assert!(auth
            .authenticate("c", Some("legacy"), Some(b"nope"))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn password_hashing_unknown_and_malformed_fail_closed() {
        // B5-01: unknown algorithm identifiers and malformed entries fail
        // closed (deny, never grant) and stay visible in `usernames()`.
        // Also covers the unreachable-store rule: with no network store,
        // every undecodable state denies access and is logged at seed.
        let conf = MqttUsersConf {
            users: vec![
                MqttUser {
                    username: "unknown-scheme".to_string(),
                    password_hash: "$scram-sha-1$4096$c2FsdA==$aGFzaA==".to_string(),
                    max_connections: None,
                    max_publish_rate: None,
                    max_publish_burst: None,
                },
                MqttUser {
                    username: "malformed".to_string(),
                    password_hash: "not-hex-at-all".to_string(),
                    max_connections: None,
                    max_publish_rate: None,
                    max_publish_burst: None,
                },
                MqttUser {
                    username: "truncated-bcrypt".to_string(),
                    password_hash: "$2b$12$short".to_string(),
                    max_connections: None,
                    max_publish_rate: None,
                    max_publish_burst: None,
                },
            ],
            acls: vec![],
            tenants: vec![],
        };
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        auth.seed_from_snapshot(&conf)
            .expect("memory-only seed with locked verifiers cannot fail");
        assert_eq!(
            auth.usernames(),
            vec!["malformed", "truncated-bcrypt", "unknown-scheme"]
        );
        for name in ["unknown-scheme", "malformed", "truncated-bcrypt"] {
            assert!(
                auth.authenticate("c", Some(name), Some(b"anything"))
                    .await
                    .is_err(),
                "{name} must fail closed"
            );
            assert!(
                auth.authenticate("c", Some(name), Some(b"")).await.is_err(),
                "{name} must fail closed on empty password too"
            );
        }
        // Unknown users and oversized passwords fail closed as well.
        assert!(auth
            .authenticate("c", Some("ghost"), Some(b"anything"))
            .await
            .is_err());
        let big = vec![b'x'; MAX_PASSWORD_BYTES + 1];
        assert!(auth
            .authenticate("c", Some("malformed"), Some(big.as_slice()))
            .await
            .is_err());
        // Creation with an oversized password stores nothing.
        assert!(auth.add_user("too-big", big.as_slice()).is_err());
        assert!(auth.verifier_string("too-big").is_none());
    }

    #[tokio::test]
    async fn test_user_quotas_roundtrip() {
        let auth = MemoryAuth::new();
        assert!(auth.get_quotas("alice").is_none());
        assert!(!auth
            .set_quotas(
                "alice",
                UserQuotas {
                    max_connections: Some(100),
                    max_publish_rate: None,
                    max_publish_burst: None,
                }
            )
            .expect("memory-only persist cannot fail"));

        auth.add_user("alice", b"s3cret")
            .expect("memory-only persist cannot fail");
        assert_eq!(
            auth.get_quotas("alice"),
            Some(UserQuotas::default()),
            "fresh users start unlimited"
        );
        assert!(auth
            .set_quotas(
                "alice",
                UserQuotas {
                    max_connections: Some(100),
                    max_publish_rate: Some(50),
                    max_publish_burst: Some(10),
                }
            )
            .expect("memory-only persist cannot fail"));
        assert_eq!(
            auth.get_quotas("alice"),
            Some(UserQuotas {
                max_connections: Some(100),
                max_publish_rate: Some(50),
                max_publish_burst: Some(10),
            })
        );

        // Password rotation preserves quotas.
        auth.add_user("alice", b"n3w")
            .expect("memory-only persist cannot fail");
        assert!(auth
            .authenticate("d", Some("alice"), Some(b"n3w"))
            .await
            .is_ok());
        assert_eq!(
            auth.get_quotas("alice")
                .expect("quotas survive")
                .max_connections,
            Some(100)
        );

        // Removing the user drops quotas with it.
        assert!(auth
            .remove_user("alice")
            .expect("memory-only persist cannot fail"));
        assert!(auth.get_quotas("alice").is_none());
    }

    #[tokio::test]
    async fn test_acl_open_mode_without_rules() {
        let auth = MemoryAuth::new();
        assert!(auth.authorize_publish("any", &topic("a/b")).await.is_ok());
        assert!(auth
            .authorize_subscribe("any", &filter("a/#"))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_acl_allow_and_deny_with_first_match_wins() {
        let auth = MemoryAuth::new();
        auth.add_rule(AclRule::new(
            "sensor-1",
            AclAction::Publish,
            "sensors/#",
            true,
        ))
        .expect("memory-only persist cannot fail");
        auth.add_rule(AclRule::new("sensor-9", AclAction::All, "#", false))
            .expect("memory-only persist cannot fail");
        auth.add_rule(AclRule::new("*", AclAction::Subscribe, "#", true))
            .expect("memory-only persist cannot fail");

        // sensor-1 publishes under sensors/: allowed by rule 1.
        assert!(auth
            .authorize_publish("sensor-1", &topic("sensors/temp"))
            .await
            .is_ok());
        // sensor-1 outside its branch: no rule matches -> denied.
        assert!(auth
            .authorize_publish("sensor-1", &topic("actuators/door"))
            .await
            .is_err());
        // sensor-9 is explicitly denied everything despite rule 3.
        assert!(auth
            .authorize_publish("sensor-9", &topic("sensors/temp"))
            .await
            .is_err());
        assert!(auth
            .authorize_subscribe("sensor-9", &filter("sensors/+"))
            .await
            .is_err());
        // Subscribes for everyone else fall through to the catch-all allow.
        assert!(auth
            .authorize_subscribe("random", &filter("a/b"))
            .await
            .is_ok());
    }

    #[tokio::test]
    async fn test_acl_subscribe_needs_covering_pattern() {
        let auth = MemoryAuth::new();
        auth.add_rule(AclRule::new(
            "*",
            AclAction::Subscribe,
            "sensors/temp",
            true,
        ))
        .expect("memory-only persist cannot fail");

        // Exact grant covers the exact request.
        assert!(auth
            .authorize_subscribe("c", &filter("sensors/temp"))
            .await
            .is_ok());
        // A concrete grant never covers a broader wildcard request.
        assert!(auth
            .authorize_subscribe("c", &filter("sensors/+"))
            .await
            .is_err());
        assert!(auth.authorize_subscribe("c", &filter("#")).await.is_err());
    }

    #[tokio::test]
    async fn test_acl_action_scoping() {
        let auth = MemoryAuth::new();
        auth.add_rule(AclRule::new("pub-only", AclAction::Publish, "#", true))
            .expect("memory-only persist cannot fail");

        assert!(auth
            .authorize_publish("pub-only", &topic("x"))
            .await
            .is_ok());
        // Publish-only grant does not cover subscribes.
        assert!(auth
            .authorize_subscribe("pub-only", &filter("x"))
            .await
            .is_err());
    }
    #[test]
    fn test_acl_action_parse() {
        assert_eq!(AclAction::parse("publish"), Some(AclAction::Publish));
        assert_eq!(AclAction::parse("SUBSCRIBE"), Some(AclAction::Subscribe));
        assert_eq!(AclAction::parse("All"), Some(AclAction::All));
        assert_eq!(AclAction::parse("delete"), None);
    }

    #[tokio::test]
    async fn test_acl_client_mqtt_wildcard_pattern() {
        let auth = MemoryAuth::new();
        // "+" as a full pattern matches any slash-free client id.
        auth.add_rule(AclRule::new("+", AclAction::Publish, "#", true))
            .expect("memory-only persist cannot fail");
        assert!(auth.authorize_publish("edge-1", &topic("a")).await.is_ok());
        // Exact patterns still require equality.
        auth.add_rule(AclRule::new("nope", AclAction::Publish, "#", false))
            .expect("memory-only persist cannot fail");
        assert!(auth.authorize_publish("edge-1", &topic("a")).await.is_ok());
    }

    /// Unique scratch data dir under the OS temp dir (no dev-dependency).
    fn unique_data_dir(prefix: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "broker-auth-{prefix}-{}-{nanos}",
            std::process::id()
        ))
    }

    /// Seed one registry-backed store with users plus representative
    /// allow and deny rules (mirrors the probe matrix below).
    fn seed_restart_fixture(auth: &MemoryAuth) {
        auth.add_user("sensor-1", b"s3cret-1")
            .expect("persist fixture user");
        auth.add_user("sensor-9", b"s3cret-9")
            .expect("persist fixture user");
        for rule in [
            AclRule::new("sensor-1", AclAction::Publish, "sensors/#", true),
            AclRule::new("sensor-9", AclAction::All, "#", false),
            AclRule::new("*", AclAction::Subscribe, "#", true),
        ] {
            auth.add_rule(rule).expect("persist fixture rule");
        }
    }

    /// Probe matrix over (user, action, topic): each entry names the
    /// client, whether it publishes (`true`) or subscribes (`false`),
    /// the topic/filter, and the expected decision.
    fn restart_probes() -> Vec<(&'static str, bool, &'static str, bool)> {
        vec![
            ("sensor-1", true, "sensors/temp", true),
            ("sensor-1", true, "actuators/door", false),
            ("sensor-1", false, "sensors/temp", true),
            ("sensor-9", true, "sensors/temp", false),
            ("sensor-9", false, "sensors/+", false),
            ("random", false, "a/b", true),
            ("random", true, "a/b", false),
        ]
    }

    async fn check_restart_probes(auth: &MemoryAuth) {
        for (client, publish, target, allowed) in restart_probes() {
            let decision = if publish {
                auth.authorize_publish(client, &topic(target)).await.is_ok()
            } else {
                auth.authorize_subscribe(client, &filter(target))
                    .await
                    .is_ok()
            };
            assert_eq!(
                decision, allowed,
                "probe (client={client}, publish={publish}, target={target}) must be {allowed}"
            );
        }
    }

    #[tokio::test]
    async fn mqtt_users_acls_survive_restart() {
        let dir = unique_data_dir("restart");
        let registry =
            Arc::new(broker_config::ConfigRegistry::load(&dir).expect("fresh data dir loads"));
        let auth = MemoryAuth::from_registry(&registry);
        seed_restart_fixture(&auth);
        assert!(dir.join(broker_config::STATE_FILE_NAME).is_file());
        check_restart_probes(&auth).await;

        // Rebuild from the same data dir (simulating a kernel restart).
        let reloaded =
            Arc::new(broker_config::ConfigRegistry::load(&dir).expect("data dir reloads"));
        let restarted = MemoryAuth::from_registry(&reloaded);
        assert_eq!(restarted.usernames(), vec!["sensor-1", "sensor-9"]);
        check_restart_probes(&restarted).await;
        // The reloaded store still authenticates with the same passwords.
        assert!(restarted
            .authenticate("device-1", Some("sensor-1"), Some(b"s3cret-1"))
            .await
            .is_ok());
        assert!(restarted
            .authenticate("device-9", Some("sensor-9"), Some(b"s3cret-9"))
            .await
            .is_ok());
        assert!(restarted
            .authenticate("device-1", Some("sensor-1"), Some(b"wrong"))
            .await
            .is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn mqtt_user_delete_survives_restart() {
        let dir = unique_data_dir("delete");
        let registry =
            Arc::new(broker_config::ConfigRegistry::load(&dir).expect("fresh data dir loads"));
        let auth = MemoryAuth::from_registry(&registry);
        seed_restart_fixture(&auth);
        assert!(auth.remove_user("sensor-1").expect("persist user delete"));

        // Rebuild from the same data dir: the deleted user stays gone.
        let reloaded =
            Arc::new(broker_config::ConfigRegistry::load(&dir).expect("data dir reloads"));
        let restarted = MemoryAuth::from_registry(&reloaded);
        assert_eq!(restarted.usernames(), vec!["sensor-9"]);
        assert!(
            restarted
                .authenticate("device-1", Some("sensor-1"), Some(b"s3cret-1"))
                .await
                .is_err(),
            "deleted user must not authenticate after restart"
        );
        assert!(restarted
            .authenticate("device-9", Some("sensor-9"), Some(b"s3cret-9"))
            .await
            .is_ok());

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn mqtt_user_quotas_survive_restart() {
        let dir = unique_data_dir("quotas");
        let registry =
            Arc::new(broker_config::ConfigRegistry::load(&dir).expect("fresh data dir loads"));
        let auth = MemoryAuth::from_registry(&registry);
        auth.add_user("capped", b"pw-capped")
            .expect("persist fixture user");
        auth.set_quotas(
            "capped",
            UserQuotas {
                max_connections: Some(2),
                max_publish_rate: Some(50),
                max_publish_burst: Some(10),
            },
        )
        .expect("persist fixture quotas");
        auth.add_user("plain", b"pw-plain")
            .expect("persist fixture user");
        // Unknown names store nothing and report false.
        assert!(!auth
            .set_quotas(
                "ghost",
                UserQuotas {
                    max_connections: Some(1),
                    ..UserQuotas::default()
                }
            )
            .expect("memory-only persist cannot fail"));
        // A password rotation keeps the configured quotas in memory and
        // on disk.
        auth.add_user("capped", b"pw-capped-new")
            .expect("persist password rotation");
        assert_eq!(
            auth.get_quotas("capped"),
            Some(UserQuotas {
                max_connections: Some(2),
                max_publish_rate: Some(50),
                max_publish_burst: Some(10),
            })
        );

        // Rebuild from the same data dir (simulating a kernel restart).
        let reloaded =
            Arc::new(broker_config::ConfigRegistry::load(&dir).expect("data dir reloads"));
        let restarted = MemoryAuth::from_registry(&reloaded);
        assert_eq!(restarted.usernames(), vec!["capped", "plain"]);
        assert_eq!(
            restarted.get_quotas("capped"),
            Some(UserQuotas {
                max_connections: Some(2),
                max_publish_rate: Some(50),
                max_publish_burst: Some(10),
            }),
            "configured quotas must survive a restart"
        );
        assert_eq!(
            restarted.get_quotas("plain"),
            Some(UserQuotas::default()),
            "unset quotas reload as unlimited"
        );
        // The rotated password (not the original) authenticates.
        assert!(restarted
            .authenticate("device-1", Some("capped"), Some(b"pw-capped-new"))
            .await
            .is_ok());
        assert!(restarted
            .authenticate("device-1", Some("capped"), Some(b"pw-capped"))
            .await
            .is_err());
        // The persisted file stays deterministic (sorted by username).
        assert_eq!(
            reloaded.snapshot().mqtt_users.users,
            restarted.export_conf().users
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn mqtt_user_password_change_survives_restart_without_reseed() {
        let dir = unique_data_dir("pwchange");
        let registry =
            Arc::new(broker_config::ConfigRegistry::load(&dir).expect("fresh data dir loads"));
        let auth = MemoryAuth::from_registry(&registry);
        auth.add_user("sensor-1", b"old-pw")
            .expect("persist fixture user");
        auth.add_user("sensor-1", b"new-pw")
            .expect("persist password change");

        // Rebuild from the same data dir: the new password sticks and no
        // default user is reseeded.
        let reloaded =
            Arc::new(broker_config::ConfigRegistry::load(&dir).expect("data dir reloads"));
        let restarted = MemoryAuth::from_registry(&reloaded);
        assert_eq!(restarted.usernames(), vec!["sensor-1"]);
        assert!(restarted
            .authenticate("device-1", Some("sensor-1"), Some(b"new-pw"))
            .await
            .is_ok());
        assert!(restarted
            .authenticate("device-1", Some("sensor-1"), Some(b"old-pw"))
            .await
            .is_err());

        std::fs::remove_dir_all(&dir).ok();
    }

    /// M1-05: a password supplied as a secret reference authenticates with
    /// the resolved value, while exports hold only the digest (never the
    /// value or the reference bytes).
    #[tokio::test]
    async fn secret_reference_password_authenticates_without_plaintext_export() {
        std::env::set_var("BROKER_AUTH_TEST_SECRET_PW", "correct-horse-9f3a");
        let auth = MemoryAuth::new();
        auth.add_user("bob", b"env:BROKER_AUTH_TEST_SECRET_PW")
            .expect("secret resolves");
        assert!(auth
            .authenticate("device-1", Some("bob"), Some(b"correct-horse-9f3a"))
            .await
            .is_ok());
        assert!(auth
            .authenticate("device-1", Some("bob"), Some(b"wrong"))
            .await
            .is_err());
        let conf = auth.export_conf();
        assert_eq!(conf.users.len(), 1);
        assert!(
            !conf.users[0].password_hash.contains("correct-horse-9f3a"),
            "export must never hold the secret value"
        );
        assert!(
            !conf.users[0]
                .password_hash
                .contains("BROKER_AUTH_TEST_SECRET_PW"),
            "export must never hold the reference"
        );
        std::env::remove_var("BROKER_AUTH_TEST_SECRET_PW");
    }

    /// M1-05: a missing or unreadable secret fails closed, naming the
    /// reference and never the value, with no user stored.
    #[tokio::test]
    async fn missing_secret_reference_fails_closed_without_storing_user() {
        let auth = MemoryAuth::new();
        let reference = "env:BROKER_AUTH_TEST_DEFINITELY_UNSET_9F3A";
        let err = auth
            .add_user("ghost", reference.as_bytes())
            .expect_err("unset secret must fail");
        assert!(
            err.to_string().contains(reference),
            "error must name the reference, got: {err}"
        );
        assert_eq!(auth.user_count(), 0);
    }

    /// MT-06: the same username in two tenants is two independent
    /// identities with independent password hashes. Fast test policy
    /// keeps the suite interactive; the code path is identical.
    #[tokio::test]
    async fn tenant_same_username_authenticates_independently() {
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        auth.add_user_in_tenant("tenant-a", "sam", b"pw-for-a")
            .expect("tenant-a user stores");
        auth.add_user_in_tenant("tenant-b", "sam", b"pw-for-b")
            .expect("tenant-b user stores");
        assert_eq!(auth.user_count(), 2);
        assert_eq!(auth.user_count_in_tenant("tenant-a"), 1);
        assert_eq!(auth.user_count_in_tenant("tenant-b"), 1);
        // Each tenant verifies only its own password.
        assert!(auth
            .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-a"))
            .await
            .is_ok());
        assert!(auth
            .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-b"))
            .await
            .is_err());
        assert!(auth
            .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-b"))
            .await
            .is_ok());
        assert!(auth
            .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-a"))
            .await
            .is_err());
        // The default tenant holds no such user: closed, never granted.
        assert!(auth
            .authenticate("c", Some("sam"), Some(b"pw-for-a"))
            .await
            .is_err());
        // Quotas and probes are per tenant too.
        assert!(auth.has_user_in_tenant("tenant-a", "sam"));
        assert!(!auth.has_user_in_tenant("tenant-b", "alice"));
        assert!(!auth.has_user("sam"));
    }

    /// MT-06: a rule granting a topic in tenant A never grants the same
    /// topic in tenant B; B starts closed (deny, counted, logged).
    #[tokio::test]
    async fn tenant_rule_in_a_does_not_grant_in_b() {
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        auth.add_user_in_tenant("tenant-a", "sam", b"pw-for-a")
            .expect("tenant-a user stores");
        auth.add_user_in_tenant("tenant-b", "sam", b"pw-for-b")
            .expect("tenant-b user stores");
        auth.add_rule_in_tenant(
            "tenant-a",
            AclRule::new("*", AclAction::Publish, "t/#", true),
        )
        .expect("tenant-a rule stores");
        auth.add_rule_in_tenant(
            "tenant-a",
            AclRule::new("*", AclAction::Subscribe, "t/#", true),
        )
        .expect("tenant-a rule stores");
        assert!(auth
            .authorize_publish_in_tenant("tenant-a", "sam", &topic("t/1"))
            .await
            .is_ok());
        assert!(auth
            .authorize_subscribe_in_tenant("tenant-a", "sam", &filter("t/#"))
            .await
            .is_ok());
        // Same topic, same client, other tenant: denied and counted.
        let before = auth.tenant_denial_count();
        assert!(auth
            .authorize_publish_in_tenant("tenant-b", "sam", &topic("t/1"))
            .await
            .is_err());
        assert!(auth
            .authorize_subscribe_in_tenant("tenant-b", "sam", &filter("t/#"))
            .await
            .is_err());
        assert!(
            auth.tenant_denial_count() > before,
            "closed-tenant authorisation denials must be counted"
        );
        // Default-tenant shims observe only the default tenant: with no
        // default rules the default keeps today's open rule set, while
        // credentialed default use denies (its user map is empty while
        // other tenants hold users — fail closed).
        assert!(auth.authorize_publish("sam", &topic("t/1")).await.is_ok());
    }

    /// MT-06: unknown and invalid tenants deny (fail closed, counted,
    /// logged); the default tenant seeded from today's tables behaves
    /// exactly as today (same passwords, same rules, same
    /// open-when-empty, same deny-if-unmatched).
    #[tokio::test]
    async fn tenant_unknown_and_invalid_deny_while_default_keeps_today_behaviour() {
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        // Empty store: today's open behaviour on the default tenant.
        assert!(auth.authenticate("c", None, None).await.is_ok());
        assert!(auth.authorize_publish("c", &topic("t/1")).await.is_ok());
        // Seed the default tenant exactly as today.
        auth.add_user("base", b"base-pw")
            .expect("default user stores");
        auth.add_rule(AclRule::new("base", AclAction::Publish, "t/#", true))
            .expect("default rule stores");
        assert!(auth
            .authenticate("c", Some("base"), Some(b"base-pw"))
            .await
            .is_ok());
        assert!(auth
            .authenticate("c", Some("base"), Some(b"wrong"))
            .await
            .is_err());
        assert!(auth.authorize_publish("base", &topic("t/1")).await.is_ok());
        // Same client outside its grant: deny-if-unmatched, unchanged.
        assert!(auth
            .authorize_publish("base", &topic("other/1"))
            .await
            .is_err());
        // Unknown tenant: every lookup denies, counted.
        let before = auth.tenant_denial_count();
        assert!(auth
            .authenticate_in_tenant("no-such-tenant", "c", Some("base"), Some(b"base-pw"))
            .await
            .is_err());
        assert!(auth
            .authorize_publish_in_tenant("no-such-tenant", "base", &topic("t/1"))
            .await
            .is_err());
        assert!(auth
            .authorize_subscribe_in_tenant("no-such-tenant", "base", &filter("t/#"))
            .await
            .is_err());
        assert!(
            auth.tenant_denial_count() > before,
            "unknown-tenant denials must be counted"
        );
        // Invalid tenant ids (control characters, overlong) deny too.
        assert!(auth
            .authenticate_in_tenant("bad-\u{0001}-id", "c", Some("base"), Some(b"base-pw"))
            .await
            .is_err());
        assert!(auth
            .authorize_publish_in_tenant(&"x".repeat(MAX_TENANT_ID_LEN + 1), "base", &topic("t/1"))
            .await
            .is_err());
    }

    /// MT-06: new tenants start closed (no users, no rules, deny) and an
    /// unreachable credential backend denies access instead of granting
    /// it. The unreachable directory is a real dial attempt against a
    /// closed loopback port (no mock transport), failing closed.
    #[tokio::test]
    async fn tenant_new_tenant_closed_and_unreachable_backend_denies() {
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        auth.add_user_in_tenant("tenant-a", "sam", b"pw-for-a")
            .expect("tenant-a user stores");
        // Tenant B was never created: closed on every lookup, counted.
        let before = auth.tenant_denial_count();
        assert!(auth
            .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-a"))
            .await
            .is_err());
        assert!(auth
            .authorize_publish_in_tenant("tenant-b", "sam", &topic("t/1"))
            .await
            .is_err());
        assert!(
            auth.tenant_denial_count() > before,
            "new-tenant closed denials must be counted"
        );
        // Unreachable directory: real dial to a closed loopback port with
        // short timeouts, must deny (fail closed, never grant).
        let ldap = crate::ldap::LdapAuthenticator::new(crate::ldap::LdapConfig {
            server_url: "ldap://127.0.0.1:9".to_string(),
            base_dn: "dc=example,dc=com".to_string(),
            bind_dn: "cn=svc,dc=example,dc=com".to_string(),
            bind_password: "svc-pw".to_string(),
            connect_timeout_ms: 500,
            read_timeout_ms: 500,
            ..Default::default()
        });
        assert!(ldap
            .authenticate_in_tenant("default", "c", Some("sam"), Some(b"pw-for-a"))
            .await
            .is_err());
        assert!(ldap
            .authenticate("c", Some("sam"), Some(b"pw-for-a"))
            .await
            .is_err());
    }

    /// MT-06: per-tenant rule tables carry finite bounds with stated
    /// defaults and reasons; writes past the caps are rejected.
    #[test]
    fn tenant_bounds_are_finite_and_enforced() {
        const {
            assert!(MAX_AUTH_TENANTS > 0 && MAX_AUTH_TENANTS <= 100_000);
            assert!(MAX_ACL_RULES_PER_TENANT > 0 && MAX_ACL_RULES_PER_TENANT <= 1_000_000);
            assert!(MAX_TENANT_ID_LEN > 0 && MAX_TENANT_ID_LEN <= 1024);
        }
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        for _ in 0..MAX_ACL_RULES_PER_TENANT {
            auth.add_rule_in_tenant(
                "tenant-cap",
                AclRule::new("*", AclAction::Publish, "t/#", true),
            )
            .expect("rules store up to the cap");
        }
        let err = auth
            .add_rule_in_tenant(
                "tenant-cap",
                AclRule::new("*", AclAction::Publish, "t/#", true),
            )
            .expect_err("rule past the per-tenant cap must fail");
        assert!(
            err.to_string().contains("acls"),
            "error must name the field, got: {err}"
        );
    }

    /// TM-01: two tenants hold the same username with different passwords
    /// and different rules. Each username authenticates only in its own
    /// tenant, through the CONNECT-time lookup (not the map alone).
    #[tokio::test]
    async fn tm01_two_tenants_share_username_with_independent_credentials() {
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        auth.add_user_in_tenant("tenant-a", "sam", b"pw-for-a")
            .expect("tenant-a user stores");
        auth.add_user_in_tenant("tenant-b", "sam", b"pw-for-b")
            .expect("tenant-b user stores");
        auth.add_rule_in_tenant(
            "tenant-a",
            AclRule::new("*", AclAction::Publish, "t/a/#", true),
        )
        .expect("tenant-a rule stores");
        auth.add_rule_in_tenant(
            "tenant-b",
            AclRule::new("*", AclAction::Publish, "t/b/#", true),
        )
        .expect("tenant-b rule stores");
        // Each password works only in its own tenant.
        assert!(auth
            .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-a"))
            .await
            .is_ok());
        assert!(auth
            .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-b"))
            .await
            .is_ok());
        assert!(auth
            .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-b"))
            .await
            .is_err());
        assert!(auth
            .authenticate_in_tenant("tenant-b", "c", Some("sam"), Some(b"pw-for-a"))
            .await
            .is_err());
        // Each rule grants only in its own tenant.
        assert!(auth
            .authorize_publish_in_tenant("tenant-a", "sam", &topic("t/a/1"))
            .await
            .is_ok());
        assert!(auth
            .authorize_publish_in_tenant("tenant-a", "sam", &topic("t/b/1"))
            .await
            .is_err());
        assert!(auth
            .authorize_publish_in_tenant("tenant-b", "sam", &topic("t/b/1"))
            .await
            .is_ok());
        assert!(auth
            .authorize_publish_in_tenant("tenant-b", "sam", &topic("t/a/1"))
            .await
            .is_err());
    }

    /// TM-01: the exported snapshot carries both tenants. A fresh store
    /// seeded from it (restart) and the same store re-seeded from it
    /// (reload) keep both tenants with the same passwords and rules.
    #[tokio::test]
    async fn tm01_restart_and_reload_keep_both_tenants() {
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        auth.add_user_in_tenant("tenant-a", "sam", b"pw-for-a")
            .expect("tenant-a user stores");
        auth.add_user_in_tenant("tenant-b", "sam", b"pw-for-b")
            .expect("tenant-b user stores");
        auth.add_rule_in_tenant(
            "tenant-a",
            AclRule::new("*", AclAction::Publish, "t/a/#", true),
        )
        .expect("tenant-a rule stores");
        auth.add_rule_in_tenant(
            "tenant-b",
            AclRule::new("*", AclAction::Subscribe, "t/b/#", true),
        )
        .expect("tenant-b rule stores");
        // The export carries both tables, tenants sorted.
        let exported = auth.export_conf();
        assert_eq!(exported.tenants.len(), 2);
        assert_eq!(exported.tenants[0].tenant, "tenant-a");
        assert_eq!(exported.tenants[1].tenant, "tenant-b");
        // Restart: a fresh store from the exported root keeps both.
        let rebooted = MemoryAuth::from_snapshot(&exported);
        for (tenant, password) in [("tenant-a", b"pw-for-a"), ("tenant-b", b"pw-for-b")] {
            assert!(rebooted
                .authenticate_in_tenant(tenant, "c", Some("sam"), Some(password))
                .await
                .is_ok());
        }
        assert!(rebooted
            .authorize_publish_in_tenant("tenant-a", "sam", &topic("t/a/1"))
            .await
            .is_ok());
        assert!(rebooted
            .authorize_subscribe_in_tenant("tenant-b", "sam", &filter("t/b/#"))
            .await
            .is_ok());
        // Reload: the live store re-seeded from the same root keeps both.
        auth.apply_snapshot_conf(&exported)
            .expect("reload of the exported root succeeds");
        for (tenant, password) in [("tenant-a", b"pw-for-a"), ("tenant-b", b"pw-for-b")] {
            assert!(auth
                .authenticate_in_tenant(tenant, "c", Some("sam"), Some(password))
                .await
                .is_ok());
        }
    }

    /// TM-01: a snapshot from before per-tenant storage (root fields
    /// only, no `tenants` list) loads as the default tenant with the same
    /// passwords and rules; other tenants stay closed.
    #[tokio::test]
    async fn tm01_pre_change_snapshot_loads_as_default_tenant() {
        let conf = MqttUsersConf {
            users: vec![MqttUser {
                username: "base".to_string(),
                password_hash: encode_stored_verifier(&PasswordVerifier::Sha256(
                    MemoryAuth::digest(b"base-pw"),
                )),
                max_connections: None,
                max_publish_rate: None,
                max_publish_burst: None,
            }],
            acls: vec![AclConf {
                username: "base".to_string(),
                topic: "t/#".to_string(),
                action: "all".to_string(),
                allow: true,
            }],
            tenants: vec![],
        };
        let auth = MemoryAuth::from_snapshot(&conf);
        assert!(auth
            .authenticate("c", Some("base"), Some(b"base-pw"))
            .await
            .is_ok());
        assert!(auth
            .authenticate("c", Some("base"), Some(b"wrong"))
            .await
            .is_err());
        assert!(auth.authorize_publish("base", &topic("t/1")).await.is_ok());
        // Another tenant with no table stays closed (deny, counted).
        let before = auth.tenant_denial_count();
        assert!(auth
            .authenticate_in_tenant("tenant-a", "c", Some("base"), Some(b"base-pw"))
            .await
            .is_err());
        assert!(
            auth.tenant_denial_count() > before,
            "unknown-tenant denials stay counted"
        );
    }

    /// TM-01: each bad per-tenant write fails with the field named and
    /// leaves the running state unchanged.
    #[tokio::test]
    async fn tm01_bad_tenant_write_fails_and_keeps_running_state() {
        let auth = MemoryAuth::with_policy(PasswordHashPolicy::for_tests());
        auth.add_user_in_tenant("tenant-a", "sam", b"pw-for-a")
            .expect("tenant-a user stores");
        // A snapshot that names the default tenant inside `tenants` is
        // invalid; the reload rejects it and the live user still works.
        let mut bad = auth.export_conf();
        bad.tenants.push(broker_config::TenantMqttConf {
            tenant: "default".to_string(),
            users: vec![],
            acls: vec![],
        });
        let err = bad
            .validate()
            .expect_err("default tenant inside `tenants` must fail");
        assert!(err.to_string().contains("tenant"), "got: {err}");
        assert!(auth.apply_snapshot_conf(&bad).is_err());
        assert!(auth
            .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-a"))
            .await
            .is_ok());
        // A duplicated tenant table is invalid too; state stays.
        let mut dup = auth.export_conf();
        dup.tenants.push(broker_config::TenantMqttConf {
            tenant: "tenant-a".to_string(),
            users: vec![],
            acls: vec![],
        });
        let err = dup.validate().expect_err("duplicated tenant must fail");
        assert!(err.to_string().contains("tenant"), "got: {err}");
        assert!(auth
            .authenticate_in_tenant("tenant-a", "c", Some("sam"), Some(b"pw-for-a"))
            .await
            .is_ok());
    }
}
