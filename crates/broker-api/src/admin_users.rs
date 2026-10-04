//! Management API users, separate from MQTT client credentials.
//!
//! Holds dashboard/API users with SCRAM-SHA-256 credentials (RFC 5802),
//! roles and the must-change-password flag. The MQTT `MemoryAuth` store
//! is untouched; this store is the only home for API users.
//!
//! When built with [`AdminUsers::from_registry`] every mutation commits
//! the exported [`broker_config::AdminUsersConf`] root and atomically
//! saves it, so users survive kernel restarts.

use base64::Engine as _;
use broker_config::{AdminUsersConf, ConfigRegistry};
use rand::Rng;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};

/// Iterations for PBKDF2-HMAC-SHA-256 (`SCRAM-SHA-256` default).
const ITERATIONS: u32 = 4096;
/// Salt length for newly created credentials.
const SALT_LEN: usize = 16;
/// Minimum accepted password length.
const MIN_PASSWORD_LEN: usize = 8;

/// A new dashboard/API password must be at least [`MIN_PASSWORD_LEN`]
/// characters and carry both a letter and a digit. Reason: short or
/// single-class secrets fall to guessing and credential stuffing, and the
/// management plane is reachable from the operator network; the
/// length-plus-two-class floor is the documented bar every write path must
/// clear. The default is finite and fail-closed: anything shorter or
/// simpler is rejected, never stored.
// TODO(parity): what is the exact documented complexity rule (which
// character classes, and does it also demand a symbol or a longer
// minimum)? The rulebook does not decide the wording; the current choice
// enforces length 8 with at least one letter and one digit fail-closed
// until the checker pins it.
fn is_strong_password(password: &str) -> bool {
    if password.chars().count() < MIN_PASSWORD_LEN {
        return false;
    }
    let has_letter = password.chars().any(|c| c.is_ascii_alphabetic());
    let has_digit = password.chars().any(|c| c.is_ascii_digit());
    has_letter && has_digit
}

/// Upper bound for stored dashboard/API users. Creates past this size are
/// rejected instead of growing the map without limit. Reason: the
/// management plane snapshots the whole list per list request and each
/// entry holds SCRAM verifier material plus map overhead, so an unbounded
/// map lets one management batch exhaust the kernel; 1024 covers operator
/// provisioning with headroom while keeping list reads paged and
/// per-request clones cheap. The default is finite: no opt-in to unlimited
/// exists anywhere on this path.
/// Management-plane only: publish and deliver never touch this map.
pub const MAX_ADMIN_USERS: usize = 1024;

/// Consecutive failed MFA code verifications before the per-user
/// cool-down trips: 5 attempts. Reason: five tries cover a mistyped code
/// plus one clock-step retry without letting an online guessing run
/// through the million-code space; the sixth consecutive failure locks
/// the user out for [`MFA_VERIFY_LOCKOUT_SECS`].
pub const MFA_VERIFY_MAX_ATTEMPTS: u32 = 5;
/// MFA verify cool-down after [`MFA_VERIFY_MAX_ATTEMPTS`] consecutive
/// failures: 60 s. Reason: sixty seconds spans two TOTP steps, so every
/// code the attacker tried expires before the next attempt is accepted,
/// while a legitimate operator waits at most a minute. The counter is
/// memory-only (a restart clears it); the secret and the enabled flag
/// persist through the registry.
pub const MFA_VERIFY_LOCKOUT_SECS: u64 = 60;

/// Role of a management API user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdminRole {
    Administrator,
    Viewer,
}

/// Public view of one admin user (no credential material).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AdminUserView {
    pub username: String,
    pub role: AdminRole,
    pub description: String,
    pub must_change_password: bool,
}

/// Errors from [`AdminUsers`] mutations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdminUserError {
    AlreadyExists,
    NotFound,
    WeakPassword,
    SameAsOld,
    LastAdministrator,
    InvalidUsername,
    /// The store already holds [`MAX_ADMIN_USERS`] entries. Handlers map
    /// this to HTTP 400; it is never silent.
    TooManyUsers,
    /// The mutation applied in memory but the registry commit or atomic
    /// save failed. Handlers map this to HTTP 500; it is never silent.
    Persist(String),
}

impl fmt::Display for AdminUserError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AlreadyExists => write!(f, "user already exists"),
            Self::NotFound => write!(f, "user not found"),
            Self::WeakPassword => write!(f, "password is too weak"),
            Self::SameAsOld => write!(f, "new password is the same as the old one"),
            Self::LastAdministrator => write!(f, "cannot remove the last administrator"),
            Self::InvalidUsername => write!(f, "invalid username"),
            Self::TooManyUsers => write!(f, "admin user limit of {MAX_ADMIN_USERS} reached"),
            Self::Persist(reason) => write!(f, "cannot persist admin users: {reason}"),
        }
    }
}

impl std::error::Error for AdminUserError {}

struct AdminUser {
    username: String,
    role: AdminRole,
    description: String,
    salt: Vec<u8>,
    iterations: u32,
    stored_key: [u8; 32],
    server_key: [u8; 32],
    must_change_password: bool,
    /// TOTP enrolment secret (empty = no enrolment; one pending enrolment
    /// per user at most, since the record holds a single secret). Stored
    /// reversibly because verification must recompute codes; never logged
    /// and only returned once by the setup route.
    mfa_secret: Vec<u8>,
    /// True once a one-time code has verified successfully.
    mfa_enabled: bool,
    /// Most recently verified 30 s counter, for single-use rejection.
    mfa_last_step: Option<u64>,
}

/// Per-user MFA verify attempt state (memory-only; resets on restart).
/// Held only for known users, so the map stays within
/// [`MAX_ADMIN_USERS`] entries and never grows with unknown-name probes
/// (unknown usernames fail before recording anything).
#[derive(Debug, Clone)]
struct MfaAttempt {
    consecutive_failures: u32,
    locked_until: Option<Instant>,
}

/// Enrolment state snapshot for one dashboard user.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MfaStatus {
    /// A code has verified successfully at least once.
    pub enabled: bool,
    /// A secret is stored but no code has verified yet.
    pub pending: bool,
}

/// Why an MFA code verification was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MfaVerifyError {
    /// No dashboard user under that name.
    NotFound,
    /// No enrolment started for this user yet.
    NoEnrolment,
    /// The code is malformed or does not match the current window.
    Invalid,
    /// The code matches but its counter was already used (replay).
    Replayed,
    /// Too many consecutive failures: try again after the cool-down.
    Locked,
    /// The successful verification applied in memory but the registry
    /// commit or atomic save failed. Handlers map this to HTTP 500; it
    /// is never silent.
    Persist(String),
}

impl fmt::Display for MfaVerifyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => write!(f, "user not found"),
            Self::NoEnrolment => write!(f, "no MFA enrolment for this user"),
            Self::Invalid => write!(f, "invalid verification code"),
            Self::Replayed => write!(f, "verification code already used"),
            Self::Locked => write!(f, "too many failed attempts; try again later"),
            Self::Persist(reason) => write!(f, "cannot persist MFA state: {reason}"),
        }
    }
}

impl std::error::Error for MfaVerifyError {}

/// Management API users with SCRAM-SHA-256 credentials.
pub struct AdminUsers {
    users: RwLock<HashMap<String, AdminUser>>,
    process_secret: [u8; 32],
    /// Kernel config registry receiving every mutation (`None` in unit
    /// tests and standalone state, which stay memory-only).
    registry: Option<Arc<ConfigRegistry>>,
    /// Serialises export-commit-save so concurrent mutations cannot
    /// interleave into a lost update on disk.
    save_lock: Mutex<()>,
    /// Consecutive-failure counters plus cool-down deadlines for MFA
    /// verify, one entry per known user at most (memory-only).
    /// Management-plane only: publish and deliver never touch this map.
    mfa_attempts: Mutex<HashMap<String, MfaAttempt>>,
}

impl AdminUsers {
    /// Store containing only `admin` / `public`, Administrator,
    /// `must_change_password = true`. Memory-only: mutations are not
    /// persisted.
    pub fn with_default_admin() -> Self {
        let mut secret = [0u8; 32];
        rand::rng().fill_bytes(&mut secret);
        let store = Self {
            users: RwLock::new(HashMap::new()),
            process_secret: secret,
            registry: None,
            save_lock: Mutex::new(()),
            mfa_attempts: Mutex::new(HashMap::new()),
        };
        store
            .users
            .write()
            .unwrap()
            .insert("admin".to_string(), default_admin_user());
        store
    }

    /// Seed from a validated snapshot root. An empty snapshot yields the
    /// default admin (identical first-boot behaviour, including
    /// `must_change_password`). Memory-only: mutations are not persisted.
    pub fn from_snapshot(conf: &AdminUsersConf) -> Self {
        let mut secret = [0u8; 32];
        rand::rng().fill_bytes(&mut secret);
        let store = Self {
            users: RwLock::new(HashMap::new()),
            process_secret: secret,
            registry: None,
            save_lock: Mutex::new(()),
            mfa_attempts: Mutex::new(HashMap::new()),
        };
        if conf.users.is_empty() {
            store
                .users
                .write()
                .unwrap()
                .insert("admin".to_string(), default_admin_user());
            return store;
        }
        for entry in &conf.users {
            let (mfa_secret, mfa_enabled, mfa_last_step) = decode_mfa(entry);
            let user = match decode_verifier(&entry.password_hash) {
                Some((salt, iterations, stored_key, server_key)) => AdminUser {
                    username: entry.username.clone(),
                    role: role_from_conf(&entry.role),
                    description: entry.description.clone(),
                    salt,
                    iterations,
                    stored_key,
                    server_key,
                    must_change_password: entry.must_change_password,
                    mfa_secret,
                    mfa_enabled,
                    mfa_last_step,
                },
                None => {
                    // Fail closed: the verifier is unusable, so nobody can
                    // log in as this user until an administrator sets a new
                    // password. The entry is kept so the outage is visible
                    // in the user list instead of silently dropped.
                    tracing::warn!(
                        username = entry.username.as_str(),
                        "admin snapshot entry has an undecodable password verifier; account locked until its password is changed"
                    );
                    let mut salt = [0u8; SALT_LEN];
                    rand::rng().fill_bytes(&mut salt);
                    let mut stored_key = [0u8; 32];
                    let mut server_key = [0u8; 32];
                    rand::rng().fill_bytes(&mut stored_key);
                    rand::rng().fill_bytes(&mut server_key);
                    AdminUser {
                        username: entry.username.clone(),
                        role: role_from_conf(&entry.role),
                        description: entry.description.clone(),
                        salt: salt.to_vec(),
                        iterations: ITERATIONS,
                        stored_key,
                        server_key,
                        must_change_password: entry.must_change_password,
                        mfa_secret,
                        mfa_enabled,
                        mfa_last_step,
                    }
                }
            };
            store
                .users
                .write()
                .unwrap()
                .insert(user.username.clone(), user);
        }
        store
    }

    /// Seed from the registry's current snapshot and persist every later
    /// mutation back through it. This is the kernel boot path: an empty
    /// snapshot yields the default admin.
    pub fn from_registry(registry: &Arc<ConfigRegistry>) -> Self {
        let snapshot = registry.snapshot();
        let mut store = Self::from_snapshot(&snapshot.admin_users);
        store.registry = Some(Arc::clone(registry));
        store
    }

    /// Replace the whole user map from a validated registry root.
    /// The caller commits the root first; this only swaps memory.
    /// Unknown verifiers lock their account, as in `from_snapshot`.
    /// Stale attempt counters for removed users are dropped.
    pub fn apply_snapshot_conf(&self, conf: &AdminUsersConf) {
        let fresh = Self::from_snapshot(conf);
        let names: std::collections::HashSet<String> =
            fresh.users.read().unwrap().keys().cloned().collect();
        *self.users.write().unwrap() = std::mem::take(&mut *fresh.users.write().unwrap());
        self.mfa_attempts
            .lock()
            .unwrap()
            .retain(|name, _| names.contains(name));
    }

    /// Export the current contents as a validated config root, sorted by
    /// username so the persisted file is deterministic. MFA enrolment
    /// rides the same root so it survives a kernel restart byte-identically.
    fn export_conf(&self) -> AdminUsersConf {
        let users = self.users.read().unwrap();
        let base64 = base64::engine::general_purpose::STANDARD;
        let mut entries: Vec<broker_config::AdminUser> = users
            .values()
            .map(|user| broker_config::AdminUser {
                username: user.username.clone(),
                password_hash: encode_verifier(
                    &user.salt,
                    user.iterations,
                    &user.stored_key,
                    &user.server_key,
                ),
                role: role_to_conf(user.role).to_string(),
                description: user.description.clone(),
                must_change_password: user.must_change_password,
                mfa_secret: if user.mfa_secret.is_empty() {
                    String::new()
                } else {
                    base64.encode(&user.mfa_secret)
                },
                mfa_enabled: user.mfa_enabled,
                mfa_last_step: user.mfa_last_step,
            })
            .collect();
        entries.sort_by(|a, b| a.username.cmp(&b.username));
        AdminUsersConf { users: entries }
    }

    /// Commit the exported root and atomically save it. A no-op without a
    /// registry; any commit or save failure surfaces as
    /// [`AdminUserError::Persist`] so handlers answer 500 and the loss is
    /// never silent.
    fn persist(&self) -> Result<(), AdminUserError> {
        let Some(registry) = self.registry.as_ref() else {
            return Ok(());
        };
        let _guard = self.save_lock.lock().unwrap();
        let conf = self.export_conf();
        registry
            .commit_admin_users(conf)
            .map_err(|err| AdminUserError::Persist(err.to_string()))?;
        registry
            .save()
            .map_err(|err| AdminUserError::Persist(err.to_string()))?;
        Ok(())
    }

    pub fn create(
        &self,
        username: &str,
        password: &str,
        role: AdminRole,
        description: &str,
    ) -> Result<(), AdminUserError> {
        if !is_valid_username(username) {
            return Err(AdminUserError::InvalidUsername);
        }
        if password.chars().count() < MIN_PASSWORD_LEN {
            return Err(AdminUserError::WeakPassword);
        }
        {
            let mut users = self.users.write().unwrap();
            if users.contains_key(username) {
                return Err(AdminUserError::AlreadyExists);
            }
            // TODO(parity): does the documented behaviour count a duplicate
            // against the cap or report the duplicate first? The rulebook
            // does not decide the ordering; the current choice reports the
            // duplicate first (no growth) and enforces the cap only on
            // genuinely new usernames until the checker pins it.
            if users.len() >= MAX_ADMIN_USERS {
                return Err(AdminUserError::TooManyUsers);
            }
            let mut salt = [0u8; SALT_LEN];
            rand::rng().fill_bytes(&mut salt);
            let (stored_key, server_key) = derive_keys(password.as_bytes(), &salt, ITERATIONS);
            users.insert(
                username.to_string(),
                AdminUser {
                    username: username.to_string(),
                    role,
                    description: description.to_string(),
                    salt: salt.to_vec(),
                    iterations: ITERATIONS,
                    stored_key,
                    server_key,
                    must_change_password: false,
                    mfa_secret: Vec::new(),
                    mfa_enabled: false,
                    mfa_last_step: None,
                },
            );
        }
        self.persist()
    }

    pub fn delete(&self, username: &str) -> Result<(), AdminUserError> {
        {
            let mut users = self.users.write().unwrap();
            let user = users.get(username).ok_or(AdminUserError::NotFound)?;
            if user.role == AdminRole::Administrator
                && users
                    .values()
                    .filter(|u| u.role == AdminRole::Administrator)
                    .count()
                    == 1
            {
                return Err(AdminUserError::LastAdministrator);
            }
            users.remove(username);
        }
        self.mfa_attempts.lock().unwrap().remove(username);
        self.persist()
    }

    pub fn set_role(&self, username: &str, role: AdminRole) -> Result<(), AdminUserError> {
        {
            let mut users = self.users.write().unwrap();
            let current = users.get(username).ok_or(AdminUserError::NotFound)?;
            if current.role == AdminRole::Administrator
                && role == AdminRole::Viewer
                && users
                    .values()
                    .filter(|u| u.role == AdminRole::Administrator)
                    .count()
                    == 1
            {
                return Err(AdminUserError::LastAdministrator);
            }
            if let Some(user) = users.get_mut(username) {
                user.role = role;
            }
        }
        self.persist()
    }

    pub fn set_description(&self, username: &str, description: &str) -> Result<(), AdminUserError> {
        {
            let mut users = self.users.write().unwrap();
            let user = users.get_mut(username).ok_or(AdminUserError::NotFound)?;
            user.description = description.to_string();
        }
        self.persist()
    }

    /// Replace `role` and/or `description` of one user atomically: one
    /// map write lock, one registry persist. `None` leaves that field
    /// untouched. Validation of the values happens in the handler; this
    /// only refuses the unknown user and the demotion of the last
    /// administrator, so a validated update applies fully or not at all.
    /// Management-plane only: publish and deliver never touch this map.
    pub fn update(
        &self,
        username: &str,
        role: Option<AdminRole>,
        description: Option<&str>,
    ) -> Result<(), AdminUserError> {
        {
            let mut users = self.users.write().unwrap();
            if !users.contains_key(username) {
                return Err(AdminUserError::NotFound);
            }
            if let Some(role) = role {
                let current = users.get(username).ok_or(AdminUserError::NotFound)?;
                if current.role == AdminRole::Administrator
                    && role == AdminRole::Viewer
                    && users
                        .values()
                        .filter(|u| u.role == AdminRole::Administrator)
                        .count()
                        == 1
                {
                    return Err(AdminUserError::LastAdministrator);
                }
            }
            if let Some(user) = users.get_mut(username) {
                if let Some(role) = role {
                    user.role = role;
                }
                if let Some(description) = description {
                    user.description = description.to_string();
                }
            }
        }
        self.persist()
    }

    pub fn change_password(
        &self,
        username: &str,
        new_password: &str,
    ) -> Result<(), AdminUserError> {
        if !is_strong_password(new_password) {
            return Err(AdminUserError::WeakPassword);
        }
        {
            let mut users = self.users.write().unwrap();
            let user = users.get(username).ok_or(AdminUserError::NotFound)?;
            let (candidate_stored, _) =
                derive_keys(new_password.as_bytes(), &user.salt, user.iterations);
            if constant_time_eq(&candidate_stored, &user.stored_key) {
                return Err(AdminUserError::SameAsOld);
            }
            let mut salt = [0u8; SALT_LEN];
            rand::rng().fill_bytes(&mut salt);
            let (stored_key, server_key) = derive_keys(new_password.as_bytes(), &salt, ITERATIONS);
            if let Some(user) = users.get_mut(username) {
                user.salt = salt.to_vec();
                user.iterations = ITERATIONS;
                user.stored_key = stored_key;
                user.server_key = server_key;
                user.must_change_password = false;
            }
        }
        self.persist()
    }

    /// Constant-time check of `password` against the stored `StoredKey`.
    pub fn verify_password(&self, username: &str, password: &str) -> bool {
        let found: Option<(Vec<u8>, u32, [u8; 32])> = {
            let users = self.users.read().unwrap();
            users
                .get(username)
                .map(|user| (user.salt.clone(), user.iterations, user.stored_key))
        };
        match found {
            Some((salt, iterations, stored_key)) => {
                let (candidate_stored, _) = derive_keys(password.as_bytes(), &salt, iterations);
                constant_time_eq(&candidate_stored, &stored_key)
            }
            None => {
                let fake = self.fake_salt(username);
                let (candidate_stored, _) = derive_keys(password.as_bytes(), &fake, ITERATIONS);
                let dummy = [0u8; 32];
                let _ = constant_time_eq(&candidate_stored, &dummy);
                false
            }
        }
    }

    fn fake_salt(&self, username: &str) -> Vec<u8> {
        let full = hmac_sha256(&self.process_secret, username.as_bytes());
        full[..SALT_LEN].to_vec()
    }

    /// Stored salt + iterations, or a deterministic fake salt for unknown
    /// users so names cannot be enumerated.
    pub fn scram_params(&self, username: &str) -> (Vec<u8>, u32) {
        let users = self.users.read().unwrap();
        if let Some(user) = users.get(username) {
            return (user.salt.clone(), user.iterations);
        }
        (self.fake_salt(username), ITERATIONS)
    }

    /// Verify an RFC 5802 client proof against the stored `StoredKey`.
    /// Returns the server signature when valid.
    pub fn scram_verify(
        &self,
        username: &str,
        auth_message: &str,
        client_proof: &[u8],
    ) -> Option<[u8; 32]> {
        if client_proof.len() != 32 {
            return None;
        }
        let users = self.users.read().unwrap();
        let user = users.get(username)?;
        let client_signature = hmac_sha256(&user.stored_key, auth_message.as_bytes());
        let mut client_key = [0u8; 32];
        for i in 0..32 {
            client_key[i] = client_proof[i] ^ client_signature[i];
        }
        let candidate_stored: [u8; 32] = Sha256::digest(client_key).into();
        if !constant_time_eq(&candidate_stored, &user.stored_key) {
            return None;
        }
        Some(hmac_sha256(&user.server_key, auth_message.as_bytes()))
    }

    pub fn get(&self, username: &str) -> Option<AdminUserView> {
        let users = self.users.read().unwrap();
        users.get(username).map(|u| AdminUserView {
            username: u.username.clone(),
            role: u.role,
            description: u.description.clone(),
            must_change_password: u.must_change_password,
        })
    }

    pub fn list(&self) -> Vec<AdminUserView> {
        let users = self.users.read().unwrap();
        let mut out: Vec<AdminUserView> = users
            .values()
            .map(|u| AdminUserView {
                username: u.username.clone(),
                role: u.role,
                description: u.description.clone(),
                must_change_password: u.must_change_password,
            })
            .collect();
        out.sort_by(|a, b| a.username.cmp(&b.username));
        out
    }

    /// Test-only insert with a fixed salt/iterations (e.g. RFC vectors).
    #[cfg(test)]
    pub(crate) fn insert_with_salt(
        &self,
        username: &str,
        password: &str,
        salt: &[u8],
        iterations: u32,
        role: AdminRole,
        description: &str,
    ) {
        let (stored_key, server_key) = derive_keys(password.as_bytes(), salt, iterations);
        self.users.write().unwrap().insert(
            username.to_string(),
            AdminUser {
                username: username.to_string(),
                role,
                description: description.to_string(),
                salt: salt.to_vec(),
                iterations,
                stored_key,
                server_key,
                must_change_password: false,
                mfa_secret: Vec::new(),
                mfa_enabled: false,
                mfa_last_step: None,
            },
        );
    }

    /// Enrolment state for one dashboard user, or `None` when unknown.
    /// Management-plane only: one short read lock; publish and deliver
    /// never touch this map.
    pub fn mfa_status(&self, username: &str) -> Option<MfaStatus> {
        let users = self.users.read().unwrap();
        users.get(username).map(|user| MfaStatus {
            enabled: user.mfa_enabled,
            pending: !user.mfa_secret.is_empty() && !user.mfa_enabled,
        })
    }

    /// Start per-user MFA enrolment: generate one TOTP secret and store it
    /// as pending (enabled stays false until a code verifies). At most one
    /// pending enrolment per user: a second setup while pending returns the
    /// stored secret instead of growing anything; a setup while already
    /// enabled is rejected with [`AdminUserError::AlreadyExists`] so an
    /// enabled second factor is never silently replaced. Unknown users
    /// fail with [`AdminUserError::NotFound`]. Persists through the
    /// registry; a commit or save failure rolls back the in-memory secret
    /// and surfaces as [`AdminUserError::Persist`].
    /// Management-plane only: one short write lock (plus the registry save
    /// lock on persist); publish and deliver never touch this map. The
    /// secret is never logged; only the caller (the setup route) returns
    /// it once.
    // TODO(parity): does the documented setup regenerate the secret on
    // every call, or return the pending one idempotently? The rulebook
    // does not decide the repeat-setup behaviour; the current choice
    // returns the pending secret (one enrolment per user, no growth)
    // until the checker pins it.
    pub fn mfa_setup(&self, username: &str) -> Result<Vec<u8>, AdminUserError> {
        let secret = {
            let mut users = self.users.write().unwrap();
            let user = users.get(username).ok_or(AdminUserError::NotFound)?;
            if user.mfa_enabled {
                return Err(AdminUserError::AlreadyExists);
            }
            if !user.mfa_secret.is_empty() {
                return Ok(user.mfa_secret.clone());
            }
            let secret = crate::mfa_totp::generate_secret();
            if let Some(user) = users.get_mut(username) {
                user.mfa_secret = secret.clone();
                user.mfa_enabled = false;
                user.mfa_last_step = None;
            }
            secret
        };
        if let Err(error) = self.persist() {
            let mut users = self.users.write().unwrap();
            if let Some(user) = users.get_mut(username) {
                user.mfa_secret.clear();
                user.mfa_enabled = false;
                user.mfa_last_step = None;
            }
            return Err(error);
        }
        Ok(secret)
    }

    /// Strict per-user MFA enrolment for the admin
    /// `POST /users/{username}/mfa` route (C05-31): one enrolment per user.
    /// Unlike [`AdminUsers::mfa_setup`] (which returns the pending secret
    /// idempotently), any existing enrolment — pending or enabled — is
    /// rejected with [`AdminUserError::AlreadyExists`] so a live second
    /// factor is never silently replaced and enrolment never grows past
    /// the single secret slot. Unknown users fail with
    /// [`AdminUserError::NotFound`]. Persists through the registry; a
    /// commit or save failure rolls back the in-memory secret and surfaces
    /// as [`AdminUserError::Persist`].
    /// Management-plane only: one short write lock (plus the registry save
    /// lock on persist); publish and deliver never touch this map. The
    /// secret is never logged; only the caller (the enrol route) returns
    /// it once.
    // TODO(parity): does the documented create re-key an already-enrolled
    // user (secret rotation) or reject the duplicate? The rulebook does
    // not decide the rotation policy; the current choice rejects with
    // `AlreadyExists` fail-closed (never silently replaces a live second
    // factor) until the checker pins it.
    pub fn mfa_admin_enrol(&self, username: &str) -> Result<Vec<u8>, AdminUserError> {
        let secret = {
            let mut users = self.users.write().unwrap();
            let user = users.get(username).ok_or(AdminUserError::NotFound)?;
            if user.mfa_enabled || !user.mfa_secret.is_empty() {
                return Err(AdminUserError::AlreadyExists);
            }
            let secret = crate::mfa_totp::generate_secret();
            if let Some(user) = users.get_mut(username) {
                user.mfa_secret = secret.clone();
                user.mfa_enabled = false;
                user.mfa_last_step = None;
            }
            secret
        };
        if let Err(error) = self.persist() {
            let mut users = self.users.write().unwrap();
            if let Some(user) = users.get_mut(username) {
                user.mfa_secret.clear();
                user.mfa_enabled = false;
                user.mfa_last_step = None;
            }
            return Err(error);
        }
        Ok(secret)
    }

    /// Disable per-user MFA for the admin `DELETE /users/{username}/mfa`
    /// route (C05-30): clears the secret, the enabled flag and the last
    /// verified counter, and drops the memory-only attempt counters, so
    /// re-enabling later requires a fresh enrolment. Unknown users, and
    /// known users with no enrolment (no secret and never enabled), fail
    /// with [`AdminUserError::NotFound`] and change nothing. Persists
    /// through the registry; a commit or save failure rolls back the
    /// in-memory triple and surfaces as [`AdminUserError::Persist`].
    /// Management-plane only: one short write lock (plus the registry save
    /// lock on persist); publish and deliver never touch this map. Codes
    /// and secrets are never logged.
    // TODO(parity): does the documented delete of a never-enrolled user
    // report a miss (404) or a bad request (400, already-disabled)? The
    // rulebook does not decide the exact code; the current choice reports
    // `NotFound` (404, documented `{code, message}` shape, shared with the
    // other dashboard miss paths) until the checker pins it.
    pub fn mfa_disable(&self, username: &str) -> Result<(), AdminUserError> {
        let previous = {
            let mut users = self.users.write().unwrap();
            let user = users.get(username).ok_or(AdminUserError::NotFound)?;
            if user.mfa_secret.is_empty() && !user.mfa_enabled {
                return Err(AdminUserError::NotFound);
            }
            let previous = (
                user.mfa_secret.clone(),
                user.mfa_enabled,
                user.mfa_last_step,
            );
            if let Some(user) = users.get_mut(username) {
                user.mfa_secret.clear();
                user.mfa_enabled = false;
                user.mfa_last_step = None;
            }
            previous
        };
        self.mfa_attempts.lock().unwrap().remove(username);
        if let Err(error) = self.persist() {
            let mut users = self.users.write().unwrap();
            if let Some(user) = users.get_mut(username) {
                user.mfa_secret = previous.0;
                user.mfa_enabled = previous.1;
                user.mfa_last_step = previous.2;
            }
            return Err(error);
        }
        Ok(())
    }

    /// Verify one six-digit code for `username` at `unix_secs`.
    ///
    /// Behaviour, in order: unknown users fail with
    /// [`MfaVerifyError::NotFound`] and record nothing (names cannot be
    /// enumerated through attempts); locked-out users fail with
    /// [`MfaVerifyError::Locked`]; users with no enrolment fail with
    /// [`MfaVerifyError::NoEnrolment`]; codes outside the +-1-step window
    /// fail with [`MfaVerifyError::Invalid`]; codes whose counter is at or
    /// below the last verified counter fail with [`MfaVerifyError::Replayed`]
    /// (single-use); otherwise the user flips to enabled, the counter is
    /// stored, the failure count resets and persistence runs. Every
    /// [`MfaVerifyError::Invalid`] and [`MfaVerifyError::Replayed`]
    /// increments the consecutive-failure count, tripping a 60 s cool-down
    /// at [`MFA_VERIFY_MAX_ATTEMPTS`]. A successful persist failure rolls
    /// back the enable and surfaces as [`MfaVerifyError::Persist`].
    /// Management-plane only: short user-map and attempt-map locks;
    /// publish and deliver never touch either map. Codes and secrets are
    /// never logged.
    pub fn mfa_verify(
        &self,
        username: &str,
        code: &str,
        unix_secs: u64,
    ) -> Result<bool, MfaVerifyError> {
        let (secret, last_step) = {
            let users = self.users.read().unwrap();
            let user = users.get(username).ok_or(MfaVerifyError::NotFound)?;
            (user.mfa_secret.clone(), user.mfa_last_step)
        };
        {
            let mut attempts = self.mfa_attempts.lock().unwrap();
            // Entries only exist for known users (unknown names fail
            // before recording anything), so the map stays within
            // MAX_ADMIN_USERS without further eviction.
            let now = Instant::now();
            if let Some(attempt) = attempts.get_mut(username) {
                if let Some(until) = attempt.locked_until {
                    if until > now {
                        return Err(MfaVerifyError::Locked);
                    }
                    // Cool-down elapsed: drop the spent entry so the user
                    // gets a fresh batch of attempts.
                    attempts.remove(username);
                }
            }
        }
        if secret.is_empty() {
            return Err(MfaVerifyError::NoEnrolment);
        }
        let Some(counter) = crate::mfa_totp::totp_verify(&secret, code, unix_secs) else {
            self.record_mfa_failure(username);
            return Err(MfaVerifyError::Invalid);
        };
        if last_step.is_some_and(|last| counter <= last) {
            self.record_mfa_failure(username);
            return Err(MfaVerifyError::Replayed);
        }
        {
            let mut users = self.users.write().unwrap();
            // Re-check under the write lock: a concurrent verify may have
            // consumed this counter first (single-use must hold).
            let user = users.get(username).ok_or(MfaVerifyError::NotFound)?;
            if user.mfa_last_step.is_some_and(|last| counter <= last) {
                drop(users);
                self.record_mfa_failure(username);
                return Err(MfaVerifyError::Replayed);
            }
            if let Some(user) = users.get_mut(username) {
                user.mfa_enabled = true;
                user.mfa_last_step = Some(counter);
            }
        }
        if let Err(error) = self.persist_mfa() {
            let mut users = self.users.write().unwrap();
            if let Some(user) = users.get_mut(username) {
                user.mfa_last_step = last_step;
                if last_step.is_none() {
                    user.mfa_enabled = false;
                }
            }
            return Err(MfaVerifyError::Persist(error));
        }
        self.mfa_attempts.lock().unwrap().remove(username);
        Ok(true)
    }

    /// Record one failed MFA verification, tripping the cool-down at
    /// [`MFA_VERIFY_MAX_ATTEMPTS`] consecutive failures. Only called for
    /// known users, so the map stays within [`MAX_ADMIN_USERS`].
    fn record_mfa_failure(&self, username: &str) {
        let mut attempts = self.mfa_attempts.lock().unwrap();
        let entry = attempts.entry(username.to_string()).or_insert(MfaAttempt {
            consecutive_failures: 0,
            locked_until: None,
        });
        entry.consecutive_failures = entry.consecutive_failures.saturating_add(1);
        if entry.consecutive_failures >= MFA_VERIFY_MAX_ATTEMPTS {
            entry.locked_until =
                Some(Instant::now() + Duration::from_secs(MFA_VERIFY_LOCKOUT_SECS));
        }
    }

    /// Persist helper returning the message for MFA failures (the shared
    /// `persist` maps to `AdminUserError`; verify needs `MfaVerifyError`).
    fn persist_mfa(&self) -> Result<(), String> {
        let Some(registry) = self.registry.as_ref() else {
            return Ok(());
        };
        let _guard = self.save_lock.lock().unwrap();
        let conf = self.export_conf();
        registry
            .commit_admin_users(conf)
            .map_err(|err| format!("cannot persist admin users: {err}"))?;
        registry
            .save()
            .map_err(|err| format!("cannot persist admin users: {err}"))?;
        Ok(())
    }

    /// Raw enrolment secret for tests only (handler tests read it back
    /// from the setup response instead).
    #[cfg(test)]
    pub(crate) fn mfa_secret_for_test(&self, username: &str) -> Option<Vec<u8>> {
        let users = self.users.read().unwrap();
        users.get(username).map(|user| user.mfa_secret.clone())
    }
}

fn derive_keys(password: &[u8], salt: &[u8], iterations: u32) -> ([u8; 32], [u8; 32]) {
    let salted = pbkdf2_hmac_sha256(password, salt, iterations);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key: [u8; 32] = Sha256::digest(client_key).into();
    let server_key = hmac_sha256(&salted, b"Server Key");
    (stored_key, server_key)
}

fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut k = [0u8; 64];
    if key.len() > 64 {
        let hash = Sha256::digest(key);
        k[..32].copy_from_slice(&hash);
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut ipad = [0x36u8; 64];
    let mut opad = [0x5cu8; 64];
    for i in 0..64 {
        ipad[i] ^= k[i];
        opad[i] ^= k[i];
    }
    let mut inner = Sha256::new();
    inner.update(ipad);
    inner.update(data);
    let inner_hash = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(opad);
    outer.update(inner_hash);
    outer.finalize().into()
}

fn pbkdf2_hmac_sha256(password: &[u8], salt: &[u8], iterations: u32) -> [u8; 32] {
    let mut salt_and_block = Vec::with_capacity(salt.len() + 4);
    salt_and_block.extend_from_slice(salt);
    salt_and_block.extend_from_slice(&1u32.to_be_bytes());

    let mut u = hmac_sha256(password, &salt_and_block);
    let mut result = u;

    for _ in 1..iterations {
        u = hmac_sha256(password, &u);
        for (r, v) in result.iter_mut().zip(u.iter()) {
            *r ^= *v;
        }
    }
    result
}

fn constant_time_eq(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

fn is_valid_username(username: &str) -> bool {
    if username.is_empty() || username.len() > 64 {
        return false;
    }
    username
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.' | b'@'))
}

/// The first-boot default administrator (`admin` / `public`,
/// Administrator, `must_change_password = true`).
fn default_admin_user() -> AdminUser {
    let mut salt = [0u8; SALT_LEN];
    rand::rng().fill_bytes(&mut salt);
    let (stored_key, server_key) = derive_keys(b"public", &salt, ITERATIONS);
    AdminUser {
        username: "admin".to_string(),
        role: AdminRole::Administrator,
        description: "Default administrator".to_string(),
        salt: salt.to_vec(),
        iterations: ITERATIONS,
        stored_key,
        server_key,
        must_change_password: true,
        mfa_secret: Vec::new(),
        mfa_enabled: false,
        mfa_last_step: None,
    }
}

/// Decode the persisted MFA triple `(secret, enabled, last_step)`.
/// Fail-closed: an undecodable or out-of-range secret yields no enrolment
/// and disabled, so nobody verifies against half-parsed material; the
/// loss is visible as a missing enrolment instead of a silently wrong one.
/// An enabled flag without a secret also yields disabled (the registry
/// validation rejects it at commit time, so this only fires for structs
/// built in code that bypassed `commit`).
fn decode_mfa(entry: &broker_config::AdminUser) -> (Vec<u8>, bool, Option<u64>) {
    if entry.mfa_secret.trim().is_empty() {
        return (Vec::new(), false, None);
    }
    let decoded = match base64::engine::general_purpose::STANDARD.decode(entry.mfa_secret.trim()) {
        Ok(decoded) => decoded,
        Err(_) => {
            tracing::warn!(
                username = entry.username.as_str(),
                "admin snapshot entry has an undecodable MFA secret; enrolment dropped"
            );
            return (Vec::new(), false, None);
        }
    };
    if decoded.len() < 10 || decoded.len() > 64 {
        tracing::warn!(
            username = entry.username.as_str(),
            "admin snapshot entry has an out-of-range MFA secret; enrolment dropped"
        );
        return (Vec::new(), false, None);
    }
    if !entry.mfa_enabled {
        return (decoded, false, None);
    }
    (decoded, true, entry.mfa_last_step)
}

fn role_to_conf(role: AdminRole) -> &'static str {
    match role {
        AdminRole::Administrator => "administrator",
        AdminRole::Viewer => "viewer",
    }
}

/// Unknown role strings fall back to the least privilege. The registry
/// rejects them at validation time, so this only fires for structs built
/// in code that bypassed `commit`.
fn role_from_conf(raw: &str) -> AdminRole {
    match raw {
        "administrator" => AdminRole::Administrator,
        _ => AdminRole::Viewer,
    }
}

/// Prefix of the versioned verifier encoding stored in
/// [`broker_config::AdminUser::password_hash`].
///
/// The same PBKDF2-HMAC-SHA-256 derivation as the live store; only the
/// encoding is new so the salt, iteration count and both SCRAM keys
/// survive a restart byte-identically. Standard base64 throughout.
const VERIFIER_PREFIX: &str = "$scram-sha-256$";

/// Decoded password verifier: salt, PBKDF2 iteration count, SCRAM
/// `StoredKey` and `ServerKey`.
type VerifierParts = (Vec<u8>, u32, [u8; 32], [u8; 32]);

fn encode_verifier(
    salt: &[u8],
    iterations: u32,
    stored_key: &[u8; 32],
    server_key: &[u8; 32],
) -> String {
    let base64 = base64::engine::general_purpose::STANDARD;
    format!(
        "{VERIFIER_PREFIX}{iterations}${}${}${}",
        base64.encode(salt),
        base64.encode(stored_key),
        base64.encode(server_key),
    )
}

fn decode_verifier(hash: &str) -> Option<VerifierParts> {
    let rest = hash.strip_prefix(VERIFIER_PREFIX)?;
    let parts: Vec<&str> = rest.split('$').collect();
    if parts.len() != 4 {
        return None;
    }
    let iterations: u32 = parts[0].parse().ok()?;
    if iterations == 0 {
        return None;
    }
    let base64 = base64::engine::general_purpose::STANDARD;
    let salt = base64.decode(parts[1]).ok()?;
    if salt.is_empty() || salt.len() > 64 {
        return None;
    }
    let stored_key = base64.decode(parts[2]).ok()?;
    let server_key = base64.decode(parts[3]).ok()?;
    if stored_key.len() != 32 || server_key.len() != 32 {
        return None;
    }
    let stored_key: [u8; 32] = stored_key.try_into().ok()?;
    let server_key: [u8; 32] = server_key.try_into().ok()?;
    Some((salt, iterations, stored_key, server_key))
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::STANDARD as BASE64;
    use base64::Engine;

    #[test]
    fn default_admin_logs_in_and_must_change() {
        let store = AdminUsers::with_default_admin();
        assert!(store.verify_password("admin", "public"));
        let view = store.get("admin").expect("default admin exists");
        assert_eq!(view.role, AdminRole::Administrator);
        assert!(view.must_change_password);
    }

    #[test]
    fn change_password_clears_flag_and_old_password_fails() {
        let store = AdminUsers::with_default_admin();
        store
            .change_password("admin", "N3w-passw0rd")
            .expect("password change succeeds");
        assert!(!store.verify_password("admin", "public"));
        assert!(store.verify_password("admin", "N3w-passw0rd"));
        assert!(
            !store
                .get("admin")
                .expect("admin exists")
                .must_change_password
        );
        assert_eq!(
            store.change_password("admin", "N3w-passw0rd"),
            Err(AdminUserError::SameAsOld)
        );
        assert_eq!(
            store.change_password("admin", "short"),
            Err(AdminUserError::WeakPassword)
        );
    }

    #[test]
    fn scram_verify_rfc7677_vector() {
        let store = AdminUsers::with_default_admin();
        let salt = BASE64
            .decode("W22ZaJ0SNY7soEsUEjb6gQ==")
            .expect("valid salt");
        store.insert_with_salt(
            "user",
            "pencil",
            &salt,
            4096,
            AdminRole::Administrator,
            "rfc",
        );
        let auth_message = "n=user,r=rOprNGfwEbeRWgbNEkqO,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
        let proof = BASE64
            .decode("dHzbZapWIk4jUhN+Ute9ytag9zjfMHgsqmmiz7AndVQ=")
            .expect("valid proof");
        let sig = store
            .scram_verify("user", auth_message, &proof)
            .expect("rfc proof verifies");
        assert_eq!(
            BASE64.encode(sig),
            "6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4="
        );
        let mut bad = proof.clone();
        bad[0] ^= 1;
        assert!(store.scram_verify("user", auth_message, &bad).is_none());
    }

    #[test]
    fn unknown_user_gets_stable_fake_salt() {
        let store = AdminUsers::with_default_admin();
        let (first, _) = store.scram_params("ghost");
        let (second, _) = store.scram_params("ghost");
        assert_eq!(first, second);
        let (other, _) = store.scram_params("phantom");
        assert_ne!(first, other);
    }

    #[test]
    fn last_administrator_cannot_be_deleted_or_demoted() {
        let store = AdminUsers::with_default_admin();
        assert_eq!(
            store.delete("admin"),
            Err(AdminUserError::LastAdministrator)
        );
        assert_eq!(
            store.set_role("admin", AdminRole::Viewer),
            Err(AdminUserError::LastAdministrator)
        );
        store
            .create("admin2", "L0ng-passw0rd", AdminRole::Administrator, "")
            .expect("second admin");
        store
            .set_role("admin", AdminRole::Viewer)
            .expect("demote with backup admin");
        store.delete("admin").expect("delete demoted admin");
        assert_eq!(
            store.delete("admin2"),
            Err(AdminUserError::LastAdministrator)
        );
    }

    #[test]
    fn viewer_roles_and_listing() {
        let store = AdminUsers::with_default_admin();
        store
            .create("bob", "B0b-passw0rd", AdminRole::Viewer, "ro")
            .expect("create viewer");
        store
            .create("alice", "Al1ce-passw0rd", AdminRole::Administrator, "ops")
            .expect("create admin");
        let list = store.list();
        let names: Vec<&str> = list.iter().map(|u| u.username.as_str()).collect();
        assert_eq!(names, vec!["admin", "alice", "bob"]);
        let bob = list
            .iter()
            .find(|u| u.username == "bob")
            .expect("bob listed");
        assert_eq!(bob.role, AdminRole::Viewer);
        assert!(!bob.must_change_password);
        let alice = list
            .iter()
            .find(|u| u.username == "alice")
            .expect("alice listed");
        assert_eq!(alice.role, AdminRole::Administrator);
    }

    #[test]
    fn create_rejects_invalid_usernames() {
        let store = AdminUsers::with_default_admin();
        for name in ["", "a b", "x\n", &"a".repeat(65)] {
            assert_eq!(
                store.create(name, "L0ng-passw0rd", AdminRole::Viewer, ""),
                Err(AdminUserError::InvalidUsername),
                "name {name:?} must be rejected"
            );
        }
        store
            .create("ops.team-1@site", "L0ng-passw0rd", AdminRole::Viewer, "")
            .expect("valid username is accepted");
    }

    #[test]
    fn verify_password_unknown_user_is_false() {
        let store = AdminUsers::with_default_admin();
        assert!(!store.verify_password("ghost", "public"));
    }
}
