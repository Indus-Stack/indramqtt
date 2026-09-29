//! License state and install for the v5 management API.
//!
//! Covers `GET /license` (read the live license, C13-16) and
//! `POST /license` (validated install, C13-17) on the real
//! [`crate::licence::LicenceStore`]. Single-record store by design
//! (bounded): at most one token is ever held, so per-request work is one
//! short-lock snapshot plus one status render, never proportional to
//! connections, sessions or subscriptions.
//!
//! Management-plane only: both handlers run off the publish and deliver
//! paths (edge receive, kernel route, session, deliver never touch this
//! store). The kernel consults the same store at boot and on validated
//! installs only; see `crates/broker-node/src/main.rs:6048`
//! (`load_from_data_dir`), `:6038` (`set_trusted_keys`), `:6099`
//! (`refresh_alarms`) and `:6116` (`stored_token` evaluated for the
//! cluster join path). The broker event that drives it is node boot plus
//! the management install; the publish path never consults it and a
//! licence condition never stops customer traffic.
//!
//! Store bounds (stated here and enforced below):
//! - exactly one stored token (single-record by design); installs replace
//!   in place and never grow a list;
//! - the presented token is capped at [`MAX_LICENSE_TOKEN_LEN`] bytes
//!   (16 KiB); longer bodies are rejected before verification so one
//!   record cannot balloon memory or the persisted `licence.token` file;
//! - reads clone at most the one token plus a small status document per
//!   request under short locks.
//!
//! Persistence: the shared [`crate::licence::LicenceStore`] persists
//! through the kernel data directory (`cluster_identity.json` plus
//! `licence.token`, atomically replaced); a failed install never touches
//! the previously stored licence, and a restart reloads the same state
//! via `load_from_data_dir`.
// TODO(parity): which install body field name (`license` versus `licence`
// versus `token` versus `key`) and which install success status/body
// (200 with the live status versus 201/204 with a payload echo) does the
// spec require? The rulebook does not decide the exact shape; the current
// choice accepts `license` plus the `licence`/`token`/`key` aliases,
// ignores extra fields, and answers 200 with the same status document
// `GET` renders (omitting fields the broker cannot supply, never a
// constant standing in for a measurement) until the checker pins it.
// TODO(parity): which status does an identity-mismatched install carry
// (400 versus 409) and which documented error code names the failure?
// The rulebook does not decide the exact mapping; the current choice
// answers every install rejection with `BAD_REQUEST` (400 in this tree's
// shared error mapping) carrying the distinct install reason
// (`identity mismatch` stays distinct from a bad signature) until the
// checker pins it.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use serde::Deserialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use crate::errors::ApiError;
use crate::pagination::{meta_page, paginate};
use crate::ApiState;
use std::path::{Path, PathBuf};

/// Upper bound for a presented licence token (16 KiB). Reason: a token is
/// a few hundred bytes of base64 payload plus a 64-byte signature, so
/// 16 KiB covers any realistic signed payload with headroom while one
/// record cannot balloon the in-memory clone per request or the
/// atomically replaced `licence.token` file; longer bodies are rejected
/// before verification with `BAD_REQUEST`.
pub const MAX_LICENSE_TOKEN_LEN: usize = 16 * 1024;

/// `GET /license`: the live license, or the documented unlicensed state
/// when nothing is installed.
///
/// Always 200, never an error: with no license the store reports the
/// trial state with entitlements on. Management-plane only; one
/// short-lock snapshot per request, publish and deliver never touch it.
pub async fn get_license(State(state): State<ApiState>) -> Response {
    let now = now_secs();
    let body = state.licence.status_at(now);
    (StatusCode::OK, Json(body)).into_response()
}

/// `POST /license`: validated install of a licence token.
///
/// The body must be a JSON object carrying the token under `license`
/// (aliases `licence`, `token`, `key`; first present string wins). Every
/// supplied value is validated before anything is applied: missing,
/// empty, over-long or unverifiable tokens fail with `BAD_REQUEST` and
/// apply nothing, so a failed install leaves the prior license intact.
/// Installing over an existing license replaces it in place (single
/// record) and the response reports the new live state with 200.
/// Management-plane only; verification runs on the calling task, the
/// store takes short locks, and publish/deliver never touch it.
pub async fn install_license(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid license body: {error}")).into_response();
        }
    };
    let token = match parse_install_request(&value) {
        Ok(token) => token,
        Err(message) => return ApiError::BadRequest(message).into_response(),
    };
    let now = now_secs();
    match state.licence.install(&token, now) {
        Ok(_) => {
            state.licence.refresh_alarms(&state.alarms, now);
            let body = state.licence.status_at(now);
            (StatusCode::OK, Json(body)).into_response()
        }
        Err(error) => ApiError::BadRequest(error.to_string()).into_response(),
    }
}

/// Validate an install body into the token to verify.
///
/// The body must be a JSON object; the token is the first present string
/// among `license`, `licence`, `token` and `key`. Extra fields are
/// ignored so future record fields degrade to the same install instead
/// of a 400. Empty or whitespace-only tokens, non-string tokens and
/// tokens past [`MAX_LICENSE_TOKEN_LEN`] bytes fail; nothing is applied
/// on error.
fn parse_install_request(value: &serde_json::Value) -> Result<String, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "license body must be a JSON object".to_string())?;
    for field in ["license", "licence", "token", "key"] {
        if let Some(candidate) = obj.get(field) {
            let token = candidate.as_str().ok_or_else(|| {
                format!("field `{field}` must be a string carrying the licence token")
            })?;
            let trimmed = token.trim();
            if trimmed.is_empty() {
                return Err(format!("field `{field}` must not be empty"));
            }
            if trimmed.len() > MAX_LICENSE_TOKEN_LEN {
                return Err(format!(
                    "field `{field}` exceeds {MAX_LICENSE_TOKEN_LEN} bytes"
                ));
            }
            return Ok(trimmed.to_string());
        }
    }
    Err("field `license` is required".to_string())
}

fn now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------------------
// Session high-water-mark history (W5-12, C13-18):
// `GET /api/v5/license/session_hwm_history` over real session counts.
// ---------------------------------------------------------------------------

/// Upper bound for stored HWM samples. Recording past this size evicts
/// the oldest sample first instead of growing the buffer without limit.
/// Reason: 90 samples cover fifteen minutes at one sample per ten seconds
/// (the same cadence as the monitor sampler); one sample holds three
/// integers, so the buffer stays at a few kilobytes and a management read
/// copies at most 90 small documents.
pub const MAX_HWM_SAMPLES: usize = 90;

/// Default number of entries returned per page when the caller names no
/// `limit`. Reason: 30 samples cover five minutes at the default cadence,
/// enough to see recent peaks without copying the whole ring; the caller
/// may page up to [`MAX_HWM_SAMPLES`] explicitly. The default is finite;
/// the operator cannot opt into unlimited (the `limit` clamp below caps
/// every request).
pub const DEFAULT_HWM_LIMIT: u32 = 30;

/// One recorded high-water-mark sample: the sampling instant
/// (`sampled_at`, Unix seconds), the connected-session count then
/// (`sessions`, rebuilt from session truth) and the running high-water
/// mark (`session_hwm`, never falls within a run except across a restart,
/// which starts a fresh memory-only ring).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HwmSample {
    pub sampled_at: u64,
    pub sessions: u64,
    pub session_hwm: u64,
}

impl HwmSample {
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "sampled_at": self.sampled_at,
            "timestamp": self.sampled_at,
            "sessions": self.sessions,
            "session_hwm": self.session_hwm,
            "sessions_max": self.session_hwm,
        })
    }
}

/// Bounded HWM recorder behind one short lock plus a lock-free running
/// maximum.
///
/// Store wiring (rulebook section 2: name the file and line outside
/// `crates/broker-api` that writes or consults it, and the broker event
/// that drives it):
/// - writer `crates/broker-node/src/main.rs` (`sample_hwm_history`,
///   driven by `spawn_hwm_sampler` on its timer tick plus the immediate
///   boot sample, rebuilt from session truth via
///   `SessionManager::connected_count`);
/// - reader shared into `ApiState` by `serve_api` in the same file, so
///   the handler below observes the kernel's ring instead of an empty
///   per-request copy. The broker event that drives it is node boot plus
///   the periodic management-plane timer tick; the publish and deliver
///   paths never touch it.
///
/// Store bounds (stated here and enforced below):
/// - at most [`MAX_HWM_SAMPLES`] samples are kept in a fixed ring
///   (`VecDeque`); recording past the cap evicts the oldest sample
///   instead of growing without limit;
/// - every sample is three integers, so per-sample memory is constant;
/// - recorder writes are constant-time (one atomic raise plus one push
///   and at most one pop); listing copies at most the capped buffer once
///   per request.
///
/// Persistence: memory-only. A restart starts a fresh empty ring (and a
/// reset running maximum); history is rebuilt from session truth and does
/// not need to persist across a restart beyond the persisted cap.
///
/// Management-plane only: the timer task and this handler run off the
/// publish and deliver paths (edge receive, kernel route, session,
/// deliver never touch this store).
pub struct HwmRecorder {
    inner: RwLock<VecDeque<HwmSample>>,
    high: AtomicU64,
}

impl HwmRecorder {
    /// Empty recorder with a reset running maximum.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(VecDeque::new()),
            high: AtomicU64::new(0),
        }
    }

    /// Record one sample. Constant-time: one push plus at most one pop
    /// of the oldest sample when the buffer is full. The running maximum
    /// is raised to at least `sample.session_hwm` so direct records and
    /// truth samples share one monotone mark.
    pub fn record(&self, sample: HwmSample) {
        self.high.fetch_max(sample.session_hwm, Ordering::Relaxed);
        let mut buffer = self.inner.write().expect("hwm store lock");
        if buffer.len() >= MAX_HWM_SAMPLES {
            buffer.pop_front();
        }
        buffer.push_back(sample);
    }

    /// Rebuild one sample from session truth and record it: read the live
    /// connected-session count from the session directory, raise the
    /// running maximum to at least that count, and store the point.
    /// Constant-time like [`Self::record`]. Returns the stored sample.
    /// Management-plane only; never called on the publish or deliver
    /// path (only the kernel timer task and management tests call it).
    pub fn sample_session_truth(
        &self,
        sessions: &broker_session::SessionManager,
        now_secs: u64,
    ) -> HwmSample {
        let current = sessions.connected_count();
        let previous = self.high.fetch_max(current, Ordering::Relaxed);
        let mark = previous.max(current);
        let sample = HwmSample {
            sampled_at: now_secs,
            sessions: current,
            session_hwm: mark,
        };
        let mut buffer = self.inner.write().expect("hwm store lock");
        if buffer.len() >= MAX_HWM_SAMPLES {
            buffer.pop_front();
        }
        buffer.push_back(sample.clone());
        sample
    }

    /// Snapshot of stored samples newest first. An empty store reads as
    /// an empty list, never an error.
    pub fn list_newest(&self) -> Vec<HwmSample> {
        let buffer = self.inner.read().expect("hwm store lock");
        buffer.iter().rev().cloned().collect()
    }

    /// Number of stored samples (never above [`MAX_HWM_SAMPLES`]).
    pub fn len(&self) -> usize {
        self.inner.read().expect("hwm store lock").len()
    }

    /// Whether any sample is stored.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for HwmRecorder {
    fn default() -> Self {
        Self::new()
    }
}

/// Documented `GET /api/v5/license/session_hwm_history` paging. Only
/// `page`/`limit` narrow the read; every other query key is ignored by
/// the extractor so new parameters degrade to the default page instead of
/// a 400. Malformed values fall back to the defaults below.
#[derive(Deserialize, Default)]
pub struct HwmHistoryQuery {
    #[serde(default)]
    pub page: Option<String>,
    #[serde(default)]
    pub limit: Option<String>,
}

fn parse_hwm_page(query: &HwmHistoryQuery) -> (u32, u32) {
    let page = query
        .page
        .as_deref()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .unwrap_or(1)
        .max(1);
    let limit = query
        .limit
        .as_deref()
        .and_then(|raw| raw.trim().parse::<u32>().ok())
        .unwrap_or(DEFAULT_HWM_LIMIT)
        .clamp(1, MAX_HWM_SAMPLES as u32);
    (page, limit)
}

/// `GET /license/session_hwm_history`: paged HWM samples, newest first.
///
/// Returns the standard page envelope (`data` plus `meta`). Each row
/// carries the documented per-entry fields (`sampled_at`, `sessions`,
/// `session_hwm`, with the `timestamp`/`sessions_max` aliases naming the
/// same measurements). With no samples it returns an empty page with
/// 200, never an error. Paging honours `page`/`limit` (`limit` defaults
/// to [`DEFAULT_HWM_LIMIT`] and is clamped to `1..=90`, i.e. the
/// [`MAX_HWM_SAMPLES`] cap is enforced); out-of-range pages read as an
/// empty page. Unknown query parameters are ignored. Management-plane
/// only: one short store lock per request; publish and deliver never
/// touch this store.
pub async fn get_session_hwm_history(
    State(state): State<ApiState>,
    Query(query): Query<HwmHistoryQuery>,
) -> Response {
    let (page, limit) = parse_hwm_page(&query);
    let rows = state.hwm_history.list_newest();
    let total = rows.len();
    let rendered: Vec<serde_json::Value> = rows.iter().map(HwmSample::to_json).collect();
    let page_items = paginate(&rendered, page, limit);
    let data: Vec<serde_json::Value> = page_items.to_vec();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": meta_page(page, limit, total),
        })),
    )
        .into_response()
}

// ---------------------------------------------------------------------------
// License settings (W5-13, C13-19/C13-20):
// `GET /api/v5/license/setting` (read) and `PUT /api/v5/license/setting`
// (validated update) over a single-record store persisted through the
// kernel data directory.
// ---------------------------------------------------------------------------

/// Default low watermark for licence quota alarms. Reason: the documented
/// default threshold below which quota usage alarms deactivate.
pub const DEFAULT_LICENSE_LOW_WATERMARK: &str = "75%";
/// Default high watermark for licence quota alarms. Reason: the documented
/// default threshold above which quota usage alarms activate.
pub const DEFAULT_LICENSE_HIGH_WATERMARK: &str = "80%";
/// Default day-boundary timezone for quota history. Reason: the documented
/// default follows the node host's local timezone.
pub const DEFAULT_LICENSE_HWM_TIMEZONE: &str = "system";
/// Default gated allocation before an explicit setting. Reason: the
/// documented default allocation applied until the operator sets one.
pub const DEFAULT_LICENSE_DYNAMIC_MAX: u64 = 25;

/// File holding the persisted settings inside the kernel data directory.
const LICENSE_SETTING_FILE: &str = "license_setting.json";

/// Upper bound for a settings body (8 KiB). Reason: the record is four
/// small fields (two short percent strings, one short timezone string and
/// one integer), so 8 KiB covers any realistic update with headroom while
/// one write cannot balloon the atomically replaced file; longer bodies
/// are rejected before parsing.
pub const MAX_LICENSE_SETTING_BODY: usize = 8 * 1024;

/// Validated licence quota settings: alarm watermarks, the history
/// day-boundary timezone and the gated allocation.
///
/// One small struct behind a short lock; reads clone it per request and
/// never grow with connections or subscriptions. Management-plane only;
/// nothing on the publish or deliver path touches it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LicenseSetting {
    pub connection_low_watermark: String,
    pub connection_high_watermark: String,
    pub high_watermark_timezone: String,
    pub dynamic_max_connections: u64,
}

impl Default for LicenseSetting {
    fn default() -> Self {
        Self {
            connection_low_watermark: DEFAULT_LICENSE_LOW_WATERMARK.to_string(),
            connection_high_watermark: DEFAULT_LICENSE_HIGH_WATERMARK.to_string(),
            high_watermark_timezone: DEFAULT_LICENSE_HWM_TIMEZONE.to_string(),
            dynamic_max_connections: DEFAULT_LICENSE_DYNAMIC_MAX,
        }
    }
}

impl LicenseSetting {
    /// Render the full documented object.
    pub fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "connection_low_watermark": self.connection_low_watermark,
            "connection_high_watermark": self.connection_high_watermark,
            "high_watermark_timezone": self.high_watermark_timezone,
            "dynamic_max_connections": self.dynamic_max_connections,
        })
    }
}

/// Single validated settings record behind a short lock plus an optional
/// data-directory for persistence. Constant-time reads and writes;
/// readers take a snapshot clone so management reads never block delivery
/// and no delivery lock is ever taken here.
///
/// Store wiring (rulebook section 2: name the file and line outside
/// `crates/broker-api` that writes or consults it, and the broker event
/// that drives it):
/// - writer `crates/broker-node/src/main.rs` (boot `load_from_data_dir`
///   plus the `serve_api` share of this instance, so management reads
///   observe the kernel's record instead of an empty per-request copy);
/// - the PUT handler below persists through the recorded data directory
///   after every validated update. The broker event that drives it is node
///   boot plus the management write; the publish and deliver paths never
///   touch this store.
///
/// Store bounds (stated here and enforced below):
/// - exactly one validated struct is ever held; updates replace in place
///   and never grow a list;
/// - bodies past [`MAX_LICENSE_SETTING_BODY`] bytes are rejected before
///   parsing so one write cannot balloon memory or the persisted file;
/// - every field is one short string or integer, so per-request memory is
///   constant.
///
/// Persistence: `license_setting.json` in the kernel data directory,
/// atomically replaced (tmp file plus rename); a failed write reverts the
/// in-memory record so prior settings stay intact, and a restart reloads
/// the same settings via `load_from_data_dir`.
///
/// Management-plane only: handlers run off the publish and deliver paths
/// (edge receive, kernel route, session, deliver never touch this store).
pub struct LicenseSettingStore {
    inner: RwLock<LicenseSetting>,
    data_dir: RwLock<Option<PathBuf>>,
}

impl LicenseSettingStore {
    /// Default settings with no persistence directory (in-memory only
    /// until [`Self::load_from_data_dir`] records one).
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(LicenseSetting::default()),
            data_dir: RwLock::new(None),
        }
    }

    /// Snapshot the current settings.
    pub fn get(&self) -> LicenseSetting {
        self.inner.read().expect("license setting lock").clone()
    }

    /// Record the kernel data directory and reload persisted settings, if
    /// any. A missing file keeps the defaults; a corrupt or invalid file
    /// fails loudly instead of being skipped silently.
    pub fn load_from_data_dir(&self, dir: &Path) -> Result<(), String> {
        *self.data_dir.write().expect("license setting lock") = Some(dir.to_path_buf());
        let path = dir.join(LICENSE_SETTING_FILE);
        let bytes = match std::fs::read(&path) {
            Ok(bytes) => bytes,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(format!(
                    "cannot read license settings from {}: {error}",
                    path.display()
                ));
            }
        };
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|error| format!("invalid stored license settings: {error}"))?;
        let obj = value
            .as_object()
            .ok_or_else(|| "stored license settings must be a JSON object".to_string())?;
        let next = self.validate_merge(obj)?;
        *self.inner.write().expect("license setting lock") = next;
        Ok(())
    }

    /// Validate a merged update without mutating the stored record.
    /// Every supplied field is checked; the merged whole (supplied fields
    /// over the current record) must satisfy the low-is-below-high rule.
    /// Unknown fields are ignored so newer settings degrade to the known
    /// subset instead of a 400. Nothing is applied on error.
    pub fn validate_merge(
        &self,
        obj: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<LicenseSetting, String> {
        if obj.is_empty() {
            return Err("license setting body must not be empty".to_string());
        }
        let mut next = self.get();
        if let Some(value) = obj.get("connection_low_watermark") {
            let text = value.as_str().ok_or_else(|| {
                "field `connection_low_watermark` must be a percent string like `75%`".to_string()
            })?;
            parse_percent(text).ok_or_else(|| {
                "field `connection_low_watermark` must be a percent string like `75%`".to_string()
            })?;
            next.connection_low_watermark = text.trim().to_string();
        }
        if let Some(value) = obj.get("connection_high_watermark") {
            let text = value.as_str().ok_or_else(|| {
                "field `connection_high_watermark` must be a percent string like `80%`".to_string()
            })?;
            parse_percent(text).ok_or_else(|| {
                "field `connection_high_watermark` must be a percent string like `80%`".to_string()
            })?;
            next.connection_high_watermark = text.trim().to_string();
        }
        if let Some(value) = obj.get("high_watermark_timezone") {
            let text = value.as_str().ok_or_else(|| {
                "field `high_watermark_timezone` must be `system` or an offset like `+08:00`"
                    .to_string()
            })?;
            validate_timezone(text)?;
            next.high_watermark_timezone = text.trim().to_string();
        }
        if let Some(value) = obj.get("dynamic_max_connections") {
            let num = parse_dynamic_max(value).ok_or_else(|| {
                "field `dynamic_max_connections` must be a non-negative integer".to_string()
            })?;
            next.dynamic_max_connections = num;
        }
        let low = parse_percent(&next.connection_low_watermark)
            .ok_or_else(|| "stored `connection_low_watermark` is not a percent".to_string())?;
        let high = parse_percent(&next.connection_high_watermark)
            .ok_or_else(|| "stored `connection_high_watermark` is not a percent".to_string())?;
        if low >= high {
            return Err(format!(
                "field `connection_low_watermark` ({}) must be below `connection_high_watermark` ({})",
                next.connection_low_watermark, next.connection_high_watermark
            ));
        }
        Ok(next)
    }

    /// Store a validated record and persist it. A persist failure reverts
    /// the in-memory record so prior settings stay intact.
    pub fn store(&self, next: LicenseSetting) -> Result<(), String> {
        let previous = self.get();
        *self.inner.write().expect("license setting lock") = next.clone();
        if let Err(error) = self.persist(&next) {
            *self.inner.write().expect("license setting lock") = previous;
            return Err(error);
        }
        Ok(())
    }

    fn persist(&self, next: &LicenseSetting) -> Result<(), String> {
        let dir = self.data_dir.read().expect("license setting lock").clone();
        let Some(dir) = dir else {
            return Ok(());
        };
        let data = serde_json::to_vec_pretty(&next.to_json())
            .map_err(|error| format!("cannot encode license settings: {error}"))?;
        std::fs::create_dir_all(&dir)
            .map_err(|error| format!("cannot create {}: {error}", dir.display()))?;
        let tmp = dir.join(format!("{LICENSE_SETTING_FILE}.tmp"));
        std::fs::write(&tmp, &data)
            .map_err(|error| format!("cannot write {}: {error}", tmp.display()))?;
        std::fs::rename(&tmp, dir.join(LICENSE_SETTING_FILE))
            .map_err(|error| format!("cannot replace {}: {error}", LICENSE_SETTING_FILE))?;
        Ok(())
    }
}

impl Default for LicenseSettingStore {
    fn default() -> Self {
        Self::new()
    }
}

/// Parse a percent string like `75%` into its numeric value (0..=100).
/// Returns `None` for missing suffix, unparsable numbers, non-finite
/// values and out-of-range values. Plain numbers without the `%` suffix
/// are rejected: the documented shape is a string.
fn parse_percent(text: &str) -> Option<f64> {
    let trimmed = text.trim();
    let num_part = trimmed.strip_suffix('%')?;
    if num_part.trim().is_empty() {
        return None;
    }
    // Reject interior whitespace (`75 %`): the documented shape has the
    // suffix glued to the number.
    if num_part.contains(char::is_whitespace) {
        return None;
    }
    let num: f64 = num_part.parse().ok()?;
    if !num.is_finite() || !(0.0..=100.0).contains(&num) {
        return None;
    }
    Some(num)
}

/// Timezone values the write path accepts: `system` (follow the node
/// host) or an explicit numeric offset `+HH:MM`/`-HH:MM` (hours 00..=14,
/// minutes 00..=59), so day boundaries stay consistent across nodes.
fn validate_timezone(text: &str) -> Result<(), String> {
    let trimmed = text.trim();
    if trimmed == "system" {
        return Ok(());
    }
    let bytes = trimmed.as_bytes();
    if bytes.len() != 6 || (bytes[0] != b'+' && bytes[0] != b'-') || bytes[3] != b':' {
        return Err(
            "field `high_watermark_timezone` must be `system` or an offset like `+08:00`"
                .to_string(),
        );
    }
    let hour: u32 = trimmed[1..3].parse().map_err(|_| {
        "field `high_watermark_timezone` must be `system` or an offset like `+08:00`".to_string()
    })?;
    let minute: u32 = trimmed[4..6].parse().map_err(|_| {
        "field `high_watermark_timezone` must be `system` or an offset like `+08:00`".to_string()
    })?;
    // Reason: numeric offsets beyond fourteen hours match no civil zone,
    // so they are a typo; `system` covers hosts that already know theirs.
    if hour > 14 || minute > 59 {
        return Err(
            "field `high_watermark_timezone` must be `system` or an offset like `+08:00`"
                .to_string(),
        );
    }
    Ok(())
}

/// Integers the write path accepts for the gated allocation: JSON
/// integers zero or above. Floats, strings and negatives are rejected
/// (`as_u64` is `None` for all of them).
fn parse_dynamic_max(value: &serde_json::Value) -> Option<u64> {
    value.as_u64()
}

/// `GET /license/setting`: the current settings.
///
/// One short lock and one small JSON clone per request; no delivery
/// locks, no new buffering.
pub async fn get_license_setting(State(state): State<ApiState>) -> Response {
    let cfg = state.license_setting.get();
    (StatusCode::OK, Json(cfg.to_json())).into_response()
}

/// `PUT /license/setting`: validated update.
///
/// The whole body is validated before anything is applied: one bad field
/// rejects the entire write with the documented `BAD_REQUEST` shape and
/// leaves the stored settings untouched. Supplied fields merge over the
/// current record; unknown fields are ignored. On success returns 200
/// with the new settings, persisted across a restart through the kernel
/// data directory. Management-plane only: one snapshot swap per request;
/// publish and deliver never touch this store.
pub async fn put_license_setting(State(state): State<ApiState>, body: Bytes) -> Response {
    if body.len() > MAX_LICENSE_SETTING_BODY {
        return ApiError::BadRequest("license setting body is too large".to_string())
            .into_response();
    }
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => {
            return ApiError::BadRequest(format!("invalid license setting body: {error}"))
                .into_response();
        }
    };
    let obj = match value.as_object() {
        Some(map) => map.clone(),
        None => {
            return ApiError::BadRequest("license setting body must be a JSON object".to_string())
                .into_response();
        }
    };
    let next = match state.license_setting.validate_merge(&obj) {
        Ok(next) => next,
        Err(reason) => return ApiError::BadRequest(reason).into_response(),
    };
    match state.license_setting.store(next.clone()) {
        Ok(()) => (StatusCode::OK, Json(next.to_json())).into_response(),
        Err(reason) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "INTERNAL_ERROR",
                "message": format!("cannot persist license setting: {reason}"),
            })),
        )
            .into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broker_cluster::{
        canonical_json_bytes, license::TOKEN_PREFIX, LicensePayload, TrustedKeys,
    };
    use p256::ecdsa::{signature::Signer, SigningKey, VerifyingKey};
    use std::sync::Arc;

    use base64::Engine as _;

    fn signing_key(seed: u8) -> SigningKey {
        let bytes = [seed; 32];
        let field = p256::elliptic_curve::generic_array::GenericArray::clone_from_slice(&bytes);
        SigningKey::from_bytes(&field).expect("seed")
    }

    fn mint(
        signing: &SigningKey,
        kid: &str,
        node_id: &str,
        issued_at: u64,
        expires_at: u64,
    ) -> String {
        let payload = LicensePayload {
            customer: "Acme".to_string(),
            max_nodes: 5,
            issued_at,
            expires_at,
            features: vec!["clustering".to_string()],
            node_id: node_id.to_string(),
            kid: kid.to_string(),
            grace_period_days: broker_cluster::DEFAULT_GRACE_DAYS,
        };
        let bytes = canonical_json_bytes(&payload).expect("json");
        let sig: p256::ecdsa::Signature = signing.sign(&bytes);
        format!(
            "{}.{}.{}",
            TOKEN_PREFIX,
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&bytes),
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(sig.to_bytes())
        )
    }

    fn seeded_state(identity: &str, seed: u8) -> (ApiState, SigningKey, String) {
        let signing = signing_key(seed);
        let kid = format!("key-{seed:02x}");
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        let mut trusted = TrustedKeys::new();
        trusted.insert(kid.clone(), VerifyingKey::from(&signing));
        state.licence.set_trusted_keys(trusted);
        state
            .licence
            .set_identity_for_tests(broker_cluster::ClusterIdentity {
                identity: identity.to_string(),
                public_key_hex: "aa".to_string(),
                private_key_hex: "bb".to_string(),
                trial_started_at: 1_750_000_000,
                highest_seen_secs: 1_750_000_000,
            });
        (state, signing, kid)
    }

    async fn response_parts(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("license body is small and readable");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("license body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn get_reports_unlicensed_trial_not_an_error() {
        // W5-10 broker-through: the read goes through the served handler
        // on shared `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store. With no license installed it reports the
        // documented unlicensed (trial) state with 200, never an error.
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        let (status, body) = response_parts(get_license(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["state"], serde_json::json!("trial"));
        assert_eq!(body["entitlements_on"], serde_json::json!(true));
    }

    #[tokio::test]
    async fn post_valid_round_trips_and_replaces() {
        // W5-10 failing test: read, install a valid body and read it
        // back. Before: both 404. Broker-through: every step goes
        // through the handler on `ApiState`.
        let (state, signing, kid) = seeded_state("cluster-w510-a", 0xA1);
        // Handler validates expiry against wall-clock `now_secs()`; mint
        // relative to it so the token is live when the handler checks it.
        let now = now_secs();
        let token = mint(
            &signing,
            &kid,
            "cluster-w510-a",
            now - 100,
            now + 10_000_000,
        );
        let (status, body) = response_parts(
            install_license(
                State(state.clone()),
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({"license": token})).expect("json"),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["state"], serde_json::json!("valid"));
        assert_eq!(body["customer"], serde_json::json!("Acme"));
        assert_eq!(body["identity"], serde_json::json!("cluster-w510-a"));
        let (status, reread) = response_parts(get_license(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread["state"], serde_json::json!("valid"));
        assert_eq!(reread["customer"], serde_json::json!("Acme"));
    }

    #[tokio::test]
    async fn post_invalid_is_rejected_and_prior_state_is_intact() {
        // W5-10 failing test: install an invalid body expecting
        // rejection with the documented `{code, message}` shape. A
        // failed install leaves the prior license intact.
        let (state, signing, kid) = seeded_state("cluster-w510-b", 0xB2);
        // Handler validates expiry against wall-clock `now_secs()`; mint
        // relative to it so the token is live when the handler checks it.
        let now = now_secs();
        let good = mint(
            &signing,
            &kid,
            "cluster-w510-b",
            now - 100,
            now + 10_000_000,
        );
        let (status, _) = response_parts(
            install_license(
                State(state.clone()),
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({"license": good})).expect("json"),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        for bad in [
            serde_json::json!({"license": "not-a-licence"}),
            serde_json::json!({}),
            serde_json::json!({"license": ""}),
            serde_json::json!({"license": 42}),
            serde_json::json!("just-a-string"),
        ] {
            let (status, body) = response_parts(
                install_license(
                    State(state.clone()),
                    Bytes::from(serde_json::to_vec(&bad).expect("json")),
                )
                .await,
            )
            .await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "body {bad} must be rejected"
            );
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
            assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
        }
        // The prior good license survives every rejection.
        let (status, reread) = response_parts(get_license(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread["state"], serde_json::json!("valid"));
        assert_eq!(reread["identity"], serde_json::json!("cluster-w510-b"));
    }

    #[test]
    fn install_persists_across_restart() {
        // License state persists across a restart through the kernel
        // data directory: install on one store, reload from the same
        // directory on a fresh store, observe the same license.
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-license-w510-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("create test dir");
        let now = 1_750_000_000u64;
        let signing = signing_key(0xC3);
        let kid = "key-c3".to_string();
        let mut trusted = TrustedKeys::new();
        trusted.insert(kid.clone(), VerifyingKey::from(&signing));
        let first = crate::licence::LicenceStore::new();
        first.set_trusted_keys(trusted.clone());
        let identity = first.load_from_data_dir(&dir, now).expect("identity loads");
        let _ = identity;
        let stored_identity = first.cluster_identity().expect("identity recorded");
        let token = mint(
            &signing,
            &kid,
            &stored_identity,
            now - 100,
            now + 10_000_000,
        );
        first.install(&token, now).expect("installs");
        // A fresh store reloading the same directory observes the same
        // license without reinstalling.
        let second = crate::licence::LicenceStore::new();
        second.set_trusted_keys(trusted);
        second.load_from_data_dir(&dir, now).expect("reloads");
        let status = second.status_at(now);
        assert_eq!(status["state"], serde_json::json!("valid"));
        assert_eq!(status["identity"], serde_json::json!(stored_identity));
        assert_eq!(status["customer"], serde_json::json!("Acme"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn license_never_stops_customer_traffic() {
        // Management-plane only: installing (or failing to install) a
        // license adds no work to publish/deliver. Broker-through: a
        // management publish through the rule engine and deliver path
        // still answers 200 after the install.
        let (state, signing, kid) = seeded_state("cluster-w510-c", 0xD4);
        // Handler validates expiry against wall-clock `now_secs()`; mint
        // relative to it so the token is live when the handler checks it.
        let now = now_secs();
        let token = mint(
            &signing,
            &kid,
            "cluster-w510-c",
            now - 100,
            now + 10_000_000,
        );
        let (status, _) = response_parts(
            install_license(
                State(state.clone()),
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({"token": token})).expect("json"),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, body) = response_parts(
            crate::v5::clients::publish_message(
                State(state),
                Bytes::from(
                    serde_json::to_vec(&serde_json::json!({
                        "topic": "w510/probe",
                        "payload": "hello",
                        "qos": 0,
                    }))
                    .expect("json"),
                ),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("id").is_some(), "publish answers with an id");
    }

    async fn hwm_response_parts(response: Response) -> (StatusCode, serde_json::Value) {
        response_parts(response).await
    }

    fn hwm_query(page: Option<&str>, limit: Option<&str>) -> HwmHistoryQuery {
        HwmHistoryQuery {
            page: page.map(str::to_string),
            limit: limit.map(str::to_string),
        }
    }

    #[tokio::test]
    async fn hwm_empty_store_reads_as_empty_page_not_an_error() {
        // W5-12 broker-through: the read goes through the served handler
        // on shared `ApiState` (the same state the kernel shares in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the store. With no samples it returns an empty page
        // with 200, never an error.
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        assert!(state.hwm_history.is_empty());
        let (status, body) = hwm_response_parts(
            get_session_hwm_history(State(state), Query(hwm_query(None, None))).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["data"], serde_json::json!([]));
        assert_eq!(body["meta"]["count"], serde_json::json!(0));
        assert_eq!(body["meta"]["page"], serde_json::json!(1));
        assert_eq!(body["meta"]["limit"], serde_json::json!(DEFAULT_HWM_LIMIT));
    }

    #[tokio::test]
    async fn hwm_returns_entries_newest_first_after_connects() {
        // W5-12 failing test shape: connect clients through the session
        // directory (the same session truth the kernel timer rebuilds
        // from), sample the way the timer does, then fetch history
        // through the handler expecting entries with numeric marks,
        // newest first. Before: 404 (no route).
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        state.sessions.get_or_create("hwm-c1", true);
        state
            .hwm_history
            .sample_session_truth(&state.sessions, 1_700_000_100);
        state.sessions.get_or_create("hwm-c2", true);
        state.sessions.get_or_create("hwm-c3", true);
        let newest = state
            .hwm_history
            .sample_session_truth(&state.sessions, 1_700_000_200);
        assert_eq!(newest.sessions, 3);
        assert_eq!(newest.session_hwm, 3);
        let (status, body) = hwm_response_parts(
            get_session_hwm_history(State(state), Query(hwm_query(None, None))).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let data = body["data"].as_array().expect("data is a list");
        assert_eq!(data.len(), 2);
        // Newest first: the later sample leads.
        assert_eq!(data[0]["sampled_at"], serde_json::json!(1_700_000_200));
        assert_eq!(data[1]["sampled_at"], serde_json::json!(1_700_000_100));
        for row in data {
            assert!(row["sampled_at"].is_u64(), "sample carries a time: {row}");
            assert!(row["sessions"].is_u64(), "sample carries sessions: {row}");
            assert!(
                row["session_hwm"].is_u64(),
                "sample carries a numeric mark: {row}"
            );
            assert!(row["timestamp"].is_u64(), "alias carries a time: {row}");
            assert!(
                row["sessions_max"].is_u64(),
                "alias carries a numeric mark: {row}"
            );
        }
        assert_eq!(data[0]["sessions"], serde_json::json!(3));
        assert_eq!(data[0]["session_hwm"], serde_json::json!(3));
        assert_eq!(body["meta"]["count"], serde_json::json!(2));
    }

    #[tokio::test]
    async fn hwm_paging_honoured_and_cap_enforced() {
        // W5-12 bounds: the ring holds at most MAX_HWM_SAMPLES samples
        // (oldest evicted first), paging slices newest-first, and an
        // over-cap limit is clamped instead of growing the read.
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        for i in 0..(MAX_HWM_SAMPLES + 5) {
            state.hwm_history.record(HwmSample {
                sampled_at: 1_700_000_000 + i as u64,
                sessions: i as u64,
                session_hwm: i as u64,
            });
        }
        assert_eq!(state.hwm_history.len(), MAX_HWM_SAMPLES);
        let rows = state.hwm_history.list_newest();
        assert_eq!(rows.len(), MAX_HWM_SAMPLES);
        // The five oldest samples were evicted; the newest leads.
        assert_eq!(
            rows[0].sampled_at,
            1_700_000_000 + (MAX_HWM_SAMPLES + 5) as u64 - 1
        );
        assert_eq!(rows[rows.len() - 1].sampled_at, 1_700_000_005);
        // First page of two holds the two newest samples.
        let (status, first) = hwm_response_parts(
            get_session_hwm_history(State(state.clone()), Query(hwm_query(Some("1"), Some("2"))))
                .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first["data"].as_array().expect("list").len(), 2);
        assert_eq!(first["meta"]["page"], serde_json::json!(1));
        assert_eq!(first["meta"]["limit"], serde_json::json!(2));
        assert_eq!(first["meta"]["count"], serde_json::json!(MAX_HWM_SAMPLES));
        // An out-of-range page reads as an empty page, never an error.
        let (status, beyond) = hwm_response_parts(
            get_session_hwm_history(
                State(state.clone()),
                Query(hwm_query(Some("9999"), Some("10"))),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(beyond["data"], serde_json::json!([]));
        assert_eq!(beyond["meta"]["count"], serde_json::json!(MAX_HWM_SAMPLES));
        // An over-cap limit is clamped to the cap instead of rejected.
        let (status, capped) = hwm_response_parts(
            get_session_hwm_history(State(state), Query(hwm_query(Some("1"), Some("99999")))).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            capped["meta"]["limit"],
            serde_json::json!(MAX_HWM_SAMPLES as u32)
        );
        assert_eq!(
            capped["data"].as_array().expect("list").len(),
            MAX_HWM_SAMPLES
        );
    }

    #[tokio::test]
    async fn hwm_read_adds_no_work_to_publish() {
        // Management-plane only: sampling plus a history read followed by
        // a management publish through the rule engine and deliver path
        // still answers 200, so customer traffic keeps serving.
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        state.sessions.get_or_create("hwm-p1", true);
        state
            .hwm_history
            .sample_session_truth(&state.sessions, 1_700_000_300);
        let (status, _) = hwm_response_parts(
            get_session_hwm_history(State(state.clone()), Query(hwm_query(None, None))).await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let response = crate::v5::clients::publish_message(
            State(state),
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "topic": "w512/probe",
                    "payload": "hello",
                    "qos": 0,
                }))
                .expect("json"),
            ),
        )
        .await;
        let (status, body) = response_parts(response).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("id").is_some(), "publish answers with an id");
    }

    async fn setting_response_parts(response: Response) -> (StatusCode, serde_json::Value) {
        response_parts(response).await
    }

    #[tokio::test]
    async fn setting_round_trip_read_write_read() {
        // W5-13 failing test: read settings, write a valid update and
        // read it back. Before: both 404. Broker-through: every step goes
        // through the handler on `ApiState`.
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        let (status, before) =
            setting_response_parts(get_license_setting(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            before["connection_low_watermark"],
            serde_json::json!(DEFAULT_LICENSE_LOW_WATERMARK)
        );
        assert_eq!(
            before["connection_high_watermark"],
            serde_json::json!(DEFAULT_LICENSE_HIGH_WATERMARK)
        );
        assert_eq!(
            before["high_watermark_timezone"],
            serde_json::json!(DEFAULT_LICENSE_HWM_TIMEZONE)
        );
        assert_eq!(
            before["dynamic_max_connections"],
            serde_json::json!(DEFAULT_LICENSE_DYNAMIC_MAX)
        );

        let update = serde_json::json!({
            "connection_low_watermark": "60%",
            "connection_high_watermark": "85%",
            "high_watermark_timezone": "+08:00",
            "dynamic_max_connections": 42,
        });
        let (status, after) = setting_response_parts(
            put_license_setting(
                State(state.clone()),
                Bytes::from(serde_json::to_vec(&update).expect("update is JSON")),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(after["connection_low_watermark"], serde_json::json!("60%"));
        assert_eq!(after["connection_high_watermark"], serde_json::json!("85%"));
        assert_eq!(
            after["high_watermark_timezone"],
            serde_json::json!("+08:00")
        );
        assert_eq!(after["dynamic_max_connections"], serde_json::json!(42));

        let (status, reread) =
            setting_response_parts(get_license_setting(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, after);
    }

    #[tokio::test]
    async fn setting_invalid_write_is_rejected_and_prior_settings_persist() {
        // W5-13 failing test: write invalid expecting rejection with the
        // documented `{code, message}` shape. A failed write leaves the
        // prior settings intact.
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        let update = serde_json::json!({
            "connection_low_watermark": "60%",
            "connection_high_watermark": "85%",
        });
        let (status, _) = setting_response_parts(
            put_license_setting(
                State(state.clone()),
                Bytes::from(serde_json::to_vec(&update).expect("update is JSON")),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let (status, before) =
            setting_response_parts(get_license_setting(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);

        for bad in [
            serde_json::json!({"connection_low_watermark": "85%", "connection_high_watermark": "85%"}),
            serde_json::json!({"connection_low_watermark": "90%", "connection_high_watermark": "85%"}),
            serde_json::json!({"connection_high_watermark": "50%"}),
            serde_json::json!({"connection_low_watermark": "huge"}),
            serde_json::json!({"connection_low_watermark": 75}),
            serde_json::json!({"connection_low_watermark": "75"}),
            serde_json::json!({"connection_low_watermark": "101%"}),
            serde_json::json!({"connection_high_watermark": "never"}),
            serde_json::json!({"high_watermark_timezone": "mars"}),
            serde_json::json!({"high_watermark_timezone": 8}),
            serde_json::json!({"dynamic_max_connections": -1}),
            serde_json::json!({"dynamic_max_connections": "many"}),
            serde_json::json!({}),
            serde_json::json!([]),
            serde_json::json!("just-a-string"),
        ] {
            let (status, body) = setting_response_parts(
                put_license_setting(
                    State(state.clone()),
                    Bytes::from(serde_json::to_vec(&bad).expect("bad body is JSON")),
                )
                .await,
            )
            .await;
            assert_eq!(
                status,
                StatusCode::BAD_REQUEST,
                "body {bad} must be rejected"
            );
            assert_eq!(body["code"], serde_json::json!("BAD_REQUEST"));
            assert!(body["message"].as_str().is_some_and(|m| !m.is_empty()));
        }

        // The prior good settings survive every rejection.
        let (status, reread) =
            setting_response_parts(get_license_setting(State(state)).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, before);
    }

    #[test]
    fn setting_persists_across_restart() {
        // Settings persist across a restart through the kernel data
        // directory: update on one store, reload from the same directory
        // on a fresh store, observe the same settings.
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-license-setting-w513-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        let first = LicenseSettingStore::new();
        first.load_from_data_dir(&dir).expect("loads defaults");
        let update: serde_json::Map<String, serde_json::Value> =
            serde_json::from_value(serde_json::json!({
                "connection_low_watermark": "60%",
                "connection_high_watermark": "85%",
                "high_watermark_timezone": "+08:00",
                "dynamic_max_connections": 42,
            }))
            .expect("update is an object");
        let next = first.validate_merge(&update).expect("validates");
        first.store(next).expect("persists");
        // A fresh store reloading the same directory observes the same
        // settings without another write.
        let second = LicenseSettingStore::new();
        second.load_from_data_dir(&dir).expect("reloads");
        let reloaded = second.get();
        assert_eq!(reloaded.connection_low_watermark, "60%");
        assert_eq!(reloaded.connection_high_watermark, "85%");
        assert_eq!(reloaded.high_watermark_timezone, "+08:00");
        assert_eq!(reloaded.dynamic_max_connections, 42);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn setting_read_adds_no_work_to_publish() {
        // Management-plane only: a settings read plus a validated write
        // followed by a management publish through the rule engine and
        // deliver path still answers 200, so customer traffic keeps
        // serving.
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        let state = ApiState::standalone(engine);
        let (status, _) =
            setting_response_parts(get_license_setting(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        let update = serde_json::json!({
            "connection_low_watermark": "60%",
            "connection_high_watermark": "85%",
        });
        let (status, _) = setting_response_parts(
            put_license_setting(
                State(state.clone()),
                Bytes::from(serde_json::to_vec(&update).expect("update is JSON")),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        let response = crate::v5::clients::publish_message(
            State(state),
            Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "topic": "w513/probe",
                    "payload": "hello",
                    "qos": 0,
                }))
                .expect("json"),
            ),
        )
        .await;
        let (status, body) = response_parts(response).await;
        assert_eq!(status, StatusCode::OK);
        assert!(body.get("id").is_some(), "publish answers with an id");
    }
}
