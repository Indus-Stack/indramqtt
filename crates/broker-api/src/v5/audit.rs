//! Management-plane audit log for the v5 management API.
//!
//! Covers `GET /audit` (paged query over real management entries,
//! C05-07). Single-node, management-plane only: entries are recorded by
//! the management authentication middleware after each authenticated
//! management request finishes, and reads serve the same bounded ring.
//! Publish and deliver never touch this store, and audit writes never
//! take a blocking lock on any message path.
//!
//! Store bounds (stated here and enforced below):
//! - at most [`MAX_AUDIT_ENTRIES`] entries are kept in a ring
//!   (`VecDeque`); recording past the cap evicts the oldest entry first
//!   instead of growing without limit;
//! - `operation_id` is capped at [`MAX_OPERATION_ID_LEN`] chars,
//!   `operation_type` at [`MAX_OPERATION_TYPE_LEN`], `source` at
//!   [`MAX_SOURCE_LEN`], `source_ip` at [`MAX_SOURCE_IP_LEN`] and `node`
//!   at [`MAX_NODE_LEN`], so one entry cannot balloon memory;
//! - every entry is eleven small fields plus the bounded strings above,
//!   so per-entry memory is constant and the whole ring stays bounded;
//! - the default store is empty; retention is memory-only and does not
//!   persist across a restart (entries are trimmed on boot by
//!   construction: a fresh ring starts empty and there is no load path);
//! - the default page size is the shared pagination default (100 rows,
//!   see [`crate::pagination::DEFAULT_LIMIT`]);
//! - recorder writes are fire-and-forget off the hot path: one
//!   non-blocking `try_write` plus at most one pop; when the lock is
//!   contended the entry is dropped (counted in `dropped_contention`)
//!   instead of blocking the audited request. Reads take one short read
//!   lock and clone at most the capped ring once per request, so reads
//!   never block writes beyond one lock hold.
// Each entry renders the request line: method, path, status,
// duration and actor. It omits `args`, `failure` and `http_request`.
// The broker tracks only the request line. The specification marks
// those fields optional, so omission stays valid.
// Time filters are `gte_created_at` and `lte_created_at`. Values accept
// RFC 3339 or epoch integers. The spec names these keys.
// Outcome filter accepts `success` and `failure`. Unknown values match
// nothing and read as an empty page.
// The list always reads. The tree has no audit toggle. An empty log
// reads as an empty page, never an error.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Deserialize;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::RwLock;

use crate::pagination::{meta_page, paginate, PageParams};
use crate::ApiState;

/// Upper bound for stored audit entries. Recording past this size evicts
/// the oldest entry first instead of growing the ring without limit.
/// Reason: the management plane snapshots the filtered ring per list
/// request and each entry holds a few small strings, so an unbounded
/// ring lets one automation batch exhaust the kernel; 100000 covers
/// sustained operator and automation activity with headroom while the
/// ring stays in the tens of megabytes worst case and list reads stay
/// paged.
pub const MAX_AUDIT_ENTRIES: usize = 100_000;
/// Longest stored operation path (`operation_id`).
/// Reason: paths are request targets plus at most one id segment, so an
/// unbounded path lets one request bloat every later list snapshot; 512
/// covers nested management paths with headroom.
const MAX_OPERATION_ID_LEN: usize = 512;
/// Longest stored operation group (`operation_type`).
/// Reason: derived from the first path segment, so a short cap keeps
/// grouping cheap; 128 covers the longest group name with headroom.
const MAX_OPERATION_TYPE_LEN: usize = 128;
/// Longest stored actor name (`source`).
/// Reason: rendered in every row and cloned per list snapshot, so an
/// unbounded name bloats each read; 256 covers long service accounts.
const MAX_SOURCE_LEN: usize = 256;
/// Longest stored peer address (`source_ip`).
/// Reason: cloned per list snapshot like the other strings; 128 covers
/// textual address forms with headroom.
const MAX_SOURCE_IP_LEN: usize = 128;
/// Longest stored node name (`node`).
/// Reason: cloned per entry and per list snapshot; 128 covers long
/// node ids with headroom.
const MAX_NODE_LEN: usize = 128;

/// Origin tag rendered on entries recorded by the management middleware.
const AUDIT_FROM: &str = "rest_api";
/// Outcome rendered when the audited response status is below 400.
const OUTCOME_SUCCESS: &str = "success";
/// Outcome rendered when the audited response status is 400 or above.
const OUTCOME_FAILURE: &str = "failure";

/// One management audit entry: when the request finished
/// (`created_at_us`, microseconds since the Unix epoch, plus the
/// RFC 3339 rendering), where (`node`, `from`, `source`, `source_ip`),
/// what (`operation_id` is the request path, `operation_type` is the
/// first path group) and how it ended (`operation_result`,
/// `http_method`, `http_status_code`, `duration_ms`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEntry {
    pub created_at_us: u64,
    pub node: String,
    pub from: String,
    pub source: String,
    pub source_ip: String,
    pub operation_id: String,
    pub operation_type: String,
    pub operation_result: String,
    pub http_method: String,
    pub http_status_code: u16,
    pub duration_ms: u64,
}

impl AuditEntry {
    fn to_json(&self) -> serde_json::Value {
        // The peer address is absent when the recorder has no measurement.
        // The spec marks `source_ip` optional, so omission stays valid.
        let mut entry = serde_json::json!({
            "created_at": micros_to_rfc3339(self.created_at_us),
            "node": self.node,
            "from": self.from,
            "source": self.source,
            "operation_id": self.operation_id,
            "operation_type": self.operation_type,
            "operation_result": self.operation_result,
            "http_method": self.http_method,
            "http_status_code": self.http_status_code,
            "duration_ms": self.duration_ms,
        });
        if !self.source_ip.trim().is_empty() {
            entry["source_ip"] = serde_json::Value::String(self.source_ip.clone());
        }
        entry
    }
}

/// Input for one audit record. Callers pass the measured values; the
/// store truncates oversized strings to the caps above so one request
/// cannot balloon the ring.
#[derive(Debug, Clone)]
pub struct AuditRecordInput {
    pub created_at_us: u64,
    pub node: String,
    pub source: String,
    pub source_ip: String,
    pub operation_id: String,
    pub operation_type: String,
    pub operation_result: String,
    pub http_method: String,
    pub http_status_code: u16,
    pub duration_ms: u64,
}

/// Bounded audit ring behind one short lock.
///
/// Every method finishes quickly and no publish or deliver path touches
/// it, so management reads never stall messaging. Writes use
/// `try_write` and drop (counted) when contended, so a slow list read
/// never blocks the audited request.
pub struct AuditLog {
    inner: RwLock<VecDeque<AuditEntry>>,
    dropped_contention: AtomicU64,
}

impl AuditLog {
    /// Empty ring.
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(VecDeque::new()),
            dropped_contention: AtomicU64::new(0),
        }
    }

    /// Record one entry. Fire-and-forget: a single non-blocking lock
    /// attempt plus at most one pop of the oldest entry when full. When
    /// the lock is contended the entry is dropped and counted instead
    /// of blocking the audited request.
    pub fn record(&self, input: AuditRecordInput) {
        let entry = AuditEntry {
            created_at_us: input.created_at_us,
            node: truncate(&input.node, MAX_NODE_LEN).to_string(),
            from: AUDIT_FROM.to_string(),
            source: truncate(&input.source, MAX_SOURCE_LEN).to_string(),
            source_ip: truncate(&input.source_ip, MAX_SOURCE_IP_LEN).to_string(),
            operation_id: truncate(&input.operation_id, MAX_OPERATION_ID_LEN).to_string(),
            operation_type: truncate(&input.operation_type, MAX_OPERATION_TYPE_LEN).to_string(),
            operation_result: input.operation_result,
            http_method: input.http_method,
            http_status_code: input.http_status_code,
            duration_ms: input.duration_ms,
        };
        let Ok(mut ring) = self.inner.try_write() else {
            self.dropped_contention.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if ring.len() >= MAX_AUDIT_ENTRIES {
            ring.pop_front();
        }
        ring.push_back(entry);
    }

    /// Entries dropped because the ring lock was contended at record
    /// time. Monotonic within the run; management-plane only.
    pub fn dropped_contention(&self) -> u64 {
        self.dropped_contention.load(Ordering::Relaxed)
    }

    /// Number of stored entries (never above [`MAX_AUDIT_ENTRIES`]).
    pub fn len(&self) -> usize {
        self.inner.read().map(|ring| ring.len()).unwrap_or(0)
    }

    /// Whether the ring holds no entries.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Snapshot of entries matching `filter`, oldest first. An empty
    /// ring reads as an empty list, never an error. Takes one short
    /// read lock and clones at most the capped ring once per request.
    pub fn query(&self, filter: &AuditFilter) -> Vec<AuditEntry> {
        let ring = self.inner.read().expect("audit log lock");
        ring.iter()
            .filter(|entry| filter.matches(entry))
            .cloned()
            .collect()
    }
}

impl Default for AuditLog {
    fn default() -> Self {
        Self::new()
    }
}

fn truncate(text: &str, max_chars: usize) -> &str {
    if text.len() <= max_chars {
        return text;
    }
    let mut end = max_chars;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    &text[..end]
}

/// Documented query scope. Only the fields below narrow the read;
/// every other query key is ignored by the extractor so new parameters
/// degrade to the unfiltered list instead of a 400. Malformed time or
/// integer values fall back to unfiltered for that bound.
#[derive(Deserialize, Default, Debug)]
pub struct AuditFilterQuery {
    #[serde(default)]
    pub node: Option<String>,
    #[serde(default)]
    pub from: Option<String>,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub source_ip: Option<String>,
    #[serde(default)]
    pub operation_id: Option<String>,
    #[serde(default)]
    pub operation_type: Option<String>,
    #[serde(default)]
    pub operation_result: Option<String>,
    #[serde(default)]
    pub http_status_code: Option<String>,
    #[serde(default)]
    pub http_method: Option<String>,
    #[serde(default)]
    pub gte_created_at: Option<String>,
    #[serde(default)]
    pub lte_created_at: Option<String>,
    #[serde(default)]
    pub gte_duration_ms: Option<String>,
    #[serde(default)]
    pub lte_duration_ms: Option<String>,
}

/// Parsed filter bounds over one [`AuditLog`] snapshot.
#[derive(Debug, Clone, Default)]
pub struct AuditFilter {
    pub node: Option<String>,
    pub from: Option<String>,
    pub source: Option<String>,
    pub source_ip: Option<String>,
    pub operation_id: Option<String>,
    pub operation_type: Option<String>,
    pub operation_result: Option<String>,
    pub http_status_code: Option<u16>,
    pub http_method: Option<String>,
    pub gte_created_at_us: Option<u64>,
    pub lte_created_at_us: Option<u64>,
    pub gte_duration_ms: Option<u64>,
    pub lte_duration_ms: Option<u64>,
}

impl AuditFilter {
    fn from_query(query: &AuditFilterQuery) -> Self {
        Self {
            node: non_empty(query.node.clone()),
            from: non_empty(query.from.clone()),
            source: non_empty(query.source.clone()),
            source_ip: non_empty(query.source_ip.clone()),
            operation_id: non_empty(query.operation_id.clone()),
            operation_type: non_empty(query.operation_type.clone()),
            operation_result: non_empty(query.operation_result.clone()),
            http_status_code: query
                .http_status_code
                .as_deref()
                .and_then(|raw| raw.trim().parse::<u16>().ok()),
            http_method: non_empty(query.http_method.clone()).map(|method| method.to_lowercase()),
            gte_created_at_us: query.gte_created_at.as_deref().and_then(parse_time_bound),
            lte_created_at_us: query.lte_created_at.as_deref().and_then(parse_time_bound),
            gte_duration_ms: query
                .gte_duration_ms
                .as_deref()
                .and_then(|raw| raw.trim().parse::<u64>().ok()),
            lte_duration_ms: query
                .lte_duration_ms
                .as_deref()
                .and_then(|raw| raw.trim().parse::<u64>().ok()),
        }
    }

    fn matches(&self, entry: &AuditEntry) -> bool {
        if let Some(node) = &self.node {
            if entry.node != *node {
                return false;
            }
        }
        if let Some(from) = &self.from {
            if entry.from != *from {
                return false;
            }
        }
        if let Some(source) = &self.source {
            if entry.source != *source {
                return false;
            }
        }
        if let Some(source_ip) = &self.source_ip {
            if entry.source_ip != *source_ip {
                return false;
            }
        }
        if let Some(operation_id) = &self.operation_id {
            if entry.operation_id != *operation_id {
                return false;
            }
        }
        if let Some(operation_type) = &self.operation_type {
            if entry.operation_type != *operation_type {
                return false;
            }
        }
        if let Some(operation_result) = &self.operation_result {
            if entry.operation_result != *operation_result {
                return false;
            }
        }
        if let Some(status) = self.http_status_code {
            if entry.http_status_code != status {
                return false;
            }
        }
        if let Some(method) = &self.http_method {
            if entry.http_method != *method {
                return false;
            }
        }
        if let Some(gte) = self.gte_created_at_us {
            if entry.created_at_us < gte {
                return false;
            }
        }
        if let Some(lte) = self.lte_created_at_us {
            if entry.created_at_us > lte {
                return false;
            }
        }
        if let Some(gte) = self.gte_duration_ms {
            if entry.duration_ms < gte {
                return false;
            }
        }
        if let Some(lte) = self.lte_duration_ms {
            if entry.duration_ms > lte {
                return false;
            }
        }
        true
    }
}

fn non_empty(value: Option<String>) -> Option<String> {
    value.filter(|text| !text.trim().is_empty())
}

/// Parse one time bound into microseconds since the Unix epoch.
///
/// Accepts integer strings (auto-detected as microseconds above
/// 10^14, milliseconds above 10^11, otherwise seconds) and RFC 3339
/// timestamps (`YYYY-MM-DDTHH:MM:SS[.ffffff][Z|+00:00]`). Returns
/// `None` when the value parses as neither, so a malformed bound
/// degrades to unfiltered for that side instead of a 400.
fn parse_time_bound(raw: &str) -> Option<u64> {
    let text = raw.trim();
    if text.is_empty() {
        return None;
    }
    if let Ok(integer) = text.parse::<u64>() {
        return Some(normalise_epoch_integer(integer));
    }
    if let Ok(integer) = text.parse::<i64>() {
        if integer < 0 {
            return None;
        }
        return Some(normalise_epoch_integer(integer as u64));
    }
    parse_rfc3339_to_micros(text)
}

fn normalise_epoch_integer(value: u64) -> u64 {
    if value >= 100_000_000_000_000 {
        value
    } else if value >= 100_000_000_000 {
        value.saturating_mul(1_000)
    } else {
        value.saturating_mul(1_000_000)
    }
}

/// Minimal RFC 3339 reader for the audit time bounds: accepts
/// `YYYY-MM-DDTHH:MM:SS[.fraction][Z|+00:00|+0000]` and returns
/// microseconds since the Unix epoch. Only UTC is accepted (any
/// non-zero offset returns `None` and degrades to unfiltered); the
/// writer only emits UTC, so this covers every value the log stores.
fn parse_rfc3339_to_micros(text: &str) -> Option<u64> {
    let (datetime, _) = split_timezone(text)?;
    let (date, time) = datetime.split_once('T')?;
    let mut date_parts = date.split('-');
    let year: i64 = date_parts.next()?.parse().ok()?;
    let month: i64 = date_parts.next()?.parse().ok()?;
    let day: i64 = date_parts.next()?.parse().ok()?;
    if date_parts.next().is_some() {
        return None;
    }
    let (hms, fraction) = match time.split_once('.') {
        Some((hms, fraction)) => (hms, fraction),
        None => (time, ""),
    };
    let mut time_parts = hms.split(':');
    let hour: i64 = time_parts.next()?.parse().ok()?;
    let minute: i64 = time_parts.next()?.parse().ok()?;
    let second: i64 = time_parts.next()?.parse().ok()?;
    if time_parts.next().is_some() {
        return None;
    }
    if !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 60
    {
        return None;
    }
    let micros = if fraction.is_empty() {
        0
    } else {
        let digits: String = fraction.chars().take(6).collect();
        if !digits.chars().all(|ch| ch.is_ascii_digit()) {
            return None;
        }
        let mut value: u64 = digits.parse().ok()?;
        for _ in digits.len()..6 {
            value *= 10;
        }
        value
    };
    let days = days_since_epoch(year, month, day)?;
    let secs = days
        .checked_mul(86_400)?
        .checked_add(hour * 3_600 + minute * 60 + second)? as u64;
    secs.checked_mul(1_000_000)?.checked_add(micros)
}

fn split_timezone(text: &str) -> Option<(&str, &str)> {
    if let Some(stripped) = text.strip_suffix('Z') {
        return Some((stripped, "Z"));
    }
    if let Some(stripped) = text.strip_suffix("+00:00") {
        return Some((stripped, "+00:00"));
    }
    if let Some(stripped) = text.strip_suffix("+0000") {
        return Some((stripped, "+0000"));
    }
    None
}

fn days_since_epoch(year: i64, month: i64, day: i64) -> Option<i64> {
    let shifted_month = if month <= 2 { month + 12 } else { month };
    let shifted_year = if month <= 2 { year - 1 } else { year };
    let era = if shifted_year >= 0 {
        shifted_year
    } else {
        shifted_year - 399
    } / 400;
    let year_of_era = shifted_year - era * 400;
    let day_of_year = (153 * (shifted_month - 3) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    Some(era * 146_097 + day_of_era - 719_468)
}

/// Current wall-clock time in microseconds since the Unix epoch.
pub fn now_micros() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| {
            duration
                .as_secs()
                .saturating_mul(1_000_000)
                .saturating_add(u64::from(duration.subsec_micros()))
        })
        .unwrap_or(0)
}

fn micros_to_rfc3339(micros: u64) -> String {
    let secs = micros / 1_000_000;
    let sub_micros = (micros % 1_000_000) as u32;
    let (year, month, day, hour, minute, second) = civil_from_epoch_secs(secs);
    format!("{year:04}-{month:02}-{day:02}T{hour:02}:{minute:02}:{second:02}.{sub_micros:06}+00:00")
}

fn civil_from_epoch_secs(secs: u64) -> (i64, u32, u32, u32, u32, u32) {
    let days = (secs / 86_400) as i64;
    let second_of_day = (secs % 86_400) as u32;
    let (year, month, day) = civil_from_day_count(days);
    (
        year,
        month,
        day,
        second_of_day / 3600,
        (second_of_day % 3600) / 60,
        second_of_day % 60,
    )
}

fn civil_from_day_count(count: i64) -> (i64, u32, u32) {
    let shifted = count + 719_468;
    let era = if shifted >= 0 {
        shifted
    } else {
        shifted - 146_096
    } / 146_097;
    let day_of_era = (shifted - era * 146_097) as u64;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era as i64 + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = (day_of_year - (153 * month_index + 2) / 5 + 1) as u32;
    let month = (if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    }) as u32;
    (if month <= 2 { year + 1 } else { year }, month, day)
}

/// Outcome for an audited response status: `success` below 400,
/// `failure` otherwise.
pub fn outcome_for_status(status: u16) -> &'static str {
    if status < 400 {
        OUTCOME_SUCCESS
    } else {
        OUTCOME_FAILURE
    }
}

/// Operation group for an audited request path: the first segment after
/// the `/api/v5` prefix (or after `/api/v1` for the native API), or the
/// full trimmed path when there is no such prefix. Lowercase paths stay
/// lowercase; nothing is invented beyond splitting on `/`.
pub fn operation_type_for_path(path: &str) -> String {
    let trimmed = path.trim();
    let without_prefix = trimmed
        .strip_prefix("/api/v5")
        .or_else(|| trimmed.strip_prefix("/api/v1"))
        .unwrap_or(trimmed);
    let first = without_prefix
        .split('/')
        .find(|segment| !segment.is_empty())
        .unwrap_or("unknown");
    first.to_string()
}

/// `GET /audit`: paged audit page (`data` plus `meta`). An empty log
/// reads as an empty page, never an error. Time-range
/// (`gte_created_at`/`lte_created_at`) and outcome (`operation_result`)
/// filters narrow the read plus the other documented management
/// filters; unknown query keys are ignored and malformed bounds fall
/// back to unfiltered for that side. Malformed `page`/`limit` fall back
/// to defaults via [`PageParams`].
///
/// Management-plane only: one short read lock per request; publish and
/// deliver never touch this store.
pub async fn list_audit(
    State(state): State<ApiState>,
    params: PageParams,
    Query(query): Query<AuditFilterQuery>,
) -> Response {
    let filter = AuditFilter::from_query(&query);
    let all = state.audit.query(&filter);
    let total = all.len();
    let page_items = paginate(&all, params.page, params.limit);
    let data: Vec<serde_json::Value> = page_items.iter().map(AuditEntry::to_json).collect();
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "data": data,
            "meta": meta_page(params.page, params.limit, total),
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_at(created_at_us: u64, result: &str, operation_id: &str) -> AuditRecordInput {
        AuditRecordInput {
            created_at_us,
            node: "indra-node-1".to_string(),
            source: "admin".to_string(),
            source_ip: String::new(),
            operation_id: operation_id.to_string(),
            operation_type: operation_type_for_path(operation_id),
            operation_result: result.to_string(),
            http_method: "post".to_string(),
            http_status_code: if result == OUTCOME_SUCCESS { 200 } else { 400 },
            duration_ms: 1,
        }
    }

    #[test]
    fn empty_ring_queries_nothing() {
        let log = AuditLog::new();
        assert!(log.is_empty());
        assert_eq!(log.len(), 0);
        let all = log.query(&AuditFilter::default());
        assert!(all.is_empty());
    }

    #[test]
    fn record_two_entries_time_filter_excludes_both() {
        let log = AuditLog::new();
        log.record(entry_at(
            1_700_000_000_000_000,
            OUTCOME_SUCCESS,
            "/api/v5/api_key",
        ));
        log.record(entry_at(
            1_700_000_000_100_000,
            OUTCOME_SUCCESS,
            "/api/v5/users",
        ));
        assert_eq!(log.len(), 2);
        let all = log.query(&AuditFilter::default());
        assert_eq!(all.len(), 2);
        assert!(all[0].created_at_us <= all[1].created_at_us);
        // A window after both entries reads as an empty page.
        let after = AuditFilter {
            gte_created_at_us: Some(1_800_000_000_000_000),
            ..AuditFilter::default()
        };
        assert!(log.query(&after).is_empty());
        // The outcome filter honours success versus failure.
        let failures = AuditFilter {
            operation_result: Some(OUTCOME_FAILURE.to_string()),
            ..AuditFilter::default()
        };
        assert!(log.query(&failures).is_empty());
        let successes = AuditFilter {
            operation_result: Some(OUTCOME_SUCCESS.to_string()),
            ..AuditFilter::default()
        };
        assert_eq!(log.query(&successes).len(), 2);
    }

    #[test]
    fn ring_evicts_oldest_past_cap() {
        let log = AuditLog::new();
        for index in 0..(MAX_AUDIT_ENTRIES + 5) {
            log.record(entry_at(
                1_700_000_000_000_000 + index as u64,
                OUTCOME_SUCCESS,
                "/api/v5/api_key",
            ));
        }
        assert_eq!(log.len(), MAX_AUDIT_ENTRIES);
        let all = log.query(&AuditFilter::default());
        assert_eq!(all.len(), MAX_AUDIT_ENTRIES);
        assert_eq!(all[0].created_at_us, 1_700_000_000_000_005);
    }

    #[test]
    fn entry_json_has_documented_shape() {
        let log = AuditLog::new();
        let mut input = entry_at(1_700_000_000_000_000, OUTCOME_SUCCESS, "/api/v5/api_key");
        input.source_ip = "127.0.0.1".to_string();
        log.record(input);
        let all = log.query(&AuditFilter::default());
        assert_eq!(all.len(), 1);
        let rendered = all[0].to_json();
        for field in [
            "created_at",
            "node",
            "from",
            "source",
            "source_ip",
            "operation_id",
            "operation_type",
            "operation_result",
            "http_method",
            "http_status_code",
            "duration_ms",
        ] {
            assert!(rendered.get(field).is_some(), "missing field {field}");
        }
        assert_eq!(rendered["from"], serde_json::json!(AUDIT_FROM));
        assert_eq!(
            rendered["operation_result"],
            serde_json::json!(OUTCOME_SUCCESS)
        );
        assert_eq!(rendered["operation_type"], serde_json::json!("api_key"));
    }

    #[test]
    fn empty_source_ip_stays_absent() {
        // No measurement means no field. The render omits `source_ip`.
        let log = AuditLog::new();
        log.record(entry_at(
            1_700_000_000_000_000,
            OUTCOME_SUCCESS,
            "/api/v5/api_key",
        ));
        let all = log.query(&AuditFilter::default());
        assert_eq!(all.len(), 1);
        let rendered = all[0].to_json();
        assert!(rendered.get("source_ip").is_none());
    }

    #[test]
    fn omitted_detail_fields_stay_absent() {
        // The broker tracks only the request line. It omits args,
        // failure and http_request. The test guards the omit rule.
        let log = AuditLog::new();
        log.record(entry_at(
            1_700_000_000_000_000,
            OUTCOME_SUCCESS,
            "/api/v5/users",
        ));
        let all = log.query(&AuditFilter::default());
        assert_eq!(all.len(), 1);
        let rendered = all[0].to_json();
        for field in ["args", "failure", "http_request"] {
            assert!(rendered.get(field).is_none(), "omitted field {field}");
        }
    }

    #[test]
    fn time_bounds_accept_integers_and_rfc3339() {
        // Microseconds, milliseconds and seconds all parse.
        assert_eq!(
            parse_time_bound("1700000000000000"),
            Some(1_700_000_000_000_000)
        );
        assert_eq!(
            parse_time_bound("1700000000000"),
            Some(1_700_000_000_000_000)
        );
        assert_eq!(parse_time_bound("1700000000"), Some(1_700_000_000_000_000));
        // RFC 3339 parses; garbage degrades to unfiltered.
        let micros = parse_rfc3339_to_micros("2023-11-14T22:13:20.000000+00:00")
            .expect("reference instant parses");
        assert_eq!(
            parse_time_bound("2023-11-14T22:13:20.000000+00:00"),
            Some(micros)
        );
        assert_eq!(parse_time_bound("not-a-time"), None);
        assert_eq!(parse_time_bound(""), None);
        // Non-UTC offsets are not accepted (writer only emits UTC).
        assert_eq!(parse_time_bound("2023-11-14T22:13:20.000000+08:00"), None);
    }

    #[test]
    fn operation_type_comes_from_first_path_segment() {
        assert_eq!(operation_type_for_path("/api/v5/api_key"), "api_key");
        assert_eq!(operation_type_for_path("/api/v5/login"), "login");
        assert_eq!(
            operation_type_for_path("/api/v5/users/admin/change_pwd"),
            "users"
        );
        assert_eq!(operation_type_for_path("/api/v1/rules"), "rules");
        assert_eq!(outcome_for_status(200), OUTCOME_SUCCESS);
        assert_eq!(outcome_for_status(404), OUTCOME_FAILURE);
    }
}
