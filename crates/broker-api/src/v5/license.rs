//! The licence endpoints of the v5 management API.
//!
//! IndraMQTT is open source and has no licence key. These endpoints are
//! here because the management console and the reference API have them:
//! `GET /license`, `POST /license`, `GET /license/setting`,
//! `PUT /license/setting` and `GET /license/session_hwm_history`.
//!
//! - `GET /license` gives one fixed document: an open-source licence
//!   that does not expire, with limits of [`UNLIMITED`]. The broker does
//!   not apply a limit from this document.
//! - `POST /license` accepts the `key` field of the reference API and
//!   gives the same document. It stores nothing, because no key changes
//!   what the broker does.
//! - The settings are the watermark values of the reference API. They
//!   are validated, stored and written to the data directory.
//! - The session history is a measurement: the kernel records the
//!   number of connected sessions on a timer, and this module keeps the
//!   highest value of each day for 24 months.
//!
//! Store bounds: one settings record, and at most [`MAX_HWM_DAYS`] day
//! records of three integers each. Management plane only: the publish
//! path and the deliver path do not touch this module.

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use crate::errors::ApiError;
use crate::v5::api_keys::{civil_from_days, epoch_to_rfc3339};
use crate::ApiState;

/// The value of each limit in the licence document: ten thousand
/// million. No licence limits the broker, and the console needs a number.
pub const UNLIMITED: u64 = 10_000_000_000;

/// The number of day records that the history keeps: 24 months.
pub const MAX_HWM_DAYS: usize = 24 * 31;
/// The default number of rows for `period=daily`.
const DEFAULT_DAILY_ROWS: usize = 30;
/// The default number of rows for `period=monthly`.
const DEFAULT_MONTHLY_ROWS: usize = 24;
/// The largest settings body that `PUT /license/setting` reads.
const MAX_SETTING_BODY: usize = 8 * 1024;
/// The file in the data directory that holds the settings and the
/// history.
const STATE_FILE: &str = "license_state.json";
/// The minimum time between two writes of the history to the file.
const HISTORY_SAVE_INTERVAL_SECS: u64 = 60;

/// The fixed licence document.
fn license_document() -> serde_json::Value {
    serde_json::json!({
        "customer": "Open Source",
        "customer_type": 0,
        "deployment": "open-source",
        "email": "",
        "expiry": false,
        "expiry_at": "9999-12-31",
        "max_sessions": UNLIMITED,
        "max_connections": UNLIMITED,
        "max_tps": UNLIMITED,
        "start_at": "2026-10-05",
        "type": "official",
    })
}

/// `GET /license`: the licence document.
pub async fn get_license() -> Response {
    (StatusCode::OK, Json(license_document())).into_response()
}

/// `POST /license`: the body must be a JSON object with a string `key`.
/// The answer is the licence document. Nothing is stored.
pub async fn update_license(body: Bytes) -> Response {
    let key_is_text = serde_json::from_slice::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| value.get("key").map(serde_json::Value::is_string))
        .unwrap_or(false);
    if !key_is_text {
        return ApiError::BadRequest("field `key` is required and must be a string".to_string())
            .into_response();
    }
    (StatusCode::OK, Json(license_document())).into_response()
}

/// The watermark settings of the reference API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LicenseSetting {
    pub connection_low_watermark: String,
    pub connection_high_watermark: String,
    pub high_watermark_timezone: String,
}

impl Default for LicenseSetting {
    fn default() -> Self {
        Self {
            connection_low_watermark: "75%".to_string(),
            connection_high_watermark: "80%".to_string(),
            high_watermark_timezone: "system".to_string(),
        }
    }
}

/// One day of the history: the highest number of connected sessions
/// and the time of that measurement.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
struct DayPeak {
    high_watermark: u64,
    observed_at: u64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
struct StoredState {
    #[serde(default)]
    setting: LicenseSetting,
    /// The key is the number of days from 1970-01-01.
    #[serde(default)]
    days: BTreeMap<u64, DayPeak>,
}

struct Inner {
    state: StoredState,
    file: Option<PathBuf>,
    history_dirty: bool,
    last_history_save: u64,
}

/// The settings and the session history behind one short lock.
pub struct LicenseState {
    inner: Mutex<Inner>,
}

impl LicenseState {
    /// An empty state that is not written to a file.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                state: StoredState::default(),
                file: None,
                history_dirty: false,
                last_history_save: 0,
            }),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|p| p.into_inner())
    }

    /// Reads the state file of the data directory, and writes each later
    /// change to it. A file that is absent gives the defaults. A file
    /// that cannot be read is logged and gives the defaults.
    pub fn load_from_dir(&self, dir: &Path) {
        let path = dir.join(STATE_FILE);
        let mut inner = self.lock();
        match std::fs::read(&path) {
            Ok(bytes) => match serde_json::from_slice::<StoredState>(&bytes) {
                Ok(mut state) => {
                    if validate_setting(&state.setting).is_err() {
                        state.setting = LicenseSetting::default();
                    }
                    trim_days(&mut state.days);
                    inner.state = state;
                }
                Err(error) => {
                    tracing::warn!(%error, "the licence state file is not valid; defaults are used");
                }
            },
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => {
                tracing::warn!(%error, "cannot read the licence state file; defaults are used");
            }
        }
        inner.file = Some(path);
    }

    /// The settings at this time.
    pub fn setting(&self) -> LicenseSetting {
        self.lock().state.setting.clone()
    }

    /// Replaces the settings and writes them to the file. When the
    /// write fails, the settings before the call stay.
    pub fn set_setting(&self, next: LicenseSetting) -> Result<LicenseSetting, String> {
        let mut inner = self.lock();
        let previous = std::mem::replace(&mut inner.state.setting, next);
        if let Err(error) = save(&inner) {
            inner.state.setting = previous;
            return Err(format!("cannot write the licence settings: {error}"));
        }
        Ok(inner.state.setting.clone())
    }

    /// Records one measurement of the number of connected sessions. The
    /// kernel calls this on a timer. The file is written at most one
    /// time in [`HISTORY_SAVE_INTERVAL_SECS`].
    pub fn record_sessions(&self, sessions: u64, now_secs: u64) {
        let mut inner = self.lock();
        let day = now_secs / 86_400;
        let changed = match inner.state.days.get_mut(&day) {
            Some(peak) if sessions > peak.high_watermark => {
                peak.high_watermark = sessions;
                peak.observed_at = now_secs;
                true
            }
            Some(_) => false,
            None => {
                inner.state.days.insert(
                    day,
                    DayPeak {
                        high_watermark: sessions,
                        observed_at: now_secs,
                    },
                );
                trim_days(&mut inner.state.days);
                true
            }
        };
        inner.history_dirty |= changed;
        if inner.history_dirty
            && now_secs.saturating_sub(inner.last_history_save) >= HISTORY_SAVE_INTERVAL_SECS
        {
            inner.last_history_save = now_secs;
            match save(&inner) {
                Ok(()) => inner.history_dirty = false,
                Err(error) => {
                    tracing::warn!(%error, "cannot write the session history");
                }
            }
        }
    }

    /// The rows of the history, the newest first.
    fn history(&self, period: Period, limit: usize) -> Vec<serde_json::Value> {
        let inner = self.lock();
        let mut rows: Vec<(String, DayPeak)> = Vec::new();
        for (day, peak) in inner.state.days.iter().rev() {
            let (year, month, date) = civil_from_days(*day as i64);
            let label = match period {
                Period::Daily => format!("{year:04}-{month:02}-{date:02}"),
                Period::Monthly => format!("{year:04}-{month:02}"),
            };
            match rows.last_mut() {
                Some((last, kept)) if *last == label => {
                    if peak.high_watermark > kept.high_watermark {
                        *kept = *peak;
                    }
                }
                _ => {
                    if rows.len() == limit {
                        break;
                    }
                    rows.push((label, *peak));
                }
            }
        }
        rows.into_iter()
            .map(|(label, peak)| {
                serde_json::json!({
                    "period": label,
                    "high_watermark": peak.high_watermark,
                    "observed_at": epoch_to_rfc3339(peak.observed_at),
                })
            })
            .collect()
    }
}

impl Default for LicenseState {
    fn default() -> Self {
        Self::new()
    }
}

/// Removes the oldest days until at most [`MAX_HWM_DAYS`] stay.
fn trim_days(days: &mut BTreeMap<u64, DayPeak>) {
    while days.len() > MAX_HWM_DAYS {
        days.pop_first();
    }
}

/// Writes the state to its file through a temporary file. Without a
/// file this function does nothing.
fn save(inner: &Inner) -> std::io::Result<()> {
    let Some(path) = &inner.file else {
        return Ok(());
    };
    let bytes = serde_json::to_vec(&inner.state).map_err(std::io::Error::other)?;
    let tmp = path.with_extension("json.part");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

/// Reads a text such as `75%`. The value must be from 0 to 100.
fn parse_percent(text: &str) -> Option<f64> {
    let number: f64 = text.strip_suffix('%')?.trim().parse().ok()?;
    (0.0..=100.0).contains(&number).then_some(number)
}

fn validate_setting(setting: &LicenseSetting) -> Result<(), String> {
    let low = parse_percent(&setting.connection_low_watermark)
        .ok_or("field `connection_low_watermark` must be a percentage such as `75%`")?;
    let high = parse_percent(&setting.connection_high_watermark)
        .ok_or("field `connection_high_watermark` must be a percentage such as `80%`")?;
    if low >= high {
        return Err(
            "field `connection_low_watermark` must be less than `connection_high_watermark`"
                .to_string(),
        );
    }
    let zone = setting.high_watermark_timezone.as_str();
    if zone.is_empty() || zone.len() > 64 || zone.chars().any(char::is_control) {
        return Err("field `high_watermark_timezone` is not valid".to_string());
    }
    Ok(())
}

/// `GET /license/setting`: the watermark settings.
pub async fn get_license_setting(State(state): State<ApiState>) -> Response {
    (StatusCode::OK, Json(state.license.setting())).into_response()
}

/// `PUT /license/setting`: changes the fields that the body has. A field
/// that the body does not have keeps its value. All the fields are
/// examined before one of them changes.
pub async fn put_license_setting(State(state): State<ApiState>, body: Bytes) -> Response {
    if body.len() > MAX_SETTING_BODY {
        return ApiError::BadRequest("the body is too large".to_string()).into_response();
    }
    let object = match serde_json::from_slice::<serde_json::Value>(&body) {
        Ok(serde_json::Value::Object(object)) => object,
        _ => {
            return ApiError::BadRequest("the body must be a JSON object".to_string())
                .into_response();
        }
    };
    let mut next = state.license.setting();
    for (field, slot) in [
        (
            "connection_low_watermark",
            &mut next.connection_low_watermark,
        ),
        (
            "connection_high_watermark",
            &mut next.connection_high_watermark,
        ),
        ("high_watermark_timezone", &mut next.high_watermark_timezone),
    ] {
        match object.get(field) {
            None => {}
            Some(serde_json::Value::String(text)) => *slot = text.clone(),
            Some(_) => {
                return ApiError::BadRequest(format!("field `{field}` must be a string"))
                    .into_response();
            }
        }
    }
    if let Err(message) = validate_setting(&next) {
        return ApiError::BadRequest(message).into_response();
    }
    match state.license.set_setting(next) {
        Ok(stored) => (StatusCode::OK, Json(stored)).into_response(),
        Err(message) => ApiError::InternalError(message).into_response(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Period {
    Daily,
    Monthly,
}

/// `GET /license/session_hwm_history`: the highest number of connected
/// sessions for each day or each month, the newest first. `period` is
/// `daily` or `monthly` (the default). `limit` is the maximum number of
/// rows: 30 for `daily` and 24 for `monthly` when it is absent.
pub async fn get_session_hwm_history(
    State(state): State<ApiState>,
    Query(query): Query<HashMap<String, String>>,
) -> Response {
    let period = match query.get("period").map(String::as_str) {
        None | Some("monthly") => Period::Monthly,
        Some("daily") => Period::Daily,
        Some(_) => {
            return ApiError::BadRequest("`period` must be `daily` or `monthly`".to_string())
                .into_response();
        }
    };
    let default_rows = match period {
        Period::Daily => DEFAULT_DAILY_ROWS,
        Period::Monthly => DEFAULT_MONTHLY_ROWS,
    };
    let limit = match query.get("limit") {
        None => default_rows,
        Some(raw) => match raw.trim().parse::<usize>() {
            Ok(value) if value >= 1 => value.min(MAX_HWM_DAYS),
            _ => {
                return ApiError::BadRequest("`limit` must be an integer from 1".to_string())
                    .into_response();
            }
        },
    };
    let data = state.license.history(period, limit);
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "period": match period {
                Period::Daily => "daily",
                Period::Monthly => "monthly",
            },
            "count": data.len(),
            "data": data,
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    const DAY: u64 = 86_400;
    // 2026-10-05 00:00:00 UTC.
    const OCT_5: u64 = 20_731 * DAY;

    #[test]
    fn the_document_has_the_fields_of_the_reference_and_no_real_limit() {
        let doc = license_document();
        for field in [
            "customer",
            "customer_type",
            "deployment",
            "email",
            "expiry",
            "expiry_at",
            "max_sessions",
            "start_at",
            "type",
        ] {
            assert!(doc.get(field).is_some(), "field {field}");
        }
        assert_eq!(doc["max_sessions"], serde_json::json!(10_000_000_000u64));
        assert_eq!(doc["expiry"], serde_json::json!(false));
    }

    #[test]
    fn the_history_keeps_the_peak_of_each_day_and_folds_days_into_months() {
        let state = LicenseState::new();
        assert_eq!(epoch_to_rfc3339(OCT_5), "2026-10-05T00:00:00+00:00");
        state.record_sessions(5, OCT_5 + 10);
        state.record_sessions(9, OCT_5 + 20);
        state.record_sessions(3, OCT_5 + 30);
        state.record_sessions(4, OCT_5 + DAY);
        // A day of the month before.
        state.record_sessions(40, OCT_5 - 10 * DAY);

        let daily = state.history(Period::Daily, 30);
        assert_eq!(daily.len(), 3);
        assert_eq!(daily[0]["period"], "2026-10-06");
        assert_eq!(daily[0]["high_watermark"], 4);
        assert_eq!(daily[1]["period"], "2026-10-05");
        assert_eq!(daily[1]["high_watermark"], 9);
        assert_eq!(daily[1]["observed_at"], "2026-10-05T00:00:20+00:00");
        assert_eq!(daily[2]["period"], "2026-09-25");

        let monthly = state.history(Period::Monthly, 24);
        assert_eq!(monthly.len(), 2);
        assert_eq!(monthly[0]["period"], "2026-10");
        assert_eq!(monthly[0]["high_watermark"], 9);
        assert_eq!(monthly[1]["period"], "2026-09");
        assert_eq!(monthly[1]["high_watermark"], 40);

        assert_eq!(state.history(Period::Daily, 1).len(), 1);
    }

    #[test]
    fn the_history_is_bounded() {
        let state = LicenseState::new();
        for day in 0..(MAX_HWM_DAYS as u64 + 50) {
            state.record_sessions(day, day * DAY);
        }
        let all = state.history(Period::Daily, usize::MAX);
        assert_eq!(all.len(), MAX_HWM_DAYS);
        assert_eq!(all[0]["high_watermark"], MAX_HWM_DAYS as u64 + 49);
    }

    #[test]
    fn the_settings_are_validated() {
        let ok = LicenseSetting::default();
        assert!(validate_setting(&ok).is_ok());
        for (low, high, zone) in [
            ("80%", "80%", "system"),
            ("90%", "80%", "system"),
            ("75", "80%", "system"),
            ("75%", "101%", "system"),
            ("75%", "80%", ""),
        ] {
            let bad = LicenseSetting {
                connection_low_watermark: low.to_string(),
                connection_high_watermark: high.to_string(),
                high_watermark_timezone: zone.to_string(),
            };
            assert!(validate_setting(&bad).is_err(), "{low} {high} {zone:?}");
        }
    }

    #[test]
    fn the_state_stays_after_a_restart() {
        let dir = std::env::temp_dir().join(format!(
            "indra-license-state-{}-{}",
            std::process::id(),
            OCT_5
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let state = LicenseState::new();
        state.load_from_dir(&dir);
        state
            .set_setting(LicenseSetting {
                connection_low_watermark: "50%".to_string(),
                connection_high_watermark: "60%".to_string(),
                high_watermark_timezone: "system".to_string(),
            })
            .expect("settings are written");
        state.record_sessions(7, OCT_5);

        let again = LicenseState::new();
        again.load_from_dir(&dir);
        assert_eq!(again.setting().connection_low_watermark, "50%");
        let rows = again.history(Period::Daily, 30);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["high_watermark"], 7);
        std::fs::remove_dir_all(&dir).ok();
    }
}
