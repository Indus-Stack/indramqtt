//! Delayed publish scheduler: one shared hierarchical timer wheel plus a
//! file-backed pending log behind it.
//!
//! The publish event records each `$delayed/<secs>/<topic>` request here
//! and gets an immediate ack; a single background driver task ticks the
//! wheel and hands due entries back for delivery through the normal
//! ingress pipeline. There is one wheel per node, never one task per
//! message.
//!
//! Wheel shape (all levels advance off the delivery path, in the driver
//! task only): tick 100 ms, L0 holds 10 slots of 100 ms (one second),
//! L1 holds 60 slots of one second (one minute), L2 holds 60 slots of one
//! minute (one hour), L3 holds 24 slots of one hour (one day). The total
//! span covers the default delay bound with room to spare; entries past
//! the span wait in a bounded overflow list that the driver scans each
//! tick (unreachable while the delay bound stays at or under a day).
//!
//! Persistence (`<data-dir>/delayed.jsonl`, one JSON object per line) is
//! appended before the publisher is acknowledged and reloaded at boot, so
//! a restart keeps pending entries on schedule. Torn lines and entries
//! already past their deadline at reload are discarded with a counter and
//! never delivered. Fired entries are removed from the file after
//! delivery by rewriting it without their ids.
//!
//! Bounds (every accumulation has one, with the reason next to it):
//! * delay bound, configurable, default one day: longer deferrals pin
//!   memory for no realistic device schedule and only bound the sleep
//!   length before; past it the request is dropped with a warning.
//! * pending backlog cap, configurable through the management API,
//!   default 10,000: past it new requests drop with a warning instead of
//!   growing memory without limit. Zero selects no cap, and only an
//!   explicit operator write selects it; the default stays finite.
//! * per-message payload cap of 1 MiB: mirrors the retained-store default
//!   ceiling so a delayed publish can never hold more than a retained
//!   publish would store; larger payloads drop with a warning.
//!
//! Runtime switch: `enable` gates deferral. While off, `$delayed/`
//! targets route as normal topics and nothing new is queued; entries
//! already queued still fire. One relaxed atomic load per publish.
//!
//! Management reads (`snapshot`, `get`, `remove_one`, `remove_by_topic`)
//! take one short wheel lock and clone bounded entries. They run on the
//! management plane only, never on the publish or deliver path.
//!
//! Publish-path cost: one relaxed atomic load for the switch, one for
//! the delay bound, one file append plus fsync of a single bounded entry,
//! and one short mutex hold for the wheel insert. Wheel cascading, expiry
//! scans and the file rewrite on fire all run in the driver task, never
//! on the publish or deliver path.

use parking_lot::{Mutex, RwLock};
use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

/// Default upper bound for `$delayed` deferrals.
///
/// Rationale: beyond a day the request pins a wheel slot and a file row
/// for no realistic device schedule; the previous per-message sleep used
/// the same one-day bound, so existing publishers see the same ceiling.
pub const DEFAULT_MAX_DELAYED_SECS: u64 = 86_400;

/// Maximum delayed entries waiting at once, in memory and on disk.
///
/// Rationale: 10,000 matches the detached offline queue bound so delayed
/// backlog shares one memory story with the existing per-session queues;
/// at the 1 MiB payload cap the worst case stays near 10 GiB only when
/// every entry is maximal, while realistic small payloads stay in the low
/// tens of MB. Past the cap new requests drop instead of growing without
/// limit.
pub const MAX_DELAYED_PENDING: usize = 10_000;

/// Maximum payload bytes held per delayed entry.
///
/// Rationale: mirrors the retained-store default ceiling (1 MiB) so a
/// delayed publish can never hold more than a retained publish would
/// store. Larger payloads are dropped with a warning and a counter.
// TODO(parity): should this cap track the configured retainer
// `max_payload_size` dynamically instead of mirroring its default? The
// spec bounds per-message memory but does not say whether the two caps
// must move together; current choice keeps the wheel independent of the
// retainer config lock on the publish path.
pub const MAX_DELAYED_PAYLOAD_BYTES: usize = 1_048_576;

/// Driver tick interval in milliseconds. Entries fire within one tick of
/// their deadline.
pub const DELAYED_TICK_MS: u64 = 100;

/// Persistence file inside the kernel data directory.
pub const DELAYED_FILE_NAME: &str = "delayed.jsonl";

const L0_SLOTS: usize = 10;
const L1_SLOTS: usize = 60;
const L2_SLOTS: usize = 60;
const L3_SLOTS: usize = 24;
const L0_TICK_MS: u64 = 100;
const L1_TICK_MS: u64 = 1_000;
const L2_TICK_MS: u64 = 60_000;
const L3_TICK_MS: u64 = 3_600_000;

/// One deferred publish waiting on the wheel.
#[derive(Debug, Clone)]
pub struct DelayedEntry {
    /// Unique id within this scheduler run (persisted, used to remove
    /// fired entries from the file).
    pub id: u64,
    /// Wall-clock deadline as millis since the Unix epoch.
    pub deliver_at_ms: u64,
    /// Schedule instant as millis since the Unix epoch. The management
    /// list renders it as `publish_at` and derives the interval from it.
    pub publish_at_ms: u64,
    /// Inner topic (without the `$delayed/<secs>/` marker).
    pub topic: String,
    /// MQTT wire QoS (0, 1 or 2).
    pub qos: u8,
    /// Retain flag applied at delivery time, not at schedule time.
    pub retain: bool,
    /// Raw payload bytes, bounded by [`MAX_DELAYED_PAYLOAD_BYTES`].
    pub payload: Vec<u8>,
    /// Publishing client id, kept so shared-subscription hashing stays
    /// stable across the defer. `None` when the session is gone.
    pub publisher: Option<String>,
    /// Publishing user name, kept so the management list can show it.
    /// `None` when the session carries no user name.
    pub username: Option<String>,
    /// Publishing tenant id, kept so fan-out after the timer stays inside
    /// the same tenant the entry was scheduled in (MT-02). Defaults to
    /// the default tenant for entries persisted before tenancy.
    pub publisher_tenant: String,
}

/// Why a schedule request was refused. Counters for every variant live
/// on the scheduler; the caller only logs the warning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ScheduleError {
    /// Past the configured delay bound.
    OverLimit,
    /// Payload past [`MAX_DELAYED_PAYLOAD_BYTES`].
    PayloadTooLarge,
    /// Wheel and file already hold the configured pending cap.
    Overflow,
    /// The file append failed; nothing was queued.
    PersistFailed(String),
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

struct WheelInner {
    l0: Vec<Vec<DelayedEntry>>,
    l1: Vec<Vec<DelayedEntry>>,
    l2: Vec<Vec<DelayedEntry>>,
    l3: Vec<Vec<DelayedEntry>>,
    overflow: Vec<DelayedEntry>,
    c0: usize,
    c1: usize,
    c2: usize,
    c3: usize,
}

impl WheelInner {
    fn new() -> Self {
        Self {
            l0: (0..L0_SLOTS).map(|_| Vec::new()).collect(),
            l1: (0..L1_SLOTS).map(|_| Vec::new()).collect(),
            l2: (0..L2_SLOTS).map(|_| Vec::new()).collect(),
            l3: (0..L3_SLOTS).map(|_| Vec::new()).collect(),
            overflow: Vec::new(),
            c0: 0,
            c1: 0,
            c2: 0,
            c3: 0,
        }
    }

    fn len(&self) -> usize {
        self.l0.iter().map(Vec::len).sum::<usize>()
            + self.l1.iter().map(Vec::len).sum::<usize>()
            + self.l2.iter().map(Vec::len).sum::<usize>()
            + self.l3.iter().map(Vec::len).sum::<usize>()
            + self.overflow.len()
    }

    fn place(&mut self, entry: DelayedEntry, now: u64) {
        let delay_ms = entry.deliver_at_ms.saturating_sub(now);
        if delay_ms <= L0_SLOTS as u64 * L0_TICK_MS {
            // Floor division alone maps any sub-tick delay to offset 0
            // (the slot just consumed), which would then need a full
            // rotation to fire. Clamp to at least one slot ahead so an
            // imminent entry fires on the next tick; early slots are
            // safe because `tick` re-queues not-yet-due entries.
            let offset = ((delay_ms / L0_TICK_MS) as usize).max(1);
            let slot = (self.c0 + offset) % L0_SLOTS;
            self.l0[slot].push(entry);
        } else if delay_ms <= L1_SLOTS as u64 * L1_TICK_MS {
            let slot = (self.c1 + (delay_ms / L1_TICK_MS) as usize) % L1_SLOTS;
            self.l1[slot].push(entry);
        } else if delay_ms <= L2_SLOTS as u64 * L2_TICK_MS {
            let slot = (self.c2 + (delay_ms / L2_TICK_MS) as usize) % L2_SLOTS;
            self.l2[slot].push(entry);
        } else if delay_ms <= L3_SLOTS as u64 * L3_TICK_MS {
            let slot = (self.c3 + (delay_ms / L3_TICK_MS) as usize) % L3_SLOTS;
            self.l3[slot].push(entry);
        } else {
            self.overflow.push(entry);
        }
    }

    /// Run one slot visitor over every stored entry bucket, including
    /// the overflow list. One pass, no allocation.
    fn each_slot_mut(&mut self, mut visit: impl FnMut(&mut Vec<DelayedEntry>)) {
        for level in [&mut self.l0, &mut self.l1, &mut self.l2, &mut self.l3] {
            for slot in level.iter_mut() {
                visit(slot);
            }
        }
        visit(&mut self.overflow);
    }

    /// Drop one entry by id from every bucket. True when found.
    fn remove_id(&mut self, id: u64) -> bool {
        let mut found = false;
        self.each_slot_mut(|slot| {
            let before = slot.len();
            slot.retain(|entry| entry.id != id);
            if slot.len() != before {
                found = true;
            }
        });
        found
    }

    /// Drop every entry for one inner topic. Returns the fired ids so
    /// the caller can rewrite the file without them.
    fn remove_topic(&mut self, topic: &str) -> Vec<u64> {
        let mut ids = Vec::new();
        self.each_slot_mut(|slot| {
            slot.retain(|entry| {
                if entry.topic == topic {
                    ids.push(entry.id);
                    false
                } else {
                    true
                }
            });
        });
        ids
    }

    /// Clone every waiting entry. The caller sorts the copy; the wheel
    /// order is slot order, not id order.
    fn snapshot(&self) -> Vec<DelayedEntry> {
        let mut out = Vec::with_capacity(self.len());
        for level in [&self.l0, &self.l1, &self.l2, &self.l3] {
            for slot in level.iter() {
                out.extend(slot.iter().cloned());
            }
        }
        out.extend(self.overflow.iter().cloned());
        out
    }

    /// Advance one 100 ms tick, cascade wrapped levels down, and return
    /// entries whose deadline has passed. Due-ness is re-checked per
    /// entry (a slot can hold entries with slightly different deadlines)
    /// and not-yet-due entries stay queued.
    fn tick(&mut self, now: u64) -> Vec<DelayedEntry> {
        let mut due = Vec::new();
        self.c0 = (self.c0 + 1) % L0_SLOTS;
        let slot = std::mem::take(&mut self.l0[self.c0]);
        for entry in slot {
            if entry.deliver_at_ms <= now {
                due.push(entry);
            } else {
                self.place(entry, now);
            }
        }
        if self.c0 == 0 {
            self.c1 = (self.c1 + 1) % L1_SLOTS;
            let cascade = std::mem::take(&mut self.l1[self.c1]);
            for entry in cascade {
                if entry.deliver_at_ms <= now {
                    due.push(entry);
                } else {
                    self.place(entry, now);
                }
            }
            if self.c1 == 0 {
                self.c2 = (self.c2 + 1) % L2_SLOTS;
                let cascade = std::mem::take(&mut self.l2[self.c2]);
                for entry in cascade {
                    if entry.deliver_at_ms <= now {
                        due.push(entry);
                    } else {
                        self.place(entry, now);
                    }
                }
                if self.c2 == 0 {
                    self.c3 = (self.c3 + 1) % L3_SLOTS;
                    let cascade = std::mem::take(&mut self.l3[self.c3]);
                    for entry in cascade {
                        if entry.deliver_at_ms <= now {
                            due.push(entry);
                        } else {
                            self.place(entry, now);
                        }
                    }
                }
            }
        }
        let mut still_waiting = Vec::new();
        for entry in std::mem::take(&mut self.overflow) {
            if entry.deliver_at_ms <= now {
                due.push(entry);
            } else {
                still_waiting.push(entry);
            }
        }
        self.overflow = still_waiting;
        due
    }
}

fn persist_path(dir: &std::path::Path) -> std::path::PathBuf {
    dir.join(DELAYED_FILE_NAME)
}

fn encode_entry(entry: &DelayedEntry) -> String {
    use base64::Engine as _;
    // One base64 string (~4/3 bytes per payload byte) instead of one JSON
    // number per byte: no intermediate Vec<u64> alloc and roughly a third
    // of the file bytes at the 1 MiB cap. Encoded on the publish path
    // before ack; decoded once at reload and once per fire.
    let payload_b64 = base64::engine::general_purpose::STANDARD.encode(&entry.payload);
    let value = serde_json::json!({
        "id": entry.id,
        "deliver_at_ms": entry.deliver_at_ms,
        "publish_at_ms": entry.publish_at_ms,
        "topic": entry.topic,
        "qos": entry.qos,
        "retain": entry.retain,
        "payload": payload_b64,
        "publisher": entry.publisher,
        "username": entry.username,
        "publisher_tenant": entry.publisher_tenant,
    });
    serde_json::to_string(&value).unwrap_or_else(|_| "{}".to_string())
}

/// Parse one file line. `None` means torn: unparseable, missing a field,
/// or carrying values the scheduler can never serve. The payload accepts
/// the current base64 string form and the legacy array-of-numbers form so
/// a file written before the compact encoding still reloads.
fn decode_entry(value: &serde_json::Value) -> Option<DelayedEntry> {
    let id = value.get("id")?.as_u64()?;
    let deliver_at_ms = value.get("deliver_at_ms")?.as_u64()?;
    // Rows persisted before schedule-time recording carry no publish
    // instant: fall back to the deadline so the interval renders as the
    // minimum instead of dropping a pending row.
    let publish_at_ms = value
        .get("publish_at_ms")
        .and_then(serde_json::Value::as_u64)
        .unwrap_or(deliver_at_ms);
    let topic = value.get("topic")?.as_str()?.to_string();
    if topic.is_empty() {
        return None;
    }
    let qos = value.get("qos")?.as_u64()?;
    if qos > 2 {
        return None;
    }
    let retain = value.get("retain")?.as_bool()?;
    let payload_value = value.get("payload")?;
    let payload = if let Some(encoded) = payload_value.as_str() {
        use base64::Engine as _;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .ok()?;
        if bytes.len() > MAX_DELAYED_PAYLOAD_BYTES {
            return None;
        }
        bytes
    } else {
        let payload_array = payload_value.as_array()?;
        if payload_array.len() > MAX_DELAYED_PAYLOAD_BYTES {
            return None;
        }
        let mut payload = Vec::with_capacity(payload_array.len());
        for byte in payload_array {
            let byte = byte.as_u64()?;
            if byte > 255 {
                return None;
            }
            payload.push(byte as u8);
        }
        payload
    };
    let publisher = match value.get("publisher") {
        None => None,
        Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(name)) => Some(name.clone()),
        Some(_) => return None,
    };
    // Rows persisted before user-name recording carry no name: an absent
    // name reads as unknown, never as another user.
    let username = match value.get("username") {
        None | Some(serde_json::Value::Null) => None,
        Some(serde_json::Value::String(name)) => Some(name.clone()),
        Some(_) => return None,
    };
    // Entries persisted before tenancy carry no tenant: fail closed to
    // the default tenant, never to another tenant's space.
    let publisher_tenant = match value.get("publisher_tenant") {
        None | Some(serde_json::Value::Null) => {
            broker_session::tenant::DEFAULT_TENANT_ID.to_string()
        }
        Some(serde_json::Value::String(tenant)) => tenant.clone(),
        Some(_) => return None,
    };
    Some(DelayedEntry {
        id,
        deliver_at_ms,
        publish_at_ms,
        topic,
        qos: qos as u8,
        retain,
        payload,
        publisher,
        username,
        publisher_tenant,
    })
}

/// One shared delayed-publish scheduler for the node.
pub struct DelayedScheduler {
    wheel: Mutex<WheelInner>,
    /// Serialises every file access (appends, reloads, rewrites) so a
    /// tick rewrite can never lose a concurrent publish append. Always
    /// taken before the wheel lock when both are needed.
    file_lock: Mutex<()>,
    persist_dir: RwLock<Option<std::path::PathBuf>>,
    max_secs: AtomicU64,
    /// Pending cap for wheel plus file. Zero means no cap, and only an
    /// explicit operator write selects it; the default stays finite.
    max_messages: AtomicUsize,
    /// Deferral switch. Off routes `$delayed/` targets as normal topics
    /// and queues nothing new; queued entries still fire.
    enabled: AtomicBool,
    next_id: AtomicU64,
    driver_started: AtomicU64,
    scheduled: AtomicU64,
    delivered: AtomicU64,
    dropped_over_limit: AtomicU64,
    dropped_malformed: AtomicU64,
    dropped_overflow: AtomicU64,
    dropped_payload: AtomicU64,
    dropped_io: AtomicU64,
    recovery_torn: AtomicU64,
    recovery_expired: AtomicU64,
}

impl DelayedScheduler {
    /// Scheduler with default bounds and no persistence directory (pure
    /// in-memory until [`DelayedScheduler::set_persist_dir`] runs).
    pub fn new() -> Self {
        Self {
            wheel: Mutex::new(WheelInner::new()),
            file_lock: Mutex::new(()),
            persist_dir: RwLock::new(None),
            max_secs: AtomicU64::new(DEFAULT_MAX_DELAYED_SECS),
            max_messages: AtomicUsize::new(MAX_DELAYED_PENDING),
            enabled: AtomicBool::new(true),
            next_id: AtomicU64::new(1),
            driver_started: AtomicU64::new(0),
            scheduled: AtomicU64::new(0),
            delivered: AtomicU64::new(0),
            dropped_over_limit: AtomicU64::new(0),
            dropped_malformed: AtomicU64::new(0),
            dropped_overflow: AtomicU64::new(0),
            dropped_payload: AtomicU64::new(0),
            dropped_io: AtomicU64::new(0),
            recovery_torn: AtomicU64::new(0),
            recovery_expired: AtomicU64::new(0),
        }
    }

    /// Configured delay bound in seconds (one relaxed load; the publish
    /// path pays nothing else to read it).
    pub fn max_secs(&self) -> u64 {
        self.max_secs.load(Ordering::Relaxed)
    }

    /// Override the delay bound (boot applies `--delayed-max-secs`; a
    /// zero value refuses every non-zero delay).
    pub fn set_max_secs(&self, secs: u64) {
        self.max_secs.store(secs, Ordering::Relaxed);
    }

    /// True while `$delayed/` targets defer (one relaxed load; the
    /// publish path pays nothing else to read it).
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Switch deferral on or off at runtime. Switching off queues
    /// nothing new; entries already queued still fire on schedule.
    pub fn set_enable(&self, enable: bool) {
        self.enabled.store(enable, Ordering::Relaxed);
    }

    /// Current pending cap for wheel plus file (one relaxed load).
    /// Zero means no cap by explicit operator choice.
    pub fn max_messages(&self) -> usize {
        self.max_messages.load(Ordering::Relaxed)
    }

    /// Override the pending cap. Zero selects no cap explicitly; any
    /// other value bounds wheel plus file together.
    pub fn set_max_messages(&self, max: usize) {
        self.max_messages.store(max, Ordering::Relaxed);
    }

    /// Effective pending cap: the configured value, or no bound when the
    /// operator explicitly selected zero.
    fn pending_cap(&self) -> usize {
        let cap = self.max_messages.load(Ordering::Relaxed);
        if cap == 0 {
            usize::MAX
        } else {
            cap
        }
    }

    /// Point persistence at `dir` (`delayed.jsonl` inside it). The
    /// directory is created when missing.
    pub fn set_persist_dir(&self, dir: impl AsRef<std::path::Path>) {
        *self.persist_dir.write() = Some(dir.as_ref().to_path_buf());
    }

    /// Next entry id (one relaxed atomic add; ids only need to be unique
    /// within one file generation).
    pub fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Entries currently waiting on the wheel.
    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.wheel.lock().len()
    }

    /// True when no entry waits on the wheel.
    #[allow(dead_code)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clone of every waiting entry, ordered by id. One short wheel
    /// lock; management plane only, never on the publish path.
    pub fn snapshot(&self) -> Vec<DelayedEntry> {
        let mut entries = self.wheel.lock().snapshot();
        entries.sort_by_key(|entry| entry.id);
        entries
    }

    /// Clone of one waiting entry by id. One short wheel lock;
    /// management plane only, never on the publish path.
    pub fn get(&self, id: u64) -> Option<DelayedEntry> {
        self.wheel
            .lock()
            .snapshot()
            .into_iter()
            .find(|entry| entry.id == id)
    }

    /// Drop one waiting entry by id from the wheel and the file. True
    /// when the entry waited. Management plane only.
    pub fn remove_one(&self, id: u64) -> bool {
        let removed = self.wheel.lock().remove_id(id);
        if removed {
            self.remove_ids(&[id]);
        }
        removed
    }

    /// Drop every waiting entry for one inner topic from the wheel and
    /// the file. Returns the count dropped. Management plane only.
    pub fn remove_by_topic(&self, topic: &str) -> usize {
        let ids = self.wheel.lock().remove_topic(topic);
        let count = ids.len();
        self.remove_ids(&ids);
        count
    }

    /// Claim the single background driver task. Returns true for exactly
    /// one caller; every other caller must not spawn a second driver.
    pub fn claim_driver(&self) -> bool {
        self.driver_started
            .compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
    }

    /// Count one malformed inner topic dropped by the caller (the topic
    /// parsed as a delayed marker but is not a servable concrete topic).
    pub fn note_malformed(&self) {
        self.dropped_malformed.fetch_add(1, Ordering::Relaxed);
    }

    /// Count one delivery completed by the driver.
    pub fn note_delivered(&self) {
        self.delivered.fetch_add(1, Ordering::Relaxed);
    }

    /// Validate, persist (when a directory is set) and queue one entry.
    /// The file append happens before the wheel insert while holding the
    /// file lock, so a persisted entry is never missing from the reload
    /// set and the publisher can be acknowledged on success.
    pub fn persist_and_schedule(
        self: &Arc<Self>,
        entry: DelayedEntry,
        delay_secs: u64,
    ) -> Result<(), ScheduleError> {
        if delay_secs > self.max_secs.load(Ordering::Relaxed) {
            self.dropped_over_limit.fetch_add(1, Ordering::Relaxed);
            return Err(ScheduleError::OverLimit);
        }
        if entry.payload.len() > MAX_DELAYED_PAYLOAD_BYTES {
            self.dropped_payload.fetch_add(1, Ordering::Relaxed);
            return Err(ScheduleError::PayloadTooLarge);
        }
        let cap = self.pending_cap();
        let _file_guard = self.file_lock.lock();
        {
            let wheel = self.wheel.lock();
            if wheel.len() >= cap {
                self.dropped_overflow.fetch_add(1, Ordering::Relaxed);
                return Err(ScheduleError::Overflow);
            }
        }
        if let Some(dir) = self.persist_dir.read().clone() {
            if let Err(reason) = append_line(&dir, &encode_entry(&entry)) {
                self.dropped_io.fetch_add(1, Ordering::Relaxed);
                return Err(ScheduleError::PersistFailed(reason));
            }
        }
        let now = now_ms();
        self.wheel.lock().place(entry, now);
        self.scheduled.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    /// Advance one tick and return due entries, already removed from the
    /// wheel. The caller delivers them and then calls
    /// [`DelayedScheduler::remove_ids`] so the file follows. File work
    /// stays in the driver task, never on the publish path. Takes only
    /// the wheel lock: the 100 ms driver tick never holds `file_lock`,
    /// so it cannot serialise against publish-path appends.
    pub fn tick_due(self: &Arc<Self>) -> Vec<DelayedEntry> {
        let now = now_ms();
        self.wheel.lock().tick(now)
    }

    /// Forget fired ids from the persistence file (rewrite without them).
    /// Torn lines met during the rewrite are compacted silently: they
    /// were already counted at load time. A failed rewrite is logged and
    /// counted in `dropped_io` (fail closed: the ids stay in the file and
    /// may redeliver after a restart rather than being lost silently).
    pub fn remove_ids(&self, ids: &[u64]) {
        if ids.is_empty() {
            return;
        }
        let Some(dir) = self.persist_dir.read().clone() else {
            return;
        };
        let _file_guard = self.file_lock.lock();
        let wanted: HashSet<u64> = ids.iter().copied().collect();
        let path = persist_path(&dir);
        let text = std::fs::read_to_string(&path).unwrap_or_default();
        let mut kept = Vec::new();
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let keep = match serde_json::from_str::<serde_json::Value>(line) {
                Ok(value) => match value.get("id").and_then(serde_json::Value::as_u64) {
                    Some(id) => !wanted.contains(&id),
                    None => false,
                },
                Err(_) => false,
            };
            if keep {
                kept.push(line.to_string());
            }
        }
        if !write_lines(&dir, &kept) {
            self.dropped_io.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                "Delayed rewrite failed: {} fired ids stay in delayed.jsonl and may redeliver after restart",
                wanted.len()
            );
        }
    }

    /// Reload persisted entries into the wheel. Torn lines and entries
    /// already past their deadline are discarded with a counter and never
    /// queued; entries past the backlog cap drop with the overflow
    /// counter. The file is rewritten to hold exactly the kept entries.
    /// Returns `(loaded, torn, expired)`.
    pub fn load(&self) -> (usize, u64, u64) {
        let Some(dir) = self.persist_dir.read().clone() else {
            return (0, 0, 0);
        };
        let _file_guard = self.file_lock.lock();
        let path = persist_path(&dir);
        let text = match std::fs::read_to_string(&path) {
            Ok(text) => text,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return (0, 0, 0),
            Err(_) => {
                self.recovery_torn.fetch_add(1, Ordering::Relaxed);
                return (0, 1, 0);
            }
        };
        let now = now_ms();
        let mut kept: Vec<DelayedEntry> = Vec::new();
        let mut torn: u64 = 0;
        let mut expired: u64 = 0;
        for line in text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let entry = serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|value| decode_entry(&value));
            match entry {
                None => torn += 1,
                Some(entry) if entry.deliver_at_ms <= now => expired += 1,
                Some(entry) => kept.push(entry),
            }
        }
        kept.sort_by_key(|entry| entry.deliver_at_ms);
        let cap = self.pending_cap();
        let mut overflow_dropped: u64 = 0;
        if kept.len() > cap {
            // A saturated `usize::MAX` cap never truncates: the length
            // check above is false for any real file.
            overflow_dropped = kept.len().saturating_sub(cap) as u64;
            kept.truncate(cap);
        }
        let mut wheel = self.wheel.lock();
        for entry in &kept {
            self.next_id
                .fetch_max(entry.id.wrapping_add(1), Ordering::Relaxed);
            wheel.place(entry.clone(), now);
        }
        drop(wheel);
        self.recovery_torn.fetch_add(torn, Ordering::Relaxed);
        self.recovery_expired.fetch_add(expired, Ordering::Relaxed);
        if overflow_dropped > 0 {
            self.dropped_overflow
                .fetch_add(overflow_dropped, Ordering::Relaxed);
        }
        let lines: Vec<String> = kept.iter().map(encode_entry).collect();
        if !write_lines(&dir, &lines) {
            self.dropped_io.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                "Delayed load compaction failed: file keeps {} rows, next boot recounts them",
                kept.len()
            );
        }
        self.scheduled
            .fetch_add(kept.len() as u64, Ordering::Relaxed);
        (kept.len(), torn, expired)
    }

    /// Counters for tests and operators (each relaxed load).
    #[allow(dead_code)]
    pub fn scheduled_count(&self) -> u64 {
        self.scheduled.load(Ordering::Relaxed)
    }

    /// Deliveries completed through [`DelayedScheduler::note_delivered`].
    #[allow(dead_code)]
    pub fn delivered_count(&self) -> u64 {
        self.delivered.load(Ordering::Relaxed)
    }

    /// Drops past the configured delay bound.
    #[allow(dead_code)]
    pub fn dropped_over_limit(&self) -> u64 {
        self.dropped_over_limit.load(Ordering::Relaxed)
    }

    /// Drops with an unservable inner topic.
    #[allow(dead_code)]
    pub fn dropped_malformed(&self) -> u64 {
        self.dropped_malformed.load(Ordering::Relaxed)
    }

    /// Drops with a full wheel/file backlog.
    #[allow(dead_code)]
    pub fn dropped_overflow(&self) -> u64 {
        self.dropped_overflow.load(Ordering::Relaxed)
    }

    /// Drops past the per-message payload cap.
    #[allow(dead_code)]
    pub fn dropped_payload(&self) -> u64 {
        self.dropped_payload.load(Ordering::Relaxed)
    }

    /// Schedule attempts lost to file errors (fail closed, never queued).
    #[allow(dead_code)]
    pub fn dropped_io(&self) -> u64 {
        self.dropped_io.load(Ordering::Relaxed)
    }

    /// Torn file lines discarded at reload, never served.
    #[allow(dead_code)]
    pub fn recovery_torn(&self) -> u64 {
        self.recovery_torn.load(Ordering::Relaxed)
    }

    /// Expired entries discarded at reload, never served late.
    #[allow(dead_code)]
    pub fn recovery_expired(&self) -> u64 {
        self.recovery_expired.load(Ordering::Relaxed)
    }
}

impl Default for DelayedScheduler {
    fn default() -> Self {
        Self::new()
    }
}

fn append_line(dir: &std::path::Path, line: &str) -> Result<(), String> {
    if let Err(e) = std::fs::create_dir_all(dir) {
        return Err(format!("cannot create delayed dir: {e}"));
    }
    let path = persist_path(dir);
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&path)
        .map_err(|e| format!("cannot open delayed file: {e}"))?;
    use std::io::Write as _;
    file.write_all(line.as_bytes())
        .map_err(|e| format!("cannot append delayed entry: {e}"))?;
    file.write_all(b"\n")
        .map_err(|e| format!("cannot append delayed entry: {e}"))?;
    file.sync_all()
        .map_err(|e| format!("cannot fsync delayed entry: {e}"))?;
    Ok(())
}

/// Rewrite the file atomically (temp plus rename). Returns true on
/// success, false on any IO failure after logging it; the caller counts
/// the failure so a lost rewrite is never silent (fail closed: fired ids
/// stay in the file and may redeliver rather than vanish).
fn write_lines(dir: &std::path::Path, lines: &[String]) -> bool {
    if std::fs::create_dir_all(dir).is_err() {
        tracing::warn!("Delayed rewrite failed: cannot create delayed dir");
        return false;
    }
    let path = persist_path(dir);
    let tmp = dir.join(format!(
        ".{}.{}-{}.tmp",
        DELAYED_FILE_NAME,
        std::process::id(),
        now_ms()
    ));
    let mut text = String::new();
    for line in lines {
        text.push_str(line);
        text.push('\n');
    }
    let ok = (|| -> std::io::Result<()> {
        use std::io::Write as _;
        let mut handle = std::fs::File::create(&tmp)?;
        handle.write_all(text.as_bytes())?;
        handle.sync_all()?;
        drop(handle);
        std::fs::rename(&tmp, &path)?;
        Ok(())
    })()
    .is_ok();
    if !ok {
        std::fs::remove_file(&tmp).ok();
        tracing::warn!("Delayed rewrite failed: temp file could not be synced or renamed");
        return false;
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry_with(id: u64, deliver_at_ms: u64) -> DelayedEntry {
        DelayedEntry {
            id,
            deliver_at_ms,
            publish_at_ms: deliver_at_ms.saturating_sub(60_000),
            topic: "sensors/temp".to_string(),
            qos: 0,
            retain: false,
            payload: b"v".to_vec(),
            publisher: None,
            username: None,
            publisher_tenant: broker_session::tenant::DEFAULT_TENANT_ID.to_string(),
        }
    }

    #[test]
    fn wheel_orders_seconds_to_hours_without_per_message_tasks() {
        let wheel = Arc::new(DelayedScheduler::new());
        let now = now_ms();
        // One entry per level: 100 ms, 30 s, 30 min, 5 h.
        for (id, at) in [
            (1, now + 100),
            (2, now + 30_000),
            (3, now + 1_800_000),
            (4, now + 18_000_000),
        ] {
            wheel
                .persist_and_schedule(entry_with(id, at), 86_400)
                .expect("fits the wheel");
        }
        assert_eq!(wheel.len(), 4);
        // The imminent entry fires on the next tick; the hour-scale
        // entries stay queued behind their levels.
        let due = wheel.wheel.lock().tick(now + 200);
        assert_eq!(due.len(), 1);
        assert_eq!(due[0].id, 1);
        assert_eq!(wheel.len(), 3);
    }

    #[test]
    fn schedule_refuses_over_limit_oversize_and_full_backlog() {
        let wheel = Arc::new(DelayedScheduler::new());
        wheel.set_max_secs(60);
        let err = wheel
            .persist_and_schedule(entry_with(1, now_ms() + 1_000), 61)
            .expect_err("past the bound must fail");
        assert_eq!(err, ScheduleError::OverLimit);
        assert_eq!(wheel.dropped_over_limit(), 1);

        let mut big = entry_with(2, now_ms() + 1_000);
        big.payload = vec![0u8; MAX_DELAYED_PAYLOAD_BYTES + 1];
        let err = wheel
            .persist_and_schedule(big, 1)
            .expect_err("oversize must fail");
        assert_eq!(err, ScheduleError::PayloadTooLarge);
        assert_eq!(wheel.dropped_payload(), 1);

        wheel.set_max_secs(86_400);
        for id in 0..MAX_DELAYED_PENDING as u64 {
            wheel
                .persist_and_schedule(entry_with(10_000 + id, now_ms() + 60_000), 60)
                .expect("fits the backlog cap");
        }
        let err = wheel
            .persist_and_schedule(entry_with(999_999, now_ms() + 60_000), 60)
            .expect_err("past the backlog cap must fail");
        assert_eq!(err, ScheduleError::Overflow);
        assert_eq!(wheel.dropped_overflow(), 1);
    }

    #[test]
    fn file_round_trip_keeps_pending_and_discards_torn_and_expired() {
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-delayed-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let wheel = Arc::new(DelayedScheduler::new());
        wheel.set_persist_dir(&dir);
        let now = now_ms();
        wheel
            .persist_and_schedule(entry_with(1, now + 60_000), 3_600)
            .expect("persist");
        // Torn tail plus an already-expired row: both must be counted
        // and never queued.
        std::fs::write(
            persist_path(&dir),
            format!(
                "{}\n{{not json\n{}\n",
                encode_entry(&entry_with(1, now + 60_000)),
                encode_entry(&entry_with(2, now.saturating_sub(1_000))),
            ),
        )
        .expect("seed torn and expired rows");
        let fresh = DelayedScheduler::new();
        fresh.set_persist_dir(&dir);
        let (loaded, torn, expired) = fresh.load();
        assert_eq!(loaded, 1);
        assert_eq!(torn, 1);
        assert_eq!(expired, 1);
        assert_eq!(fresh.recovery_torn(), 1);
        assert_eq!(fresh.recovery_expired(), 1);
        assert_eq!(fresh.len(), 1);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn enable_switch_defaults_on_and_toggles() {
        let wheel = DelayedScheduler::new();
        assert!(wheel.is_enabled());
        wheel.set_enable(false);
        assert!(!wheel.is_enabled());
        wheel.set_enable(true);
        assert!(wheel.is_enabled());
    }

    #[test]
    fn pending_cap_defaults_to_bound_and_zero_lifts_it() {
        let wheel = Arc::new(DelayedScheduler::new());
        assert_eq!(wheel.max_messages(), MAX_DELAYED_PENDING);
        wheel.set_max_messages(2);
        assert_eq!(wheel.max_messages(), 2);
        let now = now_ms();
        wheel
            .persist_and_schedule(entry_with(1, now + 60_000), 60)
            .expect("first fits");
        wheel
            .persist_and_schedule(entry_with(2, now + 60_000), 60)
            .expect("second fits");
        wheel
            .persist_and_schedule(entry_with(3, now + 60_000), 60)
            .expect_err("third exceeds the lowered cap");
        // An explicit zero lifts the cap again.
        wheel.set_max_messages(0);
        wheel
            .persist_and_schedule(entry_with(3, now + 60_000), 60)
            .expect("zero cap accepts");
        assert_eq!(wheel.len(), 3);
    }

    #[test]
    fn snapshot_get_and_remove_cover_one_entry() {
        let wheel = Arc::new(DelayedScheduler::new());
        let now = now_ms();
        let mut first = entry_with(7, now + 60_000);
        first.username = Some("alice".to_string());
        first.publisher = Some("c1".to_string());
        wheel.persist_and_schedule(first, 60).expect("schedule");
        wheel
            .persist_and_schedule(entry_with(9, now + 120_000), 120)
            .expect("schedule");

        let listed = wheel.snapshot();
        assert_eq!(listed.len(), 2);
        assert_eq!(listed[0].id, 7);
        assert_eq!(listed[1].id, 9);
        assert_eq!(listed[0].username.as_deref(), Some("alice"));

        let one = wheel.get(9).expect("entry 9 waits");
        assert_eq!(one.topic, "sensors/temp");
        assert!(wheel.get(42).is_none());

        assert!(wheel.remove_one(7));
        assert!(!wheel.remove_one(7));
        assert_eq!(wheel.len(), 1);
        assert!(wheel.get(7).is_none());
    }

    #[test]
    fn remove_by_topic_drops_only_that_topic() {
        let wheel = Arc::new(DelayedScheduler::new());
        let now = now_ms();
        let mut first = entry_with(11, now + 60_000);
        first.topic = "a/one".to_string();
        let mut second = entry_with(12, now + 60_000);
        second.topic = "a/two".to_string();
        let mut third = entry_with(13, now + 60_000);
        third.topic = "a/one".to_string();
        for entry in [first, second, third] {
            wheel.persist_and_schedule(entry, 60).expect("schedule");
        }
        assert_eq!(wheel.remove_by_topic("a/one"), 2);
        assert_eq!(wheel.remove_by_topic("a/one"), 0);
        assert_eq!(wheel.len(), 1);
        assert!(wheel.get(12).is_some());
    }

    #[test]
    fn file_round_trip_keeps_user_name_and_schedule_time() {
        let dir = std::env::temp_dir().join(format!(
            "indramqtt-delayed-names-{}-{}",
            std::process::id(),
            now_ms()
        ));
        std::fs::create_dir_all(&dir).expect("scratch dir");
        let wheel = Arc::new(DelayedScheduler::new());
        wheel.set_persist_dir(&dir);
        let now = now_ms();
        let mut entry = entry_with(21, now + 60_000);
        entry.username = Some("bob".to_string());
        entry.publisher = Some("c9".to_string());
        entry.publish_at_ms = now;
        wheel.persist_and_schedule(entry, 60).expect("persist");
        let fresh = DelayedScheduler::new();
        fresh.set_persist_dir(&dir);
        let (loaded, _, _) = fresh.load();
        assert_eq!(loaded, 1);
        let back = fresh.get(21).expect("entry reloads");
        assert_eq!(back.username.as_deref(), Some("bob"));
        assert_eq!(back.publisher.as_deref(), Some("c9"));
        assert_eq!(back.publish_at_ms, now);
        std::fs::remove_dir_all(&dir).ok();
    }
}
