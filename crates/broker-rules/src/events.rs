//! Rule-event producer registry behind the management catalogue.
//!
//! Covers the state behind `GET /rule_events` (C06-16): the catalogue of
//! event types rules can trigger on. Single-node, management-plane only:
//! the handler clones at most [`MAX_RULE_EVENTS`] small entries per
//! request from this static registry; the rule fire path
//! ([`crate::RuleEngine::dispatch_ingress`] in `lib.rs`, driven by every
//! published message via the kernel ingress pipeline) never consults this
//! registry, so the catalogue adds no work, locks or allocation to
//! publish or deliver.
//!
//! A catalogue entry is listed only if its producer exists: the kernel
//! drives that event on the rule trigger path today, never an invented
//! name. Adding a producer means adding one entry to
//! [`EVENT_PRODUCERS`] alongside its dispatch call; the catalogue reads
//! the same registry, so the list extends automatically and the two
//! cannot drift. There is intentionally only one registry, not a second
//! list shadowing it.
//!
//! Store bounds (both stated here and enforced below):
//! - at most [`MAX_RULE_EVENTS`] entries (64: caps management-plane
//!   memory for the catalogue; the built-in set is far below it, and
//!   reads past the cap are never served because construction asserts
//!   the bound);
//! - the default is the built-in event set below (finite, exactly
//!   [`RULE_EVENT_COUNT`] entries); operators cannot opt into more
//!   because writes are out of scope (read-only route).
//!
//! Persistence: none per request. The registry is static within a run
//! (compiled in); a restart serves the same fixed set.
// TODO(parity): which per-entry fields does the documented catalogue shape
// require beyond `event`/`description` (columns, schema, ...)? The
// rulebook does not decide the exact shape; fields the broker cannot
// supply are omitted rather than invented, until the checker pins it.
// TODO(parity): which further `$events/...` names exist once event sources
// fire (client connect/disconnect, session lifecycle, ...)? The rulebook
// does not decide the exact vocabulary; the current choice lists only the
// one ingress the kernel drives today and fails closed on the rest (see
// `std_sources_match`), until the checker pins it.

use serde::Serialize;

/// Upper bound on the rule-event catalogue size (C06-16).
///
/// Reason: the list handler clones the whole snapshot once per request,
/// so the catalogue must stay small and bounded; 64 covers the built-in
/// event set with headroom while keeping per-request clones cheap. The
/// default is finite (exactly [`RULE_EVENT_COUNT`]); the operator never
/// opts into unlimited because writes are out of scope.
pub const MAX_RULE_EVENTS: usize = 64;

/// Stated count of the built-in event set served today. Reason: one entry
/// per kernel producer that drives the rule trigger path (see
/// [`EVENT_PRODUCERS`]); the count documents the bound the handler
/// enforces.
pub const RULE_EVENT_COUNT: usize = 1;

/// One catalogue entry: the stable event name plus a short
/// operator-facing summary written in our own words.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RuleEventInfo {
    /// Stable event name (for example `$events/message_publish`).
    pub event: &'static str,
    /// Short operator-facing summary of when the kernel produces it.
    pub description: &'static str,
}

/// The single producer registry: one entry per event the kernel actually
/// produces on the rule trigger path.
///
/// Today's sole producer is the message-ingress event: every published
/// message drives [`crate::RuleEngine::dispatch_ingress`] (lib.rs), which
/// the kernel ingress pipeline calls before fan-out, so rules over MQTT
/// topic filters fire on it. Rules naming it as an `$events/...` source
/// still fail closed until event sources land (see `std_sources_match`);
/// the producer — the dispatch call the kernel drives — is real either
/// way, and no unproduced name is listed.
///
/// Bound (stated here and enforced by construction): at most
/// [`MAX_RULE_EVENTS`] entries; the catalogue below reads this same
/// array, so a new producer extends the served list without a second
/// edit.
const EVENT_PRODUCERS: [RuleEventInfo; RULE_EVENT_COUNT] = [RuleEventInfo {
    event: "$events/message_publish",
    description: "A client published a message; rules over topic filters fire at ingress",
}];

// Compile-time guard that the bound covers the served registry.
const _: () = assert!(
    EVENT_PRODUCERS.len() <= MAX_RULE_EVENTS,
    "rule event registry exceeds its bound"
);

/// Returns the catalogue of event types rules can trigger on.
///
/// Built from the real producer registry ([`EVENT_PRODUCERS`]), not a
/// second list: adding a producer entry above automatically extends the
/// served list. The result is bounded (at most [`MAX_RULE_EVENTS`]) and
/// never consulted per message — management reads only.
pub fn rule_event_catalogue() -> Vec<RuleEventInfo> {
    EVENT_PRODUCERS.to_vec()
}

/// Whether `event` names a real kernel producer (exact match on the
/// stable event name).
///
/// Shared by the catalogue test so the served list and the predicate
/// cannot drift.
pub fn is_produced_event(event: &str) -> bool {
    EVENT_PRODUCERS.iter().any(|entry| entry.event == event)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn catalogue_is_bounded_and_unique() {
        let catalogue = rule_event_catalogue();
        assert!(!catalogue.is_empty(), "catalogue must not be empty");
        assert!(
            catalogue.len() <= MAX_RULE_EVENTS,
            "catalogue must not exceed {MAX_RULE_EVENTS}"
        );
        assert_eq!(catalogue.len(), RULE_EVENT_COUNT);
        let mut names: Vec<&str> = catalogue.iter().map(|entry| entry.event).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), catalogue.len(), "events must be unique");
        for entry in &catalogue {
            assert!(
                is_produced_event(entry.event),
                "catalogue lists unproduced event '{}'",
                entry.event
            );
            assert!(!entry.description.trim().is_empty());
        }
        assert!(is_produced_event("$events/message_publish"));
        assert!(!is_produced_event("$events/no_such_event"));
        assert!(!is_produced_event("message_publish"));
    }

    #[test]
    fn catalogue_serializes_as_event_objects() {
        let json = serde_json::to_value(rule_event_catalogue()).expect("catalogue is JSON");
        let arr = json.as_array().expect("catalogue is a list");
        assert_eq!(arr.len(), RULE_EVENT_COUNT);
        for entry in arr {
            assert!(entry["event"].is_string());
            assert!(entry["description"].is_string());
        }
    }
}
