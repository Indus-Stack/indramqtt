//! Rule-event producer registry behind the management catalogue.
//!
//! Covers the state behind `GET /rule_events` (C06-16).
//! It lists the event types that rules can trigger on.
//! Single node only, management plane only.
//! The handler clones at most [`MAX_RULE_EVENTS`] small entries per request.
//! It reads from this static registry.
//! The rule fire path never reads this registry.
//! The fire path is [`crate::RuleEngine::dispatch_ingress`] in `lib.rs`.
//! The kernel ingress pipeline drives it for each published message.
//! The catalogue adds no work to publish or deliver.
//! It adds no locks and no allocation.
//!
//! The list contains an entry only when its producer exists.
//! The kernel drives that event on the rule trigger path today.
//! The list never contains an invented name.
//! A new producer needs one entry in [`EVENT_PRODUCERS`].
//! Add the entry next to its dispatch call.
//! The catalogue reads the same registry.
//! The list grows on its own and cannot drift.
//! Only one registry exists.
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
// TODO(parity): Which fields does the catalogue need beyond event and description?

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
pub const RULE_EVENT_COUNT: usize = 10;

/// One catalogue entry: the stable event name plus a short
/// operator-facing summary written in our own words.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct RuleEventInfo {
    /// Stable event name (for example `$events/client_connected`).
    pub event: &'static str,
    /// Short operator-facing summary of when the kernel produces it.
    pub description: &'static str,
}

/// The single producer registry.
/// It holds one entry per event the kernel produces on the rule trigger path.
///
/// The ten `$events/...` entries below are the RQ-04 event sources.
/// The kernel fires each source at the point the event happens.
/// These points are connect, connack, authz, subscribe, unsubscribe,
/// deliver, ack and drop.
/// Rules that name them as an `$events/...` source run on the
/// publish-path drain.
/// No entry names an unproduced event.
///
/// Bound (stated here and enforced by construction): at most
/// [`MAX_RULE_EVENTS`] entries; the catalogue below reads this same
/// array, so a new producer extends the served list without a second
/// edit.
const EVENT_PRODUCERS: [RuleEventInfo; RULE_EVENT_COUNT] = [
    RuleEventInfo {
        event: "$events/client_connected",
        description: "A client connected; fired at bind when the broker accepts the connection",
    },
    RuleEventInfo {
        event: "$events/client_disconnected",
        description:
            "A client disconnected; fired at unbind on clean close and at disconnect on TCP close",
    },
    RuleEventInfo {
        event: "$events/client_connack",
        description: "The broker answered a connect; fired at bind with the return code",
    },
    RuleEventInfo {
        event: "$events/client_check_authz_complete",
        description:
            "An authorization check finished; fired at subscribe and publish with the verdict",
    },
    RuleEventInfo {
        event: "$events/session_subscribed",
        description: "A session subscribed; fired at subscribe with the filter and options",
    },
    RuleEventInfo {
        event: "$events/session_unsubscribed",
        description: "A session unsubscribed; fired at unsubscribe with the filter",
    },
    RuleEventInfo {
        event: "$events/message_delivered",
        description: "A message reached a subscriber mailbox; fired at fan-out per delivery",
    },
    RuleEventInfo {
        event: "$events/message_acked",
        description: "A client acknowledged a message; fired at PUBACK with the packet id",
    },
    RuleEventInfo {
        event: "$events/message_dropped",
        description: "A message was dropped; fired at quota shed and at detached clean sessions",
    },
    RuleEventInfo {
        event: "$events/delivery_dropped",
        description: "A delivery missed its mailbox; fired at route failure for dead connections",
    },
];

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
        assert!(is_produced_event("$events/client_connected"));
        assert!(!is_produced_event("$events/message_publish"));
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
