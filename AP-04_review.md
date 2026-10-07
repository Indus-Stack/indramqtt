VERDICT: accept
FILES READ: /work/team/migration/RULEBOOK.md, /work/team/parity/REVIEW-BRIEF.md, /work/team/tickets/AP-04-mqtt-group.md, /work/state/AP-04.gates.log, /work/tree/crates/broker-api/src/v5/mod.rs, /work/tree/crates/broker-api/src/v5/delayed.rs, /work/tree/crates/broker-api/src/v5/rewrite.rs, /work/tree/crates/broker-api/src/v5/topic_metrics.rs, /work/tree/crates/broker-api/src/lib.rs, /work/tree/crates/broker-node/src/main.rs, /work/tree/crates/broker-router/src/lib.rs, /work/tree/crates/broker-delayed/src/lib.rs, /work/tree/tests/api_conformance/BASELINE.md, /work/team/parity/API-EXACT-MATCH.md

REACHABILITY:
- DelayedScheduler (crates/broker-delayed/src/lib.rs:439): written by broker-node/src/main.rs:5977-5983 (publish path checks `is_enabled()` and calls `persist_and_schedule`), broker-node/src/main.rs:6279-6283 (QoS 2 PUBREL path), broker-node/src/main.rs:9082-9084 (boot applies config and loads persisted entries), broker-node/src/main.rs:9091 (boot spawns driver task); read by broker-api/src/v5/delayed.rs:58 (get_delayed_config), delayed.rs:99 (put_delayed_config), delayed.rs:196 (list_delayed_messages), delayed.rs:270 (get_delayed_message), delayed.rs:300 (delete_delayed_message), delayed.rs:328 (delete_delayed_by_topic); driver task at broker-node/src/main.rs:3592-3609 ticks the wheel and delivers due entries
- Router rewrite_rules (crates/broker-router/src/lib.rs:807): written by broker-api/src/v5/rewrite.rs:85 (put_rewrites calls set_rewrite_rules); read by broker-api/src/v5/rewrite.rs:44 (list_rewrites calls rewrite_rules_snapshot), broker-router/src/lib.rs:1039 (rewrite_publish called from publish ingress), broker-router/src/lib.rs:1050 (rewrite_subscribe called from subscribe event)
- Topic metrics collections: no store exists; broker-api/src/v5/topic_metrics.rs reads/writes nothing (honest-empty per spec)

PROOF:
- GET /mqtt/delayed (switch + cap): test `mqtt_group_delayed_rewrite_topicmetrics_over_http` in broker-api/src/lib.rs:16003 uses API client socket; broker change: delayed.rs:58 reads `is_enabled()` and `max_messages()` from shared scheduler
- PUT /mqtt/delayed (validated update): same test line 16010; broker change: delayed.rs:99 validates then calls `set_enable` and `set_max_messages` on shared scheduler
- GET /mqtt/delayed/messages (paged list): same test line 16050; broker change: delayed.rs:196 snapshots wheel under short lock and paginates
- GET /mqtt/delayed/messages/{node}/{msgid} (detail): same test line 16060; broker change: delayed.rs:270 gets entry by id from wheel
- DELETE /mqtt/delayed/messages/{node}/{msgid} (drop one): test `list_detail_and_removal_flow_through_scheduler` in delayed.rs:439 uses API then calls `remove_one` on scheduler; broker change: delayed.rs:300 removes from wheel and file
- DELETE /mqtt/delayed/messages/{topic} (drop by topic): test `topic_removal_drops_only_that_topic` in delayed.rs:538; broker change: delayed.rs:328 calls `remove_by_topic` on scheduler
- GET /mqtt/topic_rewrite (ordered list): test `mqtt_group_delayed_rewrite_topicmetrics_over_http` line 16083; broker change: rewrite.rs:44 calls `rewrite_rules_snapshot`
- PUT /mqtt/topic_rewrite (validated replace): same test line 16088; broker change: rewrite.rs:56 validates each rule then calls `set_rewrite_rules` on shared router
- Topic metrics honest-empty (list/create/read/delete/reset/bulk delete): same test line 16115; broker change: topic_metrics.rs returns documented empty/miss/fail shapes with no store
- End-to-end delayed message appears in list and removal stops delivery: test `delayed_message_lists_and_removal_stops_delivery` in broker-node/src/main.rs:19869 publishes through broker (`apply_publish`), verifies entries in `shared.delayed.snapshot()`, removes one via `remove_one`, then `tick_due` shows only kept entry fires

SLOP:
- broker-api/src/v5/delayed.rs:41: constant `PAYLOAD_TOO_LARGE: &str = "PAYLOAD_TO_LARGE"` — marker spelling (single 'O') comes from specification per comment; not invented, matches console expectation
- broker-api/src/v5/delayed.rs:48: constant `MAX_DELAYED_MESSAGES_UPPER: u64 = 1_000_000` — reason documented: "bounds an explicit operator choice within a reviewable range; zero selects no cap explicitly. Past one million the value is almost certainly a typo"
- broker-router/src/lib.rs:1073-1080: `rewrite_for` skips system-prefixed names (`$...`) with `TODO(parity)` comment — open question per UNKNOWN rule, conservative choice documented
- broker-delayed/src/lib.rs:78-83: `MAX_DELAYED_PAYLOAD_BYTES` mirrors retainer default with `TODO(parity)` — open question whether it should track configured retainer cap dynamically
- broker-api/src/v5/topic_metrics.rs:16-19: `TODO(parity)` about per-filter counters — honest-empty per rule 6, marked as gap
- broker-router/src/lib.rs:247-256: `MAX_REWRITE_RULES = 64` — reason documented: "deployments carry tens of rules at most; past 64 the configuration is almost certainly generated in a loop"
- broker-router/src/lib.rs:258-265: `MAX_REWRITE_LEN = u16::MAX` — reason documented: "matches the on-the-wire topic length field"
- broker-delayed/src/lib.rs:61-71: `MAX_DELAYED_PENDING = 10_000` — reason documented: "matches the detached offline queue bound"
- broker-delayed/src/lib.rs:73-83: `MAX_DELAYED_PAYLOAD_BYTES = 1_048_576` — reason documented: "mirrors the retained-store default ceiling"
- tests/api_conformance/BASELINE.md additions document known specification defects per API-EXACT-MATCH.md rule 3 — not slop, required by process

CLASSES: none