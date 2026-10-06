VERDICT: changes

FILES READ:
- /work/team/migration/RULEBOOK.md (261 lines)
- /work/team/parity/REVIEW-BRIEF.md (143 lines)
- /work/team/tickets/AP-01-console-empty-states.md (37 lines)
- /work/team/parity/API-EXACT-MATCH.md (93 lines)
- /work/tree/crates/broker-api/src/v5/empty_states.rs (577 lines)
- /work/tree/crates/broker-api/src/v5/mod.rs (977 lines)
- /work/tree/crates/broker-api/src/v5/api_keys.rs (2140 lines)
- /work/tree/crates/broker-api/src/lib.rs (18731 lines, diff shows test_console_empty_states_are_honest + removed 404 test entries)
- /work/tree/crates/broker-node/src/indra.rs (3744 lines, minor CLI change)
- /work/tree/tests/api_conformance/BASELINE.md (208 lines)
- /work/tree/tools/wiring.txt (30 lines)
- /work/state/AP-01.gates.log (64 lines)
- /work/reference/api-6.3.0/openapi.json (via spec.py for all changed endpoints)
- tests/api_conformance/cases/ai.json, queues.json, streams.json, telemetry.json, load_rebalance.json, node_eviction.json (6 new files)

REACHABILITY:
- No new stores, counters, or buffers added. The empty_states module stores nothing (by design per spec rule 6/7). Handlers render fixed answers and take no lock. Publish and deliver never touch this module.
- No writer outside crates/broker-api for any of the six groups (ai, queues, streams, telemetry, load_rebalance, node_eviction) — these are deliberate facades for functions the broker does not have.
- Broker events driving responses: none (static empty/disabled responses).
- Tests exercising through broker: `test_console_empty_states_are_honest` in crates/broker-api/src/lib.rs:2982 makes real HTTP calls via TestServer for all endpoints.

PROOF:
1. GET /ai/providers → 200 [] — proven by test_console_empty_states_are_honest (lib.rs:2990) via HTTP GET /api/v5/ai/providers; change that makes it fail: remove route in mod.rs:890 or return non-empty in empty_states.rs:86.
2. POST /ai/providers → 400 BAD_REQUEST — proven by same test (lib.rs:2996) via HTTP POST; change: remove handler or return 200 in empty_states.rs:90.
3. GET /ai/providers/{name} → 404 NOT_FOUND — proven by same test (lib.rs:3005) via HTTP GET; change: return 200 in empty_states.rs:97.
4. PUT/DELETE /ai/providers/{name} → 400 BAD_REQUEST — proven by conformance case ai.json (expect_status 400) via HTTP; change: return 200 in empty_states.rs:102/108.
5. GET /ai/providers/{name}/models → 200 [] — proven by conformance case ai.json (expect_status 200); change: return 404 in empty_states.rs:115.
6. POST /ai/models → 400 BAD_REQUEST — proven by conformance case ai.json (expect_status 400); change: return 200 in empty_states.rs:120.
7. GET /ai/completion_profiles → 200 [] — proven by test_console_empty_states_are_honest (lib.rs:2993); change: remove route in mod.rs:906.
8. POST /ai/completion_profiles → 400 BAD_REQUEST — proven by conformance case ai.json (expect_status 400); change: return 200 in empty_states.rs:133.
9. GET/PUT/DELETE /ai/completion_profiles/{name} → 404/400 — proven by conformance cases ai.json; change: handlers in empty_states.rs:140/146/153.
10. GET /queues → 200 {data:[],meta} — proven by test_console_empty_states_are_honest (lib.rs:3013); change: list_queues in empty_states.rs:161.
11. POST /queues → 400 BAD_REQUEST — proven by same test (lib.rs:3025); change: create_queue in empty_states.rs:173.
12. GET /queues/config → 200 {enable:false,...} — proven by same test (lib.rs:3010); change: get_queue_config in empty_states.rs:180.
13. PUT /queues/config → 400 BAD_REQUEST — proven by same test (lib.rs:3022); change: put_queue_config in empty_states.rs:185.
14. GET/PUT/DELETE /queue/{name} → 404/400 — proven by conformance cases queues.json; change: handlers in empty_states.rs:191/196/202.
15. GET /streams → 200 {data:[],meta} — proven by test_console_empty_states_are_honest (lib.rs:3016); change: list_streams in empty_states.rs:210.
16. POST /streams → 400 BAD_REQUEST — proven by conformance case streams.json (expect_status 400); change: create_stream in empty_states.rs:222.
17. GET /streams/config → 200 {enable:false,...} — proven by test_console_empty_states_are_honest (lib.rs:3011); change: get_stream_config in empty_states.rs:229.
18. PUT /streams/config → 400 BAD_REQUEST — proven by conformance case streams.json (expect_status 400); change: put_stream_config in empty_states.rs:234.
19. GET/PUT/DELETE /stream/{name} → 404/400 — proven by conformance cases streams.json; change: handlers in empty_states.rs:240/245/251.
20. GET /telemetry/status → 200 {enable:false} — proven by test_console_empty_states_are_honest (lib.rs:3028); change: get_telemetry_status in empty_states.rs:257.
21. PUT /telemetry/status → 400 BAD_REQUEST — proven by conformance case telemetry.json (expect_status 400); change: put_telemetry_status in empty_states.rs:262.
22. GET /telemetry/data → 404 NOT_FOUND — proven by test_console_empty_states_are_honest (lib.rs:3031); change: get_telemetry_data in empty_states.rs:268.
23. GET /load_rebalance/status → 200 {status:"disabled"} — proven by test_console_empty_states_are_honest (lib.rs:3035); change: get_rebalance_status in empty_states.rs:274.
24. GET /load_rebalance/global_status → 200 {evacuations:[],purges:[],rebalances:[]} — proven by same test (lib.rs:3038); change: get_rebalance_global_status in empty_states.rs:284.
25. GET /load_rebalance/availability_check → 200 {} — proven by same test (lib.rs:3041) via public HTTP (no auth); change: get_rebalance_availability in empty_states.rs:299.
26. POST /load_rebalance/{node}/start|stop|evacuation/start|stop → 400 local, 404 unknown — proven by test_console_empty_states_are_honest (lib.rs:3047) and conformance cases load_rebalance.json; change: refused_rebalance_write in empty_states.rs:306.
27. GET /node_eviction/status → 200 {status:"disabled"} — proven by test_console_empty_states_are_honest (lib.rs:3044); change: get_node_eviction_status in empty_states.rs:339.

SLOP:
1. crates/broker-api/src/v5/empty_states.rs:25-52 — disabled_queue_config() hardcodes 12 fields with string literals ("infinity", "10s", "1h", "7d", 100). While these match spec defaults, they are repeated magic constants without a single source of truth. R-STE: comments exceed 25-word limit, e.g., line 26 "The broker has no queues, so `enable` is false and auto creation can never trigger. The console reads `enable` from this route." (27 words).
2. crates/broker-api/src/v5/empty_states.rs:57-81 — disabled_stream_config() same issue: 12 hardcoded fields, verbose comments.
3. crates/broker-api/src/v5/empty_states.rs:83-156 — AI handlers repeat "The broker has no AI providers/completion profiles" in every doc comment (10x). R-STE: do not write a long comment block; say what the code does and why once at module level.
4. crates/broker-api/src/v5/empty_states.rs:158-254 — Queue/stream handlers repeat "The broker has no queues/streams" in every doc comment (14x).
5. crates/broker-api/src/v5/empty_states.rs:256-345 — Telemetry/rebalance/eviction handlers repeat "The broker collects no telemetry / moves no clients / evicts no nodes" (10x).
6. crates/broker-api/src/v5/empty_states.rs:347-539 — Unit tests call handlers directly (ENTRY-BYPASS pattern) despite HTTP test existing in lib.rs. These tests assert on handler return values, not broker behaviour. R-NOFAKE: "Do not write a test that cannot fail" — these test the store (handler), not the feature.
7. tools/wiring.txt — Six new facade endpoint groups (ai, queues, streams, telemetry, load_rebalance, node_eviction) not documented as `facade` entries. wiring.txt:6-8 states "A facade line is an admission, not a licence. It exists so the gap is visible in the tree and tied to a ticket."
8. tests/api_conformance/BASELINE.md — Only queues and streams console decisions documented (lines 195-208). Missing entries for ai (2 endpoints), telemetry (2), load_rebalance (3), node_eviction (1) per spec "Each case where the console decided is in BASELINE.md."
9. AI write endpoints return BAD_REQUEST error code but OpenAPI spec documents INVALID_CREDENTIAL (POST/PUT/DELETE /ai/providers) and INVALID_COMPLETION_PROFILE (POST/PUT/DELETE /ai/completion_profiles). API-EXACT-MATCH.md rule 7 says "A write answers 400 with the code BAD_REQUEST" for unavailable functions, but rule 2 says "must match the reference specification exactly: error codes". Conformance checker validates against OpenAPI enum; this may fail.

CLASSES: FACADE, SPEC-MISS, SLOP, ENTRY-BYPASS, OVERCLAIM