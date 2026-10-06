VERDICT: changes
FILES READ: /work/team/migration/RULEBOOK.md, /work/team/parity/REVIEW-BRIEF.md, /work/team/tickets/AP-10-connector-store-in-state.md, /work/state/AP-10.gates.log, /work/tree/crates/broker-api/src/v5/rules.rs, /work/tree/crates/broker-api/src/lib.rs, /work/tree/crates/broker-api/src/config_runtime.rs, /work/tree/crates/broker-node/src/indra.rs, /work/tree/crates/broker-node/src/main.rs, /work/tree/tools/wiring.txt
REACHABILITY:
- ConnectorStore: writer at crates/broker-node/src/main.rs:3544 (Shared construction), crates/broker-node/src/main.rs:8890-8891 (seed_from_registry at kernel boot), crates/broker-node/src/main.rs:8901-8910 (enable flag mirror at boot), crates/broker-node/src/main.rs:8887 (serve_api shares kernel instance with ApiState)
PROOF:
- Spec item "No process static holds connector state": test connector_store_is_per_state_not_process_global (crates/broker-api/src/lib.rs:13156), production entry POST /api/v5/connectors → create_connector → state.connectors.try_insert(), breaking change would be reverting ConnectorStore to static CONNECTORS LazyLock
- Spec item "Flaky test passes 20 runs in sequence": same test, same production entry, same breaking change; gates ran once (log: "All 89 tests passed"), implementer must run 20× locally
- Spec item "Two test servers in one process: connector made on first absent on second": same test exercises two TestServer instances (separate ApiState), creates on server_a, asserts 404 on server_b
SLOP:
- crates/broker-api/src/v5/rules.rs:799-850: seed_from_registry spawns dedicated thread with new Tokio runtime to run async register_live_sink from sync boot; heavy, expect() panics on failure
- crates/broker-api/src/v5/rules.rs:1403: MAX_CONNECTORS = 512 has TODO(parity) — rulebook does not decide exact bound; 512 is conservative choice until conformance checker pins it
- crates/broker-api/src/lib.rs:364-366 + crates/broker-node/src/main.rs:8887: redundant seeding — ApiState::new() creates & seeds own ConnectorStore, then serve_api replaces with kernel's shared.connectors (also seeded); registry read + connector replay runs twice at startup
- crates/broker-node/src/indra.rs:887-890: unrelated change — cmd_status adds extra GET /api/v5/nodes call to verify key before GET /status; not in AP-10 scope
CLASSES: SLOP