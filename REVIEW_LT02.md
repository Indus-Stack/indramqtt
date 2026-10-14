VERDICT: accept

FILES READ:
/work/team/migration/RULEBOOK.md
/work/team/parity/REVIEW-BRIEF.md
/work/team/tickets/LT-02-live-test-opc-ua.md
/work/tree/crates/broker-connectors-enterprise/src/opc_ua.rs
/work/tree/crates/broker-connectors-enterprise/Cargo.toml
/work/tree/crates/broker-connectors-enterprise/src/lib.rs
/work/tree/crates/broker-api/src/lib.rs
/work/tree/crates/broker-api/src/v5/rules.rs
/work/state/LT-02.gates.log
/work/tree/crates/broker-connectors-enterprise/src/cassandra.rs (for pattern reference)

REACHABILITY:
- DriverOpcUaTransport (store: session cache + pki_dir): writer crates/broker-connectors-enterprise/src/opc_ua.rs:1253 (connect), crates/broker-api/src/lib.rs:1994 (build_connector), crates/broker-api/src/v5/rules.rs:3554 (register_live_sink)
- OpcUaSink buffer (BatchQueue): writer crates/broker-connectors-enterprise/src/opc_ua.rs:1469 (flush takes batch), crates/broker-connectors-enterprise/src/opc_ua.rs:1574 (buffer_row pushes)
- ConnectorStore (management directory): writer crates/broker-api/src/v5/rules.rs:935 (upsert in seed_from_registry), crates/broker-api/src/v5/rules.rs:1032 (try_insert)

PROOF:
- Spec item 1 (test written in tests module with #[ignore]): test_qualify_driver_write_path at opc_ua.rs:2013, uses #[ignore]; production entry: DriverOpcUaTransport.connect/write at opc_ua.rs:1255/1307; change that breaks: remove DriverOpcUaTransport or switch back to TcpOpcUaTransport in broker-api
- Spec item 2 (reads env vars, panics on missing): qual_require at opc_ua.rs:1988-1993 panics naming variable; production entry: test reads OPCUA_ENDPOINT/USERNAME/PASSWORD; change that breaks: remove env vars from qualification runner
- Spec item 3 (checks real server, sends 500+ messages via production transport): test gets endpoints at opc_ua.rs:2046-2068, asserts product contains "opcplc"/"microsoft"; sends 500 rows via DriverOpcUaTransport at opc_ua.rs:2184-2203; production entry: DriverOpcUaTransport.write; change that breaks: replace DriverOpcUaTransport with MemoryOpcUaTransport in test
- Spec item 4 (reads back via independent client, asserts exact count + first/last): independent session reads each row at opc_ua.rs:2189-2202, final read at opc_ua.rs:2212-2224 asserts count=500 first=0 last=499; production entry: opcua::client::Session.read; change that breaks: remove read-back assertions or reduce ROWS below 500
- Spec item 5 (fault: unknown node, asserts connector error with server text): bad sink at opc_ua.rs:2228-2266 sends to unknown node, expects ConnectorError::Dispatch with "BadNodeIdUnknown" at opc_ua.rs:2261-2263; production entry: DriverOpcUaTransport.write returns error; change that breaks: make DriverOpcUaTransport swallow BadNodeIdUnknown or return Ok
- Spec item 6 (removes what it created): cleanup at opc_ua.rs:604-612 writes -1 to freeze_id to restore simulation; production entry: opcua::client::Session.write; change that breaks: remove cleanup block
- Spec item 7 (fail closed if connector cannot do step): test panics on any failure via unwrap_or_else/panic; production entry: all await points; change that breaks: add early return or skip logic

SLOP:
none

CLASSES:
none