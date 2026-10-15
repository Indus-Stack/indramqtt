VERDICT: changes
FILES READ: /work/team/migration/RULEBOOK.md, /work/team/parity/REVIEW-BRIEF.md, /work/team/tickets/LT-03-live-test-oracle.md, /work/state/LT-03.gates.log, /work/tree/crates/broker-connectors-enterprise/src/oracle.rs, /work/tree/crates/broker-connectors-enterprise/Cargo.toml, /work/tree/crates/broker-connectors-enterprise/src/lib.rs

REACHABILITY:
- DriverOracleTransport (oracle.rs:461-757): new production transport using oracle-rs driver. Writer: oracle.rs connect_new() line 514, execute_many() line 635, execute() line 709. Broker event: publish matching rule targeting Oracle connector invokes Sink::send via rule engine (broker-rules). Test exercises through broker: NO — qualification test directly calls sink.send()/flush(); spec explicitly states end-to-end broker test is a later task. No invented values — test asserts via independent admin connection.
- OracleTransport::execute_many (oracle.rs:280-303, 617-706): trait method with default loop impl; DriverOracleTransport overrides with real batch. Writer: OracleSink::flush at oracle.rs:928. Broker event: batch flush triggered by batch_size. Test exercises through broker: NO (same as above).
- OracleSink::sent (AtomicU64, oracle.rs:840): written in flush at oracle.rs:931, 945. Writer: OracleSink::flush. Broker event: successful batch commit.
- OracleSink::queue (BatchQueue, oracle.rs:838): written in send at oracle.rs:965, flush at oracle.rs:876, 935, 949. Writer: OracleSink::send/flush. Broker event: ingress publish.
- OracleSink::backoff (BackoffState, oracle.rs:839): written in flush at oracle.rs:869, 930, 934, 944, 948. Writer: OracleSink::flush. Broker event: transport error.
- DriverOracleTransport::connection (Mutex<Option<Connection>>, oracle.rs:468): written in connect_new at oracle.rs:514, execute_many at oracle.rs:635, execute at oracle.rs:709. Writer: DriverOracleTransport methods. Broker event: first send or reconnect after error.

PROOF:
1. Test exists with #[ignore] — test_qualify_driver_write_path at oracle.rs:1331; production entry: DriverOracleTransport (oracle.rs:461) + OracleSink::send/flush (oracle.rs:959); change to fail: remove #[ignore] or delete test.
2. Reads env vars, panics if absent — qual_require at oracle.rs:1229 used at oracle.rs:1334-1341; production entry: std::env::var; change to fail: set env var to empty string.
3. Checks server is real product — version query v$version at oracle.rs:1346-1368; production entry: oracle_rs::Connection::query; change to fail: point to non-Oracle server.
4. Creates table — oracle.rs:1394-1407; production entry: admin.execute CREATE TABLE; change to fail: revoke CREATE TABLE privilege.
5. Sends 500 messages through production transport — oracle.rs:1432-1444 uses DriverOracleTransport; production entry: OracleSink::send -> flush -> execute_many; change to fail: break execute_many batch logic.
6. Reads back via independent client, asserts count and first/last — oracle.rs:1447-1481; production entry: admin connection query; change to fail: drop table before assert.
7. Fault test (wrong table) asserts Dispatch error with server text — oracle.rs:1484-1520; production entry: probe_batch_failure at oracle.rs:540; change to fail: remove probe logic or make it return None.
8. Cleanup drops table — oracle.rs:1523-1528; production entry: admin.execute DROP TABLE; change to fail: comment out cleanup.

SLOP:
- oracle.rs:21 — default_batch_size() returns 500 with no stated reason (protocol requirement, measured cost, or vendor limit)
- oracle.rs:56 — timeout() default 5000ms with no stated reason
- oracle.rs:850 — Duration::from_millis(50) linger in BatchQueue with no stated reason
- oracle.rs:1259 — qual_connect timeout Duration::from_secs(60) with no stated reason
- oracle.rs:1410 — test transport timeout Duration::from_secs(30) with no stated reason
- oracle.rs:1423 — test batch_size: Some(100) with no stated reason
- oracle.rs:459 — TODO(parity) marker is valid per rulebook §3, not slop

CLASSES: SLOP