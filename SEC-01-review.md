VERDICT: accept
FILES READ: /work/team/migration/RULEBOOK.md, /work/team/parity/REVIEW-BRIEF.md, /work/team/tickets/SEC-01-high-findings.md, /work/reference/security/security-review.md, /work/state/SEC-01.gates.log, crates/broker-config/src/schema.rs, crates/broker-node/src/main.rs, crates/broker-node/src/native.rs, crates/broker-router/src/lib.rs, beam/src/indra_edge_sup.erl, beam/src/indra_listener.erl, beam/src/indra_ws_listener.erl, beam/test/indra_wss_tests.erl, indramqtt.example.toml, tools/boot_smoke.sh, CHANGELOG.md

REACHABILITY:
- Conn.closing (native.rs:1635): writer is `drain_inbound` at native.rs:2150 calling `shared.conns.closing_flag(conn_id)` (router/src/lib.rs:3649), flag set by `ConnTable::order_close` (router/src/lib.rs:3659) called from `order_authz_disconnect` (main.rs:10245), `order_alias_disconnect` (main.rs:10201), `order_unbound_close` (main.rs:10284).
- ConnSlot.closing (router/src/lib.rs:3506): writer is `ConnTable::order_close` at router/src/lib.rs:3664; reader is `ConnTable::closing_flag` at router/src/lib.rs:3649 called from native.rs:2150.
- Delayed publish authorization: no new store; uses existing `shared.auth.authorize_publish_in_tenant`, `shared.db_auth`, `shared.webhook`, `shared.bans` at schedule time in `apply_publish` (main.rs:7028-7183) with inner topic from `delayed_target` (main.rs:6979-6993).
- CoAP gateway authorization: no new store; uses existing authorization machinery in `apply_coap_publish` (main.rs:8082-8120) called from `run_coap_gateway` (main.rs:8229).
- Client cert CA file validation: no new store; `validate_client_cert_opts` (schema.rs:1165-1170) and `load_client_roots` (native.rs:370-380) refuse empty cacertfile.
- Edge listener CA file: no new store; Erlang `verify_ssl_opts` (indra_listener.erl:99-104) and `wss_verify_opts` (indra_ws_listener.erl:70-75) return error when verify on and cacertfile undefined.

PROOF:
- H1 config validation refuses verify_peer/verify_client_cert without cacertfile: test `schema_refuses_client_verification_without_ca_file` (schema.rs:1334) uses `BrokerConfig::validate()`; change breaking it: remove `cacertfile.trim().is_empty()` check in `validate_client_cert_opts` (schema.rs:1165).
- H1 TLS config load refuses empty CA file: test `client_verification_without_ca_file_is_refused` (native.rs:4608) uses `load_tls_config`; change breaking it: revert `load_client_roots` to return system roots for empty path (native.rs:370).
- H1 listener controller refuses secure listener without CA file: test `controller_does_not_start_secure_listener_without_ca_file` (native.rs:4632) uses `NativeListenerController::start_listener`; change breaking it: remove validation in controller or revert `verify_ssl_opts`/`wss_verify_opts`.
- H1 Erlang edge refuses WSS listener without CA file: test `wss_verify_without_cacertfile_rejected_test` (beam/test/indra_wss_tests.erl:56) uses `indra_ws_listener:start_link`; change breaking it: revert `wss_verify_opts` to use system roots (indra_ws_listener.erl:70-75).
- H1 boot smoke validates config rejection: tools/boot_smoke.sh:224-235 runs kernel with bad config; change breaking it: remove config validation.
- H2 publish from no-session connection dropped and closed: test `publish_from_a_connection_with_no_session_is_dropped_and_closed` (main.rs:20748) uses `apply_publish` kernel entry; change breaking it: remove `else` block at main.rs:7227-7233.
- H2 denied publish with disconnect stops buffered packets: test `denied_publish_with_disconnect_stops_the_rest_of_the_buffer` (native.rs:4738) uses real TCP listener -> `drain_inbound` -> `apply_publish` -> `order_authz_disconnect` -> `order_close` -> `closing` flag; change breaking it: remove `closing` flag check in `drain_inbound` (native.rs:2071-2076) or `order_close` call in `order_authz_disconnect`.
- H2 CoAP gateway publish authorized: test `coap_publish_denied_topic_is_refused_over_udp` (main.rs:24519) uses real UDP -> `run_coap_gateway` -> `apply_coap_publish`; change breaking it: remove authorization checks in `apply_coap_publish` (main.rs:8082-8120).
- H3 delayed publish authorized on inner topic: test `delayed_publish_is_authorized_on_the_inner_topic` (native.rs:4790) uses real TCP -> `drain_inbound` -> `apply_publish` (delayed_target extraction at main.rs:6979-6993) -> `apply_delayed_publish` -> `deliver_delayed_entry`; change breaking it: remove delayed_target extraction and use outer topic for authorization in `apply_publish`.

SLOP:
- TODO(parity) at main.rs:4296: "the entry was checked at schedule time. Rules can change before it fires. Does the fire need a second check?" — valid open question per rulebook §2 UNKNOWN rule, not a defect.
- Atomic load of `closing` flag per packet in `drain_inbound` (native.rs:2071-2076) on hot path — required for H2 fix, no lock/allocation, acceptable per rulebook §2 "no new lock, allocation or unbounded work".

CLASSES: none