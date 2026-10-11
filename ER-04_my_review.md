VERDICT: accept
FILES READ: /work/team/migration/RULEBOOK.md, /work/team/parity/REVIEW-BRIEF.md, /work/team/tickets/ER-04-listener-conformance-suite.md, /work/team/parity/EDGE-REMOVAL-PLAN.md, /work/state/ER-04.gates.log, crates/broker-node/Cargo.toml, crates/broker-node/src/native.rs, crates/broker-protocol/src/wire.rs, crates/broker-node/tests/listener_conformance.rs
REACHABILITY:
- WsIo::pending_pong (native.rs:1147): writer is poll_read at native.rs:1274 (push on Ping frame); reader is ws_drain_pong at native.rs:1164 called from poll_read (native.rs:1225) and poll_flush (native.rs:1321); driven by WebSocket Ping frame from client; tested by lc_websocket_keepalive_and_ping
- MAX_QUEUED_PONGS bound (native.rs:880): constant bound = 4 with stated reason; enforced at native.rs:1268 before queuing; fail-closes connection on excess
- Wire codec functions in wire.rs: unit tests only; not a production store
PROOF:
- CONNECT levels 3,4,5: lc_connect_levels_3_4_5_accept, kernel binary + native=true config + TCP socket, break level-3 decode at wire.rs:348 or native.rs:2091
- Keepalive timeout (1.5x): lc_keepalive_timeout_closes_idle_client, kernel + TCP socket keepalive=2, change factor at native.rs:2094
- CONNECT timeout (10s): lc_connect_timeout_closes_silent_socket, kernel + TCP no CONNECT, increase CONNECT_TIMEOUT at native.rs:43
- max_connections: lc_max_connections_refuses_beyond_limit, kernel + max_connections=2 config + TCP, remove limit at native.rs:303-306
- QoS 1 DUP replay: lc_qos1_dup_replays_after_reconnect, kernel + durable session + TCP, break session resume at main.rs:790
- QoS 2 handshake + replay: lc_qos2_handshake_completes_and_replays, kernel + durable session + TCP, break QoS 2 state at broker-session
- Will on abnormal close: lc_will_fires_on_drop_not_on_disconnect, kernel + will + TCP drop vs DISCONNECT, remove will fire at native.rs:1972
- Takeover (v5 0x8E): lc_takeover_resumes_session, kernel + same client-id + TCP, remove takeover at main.rs:817
- Retained delivery: lc_retained_delivers_to_late_subscriber, kernel + retained publish + TCP, clear retained store at broker-router
- Malformed packets: lc_malformed_packet_closes_only_that_client, kernel + bad flags/QoS3/2nd CONNECT + TCP, weaken decode at wire.rs
- Topic alias both directions: lc_topic_alias_both_directions, kernel + alias publish + TCP, remove alias at native.rs:1726-1755
- Receive maximum: lc_receive_maximum_sheds_above_window, kernel + receive_max=1 + TCP, remove window at main.rs:670-694
- Max packet size (fixed 1 MiB): lc_max_packet_size_documents_fixed_cap, kernel + oversize publish + TCP, raise MAX_PACKET_BYTES at native.rs:53
- Subscription ID + no-local: lc_subscription_id_and_no_local_options, kernel + sub ID 42 / no-local opt + TCP, drop properties at main.rs:6087
- Shared subscription: lc_shared_subscription_delivers, kernel + $share/ filter + TCP, break shared sub at broker-router
- Ban/kick via API: lc_ban_and_kick_through_api, kernel + API key + HTTP DELETE/POST, remove ban/kick at clients.rs:358
- Empty client id: lc_empty_client_id_documents_shared_session, kernel + empty CONNECT + TCP, reject empty id at bind_request
- WebSocket MQTT + refusals: lc_websocket_serves_mqtt_and_refusals, kernel + WS upgrade + WS frames, break WS path at native.rs:901-945
- WebSocket keepalive + ping: lc_websocket_keepalive_and_ping, kernel + WS + Ping frame + keepalive=2, break pong flush at native.rs:1221-1226
- Codec remaining length vectors: remaining_length_known_vectors, wire.rs encode_varint/decode_varint, break encoding
- Codec concatenated packets: concatenated_packets_split_at_packet_len, wire.rs fixed_header, break parsing
- Codec 5-byte remaining length: five_byte_remaining_length_is_malformed, wire.rs decode_varint/fixed_header, accept 5-byte
- Codec fixed header flags: fixed_header_flags_for_each_packet_type, wire.rs fixed_header, accept wrong flags
- Codec connect level 3: connect_level_3_decodes_and_unknown_levels_fail, wire.rs decode_connect, reject level 3
- Codec alias vectors: alias_vectors_and_subscribe_options_ride_through, wire.rs decode_publish alias, reject alias
- Codec subscribe options: alias_vectors_and_subscribe_options_ride_through, wire.rs decode_subscribe options, reject options
- Codec user property limits: user_property_limits_fail_closed, wire.rs walk_properties user props, increase limits
- Codec CONNACK vectors: connack_vectors_for_both_levels, wire.rs encode_connack/encode_connack_v5, break encoding
SLOP:
- OTHER: listener_conformance.rs:733-734,971-972,1173-1175 — TODO(parity) markers per UNKNOWN rule for open questions (assigned client id, takeover DISCONNECT 0x8E, packet cap value); not defects
CLASSES: OTHER