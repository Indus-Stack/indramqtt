//! Smoke test for the conformance support helpers.
//!
//! Boots a kernel and speaks one MQTT 3.1.1 CONNECT over a real TCP
//! socket. The kernel answers with a CONNACK.

use super::support::*;

/// Boots a kernel, connects one 3.1.1 client with clean session and
/// keepalive 60, and asserts CONNACK carries session present 0 and
/// return code 0.
/// When the kernel refuses the connection,
/// `assert_eq!(code, 0, "return code is 0")` is the assertion that fails.
#[tokio::test]
async fn ta_000_support_boots_kernel() {
    let kernel = spawn_kernel("ta-000", None).await;
    let mut sock = dial(kernel.mqtt_port).await;
    send(&mut sock, &connect311("ta-000-client", true, 60)).await;
    let (head, body) = recv(&mut sock).await;
    assert_eq!(head, 0x20);
    let (present, code) = parse_connack311(&body);
    assert!(!present, "session present is 0");
    assert_eq!(code, 0, "return code is 0");
    kernel.finish().await;
}
