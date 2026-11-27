use super::*;
use crate::link_harness::handle_connection;
use tokio::net::TcpListener;

#[tokio::test]
async fn cluster_forwards_publish_to_subscribed_node() {
    use broker_cluster::{ChannelRoutingPlane, NodeId};

    // Two nodes meshed over in-process channels.
    let plane1 = ChannelRoutingPlane::new(NodeId::new("node-1"));
    let plane2 = ChannelRoutingPlane::new(NodeId::new("node-2"));
    ChannelRoutingPlane::link(&plane1, &plane2);

    let mut shared1 = Shared::new();
    shared1.allow_anonymous = true;
    shared1.cluster = Some(plane1.clone() as Arc<dyn RoutingPlane>);
    let mut shared2 = Shared::new();
    shared2.allow_anonymous = true;
    shared2.cluster = Some(plane2.clone() as Arc<dyn RoutingPlane>);

    // One link-harness listener per node. The production
    // `start_background_tasks` pumps the cluster inbox per node.
    async fn serve_one(shared: Shared) -> (std::net::SocketAddr, Vec<tokio::task::JoinHandle<()>>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
        let addr = listener.local_addr().expect("local addr");
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            handle_connection(stream, shared).await.expect("handle");
        });
        (addr, vec![accept])
    }
    let (addr1, tasks) = serve_one(shared1.clone()).await;
    let (addr2, tasks2) = serve_one(shared2.clone()).await;
    start_background_tasks(&shared1);
    start_background_tasks(&shared2);

    // Client A subscribes to metrics/# on node 1 as conn 701.
    let sub = bind_client(addr1, 701, "metrics-fan", true).await;
    sub.send(subscribe_frame(
        701,
        2,
        encode_subscribe_meta(3, "metrics-fan", &[("metrics/#", 1)]),
    ))
    .await
    .expect("subscribe");
    let suback = sub.recv().await.expect("recv suback");
    assert_eq!(suback.header.opcode, OpCode::SubAckOut);

    // Client B publishes to metrics/cpu on node 2 as conn 702.
    let publ = bind_client(addr2, 702, "cpu-sensor", true).await;
    let (meta, payload) = encode_publish_meta("metrics/cpu", 0, 0, false, b"42");
    publ.send(publish_frame(702, 2, meta, payload))
        .await
        .expect("publish");

    // Client A receives the single cross-node forward.
    let routed = sub.recv().await.expect("recv forwarded");
    assert_eq!(routed.header.opcode, OpCode::PublishOut);
    assert_eq!(routed.header.conn_id, 701);
    assert_eq!(routed.payload, Bytes::from_static(b"42"));
    let (topic, _, qos, _) = decode_publish_meta_parts(&routed.metadata);
    assert_eq!(topic, "metrics/cpu");
    assert_eq!(qos, 0);

    // A topic nobody subscribes to is not forwarded: silence proves
    // both no-match suppression and no duplicate delivery.
    let (meta, payload) = encode_publish_meta("unrelated/foo", 0, 0, false, b"zzz");
    publ.send(publish_frame(702, 3, meta, payload))
        .await
        .expect("publish unrelated");
    let silence = tokio::time::timeout(std::time::Duration::from_millis(400), sub.recv()).await;
    assert!(silence.is_err(), "unmatched topics must not cross nodes");

    for task in tasks.into_iter().chain(tasks2) {
        task.abort();
    }
}

/// Background tasks start without work on one node.
///
/// Calls the production entry with no cluster plane. Returns at
/// once and spawns nothing. The cross-node test above proves the
/// clustered pump path through the broker.
#[test]
fn background_tasks_start_without_cluster() {
    let shared = Shared::new();
    assert!(shared.cluster.is_none());
    start_background_tasks(&shared);
}

#[tokio::test]
async fn cluster_swim_membership_and_discovery_e2e() {
    use broker_cluster::{
        ChannelSwimNetwork, ClusterMembership, NodeId, SwimConfig, SwimMembership,
    };

    let net = ChannelSwimNetwork::new();
    let t1 = net.register(NodeId::new("node-alpha"));
    let t2 = net.register(NodeId::new("node-beta"));

    let swim1 = SwimMembership::new(NodeId::new("node-alpha"), None, SwimConfig::default(), t1);
    let swim2 = SwimMembership::new(NodeId::new("node-beta"), None, SwimConfig::default(), t2);

    // Start background tasks for both nodes
    let h1 = swim1.clone().run_background();
    let h2 = swim2.clone().run_background();

    // Node-2 joins Node-1
    swim2
        .join_seed(&NodeId::new("node-alpha"))
        .await
        .expect("join seed");

    // Wait a brief tick for Join/JoinAck exchange
    tokio::time::sleep(std::time::Duration::from_millis(150)).await;

    let active1 = swim1.active_nodes().await.expect("active nodes 1");
    let active2 = swim2.active_nodes().await.expect("active nodes 2");

    assert!(active1.contains(&NodeId::new("node-beta")));
    assert!(active2.contains(&NodeId::new("node-alpha")));

    h1.abort();
    h2.abort();
}
