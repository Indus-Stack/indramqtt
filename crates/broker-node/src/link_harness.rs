//! Test harness for the removed link server.
//!
//! Drives kernel frame logic over loopback sockets. Production serves
//! clients through the native listener instead.

use super::*;

pub(crate) async fn handle_connection(
    stream: TcpStream,
    shared: Shared,
) -> Result<(), Box<dyn std::error::Error>> {
    let peer = stream
        .peer_addr()
        .map(|a| a.to_string())
        .unwrap_or_else(|_| "<unknown>".to_string());
    debug!("BrokerLink IPC connection from {}", peer);

    let transport = FramedTransport::new(stream);
    // Mailbox for guaranteed frames routed to this transport by other
    // tasks (e.g. QoS 1/2 fan-out). QoS 0 rides the bounded
    // per-subscriber backlog in `ConnTable` (D1-02) and is drained
    // alongside this mailbox so a stalled subscriber sheds oldest-first
    // without slowing publishers. The sender is registered under our
    // conn_id once the edge binds. `waker` is this transport's directed
    // QoS 0 wakeup: each bind points its slot at it so fan-out wakes
    // exactly its owner (never the wrong shard).
    let (tx, mut rx) = unbounded_channel::<BrokerFrame>();
    let waker = Arc::new(tokio::sync::Notify::new());
    // Identities bound on this transport. One BrokerLink connection
    // multiplexes every client of its edge shard, so a single slot only
    // remembers the last bind and a dead socket would leak the rest as
    // forever-connected (and their gauge counts). Every bind pushes;
    // Unbind prunes; death detaches whatever remains. Each entry owns
    // exactly one `inc_connections`, balanced by its prune or detach.
    // Entries carry the full session key (MT-03) so a dead socket
    // detaches only the owning tenant's sessions, never another
    // tenant's namesake.
    let mut bound: Vec<(SessionKey, u64)> = Vec::new();

    loop {
        tokio::select! {
            inbound = transport.recv() => {
                let frame = match inbound {
                    Ok(frame) => frame,
                    Err(brokerlink::BrokerLinkError::ConnectionClosed) => {
                        debug!("BrokerLink IPC peer {} closed", peer);
                        detach(&bound, &shared, &tx);
                        return Ok(());
                    }
                    Err(e) => {
                        warn!("BrokerLink IPC error from {}: {}", peer, e);
                        detach(&bound, &shared, &tx);
                        return Err(Box::new(e));
                    }
                };

                debug!(
                    "BrokerLink recv opcode={:?} conn_id={} seq={}",
                    frame.header.opcode, frame.header.conn_id, frame.header.sequence_no
                );

                handle_inbound_frame(frame, &shared, &tx, &transport, &mut bound, &waker).await?;
                // Opportunistic egress: fan-out in the frame above may
                // have queued QoS 0 for our own bound connections
                // (self-delivery) while we were not waiting on the
                // notify. Drain without waiting so a QoS-0-only burst
                // never depends on winning a notify race.
                if !bound.is_empty() {
                    let mut rx_batch = Vec::new();
                    let mut batch_bytes = 0usize;
                    while rx_batch.len() < brokerlink::transport::MAX_BATCH_FRAMES
                        && batch_bytes < brokerlink::transport::MAX_BATCH_BYTES
                    {
                        match rx.try_recv() {
                            Ok(frame) => {
                                batch_bytes += frame.total_frame_len();
                                rx_batch.push(frame);
                            }
                            Err(_) => break,
                        }
                    }
                    let qos0_batch = drain_qos0_for_transport(
                        &shared,
                        &bound,
                        brokerlink::transport::MAX_BATCH_FRAMES
                            .saturating_sub(rx_batch.len()),
                        brokerlink::transport::MAX_BATCH_BYTES
                            .saturating_sub(batch_bytes),
                    );
                    if !rx_batch.is_empty() || !qos0_batch.is_empty() {
                        let batch = merge_egress_batch(rx_batch, qos0_batch);
                        rearm_egress_if_full(&waker, &batch);
                        let batch_len = batch.len() as u64;
                        match transport.send_batch(&batch).await {
                            Ok(()) => {
                                shared.metrics.inc_transport_sent_by(batch_len);
                            }
                            Err(e) => {
                                shared.metrics.inc_transport_send_failed();
                                return Err(Box::new(e));
                            }
                        }
                    }
                }
            }
            outbound = rx.recv() => {
                match outbound {
                    Some(first) => {
                        // PERF-05: flush once per poll. Coalesce what is
                        // already queued (non-blocking drain, bounded by
                        // frames and encoded bytes) into one write_all +
                        // flush; order is arrival order within each class.
                        // Guaranteed frames come from `rx`, QoS 0 from the
                        // bounded backlog; the merge restores per-destination
                        // sequence order so mixed qualities never reorder a
                        // subscriber. No artificial delay: a lone frame
                        // sends immediately.
                        let mut rx_batch = vec![first];
                        let mut batch_bytes =
                            rx_batch[0].total_frame_len();
                        while rx_batch.len() < brokerlink::transport::MAX_BATCH_FRAMES
                            && batch_bytes
                                < brokerlink::transport::MAX_BATCH_BYTES
                        {
                            match rx.try_recv() {
                                Ok(frame) => {
                                    batch_bytes += frame.total_frame_len();
                                    rx_batch.push(frame);
                                }
                                Err(_) => break,
                            }
                        }
                        let qos0_batch = drain_qos0_for_transport(
                            &shared,
                            &bound,
                            brokerlink::transport::MAX_BATCH_FRAMES
                                .saturating_sub(rx_batch.len()),
                            brokerlink::transport::MAX_BATCH_BYTES
                                .saturating_sub(batch_bytes),
                        );
                        let batch = merge_egress_batch(rx_batch, qos0_batch);
                        rearm_egress_if_full(&waker, &batch);
                        // Egress stage 0: `messages_forwarded` counted
                        // admission into the mailboxes at `route()` time;
                        // only a successful write counts as edge arrival.
                        let batch_len = batch.len() as u64;
                        match transport.send_batch(&batch).await {
                            Ok(()) => {
                                shared.metrics.inc_transport_sent_by(batch_len);
                            }
                            Err(e) => {
                                shared.metrics.inc_transport_send_failed();
                                return Err(Box::new(e));
                            }
                        }
                    }
                    None => {
                        // Guaranteed senders gone; QoS 0 may still hold
                        // queued frames for our bound connections: drain
                        // what remains before detaching.
                        let qos0_batch = drain_qos0_for_transport(
                            &shared,
                            &bound,
                            brokerlink::transport::MAX_BATCH_FRAMES,
                            brokerlink::transport::MAX_BATCH_BYTES,
                        );
                        if !qos0_batch.is_empty() {
                            let batch_len = qos0_batch.len() as u64;
                            match transport.send_batch(&qos0_batch).await {
                                Ok(()) => {
                                    shared.metrics.inc_transport_sent_by(batch_len);
                                }
                                Err(e) => {
                                    shared.metrics.inc_transport_send_failed();
                                    detach(&bound, &shared, &tx);
                                    return Err(Box::new(e));
                                }
                            }
                        }
                        detach(&bound, &shared, &tx);
                        return Ok(());
                    }
                }
            }
            () = waker.notified() => {
                // QoS-0-only burst for our own bound connections (directed
                // wakeup, never another shard's): drain both mailboxes in
                // one batch so a QoS-0-only workload never sleeps.
                let mut rx_batch = Vec::new();
                let mut batch_bytes = 0usize;
                while rx_batch.len() < brokerlink::transport::MAX_BATCH_FRAMES
                    && batch_bytes < brokerlink::transport::MAX_BATCH_BYTES
                {
                    match rx.try_recv() {
                        Ok(frame) => {
                            batch_bytes += frame.total_frame_len();
                            rx_batch.push(frame);
                        }
                        Err(_) => break,
                    }
                }
                if rx_batch.is_empty() {
                    // Spurious wakeup with nothing for us (another
                    // transport's connections): park again without a
                    // socket write. Check cheaply whether any of our
                    // connections actually holds QoS 0 before draining.
                    let mut ours = false;
                    for (_, conn_id) in &bound {
                        if shared.conns.qos0_len(*conn_id) > 0 {
                            ours = true;
                            break;
                        }
                    }
                    if !ours {
                        continue;
                    }
                }
                let qos0_batch = drain_qos0_for_transport(
                    &shared,
                    &bound,
                    brokerlink::transport::MAX_BATCH_FRAMES
                        .saturating_sub(rx_batch.len()),
                    brokerlink::transport::MAX_BATCH_BYTES
                        .saturating_sub(batch_bytes),
                );
                if rx_batch.is_empty() && qos0_batch.is_empty() {
                    continue;
                }
                let batch = merge_egress_batch(rx_batch, qos0_batch);
                rearm_egress_if_full(&waker, &batch);
                let batch_len = batch.len() as u64;
                match transport.send_batch(&batch).await {
                    Ok(()) => {
                        shared.metrics.inc_transport_sent_by(batch_len);
                    }
                    Err(e) => {
                        shared.metrics.inc_transport_send_failed();
                        return Err(Box::new(e));
                    }
                }
            }
        }
    }
}

/// Wake the egress loop again when a batch used its full budget.
///
/// One wake writes at most one batch. A full batch means that more
/// frames can be in the queues, and no later event is certain to wake
/// the loop for them. The stored permit makes the next loop turn drain
/// again, after the loop gave the inbound side its turn.
fn rearm_egress_if_full(waker: &tokio::sync::Notify, batch: &[BrokerFrame]) {
    if batch.len() >= brokerlink::transport::MAX_BATCH_FRAMES {
        waker.notify_one();
        return;
    }
    let bytes: usize = batch.iter().map(BrokerFrame::total_frame_len).sum();
    if bytes >= brokerlink::transport::MAX_BATCH_BYTES {
        waker.notify_one();
    }
}

/// Drain QoS 0 backlogs for one transport's bound connections,
/// round-robin so one stalled subscriber cannot starve a fast one on
/// the same shard. Respects `max_frames` and `max_bytes` via the
/// budget-aware drain in [`ConnTable`]; stops early when no connection
/// makes progress. Never blocks; pops only what is queued.
fn drain_qos0_for_transport(
    shared: &Shared,
    bound: &[(SessionKey, u64)],
    max_frames: usize,
    max_bytes: usize,
) -> Vec<BrokerFrame> {
    let mut out = Vec::new();
    if max_frames == 0 || bound.is_empty() {
        return out;
    }
    let mut out_bytes = 0usize;
    // Round-robin one pass per connection per round: a full backlog on
    // one connection fills at most its fair share of each batch.
    loop {
        if out.len() >= max_frames || out_bytes >= max_bytes {
            break;
        }
        let mut progressed = false;
        for (_, conn_id) in bound {
            if out.len() >= max_frames || out_bytes >= max_bytes {
                break;
            }
            let remaining_frames = max_frames - out.len();
            let remaining_bytes = max_bytes.saturating_sub(out_bytes);
            // Drain at most one frame per visit for fairness; the
            // budget-aware drain still honours a single large frame.
            let frames = shared.conns.drain_qos0_with_budget(
                *conn_id,
                remaining_frames.min(1),
                remaining_bytes,
            );
            if frames.is_empty() {
                // No backlog or budget spent for this connection.
                // A single large frame with an empty batch still drains
                // alone via the budget-aware path above (which returns
                // it even when over budget); empty here means truly
                // empty, so keep scanning the rest.
                continue;
            }
            for frame in frames {
                out_bytes += frame.total_frame_len();
                out.push(frame);
                progressed = true;
            }
        }
        if !progressed {
            break;
        }
    }
    out
}

/// Process one frame from the edge: direct replies go back on our own
/// transport, routed frames fan out through the connection table.
pub(crate) async fn handle_inbound_frame<S>(
    frame: BrokerFrame,
    shared: &Shared,
    tx: &UnboundedSender<BrokerFrame>,
    transport: &FramedTransport<S>,
    bound: &mut Vec<(SessionKey, u64)>,
    waker: &Arc<tokio::sync::Notify>,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
{
    handle_inbound_frame_with(frame, shared, tx, transport, bound, waker, None).await
}
