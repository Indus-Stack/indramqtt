//! End-to-end latency probe.
//!
//! It opens N publisher and N subscriber pairs. Each publisher sends at a
//! fixed low rate. The payload holds a nanosecond timestamp. The subscriber
//! of the pair records the age of each message in a histogram.
//! The probe runs on one thread. Pin it to its own core.
//! It prints one JSON line every `--interval` seconds and one at the end.

use hdrhistogram::Histogram;
use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

struct Cfg {
    host: String,
    port: u16,
    pairs: usize,
    rate: f64,
    qos: u8,
    size: usize,
    duration: u64,
    warmup: u64,
    interval: u64,
    run: String,
}

fn parse() -> Cfg {
    let mut c = Cfg {
        host: "127.0.0.1".into(),
        port: 1883,
        pairs: 20,
        rate: 50.0,
        qos: 0,
        size: 64,
        duration: 60,
        warmup: 10,
        interval: 10,
        run: format!("{}", std::process::id()),
    };
    let a: Vec<String> = std::env::args().collect();
    let mut i = 1;
    while i < a.len() {
        let v = a.get(i + 1).cloned().unwrap_or_default();
        match a[i].as_str() {
            "--host" => c.host = v,
            "--port" => c.port = v.parse().unwrap(),
            "--pairs" => c.pairs = v.parse().unwrap(),
            "--rate" => c.rate = v.parse().unwrap(),
            "--qos" => c.qos = v.parse().unwrap(),
            "--size" => c.size = v.parse().unwrap(),
            "--duration" => c.duration = v.parse().unwrap(),
            "--warmup" => c.warmup = v.parse().unwrap(),
            "--interval" => c.interval = v.parse().unwrap(),
            "--run" => c.run = v,
            "-h" | "--help" => {
                eprintln!("latency-probe --host H --port P --pairs 20 --rate 50 --qos 0|1|2 --size 64 --duration 60 --warmup 10 --interval 10 --run ID");
                std::process::exit(0);
            }
            x => {
                eprintln!("unknown argument {x}");
                std::process::exit(2);
            }
        }
        i += 2;
    }
    c.size = c.size.max(16);
    c
}

fn qos_of(q: u8) -> QoS {
    match q {
        0 => QoS::AtMostOnce,
        1 => QoS::AtLeastOnce,
        _ => QoS::ExactlyOnce,
    }
}

struct Stats {
    interval: Mutex<Histogram<u64>>,
    total: Mutex<Histogram<u64>>,
    sent: AtomicU64,
    received: AtomicU64,
    sent_i: AtomicU64,
    received_i: AtomicU64,
    subscribed: AtomicU64,
    connect_errors: AtomicU64,
}

fn pct(h: &Histogram<u64>) -> String {
    if h.len() == 0 {
        return "\"p50_us\":null,\"p90_us\":null,\"p99_us\":null,\"p999_us\":null,\"max_us\":null".into();
    }
    format!(
        "\"p50_us\":{},\"p90_us\":{},\"p99_us\":{},\"p999_us\":{},\"max_us\":{}",
        h.value_at_quantile(0.5),
        h.value_at_quantile(0.9),
        h.value_at_quantile(0.99),
        h.value_at_quantile(0.999),
        h.max()
    )
}

fn new_hist() -> Histogram<u64> {
    Histogram::new_with_bounds(1, 120_000_000, 3).unwrap()
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let cfg = Arc::new(parse());
    let base = Instant::now();
    let st = Arc::new(Stats {
        interval: Mutex::new(new_hist()),
        total: Mutex::new(new_hist()),
        sent: AtomicU64::new(0),
        received: AtomicU64::new(0),
        sent_i: AtomicU64::new(0),
        received_i: AtomicU64::new(0),
        subscribed: AtomicU64::new(0),
        connect_errors: AtomicU64::new(0),
    });
    let stop = Arc::new(AtomicU64::new(0));
    let q = qos_of(cfg.qos);
    // Messages sent before this time are not part of the final histogram.
    // The time is counted from the moment when all subscribers are ready.
    let ready_ns = Arc::new(AtomicU64::new(u64::MAX));
    let warm_ns = cfg.warmup * 1_000_000_000;

    for i in 0..cfg.pairs {
        let topic = format!("probe/{}/{}", cfg.run, i);
        // Subscriber.
        {
            let mut o = MqttOptions::new(format!("probe-s-{}-{}", cfg.run, i), cfg.host.clone(), cfg.port);
            o.set_keep_alive(Duration::from_secs(30));
            o.set_clean_session(true);
            let (client, mut ev) = AsyncClient::new(o, 1024);
            let st = st.clone();
            let topic = topic.clone();
            let ready_ns = ready_ns.clone();
            let pairs = cfg.pairs as u64;
            tokio::spawn(async move {
                let mut counted = false;
                loop {
                    match ev.poll().await {
                        Ok(Event::Incoming(Packet::ConnAck(_))) => {
                            let _ = client.subscribe(topic.clone(), q).await;
                        }
                        Ok(Event::Incoming(Packet::SubAck(_))) => {
                            if !counted {
                                counted = true;
                                let n = st.subscribed.fetch_add(1, Ordering::Relaxed) + 1;
                                if n == pairs {
                                    ready_ns.store(base.elapsed().as_nanos() as u64, Ordering::Relaxed);
                                }
                            }
                        }
                        Ok(Event::Incoming(Packet::Publish(p))) => {
                            let now = base.elapsed().as_nanos() as u64;
                            if p.payload.len() >= 16 {
                                let ts = u64::from_be_bytes(p.payload[0..8].try_into().unwrap());
                                let us = (now.saturating_sub(ts) / 1000).max(1).min(119_000_000);
                                st.received.fetch_add(1, Ordering::Relaxed);
                                st.received_i.fetch_add(1, Ordering::Relaxed);
                                let _ = st.interval.lock().unwrap().record(us);
                                let r = ready_ns.load(Ordering::Relaxed);
                                if r != u64::MAX && ts >= r + warm_ns {
                                    let _ = st.total.lock().unwrap().record(us);
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(_) => {
                            st.connect_errors.fetch_add(1, Ordering::Relaxed);
                            tokio::time::sleep(Duration::from_millis(500)).await;
                        }
                    }
                }
            });
        }
        // Publisher.
        {
            let mut o = MqttOptions::new(format!("probe-p-{}-{}", cfg.run, i), cfg.host.clone(), cfg.port);
            o.set_keep_alive(Duration::from_secs(30));
            o.set_clean_session(true);
            let (client, mut ev) = AsyncClient::new(o, 1024);
            tokio::spawn(async move {
                loop {
                    if ev.poll().await.is_err() {
                        tokio::time::sleep(Duration::from_millis(500)).await;
                    }
                }
            });
            let st = st.clone();
            let cfg = cfg.clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                // Wait until all subscribers are ready.
                while st.subscribed.load(Ordering::Relaxed) < cfg.pairs as u64 {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
                // Spread the first message of the pairs over one period.
                let period = Duration::from_secs_f64(1.0 / cfg.rate);
                tokio::time::sleep(period.mul_f64(i as f64 / cfg.pairs as f64)).await;
                let mut tick = tokio::time::interval(period);
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                let mut seq: u64 = 0;
                while stop.load(Ordering::Relaxed) == 0 {
                    tick.tick().await;
                    let mut buf = vec![0u8; cfg.size];
                    let ts = base.elapsed().as_nanos() as u64;
                    buf[0..8].copy_from_slice(&ts.to_be_bytes());
                    buf[8..16].copy_from_slice(&seq.to_be_bytes());
                    seq += 1;
                    if client.publish(topic.clone(), q, false, buf).await.is_ok() {
                        st.sent.fetch_add(1, Ordering::Relaxed);
                        st.sent_i.fetch_add(1, Ordering::Relaxed);
                    }
                }
            });
        }
    }

    // Reporter. The run lasts `duration` seconds after the start of publishing.
    let start = Instant::now();
    let mut next = cfg.interval;
    loop {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let el = start.elapsed().as_secs();
        if el >= next {
            next += cfg.interval;
            let h = std::mem::replace(&mut *st.interval.lock().unwrap(), new_hist());
            let sent_i = st.sent_i.swap(0, Ordering::Relaxed);
            let recv_i = st.received_i.swap(0, Ordering::Relaxed);
            println!(
                "{{\"type\":\"interval\",\"t_s\":{},\"count\":{},\"sent\":{},\"received\":{},\"lost\":{},{}}}",
                el,
                h.len(),
                sent_i,
                recv_i,
                st.sent.load(Ordering::Relaxed) as i64 - st.received.load(Ordering::Relaxed) as i64,
                pct(&h)
            );
        }
        let r = ready_ns.load(Ordering::Relaxed);
        if r != u64::MAX && base.elapsed().as_nanos() as u64 >= r + cfg.duration * 1_000_000_000 {
            break;
        }
    }
    stop.store(1, Ordering::Relaxed);
    tokio::time::sleep(Duration::from_secs(3)).await;
    let h = st.total.lock().unwrap().clone();
    let sent = st.sent.load(Ordering::Relaxed);
    let recv = st.received.load(Ordering::Relaxed);
    println!(
        "{{\"type\":\"final\",\"pairs\":{},\"rate_per_pair\":{},\"qos\":{},\"warmup_s\":{},\"count\":{},\"sent\":{},\"received\":{},\"lost\":{},\"connect_errors\":{},{}}}",
        cfg.pairs,
        cfg.rate,
        cfg.qos,
        cfg.warmup,
        h.len(),
        sent,
        recv,
        sent as i64 - recv as i64,
        st.connect_errors.load(Ordering::Relaxed),
        pct(&h)
    );
    std::process::exit(0);
}
