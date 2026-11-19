//! Time-budgeted fuzz of the MQTT wire codec on the stable toolchain.
//!
//! One seeded generator drives random bytes, mutations of valid packets
//! and a decode-encode-decode round trip. The budget comes from
//! `FUZZ_SECONDS` (default 20). The seed comes from `FUZZ_SEED` or the
//! clock. On failure the test prints the seed and stores the input in
//! `tests/corpus/`. Properties: no panic, and a packet that decodes
//! encodes back to an equal packet.

use broker_protocol::wire::{self, kind, ConnackProperties, PublishOut, WireError};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Budget when `FUZZ_SECONDS` is unset. The ticket fixes the normal run.
const DEFAULT_FUZZ_SECONDS: u64 = 20;
/// Largest random packet in bytes. Small packets reach every decoder
/// branch without long loops in the test process.
const MAX_PACKET: usize = 512;
/// Corpus files larger than this are not replayed. Reason: a corpus
/// file is one failing input; a huge file is a write bug, not a case.
const MAX_CORPUS_BYTES: u64 = 64 * 1024;

/// xorshift64star. Same shape as the unit-test generator in wire.rs.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, max: usize) -> usize {
        (self.next() as usize) % (max + 1)
    }

    fn bytes(&mut self, max_len: usize) -> Vec<u8> {
        let len = self.below(max_len);
        (0..len).map(|_| self.next() as u8).collect()
    }

    fn byte(&mut self) -> u8 {
        self.next() as u8
    }
}

fn fuzz_seconds() -> u64 {
    std::env::var("FUZZ_SECONDS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(DEFAULT_FUZZ_SECONDS)
}

fn fuzz_seed() -> u64 {
    if let Ok(text) = std::env::var("FUZZ_SEED") {
        let trimmed = text.trim();
        if let Some(hex) = trimmed.strip_prefix("0x") {
            if let Ok(value) = u64::from_str_radix(hex, 16) {
                return value;
            }
        }
        if let Ok(value) = trimmed.parse::<u64>() {
            return value;
        }
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0);
    nanos ^ (u64::from(std::process::id()) << 32)
}

fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("corpus")
}

fn save_corpus(kind: &str, seed: u64, index: u64, bytes: &[u8]) -> PathBuf {
    let dir = corpus_dir();
    std::fs::create_dir_all(&dir).expect("corpus directory");
    let path = dir.join(format!("{kind}-seed{seed:016x}-i{index}.bin"));
    std::fs::write(&path, bytes).expect("write corpus file");
    path
}

fn fail(seed: u64, index: u64, kind: &str, bytes: &[u8], message: String) -> ! {
    let path = save_corpus(kind, seed, index, bytes);
    panic!(
        "wire fuzz failure seed={seed:#x} index={index} saved {}: {message}",
        path.display()
    );
}

/// Runs every body decoder on `body` at each protocol level. A panic
/// inside a decoder is a defect and fails the test.
fn decode_all_levels(first: u8, body: &[u8]) {
    let _ = wire::decode_connect(body);
    for level in [3u8, 4, 5] {
        let _ = wire::decode_publish(first & 0x0F, body, level);
        let _ = wire::decode_subscribe(body, level);
        let _ = wire::decode_unsubscribe(body, level);
        let _ = wire::decode_ack(body, level);
        let _ = wire::decode_disconnect(body, level);
    }
}

/// One input: header parse, body decoders, no panic.
fn exercise_no_panic(bytes: &[u8]) {
    let _ = wire::decode_varint(bytes);
    let _ = wire::fixed_header(bytes);
    let first = bytes.first().copied().unwrap_or(0);
    decode_all_levels(first, bytes);
    if let Ok(Some(header)) = wire::fixed_header(bytes) {
        if header.packet_len() <= bytes.len() {
            let body = &bytes[header.header_len..header.packet_len()];
            decode_all_levels(header.flags, body);
        }
    }
}

/// Rebuilds a CONNECT packet from a decoded value. The codec has no
/// CONNECT encoder; this helper exists for the round-trip property.
fn encode_connect(connect: &wire::Connect) -> Vec<u8> {
    let mut body = Vec::new();
    if connect.level == 3 {
        body.extend_from_slice(&[0, 6, b'M', b'Q', b'I', b's', b'd', b'p']);
    } else {
        body.extend_from_slice(&[0, 4, b'M', b'Q', b'T', b'T']);
    }
    body.push(connect.level);
    let mut flags = 0u8;
    if connect.clean_start {
        flags |= 0x02;
    }
    if let Some(will) = &connect.will {
        flags |= 0x04;
        flags |= (will.qos & 0x03) << 3;
        if will.retain {
            flags |= 0x20;
        }
    }
    if connect.password.is_some() {
        flags |= 0x40;
    }
    if connect.username.is_some() {
        flags |= 0x80;
    }
    body.push(flags);
    body.extend_from_slice(&connect.keepalive.to_be_bytes());
    if connect.level == 5 {
        let props = connect.properties.clone().unwrap_or_default();
        let mut section = Vec::new();
        if let Some(value) = props.session_expiry {
            section.push(wire::prop::SESSION_EXPIRY as u8);
            section.extend_from_slice(&value.to_be_bytes());
        }
        if let Some(value) = props.receive_maximum {
            section.push(wire::prop::RECEIVE_MAXIMUM as u8);
            section.extend_from_slice(&value.to_be_bytes());
        }
        if let Some(value) = props.maximum_packet_size {
            section.push(wire::prop::MAXIMUM_PACKET_SIZE as u8);
            section.extend_from_slice(&value.to_be_bytes());
        }
        if let Some(value) = props.topic_alias_maximum {
            section.push(wire::prop::TOPIC_ALIAS_MAXIMUM as u8);
            section.extend_from_slice(&value.to_be_bytes());
        }
        if let Some(value) = props.request_response_info {
            section.push(wire::prop::REQUEST_RESPONSE_INFO as u8);
            section.push(value);
        }
        if let Some(value) = props.request_problem_info {
            section.push(wire::prop::REQUEST_PROBLEM_INFO as u8);
            section.push(value);
        }
        if let Some(method) = &props.auth_method {
            section.push(wire::prop::AUTH_METHOD as u8);
            section.extend_from_slice(&(method.len() as u16).to_be_bytes());
            section.extend_from_slice(method.as_bytes());
        }
        if let Some(data) = &props.auth_data {
            section.push(wire::prop::AUTH_DATA as u8);
            section.extend_from_slice(&(data.len() as u16).to_be_bytes());
            section.extend_from_slice(data);
        }
        for (key, value) in &props.user_properties {
            section.push(wire::prop::USER_PROPERTY as u8);
            section.extend_from_slice(&(key.len() as u16).to_be_bytes());
            section.extend_from_slice(key.as_bytes());
            section.extend_from_slice(&(value.len() as u16).to_be_bytes());
            section.extend_from_slice(value.as_bytes());
        }
        wire::encode_varint(section.len() as u32, &mut body);
        body.extend_from_slice(&section);
    }
    body.extend_from_slice(&(connect.client_id.len() as u16).to_be_bytes());
    body.extend_from_slice(connect.client_id.as_bytes());
    if let Some(will) = &connect.will {
        if connect.level == 5 {
            body.push(0);
        }
        body.extend_from_slice(&(will.topic.len() as u16).to_be_bytes());
        body.extend_from_slice(will.topic.as_bytes());
        body.extend_from_slice(&(will.payload.len() as u16).to_be_bytes());
        body.extend_from_slice(&will.payload);
    }
    if let Some(username) = &connect.username {
        body.extend_from_slice(&(username.len() as u16).to_be_bytes());
        body.extend_from_slice(username.as_bytes());
    }
    if let Some(password) = &connect.password {
        body.extend_from_slice(&(password.len() as u16).to_be_bytes());
        body.extend_from_slice(password);
    }
    let mut packet = vec![0x10u8];
    wire::encode_varint(body.len() as u32, &mut packet);
    packet.extend_from_slice(&body);
    packet
}

fn put_varint(mut value: u32, out: &mut Vec<u8>) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            break;
        }
    }
}

fn put_string(text: &str, out: &mut Vec<u8>) {
    out.extend_from_slice(&(text.len() as u16).to_be_bytes());
    out.extend_from_slice(text.as_bytes());
}

/// Round trip of a CONNECT that already decoded. `Ok(())` also means
/// the input is not a CONNECT and the check does not apply.
fn round_trip_connect(body: &[u8]) -> Result<(), String> {
    let Ok(connect) = wire::decode_connect(body) else {
        return Ok(());
    };
    let again = encode_connect(&connect);
    let header = wire::fixed_header(&again)
        .map_err(|e| format!("encoded CONNECT header: {e}"))?
        .ok_or_else(|| "encoded CONNECT header is incomplete".to_string())?;
    if header.kind != kind::CONNECT || header.packet_len() != again.len() {
        return Err("encoded CONNECT header is wrong".to_string());
    }
    let decoded =
        wire::decode_connect(&again[header.header_len..]).map_err(|e| format!("re-decode: {e}"))?;
    if decoded != connect {
        return Err(format!(
            "CONNECT changed across the round trip: {decoded:?}"
        ));
    }
    Ok(())
}

/// Round trip of a PUBLISH. An alias of 0 is the encoder sentinel for
/// "no alias"; the kernel refuses that alias with 0x94, so the check
/// does not apply to it.
fn round_trip_publish(flags: u8, body: &[u8], level: u8) -> Result<(), String> {
    let Ok(publish) = wire::decode_publish(flags, body, level) else {
        return Ok(());
    };
    if publish.alias == Some(0) {
        return Ok(());
    }
    let users: Vec<(Vec<u8>, Vec<u8>)> = publish
        .user_properties
        .iter()
        .map(|(key, value)| (key.as_bytes().to_vec(), value.as_bytes().to_vec()))
        .collect();
    let out = PublishOut {
        dup: publish.dup,
        qos: publish.qos,
        retain: publish.retain,
        topic: publish.topic,
        packet_id: publish.packet_id,
        alias: publish.alias.unwrap_or(0),
        subscription_id: 0,
        payload_format: publish.payload_format,
        message_expiry: publish.message_expiry,
        user_properties: &users,
        payload: publish.payload,
    };
    let mut bytes = Vec::new();
    wire::encode_publish(&out, level, &mut bytes);
    let header = wire::fixed_header(&bytes)
        .map_err(|e| format!("encoded PUBLISH header: {e}"))?
        .ok_or_else(|| "encoded PUBLISH header is incomplete".to_string())?;
    if header.packet_len() != bytes.len() {
        return Err("encoded PUBLISH length is wrong".to_string());
    }
    let again = wire::decode_publish(
        header.flags,
        &bytes[header.header_len..header.packet_len()],
        level,
    )
    .map_err(|e| format!("re-decode PUBLISH: {e}"))?;
    let same = publish.dup == again.dup
        && publish.qos == again.qos
        && publish.retain == again.retain
        && publish.topic == again.topic
        && publish.packet_id == again.packet_id
        && publish.alias == again.alias
        && publish.payload_format == again.payload_format
        && publish.message_expiry == again.message_expiry
        && publish.user_properties == again.user_properties
        && publish.payload == again.payload;
    if !same {
        return Err(format!(
            "PUBLISH changed across the round trip: {publish:?} then {again:?}"
        ));
    }
    Ok(())
}

fn round_trip_ack(body: &[u8], level: u8) -> Result<(), String> {
    let Ok((packet_id, reason)) = wire::decode_ack(body, level) else {
        return Ok(());
    };
    let mut bytes = Vec::new();
    wire::encode_ack(kind::PUBACK, packet_id, reason, level, &mut bytes);
    let (again_id, again_reason) =
        wire::decode_ack(&bytes[2..], level).map_err(|e| format!("re-decode ACK: {e}"))?;
    if (again_id, again_reason) != (packet_id, reason) {
        return Err(format!(
            "ACK changed across the round trip: {:?} then {:?}",
            (packet_id, reason),
            (again_id, again_reason)
        ));
    }
    Ok(())
}

fn round_trip_disconnect(body: &[u8]) -> Result<(), String> {
    let Ok(reason) = wire::decode_disconnect(body, 5) else {
        return Ok(());
    };
    let mut bytes = Vec::new();
    wire::encode_disconnect_v5(reason, &mut bytes);
    let again = wire::decode_disconnect(&bytes[2..], 5)
        .map_err(|e| format!("re-decode DISCONNECT: {e}"))?;
    if again != reason {
        return Err(format!(
            "DISCONNECT changed across the round trip: {reason} then {again}"
        ));
    }
    Ok(())
}

/// All round trips that apply to one input.
fn round_trips(seed: u64, index: u64, bytes: &[u8], fail_kind: &str) {
    let first = bytes.first().copied().unwrap_or(0);
    let body = match wire::fixed_header(bytes) {
        Ok(Some(header)) if header.packet_len() <= bytes.len() => {
            &bytes[header.header_len..header.packet_len()]
        }
        _ => bytes,
    };
    let checks: [Result<(), String>; 6] = [
        round_trip_connect(body),
        round_trip_publish(first & 0x0F, body, 4),
        round_trip_publish(first & 0x0F, body, 5),
        round_trip_ack(body, 4),
        round_trip_ack(body, 5),
        round_trip_disconnect(body),
    ];
    for check in checks {
        if let Err(message) = check {
            fail(seed, index, fail_kind, bytes, message);
        }
    }
}

/// Builds a valid CONNECT for each protocol level.
fn valid_connects() -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    // Level 3.
    let mut body = vec![0, 6, b'M', b'Q', b'I', b's', b'd', b'p', 3, 0x02, 0, 60];
    put_string("fz3", &mut body);
    let mut packet = vec![0x10];
    put_varint(body.len() as u32, &mut packet);
    packet.extend_from_slice(&body);
    out.push(packet);
    // Level 4 with will, user name and password.
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 4, 0xEE, 0, 30];
    put_string("fz4", &mut body);
    put_string("will/topic", &mut body);
    body.extend_from_slice(&[0, 2, b'h', b'i']);
    put_string("user", &mut body);
    body.extend_from_slice(&[0, 2, b'p', b'w']);
    let mut packet = vec![0x10];
    put_varint(body.len() as u32, &mut packet);
    packet.extend_from_slice(&body);
    out.push(packet);
    // Level 5 with properties and a will.
    let mut props = Vec::new();
    props.extend_from_slice(&[17, 0, 0, 0, 9]);
    props.extend_from_slice(&[33, 0, 10]);
    props.extend_from_slice(&[34, 0, 5]);
    props.extend_from_slice(&[38, 0, 1, b'k', 0, 1, b'v']);
    let mut body = vec![0, 4, b'M', b'Q', b'T', b'T', 5, 0xF6, 0, 30];
    put_varint(props.len() as u32, &mut body);
    body.extend_from_slice(&props);
    put_string("fz5", &mut body);
    body.push(0);
    put_string("w5", &mut body);
    body.extend_from_slice(&[0, 1, b'x']);
    put_string("u5", &mut body);
    body.extend_from_slice(&[0, 2, b'p', b'5']);
    let mut packet = vec![0x10];
    put_varint(body.len() as u32, &mut packet);
    packet.extend_from_slice(&body);
    out.push(packet);
    out
}

/// Builds valid PUBLISH packets at both levels.
fn valid_publishes() -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    for level in [4u8, 5] {
        for (qos, packet_id) in [(0u8, 0u16), (1, 7), (2, 9)] {
            let mut rest = Vec::new();
            put_string("fz/topic", &mut rest);
            if qos > 0 {
                rest.extend_from_slice(&packet_id.to_be_bytes());
            }
            if level == 5 {
                let mut props = Vec::new();
                props.extend_from_slice(&[35, 0, 3]);
                props.extend_from_slice(&[38, 0, 1, b'a', 0, 1, b'b']);
                put_varint(props.len() as u32, &mut rest);
                rest.extend_from_slice(&props);
            }
            rest.extend_from_slice(b"payload");
            let first = 0x30 | (qos << 1);
            let mut packet = vec![first];
            put_varint(rest.len() as u32, &mut packet);
            packet.extend_from_slice(&rest);
            out.push(packet);
        }
    }
    out
}

/// Builds other valid control packets.
fn valid_controls() -> Vec<Vec<u8>> {
    let mut out = Vec::new();
    out.push(vec![0xC0, 0x00]);
    out.push(vec![0xD0, 0x00]);
    // SUBSCRIBE level 4 and 5.
    for level in [4u8, 5] {
        let mut rest = 1u16.to_be_bytes().to_vec();
        if level == 5 {
            rest.push(0);
        }
        put_string("fz/#", &mut rest);
        rest.push(0);
        let mut packet = vec![0x82];
        put_varint(rest.len() as u32, &mut packet);
        packet.extend_from_slice(&rest);
        out.push(packet);
    }
    // UNSUBSCRIBE level 4 and 5.
    for level in [4u8, 5] {
        let mut rest = 2u16.to_be_bytes().to_vec();
        if level == 5 {
            rest.push(0);
        }
        put_string("fz/#", &mut rest);
        let mut packet = vec![0xA2];
        put_varint(rest.len() as u32, &mut packet);
        packet.extend_from_slice(&rest);
        out.push(packet);
    }
    // PUBACK, PUBREL and a v5 PUBREC with a reason.
    out.push(vec![0x40, 0x02, 0x00, 0x07]);
    out.push(vec![0x62, 0x02, 0x00, 0x07]);
    out.push(vec![0x50, 0x03, 0x00, 0x07, 0x10]);
    // DISCONNECT level 4 and 5.
    out.push(vec![0xE0, 0x00]);
    out.push(vec![0xE0, 0x01, 0x00]);
    // CONNACK level 4 (server to client; the decoder set still sees it).
    out.push(vec![0x20, 0x02, 0x00, 0x00]);
    out
}

fn seed_packets() -> Vec<Vec<u8>> {
    let mut pool = valid_connects();
    pool.extend(valid_publishes());
    pool.extend(valid_controls());
    pool
}

fn mutate(rng: &mut Rng, pool: &[Vec<u8>]) -> Vec<u8> {
    let mut packet = pool[rng.below(pool.len() - 1)].clone();
    if packet.is_empty() {
        packet.push(rng.byte());
    }
    match rng.below(3) {
        0 => {
            let index = rng.below(packet.len() - 1);
            packet[index] = rng.byte();
        }
        1 => {
            let cut = rng.below(packet.len());
            packet.truncate(cut);
        }
        _ => {
            let index = rng.below(packet.len() - 1);
            packet.insert(index, rng.byte());
        }
    }
    packet
}

/// Replays the committed corpus, then fuzzes until the budget ends.
#[test]
fn wire_codec_fuzz_stays_correct() {
    let seed = fuzz_seed();
    let budget = std::time::Duration::from_secs(fuzz_seconds());
    let deadline = Instant::now() + budget;
    eprintln!(
        "wire codec fuzz seed={seed:#x} budget={}s",
        budget.as_secs()
    );

    let mut inputs: u64 = 0;
    let mut replayed: u64 = 0;
    // Replay: every committed input must stay free of panics.
    let mut corpus_files: Vec<PathBuf> = std::fs::read_dir(corpus_dir())
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok().map(|e| e.path()))
                .filter(|path| path.extension().is_some_and(|ext| ext == "bin"))
                .filter(|path| {
                    std::fs::metadata(path)
                        .map(|meta| meta.len() <= MAX_CORPUS_BYTES)
                        .unwrap_or(false)
                })
                .collect()
        })
        .unwrap_or_default();
    corpus_files.sort();
    for path in corpus_files {
        let Ok(bytes) = std::fs::read(&path) else {
            continue;
        };
        let result = catch_unwind(AssertUnwindSafe(|| exercise_no_panic(&bytes)));
        if result.is_err() {
            panic!(
                "wire fuzz: corpus input panicked; seed={seed:#x} file={}",
                path.display()
            );
        }
        round_trips(seed, replayed, &bytes, "corpus");
        replayed += 1;
        inputs += 1;
    }

    let pool = seed_packets();
    let mut rng = Rng(seed | 1);
    let mut index: u64 = 0;
    loop {
        if Instant::now() >= deadline {
            break;
        }
        let bytes = if index.is_multiple_of(3) {
            rng.bytes(MAX_PACKET)
        } else {
            mutate(&mut rng, &pool)
        };
        let result = catch_unwind(AssertUnwindSafe(|| exercise_no_panic(&bytes)));
        if result.is_err() {
            fail(
                seed,
                index,
                "panic",
                &bytes,
                "decoder panicked on this input".to_string(),
            );
        }
        round_trips(seed, index, &bytes, "roundtrip");
        index += 1;
        inputs += 1;
        if index.is_multiple_of(4096) {
            eprintln!("wire codec fuzz seed={seed:#x} inputs={inputs}");
        }
    }
    eprintln!("WIRE_FUZZ codec inputs={inputs} replayed={replayed} seed={seed:#x} elapsed_ok=true");
    if budget.as_secs() > 0 {
        assert!(inputs > replayed, "the time budget runs new inputs");
    }
}

/// Keeps the CONNACK encoder exercised in the same binary. The budget
/// comes from `FUZZ_SECONDS`, the same source as the main target.
#[test]
fn connack_encoder_fuzz_stays_correct() {
    let seed = fuzz_seed() ^ 0xC0FF_EE00;
    let budget = std::time::Duration::from_secs(fuzz_seconds());
    let deadline = Instant::now() + budget;
    let mut rng = Rng(seed | 1);
    let mut index: u64 = 0;
    let users = vec![("k".to_string(), "v".to_string())];
    while Instant::now() < deadline {
        let props = ConnackProperties {
            session_expiry: (rng.below(1) == 1).then(|| rng.next() as u32),
            receive_maximum: (rng.below(1) == 1).then(|| rng.next() as u16),
            maximum_packet_size: (rng.below(1) == 1).then(|| rng.next() as u32),
            topic_alias_maximum: (rng.below(1) == 1).then(|| rng.next() as u16),
            assigned_client_id: if rng.below(1) == 1 {
                Some("auto")
            } else {
                None
            },
            reason_string: if rng.below(1) == 1 { Some("ok") } else { None },
            user_properties: &users,
        };
        let mut out = Vec::new();
        wire::encode_connack_v5(rng.below(1) == 1, rng.byte(), &props, &mut out);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let header = wire::fixed_header(&out)
                .expect("connack header is a result")
                .expect("connack header parses");
            assert_eq!(header.kind, kind::CONNACK);
            assert_eq!(header.packet_len(), out.len());
            // The body starts with session present and reason.
            assert!(out.len() >= 4);
        }));
        if result.is_err() {
            fail(
                seed,
                index,
                "connack",
                &out,
                "encoded CONNACK failed its own header check".to_string(),
            );
        }
        let mut plain = Vec::new();
        wire::encode_connack(rng.below(1) == 1, rng.byte() % 6, &mut plain);
        assert_eq!(plain.len(), 4, "level 4 CONNACK is four bytes");
        index += 1;
    }
    eprintln!(
        "WIRE_FUZZ connack cases={index} budget={}s seed={seed:#x}",
        budget.as_secs()
    );
}

/// Collects every error that the decoders return for one input.
fn decode_errors(bytes: &[u8]) -> Vec<WireError> {
    fn push<T>(out: &mut Vec<WireError>, result: Result<T, WireError>) {
        if let Err(error) = result {
            out.push(error);
        }
    }
    let mut errors = Vec::new();
    if let Err(error) = wire::decode_varint(bytes) {
        errors.push(error);
    }
    let mut body = bytes;
    if let Ok(Some(header)) = wire::fixed_header(bytes) {
        if header.packet_len() <= bytes.len() {
            body = &bytes[header.header_len..header.packet_len()];
        }
    }
    push(&mut errors, wire::decode_connect(body));
    for level in [3u8, 4, 5] {
        let first = bytes.first().copied().unwrap_or(0);
        push(&mut errors, wire::decode_publish(first & 0x0F, body, level));
        push(&mut errors, wire::decode_subscribe(body, level));
        push(&mut errors, wire::decode_unsubscribe(body, level));
        push(&mut errors, wire::decode_ack(body, level));
        push(&mut errors, wire::decode_disconnect(body, level));
    }
    errors
}

/// Checks the exact `Display` text of one error. Returns 0 for
/// `Malformed` and 1 for `Unsupported`.
fn check_display(error: &WireError) -> u8 {
    match error {
        WireError::Malformed(what) => {
            assert!(!what.is_empty(), "a malformed reason is not empty");
            assert_eq!(error.to_string(), format!("malformed packet: {what}"));
            0
        }
        WireError::Unsupported(what) => {
            assert!(!what.is_empty(), "an unsupported reason is not empty");
            assert_eq!(error.to_string(), format!("unsupported packet: {what}"));
            1
        }
    }
}

/// The `WireError` display path must name the kind and the reason. The
/// test reads the errors from the real decoders, not from constants.
#[test]
fn wire_error_display_never_panics() {
    assert_eq!(
        WireError::Malformed("truncated").to_string(),
        "malformed packet: truncated"
    );
    assert_eq!(
        WireError::Unsupported("protocol level").to_string(),
        "unsupported packet: protocol level"
    );

    // A known-bad CONNECT gives each kind from the real decoder.
    let unknown_level = [0x00, 0x04, b'M', b'Q', b'T', b'T', 0x06];
    let short = [0x00, 0x04, b'M', b'Q'];
    let mut kinds = [false; 2];
    for error in decode_errors(&unknown_level) {
        kinds[check_display(&error) as usize] = true;
    }
    for error in decode_errors(&short) {
        kinds[check_display(&error) as usize] = true;
    }
    assert!(kinds[1], "an unknown protocol level is unsupported");
    assert!(kinds[0], "a short CONNECT is malformed");

    // Every error from a seeded sweep prints its kind and reason.
    let seed = fuzz_seed() ^ 0x0E22_0E22;
    let mut rng = Rng(seed | 1);
    let pool = seed_packets();
    for index in 0..256u64 {
        let bytes = if index.is_multiple_of(2) {
            rng.bytes(MAX_PACKET)
        } else {
            mutate(&mut rng, &pool)
        };
        for error in decode_errors(&bytes) {
            check_display(&error);
        }
    }
}
