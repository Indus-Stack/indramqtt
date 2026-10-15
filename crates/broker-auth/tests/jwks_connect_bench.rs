//! Pipeline benchmark hook for B5-02 JWKS refresh (T-95).
//!
//! Hot-path numbers are measured by the pipeline, never by an agent
//! (rulebook §2 "Where do numbers come from?"): the gates run this file
//! three times on the base commit (with this file copied in) and three
//! times on the tree, and the numbers land in the gates log as `BENCH`
//! lines. The spec needs a line
//! `BENCH: crates/broker-auth/tests/jwks_connect_bench.rs :: cargo test -p broker-auth --test jwks_connect_bench -- --ignored --nocapture --test-threads=1`
//! for the pipeline to pick it up.
//!
//! Base-commit compatibility: the gates copy only this file onto the base
//! commit, where the JWKS types do not exist yet, so the JWKS legs (cached
//! key hit, unknown-`kid` miss with one bounded HTTPS fetch) compile under
//! the `jwks-bench` feature (on by default on the tree). Without the
//! feature the same file falls back to the base CONNECT credential check
//! (`MemoryAuth::authenticate`, the only CONNECT authentication on the
//! base commit) under the same four `BENCH` metric names: the base run
//! measures that path without the task's new work, the tree run with it.
//! Outcomes per leg are printed as plain `outcome` lines so the
//! before/after logs stay interpretable; only `BENCH` lines carry
//! numbers.
//!
//! What the tree leg measures is the per-CONNECT `JwksAuthenticator::verify_token`
//! cost against a real loopback HTTPS JWKS endpoint: the hit leg pays one
//! cache read plus one RS256 verification with no fetch, the miss leg
//! (unknown `kid`) pays one bounded HTTPS fetch capped by
//! `fetch_timeout_ms` plus verification, then fails closed. Memory stays
//! flat: one cached key and no per-iteration allocation beyond the check
//! itself, so the measured cost is handling, not growth.
//!
//! The RSA key/HTTPS-server helpers are intentionally inline in this
//! one file (mirroring `crate::jwks_test_support`, which this file cannot
//! import): the pipeline copies only this file onto the base commit for
//! the before-numbers, so a shared import would not resolve there.

#[cfg(not(feature = "jwks-bench"))]
use broker_auth::{Authenticator, MemoryAuth};
#[cfg(feature = "jwks-bench")]
use broker_auth::{JwksAuthenticator, JwksConfig};
use std::hint::black_box;
#[cfg(feature = "jwks-bench")]
use std::sync::Arc;
use std::time::Instant;

/// Loopback HTTPS fixture certificate (same generation as the unit
/// fixtures: `openssl req -x509 -newkey rsa:2048 -days 3650
/// -subj /CN=localhost` plus `subjectAltName=IP:127.0.0.1,DNS:localhost`).
/// Test-only trust: the bench disables TLS verification on loopback.
/// Tree-only (`jwks-bench`): the base commit has no JWKS types.
#[cfg(feature = "jwks-bench")]
const BENCH_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIDJTCCAg2gAwIBAgIUHGyuPek7PZ/bqQKavEGif5TDKBwwDQYJKoZIhvcNAQEL\nBQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDkyNDAwMzAwNVoXDTM2MDky\nMTAwMzAwNVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF\nAAOCAQ8AMIIBCgKCAQEAtdkihZVps667YXbUFntwwvjsoiWqrXqZmHuSh24KpDuu\nvvfzM+VuaO0O52OMJkDCbZE2GZz7mSQDaVNRfYG2z4MhQa95x4+AOBIA0f695Frh\n4plsm9zOnHdSCm/UD1UrkAvfwDuxC7vdEcsVtXAH024Y8O5+ogzcorjDwTB2BTLV\n6PLnPfwDgdsIkvH93dE2CNZ701fsxyepGa/hBEUx0IrYLroOxGZRXEs6fViGdVO4\nMffBZ9qFHJvRQNEf1UXscqkRPA5JkIQfbLw4xaGcbf1HB8ce42gD8GtMIS3czADi\nTzcbrd7SXq8alw73TW3DgLvuxypxDFvzI+59xLhqLQIDAQABo28wbTAdBgNVHQ4E\nFgQUrTQoqWVIzqrZqiKz6XbHAAx0eCowHwYDVR0jBBgwFoAUrTQoqWVIzqrZqiKz\n6XbHAAx0eCowDwYDVR0TAQH/BAUwAwEB/zAaBgNVHREEEzARhwR/AAABgglsb2Nh\nbGhvc3QwDQYJKoZIhvcNAQELBQADggEBAJPAiEsiOQylm9mooNhwchbZR60RLWM4\nHOQ2k1MEtX8BWpfHEFpgmlLKCDfaMlLSoTl5JHsZdinDAXoTexrN8ifMx9fOQttO\nQF50qYweJzkjOsmZmZd522t6covj7+odIG/FEqfXQHa9BisoFPox74B1GyDYd9K2\nBORFO61WQZhhtSltUSyhXAWLgxJTaqJWsSavf9qYWPs5oeOvs4aLvBzLhtx12NLI\nJL9Qi5rzXxzal7vqZYmiRMq8pH3Gi7DKpjScwRd/to07AetvvOGoyfdIcZS8vlF0\n16i1jz/RPXS8eTPl/XmQBz0bXYpTVJGkcx5lnGZLma7oNXCP+dp/v8s=\n-----END CERTIFICATE-----\n";
/// Tree-only (`jwks-bench`): the base commit has no JWKS types.
#[cfg(feature = "jwks-bench")]
const BENCH_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC12SKFlWmzrrth\ndtQWe3DC+OyiJaqtepmYe5KHbgqkO66+9/Mz5W5o7Q7nY4wmQMJtkTYZnPuZJANp\nU1F9gbbPgyFBr3nHj4A4EgDR/r3kWuHimWyb3M6cd1IKb9QPVSuQC9/AO7ELu90R\nyxW1cAfTbhjw7n6iDNyiuMPBMHYFMtXo8uc9/AOB2wiS8f3d0TYI1nvTV+zHJ6kZ\nr+EERTHQitguug7EZlFcSzp9WIZ1U7gx98Fn2oUcm9FA0R/VRexyqRE8DkmQhB9s\nvDjFoZxt/UcHxx7jaAPwa0whLdzMAOJPNxut3tJerxqXDvdNbcOAu+7HKnEMW/Mj\n7n3EuGotAgMBAAECggEAIsdSWOYIfzrtz2ggi+Qz3rYo26IEkIUgFw+bKJedJWfc\ntd1KACTjBuI/tXVOeopsJPReumtRmypOFLjAnxZN1kYn+B4NVmNVjGO1EHR98MyI\n4wOgx/Zk9XvEjwZwMjaBzFzZADTqWWomj56dmkPA22j1EC8svOVk1SItHiecisWp\noGSNJ/gDkAx0kK3c5YFobBAZZyP4nIIdpXBiAHypk4DT1cSIHEy4dFN9JVzLP/0k\nmD7jsovLvn4R8a6lSnMvescUoliHqE7kTOTd3OSLiXri+EjfvkS61zyjEB9DL+pH\ni4dVxWUJUBDnWUbNFqhYaJ03pXnXGkRbTlAJfe286QKBgQDdinXuCNHUTzQfDA5C\nkpnXndj1Sl+7f7aKSK1iLzf0Iy9X8AO9gamxNa68b3uY0dSnE1eICkOEqfcM64sI\nhV5wcR0tLER7fAIBkhzZZh3zlvYUn99WSb4QjR7oAUlLT0OSsoFidOaXXiTL1GQR\nndLplurj1mCZIu4KCpgFXNbhTwKBgQDSIiaWV71p0JLNUWjd592VnZdqCbztAsKR\n5ZjUrchJZb23F8aT6LrQFgceaNWvfdNd+AjB71la1y0xfIR9AOSLcprQDrPUBuT1\nYIrPYrunkrhZbvm3N1S3oGtESayzfYbJ6urh2i8JBXqC1s2SCdoFzmojmWHfBHEG\nFohZz4fFwwKBgQDYjEsRzVUtLe5ImsQllp8B/6zet0A0SnXnXXr9CiKrZOkWD+nY\nBzIToeGXF3G8wv4WAfYBZ+bveiOeYW6ZeaQCTM60JR0bhu9/EY9ZgVOtktYe+taX\nxaUfEJIxPXCjSGtIrWuDDbmII+Hby1O1VIuhAH/BDP+HMHl1Hz3RoBn6SQKBgErc\nh8q/72cnO2WSPz3vQO3weuT4GyqE9TRtC4mZb+VWLcRw3/oJy6QedOLMjnQ663Zq\nyxPsZXULe7pJlhnCm6liZu0Aj+hVnHQetNU1Y41LpAmYk7ZGLBRPPmfRp4k6iy6c\nVpmn1WHtZbv/MrV4dQfkhcOw/UEqn+l/VYxJdyFpAoGBAI+8qzjBnHSDUr3uDX7e\n21MBoYGPzGlYwom5zEhNM2xbHvZ6Dt+c5aB308q4Wq88At6eLFtFAaTJwNFEgz/3\n/yp8G6egdnUbScWzpG5qQQ4LiBjjzHrP7Y1LGWMBeDuaD74M2uByICuEyZmJUTTT\n2rpyA7NR3vcZpXi/ZeWghtZb\n-----END PRIVATE KEY-----\n";

/// One RSA-2048 signing key for the bench (fixed key, real RS256
/// via `jsonwebtoken`; nothing is mocked).
/// Tree-only (`jwks-bench`): the base commit has no JWKS types.
#[cfg(feature = "jwks-bench")]
struct BenchKey {
    kid: String,
    private_der: Vec<u8>,
    n_b64: String,
    e_b64: String,
}

/// Fixed test RSA key (openssl-generated, never deployed).
#[cfg(feature = "jwks-bench")]
const BENCH_RSA_PKCS1_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEArN5HvmgO5xgYLzTqq5mIW+ZFEM20TFz5C0bC0hP3mR13f9od\n7PiABrYWbLIwfWftR9+riLY5MOUZiZFioRdAsMBjtNy+E0/wfU/VwN1uqaFi9ww6\nSc2hmqvnDcdnD9H55Zu+2QmhSjD0nLNJczC2ceqYM4Q0nQw4EqbxHkcZ+/0p9uI8\nPFy90G0wSTwBErthZYe6xE5DIkzRThFmwm0l2PPt30tIVP8UssxTYYJggNlIYaMT\n79C3i0Nl7ImUyDexL029qAJfVOVc17Zrv9VACypuSA4QIAVarH0J3lJ3hi+ZHAJR\npiZ6Ex+Z+VPbtAmMczizpa3zt/NDhBj3NHWbVQIDAQABAoIBADbMc8O5Ktg4/Bmi\n1a02C1LOiSBNhRUb99qKQOHlcHy6KZJ+6zOehr8Z47vrNJHcawx4q+C3myOTzGee\njbxxKZGeOz0vKJXoplyi/SiZSSY+wad82dFhfRQR4Dmu9029i1Es8O/OLLHfGPu/\nnc4FKCCudDcIdsE+CVxoTVECywrk7S66ziHFLKZBIaB5pTbX0tsjyhvdrun7rYBN\n/iF97u0DeQi+odntQFtgdkOg1jiz4ffrA3JMjfO+CJGnRy5PmckQv/a5Md06Av4R\nTrsIcDK8G3GB4Phfkans1R5EFS0aQ8ergeiaPPAEeMCF/BXdTA6y+2em/dUOzLPX\nfasTsLUCgYEA4zU7lEFmfeEvuL6cKD+QKJn1rpwXCDSVsPxDE7N6omrwrELVg+7E\nsefWSvHSUk3xDtUmgywoUj+osUtkHMkMjCW4rLGsWKxRPEXSbUHKPP/prn7XA+oL\n70XsKAzG1jZiJMb5QxaEyDO7bnIBKOksh1THz81TL0XilkFrzb5Ewl8CgYEAwsY8\nMQMEXbzFE9t4D7pZ0SOFRBfaoRk7KxCjcve7Hlx5Dw/EC3GpfpKDj7MEQclZ7bfy\nwilOXUg0F564gL0MdeCN2EwsMjnGtVnOpEZt8kiBG36sF+CMTaXlwkveUiyg8m3c\nlxSp3swdJV4O56IYhr1HDch6cUru8/vU+2SmxssCgYEAjRkS3dfbiVLd6vHcUhZf\nNQefjgb7OqpFR7rxAPwVrWWR/rgmlp8h4YWjC7mvLlOe3uI53339sE4+OZQjIMku\nFJ6cWAqiiBLDBBYsPpN7v3QBr86vQowHS1l19c0xcfgl7MbwJtyGKzoNLOJ5m+lq\nMMTSrVbKUPIqa7tG8mc1lisCgYBlq9kLtIl47OrrHKslep8b3rDYBKOpaGRMbJkF\nkG9qv9apmA878g8J9S0bu9TDCnoY7uaXl7xCUUhv8/hXq1dvyNICUHDZnvMtAz4s\n3l7SFCrg0VS/+gUDG1A+BiB9QLKbGeIGM1Eg7edg9CJPo3uuCeunNDlGHllxnslk\nmPu91QKBgQCw2vZ5xhHbkCljOD9Hb+2L2y7ZbSgAnUgyE/vAkmAlZYreP+SyzrPe\n+kTnWrZuCvc0x+G7zv6mv3CIcZM0CHN1hdJa1Y/C3n1s06kJjgwGvgP9h6a/m9al\neIave8nupkT7zrwPbv9MJlWrtqplLBHSMG3dnsKd+iLdEEdhg8GK5Q==\n-----END RSA PRIVATE KEY-----";
/// Modulus of the fixed key (base64url, no pad).
#[cfg(feature = "jwks-bench")]
const BENCH_RSA_N_B64: &str = "rN5HvmgO5xgYLzTqq5mIW-ZFEM20TFz5C0bC0hP3mR13f9od7PiABrYWbLIwfWftR9-riLY5MOUZiZFioRdAsMBjtNy-E0_wfU_VwN1uqaFi9ww6Sc2hmqvnDcdnD9H55Zu-2QmhSjD0nLNJczC2ceqYM4Q0nQw4EqbxHkcZ-_0p9uI8PFy90G0wSTwBErthZYe6xE5DIkzRThFmwm0l2PPt30tIVP8UssxTYYJggNlIYaMT79C3i0Nl7ImUyDexL029qAJfVOVc17Zrv9VACypuSA4QIAVarH0J3lJ3hi-ZHAJRpiZ6Ex-Z-VPbtAmMczizpa3zt_NDhBj3NHWbVQ";
/// Exponent of the fixed key (base64url, no pad).
#[cfg(feature = "jwks-bench")]
const BENCH_RSA_E_B64: &str = "AQAB";

#[cfg(feature = "jwks-bench")]
impl BenchKey {
    fn generate(kid: &str) -> Self {
        use rustls::pki_types::pem::PemObject as _;
        let der =
            match rustls::pki_types::PrivateKeyDer::pem_slice_iter(BENCH_RSA_PKCS1_PEM.as_bytes())
                .next()
                .transpose()
                .expect("static key parses")
                .expect("static key holds")
            {
                rustls::pki_types::PrivateKeyDer::Pkcs1(key) => key.secret_pkcs1_der().to_vec(),
                _ => panic!("static key is PKCS#1"),
            };
        Self {
            kid: kid.to_string(),
            private_der: der,
            n_b64: BENCH_RSA_N_B64.to_string(),
            e_b64: BENCH_RSA_E_B64.to_string(),
        }
    }

    fn jwk_json(&self) -> serde_json::Value {
        serde_json::json!({
            "kty": "RSA",
            "kid": self.kid,
            "use": "sig",
            "alg": "RS256",
            "n": self.n_b64,
            "e": self.e_b64,
        })
    }

    fn mint(&self) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock reads")
            .as_secs() as i64;
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some(self.kid.clone());
        let key = jsonwebtoken::EncodingKey::from_rsa_der(&self.private_der);
        let claims = serde_json::json!({
            "sub": "device-1",
            "iss": "https://issuer.example",
            "aud": "indra-mqtt",
            "iat": now,
            "exp": now + 3600,
        });
        jsonwebtoken::encode(&header, &claims, &key).expect("token mints")
    }
}

/// Minimal real HTTPS JWKS server on loopback serving one fixed key set
/// at `/jwks.json` (real TLS via `tokio-rustls`, real HTTP/1.1 framing).
/// Tree-only (`jwks-bench`): the base commit has no JWKS types.
#[cfg(feature = "jwks-bench")]
async fn start_bench_server(keys: Vec<serde_json::Value>) -> String {
    use rustls::pki_types::pem::PemObject as _;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    let keys = Arc::new(parking_lot::RwLock::new(keys));
    let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
        rustls::pki_types::CertificateDer::pem_slice_iter(BENCH_CERT_PEM.as_bytes())
            .collect::<Result<Vec<_>, _>>()
            .expect("fixture cert reads");
    assert!(!certs.is_empty(), "fixture holds a certificate");
    let private_key: rustls::pki_types::PrivateKeyDer<'static> =
        rustls::pki_types::PrivateKeyDer::pem_slice_iter(BENCH_KEY_PEM.as_bytes())
            .next()
            .transpose()
            .expect("fixture key reads")
            .expect("fixture holds a private key");
    let server_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, private_key)
        .expect("TLS config builds");
    let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(server_config));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("loopback binds");
    let addr = listener.local_addr().expect("addr reads");
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let acceptor = acceptor.clone();
            let keys = Arc::clone(&keys);
            tokio::spawn(async move {
                let Ok(mut tls) = acceptor.accept(stream).await else {
                    return;
                };
                let mut buf = vec![0u8; 4096];
                let Ok(n) = tls.read(&mut buf).await else {
                    return;
                };
                let request = String::from_utf8_lossy(&buf[..n]);
                let path = request
                    .lines()
                    .next()
                    .unwrap_or_default()
                    .split_whitespace()
                    .nth(1)
                    .unwrap_or_default()
                    .to_string();
                let (status, body) = if path == "/jwks.json" {
                    let keys = keys.read();
                    ("200 OK", serde_json::json!({ "keys": *keys }).to_string())
                } else {
                    ("404 Not Found", String::new())
                };
                let reply = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = tls.write_all(reply.as_bytes()).await;
                let _ = tls.shutdown().await;
            });
        }
    });
    format!("https://{addr}/jwks.json")
}

/// Pipeline benchmark hook (B5-02 CONNECT path): `verify_token` cache-hit
/// cost (hit leg) and unknown-`kid` miss cost with one bounded HTTPS fetch
/// per CONNECT (miss leg) through the real JWKS verifier, printing `BENCH`
/// lines for the gates. `#[ignore]` so normal `cargo test` runs skip it
/// and the pipeline's bench runner picks it up explicitly.
/// Tree-only (`jwks-bench`): the base commit runs the fallback below.
#[cfg(feature = "jwks-bench")]
#[tokio::test]
#[ignore]
async fn jwks_connect_auth_cost() {
    let signing = BenchKey::generate("bench-key-a");
    let url = start_bench_server(vec![signing.jwk_json()]).await;
    let auth = JwksAuthenticator::new(JwksConfig {
        jwks_url: url,
        issuer: "https://issuer.example".to_string(),
        audience: "indra-mqtt".to_string(),
        refresh_period_secs: 60,
        fetch_timeout_ms: 3_000,
        refresh_timeout_ms: 3_000,
        cache_max_keys: 32,
        cache_ttl_secs: 60,
        max_document_bytes: 64 * 1024,
        clock_skew_secs: 60,
        // Loopback fixture only: production endpoints keep TLS on.
        tls_verify: false,
        ca_cert_path: None,
    });
    let token = signing.mint();
    // Warm up: one fetch populates the cache, so the hit leg below pays
    // cache read plus verification with no network I/O.
    auth.verify_token("bench", &token)
        .await
        .expect("warmup verifies");
    assert_eq!(auth.refresh_count(), 1, "warmup fetches exactly once");
    // 200 rounds per leg: a sample size, not an SLO.
    let iters = 200u32;
    // Hit leg: every CONNECT verifies against the cached key.
    let mut hit_ok = 0u32;
    let mut hit_max_ns = 0u128;
    let hit_start = Instant::now();
    for _ in 0..iters {
        let op_start = Instant::now();
        let ok = black_box(auth.verify_token("bench", &token).await.is_ok());
        hit_ok += u32::from(ok);
        hit_max_ns = hit_max_ns.max(op_start.elapsed().as_nanos());
    }
    let hit_ms = hit_start.elapsed().as_secs_f64() * 1000.0 / f64::from(iters);
    let hit_max_ms = hit_max_ns as f64 / 1_000_000.0;
    assert_eq!(hit_ok, iters, "every hit-leg verify must succeed");
    println!("BENCH jwks_verify_hit_ms {hit_ms:.4} ms");
    println!("BENCH jwks_verify_hit_max_ms {hit_max_ms:.4} ms");
    // Miss leg: one stranger key minted once and reused, so every CONNECT
    // names an unknown `kid`, pays one bounded HTTPS fetch, and is refused
    // fail-closed (exact counts: every refusal plus exactly one fetch each).
    let stranger = BenchKey::generate("bench-key-zzz");
    let unknown = stranger.mint();
    let fetches_before = auth.refresh_count();
    let mut miss_refused = 0u32;
    let mut miss_max_ns = 0u128;
    let miss_start = Instant::now();
    for _ in 0..iters {
        let op_start = Instant::now();
        let refused = black_box(auth.verify_token("bench", &unknown).await.is_err());
        miss_refused += u32::from(refused);
        miss_max_ns = miss_max_ns.max(op_start.elapsed().as_nanos());
    }
    let miss_ms = miss_start.elapsed().as_secs_f64() * 1000.0 / f64::from(iters);
    let miss_max_ms = miss_max_ns as f64 / 1_000_000.0;
    assert_eq!(miss_refused, iters, "every miss-leg verify must be refused");
    assert_eq!(
        auth.refresh_count() - fetches_before,
        u64::from(iters),
        "every miss-leg CONNECT must pay exactly one bounded fetch"
    );
    println!("BENCH jwks_verify_miss_ms {miss_ms:.4} ms");
    println!("BENCH jwks_verify_miss_max_ms {miss_max_ms:.4} ms");
    // Summary re-print of the hit mean alongside the miss numbers, so the
    // pipeline-measured hit mean (spec: mean and worst case on hit and on
    // miss) is present at the end of the run as well as before the miss leg.
    println!("BENCH jwks_verify_hit_ms {hit_ms:.4} ms");
    // Flush stdout so all BENCH lines reach the gates log before exit.
    use std::io::Write as _;
    std::io::stdout().flush().ok();
}

/// Base-commit fallback for the same hook: the JWKS types do not exist on
/// the base commit, so the same four `BENCH` metric names measure the base
/// CONNECT credential check (`MemoryAuth::authenticate`) instead — the hit
/// leg verifies valid credentials, the miss leg (wrong password) is
/// refused fail-closed. `#[ignore]` like the tree leg; only one of the two
/// bodies compiles, selected by the `jwks-bench` feature (on by default on
/// the tree, absent on the base commit where only this file is copied in).
/// Uses only base-commit interfaces (`MemoryAuth::new`, `add_user`,
/// `authenticate`). Counts are a CI sample size, not an SLO.
#[cfg(not(feature = "jwks-bench"))]
#[tokio::test]
#[ignore]
async fn jwks_connect_auth_cost() {
    let auth = MemoryAuth::new();
    auth.add_user("bench-user", b"bench-secret")
        .expect("memory-only persist cannot fail");
    // 200 rounds per leg: a sample size, not an SLO.
    let iters = 200u32;
    // Hit leg: every CONNECT verifies against the stored verifier.
    let mut hit_ok = 0u32;
    let mut hit_max_ns = 0u128;
    let hit_start = Instant::now();
    for _ in 0..iters {
        let op_start = Instant::now();
        let ok = black_box(
            auth.authenticate("bench", Some("bench-user"), Some(b"bench-secret"))
                .await
                .is_ok(),
        );
        hit_ok += u32::from(ok);
        hit_max_ns = hit_max_ns.max(op_start.elapsed().as_nanos());
    }
    let hit_ms = hit_start.elapsed().as_secs_f64() * 1000.0 / f64::from(iters);
    let hit_max_ms = hit_max_ns as f64 / 1_000_000.0;
    assert_eq!(hit_ok, iters, "every hit-leg authenticate must succeed");
    println!("outcome base_connect_hit ok=true (base credential check, no JWKS)");
    println!("BENCH jwks_verify_hit_ms {hit_ms:.4} ms");
    println!("BENCH jwks_verify_hit_max_ms {hit_max_ms:.4} ms");
    // Miss leg: wrong credentials are refused fail-closed with no fetch.
    let mut miss_refused = 0u32;
    let mut miss_max_ns = 0u128;
    let miss_start = Instant::now();
    for _ in 0..iters {
        let op_start = Instant::now();
        let refused = black_box(
            auth.authenticate("bench", Some("bench-user"), Some(b"wrong-secret"))
                .await
                .is_err(),
        );
        miss_refused += u32::from(refused);
        miss_max_ns = miss_max_ns.max(op_start.elapsed().as_nanos());
    }
    let miss_ms = miss_start.elapsed().as_secs_f64() * 1000.0 / f64::from(iters);
    let miss_max_ms = miss_max_ns as f64 / 1_000_000.0;
    assert_eq!(
        miss_refused, iters,
        "every miss-leg authenticate must be refused"
    );
    println!("outcome base_connect_miss refused=true (fail-closed, no JWKS)");
    println!("BENCH jwks_verify_miss_ms {miss_ms:.4} ms");
    println!("BENCH jwks_verify_miss_max_ms {miss_max_ms:.4} ms");
}
