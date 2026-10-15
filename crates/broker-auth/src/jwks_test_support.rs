//! Shared JWKS test fixtures for B5-02 (T-95), single source.
//!
//! One RSA-2048 signing key plus one minimal real HTTPS JWKS server
//! on loopback, used by the `broker-auth` authenticator tests and by the
//! `broker-node` CONNECT tests alike (the latter includes this file with
//! `#[path]`, so there is exactly one copy of the RSA/base64/
//! PEM/server logic). Test-only: never compiled into production binaries
//! (each user declares it under `#[cfg(test)]`).
//!
//! The server speaks real TLS via `tokio-rustls` and real HTTP/1.1 framing
//! at `/jwks.json` with generated key material; the key set is swappable
//! mid-test for rotation and the failing flag answers 500 to simulate an
//! outage on the same URL (a rebind would change the port).

use parking_lot::RwLock;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

/// Loopback HTTPS fixture, generated for the test project with
/// `openssl req -x509 -newkey rsa:2048 -days 3650 -subj /CN=localhost`
/// plus `subjectAltName=IP:127.0.0.1,DNS:localhost` (valid to 2036).
/// Test-only trust: clients use `tls_verify = false` on loopback.
pub const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\nMIIDJTCCAg2gAwIBAgIUHGyuPek7PZ/bqQKavEGif5TDKBwwDQYJKoZIhvcNAQEL\nBQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDkyNDAwMzAwNVoXDTM2MDky\nMTAwMzAwNVowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF\nAAOCAQ8AMIIBCgKCAQEAtdkihZVps667YXbUFntwwvjsoiWqrXqZmHuSh24KpDuu\nvvfzM+VuaO0O52OMJkDCbZE2GZz7mSQDaVNRfYG2z4MhQa95x4+AOBIA0f695Frh\n4plsm9zOnHdSCm/UD1UrkAvfwDuxC7vdEcsVtXAH024Y8O5+ogzcorjDwTB2BTLV\n6PLnPfwDgdsIkvH93dE2CNZ701fsxyepGa/hBEUx0IrYLroOxGZRXEs6fViGdVO4\nMffBZ9qFHJvRQNEf1UXscqkRPA5JkIQfbLw4xaGcbf1HB8ce42gD8GtMIS3czADi\nTzcbrd7SXq8alw73TW3DgLvuxypxDFvzI+59xLhqLQIDAQABo28wbTAdBgNVHQ4E\nFgQUrTQoqWVIzqrZqiKz6XbHAAx0eCowHwYDVR0jBBgwFoAUrTQoqWVIzqrZqiKz\n6XbHAAx0eCowDwYDVR0TAQH/BAUwAwEB/zAaBgNVHREEEzARhwR/AAABgglsb2Nh\nbGhvc3QwDQYJKoZIhvcNAQELBQADggEBAJPAiEsiOQylm9mooNhwchbZR60RLWM4\nHOQ2k1MEtX8BWpfHEFpgmlLKCDfaMlLSoTl5JHsZdinDAXoTexrN8ifMx9fOQttO\nQF50qYweJzkjOsmZmZd522t6covj7+odIG/FEqfXQHa9BisoFPox74B1GyDYd9K2\nBORFO61WQZhhtSltUSyhXAWLgxJTaqJWsSavf9qYWPs5oeOvs4aLvBzLhtx12NLI\nJL9Qi5rzXxzal7vqZYmiRMq8pH3Gi7DKpjScwRd/to07AetvvOGoyfdIcZS8vlF0\n16i1jz/RPXS8eTPl/XmQBz0bXYpTVJGkcx5lnGZLma7oNXCP+dp/v8s=\n-----END CERTIFICATE-----\n";
pub const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC12SKFlWmzrrth\ndtQWe3DC+OyiJaqtepmYe5KHbgqkO66+9/Mz5W5o7Q7nY4wmQMJtkTYZnPuZJANp\nU1F9gbbPgyFBr3nHj4A4EgDR/r3kWuHimWyb3M6cd1IKb9QPVSuQC9/AO7ELu90R\nyxW1cAfTbhjw7n6iDNyiuMPBMHYFMtXo8uc9/AOB2wiS8f3d0TYI1nvTV+zHJ6kZ\nr+EERTHQitguug7EZlFcSzp9WIZ1U7gx98Fn2oUcm9FA0R/VRexyqRE8DkmQhB9s\nvDjFoZxt/UcHxx7jaAPwa0whLdzMAOJPNxut3tJerxqXDvdNbcOAu+7HKnEMW/Mj\n7n3EuGotAgMBAAECggEAIsdSWOYIfzrtz2ggi+Qz3rYo26IEkIUgFw+bKJedJWfc\ntd1KACTjBuI/tXVOeopsJPReumtRmypOFLjAnxZN1kYn+B4NVmNVjGO1EHR98MyI\n4wOgx/Zk9XvEjwZwMjaBzFzZADTqWWomj56dmkPA22j1EC8svOVk1SItHiecisWp\noGSNJ/gDkAx0kK3c5YFobBAZZyP4nIIdpXBiAHypk4DT1cSIHEy4dFN9JVzLP/0k\nmD7jsovLvn4R8a6lSnMvescUoliHqE7kTOTd3OSLiXri+EjfvkS61zyjEB9DL+pH\ni4dVxWUJUBDnWUbNFqhYaJ03pXnXGkRbTlAJfe286QKBgQDdinXuCNHUTzQfDA5C\nkpnXndj1Sl+7f7aKSK1iLzf0Iy9X8AO9gamxNa68b3uY0dSnE1eICkOEqfcM64sI\nhV5wcR0tLER7fAIBkhzZZh3zlvYUn99WSb4QjR7oAUlLT0OSsoFidOaXXiTL1GQR\nndLplurj1mCZIu4KCpgFXNbhTwKBgQDSIiaWV71p0JLNUWjd592VnZdqCbztAsKR\n5ZjUrchJZb23F8aT6LrQFgceaNWvfdNd+AjB71la1y0xfIR9AOSLcprQDrPUBuT1\nYIrPYrunkrhZbvm3N1S3oGtESayzfYbJ6urh2i8JBXqC1s2SCdoFzmojmWHfBHEG\nFohZz4fFwwKBgQDYjEsRzVUtLe5ImsQllp8B/6zet0A0SnXnXXr9CiKrZOkWD+nY\nBzIToeGXF3G8wv4WAfYBZ+bveiOeYW6ZeaQCTM60JR0bhu9/EY9ZgVOtktYe+taX\nxaUfEJIxPXCjSGtIrWuDDbmII+Hby1O1VIuhAH/BDP+HMHl1Hz3RoBn6SQKBgErc\nh8q/72cnO2WSPz3vQO3weuT4GyqE9TRtC4mZb+VWLcRw3/oJy6QedOLMjnQ663Zq\nyxPsZXULe7pJlhnCm6liZu0Aj+hVnHQetNU1Y41LpAmYk7ZGLBRPPmfRp4k6iy6c\nVpmn1WHtZbv/MrV4dQfkhcOw/UEqn+l/VYxJdyFpAoGBAI+8qzjBnHSDUr3uDX7e\n21MBoYGPzGlYwom5zEhNM2xbHvZ6Dt+c5aB308q4Wq88At6eLFtFAaTJwNFEgz/3\n/yp8G6egdnUbScWzpG5qQQ4LiBjjzHrP7Y1LGWMBeDuaD74M2uByICuEyZmJUTTT\n2rpyA7NR3vcZpXi/ZeWghtZb\n-----END PRIVATE KEY-----\n";

/// One RSA-2048 signing key for tests (fixed key, real PKCS#1
/// DER signing through `jsonwebtoken`; nothing is mocked).
pub struct TestSigningKey {
    pub kid: String,
    pub private_der: Vec<u8>,
    pub n_b64: String,
    pub e_b64: String,
}

/// Fixed test RSA key (openssl-generated, never deployed).
/// All test kids share one key; selection stays by kid.
const STATIC_RSA_PKCS1_PEM: &str = "-----BEGIN RSA PRIVATE KEY-----\nMIIEpAIBAAKCAQEArN5HvmgO5xgYLzTqq5mIW+ZFEM20TFz5C0bC0hP3mR13f9od\n7PiABrYWbLIwfWftR9+riLY5MOUZiZFioRdAsMBjtNy+E0/wfU/VwN1uqaFi9ww6\nSc2hmqvnDcdnD9H55Zu+2QmhSjD0nLNJczC2ceqYM4Q0nQw4EqbxHkcZ+/0p9uI8\nPFy90G0wSTwBErthZYe6xE5DIkzRThFmwm0l2PPt30tIVP8UssxTYYJggNlIYaMT\n79C3i0Nl7ImUyDexL029qAJfVOVc17Zrv9VACypuSA4QIAVarH0J3lJ3hi+ZHAJR\npiZ6Ex+Z+VPbtAmMczizpa3zt/NDhBj3NHWbVQIDAQABAoIBADbMc8O5Ktg4/Bmi\n1a02C1LOiSBNhRUb99qKQOHlcHy6KZJ+6zOehr8Z47vrNJHcawx4q+C3myOTzGee\njbxxKZGeOz0vKJXoplyi/SiZSSY+wad82dFhfRQR4Dmu9029i1Es8O/OLLHfGPu/\nnc4FKCCudDcIdsE+CVxoTVECywrk7S66ziHFLKZBIaB5pTbX0tsjyhvdrun7rYBN\n/iF97u0DeQi+odntQFtgdkOg1jiz4ffrA3JMjfO+CJGnRy5PmckQv/a5Md06Av4R\nTrsIcDK8G3GB4Phfkans1R5EFS0aQ8ergeiaPPAEeMCF/BXdTA6y+2em/dUOzLPX\nfasTsLUCgYEA4zU7lEFmfeEvuL6cKD+QKJn1rpwXCDSVsPxDE7N6omrwrELVg+7E\nsefWSvHSUk3xDtUmgywoUj+osUtkHMkMjCW4rLGsWKxRPEXSbUHKPP/prn7XA+oL\n70XsKAzG1jZiJMb5QxaEyDO7bnIBKOksh1THz81TL0XilkFrzb5Ewl8CgYEAwsY8\nMQMEXbzFE9t4D7pZ0SOFRBfaoRk7KxCjcve7Hlx5Dw/EC3GpfpKDj7MEQclZ7bfy\nwilOXUg0F564gL0MdeCN2EwsMjnGtVnOpEZt8kiBG36sF+CMTaXlwkveUiyg8m3c\nlxSp3swdJV4O56IYhr1HDch6cUru8/vU+2SmxssCgYEAjRkS3dfbiVLd6vHcUhZf\nNQefjgb7OqpFR7rxAPwVrWWR/rgmlp8h4YWjC7mvLlOe3uI53339sE4+OZQjIMku\nFJ6cWAqiiBLDBBYsPpN7v3QBr86vQowHS1l19c0xcfgl7MbwJtyGKzoNLOJ5m+lq\nMMTSrVbKUPIqa7tG8mc1lisCgYBlq9kLtIl47OrrHKslep8b3rDYBKOpaGRMbJkF\nkG9qv9apmA878g8J9S0bu9TDCnoY7uaXl7xCUUhv8/hXq1dvyNICUHDZnvMtAz4s\n3l7SFCrg0VS/+gUDG1A+BiB9QLKbGeIGM1Eg7edg9CJPo3uuCeunNDlGHllxnslk\nmPu91QKBgQCw2vZ5xhHbkCljOD9Hb+2L2y7ZbSgAnUgyE/vAkmAlZYreP+SyzrPe\n+kTnWrZuCvc0x+G7zv6mv3CIcZM0CHN1hdJa1Y/C3n1s06kJjgwGvgP9h6a/m9al\neIave8nupkT7zrwPbv9MJlWrtqplLBHSMG3dnsKd+iLdEEdhg8GK5Q==\n-----END RSA PRIVATE KEY-----";
/// Modulus of the fixed key (base64url, no pad).
const STATIC_RSA_N_B64: &str = "rN5HvmgO5xgYLzTqq5mIW-ZFEM20TFz5C0bC0hP3mR13f9od7PiABrYWbLIwfWftR9-riLY5MOUZiZFioRdAsMBjtNy-E0_wfU_VwN1uqaFi9ww6Sc2hmqvnDcdnD9H55Zu-2QmhSjD0nLNJczC2ceqYM4Q0nQw4EqbxHkcZ-_0p9uI8PFy90G0wSTwBErthZYe6xE5DIkzRThFmwm0l2PPt30tIVP8UssxTYYJggNlIYaMT79C3i0Nl7ImUyDexL029qAJfVOVc17Zrv9VACypuSA4QIAVarH0J3lJ3hi-ZHAJRpiZ6Ex-Z-VPbtAmMczizpa3zt_NDhBj3NHWbVQ";
/// Exponent of the fixed key (base64url, no pad).
const STATIC_RSA_E_B64: &str = "AQAB";

impl TestSigningKey {
    pub fn generate(kid: &str) -> Self {
        use rustls::pki_types::pem::PemObject as _;
        let der =
            match rustls::pki_types::PrivateKeyDer::pem_slice_iter(STATIC_RSA_PKCS1_PEM.as_bytes())
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
            n_b64: STATIC_RSA_N_B64.to_string(),
            e_b64: STATIC_RSA_E_B64.to_string(),
        }
    }

    pub fn jwk_json(&self) -> serde_json::Value {
        serde_json::json!({
            "kty": "RSA",
            "kid": self.kid,
            "use": "sig",
            "alg": "RS256",
            "n": self.n_b64,
            "e": self.e_b64,
        })
    }

    pub fn mint(&self, issuer: &str, audience: &str, exp_offset_secs: i64, tamper: bool) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock reads")
            .as_secs() as i64;
        let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
        header.kid = Some(self.kid.clone());
        let key = jsonwebtoken::EncodingKey::from_rsa_der(&self.private_der);
        let claims = serde_json::json!({
            "sub": "device-1",
            "iss": issuer,
            "aud": audience,
            "iat": now,
            "exp": now + exp_offset_secs,
        });
        let mut token = jsonwebtoken::encode(&header, &claims, &key).expect("token mints");
        if tamper {
            token.push('x');
        }
        token
    }
}

/// Minimal real HTTPS server on loopback serving the current key set at
/// `/jwks.json` (real TLS via `tokio-rustls`, real HTTP/1.1 framing,
/// generated key material). The key set is swappable mid-test for
/// rotation; the failing flag answers 500 to simulate an outage on the
/// same URL (a rebind would change the port).
pub struct TestJwksServer {
    pub addr: std::net::SocketAddr,
    pub keys: Arc<RwLock<Vec<serde_json::Value>>>,
    pub failing: Arc<AtomicBool>,
}

impl TestJwksServer {
    pub async fn start(initial: Vec<serde_json::Value>) -> Self {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        // Install the default provider. The engine enables `ring`.
        // This keeps `ServerConfig::builder` safe under unification.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
        use rustls::pki_types::pem::PemObject as _;
        let keys = Arc::new(RwLock::new(initial));
        let certs: Vec<rustls::pki_types::CertificateDer<'static>> =
            rustls::pki_types::CertificateDer::pem_slice_iter(TEST_CERT_PEM.as_bytes())
                .collect::<Result<Vec<_>, _>>()
                .expect("fixture cert reads");
        assert!(!certs.is_empty(), "fixture holds a certificate");
        let private_key: rustls::pki_types::PrivateKeyDer<'static> =
            rustls::pki_types::PrivateKeyDer::pem_slice_iter(TEST_KEY_PEM.as_bytes())
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
        let failing = Arc::new(AtomicBool::new(false));
        let serve_keys = Arc::clone(&keys);
        let serve_failing = Arc::clone(&failing);
        tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    return;
                };
                let acceptor = acceptor.clone();
                let keys = Arc::clone(&serve_keys);
                let failing = Arc::clone(&serve_failing);
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
                    let (status, body) = if failing.load(Ordering::Relaxed) {
                        ("500 Internal Error", String::new())
                    } else if path == "/jwks.json" {
                        let keys = keys.read();
                        ("200 OK", serde_json::json!({ "keys": *keys }).to_string())
                    } else {
                        ("404 Not Found", String::new())
                    };
                    let content_len = body.len();
                    let reply = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {content_len}\r\nconnection: close\r\n\r\n{body}"
                    );
                    let _ = tls.write_all(reply.as_bytes()).await;
                    let _ = tls.shutdown().await;
                });
            }
        });
        Self {
            addr,
            keys,
            failing,
        }
    }

    pub fn url(&self) -> String {
        format!("https://{}/jwks.json", self.addr)
    }

    pub fn rotate(&self, keys: Vec<serde_json::Value>) {
        *self.keys.write() = keys;
    }

    /// Flip the outage fault (the `broker-node` CONNECT tests simulate
    /// outage with a dead port instead, so this stays unused there;
    /// allowed rather than split into a second server).
    #[allow(dead_code)]
    pub fn set_failing(&self, failing: bool) {
        self.failing.store(failing, Ordering::Relaxed);
    }
}
