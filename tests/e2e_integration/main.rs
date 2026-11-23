//! End-to-end integration and smoke suite for IndraMQTT core services.
//!
//! Validates the end-to-end messaging pipeline without requiring external network daemons:
//! 1. Multi-topic Radix Trie router registration and matching.
//! 2. Streaming SQL evaluation and rule action dispatch.
//! 3. Session state retention, QoS 1 in-flight tracking, and offline queueing.
//! 4. API kick of a connected client delivers `ConnClose` through
//!    the kernel listener and the client observes TCP close.
//!
//! Each test that needs a broker boots one real kernel with
//! [`spawn_kernel`]. The helper writes a config dir with ephemeral ports
//! for the TCP, WebSocket and API listeners, waits for `/healthz` and
//! the MQTT port, and stops the child on drop. The kernel serves every
//! enabled listener, so the configs set no `native` key.

// Some fixtures serve the other JWKS tests; this file uses the server,
// the signing key and the certificate.
#[allow(dead_code)]
#[path = "../../crates/broker-auth/src/jwks_test_support.rs"]
pub(crate) mod jwks_test_support;

pub(crate) use broker_protocol::{QoS, Topic, TopicFilter};
pub(crate) use broker_router::{Router, Subscription};
pub(crate) use broker_rules::{BackpressurePolicy, BrokerSink, RuleAction, RuleEngine};
pub(crate) use broker_session::{QueuedMessage, SessionManager};
pub(crate) use bytes::Bytes;
pub(crate) use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
pub(crate) use std::sync::{Arc, Mutex};

/// Bound for one socket step. A missing packet fails instead of hanging.
pub(crate) const STEP: std::time::Duration = std::time::Duration::from_secs(10);
/// Bound for one kernel boot. A cold debug boot needs the full minute.
pub(crate) const BOOT: std::time::Duration = std::time::Duration::from_secs(60);
/// API key for the management API. It clears the 16-char minimum.
pub(crate) const API_KEY: &str = "p6-e2e-test-key-0001";

/// One booted kernel for a test.
pub(crate) struct Kernel {
    pub(crate) child: tokio::process::Child,
    pub(crate) mqtt_port: u16,
    pub(crate) ws_port: u16,
    pub(crate) api_port: u16,
    pub(crate) scratch: std::path::PathBuf,
    pub(crate) config_dir: std::path::PathBuf,
}

impl Kernel {
    /// Stops the kernel and removes its temp dirs.
    pub(crate) async fn finish(mut self) {
        let _ = self.child.kill().await;
        let _ = std::fs::remove_dir_all(&self.scratch);
        let _ = std::fs::remove_dir_all(&self.config_dir);
    }

    /// Stop the kernel and start it again on the same config and data
    /// dirs. The caller observes persisted state and the runtime rebuild.
    pub(crate) async fn restart(&mut self) {
        let _ = self.child.kill().await;
        let mut child = tokio::process::Command::new(shard_kernel_binary())
            .arg("--config-dir")
            .arg(&self.config_dir)
            .arg("--allow-anonymous")
            .env("INDRA_API_KEYS", API_KEY)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .expect("respawn real kernel");
        let http = reqwest::Client::new();
        let base = format!("http://127.0.0.1:{}", self.api_port);
        let deadline = std::time::Instant::now() + BOOT;
        loop {
            let up = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                http.get(format!("{base}/healthz")).send(),
            )
            .await;
            if let Ok(Ok(res)) = up {
                if let Ok(text) = res.text().await {
                    if text == "OK"
                        && tokio::net::TcpStream::connect(("127.0.0.1", self.mqtt_port))
                            .await
                            .is_ok()
                        && tokio::net::TcpStream::connect(("127.0.0.1", self.ws_port))
                            .await
                            .is_ok()
                    {
                        break;
                    }
                }
            }
            if std::time::Instant::now() >= deadline {
                let _ = child.kill().await;
                panic!(
                    "kernel did not restart on API {} and MQTT {}",
                    self.api_port, self.mqtt_port
                );
            }
            if let Ok(Some(status)) = child.try_wait() {
                panic!("kernel exited during restart: {status}");
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        self.child = child;
    }
}

/// Takes one free loopback port.
pub(crate) fn ephemeral_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .expect("probe binds")
        .local_addr()
        .expect("probe addr")
        .port()
}

/// Counter for kernel boot tags. Parallel boots never share dirs.
pub(crate) static KERNEL_TAG_SLOT: AtomicU64 = AtomicU64::new(0);

/// Boots one real kernel with the kernel listener on ephemeral ports.
///
/// `extra_toml` appends to `indra.toml` (tenant rules, session limits).
/// TCP, WebSocket and API get explicit loopback ports. TLS and secure
/// WebSocket stay disabled. The link bind gets an explicit loopback port
/// so parallel tests never share the default port. The call waits for
/// `/healthz` plus the MQTT and WebSocket ports. The child dies with the
/// test (`kill_on_drop`).
pub(crate) async fn spawn_kernel(extra_toml: &str) -> Kernel {
    // Boot up to three times. A parallel boot sometimes takes a probed
    // port first, and the loser exits during boot. Each attempt uses
    // fresh ports and fresh dirs. A real boot defect fails each attempt
    // and still panics.
    let mut attempt = 0;
    loop {
        attempt += 1;
        match try_spawn_kernel(extra_toml).await {
            Ok(kernel) => return kernel,
            Err(boot) if boot.exited && attempt < 3 => continue,
            Err(boot) => panic!("{}", boot.message),
        }
    }
}

/// One kernel boot failure. `exited` marks an early child exit.
pub(crate) struct BootFailure {
    pub(crate) exited: bool,
    pub(crate) message: String,
}

/// Boots one kernel once. Returns the kernel on success. Returns the
/// failure on a timeout or an early child exit. The caller retries an
/// early exit and panics otherwise.
pub(crate) async fn try_spawn_kernel(extra_toml: &str) -> Result<Kernel, BootFailure> {
    let slot = KERNEL_TAG_SLOT.fetch_add(1, Ordering::SeqCst);
    let tag = format!(
        "p6-{}-{:?}-{}-{slot}",
        std::process::id(),
        std::thread::current().id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    );
    let scratch = std::env::temp_dir().join(format!("indramqtt-{tag}-data"));
    let config_dir = std::env::temp_dir().join(format!("indramqtt-{tag}-cfg"));
    std::fs::create_dir_all(&config_dir).expect("create temp config dir");
    let bl_port = ephemeral_port();
    let mqtt_port = ephemeral_port();
    let ws_port = ephemeral_port();
    let api_port = ephemeral_port();
    std::fs::write(
        config_dir.join("indra.toml"),
        format!(
            "[node]\n\
             id = \"p6-e2e\"\n\
             data_dir = \"{}\"\n\
             brokerlink_bind = \"127.0.0.1:{bl_port}\"\n\
             \n\
             [listeners.tcp]\n\
              bind = \"127.0.0.1:{mqtt_port}\"\n\
              \n\
              [listeners.tls]\n\
              enabled = false\n\
              \n\
              [listeners.ws]\n\
              bind = \"127.0.0.1:{ws_port}\"\n\
              \n\
              [listeners.wss]\n\
              enabled = false\n\
              \n\
              [listeners.api]\n\
              bind = \"127.0.0.1:{api_port}\"\n\
              \n\
              [logging]\n\
              level = \"warn\"\n\
              \n\
              {extra_toml}",
            scratch.display(),
        ),
    )
    .expect("write indra.toml");
    let mut child = tokio::process::Command::new(shard_kernel_binary())
        .arg("--config-dir")
        .arg(&config_dir)
        .arg("--allow-anonymous")
        .env("INDRA_API_KEYS", API_KEY)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn real kernel");
    let http = reqwest::Client::new();
    let base = format!("http://127.0.0.1:{api_port}");
    let deadline = std::time::Instant::now() + BOOT;
    loop {
        let up = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            http.get(format!("{base}/healthz")).send(),
        )
        .await;
        if let Ok(Ok(res)) = up {
            if let Ok(text) = res.text().await {
                if text == "OK"
                    && tokio::net::TcpStream::connect(("127.0.0.1", mqtt_port))
                        .await
                        .is_ok()
                    && tokio::net::TcpStream::connect(("127.0.0.1", ws_port))
                        .await
                        .is_ok()
                {
                    break;
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            let _ = child.kill().await;
            let _ = std::fs::remove_dir_all(&scratch);
            let _ = std::fs::remove_dir_all(&config_dir);
            return Err(BootFailure {
                exited: false,
                message: format!("kernel never opened API {api_port} and MQTT {mqtt_port}"),
            });
        }
        if let Ok(Some(status)) = child.try_wait() {
            let _ = std::fs::remove_dir_all(&scratch);
            let _ = std::fs::remove_dir_all(&config_dir);
            return Err(BootFailure {
                exited: true,
                message: format!("kernel exited during boot: {status}"),
            });
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
    Ok(Kernel {
        child,
        mqtt_port,
        ws_port,
        api_port,
        scratch,
        config_dir,
    })
}

pub(crate) mod support;
pub(crate) use support::*;

mod actions;
mod authn;
mod backup;
mod connectors;
mod pgsql;
mod pipeline;
mod protocol;
mod relational;
mod rules;
mod runtime;
mod sharding;
mod sources;
mod sql;
mod tenant;
mod timeseries;
