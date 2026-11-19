//! Integration tests for broker-auth panic recovery sites (PX-05).
//!
//! Tests Kerberos session key generation and WebhookAuth HTTP client construction.

use broker_auth::kerberos::random_session_key;
use broker_auth::{AuthError, WebhookAuth, WebhookConfig};

#[test]
fn test_kerberos_random_session_key_generation() {
    let key1 = random_session_key().expect("session key generation succeeds");
    assert_eq!(key1.len(), 32);

    let key2 = random_session_key().expect("session key generation succeeds");
    assert_eq!(key2.len(), 32);
    assert_ne!(key1, key2, "session keys must not be constant");
}

#[test]
fn test_webhook_auth_try_new_valid_configuration() {
    let config = WebhookConfig::default();
    let try_result = WebhookAuth::try_new(config.clone());
    assert!(try_result.is_ok());

    let auth = WebhookAuth::new(config);
    assert!(!auth.is_configured());
    assert_eq!(auth.cache_len(), 0);
}

#[test]
fn test_webhook_auth_new_safe_fallback() {
    let config = WebhookConfig {
        endpoint_url: "http://127.0.0.1:28083/auth".to_string(),
        pool_size: 0,
        request_timeout_ms: 0,
        ..WebhookConfig::default()
    };
    let auth = WebhookAuth::new(config);
    assert!(auth.is_configured());
    assert_eq!(auth.config().pool_size, 0);
    assert_eq!(auth.config().request_timeout_ms, 0);
}

#[test]
fn test_webhook_auth_error_type_variant() {
    if std::env::var("TEST_WEBHOOK_FORCE_ERROR").is_ok() {
        let config = WebhookConfig::default();
        match WebhookAuth::try_new(config) {
            Err(AuthError::AuthenticationFailed(ref msg)) => {
                assert!(msg.starts_with("failed to build webhook HTTP client:"));
                return;
            }
            Ok(_) => panic!("try_new must fail with invalid builder environment"),
            Err(other) => panic!("unexpected error variant: {other:?}"),
        }
    }

    let dir = tempfile::tempdir().expect("tempdir");
    let conf_path = dir.path().join("invalid_openssl.cnf");
    std::fs::write(
        &conf_path,
        "openssl_conf = openssl_init\n\n[openssl_init]\nproviders = provider_sect\n\n[provider_sect]\nnonexistent = nonexistent_sect\n\n[nonexistent_sect]\nactivate = 1\n",
    )
    .expect("write cnf");

    let current_exe = std::env::current_exe().expect("current exe");
    let status = std::process::Command::new(current_exe)
        .arg("--exact")
        .arg("test_webhook_auth_error_type_variant")
        .arg("--nocapture")
        .env("OPENSSL_CONF", &conf_path)
        .env("TEST_WEBHOOK_FORCE_ERROR", "1")
        .status()
        .expect("run child test");

    assert!(status.success(), "child test must succeed");
}
