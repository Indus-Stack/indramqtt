use std::sync::Arc;
use std::time::Duration;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use broker_api::admin_users::{AdminRole, AdminUsers};
use broker_api::v5::sso::sso_backend;
use broker_api::ApiState;
use broker_config::{AdminUser, AdminUsersConf};
use reqwest::StatusCode;
use serde_json::json;

/// Start the management API on a loopback socket.
///
/// Sets up the server with `ApiState::standalone`, binds a loopback
/// listener, and serves requests with Axum. Returns the base URL and
/// the state.
async fn start_server() -> (String, ApiState) {
    let engine = Arc::new(broker_rules::RuleEngine::new(
        16,
        broker_rules::BackpressurePolicy::DropOldest,
    ));
    let state = ApiState::standalone(engine);
    let app = broker_api::router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let port = listener.local_addr().expect("local addr").port();
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve test app");
    });
    (format!("http://127.0.0.1:{port}"), state)
}

/// Prove that SCRAM authentication verifies username escaping vectors.
#[test]
fn tr_006_username_escaping_vector() {
    let conf = AdminUsersConf {
        users: vec![AdminUser {
            username: "user,name=example".to_string(),
            password_hash: "$scram-sha-256$4096$W22ZaJ0SNY7soEsUEjb6gQ==$WG5d8oPm3OtcPnkdi4Uo7BkeZkBFzpcXkuLmtbsT4qY=$wfPLwcE6nTWhTAmQ7tl2KeoiWGPlZqQxSrmfPwDl2dU=".to_string(),
            role: "administrator".to_string(),
            description: "escaping vector".to_string(),
            must_change_password: false,
            mfa_secret: String::new(),
            mfa_enabled: false,
            mfa_last_step: None,
            scopes: None,
        }],
    };
    let store = AdminUsers::from_snapshot(&conf);

    let auth_message = "n=user=2Cname=3Dexample,r=rOprNGfwEbeRWgbNEkqO,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";
    let proof = BASE64
        .decode("jA+s4YTor9rmIHiMCNwLGh3OxJzt5J7AGGIFll+Mlzk=")
        .expect("valid client proof");

    let sig = store
        .scram_verify("user,name=example", auth_message, &proof)
        .expect("escaped username proof verifies");
    assert_eq!(
        BASE64.encode(sig),
        "eYccYlA2rAWZTp6yG/400lb9GthcHhtUB9ffXbzpH8o="
    );

    let mut bad_proof = proof.clone();
    bad_proof[0] ^= 1;
    assert!(store
        .scram_verify("user,name=example", auth_message, &bad_proof)
        .is_none());
}

/// Prove that SCRAM verification rejects proofs computed from incorrect passwords.
#[test]
fn tr_006_wrong_password_rejected() {
    let conf = AdminUsersConf {
        users: vec![AdminUser {
            username: "scram_user".to_string(),
            password_hash: "$scram-sha-256$4096$W22ZaJ0SNY7soEsUEjb6gQ==$rmXw8jCixPEDuqUgPnm3U1/8OQWTfpiM+sc1WMKFRoY=$cAw0SuC2mJwCfzKN17pobFdkzTcRG3FvwcBCQ1M/mVs=".to_string(),
            role: "administrator".to_string(),
            description: "scram user".to_string(),
            must_change_password: false,
            mfa_secret: String::new(),
            mfa_enabled: false,
            mfa_last_step: None,
            scopes: None,
        }],
    };
    let store = AdminUsers::from_snapshot(&conf);

    let auth_msg = "n=scram_user,r=rOprNGfwEbeRWgbNEkqO,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096,c=biws,r=rOprNGfwEbeRWgbNEkqO%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0";

    let correct_proof = BASE64
        .decode("PHdM3frSQWhwMCSuLZimRJVX2zmyTJxXDxTkluV8lGM=")
        .expect("valid proof");
    let sig = store
        .scram_verify("scram_user", auth_msg, &correct_proof)
        .expect("correct proof verifies");
    assert_eq!(
        BASE64.encode(sig),
        "VgnOMAFLPtGmt87YIaMt0/CQXJikSMjVQkpMuL+1Tvo="
    );

    let wrong_proof = BASE64
        .decode("+R/sJzVDbo3SOa8Nv00CntjWaIen0AcXzB45PqdSizw=")
        .expect("wrong proof");
    assert!(store
        .scram_verify("scram_user", auth_msg, &wrong_proof)
        .is_none());
}

/// Prove that single sign-on code exchange issues tokens and enforces single use.
#[tokio::test]
async fn tr_006_sso_code_create_and_exchange() {
    let (base_url, state) = start_server().await;
    state.sso_backends.set_unstable_flows(true);

    state
        .admin_users
        .create(
            "tr006_user",
            "Tr006-pass-1!",
            AdminRole::Viewer,
            "sso exchange user",
        )
        .expect("create test user");

    let backend = sso_backend(
        "oidc_test".to_string(),
        true,
        json!({
            "issuer": "https://idp.example.invalid",
            "redirect": "/dashboard",
        }),
    );
    state.sso_backends.insert(backend).expect("insert backend");

    let code = state
        .sso_backends
        .record_login_attempt("oidc_test", "tr006_user");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .expect("build client");

    let res = client
        .post(format!("{base_url}/api/v5/sso/token_exchange"))
        .json(&json!({
            "backend": "oidc_test",
            "code": code,
        }))
        .send()
        .await
        .expect("send exchange request");
    assert_eq!(res.status(), StatusCode::OK);

    let body: serde_json::Value = res.json().await.expect("parse response json");
    assert!(body["token"].as_str().is_some_and(|t| !t.is_empty()));
    assert_eq!(body["username"], "tr006_user");
    assert_eq!(body["backend"], "oidc_test");
    assert_eq!(body["redirect"], "/dashboard");

    let replay_res = client
        .post(format!("{base_url}/api/v5/sso/token_exchange"))
        .json(&json!({
            "backend": "oidc_test",
            "code": code,
        }))
        .send()
        .await
        .expect("send replay request");
    assert_eq!(replay_res.status(), StatusCode::BAD_REQUEST);

    let replay_body: serde_json::Value = replay_res.json().await.expect("parse replay json");
    assert_eq!(replay_body["code"], "BAD_REQUEST");
}
