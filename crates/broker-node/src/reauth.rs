//! MQTT 5 re-authentication state machine.
//!
//! One established connection starts re-authentication with an AUTH packet
//! that carries reason code 0x19. The method must match the method that the
//! CONNECT negotiated. JWT completes in one packet. SCRAM-SHA-256 uses one
//! continue round before the proof arrives. A deadline bounds the exchange.
//! The connection keeps its client attributes and its existing permissions
//! while a re-authentication is in progress.

use super::Shared;
use base64::Engine as _;
use broker_api::admin_users::AdminUsers;
use broker_protocol::wire::Auth;
use std::sync::Arc;

/// Default bound for one re-authentication exchange, in seconds.
/// Reason: the reply must arrive before the client gives up; 30 s matches
/// the authentication timeout that the ticket fixes.
pub const DEFAULT_REAUTH_TIMEOUT_SECS: u64 = 30;

/// Reason code that starts a re-authentication.
pub const RC_REAUTHENTICATE: u8 = 0x19;
/// Reason code that continues a SASL round.
pub const RC_CONTINUE_AUTHENTICATION: u8 = 0x18;
/// Reason code that reports success.
pub const RC_SUCCESS: u8 = 0x00;
/// Reason code for a bad authentication method.
pub const RC_BAD_AUTH_METHOD: u8 = 0x82;
/// Reason code for a failed credential or a deadline.
pub const RC_NOT_AUTHORIZED: u8 = 0x87;

const JWT_METHOD: &str = "JWT";
const SCRAM_METHOD: &str = "SCRAM-SHA-256";

/// What the connection task does with one AUTH packet.
pub enum Outcome {
    /// Send an AUTH packet with this reason code and keep the exchange open.
    Continue {
        reason: u8,
        method: String,
        data: Vec<u8>,
    },
    /// Send an AUTH packet with reason code 0. The authentication deadline
    /// is `Some` when the credential carries an expiry time.
    Success {
        method: Option<String>,
        data: Option<Vec<u8>>,
        expires_at: Option<u64>,
    },
    /// Send a DISCONNECT with this reason code and close the connection.
    Reject { reason: u8 },
}

pub struct ScramState {
    pub tenant: String,
    pub username: String,
    pub bare_first: String,
    pub server_first: String,
    pub combined_nonce: String,
    pub admins: Arc<AdminUsers>,
}

pub enum State {
    Scram(ScramState),
}

/// One re-authentication exchange in progress.
pub struct Session {
    pub state: State,
}

/// Handles one AUTH packet for an established connection.
///
/// `method_at_connect` is the Authentication Method that CONNECT negotiated.
/// A missing method means that the connection did not use enhanced
/// authentication, thus a re-authentication start is a protocol error.
pub async fn handle(
    shared: &Shared,
    conn_id: u64,
    client_id: &str,
    tenant: &str,
    method_at_connect: Option<&str>,
    session: &mut Option<Session>,
    auth: &Auth,
) -> Outcome {
    match auth.reason {
        RC_REAUTHENTICATE => {
            if session.is_some() {
                return Outcome::Reject {
                    reason: RC_BAD_AUTH_METHOD,
                };
            }
            let Some(negotiated) = method_at_connect else {
                return Outcome::Reject {
                    reason: RC_BAD_AUTH_METHOD,
                };
            };
            let Some(method) = auth.properties.auth_method.as_deref() else {
                return Outcome::Reject {
                    reason: RC_BAD_AUTH_METHOD,
                };
            };
            if !same_method(method, negotiated) {
                return Outcome::Reject {
                    reason: RC_BAD_AUTH_METHOD,
                };
            }
            if same_method(method, JWT_METHOD) {
                jwt(
                    shared,
                    conn_id,
                    client_id,
                    tenant,
                    auth.properties.auth_data.as_deref(),
                )
                .await
            } else if same_method(method, SCRAM_METHOD) {
                begin_scram(
                    shared,
                    tenant,
                    auth.properties.auth_data.as_deref(),
                    session,
                )
            } else {
                Outcome::Reject {
                    reason: RC_BAD_AUTH_METHOD,
                }
            }
        }
        RC_CONTINUE_AUTHENTICATION => {
            let Some(negotiated) = method_at_connect else {
                return Outcome::Reject {
                    reason: RC_BAD_AUTH_METHOD,
                };
            };
            let Some(method) = auth.properties.auth_method.as_deref() else {
                return Outcome::Reject {
                    reason: RC_BAD_AUTH_METHOD,
                };
            };
            if !same_method(method, SCRAM_METHOD) || !same_method(method, negotiated) {
                return Outcome::Reject {
                    reason: RC_BAD_AUTH_METHOD,
                };
            }
            let Some(pending) = session.take() else {
                return Outcome::Reject {
                    reason: RC_BAD_AUTH_METHOD,
                };
            };
            let State::Scram(state) = pending.state;
            finish_scram(
                shared,
                conn_id,
                client_id,
                state,
                auth.properties.auth_data.as_deref(),
                true,
            )
        }
        _ => Outcome::Reject {
            reason: RC_BAD_AUTH_METHOD,
        },
    }
}

fn same_method(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

async fn jwt(
    shared: &Shared,
    conn_id: u64,
    client_id: &str,
    tenant: &str,
    data: Option<&[u8]>,
) -> Outcome {
    let Some(data) = data else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let Ok(token) = std::str::from_utf8(data) else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let Some(jwks) = shared.jwks.as_ref() else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    match jwks.verify_token_in_tenant(tenant, client_id, token).await {
        Ok(verified) => {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs())
                .unwrap_or(0);
            if verified.expires_at > 0 && verified.expires_at <= now {
                return Outcome::Reject {
                    reason: RC_NOT_AUTHORIZED,
                };
            }
            let Some(payload_b64) = token.split('.').nth(1) else {
                return Outcome::Reject {
                    reason: RC_NOT_AUTHORIZED,
                };
            };
            let Some(payload_bytes) = decode_base64(payload_b64) else {
                return Outcome::Reject {
                    reason: RC_NOT_AUTHORIZED,
                };
            };
            let Ok(claims) = serde_json::from_slice::<serde_json::Value>(&payload_bytes) else {
                return Outcome::Reject {
                    reason: RC_NOT_AUTHORIZED,
                };
            };
            if let Some(claim_cid) = claims.get("client_id").and_then(|v| v.as_str()) {
                if claim_cid != client_id {
                    return Outcome::Reject {
                        reason: RC_NOT_AUTHORIZED,
                    };
                }
            }
            if finish_success(shared, conn_id, client_id, tenant, verified.acls).is_err() {
                return Outcome::Reject {
                    reason: RC_NOT_AUTHORIZED,
                };
            }
            Outcome::Success {
                method: Some(JWT_METHOD.to_string()),
                data: None,
                expires_at: (verified.expires_at > 0).then_some(verified.expires_at),
            }
        }
        Err(_) => Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        },
    }
}

pub fn begin_scram(
    shared: &Shared,
    tenant: &str,
    data: Option<&[u8]>,
    session: &mut Option<Session>,
) -> Outcome {
    let Some(data) = data else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let Ok(client_first) = std::str::from_utf8(data) else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let Some(username) = parameter(client_first, "n") else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let Some(client_nonce) = parameter(client_first, "r") else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let admins = Arc::new(AdminUsers::from_registry(&shared.config));
    let (salt, iterations) = admins.scram_params(&username);
    let Ok(key) = broker_auth::random_session_key() else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let server_nonce = base64::engine::general_purpose::STANDARD_NO_PAD.encode(key);
    let combined_nonce = format!("{client_nonce}{server_nonce}");
    let server_first = format!(
        "r={combined_nonce},s={},i={iterations}",
        base64::engine::general_purpose::STANDARD.encode(salt)
    );
    let bare_first = bare_client_first(client_first).to_string();
    *session = Some(Session {
        state: State::Scram(ScramState {
            tenant: tenant.to_string(),
            username,
            bare_first,
            server_first: server_first.clone(),
            combined_nonce,
            admins,
        }),
    });
    Outcome::Continue {
        reason: RC_CONTINUE_AUTHENTICATION,
        method: SCRAM_METHOD.to_string(),
        data: server_first.into_bytes(),
    }
}

pub fn finish_scram(
    shared: &Shared,
    conn_id: u64,
    client_id: &str,
    state: ScramState,
    data: Option<&[u8]>,
    is_reauth: bool,
) -> Outcome {
    let Some(data) = data else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let Ok(client_final) = std::str::from_utf8(data) else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let Some(nonce) = parameter(client_final, "r") else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    if nonce != state.combined_nonce {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    }
    let Some(proof_text) = parameter(client_final, "p") else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let Some(proof) = decode_base64(&proof_text) else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let Some(without_proof) = client_final.rsplit_once(",p=").map(|(head, _)| head) else {
        return Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        };
    };
    let auth_message = format!(
        "{},{},{}",
        state.bare_first, state.server_first, without_proof
    );
    match state
        .admins
        .scram_verify(&state.username, &auth_message, &proof)
    {
        Some(signature) => {
            if is_reauth
                && finish_success(shared, conn_id, client_id, &state.tenant, Vec::new()).is_err()
            {
                return Outcome::Reject {
                    reason: RC_NOT_AUTHORIZED,
                };
            }
            Outcome::Success {
                method: Some(SCRAM_METHOD.to_string()),
                data: Some(
                    format!(
                        "v={}",
                        base64::engine::general_purpose::STANDARD.encode(signature)
                    )
                    .into_bytes(),
                ),
                expires_at: None,
            }
        }
        None => Outcome::Reject {
            reason: RC_NOT_AUTHORIZED,
        },
    }
}

pub fn finish_success(
    shared: &Shared,
    conn_id: u64,
    client_id: &str,
    tenant: &str,
    acls: Vec<broker_auth::AclRule>,
) -> Result<(), ()> {
    if let Some(session) = shared.sessions.session_for_conn(conn_id) {
        session.clear_authz_cache();
    }
    shared
        .auth
        .apply_reauth_acl(tenant, client_id, acls)
        .map_err(|_| ())
}

pub fn parameter(message: &str, name: &str) -> Option<String> {
    message.split(',').find_map(|part| {
        let (key, value) = part.split_once('=')?;
        (key == name && !value.is_empty()).then(|| value.to_string())
    })
}

fn bare_client_first(client_first: &str) -> &str {
    match client_first.find("n=") {
        Some(index) => &client_first[index..],
        None => client_first,
    }
}

pub fn decode_base64(text: &str) -> Option<Vec<u8>> {
    base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(text)
        .or_else(|_| base64::engine::general_purpose::URL_SAFE.decode(text))
        .or_else(|_| base64::engine::general_purpose::STANDARD_NO_PAD.decode(text))
        .or_else(|_| base64::engine::general_purpose::STANDARD.decode(text))
        .ok()
}
