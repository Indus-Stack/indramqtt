//! Management error-code catalogue for the v5 management API.
//!
//! Covers `GET /error_codes` (C15-01, paged list of the documented
//! catalogue entries) and `GET /error_codes/:code` (C15-02, single entry
//! read with a 404 for an unknown code). Management-plane only: the
//! catalogue is code-defined, takes no lock, and publish/deliver never
//! touch it, so it adds no work, locks, or allocation to the per-message
//! path.
//!
//! Store wiring (rulebook section 2): no store. The catalogue is a fixed
//! code-defined array ([`ERROR_CATALOG`]), shared with the lookup
//! predicate [`find_error_code`] so the list and the detail read cannot
//! drift. There is no broker event driving it beyond serving the
//! management read, and nothing outside `crates/broker-api` writes or
//! consults it, so `tools/wiring_check.sh` needs no new entry (it only
//! tracks `Arc<crate::v5::...>` store fields on `ApiState`, of which this
//! task adds none).
//!
//! Persistence: none per request. The catalogue is static within a run
//! (compiled in, rebuilt at startup with the binary); a restart serves
//! the same fixed set.
//!
//! Store bounds (stated here and enforced by construction):
//! - exactly [`ERROR_CODE_COUNT`] entries; the list lives in code, so
//!   there is no persistence beyond it and no per-request growth;
//! - an empty catalogue read (only possible if the array were empty)
//!   reads as an empty page, never an error; an empty filter likewise
//!   returns the full page, never an error;
//! - reads clone at most one page of small documents (each entry is two
//!   short strings plus one integer) with no lock held.
//!
//! Paging (stated here): the list is a bare array and takes no paging
//! parameters, as the reference API documents. Query parameters are
//! ignored.
// The catalogue serves the stable code name, the numeric HTTP status
// the shared error mapping assigns, and a short operator summary. It
// omits fields the broker cannot supply, never a constant standing in
// for a measurement.

use axum::{
    extract::Path,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};

use crate::errors::ApiError;

/// One catalogue entry: the stable code name, the numeric HTTP status
/// the shared error mapping assigns to it, and a short operator-facing
/// summary. The summary is static help text for operators, not a
/// measured value.
pub struct ErrorCatalogEntry {
    code: &'static str,
    status: u16,
    desc: &'static str,
}

/// Fixed error-code catalogue (C15-01/C15-02).
///
/// Bound (stated here and enforced by construction): exactly
/// [`ERROR_CODE_COUNT`] entries, one per variant of the shared
/// [`ApiError`] management error type, in the same order. Reason: the
/// shared error type is the whole vocabulary management handlers can
/// render, so one entry per variant covers every code a client can
/// observe while each response clones at most one page of small
/// documents; a new error variant grows this array with its own reason.
/// The catalogue is code-defined, so the cap is enforced by
/// construction and management memory cannot grow from this route. The
/// default is finite (`22`); the operator cannot opt into more because
/// writes are out of scope (read-only routes).
const ERROR_CATALOG: [ErrorCatalogEntry; 22] = [
    ErrorCatalogEntry {
        code: "NOT_FOUND",
        status: 404,
        desc: "Requested management object does not exist",
    },
    ErrorCatalogEntry {
        code: "BAD_REQUEST",
        status: 400,
        desc: "Request body, query, or filter is malformed",
    },
    ErrorCatalogEntry {
        code: "CLIENTID_NOT_FOUND",
        status: 404,
        desc: "No connected client holds that client id",
    },
    ErrorCatalogEntry {
        code: "BAD_USERNAME_OR_PWD",
        status: 401,
        desc: "Dashboard login used a wrong username or password",
    },
    ErrorCatalogEntry {
        code: "NAME_PWD_ERROR",
        status: 401,
        desc: "Legacy login failure kept for older login endpoints",
    },
    ErrorCatalogEntry {
        code: "TOO_MANY_REQUESTS",
        status: 429,
        desc: "Login or challenge rate limit reached; retry later",
    },
    ErrorCatalogEntry {
        code: "ALREADY_EXISTS",
        status: 400,
        desc: "A user, rule, or connector with that name already exists",
    },
    ErrorCatalogEntry {
        code: "NOT_AUTHORIZED",
        status: 403,
        desc: "Caller lacks the scope or role for this operation",
    },
    ErrorCatalogEntry {
        code: "FORBIDDEN",
        status: 403,
        desc: "Operation denied under the current licence or node role",
    },
    ErrorCatalogEntry {
        code: "BAD_LISTENER_ID",
        status: 404,
        desc: "No listener holds that id",
    },
    ErrorCatalogEntry {
        code: "BAD_NODE",
        status: 404,
        desc: "No cluster node holds that name",
    },
    ErrorCatalogEntry {
        code: "TEST_FAILED",
        status: 400,
        desc: "Rule dry-run did not match the supplied input",
    },
    ErrorCatalogEntry {
        code: "CONNECTION_FAILED",
        status: 400,
        desc: "Connector or action probe could not reach its target",
    },
    ErrorCatalogEntry {
        code: "UNAUTHORIZED",
        status: 401,
        desc: "Bearer token or key is missing, expired, or malformed",
    },
    ErrorCatalogEntry {
        code: "USER_NOT_FOUND",
        status: 404,
        desc: "No dashboard user holds that name",
    },
    ErrorCatalogEntry {
        code: "ERROR_PWD_NOT_MATCH",
        status: 400,
        desc: "The old password of a password change does not match",
    },
    ErrorCatalogEntry {
        code: "NOT_ALLOWED",
        status: 400,
        desc: "The broker does not allow this change of a dashboard user",
    },
    ErrorCatalogEntry {
        code: "VALIDATION_FAILED",
        status: 400,
        desc: "Backup import validation failed",
    },
    ErrorCatalogEntry {
        code: "INTERNAL_ERROR",
        status: 500,
        desc: "Backup import apply or persist failed",
    },
    ErrorCatalogEntry {
        code: "STORE_NOT_READY",
        status: 400,
        desc: "Backup import rejected because a store is not yet initialised",
    },
    ErrorCatalogEntry {
        code: "INVALID_PARAMETER",
        status: 400,
        desc: "Request query or paging input is malformed",
    },
    ErrorCatalogEntry {
        code: "BAD_RPC",
        status: 400,
        desc: "Monitor query failed on the node",
    },
];

/// Stated count of the fixed bounded set served by these routes. Reason:
/// one entry per shared error variant above is the whole vocabulary the
/// management plane can render, so the count documents the bound the
/// handlers enforce.
pub const ERROR_CODE_COUNT: usize = 22;

// Compile-time guard that the bound covers the served catalogue.
const _: () = assert!(ERROR_CATALOG.len() == ERROR_CODE_COUNT);

fn render_entry(entry: &ErrorCatalogEntry) -> serde_json::Value {
    serde_json::json!({
        "code": entry.code,
        "status": entry.status,
        "desc": entry.desc,
    })
}

/// Whether `code` names one of the catalogue entries (exact,
/// case-sensitive match on the stable code name).
///
/// Shared by the detail read so the served list and the predicate cannot
/// drift.
pub fn find_error_code(code: &str) -> Option<&'static ErrorCatalogEntry> {
    ERROR_CATALOG.iter().find(|entry| entry.code == code)
}

/// `GET /error_codes`: the fixed catalogue as a bare JSON array, as the
/// reference API documents. The reference API documents no paging, so the
/// route takes none; query parameters are ignored.
/// Management-plane only: reads a code-defined list, takes no lock, and
/// publish/deliver never touch it.
pub async fn list_error_codes() -> Response {
    let rows: Vec<serde_json::Value> = ERROR_CATALOG.iter().map(render_entry).collect();
    (StatusCode::OK, Json(rows)).into_response()
}

/// `GET /error_codes/:code`: single catalogue entry by its stable code
/// name (exact, case-sensitive).
///
/// A known code returns its entry with 200. An unknown code returns 404
/// with the documented error shape (`{code, message}` rendered by the
/// shared [`ApiError`] type), never an empty body. Management-plane
/// only: reads a code-defined list, takes no lock, and publish/deliver
/// never touch it.
pub async fn get_error_code(Path(code): Path<String>) -> Response {
    match find_error_code(&code) {
        Some(entry) => (StatusCode::OK, Json(render_entry(entry))).into_response(),
        None => ApiError::NotFound(format!("error code {code:?} not found")).into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ApiState;
    use axum::extract::State;
    use std::sync::Arc;

    fn standalone_state() -> ApiState {
        let engine = Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    async fn response_parts(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 256 * 1024)
            .await
            .expect("error-code body is small and readable");
        let body: serde_json::Value =
            serde_json::from_slice(&bytes).expect("error-code body is JSON");
        (status, body)
    }

    #[tokio::test]
    async fn list_returns_catalogue_as_a_bare_array() {
        // W5-22 broker-through: the read goes through the served handler
        // (the same handler the kernel serves in
        // `crates/broker-node/src/main.rs`, `serve_api`), not only
        // through the catalogue array. The list carries the catalogue
        // entries as a bare array.
        let (status, body) = response_parts(list_error_codes().await).await;
        assert_eq!(status, StatusCode::OK);
        let data = body.as_array().expect("the body is a list");
        assert_eq!(data.len(), ERROR_CODE_COUNT);
        for row in data {
            assert!(row["code"].as_str().is_some_and(|c| !c.is_empty()));
            assert!(row["status"].is_u64(), "entry carries a status: {row}");
            assert!(
                row["desc"].as_str().is_some_and(|d| !d.is_empty()),
                "entry carries a summary: {row}"
            );
        }
        // Every shared error code name is present exactly once.
        let mut codes: Vec<&str> = data
            .iter()
            .map(|row| row["code"].as_str().expect("entry has a code"))
            .collect();
        codes.sort_unstable();
        codes.dedup();
        assert_eq!(codes.len(), ERROR_CODE_COUNT);
        for expected in [
            "NOT_FOUND",
            "BAD_REQUEST",
            "CLIENTID_NOT_FOUND",
            "BAD_USERNAME_OR_PWD",
            "NAME_PWD_ERROR",
            "TOO_MANY_REQUESTS",
            "ALREADY_EXISTS",
            "NOT_AUTHORIZED",
            "FORBIDDEN",
            "BAD_LISTENER_ID",
            "BAD_NODE",
            "TEST_FAILED",
            "CONNECTION_FAILED",
            "UNAUTHORIZED",
            "VALIDATION_FAILED",
            "INTERNAL_ERROR",
            "STORE_NOT_READY",
            "USER_NOT_FOUND",
            "ERROR_PWD_NOT_MATCH",
            "NOT_ALLOWED",
            "INVALID_PARAMETER",
            "BAD_RPC",
        ] {
            assert!(codes.contains(&expected), "catalogue holds {expected}");
            assert!(
                find_error_code(expected).is_some(),
                "predicate agrees for {expected}"
            );
        }
        assert!(find_error_code("no-such-code").is_none());
        assert!(find_error_code("not_found").is_none());
    }

    #[tokio::test]
    async fn detail_hit_returns_its_entry() {
        // W5-22 failing test shape: fetch a known code expecting its
        // entry. Before: 404 route-absent.
        let (status, body) =
            response_parts(get_error_code(Path("NOT_FOUND".to_string())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        assert_eq!(body["status"], serde_json::json!(404));
        assert!(body["desc"].as_str().is_some_and(|d| !d.is_empty()));
    }

    #[tokio::test]
    async fn unknown_code_is_404_with_error_shape() {
        // W5-22 failing test shape: fetch an unknown code expecting 404
        // with the documented error shape, not an empty body. Before:
        // 404 route-absent with no such shape.
        let (status, body) =
            response_parts(get_error_code(Path("NO_SUCH_CODE".to_string())).await).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        assert_eq!(body["code"], serde_json::json!("NOT_FOUND"));
        assert!(
            body["message"].as_str().is_some_and(|m| !m.is_empty()),
            "unknown code must carry {{code, message}}, got: {body}"
        );
        assert_eq!(body.as_object().map(|o| o.len()), Some(2));
    }

    #[tokio::test]
    async fn catalogue_is_static_across_reads() {
        let (status, first) = response_parts(list_error_codes().await).await;
        assert_eq!(status, StatusCode::OK);
        let (status, second) = response_parts(list_error_codes().await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(first, second);
    }

    #[tokio::test]
    async fn error_code_read_adds_no_work_to_publish() {
        // Management-plane only: catalogue reads followed by a
        // management publish through the rule engine and deliver path
        // still answer, so customer traffic keeps serving.
        let state = standalone_state();
        let (status, _) = response_parts(list_error_codes().await).await;
        assert_eq!(status, StatusCode::OK);
        let (status, _) =
            response_parts(get_error_code(Path("BAD_REQUEST".to_string())).await).await;
        assert_eq!(status, StatusCode::OK);
        let response = crate::v5::clients::publish_message(
            State(state),
            bytes::Bytes::from(
                serde_json::to_vec(&serde_json::json!({
                    "topic": "w522/probe",
                    "payload": "hello",
                    "qos": 0,
                }))
                .expect("json"),
            ),
        )
        .await;
        let (status, body) = response_parts(response).await;
        assert_eq!(status, StatusCode::ACCEPTED);
        assert!(body.get("reason_code").is_some(), "publish answers 202");
    }
}
