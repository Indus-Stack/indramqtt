//! Topic rewrite rules for the v5 management API.
//!
//! Covers `GET /mqtt/topic_rewrite` (ordered list) and
//! `PUT /mqtt/topic_rewrite` (validated full replacement). Both run on
//! the shared subscription router the kernel also rewrites publishes
//! and subscribes with, so a stored rule changes live traffic without a
//! restart. The router is the single copy: this module keeps no second
//! list.
//!
//! Store bounds (stated here and enforced by the router):
//! - at most [`MAX_REWRITE_RULES`](broker_router::MAX_REWRITE_RULES)
//!   rules; writes past the cap fail with the documented `EXCEED_LIMIT`
//!   shape instead of growing without limit;
//! - one input or output is at most [`MAX_REWRITE_LEN`](broker_router::MAX_REWRITE_LEN)
//!   bytes; longer values fail closed to no rewrite;
//! - reads clone at most the capped rule list once per request under a
//!   short lock; the publish path takes the same short lock and never
//!   compiles.
//!
//! Gaps (marked where they apply):
//! - destination templates render `$1`-style captures only;
//!   `${clientid}` and `${username}` placeholders stay literal.
// TODO(parity): should the kernel fill `${clientid}` and `${username}`
// in rewrite destinations from the publishing or subscribing session?
// The reference fills them per client. The router has no client context
// on the match path today, so templates keep the placeholders literally.

use axum::{
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use bytes::Bytes;

use crate::ApiState;

/// `GET /mqtt/topic_rewrite`: the ordered rule list.
///
/// One bounded snapshot per request (at most
/// [`MAX_REWRITE_RULES`](broker_router::MAX_REWRITE_RULES) small clones
/// under a short lock); an empty table reads as `[]`, never an error.
pub async fn list_rewrites(State(state): State<ApiState>) -> Response {
    let rules = state.router.rewrite_rules_snapshot();
    let data: Vec<serde_json::Value> = rules.iter().map(render_rule).collect();
    (StatusCode::OK, Json(data)).into_response()
}

/// `PUT /mqtt/topic_rewrite`: validated full replacement.
///
/// The body must be a JSON array. Every entry is validated before
/// anything applies, so one bad entry rejects the entire write with the
/// documented `BAD_REQUEST` shape and leaves the installed table
/// untouched. Lists past the router cap fail with the documented
/// `EXCEED_LIMIT` shape. An empty array clears the table.
pub async fn put_rewrites(State(state): State<ApiState>, body: Bytes) -> Response {
    let value: serde_json::Value = match serde_json::from_slice(&body) {
        Ok(value) => value,
        Err(error) => return bad_request(format!("invalid topic_rewrite body: {error}")),
    };
    let items = match value.as_array() {
        Some(items) => items,
        None => return bad_request("topic_rewrite body must be a JSON array".to_string()),
    };
    if items.len() > broker_router::MAX_REWRITE_RULES {
        return (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "code": "EXCEED_LIMIT",
                "message": format!(
                    "Max topic rewrite rule count is {}",
                    broker_router::MAX_REWRITE_RULES
                ),
            })),
        )
            .into_response();
    }
    let mut next = Vec::with_capacity(items.len());
    for (index, item) in items.iter().enumerate() {
        match parse_rule(item) {
            Ok(rule) => next.push(rule),
            Err(reason) => return bad_request(format!("rules[{index}]: {reason}")),
        }
    }
    if let Err(error) = state.router.set_rewrite_rules(next) {
        return map_router_error(error);
    }
    let rules = state.router.rewrite_rules_snapshot();
    let data: Vec<serde_json::Value> = rules.iter().map(render_rule).collect();
    (StatusCode::OK, Json(data)).into_response()
}

fn bad_request(reason: String) -> Response {
    (
        StatusCode::BAD_REQUEST,
        Json(serde_json::json!({
            "code": "BAD_REQUEST",
            "message": reason,
        })),
    )
        .into_response()
}

fn map_router_error(error: broker_router::RewriteRuleError) -> Response {
    use broker_router::RewriteRuleError as E;
    match error {
        E::TooManyRules(_) => (
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(serde_json::json!({
                "code": "EXCEED_LIMIT",
                "message": error.to_string(),
            })),
        )
            .into_response(),
        _ => bad_request(error.to_string()),
    }
}

/// Render one installed rule in the documented shape. The kernel scope
/// `both` renders as the documented `all`.
fn render_rule(rule: &broker_router::RewriteRuleConfig) -> serde_json::Value {
    let action = match rule.scope {
        broker_router::RewriteScope::Publish => "publish",
        broker_router::RewriteScope::Subscribe => "subscribe",
        broker_router::RewriteScope::Both => "all",
    };
    serde_json::json!({
        "action": action,
        "source_topic": rule.source.clone().unwrap_or_default(),
        "dest_topic": rule.replacement,
        "re": rule.pattern,
    })
}

/// Parse one rule entry (`{action, source_topic, dest_topic, re}`).
/// `action` accepts the documented names (case-insensitive);
/// `source_topic` must be a valid filter; `re` must compile;
/// `dest_topic` must be non-empty and within the router budget.
/// Unknown fields are ignored.
fn parse_rule(value: &serde_json::Value) -> Result<broker_router::RewriteRuleConfig, String> {
    let obj = value
        .as_object()
        .ok_or_else(|| "entry must be a JSON object".to_string())?;
    let action = obj
        .get("action")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `action` is required".to_string())?;
    let scope = match action.trim().to_ascii_lowercase().as_str() {
        "publish" => broker_router::RewriteScope::Publish,
        "subscribe" => broker_router::RewriteScope::Subscribe,
        "all" | "both" => broker_router::RewriteScope::Both,
        _ => {
            return Err("field `action` must be `subscribe`, `publish` or `all`".to_string());
        }
    };
    let source = obj
        .get("source_topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `source_topic` is required".to_string())?;
    if source.is_empty() {
        return Err("field `source_topic` must not be empty".to_string());
    }
    broker_protocol::TopicFilter::new(source)
        .map_err(|e| format!("invalid source_topic `{source}`: {e}"))?;
    let dest = obj
        .get("dest_topic")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `dest_topic` is required".to_string())?;
    if dest.is_empty() {
        return Err("field `dest_topic` must not be empty".to_string());
    }
    if dest.len() > broker_router::MAX_REWRITE_LEN {
        return Err("field `dest_topic` exceeds the topic budget".to_string());
    }
    let re = obj
        .get("re")
        .and_then(|v| v.as_str())
        .ok_or_else(|| "field `re` is required".to_string())?;
    if re.is_empty() {
        return Err("field `re` must not be empty".to_string());
    }
    regex::Regex::new(re).map_err(|e| format!("invalid re `{re}`: {e}"))?;
    Ok(broker_router::RewriteRuleConfig {
        pattern: re.to_string(),
        replacement: dest.to_string(),
        scope,
        source: Some(source.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::response::Response;

    fn standalone_state() -> ApiState {
        let engine = std::sync::Arc::new(broker_rules::RuleEngine::new(
            16,
            broker_rules::BackpressurePolicy::DropOldest,
        ));
        ApiState::standalone(engine)
    }

    async fn read(response: Response) -> (StatusCode, serde_json::Value) {
        let status = response.status();
        let bytes = axum::body::to_bytes(response.into_body(), 1024 * 1024)
            .await
            .expect("rewrite body is small and readable");
        let body: serde_json::Value = serde_json::from_slice(&bytes).expect("rewrite body is JSON");
        (status, body)
    }

    fn rule(action: &str, source: &str, dest: &str, re: &str) -> serde_json::Value {
        serde_json::json!({
            "action": action,
            "source_topic": source,
            "dest_topic": dest,
            "re": re,
        })
    }

    #[tokio::test]
    async fn list_starts_empty_round_trip_and_replaces() {
        let state = standalone_state();
        let (status, before) = read(list_rewrites(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(before, serde_json::json!([]));

        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!([rule(
                "publish",
                "conf/rw/#",
                "rewritten/$1",
                "^conf/rw/(.*)$"
            )]))
            .expect("update is JSON"),
        );
        let (status, after) = read(put_rewrites(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(
            after,
            serde_json::json!([rule(
                "publish",
                "conf/rw/#",
                "rewritten/$1",
                "^conf/rw/(.*)$"
            )])
        );

        // The router owns the installed table: a publish rewrites.
        assert_eq!(
            state.router.rewrite_publish("conf/rw/temp"),
            Some("rewritten/temp".to_string())
        );
        assert_eq!(state.router.rewrite_publish("conf/other/temp"), None);

        let (status, reread) = read(list_rewrites(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, after);
    }

    #[tokio::test]
    async fn source_gate_blocks_non_matching_topics() {
        let state = standalone_state();
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!([rule(
                "all",
                "conf/rw/#",
                "rewritten/$1",
                "^conf/(.*)$"
            )]))
            .expect("update is JSON"),
        );
        let (status, after) = read(put_rewrites(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(after[0]["action"], serde_json::json!("all"));
        // The regex alone would match, but the source filter misses.
        assert_eq!(state.router.rewrite_publish("conf/other/x"), None);
        assert_eq!(
            state.router.rewrite_publish("conf/rw/x"),
            Some("rewritten/rw/x".to_string())
        );
    }

    #[tokio::test]
    async fn invalid_entries_rejected_without_applying() {
        let state = standalone_state();
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!([rule(
                "publish",
                "conf/rw-ok/#",
                "rewritten/$1",
                "^conf/rw-ok/(.*)$"
            )]))
            .expect("update is JSON"),
        );
        let (status, before) = read(put_rewrites(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::OK);

        for bad in [
            serde_json::json!([rule("bogus", "a/#", "b/$1", "^a/(.*)$")]),
            serde_json::json!([rule("publish", "", "b/$1", "^a/(.*)$")]),
            serde_json::json!([rule("publish", "sport/#/bogus", "b/$1", "^a/(.*)$")]),
            serde_json::json!([rule("publish", "a/#", "", "^a/(.*)$")]),
            serde_json::json!([rule("publish", "a/#", "b/$1", "")]),
            serde_json::json!([rule("publish", "a/#", "b/$1", "(unclosed")]),
            serde_json::json!([{"action": "publish"}]),
            serde_json::json!({"action": "publish"}),
            serde_json::json!("rules"),
        ] {
            let body = Bytes::from(serde_json::to_vec(&bad).expect("bad body is JSON"));
            let (status, err) = read(put_rewrites(State(state.clone()), body).await).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "body {bad} must fail");
            assert_eq!(err["code"], serde_json::json!("BAD_REQUEST"));
        }

        let (status, reread) = read(list_rewrites(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, before, "failed writes must not apply");
    }

    #[tokio::test]
    async fn too_many_rules_rejected_with_limit() {
        let state = standalone_state();
        let many: Vec<serde_json::Value> = (0..broker_router::MAX_REWRITE_RULES + 1)
            .map(|i| {
                rule(
                    "publish",
                    &format!("conf/rw-limit/{i}/#"),
                    "rewritten/$1",
                    &format!("^conf/rw-limit/{i}/(.*)$"),
                )
            })
            .collect();
        let body = Bytes::from(serde_json::to_vec(&many).expect("update is JSON"));
        let (status, err) = read(put_rewrites(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE);
        assert_eq!(err["code"], serde_json::json!("EXCEED_LIMIT"));

        let (status, reread) = read(list_rewrites(State(state.clone())).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reread, serde_json::json!([]));
    }

    #[tokio::test]
    async fn empty_array_clears_the_table() {
        let state = standalone_state();
        let body = Bytes::from(
            serde_json::to_vec(&serde_json::json!([rule(
                "publish",
                "conf/rw-clear/#",
                "rewritten/$1",
                "^conf/rw-clear/(.*)$"
            )]))
            .expect("update is JSON"),
        );
        let (status, _) = read(put_rewrites(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(state.router.rewrite_rule_count(), 1);

        let body = Bytes::from(serde_json::to_vec(&serde_json::json!([])).expect("empty is JSON"));
        let (status, after) = read(put_rewrites(State(state.clone()), body).await).await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(after, serde_json::json!([]));
        assert_eq!(state.router.rewrite_rule_count(), 0);
    }
}
