//! Feature availability read for the v5 management API.
//!
//! Covers `GET /features` (C13-15). The management console reads this
//! route to hide the pages of features that the broker does not have. The
//! answer has the shape that the reference API documents: `preset` (a
//! string), `enabled` and `disabled` (lists of feature names).
//!
//! The feature names are the names that the management console knows. The
//! answer states which of them this build provides. Whether a build
//! provides a feature does not change while the process runs, so the table
//! below is the truth for the life of the process. A feature is available
//! when the management routes that the console uses for it exist in this
//! build. The table lists the evidence for each name.
//!
//! The reference API names three presets. `full` means that every feature
//! is on. Any other set is `custom`. This build provides some features
//! only, so the preset is `custom`.
//!
//! Management-plane only: a read of a constant table. The route takes no
//! lock, and publish and deliver never touch it.

use axum::{http::StatusCode, response::IntoResponse, response::Response, Json};

/// One feature name of the management console and whether this build
/// provides it.
struct Feature {
    name: &'static str,
    available: bool,
}

/// The feature table. The names are the names in the feature gate of the
/// management console. Each line states why the feature is on or off.
const FEATURES: [Feature; 15] = [
    // Not available: the A2A registry and the AI routes are not part of
    // this broker.
    Feature {
        name: "ai",
        available: false,
    },
    // Not available: the broker has no cluster linking routes.
    Feature {
        name: "cluster_link",
        available: false,
    },
    // Available: the management console is part of this broker.
    Feature {
        name: "dashboard",
        available: true,
    },
    // Available: rules, connectors, actions and sources are routes of this
    // broker.
    Feature {
        name: "data_integration",
        available: true,
    },
    // Not available: the broker has no extension hook routes.
    Feature {
        name: "exhook",
        available: false,
    },
    // Not available: the broker has no file transfer routes.
    Feature {
        name: "file_transfer",
        available: false,
    },
    // Not available: the gateway routes are a placeholder in this build.
    Feature {
        name: "gateways",
        available: false,
    },
    // Available: message transformations are routes of this broker.
    Feature {
        name: "message_transformation",
        available: true,
    },
    // Not available: the broker has no topic metrics routes.
    Feature {
        name: "metrics",
        available: false,
    },
    // Available: automatic subscription, the retainer and slow
    // subscriptions are routes of this broker.
    Feature {
        name: "mqtt_extensions",
        available: true,
    },
    // Not available: the tenant routes are not part of this build.
    Feature {
        name: "multi_tenancy",
        available: false,
    },
    // Not available: the broker has no OpenTelemetry routes.
    Feature {
        name: "opentelemetry",
        available: false,
    },
    // Not available: the broker has no plugin routes.
    Feature {
        name: "plugins",
        available: false,
    },
    // Available: the schema registry is a route of this broker.
    Feature {
        name: "schema_registry",
        available: true,
    },
    // Not available: the broker has no schema validation routes.
    Feature {
        name: "schema_validation",
        available: false,
    },
];

/// The preset name for the current feature set: `full` when every feature
/// is on, otherwise `custom`.
fn preset_name() -> &'static str {
    if FEATURES.iter().all(|feature| feature.available) {
        "full"
    } else {
        "custom"
    }
}

/// `GET /features`: the preset, the enabled features and the disabled
/// features, as the reference API documents. Query parameters are ignored.
pub async fn list_features() -> Response {
    let names = |want: bool| -> Vec<&'static str> {
        FEATURES
            .iter()
            .filter(|feature| feature.available == want)
            .map(|feature| feature.name)
            .collect()
    };
    (
        StatusCode::OK,
        Json(serde_json::json!({
            "preset": preset_name(),
            "enabled": names(true),
            "disabled": names(false),
        })),
    )
        .into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn read() -> serde_json::Value {
        let response = list_features().await;
        assert_eq!(response.status(), StatusCode::OK);
        let bytes = axum::body::to_bytes(response.into_body(), 64 * 1024)
            .await
            .expect("features body is small and readable");
        serde_json::from_slice(&bytes).expect("features body is JSON")
    }

    #[tokio::test]
    async fn answer_has_the_documented_shape() {
        let body = read().await;
        assert!(body["preset"].is_string());
        let enabled = body["enabled"].as_array().expect("enabled is a list");
        let disabled = body["disabled"].as_array().expect("disabled is a list");
        assert_eq!(enabled.len() + disabled.len(), FEATURES.len());
        assert!(enabled.iter().chain(disabled).all(|name| name.is_string()));
    }

    #[tokio::test]
    async fn every_feature_is_in_exactly_one_list() {
        let body = read().await;
        let enabled: Vec<&str> = body["enabled"]
            .as_array()
            .expect("enabled")
            .iter()
            .filter_map(|name| name.as_str())
            .collect();
        let disabled: Vec<&str> = body["disabled"]
            .as_array()
            .expect("disabled")
            .iter()
            .filter_map(|name| name.as_str())
            .collect();
        for feature in &FEATURES {
            let on = enabled.contains(&feature.name);
            let off = disabled.contains(&feature.name);
            assert!(on != off, "{} is in exactly one list", feature.name);
            assert_eq!(on, feature.available, "{}", feature.name);
        }
        assert!(enabled.contains(&"dashboard"));
        assert!(enabled.contains(&"data_integration"));
    }

    #[tokio::test]
    async fn preset_is_custom_while_a_feature_is_off() {
        let body = read().await;
        let any_off = FEATURES.iter().any(|feature| !feature.available);
        let expected = if any_off { "custom" } else { "full" };
        assert_eq!(body["preset"], serde_json::json!(expected));
    }
}
