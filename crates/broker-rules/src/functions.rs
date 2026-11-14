//! Built-in rule functions of the reference API: `republish` and `console`.
//!
//! The management API documents a rule action as a stored-action id, a
//! built-in function (`republish`, `console`) or a user function. This
//! module holds the three parts the broker needs for the built-in ones:
//!
//! - the parser and validator for the documented argument shape,
//! - the projection back to that shape for the read routes,
//! - the renderer that turns one rule result row into one message.
//!
//! The renderer reuses [`broker_connectors::render_template_with`], the
//! scanner that all other `${...}` templates in the broker share. It adds
//! only the variable source: the fields of the rule result.
//!
//! Variables. A name is a dotted path, and a segment may be quoted with
//! single quotes (`pub_props.'User-Property'`). The path is first looked
//! up in the rule result. If the result has no such field, these names
//! fall back to the input message: `payload` (the whole result row,
//! which for `SELECT *` is the input payload), `payload.<path>` (a field
//! of the result row), `topic`, `qos` and `retain`.
//!
//! No lock and no allocation happens here for a message that matches no
//! rule: the engine calls this code only when a rule fired.

use bytes::Bytes;
use serde_json::Value;

use broker_config::{RepublishArgs, RepublishFlag, RepublishQos};
use broker_protocol::{QoS, Topic};

use crate::RuleAction;

/// Longest text the `console` function writes, in bytes.
pub const CONSOLE_LINE_LIMIT: usize = 1024;
/// Most user properties one republished message may carry.
pub const MAX_REPUBLISH_USER_PROPERTIES: usize = 16;
/// Longest key or value of a user property, in bytes.
pub const MAX_REPUBLISH_PROPERTY_LEN: usize = 1024;
/// Longest template text in the arguments, in bytes.
const MAX_TEMPLATE_LEN: usize = 64 * 1024;

const PROP_PAYLOAD_FORMAT: &str = "Payload-Format-Indicator";
const PROP_MESSAGE_EXPIRY: &str = "Message-Expiry-Interval";
/// MQTT properties of the reference API that the kernel publish path
/// cannot carry. A non-empty value is refused.
const UNSUPPORTED_PROPS: [&str; 3] = ["Content-Type", "Correlation-Data", "Response-Topic"];

/// Why a function item of the `actions` list was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FunctionActionError {
    /// A user provided function: this broker cannot load user code.
    Unavailable(String),
    /// The item does not follow the documented shape.
    Invalid(String),
}

/// MQTT v5 properties a republished message carries.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RepublishProperties {
    /// Payload format indicator, 0 or 1.
    pub payload_format: u8,
    /// Message expiry interval in seconds; 0 means unset.
    pub message_expiry: u32,
    /// User properties in order.
    pub user_properties: Vec<(String, String)>,
}

impl RepublishProperties {
    /// True when no property is set.
    pub fn is_empty(&self) -> bool {
        self.payload_format == 0 && self.message_expiry == 0 && self.user_properties.is_empty()
    }
}

/// One message that a `republish` function produced from a result row.
#[derive(Debug, Clone)]
pub struct RepublishMessage {
    /// Target topic after rendering.
    pub topic: Topic,
    /// Payload after rendering.
    pub payload: Bytes,
    /// QoS of the new message.
    pub qos: QoS,
    /// Retain flag of the new message.
    pub retain: bool,
    /// MQTT v5 properties.
    pub properties: RepublishProperties,
}

/// The input message of the rule result that the templates can name.
#[derive(Debug, Clone, Copy)]
pub struct InputMessage<'a> {
    /// Topic the rule matched.
    pub topic: &'a Topic,
    /// QoS of the input message.
    pub qos: QoS,
    /// Retain flag of the input message.
    pub retain: bool,
}

// ---------------------------------------------------------------------------
// Parsing and validation of the documented shape.
// ---------------------------------------------------------------------------

/// Parse one object item of the `actions` list that carries a `function`
/// key. `republish` and `console` become engine actions. A name in the
/// `module:function` form is a user function and is not available here.
pub fn parse_function_action(
    function: &str,
    args: Option<&Value>,
) -> Result<RuleAction, FunctionActionError> {
    match function {
        "console" => {
            match args {
                None => {}
                Some(Value::Object(_)) => {}
                Some(_) => {
                    return Err(FunctionActionError::Invalid(
                        "field `args` of the console function must be an object".to_string(),
                    ))
                }
            }
            Ok(RuleAction::Console)
        }
        "republish" => {
            let empty = Value::Object(serde_json::Map::new());
            let parsed = republish_args_from_json(args.unwrap_or(&empty))
                .map_err(FunctionActionError::Invalid)?;
            validate_republish_args(&parsed).map_err(FunctionActionError::Invalid)?;
            Ok(RuleAction::RepublishFn { args: parsed })
        }
        other if other.contains(':') => Err(FunctionActionError::Unavailable(format!(
            "function {other:?} is not available in this broker: it cannot load user code"
        ))),
        other => Err(FunctionActionError::Invalid(format!(
            "unknown function {other:?}: use \"republish\", \"console\" or \"module:function\""
        ))),
    }
}

/// Read the `args` object of the `republish` function with the documented
/// types. Defaults fill the absent keys. Unknown keys are refused.
pub fn republish_args_from_json(value: &Value) -> Result<RepublishArgs, String> {
    let Some(obj) = value.as_object() else {
        return Err("field `args` of the republish function must be an object".to_string());
    };
    const KNOWN: [&str; 7] = [
        "topic",
        "payload",
        "qos",
        "retain",
        "mqtt_properties",
        "user_properties",
        "direct_dispatch",
    ];
    for key in obj.keys() {
        if !KNOWN.contains(&key.as_str()) {
            return Err(format!(
                "unknown argument {key:?} of the republish function (field `args`)"
            ));
        }
    }
    let topic = match obj.get("topic") {
        Some(Value::String(text)) => text.clone(),
        Some(_) => return Err("argument `topic` must be a string (field `args`)".to_string()),
        None => return Err("argument `topic` is required (field `args`)".to_string()),
    };
    let mut args = RepublishArgs {
        topic,
        payload: "${payload}".to_string(),
        qos: RepublishQos::Template("${qos}".to_string()),
        retain: RepublishFlag::Template("${retain}".to_string()),
        mqtt_properties: Default::default(),
        user_properties: "${user_properties}".to_string(),
        direct_dispatch: RepublishFlag::Fixed(false),
    };
    if let Some(value) = obj.get("payload") {
        match value {
            Value::String(text) => args.payload = text.clone(),
            _ => return Err("argument `payload` must be a string (field `args`)".to_string()),
        }
    }
    if let Some(value) = obj.get("qos") {
        args.qos = match value {
            Value::String(text) => RepublishQos::Template(text.clone()),
            Value::Number(number) => match number.as_u64() {
                Some(n) if n <= 2 => RepublishQos::Fixed(n as u8),
                _ => {
                    return Err(
                        "argument `qos` must be 0, 1 or 2, or a template (field `args`)"
                            .to_string(),
                    )
                }
            },
            _ => {
                return Err(
                    "argument `qos` must be an integer or a string (field `args`)".to_string(),
                )
            }
        };
    }
    if let Some(value) = obj.get("retain") {
        args.retain = flag_from_json("retain", value)?;
    }
    if let Some(value) = obj.get("direct_dispatch") {
        args.direct_dispatch = flag_from_json("direct_dispatch", value)?;
    }
    if let Some(value) = obj.get("user_properties") {
        match value {
            Value::String(text) => args.user_properties = text.clone(),
            _ => {
                return Err("argument `user_properties` must be a string (field `args`)".to_string())
            }
        }
    }
    if let Some(value) = obj.get("mqtt_properties") {
        let Some(map) = value.as_object() else {
            return Err("argument `mqtt_properties` must be an object (field `args`)".to_string());
        };
        for (key, item) in map {
            match item {
                Value::String(text) => {
                    args.mqtt_properties.insert(key.clone(), text.clone());
                }
                _ => {
                    return Err(format!(
                        "mqtt_properties.{key} must be a string (field `args`)"
                    ))
                }
            }
        }
    }
    Ok(args)
}

fn flag_from_json(name: &str, value: &Value) -> Result<RepublishFlag, String> {
    match value {
        Value::Bool(flag) => Ok(RepublishFlag::Fixed(*flag)),
        Value::String(text) => Ok(RepublishFlag::Template(text.clone())),
        _ => Err(format!(
            "argument `{name}` must be a boolean or a string (field `args`)"
        )),
    }
}

/// Check the arguments of the `republish` function. A refusal names the
/// argument. Template syntax uses the shared scanner.
pub fn validate_republish_args(args: &RepublishArgs) -> Result<(), String> {
    if args.topic.trim().is_empty() {
        return Err("argument `topic` must not be empty (field `args`)".to_string());
    }
    for (name, text) in [
        ("topic", &args.topic),
        ("payload", &args.payload),
        ("user_properties", &args.user_properties),
    ] {
        if text.len() > MAX_TEMPLATE_LEN {
            return Err(format!(
                "argument `{name}` holds {} bytes, at most {MAX_TEMPLATE_LEN} (field `args`)",
                text.len()
            ));
        }
        check_template_syntax(name, text)?;
    }
    if !args.topic.contains("${") {
        Topic::validate(&args.topic).map_err(|e| format!("argument `topic` is not valid: {e}"))?;
    }
    match &args.qos {
        RepublishQos::Fixed(n) => {
            if *n > 2 {
                return Err("argument `qos` must be 0, 1 or 2 (field `args`)".to_string());
            }
        }
        RepublishQos::Template(text) => {
            check_template_syntax("qos", text)?;
            if !text.contains("${") && !matches!(text.trim(), "0" | "1" | "2") {
                return Err(
                    "argument `qos` must be 0, 1 or 2, or a template (field `args`)".to_string(),
                );
            }
        }
    }
    check_flag("retain", &args.retain)?;
    check_flag("direct_dispatch", &args.direct_dispatch)?;
    if !args.user_properties.trim().is_empty()
        && single_placeholder(&args.user_properties).is_none()
    {
        return Err(
            "argument `user_properties` must be one reference such as ${user_properties} (field `args`)"
                .to_string(),
        );
    }
    for (key, text) in &args.mqtt_properties {
        let key = key.as_str();
        if UNSUPPORTED_PROPS.contains(&key) {
            if text.is_empty() {
                continue;
            }
            return Err(format!(
                "mqtt_properties.{key} is not supported by this broker (field `args`)"
            ));
        }
        if key != PROP_PAYLOAD_FORMAT && key != PROP_MESSAGE_EXPIRY {
            return Err(format!(
                "unknown MQTT property {key:?} in mqtt_properties (field `args`)"
            ));
        }
        if text.len() > MAX_TEMPLATE_LEN {
            return Err(format!("mqtt_properties.{key} is too long (field `args`)"));
        }
        check_template_syntax(key, text)?;
        if !text.contains("${") && !text.is_empty() {
            if key == PROP_PAYLOAD_FORMAT && !matches!(text.as_str(), "0" | "1") {
                return Err(format!(
                    "mqtt_properties.{key} must be 0 or 1 (field `args`)"
                ));
            }
            if key == PROP_MESSAGE_EXPIRY && text.parse::<u32>().is_err() {
                return Err(format!(
                    "mqtt_properties.{key} must be a number of seconds (field `args`)"
                ));
            }
        }
    }
    Ok(())
}

fn check_flag(name: &str, flag: &RepublishFlag) -> Result<(), String> {
    if let RepublishFlag::Template(text) = flag {
        check_template_syntax(name, text)?;
        if !text.contains("${") && !matches!(text.trim(), "true" | "false") {
            return Err(format!(
                "argument `{name}` must be true or false, or a template (field `args`)"
            ));
        }
    }
    Ok(())
}

/// Run the shared scanner with a lookup that accepts every name. It
/// reports an unclosed or empty `${}`.
fn check_template_syntax(name: &str, text: &str) -> Result<(), String> {
    broker_connectors::render_template_with(text, |_| Some(String::new()))
        .map(|_| ())
        .map_err(|e| format!("argument `{name}` is not a valid template: {e} (field `args`)"))
}

/// The variable name when the whole text is one `${name}` reference.
fn single_placeholder(text: &str) -> Option<&str> {
    let inner = text.trim().strip_prefix("${")?.strip_suffix('}')?;
    if inner.is_empty() || inner.contains('}') || inner.contains("${") {
        return None;
    }
    Some(inner)
}

// ---------------------------------------------------------------------------
// Projection back to the documented shape.
// ---------------------------------------------------------------------------

/// Render one stored action in the shape the documented API reads an
/// action forward: a string carrying the action id. A v5 action-owned
/// sink registers under the reserved `action:{id}` dispatch key; reads
/// strip the prefix back so the API keeps naming the bare action id (a
/// v1 plain connector id passes through unchanged).
pub fn action_to_api_json(action: &RuleAction) -> Value {
    match action {
        RuleAction::ForwardConnector { connector_id } => Value::String(
            broker_config::v5_action_sink_id(connector_id)
                .unwrap_or(connector_id)
                .to_string(),
        ),
        RuleAction::Console | RuleAction::Log => serde_json::json!({ "function": "console" }),
        RuleAction::RepublishFn { args } => serde_json::json!({
            "function": "republish",
            "args": serde_json::to_value(args).unwrap_or(Value::Null),
        }),
        RuleAction::Republish { topic, qos } => serde_json::json!({
            "function": "republish",
            "args": {
                "topic": topic.as_str(),
                "payload": "${payload}",
                "qos": u8::from(*qos),
                "retain": false,
                "mqtt_properties": {},
                "user_properties": "",
                "direct_dispatch": false,
            },
        }),
    }
}

// ---------------------------------------------------------------------------
// Rendering one result row.
// ---------------------------------------------------------------------------

/// The text the `console` function writes for one result row: lossy
/// UTF-8, at most [`CONSOLE_LINE_LIMIT`] bytes, on one line.
pub fn console_text(result: &[u8]) -> String {
    let text = String::from_utf8_lossy(result);
    let mut end = text.len().min(CONSOLE_LINE_LIMIT);
    while !text.is_char_boundary(end) {
        end -= 1;
    }
    let mut line: String = text[..end]
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect();
    if end < text.len() {
        line.push_str("...");
    }
    line
}

/// Variable source over one result row.
struct RowVars<'a> {
    raw: &'a [u8],
    json: std::cell::OnceCell<Option<Value>>,
    input: InputMessage<'a>,
}

impl<'a> RowVars<'a> {
    fn new(raw: &'a [u8], input: InputMessage<'a>) -> Self {
        Self {
            raw,
            json: std::cell::OnceCell::new(),
            input,
        }
    }

    /// The result row as a JSON object, parsed once.
    fn object(&self) -> Option<&Value> {
        self.json
            .get_or_init(|| match serde_json::from_slice::<Value>(self.raw) {
                Ok(value @ Value::Object(_)) => Some(value),
                _ => None,
            })
            .as_ref()
    }

    /// The value a name stands for, as JSON.
    fn value(&self, name: &str) -> Option<Value> {
        let path = split_path(name)?;
        let first = *path.first()?;
        let object = self.object();
        if let Some(found) = object.and_then(|o| walk(o, &path)) {
            return Some(found.clone());
        }
        match first {
            "payload" => {
                let has_field = object
                    .and_then(Value::as_object)
                    .is_some_and(|o| o.contains_key("payload"));
                if has_field {
                    return None;
                }
                if path.len() == 1 {
                    return Some(Value::String(
                        String::from_utf8_lossy(self.raw).into_owned(),
                    ));
                }
                object.and_then(|o| walk(o, &path[1..])).cloned()
            }
            "topic" if path.len() == 1 => {
                Some(Value::String(self.input.topic.as_str().to_string()))
            }
            "qos" if path.len() == 1 => Some(Value::from(u8::from(self.input.qos))),
            "retain" if path.len() == 1 => Some(Value::Bool(self.input.retain)),
            _ => None,
        }
    }

    fn text(&self, name: &str) -> Option<String> {
        self.value(name).map(|v| text_of(&v))
    }
}

/// Split a variable name at the dots outside single quotes.
fn split_path(name: &str) -> Option<Vec<&str>> {
    let mut parts = Vec::new();
    let mut start = 0;
    let mut quoted = false;
    for (index, ch) in name.char_indices() {
        match ch {
            '\'' => quoted = !quoted,
            '.' if !quoted => {
                parts.push(unquote(&name[start..index]));
                start = index + 1;
            }
            _ => {}
        }
    }
    if quoted {
        return None;
    }
    parts.push(unquote(&name[start..]));
    if parts.iter().any(|p| p.is_empty()) {
        return None;
    }
    Some(parts)
}

fn unquote(segment: &str) -> &str {
    segment
        .strip_prefix('\'')
        .and_then(|s| s.strip_suffix('\''))
        .unwrap_or(segment)
}

/// Follow a path through objects and, for a numeric segment, arrays.
fn walk<'v>(root: &'v Value, path: &[&str]) -> Option<&'v Value> {
    let mut current = root;
    for segment in path {
        current = match current {
            Value::Object(map) => map.get(*segment)?,
            Value::Array(items) => items.get(segment.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(current)
}

/// The text of one JSON value as a template inserts it: a string as is,
/// anything else as JSON.
fn text_of(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        other => other.to_string(),
    }
}

/// Render the `republish` arguments for one result row.
///
/// A missing variable in `payload` renders as `undefined`. A missing
/// variable in `topic` is an error, so a message never goes to a topic
/// that holds the word `undefined`. A missing `qos` gives 0 and a missing
/// `retain` gives false.
pub fn render_republish(
    args: &RepublishArgs,
    result: &Bytes,
    input: InputMessage<'_>,
) -> Result<RepublishMessage, String> {
    let vars = RowVars::new(result, input);

    let topic_text = broker_connectors::render_template_with(&args.topic, |name| vars.text(name))
        .map_err(|e| format!("topic template: {e}"))?;
    let topic = Topic::new(topic_text).map_err(|e| format!("rendered topic is not valid: {e}"))?;

    // The default payload template is the result row itself. Skip the
    // scanner and the JSON parse for it.
    let payload = if args.payload == "${payload}" && !row_has_payload_field(&vars) {
        result.clone()
    } else {
        let text = broker_connectors::render_template_with(&args.payload, |name| {
            Some(vars.text(name).unwrap_or_else(|| "undefined".to_string()))
        })
        .map_err(|e| format!("payload template: {e}"))?;
        Bytes::from(text.into_bytes())
    };

    let qos = match &args.qos {
        RepublishQos::Fixed(n) => QoS::try_from(*n).unwrap_or(QoS::AtMostOnce),
        RepublishQos::Template(text) => {
            let rendered = broker_connectors::render_template_with(text, |name| {
                Some(vars.text(name).unwrap_or_default())
            })
            .map_err(|e| format!("qos template: {e}"))?;
            rendered
                .trim()
                .parse::<u8>()
                .ok()
                .and_then(|n| QoS::try_from(n).ok())
                .unwrap_or(QoS::AtMostOnce)
        }
    };

    let direct = resolve_flag(&args.direct_dispatch, &vars, "direct_dispatch")?;
    // A direct dispatch message is not retained.
    let retain = !direct && resolve_flag(&args.retain, &vars, "retain")?;

    let mut properties = RepublishProperties::default();
    for (key, text) in &args.mqtt_properties {
        if text.is_empty() || UNSUPPORTED_PROPS.contains(&key.as_str()) {
            continue;
        }
        let rendered = broker_connectors::render_template_with(text, |name| {
            Some(vars.text(name).unwrap_or_default())
        })
        .map_err(|e| format!("{key} template: {e}"))?;
        let rendered = rendered.trim();
        if rendered.is_empty() {
            continue;
        }
        if key == PROP_PAYLOAD_FORMAT {
            properties.payload_format = match rendered {
                "0" => 0,
                "1" => 1,
                _ => return Err(format!("{key} must be 0 or 1, got {rendered:?}")),
            };
        } else if key == PROP_MESSAGE_EXPIRY {
            properties.message_expiry = rendered
                .parse::<u32>()
                .map_err(|_| format!("{key} must be a number of seconds, got {rendered:?}"))?;
        }
    }
    if let Some(name) = single_placeholder(&args.user_properties) {
        if let Some(Value::Object(map)) = vars.value(name) {
            for (key, value) in map {
                if value.is_null() {
                    continue;
                }
                let value = text_of(&value);
                if key.is_empty()
                    || key.len() > MAX_REPUBLISH_PROPERTY_LEN
                    || value.len() > MAX_REPUBLISH_PROPERTY_LEN
                {
                    return Err("a user property is empty or too long".to_string());
                }
                properties.user_properties.push((key, value));
            }
            if properties.user_properties.len() > MAX_REPUBLISH_USER_PROPERTIES {
                return Err(format!(
                    "more than {MAX_REPUBLISH_USER_PROPERTIES} user properties"
                ));
            }
        }
    }

    Ok(RepublishMessage {
        topic,
        payload,
        qos,
        retain,
        properties,
    })
}

/// True when the result row is a JSON object with its own `payload` key.
fn row_has_payload_field(vars: &RowVars<'_>) -> bool {
    // A row that does not start with `{` cannot hold a key, so the
    // parse is skipped for raw payloads.
    vars.raw.first() == Some(&b'{')
        && vars
            .object()
            .and_then(Value::as_object)
            .is_some_and(|o| o.contains_key("payload"))
}

fn resolve_flag(flag: &RepublishFlag, vars: &RowVars<'_>, name: &str) -> Result<bool, String> {
    match flag {
        RepublishFlag::Fixed(value) => Ok(*value),
        RepublishFlag::Template(text) => {
            let rendered = broker_connectors::render_template_with(text, |var| {
                Some(vars.text(var).unwrap_or_default())
            })
            .map_err(|e| format!("{name} template: {e}"))?;
            Ok(rendered.trim() == "true")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn input(topic: &Topic, qos: QoS, retain: bool) -> InputMessage<'_> {
        InputMessage { topic, qos, retain }
    }

    fn args(value: Value) -> RepublishArgs {
        let parsed = republish_args_from_json(&value).expect("args parse");
        validate_republish_args(&parsed).expect("args validate");
        parsed
    }

    #[test]
    fn defaults_fill_in_and_pass_the_row_through() {
        let topic = Topic::new("in/a").unwrap();
        let a = args(json!({"topic": "out/a"}));
        assert_eq!(a.payload, "${payload}");
        let row = Bytes::from_static(br#"{"v":1}"#);
        let message = render_republish(&a, &row, input(&topic, QoS::AtLeastOnce, true)).unwrap();
        assert_eq!(message.topic.as_str(), "out/a");
        assert_eq!(message.payload, row);
        assert_eq!(message.qos, QoS::AtLeastOnce);
        assert!(message.retain);
    }

    #[test]
    fn templates_read_result_fields_and_input() {
        let topic = Topic::new("in/a").unwrap();
        let a = args(json!({
            "topic": "out/${device.id}/${topic}",
            "payload": "t=${temp} d=${device.id} p=${payload.temp}",
            "qos": "${lvl}",
            "retain": "${keep}",
        }));
        let row = Bytes::from_static(br#"{"temp":21.5,"device":{"id":"d7"},"lvl":2,"keep":true}"#);
        let m = render_republish(&a, &row, input(&topic, QoS::AtMostOnce, false)).unwrap();
        assert_eq!(m.topic.as_str(), "out/d7/in/a");
        assert_eq!(m.payload, Bytes::from_static(b"t=21.5 d=d7 p=21.5"));
        assert_eq!(m.qos, QoS::ExactlyOnce);
        assert!(m.retain);
    }

    #[test]
    fn missing_payload_variable_is_undefined_but_topic_fails() {
        let topic = Topic::new("in/a").unwrap();
        let row = Bytes::from_static(br#"{"v":1}"#);
        let a = args(json!({"topic": "out", "payload": "x=${nope}"}));
        let m = render_republish(&a, &row, input(&topic, QoS::AtMostOnce, false)).unwrap();
        assert_eq!(m.payload, Bytes::from_static(b"x=undefined"));
        let a = args(json!({"topic": "out/${nope}"}));
        assert!(render_republish(&a, &row, input(&topic, QoS::AtMostOnce, false)).is_err());
    }

    #[test]
    fn rendered_topic_with_wildcard_is_refused() {
        let topic = Topic::new("in/a").unwrap();
        let a = args(json!({"topic": "out/${f}"}));
        let row = Bytes::from_static(br#"{"f":"+"}"#);
        assert!(render_republish(&a, &row, input(&topic, QoS::AtMostOnce, false)).is_err());
    }

    #[test]
    fn direct_dispatch_clears_retain_and_user_properties_are_read() {
        let topic = Topic::new("in/a").unwrap();
        let a = args(json!({
            "topic": "out",
            "retain": true,
            "direct_dispatch": true,
            "mqtt_properties": {"Message-Expiry-Interval": "30", "Payload-Format-Indicator": "1"},
            "user_properties": "${props}",
        }));
        let row = Bytes::from_static(br#"{"props":{"k":"v","n":3}}"#);
        let m = render_republish(&a, &row, input(&topic, QoS::AtMostOnce, false)).unwrap();
        assert!(!m.retain);
        assert_eq!(m.properties.message_expiry, 30);
        assert_eq!(m.properties.payload_format, 1);
        assert_eq!(
            m.properties.user_properties,
            vec![
                ("k".to_string(), "v".to_string()),
                ("n".to_string(), "3".to_string())
            ]
        );
    }

    #[test]
    fn quoted_path_segments_are_one_key() {
        let topic = Topic::new("in/a").unwrap();
        let a = args(json!({"topic": "out", "payload": "${a.'b.c'}"}));
        let row = Bytes::from_static(br#"{"a":{"b.c":"hit"}}"#);
        let m = render_republish(&a, &row, input(&topic, QoS::AtMostOnce, false)).unwrap();
        assert_eq!(m.payload, Bytes::from_static(b"hit"));
    }

    #[test]
    fn unsupported_and_bad_arguments_are_refused() {
        for bad in [
            json!({}),
            json!({"topic": ""}),
            json!({"topic": "a/#"}),
            json!({"topic": "a", "qos": 3}),
            json!({"topic": "a", "qos": "five"}),
            json!({"topic": "a", "retain": "maybe"}),
            json!({"topic": "a", "extra": 1}),
            json!({"topic": "a", "payload": "${open"}),
            json!({"topic": "a", "mqtt_properties": {"Content-Type": "text/plain"}}),
            json!({"topic": "a", "mqtt_properties": {"Bogus": "x"}}),
            json!({"topic": "a", "user_properties": "plain text"}),
        ] {
            let outcome = republish_args_from_json(&bad).and_then(|a| validate_republish_args(&a));
            assert!(outcome.is_err(), "{bad} must be refused");
        }
        // The empty forms the management console sends are accepted.
        let ok = json!({
            "topic": "a", "qos": 0, "retain": false, "payload": "${payload}",
            "mqtt_properties": {}, "user_properties": "", "direct_dispatch": false
        });
        let parsed = republish_args_from_json(&ok).unwrap();
        validate_republish_args(&parsed).unwrap();
    }

    #[test]
    fn function_names_are_sorted_out() {
        assert!(matches!(
            parse_function_action("console", None),
            Ok(RuleAction::Console)
        ));
        assert!(matches!(
            parse_function_action("mod:fun", Some(&json!({}))),
            Err(FunctionActionError::Unavailable(_))
        ));
        assert!(matches!(
            parse_function_action("nothing", None),
            Err(FunctionActionError::Invalid(_))
        ));
    }

    #[test]
    fn console_text_is_one_bounded_line() {
        let long = "x".repeat(CONSOLE_LINE_LIMIT * 2);
        let text = console_text(long.as_bytes());
        assert_eq!(text.len(), CONSOLE_LINE_LIMIT + 3);
        assert_eq!(console_text(b"a\nb"), "a b");
        let wide = "\u{4e2d}".repeat(CONSOLE_LINE_LIMIT);
        assert!(console_text(wide.as_bytes()).ends_with("..."));
    }
}
