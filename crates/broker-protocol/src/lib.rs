/// MQTT 5 topic-alias helpers (B4-05, T-92). The alias tables themselves
/// live in the kernel session (`broker-session`); the edge only frames bytes.
pub mod v5;
pub mod wire;

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Error, Debug, PartialEq, Eq)]
pub enum ProtocolError {
    #[error("Invalid QoS level: {0}")]
    InvalidQoS(u8),

    #[error("Invalid topic: {0}")]
    InvalidTopic(String),

    #[error("Topic contains empty level")]
    EmptyTopicLevel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum QoS {
    AtMostOnce = 0,
    AtLeastOnce = 1,
    ExactlyOnce = 2,
}

impl TryFrom<u8> for QoS {
    type Error = ProtocolError;

    fn try_from(val: u8) -> Result<Self, Self::Error> {
        match val {
            0 => Ok(Self::AtMostOnce),
            1 => Ok(Self::AtLeastOnce),
            2 => Ok(Self::ExactlyOnce),
            other => Err(ProtocolError::InvalidQoS(other)),
        }
    }
}

impl From<QoS> for u8 {
    fn from(qos: QoS) -> Self {
        qos as u8
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Topic(String);

impl Topic {
    pub fn new(topic: impl Into<String>) -> Result<Self, ProtocolError> {
        let t = topic.into();
        Self::validate(&t)?;
        Ok(Self(t))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn validate(topic: &str) -> Result<(), ProtocolError> {
        if topic.is_empty() {
            return Err(ProtocolError::InvalidTopic(
                "topic cannot be empty".to_string(),
            ));
        }
        if topic.contains('+') || topic.contains('#') {
            return Err(ProtocolError::InvalidTopic(
                "concrete topic cannot contain wildcards".to_string(),
            ));
        }
        Ok(())
    }
}

impl std::fmt::Display for Topic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TopicFilter(String);

impl TopicFilter {
    pub fn new(filter: impl Into<String>) -> Result<Self, ProtocolError> {
        let f = filter.into();
        Self::validate(&f)?;
        Ok(Self(f))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn validate(filter: &str) -> Result<(), ProtocolError> {
        if filter.is_empty() {
            return Err(ProtocolError::InvalidTopic(
                "filter cannot be empty".to_string(),
            ));
        }
        // Multi-level wildcard '#' can only appear as the last character or preceded by '/'
        if let Some(pos) = filter.find('#') {
            if pos != filter.len() - 1 {
                return Err(ProtocolError::InvalidTopic(
                    "'#' wildcard must be the final level".to_string(),
                ));
            }
            if pos > 0 && filter.as_bytes()[pos - 1] != b'/' {
                return Err(ProtocolError::InvalidTopic(
                    "'#' wildcard must be prefixed with '/'".to_string(),
                ));
            }
        }
        Ok(())
    }

    /// Whether this filter matches one topic.
    ///
    /// A `#` or `+` at the first level never matches a `$` topic
    /// (MQTT 5.0 section 4.7.2). A `$` filter matches a `$` topic.
    pub fn matches(&self, topic: &Topic) -> bool {
        let filter_levels: Vec<&str> = self.0.split('/').collect();
        // A wildcard head never reaches `$` topics.
        if topic.as_str().starts_with('$') {
            if let Some(&"#" | &"+") = filter_levels.first() {
                return false;
            }
        }
        Self::match_levels(&filter_levels, topic.as_str())
    }

    /// Whether this filter matches one topic on the rule path.
    ///
    /// The rule path ignores the `$` guard. The engine flag gates
    /// `$SYS` topics. Other paths keep `matches`.
    pub fn matches_for_rule_engine(&self, topic: &Topic) -> bool {
        let filter_levels: Vec<&str> = self.0.split('/').collect();
        Self::match_levels(&filter_levels, topic.as_str())
    }

    /// Level match without the `$` guard. Both callers share it.
    fn match_levels(filter_levels: &[&str], topic_str: &str) -> bool {
        let topic_levels: Vec<&str> = topic_str.split('/').collect();
        let mut t_idx = 0;
        for (f_idx, &f_part) in filter_levels.iter().enumerate() {
            if f_part == "#" {
                return true;
            }

            if t_idx >= topic_levels.len() {
                return false;
            }

            if f_part == "+" || f_part == topic_levels[t_idx] {
                t_idx += 1;
            } else {
                return false;
            }

            if f_idx == filter_levels.len() - 1 && t_idx < topic_levels.len() {
                return false;
            }
        }

        t_idx == topic_levels.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_qos_conversion() {
        assert_eq!(QoS::try_from(0).unwrap(), QoS::AtMostOnce);
        assert_eq!(QoS::try_from(1).unwrap(), QoS::AtLeastOnce);
        assert_eq!(QoS::try_from(2).unwrap(), QoS::ExactlyOnce);
        assert!(QoS::try_from(3).is_err());
    }

    #[test]
    fn test_topic_matching() {
        let t1 = Topic::new("sensors/temperature/living_room").unwrap();
        let f1 = TopicFilter::new("sensors/temperature/+").unwrap();
        let f2 = TopicFilter::new("sensors/#").unwrap();
        let f3 = TopicFilter::new("sensors/humidity/+").unwrap();
        let f4 = TopicFilter::new("#").unwrap();

        assert!(f1.matches(&t1));
        assert!(f2.matches(&t1));
        assert!(!f3.matches(&t1));
        assert!(f4.matches(&t1));
    }
}

#[cfg(test)]
mod utf8_filter_tests {
    use super::TopicFilter;
    #[test]
    fn topic_filter_rejects_multibyte_utf8_before_hash_without_panicking() {
        assert!(TopicFilter::new("sensors/é#").is_err());
        assert!(TopicFilter::new("a/🔥#").is_err());
    }
}
