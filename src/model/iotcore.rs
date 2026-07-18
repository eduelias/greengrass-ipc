//! AWS IoT Core MQTT pub/sub shapes (`PublishToIoTCore` / `SubscribeToIoTCore`).
//!
//! These reuse the nucleus's own MQTT connection to AWS IoT Core, so a component
//! needs no device certificate of its own — only an `aws.greengrass.ipc.mqttproxy`
//! authorization policy for the topics it uses. Field names and JSON encoding
//! match the AWS model (`greengrasscoreipc/model.py`).

use serde::{Deserialize, Serialize};

/// MQTT quality of service (`aws.greengrass#QOS`).
///
/// On the wire the Greengrass IPC service encodes QoS as the **string form of the
/// integer** (`"0"` / `"1"`), matching the AWS SDKs — not the symbolic name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QoS {
    /// QoS 0 — delivered at most once.
    AtMostOnce,
    /// QoS 1 — delivered at least once.
    AtLeastOnce,
}

impl Serialize for QoS {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let s = match self {
            QoS::AtMostOnce => "0",
            QoS::AtLeastOnce => "1",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> Deserialize<'de> for QoS {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "0" => Ok(QoS::AtMostOnce),
            "1" => Ok(QoS::AtLeastOnce),
            other => Err(serde::de::Error::custom(format!("invalid QoS: {other}"))),
        }
    }
}

/// Request for `PublishToIoTCore`.
#[derive(Debug, Clone, Serialize)]
pub struct PublishToIoTCoreRequest {
    /// The topic to publish to.
    #[serde(rename = "topicName")]
    pub topic_name: String,
    /// The MQTT QoS to use.
    #[serde(rename = "qos")]
    pub qos: QoS,
    /// The message payload (base64-encoded blob on the wire).
    #[serde(rename = "payload", skip_serializing_if = "Option::is_none")]
    pub payload: Option<Blob>,
}

/// A binary blob that serializes to / deserializes from base64 (the Smithy `Blob`
/// wire encoding used by Greengrass IPC).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Blob(pub Vec<u8>);

impl Serialize for Blob {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use base64::Engine as _;
        let encoded = base64::engine::general_purpose::STANDARD.encode(&self.0);
        serializer.serialize_str(&encoded)
    }
}

impl<'de> Deserialize<'de> for Blob {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use base64::Engine as _;
        let s = String::deserialize(deserializer)?;
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(s.as_bytes())
            .map_err(serde::de::Error::custom)?;
        Ok(Blob(bytes))
    }
}

/// Response for `PublishToIoTCore` (empty).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PublishToIoTCoreResponse {}

/// Request for `SubscribeToIoTCore`.
#[derive(Debug, Clone, Serialize)]
pub struct SubscribeToIoTCoreRequest {
    /// The topic or topic filter to subscribe to (MQTT wildcards allowed).
    #[serde(rename = "topicName")]
    pub topic_name: String,
    /// The MQTT QoS to use.
    #[serde(rename = "qos")]
    pub qos: QoS,
}

/// Response for `SubscribeToIoTCore` (empty; messages arrive as a stream).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SubscribeToIoTCoreResponse {}

/// A single MQTT message (`aws.greengrass#MQTTMessage`).
#[derive(Debug, Clone, Deserialize)]
pub struct MqttMessage {
    /// The topic the message was published to.
    #[serde(rename = "topicName")]
    pub topic_name: String,
    /// The message payload (base64-decoded from the wire).
    #[serde(rename = "payload", default)]
    pub payload: Option<Blob>,
}

/// A streamed IoT Core message event (`aws.greengrass#IoTCoreMessage`).
#[derive(Debug, Clone, Deserialize)]
pub struct IoTCoreMessage {
    /// The MQTT message.
    #[serde(rename = "message", default)]
    pub message: Option<MqttMessage>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publish_request_serializes_with_base64_payload() {
        let req = PublishToIoTCoreRequest {
            topic_name: "test/topic".into(),
            qos: QoS::AtLeastOnce,
            payload: Some(Blob(b"hi".to_vec())),
        };
        let v: serde_json::Value = serde_json::to_value(&req).unwrap();
        assert_eq!(v["topicName"], "test/topic");
        assert_eq!(v["qos"], "1");
        assert_eq!(v["payload"], "aGk="); // base64("hi")
    }

    #[test]
    fn iotcore_message_deserializes_base64_payload() {
        let raw = r#"{"message":{"topicName":"a/b","payload":"aGk="}}"#;
        let m: IoTCoreMessage = serde_json::from_str(raw).unwrap();
        let msg = m.message.expect("message");
        assert_eq!(msg.topic_name, "a/b");
        assert_eq!(msg.payload.unwrap().0, b"hi");
    }

    #[test]
    fn publish_omits_absent_payload() {
        let req = PublishToIoTCoreRequest {
            topic_name: "t".into(),
            qos: QoS::AtMostOnce,
            payload: None,
        };
        let s = serde_json::to_string(&req).unwrap();
        assert_eq!(s, r#"{"topicName":"t","qos":"0"}"#);
    }
}
