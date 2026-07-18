//! Local publish/subscribe (component-to-component) shapes.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Contextual information about a received message (`aws.greengrass#MessageContext`).
#[derive(Debug, Clone, Deserialize)]
pub struct MessageContext {
    /// The topic the message was published to.
    #[serde(rename = "topic")]
    pub topic: String,
}

/// A binary local pub/sub message (`aws.greengrass#BinaryMessage`).
#[derive(Debug, Clone)]
pub struct BinaryMessage {
    /// The raw message bytes.
    pub message: Vec<u8>,
    /// The context of the message (topic).
    pub context: Option<MessageContext>,
}

/// A JSON local pub/sub message (`aws.greengrass#JsonMessage`).
#[derive(Debug, Clone, Deserialize)]
pub struct JsonMessage {
    /// The JSON message object.
    #[serde(rename = "message", default)]
    pub message: Option<Value>,
    /// The context of the message (topic).
    #[serde(rename = "context", default)]
    pub context: Option<MessageContext>,
}

/// A message to publish to a local topic (`aws.greengrass#PublishMessage`, a tagged union). Set
/// exactly one variant.
#[derive(Debug, Clone, Serialize)]
pub struct PublishMessage {
    /// A JSON message.
    #[serde(rename = "jsonMessage", skip_serializing_if = "Option::is_none")]
    pub json_message: Option<JsonMessageOut>,
    /// A binary message (base64-encoded on the wire).
    #[serde(rename = "binaryMessage", skip_serializing_if = "Option::is_none")]
    pub binary_message: Option<BinaryMessageOut>,
}

/// Outbound JSON message content.
#[derive(Debug, Clone, Serialize)]
pub struct JsonMessageOut {
    /// The JSON message object.
    #[serde(rename = "message")]
    pub message: Value,
}

/// Outbound binary message content (serialized as base64).
#[derive(Debug, Clone)]
pub struct BinaryMessageOut {
    /// The raw message bytes.
    pub message: Vec<u8>,
}

impl Serialize for BinaryMessageOut {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use base64::Engine as _;
        use serde::ser::SerializeStruct as _;
        let encoded = base64::engine::general_purpose::STANDARD.encode(&self.message);
        let mut s = serializer.serialize_struct("BinaryMessage", 1)?;
        s.serialize_field("message", &encoded)?;
        s.end()
    }
}

/// Request for `PublishToTopic`.
#[derive(Debug, Clone, Serialize)]
pub struct PublishToTopicRequest {
    /// The topic to publish to.
    #[serde(rename = "topic")]
    pub topic: String,
    /// The message to publish.
    #[serde(rename = "publishMessage")]
    pub publish_message: PublishMessage,
}

/// Response for `PublishToTopic` (empty).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PublishToTopicResponse {}

/// Request for `SubscribeToTopic`.
#[derive(Debug, Clone, Serialize)]
pub struct SubscribeToTopicRequest {
    /// The topic (filter) to subscribe to.
    #[serde(rename = "topic")]
    pub topic: String,
    /// Whether to receive messages published by this component too.
    #[serde(rename = "receiveMode", skip_serializing_if = "Option::is_none")]
    pub receive_mode: Option<String>,
}

/// Response for `SubscribeToTopic`.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SubscribeToTopicResponse {
    /// The topic the subscription was established for.
    #[serde(rename = "topicName", default)]
    pub topic_name: Option<String>,
}

/// A streamed local pub/sub message (`aws.greengrass#SubscriptionResponseMessage`, a tagged union).
#[derive(Debug, Clone, Deserialize)]
pub struct SubscriptionResponseMessage {
    /// A JSON message.
    #[serde(rename = "jsonMessage", default)]
    pub json_message: Option<JsonMessage>,
    /// A binary message (base64-decoded from the wire).
    #[serde(rename = "binaryMessage", default)]
    pub binary_message: Option<BinaryMessageIn>,
}

/// Inbound binary message content (base64-decoded from the wire).
#[derive(Debug, Clone)]
pub struct BinaryMessageIn {
    /// The raw message bytes.
    pub message: Vec<u8>,
    /// The context of the message (topic).
    pub context: Option<MessageContext>,
}

impl<'de> Deserialize<'de> for BinaryMessageIn {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use base64::Engine as _;

        #[derive(Deserialize)]
        struct Raw {
            #[serde(default)]
            message: Option<String>,
            #[serde(default)]
            context: Option<MessageContext>,
        }

        let raw = Raw::deserialize(deserializer)?;
        let message = match raw.message {
            Some(b64) => base64::engine::general_purpose::STANDARD
                .decode(b64.as_bytes())
                .map_err(serde::de::Error::custom)?,
            None => Vec::new(),
        };
        Ok(BinaryMessageIn {
            message,
            context: raw.context,
        })
    }
}
