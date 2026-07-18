//! Component configuration shapes.

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Request for `GetConfiguration`.
#[derive(Debug, Clone, Serialize)]
pub struct GetConfigurationRequest {
    /// The name of the component to read (defaults to this component if `None`).
    #[serde(rename = "componentName", skip_serializing_if = "Option::is_none")]
    pub component_name: Option<String>,
    /// The key path to the configuration value. Empty reads the whole configuration.
    #[serde(rename = "keyPath")]
    pub key_path: Vec<String>,
}

/// Response for `GetConfiguration`.
#[derive(Debug, Clone, Deserialize)]
pub struct GetConfigurationResponse {
    /// The name of the component whose configuration was read.
    #[serde(rename = "componentName", default)]
    pub component_name: Option<String>,
    /// The requested configuration value as a JSON object.
    #[serde(rename = "value", default)]
    pub value: Option<Value>,
}

/// Request for `UpdateConfiguration`.
#[derive(Debug, Clone, Serialize)]
pub struct UpdateConfigurationRequest {
    /// The key path to update. Empty updates the root.
    #[serde(rename = "keyPath", skip_serializing_if = "Option::is_none")]
    pub key_path: Option<Vec<String>>,
    /// The timestamp of the update, in epoch milliseconds.
    #[serde(rename = "timestamp")]
    pub timestamp: f64,
    /// The configuration object to merge in.
    #[serde(rename = "valueToMerge")]
    pub value_to_merge: Value,
}

/// Response for `UpdateConfiguration` (empty).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct UpdateConfigurationResponse {}

/// Request for `SubscribeToConfigurationUpdate`.
#[derive(Debug, Clone, Serialize)]
pub struct SubscribeToConfigurationUpdateRequest {
    /// The component to watch (defaults to this component if `None`).
    #[serde(rename = "componentName", skip_serializing_if = "Option::is_none")]
    pub component_name: Option<String>,
    /// The key path to watch.
    #[serde(rename = "keyPath")]
    pub key_path: Vec<String>,
}

/// Response for `SubscribeToConfigurationUpdate` (empty subscription ack).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SubscribeToConfigurationUpdateResponse {}

/// A configuration-changed event (`aws.greengrass#ConfigurationUpdateEvent`).
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigurationUpdateEvent {
    /// The name of the component whose configuration updated.
    #[serde(rename = "componentName")]
    pub component_name: String,
    /// The key path of the configuration value that updated.
    #[serde(rename = "keyPath")]
    pub key_path: Vec<String>,
}

/// The streamed configuration-update event union
/// (`aws.greengrass#ConfigurationUpdateEvents`).
#[derive(Debug, Clone, Deserialize)]
pub struct ConfigurationUpdateEvents {
    /// The configuration-update event.
    #[serde(rename = "configurationUpdateEvent", default)]
    pub configuration_update_event: Option<ConfigurationUpdateEvent>,
}
