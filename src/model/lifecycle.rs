//! Component lifecycle and update-management shapes.

use serde::{Deserialize, Serialize};

/// The lifecycle state of a component on the core device (`aws.greengrass#LifecycleState`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LifecycleState {
    /// The component is running.
    #[serde(rename = "RUNNING")]
    Running,
    /// The component encountered an error.
    #[serde(rename = "ERRORED")]
    Errored,
    /// The component is new.
    #[serde(rename = "NEW")]
    New,
    /// The component finished running.
    #[serde(rename = "FINISHED")]
    Finished,
    /// The component is installed.
    #[serde(rename = "INSTALLED")]
    Installed,
    /// The component is broken.
    #[serde(rename = "BROKEN")]
    Broken,
    /// The component is starting.
    #[serde(rename = "STARTING")]
    Starting,
    /// The component is stopping.
    #[serde(rename = "STOPPING")]
    Stopping,
}

/// Request for `UpdateState`.
#[derive(Debug, Clone, Serialize)]
pub struct UpdateStateRequest {
    /// The state to report.
    pub state: LifecycleState,
}

/// Response for `UpdateState` (empty).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct UpdateStateResponse {}

/// Request for `SubscribeToComponentUpdates` (empty).
#[derive(Debug, Clone, Default, Serialize)]
pub struct SubscribeToComponentUpdatesRequest {}

/// Response for `SubscribeToComponentUpdates` (empty subscription ack).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct SubscribeToComponentUpdatesResponse {}

/// An event indicating the nucleus wants to update a component (`aws.greengrass#PreComponentUpdateEvent`).
#[derive(Debug, Clone, Deserialize)]
pub struct PreComponentUpdateEvent {
    /// The ID of the deployment that will update the component.
    #[serde(rename = "deploymentId")]
    pub deployment_id: String,
    /// Whether the nucleus needs to restart to apply the update.
    #[serde(rename = "isGgcRestarting", default)]
    pub is_ggc_restarting: bool,
}

/// An event indicating the nucleus updated a component (`aws.greengrass#PostComponentUpdateEvent`).
#[derive(Debug, Clone, Deserialize)]
pub struct PostComponentUpdateEvent {
    /// The ID of the deployment that updated the component.
    #[serde(rename = "deploymentId")]
    pub deployment_id: String,
}

/// A streamed component-update event (`aws.greengrass#ComponentUpdatePolicyEvents`, a tagged union).
#[derive(Debug, Clone, Deserialize)]
pub struct ComponentUpdatePolicyEvents {
    /// Set when the nucleus wants to update the component.
    #[serde(rename = "preUpdateEvent", default)]
    pub pre_update_event: Option<PreComponentUpdateEvent>,
    /// Set when the nucleus finished updating the component.
    #[serde(rename = "postUpdateEvent", default)]
    pub post_update_event: Option<PostComponentUpdateEvent>,
}

/// Request for `DeferComponentUpdate`.
#[derive(Debug, Clone, Serialize)]
pub struct DeferComponentUpdateRequest {
    /// The ID of the deployment to defer.
    #[serde(rename = "deploymentId")]
    pub deployment_id: String,
    /// Optional component name for which to defer updates (defaults to this component).
    #[serde(rename = "message", skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Milliseconds to defer the update. `0` (or `None`) acknowledges/allows the update.
    #[serde(rename = "recheckAfterMs", skip_serializing_if = "Option::is_none")]
    pub recheck_after_ms: Option<i64>,
}

/// Response for `DeferComponentUpdate` (empty).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct DeferComponentUpdateResponse {}

/// Request for `PauseComponent`.
#[derive(Debug, Clone, Serialize)]
pub struct PauseComponentRequest {
    /// The name of the component to pause.
    #[serde(rename = "componentName")]
    pub component_name: String,
}

/// Response for `PauseComponent` (empty).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PauseComponentResponse {}

/// Request for `ResumeComponent`.
#[derive(Debug, Clone, Serialize)]
pub struct ResumeComponentRequest {
    /// The name of the component to resume.
    #[serde(rename = "componentName")]
    pub component_name: String,
}

/// Response for `ResumeComponent` (empty).
#[derive(Debug, Clone, Default, Deserialize)]
pub struct ResumeComponentResponse {}

/// Request for `RestartComponent`.
#[derive(Debug, Clone, Serialize)]
pub struct RestartComponentRequest {
    /// The name of the component to restart.
    #[serde(rename = "componentName")]
    pub component_name: String,
}

/// Response for `RestartComponent`.
#[derive(Debug, Clone, Deserialize)]
pub struct RestartComponentResponse {
    /// The status of the restart request (`SUCCEEDED` / `FAILED`).
    #[serde(rename = "restartStatus", default)]
    pub restart_status: Option<String>,
    /// An optional message about the restart.
    #[serde(rename = "message", default)]
    pub message: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_state_serializes_enum_as_screaming_case() {
        let json = serde_json::to_value(UpdateStateRequest {
            state: LifecycleState::Running,
        })
        .unwrap();
        assert_eq!(json, serde_json::json!({ "state": "RUNNING" }));
    }

    #[test]
    fn defer_omits_none_fields() {
        let json = serde_json::to_value(DeferComponentUpdateRequest {
            deployment_id: "d".into(),
            message: None,
            recheck_after_ms: Some(0),
        })
        .unwrap();
        assert_eq!(
            json,
            serde_json::json!({ "deploymentId": "d", "recheckAfterMs": 0 })
        );
    }

    #[test]
    fn pre_update_event_deserializes() {
        let ev: ComponentUpdatePolicyEvents = serde_json::from_value(serde_json::json!({
            "preUpdateEvent": { "deploymentId": "abc", "isGgcRestarting": true }
        }))
        .unwrap();
        let pre = ev.pre_update_event.unwrap();
        assert_eq!(pre.deployment_id, "abc");
        assert!(pre.is_ggc_restarting);
    }
}
