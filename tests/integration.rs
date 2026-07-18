mod support;

use futures_util::StreamExt;
use greengrass_ipc::{Client, Error, IpcEnv, LifecycleState};
use std::collections::HashMap;
use support::{Behavior, MockNucleus};

fn env_for(mock: &MockNucleus) -> IpcEnv {
    IpcEnv {
        socket_path: mock.socket_path.clone(),
        auth_token: mock.auth_token.clone(),
    }
}

#[tokio::test]
async fn handshake_and_update_state() {
    let mut behavior = HashMap::new();
    behavior.insert(
        "aws.greengrass#UpdateState".to_string(),
        Behavior::Respond {
            payload: serde_json::json!({}),
        },
    );
    let mut mock = MockNucleus::start(behavior).await.unwrap();

    let client = Client::connect(&env_for(&mock)).await.unwrap();
    client.update_state(LifecycleState::Running).await.unwrap();

    let (op, payload) = mock.seen.recv().await.unwrap();
    assert_eq!(op, "aws.greengrass#UpdateState");
    assert_eq!(payload, serde_json::json!({ "state": "RUNNING" }));
}

#[tokio::test]
async fn rejected_handshake_is_an_error() {
    let mock = MockNucleus::start(HashMap::new()).await.unwrap();
    let mut env = env_for(&mock);
    env.auth_token = "wrong-token".to_string();

    let err = Client::connect(&env).await.unwrap_err();
    assert!(matches!(err, Error::Handshake(_)), "got {err:?}");
}

#[tokio::test]
async fn defer_component_update_sends_correct_payload() {
    let mut behavior = HashMap::new();
    behavior.insert(
        "aws.greengrass#DeferComponentUpdate".to_string(),
        Behavior::Respond {
            payload: serde_json::json!({}),
        },
    );
    let mut mock = MockNucleus::start(behavior).await.unwrap();
    let client = Client::connect(&env_for(&mock)).await.unwrap();

    client
        .defer_component_update("deploy-123", Some(30_000), None)
        .await
        .unwrap();

    let (op, payload) = mock.seen.recv().await.unwrap();
    assert_eq!(op, "aws.greengrass#DeferComponentUpdate");
    assert_eq!(
        payload,
        serde_json::json!({ "deploymentId": "deploy-123", "recheckAfterMs": 30_000 })
    );
}

#[tokio::test]
async fn service_error_is_surfaced() {
    let mut behavior = HashMap::new();
    behavior.insert(
        "aws.greengrass#RestartComponent".to_string(),
        Behavior::Error {
            model: "aws.greengrass#ResourceNotFoundError".to_string(),
            message: "no such component".to_string(),
        },
    );
    let mock = MockNucleus::start(behavior).await.unwrap();
    let client = Client::connect(&env_for(&mock)).await.unwrap();

    let err = client.restart_component("nope").await.unwrap_err();
    match err {
        Error::Service { model, message } => {
            assert_eq!(model, "aws.greengrass#ResourceNotFoundError");
            assert_eq!(message, "no such component");
        }
        other => panic!("expected service error, got {other:?}"),
    }
}

#[tokio::test]
async fn subscribe_to_component_updates_streams_events() {
    let mut behavior = HashMap::new();
    behavior.insert(
        "aws.greengrass#SubscribeToComponentUpdates".to_string(),
        Behavior::Subscribe {
            ack: serde_json::json!({}),
            events: vec![(
                "aws.greengrass#ComponentUpdatePolicyEvents".to_string(),
                serde_json::json!({
                    "preUpdateEvent": { "deploymentId": "d-1", "isGgcRestarting": true }
                }),
            )],
        },
    );
    let mock = MockNucleus::start(behavior).await.unwrap();
    let client = Client::connect(&env_for(&mock)).await.unwrap();

    let mut updates = client.subscribe_to_component_updates().await.unwrap();
    let event = updates.next().await.unwrap().unwrap();
    let pre = event.pre_update_event.expect("preUpdateEvent");
    assert_eq!(pre.deployment_id, "d-1");
    assert!(pre.is_ggc_restarting);
}
