mod support;

use futures_util::StreamExt;
use greengrass_ipc::{Client, ClientConfig, Error, IpcEnv, LifecycleState};
use std::collections::HashMap;
use support::{
    Behavior, Control, MockNucleus, MT_INTERNAL_ERROR, MT_PING_RESPONSE, MT_PROTOCOL_ERROR,
};

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

// ---------------------------------------------------------------------------------------------
// Connection-level protocol messages (stream-id 0).
//
// These used to be discarded wholesale, which meant an unanswered Ping and -- worse -- a nucleus
// reporting a fatal protocol error that the client never noticed, leaving operations waiting on a
// socket that would never deliver another frame.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn ping_is_answered_with_a_ping_response() {
    let mut mock = MockNucleus::start(HashMap::new()).await.unwrap();
    let _client = Client::connect(&env_for(&mock)).await.unwrap();

    mock.control
        .send(Control::SendPing(b"beat".to_vec()))
        .unwrap();

    let response = mock
        .next_frame_matching(TIMEOUT, |f| f.message_type == MT_PING_RESPONSE)
        .await
        .expect("client should answer a Ping with a PingResponse");

    assert_eq!(response.stream_id, 0, "protocol messages ride stream-0");
    assert_eq!(
        response.payload, b"beat",
        "the spec requires the ping payload to be echoed back"
    );
}

#[tokio::test]
async fn protocol_error_fails_in_flight_operations() {
    let mut behavior = HashMap::new();
    // The nucleus accepts the request but answers with a connection-level error instead.
    behavior.insert("aws.greengrass#UpdateState".to_string(), Behavior::Silent);
    let mock = MockNucleus::start(behavior).await.unwrap();
    let client = Client::connect_with_config(
        &env_for(&mock),
        ClientConfig::default().with_request_timeout(std::time::Duration::from_secs(30)),
    )
    .await
    .unwrap();

    let control = mock.control.clone();
    let pending = tokio::spawn(async move { client.update_state(LifecycleState::Running).await });

    // Give the request time to reach the mock, then report a fatal protocol error.
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    control
        .send(Control::SendProtocolError {
            message_type: MT_PROTOCOL_ERROR,
            body: r#"{"error":"boom"}"#.to_string(),
        })
        .unwrap();

    let result = tokio::time::timeout(TIMEOUT, pending)
        .await
        .expect("must not hang: the whole point is that we notice the error")
        .unwrap();

    assert!(
        matches!(result, Err(Error::ConnectionClosed)),
        "expected ConnectionClosed, got {result:?}"
    );
}

#[tokio::test]
async fn internal_error_also_closes_the_connection() {
    let mock = MockNucleus::start(HashMap::new()).await.unwrap();
    let client = Client::connect(&env_for(&mock)).await.unwrap();

    mock.control
        .send(Control::SendProtocolError {
            message_type: MT_INTERNAL_ERROR,
            body: r#"{"error":"internal"}"#.to_string(),
        })
        .unwrap();

    tokio::time::timeout(TIMEOUT, client.closed())
        .await
        .expect("InternalError must tear the connection down");
}

// ---------------------------------------------------------------------------------------------
// Connection loss detection.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn closed_resolves_when_the_socket_drops() {
    let mock = MockNucleus::start(HashMap::new()).await.unwrap();
    let client = Client::connect(&env_for(&mock)).await.unwrap();

    mock.control.send(Control::Close).unwrap();

    tokio::time::timeout(TIMEOUT, client.closed())
        .await
        .expect("closed() must resolve when the nucleus goes away");
}

#[tokio::test]
async fn closed_resolves_immediately_if_already_dead() {
    // A late caller must not wait for a state change that already happened.
    let mock = MockNucleus::start(HashMap::new()).await.unwrap();
    let client = Client::connect(&env_for(&mock)).await.unwrap();

    mock.control.send(Control::Close).unwrap();
    tokio::time::timeout(TIMEOUT, client.closed())
        .await
        .unwrap();

    tokio::time::timeout(std::time::Duration::from_millis(100), client.closed())
        .await
        .expect("a second call must return immediately");
}

#[tokio::test]
async fn eof_mid_request_returns_connection_closed() {
    let mut behavior = HashMap::new();
    behavior.insert("aws.greengrass#UpdateState".to_string(), Behavior::Silent);
    let mock = MockNucleus::start(behavior).await.unwrap();
    let client = Client::connect(&env_for(&mock)).await.unwrap();

    let control = mock.control.clone();
    let pending = tokio::spawn(async move { client.update_state(LifecycleState::Running).await });

    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    control.send(Control::Close).unwrap();

    let result = tokio::time::timeout(TIMEOUT, pending)
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(result, Err(Error::ConnectionClosed)),
        "expected ConnectionClosed, got {result:?}"
    );
}

// ---------------------------------------------------------------------------------------------
// Request timeout.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn request_times_out_when_the_nucleus_never_replies() {
    let mut behavior = HashMap::new();
    behavior.insert("aws.greengrass#UpdateState".to_string(), Behavior::Silent);
    let mock = MockNucleus::start(behavior).await.unwrap();

    let client = Client::connect_with_config(
        &env_for(&mock),
        ClientConfig::default().with_request_timeout(std::time::Duration::from_millis(150)),
    )
    .await
    .unwrap();

    let result = client.update_state(LifecycleState::Running).await;

    match result {
        Err(Error::Timeout { operation, timeout }) => {
            assert_eq!(operation, "aws.greengrass#UpdateState");
            assert_eq!(timeout, std::time::Duration::from_millis(150));
        }
        other => panic!("expected Timeout, got {other:?}"),
    }
}

#[tokio::test]
async fn a_timed_out_request_does_not_resolve_a_later_caller() {
    // The stream must be de-registered on timeout, or a late reply could resolve an unrelated
    // operation that reused the id.
    let mut behavior = HashMap::new();
    behavior.insert("aws.greengrass#UpdateState".to_string(), Behavior::Silent);
    let mock = MockNucleus::start(behavior).await.unwrap();
    let client = Client::connect_with_config(
        &env_for(&mock),
        ClientConfig::default().with_request_timeout(std::time::Duration::from_millis(100)),
    )
    .await
    .unwrap();

    assert!(matches!(
        client.update_state(LifecycleState::Running).await,
        Err(Error::Timeout { .. })
    ));
    // The connection is still usable; a second request gets its own fresh stream.
    assert!(matches!(
        client.update_state(LifecycleState::Running).await,
        Err(Error::Timeout { .. })
    ));
}

// ---------------------------------------------------------------------------------------------
// Subscription cleanup.
// ---------------------------------------------------------------------------------------------

#[tokio::test]
async fn dropping_an_event_stream_terminates_it_on_the_nucleus() {
    let mut behavior = HashMap::new();
    behavior.insert(
        "aws.greengrass#SubscribeToComponentUpdates".to_string(),
        Behavior::Subscribe {
            ack: serde_json::json!({}),
            events: vec![],
        },
    );
    let mut mock = MockNucleus::start(behavior).await.unwrap();
    let client = Client::connect(&env_for(&mock)).await.unwrap();

    let stream = client.subscribe_to_component_updates().await.unwrap();
    drop(stream);

    let terminate = mock
        .next_frame_matching(TIMEOUT, |f| f.flags & support::FLAG_TERMINATE_STREAM != 0)
        .await
        .expect("dropping an EventStream must unsubscribe on the nucleus");

    assert_ne!(
        terminate.stream_id, 0,
        "termination targets the operation stream"
    );
}

const TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
