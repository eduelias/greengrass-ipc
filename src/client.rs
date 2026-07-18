//! The public [`Client`] and the [`EventStream`] subscription type.

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::eventstream::RpcMessage;
use crate::model::*;
use crate::IpcEnv;
use futures_core::Stream;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::mpsc;

/// An async client for the Greengrass v2 component IPC service.
///
/// Create one with [`Client::connect_from_env`]. Cloning is cheap (the underlying connection is
/// shared); all clones share the single socket and read loop.
#[derive(Clone)]
pub struct Client {
    conn: Arc<Connection>,
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client").finish_non_exhaustive()
    }
}

impl Client {
    /// Connects to the nucleus IPC socket using the `SVCUID` and socket-path environment variables
    /// the nucleus injects, and performs the authentication handshake.
    ///
    /// Returns [`Error::NotUnderGreengrass`] if those variables are absent, so a caller that may run
    /// outside Greengrass can fall back gracefully.
    pub async fn connect_from_env() -> Result<Self> {
        let env = IpcEnv::from_env()?;
        Self::connect(&env).await
    }

    /// Connects using explicit [`IpcEnv`] parameters (useful for tests).
    pub async fn connect(env: &IpcEnv) -> Result<Self> {
        let conn = Connection::connect(env).await?;
        Ok(Self {
            conn: Arc::new(conn),
        })
    }

    /// Sends a request/response operation and decodes the typed response.
    async fn request<Req, Resp>(&self, op: &str, model: &str, req: &Req) -> Result<Resp>
    where
        Req: Serialize,
        Resp: DeserializeOwned,
    {
        let response = self.conn.request(op, model, req).await?;
        decode_payload(&response)
    }

    /// Opens a subscription and returns a typed [`EventStream`].
    async fn subscribe<Req, Ev>(&self, op: &str, model: &str, req: &Req) -> Result<EventStream<Ev>>
    where
        Req: Serialize,
        Ev: DeserializeOwned,
    {
        let (stream_id, rx) = self.conn.subscribe(op, model, req).await?;
        Ok(EventStream {
            conn: self.conn.clone(),
            stream_id,
            rx,
            closed: false,
            _marker: PhantomData,
        })
    }

    // ---- Lifecycle / update management ----

    /// Reports this component's lifecycle state to the nucleus (`UpdateState`).
    pub async fn update_state(&self, state: LifecycleState) -> Result<()> {
        let _: UpdateStateResponse = self
            .request(
                "aws.greengrass#UpdateState",
                "aws.greengrass#UpdateStateRequest",
                &UpdateStateRequest { state },
            )
            .await?;
        Ok(())
    }

    /// Subscribes to component-update notifications (`SubscribeToComponentUpdates`).
    pub async fn subscribe_to_component_updates(
        &self,
    ) -> Result<EventStream<ComponentUpdatePolicyEvents>> {
        self.subscribe(
            "aws.greengrass#SubscribeToComponentUpdates",
            "aws.greengrass#SubscribeToComponentUpdatesRequest",
            &SubscribeToComponentUpdatesRequest {},
        )
        .await
    }

    /// Defers (or acknowledges) a component update (`DeferComponentUpdate`).
    ///
    /// Pass `recheck_after_ms = Some(0)` or `None` to acknowledge/allow the update; a positive value
    /// defers it for that many milliseconds.
    pub async fn defer_component_update(
        &self,
        deployment_id: impl Into<String>,
        recheck_after_ms: Option<i64>,
        message: Option<String>,
    ) -> Result<()> {
        let _: DeferComponentUpdateResponse = self
            .request(
                "aws.greengrass#DeferComponentUpdate",
                "aws.greengrass#DeferComponentUpdateRequest",
                &DeferComponentUpdateRequest {
                    deployment_id: deployment_id.into(),
                    message,
                    recheck_after_ms,
                },
            )
            .await?;
        Ok(())
    }

    /// Pauses another (generic) component (`PauseComponent`).
    pub async fn pause_component(&self, component_name: impl Into<String>) -> Result<()> {
        let _: PauseComponentResponse = self
            .request(
                "aws.greengrass#PauseComponent",
                "aws.greengrass#PauseComponentRequest",
                &PauseComponentRequest {
                    component_name: component_name.into(),
                },
            )
            .await?;
        Ok(())
    }

    /// Resumes a paused component (`ResumeComponent`).
    pub async fn resume_component(&self, component_name: impl Into<String>) -> Result<()> {
        let _: ResumeComponentResponse = self
            .request(
                "aws.greengrass#ResumeComponent",
                "aws.greengrass#ResumeComponentRequest",
                &ResumeComponentRequest {
                    component_name: component_name.into(),
                },
            )
            .await?;
        Ok(())
    }

    /// Restarts a component (`RestartComponent`).
    pub async fn restart_component(
        &self,
        component_name: impl Into<String>,
    ) -> Result<RestartComponentResponse> {
        self.request::<_, RestartComponentResponse>(
            "aws.greengrass#RestartComponent",
            "aws.greengrass#RestartComponentRequest",
            &RestartComponentRequest {
                component_name: component_name.into(),
            },
        )
        .await
    }

    // ---- Configuration ----

    /// Reads component configuration (`GetConfiguration`).
    pub async fn get_configuration(
        &self,
        component_name: Option<String>,
        key_path: Vec<String>,
    ) -> Result<GetConfigurationResponse> {
        self.request::<_, GetConfigurationResponse>(
            "aws.greengrass#GetConfiguration",
            "aws.greengrass#GetConfigurationRequest",
            &GetConfigurationRequest {
                component_name,
                key_path,
            },
        )
        .await
    }

    /// Merges an update into this component's configuration (`UpdateConfiguration`).
    pub async fn update_configuration(
        &self,
        key_path: Option<Vec<String>>,
        timestamp_ms: f64,
        value_to_merge: serde_json::Value,
    ) -> Result<()> {
        let _: UpdateConfigurationResponse = self
            .request(
                "aws.greengrass#UpdateConfiguration",
                "aws.greengrass#UpdateConfigurationRequest",
                &UpdateConfigurationRequest {
                    key_path,
                    timestamp: timestamp_ms,
                    value_to_merge,
                },
            )
            .await?;
        Ok(())
    }

    /// Subscribes to configuration-change notifications (`SubscribeToConfigurationUpdate`).
    pub async fn subscribe_to_configuration_update(
        &self,
        component_name: Option<String>,
        key_path: Vec<String>,
    ) -> Result<EventStream<ConfigurationUpdateEvents>> {
        self.subscribe(
            "aws.greengrass#SubscribeToConfigurationUpdate",
            "aws.greengrass#SubscribeToConfigurationUpdateRequest",
            &SubscribeToConfigurationUpdateRequest {
                component_name,
                key_path,
            },
        )
        .await
    }

    // ---- Local pub/sub ----

    /// Publishes a message to a local topic (`PublishToTopic`).
    pub async fn publish_to_topic(
        &self,
        topic: impl Into<String>,
        message: PublishMessage,
    ) -> Result<()> {
        let _: PublishToTopicResponse = self
            .request(
                "aws.greengrass#PublishToTopic",
                "aws.greengrass#PublishToTopicRequest",
                &PublishToTopicRequest {
                    topic: topic.into(),
                    publish_message: message,
                },
            )
            .await?;
        Ok(())
    }

    /// Subscribes to a local topic (`SubscribeToTopic`).
    pub async fn subscribe_to_topic(
        &self,
        topic: impl Into<String>,
    ) -> Result<EventStream<SubscriptionResponseMessage>> {
        self.subscribe(
            "aws.greengrass#SubscribeToTopic",
            "aws.greengrass#SubscribeToTopicRequest",
            &SubscribeToTopicRequest {
                topic: topic.into(),
                receive_mode: None,
            },
        )
        .await
    }

    /// Publishes an MQTT message to AWS IoT Core (`PublishToIoTCore`).
    ///
    /// Reuses the nucleus's MQTT connection; the component must be authorized for
    /// the topic via an `aws.greengrass.ipc.mqttproxy` policy.
    pub async fn publish_to_iot_core(
        &self,
        topic_name: impl Into<String>,
        qos: QoS,
        payload: impl Into<Vec<u8>>,
    ) -> Result<()> {
        let _: PublishToIoTCoreResponse = self
            .request(
                "aws.greengrass#PublishToIoTCore",
                "aws.greengrass#PublishToIoTCoreRequest",
                &PublishToIoTCoreRequest {
                    topic_name: topic_name.into(),
                    qos,
                    payload: Some(Blob(payload.into())),
                },
            )
            .await?;
        Ok(())
    }

    /// Subscribes to AWS IoT Core MQTT messages on a topic or filter
    /// (`SubscribeToIoTCore`). Yields [`IoTCoreMessage`] events.
    ///
    /// Reuses the nucleus's MQTT connection; the component must be authorized for
    /// the topic via an `aws.greengrass.ipc.mqttproxy` policy.
    pub async fn subscribe_to_iot_core(
        &self,
        topic_name: impl Into<String>,
        qos: QoS,
    ) -> Result<EventStream<IoTCoreMessage>> {
        self.subscribe(
            "aws.greengrass#SubscribeToIoTCore",
            "aws.greengrass#SubscribeToIoTCoreRequest",
            &SubscribeToIoTCoreRequest {
                topic_name: topic_name.into(),
                qos,
            },
        )
        .await
    }
}

/// A stream of typed events from a subscription operation.
///
/// Yields `Result<Ev>` items. The subscription is closed when this value is dropped.
pub struct EventStream<Ev> {
    conn: Arc<Connection>,
    stream_id: i32,
    rx: mpsc::UnboundedReceiver<Result<RpcMessage>>,
    closed: bool,
    _marker: PhantomData<fn() -> Ev>,
}

impl<Ev: DeserializeOwned> Stream for EventStream<Ev> {
    type Item = Result<Ev>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(Ok(rpc))) => Poll::Ready(Some(decode_event(&rpc))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

impl<Ev> Drop for EventStream<Ev> {
    fn drop(&mut self) {
        if !self.closed {
            self.closed = true;
            let conn = self.conn.clone();
            let stream_id = self.stream_id;
            // Best-effort: tell the nucleus to close the stream.
            tokio::spawn(async move {
                conn.close_stream(stream_id).await;
            });
        }
    }
}

/// Decodes a JSON response payload. An empty payload is treated as an empty JSON object, so
/// response types whose fields are all optional deserialize correctly.
fn decode_payload<T: DeserializeOwned>(rpc: &RpcMessage) -> Result<T> {
    let bytes: &[u8] = if rpc.payload.is_empty() {
        b"{}"
    } else {
        &rpc.payload
    };
    serde_json::from_slice(bytes).map_err(Error::from)
}

/// Decodes a JSON event payload (no `Default` requirement).
fn decode_event<T: DeserializeOwned>(rpc: &RpcMessage) -> Result<T> {
    serde_json::from_slice(&rpc.payload).map_err(Error::from)
}
