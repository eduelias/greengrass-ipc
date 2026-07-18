//! The IPC connection: the Unix socket, the Connect handshake, and the background read loop that
//! demultiplexes incoming frames to the waiting operations.

use crate::error::{Error, Result};
use crate::eventstream::{MessageType, RpcMessage};
use crate::IpcEnv;
use aws_smithy_eventstream::frame::{write_message_to, DecodedFrame, MessageFrameDecoder};
use aws_smithy_types::event_stream::Message;
use bytes::{Bytes, BytesMut};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot, Mutex as AsyncMutex};

/// Reads complete EventStream frames off a socket, keeping a persistent buffer + decoder so partial
/// frames and pipelined bytes are never lost between reads.
struct FramedReader {
    read_half: tokio::net::unix::OwnedReadHalf,
    decoder: MessageFrameDecoder,
    buf: BytesMut,
}

impl FramedReader {
    fn new(read_half: tokio::net::unix::OwnedReadHalf) -> Self {
        Self {
            read_half,
            decoder: MessageFrameDecoder::new(),
            buf: BytesMut::with_capacity(8192),
        }
    }

    /// Reads the next complete frame. Returns `Ok(None)` on clean EOF.
    async fn next_frame(&mut self) -> Result<Option<Message>> {
        loop {
            match self
                .decoder
                .decode_frame(&mut self.buf)
                .map_err(|e| Error::frame(e.to_string()))?
            {
                DecodedFrame::Complete(message) => return Ok(Some(message)),
                DecodedFrame::Incomplete => {}
            }

            let n = self.read_half.read_buf(&mut self.buf).await?;
            if n == 0 {
                return Ok(None);
            }
        }
    }
}

/// A sink waiting for messages on a given stream-id.
enum StreamSink {
    /// A single request/response: resolved once with the first response (or error).
    Request(oneshot::Sender<Result<RpcMessage>>),
    /// A subscription: each event is forwarded until the stream terminates.
    Subscription(mpsc::UnboundedSender<Result<RpcMessage>>),
}

/// Shared registry mapping stream-ids to their waiting sinks.
type Registry = Arc<Mutex<HashMap<i32, StreamSink>>>;

/// A live, authenticated connection to the nucleus IPC server.
pub(crate) struct Connection {
    write_half: AsyncMutex<OwnedWriteHalf>,
    registry: Registry,
    next_stream_id: AtomicI32,
    /// Kept so the read loop is aborted when the connection is dropped.
    read_task: tokio::task::JoinHandle<()>,
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.read_task.abort();
    }
}

impl Connection {
    /// Connects to the nucleus, performs the Connect/ConnectAck handshake, and starts the read loop.
    pub(crate) async fn connect(env: &IpcEnv) -> Result<Self> {
        let stream = UnixStream::connect(&env.socket_path).await?;
        let (read_half, mut write_half) = stream.into_split();

        // --- Handshake: send Connect, await ConnectAck on stream-id 0. ---
        let connect_payload = serde_json::to_vec(&ConnectPayload {
            auth_token: &env.auth_token,
        })?;
        let connect = RpcMessage::connect(Bytes::from(connect_payload));
        write_frame(&mut write_half, &connect).await?;

        let mut reader = FramedReader::new(read_half);
        let ack = reader
            .next_frame()
            .await?
            .ok_or_else(|| Error::handshake("connection closed during handshake"))?;
        let ack = RpcMessage::parse(&ack)?;

        if ack.message_type != MessageType::ConnectAck {
            return Err(Error::handshake(format!(
                "expected ConnectAck, got {:?}",
                ack.message_type
            )));
        }
        if !ack.connection_accepted() {
            return Err(Error::handshake(
                "nucleus rejected the connection (invalid or expired SVCUID auth token)",
            ));
        }

        // --- Start the read loop. ---
        let registry: Registry = Arc::new(Mutex::new(HashMap::new()));
        let read_task = tokio::spawn(read_loop(reader, registry.clone()));

        Ok(Self {
            write_half: AsyncMutex::new(write_half),
            registry,
            // Stream-ids are positive; the CRT increments from 1.
            next_stream_id: AtomicI32::new(1),
            read_task,
        })
    }

    fn alloc_stream_id(&self) -> i32 {
        self.next_stream_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Sends a request and awaits the single response on a fresh stream.
    pub(crate) async fn request<Req: Serialize>(
        &self,
        operation: &str,
        request_model: &str,
        request: &Req,
    ) -> Result<RpcMessage> {
        let stream_id = self.alloc_stream_id();
        let (tx, rx) = oneshot::channel();
        self.registry
            .lock()
            .unwrap()
            .insert(stream_id, StreamSink::Request(tx));

        let payload = serde_json::to_vec(request)?;
        let msg = RpcMessage::activate(stream_id, operation, request_model, Bytes::from(payload));
        if let Err(e) = self.write_message(&msg).await {
            self.registry.lock().unwrap().remove(&stream_id);
            return Err(e);
        }

        rx.await.map_err(|_| Error::ConnectionClosed)?
    }

    /// Opens a subscription stream and returns the receiver for its events. The first response (the
    /// subscription ack) is consumed here; subsequent events flow into the returned channel.
    pub(crate) async fn subscribe<Req: Serialize>(
        &self,
        operation: &str,
        request_model: &str,
        request: &Req,
    ) -> Result<(i32, mpsc::UnboundedReceiver<Result<RpcMessage>>)> {
        let stream_id = self.alloc_stream_id();

        // Install the subscription sink up front so no early event is dropped between the ack and
        // the sink swap. Both the ack and the events arrive on this channel.
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        self.registry
            .lock()
            .unwrap()
            .insert(stream_id, StreamSink::Subscription(event_tx));

        let payload = serde_json::to_vec(request)?;
        let msg = RpcMessage::activate(stream_id, operation, request_model, Bytes::from(payload));
        if let Err(e) = self.write_message(&msg).await {
            self.registry.lock().unwrap().remove(&stream_id);
            return Err(e);
        }

        // The first message on the stream is the subscription ack. Consume and validate it.
        match event_rx.recv().await {
            Some(Ok(ack)) if ack.message_type == MessageType::ApplicationError => {
                self.registry.lock().unwrap().remove(&stream_id);
                return Err(service_error_from(&ack));
            }
            Some(Ok(_ack)) => {} // accepted
            Some(Err(e)) => {
                self.registry.lock().unwrap().remove(&stream_id);
                return Err(e);
            }
            None => {
                self.registry.lock().unwrap().remove(&stream_id);
                return Err(Error::ConnectionClosed);
            }
        }

        Ok((stream_id, event_rx))
    }

    /// Sends a terminate-stream message to close a subscription and removes its sink.
    pub(crate) async fn close_stream(&self, stream_id: i32) {
        self.registry.lock().unwrap().remove(&stream_id);
        let msg = RpcMessage::terminate(stream_id);
        let _ = self.write_message(&msg).await;
    }

    async fn write_message(&self, message: &Message) -> Result<()> {
        let mut buf = BytesMut::new();
        write_message_to(message, &mut buf).map_err(|e| Error::frame(e.to_string()))?;
        let mut guard = self.write_half.lock().await;
        guard.write_all(&buf).await?;
        guard.flush().await?;
        Ok(())
    }
}

/// The background task: decode frames off the socket and dispatch to waiting sinks by stream-id.
async fn read_loop(mut reader: FramedReader, registry: Registry) {
    loop {
        match reader.next_frame().await {
            Ok(Some(message)) => match RpcMessage::parse(&message) {
                Ok(rpc) => dispatch(&registry, rpc),
                Err(e) => tracing::warn!(error = %e, "failed to parse incoming IPC frame"),
            },
            Ok(None) => {
                // Socket closed: fail all pending sinks.
                fail_all(&registry, Error::ConnectionClosed);
                return;
            }
            Err(e) => {
                tracing::warn!(error = %e, "IPC read loop error; closing");
                fail_all(&registry, Error::ConnectionClosed);
                return;
            }
        }
    }
}

fn dispatch(registry: &Registry, rpc: RpcMessage) {
    let stream_id = rpc.stream_id;
    if stream_id == 0 {
        // Connection-level message after handshake (e.g. Ping / protocol error). Ignore for now.
        return;
    }

    let terminates = rpc.terminates_stream();
    let is_error = rpc.message_type == MessageType::ApplicationError;

    let mut guard = registry.lock().unwrap();
    match guard.remove(&stream_id) {
        Some(StreamSink::Request(tx)) => {
            let result = if is_error {
                Err(service_error_from(&rpc))
            } else {
                Ok(rpc)
            };
            let _ = tx.send(result);
        }
        Some(StreamSink::Subscription(tx)) => {
            let result = if is_error {
                Err(service_error_from(&rpc))
            } else {
                Ok(rpc)
            };
            let _ = tx.send(result);
            // Keep the subscription open unless this message closes the stream.
            if !terminates && !is_error {
                guard.insert(stream_id, StreamSink::Subscription(tx));
            }
        }
        None => {
            // Unknown / already-closed stream. Ignore.
        }
    }
}

fn fail_all(registry: &Registry, err: Error) {
    let mut guard = registry.lock().unwrap();
    for (_id, sink) in guard.drain() {
        match sink {
            StreamSink::Request(tx) => {
                let _ = tx.send(Err(clone_err(&err)));
            }
            StreamSink::Subscription(tx) => {
                let _ = tx.send(Err(clone_err(&err)));
            }
        }
    }
}

fn clone_err(err: &Error) -> Error {
    match err {
        Error::ConnectionClosed => Error::ConnectionClosed,
        other => Error::frame(other.to_string()),
    }
}

/// Builds an [`Error::Service`] from an application-error frame.
fn service_error_from(rpc: &RpcMessage) -> Error {
    let model = rpc
        .service_model_type
        .clone()
        .unwrap_or_else(|| "aws.greengrass#ServiceError".to_owned());
    let message = serde_json::from_slice::<ServiceErrorPayload>(&rpc.payload)
        .ok()
        .and_then(|p| p.message)
        .unwrap_or_else(|| "unknown service error".to_owned());
    Error::Service { model, message }
}

async fn write_frame(write_half: &mut OwnedWriteHalf, message: &Message) -> Result<()> {
    let mut buf = BytesMut::new();
    write_message_to(message, &mut buf).map_err(|e| Error::frame(e.to_string()))?;
    write_half.write_all(&buf).await?;
    write_half.flush().await?;
    Ok(())
}

#[derive(Serialize)]
struct ConnectPayload<'a> {
    #[serde(rename = "authToken")]
    auth_token: &'a str,
}

#[derive(serde::Deserialize)]
struct ServiceErrorPayload {
    #[serde(rename = "message")]
    message: Option<String>,
}
