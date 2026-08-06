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
use std::ops::ControlFlow;
use std::sync::atomic::{AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot, watch, Mutex as AsyncMutex};

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
    /// Shared with the read loop, which needs it to answer `Ping` and to shut the socket down when
    /// the nucleus reports a connection-level error.
    write_half: Arc<AsyncMutex<OwnedWriteHalf>>,
    registry: Registry,
    next_stream_id: AtomicI32,
    /// Flipped to `true` by the read loop just before it exits, for [`Connection::closed`].
    closed: watch::Receiver<bool>,
    /// How long to wait for a response before giving up on a request.
    request_timeout: Duration,
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
    pub(crate) async fn connect(env: &IpcEnv, request_timeout: Duration) -> Result<Self> {
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
        let write_half = Arc::new(AsyncMutex::new(write_half));
        let (closed_tx, closed_rx) = watch::channel(false);
        let read_task = tokio::spawn(read_loop(
            reader,
            registry.clone(),
            write_half.clone(),
            closed_tx,
        ));

        Ok(Self {
            write_half,
            registry,
            // Stream-ids are positive; the CRT increments from 1.
            next_stream_id: AtomicI32::new(1),
            closed: closed_rx,
            request_timeout,
            read_task,
        })
    }

    /// Resolves once the connection is gone. See [`crate::Client::closed`].
    pub(crate) async fn closed(&self) {
        let mut rx = self.closed.clone();
        // `wait_for` checks the current value first, so a connection that is already dead resolves
        // immediately rather than waiting for a change that will never come.
        let _ = rx.wait_for(|closed| *closed).await;
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

        match tokio::time::timeout(self.request_timeout, rx).await {
            Ok(Ok(result)) => result,
            // The read loop dropped our sender: the connection went away.
            Ok(Err(_)) => Err(Error::ConnectionClosed),
            Err(_elapsed) => {
                // Stop tracking the stream so a late reply is discarded rather than resolving a
                // caller that has already given up.
                self.registry.lock().unwrap().remove(&stream_id);
                Err(Error::Timeout {
                    operation: operation.to_owned(),
                    timeout: self.request_timeout,
                })
            }
        }
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
        let result = async {
            guard.write_all(&buf).await?;
            guard.flush().await?;
            Ok::<(), std::io::Error>(())
        }
        .await;

        if let Err(error) = result {
            // A write failure (EPIPE and friends) means the socket is gone. Shut the write half
            // down so the nucleus sees EOF and drops its end; our read loop then observes EOF and
            // fails everything still in flight. Without this, other pending operations would sit
            // waiting for a read-side EOF that may take a while to arrive.
            tracing::warn!(%error, "IPC write failed; shutting the connection down");
            let _ = guard.shutdown().await;
            return Err(Error::from(error));
        }

        Ok(())
    }
}

/// The background task: decode frames off the socket and dispatch to waiting sinks by stream-id.
///
/// Owns the connection's liveness: whatever ends this loop -- EOF, a read error, or a
/// connection-level error from the nucleus -- fails every in-flight operation and flips the
/// `closed` watch so [`Connection::closed`] resolves.
async fn read_loop(
    mut reader: FramedReader,
    registry: Registry,
    write_half: Arc<AsyncMutex<OwnedWriteHalf>>,
    closed: watch::Sender<bool>,
) {
    loop {
        match reader.next_frame().await {
            Ok(Some(message)) => match RpcMessage::parse(&message) {
                Ok(rpc) => {
                    if rpc.stream_id == 0 {
                        if handle_protocol_message(&write_half, rpc).await.is_break() {
                            break;
                        }
                    } else {
                        dispatch(&registry, rpc);
                    }
                }
                Err(e) => tracing::warn!(error = %e, "failed to parse incoming IPC frame"),
            },
            Ok(None) => {
                tracing::debug!("IPC socket closed by the nucleus");
                break;
            }
            Err(e) => {
                tracing::warn!(error = %e, "IPC read loop error; closing");
                break;
            }
        }
    }

    fail_all(&registry, Error::ConnectionClosed);
    // Ignored: a send failure just means every `closed()` waiter has already gone away.
    let _ = closed.send(true);
}

/// Handles a message on stream-id 0, the connection-level channel.
///
/// Returns [`ControlFlow::Break`] when the connection must be torn down.
async fn handle_protocol_message(
    write_half: &Arc<AsyncMutex<OwnedWriteHalf>>,
    rpc: RpcMessage,
) -> ControlFlow<()> {
    match rpc.message_type {
        MessageType::Ping => {
            // The EventStream RPC spec requires a PingResponse echoing the payload. The Greengrass
            // nucleus does not currently ping components -- it only answers pings, see
            // `ServiceOperationMappingContinuationHandler.onProtocolMessage` -- but other servers
            // (and future nucleus versions) may, and an unanswered ping is grounds to drop us.
            tracing::debug!("received Ping; replying PingResponse");
            let response = RpcMessage::ping_response(rpc.payload);
            let mut guard = write_half.lock().await;
            if let Err(error) = write_frame(&mut guard, &response).await {
                tracing::warn!(%error, "failed to send PingResponse");
            }
            ControlFlow::Continue(())
        }
        // We never send pings, so a response is unsolicited. Harmless.
        MessageType::PingResponse => ControlFlow::Continue(()),
        // The nucleus reports connection-level failures here and then closes the socket. Treating
        // these as fatal means pending operations fail now, rather than waiting on a socket that
        // will never deliver another frame.
        MessageType::ProtocolError | MessageType::InternalError => {
            tracing::error!(
                message_type = ?rpc.message_type,
                payload = %String::from_utf8_lossy(&rpc.payload),
                "nucleus reported a connection-level error; closing the connection"
            );
            ControlFlow::Break(())
        }
        other => {
            tracing::debug!(message_type = ?other, "ignoring unexpected stream-0 message");
            ControlFlow::Continue(())
        }
    }
}

fn dispatch(registry: &Registry, rpc: RpcMessage) {
    // Stream-0 (connection-level) messages are handled in `read_loop` before reaching here.
    debug_assert_ne!(rpc.stream_id, 0);
    let stream_id = rpc.stream_id;

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
                let _ = tx.send(Err(err.clone()));
            }
            StreamSink::Subscription(tx) => {
                let _ = tx.send(Err(err.clone()));
            }
        }
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
