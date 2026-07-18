//! A minimal mock Greengrass nucleus IPC server for integration tests.
//!
//! It speaks just enough of the EventStream RPC protocol to exercise the client: accepts a Connect
//! (validating the auth token), replies ConnectAck, and lets the test script per-operation
//! responses and streamed events. The wire constants mirror
//! `.opencode/skills/greengrass-ipc-dev/reference/WIRE_PROTOCOL.md`.

use aws_smithy_eventstream::frame::{write_message_to, DecodedFrame, MessageFrameDecoder};
use aws_smithy_types::event_stream::{Header, HeaderValue, Message};
use aws_smithy_types::str_bytes::StrBytes;
use bytes::BytesMut;
use std::path::PathBuf;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

pub const MT_APPLICATION_MESSAGE: i32 = 0;
pub const MT_APPLICATION_ERROR: i32 = 1;
pub const MT_CONNECT: i32 = 4;
pub const MT_CONNECT_ACK: i32 = 5;

pub const FLAG_CONNECTION_ACCEPTED: i32 = 0x1;
pub const FLAG_TERMINATE_STREAM: i32 = 0x2;

/// What the mock should do when it receives an operation's first (activate) message.
#[derive(Clone)]
pub enum Behavior {
    /// Reply once with a JSON response (terminating the stream), keyed by operation name.
    Respond { payload: serde_json::Value },
    /// Reply with a service error.
    Error { model: String, message: String },
    /// Reply with a subscription ack (empty), then emit the given events, each as its own frame.
    Subscribe {
        ack: serde_json::Value,
        events: Vec<(String, serde_json::Value)>,
    },
}

pub struct MockNucleus {
    pub socket_path: PathBuf,
    pub auth_token: String,
    /// Receives the operation name + payload of every request the mock handled.
    pub seen: mpsc::UnboundedReceiver<(String, serde_json::Value)>,
    _tempdir: tempfile::TempDir,
    _task: tokio::task::JoinHandle<()>,
}

impl MockNucleus {
    /// Starts the mock. `behavior` maps operation name → behavior.
    pub async fn start(
        behavior: std::collections::HashMap<String, Behavior>,
    ) -> std::io::Result<Self> {
        let tempdir = tempfile::tempdir()?;
        let socket_path = tempdir.path().join("ipc.sock");
        let auth_token = "test-svcuid".to_string();
        let listener = UnixListener::bind(&socket_path)?;
        let (seen_tx, seen_rx) = mpsc::unbounded_channel();

        let expected_token = auth_token.clone();
        let task = tokio::spawn(async move {
            if let Ok((stream, _)) = listener.accept().await {
                let _ = handle_conn(stream, expected_token, behavior, seen_tx).await;
            }
        });

        Ok(Self {
            socket_path,
            auth_token,
            seen: seen_rx,
            _tempdir: tempdir,
            _task: task,
        })
    }
}

async fn handle_conn(
    stream: UnixStream,
    expected_token: String,
    behavior: std::collections::HashMap<String, Behavior>,
    seen_tx: mpsc::UnboundedSender<(String, serde_json::Value)>,
) -> std::io::Result<()> {
    let (mut read_half, mut write_half) = stream.into_split();
    let mut decoder = MessageFrameDecoder::new();
    let mut buf = BytesMut::with_capacity(8192);

    // --- Handshake ---
    let connect = read_frame(&mut read_half, &mut decoder, &mut buf)
        .await?
        .expect("connect frame");
    let (mt, _flags, _sid, _smt, payload) = parse(&connect);
    assert_eq!(mt, MT_CONNECT, "first message must be Connect");
    let token = serde_json::from_slice::<serde_json::Value>(&payload)
        .ok()
        .and_then(|v| {
            v.get("authToken")
                .and_then(|t| t.as_str().map(String::from))
        });
    let accepted = token.as_deref() == Some(expected_token.as_str());

    let ack = Message::new(bytes::Bytes::new())
        .add_header(int32(":message-type", MT_CONNECT_ACK))
        .add_header(int32(
            ":message-flags",
            if accepted {
                FLAG_CONNECTION_ACCEPTED
            } else {
                0
            },
        ))
        .add_header(int32(":stream-id", 0));
    write_frame(&mut write_half, &ack).await?;
    if !accepted {
        return Ok(());
    }

    // --- Operation loop ---
    loop {
        let frame = match read_frame(&mut read_half, &mut decoder, &mut buf).await? {
            Some(f) => f,
            None => return Ok(()),
        };
        let (mt, flags, stream_id, _smt, payload) = parse(&frame);
        if mt != MT_APPLICATION_MESSAGE {
            continue;
        }
        // Terminate messages carry no operation; ignore.
        if flags & FLAG_TERMINATE_STREAM != 0 && payload.is_empty() {
            continue;
        }
        let operation = operation_header(&frame);
        let payload_json: serde_json::Value =
            serde_json::from_slice(&payload).unwrap_or(serde_json::Value::Null);
        if let Some(op) = &operation {
            let _ = seen_tx.send((op.clone(), payload_json.clone()));
        }

        let Some(op) = operation else { continue };
        let Some(beh) = behavior.get(&op) else {
            continue;
        };
        match beh.clone() {
            Behavior::Respond { payload } => {
                let msg = app_message(stream_id, &format!("{op}Response"), &payload, true);
                write_frame(&mut write_half, &msg).await?;
            }
            Behavior::Error { model, message } => {
                let body = serde_json::json!({ "message": message });
                let msg = app_error(stream_id, &model, &body);
                write_frame(&mut write_half, &msg).await?;
            }
            Behavior::Subscribe { ack, events } => {
                let ack_msg = app_message(stream_id, &format!("{op}Response"), &ack, false);
                write_frame(&mut write_half, &ack_msg).await?;
                for (model, ev) in events {
                    let ev_msg = app_message(stream_id, &model, &ev, false);
                    write_frame(&mut write_half, &ev_msg).await?;
                }
            }
        }
    }
}

fn app_message(stream_id: i32, smt: &str, payload: &serde_json::Value, terminate: bool) -> Message {
    let body = serde_json::to_vec(payload).unwrap();
    Message::new(bytes::Bytes::from(body))
        .add_header(int32(":message-type", MT_APPLICATION_MESSAGE))
        .add_header(int32(
            ":message-flags",
            if terminate { FLAG_TERMINATE_STREAM } else { 0 },
        ))
        .add_header(int32(":stream-id", stream_id))
        .add_header(string(":content-type", "application/json"))
        .add_header(string("service-model-type", smt))
}

fn app_error(stream_id: i32, model: &str, payload: &serde_json::Value) -> Message {
    let body = serde_json::to_vec(payload).unwrap();
    Message::new(bytes::Bytes::from(body))
        .add_header(int32(":message-type", MT_APPLICATION_ERROR))
        .add_header(int32(":message-flags", FLAG_TERMINATE_STREAM))
        .add_header(int32(":stream-id", stream_id))
        .add_header(string(":content-type", "application/json"))
        .add_header(string("service-model-type", model))
}

fn parse(message: &Message) -> (i32, i32, i32, Option<String>, bytes::Bytes) {
    let mut mt = 0;
    let mut flags = 0;
    let mut sid = 0;
    let mut smt = None;
    for h in message.headers() {
        match h.name().as_str() {
            ":message-type" => mt = as_i32(h),
            ":message-flags" => flags = as_i32(h),
            ":stream-id" => sid = as_i32(h),
            "service-model-type" => {
                if let HeaderValue::String(s) = h.value() {
                    smt = Some(s.as_str().to_owned());
                }
            }
            _ => {}
        }
    }
    (mt, flags, sid, smt, message.payload().clone())
}

fn operation_header(message: &Message) -> Option<String> {
    for h in message.headers() {
        if h.name().as_str() == "operation" {
            if let HeaderValue::String(s) = h.value() {
                return Some(s.as_str().to_owned());
            }
        }
    }
    None
}

fn as_i32(h: &Header) -> i32 {
    match h.value() {
        HeaderValue::Int32(v) => *v,
        HeaderValue::Int16(v) => i32::from(*v),
        HeaderValue::Byte(v) => i32::from(*v),
        _ => 0,
    }
}

fn int32(name: &str, value: i32) -> Header {
    Header::new(name.to_owned(), HeaderValue::Int32(value))
}

fn string(name: &str, value: &str) -> Header {
    Header::new(
        name.to_owned(),
        HeaderValue::String(StrBytes::copy_from_str(value)),
    )
}

async fn read_frame(
    read_half: &mut tokio::net::unix::OwnedReadHalf,
    decoder: &mut MessageFrameDecoder,
    buf: &mut BytesMut,
) -> std::io::Result<Option<Message>> {
    loop {
        match decoder.decode_frame(&mut *buf) {
            Ok(DecodedFrame::Complete(m)) => return Ok(Some(m)),
            Ok(DecodedFrame::Incomplete) => {}
            Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e)),
        }
        let n = read_half.read_buf(buf).await?;
        if n == 0 {
            return Ok(None);
        }
    }
}

async fn write_frame(
    write_half: &mut tokio::net::unix::OwnedWriteHalf,
    message: &Message,
) -> std::io::Result<()> {
    let mut out = BytesMut::new();
    write_message_to(message, &mut out)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    write_half.write_all(&out).await?;
    write_half.flush().await
}
