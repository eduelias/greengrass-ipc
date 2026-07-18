//! EventStream RPC layer for Greengrass IPC.
//!
//! This module sits on top of [`aws_smithy_eventstream`] (which provides the binary frame codec —
//! prelude, headers, CRC32) and implements the thin RPC semantics Greengrass uses: message types,
//! message flags, the well-known headers, and the `Connect`/`ConnectAck` handshake.
//!
//! All wire constants here are transcribed from AWS's own implementations; see
//! `.opencode/skills/greengrass-ipc-dev/reference/WIRE_PROTOCOL.md`.

pub(crate) mod headers;
pub(crate) mod message;

pub(crate) use message::RpcMessage;

/// EventStream RPC message types (header `:message-type`, `Int32`).
///
/// `Application*` types are only valid on a stream (positive stream-id); all others are protocol
/// messages on stream-id 0.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub(crate) enum MessageType {
    ApplicationMessage = 0,
    ApplicationError = 1,
    Ping = 2,
    PingResponse = 3,
    Connect = 4,
    ConnectAck = 5,
    ProtocolError = 6,
    InternalError = 7,
}

impl MessageType {
    pub(crate) fn from_i32(v: i32) -> Option<Self> {
        Some(match v {
            0 => Self::ApplicationMessage,
            1 => Self::ApplicationError,
            2 => Self::Ping,
            3 => Self::PingResponse,
            4 => Self::Connect,
            5 => Self::ConnectAck,
            6 => Self::ProtocolError,
            7 => Self::InternalError,
            _ => return None,
        })
    }
}

/// EventStream RPC message flags (header `:message-flags`, `Int32`; bit flags).
pub(crate) mod flags {
    /// No flags.
    pub(crate) const NONE: i32 = 0x0;
    /// Present on a `ConnectAck` when the connection is accepted. Absent ⇒ rejected.
    pub(crate) const CONNECTION_ACCEPTED: i32 = 0x1;
    /// Closes the stream after this message.
    pub(crate) const TERMINATE_STREAM: i32 = 0x2;
}
