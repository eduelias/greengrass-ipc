//! Well-known EventStream RPC header names used by Greengrass IPC.

/// `:message-type` (Int32) — see [`super::MessageType`].
pub(crate) const MESSAGE_TYPE: &str = ":message-type";
/// `:message-flags` (Int32) — see [`super::flags`].
pub(crate) const MESSAGE_FLAGS: &str = ":message-flags";
/// `:stream-id` (Int32) — 0 for protocol messages, positive for operation streams.
pub(crate) const STREAM_ID: &str = ":stream-id";
/// `:content-type` (String) — always `application/json` for Greengrass.
pub(crate) const CONTENT_TYPE: &str = ":content-type";
/// `:version` (String) — protocol version, sent on `Connect`.
pub(crate) const VERSION: &str = ":version";
/// `service-model-type` (String) — the request/response/event shape name.
pub(crate) const SERVICE_MODEL_TYPE: &str = "service-model-type";
/// `operation` (String) — the operation name, sent on the first message of a stream.
pub(crate) const OPERATION: &str = "operation";

/// The content type used for all Greengrass IPC payloads.
pub(crate) const CONTENT_TYPE_JSON: &str = "application/json";

/// The EventStream RPC protocol version Greengrass expects on `Connect`.
pub(crate) const PROTOCOL_VERSION: &str = "0.1.0";
