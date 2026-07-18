//! Error types for the Greengrass IPC client.

use std::io;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur while connecting to or communicating with the Greengrass nucleus over IPC.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The process is not running as a Greengrass component: the required environment variables
    /// (`SVCUID` and `AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT`) are not set.
    ///
    /// Callers that want to run both under and outside Greengrass can treat this as "no IPC
    /// available" and continue.
    #[error("not running under Greengrass: missing environment variable `{0}`")]
    NotUnderGreengrass(&'static str),

    /// Failed to connect to, read from, or write to the nucleus IPC socket.
    #[error("greengrass ipc I/O error: {0}")]
    Io(#[from] io::Error),

    /// The connection handshake failed: the nucleus rejected the `Connect` message (usually an
    /// invalid or expired `SVCUID`), or replied with an unexpected message.
    #[error("greengrass ipc handshake failed: {0}")]
    Handshake(String),

    /// An EventStream frame could not be encoded or decoded.
    #[error("eventstream framing error: {0}")]
    Frame(String),

    /// A request or event payload could not be serialized/deserialized to/from JSON.
    #[error("payload (de)serialization error: {0}")]
    Payload(#[from] serde_json::Error),

    /// The nucleus returned a modeled service error for an operation.
    #[error("greengrass service error [{model}]: {message}")]
    Service {
        /// The service-model type of the error (e.g. `aws.greengrass#ResourceNotFoundError`).
        model: String,
        /// The human-readable error message from the nucleus.
        message: String,
    },

    /// The connection was closed while an operation was in flight.
    #[error("greengrass ipc connection closed")]
    ConnectionClosed,
}

impl Error {
    pub(crate) fn frame(msg: impl Into<String>) -> Self {
        Error::Frame(msg.into())
    }

    pub(crate) fn handshake(msg: impl Into<String>) -> Self {
        Error::Handshake(msg.into())
    }
}
