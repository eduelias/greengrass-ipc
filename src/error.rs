//! Error types for the Greengrass IPC client.

use std::io;
use std::sync::Arc;
use std::time::Duration;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;

/// Errors that can occur while connecting to or communicating with the Greengrass nucleus over IPC.
///
/// `Error` is [`Clone`]: when a connection drops, the same error is delivered to every operation
/// that was in flight, so it has to be duplicable. [`io::Error`] and [`serde_json::Error`] are not
/// themselves `Clone`, so they are wrapped in an [`Arc`].
#[derive(Debug, Clone, thiserror::Error)]
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
    Io(Arc<io::Error>),

    /// The connection handshake failed: the nucleus rejected the `Connect` message (usually an
    /// invalid or expired `SVCUID`), or replied with an unexpected message.
    #[error("greengrass ipc handshake failed: {0}")]
    Handshake(String),

    /// An EventStream frame could not be encoded or decoded.
    #[error("eventstream framing error: {0}")]
    Frame(String),

    /// A request or event payload could not be serialized/deserialized to/from JSON.
    #[error("payload (de)serialization error: {0}")]
    Payload(Arc<serde_json::Error>),

    /// The nucleus returned a modeled service error for an operation.
    #[error("greengrass service error [{model}]: {message}")]
    Service {
        /// The service-model type of the error (e.g. `aws.greengrass#ResourceNotFoundError`).
        model: String,
        /// The human-readable error message from the nucleus.
        message: String,
    },

    /// The nucleus did not answer a request within the configured timeout.
    ///
    /// The request was sent and the connection is still believed to be alive; the nucleus simply
    /// never replied on that stream. The operation is **not** retried automatically, because the
    /// nucleus may have already applied it.
    #[error("greengrass ipc request `{operation}` timed out after {timeout:?}")]
    Timeout {
        /// The operation that timed out (e.g. `aws.greengrass#UpdateState`).
        operation: String,
        /// How long the client waited.
        timeout: Duration,
    },

    /// The connection was closed while an operation was in flight, or the nucleus reported a
    /// connection-level protocol error.
    ///
    /// # Do not reconnect in-process
    ///
    /// The nucleus issues every component a fresh `SVCUID` each time it starts that component.
    /// A running process only ever sees the token it was `exec`'d with, and there is no way to
    /// refresh it: [`crate::Client::connect_from_env`] re-reads the environment, but the
    /// environment of a live process does not change. Reconnecting after the nucleus has restarted
    /// therefore authenticates with a token the nucleus no longer honours, and fails forever.
    ///
    /// The correct response is to **exit the process**. The nucleus restarts its components when it
    /// comes back, and the replacement gets a valid token. Lingering can be actively harmful: a
    /// process that holds an exclusive resource (a serial port opened with `TIOCEXCL`, a lock file,
    /// a listening socket) will block its own replacement from starting.
    ///
    /// See [`crate::Client::closed`] for detecting this without an operation in flight.
    #[error("greengrass ipc connection closed")]
    ConnectionClosed,
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::Io(Arc::new(err))
    }
}

impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Payload(Arc::new(err))
    }
}

impl Error {
    pub(crate) fn frame(msg: impl Into<String>) -> Self {
        Error::Frame(msg.into())
    }

    pub(crate) fn handshake(msg: impl Into<String>) -> Self {
        Error::Handshake(msg.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn errors_are_cloneable() {
        // `fail_all` hands the same error to every in-flight operation, so this has to hold for
        // every variant -- including the ones wrapping non-Clone foreign types.
        let io = Error::from(io::Error::new(io::ErrorKind::BrokenPipe, "gone"));
        let payload = Error::from(serde_json::from_str::<u32>("nope").unwrap_err());

        for error in [
            Error::NotUnderGreengrass("SVCUID"),
            io,
            Error::handshake("rejected"),
            Error::frame("bad frame"),
            payload,
            Error::Service {
                model: "aws.greengrass#ResourceNotFoundError".into(),
                message: "nope".into(),
            },
            Error::Timeout {
                operation: "aws.greengrass#UpdateState".into(),
                timeout: Duration::from_secs(30),
            },
            Error::ConnectionClosed,
        ] {
            assert_eq!(error.clone().to_string(), error.to_string());
        }
    }

    #[test]
    fn io_errors_keep_their_kind_through_the_arc() {
        // Wrapping in Arc must not flatten the cause into a string -- callers still branch on kind.
        let error = Error::from(io::Error::new(io::ErrorKind::PermissionDenied, "denied"));
        match error {
            Error::Io(inner) => assert_eq!(inner.kind(), io::ErrorKind::PermissionDenied),
            other => panic!("expected Io, got {other:?}"),
        }
    }
}
