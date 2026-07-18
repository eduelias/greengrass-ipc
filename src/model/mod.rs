//! Typed request/response/event shapes for the Greengrass IPC operations.
//!
//! Field names and JSON encoding match the AWS model exactly (see the Python SDK
//! `greengrasscoreipc/model.py`). Renames use camelCase; blobs are base64-encoded strings.

mod config;
mod lifecycle;
mod pubsub;

pub use config::*;
pub use lifecycle::*;
pub use pubsub::*;
