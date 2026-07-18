//! Pure-Rust, async client for the AWS IoT Greengrass v2 component IPC service.
//!
//! Greengrass components talk to the nucleus over a Unix-domain-socket using EventStream RPC. This
//! crate implements that protocol in pure Rust (no C/FFI) and exposes typed, async operations.
//!
//! # Quick start
//!
//! ```rust,no_run
//! use greengrass_ipc::{Client, LifecycleState};
//! use futures_util::StreamExt;
//!
//! # async fn run() -> greengrass_ipc::Result<()> {
//! let client = Client::connect_from_env().await?;
//! client.update_state(LifecycleState::Running).await?;
//!
//! let mut updates = client.subscribe_to_component_updates().await?;
//! while let Some(event) = updates.next().await {
//!     if let Some(pre) = event?.pre_update_event {
//!         client.defer_component_update(pre.deployment_id, Some(30_000), None).await?;
//!     }
//! }
//! # Ok(())
//! # }
//! ```
//!
//! This crate is **unofficial** and not affiliated with Amazon.
#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod client;
mod connection;
mod env;
mod error;
mod eventstream;
mod model;

pub use client::{Client, EventStream};
pub use env::{IpcEnv, AUTH_TOKEN_ENV, SOCKET_PATH_ENV};
pub use error::{Error, Result};
pub use model::*;
