//! Discovery of the Greengrass IPC connection parameters from the environment.
//!
//! When the nucleus launches a component, it injects two environment variables into the process:
//!
//! - `AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT` — the path of the Unix domain socket the
//!   nucleus's IPC server listens on.
//! - `SVCUID` — the per-component authentication token, sent in the `Connect` handshake so the
//!   nucleus can identify and authorize this component.

use crate::error::{Error, Result};
use std::path::PathBuf;

/// Environment variable holding the IPC socket path.
pub const SOCKET_PATH_ENV: &str = "AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT";

/// Environment variable holding the component auth token.
pub const AUTH_TOKEN_ENV: &str = "SVCUID";

/// The connection parameters needed to reach and authenticate with the nucleus IPC server.
#[derive(Debug, Clone)]
pub struct IpcEnv {
    /// Path to the nucleus IPC Unix domain socket.
    pub socket_path: PathBuf,
    /// The `SVCUID` auth token for this component.
    pub auth_token: String,
}

impl IpcEnv {
    /// Reads [`IpcEnv`] from the process environment.
    ///
    /// Returns [`Error::NotUnderGreengrass`] if either required variable is missing, which lets a
    /// caller detect "not running under Greengrass" and fall back gracefully.
    pub fn from_env() -> Result<Self> {
        let socket_path =
            std::env::var_os(SOCKET_PATH_ENV).ok_or(Error::NotUnderGreengrass(SOCKET_PATH_ENV))?;
        let auth_token =
            std::env::var(AUTH_TOKEN_ENV).map_err(|_| Error::NotUnderGreengrass(AUTH_TOKEN_ENV))?;

        if auth_token.is_empty() {
            return Err(Error::NotUnderGreengrass(AUTH_TOKEN_ENV));
        }

        Ok(Self {
            socket_path: PathBuf::from(socket_path),
            auth_token,
        })
    }
}
