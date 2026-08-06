# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-08-06

### Breaking

- `Error` now derives `Clone`. `Error::Io` wraps `Arc<io::Error>` and `Error::Payload` wraps
  `Arc<serde_json::Error>` so the type can be duplicated -- when a connection drops, the same error
  is handed to every operation that was in flight. `From<io::Error>` and `From<serde_json::Error>`
  still work, so `?` is unaffected; only code that destructures `Error::Io(e)` and expects an owned
  `io::Error` needs updating.
- Added `Error::Timeout`. `Error` is `#[non_exhaustive]`, so matches with a wildcard arm are
  unaffected.
- `futures-util` is no longer a runtime dependency. It was only ever used by the doc example;
  `EventStream` implements `futures_core::Stream`, and callers who want the `StreamExt`
  combinators (`.next()`) should depend on `futures-util` directly.

### Added

- `Client::closed()` -- a future that resolves when the IPC connection is lost. Previously there
  was no way to notice a nucleus restart without an operation in flight, so a component that only
  published occasionally would not find out until its next call.
- Request timeouts, default 30s, configurable with `ClientConfig::with_request_timeout` via the new
  `Client::connect_with_config`. Previously a request the nucleus accepted but never answered would
  wait forever.
- `Ping` is now answered with a `PingResponse` echoing the payload, per the EventStream RPC spec.
  The Greengrass nucleus does not currently ping components -- it only answers pings -- but other
  servers may, and an unanswered ping is grounds to drop the connection.

### Fixed

- Connection-level messages on stream-id 0 were silently discarded. `ProtocolError` and
  `InternalError` now fail every in-flight operation and close the connection; previously the read
  loop kept waiting on a socket the nucleus had already given up on.
- A failed write (`EPIPE` and friends) now shuts the connection down instead of only failing the
  one caller, so other pending operations fail immediately rather than waiting for the read side to
  notice EOF.
- `fail_all` no longer collapses non-`ConnectionClosed` errors into `Error::Frame`, which used to
  destroy the cause; it is now possible to tell an I/O failure from a framing failure.

### Changed

- `aws-smithy-eventstream` requirement relaxed from `=0.61.1` to `0.61`, so a consumer that also
  depends on an AWS SDK crate can unify on a compatible version.
- Documented the connection lifecycle: the nucleus rotates `SVCUID` on every component start, so an
  in-process reconnect can never re-authenticate. The correct response to a lost connection is to
  exit and let the nucleus restart the component.

## [0.1.1] - 2026-07-18

### Changed
- Documentation and metadata only (no API changes): the crate description now leads with
  "AWS IoT Greengrass v2 ... nucleus IPC", and the README makes the (unofficial) AWS IoT Greengrass
  association explicit.

### Added
- Release automation: a GitHub Actions workflow that publishes to crates.io via Trusted Publishing
  (OIDC, no stored token) on `v*` tags, plus `RELEASING.md`.

## [0.1.0] - 2026-07-18

### Added
- Pure-Rust, async EventStream RPC transport for Greengrass IPC: Unix-socket connect, the
  `Connect`/`ConnectAck` handshake (with the `SVCUID` auth token), and a background read loop that
  demultiplexes frames to per-operation channels.
- `Client` with Tier 1 operations:
  - Lifecycle / updates: `update_state`, `subscribe_to_component_updates`, `defer_component_update`,
    `pause_component`, `resume_component`, `restart_component`.
  - Configuration: `get_configuration`, `update_configuration`, `subscribe_to_configuration_update`.
  - Local pub/sub: `publish_to_topic`, `subscribe_to_topic`.
  - AWS IoT Core MQTT pub/sub (Tier 2): `publish_to_iot_core`, `subscribe_to_iot_core` (with the
    `QoS` enum, base64 `Blob` payloads, and `IoTCoreMessage`/`MqttMessage` events) — reuse the
    nucleus's MQTT connection, no device certificate needed.
- Subscriptions are exposed as an `EventStream` (`futures::Stream`); IPC calls are safe to make from
  inside a subscription loop.
- Typed error surface, including modeled service errors.
- A mock nucleus and integration tests; unit tests for framing and shape (de)serialization.
