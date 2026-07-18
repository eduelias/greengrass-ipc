# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Pure-Rust, async EventStream RPC transport for Greengrass IPC: Unix-socket connect, the
  `Connect`/`ConnectAck` handshake (with the `SVCUID` auth token), and a background read loop that
  demultiplexes frames to per-operation channels.
- `Client` with Tier 1 operations:
  - Lifecycle / updates: `update_state`, `subscribe_to_component_updates`, `defer_component_update`,
    `pause_component`, `resume_component`, `restart_component`.
  - Configuration: `get_configuration`, `update_configuration`, `subscribe_to_configuration_update`.
  - Local pub/sub: `publish_to_topic`, `subscribe_to_topic`.
- Subscriptions are exposed as an `EventStream` (`futures::Stream`); IPC calls are safe to make from
  inside a subscription loop.
- Typed error surface, including modeled service errors.
- A mock nucleus and integration tests; unit tests for framing and shape (de)serialization.
