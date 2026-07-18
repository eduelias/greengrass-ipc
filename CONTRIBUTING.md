# Contributing to greengrass-ipc

Thanks for your interest! This is an unofficial, community project. Contributions are welcome.

## Development

- Rust stable (MSRV 1.94.1). Install via [rustup](https://rustup.rs/).
- Run all checks locally before opening a PR:

  ```bash
  ./scripts/check.sh
  ```

  This runs `cargo fmt --check`, `cargo clippy -D warnings`, `cargo test`, and `cargo doc` — the same
  gates as CI.

## Guidelines

- **Keep it pure Rust.** No C, no `bindgen`, no linking the AWS C SDK.
- **Do not guess wire constants.** The EventStream RPC framing (message types, flags, headers, the
  Connect handshake) is captured, with sources, in
  [`docs/WIRE_PROTOCOL.md`](docs/WIRE_PROTOCOL.md). Implement against it.
- **Match JSON field names exactly** to the AWS model (`greengrasscoreipc/model.py` / the Java
  generated shapes). A wrong key silently fails at runtime.
- Add tests for new operations (shape round-trip + mock-nucleus integration where feasible).
- Keep public API documented; `cargo doc` must be warning-free.

## Adding an IPC operation

The pattern is: define the request/response shapes in `src/model/` (JSON field names must match the
AWS model exactly — see the Python SDK `greengrasscoreipc/model.py`), then add a typed method on
`Client` that calls `request(...)` (unary) or `subscribe(...)` (streaming) with the operation name and
model type (e.g. `aws.greengrass#PublishToIoTCore`). Add a shape round-trip unit test and, where
feasible, a mock-nucleus integration test. See existing operations in `src/client.rs` for reference.

## Licensing

By contributing you agree your contribution is dual-licensed under Apache-2.0 OR MIT.
