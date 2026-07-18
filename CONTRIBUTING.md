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
  `.opencode/skills/greengrass-ipc-dev/reference/WIRE_PROTOCOL.md`. Implement against it.
- **Match JSON field names exactly** to the AWS model (`greengrasscoreipc/model.py` / the Java
  generated shapes). A wrong key silently fails at runtime.
- Add tests for new operations (shape round-trip + mock-nucleus integration where feasible).
- Keep public API documented; `cargo doc` must be warning-free.

## Adding an IPC operation

See the `greengrass-ipc-dev` skill (`.opencode/skills/greengrass-ipc-dev/SKILL.md`) for the
step-by-step. In short: define the shapes in `src/model/`, register the operation name + model type,
add a typed `Client` method, and test it.

## Licensing

By contributing you agree your contribution is dual-licensed under Apache-2.0 OR MIT.
