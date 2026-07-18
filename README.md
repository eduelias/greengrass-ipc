# greengrass-ipc

**An unofficial, pure-Rust async client for [AWS IoT Greengrass](https://aws.amazon.com/greengrass/) v2
on-device component IPC.**

[![CI](https://github.com/eduelias/greengrass-ipc/actions/workflows/ci.yml/badge.svg)](https://github.com/eduelias/greengrass-ipc/actions/workflows/ci.yml)
[![Crates.io](https://img.shields.io/crates/v/greengrass-ipc.svg)](https://crates.io/crates/greengrass-ipc)
[![Docs.rs](https://docs.rs/greengrass-ipc/badge.svg)](https://docs.rs/greengrass-ipc)
[![License](https://img.shields.io/crates/l/greengrass-ipc.svg)](#license)

A **pure-Rust, async** client for the **AWS IoT Greengrass v2 component IPC** service — the
on-device Unix-domain-socket API that Greengrass components use to talk to the Greengrass nucleus.

> ⚠️ **Unofficial.** Not affiliated with, endorsed by, or sponsored by Amazon. "AWS", "AWS IoT", and
> "Greengrass" are used descriptively to say what this interoperates with.

- **Pure Rust.** No C, no FFI, no `bindgen`, no linking the AWS C SDK. Just `tokio` +
  [`aws-smithy-eventstream`](https://crates.io/crates/aws-smithy-eventstream) for the frame codec.
- **Async-first**, ergonomic, typed operations.
- **IPC calls are safe from anywhere** — including from inside a subscription's event loop. (The
  official AWS C SDK forbids making IPC calls from within subscription callbacks; this crate does
  not.)
- Implements the operations the official Rust binding is missing — notably
  `SubscribeToComponentUpdates`, `DeferComponentUpdate`, `PauseComponent`, `ResumeComponent`.

> **Unofficial.** This project is community-maintained and is **not affiliated with, endorsed by, or
> sponsored by Amazon**. "AWS", "IoT Greengrass", and related marks belong to Amazon; they are used
> here only to describe compatibility.

## Status

Early development. The API surface is being built in tiers. Tier 1 (component lifecycle + update
management, local pub/sub, configuration) and Tier 2 (AWS IoT Core MQTT pub/sub) are implemented.

## Example

```rust,no_run
use greengrass_ipc::{Client, LifecycleState};
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Connects using the SVCUID + socket path env vars the nucleus injects.
    let client = Client::connect_from_env().await?;

    // Report the component healthy.
    client.update_state(LifecycleState::Running).await?;

    // React to deployments, and defer them while we're busy.
    let mut updates = client.subscribe_to_component_updates().await?;
    while let Some(event) = updates.next().await {
        if let Some(pre) = event?.pre_update_event {
            // Safe to call IPC from right here:
            client
                .defer_component_update(pre.deployment_id, Some(30_000), None)
                .await?;
        }
    }
    Ok(())
}
```

## Why this exists

The only pre-existing Rust option is AWS's official `aws-greengrass-component-sdk` (crate `gg_sdk`),
which is a **C library with thin Rust FFI bindings**: it needs `clang`/`bindgen`, is effectively
Linux-only, and omits several IPC operations (forcing consumers into a fragile generic escape hatch,
which also rejects IPC calls made from inside subscription callbacks). `greengrass-ipc` is a
from-scratch pure-Rust alternative.

## Supported operations

Being implemented in tiers (of the ~34 Greengrass IPC operations). Currently: component lifecycle &
update management (`UpdateState`, `SubscribeToComponentUpdates`, `DeferComponentUpdate`,
`Pause`/`Resume`/`RestartComponent`), configuration (`Get`/`UpdateConfiguration`,
`SubscribeToConfigurationUpdate`), local pub/sub (`Publish`/`SubscribeToTopic`), and AWS IoT Core
MQTT pub/sub (`Publish`/`SubscribeToIoTCore`).

The EventStream RPC wire protocol (framing, the Connect handshake, headers) is documented in
[`docs/WIRE_PROTOCOL.md`](docs/WIRE_PROTOCOL.md).

## Minimum Supported Rust Version (MSRV)

Rust **1.94.1**. MSRV bumps are treated as minor version changes.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE))
- MIT license ([LICENSE-MIT](LICENSE-MIT))

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the
work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
