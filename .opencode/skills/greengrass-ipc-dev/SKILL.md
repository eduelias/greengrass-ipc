---
name: greengrass-ipc-dev
description: Use when implementing, extending, or debugging the greengrass-ipc crate (a pure-Rust AWS IoT Greengrass v2 IPC client). Covers the EventStream RPC wire protocol, the Connect/SVCUID handshake, operation/shape conventions, adding a new IPC operation, and how to verify against a mock nucleus or real device.
---

# greengrass-ipc development

Pure-Rust, async (tokio) client for the AWS IoT Greengrass v2 component IPC service. No C/FFI/bindgen.
Framing comes from `aws-smithy-eventstream`; the RPC layer + shapes are hand-written Rust.

Read `.opencode/PLAN.md` for the full plan. Read `reference/WIRE_PROTOCOL.md` for the exact,
verified wire contract (this is the source of truth for framing — do not guess message-type/flag
values or header names).

## Core mental model

- One Unix domain socket. Client connects, sends a **Connect** message, gets **ConnectAck**, then
  issues operations. Each operation uses a unique `:stream-id`; a subscription keeps its stream open
  and streams events until terminated.
- Everything is EventStream RPC frames. `aws-smithy-eventstream` handles the binary frame
  (prelude/headers/CRC). We add the RPC headers (`:message-type`, `:message-flags`, `:stream-id`,
  `:content-type`, `service-model-type`, `operation`) and the JSON payloads.
- A single background read-loop task decodes frames and routes them by `:stream-id` to per-operation
  channels (`oneshot` for request/response, `mpsc` for subscriptions). Because responses come back
  via channels, **the caller can make IPC calls from anywhere** — this is the key ergonomic win over
  the official C SDK (which forbids IPC from inside subscription callbacks).

## Adding a new IPC operation (the common task)

1. Find the exact shape in the authoritative model: the Python SDK `greengrasscoreipc/model.py`
   (fetch raw from `aws/aws-iot-device-sdk-python-v2`) and the local Java Nucleus generated shapes at
   `~/reps/du7/aws-greengrass-nucleus/src/main/java/software/amazon/awssdk/aws/greengrass/`.
2. Add request/response/event structs in `src/model/` with `serde` + `#[serde(rename = "...")]`
   matching the exact JSON keys (camelCase; blobs are base64).
3. Register the operation: name `aws.greengrass#<Op>`, request model type
   `aws.greengrass#<Op>Request` (streamed event types are the union shapes, e.g.
   `aws.greengrass#ComponentUpdatePolicyEvents`).
4. Add a typed method on `Client` (request → `request::<Op>()`; subscription → `subscribe::<Op>()`
   returning an `EventStream`).
5. Add a unit test (shape (de)serialization vs a captured JSON fixture) and, if feasible, a
   mock-nucleus integration test.

## Verifying changes

- `scripts/check.sh` runs fmt + clippy (-D warnings) + tests. Run it before every commit.
- Unit tests cover framing, handshake, and shape round-trips (no network).
- Integration tests run against the in-repo mock nucleus (`tests/`), so CI needs no real Greengrass.
- Real-device validation (optional, when hardware is available): build aarch64 and deploy as a
  Greengrass component; confirm the component reaches RUNNING and the operation works. See
  `.opencode/PLAN.md` §6 for the exact dev-account/board/CLI flow used previously.

## Guardrails

- Never guess wire constants — use `reference/WIRE_PROTOCOL.md`.
- Keep it pure Rust: no C, no `bindgen`, no linking the AWS C SDK.
- Keep `Client::connect_from_env` lenient: a clear typed error when not under Greengrass, so
  consumers can no-op.
- Match JSON field names exactly to `model.py` — a wrong key silently fails as "Invalid" at runtime.
- Pin `aws-smithy-eventstream` (its docs say it's not intended for direct use; we control upgrades).
