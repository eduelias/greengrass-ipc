# greengrass-ipc — Implementation Plan

> Authoritative plan for building a pure-Rust, async AWS IoT Greengrass v2 IPC client SDK.
> This document is the single source of truth for a fresh implementation session. All the
> architecture decisions below are **locked** (they were chosen deliberately during planning).

---

## 0. TL;DR

Build `greengrass-ipc`: a pure-Rust (no C/FFI/bindgen), `tokio`-async client for the Greengrass v2
component **IPC** service (the on-device Unix-domain-socket API that components use to talk to the
nucleus). Reuse `aws-smithy-eventstream` for the binary frame codec; hand-roll the thin EventStream
**RPC** layer (Connect handshake, stream multiplexing) and the per-operation JSON shapes. Ship
**Tier 1** operations first, dogfood on real hardware, then iterate to full coverage.

**Locked decisions**
- New pure-Rust SDK (not a fork of the AWS C SDK).
- Framing via `aws-smithy-eventstream` (do not hand-roll CRC32/frames).
- Tier 1 operations for v0.1, iterate to all 34.
- Name `greengrass-ipc`; license `Apache-2.0 OR MIT`; unofficial/community.
- Lives in `~/reps/du7/greengrass/greengrass-ipc/`.

---

## 1. Background: how Greengrass IPC works

- A Greengrass **component** (any process the nucleus launches) talks to the nucleus over a **Unix
  domain socket** using **EventStream RPC** (the same binary framing AWS uses in aws-crt / Kinesis
  Data Streams).
- The nucleus injects two environment variables into every component process:
  - `AWS_GG_NUCLEUS_DOMAIN_SOCKET_FILEPATH_FOR_COMPONENT` — the socket path to connect to.
  - `SVCUID` — a per-component auth token sent in the Connect handshake so the nucleus can identify
    and authorize the caller.
- After connecting, the client sends a **Connect** message (carrying `SVCUID`); the nucleus replies
  **ConnectAck**. Then the client issues operations.
- Two operation classes:
  - **request/response** (e.g. `UpdateState`, `DeferComponentUpdate`) — one message out, one in.
  - **subscription/streaming** (e.g. `SubscribeToComponentUpdates`) — one request, then a stream of
    event messages until the stream is closed.
- Operations are named `aws.greengrass#<Operation>`; request/response/event shapes are
  `aws.greengrass#<Operation>Request` / `Response` / event unions like
  `aws.greengrass#ComponentUpdatePolicyEvents`. Payloads are **JSON**.

### EventStream framing (what aws-smithy-eventstream gives us)
A frame is: `[total-length][headers-length][prelude-CRC][headers...][payload...][message-CRC]`.
Headers are typed key/value pairs. The RPC layer uses well-known headers:
- `:message-type` (int) — 0 = application message, 1 = application error, 4 = Connect, 5 = ConnectAck…
  (verify exact numeric values against the Java `eventstreamrpc` server, see §6).
- `:message-flags` (int) — bit flags, e.g. terminate-stream, connection-accepted.
- `:stream-id` (int) — multiplexes concurrent operations/streams over one socket. 0 = connection-level
  (Connect/ConnectAck); each operation gets a new positive id.
- `:content-type` (string) — `application/json`.
- `:version` (string) — protocol version on Connect (e.g. `0.1.0`).
- `operation` / `service-model-type` headers on application messages identify the operation and the
  message shape (`aws.greengrass#<Op>` / `aws.greengrass#<Op>Request`).

`aws-smithy-eventstream::frame` API to use:
- `MessageFrameDecoder` — streaming decoder for the socket read loop.
- `read_message_from` / `write_message_to` / `write_headers_to`.
- `Message` + header types from `aws-smithy-types::event_stream`.
It handles prelude/headers/CRC. It does **NOT** implement the RPC handshake/semantics — that's ours.

---

## 2. Scope: operations

There are **34** IPC operations total (full list in §7). Ship in tiers.

### Tier 1 (v0.1 — our needs + core)
- Lifecycle / updates: `UpdateState`, `SubscribeToComponentUpdates`, `DeferComponentUpdate`,
  `PauseComponent`, `ResumeComponent`, `RestartComponent`.
- Local pub/sub: `PublishToTopic`, `SubscribeToTopic`.
- Configuration: `GetConfiguration`, `UpdateConfiguration`, `SubscribeToConfigurationUpdate`.

### Tier 2
- IoT Core MQTT: `PublishToIoTCore`, `SubscribeToIoTCore`.
- Local deployments: `CreateLocalDeployment`, `CancelLocalDeployment`, `ListLocalDeployments`,
  `GetLocalDeploymentStatus`, `ListComponents`, `GetComponentDetails`.
- Metrics/validation: `PutComponentMetric`, `SendConfigurationValidityReport`,
  `SubscribeToValidateConfigurationUpdates`.

### Tier 3
- Shadows: `GetThingShadow`, `UpdateThingShadow`, `DeleteThingShadow`, `ListNamedShadowsForThing`.
- Client-device auth: `AuthorizeClientDeviceAction`, `GetClientDeviceAuthToken`,
  `VerifyClientDeviceIdentity`, `SubscribeToCertificateUpdates`.
- Secrets/misc: `GetSecretValue`, `CreateDebugPassword`, `ValidateAuthorizationToken`,
  `StopComponent`.

---

## 3. Crate layout

```
greengrass-ipc/
├── Cargo.toml                 # license = "Apache-2.0 OR MIT", keywords, categories=api-bindings
├── LICENSE-APACHE
├── LICENSE-MIT
├── README.md                  # usage + "unofficial, not affiliated with Amazon" disclaimer
├── CONTRIBUTING.md
├── CODE_OF_CONDUCT.md
├── CHANGELOG.md
├── rustfmt.toml
├── .github/workflows/ci.yml   # fmt + clippy -D warnings + test on stable (+ MSRV check)
├── src/
│   ├── lib.rs                 # crate docs, re-exports, feature gates
│   ├── error.rs               # Error enum: transport, handshake, service error, (de)serialize
│   ├── env.rs                 # read SVCUID + socket path env vars
│   ├── transport.rs           # UnixStream connect; split read/write halves
│   ├── eventstream/
│   │   ├── mod.rs
│   │   ├── headers.rs         # well-known header names + typed accessors
│   │   ├── message.rs         # RpcMessage: message-type/flags/stream-id helpers over Message
│   │   └── connection.rs      # Connect/ConnectAck handshake + read-loop demux to per-stream chans
│   ├── client.rs              # Client: connect_from_env(), request(), subscribe()
│   ├── model/
│   │   ├── mod.rs             # shared enums (LifecycleState, DeploymentStatus, ...), ErrorShape
│   │   ├── lifecycle.rs       # PreComponentUpdateEvent, ComponentUpdatePolicyEvents, ...
│   │   ├── pubsub.rs
│   │   └── config.rs
│   └── operations/
│       ├── mod.rs             # Operation trait: NAME, REQUEST_MODEL, Req/Resp/Event assoc types
│       ├── lifecycle.rs       # update_state, subscribe_to_component_updates, defer, pause, resume
│       ├── pubsub.rs
│       └── config.rs
├── examples/
│   ├── update_state.rs
│   ├── defer_component_update.rs
│   ├── pubsub.rs
│   └── config.rs
└── tests/
    ├── framing.rs             # frame/handshake round-trips
    └── mock_nucleus.rs        # integration tests against an in-repo mock nucleus server
```

---

## 4. Architecture / design

### Connection & read loop
- `Client::connect_from_env()` reads the env vars (§1), connects the `UnixStream`, performs the
  Connect→ConnectAck handshake, then spawns a background **read task**:
  - Uses `MessageFrameDecoder` to decode frames off the socket.
  - Demultiplexes by `:stream-id` to the right waiting `oneshot` (request/response) or `mpsc`
    (subscription stream). Stream-id 0 handles connection-level messages.
- Also provide `Client::connect_from_env_optional() -> Result<Option<Client>>` (or a clear typed
  "NotUnderGreengrass" error) so consumers can no-op gracefully when not running under the nucleus
  (this is important — the locker-manager daemon relies on that lenient behavior).

### Issuing operations
- A private `Client::request::<Op>(req) -> Result<Op::Response>`:
  1. allocate a new stream-id,
  2. write the application message (headers: `operation`, `service-model-type`, `:content-type`,
     `:message-type`, `:stream-id`), payload = JSON of `req`,
  3. register a `oneshot` keyed by stream-id, await the response frame, deserialize.
- A private `Client::subscribe::<Op>(req) -> Result<EventStream<Op::Event>>`:
  1. same as request but the initial response is the subscription ack,
  2. subsequent event frames on the stream-id flow into an `mpsc`; return a
     `futures::Stream` wrapper (`EventStream`) the caller can `.next().await`,
  3. dropping the stream sends a terminate-stream flagged message.
- **Crucially:** because responses are delivered via channels drained by the read task, the caller
  can issue further IPC calls from within a stream handler or anywhere — no callback-thread
  restriction (this is the ergonomic fix vs. the C SDK).

### Typed public API (target ergonomics)
```rust
use greengrass_ipc::{Client, LifecycleState};
use futures::StreamExt;

let client = Client::connect_from_env().await?;
client.update_state(LifecycleState::Running).await?;

let mut updates = client.subscribe_to_component_updates().await?;
while let Some(event) = updates.next().await {
    let event = event?;
    if let Some(pre) = event.pre_update_event {
        // Safe to call IPC here (unlike the C SDK):
        let recheck_ms = if busy { Some(30_000) } else { Some(0) };
        client.defer_component_update(pre.deployment_id, recheck_ms, None).await?;
    }
}
```

### Errors
`Error` enum: `NotUnderGreengrass`, `Connect`, `Handshake`, `Io`, `Frame`, `Serialize`,
`Deserialize`, and `Service { model: String, message: String, retryable: bool }` (from the
`aws.greengrass#*Error` shapes; `is_server`/`is_client`/retryable per the model).

### Feature flags
- Default = Tier 1. Consider `tier2`, `tier3` (or per-group features) to keep compile times/API
  surface manageable. Keep `tokio` required; consider `rt` vs `net` minimal feature set.

---

## 5. Model / shapes source of truth

Do **not** guess field names. Port from these authoritative, generated sources:
- **Python SDK** `awsiot/greengrasscoreipc/model.py` — every shape with exact JSON field casing and
  encoding (e.g. blobs are base64, enums are strings, `recheckAfterMs` is an int, `deploymentId`
  string, `isGgcRestarting` bool). Fetch raw:
  `https://raw.githubusercontent.com/aws/aws-iot-device-sdk-python-v2/main/awsiot/greengrasscoreipc/model.py`
- **Java Nucleus (local clone)** `~/reps/du7/aws-greengrass-nucleus`:
  - `src/main/java/software/amazon/awssdk/aws/greengrass/` — generated `*Request`/`*Response` shapes
    and `GeneratedAbstract*OperationHandler` for all 34 operations (operation names, model types).
  - `src/main/java/software/amazon/awssdk/eventstreamrpc/` — the **server** side of the RPC layer:
    authoritative for the Connect/ConnectAck handshake, message-type/flags numeric values, and
    stream semantics. Use it to validate our client framing.

Represent shapes as plain Rust structs with `serde` (`serde_json`), using `#[serde(rename = "...")]`
to match the JSON keys from `model.py`. Blob fields: base64 on the wire (use `base64` crate, matching
`model.py`'s `base64.b64encode`).

Tier-1 shapes to define first (names per model): `LifecycleState` (enum), `UpdateStateRequest`,
`PreComponentUpdateEvent`, `PostComponentUpdateEvent`, `ComponentUpdatePolicyEvents` (tagged union
with `preUpdateEvent`/`postUpdateEvent`), `SubscribeToComponentUpdatesRequest/Response`,
`DeferComponentUpdateRequest/Response`, `PauseComponentRequest/Response`,
`ResumeComponentRequest/Response`, `RestartComponentRequest/Response`,
`PublishToTopicRequest/Response` (+ `PublishMessage`, `BinaryMessage`, `JsonMessage`, `MessageContext`),
`SubscribeToTopicRequest/Response` (+ `SubscriptionResponseMessage`),
`GetConfigurationRequest/Response`, `UpdateConfigurationRequest/Response`,
`SubscribeToConfigurationUpdateRequest/Response` (+ `ConfigurationUpdateEvents`,
`ConfigurationUpdateEvent`).

> Consideration for later: a build-time codegen step from a Smithy model could generate all 34 ops.
> For Tier 1, hand-porting is faster and clearer. Revisit codegen when expanding to Tier 2/3.

---

## 6. Validation & testing

1. **Unit tests** — frame encode/decode round-trip; Connect/ConnectAck construction; each Tier-1
   shape (de)serializes to/from the exact JSON in the model (use fixtures captured from `model.py`
   docstrings / a real nucleus).
2. **Mock nucleus** (`tests/mock_nucleus.rs`) — a minimal in-repo EventStream-RPC server that
   accepts Connect, replies ConnectAck, echoes/serves canned responses and emits a stream event.
   Cross-check its behavior against the Java `eventstreamrpc` server and, if useful, the C server in
   `aws-greengrass-lite/modules/ggipcd`. This lets CI run full client tests with no real Greengrass.
3. **Real hardware dogfood** — swap this SDK into `~/reps/pnl-apl/apl-embedded-locker-manager`
   (behind its existing `greengrass` cargo feature, replacing `gg_sdk`), build aarch64, deploy to the
   prototype board `root@192.168.2.95` via the manual AWS-CLI dev flow (dev account `590183682129`,
   region eu-west-1, thing group `EduardosDynamicTestGroup`, thing `fp_777007_000000007bac5a8e`,
   component bucket `dev-aple-ggv2-components`, profile `pnl-apl-dev-elevated`). Confirm the component
   reaches RUNNING, subscribes to component updates, and issues a clean `DeferComponentUpdate`
   (no "may not be made from within subscription callbacks", no `error=Invalid`) — the exact flow that
   the C SDK forced a worker-thread workaround for.

---

## 7. Full operation list (from the Java Nucleus generated handlers)

```
AuthorizeClientDeviceAction        GetSecretValue                 SendConfigurationValidityReport
CancelLocalDeployment              GetThingShadow                 StopComponent
CreateDebugPassword                ListComponents                 SubscribeToCertificateUpdates
CreateLocalDeployment              ListLocalDeployments           SubscribeToComponentUpdates
DeferComponentUpdate               ListNamedShadowsForThing       SubscribeToConfigurationUpdate
DeleteThingShadow                  PauseComponent                 SubscribeToIoTCore
GetClientDeviceAuthToken           PublishToIoTCore               SubscribeToTopic
GetComponentDetails                PublishToTopic                 SubscribeToValidateConfigurationUpdates
GetConfiguration                   PutComponentMetric             UpdateConfiguration
GetLocalDeploymentStatus           RestartComponent               UpdateState
                                   ResumeComponent                UpdateThingShadow
                                                                  ValidateAuthorizationToken
                                                                  VerifyClientDeviceIdentity
```

---

## 8. Sequencing (suggested session order)

1. **Scaffold**: `Cargo.toml`, dual license files, README (+ disclaimer), CONTRIBUTING, CoC,
   CHANGELOG, rustfmt, CI workflow. Empty `lib.rs` compiles.
2. **Transport + eventstream RPC**: env discovery, UnixStream, framing wrappers, Connect/ConnectAck,
   read-loop demux. Prove the handshake against the mock nucleus (and/or the board).
3. **Client core**: `request`/`subscribe` plumbing over the read loop; `Error` type.
4. **Model (Tier 1)** + **operations (Tier 1)** + typed convenience methods on `Client`.
5. **Examples + tests** (unit + mock-nucleus integration).
6. **Dogfood** on the board via the locker-manager; fix ergonomics; tag `v0.1.0`.
7. **Iterate**: Tier 2, then Tier 3. Consider Smithy codegen when expanding.

---

## 9. Dependencies (initial)

- `tokio` (net, rt, sync, macros, io-util)
- `aws-smithy-eventstream` (pinned, e.g. `=0.61.x`) — frame codec
- `aws-smithy-types` (event_stream `Message`/headers; transitively pulled)
- `serde`, `serde_json`
- `base64` (blob fields)
- `bytes`
- `futures` (Stream for subscriptions)
- `thiserror` (errors)
- `tracing` (diagnostics)
- dev: a small mock server (built from the above), `tokio-test`

Pin `aws-smithy-eventstream` — its docs note it isn't intended for direct use (no API stability
guarantee), so we control upgrades.

---

## 10. Open items to confirm at start of implementation

- Final crate name inside the folder (default `greengrass-ipc`).
- Whether to also file an upstream issue/PR on `aws-greengrass/aws-greengrass-component-sdk` for the
  four missing ops (parallel community contribution; not a blocker).
- MSRV (suggest a recent stable, e.g. matching the daemon's toolchain).
- Whether to hand-port shapes (Tier-1, faster) or invest in Smithy codegen now (better for all 34).

---

## 11. References

- IPC overview: https://docs.aws.amazon.com/greengrass/v2/developerguide/interprocess-communication.html
- Component lifecycle IPC (UpdateState / SubscribeToComponentUpdates / DeferComponentUpdate /
  Pause/Resume): https://docs.aws.amazon.com/greengrass/v2/developerguide/ipc-component-lifecycle.html
- Defer-updates tutorial: https://docs.aws.amazon.com/greengrass/v2/developerguide/defer-component-updates-tutorial.html
- Framing crate: https://docs.rs/aws-smithy-eventstream/latest/aws_smithy_eventstream/frame/
- Shape model (Python, generated): https://github.com/aws/aws-iot-device-sdk-python-v2 →
  `awsiot/greengrasscoreipc/model.py`
- Java Nucleus (local clone, server + generated shapes): `~/reps/du7/aws-greengrass-nucleus`
- Server reference impl (C): https://github.com/aws-greengrass/aws-greengrass-lite → `modules/ggipcd`
- Official C SDK (what we are replacing in Rust): https://github.com/aws-greengrass/aws-greengrass-component-sdk
- Rust trademark policy: https://rustfoundation.org/policy/trademark-policy/
