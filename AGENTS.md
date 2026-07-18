# greengrass-ipc

A **pure-Rust, async** client SDK for AWS IoT Greengrass v2 **inter-process communication (IPC)** —
no C, no FFI, no bindgen. This is a community, unofficial project (not affiliated with or endorsed
by Amazon).

## For AI agents / new sessions

- Read [`.opencode/PLAN.md`](.opencode/PLAN.md) first — it is the authoritative implementation plan
  (scope, architecture, wire protocol, references, sequencing, and all locked decisions).
- This is a **greenfield** crate. Nothing is implemented yet; the plan describes what to build.
- Sibling project: `../nucleus-job-plugin` (a Rust Nucleus plugin for IoT Jobs execution). They are
  independent, but the plugin will eventually depend on / validate against this SDK.

## Ground rules

- **License:** dual `Apache-2.0 OR MIT` (Rust ecosystem norm). Ship `LICENSE-APACHE` + `LICENSE-MIT`.
- **Naming/trademark:** crate name is `greengrass-ipc` (no `aws-` prefix — that namespace implies
  official ownership). "Greengrass" is used descriptively/nominatively. The README must state it is
  unofficial and not affiliated with Amazon.
- **No C/FFI.** Pure Rust. Framing comes from the `aws-smithy-eventstream` crate; everything else is
  hand-written Rust.
- **Async-first** on `tokio`. IPC calls must be safe to make from anywhere (unlike the official C SDK,
  which forbids IPC calls from inside subscription callbacks — a real bug we hit in production).
- Follow open-source standards: CI (fmt/clippy/test), CONTRIBUTING, CODE_OF_CONDUCT, CHANGELOG,
  examples, good docs.

## Why this exists

The only Rust option today is AWS's official `aws-greengrass-component-sdk` (crate `gg_sdk`): a C
library with thin Rust FFI bindings. It requires clang/bindgen, is effectively Linux-only, and is
**missing** the operations this ecosystem needs — notably `SubscribeToComponentUpdates`,
`DeferComponentUpdate`, `PauseComponent`, `ResumeComponent` — forcing consumers into a fragile
generic escape hatch. No pure-Rust Greengrass IPC client exists. This crate fills that gap.
