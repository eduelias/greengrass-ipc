# Component demo (Docker + a real Greengrass nucleus)

Try the `greengrass-ipc` crate against a **real Greengrass nucleus** with **only your AWS
credentials**. This spins up the official AWS IoT Greengrass nucleus in Docker (auto-provisioning the
thing, thing group, and IAM role), builds a tiny component that uses `greengrass-ipc` **from
crates.io**, deploys it as a **local component**, and shows it reach **`RUNNING`** — it reports
`RUNNING` to the nucleus over IPC.

## ⚠️ This creates billable AWS resources

The demo creates an **IoT thing**, **thing group**, and an **IAM token-exchange role + alias**, and
exchanges **MQTT messages**. Costs are small but non-zero. **Always run [`./teardown.sh`](teardown.sh)**
when finished.

## Requirements

- Docker (Engine 20+) with the Compose plugin, able to run **`linux/amd64`** images.
  On Apple Silicon / ARM hosts this runs under emulation — it works but is slower.
- AWS CLI v2, `git`.
- An IAM identity allowed to provision Greengrass resources
  ([minimal IAM policy](https://docs.aws.amazon.com/greengrass/v2/developerguide/provision-minimal-iam-policy.html)).

The component is built in a `rust` container (no local Rust toolchain needed) and depends on the
published `greengrass-ipc` crate — so a green run also proves the crate works end to end.

## Quickstart

```bash
cd examples/docker

cp .env.example .env                                   # set AWS_REGION
cp greengrass-v2-credentials/credentials.example \
   greengrass-v2-credentials/credentials               # add your AWS keys

./run-demo.sh                                          # build + provision + deploy
./teardown.sh                                          # clean up AWS + container
rm -f greengrass-v2-credentials/credentials
```

Expected tail:

```
[demo] component state: RUNNING
[demo] ✅ SUCCESS — io.github.eduelias.greengrass-ipc-demo is RUNNING (reported via greengrass-ipc over IPC).
greengrass-ipc-demo: connected to the nucleus and reported RUNNING
```

## What the demo does

1. Builds `component/` (a minimal Rust bin using `greengrass-ipc = "0.1"` from crates.io) for
   `x86_64-unknown-linux-gnu` inside a `rust` container.
2. Builds the Greengrass nucleus image from the official
   [`aws-greengrass/aws-greengrass-docker`](https://github.com/aws-greengrass/aws-greengrass-docker)
   Dockerfile and starts it with `PROVISION=true`.
3. Deploys the component locally with `greengrass-cli deployment create --merge …` (no S3, no cloud
   component version).
4. Waits for the component to reach `RUNNING`.

The component code is [`component/src/main.rs`](component/src/main.rs): connect via IPC →
`update_state(RUNNING)` → stay alive with a heartbeat.

## Troubleshooting

- **Core device never HEALTHY** → `docker logs ggipc-greengrass-demo` (usually credentials/region or
  installer IAM permissions).
- **Component build fails** → ensure Docker can run `linux/amd64` and pull `rust:1-bookworm`.
- **Component not RUNNING** → `docker exec ggipc-greengrass-demo tail -100 /greengrass/v2/logs/io.github.eduelias.greengrass-ipc-demo.log`.

> Unofficial — not affiliated with, endorsed by, or sponsored by Amazon.
