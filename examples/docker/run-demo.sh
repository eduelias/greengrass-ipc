#!/usr/bin/env bash
# Demo for the greengrass-ipc crate.
#
# Brings up the AWS IoT Greengrass nucleus in Docker (auto-provisioned from your
# AWS credentials), builds a tiny component that uses greengrass-ipc (from
# crates.io), deploys it as a LOCAL component via the in-container Greengrass CLI,
# and shows it reach RUNNING (it reports RUNNING to the nucleus over IPC).
#
# Requirements: docker, aws CLI v2, git, curl. Linux/x86_64 host (or Docker able
# to run linux/amd64 images — see README for Apple Silicon).
#
# ⚠️  Creates billable AWS resources (IoT thing, IAM role/alias). Run
#     ./teardown.sh afterwards. See README.md.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
cd "$HERE"

COMPONENT_NAME="io.github.eduelias.greengrass-ipc-demo"
COMPONENT_VERSION="0.1.0"
GG_IMAGE="greengrass-nucleus:demo"
CONTAINER="ggipc-greengrass-demo"
BUILD_IMAGE="${BUILD_IMAGE:-docker.io/library/rust:1-bookworm}"
STATE_FILE="$HERE/.demo-state"

log()  { printf '\033[0;32m[demo]\033[0m %s\n' "$*"; }
warn() { printf '\033[0;33m[demo]\033[0m %s\n' "$*"; }
die()  { printf '\033[0;31m[demo] ERROR:\033[0m %s\n' "$*" >&2; exit 1; }

# ---------------------------------------------------------------------------
# 1. Preflight
# ---------------------------------------------------------------------------
command -v docker >/dev/null || die "docker not found"
command -v aws    >/dev/null || die "aws CLI not found"
docker compose version >/dev/null 2>&1 || die "docker compose plugin not found"

[ -f .env ] || die "Missing .env — copy .env.example to .env and set AWS_REGION."
[ -f greengrass-v2-credentials/credentials ] || \
  die "Missing greengrass-v2-credentials/credentials — copy the .example and add your keys."

# shellcheck disable=SC1091
set -a; . ./.env; set +a
: "${AWS_REGION:?set AWS_REGION in .env}"
export AWS_DEFAULT_REGION="$AWS_REGION"

if [ -f "$STATE_FILE" ]; then
  # shellcheck disable=SC1090
  . "$STATE_FILE"
  log "Resuming demo run: THING_NAME=$THING_NAME"
else
  SUFFIX="$(LC_ALL=C tr -dc 'a-z0-9' </dev/urandom | head -c6)"
  THING_NAME="${THING_NAME/REPLACE/$SUFFIX}"
  THING_GROUP_NAME="${THING_GROUP_NAME/REPLACE/$SUFFIX}"
  cat > "$STATE_FILE" <<EOF
THING_NAME=$THING_NAME
THING_GROUP_NAME=$THING_GROUP_NAME
AWS_REGION=$AWS_REGION
EOF
  log "Demo thing: $THING_NAME  group: $THING_GROUP_NAME  region: $AWS_REGION"
fi

tmp_env="$(mktemp)"
sed -e "s/^THING_NAME=.*/THING_NAME=$THING_NAME/" \
    -e "s/^THING_GROUP_NAME=.*/THING_GROUP_NAME=$THING_GROUP_NAME/" .env > "$tmp_env"
mv "$tmp_env" .env

# ---------------------------------------------------------------------------
# 2. Build the demo component (x86_64 Linux) using the greengrass-ipc crate
# ---------------------------------------------------------------------------
log "Building the demo component (x86_64) with greengrass-ipc from crates.io..."
docker run --rm --platform linux/amd64 \
  -v "$HERE/component":/src:Z -w /src -e CARGO_HOME=/src/.cargo-container \
  "$BUILD_IMAGE" \
  bash -c 'cargo build --release --locked 2>/dev/null || cargo build --release; strip target/release/greengrass-ipc-demo' \
  || die "component build failed"
BIN="$HERE/component/target/release/greengrass-ipc-demo"
[ -f "$BIN" ] || die "built binary not found at $BIN"

# ---------------------------------------------------------------------------
# 3. Build the Greengrass image + start the nucleus (auto-provision)
# ---------------------------------------------------------------------------
if ! docker image inspect "$GG_IMAGE" >/dev/null 2>&1; then
  log "Building the Greengrass nucleus image from the official AWS Dockerfile..."
  BUILD_DIR="$(mktemp -d)"
  git clone --depth 1 https://github.com/aws-greengrass/aws-greengrass-docker.git "$BUILD_DIR" \
    || die "failed to clone aws-greengrass-docker"
  docker build --platform linux/amd64 -t "$GG_IMAGE" "$BUILD_DIR" \
    || die "failed to build Greengrass image"
  rm -rf "$BUILD_DIR"
fi

log "Starting the Greengrass nucleus (this provisions AWS resources)..."
docker compose up -d

log "Waiting for the core device to become HEALTHY (up to ~5 min)..."
healthy=""
for _ in $(seq 1 60); do
  status="$(aws greengrassv2 get-core-device \
    --core-device-thing-name "$THING_NAME" \
    --query coreDeviceStatus --output text 2>/dev/null || true)"
  [ -n "$status" ] && log "  core device status: $status"
  if [ "$status" = "HEALTHY" ]; then healthy=1; break; fi
  sleep 5
done
[ -n "$healthy" ] || die "core device did not become HEALTHY — check: docker logs $CONTAINER"

# ---------------------------------------------------------------------------
# 4. Deploy the component locally via the Greengrass CLI (no S3)
# ---------------------------------------------------------------------------
DL="$(mktemp -d)"
RECIPE_DIR="$DL/recipes"
ARTIFACT_DIR="$DL/artifacts/$COMPONENT_NAME/$COMPONENT_VERSION"
mkdir -p "$RECIPE_DIR" "$ARTIFACT_DIR"
cp "$BIN" "$ARTIFACT_DIR/greengrass-ipc-demo"

# Strip the S3 Artifacts from the recipe for a local deployment.
python3 - "$HERE/recipe.json" "$RECIPE_DIR/${COMPONENT_NAME}-${COMPONENT_VERSION}.json" <<'PY'
import json, sys
src, dst = sys.argv[1:3]
r = json.load(open(src))
for m in r.get("Manifests", []):
    m.pop("Artifacts", None)
json.dump(r, open(dst, "w"), indent=2)
PY

log "Deploying $COMPONENT_NAME $COMPONENT_VERSION as a local component..."
docker exec "$CONTAINER" mkdir -p /tmp/ggipc-demo
docker cp "$RECIPE_DIR"   "$CONTAINER:/tmp/ggipc-demo/recipes"
docker cp "$DL/artifacts" "$CONTAINER:/tmp/ggipc-demo/artifacts"
docker exec "$CONTAINER" /greengrass/v2/bin/greengrass-cli deployment create \
  --recipeDir /tmp/ggipc-demo/recipes \
  --artifactDir /tmp/ggipc-demo/artifacts \
  --merge "${COMPONENT_NAME}=${COMPONENT_VERSION}" \
  || die "local deployment failed"
rm -rf "$DL"

log "Waiting for $COMPONENT_NAME to reach RUNNING..."
running=""
for _ in $(seq 1 40); do
  state="$(docker exec "$CONTAINER" /greengrass/v2/bin/greengrass-cli component list 2>/dev/null \
    | awk -v c="$COMPONENT_NAME" '$0 ~ c {found=1} found && /State/ {print $NF; exit}')"
  [ -n "$state" ] && log "  component state: $state"
  if [ "$state" = "RUNNING" ]; then running=1; break; fi
  sleep 3
done

echo
if [ -n "$running" ]; then
  log "✅ SUCCESS — $COMPONENT_NAME is RUNNING (reported via greengrass-ipc over IPC)."
  log "Recent component log:"
  docker exec "$CONTAINER" tail -15 "/greengrass/v2/logs/${COMPONENT_NAME}.log" 2>/dev/null \
    | sed 's/\x1b\[[0-9;]*m//g' || true
else
  warn "Component not RUNNING yet."
  warn "Inspect: docker exec $CONTAINER tail -100 /greengrass/v2/logs/${COMPONENT_NAME}.log"
fi

echo
log "Done. Run ./teardown.sh to delete the container and the AWS resources this demo created."
