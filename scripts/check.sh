#!/usr/bin/env bash
# Local pre-commit / CI-parity check for greengrass-ipc.
# Runs the same gates as CI so a green run here means a green run there.
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> cargo fmt --check"
cargo fmt --all -- --check

echo "==> cargo clippy (all targets, all features, -D warnings)"
cargo clippy --all-targets --all-features -- -D warnings

echo "==> cargo test (all features)"
cargo test --all-features

echo "==> cargo doc (warnings as errors)"
RUSTDOCFLAGS="-D warnings" cargo doc --no-deps --all-features

echo "OK: all checks passed."
