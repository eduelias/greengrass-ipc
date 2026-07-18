# Releasing greengrass-ipc

Releases are published to [crates.io](https://crates.io/crates/greengrass-ipc) via
crates.io **Trusted Publishing** (OIDC) — no long-lived API token is stored in GitHub.

## One-time setup

Trusted Publishing can only be configured on a crate that already exists, so the **first** release is
published manually; every release afterwards is automated.

1. **Create a crates.io account** — sign in at <https://crates.io> with GitHub and verify your email
   (required before the first publish).

2. **First publish (manual):**

   ```bash
   cargo login            # paste a token from https://crates.io/settings/tokens
   cargo publish          # from a clean checkout at the tagged commit
   ```

3. **Configure Trusted Publishing** on crates.io: open the crate's
   **Settings → Trusted Publishing → Add** and enter:
   - Repository owner: `eduelias`
   - Repository name: `greengrass-ipc`
   - Workflow filename: `release.yml`
   - Environment: `release`

4. **Create the `release` environment** in the GitHub repo
   (**Settings → Environments → New environment → `release`**). Optionally add protection rules
   (required reviewers) so a human approves each publish.

After this, you can revoke the manual token from <https://crates.io/settings/tokens> — the workflow no
longer needs it.

## Cutting a release

1. Bump `version` in `Cargo.toml` and move the `CHANGELOG.md` **Unreleased** section under the new
   version. Commit to `main`.
2. Tag and push:

   ```bash
   git tag -a vX.Y.Z -m "greengrass-ipc vX.Y.Z"
   git push origin vX.Y.Z
   ```

The `release.yml` workflow then verifies the tag matches `Cargo.toml`, publishes to crates.io via
Trusted Publishing, and creates a GitHub release. Versioning follows
[SemVer](https://semver.org/); MSRV bumps are treated as minor releases.
