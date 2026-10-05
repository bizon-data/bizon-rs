# CLAUDE.md

Guidance for agents working in this repo. `README.md` covers what the project is; `docs/DESIGN.md` covers why
it is built this way.

## Hard rules

- **Byte parity with bizon is the contract.** Any change to decoding, transforms, JSON output or proto encoding
  must keep `cargo test -p bizon-stream --test parity` green. If a divergence is intentional, document it in
  `docs/DESIGN.md` with its reason.
- **Python is the oracle, not the spec.** When unsure how something should behave, run bizon's code
  (`parity/golden.py`) instead of reasoning from its source. orjson, fastavro and protobuf's `ParseDict` all have
  quirks that only show up in bytes.
- **Never execute Python.** Transforms are native built-ins; inline `python` is only accepted when it matches a
  known template.
- **At-least-once is the delivery contract.** Do not add exactly-once machinery.
- **No real message payloads in the repo.** Fixtures are synthetic (`parity/synth.py`).

## Where things are

- **Pipeline:** `pipeline.rs` is the single per-message path (parse → decode → transform → encode). The worker
  (`worker/`) and the parity test both call it, so they cannot drift apart.
- **JSON output:** `json.rs` is the only JSON writer for values that reach a row. serde_json's own output
  differs from orjson's (`1e+20` vs `1e20`).
- **Offsets:** `worker/offsets.rs` decides what is committable. Rebalance handling is `Ctx::pre_rebalance` in
  `worker/mod.rs`.
- **Config:** `config.rs` is strict (`deny_unknown_fields`). Keys bizon also ignores are listed explicitly.

## Commands

```bash
cargo fmt --all && cargo clippy --workspace --all-targets && cargo test --workspace
cargo build --release -p bizon-stream -p fake-bqwrite && scripts/e2e-local.sh   # needs Docker
```

## Gotchas

- `gcp_auth` treats `GOOGLE_APPLICATION_CREDENTIALS` as a service-account key. With gcloud user credentials,
  run with that variable unset.
- Writing files through shell heredocs can turn `\uXXXX` escapes into literal characters. Use file-writing tools
  for content with escapes, and check generated fixtures for unexpected non-ASCII.
- Release binaries are built in a `rust:*-bookworm` container so they run on Debian 12 (glibc 2.36). A binary
  built on a newer host glibc will not start in the published image.

## Style

- Comments only for a non-obvious *why*, matching the surrounding density. Keep PR history out of code.
- PR descriptions: what changed and why, then the checks actually run.
