# bizon-rs

A native streaming worker for [bizon](https://github.com/bizon-data/bizon-core): it reads Kafka and writes to
BigQuery through the Storage Write API. It accepts the same YAML config as bizon's Python stream runner
(`source: kafka`, `destination: bigquery_streaming_v2`, `engine.runner.type: stream`) and writes
**byte-identical rows**, so it can replace a Python worker without changing tables or downstream queries.

On a production-like Debezium sample it needs about 6.6 µs of CPU per row, against roughly 35–40 µs for the
Python worker, and its resident memory stays in the tens of MiB. One Storage Write connection sustained about
118 MiB/s.

## How it works

```
Kafka ──▶ decode (Avro via schema registry | UTF-8 JSON) ──▶ built-in transform ──▶ proto row
     ◀── commit acknowledged offsets ◀── AppendRows on <table>/_default, one pipelined connection per table
```

- **Delivery is at-least-once.** Offsets are committed only past rows BigQuery has acknowledged. Cooperative
  rebalances and SIGTERM flush and commit before letting go.
- **Tables** are created and extended like bizon does it: DAY partitioning, clustering, and added columns only.
  Rows over 8 MiB go through a load job.
- **Memory is bounded by bytes in flight**, not by a record count.

## Transforms

Two built-ins cover the common streaming shapes:

```yaml
transforms:
  - label: debezium
    builtin:
      name: debezium_unwrap        # payload = after (before for deletes), operation, kafka coordinates,
      cluster: my-cluster          #   record schema, event and insert timestamps
      columns_to_remove:           # optional, per topic
        app.cdc.users: [password_hash]
```

```yaml
transforms:
  - label: events
    builtin: {name: cloudevents, cluster: my-cluster}   # payload = value, ce_type/ce_id/ce_time from headers
```

Configs that carry the equivalent inline `python` transforms (the templates in
`crates/bizon-stream/src/transform/templates/`) run unchanged. Any other inline Python is rejected at startup:
bizon-rs never executes Python. Full examples are in `fixtures/configs/`.

## Install

Release binaries (linux x86_64 and aarch64) and a container image are published from tags:

```bash
docker run --rm ghcr.io/bizon-data/bizon-rs:<version> --help
```

To build from source, you need Rust 1.87+ and a C toolchain; librdkafka and protoc are vendored:

```bash
cargo build --release -p bizon-stream   # target/release/bizon-stream
```

## Run

```bash
bizon-stream check-config --config config.yml   # validate a config against what bizon-rs supports
bizon-stream run --config config.yml            # the worker
bizon-stream capture --config config.yml --since 1h --out ./capture   # sample messages for parity tests
```

`BIZON_ENV_*` values in the config are resolved from the environment, as in bizon. Settings that are not part of
bizon's config come from `BIZON_RS_*` variables:

| Variable | Default | Meaning |
|---|---|---|
| `BIZON_RS_INFLIGHT_BYTES` | 64 MiB | Encoded bytes in flight before consuming pauses |
| `BIZON_RS_LINGER_MS` | 1000 | Max time a partial batch waits; capped at `consumer_timeout` |
| `BIZON_RS_QUEUE_KBYTES` | 16384 | librdkafka `queued.max.messages.kbytes` |
| `BIZON_RS_HEALTH_PORT` | 8080 | `/healthz`, `/readyz`, `/metrics` |
| `BIZON_RS_DRAIN_SECS` | 20 | SIGTERM drain budget |
| `BIZON_RS_ENSURE_TABLES` | true | Create and extend tables before appending |
| `BIZON_RS_BQ_WRITE_ENDPOINT` / `BIZON_RS_BQ_REST_ENDPOINT` | Google | Overrides, e.g. `http://127.0.0.1:50051` for `fake-bqwrite` |

Offsets are committed only when `ENVIRONMENT=production`, matching bizon. Google credentials come from the usual
sources: the metadata server / Workload Identity, or a service-account key in `GOOGLE_APPLICATION_CREDENTIALS`.
If `DD_AGENT_HOST` is set, counters are also sent to DogStatsD.

## Correctness: parity with bizon

The acceptance test is that each message produces exactly the same protobuf row bytes as bizon's Python worker.
`parity/golden.py` runs bizon's own code path on a set of messages: decoding, the inline transform, the
destination frame and proto serialization. The Rust parity test then compares every outcome and every row byte
for byte. The synthetic edge cases in `fixtures/parity/` run with `cargo test`; real captures can be checked
with:

```bash
PARITY_CAPTURE=./capture PARITY_CONFIG=$PWD/config.yml cargo test -p bizon-stream --test parity
```

See `docs/DESIGN.md` for the design and the deliberate divergences.

## Develop

```bash
cargo test --workspace
cargo clippy --workspace --all-targets
cargo build --release -p bizon-stream -p fake-bqwrite && scripts/e2e-local.sh   # Redpanda end to end, needs Docker
```

`scripts/e2e-local.sh` runs a normal stream, a SIGTERM drain, a `kill -9` with restart, and a 1 → 2 → 1
rebalance under load against a local Redpanda and `fake-bqwrite`. It asserts that no row is lost and that the
group ends with zero lag.

To regenerate fixtures and golden output, use bizon-core's environment:

```bash
cd <bizon-core checkout> && uv sync --extra kafka --extra bigquery && uv pip install freezegun
uv run python <bizon-rs>/parity/synth.py --repo <bizon-rs>      # then golden.py, see its docstring
```

## License

GPL-3.0, like bizon-core. The protobuf definitions vendored under `proto/` are Google's, under Apache-2.0.
