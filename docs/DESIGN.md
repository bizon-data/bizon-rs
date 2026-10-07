# Design

## Goal

Run bizon's Kafka → BigQuery streaming pipelines with far less CPU and memory, without changing configs, tables or
row contents. Python's per-record overhead (dicts, JSON round trips, `exec` of inline transforms) and its
latency-bound loop (poll, write, wait, commit) make it the costly part of a bizon deployment. Everything here
serves one constraint: **a row written by bizon-rs must be byte-identical to the one bizon would have written.**

## Data path

1. **Consume.** librdkafka through `rdkafka`, with cooperative-sticky rebalancing and static membership
   (`group.instance.id = <group>-$HOSTNAME`), configured from the config's `consumer_config` exactly as bizon
   passes it.
2. **Parse.** `kafka/message.rs` follows bizon's `parse_encoded_messages`, in the same order of checks:
   - tombstone skip
   - key JSON parsing; a non-UTF-8 key is fatal, as in bizon
   - value decoding
   - headers
   - topic routing
3. **Decode.**
   - Avro uses Confluent 4-byte or Apicurio 8-byte framing and a schema-driven decoder (`avro.rs`) straight to
     JSON values. Field order is kept, and logical types are rendered the way fastavro and orjson render them.
     Decimals follow bizon's round trip: fastavro's rounding to `precision`, then `str(Decimal)` read back by
     `json.loads`, which gives an int only when the final exponent is 0.
   - UTF-8 JSON has bizon's surrogate and control-character sanitising.
4. **Transform.** Native built-ins (`transform/`). `__schema` depends only on the schema and topic, so it is
   computed once per (schema id, topic) and reused.
   Steps 3–5 run on a bounded window of blocking tasks (`BIZON_RS_DECODE_WINDOW`, default 4), so one busy
   partition isn't limited to one core. Messages are read, tracked and handed to the table writers in delivery
   order, so acks, commits and error handling are unchanged.
5. **Encode.** A hand-written proto2 encoder (`proto/encode.rs`) over a descriptor built like bizon's
   `proto_utils`. It reproduces both of bizon's paths: protobuf's constructor fast path, and the `ParseDict`
   fallback that, for example, turns `"30"` into an INT64 30.
6. **Append.** One long-lived AppendRows connection per table on `_default`, with requests pipelined and acks
   matched in FIFO order. Retryable failures reconnect and resend un-acked requests. Per-row errors are fatal.
7. **Commit.** `worker/offsets.rs` tracks, per partition, the contiguous prefix of offsets whose rows are
   acknowledged (or skipped), and commits only that.

## Correctness strategy

Unit tests alone can't establish parity, because the behaviour to match lives in orjson, fastavro and protobuf's
Python runtime. So:

- **Golden output.** `parity/golden.py` imports bizon and runs its real functions on each message, one at a time
  so each outcome is recorded. Only the Kafka consumer and the registry HTTP call are stubbed. The clock is frozen
  around the transform so `__inserted_at` is deterministic.
- **Parity test.** `tests/parity.rs` replays the same messages through `pipeline.rs` and compares:
  - every outcome: row, skip, or which stage failed
  - every row's bytes
  - every table descriptor's bytes
- **Synthetic edge cases** (`parity/synth.py`):
  - Avro: logical types; NaN, ±inf, -0.0 and large exponents; unicode, control characters and non-UTF-8 bytes;
    schema evolution; invalid, numeric-string and missing keys; bad framing; unknown schemas; truncated data;
    column deny-lists.
  - JSON: lone and paired surrogates; integers beyond 64 bits; missing, null and duplicate headers.
  - Per transform: string and non-mapping keys, keys that collide with template columns, non-object values,
    a missing `after` (`json_cdc`), and avro-events' `"fields" in field['type']` schema filter, which inlines
    record fields as a nested list and fails on a type name that merely contains `fields`.
- **Real captures.** The same test runs on any capture taken with `bizon-stream capture`.

## Deliberate divergences from bizon

- **A schema-registry outage stops the worker.** bizon skips such messages under `skip_message_on_decode_error`,
  which silently loses data.
- **Storage Write responses are inspected.** Per-row errors and in-response errors fail the append; bizon ignores
  the response.
- **INVALID_ARGUMENT is retried only for schema propagation.** It is retried only within 10 minutes of adding
  columns. bizon retries it for 4 minutes in every case, which delays real failures.
- **No unbounded caches.** bizon caches parsed schema ids keyed by full message bytes.
- **Batching is streaming, not per poll.** Rows are batched per table by count, bytes and linger time. Request
  limits match bizon (`bq_max_rows_per_request`, under 10 MB per request, rows over 8 MiB through a load job),
  but batch boundaries differ.
- **A scale-0 Avro decimal beyond 64 bits fails while decoding.** orjson refuses the integer later, in bizon's
  transform, so both stop on the message; only the reported stage differs.
- **NaN or Infinity in a JSON-string key fails in the transform.** Python's `json.loads` accepts them, and bizon
  then fails at encode, so both stop on the message; only the reported stage differs.
- **`__inserted_at` is taken per message from the wall clock.** Values differ from bizon's by microseconds, so
  parity tests freeze the clock on both sides.

## Delivery and shutdown

- **Rebalance.** On a cooperative revoke, the worker flushes the revoked partitions' tables, waits up to 10 s for
  their acknowledgements, and commits them. It then drops their state; a per-partition generation makes late
  acks harmless.
- **SIGTERM.** Stops consuming, drains for up to `BIZON_RS_DRAIN_SECS`, commits, and exits 0.
- **Errors.** A pipeline error drains what is safe, commits it (never the failing message), and exits non-zero.
- **Scale-down.** With static membership, a member that leaves does not send LeaveGroup, so its partitions wait
  for `session.timeout.ms` before reassignment. The same setting is what lets restarts rejoin without a rebalance.

## Performance

These are reference numbers, not guarantees:

- **CPU:** about 6.6 µs per row (3.9 µs per message including tombstones) on one core, measured on a Debezium
  sample with about 2 KB rows. bizon measured about 35–40 µs per row on the same kind of data.
- **Write throughput:** about 118 MiB/s sustained for 15 minutes on one AppendRows connection from GKE, with ack
  latency p50 146 ms and p99 250 ms.
- **Memory:** tens of MiB resident for a low-volume CDC pipeline. The ceiling is set by `BIZON_RS_INFLIGHT_BYTES`
  plus `BIZON_RS_QUEUE_KBYTES`.
