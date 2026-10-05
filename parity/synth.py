"""Generates the example configs and the synthetic parity fixtures.

Writes fixtures/configs/*.yml (configs the config tests parse) and fixtures/parity/<case>/
{config.yml, messages.ndjson, schemas/} for golden.py and the Rust parity test. Run it with bizon's
environment (needs fastavro and PyYAML), then regenerate golden output:

    cd <bizon-core checkout> && uv run python <this repo>/parity/synth.py --repo <this repo>
    for c in debezium cloudevents; do
      uv run python <this repo>/parity/golden.py --config <this repo>/fixtures/parity/$c/config.yml \
        --capture <this repo>/fixtures/parity/$c --out <this repo>/fixtures/parity/$c/golden.ndjson
    done
"""

import argparse
import base64
import copy
import io
import json
import struct
from pathlib import Path

import fastavro
import yaml

CLUSTER = "my-cluster"


def _block_strings(dumper, data):
    # Inline transforms read as code in the generated configs.
    style = "|" if "\n" in data else None
    return dumper.represent_scalar("tag:yaml.org,2002:str", data, style=style)


yaml.SafeDumper.add_representer(str, _block_strings)
TEMPLATES = Path(__file__).resolve().parent.parent / "crates/bizon-stream/src/transform/templates"


def col(name, type_, mode="NULLABLE", **extra):
    return {"name": name, "type": type_, "mode": mode, **extra}


INSERTED_AT = col("__inserted_at", "TIMESTAMP", default_value_expression="CURRENT_TIMESTAMP()")
DEBEZIUM_COLUMNS = [
    col("payload", "JSON"), col("__operation", "STRING"), col("__deleted", "BOOLEAN"), col("__cluster", "STRING"),
    col("__kafka_partition", "INTEGER"), col("__kafka_offset", "INTEGER"), col("__kafka_topic", "STRING"),
    col("__schema", "JSON"), col("__event_timestamp", "TIMESTAMP"), INSERTED_AT,
]
CLOUDEVENTS_COLUMNS = [
    col("payload", "JSON"), col("__ce_type", "STRING"), col("__ce_id", "STRING"), col("__ce_time", "STRING"),
    col("__deleted", "BOOLEAN"), col("__cluster", "STRING"), col("__kafka_partition", "INTEGER"),
    col("__kafka_offset", "INTEGER"), col("__kafka_topic", "STRING"), col("__schema", "JSON"),
    col("__event_timestamp", "TIMESTAMP"), INSERTED_AT,
]


def template(name, deny_list=None):
    code = (TEMPLATES / f"{name}.py").read_text().replace("${cluster-name}", CLUSTER)
    return code.replace("${topic-column-to-remove}", repr(deny_list or {}))


def config(name, encoding, topics, keys, columns, transform, **source_extra):
    """A config shaped like the ones the bizon_streaming Helm chart renders."""
    source = {
        "name": "kafka", "stream": "topic", "sync_mode": "stream", "force_ignore_checkpoint": False,
        "topics": [{"name": t, "destination_id": d} for t, d in topics],
        "batch_size": "BIZON_ENV_BATCH_SIZE", "consumer_timeout": "BIZON_ENV_CONSUMER_TIMEOUT",
        "bootstrap_servers": "BIZON_ENV_BOOTSTRAP_SERVERS", "group_id": "my-consumer-group",
        "consumer_config": {
            "auto.offset.reset": "earliest", "enable.auto.commit": False, "session.timeout.ms": 60000,
            "security.protocol": "SASL_SSL", "partition.assignment.strategy": "cooperative-sticky",
        },
        "authentication": {
            "type": "basic", "schema_registry_url": "BIZON_ENV_APICURIO_URL",
            "schema_registry_username": "BIZON_ENV_APICURIO_USER", "schema_registry_password": "BIZON_ENV_APICURIO_PASSWORD",
            "params": {"username": "BIZON_ENV_KAFKA_USERNAME", "password": "BIZON_ENV_KAFKA_PASSWORD"},
        },
        **source_extra,
    }
    if encoding == "utf-8":
        source["message_encoding"] = "utf-8"
    return {
        "name": name,
        "source": source,
        "destination": {"name": "bigquery_streaming_v2", "config": {
            "dataset_id": "BIZON_ENV_DATASET_ID", "dataset_location": "BIZON_ENV_DATASET_LOCATION",
            "project_id": "BIZON_ENV_PROJECT_ID", "bq_max_rows_per_request": "BIZON_ENV_BQ_MAX_ROWS_PER_REQUEST",
            "max_concurrent_threads": "BIZON_ENV_BQ_MAX_CONCURRENT_THREADS", "unnest": True,
            "time_partitioning": {"type": "DAY", "field": "__inserted_at"},
            "record_schemas": [
                {"destination_id": d, "record_schema": keys + copy.deepcopy(columns), "clustering_keys": [k["name"] for k in keys]}
                for _, d in topics
            ],
        }},
        "transforms": [transform],
        "engine": {"runner": {"type": "stream", "log_level": "WARNING"}},
    }


ID_KEY = [col("id", "INTEGER", "REQUIRED")]
AVRO_TOPICS = [("app.cdc.orders", "my-project.cdc.orders"), ("app.cdc.users", "my-project.cdc.users")]
CE_TOPICS = [("app.events.orders", "my-project.events.orders")]


def example_configs(repo: Path):
    out = repo / "fixtures/configs"
    out.mkdir(parents=True, exist_ok=True)
    deny = {"app.cdc.users": ["password_hash"]}
    configs = {
        "avro-cdc": config("avro-cdc", "avro", AVRO_TOPICS, ID_KEY, DEBEZIUM_COLUMNS,
                           {"label": "debezium", "python": template("debezium", deny)}),
        "avro-cdc-builtin": config("avro-cdc-builtin", "avro", AVRO_TOPICS, ID_KEY, DEBEZIUM_COLUMNS,
                                   {"label": "debezium", "builtin": {"name": "debezium_unwrap", "cluster": CLUSTER, "columns_to_remove": deny}}),
        "cloudevents": config("cloudevents", "utf-8", CE_TOPICS, [col("accountId", "INTEGER")], CLOUDEVENTS_COLUMNS,
                              {"label": "parse_events", "python": template("cloudevents")}, skip_message_invalid_keys=True),
        "unsupported": config("unsupported", "utf-8", CE_TOPICS, [col("accountId", "INTEGER")], CLOUDEVENTS_COLUMNS,
                              {"label": "parse_events", "python": "data = {'payload': data['value']}\n"}),
    }
    for name, cfg in configs.items():
        (out / f"{name}.yml").write_text(yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True))


VALUE = {
    "type": "record",
    "name": "Value",
    "fields": [
        {"name": "id", "type": "long"},
        {"name": "name", "type": ["null", "string"], "default": None},
        {"name": "score", "type": ["null", "double"], "default": None},
        {"name": "ratio", "type": "float"},
        {"name": "flag", "type": "boolean"},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "attrs", "type": {"type": "map", "values": "long"}},
        {"name": "status", "type": {"type": "enum", "name": "Status", "symbols": ["ACTIVE", "DELETED"]}},
        {"name": "blob", "type": ["null", "bytes"], "default": None},
        {"name": "created", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "updated", "type": {"type": "long", "logicalType": "timestamp-micros"}},
        {"name": "day", "type": {"type": "int", "logicalType": "date"}},
        {"name": "ext", "type": ["null", {"type": "string", "logicalType": "uuid"}], "default": None},
        {"name": "secret", "type": ["null", "string"], "default": None},
        {"name": "nested", "type": ["null", {"type": "record", "name": "Inner", "fields": [{"name": "a", "type": "int"}]}], "default": None},
        {"name": "count", "type": "int", "connect.default": 0, "default": 0},
    ],
}


def envelope(value):
    return {
        "type": "record",
        "name": "synth.public.thing.Envelope",
        "fields": [
            {"name": "before", "type": ["null", value], "default": None},
            {"name": "after", "type": ["null", "Value"], "default": None},
            {"name": "source", "type": {"type": "record", "name": "Source", "namespace": "io.debezium", "fields": [
                {"name": "ts_ms", "type": ["null", "long"]},
                {"name": "db", "type": "string"},
            ]}},
            {"name": "op", "type": ["null", "string"]},
            {"name": "ts_ms", "type": ["null", "long"]},
        ],
        "connect.name": "synth.public.thing.Envelope",
    }


V2 = copy.deepcopy(VALUE)
V2["fields"].append({"name": "added_later", "type": ["null", "string"], "default": None})
SCHEMAS = {101: envelope(VALUE), 102: envelope(V2)}


def row(i, **over):
    r = {
        "id": i, "name": f"row {i}", "score": 1.5, "ratio": 0.1, "flag": True, "tags": ["a", "b"],
        "attrs": {"k": 1}, "status": "ACTIVE", "blob": None, "created": 1700000000123,
        "updated": 1700000000123456, "day": 19000, "ext": None, "secret": "s3cr3t", "nested": None, "count": 3,
    }
    r.update(over)
    return r


def avro(schema_id, rec):
    buf = io.BytesIO()
    fastavro.schemaless_writer(buf, fastavro.parse_schema(SCHEMAS[schema_id]), rec)
    return b"\x00" + struct.pack(">I", schema_id) + buf.getvalue()


def msg(topic, offset, key, value, headers=None, ts=1700000000000):
    b = lambda x: None if x is None else base64.b64encode(x if isinstance(x, bytes) else x.encode()).decode()
    return {
        "topic": topic, "partition": 0, "offset": offset, "timestamp_type": "create", "timestamp": ts,
        "key": b(key), "value": b(value),
        "headers": None if headers is None else [[k, b(v)] for k, v in headers],
    }


def debezium_case(repo: Path):
    def env(op, before=None, after=None, ts_ms=1700000000456, schema_id=101):
        return avro(schema_id, {"before": before, "after": after, "source": {"ts_ms": ts_ms, "db": "x"}, "op": op, "ts_ms": 1})

    k = lambda i: json.dumps({"id": i})
    a, b = "synth.a", "synth.b"
    cases = [
        (a, k(1), env("c", after=row(1))),
        (a, k(2), env("u", before=row(2), after=row(2, name="renamed"))),
        (a, k(3), env("d", before=row(3))),
        (a, k(4), env("r", after=row(4))),
        (a, k(5), None),  # tombstone
        (a, k(6), b""),  # empty value
        (a, k(7), env("c", after=row(7, name="unicode é ✓ 😀   \"q\" \\ / \x01 tab\t"))),
        (a, k(8), env("c", after=row(8, score=float("nan"), ratio=float("inf")))),
        (a, k(9), env("c", after=row(9, score=1e20, ratio=1e-7))),
        (a, k(10), env("c", after=row(10, score=-0.0, ratio=3.4028234663852886e38))),
        (a, k(11), env("c", after=row(11, score=0.1 + 0.2, ratio=1.1))),
        (a, k(12), env("c", after=row(12, created=0, updated=1700000000000000, day=0))),
        (a, k(13), env("c", after=row(13, created=-1, updated=-1, day=-1))),
        (a, k(14), env("c", after=row(14, ext="0A1B2C3D-0000-4000-8000-00000000ABCD"))),
        (a, k(15), env("c", after=row(15, blob=b"plain utf8"))),
        (a, k(16), env("c", after=row(16, blob=b"\xff\xfe"))),  # non-UTF-8 bytes
        (a, k(17), env("c", after=row(17, nested={"a": 5}, tags=[], attrs={}))),
        (a, k(18), env("c", after=row(18, id=-9223372036854775808))),
        (a, k(19), env("c", after=row(19, id=9223372036854775807))),
        (a, k(20), env(None, after=row(20))),  # null op
        (a, k(21), env("c", after=row(21), ts_ms=None)),  # null ts_ms
        (a, k(22), env("c", after=row(22), ts_ms=-1)),
        (a, k(23), env("c", after=row(23), schema_id=102)),  # schema evolution
        (a, k(24), env("c", after=dict(row(24), added_later="x"), schema_id=102)),
        (a, json.dumps({"id": "25"}), env("c", after=row(25))),  # numeric string key
        (a, json.dumps({"id": "x26"}), env("c", after=row(26))),  # bad string for INTEGER
        (a, json.dumps({"id": 27, "extra": 1}), env("c", after=row(27))),  # unknown column
        (a, "not json", env("c", after=row(28))),  # invalid key
        (a, b"\xff", env("c", after=row(29))),  # non-UTF-8 key
        (a, None, env("c", after=row(30))),  # no key: REQUIRED id missing
        (a, json.dumps([1, 2]), env("c", after=row(31))),  # key not a mapping
        (a, k(32), b"\x00\x00\x00\x00"),  # too short
        (a, k(33), b"\x01" + avro(101, {"before": None, "after": row(33), "source": {"ts_ms": 1, "db": "x"}, "op": "c", "ts_ms": 1})[1:]),
        (a, k(34), b"\x00\x00\x00\x03\xe7" + b"\x00"),  # unknown schema id 999
        (a, k(35), avro(101, {"before": None, "after": row(35), "source": {"ts_ms": 1, "db": "x"}, "op": "c", "ts_ms": 1})[:-6]),  # truncated
        ("synth.unknown", k(36), env("c", after=row(36))),  # topic without destination
        (b, k(37), env("c", after=row(37))),  # second table, no deny-list
        (b, k(38), env("d", before=row(38, secret=None))),
    ]
    messages = [msg(t, i, key, value) for i, (t, key, value) in enumerate(cases)]
    cfg = config("synth-debezium", "avro", [(a, "p.d.a"), (b, "p.d.b")], ID_KEY, DEBEZIUM_COLUMNS,
                 {"label": "debezium", "python": template("debezium", {a: ["secret"]})})
    write(repo / "fixtures/parity/debezium", cfg, messages, SCHEMAS)


def cloudevents_case(repo: Path):
    h = lambda **extra: [("ce_type", "t"), ("ce_id", "1"), *extra.items()]
    cases = [
        ('{"accountId": 1}', '{"a": 1, "b": [1, 2.5, null, true]}', h(ce_time="2026-01-01T00:00:00Z")),
        ('{"accountId": 2}', '{"x": "\\ud83d\\ude00 pair"}', h()),
        ('{"accountId": 3}', '{"x": "lone \\udf31 here"}', h()),
        ('{"accountId": 4}', '{"x": "ctl \x01 char"}', h()),
        ('{"accountId": 5}', b'{"x": "bad \xff utf8"}', h()),
        ('{"accountId": 6}', '{"big": 18446744073709551615, "huge": 123456789012345678901234567890, "neg": -9223372036854775809}', h()),
        ('{"accountId": 7}', '{"f": 1e20, "g": 1.5e-7, "h": 0.1, "i": -0.0, "j": 100.0}', h()),
        ('{"accountId": 8}', '[1, 2, 3]', h()),
        ('{"accountId": 9}', '"just a string"', h()),
        ('{"accountId": 10}', '42', h()),
        ('{"accountId": 11}', '{"a": 1}', [("ce_id", "1")]),  # missing ce_type
        ('{"accountId": 12}', '{"a": 1}', [("ce_type", "t"), ("ce_id", None)]),  # null header value
        ('{"accountId": 13}', '{"a": 1}', [("ce_type", "t"), ("ce_id", "1"), ("ce_id", "2")]),  # duplicate header
        ('"{\\"accountId\\": 14}"', '{"a": 1}', h()),  # keys as a JSON string
        ('{"accountId": "15"}', '{"a": 1}', h()),
        ('nope', '{"a": 1}', h()),
        (None, '{"a": 1}', h()),
        ('{"accountId": 18}', '{not json', h()),
        ('{"accountId": 19}', '{"a": 1}', None),  # no headers at all
        ('{"accountId": 20}', '{"k": "v", "k": "w"}', h()),  # duplicate JSON keys
        ('{"accountId": 21}', '{"a": 1}', h()),
    ]
    messages = [msg("ce.a", i, key, value, headers) for i, (key, value, headers) in enumerate(cases)]
    messages[-1]["timestamp"] = -1
    cfg = config("synth-cloudevents", "utf-8", [("ce.a", "p.d.ce")], [col("accountId", "INTEGER")], CLOUDEVENTS_COLUMNS,
                 {"label": "parse_events", "python": template("cloudevents")}, skip_message_invalid_keys=True)
    write(repo / "fixtures/parity/cloudevents", cfg, messages, {})


def write(dir: Path, cfg, messages, schemas):
    (dir / "schemas").mkdir(parents=True, exist_ok=True)
    (dir / "config.yml").write_text(yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True))
    with open(dir / "messages.ndjson", "w") as f:
        for m in messages:
            f.write(json.dumps(m) + "\n")
    for sid, s in schemas.items():
        (dir / "schemas" / f"{sid}.json").write_text(json.dumps(s, indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", required=True, type=Path)
    a = ap.parse_args()
    example_configs(a.repo)
    debezium_case(a.repo)
    cloudevents_case(a.repo)
