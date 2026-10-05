"""Golden output for byte parity: runs bizon's real streaming path on captured messages.

Only the Kafka consumer and the schema-registry HTTP call are replaced; parse_encoded_messages,
the inline transform, the destination frame and to_protobuf_serialization are bizon's own code.
Each message runs alone so one failure does not hide the outcome of the others.

    cd <bizon-core checkout> && uv run python <this repo>/parity/golden.py \
        --config fixtures/parity/debezium/config.yml --capture fixtures/parity/debezium --out fixtures/parity/debezium/golden.ndjson
"""

import argparse
import base64
import json
import os
from datetime import datetime
from pathlib import Path

import orjson
import yaml
from freezegun import freeze_time
from google.cloud.bigquery import SchemaField
from pytz import UTC

from bizon.connectors.destinations.bigquery_streaming_v2.src.destination import BigQueryStreamingV2Destination
from bizon.connectors.destinations.bigquery_streaming_v2.src.proto_utils import get_proto_schema_and_class
from bizon.connectors.sources.kafka.src.config import KafkaSourceConfig
from bizon.connectors.sources.kafka.src.source import KafkaSource
from bizon.destination.models import transform_to_df_destination_records
from bizon.engine.engine import replace_env_variables_in_config
from bizon.source.models import source_records_to_df
from bizon.transform.config import TransformModel
from bizon.transform.transform import Transform

ENV_DEFAULTS = {
    "BIZON_ENV_BATCH_SIZE": "50000",
    "BIZON_ENV_CONSUMER_TIMEOUT": "30",
    "BIZON_ENV_BOOTSTRAP_SERVERS": "localhost:9092",
    "BIZON_ENV_KAFKA_USERNAME": "u",
    "BIZON_ENV_KAFKA_PASSWORD": "p",
    "BIZON_ENV_APICURIO_URL": "http://registry",
    "BIZON_ENV_APICURIO_USER": "u",
    "BIZON_ENV_APICURIO_PASSWORD": "p",
}


class FakeMessage:
    def __init__(self, m: dict):
        self.m = m
        dec = lambda v: None if v is None else base64.b64decode(v)
        self._key, self._value = dec(m["key"]), dec(m["value"])
        self._headers = None if m["headers"] is None else [(k, dec(v)) for k, v in m["headers"]]

    def topic(self): return self.m["topic"]
    def partition(self): return self.m["partition"]
    def offset(self): return self.m["offset"]
    def timestamp(self): return (1, self.m["timestamp"])
    def key(self): return self._key
    def value(self): return self._value
    def headers(self): return self._headers
    def error(self): return None


class FakeResponse:
    def __init__(self, raw: bytes | None):
        self.raw = raw
        self.status_code = 200 if raw is not None else 404

    def json(self):
        return json.loads(self.raw)


class FakeSession:
    def __init__(self, schemas: Path):
        self.schemas = schemas

    def get(self, url, auth=None):
        path = self.schemas / f"{url.rsplit('/', 1)[1]}.json"
        return FakeResponse(path.read_bytes() if path.exists() else None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--capture", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--frozen-at", default="2026-01-01T00:00:00.123456")
    args = ap.parse_args()

    for k, v in ENV_DEFAULTS.items():
        os.environ.setdefault(k, v)
    cfg = replace_env_variables_in_config(yaml.safe_load(Path(args.config).read_text()))

    source = KafkaSource.__new__(KafkaSource)
    source.config = KafkaSourceConfig.model_validate(cfg["source"])
    source.topic_map = {t.name: t.destination_id for t in source.config.topics}
    source.session = FakeSession(Path(args.capture) / "schemas")
    transform = Transform([TransformModel(**t) for t in cfg.get("transforms", [])])
    schemas = {s["destination_id"]: s["record_schema"] for s in cfg["destination"]["config"]["record_schemas"]}
    tables = {}

    def table(destination_id):
        if destination_id not in tables:
            fields = [
                SchemaField(c["name"], c["type"], mode=c.get("mode", "NULLABLE"), default_value_expression=c.get("default_value_expression"))
                for c in schemas[destination_id]
            ]
            tables[destination_id] = get_proto_schema_and_class(fields)
        return tables[destination_id]

    out = open(args.out, "w")

    def emit(m, **fields):
        out.write(json.dumps({"topic": m["topic"], "partition": m["partition"], "offset": m["offset"], **fields}) + "\n")

    def error(m, stage, e):
        emit(m, outcome="error", stage=stage, error_class=type(e).__name__, error=str(e)[:500])

    for line in open(Path(args.capture) / "messages.ndjson"):
        m = json.loads(line)
        try:
            records = source.parse_encoded_messages([FakeMessage(m)])
        except Exception as e:
            error(m, "source", e)
            continue
        if not records:
            emit(m, outcome="skipped")
            continue
        destination_id = records[0].destination_id
        try:
            df = source_records_to_df(records)
        except Exception as e:
            error(m, "frame", e)
            continue
        try:
            # Frozen only here: under freezegun fastavro builds FakeDatetime, which orjson rejects.
            with freeze_time(args.frozen_at):
                df = transform.apply_transforms(df)
        except Exception as e:
            error(m, "transform", e)
            continue
        source_data = transform_to_df_destination_records(df, datetime.now(tz=UTC))["source_data"][0]
        _, row_class = table(destination_id)
        try:
            row = BigQueryStreamingV2Destination.to_protobuf_serialization(row_class, orjson.loads(source_data))
        except Exception as e:
            error(m, "encode", e)
            continue
        emit(m, outcome="row", destination_id=destination_id, source_data=source_data, row_b64=base64.b64encode(row).decode())

    with open(Path(args.out).with_name("descriptors.ndjson"), "w") as d:
        for destination_id in sorted(schemas):
            proto_schema, _ = table(destination_id)
            d.write(json.dumps({"destination_id": destination_id, "proto_schema_b64": base64.b64encode(type(proto_schema).serialize(proto_schema)).decode()}) + "\n")


if __name__ == "__main__":
    main()
