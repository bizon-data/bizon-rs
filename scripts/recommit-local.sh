#!/usr/bin/env bash
# Offset expiry on a quiet topic while no group member subscribes to it (KIP-211), with and without
# BIZON_RS_RECOMMIT_SECS. Apache Kafka (KRaft) runs with offset retention cut to 1 minute; a console
# consumer on another topic keeps the group non-empty, like a group shared by several pipelines.
# Expected: offsets survive step 1 and are gone after step 2. Requires docker; run from the repo root
# after `cargo build --release -p bizon-stream -p fake-bqwrite`.
set -euo pipefail
cd "$(dirname "$0")/.."
BIN=target/release; WORK=$(mktemp -d); G=recommit-group
cleanup() { kill ${W:-} ${FAKE:-} ${RPK:-} 2>/dev/null || true; docker rm -f rs-recommit-kafka >/dev/null 2>&1 || true; }
trap cleanup EXIT
docker run -d --name rs-recommit-kafka -p 29094:29094 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:29094,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:29094 -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 -e KAFKA_OFFSETS_RETENTION_MINUTES=1 \
  -e KAFKA_OFFSETS_RETENTION_CHECK_INTERVAL_MS=5000 apache/kafka:3.8.0 >/dev/null
K="docker exec rs-recommit-kafka /opt/kafka/bin"
until $K/kafka-topics.sh --bootstrap-server localhost:29094 --list >/dev/null 2>&1; do sleep 1; done
$K/kafka-topics.sh --bootstrap-server localhost:29094 --create --topic quiet --partitions 2 >/dev/null
$K/kafka-topics.sh --bootstrap-server localhost:29094 --create --topic busy --partitions 1 >/dev/null
for i in $(seq 1 10); do printf 'ce_type:t,ce_id:1\t{"accountId": %d}\t{"n": %d}\n' $i $i; done | docker exec -i rs-recommit-kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:29094 --topic quiet --property parse.key=true --property parse.headers=true \
  --property key.separator=$'\t' --property headers.delimiter=$'\t' >/dev/null
python3 - "$WORK/config.yml" <<'PY'
import sys, yaml
cfg = yaml.safe_load(open("fixtures/parity/cloudevents/config.yml"))
cfg["name"] = "recommit"
cfg["source"]["topics"] = [{"name": "quiet", "destination_id": "p.d.ce"}]
cfg["source"]["bootstrap_servers"] = "localhost:29094"
cfg["source"]["group_id"] = "recommit-group"
cfg["source"]["consumer_config"] = {"auto.offset.reset": "earliest", "enable.auto.commit": False,
    "security.protocol": "PLAINTEXT", "partition.assignment.strategy": "cooperative-sticky", "session.timeout.ms": 10000}
yaml.safe_dump(cfg, open(sys.argv[1], "w"), sort_keys=False)
PY
$BIN/fake-bqwrite --addr 127.0.0.1:50063 >"$WORK/fake.log" 2>&1 & FAKE=$!
# Keeps the group non-empty without subscribing to `quiet`.
docker exec rs-recommit-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:29094 --topic busy --group $G >/dev/null 2>&1 & RPK=$!
export ENVIRONMENT=production BIZON_RS_ENSURE_TABLES=false BIZON_RS_BQ_WRITE_ENDPOINT=http://127.0.0.1:50063 BIZON_RS_HEALTH_PORT=18095 \
  BIZON_RS_LINGER_MS=200 BIZON_ENV_BATCH_SIZE=500 BIZON_ENV_CONSUMER_TIMEOUT=5 BIZON_ENV_BOOTSTRAP_SERVERS=localhost:29094 \
  BIZON_ENV_KAFKA_USERNAME=u BIZON_ENV_KAFKA_PASSWORD=p BIZON_ENV_APICURIO_URL=http://unused BIZON_ENV_APICURIO_USER=u \
  BIZON_ENV_APICURIO_PASSWORD=p BIZON_ENV_PROJECT_ID=p BIZON_ENV_DATASET_ID=d BIZON_ENV_DATASET_LOCATION=US \
  BIZON_ENV_BQ_MAX_ROWS_PER_REQUEST=500 BIZON_ENV_BQ_MAX_CONCURRENT_THREADS=4
offsets() { $K/kafka-consumer-groups.sh --bootstrap-server localhost:29094 --describe --group $G 2>/dev/null | awk '$2=="quiet" {printf "p%s=%s ", $3, $4} END{print ""}'; }
run() {  # $1 = recommit secs, $2 = seconds to run, $3 = hostname
  HOSTNAME=$3 BIZON_RS_RECOMMIT_SECS=$1 $BIN/bizon-stream run --config "$WORK/config.yml" >>"$WORK/worker.log" 2>&1 & W=$!
  sleep "$2"; kill -TERM $W; wait $W || true
}
echo "0. consume the 10 messages and commit"
run 0 15 w0 >/dev/null; echo "   committed: $(offsets)"
echo "1. idle 90 s with recommit every 10 s, stop, wait 25 s (static member leaves after its 10 s session)"
run 10 90 w1 >/dev/null; sleep 25; echo "   committed: $(offsets)"
echo "2. idle 90 s with recommit off, stop, wait 25 s"
run 0 90 w2 >/dev/null; sleep 25; echo "   committed: $(offsets)"
