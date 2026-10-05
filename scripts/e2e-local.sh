#!/usr/bin/env bash
# End-to-end check on a local Redpanda with the fake BigQuery write server: normal run, SIGTERM
# drain, kill -9 and restart. Asserts every produced row was appended at least once and the
# consumer group ends with zero lag. Requires docker; run from the repo root after
# `cargo build --release -p bizon-stream -p fake-bqwrite`.
set -euo pipefail

N=${N:-20000}
PARTITIONS=6
TOPIC=e2e.ce
WORK=$(mktemp -d)
BIN=target/release

cleanup() {
  kill "${FAKE_PID:-}" "${WORKER_PID:-}" 2>/dev/null || true
  docker rm -f rs-e2e-redpanda >/dev/null 2>&1 || true
}
trap cleanup EXIT

docker run -d --name rs-e2e-redpanda -p 29092:29092 redpandadata/redpanda:latest redpanda start --mode dev-container --smp 1 \
  --kafka-addr internal://0.0.0.0:9092,external://0.0.0.0:29092 \
  --advertise-kafka-addr internal://localhost:9092,external://localhost:29092 >/dev/null
until docker exec rs-e2e-redpanda rpk cluster health 2>/dev/null | grep -q 'Healthy:.*true'; do sleep 1; done
docker exec rs-e2e-redpanda rpk topic create $TOPIC -p $PARTITIONS >/dev/null

python3 - "$WORK/config.yml" "$TOPIC" <<'PY'
import sys, yaml
cfg = yaml.safe_load(open("fixtures/parity/cloudevents/config.yml"))
cfg["name"] = "e2e"
cfg["source"]["topics"] = [{"name": sys.argv[2], "destination_id": "p.d.ce"}]
cfg["source"]["bootstrap_servers"] = "localhost:29092"
cfg["source"]["group_id"] = "e2e-group"
cfg["source"]["consumer_config"] = {"auto.offset.reset": "earliest", "enable.auto.commit": False,
    "security.protocol": "PLAINTEXT", "partition.assignment.strategy": "cooperative-sticky"}
yaml.safe_dump(cfg, open(sys.argv[1], "w"), sort_keys=False)
PY

produce() {  # $1 = first id, $2 = count
  python3 -c "
import json, sys
for i in range($1, $1 + $2):
    print(json.dumps({'accountId': i}) + '\t' + json.dumps({'n': i, 'body': 'x' * 200}))
" | docker exec -i rs-e2e-redpanda rpk topic produce $TOPIC -f '%k\t%v\n' -H ce_type:demo -H ce_id:1 >/dev/null
}

# HOSTNAME gives a static group.instance.id, so a restart rejoins without waiting out the session.
export HOSTNAME=e2e-host ENVIRONMENT=production BIZON_RS_ENSURE_TABLES=false BIZON_RS_BQ_WRITE_ENDPOINT=http://127.0.0.1:50061 \
  BIZON_RS_HEALTH_PORT=18080 BIZON_RS_LINGER_MS=200 \
  BIZON_ENV_BATCH_SIZE=500 BIZON_ENV_CONSUMER_TIMEOUT=5 BIZON_ENV_BOOTSTRAP_SERVERS=localhost:29092 \
  BIZON_ENV_KAFKA_USERNAME=u BIZON_ENV_KAFKA_PASSWORD=p BIZON_ENV_APICURIO_URL=http://unused \
  BIZON_ENV_APICURIO_USER=u BIZON_ENV_APICURIO_PASSWORD=p BIZON_ENV_PROJECT_ID=p BIZON_ENV_DATASET_ID=d \
  BIZON_ENV_DATASET_LOCATION=US BIZON_ENV_BQ_MAX_ROWS_PER_REQUEST=500 BIZON_ENV_BQ_MAX_CONCURRENT_THREADS=4

# __inserted_at is wall clock, so re-deliveries differ there; leave it out of the distinct hash.
INSERTED_AT=$(python3 -c "
import yaml
cols = yaml.safe_load(open('$WORK/config.yml'))['destination']['config']['record_schemas'][0]['record_schema']
print([c['name'] for c in cols].index('__inserted_at') + 1)")
# 20 ms per append keeps phase 2 slow enough to kill the worker mid-stream.
$BIN/fake-bqwrite --addr 127.0.0.1:50061 --ignore-field "$INSERTED_AT" --latency-ms 20 >"$WORK/fake.log" 2>&1 &
FAKE_PID=$!

distinct() { grep -o 'distinct=[0-9]*' "$WORK/fake.log" | tail -1 | cut -d= -f2; }
wait_distinct() {  # $1 = expected, $2 = timeout seconds
  for _ in $(seq 1 "$2"); do [ "$(distinct)" = "$1" ] && return 0; sleep 1; done
  echo "FAIL: expected $1 distinct rows, have $(distinct)"; tail -20 "$WORK/worker.log"; tail -3 "$WORK/fake.log"; return 1
}
lag() { docker exec rs-e2e-redpanda rpk group describe e2e-group 2>/dev/null | awk '/^TOTAL-LAG/ {print $2}'; }
start_worker() {  # $1 = health port (default 18080), $2 = hostname suffix
  local port=${1:-18080}
  HOSTNAME=e2e-host${2:-} BIZON_RS_HEALTH_PORT=$port NO_COLOR=1 $BIN/bizon-stream run --config "$WORK/config.yml" >>"$WORK/worker${2:-}.log" 2>&1 &
  WORKER_PID=$!
  until curl -sf localhost:$port/readyz >/dev/null; do sleep 0.2; done
}

echo "1. normal run + SIGTERM drain"
produce 0 $N
start_worker
echo "   readyz ok"
wait_distinct $N 60
kill -TERM $WORKER_PID; wait $WORKER_PID && echo "   SIGTERM exit 0"
sleep 1
echo "   lag after drain: $(lag)"; [ "$(lag)" = "0" ]

echo "2. kill -9 mid-stream, restart"
KILL_N=$((10 * N))
produce $N $KILL_N
start_worker
until [ "$(distinct)" -gt $((N + KILL_N / 4)) ]; do sleep 0.1; done
kill -9 $WORKER_PID; wait $WORKER_PID 2>/dev/null || true
echo "   distinct at kill: $(distinct) of $((N + KILL_N))"
start_worker
wait_distinct $((N + KILL_N)) 180
for _ in $(seq 1 30); do [ "$(lag)" = "0" ] && break; sleep 1; done
kill -TERM $WORKER_PID; wait $WORKER_PID
sleep 1
echo "   lag after restart: $(lag)"; [ "$(lag)" = "0" ]
total=$(grep -o ' rows=[0-9]*' "$WORK/fake.log" | tail -1 | cut -d= -f2)
echo "   $((N + KILL_N)) distinct rows, $total appended in total ($((total - N - KILL_N)) duplicates from the kill)"

echo "3. cooperative rebalance under load: scale 1 -> 2 -> 1"
EXPECTED=$((N + KILL_N))
start_worker 18080
FIRST=$WORKER_PID
produce $EXPECTED $((5 * N)); EXPECTED=$((EXPECTED + 5 * N))
start_worker 18081 -b
SECOND=$WORKER_PID
produce $EXPECTED $((5 * N)); EXPECTED=$((EXPECTED + 5 * N))
sleep 2
kill -TERM $SECOND; wait $SECOND && echo "   second worker left cleanly"
produce $EXPECTED $((5 * N)); EXPECTED=$((EXPECTED + 5 * N))
wait_distinct $EXPECTED 180
for _ in $(seq 1 30); do [ "$(lag)" = "0" ] && break; sleep 1; done
echo "   lag: $(lag)"; [ "$(lag)" = "0" ]
grep -h "partitions assigned\|partitions revoked" "$WORK"/worker*.log | tail -4 | sed 's/^/   /'
kill -TERM $FIRST; wait $FIRST
echo "PASS: $EXPECTED distinct rows across kill, restart and rebalances; group lag 0"
