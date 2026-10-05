from datetime import datetime
import json

keys = data["keys"]
if isinstance(keys, str):
    keys = json.loads(keys)
timestamp = datetime.utcfromtimestamp(data['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')

partition = data['partition']
offset = data['offset']
topic = data['topic']
schema = data['schema']

data = {
    **keys,
    "payload": data["value"]["after"],
    "__before": data["value"].get("before"),
    "__ce_type": data["headers"].get("ce_type"),
    "__ce_id": data["headers"].get("ce_id"),
    "__event_timestamp": timestamp,
    "__kafka_partition": partition,
    "__kafka_offset": offset,
    "__kafka_topic": topic,
    "__schema": schema,
    "__cluster": "${cluster-name}",
    "__inserted_at": datetime.utcnow().isoformat()
}
