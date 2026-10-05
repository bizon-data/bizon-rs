import json
from datetime import datetime

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
    "payload": data["value"],
    "__ce_type": data["headers"]["ce_type"],
    "__ce_id": data["headers"]["ce_id"],
    "__ce_time": data["headers"].get("ce_time"),
    "__event_timestamp": timestamp,
    "__kafka_partition": partition,
    "__kafka_offset": offset,
    "__kafka_topic": topic,
    "__schema": schema,
    "__cluster": "${cluster-name}",
    "__inserted_at": datetime.utcnow().isoformat()
}
