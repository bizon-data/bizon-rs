from datetime import datetime

keys = data["keys"]
timestamp = datetime.utcfromtimestamp(data['timestamp'] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')

partition = data['partition']
offset = data['offset']
topic = data['topic']
schema = data['schema']

# Transform Avro schema to keep only the fields that are present in the payload
filtered_schema = []
for field in schema['fields']:
    if "fields" in field['type']:
        filtered_schema.append(field['type']['fields'])
    else:
        filtered_schema.append(field)


data = {
    **keys,
    "payload": data["value"],
    "__operation": None,
    "__event_timestamp": timestamp,
    "__kafka_partition": partition,
    "__kafka_offset": offset,
    "__kafka_topic": topic,
    "__schema": filtered_schema,
    "__cluster": "${cluster-name}",
    "__inserted_at": datetime.utcnow().isoformat()
}
