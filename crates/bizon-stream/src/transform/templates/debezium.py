from datetime import datetime
import orjson

partition = data['partition']
offset = data['offset']
topic = data['topic']
schema = data['schema']
keys = data['keys'] # Parse kafka message keys, we extract all of them
operation = data['value']['op']

# Retrieve timestamp when event has been emitted by debezium in Kafka
# Same behavior as Kafka Connect Debezium transfo
kafka_timestamp = datetime.utcfromtimestamp(data['value']['source']['ts_ms'] / 1000).strftime('%Y-%m-%d %H:%M:%S.%f')

# Debezium SMT implementation
deleted = False

if data['value']['op'] == 'd':
  payload = data['value']['before']
  deleted = True
else:
  payload = data['value']['after']

# Transform Avro schema to keep only the fields that are present in the payload
filtered_schema = []
for field in schema['fields']:
  if field['name'] == 'before':
    for before_type in field['type']:
      if "fields" in before_type:
        filtered_schema = before_type['fields']
        break

if not filtered_schema:
  raise Exception("No fields found in the Avro schema, please check the schema and custom transform")

# --- Filtering configuration ---
# Mapping from topic name to filter out columns from payload
# Example topic-specific filters:
# {'topic-name': ['colA', 'colB'], 'topic-name-2': ['colC', 'colD']}

TOPIC_COLUMN_TO_FILTER = ${topic-column-to-remove}

# --- Filter payload columns based on topic if applicable ---
filtered_columns = TOPIC_COLUMN_TO_FILTER.get(topic, None) # Returns a list

# Delete all filtered_columns from payload and filtered_schema
if filtered_columns:
  for filtered_column in filtered_columns:
    payload.pop(filtered_column, None)

  new_filtered_schema = []
  for f in filtered_schema:
    if f['name'] not in filtered_columns:
      new_filtered_schema.append(f)
  filtered_schema = new_filtered_schema

data = {
  **keys,
  "payload": orjson.dumps(payload).decode('utf-8'),
  "__operation": operation,
  "__deleted": deleted,
  "__cluster": "${cluster-name}",
  "__kafka_partition": partition,
  "__kafka_offset": offset,
  "__kafka_topic": topic,
  "__schema": filtered_schema,
  "__event_timestamp": kafka_timestamp,
  "__inserted_at": datetime.utcnow().isoformat()
}
