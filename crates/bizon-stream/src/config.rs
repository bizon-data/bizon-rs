//! The subset of bizon's YAML config the streaming worker supports, read from the exact `config.yml`
//! the chart mounts. Anything outside that subset is rejected at startup.

use std::collections::BTreeMap;

use serde::Deserialize;
use serde_yaml::Value;

use crate::proto::descriptor::Column;
use crate::transform::{Builtin, TransformSelectionError};

#[derive(Debug, thiserror::Error)]
pub enum ConfigError {
    #[error("invalid YAML: {0}")]
    Yaml(#[from] serde_yaml::Error),
    #[error("invalid config: {source} (unset environment variables: {unset})")]
    YamlWithUnsetEnv { source: serde_yaml::Error, unset: String },
    #[error("environment variable {0} referenced by env:// is not set")]
    MissingEnv(String),
    #[error("unsupported config: {0}")]
    Unsupported(String),
    #[error(transparent)]
    Transform(#[from] TransformSelectionError),
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawConfig {
    name: String,
    source: SourceConfig,
    destination: RawDestination,
    #[serde(default)]
    transforms: Vec<TransformConfig>,
    engine: EngineConfig,
    #[serde(default, rename = "monitoring")]
    _monitoring: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawDestination {
    name: String,
    config: DestinationConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct EngineConfig {
    runner: RunnerConfig,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RunnerConfig {
    #[serde(rename = "type")]
    kind: String,
    #[serde(default, rename = "log_level")]
    _log_level: Option<String>,
}

/// A transform is either a named built-in, or bizon's inline `python` matched against a known
/// template (so existing configs run unchanged).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformConfig {
    pub label: String,
    #[serde(default)]
    pub python: Option<String>,
    #[serde(default)]
    pub builtin: Option<BuiltinConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(tag = "name", rename_all = "snake_case", deny_unknown_fields)]
pub enum BuiltinConfig {
    /// Debezium envelope unwrap: `payload` is `after` (`before` for deletes), plus operation,
    /// kafka coordinates, the record schema and timestamps.
    DebeziumUnwrap {
        /// Written to `__cluster`.
        cluster: String,
        /// Per topic, payload columns to drop (and their fields in `__schema`).
        #[serde(default)]
        columns_to_remove: BTreeMap<String, Vec<String>>,
    },
    /// CloudEvents over JSON: `payload` is the message value; `ce_type`/`ce_id`/`ce_time` from headers.
    Cloudevents { cluster: String },
    /// As `cloudevents`, plus all headers in `headers`; the `ce_*` headers are optional.
    CloudeventsEnriched { cluster: String },
    /// JSON value as `payload`, with kafka coordinates and timestamps.
    JsonEvents { cluster: String },
    /// JSON change event: `payload` is `after`, `__before` is `before`, plus `ce_type`/`ce_id` headers.
    JsonCdc { cluster: String },
    /// Avro value as `payload`; `__schema` keeps the top-level fields, with record fields inlined.
    AvroEvents { cluster: String },
}

impl From<BuiltinConfig> for Builtin {
    fn from(c: BuiltinConfig) -> Self {
        match c {
            BuiltinConfig::DebeziumUnwrap {
                cluster,
                columns_to_remove,
            } => Builtin::Debezium {
                cluster,
                deny_list: columns_to_remove,
            },
            BuiltinConfig::Cloudevents { cluster } => Builtin::CloudEvents { cluster },
            BuiltinConfig::CloudeventsEnriched { cluster } => Builtin::CloudEventsEnriched { cluster },
            BuiltinConfig::JsonEvents { cluster } => Builtin::JsonEvents { cluster },
            BuiltinConfig::JsonCdc { cluster } => Builtin::JsonCdc { cluster },
            BuiltinConfig::AvroEvents { cluster } => Builtin::AvroEvents { cluster },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
pub enum MessageEncoding {
    #[serde(rename = "avro")]
    Avro,
    #[serde(rename = "utf-8")]
    Utf8,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceConfig {
    pub name: String,
    #[serde(rename = "stream")]
    _stream: Option<String>,
    pub sync_mode: String,
    #[serde(default, rename = "force_ignore_checkpoint")]
    _force_ignore_checkpoint: Option<bool>,
    /// Ignored by bizon too (no such field on KafkaSourceConfig; pydantic drops it).
    #[serde(default, rename = "timestamp_ms_name")]
    _timestamp_ms_name: Option<String>,
    #[serde(default)]
    pub topics: Vec<TopicConfig>,
    pub bootstrap_servers: String,
    #[serde(default = "default_group_id")]
    pub group_id: String,
    #[serde(default = "yes")]
    pub skip_message_empty_value: bool,
    #[serde(default)]
    pub skip_message_invalid_keys: bool,
    #[serde(default)]
    pub skip_message_on_decode_error: bool,
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_consumer_timeout")]
    pub consumer_timeout: u64,
    /// Given in full, it replaces bizon's defaults rather than merging with them.
    #[serde(default = "default_consumer_config")]
    pub consumer_config: BTreeMap<String, Value>,
    #[serde(default = "default_encoding")]
    pub message_encoding: MessageEncoding,
    pub authentication: KafkaAuth,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TopicConfig {
    pub name: String,
    pub destination_id: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaAuth {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default = "default_registry_type")]
    pub schema_registry_type: String,
    #[serde(default)]
    pub schema_registry_url: String,
    #[serde(default)]
    pub schema_registry_username: String,
    #[serde(default)]
    pub schema_registry_password: String,
    pub params: BasicAuthParams,
}

#[derive(Clone, Deserialize)]
pub struct BasicAuthParams {
    pub username: String,
    #[serde(default)]
    pub password: String,
}

impl std::fmt::Debug for BasicAuthParams {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BasicAuthParams")
            .field("username", &"<redacted>")
            .finish_non_exhaustive()
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DestinationConfig {
    pub project_id: String,
    pub dataset_id: String,
    #[serde(default = "default_location")]
    pub dataset_location: String,
    #[serde(default = "default_max_rows")]
    pub bq_max_rows_per_request: usize,
    #[serde(default = "default_threads")]
    pub max_concurrent_threads: usize,
    #[serde(default)]
    pub unnest: bool,
    #[serde(default)]
    pub time_partitioning: Option<TimePartitioning>,
    #[serde(default)]
    pub record_schemas: Vec<RecordSchema>,
    #[serde(default, rename = "buffer_size")]
    _buffer_size: Option<Value>,
    #[serde(default, rename = "buffer_flush_timeout")]
    _buffer_flush_timeout: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TimePartitioning {
    #[serde(rename = "type")]
    pub kind: String,
    pub field: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordSchema {
    pub destination_id: String,
    pub record_schema: Vec<SchemaColumn>,
    #[serde(default)]
    pub clustering_keys: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub bq_type: String,
    #[serde(default = "default_mode")]
    pub mode: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub default_value_expression: Option<String>,
}

impl RecordSchema {
    pub fn columns(&self) -> Vec<Column> {
        self.record_schema
            .iter()
            .map(|c| Column {
                name: c.name.clone(),
                bq_type: c.bq_type.clone(),
                required: c.mode == "REQUIRED",
            })
            .collect()
    }
}

fn default_group_id() -> String {
    "bizon".into()
}
fn yes() -> bool {
    true
}
fn default_batch_size() -> usize {
    100
}
fn default_consumer_timeout() -> u64 {
    10
}
fn default_encoding() -> MessageEncoding {
    MessageEncoding::Avro
}
fn default_registry_type() -> String {
    "apicurio".into()
}
fn default_location() -> String {
    "US".into()
}
fn default_max_rows() -> usize {
    5000
}
fn default_threads() -> usize {
    10
}
fn default_mode() -> String {
    "NULLABLE".into()
}
fn default_consumer_config() -> BTreeMap<String, Value> {
    [
        ("auto.offset.reset", Value::from("earliest")),
        ("enable.auto.commit", Value::from(false)),
        ("session.timeout.ms", Value::from(45000)),
        ("max.poll.interval.ms", Value::from(600000)),
        ("security.protocol", Value::from("SASL_SSL")),
    ]
    .into_iter()
    .map(|(k, v)| (k.to_string(), v))
    .collect()
}

/// A validated streaming config.
#[derive(Debug, Clone)]
pub struct Config {
    pub name: String,
    pub source: SourceConfig,
    pub destination: DestinationConfig,
    pub transform: Builtin,
}

impl Config {
    pub fn from_yaml(text: &str, env: impl Fn(&str) -> Option<String>) -> Result<Self, ConfigError> {
        let mut value: Value = serde_yaml::from_str(text)?;
        let mut unset = Vec::new();
        resolve_env(&mut value, &env, &mut unset)?;
        let raw: RawConfig = serde_yaml::from_value(value).map_err(|source| match unset.is_empty() {
            true => ConfigError::Yaml(source),
            false => ConfigError::YamlWithUnsetEnv {
                source,
                unset: unset.join(", "),
            },
        })?;

        let unsupported = |m: String| Err(ConfigError::Unsupported(m));
        if raw.source.name != "kafka" {
            return unsupported(format!("source.name {}", raw.source.name));
        }
        if raw.source.sync_mode != "stream" {
            return unsupported(format!("source.sync_mode {}", raw.source.sync_mode));
        }
        if raw.source.authentication.kind != "basic" || raw.source.authentication.schema_registry_type != "apicurio" {
            return unsupported("only basic auth with an Apicurio registry".into());
        }
        if raw.source.topics.is_empty() {
            return unsupported("source.topics is empty (streams: config is not supported)".into());
        }
        if raw.destination.name != "bigquery_streaming_v2" {
            return unsupported(format!("destination.name {}", raw.destination.name));
        }
        if !raw.destination.config.unnest {
            return unsupported("destination.config.unnest must be true".into());
        }
        if raw.engine.runner.kind != "stream" {
            return unsupported(format!("engine.runner.type {}", raw.engine.runner.kind));
        }
        let dest = &raw.destination.config;
        if dest.bq_max_rows_per_request > 10_000 {
            return unsupported("bq_max_rows_per_request must be <= 10000".into());
        }
        for topic in &raw.source.topics {
            if !dest.record_schemas.iter().any(|s| s.destination_id == topic.destination_id) {
                return unsupported(format!("no record_schema for {}", topic.destination_id));
            }
        }
        let transform = match raw.transforms.as_slice() {
            [TransformConfig {
                builtin: Some(b),
                python: None,
                ..
            }] => Builtin::from(b.clone()),
            [TransformConfig {
                label,
                python: Some(code),
                builtin: None,
            }] => Builtin::select(label, code)?,
            [t] => return unsupported(format!("transform `{}` must set exactly one of `builtin` or `python`", t.label)),
            _ => return unsupported(format!("exactly one transform is supported, got {}", raw.transforms.len())),
        };
        Ok(Self {
            name: raw.name,
            source: raw.source,
            destination: raw.destination.config,
            transform,
        })
    }

    pub fn record_schema(&self, destination_id: &str) -> Option<&RecordSchema> {
        self.destination.record_schemas.iter().find(|s| s.destination_id == destination_id)
    }
}

/// bizon's `replace_env_variables_in_config` then `env://` resolution: a dict value that is a string
/// starting with `BIZON_ENV_` becomes that variable's value (null if unset). Lists are not
/// traversed, as in Python. The value is re-read as a YAML scalar so `"50000"` validates as an int,
/// matching pydantic's coercion.
fn resolve_env(value: &mut Value, env: &impl Fn(&str) -> Option<String>, unset: &mut Vec<String>) -> Result<(), ConfigError> {
    let Value::Mapping(map) = value else { return Ok(()) };
    for (_, v) in map.iter_mut() {
        match v {
            Value::Mapping(_) => resolve_env(v, env, unset)?,
            Value::String(s) if s.starts_with("BIZON_ENV_") => {
                *v = match env(s) {
                    Some(e) => scalar(&e),
                    None => {
                        unset.push(s.clone());
                        Value::Null
                    }
                };
            }
            Value::String(s) if s.starts_with("env://") => {
                let name = s["env://".len()..].trim().to_string();
                *v = scalar(&env(&name).ok_or(ConfigError::MissingEnv(name))?);
            }
            _ => {}
        }
    }
    Ok(())
}

fn scalar(s: &str) -> Value {
    match serde_yaml::from_str::<Value>(s) {
        Ok(v @ (Value::Number(_) | Value::Bool(_))) => v,
        _ => Value::String(s.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(name: &str) -> Option<String> {
        Some(
            match name {
                "BIZON_ENV_BATCH_SIZE" => "50000",
                "BIZON_ENV_CONSUMER_TIMEOUT" => "30",
                "BIZON_ENV_BQ_MAX_ROWS_PER_REQUEST" => "5000",
                "BIZON_ENV_BQ_MAX_CONCURRENT_THREADS" => "20",
                "BIZON_ENV_PROJECT_ID" => "proj",
                "BIZON_ENV_DATASET_LOCATION" => "US",
                other => other,
            }
            .to_string(),
        )
    }

    fn load(name: &str) -> Result<Config, ConfigError> {
        let path = format!("{}/../../fixtures/configs/{name}.yml", env!("CARGO_MANIFEST_DIR"));
        Config::from_yaml(&std::fs::read_to_string(path).unwrap(), env)
    }

    #[test]
    fn parses_avro_cdc_config() {
        let c = load("avro-cdc").unwrap();
        assert_eq!(c.source.batch_size, 50000);
        assert_eq!(c.source.consumer_timeout, 30);
        assert_eq!(c.source.message_encoding, MessageEncoding::Avro);
        assert_eq!(c.source.topics.len(), 2);
        assert_eq!(c.destination.bq_max_rows_per_request, 5000);
        assert_eq!(
            c.source.consumer_config["partition.assignment.strategy"],
            Value::from("cooperative-sticky")
        );
        let schema = c.record_schema(&c.source.topics[0].destination_id).unwrap();
        assert_eq!(
            schema.record_schema.last().unwrap().default_value_expression.as_deref(),
            Some("CURRENT_TIMESTAMP()")
        );
        assert!(matches!(c.transform, Builtin::Debezium { .. }));
    }

    #[test]
    fn parses_cloudevents_config() {
        let c = load("cloudevents").unwrap();
        assert_eq!(c.source.message_encoding, MessageEncoding::Utf8);
        assert!(c.source.skip_message_invalid_keys);
        assert_eq!(
            c.transform,
            Builtin::CloudEvents {
                cluster: "my-cluster".into()
            }
        );
    }

    #[test]
    fn extracts_deny_list_and_cluster() {
        let Builtin::Debezium { cluster, deny_list } = load("avro-cdc").unwrap().transform else {
            panic!()
        };
        assert_eq!(cluster, "my-cluster");
        assert_eq!(deny_list["app.cdc.users"], vec!["password_hash".to_string()]);
    }

    #[test]
    fn rejects_unknown_inline_transforms() {
        assert!(matches!(load("unsupported"), Err(ConfigError::Transform(_))));
    }

    #[test]
    fn builtin_transform_matches_the_template_it_replaces() {
        assert_eq!(load("avro-cdc-builtin").unwrap().transform, load("avro-cdc").unwrap().transform);
        for (name, want) in [
            ("cloudevents", Builtin::CloudEvents { cluster: "c1".into() }),
            ("cloudevents_enriched", Builtin::CloudEventsEnriched { cluster: "c1".into() }),
            ("json_events", Builtin::JsonEvents { cluster: "c1".into() }),
            ("json_cdc", Builtin::JsonCdc { cluster: "c1".into() }),
            ("avro_events", Builtin::AvroEvents { cluster: "c1".into() }),
        ] {
            let c = with_transform(&format!("- label: x\n  builtin: {{name: {name}, cluster: c1}}")).unwrap();
            assert_eq!(c.transform, want);
        }
    }

    #[test]
    fn selects_each_inline_template_by_code_not_label() {
        let cluster = || "my-cluster".to_string();
        // Three of these label their transform `parse_events`.
        for (name, want) in [
            ("cloudevents", Builtin::CloudEvents { cluster: cluster() }),
            ("cloudevents-enriched", Builtin::CloudEventsEnriched { cluster: cluster() }),
            ("json-events", Builtin::JsonEvents { cluster: cluster() }),
            ("json-cdc", Builtin::JsonCdc { cluster: cluster() }),
            ("avro-events", Builtin::AvroEvents { cluster: cluster() }),
        ] {
            assert_eq!(load(name).unwrap().transform, want, "{name}");
        }
    }

    #[test]
    fn missing_bizon_env_becomes_null_and_lists_are_not_resolved() {
        let mut v: Value = serde_yaml::from_str("a: BIZON_ENV_NOPE\nb: [BIZON_ENV_X]\nc: {d: BIZON_ENV_N}").unwrap();
        let mut unset = Vec::new();
        resolve_env(&mut v, &|n| (n == "BIZON_ENV_N").then(|| "7".to_string()), &mut unset).unwrap();
        assert_eq!(unset, vec!["BIZON_ENV_NOPE"]);
        assert_eq!(v["a"], Value::Null);
        assert_eq!(v["b"][0], Value::from("BIZON_ENV_X"));
        assert_eq!(v["c"]["d"], Value::from(7));
    }

    fn with_transform(transform: &str) -> Result<Config, ConfigError> {
        let text = std::fs::read_to_string(format!("{}/../../fixtures/configs/avro-cdc.yml", env!("CARGO_MANIFEST_DIR"))).unwrap();
        let head = &text[..text.find("transforms:").unwrap()];
        let tail = &text[text.find("engine:").unwrap()..];
        Config::from_yaml(&format!("{head}transforms:\n{transform}\n{tail}"), env)
    }

    #[test]
    fn transform_must_be_builtin_xor_python() {
        assert!(with_transform("- label: x\n  builtin: {name: nope, cluster: c}").is_err());
        assert!(with_transform("- label: x\n  builtin: {name: cloudevents, cluster: c, extra: 1}").is_err());
        assert!(matches!(with_transform("- label: x"), Err(ConfigError::Unsupported(_))));
        assert!(matches!(
            with_transform("- label: x\n  python: 'data = data'\n  builtin: {name: cloudevents, cluster: c}"),
            Err(ConfigError::Unsupported(_))
        ));
    }

    #[test]
    fn unknown_keys_are_rejected() {
        let text = std::fs::read_to_string(format!("{}/../../fixtures/configs/avro-cdc.yml", env!("CARGO_MANIFEST_DIR")))
            .unwrap()
            .replace("  group_id: my-consumer-group", "  group_id: my-consumer-group\n  surprise: 1");
        assert!(matches!(
            Config::from_yaml(&text, env),
            Err(ConfigError::Yaml(_) | ConfigError::YamlWithUnsetEnv { .. })
        ));
    }
}
