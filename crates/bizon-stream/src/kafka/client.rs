//! librdkafka settings built the way bizon's `KafkaSource.__init__` builds them.

use std::collections::{BTreeMap, HashMap};

use rdkafka::ClientConfig;
use serde_yaml::Value;

use crate::config::SourceConfig;

pub fn client_config(cfg: &SourceConfig, hostname: Option<&str>) -> ClientConfig {
    let mut c = ClientConfig::new();
    for (k, v) in &cfg.consumer_config {
        c.set(k, scalar_to_string(v));
    }
    c.set("sasl.mechanisms", "PLAIN");
    c.set("sasl.username", &cfg.authentication.params.username);
    c.set("sasl.password", &cfg.authentication.params.password);
    c.set("group.id", &cfg.group_id);
    c.set("bootstrap.servers", &cfg.bootstrap_servers);
    if c.get("group.instance.id").is_none() {
        if let Some(h) = hostname {
            c.set("group.instance.id", format!("{}-{h}", cfg.group_id));
        }
    }
    c
}

fn scalar_to_string(v: &Value) -> String {
    match v {
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        other => serde_yaml::to_string(other).unwrap_or_default().trim().to_string(),
    }
}

#[derive(serde::Deserialize)]
struct Stats {
    #[serde(default)]
    topics: HashMap<String, TopicStats>,
}

#[derive(serde::Deserialize)]
struct TopicStats {
    #[serde(default)]
    partitions: HashMap<String, PartitionStats>,
}

#[derive(serde::Deserialize)]
struct PartitionStats {
    partition: i32,
    desired: bool,
    consumer_lag: i64,
}

/// Committed-offset lag per topic over this consumer's partitions, from librdkafka's statistics JSON.
/// librdkafka reports -1 until a partition has a committed offset; those are left out.
pub fn consumer_lag_by_topic(stats_json: &[u8]) -> Option<BTreeMap<String, i64>> {
    let stats: Stats = serde_json::from_slice(stats_json).ok()?;
    let mut out = BTreeMap::new();
    for (topic, t) in stats.topics {
        let lags: Vec<i64> = t
            .partitions
            .values()
            .filter(|p| p.partition >= 0 && p.desired && p.consumer_lag >= 0)
            .map(|p| p.consumer_lag)
            .collect();
        if !lags.is_empty() {
            out.insert(topic, lags.iter().sum());
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lag_sums_desired_partitions_with_known_lag() {
        let json = br#"{"name": "c", "topics": {
            "a": {"topic": "a", "partitions": {
                "0": {"partition": 0, "desired": true, "consumer_lag": 5, "hi_offset": 10},
                "1": {"partition": 1, "desired": true, "consumer_lag": 7},
                "2": {"partition": 2, "desired": false, "consumer_lag": 100},
                "-1": {"partition": -1, "desired": false, "consumer_lag": -1}}},
            "b": {"topic": "b", "partitions": {
                "0": {"partition": 0, "desired": true, "consumer_lag": -1}}}}}"#;
        assert_eq!(consumer_lag_by_topic(json), Some(BTreeMap::from([("a".to_string(), 12)])));
        assert_eq!(consumer_lag_by_topic(b"{}"), Some(BTreeMap::new()));
        assert_eq!(consumer_lag_by_topic(b"nope"), None);
    }
}
