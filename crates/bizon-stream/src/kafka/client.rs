//! librdkafka settings built the way bizon's `KafkaSource.__init__` builds them.

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
