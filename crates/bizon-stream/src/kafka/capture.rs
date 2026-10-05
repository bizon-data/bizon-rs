//! `bizon-stream capture`: reads a sample of messages for parity fixtures. It uses `assign()` from a
//! timestamp, never subscribes and never commits, so it joins no consumer group and moves no offsets.

use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::path::Path;
use std::time::{Duration, Instant};

use anyhow::Context;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use rdkafka::consumer::{BaseConsumer, Consumer};
use rdkafka::message::{Headers, Message, Timestamp};
use rdkafka::{Offset, TopicPartitionList};
use serde_json::json;

use super::client::client_config;
use super::framing;
use super::registry::Registry;
use crate::config::{Config, MessageEncoding};

pub struct CaptureOptions {
    pub since_ms: i64,
    pub per_topic: usize,
    pub max_wait: Duration,
    pub topics: Option<Vec<String>>,
}

pub async fn capture(cfg: &Config, opts: &CaptureOptions, out_dir: &Path) -> anyhow::Result<()> {
    let topics: Vec<String> = cfg
        .source
        .topics
        .iter()
        .map(|t| t.name.clone())
        .filter(|t| opts.topics.as_ref().is_none_or(|f| f.contains(t)))
        .collect();
    anyhow::ensure!(!topics.is_empty(), "no topics selected");

    let mut cc = client_config(&cfg.source, None);
    cc.set("group.id", format!("{}-capture-{}", cfg.source.group_id, std::process::id()))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false");
    cc.remove("group.instance.id");
    cc.remove("partition.assignment.strategy");
    let consumer: BaseConsumer = cc.create().context("creating consumer")?;

    let mut tpl = TopicPartitionList::new();
    for topic in &topics {
        let md = consumer.fetch_metadata(Some(topic), Duration::from_secs(30))?;
        let t = md.topics().first().context("no metadata")?;
        anyhow::ensure!(t.error().is_none(), "topic {topic}: {:?}", t.error());
        for p in t.partitions() {
            tpl.add_partition_offset(topic, p.id(), Offset::Offset(opts.since_ms))?;
        }
    }
    let resolved = consumer.offsets_for_times(tpl, Duration::from_secs(60))?;
    // A partition is done once its last message at assignment time has been read.
    let mut assign = TopicPartitionList::new();
    let mut open: HashMap<(String, i32), i64> = HashMap::new();
    for e in resolved.elements() {
        if let Offset::Offset(start) = e.offset() {
            let (_, high) = consumer.fetch_watermarks(e.topic(), e.partition(), Duration::from_secs(30))?;
            if start < high {
                assign.add_partition_offset(e.topic(), e.partition(), Offset::Offset(start))?;
                open.insert((e.topic().to_string(), e.partition()), high - 1);
            }
        }
    }
    consumer.assign(&assign)?;
    tracing::info!(partitions = open.len(), topics = topics.len(), "capturing");

    std::fs::create_dir_all(out_dir.join("schemas"))?;
    let mut out = std::io::BufWriter::new(std::fs::File::create(out_dir.join("messages.ndjson"))?);
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut schema_ids = BTreeSet::new();
    let started = Instant::now();

    while !open.is_empty()
        && started.elapsed() < opts.max_wait
        && topics.iter().any(|t| counts.get(t).copied().unwrap_or(0) < opts.per_topic)
    {
        let Some(result) = consumer.poll(Duration::from_secs(1)) else {
            continue;
        };
        let m = result?;
        let key = (m.topic().to_string(), m.partition());
        if open.get(&key).is_some_and(|last| m.offset() >= *last) {
            open.remove(&key);
        }
        let n = counts.entry(m.topic().to_string()).or_default();
        if *n >= opts.per_topic {
            continue;
        }
        *n += 1;
        let (ts_type, ts) = match m.timestamp() {
            Timestamp::NotAvailable => ("none", -1),
            Timestamp::CreateTime(t) => ("create", t),
            Timestamp::LogAppendTime(t) => ("log_append", t),
        };
        let headers = m
            .headers()
            .map(|h| h.iter().map(|h| json!([h.key, h.value.map(|v| B64.encode(v))])).collect::<Vec<_>>());
        if cfg.source.message_encoding == MessageEncoding::Avro {
            if let Some(f) = m.payload().and_then(|v| framing::parse(v).ok()) {
                schema_ids.insert(f.global_id);
            }
        }
        let line = json!({
            "topic": m.topic(),
            "partition": m.partition(),
            "offset": m.offset(),
            "timestamp_type": ts_type,
            "timestamp": ts,
            "key": m.key().map(|k| B64.encode(k)),
            "value": m.payload().map(|v| B64.encode(v)),
            "headers": headers,
        });
        serde_json::to_writer(&mut out, &line)?;
        out.write_all(b"\n")?;
    }
    out.flush()?;

    let auth = &cfg.source.authentication;
    if !schema_ids.is_empty() {
        let registry = Registry::new(
            &auth.schema_registry_url,
            &auth.schema_registry_username,
            &auth.schema_registry_password,
        );
        for id in &schema_ids {
            let s = registry.get(*id).await?;
            std::fs::write(out_dir.join("schemas").join(format!("{id}.json")), &s.raw)?;
        }
    }
    let total: usize = counts.values().sum();
    tracing::info!(messages = total, schemas = schema_ids.len(), elapsed = ?started.elapsed(), "capture done");
    println!(
        "captured {total} messages from {} topics, {} schemas",
        counts.len(),
        schema_ids.len()
    );
    Ok(())
}
