//! Rough per-message CPU cost of the parse → decode → transform → encode path on a real capture.
//! `PARITY_CAPTURE=... PARITY_CONFIG=... cargo test --release --test throughput -- --ignored --nocapture`

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bizon_stream::config::Config;
use bizon_stream::kafka::message::{MessageParser, RawMessage};
use bizon_stream::kafka::registry::Registry;
use bizon_stream::pipeline::{Outcome, Pipeline};
use bizon_stream::proto::descriptor::TableDescriptor;
use serde_json::Value;

#[test]
#[ignore]
fn per_message_cost() {
    let (Ok(capture), Ok(config)) = (std::env::var("PARITY_CAPTURE"), std::env::var("PARITY_CONFIG")) else {
        return;
    };
    let capture = std::path::PathBuf::from(capture);
    let env = |n: &str| {
        Some(match n {
            "BIZON_ENV_BATCH_SIZE" | "BIZON_ENV_BQ_MAX_ROWS_PER_REQUEST" => "5000".to_string(),
            "BIZON_ENV_CONSUMER_TIMEOUT" | "BIZON_ENV_BQ_MAX_CONCURRENT_THREADS" => "10".to_string(),
            _ => "x".to_string(),
        })
    };
    let cfg = Config::from_yaml(&std::fs::read_to_string(config).unwrap(), env).unwrap();
    let schemas: HashMap<i64, Arc<_>> = std::fs::read_dir(capture.join("schemas"))
        .unwrap()
        .map(|e| {
            let p = e.unwrap().path();
            let id: i64 = p.file_stem().unwrap().to_str().unwrap().parse().unwrap();
            (id, Arc::new(Registry::build(id, std::fs::read(&p).unwrap()).unwrap()))
        })
        .collect();
    let tables: HashMap<String, TableDescriptor> = cfg
        .destination
        .record_schemas
        .iter()
        .map(|s| (s.destination_id.clone(), TableDescriptor::new(&s.columns()).unwrap()))
        .collect();
    let parser = MessageParser::new(&cfg.source);
    let lookup = |id: &str| tables.get(id);
    let pipeline = Pipeline::new(&parser, &cfg.transform, &lookup);

    type Fixture = (Value, Option<Vec<u8>>, Option<Vec<u8>>);
    let decoded: Vec<Fixture> = std::fs::read_to_string(capture.join("messages.ndjson"))
        .unwrap()
        .lines()
        .map(|l| {
            let m: Value = serde_json::from_str(l).unwrap();
            let b = |v: &Value| v.as_str().map(|s| B64.decode(s).unwrap());
            let (k, v) = (b(&m["key"]), b(&m["value"]));
            (m, k, v)
        })
        .collect();
    let (mut rows, mut wire, mut out) = (0usize, 0usize, 0usize);
    let rounds = 50;
    let started = Instant::now();
    for _ in 0..rounds {
        for (m, key, value) in &decoded {
            let raw = RawMessage {
                topic: m["topic"].as_str().unwrap(),
                partition: m["partition"].as_i64().unwrap() as i32,
                offset: m["offset"].as_i64().unwrap(),
                timestamp_ms: m["timestamp"].as_i64().unwrap(),
                key: key.as_deref(),
                value: value.as_deref(),
                headers: None,
            };
            wire += value.as_ref().map_or(0, |v| v.len());
            if let Ok(Outcome::Row { bytes, .. }) = pipeline.process(&raw, |id| schemas.get(&id).cloned(), "2026-01-01T00:00:00.123456") {
                rows += 1;
                out += bytes.len();
            }
        }
    }
    let n = decoded.len() * rounds;
    let el = started.elapsed();
    eprintln!(
        "{n} messages ({rows} rows) in {el:?}: {:.2} us/message, {:.2} us/row, wire {:.0} B/msg, row {:.0} B/row",
        el.as_secs_f64() * 1e6 / n as f64,
        el.as_secs_f64() * 1e6 / rows as f64,
        wire as f64 / n as f64,
        out as f64 / rows as f64
    );
}
