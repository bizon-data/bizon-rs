//! Byte parity with bizon's Python worker, against golden output from parity/golden.py. The
//! synthetic cases under fixtures/parity always run; a real capture (kept out of git) runs when
//! PARITY_CAPTURE and PARITY_CONFIG point at it.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bizon_stream::avro::AvroError;
use bizon_stream::config::Config;
use bizon_stream::kafka::message::{MessageParser, RawMessage};
use bizon_stream::kafka::registry::{Registry, RegistrySchema};
use bizon_stream::pipeline::{Outcome, Pipeline, PipelineError};
use bizon_stream::proto::descriptor::TableDescriptor;
use bizon_stream::transform::TransformError;
use serde_json::Value;

const FROZEN_AT: &str = "2026-01-01T00:00:00.123456";

fn env_defaults(name: &str) -> Option<String> {
    let v = match name {
        "BIZON_ENV_BATCH_SIZE" => "50000",
        "BIZON_ENV_CONSUMER_TIMEOUT" => "30",
        "BIZON_ENV_BOOTSTRAP_SERVERS" => "localhost:9092",
        "BIZON_ENV_APICURIO_URL" => "http://registry",
        "BIZON_ENV_BQ_MAX_ROWS_PER_REQUEST" => "5000",
        "BIZON_ENV_BQ_MAX_CONCURRENT_THREADS" => "20",
        "BIZON_ENV_DATASET_LOCATION" => "US",
        _ if name.starts_with("BIZON_ENV_") => "x",
        _ => return None,
    };
    Some(v.to_string())
}

fn b64(v: &Value) -> Option<Vec<u8>> {
    v.as_str().map(|s| B64.decode(s).unwrap())
}

fn synthetic(case: &str) {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../fixtures/parity").join(case);
    assert_parity(&dir, &dir.join("config.yml"));
}

#[test]
fn synthetic_debezium() {
    synthetic("debezium");
}

#[test]
fn synthetic_cloudevents() {
    synthetic("cloudevents");
}

#[test]
fn synthetic_cloudevents_enriched() {
    synthetic("cloudevents_enriched");
}

#[test]
fn synthetic_json_events() {
    synthetic("json_events");
}

#[test]
fn synthetic_json_cdc() {
    synthetic("json_cdc");
}

#[test]
fn synthetic_avro_events() {
    synthetic("avro_events");
}

#[test]
fn synthetic_decimal() {
    synthetic("decimal");
}

#[test]
fn captured() {
    let (Ok(capture), Ok(config)) = (std::env::var("PARITY_CAPTURE"), std::env::var("PARITY_CONFIG")) else {
        eprintln!("PARITY_CAPTURE/PARITY_CONFIG not set; skipping");
        return;
    };
    assert_parity(&PathBuf::from(capture), &PathBuf::from(config));
}

fn assert_parity(capture: &std::path::Path, config: &std::path::Path) {
    let cfg = Config::from_yaml(&std::fs::read_to_string(config).unwrap(), env_defaults).unwrap();

    let mut schemas: HashMap<i64, Arc<RegistrySchema>> = HashMap::new();
    for entry in std::fs::read_dir(capture.join("schemas")).into_iter().flatten() {
        let path = entry.unwrap().path();
        let id: i64 = path.file_stem().unwrap().to_str().unwrap().parse().unwrap();
        schemas.insert(id, Arc::new(Registry::build(id, std::fs::read(&path).unwrap()).unwrap()));
    }

    let tables: HashMap<String, TableDescriptor> = cfg
        .destination
        .record_schemas
        .iter()
        .map(|s| (s.destination_id.clone(), TableDescriptor::new(&s.columns()).unwrap()))
        .collect();

    for line in std::fs::read_to_string(capture.join("descriptors.ndjson")).unwrap().lines() {
        let d: Value = serde_json::from_str(line).unwrap();
        let ours = tables[d["destination_id"].as_str().unwrap()].proto_schema_bytes();
        assert_eq!(Some(ours), b64(&d["proto_schema_b64"]), "descriptor for {}", d["destination_id"]);
    }

    let parser = MessageParser::new(&cfg.source);
    let lookup = |id: &str| tables.get(id);
    let pipeline = Pipeline::new(&parser, &cfg.transform, &lookup);

    let messages = std::fs::read_to_string(capture.join("messages.ndjson")).unwrap();
    let golden = std::fs::read_to_string(capture.join("golden.ndjson")).unwrap();
    let (mut matched, mut mismatched) = (0usize, Vec::new());
    for (m, g) in messages.lines().zip(golden.lines()) {
        let m: Value = serde_json::from_str(m).unwrap();
        let g: Value = serde_json::from_str(g).unwrap();
        let (key, value) = (b64(&m["key"]), b64(&m["value"]));
        let header_bytes: Vec<(String, Option<Vec<u8>>)> = m["headers"]
            .as_array()
            .map(|hs| hs.iter().map(|h| (h[0].as_str().unwrap().to_string(), b64(&h[1]))).collect())
            .unwrap_or_default();
        let headers: Vec<(&str, Option<&[u8]>)> = header_bytes.iter().map(|(k, v)| (k.as_str(), v.as_deref())).collect();
        let raw = RawMessage {
            topic: m["topic"].as_str().unwrap(),
            partition: m["partition"].as_i64().unwrap() as i32,
            offset: m["offset"].as_i64().unwrap(),
            timestamp_ms: m["timestamp"].as_i64().unwrap(),
            key: key.as_deref(),
            value: value.as_deref(),
            headers: m["headers"].is_array().then_some(headers.as_slice()),
        };
        let ours = pipeline.process(&raw, |id| schemas.get(&id).cloned(), FROZEN_AT);
        let at = format!("{}[{}]@{}", raw.topic, raw.partition, raw.offset);
        let verdict = match (g["outcome"].as_str().unwrap(), &ours) {
            ("skipped", Ok(Outcome::Skipped)) => Ok(()),
            ("row", Ok(Outcome::Row { destination_id, bytes })) => {
                if g["destination_id"] != destination_id.as_ref() {
                    Err(format!("destination {} vs {}", g["destination_id"], destination_id))
                } else if let Some(diff) = first_field_diff(&b64(&g["row_b64"]).unwrap(), bytes) {
                    Err(diff)
                } else {
                    Ok(())
                }
            }
            // bizon's frame-building step (source_records_to_df) fails on values our decoder rejects.
            ("error", Err(e)) if g["stage"] == e.stage() || (g["stage"] == "frame" && e.stage() == "source") => Ok(()),
            // json.loads accepts NaN/Infinity in string keys and bizon fails later, at encode; see docs/decisions.md.
            ("error", Err(PipelineError::Transform(TransformError::KeysJson(_)))) if g["stage"] == "encode" => Ok(()),
            // A scale-0 decimal beyond 64 bits: orjson refuses it in bizon's transform, serde_json while decoding.
            ("error", Err(PipelineError::Avro(AvroError::Unsupported("integer decimal beyond 64 bits")))) if g["stage"] == "transform" => {
                Ok(())
            }
            (want, got) => Err(format!(
                "python {want} ({}) vs rust {:?}",
                g["error"],
                got.as_ref().map(|_| "row/skip")
            )),
        };
        match verdict {
            Ok(()) => matched += 1,
            Err(why) => mismatched.push(format!("{at}: {why}")),
        }
    }
    eprintln!("parity {}: {matched} matched, {} mismatched", capture.display(), mismatched.len());
    for m in mismatched.iter().take(10) {
        eprintln!("  {m}");
    }
    assert!(mismatched.is_empty(), "{} messages differ from Python", mismatched.len());
}

/// Field-by-field comparison of two proto rows, for a readable mismatch report.
fn first_field_diff(python: &[u8], rust: &[u8]) -> Option<String> {
    if python == rust {
        return None;
    }
    let (p, r) = (fields(python), fields(rust));
    for i in 0..p.len().max(r.len()) {
        if p.get(i) != r.get(i) {
            return Some(format!("field #{i}: python {:?} vs rust {:?}", p.get(i), r.get(i)));
        }
    }
    Some("bytes differ but fields match".into())
}

fn fields(mut b: &[u8]) -> Vec<(u64, String)> {
    fn varint(b: &mut &[u8]) -> u64 {
        let (mut v, mut shift) = (0u64, 0);
        loop {
            let byte = b[0];
            *b = &b[1..];
            v |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return v;
            }
            shift += 7;
        }
    }
    let mut out = Vec::new();
    while !b.is_empty() {
        let tag = varint(&mut b);
        let value = match tag & 7 {
            0 => varint(&mut b).to_string(),
            1 => {
                let v = f64::from_le_bytes(b[..8].try_into().unwrap());
                b = &b[8..];
                v.to_string()
            }
            2 => {
                let n = varint(&mut b) as usize;
                let s = String::from_utf8_lossy(&b[..n]).into_owned();
                b = &b[n..];
                s
            }
            w => format!("<wire type {w}>"),
        };
        out.push((tag >> 3, value));
    }
    out
}
