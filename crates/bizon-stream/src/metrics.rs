//! Process counters, exposed as Prometheus text on /metrics and pushed to DogStatsD when
//! DD_AGENT_HOST is set (the chart sets it to the node's IP).

use std::collections::BTreeMap;
use std::net::UdpSocket;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::kafka::message::SkipReason;

#[derive(Default)]
pub struct Metrics {
    pub messages: AtomicU64,
    skipped_empty_value: AtomicU64,
    skipped_invalid_key: AtomicU64,
    skipped_decode_error: AtomicU64,
    pub rows_appended: AtomicU64,
    pub bytes_appended: AtomicU64,
    pub append_requests: AtomicU64,
    pub append_latency_us_total: AtomicU64,
    /// Largest ack latency since the last DogStatsD push.
    pub append_latency_us_max: AtomicU64,
    pub append_retries: AtomicU64,
    pub large_rows: AtomicU64,
    pub commits: AtomicU64,
    pub commit_failures: AtomicU64,
    /// Offsets of idle partitions committed again to keep them from expiring.
    pub recommits: AtomicU64,
    pub partitions_assigned: AtomicU64,
    pub partitions_revoked: AtomicU64,
    pub schema_changes: AtomicU64,
    pub inflight_bytes: AtomicU64,
    pub assigned_partitions: AtomicU64,
    /// Age of the oldest consumed message whose row is not yet acknowledged; 0 when none is pending.
    pub oldest_unacked_secs: AtomicU64,
    /// Committed-offset lag per topic, from librdkafka statistics.
    consumer_lag: Mutex<BTreeMap<String, i64>>,
    /// Acknowledged (rows, large rows) per destination_id since the last push.
    synced: Mutex<BTreeMap<Arc<str>, (u64, u64)>>,
}

#[derive(Clone, Copy, PartialEq)]
enum Kind {
    Counter,
    Gauge,
    /// A gauge reset after each push.
    Max,
}

const SERIES: usize = 20;

impl Metrics {
    pub fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Relaxed);
    }

    pub fn skipped(&self, reason: SkipReason) {
        let c = match reason {
            SkipReason::EmptyValue => &self.skipped_empty_value,
            SkipReason::InvalidKey => &self.skipped_invalid_key,
            SkipReason::DecodeError => &self.skipped_decode_error,
        };
        c.fetch_add(1, Relaxed);
    }

    pub fn observe_append(&self, rows: u64, bytes: u64, latency: Duration) {
        let us = latency.as_micros() as u64;
        self.rows_appended.fetch_add(rows, Relaxed);
        self.bytes_appended.fetch_add(bytes, Relaxed);
        self.append_requests.fetch_add(1, Relaxed);
        self.append_latency_us_total.fetch_add(us, Relaxed);
        self.append_latency_us_max.fetch_max(us, Relaxed);
    }

    pub fn synced(&self, destination_id: &Arc<str>, rows: u64, large: u64) {
        let mut synced = self.synced.lock().unwrap();
        let e = synced.entry(destination_id.clone()).or_default();
        e.0 += rows;
        e.1 += large;
    }

    pub fn set_consumer_lag(&self, lag: BTreeMap<String, i64>) {
        *self.consumer_lag.lock().unwrap() = lag;
    }

    fn series(&self) -> [(&'static str, Option<&'static str>, &AtomicU64, Kind); SERIES] {
        use Kind::*;
        [
            ("messages", None, &self.messages, Counter),
            ("skipped", Some("reason:empty_value"), &self.skipped_empty_value, Counter),
            ("skipped", Some("reason:invalid_key"), &self.skipped_invalid_key, Counter),
            ("skipped", Some("reason:decode_error"), &self.skipped_decode_error, Counter),
            ("rows_appended", None, &self.rows_appended, Counter),
            ("bytes_appended", None, &self.bytes_appended, Counter),
            ("append_requests", None, &self.append_requests, Counter),
            ("append_latency_us_total", None, &self.append_latency_us_total, Counter),
            ("append_latency_us_max", None, &self.append_latency_us_max, Max),
            ("append_retries", None, &self.append_retries, Counter),
            ("large_rows", None, &self.large_rows, Counter),
            ("commits", None, &self.commits, Counter),
            ("commit_failures", None, &self.commit_failures, Counter),
            ("recommits", None, &self.recommits, Counter),
            ("partitions_assigned", None, &self.partitions_assigned, Counter),
            ("partitions_revoked", None, &self.partitions_revoked, Counter),
            ("schema_changes", None, &self.schema_changes, Counter),
            ("inflight_bytes", None, &self.inflight_bytes, Gauge),
            ("assigned_partitions", None, &self.assigned_partitions, Gauge),
            ("oldest_unacked_secs", None, &self.oldest_unacked_secs, Gauge),
        ]
    }

    pub fn prometheus(&self) -> String {
        let mut out = String::new();
        let mut last_name = "";
        for (name, tag, v, kind) in self.series() {
            if name != last_name {
                let kind = if kind == Kind::Counter { "counter" } else { "gauge" };
                out.push_str(&format!("# TYPE bizon_rs_{name} {kind}\n"));
                last_name = name;
            }
            let labels = tag
                .and_then(|t| t.split_once(':'))
                .map(|(k, v)| format!("{{{k}=\"{v}\"}}"))
                .unwrap_or_default();
            out.push_str(&format!("bizon_rs_{name}{labels} {}\n", v.load(Relaxed)));
        }
        out.push_str("# TYPE bizon_rs_consumer_lag gauge\n");
        for (topic, lag) in self.consumer_lag.lock().unwrap().iter() {
            out.push_str(&format!("bizon_rs_consumer_lag{{topic=\"{topic}\"}} {lag}\n"));
        }
        out
    }

    /// One push: deltas for counters, values for gauges; interval maxima are reset.
    fn statsd_payload(&self, last: &mut [u64; SERIES], tags: &str, pipeline_tags: &str) -> String {
        let mut payload = String::new();
        for (i, (name, tag, v, kind)) in self.series().into_iter().enumerate() {
            let tags = match tag {
                Some(t) => format!("{tags},{t}"),
                None => tags.to_string(),
            };
            let line = match kind {
                Kind::Counter => {
                    let now = v.load(Relaxed);
                    let d = now.saturating_sub(last[i]);
                    last[i] = now;
                    format!("bizon_rs.{name}:{d}|c|#{tags}\n")
                }
                Kind::Gauge => format!("bizon_rs.{name}:{}|g|#{tags}\n", v.load(Relaxed)),
                Kind::Max => format!("bizon_rs.{name}:{}|g|#{tags}\n", v.swap(0, Relaxed)),
            };
            payload.push_str(&line);
        }
        for (topic, lag) in self.consumer_lag.lock().unwrap().iter() {
            payload.push_str(&format!("bizon_rs.consumer_lag:{lag}|g|#{tags},topic:{topic}\n"));
        }
        for (id, (rows, large)) in std::mem::take(&mut *self.synced.lock().unwrap()) {
            let tags = format!("{pipeline_tags},destination_id:{id}");
            payload.push_str(&format!("bizon_pipeline.records_synced:{rows}|c|#{tags}\n"));
            if large > 0 {
                payload.push_str(&format!("bizon_pipeline.large_records:{large}|c|#{tags}\n"));
            }
        }
        payload
    }

    /// Pushes every 10 s. `tags` is a comma-separated DogStatsD tag list added to every `bizon_rs.*`
    /// line; `pipeline_tags` to the `bizon_pipeline.*` lines, which use bizon's tag keys.
    pub fn spawn_statsd(self: Arc<Self>, tags: String, pipeline_tags: String) {
        let Ok(host) = std::env::var("DD_AGENT_HOST") else { return };
        let Ok(sock) = UdpSocket::bind("0.0.0.0:0") else { return };
        let addr = format!("{host}:8125");
        tokio::spawn(async move {
            let mut last = [0u64; SERIES];
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let payload = self.statsd_payload(&mut last, &tags, &pipeline_tags);
                // The agent drops datagrams over 8 KiB, which per-topic lag lines can exceed.
                for chunk in chunks(&payload, 8000) {
                    let _ = sock.send_to(chunk.as_bytes(), &addr);
                }
            }
        });
    }
}

/// Splits newline-terminated lines into pieces of at most `max` bytes, on line boundaries.
fn chunks(s: &str, max: usize) -> Vec<&str> {
    let mut out = Vec::new();
    let mut start = 0;
    let mut end = 0;
    for (i, _) in s.match_indices('\n') {
        if i + 1 - start > max && end > start {
            out.push(&s[start..end]);
            start = end;
        }
        end = i + 1;
    }
    if end > start {
        out.push(&s[start..end]);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statsd_counters_are_deltas_and_max_resets() {
        let m = Metrics::default();
        let mut last = [0u64; SERIES];
        m.observe_append(10, 100, Duration::from_millis(250));
        m.observe_append(10, 100, Duration::from_millis(40));
        m.skipped(SkipReason::EmptyValue);
        m.set_consumer_lag(BTreeMap::from([("t".to_string(), 42)]));
        let p = m.statsd_payload(&mut last, "pipeline:p", "pipeline_name:p");
        assert!(p.contains("bizon_rs.rows_appended:20|c|#pipeline:p\n"));
        assert!(p.contains("bizon_rs.append_latency_us_max:250000|g|#pipeline:p\n"));
        assert!(p.contains("bizon_rs.skipped:1|c|#pipeline:p,reason:empty_value\n"));
        assert!(p.contains("bizon_rs.skipped:0|c|#pipeline:p,reason:invalid_key\n"));
        assert!(p.contains("bizon_rs.consumer_lag:42|g|#pipeline:p,topic:t\n"));

        m.observe_append(1, 1, Duration::from_millis(5));
        let p = m.statsd_payload(&mut last, "pipeline:p", "pipeline_name:p");
        assert!(p.contains("bizon_rs.rows_appended:1|c|"));
        assert!(
            p.contains("bizon_rs.append_latency_us_max:5000|g|"),
            "max covers only the last interval"
        );
    }

    #[test]
    fn statsd_records_synced_per_destination() {
        let m = Metrics::default();
        let mut last = [0u64; SERIES];
        let (a, b): (Arc<str>, Arc<str>) = ("p.d.a".into(), "p.d.b".into());
        m.synced(&a, 10, 0);
        m.synced(&a, 1, 1);
        m.synced(&b, 5, 0);
        let p = m.statsd_payload(&mut last, "pipeline:p", "pipeline_name:p");
        assert!(p.contains("bizon_pipeline.records_synced:11|c|#pipeline_name:p,destination_id:p.d.a\n"));
        assert!(p.contains("bizon_pipeline.large_records:1|c|#pipeline_name:p,destination_id:p.d.a\n"));
        assert!(p.contains("bizon_pipeline.records_synced:5|c|#pipeline_name:p,destination_id:p.d.b\n"));
        assert_eq!(p.matches("bizon_pipeline.").count(), 3);

        let p = m.statsd_payload(&mut last, "pipeline:p", "pipeline_name:p");
        assert!(!p.contains("bizon_pipeline."), "tables without new rows are not sent");
    }

    #[test]
    fn prometheus_declares_each_family_once() {
        let m = Metrics::default();
        m.skipped(SkipReason::InvalidKey);
        let text = m.prometheus();
        assert_eq!(text.matches("# TYPE bizon_rs_skipped counter").count(), 1);
        assert!(text.contains("bizon_rs_skipped{reason=\"invalid_key\"} 1\n"));
    }

    #[test]
    fn chunks_split_on_lines() {
        assert_eq!(chunks("aa\nbb\ncc\n", 6), vec!["aa\nbb\n", "cc\n"]);
        assert_eq!(chunks("aa\n", 1), vec!["aa\n"]);
        assert!(chunks("", 10).is_empty());
    }
}
