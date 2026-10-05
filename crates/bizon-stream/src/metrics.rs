//! Process counters, exposed as Prometheus text on /metrics and pushed to DogStatsD when
//! DD_AGENT_HOST is set (the chart sets it to the node's IP).

use std::net::UdpSocket;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::Arc;
use std::time::Duration;

#[derive(Default)]
pub struct Metrics {
    pub messages: AtomicU64,
    pub skipped: AtomicU64,
    pub rows_appended: AtomicU64,
    pub bytes_appended: AtomicU64,
    pub append_requests: AtomicU64,
    pub append_latency_us_total: AtomicU64,
    pub append_latency_us_max: AtomicU64,
    pub large_rows: AtomicU64,
    pub commits: AtomicU64,
    pub inflight_bytes: AtomicU64,
    pub assigned_partitions: AtomicU64,
}

impl Metrics {
    pub fn add(c: &AtomicU64, n: u64) {
        c.fetch_add(n, Relaxed);
    }

    pub fn observe_append(&self, rows: u64, bytes: u64, latency: Duration) {
        let us = latency.as_micros() as u64;
        self.rows_appended.fetch_add(rows, Relaxed);
        self.bytes_appended.fetch_add(bytes, Relaxed);
        self.append_requests.fetch_add(1, Relaxed);
        self.append_latency_us_total.fetch_add(us, Relaxed);
        self.append_latency_us_max.fetch_max(us, Relaxed);
    }

    fn pairs(&self) -> [(&'static str, &AtomicU64, bool); 11] {
        [
            ("messages", &self.messages, true),
            ("skipped", &self.skipped, true),
            ("rows_appended", &self.rows_appended, true),
            ("bytes_appended", &self.bytes_appended, true),
            ("append_requests", &self.append_requests, true),
            ("append_latency_us_total", &self.append_latency_us_total, true),
            ("append_latency_us_max", &self.append_latency_us_max, false),
            ("large_rows", &self.large_rows, true),
            ("commits", &self.commits, true),
            ("inflight_bytes", &self.inflight_bytes, false),
            ("assigned_partitions", &self.assigned_partitions, false),
        ]
    }

    pub fn prometheus(&self) -> String {
        self.pairs()
            .iter()
            .map(|(name, v, counter)| {
                let kind = if *counter { "counter" } else { "gauge" };
                format!("# TYPE bizon_rs_{name} {kind}\nbizon_rs_{name} {}\n", v.load(Relaxed))
            })
            .collect()
    }

    /// Pushes deltas (counters) and values (gauges) every 10 s.
    pub fn spawn_statsd(self: Arc<Self>, pipeline: String) {
        let Ok(host) = std::env::var("DD_AGENT_HOST") else { return };
        let Ok(sock) = UdpSocket::bind("0.0.0.0:0") else { return };
        let addr = format!("{host}:8125");
        tokio::spawn(async move {
            let mut last = [0u64; 11];
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            loop {
                tick.tick().await;
                let mut payload = String::new();
                for (i, (name, v, counter)) in self.pairs().iter().enumerate() {
                    let now = v.load(Relaxed);
                    let line = if *counter {
                        let d = now.saturating_sub(last[i]);
                        last[i] = now;
                        format!("bizon_rs.{name}:{d}|c|#pipeline:{pipeline}\n")
                    } else {
                        format!("bizon_rs.{name}:{now}|g|#pipeline:{pipeline}\n")
                    };
                    payload.push_str(&line);
                }
                let _ = sock.send_to(payload.as_bytes(), &addr);
            }
        });
    }
}
