//! `bizon-stream run`: consume → decode → transform → encode → append → commit acknowledged offsets.
//!
//! Decoding runs inline on the consumer task, which keeps per-partition order for free; appends run
//! concurrently per table. In-flight rows are bounded by bytes, and offsets are committed only past
//! rows BigQuery has acknowledged (at-least-once, like bizon).

pub mod offsets;
pub mod table;

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::Context as _;
use bytes::Bytes;
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::{KafkaError, KafkaResult, RDKafkaErrorCode};
use rdkafka::message::{Headers, Message};
use rdkafka::{ClientContext, Offset, TopicPartitionList};
use tokio::sync::{mpsc, Notify, Semaphore};

use self::offsets::{Tp, Tracker};
use self::table::{Shared, TableHandle, TableMsg};
use crate::bq::rest::BigQueryRest;
use crate::bq::write::{TableRef, WriteClient, WriterOptions};
use crate::config::Config;
use crate::kafka::client::client_config;
use crate::kafka::message::{MessageParser, RawMessage};
use crate::kafka::registry::{Registry, RegistrySchema};
use crate::metrics::Metrics;
use crate::pipeline::{Outcome, Pipeline};
use crate::proto::descriptor::TableDescriptor;

/// Knobs that are not part of bizon's config.yml, read from `BIZON_RS_*` so the mounted config stays
/// identical to the Python worker's.
#[derive(Debug, Clone)]
pub struct RunOptions {
    pub inflight_bytes: usize,
    pub linger: Duration,
    pub queue_kbytes: u64,
    pub write_endpoint: Option<String>,
    pub rest_endpoint: Option<String>,
    pub ensure_tables: bool,
    pub health_port: u16,
    /// bizon commits only when ENVIRONMENT=production; elsewhere a restart re-reads.
    pub commit: bool,
    pub hostname: Option<String>,
    pub drain_timeout: Duration,
}

impl RunOptions {
    pub fn from_env(cfg: &Config) -> Self {
        let var = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        let num = |k: &str, d: u64| var(k).and_then(|v| v.parse().ok()).unwrap_or(d);
        Self {
            inflight_bytes: num("BIZON_RS_INFLIGHT_BYTES", 64 << 20) as usize,
            linger: Duration::from_millis(num("BIZON_RS_LINGER_MS", 1000).min(cfg.source.consumer_timeout * 1000)),
            queue_kbytes: num("BIZON_RS_QUEUE_KBYTES", 16 << 10),
            write_endpoint: var("BIZON_RS_BQ_WRITE_ENDPOINT"),
            rest_endpoint: var("BIZON_RS_BQ_REST_ENDPOINT"),
            ensure_tables: var("BIZON_RS_ENSURE_TABLES").is_none_or(|v| v != "false"),
            health_port: num("BIZON_RS_HEALTH_PORT", 8080) as u16,
            commit: var("ENVIRONMENT").as_deref() == Some("production"),
            hostname: var("HOSTNAME"),
            drain_timeout: Duration::from_secs(num("BIZON_RS_DRAIN_SECS", 20)),
        }
    }
}

struct Ctx {
    tracker: Arc<Mutex<Tracker>>,
    flushes: Arc<Mutex<Vec<Arc<Notify>>>>,
    topics: HashMap<String, Arc<str>>,
    commit: bool,
    ready: Arc<AtomicBool>,
    metrics: Arc<Metrics>,
}

impl Ctx {
    fn tps(&self, tpl: &TopicPartitionList) -> Vec<Tp> {
        tpl.elements()
            .iter()
            .filter_map(|e| {
                Some(Tp {
                    topic: self.topics.get(e.topic())?.clone(),
                    partition: e.partition(),
                })
            })
            .collect()
    }
}

fn commit_list(commits: &[(Tp, i64)]) -> TopicPartitionList {
    let mut tpl = TopicPartitionList::new();
    for (tp, offset) in commits {
        let _ = tpl.add_partition_offset(&tp.topic, tp.partition, Offset::Offset(*offset));
    }
    tpl
}

/// Commit errors that only mean "you are no longer the owner": the next owner re-reads.
fn benign_commit_error(e: &KafkaError) -> bool {
    matches!(
        e.rdkafka_error_code(),
        Some(
            RDKafkaErrorCode::IllegalGeneration
                | RDKafkaErrorCode::UnknownMemberId
                | RDKafkaErrorCode::RebalanceInProgress
                | RDKafkaErrorCode::NoOffset
        )
    )
}

impl ClientContext for Ctx {
    fn error(&self, error: KafkaError, reason: &str) {
        tracing::warn!(%error, reason, "kafka client error");
    }
}

impl ConsumerContext for Ctx {
    /// Before giving partitions away: flush, wait (bounded) for their rows to be acknowledged, and
    /// commit them. Anything still unacknowledged is re-read by the next owner.
    fn pre_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        let Rebalance::Revoke(tpl) = rebalance else { return };
        let tps = self.tps(tpl);
        for f in self.flushes.lock().unwrap().iter() {
            f.notify_one();
        }
        let started = Instant::now();
        while self.tracker.lock().unwrap().in_flight(Some(&tps)) > 0 && started.elapsed() < Duration::from_secs(10) {
            std::thread::sleep(Duration::from_millis(25));
        }
        let mut tracker = self.tracker.lock().unwrap();
        let commits = tracker.take_commits(Some(&tps));
        if self.commit && !commits.is_empty() {
            match consumer.commit(&commit_list(&commits), CommitMode::Sync) {
                Ok(()) => Metrics::add(&self.metrics.commits, 1),
                Err(e) => tracing::warn!(error = %e, "commit on revoke failed"),
            }
        }
        for tp in &tps {
            tracker.revoke(tp);
        }
        self.metrics
            .assigned_partitions
            .store(tracker.assigned().count() as u64, Ordering::Relaxed);
        tracing::info!(
            revoked = tps.len(),
            unacked_dropped = started.elapsed() >= Duration::from_secs(10),
            "partitions revoked"
        );
    }

    fn post_rebalance(&self, _consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if let Rebalance::Assign(tpl) = rebalance {
            let tps = self.tps(tpl);
            let mut tracker = self.tracker.lock().unwrap();
            for tp in &tps {
                tracker.assign(tp.clone());
            }
            self.metrics
                .assigned_partitions
                .store(tracker.assigned().count() as u64, Ordering::Relaxed);
            self.ready.store(true, Ordering::Relaxed);
            tracing::info!(assigned = tps.len(), "partitions assigned");
        }
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        if let Err(e) = result {
            if !benign_commit_error(&e) {
                tracing::warn!(error = %e, "async commit failed");
            }
        }
    }
}

pub async fn run(cfg: Config, opts: RunOptions) -> anyhow::Result<()> {
    // Installed first so a SIGTERM during startup still stops cleanly.
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let metrics = Arc::new(Metrics::default());
    let ready = Arc::new(AtomicBool::new(false));
    crate::health::serve(opts.health_port, ready.clone(), metrics.clone()).await?;
    metrics.clone().spawn_statsd(cfg.name.clone());

    let tracker = Arc::new(Mutex::new(Tracker::default()));
    let flushes = Arc::new(Mutex::new(Vec::new()));
    let topics: HashMap<String, Arc<str>> = cfg
        .source
        .topics
        .iter()
        .map(|t| (t.name.clone(), Arc::from(t.name.as_str())))
        .collect();
    let ctx = Ctx {
        tracker: tracker.clone(),
        flushes: flushes.clone(),
        topics: topics.clone(),
        commit: opts.commit,
        ready: ready.clone(),
        metrics: metrics.clone(),
    };
    let mut cc = client_config(&cfg.source, opts.hostname.as_deref());
    cc.set("queued.max.messages.kbytes", opts.queue_kbytes.to_string());
    let consumer: Arc<StreamConsumer<Ctx>> = Arc::new(cc.create_with_context(ctx).context("creating Kafka consumer")?);
    let names: Vec<&str> = cfg.source.topics.iter().map(|t| t.name.as_str()).collect();
    consumer.subscribe(&names)?;
    tracing::info!(topics = names.len(), group = %cfg.source.group_id, commit = opts.commit, "subscribed");

    let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        write: WriteClient::connect(opts.write_endpoint.as_deref(), None).await?,
        rest: if opts.ensure_tables {
            Some(BigQueryRest::new(opts.rest_endpoint.as_deref()).await?)
        } else {
            None
        },
        tracker: tracker.clone(),
        metrics: metrics.clone(),
        fatal: fatal_tx,
        max_rows: cfg.destination.bq_max_rows_per_request,
        linger: opts.linger,
        writer: WriterOptions::default(),
        location: cfg.destination.dataset_location.clone(),
        partitioning: cfg.destination.time_partitioning.clone(),
    });

    if opts.commit {
        let (consumer, tracker, metrics) = (consumer.clone(), tracker.clone(), metrics.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                let commits = tracker.lock().unwrap().take_commits(None);
                if !commits.is_empty() {
                    match consumer.commit(&commit_list(&commits), CommitMode::Async) {
                        Ok(()) => Metrics::add(&metrics.commits, 1),
                        Err(e) if benign_commit_error(&e) => {}
                        Err(e) => tracing::warn!(error = %e, "commit failed"),
                    }
                }
            }
        });
    }

    let descriptors: HashMap<String, TableDescriptor> = cfg
        .destination
        .record_schemas
        .iter()
        .map(|s| Ok((s.destination_id.clone(), TableDescriptor::new(&s.columns())?)))
        .collect::<anyhow::Result<_>>()?;
    let parser = MessageParser::new(&cfg.source);
    let lookup = |id: &str| descriptors.get(id);
    let pipeline = Pipeline::new(&parser, &cfg.transform, &lookup);
    let auth = &cfg.source.authentication;
    let registry = Registry::new(
        &auth.schema_registry_url,
        &auth.schema_registry_username,
        &auth.schema_registry_password,
    );
    let mut schemas: HashMap<i64, Arc<RegistrySchema>> = HashMap::new();
    let budget = Arc::new(Semaphore::new(opts.inflight_bytes / 1024));
    let budget_total = (opts.inflight_bytes / 1024) as u32;
    let mut tables: HashMap<Arc<str>, TableHandle> = HashMap::new();

    let outcome: anyhow::Result<()> = loop {
        let msg = tokio::select! {
            biased;
            _ = sigterm.recv() => break Ok(()),
            _ = tokio::signal::ctrl_c() => break Ok(()),
            Some(e) = fatal_rx.recv() => break Err(e),
            m = consumer.recv() => m,
        };
        let m = match msg {
            Ok(m) => m,
            Err(e) => break Err(anyhow::Error::from(e).context("consuming")),
        };
        Metrics::add(&metrics.messages, 1);
        let header_vec: Vec<(&str, Option<&[u8]>)> = m
            .headers()
            .map(|h| h.iter().map(|h| (h.key, h.value)).collect())
            .unwrap_or_default();
        let raw = RawMessage {
            topic: m.topic(),
            partition: m.partition(),
            offset: m.offset(),
            timestamp_ms: m.timestamp().to_millis().unwrap_or(-1),
            key: m.key(),
            value: m.payload(),
            headers: m.headers().map(|_| header_vec.as_slice()),
        };
        if let Some(id) = parser.schema_id(&raw) {
            if let std::collections::hash_map::Entry::Vacant(slot) = schemas.entry(id) {
                match registry.get(id).await {
                    Ok(s) => {
                        slot.insert(s);
                    }
                    Err(e) => break Err(anyhow::Error::from(e).context("schema registry")),
                }
            }
        }
        let Some(topic) = topics.get(raw.topic) else {
            break Err(anyhow::anyhow!("message from unsubscribed topic {}", raw.topic));
        };
        let tp = Tp {
            topic: topic.clone(),
            partition: raw.partition,
        };
        // A partition revoked between delivery and here is re-read by its new owner.
        let Some(ticket) = tracker.lock().unwrap().track(&tp, raw.offset) else {
            continue;
        };
        let at = format!("{}[{}]@{}", raw.topic, raw.partition, raw.offset);
        let inserted_at = crate::timefmt::now_isoformat();
        let (destination_id, large, data) = match pipeline.process(&raw, |id| schemas.get(&id).cloned(), &inserted_at) {
            Ok(Outcome::Skipped) => {
                Metrics::add(&metrics.skipped, 1);
                tracker.lock().unwrap().done(&ticket);
                continue;
            }
            Ok(Outcome::Row { destination_id, bytes }) => (destination_id, false, bytes),
            Ok(Outcome::Large { destination_id, ndjson }) => (destination_id, true, ndjson),
            Err(e) => break Err(anyhow::anyhow!("{at}: {e}")),
        };
        let permits = ((data.len() / 1024) as u32).clamp(1, budget_total);
        let permit = budget.clone().acquire_many_owned(permits).await.expect("semaphore is never closed");
        metrics
            .inflight_bytes
            .store((opts.inflight_bytes - budget.available_permits() * 1024) as u64, Ordering::Relaxed);
        let handle = tables.entry(destination_id.clone()).or_insert_with(|| {
            let schema = cfg.record_schema(&destination_id).expect("validated at startup").clone();
            let desc = &descriptors[&*destination_id];
            let table = TableRef::parse(&destination_id).expect("destination_id is project.dataset.table");
            let h = table::spawn(shared.clone(), table, schema, desc);
            flushes.lock().unwrap().push(h.flush.clone());
            h
        });
        let msg = match large {
            false => TableMsg::Row {
                bytes: Bytes::from(data),
                ticket,
                permit,
            },
            true => TableMsg::Large {
                ndjson: data,
                ticket,
                permit,
            },
        };
        if handle.tx.send(msg).await.is_err() {
            break Err(fatal_rx
                .try_recv()
                .unwrap_or_else(|_| anyhow::anyhow!("table task for {destination_id} stopped")));
        }
    };

    // After an error the failed message never completes, so don't wait the full timeout for it.
    let timeout = if outcome.is_ok() {
        opts.drain_timeout
    } else {
        opts.drain_timeout.min(Duration::from_secs(5))
    };
    drain(&tables, &tracker, timeout).await;
    if opts.commit {
        let commits = tracker.lock().unwrap().take_commits(None);
        if !commits.is_empty() {
            if let Err(e) = consumer.commit(&commit_list(&commits), CommitMode::Sync) {
                tracing::warn!(error = %e, "final commit failed");
            }
        }
    }
    tracing::info!(ok = outcome.is_ok(), "stopped");
    outcome
}

/// Flushes every table and waits (bounded) for in-flight rows to be acknowledged.
async fn drain(tables: &HashMap<Arc<str>, TableHandle>, tracker: &Mutex<Tracker>, timeout: Duration) {
    for h in tables.values() {
        h.flush.notify_one();
    }
    let started = Instant::now();
    while tracker.lock().unwrap().in_flight(None) > 0 && started.elapsed() < timeout {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
