//! `bizon-stream run`: consume → decode → transform → encode → append → commit acknowledged offsets.
//!
//! The consumer task reads, tracks and dispatches messages in delivery order; decoding runs on a
//! bounded window of blocking tasks, and results reach the tables oldest first, so the effect is the
//! same as decoding inline. Appends run concurrently per table. In-flight rows are bounded by bytes,
//! and offsets are committed only past rows BigQuery has acknowledged (at-least-once, like bizon).

pub mod offsets;
pub mod table;

use std::collections::{HashMap, VecDeque};
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
use crate::kafka::client::{client_config, consumer_lag_by_topic};
use crate::kafka::message::{MessageParser, RawMessage};
use crate::kafka::registry::{Registry, RegistrySchema};
use crate::metrics::Metrics;
use crate::pipeline::{Outcome, Pipeline, PipelineError};
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
    /// Messages decoded concurrently. A single hot partition is otherwise bound to one core.
    pub decode_window: usize,
    /// AppendRows requests in flight per table connection. With slow acks this, not decoding, caps
    /// a table's throughput.
    pub append_depth: usize,
    /// /healthz fails once a consumed message has waited this long for its ack. Longer than the
    /// 5-minute append retry budget, after which the worker exits on its own.
    pub stall: Duration,
    /// How often idle partitions' offsets are committed again; zero disables it.
    pub recommit: Duration,
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
            decode_window: num("BIZON_RS_DECODE_WINDOW", 4).max(1) as usize,
            append_depth: num("BIZON_RS_APPEND_DEPTH", 4).max(1) as usize,
            stall: Duration::from_secs(num("BIZON_RS_STALL_SECS", 600)),
            recommit: Duration::from_secs(num("BIZON_RS_RECOMMIT_SECS", 6 * 3600)),
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

fn commit_async(consumer: &StreamConsumer<Ctx>, metrics: &Metrics, commits: &[(Tp, i64)]) -> bool {
    match consumer.commit(&commit_list(commits), CommitMode::Async) {
        Ok(()) => true,
        Err(e) if benign_commit_error(&e) => false,
        Err(e) => {
            Metrics::add(&metrics.commit_failures, 1);
            tracing::warn!(error = %e, "commit failed");
            false
        }
    }
}

/// Commits idle partitions' offsets again so the broker keeps them (see `Tracker::idle_commits`).
/// Partitions not committed by this worker yet take the broker's offset first.
async fn recommit_idle(
    consumer: &Arc<StreamConsumer<Ctx>>,
    tracker: &Mutex<Tracker>,
    metrics: &Metrics,
    topics: &HashMap<String, Arc<str>>,
) {
    let unknown = tracker.lock().unwrap().idle_commits().1;
    if !unknown.is_empty() {
        let mut tpl = TopicPartitionList::new();
        for tp in &unknown {
            tpl.add_partition(&tp.topic, tp.partition);
        }
        let c = consumer.clone();
        match tokio::task::spawn_blocking(move || c.committed_offsets(tpl, Duration::from_secs(30))).await {
            Ok(Ok(tpl)) => {
                let mut t = tracker.lock().unwrap();
                for e in tpl.elements() {
                    if let (Offset::Offset(o), Some(topic)) = (e.offset(), topics.get(e.topic())) {
                        t.adopt_committed(
                            &Tp {
                                topic: topic.clone(),
                                partition: e.partition(),
                            },
                            o,
                        );
                    }
                }
            }
            Ok(Err(e)) => tracing::warn!(error = %e, "reading committed offsets failed"),
            Err(e) => tracing::warn!(error = %e, "reading committed offsets failed"),
        }
    }
    let tracker = tracker.lock().unwrap();
    let (idle, _) = tracker.idle_commits();
    if !idle.is_empty() && commit_async(consumer, metrics, &idle) {
        Metrics::add(&metrics.recommits, idle.len() as u64);
    }
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

    fn stats_raw(&self, statistics: &[u8]) {
        if let Some(lag) = consumer_lag_by_topic(statistics) {
            self.metrics.set_consumer_lag(lag);
        }
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
                Err(e) => {
                    Metrics::add(&self.metrics.commit_failures, 1);
                    tracing::warn!(error = %e, "commit on revoke failed")
                }
            }
        }
        for tp in &tps {
            tracker.revoke(tp);
        }
        Metrics::add(&self.metrics.partitions_revoked, tps.len() as u64);
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
            Metrics::add(&self.metrics.partitions_assigned, tps.len() as u64);
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
                Metrics::add(&self.metrics.commit_failures, 1);
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
    crate::health::serve(opts.health_port, ready.clone(), metrics.clone(), opts.stall).await?;
    metrics
        .clone()
        .spawn_statsd(statsd_tags(&cfg, opts.hostname.as_deref()), pipeline_tags(&cfg));

    let tracker = Arc::new(Mutex::new(Tracker::default()));
    {
        let (tracker, metrics) = (tracker.clone(), metrics.clone());
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            loop {
                tick.tick().await;
                let oldest = tracker.lock().unwrap().oldest_pending();
                let age = oldest.map_or(0, |t| t.elapsed().as_secs());
                metrics.oldest_unacked_secs.store(age, Ordering::Relaxed);
            }
        });
    }
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
    if cc.get("statistics.interval.ms").is_none() {
        cc.set("statistics.interval.ms", "10000");
    }
    let consumer: Arc<StreamConsumer<Ctx>> = Arc::new(cc.create_with_context(ctx).context("creating Kafka consumer")?);
    let names: Vec<&str> = cfg.source.topics.iter().map(|t| t.name.as_str()).collect();
    consumer.subscribe(&names)?;
    tracing::info!(topics = names.len(), group = %cfg.source.group_id, commit = opts.commit, "subscribed");

    let (fatal_tx, mut fatal_rx) = mpsc::unbounded_channel();
    let shared = Arc::new(Shared {
        write: WriteClient::connect(opts.write_endpoint.as_deref(), None)
            .await?
            .with_metrics(metrics.clone()),
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
        writer: WriterOptions {
            max_inflight: opts.append_depth,
            ..WriterOptions::default()
        },
        location: cfg.destination.dataset_location.clone(),
        partitioning: cfg.destination.time_partitioning.clone(),
    });

    if opts.commit {
        let (consumer, tracker, metrics, topics) = (consumer.clone(), tracker.clone(), metrics.clone(), topics.clone());
        let recommit = opts.recommit;
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(5));
            let mut last_recommit = Instant::now();
            loop {
                tick.tick().await;
                if !recommit.is_zero() && last_recommit.elapsed() >= recommit {
                    last_recommit = Instant::now();
                    recommit_idle(&consumer, &tracker, &metrics, &topics).await;
                }
                // Committing under the tracker lock keeps these commits in order with the ones made on revoke.
                let mut tracker = tracker.lock().unwrap();
                let commits = tracker.take_commits(None);
                if !commits.is_empty() && commit_async(&consumer, &metrics, &commits) {
                    Metrics::add(&metrics.commits, 1);
                }
            }
        });
    }

    // `run` lives as long as the process, so the decode inputs are leaked to be shared with the
    // blocking decode tasks.
    let descriptors: &'static HashMap<String, TableDescriptor> = Box::leak(Box::new(
        cfg.destination
            .record_schemas
            .iter()
            .map(|s| Ok((s.destination_id.clone(), TableDescriptor::new(&s.columns())?)))
            .collect::<anyhow::Result<_>>()?,
    ));
    let parser: &'static MessageParser = Box::leak(Box::new(MessageParser::new(&cfg.source)));
    let lookup: &'static (dyn Fn(&str) -> Option<&'static TableDescriptor> + Sync) = Box::leak(Box::new(|id: &str| descriptors.get(id)));
    let pipeline: &'static Pipeline<'static> =
        Box::leak(Box::new(Pipeline::new(parser, Box::leak(Box::new(cfg.transform.clone())), lookup)));
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

    let window = opts.decode_window;
    let mut pending: VecDeque<Decoding> = VecDeque::with_capacity(window);
    let outcome: anyhow::Result<()> = loop {
        let event = tokio::select! {
            biased;
            _ = sigterm.recv() => break Ok(()),
            _ = tokio::signal::ctrl_c() => break Ok(()),
            Some(e) = fatal_rx.recv() => break Err(e),
            done = async { (&mut pending.front_mut().expect("guarded by the branch condition").task).await }, if !pending.is_empty() => Event::Decoded(done),
            m = consumer.recv(), if pending.len() < window => Event::Message(m),
        };
        let m = match event {
            Event::Decoded(done) => {
                let Decoding { ticket, at, .. } = pending.pop_front().expect("the front was just awaited");
                let (destination_id, large, data) = match done {
                    Ok(Ok(Outcome::Skipped(reason))) => {
                        metrics.skipped(reason);
                        tracker.lock().unwrap().done(&ticket);
                        continue;
                    }
                    Ok(Ok(Outcome::Row { destination_id, bytes })) => (destination_id, false, bytes),
                    Ok(Ok(Outcome::Large { destination_id, ndjson })) => (destination_id, true, ndjson),
                    Ok(Err(e)) => break Err(anyhow::anyhow!("{at}: {e}")),
                    Err(e) => break Err(anyhow::anyhow!("{at}: decode task failed: {e}")),
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
                    let h = table::spawn(shared.clone(), destination_id.clone(), table, schema, desc);
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
                continue;
            }
            Event::Message(Ok(m)) => m,
            Event::Message(Err(e)) => break Err(anyhow::Error::from(e).context("consuming")),
        };
        Metrics::add(&metrics.messages, 1);
        let header_vec = headers(&m);
        let raw = raw_message(&m, &header_vec);
        let schema = match parser.schema_id(&raw) {
            Some(id) => match schemas.get(&id) {
                Some(s) => Some((id, s.clone())),
                None => match registry.get(id).await {
                    Ok(s) => {
                        schemas.insert(id, s.clone());
                        Some((id, s))
                    }
                    Err(e) => break Err(anyhow::Error::from(e).context("schema registry")),
                },
            },
            None => None,
        };
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
        let owned = m.detach();
        let task = tokio::task::spawn_blocking(move || {
            let header_vec = headers(&owned);
            let raw = raw_message(&owned, &header_vec);
            let inserted_at = crate::timefmt::now_isoformat();
            pipeline.process(
                &raw,
                |id| schema.as_ref().filter(|(sid, _)| *sid == id).map(|(_, s)| s.clone()),
                &inserted_at,
            )
        });
        pending.push_back(Decoding { ticket, at, task });
    };

    // Decodes still pending were never sent; their offsets stay uncommitted and are re-read.
    let abandoned = pending.len();
    // After an error the failed message never completes, so don't wait the full timeout for it.
    let timeout = if outcome.is_ok() {
        opts.drain_timeout
    } else {
        opts.drain_timeout.min(Duration::from_secs(5))
    };
    drain(&tables, &tracker, timeout, abandoned).await;
    if opts.commit {
        let commits = tracker.lock().unwrap().take_commits(None);
        if !commits.is_empty() {
            if let Err(e) = consumer.commit(&commit_list(&commits), CommitMode::Sync) {
                Metrics::add(&metrics.commit_failures, 1);
                tracing::warn!(error = %e, "final commit failed");
            }
        }
    }
    tracing::info!(ok = outcome.is_ok(), "stopped");
    outcome
}

/// Without these, the pods of one pipeline (one per Kafka cluster when a pipeline spans several) report
/// under the same tags and overwrite each other's gauges.
fn statsd_tags(cfg: &Config, hostname: Option<&str>) -> String {
    let mut tags = vec![
        format!("pipeline:{}", cfg.name),
        format!("kafka_cluster:{}", cfg.transform.cluster()),
        format!("version:{}", env!("CARGO_PKG_VERSION")),
    ];
    if let Some(h) = hostname {
        tags.push(format!("pod_name:{h}"));
    }
    tags.join(",")
}

/// bizon's own tag keys, so dashboards on `bizon_pipeline.*` cover both runtimes. `kafka_cluster` tells
/// the Rust series apart; `pod_name` is left out because counters from co-located pods sum correctly at the
/// agent, and tables x pods would multiply the series count.
fn pipeline_tags(cfg: &Config) -> String {
    let mut tags = vec![format!("pipeline_name:{}", cfg.name)];
    if let Some(stream) = &cfg.source.stream {
        tags.push(format!("pipeline_stream:{stream}"));
    }
    tags.extend([
        format!("pipeline_source:{}", cfg.source.name),
        "pipeline_destination:bigquery_streaming_v2".to_string(),
        format!("kafka_cluster:{}", cfg.transform.cluster()),
        format!("version:{}", env!("CARGO_PKG_VERSION")),
    ]);
    tags.join(",")
}

/// Flushes every table and waits (bounded) for in-flight rows to be acknowledged. `abandoned` tracked
/// messages were never sent and will not complete.
async fn drain(tables: &HashMap<Arc<str>, TableHandle>, tracker: &Mutex<Tracker>, timeout: Duration, abandoned: usize) {
    for h in tables.values() {
        h.flush.notify_one();
    }
    let started = Instant::now();
    while tracker.lock().unwrap().in_flight(None) > abandoned && started.elapsed() < timeout {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

struct Decoding {
    ticket: offsets::Ticket,
    at: String,
    task: tokio::task::JoinHandle<Result<Outcome, PipelineError>>,
}

enum Event<M> {
    Decoded(Result<Result<Outcome, PipelineError>, tokio::task::JoinError>),
    Message(KafkaResult<M>),
}

fn headers<M: Message>(m: &M) -> Vec<(&str, Option<&[u8]>)> {
    m.headers()
        .map(|h| h.iter().map(|h| (h.key, h.value)).collect())
        .unwrap_or_default()
}

fn raw_message<'a, M: Message>(m: &'a M, headers: &'a [(&'a str, Option<&'a [u8]>)]) -> RawMessage<'a> {
    RawMessage {
        topic: m.topic(),
        partition: m.partition(),
        offset: m.offset(),
        timestamp_ms: m.timestamp().to_millis().unwrap_or(-1),
        key: m.key(),
        value: m.payload(),
        headers: m.headers().map(|_| headers),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pipeline_tags_use_bizon_keys() {
        let cfg = crate::config::tests::load("avro-cdc").unwrap();
        let tags = pipeline_tags(&cfg);
        assert!(
            tags.starts_with(
                "pipeline_name:avro-cdc,pipeline_stream:topic,pipeline_source:kafka,pipeline_destination:bigquery_streaming_v2,kafka_cluster:"
            ),
            "{tags}"
        );
    }
}
