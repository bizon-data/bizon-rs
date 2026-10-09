use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use std::path::PathBuf;

use bizon_stream::bq::write::{AppendResult, TableRef, WriteClient, WriterOptions};
use bizon_stream::config::Config;
use bizon_stream::kafka::capture::{capture, CaptureOptions};
use bizon_stream::proto::descriptor::{Column, TableDescriptor};
use bizon_stream::proto::encode::{encode_row, Value};
use bytes::Bytes;
use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(name = "bizon-stream", version)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Push synthetic rows through AppendRows to measure the write client.
    /// Target table schema: `id INTEGER REQUIRED, payload JSON, __inserted_at TIMESTAMP`.
    AppendSmoke(SmokeArgs),
    /// Run the streaming worker on a rendered bizon config.yml.
    Run {
        #[arg(long, default_value = "/app/config/config.yml")]
        config: PathBuf,
    },
    /// Validate a rendered bizon config.yml against what this worker supports.
    CheckConfig {
        #[arg(long, default_value = "/app/config/config.yml")]
        config: PathBuf,
    },
    /// Read a sample of messages into parity fixtures, without joining a group or committing.
    Capture {
        #[arg(long, default_value = "/app/config/config.yml")]
        config: PathBuf,
        /// Start point: epoch milliseconds, or a duration ago such as `30m`, `6h`, `2d`.
        #[arg(long, default_value = "1h")]
        since: String,
        #[arg(long, default_value_t = 200)]
        per_topic: usize,
        #[arg(long, default_value_t = 300)]
        max_wait_secs: u64,
        /// Comma-separated subset of the config's topics.
        #[arg(long, value_delimiter = ',')]
        topics: Option<Vec<String>>,
        #[arg(long)]
        out: PathBuf,
    },
}

#[derive(Args)]
struct SmokeArgs {
    #[arg(long)]
    table: String,
    /// Stop after this many rows per writer, or when --duration-secs elapses, whichever comes first.
    #[arg(long, default_value_t = u64::MAX)]
    rows: u64,
    #[arg(long)]
    duration_secs: Option<u64>,
    #[arg(long, default_value_t = 500)]
    batch: u64,
    /// Approximate payload size per row in bytes.
    #[arg(long, default_value_t = 1024)]
    row_bytes: usize,
    #[arg(long, default_value_t = 4)]
    max_inflight: usize,
    /// Independent writers (connections) appending to the same table concurrently.
    #[arg(long, default_value_t = 1)]
    writers: usize,
    /// e.g. http://127.0.0.1:50051 for fake-bqwrite.
    #[arg(long, env = "BIZON_RS_BQ_WRITE_ENDPOINT")]
    endpoint: Option<String>,
    #[arg(long)]
    quota_project: Option<String>,
}

// glibc malloc keeps freed payload memory in per-thread arenas, so RSS ratchets up on pods holding
// many partitions of large messages; jemalloc returns it.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    match Cli::parse().cmd {
        Cmd::AppendSmoke(args) => append_smoke(args).await,
        Cmd::Run { config } => {
            let cfg = load_config(&config)?;
            let opts = bizon_stream::worker::RunOptions::from_env(&cfg);
            tracing::info!(?opts, pipeline = %cfg.name, "starting");
            bizon_stream::worker::run(cfg, opts).await
        }
        Cmd::CheckConfig { config } => {
            let cfg = load_config(&config)?;
            println!(
                "ok: {} topics, {} record schemas, transform {:?}, encoding {:?}, batch_size {}, consumer_timeout {}s",
                cfg.source.topics.len(),
                cfg.destination.record_schemas.len(),
                cfg.transform,
                cfg.source.message_encoding,
                cfg.source.batch_size,
                cfg.source.consumer_timeout
            );
            Ok(())
        }
        Cmd::Capture {
            config,
            since,
            per_topic,
            max_wait_secs,
            topics,
            out,
        } => {
            let cfg = load_config(&config)?;
            let opts = CaptureOptions {
                since_ms: parse_since(&since)?,
                per_topic,
                max_wait: Duration::from_secs(max_wait_secs),
                topics,
            };
            capture(&cfg, &opts, &out).await
        }
    }
}

type AckRx = tokio::sync::oneshot::Receiver<AppendResult>;

fn load_config(path: &std::path::Path) -> anyhow::Result<Config> {
    let text = std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    Ok(Config::from_yaml(&text, |name| std::env::var(name).ok())?)
}

fn parse_since(s: &str) -> anyhow::Result<i64> {
    if let Ok(ms) = s.parse::<i64>() {
        return Ok(ms);
    }
    let (n, unit) = s.split_at(s.len().saturating_sub(1));
    let n: i64 = n.parse().with_context(|| format!("invalid --since {s}"))?;
    let secs = match unit {
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86400,
        _ => anyhow::bail!("invalid --since unit in {s} (use m, h or d)"),
    };
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as i64;
    Ok(now - secs * 1000)
}

#[derive(Default)]
struct Stats {
    rows: AtomicU64,
    bytes: AtomicU64,
    requests: AtomicU64,
}

async fn append_smoke(args: SmokeArgs) -> anyhow::Result<()> {
    if args.rows == u64::MAX && args.duration_secs.is_none() {
        anyhow::bail!("pass --rows or --duration-secs");
    }
    let table = TableRef::parse(&args.table).context("--table must be project.dataset.table")?;
    let col = |name: &str, t: &str, required| Column {
        name: name.into(),
        bq_type: t.into(),
        required,
    };
    let desc = Arc::new(TableDescriptor::new(&[
        col("id", "INTEGER", true),
        col("payload", "JSON", false),
        col("__inserted_at", "TIMESTAMP", false),
    ])?);
    let client = WriteClient::connect(args.endpoint.as_deref(), args.quota_project.clone()).await?;
    let deadline = args.duration_secs.map(|s| Instant::now() + Duration::from_secs(s));
    let stats = Arc::new(Stats::default());
    let started = Instant::now();

    let progress = {
        let stats = stats.clone();
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(Duration::from_secs(10));
            tick.tick().await;
            let mut last = 0u64;
            loop {
                tick.tick().await;
                let b = stats.bytes.load(Ordering::Relaxed);
                println!(
                    "t={:>5.0}s acked_rows={} acked={:.0}MiB last10s={:.1}MiB/s",
                    started.elapsed().as_secs_f64(),
                    stats.rows.load(Ordering::Relaxed),
                    b as f64 / 1_048_576.0,
                    (b - last) as f64 / 1_048_576.0 / 10.0
                );
                last = b;
            }
        })
    };

    let mut tasks = Vec::new();
    for w in 0..args.writers {
        let writer = client.table_writer(
            &table,
            desc.proto_schema.clone(),
            WriterOptions {
                max_inflight: args.max_inflight,
                ..Default::default()
            },
        );
        let (desc, stats) = (desc.clone(), stats.clone());
        let (rows, batch, row_bytes) = (args.rows, args.batch, args.row_bytes);
        tasks.push(tokio::spawn(async move {
            let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel::<(Instant, u64, u64, AckRx)>();
            let collector = tokio::spawn({
                let stats = stats.clone();
                async move {
                    let mut lat = Vec::new();
                    while let Some((sent, n, bytes, ack)) = ack_rx.recv().await {
                        ack.await.context("writer task dropped")??;
                        lat.push(sent.elapsed());
                        stats.rows.fetch_add(n, Ordering::Relaxed);
                        stats.bytes.fetch_add(bytes, Ordering::Relaxed);
                        stats.requests.fetch_add(1, Ordering::Relaxed);
                    }
                    anyhow::Ok(lat)
                }
            });
            let filler = "x".repeat(row_bytes.saturating_sub(40));
            let base = (w as i64) << 40;
            let mut next = 0u64;
            while next < rows && deadline.is_none_or(|d| Instant::now() < d) {
                let n = batch.min(rows - next);
                let mut batch_rows = Vec::with_capacity(n as usize);
                let mut bytes = 0u64;
                for i in next..next + n {
                    let id = base + i as i64;
                    let payload = format!(r#"{{"id":{id},"filler":"{filler}"}}"#);
                    let mut out = Vec::with_capacity(payload.len() + 48);
                    encode_row(
                        &desc,
                        [
                            ("id", Value::Int(id)),
                            ("payload", Value::Str(payload.into())),
                            ("__inserted_at", Value::Str("2026-01-01T00:00:00.123456".into())),
                        ],
                        &mut out,
                    )?;
                    bytes += out.len() as u64;
                    batch_rows.push(Bytes::from(out));
                }
                let sent = Instant::now();
                let ack = writer.append(batch_rows).await?;
                ack_tx.send((sent, n, bytes, ack)).ok();
                next += n;
            }
            drop(ack_tx);
            collector.await?
        }));
    }

    let mut lat = Vec::new();
    for t in tasks {
        lat.extend(t.await??);
    }
    progress.abort();
    lat.sort();
    let pct = |p: f64| {
        lat.get(((lat.len() as f64 * p) as usize).min(lat.len().saturating_sub(1)))
            .copied()
            .unwrap_or_default()
    };
    let elapsed = started.elapsed().as_secs_f64();
    let mib = stats.bytes.load(Ordering::Relaxed) as f64 / 1_048_576.0;
    println!(
        "done: writers={} rows={} requests={} bytes={:.1}MiB elapsed={:.1}s throughput={:.1}MiB/s ack_p50={:?} ack_p99={:?} ack_max={:?}",
        args.writers,
        stats.rows.load(Ordering::Relaxed),
        stats.requests.load(Ordering::Relaxed),
        mib,
        elapsed,
        mib / elapsed,
        pct(0.5),
        pct(0.99),
        lat.last().copied().unwrap_or_default()
    );
    Ok(())
}
