use std::time::Duration;

use clap::Parser;
use fake_bqwrite::{Behaviour, FakeWrite};

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:50051")]
    addr: std::net::SocketAddr,
    #[arg(long, default_value_t = 0)]
    latency_ms: u64,
    #[arg(long)]
    close_after: Option<usize>,
    /// Proto field number to leave out of the distinct-row hash.
    #[arg(long)]
    ignore_field: Option<u32>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_ansi(false).init();
    let args = Args::parse();
    let behaviour = Behaviour {
        latency: Duration::from_millis(args.latency_ms),
        close_after: args.close_after,
        ignore_field: args.ignore_field,
        ..Default::default()
    };
    let (fake, addr) = FakeWrite::start(behaviour, args.addr).await?;
    tracing::info!(%addr, "fake BigQueryWrite listening");
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tick.tick().await;
        let r = fake.recorded();
        tracing::info!(
            connections = r.connections,
            requests = r.requests,
            rows = r.rows,
            distinct = r.distinct.len(),
            bytes = r.rows_bytes,
            "stats"
        );
    }
}
