//! One task per destination table: creates the table on first use, batches encoded rows by count,
//! bytes and linger time, appends them, and marks their offsets done once BigQuery acknowledges.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, Notify, OwnedSemaphorePermit};

use super::offsets::{Ticket, Tracker};
use crate::bq::ensure_table::{ensure_table, Ensured};
use crate::bq::rest::BigQueryRest;
use crate::bq::write::{TableRef, WriteClient, WriterOptions};
use crate::config::{RecordSchema, TimePartitioning};
use crate::metrics::Metrics;
use crate::proto::descriptor::TableDescriptor;

/// AppendRows accepts 10 MB per request; leave room for framing and the writer schema.
const MAX_REQUEST_BYTES: usize = 9 * 1024 * 1024 + 512 * 1024;
/// Upper bound on per-row framing (field tag + length prefix) inside ProtoRows.
const ROW_FRAMING: usize = 8;

pub enum TableMsg {
    Row {
        bytes: Bytes,
        ticket: Ticket,
        permit: OwnedSemaphorePermit,
    },
    Large {
        ndjson: Vec<u8>,
        ticket: Ticket,
        permit: OwnedSemaphorePermit,
    },
}

pub struct TableHandle {
    pub tx: mpsc::Sender<TableMsg>,
    pub flush: Arc<Notify>,
}

pub struct Shared {
    pub write: WriteClient,
    pub rest: Option<BigQueryRest>,
    pub tracker: Arc<Mutex<Tracker>>,
    pub metrics: Arc<Metrics>,
    pub fatal: mpsc::UnboundedSender<anyhow::Error>,
    pub max_rows: usize,
    pub linger: Duration,
    pub writer: WriterOptions,
    pub location: String,
    pub partitioning: Option<TimePartitioning>,
}

#[derive(Default)]
struct Batch {
    rows: Vec<Bytes>,
    tickets: Vec<Ticket>,
    permits: Vec<OwnedSemaphorePermit>,
    bytes: usize,
    started: Option<Instant>,
}

pub fn spawn(shared: Arc<Shared>, table: TableRef, schema: RecordSchema, desc: &TableDescriptor) -> TableHandle {
    let (tx, rx) = mpsc::channel(1024);
    let flush = Arc::new(Notify::new());
    let task = TableTask {
        overhead: desc.proto_schema_bytes().len(),
        writer: shared.write.table_writer(&table, desc.proto_schema.clone(), shared.writer.clone()),
        shared,
        table,
        schema,
        rx,
        flush: flush.clone(),
        batch: Batch::default(),
    };
    tokio::spawn(task.run());
    TableHandle { tx, flush }
}

struct TableTask {
    shared: Arc<Shared>,
    table: TableRef,
    schema: RecordSchema,
    writer: crate::bq::write::TableWriter,
    overhead: usize,
    rx: mpsc::Receiver<TableMsg>,
    flush: Arc<Notify>,
    batch: Batch,
}

impl TableTask {
    async fn run(mut self) {
        if let Some(rest) = &self.shared.rest {
            match ensure_table(rest, &self.table, &self.schema, self.shared.partitioning.as_ref()).await {
                Ok(Ensured::AddedColumns(_)) => self.writer.notify_schema_change(),
                Ok(_) => {}
                Err(e) => {
                    let _ = self.shared.fatal.send(e.context(format!("ensuring table {}", self.table.table)));
                    return;
                }
            }
        }
        loop {
            let deadline = self.batch.started.map(|t| tokio::time::Instant::from_std(t + self.shared.linger));
            tokio::select! {
                msg = self.rx.recv() => match msg {
                    None => {
                        self.flush_batch().await;
                        return;
                    }
                    Some(TableMsg::Row { bytes, ticket, permit }) => self.push(bytes, ticket, permit).await,
                    Some(TableMsg::Large { ndjson, ticket, permit }) => self.load(ndjson, ticket, permit),
                },
                _ = self.flush.notified() => self.flush_batch().await,
                _ = tokio::time::sleep_until(deadline.unwrap_or_else(tokio::time::Instant::now)), if deadline.is_some() => {
                    self.flush_batch().await
                }
            }
        }
    }

    async fn push(&mut self, bytes: Bytes, ticket: Ticket, permit: OwnedSemaphorePermit) {
        let size = bytes.len() + ROW_FRAMING;
        if !self.batch.rows.is_empty() && self.overhead + self.batch.bytes + size > MAX_REQUEST_BYTES {
            self.flush_batch().await;
        }
        let b = &mut self.batch;
        b.started.get_or_insert_with(Instant::now);
        b.bytes += size;
        b.rows.push(bytes);
        b.tickets.push(ticket);
        b.permits.push(permit);
        if b.rows.len() >= self.shared.max_rows {
            self.flush_batch().await;
        }
    }

    async fn flush_batch(&mut self) {
        let batch = std::mem::take(&mut self.batch);
        if batch.rows.is_empty() {
            return;
        }
        let (n, bytes) = (batch.rows.len() as u64, batch.bytes as u64);
        let sent = Instant::now();
        let ack = match self.writer.append(batch.rows).await {
            Ok(ack) => ack,
            Err(e) => {
                let _ = self.shared.fatal.send(anyhow::anyhow!("appending to {}: {e}", self.table.table));
                return;
            }
        };
        let shared = self.shared.clone();
        let table = self.table.table.clone();
        let (tickets, permits) = (batch.tickets, batch.permits);
        tokio::spawn(async move {
            match ack.await {
                Ok(Ok(())) => {
                    shared.metrics.observe_append(n, bytes, sent.elapsed());
                    let mut t = shared.tracker.lock().unwrap();
                    for ticket in &tickets {
                        t.done(ticket);
                    }
                    drop(permits);
                }
                Ok(Err(e)) => {
                    let _ = shared.fatal.send(anyhow::anyhow!("appending to {table}: {e}"));
                }
                Err(_) => {
                    let _ = shared.fatal.send(anyhow::anyhow!("appending to {table}: writer stopped"));
                }
            }
        });
    }

    fn load(&self, mut ndjson: Vec<u8>, ticket: Ticket, permit: OwnedSemaphorePermit) {
        let shared = self.shared.clone();
        let table = self.table.clone();
        tokio::spawn(async move {
            if let Some(rest) = &shared.rest {
                ndjson.push(b'\n');
                let result = async {
                    let live = rest.get_table(&table).await?;
                    let fields = live["schema"]["fields"].as_array().cloned().unwrap_or_default();
                    rest.load_ndjson(&table, &shared.location, &fields, ndjson).await
                }
                .await;
                if let Err(e) = result {
                    let _ = shared.fatal.send(anyhow::anyhow!("large-row load into {}: {e}", table.table));
                    return;
                }
            }
            Metrics::add(&shared.metrics.large_rows, 1);
            shared.tracker.lock().unwrap().done(&ticket);
            drop(permit);
        });
    }
}
