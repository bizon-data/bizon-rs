//! Storage Write API client for the `_default` stream.
//!
//! One long-lived AppendRows connection per table, up to `max_inflight` requests pipelined on it,
//! acks matched FIFO. On a retryable failure the connection is dropped and every un-acked request is
//! resent in order on a new one (at-least-once; `_default` accepts duplicates).

use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use bqstorage_proto::storage::append_rows_request::{ProtoData, Rows};
use bqstorage_proto::storage::append_rows_response::Response;
use bqstorage_proto::storage::big_query_write_client::BigQueryWriteClient;
use bqstorage_proto::storage::{AppendRowsRequest, AppendRowsResponse, ProtoRows, ProtoSchema};
use bytes::Bytes;
use percent_encoding::{utf8_percent_encode, NON_ALPHANUMERIC};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Status, Streaming};

const DEFAULT_ENDPOINT: &str = "https://bigquerystorage.googleapis.com";
const SCOPES: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];

#[derive(Debug, Clone, thiserror::Error)]
pub enum AppendError {
    #[error("append rejected ({code:?}): {message}")]
    Rejected { code: Code, message: String },
    #[error("{count} row errors, first: {first}")]
    RowErrors { count: usize, first: String },
    #[error("gave up after {attempts} attempts over {elapsed:?}: {last}")]
    RetriesExhausted { attempts: u32, elapsed: Duration, last: String },
    #[error("writer stopped")]
    Closed,
}

#[derive(Debug, Clone)]
pub struct WriterOptions {
    pub max_inflight: usize,
    pub idle_timeout: Duration,
    pub backoff_initial: Duration,
    pub backoff_max: Duration,
    pub retry_budget: Duration,
    /// How long after a schema change INVALID_ARGUMENT is treated as propagation lag and retried.
    pub schema_change_window: Duration,
}

impl Default for WriterOptions {
    fn default() -> Self {
        Self {
            max_inflight: 4,
            idle_timeout: Duration::from_secs(60),
            backoff_initial: Duration::from_secs(1),
            backoff_max: Duration::from_secs(60),
            retry_budget: Duration::from_secs(300),
            schema_change_window: Duration::from_secs(600),
        }
    }
}

#[derive(Clone)]
pub struct WriteClient {
    channel: Channel,
    auth: Option<Arc<dyn gcp_auth::TokenProvider>>,
    quota_project: Option<String>,
}

impl WriteClient {
    /// `endpoint` overrides the Google endpoint; a plain `http://` endpoint skips TLS and auth (fake server).
    pub async fn connect(endpoint: Option<&str>, quota_project: Option<String>) -> anyhow::Result<Self> {
        let uri = endpoint.unwrap_or(DEFAULT_ENDPOINT);
        let insecure = uri.starts_with("http://");
        let mut ep = Endpoint::from_shared(uri.to_string())?
            .http2_adaptive_window(true)
            .http2_keep_alive_interval(Duration::from_secs(30))
            .keep_alive_while_idle(true)
            .connect_timeout(Duration::from_secs(10));
        if !insecure {
            ep = ep.tls_config(ClientTlsConfig::new().with_webpki_roots())?;
        }
        let auth = if insecure { None } else { Some(gcp_auth::provider().await?) };
        Ok(Self {
            channel: ep.connect_lazy(),
            auth,
            quota_project,
        })
    }

    pub fn table_writer(&self, table: &TableRef, schema: ProtoSchema, opts: WriterOptions) -> TableWriter {
        let (tx, rx) = mpsc::channel(opts.max_inflight);
        let schema_changed_at = Arc::new(AtomicU64::new(0));
        let task = WriterTask {
            client: self.clone(),
            stream: table.default_stream(),
            schema,
            opts,
            rx,
            schema_changed_at: schema_changed_at.clone(),
            conn: None,
            inflight: VecDeque::new(),
        };
        tokio::spawn(task.run());
        TableWriter { tx, schema_changed_at }
    }

    async fn open(&self, stream: &str, first: AppendRowsRequest, depth: usize) -> Result<Conn, Status> {
        let (tx, rx) = mpsc::channel(depth);
        // The first request must be queued before awaiting the call: the server sends no headers
        // until it has seen one.
        tx.send(first).await.map_err(|_| Status::internal("request channel closed"))?;
        let mut req = tonic::Request::new(ReceiverStream::new(rx));
        let md = req.metadata_mut();
        let params = format!("write_stream={}", utf8_percent_encode(stream, NON_ALPHANUMERIC));
        md.insert("x-goog-request-params", meta(&params)?);
        if let Some(p) = &self.quota_project {
            md.insert("x-goog-user-project", meta(p)?);
        }
        if let Some(auth) = &self.auth {
            let token = auth
                .token(SCOPES)
                .await
                .map_err(|e| Status::unavailable(format!("auth token: {e}")))?;
            md.insert("authorization", meta(&format!("Bearer {}", token.as_str()))?);
        }
        let resp = BigQueryWriteClient::new(self.channel.clone())
            .max_encoding_message_size(usize::MAX)
            .append_rows(req)
            .await?
            .into_inner();
        Ok(Conn { tx, resp })
    }
}

#[allow(clippy::result_large_err)]
fn meta(v: &str) -> Result<MetadataValue<tonic::metadata::Ascii>, Status> {
    v.parse().map_err(|_| Status::internal("invalid metadata value"))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableRef {
    pub project: String,
    pub dataset: String,
    pub table: String,
}

impl TableRef {
    pub fn parse(id: &str) -> Option<Self> {
        let mut it = id.split('.');
        let (p, d, t) = (it.next()?, it.next()?, it.next()?);
        it.next().is_none().then(|| Self {
            project: p.into(),
            dataset: d.into(),
            table: t.into(),
        })
    }

    pub fn default_stream(&self) -> String {
        format!(
            "projects/{}/datasets/{}/tables/{}/streams/_default",
            self.project, self.dataset, self.table
        )
    }
}

pub struct TableWriter {
    tx: mpsc::Sender<Job>,
    schema_changed_at: Arc<AtomicU64>,
}

pub type AppendResult = Result<(), AppendError>;

impl TableWriter {
    /// Waits for a pipeline slot (back-pressure), then returns a receiver that resolves on ack.
    pub async fn append(&self, rows: Vec<Bytes>) -> Result<oneshot::Receiver<AppendResult>, AppendError> {
        let (ack, rx) = oneshot::channel();
        self.tx.send(Job { rows, ack }).await.map_err(|_| AppendError::Closed)?;
        Ok(rx)
    }

    pub fn notify_schema_change(&self) {
        self.schema_changed_at.store(now_millis(), Ordering::Relaxed);
    }
}

fn now_millis() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_millis() as u64
}

struct Job {
    rows: Vec<Bytes>,
    ack: oneshot::Sender<AppendResult>,
}

struct InFlight {
    rows: Vec<Bytes>,
    ack: oneshot::Sender<AppendResult>,
    first_sent: Instant,
    attempts: u32,
}

struct Conn {
    tx: mpsc::Sender<AppendRowsRequest>,
    resp: Streaming<AppendRowsResponse>,
}

enum Disposition {
    Retry(String),
    Fatal(AppendError),
}

struct WriterTask {
    client: WriteClient,
    stream: String,
    schema: ProtoSchema,
    opts: WriterOptions,
    rx: mpsc::Receiver<Job>,
    schema_changed_at: Arc<AtomicU64>,
    conn: Option<Conn>,
    inflight: VecDeque<InFlight>,
}

impl WriterTask {
    async fn run(mut self) {
        let mut closed = false;
        loop {
            if closed && self.inflight.is_empty() {
                return;
            }
            let can_take = !closed && self.inflight.len() < self.opts.max_inflight;
            tokio::select! {
                biased;
                msg = next_response(&mut self.conn), if !self.inflight.is_empty() => {
                    self.on_response(msg).await;
                }
                job = self.rx.recv(), if can_take => match job {
                    Some(job) => self.submit(job).await,
                    None => closed = true,
                },
                _ = tokio::time::sleep(self.opts.idle_timeout), if self.inflight.is_empty() && self.conn.is_some() => {
                    self.conn = None;
                }
            }
        }
    }

    fn request(&self, rows: &[Bytes], with_schema: bool) -> AppendRowsRequest {
        AppendRowsRequest {
            write_stream: if with_schema { self.stream.clone() } else { String::new() },
            rows: Some(Rows::ProtoRows(ProtoData {
                writer_schema: with_schema.then(|| self.schema.clone()),
                rows: Some(ProtoRows {
                    serialized_rows: rows.to_vec(),
                }),
            })),
            ..Default::default()
        }
    }

    async fn submit(&mut self, job: Job) {
        self.inflight.push_back(InFlight {
            rows: job.rows,
            ack: job.ack,
            first_sent: Instant::now(),
            attempts: 1,
        });
        let idx = self.inflight.len() - 1;
        if let Err(reason) = self.send(idx).await {
            self.recover(reason).await;
        }
    }

    /// Sends `inflight[idx]` on the current connection, opening one if needed.
    async fn send(&mut self, idx: usize) -> Result<(), String> {
        if let Some(conn) = &self.conn {
            let req = self.request(&self.inflight[idx].rows, false);
            return conn.tx.send(req).await.map_err(|_| "connection closed while sending".to_string());
        }
        let req = self.request(&self.inflight[idx].rows, true);
        match self.client.open(&self.stream, req, self.opts.max_inflight + 1).await {
            Ok(conn) => {
                self.conn = Some(conn);
                Ok(())
            }
            Err(status) => match self.classify(&status) {
                Disposition::Retry(r) => Err(r),
                Disposition::Fatal(e) => {
                    // Opening failed for a non-retryable reason: nothing queued can succeed.
                    self.fail_all(e);
                    Err(String::new())
                }
            },
        }
    }

    async fn on_response(&mut self, msg: Result<Option<AppendRowsResponse>, Status>) {
        let outcome = match msg {
            Ok(Some(resp)) => match resp.response {
                Some(Response::Error(st)) => {
                    let status = Status::new(Code::from_i32(st.code), st.message);
                    Err(self.classify(&status))
                }
                _ if !resp.row_errors.is_empty() => Err(Disposition::Fatal(AppendError::RowErrors {
                    count: resp.row_errors.len(),
                    first: format!("row {}: {}", resp.row_errors[0].index, resp.row_errors[0].message),
                })),
                _ => Ok(()),
            },
            Ok(None) => Err(Disposition::Retry("server closed the stream".into())),
            Err(status) => Err(self.classify(&status)),
        };
        match outcome {
            Ok(()) => {
                let done = self.inflight.pop_front().expect("response without request");
                let _ = done.ack.send(Ok(()));
            }
            Err(Disposition::Fatal(e)) => {
                let done = self.inflight.pop_front().expect("response without request");
                let _ = done.ack.send(Err(e));
                self.conn = None;
                if !self.inflight.is_empty() {
                    self.recover("resending after a rejected request".into()).await;
                }
            }
            Err(Disposition::Retry(reason)) => self.recover(reason).await,
        }
    }

    /// Drops the connection and resends every un-acked request in order, backing off between attempts.
    async fn recover(&mut self, mut reason: String) {
        loop {
            self.conn = None;
            if self.inflight.is_empty() {
                return;
            }
            let budget = self.opts.retry_budget;
            while let Some(front) = self.inflight.front() {
                if front.first_sent.elapsed() < budget {
                    break;
                }
                let f = self.inflight.pop_front().unwrap();
                let _ = f.ack.send(Err(AppendError::RetriesExhausted {
                    attempts: f.attempts,
                    elapsed: f.first_sent.elapsed(),
                    last: reason.clone(),
                }));
            }
            let Some(attempts) = self.inflight.front().map(|f| f.attempts) else {
                return;
            };
            let backoff = self.opts.backoff_initial.saturating_mul(1 << attempts.saturating_sub(1).min(16));
            tracing::warn!(stream = %self.stream, %reason, attempts, ?backoff, pending = self.inflight.len(), "append retry");
            tokio::time::sleep(backoff.min(self.opts.backoff_max)).await;

            let mut failed = None;
            for idx in 0..self.inflight.len() {
                self.inflight[idx].attempts += 1;
                if let Err(r) = self.send(idx).await {
                    failed = Some(r);
                    break;
                }
            }
            match failed {
                None => return,
                Some(r) => reason = r,
            }
        }
    }

    fn fail_all(&mut self, e: AppendError) {
        for f in self.inflight.drain(..) {
            let _ = f.ack.send(Err(e.clone()));
        }
    }

    fn classify(&self, status: &Status) -> Disposition {
        let msg = || format!("{:?}: {}", status.code(), status.message());
        match status.code() {
            Code::Unavailable
            | Code::Internal
            | Code::Aborted
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Unknown
            | Code::Cancelled => Disposition::Retry(msg()),
            Code::InvalidArgument if self.in_schema_change_window() && status.message().to_lowercase().contains("schema") => {
                Disposition::Retry(msg())
            }
            code => Disposition::Fatal(AppendError::Rejected {
                code,
                message: status.message().to_string(),
            }),
        }
    }

    fn in_schema_change_window(&self) -> bool {
        let at = self.schema_changed_at.load(Ordering::Relaxed);
        at != 0 && now_millis().saturating_sub(at) < self.opts.schema_change_window.as_millis() as u64
    }
}

async fn next_response(conn: &mut Option<Conn>) -> Result<Option<AppendRowsResponse>, Status> {
    match conn {
        Some(c) => c.resp.message().await,
        // In-flight requests with no connection only happen mid-recovery; report it as a closed stream.
        None => Ok(None),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_ref_and_default_stream() {
        let t = TableRef::parse("p.d.t").unwrap();
        assert_eq!(t.default_stream(), "projects/p/datasets/d/tables/t/streams/_default");
        assert!(TableRef::parse("d.t").is_none());
        assert!(TableRef::parse("a.b.c.d").is_none());
    }
}
