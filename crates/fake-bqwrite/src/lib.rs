//! In-process BigQueryWrite server for tests and benchmarks: acks AppendRows on any stream, records
//! what it received, and can inject latency, errors and server-side stream closes.

use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bqstorage_proto::storage::append_rows_request::Rows;
use bqstorage_proto::storage::append_rows_response::{AppendResult, Response};
use bqstorage_proto::storage::big_query_write_server::{BigQueryWrite, BigQueryWriteServer};
use bqstorage_proto::storage::*;
use bqstorage_proto::RpcStatus;
use bytes::Bytes;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tokio_stream::{Stream, StreamExt};
use tonic::{Code, Request, Status, Streaming};

#[derive(Debug, Clone, Default)]
pub struct Behaviour {
    pub latency: Duration,
    /// End the response stream after this many responses on a connection.
    pub close_after: Option<usize>,
    /// Answer every nth request (1-based, counted globally) with this error in the response.
    pub error_every: Option<(usize, Code, String)>,
    /// Answer every nth request with one row error.
    pub row_error_every: Option<usize>,
    /// Keep acked row bytes in memory (tests); off for long benchmark runs.
    pub retain_rows: bool,
    /// Proto field number left out when hashing rows for `distinct`, e.g. a wall-clock column that
    /// differs between re-deliveries of the same message.
    pub ignore_field: Option<u32>,
}

#[derive(Debug, Default)]
pub struct Recorded {
    pub connections: usize,
    pub requests: usize,
    pub rows: usize,
    /// Hashes of acked rows, so duplicates (expected under at-least-once) can be told from loss.
    pub distinct: std::collections::HashSet<u64>,
    pub acked_rows: Vec<Bytes>,
    pub rows_bytes: usize,
    /// Protocol violations seen (missing schema on first request, missing routing header, ...).
    pub violations: Vec<String>,
}

#[derive(Clone, Default)]
pub struct FakeWrite {
    pub behaviour: Arc<Mutex<Behaviour>>,
    pub recorded: Arc<Mutex<Recorded>>,
}

type RespStream = Pin<Box<dyn Stream<Item = Result<AppendRowsResponse, Status>> + Send>>;

impl FakeWrite {
    pub async fn start(behaviour: Behaviour, addr: SocketAddr) -> anyhow::Result<(Self, SocketAddr)> {
        let svc = FakeWrite {
            behaviour: Arc::new(Mutex::new(behaviour)),
            ..Default::default()
        };
        let listener = TcpListener::bind(addr).await?;
        let local = listener.local_addr()?;
        let server = BigQueryWriteServer::new(svc.clone()).max_decoding_message_size(usize::MAX);
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(server)
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        Ok((svc, local))
    }

    pub fn recorded(&self) -> std::sync::MutexGuard<'_, Recorded> {
        self.recorded.lock().unwrap()
    }
}

#[tonic::async_trait]
impl BigQueryWrite for FakeWrite {
    type AppendRowsStream = RespStream;

    async fn append_rows(&self, req: Request<Streaming<AppendRowsRequest>>) -> Result<tonic::Response<RespStream>, Status> {
        let routed = req
            .metadata()
            .get("x-goog-request-params")
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| v.starts_with("write_stream=projects%2F"));
        let mut inbound = req.into_inner();
        let behaviour = self.behaviour.lock().unwrap().clone();
        let recorded = self.recorded.clone();
        {
            let mut r = recorded.lock().unwrap();
            r.connections += 1;
            if !routed {
                r.violations.push("missing or malformed x-goog-request-params".into());
            }
        }

        let out = async_stream_from(move |tx| async move {
            let mut on_conn = 0usize;
            while let Some(msg) = inbound.next().await {
                let msg = match msg {
                    Ok(m) => m,
                    Err(_) => return,
                };
                if !behaviour.latency.is_zero() {
                    tokio::time::sleep(behaviour.latency).await;
                }
                on_conn += 1;
                let Some(Rows::ProtoRows(data)) = msg.rows else {
                    let _ = tx.send(Err(Status::invalid_argument("rows must be proto_rows"))).await;
                    return;
                };
                let n = {
                    let mut r = recorded.lock().unwrap();
                    r.requests += 1;
                    if on_conn == 1 && (data.writer_schema.is_none() || msg.write_stream.is_empty()) {
                        r.violations
                            .push("first request on a connection lacks writer_schema/write_stream".into());
                    }
                    r.requests
                };
                let rows = data.rows.map(|r| r.serialized_rows).unwrap_or_default();

                let resp = if let Some((_, code, message)) = behaviour.error_every.as_ref().filter(|(e, ..)| n % e == 0) {
                    AppendRowsResponse {
                        response: Some(Response::Error(RpcStatus {
                            code: *code as i32,
                            message: message.clone(),
                            details: vec![],
                        })),
                        ..Default::default()
                    }
                } else if behaviour.row_error_every.is_some_and(|e| n % e == 0) {
                    AppendRowsResponse {
                        row_errors: vec![RowError {
                            index: 0,
                            code: row_error::RowErrorCode::FieldsError as i32,
                            message: "injected row error".into(),
                        }],
                        ..Default::default()
                    }
                } else {
                    let mut r = recorded.lock().unwrap();
                    r.rows_bytes += rows.iter().map(|b| b.len()).sum::<usize>();
                    r.rows += rows.len();
                    for row in &rows {
                        r.distinct.insert(row_hash(row, behaviour.ignore_field));
                    }
                    if behaviour.retain_rows {
                        r.acked_rows.extend(rows);
                    }
                    AppendRowsResponse {
                        response: Some(Response::AppendResult(AppendResult { offset: None })),
                        ..Default::default()
                    }
                };
                if tx.send(Ok(resp)).await.is_err() {
                    return;
                }
                if behaviour.close_after.is_some_and(|c| on_conn >= c) {
                    return;
                }
            }
        });
        Ok(tonic::Response::new(out))
    }

    async fn create_write_stream(&self, _: Request<CreateWriteStreamRequest>) -> Result<tonic::Response<WriteStream>, Status> {
        Err(Status::unimplemented("fake: only _default is supported"))
    }
    async fn get_write_stream(&self, _: Request<GetWriteStreamRequest>) -> Result<tonic::Response<WriteStream>, Status> {
        Err(Status::unimplemented("fake"))
    }
    async fn finalize_write_stream(
        &self,
        _: Request<FinalizeWriteStreamRequest>,
    ) -> Result<tonic::Response<FinalizeWriteStreamResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }
    async fn batch_commit_write_streams(
        &self,
        _: Request<BatchCommitWriteStreamsRequest>,
    ) -> Result<tonic::Response<BatchCommitWriteStreamsResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }
    async fn flush_rows(&self, _: Request<FlushRowsRequest>) -> Result<tonic::Response<FlushRowsResponse>, Status> {
        Err(Status::unimplemented("fake"))
    }
}

fn async_stream_from<F, Fut>(f: F) -> RespStream
where
    F: FnOnce(tokio::sync::mpsc::Sender<Result<AppendRowsResponse, Status>>) -> Fut,
    Fut: std::future::Future<Output = ()> + Send + 'static,
{
    let (tx, rx) = tokio::sync::mpsc::channel(16);
    tokio::spawn(f(tx));
    Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx))
}

fn row_hash(row: &[u8], ignore: Option<u32>) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    let Some(ignore) = ignore else {
        row.hash(&mut h);
        return h.finish();
    };
    let varint = |b: &mut &[u8]| -> Option<u64> {
        let (mut v, mut shift) = (0u64, 0);
        loop {
            let (&byte, rest) = b.split_first()?;
            *b = rest;
            v |= ((byte & 0x7f) as u64) << shift;
            if byte & 0x80 == 0 {
                return Some(v);
            }
            shift += 7;
        }
    };
    let mut b = row;
    while !b.is_empty() {
        let start = b;
        let Some(tag) = varint(&mut b) else { break };
        let len = match tag & 7 {
            0 => {
                varint(&mut b);
                0
            }
            1 => 8,
            2 => varint(&mut b).unwrap_or(0) as usize,
            5 => 4,
            _ => break,
        };
        b = &b[len.min(b.len())..];
        if (tag >> 3) as u32 != ignore {
            start[..start.len() - b.len()].hash(&mut h);
        }
    }
    h.finish()
}
