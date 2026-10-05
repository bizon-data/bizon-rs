//! /healthz (process alive), /readyz (partitions assigned) and /metrics on a plain TCP listener.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::metrics::Metrics;

pub async fn serve(port: u16, ready: Arc<AtomicBool>, metrics: Arc<Metrics>) -> anyhow::Result<()> {
    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tokio::spawn(async move {
        while let Ok((mut sock, _)) = listener.accept().await {
            let (ready, metrics) = (ready.clone(), metrics.clone());
            tokio::spawn(async move {
                let mut buf = [0u8; 1024];
                let n = sock.read(&mut buf).await.unwrap_or(0);
                let line = String::from_utf8_lossy(&buf[..n]);
                let path = line.split_whitespace().nth(1).unwrap_or("/");
                let (status, body) = match path {
                    "/healthz" => ("200 OK", "ok\n".to_string()),
                    "/readyz" if ready.load(Ordering::Relaxed) => ("200 OK", "ready\n".to_string()),
                    "/readyz" => ("503 Service Unavailable", "not ready\n".to_string()),
                    "/metrics" => ("200 OK", metrics.prometheus()),
                    _ => ("404 Not Found", String::new()),
                };
                let resp = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: text/plain\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                let _ = sock.write_all(resp.as_bytes()).await;
            });
        }
    });
    Ok(())
}
