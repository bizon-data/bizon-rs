//! The few BigQuery REST calls the worker needs: table create/get/patch and NDJSON load jobs.

use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use super::write::TableRef;

const SCOPES: &[&str] = &["https://www.googleapis.com/auth/cloud-platform"];
const DEFAULT_BASE: &str = "https://bigquery.googleapis.com";

#[derive(Debug, thiserror::Error)]
pub enum RestError {
    #[error("{method} {url}: HTTP {status}: {body}")]
    Status {
        method: &'static str,
        url: String,
        status: u16,
        body: String,
    },
    #[error("{0}")]
    Transport(String),
    #[error("load job {job}: {error}")]
    Job { job: String, error: String },
}

impl RestError {
    pub fn status(&self) -> Option<u16> {
        match self {
            RestError::Status { status, .. } => Some(*status),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct BigQueryRest {
    http: reqwest::Client,
    base: String,
    auth: Option<Arc<dyn gcp_auth::TokenProvider>>,
}

impl BigQueryRest {
    /// `base` overrides the API root (tests); auth is skipped for plain `http://` roots.
    pub async fn new(base: Option<&str>) -> anyhow::Result<Self> {
        let base = base.unwrap_or(DEFAULT_BASE).trim_end_matches('/').to_string();
        let auth = if base.starts_with("http://") {
            None
        } else {
            Some(gcp_auth::provider().await?)
        };
        Ok(Self {
            http: reqwest::Client::builder().timeout(Duration::from_secs(300)).build()?,
            base,
            auth,
        })
    }

    async fn send(
        &self,
        method: &'static str,
        url: String,
        build: impl Fn(reqwest::RequestBuilder) -> reqwest::RequestBuilder,
    ) -> Result<Value, RestError> {
        let mut delay = Duration::from_secs(1);
        for attempt in 1.. {
            let mut req = match method {
                "GET" => self.http.get(&url),
                "POST" => self.http.post(&url),
                _ => self.http.patch(&url),
            };
            if let Some(auth) = &self.auth {
                let token = auth
                    .token(SCOPES)
                    .await
                    .map_err(|e| RestError::Transport(format!("auth token: {e}")))?;
                req = req.bearer_auth(token.as_str());
            }
            let retryable = match build(req).send().await {
                Ok(resp) if resp.status().is_success() => {
                    let text = resp.text().await.map_err(|e| RestError::Transport(e.to_string()))?;
                    return Ok(if text.is_empty() {
                        Value::Null
                    } else {
                        serde_json::from_str(&text).map_err(|e| RestError::Transport(e.to_string()))?
                    });
                }
                Ok(resp) => {
                    let status = resp.status().as_u16();
                    let body = resp.text().await.unwrap_or_default();
                    // 403 rateLimitExceeded is BigQuery's per-table metadata quota.
                    let retry = status >= 500 || status == 429 || (status == 403 && body.contains("rateLimitExceeded"));
                    if !retry || attempt >= 8 {
                        return Err(RestError::Status {
                            method,
                            url,
                            status,
                            body: body.chars().take(500).collect(),
                        });
                    }
                    format!("HTTP {status}")
                }
                Err(e) if attempt >= 8 => return Err(RestError::Transport(e.to_string())),
                Err(e) => e.to_string(),
            };
            tracing::warn!(%url, attempt, reason = %retryable, "BigQuery REST retry");
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(60));
        }
        unreachable!()
    }

    fn table_url(&self, t: &TableRef) -> String {
        format!(
            "{}/bigquery/v2/projects/{}/datasets/{}/tables/{}",
            self.base, t.project, t.dataset, t.table
        )
    }

    pub async fn get_table(&self, t: &TableRef) -> Result<Value, RestError> {
        self.send("GET", self.table_url(t), |r| r).await
    }

    pub async fn insert_table(&self, t: &TableRef, body: &Value) -> Result<Value, RestError> {
        let url = format!("{}/bigquery/v2/projects/{}/datasets/{}/tables", self.base, t.project, t.dataset);
        self.send("POST", url, |r| r.json(body)).await
    }

    pub async fn patch_schema(&self, t: &TableRef, fields: &[Value]) -> Result<Value, RestError> {
        let body = json!({"schema": {"fields": fields}});
        self.send("PATCH", self.table_url(t), |r| r.json(&body)).await
    }

    /// Appends newline-delimited JSON rows with the table's live schema, ignoring unknown values, and
    /// waits for the job, as bizon's large-row path does.
    pub async fn load_ndjson(&self, t: &TableRef, location: &str, schema_fields: &[Value], ndjson: Vec<u8>) -> Result<(), RestError> {
        let config = json!({"configuration": {"load": {
            "destinationTable": {"projectId": t.project, "datasetId": t.dataset, "tableId": t.table},
            "sourceFormat": "NEWLINE_DELIMITED_JSON",
            "schema": {"fields": schema_fields},
            "ignoreUnknownValues": true,
            "writeDisposition": "WRITE_APPEND",
        }}, "jobReference": {"projectId": t.project, "location": location}});
        let boundary = "bizon-stream-load-boundary";
        let mut body = format!("--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n{config}\r\n--{boundary}\r\nContent-Type: application/octet-stream\r\n\r\n").into_bytes();
        body.extend_from_slice(&ndjson);
        body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
        let url = format!("{}/upload/bigquery/v2/projects/{}/jobs?uploadType=multipart", self.base, t.project);
        let job = self
            .send("POST", url, |r| {
                r.header("Content-Type", format!("multipart/related; boundary={boundary}"))
                    .body(body.clone())
            })
            .await?;
        let id = job["jobReference"]["jobId"].as_str().unwrap_or_default().to_string();
        let url = format!("{}/bigquery/v2/projects/{}/jobs/{id}?location={location}", self.base, t.project);
        loop {
            let status = &self.send("GET", url.clone(), |r| r).await?["status"];
            if status["state"] == "DONE" {
                return match status.get("errorResult") {
                    Some(e) => Err(RestError::Job {
                        job: id,
                        error: e.to_string(),
                    }),
                    None => Ok(()),
                };
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
    }
}
