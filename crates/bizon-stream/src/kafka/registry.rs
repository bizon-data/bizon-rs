//! Apicurio schema lookup by global id, cached for the life of the process (schemas are immutable).
//! The cached schema is the registry JSON with its top-level `name` forced to `Envelope`, as bizon
//! does before handing it to fastavro; key order is preserved because `__schema` is built from it.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::Value;

#[derive(Debug)]
pub struct RegistrySchema {
    pub global_id: i64,
    /// Exactly what the registry returned.
    pub raw: Vec<u8>,
    pub schema: Value,
    /// Compiled once; a schema that does not compile fails each message that uses it, as in bizon.
    avro: Result<crate::avro::Schema, crate::avro::AvroError>,
}

impl RegistrySchema {
    pub fn avro(&self) -> Result<&crate::avro::Schema, crate::avro::AvroError> {
        self.avro.as_ref().map_err(Clone::clone)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RegistryError {
    #[error("schema {id}: registry returned HTTP {status}")]
    Status { id: i64, status: u16 },
    #[error("schema {id}: {source}")]
    Http { id: i64, source: reqwest::Error },
    #[error("schema {id}: invalid JSON: {source}")]
    Json { id: i64, source: serde_json::Error },
}

pub struct Registry {
    http: reqwest::Client,
    base_url: String,
    auth: (String, String),
    cache: Mutex<HashMap<i64, Arc<RegistrySchema>>>,
}

impl Registry {
    pub fn new(base_url: &str, username: &str, password: &str) -> Self {
        Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()
                .expect("reqwest client"),
            base_url: base_url.trim_end_matches('/').to_string(),
            auth: (username.to_string(), password.to_string()),
            cache: Mutex::new(HashMap::new()),
        }
    }

    pub async fn get(&self, id: i64) -> Result<Arc<RegistrySchema>, RegistryError> {
        if let Some(s) = self.cache.lock().unwrap().get(&id) {
            return Ok(s.clone());
        }
        let raw = self.fetch(id).await?;
        let schema = Arc::new(Self::build(id, raw)?);
        self.cache.lock().unwrap().insert(id, schema.clone());
        Ok(schema)
    }

    pub fn build(id: i64, raw: Vec<u8>) -> Result<RegistrySchema, RegistryError> {
        let mut schema: Value = serde_json::from_slice(&raw).map_err(|source| RegistryError::Json { id, source })?;
        if let Value::Object(map) = &mut schema {
            map.insert("name".into(), Value::from("Envelope"));
        }
        let avro = crate::avro::Schema::parse(&schema);
        Ok(RegistrySchema {
            global_id: id,
            raw,
            schema,
            avro,
        })
    }

    /// Retries transport errors and 5xx/429 with backoff, like bizon's requests Session; other
    /// statuses fail immediately (bizon's `raise_for_status` hook).
    async fn fetch(&self, id: i64) -> Result<Vec<u8>, RegistryError> {
        let url = format!("{}/apis/registry/v2/ids/globalIds/{id}", self.base_url);
        let mut delay = Duration::from_millis(500);
        let mut attempt = 0;
        loop {
            attempt += 1;
            let result = self.http.get(&url).basic_auth(&self.auth.0, Some(&self.auth.1)).send().await;
            let retryable = match result {
                Ok(resp) if resp.status().is_success() => {
                    return resp
                        .bytes()
                        .await
                        .map(|b| b.to_vec())
                        .map_err(|source| RegistryError::Http { id, source });
                }
                Ok(resp) => {
                    let status = resp.status();
                    if !(status.is_server_error() || status.as_u16() == 429) || attempt >= 10 {
                        return Err(RegistryError::Status {
                            id,
                            status: status.as_u16(),
                        });
                    }
                    format!("HTTP {status}")
                }
                Err(source) if attempt >= 10 => return Err(RegistryError::Http { id, source }),
                Err(e) => e.to_string(),
            };
            tracing::warn!(id, attempt, %retryable, "schema registry retry");
            tokio::time::sleep(delay).await;
            delay = (delay * 2).min(Duration::from_secs(30));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn name_is_forced_to_envelope_in_place() {
        let s = Registry::build(1, br#"{"type":"record","name":"X","fields":[]}"#.to_vec()).unwrap();
        assert_eq!(
            serde_json::to_string(&s.schema).unwrap(),
            r#"{"type":"record","name":"Envelope","fields":[]}"#
        );
        let s = Registry::build(1, br#"{"type":"record","fields":[]}"#.to_vec()).unwrap();
        assert_eq!(
            serde_json::to_string(&s.schema).unwrap(),
            r#"{"type":"record","fields":[],"name":"Envelope"}"#
        );
    }
}
