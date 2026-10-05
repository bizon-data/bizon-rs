//! One message from Kafka bytes to a proto row: parse, decode, transform, encode. Shared by the
//! worker and the parity test so both exercise the same path.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde_json::Value;

use crate::avro::AvroError;
use crate::kafka::message::{MessageParser, ParseError, Parsed, Payload, RawMessage};
use crate::kafka::registry::RegistrySchema;
use crate::proto::descriptor::TableDescriptor;
use crate::proto::encode::{encode_row, EncodeError, Value as Cell};
use crate::transform::{Builtin, TransformError};

/// Rows over this many serialized bytes go through a load job, as in bizon.
pub const MAX_STREAM_ROW_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug)]
pub enum Outcome {
    Skipped,
    Row {
        destination_id: Arc<str>,
        bytes: Vec<u8>,
    },
    /// One NDJSON line (no trailing newline) for the load-job path.
    Large {
        destination_id: Arc<str>,
        ndjson: Vec<u8>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum PipelineError {
    #[error(transparent)]
    Source(#[from] ParseError),
    #[error("avro decode: {0}")]
    Avro(#[from] AvroError),
    #[error("transform: {0}")]
    Transform(#[from] TransformError),
    #[error("encode: {0}")]
    Encode(#[from] EncodeError),
    #[error("no table descriptor for {0}")]
    NoTable(String),
    #[error("large row: JSON column {0} does not hold valid JSON")]
    LargeRowJson(String),
}

impl PipelineError {
    /// Which bizon stage would have raised: `source`, `transform` or `encode`.
    pub fn stage(&self) -> &'static str {
        match self {
            PipelineError::Source(_) | PipelineError::Avro(_) => "source",
            PipelineError::Transform(_) => "transform",
            PipelineError::Encode(_) | PipelineError::NoTable(_) | PipelineError::LargeRowJson(_) => "encode",
        }
    }
}

pub fn cell(v: &Value) -> Cell<'_> {
    match v {
        Value::Null => Cell::Null,
        Value::Bool(b) => Cell::Bool(*b),
        Value::Number(n) => match (n.as_i64(), n.as_u64()) {
            (Some(i), _) => Cell::Int(i),
            (None, Some(u)) => Cell::UInt(u),
            _ => Cell::Float(n.as_f64().unwrap_or_default()),
        },
        Value::String(s) => Cell::Str(s.as_str().into()),
        other => Cell::Str(crate::json::to_string(other).into()),
    }
}

pub struct Pipeline<'a> {
    parser: &'a MessageParser,
    transform: &'a Builtin,
    tables: &'a dyn Fn(&str) -> Option<&'a TableDescriptor>,
    /// `__schema` text per (registry id, topic); -1 stands for JSON messages, which have no schema.
    schema_columns: RwLock<HashMap<(i64, String), Arc<str>>>,
}

impl<'a> Pipeline<'a> {
    pub fn new(parser: &'a MessageParser, transform: &'a Builtin, tables: &'a dyn Fn(&str) -> Option<&'a TableDescriptor>) -> Self {
        Self {
            parser,
            transform,
            tables,
            schema_columns: RwLock::new(HashMap::new()),
        }
    }

    fn schema_column(&self, id: i64, topic: &str, schema: &Value) -> Result<Arc<str>, TransformError> {
        let key = (id, topic.to_string());
        if let Some(s) = self.schema_columns.read().unwrap().get(&key) {
            return Ok(s.clone());
        }
        let s: Arc<str> = self.transform.schema_column(topic, schema)?.into();
        self.schema_columns.write().unwrap().insert(key, s.clone());
        Ok(s)
    }

    pub fn process(
        &self,
        msg: &RawMessage<'_>,
        schemas: impl Fn(i64) -> Option<Arc<RegistrySchema>>,
        inserted_at: &str,
    ) -> Result<Outcome, PipelineError> {
        let rec = match self.parser.parse(msg, schemas)? {
            Parsed::Skipped(_) => return Ok(Outcome::Skipped),
            Parsed::Record(r) => r,
        };
        let (value, schema_column) = match &rec.value {
            Payload::Json(v) => (v.clone(), self.schema_column(-1, rec.topic, &Value::Object(Default::default()))?),
            Payload::Avro { schema, body } => (
                schema.avro()?.decode(body)?,
                self.schema_column(schema.global_id, rec.topic, &schema.schema)?,
            ),
        };
        let row = self.transform.apply(&rec, value, &schema_column, inserted_at)?;
        let table = (self.tables)(&rec.destination_id).ok_or_else(|| PipelineError::NoTable(rec.destination_id.to_string()))?;
        let mut bytes = Vec::with_capacity(256);
        encode_row(table, row.iter().map(|(k, v)| (k.as_str(), cell(v))), &mut bytes)?;
        if bytes.len() > MAX_STREAM_ROW_BYTES {
            return Ok(Outcome::Large {
                destination_id: rec.destination_id.clone(),
                ndjson: large_row_json(table, row)?,
            });
        }
        Ok(Outcome::Row {
            destination_id: rec.destination_id.clone(),
            bytes,
        })
    }
}

/// The load-job form of a row: later duplicates win, nulls are dropped, and JSON columns carry the
/// parsed document (a string would be stored as a JSON string scalar), like bizon's large-row path.
fn large_row_json(table: &TableDescriptor, row: crate::transform::Row) -> Result<Vec<u8>, PipelineError> {
    let mut obj = serde_json::Map::new();
    for (k, v) in row {
        if v.is_null() {
            obj.shift_remove(&k);
            continue;
        }
        let is_json = table.index_of(&k).is_some_and(|i| table.fields[i].bq_type == "JSON");
        let v = match v {
            Value::String(s) if is_json => serde_json::from_str(&s).map_err(|_| PipelineError::LargeRowJson(k.clone()))?,
            other => other,
        };
        obj.insert(k, v);
    }
    Ok(crate::json::to_string(&Value::Object(obj)).into_bytes())
}
