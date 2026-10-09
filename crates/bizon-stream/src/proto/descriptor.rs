//! Mirrors bizon's `proto_utils.get_proto_schema_and_class`: a proto2 `TableRow` message with
//! fields numbered 1..n in record_schema order. `ProtoSchema` bytes must match Python's exactly.

use std::collections::HashMap;

use bqstorage_proto::storage::ProtoSchema;
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    pub name: String,
    pub bq_type: String,
    pub required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WireType {
    Int64,
    Double,
    Bool,
    String,
    Bytes,
}

impl WireType {
    /// Unknown BigQuery types (JSON, INT64, FLOAT64, GEOGRAPHY, ...) fall back to string, as in Python.
    pub fn for_bq_type(bq_type: &str) -> Result<Self, DescriptorError> {
        Ok(match bq_type {
            "BYTES" => WireType::Bytes,
            "INTEGER" => WireType::Int64,
            "FLOAT" => WireType::Double,
            "BOOLEAN" => WireType::Bool,
            "RECORD" => return Err(DescriptorError::UnsupportedType(bq_type.to_string())),
            _ => WireType::String,
        })
    }

    fn proto_type(self) -> Type {
        match self {
            WireType::Int64 => Type::Int64,
            WireType::Double => Type::Double,
            WireType::Bool => Type::Bool,
            WireType::String => Type::String,
            WireType::Bytes => Type::Bytes,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum DescriptorError {
    #[error("BigQuery type {0} is not supported by the streaming writer")]
    UnsupportedType(String),
}

#[derive(Debug, Clone)]
pub struct FieldPlan {
    pub name: String,
    pub number: u32,
    pub wire: WireType,
    pub required: bool,
    pub bq_type: String,
}

#[derive(Debug, Clone)]
pub struct TableDescriptor {
    pub fields: Vec<FieldPlan>,
    pub proto_schema: ProtoSchema,
    index: HashMap<String, usize>,
    /// protobuf's default JSON names (`account_id` -> `accountId`), which `ParseDict` also accepts.
    json_index: HashMap<String, usize>,
}

impl TableDescriptor {
    pub fn new(columns: &[Column]) -> Result<Self, DescriptorError> {
        let mut fields = Vec::with_capacity(columns.len());
        let mut message = DescriptorProto {
            name: Some("TableRow".to_string()),
            ..Default::default()
        };
        for (i, col) in columns.iter().enumerate() {
            let wire = WireType::for_bq_type(&col.bq_type)?;
            let number = i as u32 + 1;
            message.field.push(FieldDescriptorProto {
                name: Some(col.name.clone()),
                number: Some(number as i32),
                label: Some(if col.required { Label::Required } else { Label::Optional } as i32),
                r#type: Some(wire.proto_type() as i32),
                ..Default::default()
            });
            fields.push(FieldPlan {
                name: col.name.clone(),
                number,
                wire,
                required: col.required,
                bq_type: col.bq_type.clone(),
            });
        }
        let index = fields.iter().enumerate().map(|(i, f)| (f.name.clone(), i)).collect();
        let json_index = fields.iter().enumerate().map(|(i, f)| (json_name(&f.name), i)).collect();
        Ok(Self {
            index,
            json_index,
            fields,
            proto_schema: ProtoSchema {
                proto_descriptor: Some(message),
            },
        })
    }

    pub fn index_of(&self, name: &str) -> Option<usize> {
        self.index.get(name).copied()
    }

    /// Field lookup as `ParseDict` does it: JSON name first, then proto name.
    pub fn index_of_parse_dict(&self, name: &str) -> Option<usize> {
        self.json_index.get(name).or_else(|| self.index.get(name)).copied()
    }

    pub fn proto_schema_bytes(&self) -> Vec<u8> {
        self.proto_schema.encode_to_vec()
    }
}

/// protobuf's `ToJsonName`: underscores are dropped and the character after each is upper-cased.
fn json_name(name: &str) -> String {
    let mut out = String::with_capacity(name.len());
    let mut upper = false;
    for c in name.chars() {
        if c == '_' {
            upper = true;
        } else if upper {
            out.extend(c.to_uppercase());
            upper = false;
        } else {
            out.push(c);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn col(name: &str, t: &str, required: bool) -> Column {
        Column {
            name: name.into(),
            bq_type: t.into(),
            required,
        }
    }

    #[test]
    fn proto_schema_bytes_match_python() {
        // bizon proto_utils.get_proto_schema_and_class([id INTEGER REQUIRED, s STRING, j JSON, b BOOLEAN, f FLOAT])
        let golden = "0a420a085461626c65526f77120a0a02696418012002280312090a017318022001280912090a016a18032001280912090a016218042001280812090a0166180520012801";
        let d = TableDescriptor::new(&[
            col("id", "INTEGER", true),
            col("s", "STRING", false),
            col("j", "JSON", false),
            col("b", "BOOLEAN", false),
            col("f", "FLOAT", false),
        ])
        .unwrap();
        let hex: String = d.proto_schema_bytes().iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, golden);
    }

    #[test]
    fn json_names_follow_protobuf() {
        assert_eq!(json_name("account_id"), "accountId");
        assert_eq!(json_name("__ce_type"), "CeType");
        assert_eq!(json_name("payload"), "payload");
        assert_eq!(json_name("a__b_"), "aB");
    }

    #[test]
    fn unknown_types_are_strings_and_record_is_rejected() {
        let d = TableDescriptor::new(&[col("j", "JSON", false), col("i", "INT64", false)]).unwrap();
        assert!(d.fields.iter().all(|f| f.wire == WireType::String));
        assert!(TableDescriptor::new(&[col("r", "RECORD", false)]).is_err());
    }
}
