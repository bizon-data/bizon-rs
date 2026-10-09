//! Row → proto2 bytes, emulating `to_protobuf_serialization`: upb's `TableRow(**row)` fast path, and
//! when any field is rejected there, `ParseDict` rules applied to the whole row.

use std::borrow::Cow;

use super::descriptor::{TableDescriptor, WireType};

#[derive(Debug, Clone, PartialEq)]
pub enum Value<'a> {
    Null,
    Bool(bool),
    Int(i64),
    UInt(u64),
    Float(f64),
    Str(Cow<'a, str>),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum EncodeError {
    /// Python: `ParseError` "Message type "dynamic_package.TableRow" has no field named ..."
    #[error("no field named {0}")]
    UnknownField(String),
    /// Python: `ParseError` from `ParseDict`.
    #[error("field {field}: {reason}")]
    Parse { field: String, reason: String },
    /// Python: `EncodeError` on `SerializeToString` (missing required field).
    #[error("missing required field {0}")]
    MissingRequired(String),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum Scalar<'v> {
    Varint(u64),
    Fixed64(u64),
    Len(&'v [u8]),
}

enum Coerced<'v> {
    Ok(Scalar<'v>),
    Owned(Vec<u8>),
    Reject,
}

fn fast_path<'v>(wire: WireType, v: &'v Value<'_>) -> Coerced<'v> {
    use Coerced::*;
    match (wire, v) {
        (WireType::Int64, Value::Int(i)) => Ok(Scalar::Varint(*i as u64)),
        (WireType::Int64, Value::Bool(b)) => Ok(Scalar::Varint(*b as u64)),
        (WireType::Int64, Value::UInt(u)) if *u <= i64::MAX as u64 => Ok(Scalar::Varint(*u)),
        (WireType::Double, Value::Float(f)) => Ok(Scalar::Fixed64(f.to_bits())),
        (WireType::Double, Value::Int(i)) => Ok(Scalar::Fixed64((*i as f64).to_bits())),
        (WireType::Double, Value::UInt(u)) => Ok(Scalar::Fixed64((*u as f64).to_bits())),
        (WireType::Double, Value::Bool(b)) => Ok(Scalar::Fixed64((*b as u8 as f64).to_bits())),
        (WireType::Bool, Value::Bool(b)) => Ok(Scalar::Varint(*b as u64)),
        (WireType::Bool, Value::Int(i)) => Ok(Scalar::Varint((*i != 0) as u64)),
        (WireType::Bool, Value::UInt(u)) => Ok(Scalar::Varint((*u != 0) as u64)),
        (WireType::String, Value::Str(s)) => Ok(Scalar::Len(s.as_bytes())),
        _ => Reject,
    }
}

fn parse_dict<'v>(field: &str, wire: WireType, v: &'v Value<'_>) -> Result<Coerced<'v>, EncodeError> {
    let err = |reason: &str| EncodeError::Parse {
        field: field.to_string(),
        reason: reason.to_string(),
    };
    Ok(match (wire, v) {
        (WireType::Int64, Value::Int(i)) => Coerced::Ok(Scalar::Varint(*i as u64)),
        (WireType::Int64, Value::UInt(u)) if *u <= i64::MAX as u64 => Coerced::Ok(Scalar::Varint(*u)),
        (WireType::Int64, Value::UInt(_)) => return Err(err("Value out of range")),
        (WireType::Int64, Value::Float(f)) if f.fract() == 0.0 && f.is_finite() => {
            Coerced::Ok(Scalar::Varint(float_to_i64(*f).ok_or_else(|| err("Value out of range"))? as u64))
        }
        (WireType::Int64, Value::Str(s)) => {
            Coerced::Ok(Scalar::Varint(parse_int_str(s).ok_or_else(|| err("Couldn't parse integer"))? as u64))
        }
        (WireType::Double, Value::Float(f)) => Coerced::Ok(Scalar::Fixed64(f.to_bits())),
        (WireType::Double, Value::Int(i)) => Coerced::Ok(Scalar::Fixed64((*i as f64).to_bits())),
        (WireType::Double, Value::UInt(u)) => Coerced::Ok(Scalar::Fixed64((*u as f64).to_bits())),
        (WireType::Double, Value::Str(s)) => Coerced::Ok(Scalar::Fixed64(
            parse_float_str(s).ok_or_else(|| err("Couldn't parse float"))?.to_bits(),
        )),
        (WireType::Bool, Value::Bool(b)) => Coerced::Ok(Scalar::Varint(*b as u64)),
        (WireType::String, Value::Str(s)) => Coerced::Ok(Scalar::Len(s.as_bytes())),
        (WireType::Bytes, Value::Str(s)) => Coerced::Owned(decode_base64(s).ok_or_else(|| err("Failed to parse bytes field"))?),
        _ => return Err(err("unexpected type")),
    })
}

fn float_to_i64(f: f64) -> Option<i64> {
    (f >= i64::MIN as f64 && f < i64::MAX as f64).then_some(f as i64)
}

fn parse_int_str(s: &str) -> Option<i64> {
    if s.contains(' ') {
        return None;
    }
    if let Ok(i) = s.parse::<i64>() {
        return Some(i);
    }
    let f = parse_float_str(s)?;
    if f.fract() == 0.0 && f.is_finite() {
        float_to_i64(f)
    } else {
        None
    }
}

fn parse_float_str(s: &str) -> Option<f64> {
    match s {
        "NaN" => Some(f64::NAN),
        "Infinity" => Some(f64::INFINITY),
        "-Infinity" => Some(f64::NEG_INFINITY),
        _ if s.chars().any(|c| c.is_ascii_alphabetic() && c != 'e' && c != 'E') => None,
        _ => s.parse::<f64>().ok(),
    }
}

fn decode_base64(s: &str) -> Option<Vec<u8>> {
    // urlsafe_b64decode with padding added, as protobuf's json_format does.
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut buf = 0u32;
    let mut bits = 0;
    for c in s.bytes() {
        let v = match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'-' | b'+' => 62,
            b'_' | b'/' => 63,
            b'=' => continue,
            _ => return None,
        };
        buf = (buf << 6) | v as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((buf >> bits) as u8);
        }
    }
    Some(out)
}

fn put_varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn put_field(out: &mut Vec<u8>, number: u32, s: Scalar<'_>) {
    match s {
        Scalar::Varint(v) => {
            put_varint(out, (number as u64) << 3);
            put_varint(out, v);
        }
        Scalar::Fixed64(v) => {
            put_varint(out, ((number as u64) << 3) | 1);
            out.extend_from_slice(&v.to_le_bytes());
        }
        Scalar::Len(b) => {
            put_varint(out, ((number as u64) << 3) | 2);
            put_varint(out, b.len() as u64);
            out.extend_from_slice(b);
        }
    }
}

/// Python's steps for a row with a key that isn't a proto field name: the dict keeps each key at its
/// first position with its last value, `None` values are dropped, and `ParseDict` assigns in dict
/// order, accepting JSON names (`accountId` for `account_id`); of two keys naming one field, the later
/// one wins.
fn parse_dict_slots<'a>(desc: &TableDescriptor, row: Vec<(&'a str, Value<'a>)>) -> Result<Vec<Option<Value<'a>>>, EncodeError> {
    let mut dict: Vec<(&'a str, Value<'a>)> = Vec::with_capacity(row.len());
    for (name, value) in row {
        match dict.iter_mut().find(|(n, _)| *n == name) {
            Some(e) => e.1 = value,
            None => dict.push((name, value)),
        }
    }
    let mut slots = vec![None; desc.fields.len()];
    for (name, value) in dict {
        if value == Value::Null {
            continue;
        }
        let idx = desc
            .index_of_parse_dict(name)
            .ok_or_else(|| EncodeError::UnknownField(name.to_string()))?;
        slots[idx] = Some(value);
    }
    Ok(slots)
}

/// Encodes one row. Later duplicates of a column win, like a Python dict literal. `Null` values are
/// dropped before encoding, as Python does.
pub fn encode_row<'a>(
    desc: &TableDescriptor,
    row: impl IntoIterator<Item = (&'a str, Value<'a>)>,
    out: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let row: Vec<(&'a str, Value<'a>)> = row.into_iter().collect();
    let proto_names = row.iter().all(|(name, _)| desc.index_of(name).is_some());
    let slots = if proto_names {
        let mut slots: Vec<Option<Value<'a>>> = vec![None; desc.fields.len()];
        for (name, value) in row {
            slots[desc.index_of(name).expect("checked above")] = (value != Value::Null).then_some(value);
        }
        slots
    } else {
        parse_dict_slots(desc, row)?
    };

    // Any other key fails `TableRow(**row)`, so the whole row goes through `ParseDict`.
    let fast = proto_names
        && desc
            .fields
            .iter()
            .zip(&slots)
            .all(|(f, v)| v.as_ref().is_none_or(|v| !matches!(fast_path(f.wire, v), Coerced::Reject)));

    for (f, v) in desc.fields.iter().zip(&slots) {
        let Some(v) = v else {
            if f.required {
                return Err(EncodeError::MissingRequired(f.name.clone()));
            }
            continue;
        };
        let coerced = if fast {
            fast_path(f.wire, v)
        } else {
            parse_dict(&f.name, f.wire, v)?
        };
        match coerced {
            Coerced::Ok(s) => put_field(out, f.number, s),
            Coerced::Owned(b) => put_field(out, f.number, Scalar::Len(&b)),
            Coerced::Reject => unreachable!("fast path rejected after check"),
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::descriptor::Column;

    fn desc() -> TableDescriptor {
        let c = |n: &str, t: &str, r: bool| Column {
            name: n.into(),
            bq_type: t.into(),
            required: r,
        };
        TableDescriptor::new(&[
            c("id", "INTEGER", false),
            c("s", "STRING", false),
            c("b", "BOOLEAN", false),
            c("f", "FLOAT", false),
        ])
        .unwrap()
    }

    fn enc(row: Vec<(&'static str, Value<'static>)>) -> Result<Vec<u8>, EncodeError> {
        let mut out = Vec::new();
        encode_row(&desc(), row, &mut out).map(|_| out)
    }

    #[test]
    fn explicit_zero_values_are_emitted_in_field_order() {
        // Python: {id:0, s:"", b:False, f:0.0} -> 0800 1200 1800 21 0000000000000000
        let out = enc(vec![
            ("f", Value::Float(0.0)),
            ("b", Value::Bool(false)),
            ("s", Value::Str("".into())),
            ("id", Value::Int(0)),
        ])
        .unwrap();
        assert_eq!(out, b"\x08\x00\x12\x00\x18\x00\x21\x00\x00\x00\x00\x00\x00\x00\x00");
    }

    #[test]
    fn nulls_are_dropped_and_bool_goes_into_int64_on_fast_path() {
        assert_eq!(enc(vec![("id", Value::Bool(true)), ("s", Value::Null)]).unwrap(), b"\x08\x01");
    }

    #[test]
    fn numeric_string_falls_back_to_parse_dict() {
        assert_eq!(enc(vec![("id", Value::Str("30".into()))]).unwrap(), b"\x08\x1e");
        assert_eq!(enc(vec![("id", Value::Str("1e3".into()))]).unwrap(), b"\x08\xe8\x07");
        assert!(enc(vec![("id", Value::Str("3.5".into()))]).is_err());
        assert!(enc(vec![("id", Value::Str("3 0".into()))]).is_err());
    }

    #[test]
    fn fallback_applies_parse_dict_to_the_whole_row() {
        // The bool is fine on the fast path but ParseDict rejects bool for int64 once "30" forces the fallback.
        let int = |n: &str| Column {
            name: n.into(),
            bq_type: "INTEGER".into(),
            required: false,
        };
        let d = TableDescriptor::new(&[int("a"), int("b")]).unwrap();
        let mut out = Vec::new();
        let r = encode_row(&d, vec![("a", Value::Bool(true)), ("b", Value::Str("30".into()))], &mut out);
        assert!(matches!(r, Err(EncodeError::Parse { .. })));
    }

    #[test]
    fn string_fields_reject_non_strings_and_unknown_fields_fail() {
        assert!(enc(vec![("s", Value::Int(1))]).is_err());
        assert_eq!(enc(vec![("nope", Value::Int(1))]), Err(EncodeError::UnknownField("nope".into())));
    }

    #[test]
    fn negative_int64_is_ten_byte_varint() {
        let out = enc(vec![("id", Value::Int(-1))]).unwrap();
        assert_eq!(out.len(), 11);
    }

    #[test]
    fn missing_required_is_an_encode_error() {
        let d = TableDescriptor::new(&[Column {
            name: "id".into(),
            bq_type: "INTEGER".into(),
            required: true,
        }])
        .unwrap();
        let mut out = Vec::new();
        assert_eq!(encode_row(&d, vec![], &mut out), Err(EncodeError::MissingRequired("id".into())));
    }
}
