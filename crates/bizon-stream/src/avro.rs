//! Avro binary decoding into JSON values, rendered the way fastavro's output looks after bizon dumps
//! it with orjson: record fields in schema order, logical types as orjson writes datetimes and UUIDs,
//! non-finite floats as null, and `bytes`/`fixed` as strict UTF-8 text.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::timefmt;

type NodeId = usize;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Logical {
    None,
    TimestampMillis,
    TimestampMicros,
    LocalTimestampMillis,
    LocalTimestampMicros,
    Date,
    TimeMillis,
    TimeMicros,
    Uuid,
    /// `precision` is required by fastavro at decode time, so a missing one fails the record there.
    Decimal {
        precision: Option<u32>,
        scale: u32,
    },
}

#[derive(Debug)]
enum Node {
    Null,
    Boolean,
    Int(Logical),
    Long(Logical),
    Float,
    Double,
    Bytes(Logical),
    String(Logical),
    Record(Vec<(String, NodeId)>),
    Enum(Vec<String>),
    Array(NodeId),
    Map(NodeId),
    Union(Vec<NodeId>),
    Fixed(usize, Logical),
}

#[derive(Debug)]
pub struct Schema {
    nodes: Vec<Node>,
    root: NodeId,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AvroError {
    #[error("invalid schema: {0}")]
    Schema(String),
    #[error("unexpected end of data")]
    Eof,
    #[error("invalid varint")]
    Varint,
    #[error("invalid UTF-8 in {0}")]
    Utf8(&'static str),
    #[error("union index {0} out of range")]
    UnionIndex(i64),
    #[error("enum index {0} out of range")]
    EnumIndex(i64),
    #[error("negative length {0}")]
    Length(i64),
    #[error("{0} is not supported")]
    Unsupported(&'static str),
}

struct Builder {
    nodes: Vec<Node>,
    named: HashMap<String, NodeId>,
}

fn full_name(name: &str, namespace: Option<&str>) -> String {
    match namespace {
        Some(ns) if !name.contains('.') && !ns.is_empty() => format!("{ns}.{name}"),
        _ => name.to_string(),
    }
}

fn logical(v: &Value, base: &str) -> Logical {
    match (base, v.get("logicalType").and_then(Value::as_str)) {
        ("long", Some("timestamp-millis")) => Logical::TimestampMillis,
        ("long", Some("timestamp-micros")) => Logical::TimestampMicros,
        ("long", Some("local-timestamp-millis")) => Logical::LocalTimestampMillis,
        ("long", Some("local-timestamp-micros")) => Logical::LocalTimestampMicros,
        ("int", Some("date")) => Logical::Date,
        ("int", Some("time-millis")) => Logical::TimeMillis,
        ("long", Some("time-micros")) => Logical::TimeMicros,
        ("string", Some("uuid")) => Logical::Uuid,
        ("bytes" | "fixed", Some("decimal")) => Logical::Decimal {
            precision: v.get("precision").and_then(Value::as_u64).map(|p| p as u32),
            scale: v.get("scale").and_then(Value::as_u64).unwrap_or(0) as u32,
        },
        _ => Logical::None,
    }
}

impl Builder {
    fn push(&mut self, n: Node) -> NodeId {
        self.nodes.push(n);
        self.nodes.len() - 1
    }

    fn primitive(&mut self, name: &str, lt: Logical) -> Option<NodeId> {
        let n = match name {
            "null" => Node::Null,
            "boolean" => Node::Boolean,
            "int" => Node::Int(lt),
            "long" => Node::Long(lt),
            "float" => Node::Float,
            "double" => Node::Double,
            "bytes" => Node::Bytes(lt),
            "string" => Node::String(lt),
            _ => return None,
        };
        Some(self.push(n))
    }

    fn reference(&self, name: &str, ns: Option<&str>) -> Result<NodeId, AvroError> {
        self.named
            .get(&full_name(name, ns))
            .or_else(|| self.named.get(name))
            .copied()
            .ok_or_else(|| AvroError::Schema(format!("unknown type {name}")))
    }

    fn build(&mut self, v: &Value, ns: Option<&str>) -> Result<NodeId, AvroError> {
        match v {
            Value::String(name) => match self.primitive(name, Logical::None) {
                Some(id) => Ok(id),
                None => self.reference(name, ns),
            },
            Value::Array(branches) => {
                let ids = branches.iter().map(|b| self.build(b, ns)).collect::<Result<_, _>>()?;
                Ok(self.push(Node::Union(ids)))
            }
            Value::Object(o) => {
                let ty = o.get("type").ok_or_else(|| AvroError::Schema("missing type".into()))?;
                let Value::String(ty) = ty else { return self.build(ty, ns) };
                if let Some(id) = self.primitive(ty, logical(v, ty)) {
                    return Ok(id);
                }
                let name = o.get("name").and_then(Value::as_str);
                let own_ns = o.get("namespace").and_then(Value::as_str).or(ns);
                let named = |b: &mut Self, id: NodeId| {
                    if let Some(n) = name {
                        let full = full_name(n, own_ns);
                        // Names with dots carry their own namespace for nested definitions.
                        b.named.insert(full, id);
                    }
                };
                match ty.as_str() {
                    "record" | "error" => {
                        let id = self.push(Node::Record(Vec::new()));
                        named(self, id);
                        let inner_ns = name.filter(|n| n.contains('.')).map(|n| &n[..n.rfind('.').unwrap()]).or(own_ns);
                        let mut fields = Vec::new();
                        for f in o
                            .get("fields")
                            .and_then(Value::as_array)
                            .ok_or_else(|| AvroError::Schema("record without fields".into()))?
                        {
                            let fname = f
                                .get("name")
                                .and_then(Value::as_str)
                                .ok_or_else(|| AvroError::Schema("field without name".into()))?;
                            let fty = f
                                .get("type")
                                .ok_or_else(|| AvroError::Schema(format!("field {fname} without type")))?;
                            fields.push((fname.to_string(), self.build(fty, inner_ns)?));
                        }
                        self.nodes[id] = Node::Record(fields);
                        Ok(id)
                    }
                    "enum" => {
                        let symbols = o
                            .get("symbols")
                            .and_then(Value::as_array)
                            .ok_or_else(|| AvroError::Schema("enum without symbols".into()))?
                            .iter()
                            .map(|s| s.as_str().unwrap_or_default().to_string())
                            .collect();
                        let id = self.push(Node::Enum(symbols));
                        named(self, id);
                        Ok(id)
                    }
                    "fixed" => {
                        let size = o
                            .get("size")
                            .and_then(Value::as_u64)
                            .ok_or_else(|| AvroError::Schema("fixed without size".into()))?;
                        let id = self.push(Node::Fixed(size as usize, logical(v, "fixed")));
                        named(self, id);
                        Ok(id)
                    }
                    "array" => {
                        let items = self.build(o.get("items").ok_or_else(|| AvroError::Schema("array without items".into()))?, ns)?;
                        Ok(self.push(Node::Array(items)))
                    }
                    "map" => {
                        let values = self.build(o.get("values").ok_or_else(|| AvroError::Schema("map without values".into()))?, ns)?;
                        Ok(self.push(Node::Map(values)))
                    }
                    other => self.reference(other, ns),
                }
            }
            _ => Err(AvroError::Schema(format!("unexpected schema value {v}"))),
        }
    }
}

impl Schema {
    pub fn parse(v: &Value) -> Result<Self, AvroError> {
        let mut b = Builder {
            nodes: Vec::new(),
            named: HashMap::new(),
        };
        let root = b.build(v, None)?;
        Ok(Self { nodes: b.nodes, root })
    }

    pub fn decode(&self, mut data: &[u8]) -> Result<Value, AvroError> {
        self.read(self.root, &mut data)
    }

    fn read(&self, id: NodeId, d: &mut &[u8]) -> Result<Value, AvroError> {
        Ok(match &self.nodes[id] {
            Node::Null => Value::Null,
            Node::Boolean => Value::Bool(take(d, 1)?[0] != 0),
            Node::Int(lt) | Node::Long(lt) => {
                let n = varint(d)?;
                match lt {
                    Logical::TimestampMillis => Value::from(timefmt::parts(n.saturating_mul(1000)).isoformat_utc()),
                    Logical::TimestampMicros => Value::from(timefmt::parts(n).isoformat_utc()),
                    Logical::LocalTimestampMillis => Value::from(timefmt::parts(n.saturating_mul(1000)).isoformat()),
                    Logical::LocalTimestampMicros => Value::from(timefmt::parts(n).isoformat()),
                    Logical::Date => Value::from(timefmt::parts(n.saturating_mul(86_400_000_000)).date()),
                    Logical::TimeMillis => Value::from(time_of_day(n.saturating_mul(1000))),
                    Logical::TimeMicros => Value::from(time_of_day(n)),
                    _ => Value::from(n),
                }
            }
            Node::Float => Value::from(f32::from_le_bytes(take(d, 4)?.try_into().unwrap()) as f64),
            Node::Double => Value::from(f64::from_le_bytes(take(d, 8)?.try_into().unwrap())),
            Node::Bytes(lt) => {
                let n = length(d)?;
                bytes_value(take(d, n)?, *lt)?
            }
            Node::Fixed(size, lt) => bytes_value(take(d, *size)?, *lt)?,
            Node::String(lt) => {
                let n = length(d)?;
                let s = std::str::from_utf8(take(d, n)?).map_err(|_| AvroError::Utf8("string"))?;
                match lt {
                    Logical::Uuid => Value::from(canonical_uuid(s)),
                    _ => Value::from(s),
                }
            }
            Node::Record(fields) => {
                let mut m = Map::with_capacity(fields.len());
                for (name, f) in fields {
                    m.insert(name.clone(), self.read(*f, d)?);
                }
                Value::Object(m)
            }
            Node::Enum(symbols) => {
                let i = varint(d)?;
                Value::from(symbols.get(i as usize).ok_or(AvroError::EnumIndex(i))?.as_str())
            }
            Node::Union(branches) => {
                let i = varint(d)?;
                let b = *branches
                    .get(usize::try_from(i).map_err(|_| AvroError::UnionIndex(i))?)
                    .ok_or(AvroError::UnionIndex(i))?;
                self.read(b, d)?
            }
            Node::Array(items) => {
                let mut out = Vec::new();
                blocks(d, |d| {
                    out.push(self.read(*items, d)?);
                    Ok(())
                })?;
                Value::Array(out)
            }
            Node::Map(values) => {
                let mut out = Map::new();
                blocks(d, |d| {
                    let n = length(d)?;
                    let k = std::str::from_utf8(take(d, n)?)
                        .map_err(|_| AvroError::Utf8("map key"))?
                        .to_string();
                    out.insert(k, self.read(*values, d)?);
                    Ok(())
                })?;
                Value::Object(out)
            }
        })
    }
}

fn take<'a>(d: &mut &'a [u8], n: usize) -> Result<&'a [u8], AvroError> {
    if d.len() < n {
        return Err(AvroError::Eof);
    }
    let (head, tail) = d.split_at(n);
    *d = tail;
    Ok(head)
}

fn varint(d: &mut &[u8]) -> Result<i64, AvroError> {
    let mut raw = 0u64;
    for shift in (0..64).step_by(7) {
        let b = take(d, 1)?[0];
        raw |= ((b & 0x7f) as u64) << shift;
        if b & 0x80 == 0 {
            return Ok((raw >> 1) as i64 ^ -((raw & 1) as i64));
        }
    }
    Err(AvroError::Varint)
}

fn length(d: &mut &[u8]) -> Result<usize, AvroError> {
    let n = varint(d)?;
    usize::try_from(n).map_err(|_| AvroError::Length(n))
}

/// Array and map blocks: a count, negative when followed by the block's byte size; 0 ends.
fn blocks(d: &mut &[u8], mut item: impl FnMut(&mut &[u8]) -> Result<(), AvroError>) -> Result<(), AvroError> {
    loop {
        let mut count = varint(d)?;
        if count == 0 {
            return Ok(());
        }
        if count < 0 {
            count = -count;
            varint(d)?;
        }
        for _ in 0..count {
            item(d)?;
        }
    }
}

fn bytes_value(b: &[u8], lt: Logical) -> Result<Value, AvroError> {
    if let Logical::Decimal { precision, scale } = lt {
        return decimal(b, precision.ok_or(AvroError::Schema("decimal without precision".into()))?, scale);
    }
    // bizon's orjson default decodes bytes as strict UTF-8; anything else fails the record.
    Ok(Value::from(std::str::from_utf8(b).map_err(|_| AvroError::Utf8("bytes"))?))
}

/// What bizon ends up with for a decimal: fastavro rounds the unscaled value to `precision` digits
/// (half-even) and applies `scaleb(-scale)`; the frame stores `str(Decimal)`, which the transform
/// reads back with `json.loads`. That gives an int only when the final exponent is 0, otherwise the
/// correctly rounded float.
fn decimal(b: &[u8], precision: u32, scale: u32) -> Result<Value, AvroError> {
    if b.len() > 16 {
        return Err(AvroError::Unsupported("decimal wider than 128 bits"));
    }
    let mut unscaled: i128 = if b.first().is_some_and(|x| x & 0x80 != 0) { -1 } else { 0 };
    for &x in b {
        unscaled = (unscaled << 8) | x as i128;
    }
    let negative = unscaled < 0;
    let mut coefficient = unscaled.unsigned_abs();
    let mut exponent = -(scale as i64);
    let digits = if coefficient == 0 { 1 } else { coefficient.ilog10() + 1 };
    if precision == 0 {
        return Err(AvroError::Schema("decimal precision must be positive".into()));
    }
    if digits > precision {
        let drop = digits - precision;
        let div = 10u128.pow(drop);
        let (q, r) = (coefficient / div, coefficient % div);
        let half = div / 2;
        coefficient = if r > half || (r == half && q % 2 == 1) { q + 1 } else { q };
        exponent += drop as i64;
        if coefficient == 10u128.pow(precision) {
            coefficient /= 10;
            exponent += 1;
        }
    }
    let sign = if negative { "-" } else { "" };
    if exponent == 0 {
        let n = i128::try_from(coefficient).ok().map(|c| if negative { -c } else { c });
        let int = n.and_then(|n| {
            i64::try_from(n)
                .map(Value::from)
                .or_else(|_| u64::try_from(n).map(Value::from))
                .ok()
        });
        return int.ok_or(AvroError::Unsupported("integer decimal beyond 64 bits"));
    }
    let f: f64 = format!("{sign}{coefficient}e{exponent}")
        .parse()
        .map_err(|_| AvroError::Unsupported("decimal value"))?;
    Ok(Value::from(f))
}

fn time_of_day(micros: i64) -> String {
    let p = timefmt::parts(micros);
    let base = format!("{:02}:{:02}:{:02}", p.hour, p.minute, p.second);
    match p.micros {
        0 => base,
        us => format!("{base}.{us:06}"),
    }
}

/// fastavro returns `uuid.UUID`, which orjson writes in lowercase hyphenated form.
fn canonical_uuid(s: &str) -> String {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() == 32 && hex.chars().all(|c| c.is_ascii_hexdigit()) {
        let h = hex.to_lowercase();
        format!("{}-{}-{}-{}-{}", &h[..8], &h[8..12], &h[12..16], &h[16..20], &h[20..])
    } else {
        s.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn zz(n: i64) -> Vec<u8> {
        let mut v = ((n << 1) ^ (n >> 63)) as u64;
        let mut out = Vec::new();
        while v >= 0x80 {
            out.push((v as u8) | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
        out
    }

    fn s(x: &str) -> Vec<u8> {
        [zz(x.len() as i64), x.as_bytes().to_vec()].concat()
    }

    #[test]
    fn debezium_like_envelope_with_named_reference() {
        let schema = Schema::parse(&json!({
            "type": "record", "name": "Envelope", "namespace": "srv.public.t",
            "fields": [
                {"name": "before", "type": ["null", {"type": "record", "name": "Value", "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "tags", "type": {"type": "array", "items": "string"}},
                    {"name": "attrs", "type": ["null", {"type": "map", "values": "int"}]},
                    {"name": "status", "type": {"type": "enum", "name": "S", "symbols": ["A", "B"]}},
                    {"name": "score", "type": "float"}
                ]}]},
                {"name": "after", "type": ["null", "Value"]},
                {"name": "op", "type": "string"}
            ]
        }))
        .unwrap();
        let after = [
            zz(-42),
            zz(-2),
            zz(4),
            s("x"),
            s("y"),
            zz(0),
            zz(1),
            zz(1),
            s("k"),
            zz(7),
            zz(0),
            zz(1),
            1.5f32.to_le_bytes().to_vec(),
        ]
        .concat();
        let data = [zz(0), zz(1), after, s("c")].concat();
        let v = schema.decode(&data).unwrap();
        assert_eq!(
            serde_json::to_string(&v).unwrap(),
            r#"{"before":null,"after":{"id":-42,"tags":["x","y"],"attrs":{"k":7},"status":"B","score":1.5},"op":"c"}"#
        );
    }

    #[test]
    fn logical_types_render_like_orjson() {
        let ts = Schema::parse(&json!({"type": "long", "logicalType": "timestamp-millis"})).unwrap();
        assert_eq!(ts.decode(&zz(1_700_000_000_123)).unwrap(), "2023-11-14T22:13:20.123000+00:00");
        let date = Schema::parse(&json!({"type": "int", "logicalType": "date"})).unwrap();
        assert_eq!(date.decode(&zz(19_000)).unwrap(), "2022-01-08");
        let f = Schema::parse(&json!("float")).unwrap();
        assert_eq!(
            serde_json::to_string(&f.decode(&1.1f32.to_le_bytes()).unwrap()).unwrap(),
            "1.100000023841858"
        );
        assert_eq!(f.decode(&f32::NAN.to_le_bytes()).unwrap(), Value::Null);
    }

    #[test]
    fn truncated_and_invalid_data_fail() {
        let schema = Schema::parse(&json!("string")).unwrap();
        assert_eq!(schema.decode(&zz(5)), Err(AvroError::Eof));
        assert_eq!(schema.decode(&[zz(1), vec![0xff]].concat()), Err(AvroError::Utf8("string")));
    }
}
