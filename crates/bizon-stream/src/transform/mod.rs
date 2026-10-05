//! Built-in replacements for the inline Python transforms production uses. A config's transform is
//! accepted only if its code matches a known template once the per-pipeline literals (cluster name,
//! column deny-list) are swapped for placeholders; anything else fails at startup.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::kafka::message::Record;
use crate::timefmt;

const DEBEZIUM_TEMPLATE: &str = include_str!("templates/debezium.py");
const CLOUDEVENTS_TEMPLATE: &str = include_str!("templates/cloudevents.py");

const CLUSTER_PREFIX: &str = "\"__cluster\": \"";
const DENY_LIST_PREFIX: &str = "TOPIC_COLUMN_TO_FILTER = ";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Builtin {
    /// avro-cdc (`label: debezium`).
    Debezium {
        cluster: String,
        deny_list: BTreeMap<String, Vec<String>>,
    },
    /// json-cloudevents (`label: parse_events`).
    CloudEvents { cluster: String },
}

#[derive(Debug, thiserror::Error)]
pub enum TransformSelectionError {
    #[error("transform `{label}` does not match a built-in template (only avro-cdc and json-cloudevents are supported)")]
    Unknown { label: String },
    #[error("transform `{label}`: {reason}")]
    Malformed { label: String, reason: String },
}

impl Builtin {
    pub fn select(label: &str, python: &str) -> Result<Self, TransformSelectionError> {
        let malformed = |reason: &str| TransformSelectionError::Malformed {
            label: label.to_string(),
            reason: reason.to_string(),
        };
        let code = normalize(python);
        let (code, cluster) = extract(&code, CLUSTER_PREFIX, |rest| {
            rest.split_once('"').map(|(v, tail)| (v.to_string(), tail))
        });
        let (code, deny) = extract(&code, DENY_LIST_PREFIX, |rest| Some((rest.to_string(), "")));

        if code == normalize(DEBEZIUM_TEMPLATE) {
            let cluster = cluster.ok_or_else(|| malformed("no __cluster literal"))?;
            let deny = deny.ok_or_else(|| malformed("no TOPIC_COLUMN_TO_FILTER literal"))?;
            let deny_list = parse_deny_list(&deny).map_err(|e| malformed(&format!("TOPIC_COLUMN_TO_FILTER: {e}")))?;
            return Ok(Builtin::Debezium { cluster, deny_list });
        }
        if code == normalize(CLOUDEVENTS_TEMPLATE) {
            let cluster = cluster.ok_or_else(|| malformed("no __cluster literal"))?;
            return Ok(Builtin::CloudEvents { cluster });
        }
        Err(TransformSelectionError::Unknown { label: label.to_string() })
    }
}

/// Transform output in bizon's dict order; later duplicates win, as in a Python dict literal.
pub type Row = Vec<(String, Value)>;

/// Failures the inline Python would raise (KeyError, TypeError, ...), which stop the worker.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TransformError {
    #[error("missing {0}")]
    Missing(&'static str),
    #[error("{0} is not a mapping")]
    NotMapping(&'static str),
    #[error("{0} is not a number")]
    NotNumber(&'static str),
    #[error("No fields found in the Avro schema, please check the schema and custom transform")]
    NoBeforeFields,
    #[error("invalid JSON in string keys: {0}")]
    KeysJson(String),
}

impl Builtin {
    /// The `__schema` column as JSON text. It depends only on the registry schema and the topic, so
    /// callers compute it once per (schema, topic) and pass it to `apply`.
    pub fn schema_column(&self, topic: &str, schema: &Value) -> Result<String, TransformError> {
        match self {
            Builtin::Debezium { deny_list, .. } => {
                let mut fields = before_fields(schema)?;
                if let Some(columns) = deny_list.get(topic).filter(|c| !c.is_empty()) {
                    fields.retain(|f| {
                        !f.get("name")
                            .and_then(Value::as_str)
                            .is_some_and(|n| columns.iter().any(|c| c == n))
                    });
                }
                Ok(crate::json::to_string(&Value::Array(fields)))
            }
            Builtin::CloudEvents { .. } => Ok(crate::json::to_string(schema)),
        }
    }

    /// `value` is the decoded message value, `schema_column` comes from `schema_column`, and
    /// `inserted_at` is the `datetime.utcnow().isoformat()` the Python transform would compute.
    pub fn apply(&self, rec: &Record<'_>, value: Value, schema_column: &str, inserted_at: &str) -> Result<Row, TransformError> {
        match self {
            Builtin::Debezium { cluster, deny_list } => debezium(rec, value, schema_column, inserted_at, cluster, deny_list),
            Builtin::CloudEvents { cluster } => cloudevents(rec, value, schema_column, inserted_at, cluster),
        }
    }
}

fn spread_keys(keys: &Value) -> Result<Row, TransformError> {
    match keys {
        Value::Object(m) => Ok(m.iter().map(|(k, v)| (k.clone(), v.clone())).collect()),
        _ => Err(TransformError::NotMapping("keys")),
    }
}

fn event_timestamp(ms: &Value, what: &'static str) -> Result<String, TransformError> {
    let ms = match ms {
        Value::Number(n) if n.is_i64() => n.as_i64().unwrap(),
        Value::Number(n) => n.as_f64().map(|f| f as i64).ok_or(TransformError::NotNumber(what))?,
        Value::Null => return Err(TransformError::Missing(what)),
        _ => return Err(TransformError::NotNumber(what)),
    };
    Ok(timefmt::from_millis(ms).strftime_micros())
}

/// Fields of the first branch of the `before` union that is a record, as the template looks them up.
fn before_fields(schema: &Value) -> Result<Vec<Value>, TransformError> {
    let mut fields = Vec::new();
    for field in schema.get("fields").and_then(Value::as_array).into_iter().flatten() {
        if field.get("name").and_then(Value::as_str) != Some("before") {
            continue;
        }
        let Some(Value::Array(branches)) = field.get("type") else {
            continue;
        };
        if let Some(found) = branches.iter().find_map(|b| b.get("fields").and_then(Value::as_array)) {
            fields = found.clone();
        }
    }
    if fields.is_empty() {
        return Err(TransformError::NoBeforeFields);
    }
    Ok(fields)
}

/// The column holds JSON text already; the encoder writes strings verbatim, which yields the same
/// bytes as serializing the list in place.
fn json_text(s: &str) -> Value {
    Value::String(s.to_string())
}

fn debezium(
    rec: &Record<'_>,
    mut value: Value,
    schema_column: &str,
    inserted_at: &str,
    cluster: &str,
    deny_list: &BTreeMap<String, Vec<String>>,
) -> Result<Row, TransformError> {
    let mut row = spread_keys(&rec.keys)?;
    let Value::Object(v) = &mut value else {
        return Err(TransformError::NotMapping("value"));
    };
    let operation = v.get("op").cloned().ok_or(TransformError::Missing("value.op"))?;
    let ts_ms = v
        .get("source")
        .ok_or(TransformError::Missing("value.source"))?
        .get("ts_ms")
        .ok_or(TransformError::Missing("value.source.ts_ms"))?;
    let event_ts = event_timestamp(ts_ms, "value.source.ts_ms")?;
    let deleted = operation == "d";
    let mut payload = v
        .remove(if deleted { "before" } else { "after" })
        .ok_or(TransformError::Missing("value.before/after"))?;
    if let Some(columns) = deny_list.get(rec.topic).filter(|c| !c.is_empty()) {
        let Value::Object(p) = &mut payload else {
            return Err(TransformError::NotMapping("payload"));
        };
        for c in columns {
            p.shift_remove(c);
        }
    }

    row.extend([
        ("payload".to_string(), Value::from(crate::json::to_string(&payload))),
        ("__operation".to_string(), operation),
        ("__deleted".to_string(), Value::from(deleted)),
        ("__cluster".to_string(), Value::from(cluster)),
        ("__kafka_partition".to_string(), Value::from(rec.partition)),
        ("__kafka_offset".to_string(), Value::from(rec.offset)),
        ("__kafka_topic".to_string(), Value::from(rec.topic)),
        ("__schema".to_string(), json_text(schema_column)),
        ("__event_timestamp".to_string(), Value::from(event_ts)),
        ("__inserted_at".to_string(), Value::from(inserted_at)),
    ]);
    Ok(row)
}

fn cloudevents(rec: &Record<'_>, value: Value, schema_column: &str, inserted_at: &str, cluster: &str) -> Result<Row, TransformError> {
    let keys = match &rec.keys {
        Value::String(s) => serde_json::from_str(s).map_err(|e| TransformError::KeysJson(e.to_string()))?,
        other => other.clone(),
    };
    let mut row = spread_keys(&keys)?;
    let event_ts = event_timestamp(&Value::from(rec.timestamp_ms), "timestamp")?;
    let header = |name: &'static str| rec.headers.get(name).cloned();
    row.extend([
        ("payload".to_string(), value),
        (
            "__ce_type".to_string(),
            header("ce_type").ok_or(TransformError::Missing("headers.ce_type"))?,
        ),
        (
            "__ce_id".to_string(),
            header("ce_id").ok_or(TransformError::Missing("headers.ce_id"))?,
        ),
        ("__ce_time".to_string(), header("ce_time").unwrap_or(Value::Null)),
        ("__event_timestamp".to_string(), Value::from(event_ts)),
        ("__kafka_partition".to_string(), Value::from(rec.partition)),
        ("__kafka_offset".to_string(), Value::from(rec.offset)),
        ("__kafka_topic".to_string(), Value::from(rec.topic)),
        ("__schema".to_string(), json_text(schema_column)),
        ("__cluster".to_string(), Value::from(cluster)),
        ("__inserted_at".to_string(), Value::from(inserted_at)),
    ]);
    Ok(row)
}

/// `textwrap.dedent` plus trailing-whitespace trimming, so YAML indentation does not matter.
fn normalize(code: &str) -> String {
    let lines: Vec<&str> = code.lines().map(str::trim_end).collect();
    let indent = lines
        .iter()
        .filter(|l| !l.is_empty())
        .map(|l| l.len() - l.trim_start().len())
        .min()
        .unwrap_or(0);
    let body: Vec<&str> = lines.iter().map(|l| if l.len() >= indent { &l[indent..] } else { "" }).collect();
    body.join("\n").trim_matches('\n').to_string()
}

/// Finds the first line containing `prefix`, hands the text after it to `take`, and replaces the
/// taken literal with the template placeholder so the rest of the code can be compared.
fn extract(code: &str, prefix: &str, take: impl Fn(&str) -> Option<(String, &str)>) -> (String, Option<String>) {
    let placeholder = if prefix == CLUSTER_PREFIX {
        "${cluster-name}\""
    } else {
        "${topic-column-to-remove}"
    };
    let mut found = None;
    let lines: Vec<String> = code
        .lines()
        .map(|line| {
            if found.is_some() {
                return line.to_string();
            }
            let Some(at) = line.find(prefix) else { return line.to_string() };
            let (head, rest) = line.split_at(at + prefix.len());
            match take(rest) {
                Some((value, tail)) => {
                    found = Some(value);
                    format!("{head}{placeholder}{tail}")
                }
                None => line.to_string(),
            }
        })
        .collect();
    (lines.join("\n"), found)
}

/// Parses the Python dict literal `{'topic': ['col', ...], ...}` that regen-values.py inlines.
fn parse_deny_list(text: &str) -> Result<BTreeMap<String, Vec<String>>, String> {
    let mut p = Literal {
        s: text.trim().as_bytes(),
        i: 0,
    };
    let mut out = BTreeMap::new();
    p.expect(b'{')?;
    if !p.eat(b'}') {
        loop {
            let topic = p.string()?;
            p.expect(b':')?;
            p.expect(b'[')?;
            let mut cols = Vec::new();
            if !p.eat(b']') {
                loop {
                    cols.push(p.string()?);
                    if p.eat(b']') {
                        break;
                    }
                    p.expect(b',')?;
                    if p.eat(b']') {
                        break;
                    }
                }
            }
            out.insert(topic, cols);
            if p.eat(b'}') {
                break;
            }
            p.expect(b',')?;
            if p.eat(b'}') {
                break;
            }
        }
    }
    p.ws();
    if p.i != p.s.len() {
        return Err("trailing characters".into());
    }
    Ok(out)
}

struct Literal<'a> {
    s: &'a [u8],
    i: usize,
}

impl Literal<'_> {
    fn ws(&mut self) {
        while self.i < self.s.len() && self.s[self.i].is_ascii_whitespace() {
            self.i += 1;
        }
    }
    fn eat(&mut self, c: u8) -> bool {
        self.ws();
        let hit = self.s.get(self.i) == Some(&c);
        self.i += hit as usize;
        hit
    }
    fn expect(&mut self, c: u8) -> Result<(), String> {
        self.eat(c)
            .then_some(())
            .ok_or_else(|| format!("expected `{}` at {}", c as char, self.i))
    }
    fn string(&mut self) -> Result<String, String> {
        self.ws();
        let q = *self.s.get(self.i).ok_or("unexpected end")?;
        if q != b'\'' && q != b'"' {
            return Err(format!("expected a quoted string at {}", self.i));
        }
        let start = self.i + 1;
        let end = self.s[start..].iter().position(|&c| c == q).ok_or("unterminated string")? + start;
        if self.s[start..end].contains(&b'\\') {
            return Err("escapes are not supported".into());
        }
        self.i = end + 1;
        String::from_utf8(self.s[start..end].to_vec()).map_err(|e| e.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deny_list_literals() {
        assert!(parse_deny_list("{}").unwrap().is_empty());
        let d = parse_deny_list("{'a': ['x', 'y'], \"b\": []}").unwrap();
        assert_eq!(d["a"], vec!["x", "y"]);
        assert!(d["b"].is_empty());
        assert!(parse_deny_list("{'a': 'x'}").is_err());
    }

    #[test]
    fn templates_select_themselves_after_substitution() {
        let deb = DEBEZIUM_TEMPLATE
            .replace("${cluster-name}", "us-east1-635c")
            .replace("${topic-column-to-remove}", "{'t': ['c']}");
        let indented: String = deb.lines().map(|l| format!("    {l}\n")).collect();
        let b = Builtin::select("debezium", &indented).unwrap();
        assert_eq!(
            b,
            Builtin::Debezium {
                cluster: "us-east1-635c".into(),
                deny_list: [("t".to_string(), vec!["c".to_string()])].into(),
            }
        );
        let ce = CLOUDEVENTS_TEMPLATE.replace("${cluster-name}", "my-cluster");
        assert_eq!(
            Builtin::select("parse_events", &ce).unwrap(),
            Builtin::CloudEvents {
                cluster: "my-cluster".into()
            }
        );
    }

    #[test]
    fn any_code_change_is_rejected() {
        let ce = CLOUDEVENTS_TEMPLATE
            .replace("${cluster-name}", "x")
            .replace("data[\"headers\"][\"ce_type\"]", "data[\"headers\"].get(\"ce_type\")");
        assert!(matches!(
            Builtin::select("parse_events", &ce),
            Err(TransformSelectionError::Unknown { .. })
        ));
    }
}
