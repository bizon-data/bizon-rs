//! Per-message handling from bizon's `KafkaSource.parse_encoded_messages`: skip rules, key parsing,
//! value decoding, headers and topic routing, with the same order of checks so the same message
//! fails or is skipped for the same reason.

use std::collections::HashMap;
use std::sync::Arc;

use serde_json::{Map, Value};

use super::framing::{self, Framing, FramingError};
use super::registry::RegistrySchema;
use crate::config::{MessageEncoding, SourceConfig};

pub type Headers<'a> = &'a [(&'a str, Option<&'a [u8]>)];

/// A consumed message, independent of rdkafka so fixtures can be replayed.
#[derive(Debug, Clone, Copy)]
pub struct RawMessage<'a> {
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    /// `message.timestamp()[1]`: epoch ms, -1 when unavailable.
    pub timestamp_ms: i64,
    pub key: Option<&'a [u8]>,
    pub value: Option<&'a [u8]>,
    /// `None` when the message carries no headers.
    pub headers: Option<Headers<'a>>,
}

#[derive(Debug)]
pub enum Payload<'a> {
    Json(Value),
    /// Decoded later against `schema`; `body` starts after the framing header.
    Avro {
        schema: Arc<RegistrySchema>,
        body: &'a [u8],
    },
}

#[derive(Debug)]
pub struct Record<'a> {
    pub destination_id: Arc<str>,
    pub topic: &'a str,
    pub partition: i32,
    pub offset: i64,
    pub timestamp_ms: i64,
    pub keys: Value,
    pub headers: Map<String, Value>,
    pub value: Payload<'a>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkipReason {
    EmptyValue,
    InvalidKey,
    DecodeError,
}

#[derive(Debug)]
pub enum Parsed<'a> {
    Record(Box<Record<'a>>),
    Skipped(SkipReason),
}

/// Failures bizon does not catch; the worker must stop without committing.
#[derive(Debug, thiserror::Error)]
pub enum ParseError {
    /// Python: `UnicodeDecodeError` on `message.key().decode("utf-8")`, raised regardless of flags.
    #[error("{at}: key is not valid UTF-8")]
    KeyNotUtf8 { at: String },
    #[error("{at}: invalid JSON key: {source}")]
    KeyJson { at: String, source: serde_json::Error },
    #[error("{at}: {source}")]
    Decode { at: String, source: DecodeError },
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error(transparent)]
    Framing(#[from] FramingError),
    #[error("schema {0} was not loaded")]
    SchemaMissing(i64),
    #[error("invalid JSON value: {0}")]
    Json(#[from] serde_json::Error),
    #[error("header {0} is not valid UTF-8")]
    HeaderNotUtf8(String),
    #[error("header {0} has no value")]
    HeaderNull(String),
    #[error("topic {0} has no destination")]
    UnknownTopic(String),
}

pub struct MessageParser {
    encoding: MessageEncoding,
    skip_empty_value: bool,
    skip_invalid_keys: bool,
    skip_on_decode_error: bool,
    topic_map: HashMap<String, Arc<str>>,
}

impl MessageParser {
    pub fn new(cfg: &SourceConfig) -> Self {
        Self {
            encoding: cfg.message_encoding,
            skip_empty_value: cfg.skip_message_empty_value,
            skip_invalid_keys: cfg.skip_message_invalid_keys,
            skip_on_decode_error: cfg.skip_message_on_decode_error,
            topic_map: cfg
                .topics
                .iter()
                .map(|t| (t.name.clone(), Arc::from(t.destination_id.as_str())))
                .collect(),
        }
    }

    /// The registry id an Avro message needs loaded before `parse`, if any.
    pub fn schema_id(&self, msg: &RawMessage<'_>) -> Option<i64> {
        if self.encoding != MessageEncoding::Avro {
            return None;
        }
        msg.value.and_then(|v| framing::parse(v).ok()).map(|f| f.global_id)
    }

    pub fn parse<'a>(&self, msg: &RawMessage<'a>, schemas: impl Fn(i64) -> Option<Arc<RegistrySchema>>) -> Result<Parsed<'a>, ParseError> {
        let at = || format!("{}[{}]@{}", msg.topic, msg.partition, msg.offset);
        let value = msg.value.unwrap_or_default();
        if self.skip_empty_value && value.is_empty() {
            return Ok(Parsed::Skipped(SkipReason::EmptyValue));
        }

        let keys = match msg.key.filter(|k| !k.is_empty()) {
            None => Value::Object(Map::new()),
            Some(raw) => {
                let text = std::str::from_utf8(raw).map_err(|_| ParseError::KeyNotUtf8 { at: at() })?;
                match serde_json::from_str(text) {
                    Ok(v) => v,
                    Err(_) if self.skip_invalid_keys => return Ok(Parsed::Skipped(SkipReason::InvalidKey)),
                    Err(source) => return Err(ParseError::KeyJson { at: at(), source }),
                }
            }
        };

        match self.decode(msg, value, &schemas) {
            Ok((payload, headers, destination_id)) => Ok(Parsed::Record(Box::new(Record {
                destination_id,
                topic: msg.topic,
                partition: msg.partition,
                offset: msg.offset,
                timestamp_ms: msg.timestamp_ms,
                keys,
                headers,
                value: payload,
            }))),
            Err(_) if self.skip_on_decode_error => Ok(Parsed::Skipped(SkipReason::DecodeError)),
            Err(source) => Err(ParseError::Decode { at: at(), source }),
        }
    }

    /// Everything inside bizon's `try` block, in its order: value, headers, topic routing.
    #[allow(clippy::type_complexity)]
    fn decode<'a>(
        &self,
        msg: &RawMessage<'a>,
        value: &'a [u8],
        schemas: &impl Fn(i64) -> Option<Arc<RegistrySchema>>,
    ) -> Result<(Payload<'a>, Map<String, Value>, Arc<str>), DecodeError> {
        let payload = match self.encoding {
            MessageEncoding::Avro => {
                let Framing { global_id, body_offset } = framing::parse(value)?;
                let schema = schemas(global_id).ok_or(DecodeError::SchemaMissing(global_id))?;
                Payload::Avro {
                    schema,
                    body: &value[body_offset..],
                }
            }
            MessageEncoding::Utf8 => Payload::Json(decode_utf8_json(value)?),
        };

        let mut headers = Map::new();
        for (k, v) in msg.headers.unwrap_or_default() {
            let v = v.ok_or_else(|| DecodeError::HeaderNull(k.to_string()))?;
            let v = std::str::from_utf8(v).map_err(|_| DecodeError::HeaderNotUtf8(k.to_string()))?;
            headers.insert(k.to_string(), Value::from(v));
        }

        let destination_id = self
            .topic_map
            .get(msg.topic)
            .cloned()
            .ok_or_else(|| DecodeError::UnknownTopic(msg.topic.to_string()))?;
        Ok((payload, headers, destination_id))
    }
}

/// bizon's `decode_utf_8`: lossy UTF-8, then JSON; on a parse error mentioning surrogates or control
/// characters, sanitize and retry once.
pub fn decode_utf8_json(value: &[u8]) -> Result<Value, serde_json::Error> {
    let text = String::from_utf8_lossy(value);
    let err = match serde_json::from_str(&text) {
        Ok(v) => return Ok(v),
        Err(e) => e,
    };
    let msg = err.to_string().to_lowercase();
    let mut sanitized = text.to_string();
    if msg.contains("surrogate") {
        sanitized = sanitize_lone_surrogates(&sanitized);
    }
    if msg.contains("control character") {
        sanitized = sanitize_control_characters(&sanitized);
    }
    if sanitized == text {
        return Err(err);
    }
    serde_json::from_str(&sanitized)
}

fn surrogate_escape(s: &[u8], low: bool) -> bool {
    s.len() >= 6
        && s[0] == b'\\'
        && s[1] == b'u'
        && matches!(s[2], b'd' | b'D')
        && if low {
            matches!(s[3], b'c'..=b'f' | b'C'..=b'F')
        } else {
            matches!(s[3], b'8'..=b'9' | b'a'..=b'b' | b'A'..=b'B')
        }
        && s[4].is_ascii_hexdigit()
        && s[5].is_ascii_hexdigit()
}

/// Same matches as bizon's `_SURROGATE_RE` substitution: a high+low escape pair is kept, any other
/// `\uD800`–`\uDFFF` escape becomes the text `\ufffd`.
pub fn sanitize_lone_surrogates(text: &str) -> String {
    let b = text.as_bytes();
    let mut out = String::with_capacity(text.len());
    let mut i = 0;
    let mut copied = 0;
    while i < b.len() {
        if surrogate_escape(&b[i..], false) && surrogate_escape(&b[i + 6..], true) {
            i += 12;
            continue;
        }
        let lone = b.len() - i >= 6
            && b[i] == b'\\'
            && b[i + 1] == b'u'
            && matches!(b[i + 2], b'd' | b'D')
            && matches!(b[i + 3], b'8'..=b'9' | b'a'..=b'f' | b'A'..=b'F')
            && b[i + 4].is_ascii_hexdigit()
            && b[i + 5].is_ascii_hexdigit();
        if lone {
            out.push_str(&text[copied..i]);
            out.push_str("\\ufffd");
            i += 6;
            copied = i;
            continue;
        }
        i += 1;
    }
    out.push_str(&text[copied..]);
    out
}

pub fn sanitize_control_characters(text: &str) -> String {
    text.chars()
        .filter(|&c| !matches!(c, '\x00'..='\x08' | '\x0b' | '\x0c' | '\x0e'..='\x1f'))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surrogates_mirror_bizon_regex() {
        assert_eq!(sanitize_lone_surrogates(r#""a\ud83d\ude00b""#), r#""a\ud83d\ude00b""#);
        assert_eq!(sanitize_lone_surrogates(r#""a\udf31b""#), r#""a\ufffdb""#);
        assert_eq!(sanitize_lone_surrogates(r#""\ud83d""#), r#""\ufffd""#);
        assert_eq!(sanitize_lone_surrogates(r#""\uDE00\uD83D""#), r#""\ufffd\ufffd""#);
        assert_eq!(sanitize_lone_surrogates(r#""\ud83d\ud83d\ude00""#), r#""\ufffd\ud83d\ude00""#);
    }

    #[test]
    fn utf8_json_sanitizes_then_retries() {
        assert_eq!(decode_utf8_json(br#"{"a":"x\udf31"}"#).unwrap()["a"], "x\u{fffd}");
        assert_eq!(decode_utf8_json(b"{\"a\":\"x\x01y\"}").unwrap()["a"], "xy");
        assert_eq!(decode_utf8_json(b"{\"a\":\"\xff\"}").unwrap()["a"], "\u{fffd}");
        assert!(decode_utf8_json(b"{nope").is_err());
    }

    fn parser(encoding: MessageEncoding, skip_keys: bool, skip_decode: bool) -> MessageParser {
        MessageParser {
            encoding,
            skip_empty_value: true,
            skip_invalid_keys: skip_keys,
            skip_on_decode_error: skip_decode,
            topic_map: [("t".to_string(), Arc::from("p.d.t"))].into(),
        }
    }

    fn msg<'a>(key: Option<&'a [u8]>, value: Option<&'a [u8]>, headers: Option<Headers<'a>>) -> RawMessage<'a> {
        RawMessage {
            topic: "t",
            partition: 0,
            offset: 7,
            timestamp_ms: 1,
            key,
            value,
            headers,
        }
    }

    #[test]
    fn skip_and_failure_rules() {
        let p = parser(MessageEncoding::Utf8, false, false);
        let none = |_| None;
        assert!(matches!(
            p.parse(&msg(None, None, None), none),
            Ok(Parsed::Skipped(SkipReason::EmptyValue))
        ));
        assert!(matches!(
            p.parse(&msg(Some(b"\xff"), Some(b"{}"), None), none),
            Err(ParseError::KeyNotUtf8 { .. })
        ));
        assert!(matches!(
            p.parse(&msg(Some(b"nope"), Some(b"{}"), None), none),
            Err(ParseError::KeyJson { .. })
        ));
        let skipping = parser(MessageEncoding::Utf8, true, true);
        assert!(matches!(
            skipping.parse(&msg(Some(b"nope"), Some(b"{}"), None), none),
            Ok(Parsed::Skipped(SkipReason::InvalidKey))
        ));
        // Non-UTF-8 keys crash bizon even with skip flags on.
        assert!(matches!(
            skipping.parse(&msg(Some(b"\xff"), Some(b"{}"), None), none),
            Err(ParseError::KeyNotUtf8 { .. })
        ));
        let null_header: &[(&str, Option<&[u8]>)] = &[("ce_id", None)];
        assert!(matches!(
            p.parse(&msg(None, Some(b"{}"), Some(null_header)), none),
            Err(ParseError::Decode { .. })
        ));
        assert!(matches!(
            skipping.parse(&msg(None, Some(b"{}"), Some(null_header)), none),
            Ok(Parsed::Skipped(SkipReason::DecodeError))
        ));
    }

    #[test]
    fn record_fields() {
        let p = parser(MessageEncoding::Utf8, false, false);
        let headers: &[(&str, Option<&[u8]>)] = &[("ce_id", Some(b"1")), ("ce_type", Some(b"x")), ("ce_id", Some(b"2"))];
        let Parsed::Record(r) = p
            .parse(&msg(Some(br#"{"id": 3}"#), Some(br#"{"v":1}"#), Some(headers)), |_| None)
            .unwrap()
        else {
            panic!()
        };
        assert_eq!(&*r.destination_id, "p.d.t");
        assert_eq!(r.keys["id"], 3);
        // Duplicate header: last value wins, first position kept (Python dict comprehension).
        assert_eq!(serde_json::to_string(&r.headers).unwrap(), r#"{"ce_id":"2","ce_type":"x"}"#);
    }

    #[test]
    fn avro_needs_its_schema_loaded() {
        let p = parser(MessageEncoding::Avro, false, false);
        let value = [0u8, 0, 0, 0, 0, 0, 0, 0, 42, 2, 4];
        assert_eq!(p.schema_id(&msg(None, Some(&value), None)), Some(42));
        assert!(matches!(
            p.parse(&msg(None, Some(&value), None), |_| None),
            Err(ParseError::Decode {
                source: DecodeError::SchemaMissing(42),
                ..
            })
        ));
        let schema = Arc::new(super::super::registry::Registry::build(42, b"{}".to_vec()).unwrap());
        let Parsed::Record(r) = p.parse(&msg(None, Some(&value), None), |_| Some(schema.clone())).unwrap() else {
            panic!()
        };
        assert!(matches!(r.value, Payload::Avro { body: [2, 4], .. }));
    }
}
