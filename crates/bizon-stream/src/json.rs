//! JSON text as orjson writes it, which is what ends up in JSON-typed proto fields: compact, raw
//! UTF-8, insertion order, and shortest round-trip floats with exponents like `1e20` (serde_json
//! writes `1e+20`).

use serde_json::Value;

pub fn to_string(v: &Value) -> String {
    let mut out = String::with_capacity(128);
    write(&mut out, v);
    out
}

pub fn write(out: &mut String, v: &Value) {
    match v {
        Value::Null => out.push_str("null"),
        Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                out.push_str(&i.to_string());
            } else if let Some(u) = n.as_u64() {
                out.push_str(&u.to_string());
            } else {
                let f = n.as_f64().unwrap_or(f64::NAN);
                if f.is_finite() {
                    out.push_str(ryu::Buffer::new().format_finite(f));
                } else {
                    out.push_str("null");
                }
            }
        }
        Value::String(s) => string(out, s),
        Value::Array(items) => {
            out.push('[');
            for (i, item) in items.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                write(out, item);
            }
            out.push(']');
        }
        Value::Object(map) => {
            out.push('{');
            for (i, (k, item)) in map.iter().enumerate() {
                if i > 0 {
                    out.push(',');
                }
                string(out, k);
                out.push(':');
                write(out, item);
            }
            out.push('}');
        }
    }
}

fn string(out: &mut String, s: &str) {
    out.push('"');
    let mut start = 0;
    for (i, c) in s.char_indices() {
        let esc = match c {
            '"' => "\\\"",
            '\\' => "\\\\",
            '\n' => "\\n",
            '\r' => "\\r",
            '\t' => "\\t",
            '\u{8}' => "\\b",
            '\u{c}' => "\\f",
            c if (c as u32) < 0x20 => {
                out.push_str(&s[start..i]);
                out.push_str(&format!("\\u{:04x}", c as u32));
                start = i + 1;
                continue;
            }
            _ => continue,
        };
        out.push_str(&s[start..i]);
        out.push_str(esc);
        start = i + c.len_utf8();
    }
    out.push_str(&s[start..]);
    out.push('"');
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn matches_orjson() {
        let v = json!({"f": 1e20, "g": 1.5e-7, "h": 100.0, "i": -0.0, "big": 18446744073709551615u64, "s": "q\"\\\n\u{1}é😀\u{2028}/"});
        assert_eq!(
            to_string(&v),
            "{\"f\":1e20,\"g\":1.5e-7,\"h\":100.0,\"i\":-0.0,\"big\":18446744073709551615,\"s\":\"q\\\"\\\\\\n\\u0001é😀\u{2028}/\"}"
        );
    }
}
