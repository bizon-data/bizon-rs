//! Schema-registry wire framing, as bizon's `parse_global_id_from_serialized_message` reads it:
//! magic byte 0, then a 4-byte big-endian Confluent id; if that id is 0, the same bytes are re-read
//! as an 8-byte signed Apicurio global id.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Framing {
    pub global_id: i64,
    /// Offset of the Avro body in the message value.
    pub body_offset: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FramingError {
    #[error("Invalid message. Missing schema id")]
    TooShort,
    #[error("Invalid Apicurio message. Missing schema id")]
    ApicurioTooShort,
    #[error("Unexpected magic byte {0}. This message was not produced with a Schema Registry serializer")]
    MagicByte(u8),
}

pub fn parse(value: &[u8]) -> Result<Framing, FramingError> {
    if value.len() < 5 {
        return Err(FramingError::TooShort);
    }
    if value[0] != 0 {
        return Err(FramingError::MagicByte(value[0]));
    }
    let confluent = u32::from_be_bytes(value[1..5].try_into().unwrap());
    if confluent != 0 {
        return Ok(Framing {
            global_id: confluent as i64,
            body_offset: 5,
        });
    }
    if value.len() < 9 {
        return Err(FramingError::ApicurioTooShort);
    }
    Ok(Framing {
        global_id: i64::from_be_bytes(value[1..9].try_into().unwrap()),
        body_offset: 9,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn confluent_and_apicurio_ids() {
        assert_eq!(
            parse(&[0, 0, 0, 1, 2, 0xAA]).unwrap(),
            Framing {
                global_id: 258,
                body_offset: 5
            }
        );
        assert_eq!(
            parse(&[0, 0, 0, 0, 0, 0, 0, 0x30, 0x39, 0xAA]).unwrap(),
            Framing {
                global_id: 12345,
                body_offset: 9
            }
        );
        assert_eq!(parse(&[0, 0, 0, 0, 0, 0]), Err(FramingError::ApicurioTooShort));
        assert_eq!(parse(&[1, 0, 0, 0, 1]), Err(FramingError::MagicByte(1)));
        assert_eq!(parse(&[0, 0]), Err(FramingError::TooShort));
    }
}
