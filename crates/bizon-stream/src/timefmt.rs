//! UTC formatting that matches the Python calls bizon's transforms and fastavro make.

/// Days since 1970-01-01 to (year, month, day), proleptic Gregorian (Howard Hinnant's algorithm).
fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (yoe + era * 400 + (m <= 2) as i64, m, d)
}

/// Splits epoch microseconds into date/time parts, flooring for negative values like Python.
pub struct Parts {
    pub year: i64,
    pub month: u32,
    pub day: u32,
    pub hour: u32,
    pub minute: u32,
    pub second: u32,
    pub micros: u32,
}

pub fn parts(epoch_micros: i64) -> Parts {
    let secs = epoch_micros.div_euclid(1_000_000);
    let micros = epoch_micros.rem_euclid(1_000_000) as u32;
    let (year, month, day) = civil(secs.div_euclid(86_400));
    let sod = secs.rem_euclid(86_400) as u32;
    Parts {
        year,
        month,
        day,
        hour: sod / 3600,
        minute: sod % 3600 / 60,
        second: sod % 60,
        micros,
    }
}

impl Parts {
    fn date_time(&self, sep: char) -> String {
        format!(
            "{:04}-{:02}-{:02}{sep}{:02}:{:02}:{:02}",
            self.year, self.month, self.day, self.hour, self.minute, self.second
        )
    }

    /// `strftime('%Y-%m-%d %H:%M:%S.%f')`
    pub fn strftime_micros(&self) -> String {
        format!("{}.{:06}", self.date_time(' '), self.micros)
    }

    /// Naive `datetime.isoformat()`: fraction only when non-zero.
    pub fn isoformat(&self) -> String {
        match self.micros {
            0 => self.date_time('T'),
            us => format!("{}.{us:06}", self.date_time('T')),
        }
    }

    /// orjson's rendering of an aware UTC datetime.
    pub fn isoformat_utc(&self) -> String {
        format!("{}+00:00", self.isoformat())
    }

    pub fn date(&self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }
}

/// `datetime.utcfromtimestamp(ms / 1000)`: exact for millisecond inputs.
pub fn from_millis(ms: i64) -> Parts {
    parts(ms.saturating_mul(1000))
}

pub fn now_isoformat() -> String {
    let us = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or_default();
    parts(us).isoformat()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn matches_python_datetime() {
        assert_eq!(from_millis(1_700_000_000_123).strftime_micros(), "2023-11-14 22:13:20.123000");
        assert_eq!(from_millis(-1).strftime_micros(), "1969-12-31 23:59:59.999000");
        assert_eq!(from_millis(0).isoformat(), "1970-01-01T00:00:00");
        assert_eq!(parts(951_782_400_000_001).isoformat_utc(), "2000-02-29T00:00:00.000001+00:00");
        assert_eq!(parts(-62_135_596_800_000_000).date(), "0001-01-01");
    }
}
