use std::fmt;

/// Timestamp precision for write operations.
///
/// Matches the precision values accepted by the InfluxDB 3 write API.
/// The default is [`Precision::Nanosecond`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum Precision {
    /// Nanoseconds (default)
    #[default]
    Nanosecond,
    /// Microseconds
    Microsecond,
    /// Milliseconds
    Millisecond,
    /// Seconds
    Second,
}

impl Precision {
    /// Returns the API query-parameter string for this precision.
    pub fn as_str(self) -> &'static str {
        match self {
            Precision::Nanosecond => "nanosecond",
            Precision::Microsecond => "microsecond",
            Precision::Millisecond => "millisecond",
            Precision::Second => "second",
        }
    }

    /// Number of nanoseconds in one unit of this precision.
    pub(crate) fn nanos_per_unit(self) -> i64 {
        match self {
            Precision::Nanosecond => 1,
            Precision::Microsecond => 1_000,
            Precision::Millisecond => 1_000_000,
            Precision::Second => 1_000_000_000,
        }
    }

    /// Convert a nanosecond epoch timestamp to this precision.
    pub(crate) fn scale_timestamp(self, nanos: i64) -> i64 {
        nanos / self.nanos_per_unit()
    }
}

impl fmt::Display for Precision {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for Precision {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "nanosecond" | "ns" => Ok(Precision::Nanosecond),
            "microsecond" | "us" | "µs" => Ok(Precision::Microsecond),
            "millisecond" | "ms" => Ok(Precision::Millisecond),
            "second" | "s" => Ok(Precision::Second),
            other => Err(format!("unknown precision: {other}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api() {
        let ns: i64 = 1_700_000_000_123_456_789;
        for (p, scaled) in [
            (Precision::Nanosecond, ns),
            (Precision::Microsecond, ns / 1_000),
            (Precision::Millisecond, ns / 1_000_000),
            (Precision::Second, ns / 1_000_000_000),
        ] {
            assert_eq!(p.as_str().parse::<Precision>().unwrap(), p);
            assert_eq!(p.scale_timestamp(ns), scaled);
        }
    }
}
