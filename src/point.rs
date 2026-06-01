use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;

use chrono::{DateTime, Utc};
use indexmap::IndexMap;

use crate::{error::Error, precision::Precision};

/// A typed value that can be stored in an InfluxDB field.
#[derive(Debug, Clone, PartialEq)]
pub enum FieldValue {
    Float(f64),
    Integer(i64),
    UInteger(u64),
    String(String),
    Boolean(bool),
}

impl FieldValue {
    /// Write the value in line-protocol notation into `buf`.
    pub(crate) fn write_lp(&self, buf: &mut Vec<u8>) {
        match self {
            FieldValue::Float(v) => {
                if v.is_finite() {
                    let mut ryu = ryu::Buffer::new();
                    let s = ryu.format(*v);
                    buf.extend_from_slice(s.as_bytes());
                    // ryu always includes a decimal point for finite floats.
                } else {
                    // NaN / inf are not valid LP; emit the debug form so server
                    // returns a clear per-line error rather than silent corruption.
                    use std::io::Write;
                    let _ = write!(buf, "{v}");
                }
            }
            FieldValue::Integer(v) => {
                let mut itoa_buf = itoa::Buffer::new();
                buf.extend_from_slice(itoa_buf.format(*v).as_bytes());
                buf.push(b'i');
            }
            FieldValue::UInteger(v) => {
                let mut itoa_buf = itoa::Buffer::new();
                buf.extend_from_slice(itoa_buf.format(*v).as_bytes());
                buf.push(b'u');
            }
            FieldValue::String(v) => {
                buf.push(b'"');
                write_escaped_string_field(buf, v);
                buf.push(b'"');
            }
            FieldValue::Boolean(v) => {
                buf.extend_from_slice(if *v { b"true" } else { b"false" });
            }
        }
    }
}

impl fmt::Display for FieldValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FieldValue::Float(v) => write!(f, "{v}"),
            FieldValue::Integer(v) => write!(f, "{v}"),
            FieldValue::UInteger(v) => write!(f, "{v}"),
            FieldValue::String(v) => f.write_str(v),
            FieldValue::Boolean(v) => write!(f, "{v}"),
        }
    }
}

impl From<f64> for FieldValue {
    fn from(v: f64) -> Self {
        FieldValue::Float(v)
    }
}
impl From<f32> for FieldValue {
    fn from(v: f32) -> Self {
        FieldValue::Float(v as f64)
    }
}
impl From<i64> for FieldValue {
    fn from(v: i64) -> Self {
        FieldValue::Integer(v)
    }
}
impl From<i32> for FieldValue {
    fn from(v: i32) -> Self {
        FieldValue::Integer(v as i64)
    }
}
impl From<i16> for FieldValue {
    fn from(v: i16) -> Self {
        FieldValue::Integer(v as i64)
    }
}
impl From<i8> for FieldValue {
    fn from(v: i8) -> Self {
        FieldValue::Integer(v as i64)
    }
}
impl From<u64> for FieldValue {
    fn from(v: u64) -> Self {
        FieldValue::UInteger(v)
    }
}
impl From<u32> for FieldValue {
    fn from(v: u32) -> Self {
        FieldValue::UInteger(v as u64)
    }
}
impl From<u16> for FieldValue {
    fn from(v: u16) -> Self {
        FieldValue::UInteger(v as u64)
    }
}
impl From<u8> for FieldValue {
    fn from(v: u8) -> Self {
        FieldValue::UInteger(v as u64)
    }
}
impl From<bool> for FieldValue {
    fn from(v: bool) -> Self {
        FieldValue::Boolean(v)
    }
}
impl From<String> for FieldValue {
    fn from(v: String) -> Self {
        FieldValue::String(v)
    }
}
impl From<&str> for FieldValue {
    fn from(v: &str) -> Self {
        FieldValue::String(v.to_owned())
    }
}

/// A single time-series data point ready to be written to InfluxDB 3.
///
/// Tags and fields use [`IndexMap`] internally so `tag()` / `field()` dedupe in
/// O(1) regardless of point width, so wide points (hundreds-to-thousands of
/// fields, typical for flight-test telemetry and IIoT4.0 PLC data) build in
/// linear time.
#[derive(Debug, Clone, Default)]
pub struct Point {
    pub(crate) measurement: String,
    pub(crate) tags: IndexMap<String, String>,
    pub(crate) fields: IndexMap<String, FieldValue>,
    pub(crate) timestamp: Option<i64>,
}

impl Point {
    /// Create a new point with the given measurement name.
    pub fn new(measurement: impl Into<String>) -> Self {
        Point {
            measurement: measurement.into(),
            ..Default::default()
        }
    }

    /// Pre-allocate space for `n` fields.  Useful when building wide points
    /// (hundreds of fields per point) where you know the field count up front.
    pub fn with_capacity(measurement: impl Into<String>, n_fields: usize) -> Self {
        Point {
            measurement: measurement.into(),
            tags: IndexMap::new(),
            fields: IndexMap::with_capacity(n_fields),
            timestamp: None,
        }
    }

    /// Add or update a tag.  O(1) deduplication.
    pub fn tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }

    /// Add or update a field.  O(1) deduplication.
    pub fn field(mut self, key: impl Into<String>, value: impl Into<FieldValue>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    /// Set the timestamp as nanoseconds since the Unix epoch.
    pub fn timestamp_nanos(mut self, nanos: i64) -> Self {
        self.timestamp = Some(nanos);
        self
    }

    /// Set the timestamp from a [`DateTime<Utc>`].
    pub fn timestamp_datetime(mut self, dt: DateTime<Utc>) -> Self {
        self.timestamp = dt.timestamp_nanos_opt();
        self
    }

    pub fn measurement(&self) -> &str {
        &self.measurement
    }
    pub fn tags(&self) -> &IndexMap<String, String> {
        &self.tags
    }
    pub fn fields(&self) -> &IndexMap<String, FieldValue> {
        &self.fields
    }
    pub fn timestamp(&self) -> Option<i64> {
        self.timestamp
    }

    /// Serialise the point to InfluxDB line protocol with the given precision.
    ///
    /// Returns an error if the point has no fields.
    pub fn to_line_protocol(&self, precision: Precision) -> Result<String, Error> {
        let mut buf = Vec::with_capacity(64);
        let mut key_scratch = Vec::new();
        self.write_line_protocol(&mut buf, precision, &HashMap::new(), None, &mut key_scratch)?;
        Ok(String::from_utf8(buf).expect("line protocol is valid UTF-8"))
    }

    /// Write the line-protocol representation directly into `buf`, serialising
    /// straight into the batch buffer without intermediate allocations.
    ///
    /// `key_scratch` is a caller-owned buffer reused across points in a batch so
    /// that sorting tag keys does not allocate per point; its contents are
    /// overwritten on each call.
    pub(crate) fn write_line_protocol<'p>(
        &'p self,
        buf: &mut Vec<u8>,
        precision: Precision,
        default_tags: &HashMap<String, String>,
        tag_order: Option<&[String]>,
        key_scratch: &mut Vec<&'p str>,
    ) -> Result<(), Error> {
        if self.fields.is_empty() {
            return Err(Error::Config(format!(
                "point '{}' has no fields; at least one field is required",
                self.measurement
            )));
        }

        // Measurement
        write_escaped_measurement(buf, &self.measurement);

        // Tags: merge defaults with point tags (point tags win on conflict).
        // Skip the merge entirely when there are no default tags (the common
        // hot-path case): we already have a deduplicated IndexMap.
        if default_tags.is_empty() && tag_order.is_none() {
            if !self.tags.is_empty() {
                key_scratch.clear();
                key_scratch.extend(self.tags.keys().map(String::as_str));
                key_scratch.sort_unstable();
                for &k in key_scratch.iter() {
                    buf.push(b',');
                    write_escaped_tag_key(buf, k);
                    buf.push(b'=');
                    write_escaped_tag_value(buf, &self.tags[k]);
                }
            }
        } else {
            // Merge path: only walked when default_tags or tag_order is set.
            let mut tag_map: HashMap<&str, &str> =
                HashMap::with_capacity(default_tags.len() + self.tags.len());
            for (k, v) in default_tags {
                tag_map.insert(k.as_str(), v.as_str());
            }
            for (k, v) in &self.tags {
                tag_map.insert(k.as_str(), v.as_str());
            }

            if !tag_map.is_empty() {
                let ordered_keys: Vec<&str> = if let Some(order) = tag_order {
                    let mut ordered: Vec<&str> = order
                        .iter()
                        .filter(|k| tag_map.contains_key(k.as_str()))
                        .map(|k| k.as_str())
                        .collect();
                    let mut rest: Vec<&str> = tag_map
                        .keys()
                        .copied()
                        .filter(|k| !order.iter().any(|o| o.as_str() == *k))
                        .collect();
                    rest.sort_unstable();
                    ordered.extend(rest);
                    ordered
                } else {
                    let mut keys: Vec<&str> = tag_map.keys().copied().collect();
                    keys.sort_unstable();
                    keys
                };

                for k in ordered_keys {
                    buf.push(b',');
                    write_escaped_tag_key(buf, k);
                    buf.push(b'=');
                    write_escaped_tag_value(buf, tag_map[k]);
                }
            }
        }

        // Fields
        buf.push(b' ');
        let mut first = true;
        for (k, v) in &self.fields {
            if !first {
                buf.push(b',');
            }
            first = false;
            write_escaped_tag_key(buf, k); // same escape rules as tag keys
            buf.push(b'=');
            v.write_lp(buf);
        }

        // Timestamp
        if let Some(ts) = self.timestamp {
            buf.push(b' ');
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(precision.scale_timestamp(ts)).as_bytes());
        }

        Ok(())
    }
}

// See: https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/

/// Returns `Cow::Borrowed` when no escaping is required, avoiding allocation on
/// the common path where measurement/tag/field names are safe identifiers.
fn escape_with(input: &str, needs_escape: fn(u8) -> bool) -> Cow<'_, str> {
    if !input.bytes().any(needs_escape) {
        return Cow::Borrowed(input);
    }
    let mut out = String::with_capacity(input.len() + 8);
    for ch in input.chars() {
        if ch.is_ascii() && needs_escape(ch as u8) {
            out.push('\\');
        }
        out.push(ch);
    }
    Cow::Owned(out)
}

fn measurement_needs_escape(b: u8) -> bool {
    matches!(b, b',' | b' ')
}

fn tag_needs_escape(b: u8) -> bool {
    matches!(b, b',' | b'=' | b' ')
}

/// Escape a measurement name (commas and spaces). Shared with the DataFrame
/// writer so both paths use the same rules.
pub(crate) fn escape_measurement(s: &str) -> Cow<'_, str> {
    escape_with(s, measurement_needs_escape)
}

/// Escape a tag key, tag value, or field key (commas, equals, spaces).
pub(crate) fn escape_tag(s: &str) -> Cow<'_, str> {
    escape_with(s, tag_needs_escape)
}

/// Escape the contents of a string field (backslash and double-quote). The
/// caller is responsible for the surrounding quotes.
pub(crate) fn escape_string_field(s: &str) -> Cow<'_, str> {
    if !s.bytes().any(|b| b == b'\\' || b == b'"') {
        return Cow::Borrowed(s);
    }
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        if ch == '\\' || ch == '"' {
            out.push('\\');
        }
        out.push(ch);
    }
    Cow::Owned(out)
}

fn write_escaped_measurement(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(escape_measurement(s).as_bytes());
}

fn write_escaped_tag_key(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(escape_tag(s).as_bytes());
}

fn write_escaped_tag_value(buf: &mut Vec<u8>, s: &str) {
    write_escaped_tag_key(buf, s); // same rules
}

fn write_escaped_string_field(buf: &mut Vec<u8>, s: &str) {
    buf.extend_from_slice(escape_string_field(s).as_bytes());
}
