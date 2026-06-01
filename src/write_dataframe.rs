//! Polars [`DataFrame`] to InfluxDB line protocol serialiser.
//!
//! This module is compiled **only** when the `polars` Cargo feature is enabled.
//! It converts every row of a DataFrame into an InfluxDB line-protocol line,
//! using the same escaping and type-suffix rules as [`crate::point::Point`].

use std::collections::HashSet;

use polars::prelude::{AnyValue, DataFrame, TimeUnit};

use crate::point::{escape_measurement, escape_string_field, escape_tag};
use crate::{error::Error, precision::Precision};

/// Convert an `AnyValue` to a **tag value** string (escaped, unquoted).
/// Returns `None` for null values, which are silently omitted.
fn to_tag_value(val: AnyValue<'_>) -> Option<String> {
    match val {
        AnyValue::Null => None,
        AnyValue::String(s) => Some(escape_tag(s).into_owned()),
        AnyValue::StringOwned(s) => Some(escape_tag(s.as_str()).into_owned()),
        other => Some(escape_tag(&format!("{other}")).into_owned()),
    }
}

/// Convert an `AnyValue` to a typed **field** string with the correct suffix.
/// Returns `None` for null values, which are silently omitted.
fn to_field_value(val: AnyValue<'_>) -> Option<String> {
    match val {
        AnyValue::Null => None,
        AnyValue::Boolean(v) => Some(if v { "true".into() } else { "false".into() }),
        AnyValue::Int8(v) => Some(format!("{v}i")),
        AnyValue::Int16(v) => Some(format!("{v}i")),
        AnyValue::Int32(v) => Some(format!("{v}i")),
        AnyValue::Int64(v) => Some(format!("{v}i")),
        AnyValue::Int128(v) => Some(format!("{v}i")),
        AnyValue::UInt8(v) => Some(format!("{v}u")),
        AnyValue::UInt16(v) => Some(format!("{v}u")),
        AnyValue::UInt32(v) => Some(format!("{v}u")),
        AnyValue::UInt64(v) => Some(format!("{v}u")),
        AnyValue::UInt128(v) => Some(format!("{v}u")),
        AnyValue::Float32(v) => {
            if v.fract() == 0.0 && v.is_finite() {
                Some(format!("{v}.0"))
            } else {
                Some(format!("{v}"))
            }
        }
        AnyValue::Float64(v) => {
            if v.fract() == 0.0 && v.is_finite() {
                Some(format!("{v}.0"))
            } else {
                Some(format!("{v}"))
            }
        }
        // f16 via f32
        AnyValue::Float16(v) => {
            let f = f32::from(v);
            if f.fract() == 0.0 && f.is_finite() {
                Some(format!("{f}.0"))
            } else {
                Some(format!("{f}"))
            }
        }
        AnyValue::String(s) => Some(format!("\"{}\"", escape_string_field(s))),
        AnyValue::StringOwned(s) => Some(format!("\"{}\"", escape_string_field(s.as_str()))),
        // Temporal types that end up as field columns are emitted as integers.
        // These should normally be the timestamp column; this is a graceful fallback.
        AnyValue::Datetime(v, _, _) | AnyValue::DatetimeOwned(v, _, _) => Some(format!("{v}i")),
        AnyValue::Date(v) => Some(format!("{v}i")),
        AnyValue::Duration(v, _) => Some(format!("{v}i")),
        AnyValue::Time(v) => Some(format!("{v}i")),
        // Everything else: stringify as a quoted string field.
        other => Some(format!("\"{}\"", escape_string_field(&format!("{other}")))),
    }
}

/// Convert a timestamp `AnyValue` to the integer form used in line protocol.
///
/// * `Int32/Int64/UInt32/UInt64` columns are treated as **already** in the
///   target `precision`, returned as-is.
/// * `Datetime` columns are converted from their stored `TimeUnit` to the
///   target precision automatically.
/// * `Null` returns `None` (no timestamp; server assigns).
fn to_timestamp(val: AnyValue<'_>, precision: Precision) -> Option<i64> {
    match val {
        AnyValue::Null => None,
        // Integer columns: caller's precision defines the unit.
        AnyValue::Int64(v) => Some(v),
        AnyValue::Int32(v) => Some(v as i64),
        AnyValue::UInt64(v) => Some(v as i64),
        AnyValue::UInt32(v) => Some(v as i64),
        // Polars Datetime: stored in the column's own TimeUnit, converted to ns
        // then rescaled to the target precision.
        AnyValue::Datetime(v, tu, _) | AnyValue::DatetimeOwned(v, tu, _) => {
            let nanos = match tu {
                TimeUnit::Nanoseconds => v,
                TimeUnit::Microseconds => v * 1_000,
                TimeUnit::Milliseconds => v * 1_000_000,
            };
            Some(precision.scale_timestamp(nanos))
        }
        _ => None,
    }
}

/// Serialise a polars [`DataFrame`] to newline-separated InfluxDB line protocol.
///
/// # Arguments
///
/// * `df`: the DataFrame to serialise.
/// * `measurement`: the measurement name written for every row.
/// * `tags`: column names to emit as `tag=value` pairs. Order is preserved.
/// * `timestamp_column`: column whose value becomes the row timestamp.
///   - `Datetime` columns are converted to the target `precision`.
///   - Numeric (`Int64`, `UInt64`, ...) columns are assumed already in `precision`.
///   - `None` leaves the timestamp off, so InfluxDB assigns the server time.
/// * `precision`: controls timestamp scaling and the `?precision=` URL
///   parameter sent with the write request.
///
/// # Behaviour
///
/// * Null tag values omit that tag for the row.
/// * Null field values omit that field for the row.
/// * Rows where **all** fields are null are dropped entirely.
/// * A null timestamp is omitted, so the server assigns the time.
pub fn dataframe_to_line_protocol(
    df: &DataFrame,
    measurement: &str,
    tags: &[&str],
    timestamp_column: Option<&str>,
    precision: Precision,
) -> Result<String, Error> {
    let height = df.height();
    if height == 0 {
        return Ok(String::new());
    }

    let meas_escaped = escape_measurement(measurement);
    let tag_set: HashSet<&str> = tags.iter().copied().collect();

    // Pre-fetch all columns by index once (polars 0.53: Column is not Series).
    let width = df.width();
    let all_columns: Vec<&polars::frame::column::Column> =
        (0..width).filter_map(|i| df.select_at_idx(i)).collect();

    let mut lines: Vec<String> = Vec::with_capacity(height);

    for row_idx in 0..height {
        let mut line = String::with_capacity(128);

        line.push_str(&meas_escaped);

        // Tags are emitted in the order given by the caller.
        for &tag in tags {
            if let Ok(col) = df.column(tag) {
                let val = col
                    .get(row_idx)
                    .map_err(|e| Error::Config(format!("polars row access error: {e}")))?;
                if let Some(tv) = to_tag_value(val) {
                    line.push(',');
                    line.push_str(&escape_tag(tag));
                    line.push('=');
                    line.push_str(&tv);
                }
            }
        }

        line.push(' ');

        // All columns that are not tag columns and not the timestamp column.
        let field_start = line.len();
        let mut first_field = true;
        for col in &all_columns {
            let name = col.name().as_str();
            if tag_set.contains(name) || Some(name) == timestamp_column {
                continue;
            }
            let val = col
                .get(row_idx)
                .map_err(|e| Error::Config(format!("polars row access error: {e}")))?;
            if let Some(fv) = to_field_value(val) {
                if !first_field {
                    line.push(',');
                }
                line.push_str(&escape_tag(name));
                line.push('=');
                line.push_str(&fv);
                first_field = false;
            }
        }

        if line.len() == field_start {
            // All fields were null; skip this row entirely.
            continue;
        }

        if let Some(ts_col) = timestamp_column {
            if let Ok(ts_column) = df.column(ts_col) {
                let val = ts_column
                    .get(row_idx)
                    .map_err(|e| Error::Config(format!("polars row access error: {e}")))?;
                if let Some(ts) = to_timestamp(val, precision) {
                    line.push(' ');
                    line.push_str(&ts.to_string());
                }
            }
        }

        lines.push(line);
    }

    Ok(lines.join("\n"))
}

/// A polars [`DataFrame`] bundled with the metadata needed to write it as line
/// protocol.
///
/// Implements [`crate::write::WriteInput`], so it can be passed directly to
/// [`crate::Client::write`].
///
/// ```rust,no_run
/// # #[cfg(feature = "polars")]
/// # async fn example(client: &influxdb3_client::Client) -> influxdb3_client::Result<()> {
/// use polars::prelude::*;
/// use influxdb3_client::write_dataframe::DataFrameWrite;
///
/// let df = df![
///     "host"    => ["srv1", "srv2"],
///     "cpu_pct" => [42.5_f64, 71.0_f64],
///     "time_ns" => [1_700_000_000_000_000_000_i64, 1_700_000_001_000_000_000_i64],
/// ]
/// .unwrap();
///
/// client
///     .write(
///         DataFrameWrite::new(&df, "server_metrics")
///             .tags(&["host"])
///             .timestamp_column("time_ns"),
///     )
///     .await?;
/// # Ok(())
/// # }
/// ```
pub struct DataFrameWrite<'a> {
    df: &'a polars::frame::DataFrame,
    measurement: String,
    tags: Vec<String>,
    timestamp_column: Option<String>,
}

impl<'a> DataFrameWrite<'a> {
    /// Create a new write descriptor for `df` written to `measurement`.
    pub fn new(df: &'a polars::frame::DataFrame, measurement: impl Into<String>) -> Self {
        Self {
            df,
            measurement: measurement.into(),
            tags: Vec::new(),
            timestamp_column: None,
        }
    }

    /// Columns to emit as tag key=value pairs (in the order given).
    pub fn tags(mut self, tags: &[impl AsRef<str>]) -> Self {
        self.tags = tags.iter().map(|s| s.as_ref().to_string()).collect();
        self
    }

    /// Column whose value becomes the line-protocol timestamp.
    ///
    /// - `Datetime` columns are converted to the write precision automatically.
    /// - Integer (`Int64`, `UInt64`, ...) columns are used as-is.
    /// - `None` (the default) leaves the timestamp off; InfluxDB assigns it.
    pub fn timestamp_column(mut self, col: impl Into<String>) -> Self {
        self.timestamp_column = Some(col.into());
        self
    }
}

impl crate::write::WriteInput for DataFrameWrite<'_> {
    fn into_lp_batches(
        self,
        opts: &crate::write::WriteOptions,
    ) -> Box<dyn Iterator<Item = crate::Result<Vec<u8>>> + Send> {
        let precision = opts.precision;
        let batch_size = opts.batch_size.max(1);
        let height = self.df.height();
        let tag_refs: Vec<&str> = self.tags.iter().map(|s| s.as_str()).collect();
        let ts_col = self.timestamp_column.as_deref();

        // Serialise upfront: DataFrameWrite borrows `&'a DataFrame`, so the
        // iterator can't outlive the call. Only Vec<Point> benefits from the
        // lazy per-batch chunking.
        let mut batches: Vec<crate::Result<Vec<u8>>> = Vec::new();
        for start in (0..height).step_by(batch_size) {
            let end = (start + batch_size).min(height);
            let slice = self.df.slice(start as i64, end - start);
            match dataframe_to_line_protocol(
                &slice,
                &self.measurement,
                &tag_refs,
                ts_col,
                precision,
            ) {
                Ok(lp) if !lp.is_empty() => batches.push(Ok(lp.into_bytes())),
                Ok(_) => {}
                Err(e) => {
                    batches.push(Err(e));
                    break;
                }
            }
        }
        Box::new(batches.into_iter())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use polars::prelude::*;

    #[test]
    fn full_serialisation() {
        // Covers: tag order, field order, type suffixes (f64/i64/bool), tag
        // column excluded from field set, integer timestamp pass-through,
        // measurement + string-field escaping.
        let df = df![
            "host"    => ["srv,1"],
            "msg"     => [r#"say "hi""#],
            "cpu_pct" => [42.5_f64],
            "mem_mb"  => [8192_i64],
            "online"  => [true],
            "ts"      => [1_700_000_000_000_i64],
        ]
        .unwrap();
        let lp = dataframe_to_line_protocol(
            &df,
            "m,name",
            &["host"],
            Some("ts"),
            Precision::Millisecond,
        )
        .unwrap();
        assert!(lp.starts_with(r"m\,name,host=srv\,1 "), "got: {lp}");
        assert!(lp.contains("cpu_pct=42.5"));
        assert!(lp.contains("mem_mb=8192i"));
        assert!(lp.contains("online=true"));
        assert!(lp.contains(r#"msg="say \"hi\"""#));
        assert!(lp.ends_with("1700000000000"));
        // tag column must not appear in the field section
        assert!(!lp.split(' ').nth(1).unwrap().contains("host="));
    }

    #[test]
    fn null_and_empty_handling() {
        // Row dropped when all fields null; empty DF yields empty LP.
        let df = df![
            "v"  => [Some(1.0_f64), None::<f64>],
            "ts" => [100_i64, 200_i64],
        ]
        .unwrap();
        let lp =
            dataframe_to_line_protocol(&df, "m", &[], Some("ts"), Precision::Nanosecond).unwrap();
        assert_eq!(lp.lines().count(), 1);
        assert!(lp.contains("v=1.0"));

        let df = df!["v" => Vec::<i64>::new()].unwrap();
        assert!(
            dataframe_to_line_protocol(&df, "m", &[], None, Precision::Nanosecond)
                .unwrap()
                .is_empty()
        );
    }
}
