//! Polars [`DataFrame`] to InfluxDB line protocol serialiser.
//!
//! This module is compiled **only** when the `polars` Cargo feature is enabled.
//! It converts every row of a DataFrame into an InfluxDB line-protocol line,
//! using the same escaping and type-suffix rules as [`crate::point::Point`].

use std::borrow::Cow;
use std::collections::HashSet;

use polars::prelude::{AnyValue, Column, DataFrame, DataType, TimeUnit};

use crate::point::{
    escape_measurement, escape_string_field, escape_tag, write_escaped_tag_value, write_lp_bool,
    write_lp_f32, write_lp_f64, write_lp_int, write_lp_string_field, write_lp_uint,
};
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
///
/// This is the fallback for dtypes without a typed reader below; the common
/// numeric/bool/string dtypes never pass through here.
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
            let mut buf = Vec::new();
            write_lp_f32(&mut buf, v);
            Some(String::from_utf8(buf).expect("line protocol is valid UTF-8"))
        }
        AnyValue::Float64(v) => {
            let mut buf = Vec::new();
            write_lp_f64(&mut buf, v);
            Some(String::from_utf8(buf).expect("line protocol is valid UTF-8"))
        }
        // f16 via f32
        AnyValue::Float16(v) => {
            let mut buf = Vec::new();
            write_lp_f32(&mut buf, f32::from(v));
            Some(String::from_utf8(buf).expect("line protocol is valid UTF-8"))
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

type OptIter<'a, T> = Box<dyn Iterator<Item = Option<T>> + 'a>;

/// Typed per-column value reader, downcast from the column's dtype once so the
/// row loop iterates chunked arrays directly instead of going through
/// per-cell `AnyValue` dispatch.
enum FieldReader<'a> {
    Int(OptIter<'a, i64>),
    UInt(OptIter<'a, u64>),
    F32(OptIter<'a, f32>),
    F64(OptIter<'a, f64>),
    Bool(OptIter<'a, bool>),
    Str(OptIter<'a, &'a str>),
    /// Row-wise `AnyValue` access for dtypes without a typed reader.
    Fallback(&'a Column),
}

fn field_reader(col: &Column) -> FieldReader<'_> {
    let s = col.as_materialized_series();
    match col.dtype() {
        DataType::Int8 => FieldReader::Int(Box::new(
            s.i8().unwrap().into_iter().map(|o| o.map(i64::from)),
        )),
        DataType::Int16 => FieldReader::Int(Box::new(
            s.i16().unwrap().into_iter().map(|o| o.map(i64::from)),
        )),
        DataType::Int32 => FieldReader::Int(Box::new(
            s.i32().unwrap().into_iter().map(|o| o.map(i64::from)),
        )),
        DataType::Int64 => FieldReader::Int(Box::new(s.i64().unwrap().into_iter())),
        DataType::UInt8 => FieldReader::UInt(Box::new(
            s.u8().unwrap().into_iter().map(|o| o.map(u64::from)),
        )),
        DataType::UInt16 => FieldReader::UInt(Box::new(
            s.u16().unwrap().into_iter().map(|o| o.map(u64::from)),
        )),
        DataType::UInt32 => FieldReader::UInt(Box::new(
            s.u32().unwrap().into_iter().map(|o| o.map(u64::from)),
        )),
        DataType::UInt64 => FieldReader::UInt(Box::new(s.u64().unwrap().into_iter())),
        DataType::Float32 => FieldReader::F32(Box::new(s.f32().unwrap().into_iter())),
        DataType::Float64 => FieldReader::F64(Box::new(s.f64().unwrap().into_iter())),
        DataType::Boolean => FieldReader::Bool(Box::new(s.bool().unwrap().into_iter())),
        DataType::String => FieldReader::Str(Box::new(s.str().unwrap().into_iter())),
        // Temporal field columns are emitted as their physical integer with an
        // `i` suffix, matching the `to_field_value` fallback.
        DataType::Datetime(_, _) => {
            FieldReader::Int(Box::new(s.datetime().unwrap().physical().into_iter()))
        }
        DataType::Date => FieldReader::Int(Box::new(
            s.date()
                .unwrap()
                .physical()
                .into_iter()
                .map(|o| o.map(i64::from)),
        )),
        _ => FieldReader::Fallback(col),
    }
}

enum TagReader<'a> {
    Str(OptIter<'a, &'a str>),
    /// Row-wise `AnyValue` access for non-string tag columns.
    Fallback(&'a Column),
}

fn tag_reader(col: &Column) -> TagReader<'_> {
    match col.dtype() {
        DataType::String => TagReader::Str(Box::new(
            col.as_materialized_series().str().unwrap().into_iter(),
        )),
        _ => TagReader::Fallback(col),
    }
}

/// Build the timestamp reader, with unit conversion baked in at construction.
///
/// * `Int32/Int64/UInt32/UInt64` columns are treated as **already** in the
///   target `precision`, passed through as-is.
/// * `Datetime` columns are converted from their stored `TimeUnit` to the
///   target precision automatically.
/// * Any other dtype yields no reader, so the timestamp is omitted for every
///   row (server assigns the time; unchanged behaviour).
fn timestamp_reader(col: &Column, precision: Precision) -> Option<OptIter<'_, i64>> {
    let s = col.as_materialized_series();
    match col.dtype() {
        DataType::Int64 => Some(Box::new(s.i64().unwrap().into_iter())),
        DataType::Int32 => Some(Box::new(
            s.i32().unwrap().into_iter().map(|o| o.map(i64::from)),
        )),
        DataType::UInt64 => Some(Box::new(
            s.u64().unwrap().into_iter().map(|o| o.map(|v| v as i64)),
        )),
        DataType::UInt32 => Some(Box::new(
            s.u32().unwrap().into_iter().map(|o| o.map(i64::from)),
        )),
        DataType::Datetime(tu, _) => {
            let tu = *tu;
            Some(Box::new(s.datetime().unwrap().physical().into_iter().map(
                move |o| {
                    o.map(|v| {
                        let nanos = match tu {
                            TimeUnit::Nanoseconds => v,
                            TimeUnit::Microseconds => v * 1_000,
                            TimeUnit::Milliseconds => v * 1_000_000,
                        };
                        precision.scale_timestamp(nanos)
                    })
                },
            )))
        }
        _ => None,
    }
}

fn write_field_prefix(buf: &mut Vec<u8>, first: &mut bool, escaped_name: &str) {
    if !*first {
        buf.push(b',');
    }
    *first = false;
    buf.extend_from_slice(escaped_name.as_bytes());
    buf.push(b'=');
}

fn row_access_err(e: polars::error::PolarsError) -> Error {
    Error::Config(format!("polars row access error: {e}"))
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

    // Resolve columns and escape their names once, before the row loop.
    // Missing tag columns are silently skipped (unchanged behaviour).
    let mut tag_cols: Vec<(Cow<'_, str>, TagReader<'_>)> = tags
        .iter()
        .filter_map(|&t| df.column(t).ok().map(|c| (escape_tag(t), tag_reader(c))))
        .collect();

    // All columns that are not tag columns and not the timestamp column,
    // in frame order.
    let mut field_cols: Vec<(Cow<'_, str>, FieldReader<'_>)> = (0..df.width())
        .filter_map(|i| df.select_at_idx(i))
        .filter(|c| {
            let name = c.name().as_str();
            !tag_set.contains(name) && Some(name) != timestamp_column
        })
        .map(|c| (escape_tag(c.name().as_str()), field_reader(c)))
        .collect();

    let mut ts_reader = timestamp_column
        .and_then(|t| df.column(t).ok())
        .and_then(|c| timestamp_reader(c, precision));

    let mut buf: Vec<u8> = Vec::with_capacity(height * 64);

    for row_idx in 0..height {
        let row_start = buf.len();
        if row_start > 0 {
            buf.push(b'\n');
        }
        buf.extend_from_slice(meas_escaped.as_bytes());

        // Tags are emitted in the order given by the caller.
        for (name, reader) in tag_cols.iter_mut() {
            match reader {
                TagReader::Str(it) => {
                    if let Some(v) = it.next().flatten() {
                        buf.push(b',');
                        buf.extend_from_slice(name.as_bytes());
                        buf.push(b'=');
                        write_escaped_tag_value(&mut buf, v);
                    }
                }
                TagReader::Fallback(col) => {
                    let val = col.get(row_idx).map_err(row_access_err)?;
                    if let Some(tv) = to_tag_value(val) {
                        buf.push(b',');
                        buf.extend_from_slice(name.as_bytes());
                        buf.push(b'=');
                        buf.extend_from_slice(tv.as_bytes()); // already escaped
                    }
                }
            }
        }

        buf.push(b' ');
        let field_start = buf.len();
        let mut first = true;
        for (name, reader) in field_cols.iter_mut() {
            match reader {
                FieldReader::Int(it) => {
                    if let Some(v) = it.next().flatten() {
                        write_field_prefix(&mut buf, &mut first, name);
                        write_lp_int(&mut buf, v);
                    }
                }
                FieldReader::UInt(it) => {
                    if let Some(v) = it.next().flatten() {
                        write_field_prefix(&mut buf, &mut first, name);
                        write_lp_uint(&mut buf, v);
                    }
                }
                FieldReader::F32(it) => {
                    if let Some(v) = it.next().flatten() {
                        write_field_prefix(&mut buf, &mut first, name);
                        write_lp_f32(&mut buf, v);
                    }
                }
                FieldReader::F64(it) => {
                    if let Some(v) = it.next().flatten() {
                        write_field_prefix(&mut buf, &mut first, name);
                        write_lp_f64(&mut buf, v);
                    }
                }
                FieldReader::Bool(it) => {
                    if let Some(v) = it.next().flatten() {
                        write_field_prefix(&mut buf, &mut first, name);
                        write_lp_bool(&mut buf, v);
                    }
                }
                FieldReader::Str(it) => {
                    if let Some(v) = it.next().flatten() {
                        write_field_prefix(&mut buf, &mut first, name);
                        write_lp_string_field(&mut buf, v);
                    }
                }
                FieldReader::Fallback(col) => {
                    let val = col.get(row_idx).map_err(row_access_err)?;
                    if let Some(fv) = to_field_value(val) {
                        write_field_prefix(&mut buf, &mut first, name);
                        buf.extend_from_slice(fv.as_bytes());
                    }
                }
            }
        }

        // Consume the timestamp before the all-null check so the reader stays
        // in lockstep with the row index even when the row is dropped.
        let ts = ts_reader.as_mut().and_then(|it| it.next().flatten());

        if buf.len() == field_start {
            // All fields were null; drop this row entirely.
            buf.truncate(row_start);
            continue;
        }

        if let Some(ts) = ts {
            buf.push(b' ');
            let mut itoa_buf = itoa::Buffer::new();
            buf.extend_from_slice(itoa_buf.format(ts).as_bytes());
        }
    }

    Ok(String::from_utf8(buf).expect("line protocol is valid UTF-8"))
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
    use crate::write::{WriteInput, WriteOptions};
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

    #[test]
    fn field_value_serializes_scalar_types() {
        let cases = [
            (AnyValue::Int8(-8), "-8i"),
            (AnyValue::Int16(-16), "-16i"),
            (AnyValue::Int32(-32), "-32i"),
            (AnyValue::Int64(-64), "-64i"),
            (AnyValue::Int128(-128), "-128i"),
            (AnyValue::UInt8(8), "8u"),
            (AnyValue::UInt16(16), "16u"),
            (AnyValue::UInt32(32), "32u"),
            (AnyValue::UInt64(64), "64u"),
            (AnyValue::UInt128(128), "128u"),
            (AnyValue::Float32(2.0), "2.0"),
            (AnyValue::Float32(2.5), "2.5"),
            (AnyValue::Float64(4.0), "4.0"),
            (AnyValue::Float64(4.25), "4.25"),
            (AnyValue::Float16(pf16::from(8.0_f32)), "8.0"),
            (AnyValue::Float16(pf16::from(8.5_f32)), "8.5"),
            (AnyValue::String(r#"say "hi""#), r#""say \"hi\"""#),
            (AnyValue::StringOwned("owned".into()), r#""owned""#),
            (AnyValue::Datetime(42, TimeUnit::Nanoseconds, None), "42i"),
            (AnyValue::Date(7), "7i"),
            (AnyValue::Duration(9, TimeUnit::Milliseconds), "9i"),
            (AnyValue::Time(11), "11i"),
        ];

        for (value, expected) in cases {
            assert_eq!(to_field_value(value).as_deref(), Some(expected));
        }

        let fallback = to_field_value(AnyValue::Binary(b"abc")).unwrap();
        assert!(fallback.starts_with('"'), "got: {fallback}");
        assert!(fallback.ends_with('"'), "got: {fallback}");
    }

    #[test]
    fn timestamp_reader_maps_supported_dtypes() {
        fn first_ts(col: &Column, precision: Precision) -> Option<i64> {
            timestamp_reader(col, precision).and_then(|mut it| it.next().flatten())
        }

        // Integer columns pass through, already in the caller's precision.
        let cases = [
            (Column::new("ts".into(), [32_i32]), Some(32)),
            (Column::new("ts".into(), [64_i64]), Some(64)),
            (Column::new("ts".into(), [32_u32]), Some(32)),
            (Column::new("ts".into(), [64_u64]), Some(64)),
            // A null timestamp is omitted.
            (Column::new("ts".into(), vec![None::<i64>]), None),
        ];
        for (col, expected) in cases {
            assert_eq!(first_ts(&col, Precision::Nanosecond), expected);
        }

        // Datetime columns rescale from their stored TimeUnit to the target
        // precision.
        let dt = |v: i64, tu: TimeUnit| {
            Column::new("ts".into(), [v])
                .cast(&DataType::Datetime(tu, None))
                .unwrap()
        };
        let cases = [
            (
                dt(1_234_567_890, TimeUnit::Nanoseconds),
                Precision::Millisecond,
                Some(1_234),
            ),
            (
                dt(1_234, TimeUnit::Microseconds),
                Precision::Microsecond,
                Some(1_234),
            ),
            (
                dt(12, TimeUnit::Milliseconds),
                Precision::Nanosecond,
                Some(12_000_000),
            ),
        ];
        for (col, precision, expected) in cases {
            assert_eq!(first_ts(&col, precision), expected);
        }

        // Unsupported dtypes yield no reader: timestamp omitted on every row.
        assert!(timestamp_reader(
            &Column::new("ts".into(), ["not a timestamp"]),
            Precision::Nanosecond
        )
        .is_none());
    }

    #[test]
    fn dataframe_write_batches_with_tags_and_timestamp() {
        let df = df![
            "host" => ["a", "b", "c"],
            "v" => [Some(1_i64), None, Some(3_i64)],
            "ts" => [10_i64, 20_i64, 30_i64],
        ]
        .unwrap();

        for batch_size in [0, 2] {
            let opts = WriteOptions {
                batch_size,
                ..WriteOptions::default()
            };
            let batches = DataFrameWrite::new(&df, "m")
                .tags(&["host"])
                .timestamp_column("ts")
                .into_lp_batches(&opts)
                .map(|batch| String::from_utf8(batch.unwrap()).unwrap())
                .collect::<Vec<_>>();

            assert_eq!(
                batches,
                vec![
                    "m,host=a v=1i 10".to_string(),
                    "m,host=c v=3i 30".to_string(),
                ],
                "batch_size={batch_size}",
            );
        }
    }

    #[test]
    fn typed_readers_cover_narrow_and_unsigned_dtypes() {
        // i32/u32/f32 columns take the typed reader paths and must produce
        // the same suffixes as the AnyValue fallback.
        let df = df![
            "i32v" => [-32_i32],
            "u32v" => [32_u32],
            "f32v" => [2.5_f32],
            "ts"   => [10_i64],
        ]
        .unwrap();
        let lp =
            dataframe_to_line_protocol(&df, "m", &[], Some("ts"), Precision::Nanosecond).unwrap();
        assert_eq!(lp, "m i32v=-32i,u32v=32u,f32v=2.5 10");
    }

    #[test]
    fn chunked_columns_serialize_across_chunk_boundaries() {
        // vstack leaves each column with two chunks; the typed iterators must
        // walk both.
        let mut df = df![
            "host" => ["a"],
            "v"    => [1.5_f64],
            "ts"   => [10_i64],
        ]
        .unwrap();
        let df2 = df![
            "host" => ["b"],
            "v"    => [2.5_f64],
            "ts"   => [20_i64],
        ]
        .unwrap();
        df.vstack_mut(&df2).unwrap();

        let lp = dataframe_to_line_protocol(&df, "m", &["host"], Some("ts"), Precision::Nanosecond)
            .unwrap();
        assert_eq!(lp, "m,host=a v=1.5 10\nm,host=b v=2.5 20");
    }
}
