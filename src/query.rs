use std::collections::HashMap;
use std::fmt;
use std::ops::Index;
use std::sync::Arc;

use arrow_array::array::{
    Array, BinaryArray, BooleanArray, Decimal128Array, Decimal256Array, DictionaryArray,
    Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array,
    UInt8Array,
};
use arrow_array::types::{
    Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;

use crate::error::Error;

/// Selects the query language used for a query operation.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum QueryType {
    /// Standard SQL (default)
    #[default]
    Sql,
    /// InfluxQL, the InfluxDB 1.x query language
    InfluxQL,
}

impl QueryType {
    pub fn as_str(self) -> &'static str {
        match self {
            QueryType::Sql => "sql",
            QueryType::InfluxQL => "influxql",
        }
    }
}

/// Named query parameters for parameterised SQL / InfluxQL statements.
///
/// Prefer chaining `.param("k", v)` on [`crate::QueryRequest`]; use this type
/// directly when you need to assemble parameters dynamically.
pub type QueryParameters = HashMap<String, serde_json::Value>;

/// Options controlling a single query operation.
#[derive(Debug, Clone, Default)]
pub struct QueryOptions {
    pub(crate) query_type: QueryType,
    /// Extra gRPC metadata headers sent with the Flight DoGet request.
    pub headers: HashMap<String, String>,
}

/// A dynamically typed value extracted from a query result row.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Bool(bool),
    I8(i8),
    I16(i16),
    I32(i32),
    I64(i64),
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    String(String),
    Binary(Vec<u8>),
    /// Nanosecond-epoch timestamp
    Timestamp(i64),
    Null,
}

impl Value {
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::F64(v) => Some(*v),
            Value::F32(v) => Some(*v as f64),
            Value::I64(v) => Some(*v as f64),
            Value::I32(v) => Some(*v as f64),
            Value::U64(v) => Some(*v as f64),
            Value::U32(v) => Some(*v as f64),
            _ => None,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::I64(v) => Some(*v),
            Value::I32(v) => Some(*v as i64),
            Value::I16(v) => Some(*v as i64),
            Value::I8(v) => Some(*v as i64),
            Value::Timestamp(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Bool(v) => write!(f, "{v}"),
            Value::I8(v) => write!(f, "{v}"),
            Value::I16(v) => write!(f, "{v}"),
            Value::I32(v) => write!(f, "{v}"),
            Value::I64(v) => write!(f, "{v}"),
            Value::U8(v) => write!(f, "{v}"),
            Value::U16(v) => write!(f, "{v}"),
            Value::U32(v) => write!(f, "{v}"),
            Value::U64(v) => write!(f, "{v}"),
            Value::F32(v) => write!(f, "{v}"),
            Value::F64(v) => write!(f, "{v}"),
            Value::String(v) => f.write_str(v),
            Value::Binary(v) => write!(f, "{}b", v.len()),
            Value::Timestamp(v) => write!(f, "{v}"),
            Value::Null => f.write_str("null"),
        }
    }
}

/// A single row from a query result.
///
/// Holds the raw `Vec<Value>` (one slot per column) and a shared index mapping
/// column names to slot positions.  Lookup by name is O(1) via the shared
/// `Arc<HashMap>`, so iteration allocates no per-row map.
#[derive(Debug, Clone)]
pub struct Row {
    values: Vec<Value>,
    columns: Arc<Vec<String>>,
    index: Arc<HashMap<String, usize>>,
}

impl Row {
    /// Look up a value by column name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.index.get(name).and_then(|&i| self.values.get(i))
    }

    /// Look up a value by column position.
    pub fn at(&self, idx: usize) -> Option<&Value> {
        self.values.get(idx)
    }

    /// All column names, in schema order.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// All values, in schema order.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Number of columns in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Convert to a `HashMap<String, Value>` for callers that prefer map-shaped
    /// rows.  Allocates one HashMap and clones every column name.
    pub fn into_map(self) -> HashMap<String, Value> {
        self.columns.iter().cloned().zip(self.values).collect()
    }
}

impl Index<&str> for Row {
    type Output = Value;
    fn index(&self, name: &str) -> &Value {
        self.get(name)
            .unwrap_or_else(|| panic!("no column named '{name}'"))
    }
}

impl Index<usize> for Row {
    type Output = Value;
    fn index(&self, idx: usize) -> &Value {
        &self.values[idx]
    }
}

/// The complete result of a query: a collection of Arrow [`RecordBatch`]es.
///
/// Use `for row in result` (yields [`Row`]) for row-oriented access, or
/// [`QueryResult::record_batches()`] for direct Arrow access.
pub struct QueryResult {
    pub(crate) schema: SchemaRef,
    pub(crate) batches: Vec<RecordBatch>,
}

impl QueryResult {
    pub fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        QueryResult { schema, batches }
    }

    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    /// The underlying Arrow record batches (zero-copy).
    pub fn record_batches(&self) -> &[RecordBatch] {
        &self.batches
    }

    /// Total number of rows across all batches.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }

    /// Column names in schema order.
    pub fn column_names(&self) -> Vec<&str> {
        self.schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect()
    }

    /// Collect all rows into a `Vec<Row>`.
    pub fn rows(self) -> Result<Vec<Row>, Error> {
        self.into_iter().collect()
    }

    /// Convert the query result to a polars [`DataFrame`].
    ///
    /// Requires the `polars` Cargo feature.
    ///
    /// Note: this serialises the batches to Arrow IPC and reads them back
    /// through polars, so it transiently holds roughly twice the result in
    /// memory. For very large results, prefer streaming the
    /// [`RecordBatch`]es via [`crate::Client::sql`]`(..).stream()` and
    /// converting incrementally.
    #[cfg(feature = "polars")]
    pub fn to_polars(self) -> crate::Result<polars::prelude::DataFrame> {
        use arrow::ipc::writer::FileWriter;
        use polars::io::SerReader;
        use polars::prelude::IpcReader;
        use std::io::Cursor;

        let mut buf: Vec<u8> = Vec::new();
        {
            let mut writer = FileWriter::try_new(&mut buf, &self.schema)?;
            for batch in &self.batches {
                writer.write(batch)?;
            }
            writer.finish()?;
        }

        let cursor = Cursor::new(buf);
        IpcReader::new(cursor)
            .finish()
            .map_err(|e| crate::error::Error::Config(format!("polars conversion error: {e}")))
    }
}

impl IntoIterator for QueryResult {
    type Item = Result<Row, Error>;
    type IntoIter = QueryIterator;

    fn into_iter(self) -> Self::IntoIter {
        QueryIterator::new(self.schema, self.batches)
    }
}

/// Row-by-row iterator over a [`QueryResult`].
///
/// Holds the column-name index in an `Arc` so each yielded [`Row`] can share
/// the same name-to-position map, so there is no per-row HashMap allocation.
pub struct QueryIterator {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    batch_idx: usize,
    row_idx: usize,
    columns: Arc<Vec<String>>,
    index: Arc<HashMap<String, usize>>,
}

impl QueryIterator {
    pub(crate) fn new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Self {
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let index: HashMap<String, usize> = columns
            .iter()
            .enumerate()
            .map(|(i, n)| (n.clone(), i))
            .collect();
        QueryIterator {
            schema,
            batches,
            batch_idx: 0,
            row_idx: 0,
            columns: Arc::new(columns),
            index: Arc::new(index),
        }
    }

    /// The column names, in schema order.
    pub fn column_names(&self) -> &[String] {
        &self.columns
    }

    /// Total number of rows across all batches.
    pub fn num_rows(&self) -> usize {
        self.batches.iter().map(|b| b.num_rows()).sum()
    }
}

impl Iterator for QueryIterator {
    type Item = Result<Row, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.batch_idx < self.batches.len()
            && self.row_idx >= self.batches[self.batch_idx].num_rows()
        {
            self.batch_idx += 1;
            self.row_idx = 0;
        }

        if self.batch_idx >= self.batches.len() {
            return None;
        }

        let batch = &self.batches[self.batch_idx];
        let row = self.row_idx;
        self.row_idx += 1;

        let mut values = Vec::with_capacity(batch.num_columns());
        for col_idx in 0..self.schema.fields().len() {
            let col = batch.column(col_idx);
            match extract_value(col.as_ref(), row) {
                Ok(value) => values.push(value),
                Err(err) => return Some(Err(err)),
            }
        }

        Some(Ok(Row {
            values,
            columns: Arc::clone(&self.columns),
            index: Arc::clone(&self.index),
        }))
    }
}

fn extract_value(array: &dyn Array, row: usize) -> Result<Value, Error> {
    use arrow_schema::DataType::*;

    if array.is_null(row) {
        return Ok(Value::Null);
    }

    match array.data_type() {
        Boolean => Ok(Value::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row),
        )),
        Int8 => Ok(Value::I8(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row),
        )),
        Int16 => Ok(Value::I16(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row),
        )),
        Int32 => Ok(Value::I32(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row),
        )),
        Int64 => Ok(Value::I64(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row),
        )),
        UInt8 => Ok(Value::U8(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row),
        )),
        UInt16 => Ok(Value::U16(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row),
        )),
        UInt32 => Ok(Value::U32(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row),
        )),
        UInt64 => Ok(Value::U64(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row),
        )),
        Float32 => Ok(Value::F32(
            array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row),
        )),
        Float64 => Ok(Value::F64(
            array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row),
        )),
        Utf8 => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row)
                .to_owned(),
        )),
        LargeUtf8 => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row)
                .to_owned(),
        )),
        Binary => Ok(Value::Binary(
            array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .unwrap()
                .value(row)
                .to_owned(),
        )),
        LargeBinary => Ok(Value::Binary(
            array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .unwrap()
                .value(row)
                .to_owned(),
        )),
        Timestamp(arrow_schema::TimeUnit::Nanosecond, _) => Ok(Value::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .value(row),
        )),
        Timestamp(arrow_schema::TimeUnit::Microsecond, _) => Ok(Value::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .value(row)
                * 1_000,
        )),
        Timestamp(arrow_schema::TimeUnit::Millisecond, _) => Ok(Value::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(row)
                * 1_000_000,
        )),
        Timestamp(arrow_schema::TimeUnit::Second, _) => Ok(Value::Timestamp(
            array
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap()
                .value(row)
                * 1_000_000_000,
        )),
        // Dictionary-encoded columns: InfluxDB 3 returns tag columns as
        // Dictionary(Int32, Utf8).  Resolve the key for this row and recurse
        // into the values array, so the actual tag value is returned rather
        // than a debug dump of the column.
        Dictionary(key_type, _) => {
            macro_rules! resolve {
                ($t:ty) => {{
                    let dict = array
                        .as_any()
                        .downcast_ref::<DictionaryArray<$t>>()
                        .unwrap();
                    let key = dict.keys().value(row) as usize;
                    extract_value(dict.values().as_ref(), key)
                }};
            }
            match key_type.as_ref() {
                Int8 => resolve!(Int8Type),
                Int16 => resolve!(Int16Type),
                Int32 => resolve!(Int32Type),
                Int64 => resolve!(Int64Type),
                UInt8 => resolve!(UInt8Type),
                UInt16 => resolve!(UInt16Type),
                UInt32 => resolve!(UInt32Type),
                UInt64 => resolve!(UInt64Type),
                _ => Err(Error::UnsupportedArrowType {
                    data_type: array.data_type().to_string(),
                }),
            }
        }
        // Decimals carry a scale that doesn't map onto an f64/i64 cleanly;
        // render them as their exact decimal string.
        Decimal128(_, _) => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .unwrap()
                .value_as_string(row),
        )),
        Decimal256(_, _) => Ok(Value::String(
            array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .unwrap()
                .value_as_string(row),
        )),
        _other => Err(Error::UnsupportedArrowType {
            data_type: array.data_type().to_string(),
        }),
    }
}
