/// QueryResult / QueryIterator / Value tests using in-memory Arrow batches.
use std::sync::Arc;

use arrow_array::{
    BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray, TimestampNanosecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use influxdb3_client::query::{extract_value, QueryResult, Value};

fn make_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new(
            "time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("host", DataType::Utf8, false),
        Field::new("usage", DataType::Float64, false),
        Field::new("count", DataType::Int64, false),
        Field::new("active", DataType::Boolean, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![
                1_700_000_000_000_000_000_i64,
                1_700_000_001_000_000_000_i64,
            ])),
            Arc::new(StringArray::from(vec!["srv1", "srv2"])),
            Arc::new(Float64Array::from(vec![42.5, 13.7])),
            Arc::new(Int64Array::from(vec![100_i64, 200_i64])),
            Arc::new(BooleanArray::from(vec![true, false])),
        ],
    )
    .unwrap()
}

#[test]
fn iteration() {
    // Covers: IntoIterator, multi-batch traversal, Row indexing by name,
    // empty result, and Row::into_map roundtrip.
    let batch = make_batch();
    let schema = batch.schema();
    let rows: Vec<_> = QueryResult::new(schema.clone(), vec![batch.clone(), batch.clone()])
        .into_iter()
        .map(Result::unwrap)
        .collect();
    assert_eq!(rows.len(), 4);

    let r = &rows[0];
    assert_eq!(r["host"], Value::String("srv1".into()));
    assert_eq!(r["usage"], Value::F64(42.5));
    assert_eq!(r["count"], Value::I64(100));
    assert_eq!(r["active"], Value::Bool(true));
    assert!(matches!(r["time"], Value::Timestamp(t) if t == 1_700_000_000_000_000_000));

    // Into-map roundtrip
    let m = r.clone().into_map();
    assert_eq!(m["usage"], Value::F64(42.5));

    // Empty result
    let empty = QueryResult::new(schema, vec![]);
    assert_eq!(empty.into_iter().count(), 0);
}

#[test]
fn value_api() {
    // Type extraction across Arrow array types.
    assert_eq!(
        extract_value(&Int64Array::from(vec![None as Option<i64>]), 0),
        Value::Null
    );
    assert_eq!(
        extract_value(&Float64Array::from(vec![2.5]), 0),
        Value::F64(2.5)
    );
    assert_eq!(
        extract_value(&StringArray::from(vec!["x"]), 0),
        Value::String("x".into())
    );
    assert_eq!(
        extract_value(&BooleanArray::from(vec![true]), 0),
        Value::Bool(true)
    );
    assert_eq!(
        extract_value(
            &TimestampNanosecondArray::from(vec![1_700_000_000_000_000_000_i64]),
            0
        ),
        Value::Timestamp(1_700_000_000_000_000_000),
    );

    // Coercion helpers
    assert_eq!(Value::I64(42).as_f64(), Some(42.0));
    assert_eq!(Value::I32(42).as_i64(), Some(42));
    assert_eq!(Value::Timestamp(123).as_i64(), Some(123));
    assert_eq!(Value::String("hi".into()).as_str(), Some("hi"));
    assert!(Value::Null.is_null());

    // Display
    assert_eq!(format!("{}", Value::I64(42)), "42");
    assert_eq!(format!("{}", Value::String("hi".into())), "hi");
    assert_eq!(format!("{}", Value::Null), "null");

    // InfluxDB 3 returns tag columns as Dictionary(Int32, Utf8); the row value
    // must be the underlying string, not a debug dump of the column.
    use arrow_array::DictionaryArray;
    let dict: DictionaryArray<arrow_array::types::Int32Type> =
        vec!["us-east", "us-west", "us-east"].into_iter().collect();
    assert_eq!(extract_value(&dict, 0), Value::String("us-east".into()));
    assert_eq!(extract_value(&dict, 1), Value::String("us-west".into()));
    assert_eq!(extract_value(&dict, 2), Value::String("us-east".into()));
}
