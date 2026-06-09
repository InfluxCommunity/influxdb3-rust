/// QueryResult / QueryIterator / Value tests using in-memory Arrow batches.
use std::sync::Arc;

use arrow_array::types::{UInt16Type, UInt32Type, UInt64Type};
use arrow_array::{
    ArrayRef, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Decimal256Array,
    DictionaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    LargeBinaryArray, LargeStringArray, RecordBatch, StringArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use influxdb3_client::{query::Value, Error, QueryResult};

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

fn extract_values(array: ArrayRef) -> Result<Vec<Value>, Error> {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        array.data_type().clone(),
        true,
    )]));
    let batch = RecordBatch::try_new(Arc::clone(&schema), vec![array]).unwrap();
    QueryResult::new(schema, vec![batch]).rows().map(|rows| {
        rows.into_iter()
            .map(|row| row.at(0).unwrap().clone())
            .collect()
    })
}

fn extract_first_value(array: ArrayRef) -> Result<Value, Error> {
    Ok(extract_values(array)?.remove(0))
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
    let cases: Vec<(&str, ArrayRef, Value)> = vec![
        (
            "null",
            Arc::new(Int64Array::from(vec![None as Option<i64>])),
            Value::Null,
        ),
        (
            "bool",
            Arc::new(BooleanArray::from(vec![true])),
            Value::Bool(true),
        ),
        ("int8", Arc::new(Int8Array::from(vec![1_i8])), Value::I8(1)),
        (
            "int16",
            Arc::new(Int16Array::from(vec![2_i16])),
            Value::I16(2),
        ),
        (
            "int32",
            Arc::new(Int32Array::from(vec![3_i32])),
            Value::I32(3),
        ),
        (
            "int64",
            Arc::new(Int64Array::from(vec![4_i64])),
            Value::I64(4),
        ),
        (
            "uint8",
            Arc::new(UInt8Array::from(vec![5_u8])),
            Value::U8(5),
        ),
        (
            "uint16",
            Arc::new(UInt16Array::from(vec![6_u16])),
            Value::U16(6),
        ),
        (
            "uint32",
            Arc::new(UInt32Array::from(vec![7_u32])),
            Value::U32(7),
        ),
        (
            "uint64",
            Arc::new(UInt64Array::from(vec![8_u64])),
            Value::U64(8),
        ),
        (
            "float32",
            Arc::new(Float32Array::from(vec![1.5_f32])),
            Value::F32(1.5),
        ),
        (
            "float64",
            Arc::new(Float64Array::from(vec![2.5_f64])),
            Value::F64(2.5),
        ),
        (
            "utf8",
            Arc::new(StringArray::from(vec!["x"])),
            Value::String("x".into()),
        ),
        (
            "large_utf8",
            Arc::new(LargeStringArray::from(vec!["large"])),
            Value::String("large".into()),
        ),
        (
            "binary",
            Arc::new(BinaryArray::from_vec(vec![b"payload".as_slice()])),
            Value::Binary(b"payload".to_vec()),
        ),
        (
            "large_binary",
            Arc::new(LargeBinaryArray::from_vec(vec![b"payload".as_slice()])),
            Value::Binary(b"payload".to_vec()),
        ),
        (
            "timestamp_s",
            Arc::new(TimestampSecondArray::from(vec![1_i64])),
            Value::Timestamp(1_000_000_000),
        ),
        (
            "timestamp_ms",
            Arc::new(TimestampMillisecondArray::from(vec![1_i64])),
            Value::Timestamp(1_000_000),
        ),
        (
            "timestamp_us",
            Arc::new(TimestampMicrosecondArray::from(vec![1_i64])),
            Value::Timestamp(1_000),
        ),
        (
            "timestamp_ns",
            Arc::new(TimestampNanosecondArray::from(vec![1_i64])),
            Value::Timestamp(1),
        ),
    ];

    for (name, array, expected) in cases {
        assert_eq!(extract_first_value(array).unwrap(), expected, "{name}");
    }

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
    let dict: DictionaryArray<arrow_array::types::Int32Type> =
        vec!["us-east", "us-west", "us-east"].into_iter().collect();
    assert_eq!(
        extract_values(Arc::new(dict)).unwrap(),
        vec![
            Value::String("us-east".into()),
            Value::String("us-west".into()),
            Value::String("us-east".into())
        ]
    );

    let dict_u16: DictionaryArray<UInt16Type> =
        vec!["alpha", "beta", "alpha"].into_iter().collect();
    assert_eq!(
        extract_values(Arc::new(dict_u16)).unwrap(),
        vec![
            Value::String("alpha".into()),
            Value::String("beta".into()),
            Value::String("alpha".into())
        ]
    );

    let dict_u32: DictionaryArray<UInt32Type> =
        vec!["alpha", "beta", "alpha"].into_iter().collect();
    assert_eq!(
        extract_values(Arc::new(dict_u32)).unwrap(),
        vec![
            Value::String("alpha".into()),
            Value::String("beta".into()),
            Value::String("alpha".into())
        ]
    );

    let dict_u64: DictionaryArray<UInt64Type> =
        vec!["alpha", "beta", "alpha"].into_iter().collect();
    assert_eq!(
        extract_values(Arc::new(dict_u64)).unwrap(),
        vec![
            Value::String("alpha".into()),
            Value::String("beta".into()),
            Value::String("alpha".into())
        ]
    );

    let decimal128 = Decimal128Array::from(vec![12345])
        .with_precision_and_scale(10, 2)
        .unwrap();
    assert_eq!(
        extract_first_value(Arc::new(decimal128)).unwrap(),
        Value::String("123.45".into())
    );

    let decimal256 = Decimal256Array::from(vec![12345_i32.into()])
        .with_precision_and_scale(10, 2)
        .unwrap();
    assert_eq!(
        extract_first_value(Arc::new(decimal256)).unwrap(),
        Value::String("123.45".into())
    );

    let err = extract_first_value(Arc::new(Date32Array::from(vec![1_i32]))).unwrap_err();
    assert!(matches!(err, Error::UnsupportedArrowType { .. }));
}
