//! Benchmark for the polars DataFrame → line protocol serializer.
//!
//! Run with: `cargo bench --features polars`
//!
//! The frame shape (one string tag, a block of f64 channels, a few i64
//! counters, a datetime timestamp) mirrors the bulk-backfill workload the
//! serializer exists for: wide numeric telemetry read from parquet.

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use influxdb3_client::write_dataframe::dataframe_to_line_protocol;
use influxdb3_client::Precision;
use polars::prelude::*;

const ROWS: usize = 10_000;
const F64_COLS: usize = 20;
const I64_COLS: usize = 4;

fn build_frame() -> DataFrame {
    let mut columns: Vec<Column> = Vec::new();

    let hosts: Vec<String> = (0..ROWS).map(|i| format!("host-{:03}", i % 32)).collect();
    columns.push(Column::new("host".into(), hosts));

    for c in 0..F64_COLS {
        let vals: Vec<f64> = (0..ROWS).map(|i| (i * (c + 1)) as f64 * 0.25).collect();
        columns.push(Column::new(format!("f{c}").into(), vals));
    }
    for c in 0..I64_COLS {
        let vals: Vec<i64> = (0..ROWS).map(|i| (i * (c + 7)) as i64).collect();
        columns.push(Column::new(format!("i{c}").into(), vals));
    }

    let ts: Vec<i64> = (0..ROWS)
        .map(|i| 1_700_000_000_000_000_000_i64 + i as i64 * 1_000_000)
        .collect();
    let ts = Column::new("time".into(), ts)
        .cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
        .unwrap();
    columns.push(ts);

    DataFrame::new(ROWS, columns).unwrap()
}

fn bench_serialize(c: &mut Criterion) {
    let df = build_frame();

    let mut group = c.benchmark_group("dataframe_to_line_protocol");
    group.throughput(Throughput::Elements(ROWS as u64));
    group.bench_function("wide_numeric_10k_rows", |b| {
        b.iter(|| {
            dataframe_to_line_protocol(
                std::hint::black_box(&df),
                "telemetry",
                &["host"],
                Some("time"),
                Precision::Nanosecond,
            )
            .unwrap()
        })
    });
    group.finish();
}

criterion_group!(benches, bench_serialize);
criterion_main!(benches);
