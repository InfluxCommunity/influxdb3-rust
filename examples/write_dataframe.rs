//! Polars DataFrame write + query-to-DataFrame example.
//!
//! Demonstrates:
//!  1. Writing a polars DataFrame via `client.write(DataFrameWrite::new(...))`.
//!  2. Reading the data back with `client.sql()` and converting to a DataFrame
//!     via `QueryResult::to_polars()`.
//!
//! **Requires** the `polars` Cargo feature:
//!
//! ```bash
//! export INFLUX_HOST=http://localhost:8086
//! export INFLUX_TOKEN=your-token
//! export INFLUX_DATABASE=mydb
//! cargo run --example write_dataframe --features polars
//! ```
//!
//! ## Ingesting a parquet or CSV file
//!
//! File IO lives in your code, not the client: read the file with polars, then
//! hand the frame to `DataFrameWrite`. Enable the reader you need on polars in
//! your own Cargo.toml (`features = ["parquet"]` or `["csv"]`):
//!
//! ```ignore
//! use std::fs::File;
//! use polars::prelude::*;
//!
//! let df = ParquetReader::new(File::open("sensors.parquet")?).finish()?;
//! client
//!     .write(
//!         DataFrameWrite::new(&df, "sensor_data")
//!             .tags(&["host", "region"])
//!             .timestamp_column("time"),
//!     )
//!     .await?;
//! ```
//!
//! CSV works the same via `CsvReadOptions`, but its columns infer as strings
//! unless you supply dtypes; cast the numeric and bool columns before writing
//! or they will land as string fields.

use influxdb3_client::write_dataframe::DataFrameWrite;
use influxdb3_client::Client;
use polars::prelude::*;

#[tokio::main]
async fn main() -> influxdb3_client::Result<()> {
    let client = Client::from_env().await?;

    let df = df![
        "host"    => ["srv1", "srv2", "srv3"],
        "region"  => ["us-east", "us-west", "eu-west"],
        "cpu_pct" => [42.5_f64, 71.0_f64, 55.3_f64],
        "mem_mb"  => [8192_i64, 16384_i64, 4096_i64],
        "time_ns" => [
            1_700_000_000_000_000_000_i64,
            1_700_000_001_000_000_000_i64,
            1_700_000_002_000_000_000_i64,
        ],
    ]
    .unwrap();

    println!("=== DataFrame to write ===\n{df}\n");

    client
        .write(
            DataFrameWrite::new(&df, "server_metrics")
                .tags(&["host", "region"])
                .timestamp_column("time_ns"),
        )
        .await?;

    println!("Wrote {} rows to 'server_metrics'", df.height());

    let result = client
        .sql("SELECT host, region, cpu_pct, mem_mb FROM server_metrics ORDER BY time")
        .await?;

    let df_back = result.to_polars()?;
    println!("\n=== Data read back as polars DataFrame ===\n{df_back}");

    Ok(())
}
