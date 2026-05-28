//! # influxdb3-client
//!
//! Async Rust client for **InfluxDB 3 Core** and **InfluxDB 3 Enterprise**.
//!
//! Modelled after the official
//! [Go](https://github.com/InfluxCommunity/influxdb3-go) and
//! [Python](https://github.com/InfluxCommunity/influxdb3-python) clients:
//! identical feature set, idiomatic Rust API.
//!
//! ## Quick start
//!
//! ```rust,no_run
//! use influxdb3_client::{Client, ClientConfig, Point, Precision};
//!
//! #[tokio::main]
//! async fn main() -> influxdb3_client::Result<()> {
//!     let client = Client::new(
//!         ClientConfig::builder()
//!             .host("https://cluster.example.io")
//!             .token("my-api-token")
//!             .database("sensors")
//!             .build()?,
//!     ).await?;
//!
//!     // Write points — chain options, then await.
//!     let points = vec![
//!         Point::new("temperature")
//!             .tag("location", "office")
//!             .field("value", 22.5_f64)
//!             .field("humidity", 48_i64),
//!     ];
//!     client.write(points)
//!         .precision(Precision::Millisecond)
//!         .await?;
//!
//!     // Raw line protocol — low-level escape hatch
//!     client.write("cpu,host=srv1 usage=0.72").await?;
//!
//!     // Query with SQL — `.sql()` is sugar for `.query(q, QueryType::Sql)`
//!     let result = client
//!         .sql("SELECT * FROM temperature ORDER BY time DESC LIMIT 10")
//!         .await?;
//!
//!     for row in result {
//!         let row = row?;
//!         println!("{} = {}", row["location"], row["value"]);
//!     }
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Streaming millions of rows
//!
//! For results too large to materialise in memory, use `.stream()` on a query
//! builder.  The gRPC channel is consumed lazily as batches are polled:
//!
//! ```rust,no_run
//! # use influxdb3_client::Client;
//! # use futures_util::TryStreamExt;
//! # async fn example(client: &Client) -> influxdb3_client::Result<()> {
//! let mut stream = client.sql("SELECT * FROM huge_table").stream().await?;
//! while let Some(batch) = stream.try_next().await? {
//!     // batch is an Arrow RecordBatch — process columns directly
//!     println!("got {} rows", batch.num_rows());
//! }
//! # Ok(()) }
//! ```
//!
//! ## High-throughput writes
//!
//! For sustained ingest (flight-test telemetry, IIoT4.0 PLC streams), tune the
//! batch size and inflight window:
//!
//! ```rust,no_run
//! # use influxdb3_client::{Client, Point};
//! # async fn example(client: &Client, points: Vec<Point>) -> influxdb3_client::Result<()> {
//! client.write(points)
//!     .batch_size(10_000)
//!     .max_inflight(8)
//!     .no_sync()              // skip WAL sync — higher throughput
//!     .await?;
//! # Ok(()) }
//! ```

pub mod client;
pub mod config;
pub mod error;
pub mod flight;
pub mod point;
pub mod precision;
pub mod query;
pub mod write;

#[cfg(feature = "polars")]
pub mod write_dataframe;


pub use client::{Client, QueryRequest, WriteRequest};
pub use config::{ClientConfig, ClientConfigBuilder};
pub use error::{Error, PartialWriteError};
pub use flight::BatchStream;
pub use point::{FieldValue, Point};
pub use precision::Precision;
pub use query::{
    QueryIterator, QueryParameters, QueryResult, QueryType, Row, Value,
};
pub use write::{
    WriteInput, WriteOptions, WriteOptionsBuilder,
    DEFAULT_BATCH_SIZE, DEFAULT_MAX_INFLIGHT,
};

/// Convenience alias for `std::result::Result<T, Error>`.
pub type Result<T> = std::result::Result<T, Error>;
