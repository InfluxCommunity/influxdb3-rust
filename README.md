# influxdb3-client

<p align="center">
    <a href="https://crates.io/crates/influxdb3-client">
        <img src="https://img.shields.io/crates/v/influxdb3-client.svg" alt="Crates.io">
    </a>
    <a href="https://docs.rs/influxdb3-client">
        <img src="https://docs.rs/influxdb3-client/badge.svg" alt="docs.rs">
    </a>
    <a href="https://github.com/InfluxCommunity/influxdb3-rust/actions/workflows/codeql-analysis.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-rust/actions/workflows/codeql-analysis.yml/badge.svg?branch=main" alt="CodeQL analysis">
    </a>
    <a href="https://github.com/InfluxCommunity/influxdb3-rust/actions/workflows/linter.yml">
        <img src="https://github.com/InfluxCommunity/influxdb3-rust/actions/workflows/linter.yml/badge.svg" alt="Lint Code Base">
    </a>
    <a href="https://dl.circleci.com/status-badge/redirect/gh/InfluxCommunity/influxdb3-rust/tree/main">
        <img src="https://dl.circleci.com/status-badge/img/gh/InfluxCommunity/influxdb3-rust/tree/main.svg?style=svg" alt="CircleCI">
    </a>
    <a href="https://codecov.io/gh/InfluxCommunity/influxdb3-rust">
        <img src="https://codecov.io/gh/InfluxCommunity/influxdb3-rust/branch/main/graph/badge.svg" alt="Code Cov">
    </a>
    <a href="https://app.slack.com/huddle/TH8RGQX5Z/C02UDUPLQKA">
        <img src="https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social" alt="Community Slack">
    </a>
</p>

An async Rust client for [InfluxDB 3](https://www.influxdata.com/) Core and Enterprise.

InfluxDB 3 is the latest generation of the InfluxDB time series engine, built on
Apache Arrow and DataFusion. **Core** is the free, single-node build for recent
data and edge workloads; **Enterprise** adds clustering, high availability, and
historical query performance on top of the same engine. Both speak the same HTTP
write API and serve queries over Arrow Flight, so this client works against
either.

- [Write data guide](https://docs.influxdata.com/influxdb3/enterprise/write-data/)
- [Downloads](https://www.influxdata.com/downloads/)

This client is part of the InfluxDB 3 client family and mirrors the feature set
of the official [Go](https://github.com/InfluxCommunity/influxdb3-go) and
[Python](https://github.com/InfluxCommunity/influxdb3-python) clients with an
idiomatic Rust API.

## Installation

Requires Rust 1.86 or later. The optional `polars` feature requires Rust 1.88 or later.

```bash
cargo add influxdb3-client
```

Or add it to `Cargo.toml`:

```toml
[dependencies]
influxdb3-client = "0.2"
tokio = { version = "1", features = ["full"] }
```

The optional `polars` feature adds DataFrame writes and query-to-DataFrame
conversion:

```toml
influxdb3-client = { version = "0.2", features = ["polars"] }
```

## Configuring a client

A client needs a host, a database, and (usually) an API token. Build the
configuration explicitly:

```rust
use influxdb3_client::{Client, ClientConfig};

#[tokio::main]
async fn main() -> influxdb3_client::Result<()> {
    let client = Client::new(
        ClientConfig::builder()
            .host("http://localhost:8181")
            .token("my-api-token")
            .database("sensors")
            .build()?,
    )
    .await?;
    Ok(())
}
```

Or read `INFLUX_HOST`, `INFLUX_TOKEN`, and `INFLUX_DATABASE` from the
environment:

```rust
let client = influxdb3_client::Client::from_env().await?;
```

Optional environment variables configure the same write defaults used by the
builder:

- `INFLUX_AUTH_SCHEME`: authentication scheme, such as `Bearer` or `Token`.
- `INFLUX_ORG`: organization name for V2 write compatibility.
- `INFLUX_PRECISION`: write precision (`ns`, `us`, `ms`, or `s`).
- `INFLUX_GZIP_THRESHOLD`: gzip write bodies larger than this many bytes.
- `INFLUX_WRITE_NO_SYNC`: skip WAL synchronization on V3 writes.
- `INFLUX_WRITE_ACCEPT_PARTIAL`: allow partial success on V3 writes.
- `INFLUX_WRITE_USE_V2_API`: use the V2 write endpoint.

Or parse a connection string:

```rust
let client = influxdb3_client::Client::from_connection_string(
    "https://cluster.example.io/?token=TOKEN&database=mydb",
).await?;
```

Connection strings support the matching query parameters: `token`, `database`,
`org`, `authScheme`, `precision`, `gzipThreshold`, `writeNoSync`,
`writeAcceptPartial`, and `writeUseV2Api`.

The Arrow Flight channel used for queries is opened lazily on the first query,
so constructing a client never blocks on query connectivity.

## Writing data

`client.write(data)` returns a builder. Chain options, then `.await`. `data` can
be a line-protocol string, a `Vec<Point>`, or a polars DataFrame (see below).

### Points

```rust
use influxdb3_client::{Point, Precision};

let points = vec![
    Point::new("temperature")
        .tag("location", "office")
        .tag("floor", "2")
        .field("celsius", 22.5_f64)
        .field("humidity", 48_i64)
        .field("occupied", true),
];

client.write(points).precision(Precision::Millisecond).await?;
```

### Raw line protocol

```rust
client
    .write("cpu,host=server01 usage_user=42.3,usage_system=1.2")
    .await?;
```

### Write options

```rust
client.write(points)
    .precision(Precision::Nanosecond)
    .batch_size(10_000)          // points per HTTP request
    .max_inflight(8)             // concurrent in-flight requests
    .default_tag("region", "us-east")
    .tag_order(["region", "host"])
    .await?;
```

Large inputs are split into batches and sent as multiple pipelined requests; one
batch buffer is held in memory at a time.

The first write defines physical tag column order, which can affect query
performance. Use `.tag_order(...)` to serialize frequently filtered tags first.
Listed tags are emitted first when present; remaining tags are appended in
deterministic lexicographic order. For background, see
[Sort tags by query priority](https://docs.influxdata.com/influxdb3/core/write-data/best-practices/optimize-writes/#sort-tags-by-query-priority).

### High-throughput ingest

For sustained, high-volume writes the throughput levers are `batch_size` (points
per request) and `max_inflight` (concurrent requests per call). On the V3
endpoint, `no_sync()` can acknowledge writes before the WAL is synced, trading
durability for speed.

A single `write` call serialises its batches on one task. To use more CPU cores
and connections, run several `write` calls concurrently. `Client` is cheap to
share, and its HTTP connection pool is reused, so wrap it in an `Arc`, spread
chunks across tasks, and cap concurrency with a semaphore to keep in-flight
buffers bounded:

```rust
use std::sync::Arc;
use tokio::sync::Semaphore;

let client = Arc::new(client);
let gate = Arc::new(Semaphore::new(8)); // cap concurrent writes

for chunk in chunks {                    // each chunk is a Vec<Point>
    let permit = gate.clone().acquire_owned().await.unwrap();
    let client = Arc::clone(&client);
    tokio::spawn(async move {
        let _permit = permit;            // released when the write completes
        client
            .write(chunk)
            .batch_size(10_000)
            .max_inflight(8)
            .no_sync() // V3 endpoint only
            .await
    });
}
```

To spread load across multiple ingest nodes, put a load balancer in front of the
cluster, or construct one `Client` per node and distribute chunks across them.
Set `max_idle_connections` to at least the total number of concurrent requests
you expect.

### Updates and deletes

Writes are idempotent at the `(series, timestamp, field)` level: writing a point
with the same measurement, tag set, and timestamp overwrites the previous field
values (last write wins). Data deletion and retention are managed at the database
level and are not exposed by this client.

## Querying data

InfluxDB 3 supports both **SQL** and **InfluxQL**. Use `client.sql(q)` or
`client.influxql(q)`; both return a query builder.

```rust
let result = client
    .sql("SELECT * FROM temperature ORDER BY time DESC LIMIT 10")
    .await?;

for row in result {
    let row = row?;
    let loc = row["location"].as_str().unwrap_or("");
    let c = row["celsius"].as_f64().unwrap_or(0.0);
    println!("{loc}: {c}");
}
```

InfluxQL is called the same way:

```rust
let result = client
    .influxql("SELECT MEAN(celsius) FROM temperature WHERE time > now() - 1h")
    .await?;
```

### Parameterised queries

```rust
let rows = client
    .sql("SELECT COUNT(*) AS n FROM cpu WHERE host = $host")
    .param("host", "server01")
    .await?
    .rows()?;

if let Some(r) = rows.first() {
    println!("count: {}", r["n"]);
}
```

### Working with rows

A `QueryResult` can be iterated row by row, collected with `.rows()`, or accessed
as raw Arrow `RecordBatch`es with `.record_batches()`. A `Row` is indexed by
column name (`row["col"]`) or position (`row[0]`), and yields a `Value` with
typed accessors (`as_f64`, `as_i64`, `as_str`, `as_bool`, `is_null`).

### Streaming large results

For results too large to hold in memory, stream the Arrow batches:

```rust
use futures_util::TryStreamExt;

let mut stream = client.sql("SELECT * FROM temperature").stream().await?;
while let Some(batch) = stream.try_next().await? {
    println!("got {} rows", batch.num_rows());
}
```

## Reliability

Transient failures are retried automatically with exponential backoff and full
jitter. Connection errors, timeouts, `429`, and `5xx` responses are retried;
`Retry-After` is honoured when present. Deterministic failures (other `4xx`, and
partial writes) are never retried. Retrying writes is safe because line-protocol
writes are idempotent.

```rust
use influxdb3_client::RetryConfig;
use std::time::Duration;

// Per-request override.
client.write(points)
    .retry(RetryConfig { max_retries: 5, base_delay: Duration::from_millis(100), ..RetryConfig::default() })
    .await?;

// Disable retries for a single call.
client.write(points).no_retry().await?;
```

Set a default policy for all requests with `ClientConfig::builder().retry(...)`.

### Partial writes

Partial writes apply when writes use the V3 `/api/v3/write_lp` endpoint
(`use_v2_api=false`). When a batch contains invalid lines, the server accepts
the valid ones and reports the rest. This surfaces as `Error::PartialWrite`,
which lists the rejected lines:

```rust
use influxdb3_client::{Client, ClientConfig, Error};

let client = Client::new(
    ClientConfig::builder()
        .host("http://localhost:8181")
        .token("token")
        .database("db")
        .write_use_v2_api(false)
        .build()?,
)
.await?;

if let Err(Error::PartialWrite(e)) = client.write(line_protocol).await {
    for line_error in &e.line_errors {
        eprintln!("line {}: {}", line_error.line, line_error.message);
    }
}
```

Set `accept_partial` to `false` in `WriteOptions` to reject the full batch when
any line fails.

### Write API compatibility

Writes use the V2 `/api/v2/write` endpoint by default for compatibility with
InfluxDB Clustered and InfluxDB Cloud Dedicated/Serverless.

Set `use_v2_api` to `false`, set `INFLUX_WRITE_USE_V2_API=false`, or use
`writeUseV2Api=false` in a connection string to send writes through the V3
endpoint. The V3 endpoint supports `accept_partial` and `no_sync`; those options
are not sent when the V2 endpoint is used.

## Polars integration

With the `polars` feature, write a DataFrame directly and read query results back
as a DataFrame.

```rust
use influxdb3_client::write_dataframe::DataFrameWrite;
use polars::prelude::*;

let df = df![
    "host"    => ["srv1", "srv2"],
    "region"  => ["us-east", "us-west"],
    "cpu_pct" => [42.5_f64, 71.0_f64],
    "time"    => [1_700_000_000_000_000_000_i64, 1_700_000_001_000_000_000_i64],
]?;

client
    .write(
        DataFrameWrite::new(&df, "server_metrics")
            .tags(&["host", "region"])
            .timestamp_column("time"),
    )
    .await?;

let df_back = client
    .sql("SELECT * FROM server_metrics")
    .await?
    .to_polars()?;
```

### Reading from a parquet or CSV file

File IO lives in your code, not the client: read the file with polars, then hand
the frame to `DataFrameWrite`. Enable the reader you need on polars in your own
`Cargo.toml`:

```toml
polars = { version = "0.53", features = ["parquet"] } # or "csv"
```

```rust
use std::fs::File;
use polars::prelude::*;

let df = ParquetReader::new(File::open("sensors.parquet")?).finish()?;

client
    .write(
        DataFrameWrite::new(&df, "sensor_data")
            .tags(&["host", "region"])
            .timestamp_column("time"),
    )
    .await?;
```

CSV works the same via `CsvReadOptions`, but its columns infer as strings unless
you supply dtypes; cast the numeric and bool columns before writing or they will
land as string fields.

## Examples

Runnable examples are in [`examples/`](examples/):

- `quickstart.rs`: end-to-end write and query.
- `cloud_dedicated.rs`: connecting to InfluxDB Cloud Dedicated.
- `write_dataframe.rs`: polars DataFrame write and read-back (requires `--features polars`).

```bash
INFLUX_HOST=http://localhost:8181 INFLUX_TOKEN=token INFLUX_DATABASE=mydb \
    cargo run --example quickstart
```

## Feedback

For bugs and feature requests, open an issue in
[InfluxCommunity/influxdb3-rust](https://github.com/InfluxCommunity/influxdb3-rust/issues).

## Contributing

Contributions are welcome. To build and check locally:

```bash
cargo build
cargo test
cargo clippy --all-targets
```

The `polars` feature is gated behind a flag, so test it separately:

```bash
cargo test --features polars
```

A few conventions to keep in mind:

- Keep `cargo clippy --all-targets` free of errors.
- The `config_tests` env-var tests mutate process environment, so run that file
  single-threaded if you see collisions: `cargo test --test config_tests -- --test-threads=1`.
- Comments and docs are ASCII only.

Please open an issue to discuss substantial changes before sending a pull
request.

## License

MIT
