//! End-to-end write and query against a running InfluxDB 3 instance.
//!
//! ```bash
//! export INFLUX_HOST=http://localhost:8081
//! export INFLUX_TOKEN=your-token
//! export INFLUX_DATABASE=mydb
//! cargo run --example quickstart
//! ```

use futures_util::TryStreamExt;
use influxdb3_client::{Client, Point, Precision};

#[tokio::main]
async fn main() -> influxdb3_client::Result<()> {
    let client = Client::from_env().await?;

    // Write points built with the Point API.
    let points = vec![Point::new("temperature")
        .tag("location", "office")
        .tag("floor", "2")
        .field("celsius", 22.5_f64)
        .field("humidity", 48_i64)
        .field("occupied", true)];
    client
        .write(points)
        .precision(Precision::Millisecond)
        .await?;

    // Write raw line protocol when you already have it.
    client
        .write("cpu,host=server01 usage_user=42.3,usage_system=1.2")
        .await?;
    println!("writes ok");

    // SQL query. `.sql()` is shorthand for `.query(q, QueryType::Sql)`.
    let result = client
        .sql("SELECT * FROM temperature ORDER BY time DESC LIMIT 5")
        .await?;
    for row in result {
        let row = row?;
        println!("{:?}", row.values());
    }

    // Parameterised SQL.
    let rows = client
        .sql("SELECT COUNT(*) AS n FROM cpu WHERE host = $host")
        .param("host", "server01")
        .await?
        .rows()?;
    if let Some(r) = rows.first() {
        println!("cpu rows for server01: {}", r["n"]);
    }

    // InfluxQL is also supported.
    let influx = client
        .influxql("SELECT MEAN(celsius) FROM temperature")
        .await?;
    println!("influxql returned {} rows", influx.num_rows());

    // Stream results that are too large to hold in memory.
    let mut stream = client.sql("SELECT * FROM temperature").stream().await?;
    let mut total = 0;
    while let Some(batch) = stream.try_next().await? {
        total += batch.num_rows();
    }
    println!("streamed {total} rows");

    Ok(())
}
