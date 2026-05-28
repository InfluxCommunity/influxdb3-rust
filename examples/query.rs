//! Query example — requires a running InfluxDB 3 instance.
//!
//! ```bash
//! export INFLUX_HOST=http://localhost:8086
//! export INFLUX_TOKEN=your-token
//! export INFLUX_DATABASE=mydb
//! cargo run --example query
//! ```

use futures_util::TryStreamExt;
use influxdb3_client::Client;

#[tokio::main]
async fn main() -> influxdb3_client::Result<()> {
    let client = Client::from_env().await?;

    // SQL query — `.sql()` is sugar for `.query(q, QueryType::Sql)`.
    println!("=== SQL query ===");
    let result = client
        .sql("SELECT * FROM cpu ORDER BY time DESC LIMIT 5")
        .await?;

    println!("Schema: {:?}", result.schema().fields());
    println!("Rows:   {}", result.num_rows());

    for row in result {
        let row = row?;
        // Row::Index works by column name.
        println!("{:?}", row.values());
    }

    // InfluxQL query
    println!("\n=== InfluxQL query ===");
    let result = client
        .influxql("SELECT * FROM cpu WHERE time >= now() - 1h LIMIT 5")
        .await?;
    println!("InfluxQL returned {} rows", result.num_rows());

    // Parameterised SQL — chain .param() / .params() onto the builder.
    println!("\n=== Parameterised SQL query ===");
    let rows = client
        .sql("SELECT * FROM cpu WHERE host = $host ORDER BY time DESC LIMIT 3")
        .param("host", "server01")
        .await?
        .rows()?;
    println!("Got {} rows", rows.len());
    for row in &rows {
        if let Some(v) = row.get("usage_user") {
            println!("  usage_user = {v}");
        }
    }

    // Streaming — for huge result sets that won't fit in memory.
    println!("\n=== Streaming ===");
    let mut stream = client.sql("SELECT * FROM cpu LIMIT 100").stream().await?;
    let mut total = 0;
    while let Some(batch) = stream.try_next().await? {
        total += batch.num_rows();
    }
    println!("Streamed {total} rows");

    Ok(())
}
