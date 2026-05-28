//! Connecting to InfluxDB Cloud Dedicated.
//!
//! Cloud Dedicated serves both HTTP writes and Arrow Flight queries on the
//! same host over port 443.  The host is your cluster ID subdomain:
//!
//! ```text
//! https://CLUSTER_ID.a.influxdb.io
//! ```
//!
//! Pass your bucket name as `database`.  The `org` parameter is not used by
//! Cloud Dedicated — the server ignores it.
//!
//! ```bash
//! export INFLUX_HOST=https://CLUSTER_ID.a.influxdb.io
//! export INFLUX_TOKEN=your-database-token
//! export INFLUX_DATABASE=your-bucket-name
//! cargo run --example cloud_dedicated
//! ```

use influxdb3_client::{Client, Point};

#[tokio::main]
async fn main() -> influxdb3_client::Result<()> {
    // Reads INFLUX_HOST, INFLUX_TOKEN, INFLUX_DATABASE from the environment.
    // Set them before running, or swap in Client::from_connection_string() /
    // ClientConfig::builder() if you prefer explicit configuration.
    let client = Client::from_env().await?;

    println!("Connected to {}", client.config().host_url());
    println!("Database:    {}", client.config().database);

    // Write a point
    let point = Point::new("temperature")
        .tag("location", "office")
        .field("value", 22.5_f64);

    client.write(vec![point]).await?;
    println!("Write OK");

    // Query it back
    let result = client
        .sql("SELECT * FROM temperature ORDER BY time DESC LIMIT 5")
        .await?;

    for row in result {
        let row = row?;
        println!("{:?}", row.values());
    }

    Ok(())
}
