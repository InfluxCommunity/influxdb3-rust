//! Connecting to InfluxDB Cloud Dedicated.
//!
//! Cloud Dedicated serves HTTP writes and Arrow Flight queries on the same host
//! over port 443. A few connection specifics differ from other deployments:
//!
//!   * Host is your cluster-ID subdomain, with the `https://` prefix:
//!     `https://CLUSTER_ID.a.influxdb.io`
//!   * Pass your bucket name as `database`.
//!   * `org` is unused (the server ignores it).
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
    // from_env reads INFLUX_HOST / INFLUX_TOKEN / INFLUX_DATABASE. Swap in
    // ClientConfig::builder() or Client::from_connection_string() to configure
    // it explicitly.
    let client = Client::from_env().await?;
    println!(
        "connected to {} (database {})",
        client.config().host_url(),
        client.config().database
    );

    client
        .write(vec![Point::new("temperature")
            .tag("location", "office")
            .field("value", 22.5_f64)])
        .await?;

    let result = client
        .sql("SELECT * FROM temperature ORDER BY time DESC LIMIT 5")
        .await?;
    for row in result {
        println!("{:?}", row?.values());
    }

    Ok(())
}
