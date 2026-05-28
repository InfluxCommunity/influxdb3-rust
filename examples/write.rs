//! Write example — requires a running InfluxDB 3 instance.
//!
//! ```bash
//! export INFLUX_HOST=http://localhost:8086
//! export INFLUX_TOKEN=your-token
//! export INFLUX_DATABASE=mydb
//! cargo run --example write
//! ```

use influxdb3_client::{Client, Point, Precision};

#[tokio::main]
async fn main() -> influxdb3_client::Result<()> {
    let client = Client::from_env().await?;

    // Raw line protocol — low-level escape hatch
    println!("Writing raw line protocol…");
    client
        .write("cpu,host=server01,region=us-east usage_user=42.3,usage_system=1.2 1700000000000000000")
        .await?;

    // Point builder
    println!("Writing via Point builder…");
    let point = Point::new("temperature")
        .tag("location", "office")
        .tag("floor", "2")
        .field("celsius", 22.5_f64)
        .field("humidity", 48_i64)
        .field("occupied", true);

    client.write(vec![point]).await?;

    // Batch with option overrides — chain options before `.await`.
    println!("Batch writing with millisecond precision…");
    use chrono::Utc;
    let now_ms = Utc::now().timestamp_millis();

    let points = vec![
        Point::new("sensor")
            .tag("id", "S1")
            .field("value", 1.0_f64)
            .timestamp_nanos(now_ms * 1_000_000),
        Point::new("sensor")
            .tag("id", "S2")
            .field("value", 2.0_f64)
            .timestamp_nanos(now_ms * 1_000_000),
    ];

    client
        .write(points)
        .precision(Precision::Millisecond)
        .await?;

    println!("All writes succeeded!");
    Ok(())
}
