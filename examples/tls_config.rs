//! Config TLS/mTLS for InfluxDB client.

use influxdb3_client::{Client, ClientConfig};

#[tokio::main]
async fn main() -> influxdb3_client::Result<()> {
    let mut builder = ClientConfig::builder()
        .host("https://localhost:443".to_string())
        .token("your-database-token")
        .database("your-bucket-name");

    // Config server root certificate.
    builder = builder.ssl_roots_path("path/to/your/root/ca.pem");

    // Optional. Config client certificate when working with mTLS.
    // builder = builder.client_cert_path("path/to/your/client/cert.crt");
    // builder = builder.client_key_path("path/to/your/client/cert-key.key");

    // If your certificate and key are in the same file, set client_cert_path and client_key_path
    // with the same path.
    // builder = builder.client_cert_path("path/to/your/client/cert.pem");
    // builder = builder.client_key_path("path/to/your/client/cert.pem");

    let client = Client::new(builder.build()?).await?;
    let result = client
        .sql("SELECT * FROM temperature ORDER BY time DESC LIMIT 5")
        .await?;
    for row in result {
        println!("{:?}", row?.values());
    }

    Ok(())
}
