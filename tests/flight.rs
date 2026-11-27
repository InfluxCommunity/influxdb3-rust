mod test_utils;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use arrow_flight::flight_service_server::FlightServiceServer;
use influxdb3_client::{Client, ClientConfig, RetryConfig};
use test_utils::*;
use tokio::{net::TcpListener, sync::oneshot};
use tonic::transport::{server::TcpIncoming, Server};

fn fast_retry(max_retries: u32) -> RetryConfig {
    RetryConfig {
        max_retries,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(1),
        ..RetryConfig::default()
    }
}

#[tokio::test]
async fn query_stream_sends_metadata_headers(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let metadata = Arc::new(Mutex::new(None));
    let service = CapturingFlightService::new(
        Arc::clone(&metadata),
        Arc::new(AtomicUsize::new(0)),
        Arc::new(AtomicUsize::new(0)),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpIncoming::from(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = Client::new(
        ClientConfig::builder()
            .host(format!("http://{addr}"))
            .token("TEST_TOKEN")
            .database("db")
            .query_timeout(Duration::from_secs(5))
            .build()?,
    )
    .await?;

    let stream = client
        .sql("SELECT * FROM test")
        .header("X-Tracing-Id", "123")
        .stream()
        .await?;
    drop(stream);

    let captured = metadata.lock().unwrap().clone().unwrap();
    assert_eq!(
        captured.get("authorization").unwrap().to_str().unwrap(),
        "Bearer TEST_TOKEN"
    );
    assert_eq!(
        captured.get("x-tracing-id").unwrap().to_str().unwrap(),
        "123"
    );

    shutdown_tx.send(()).unwrap();
    server.await??;

    Ok(())
}

#[tokio::test]
async fn query_stream_retries_transient_flight_errors(
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let do_get_calls = Arc::new(AtomicUsize::new(0));
    let service = CapturingFlightService::new(
        Arc::new(Mutex::new(None)),
        Arc::clone(&do_get_calls),
        Arc::new(AtomicUsize::new(1)),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let incoming = TcpIncoming::from(listener);
    let (shutdown_tx, shutdown_rx) = oneshot::channel();

    let server = tokio::spawn(async move {
        Server::builder()
            .add_service(FlightServiceServer::new(service))
            .serve_with_incoming_shutdown(incoming, async {
                let _ = shutdown_rx.await;
            })
            .await
    });

    let client = Client::new(
        ClientConfig::builder()
            .host(format!("http://{addr}"))
            .token("TEST_TOKEN")
            .database("db")
            .query_timeout(Duration::from_secs(5))
            .build()?,
    )
    .await?;

    let stream = client
        .sql("SELECT * FROM test")
        .retry(fast_retry(1))
        .stream()
        .await?;
    drop(stream);

    assert_eq!(do_get_calls.load(Ordering::SeqCst), 2);

    shutdown_tx.send(()).unwrap();
    server.await??;

    Ok(())
}

#[tokio::test]
async fn query_stream_works_over_tls() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = rustls::crypto::ring::default_provider().install_default(); // pick one provider explicitly

    let do_get_calls = Arc::new(AtomicUsize::new(0));
    let service = CapturingFlightService::new(
        Arc::new(Mutex::new(None)),
        Arc::clone(&do_get_calls),
        Arc::new(AtomicUsize::new(0)),
    );

    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let incoming = TcpIncoming::from(listener);
    let generated_certs = generate_certs();
    let server = start_secure_query_server(
        &generated_certs.server_cert_path,
        &generated_certs.server_key_path,
        &generated_certs.ca_cert_path,
        incoming,
        service,
        false,
    );

    let client = Client::new(
        ClientConfig::builder()
            .host(format!("https://localhost:{port}"))
            .token("TEST_TOKEN")
            .database("db")
            .ssl_roots_path(&generated_certs.ca_cert_path)
            .query_timeout(Duration::from_secs(5))
            .build()?,
    )
    .await?;

    // A successful query proves the TLS handshake works end-to-end: tonic's
    // `tls-ring` provider negotiated the connection and trusted the CA cert.
    let stream = client.sql("SELECT * FROM test").stream().await?;
    drop(stream);
    assert_eq!(do_get_calls.load(Ordering::SeqCst), 1);

    server
        .shutdown()
        .await
        .expect("Shutdown Flight server failed");
    generated_certs.remove_temp_dirs();
    Ok(())
}
