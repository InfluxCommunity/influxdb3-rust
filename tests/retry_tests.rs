//! Retry behaviour against a mockito HTTP server.
use std::time::Duration;

use influxdb3_client::{Client, ClientConfig, Error, RetryConfig};
use mockito::{Matcher, Server};

async fn make_client(server: &Server) -> Client {
    Client::new(
        ClientConfig::builder()
            .host(server.url())
            .database("testdb")
            .token("test-token")
            .write_use_v2_api(false)
            .build()
            .unwrap(),
    )
    .await
    .unwrap()
}

/// Fast policy so tests don't spend real time backing off.
fn fast(max_retries: u32) -> RetryConfig {
    RetryConfig {
        max_retries,
        base_delay: Duration::from_millis(1),
        max_delay: Duration::from_millis(5),
        ..RetryConfig::default()
    }
}

#[tokio::test]
async fn recovers_after_transient_5xx() {
    // 503, 503, then 204: the write succeeds after two retries.
    let mut server = Server::new_async().await;
    let m503 = server
        .mock("POST", "/api/v3/write_lp")
        .match_query(Matcher::Any)
        .with_status(503)
        .expect(2)
        .create_async()
        .await;
    let m204 = server
        .mock("POST", "/api/v3/write_lp")
        .match_query(Matcher::Any)
        .with_status(204)
        .expect(1)
        .create_async()
        .await;

    let client = make_client(&server).await;
    client.write("cpu usage=1.0").retry(fast(3)).await.unwrap();

    m503.assert_async().await;
    m204.assert_async().await;
}

#[tokio::test]
async fn exhausts_and_surfaces_last_error() {
    // Always 503 with max_retries=2: 3 attempts, then the 503 surfaces.
    let mut server = Server::new_async().await;
    let m = server
        .mock("POST", "/api/v3/write_lp")
        .match_query(Matcher::Any)
        .with_status(503)
        .with_body(r#"{"error":"node overloaded"}"#)
        .expect(3)
        .create_async()
        .await;

    let client = make_client(&server).await;
    let err = client
        .write("cpu usage=1.0")
        .retry(fast(2))
        .await
        .unwrap_err();
    match err {
        Error::Server { code: 503, .. } => {}
        other => panic!("expected Server 503, got: {other}"),
    }
    m.assert_async().await;
}

#[tokio::test]
async fn no_retry_sends_once() {
    let mut server = Server::new_async().await;
    let m = server
        .mock("POST", "/api/v3/write_lp")
        .match_query(Matcher::Any)
        .with_status(503)
        .expect(1)
        .create_async()
        .await;

    let client = make_client(&server).await;
    let err = client.write("cpu usage=1.0").no_retry().await.unwrap_err();
    assert!(matches!(err, Error::Server { code: 503, .. }), "got: {err}");
    m.assert_async().await;
}

#[tokio::test]
async fn partial_write_is_not_retried() {
    // A 400 partial write is a deterministic data error: sent once, surfaced
    // as PartialWrite, never retried even with a generous policy.
    let mut server = Server::new_async().await;
    let body = r#"{"error":"partial write of line protocol occurred","data":[{"line_number":2,"error_message":"invalid field value","original_line":"bad line"}]}"#;
    let m = server
        .mock("POST", "/api/v3/write_lp")
        .match_query(Matcher::Any)
        .with_status(400)
        .with_body(body)
        .expect(1)
        .create_async()
        .await;

    let client = make_client(&server).await;
    let err = client
        .write("good usage=1.0\nbad line\ngood2 usage=2.0")
        .accept_partial(true)
        .retry(fast(5))
        .await
        .unwrap_err();

    match err {
        Error::PartialWrite(e) => assert_eq!(e.line_errors.len(), 1),
        other => panic!("expected PartialWrite, got: {other}"),
    }
    m.assert_async().await;
}
