/// Write-path integration tests against a mockito HTTP server.
use influxdb3_client::{Client, ClientConfig, Point, Precision};
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

#[tokio::test]
async fn lp_string_with_overrides() {
    // Covers: V3 endpoint, db param, auth header, content-type,
    // precision + no_sync overrides reaching the URL.
    let mut server = Server::new_async().await;
    let _m = server
        .mock("POST", "/api/v3/write_lp")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("db".into(), "testdb".into()),
            Matcher::UrlEncoded("precision".into(), "millisecond".into()),
            Matcher::UrlEncoded("no_sync".into(), "true".into()),
        ]))
        .match_header("Authorization", "Bearer test-token")
        .match_header("Content-Type", Matcher::Regex("text/plain.*".into()))
        .with_status(204)
        .create_async()
        .await;

    let client = make_client(&server).await;
    client
        .write("cpu usage=1.0")
        .precision(Precision::Millisecond)
        .no_sync()
        .await
        .unwrap();
    _m.assert_async().await;
}

#[tokio::test]
async fn v2_write_uses_bucket_query_parameter() {
    let mut server = Server::new_async().await;
    let m = server
        .mock("POST", "/api/v2/write")
        .match_query(Matcher::AllOf(vec![
            Matcher::UrlEncoded("bucket".into(), "testdb".into()),
            Matcher::UrlEncoded("precision".into(), "nanosecond".into()),
        ]))
        .match_header("Authorization", "Bearer test-token")
        .match_header("Content-Type", Matcher::Regex("text/plain.*".into()))
        .with_status(204)
        .create_async()
        .await;

    let client = Client::new(
        ClientConfig::builder()
            .host(server.url())
            .database("testdb")
            .token("test-token")
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    client.write("cpu usage=1.0").await.unwrap();
    m.assert_async().await;
}

#[tokio::test]
async fn no_sync_requires_v3_endpoint() {
    let server = Server::new_async().await;
    let client = Client::new(
        ClientConfig::builder()
            .host(server.url())
            .database("testdb")
            .token("test-token")
            .build()
            .unwrap(),
    )
    .await
    .unwrap();

    let err = client.write("cpu usage=1.0").no_sync().await.unwrap_err();
    assert!(
        err.to_string()
            .contains("no_sync requires use_v2_api=false"),
        "got: {err}"
    );
}

#[tokio::test]
async fn points_batch_splitting() {
    // 5 points at batch_size=2 means 3 sequential requests.
    let mut server = Server::new_async().await;
    let m = server
        .mock("POST", "/api/v3/write_lp")
        .match_query(Matcher::Any)
        .with_status(204)
        .expect(3)
        .create_async()
        .await;

    let client = make_client(&server).await;
    let points: Vec<Point> = (0..5)
        .map(|i| {
            Point::new("cpu")
                .tag("h", format!("s{i}"))
                .field("v", i as f64)
        })
        .collect();
    client
        .write(points)
        .batch_size(2)
        .max_inflight(1)
        .await
        .unwrap();
    m.assert_async().await;
}

#[tokio::test]
async fn default_tags_and_order_reach_the_wire() {
    // default tags merge in (point wins on conflict); explicit tag_order is
    // honoured with leftover tags appended alphabetically.
    let mut server = Server::new_async().await;
    let m = server
        .mock("POST", "/api/v3/write_lp")
        .match_query(Matcher::Any)
        .match_body("m,host=override,z=1,a=2,env=prod v=1i")
        .with_status(204)
        .create_async()
        .await;

    let client = make_client(&server).await;
    let point = Point::new("m")
        .tag("host", "override")
        .tag("z", "1")
        .tag("a", "2")
        .field("v", 1_i64);
    client
        .write(vec![point])
        .default_tag("env", "prod")
        .default_tag("host", "default")
        .tag_order(["host", "z"])
        .await
        .unwrap();
    m.assert_async().await;
}

#[tokio::test]
async fn non_retryable_error_surfaces_once() {
    // A 404 is deterministic, so it surfaces immediately without retrying.
    // (Transient 5xx/retry behaviour is covered in retry_tests.rs.)
    let mut server = Server::new_async().await;
    let m = server
        .mock("POST", "/api/v3/write_lp")
        .match_query(Matcher::Any)
        .with_status(404)
        .with_body(r#"{"error":"database not found"}"#)
        .expect(1)
        .create_async()
        .await;

    let client = make_client(&server).await;
    let err = client.write("bad").await.unwrap_err().to_string();
    assert!(
        err.contains("404") || err.contains("server error"),
        "got: {err}"
    );
    m.assert_async().await;
}

#[tokio::test]
async fn empty_point_pre_flight_error() {
    // Pre-flight validation; no HTTP request made.
    let server = Server::new_async().await;
    let client = make_client(&server).await;
    let err = client
        .write(vec![Point::new("x").tag("k", "v")])
        .await
        .unwrap_err();
    assert!(err.to_string().contains("no fields"), "got: {err}");
}
