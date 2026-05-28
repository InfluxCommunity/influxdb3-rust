/// Write-path integration tests against a mockito HTTP server.
use influxdb3_client::{Client, ClientConfig, Point, Precision};
use mockito::{Matcher, Server};

async fn make_client(server: &Server) -> Client {
    Client::new(
        ClientConfig::builder()
            .host(server.url())
            .database("testdb")
            .token("test-token")
            .build()
            .unwrap(),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn lp_string_with_overrides() {
    // Covers: v3 endpoint, db param, auth header, content-type,
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
    client.write("cpu usage=1.0")
        .precision(Precision::Millisecond)
        .no_sync()
        .await
        .unwrap();
    _m.assert_async().await;
}

#[tokio::test]
async fn points_batch_splitting() {
    // 5 points at batch_size=2 → 3 sequential requests.
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
        .map(|i| Point::new("cpu").tag("h", format!("s{i}")).field("v", i as f64))
        .collect();
    client.write(points).batch_size(2).max_inflight(1).await.unwrap();
    m.assert_async().await;
}

#[tokio::test]
async fn server_error_surfaces() {
    let mut server = Server::new_async().await;
    let _m = server
        .mock("POST", "/api/v3/write_lp")
        .match_query(Matcher::Any)
        .with_status(500)
        .with_body(r#"{"error":"internal"}"#)
        .create_async()
        .await;

    let client = make_client(&server).await;
    let err = client.write("bad").await.unwrap_err().to_string();
    assert!(err.contains("500") || err.contains("server error"), "got: {err}");
}

#[tokio::test]
async fn empty_point_pre_flight_error() {
    // Pre-flight validation — no HTTP request made.
    let server = Server::new_async().await;
    let client = make_client(&server).await;
    let err = client.write(vec![Point::new("x").tag("k", "v")]).await.unwrap_err();
    assert!(err.to_string().contains("no fields"), "got: {err}");
}
