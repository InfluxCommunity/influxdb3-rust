use influxdb3_client::error::LineError;
/// Write-path integration tests against a mockito HTTP server.
use influxdb3_client::{Client, ClientConfig, Error, Point, Precision};
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

struct TestCase {
    name: &'static str,
    status_code: u16,
    response_body: String,
    use_v2_api: bool,
    accept_partial: bool,
    expected_msg: String,
    expect_partial: bool,
    expected_lines: Vec<LineError>,
}

#[tokio::test]
async fn test_write_error_classification() {
    const REJECTED_LINE: &str = "home,room=Sunroom temp=\"hi\" 1735545610";
    const REJECTED_LINE_JSON: &str = r#"home,room=Sunroom temp=\"hi\" 1735545610"#;
    const LINE_ERROR: &str = "invalid column type for column 'temp', expected \
    iox::column_type::field::float, got iox::column_type::field::string";

    fn line_err(message: &str, line: Option<u64>, original_line: Option<&str>) -> LineError {
        LineError {
            message: message.to_string(),
            line,
            original_line: original_line.map(str::to_string),
        }
    }

    fn assert_line_error(actual: &LineError, expected: &LineError, test_name: &str) {
        assert_eq!(
            actual.line, expected.line,
            "test case '{test_name}': line mismatch"
        );
        assert_eq!(
            actual.message, expected.message,
            "test case '{test_name}': message mismatch"
        );
        assert_eq!(
            actual.original_line, expected.original_line,
            "test case '{test_name}': original_line mismatch"
        );
    }

    let default_rejected_body = format!(
        r#"{{"error":"write completed with rejected rows","data":[{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}]}}"#
    );
    let default_expected_msg =
        format!("write completed with rejected rows:\n\tline 2: {LINE_ERROR} ({REJECTED_LINE})");
    let default_expected_lines = vec![line_err(LINE_ERROR, Some(2), Some(REJECTED_LINE))];

    let test_cases = vec![
        TestCase {
            name: "V3 accept partial with renamed error and non-empty array",
            status_code: 400,
            response_body: format!(
                r#"{{"error":"partial write of line protocol occurred","data":[{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}]}}"#
            ),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: format!(
                "partial write of line protocol occurred:\n\tline 2: {LINE_ERROR} ({REJECTED_LINE})"
            ),
            expect_partial: true,
            expected_lines: default_expected_lines.clone(),
        },
        TestCase {
            name: "V3 accept partial without content type",
            status_code: 400,
            response_body: default_rejected_body.clone(),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: default_expected_msg.clone(),
            expect_partial: true,
            expected_lines: default_expected_lines.clone(),
        },
        TestCase {
            name: "V3 accept partial with malformed non-empty array",
            status_code: 400,
            response_body: format!(
                r#"{{"error":"write completed with rejected rows","data":[{{"line_number":"invalid","original_line":"{REJECTED_LINE_JSON}"}}]}}"#
            ),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: format!(
                "write completed with rejected rows:\n\t{{\"line_number\":\"invalid\",\"original_line\":\"{REJECTED_LINE_JSON}\"}}"
            ),
            expect_partial: true,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 accept partial with mixed primitive and typed entries",
            status_code: 400,
            response_body: format!(
                r#"{{"error":"write completed with rejected rows","data":[1,{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}]}}"#
            ),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: format!(
                "write completed with rejected rows:\n\t1\n\t{{\"error_message\":\"{LINE_ERROR}\",\"line_number\":2,\"original_line\":\"{REJECTED_LINE_JSON}\"}}"
            ),
            expect_partial: true,
            expected_lines: default_expected_lines.clone(),
        },
        TestCase {
            name: "V3 accept partial with string entries",
            status_code: 400,
            response_body: format!(
                r#"{{"error":"write completed with rejected rows","data":["{REJECTED_LINE_JSON}"]}}"#
            ),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: format!(
                "write completed with rejected rows:\n\t\"{REJECTED_LINE_JSON}\""
            ),
            expect_partial: true,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 accept partial with error message only",
            status_code: 400,
            response_body: format!(
                r#"{{"error":"write completed with rejected rows","data":[{{"error_message":"{LINE_ERROR}"}}]}}"#
            ),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: format!("write completed with rejected rows:\n\t{LINE_ERROR}"),
            expect_partial: true,
            expected_lines: vec![line_err(LINE_ERROR, None, None)],
        },
        TestCase {
            name: "V3 accept partial with line number but no original line",
            status_code: 400,
            response_body: format!(
                r#"{{"error":"write completed with rejected rows","data":[{{"error_message":"{LINE_ERROR}","line_number":2}}]}}"#
            ),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: format!("write completed with rejected rows:\n\tline 2: {LINE_ERROR}"),
            expect_partial: true,
            expected_lines: vec![line_err(LINE_ERROR, Some(2), None)],
        },
        TestCase {
            name: "V3 accept partial with entry missing error message",
            status_code: 400,
            response_body: format!(
                r#"{{"error":"write completed with rejected rows","data":[{{"line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}]}}"#
            ),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: format!(
                "write completed with rejected rows:\n\t{{\"line_number\":2,\"original_line\":\"{REJECTED_LINE_JSON}\"}}"
            ),
            expect_partial: true,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 accept partial with empty array",
            status_code: 400,
            response_body: r#"{"error":"write failed","data":[]}"#.to_string(),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: "write failed".to_string(),
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 accept partial with object details remains generic",
            status_code: 400,
            response_body: format!(
                r#"{{"error":"line protocol parsing error","data":{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}}}"#
            ),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: format!(
                "line protocol parsing error:\n\tline 2: {LINE_ERROR} ({REJECTED_LINE})"
            ),
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 reject partial with object details",
            status_code: 400,
            response_body: format!(
                r#"{{"error":"line protocol parsing error","data":{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}}}"#
            ),
            use_v2_api: false,
            accept_partial: false,
            expected_msg: format!(
                "line protocol parsing error:\n\tline 2: {LINE_ERROR} ({REJECTED_LINE})"
            ),
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 reject write with object details invalid line_number",
            status_code: 400,
            response_body: r#"{"error":"line protocol parsing error","data":{"error_message":"bad line","line_number":"aa","original_line":"home,room=Sunroom temp=\"hi\" 1735545610"}}"#.to_string(),
            use_v2_api: false,
            accept_partial: false,
            expected_msg: "line protocol parsing error:\n\tbad line".to_string(),
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "V2 never returns partial write error",
            status_code: 400,
            response_body: format!(
                r#"{{"error":"partial write of line protocol occurred","data":[{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}]}}"#
            ),
            use_v2_api: true,
            accept_partial: true,
            expected_msg: "partial write of line protocol occurred".to_string(),
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 non-400 never returns partial write error",
            status_code: 500,
            response_body: format!(
                r#"{{"error":"partial write of line protocol occurred","data":[{{"error_message":"{LINE_ERROR}","line_number":2,"original_line":"{REJECTED_LINE_JSON}"}}]}}"#
            ),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: "partial write of line protocol occurred".to_string(),
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 scalar data remains generic",
            status_code: 400,
            response_body: r#"{"error":"write failed","data":"invalid"}"#.to_string(),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: "write failed".to_string(),
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 empty object data remains generic",
            status_code: 400,
            response_body: r#"{"error":"write failed","data":{}}"#.to_string(),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: "write failed".to_string(),
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 null data remains generic",
            status_code: 400,
            response_body: r#"{"error":"write failed","data":null}"#.to_string(),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: "write failed".to_string(),
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "V3 malformed JSON preserves raw response",
            status_code: 400,
            response_body: r#"{"error":"write failed""#.to_string(),
            use_v2_api: false,
            accept_partial: true,
            expected_msg: r#"{"error":"write failed""#.to_string(),
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "Response with message at root",
            status_code: 400,
            response_body: r#"{"message": "invalid token"}"#.to_string(),
            expected_msg: "invalid token".to_string(),
            use_v2_api: false,
            accept_partial: false,
            expect_partial: false,
            expected_lines: vec![],
        },
        TestCase {
            name: "Response Html",
            status_code: 400,
            response_body: "<html><body><h1>Not found</h1></body></html>".to_string(),
            expected_msg: "<html><body><h1>Not found</h1></body></html>".to_string(),
            use_v2_api: false,
            accept_partial: false,
            expect_partial: false,
            expected_lines: vec![],
        },
    ];

    for tc in test_cases {
        let mut server = Server::new_async().await;
        let path = if tc.use_v2_api {
            "/api/v2/write"
        } else {
            "/api/v3/write_lp"
        };

        let _m = server
            .mock("POST", path)
            .match_query(Matcher::Any)
            .with_status(usize::from(tc.status_code))
            .with_body(tc.response_body)
            .expect_at_least(1)
            .create_async()
            .await;

        let client = Client::new(
            ClientConfig::builder()
                .host(server.url())
                .database("testdb")
                .token("test-token")
                .write_accept_partial(tc.accept_partial)
                .write_use_v2_api(tc.use_v2_api)
                .build()
                .unwrap(),
        )
        .await
        .unwrap();

        let err = client.write("cpu usage=1.0").await.unwrap_err();

        if tc.expect_partial {
            match err {
                Error::PartialWrite(e) => {
                    assert_eq!(e.message, tc.expected_msg, "test case: {}", tc.name);
                    assert_eq!(
                        e.line_errors.len(),
                        tc.expected_lines.len(),
                        "test case: {}",
                        tc.name
                    );
                    for (actual, expected) in e.line_errors.iter().zip(tc.expected_lines.iter()) {
                        assert_line_error(actual, expected, tc.name);
                    }
                }
                other => panic!(
                    "test '{}': expected Err::PartialWrite, got {:?}",
                    tc.name, other
                ),
            }
        } else {
            match err {
                Error::Server { code, message } => {
                    assert_eq!(message, tc.expected_msg, "test case: {}", tc.name);
                    assert_eq!(code, tc.status_code, "test case: {}", tc.name);
                }
                other => panic!("test '{}': expected Err::Server, got {:?}", tc.name, other),
            }
        }

        _m.assert_async().await;
    }
}
