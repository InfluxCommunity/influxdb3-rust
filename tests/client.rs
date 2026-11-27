mod test_utils;

use influxdb3_client::{Client, ClientConfig, Error, Point, Precision, Row, Value, WriteOptions};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use test_utils::{
    error_chain, generate_certs, start_secure_query_server, start_secure_write_server,
    CapturingFlightService, TestCerts,
};
use tokio::net::TcpListener;
use tonic::transport::server::TcpIncoming;

const MEASUREMENT: &str = "rust_e2e";
const LOCATION: &str = "sun-valley-1";

fn testing_config() -> Option<ClientConfig> {
    let host = required_env("TESTING_INFLUXDB_URL")?;
    let token = required_env("TESTING_INFLUXDB_TOKEN")?;
    let database = required_env("TESTING_INFLUXDB_DATABASE")?;

    Some(
        ClientConfig::builder()
            .host(host)
            .token(token)
            .database(database)
            .build()
            .expect("TESTING_INFLUXDB_* values should build a valid client config"),
    )
}

fn required_env(name: &str) -> Option<String> {
    let value = std::env::var(name).ok()?;
    (!value.is_empty()).then_some(value)
}

#[tokio::test]
async fn write_and_query_data() -> Result<(), Box<dyn std::error::Error>> {
    let Some(config) = testing_config() else {
        eprintln!("skipping e2e test: TESTING_INFLUXDB_* env vars are not set");
        return Ok(());
    };

    let client = Client::new(config).await?;
    let test_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i64;

    let point = Point::new(MEASUREMENT)
        .tag("location", LOCATION)
        .field("temp", 15.5_f64)
        .field("index", 80_i64)
        .field("uindex", 800_u64)
        .field("valid", true)
        .field("testId", test_id)
        .field("text", "a1")
        .timestamp_nanos(test_id);

    client.write(vec![point]).await?;

    let row = query_written_point(&client, test_id)
        .await?
        .unwrap_or_else(|| panic!("expected to query back point with test_id={test_id}"));

    assert_eq!(row["location"].as_str(), Some(LOCATION));
    assert_eq!(row["temp"].as_f64(), Some(15.5));
    assert_eq!(row["index"].as_i64(), Some(80));
    assert_eq!(row["uindex"], Value::U64(800));
    assert_eq!(row["valid"].as_bool(), Some(true));
    assert_eq!(row["testId"].as_i64(), Some(test_id));
    assert_eq!(row["text"].as_str(), Some("a1"));
    assert_eq!(row["time"], Value::Timestamp(test_id));

    Ok(())
}

#[tokio::test]
async fn write_error_api_v2() -> Result<(), Box<dyn std::error::Error>> {
    let Some(config) = testing_config() else {
        eprintln!("skipping e2e test: TESTING_INFLUXDB_* env vars are not set");
        return Ok(());
    };

    const POINTS: &str = "temperature,room=room1 value=18.94647\n\
     temperatureroom=room2value=20.268019\n\
     temperature,room=room3 value=24.064857\n\
     temperature,room=room4 value=43i";

    let client = Client::new(config).await?;
    match client
        .write(POINTS)
        .with_options(WriteOptions {
            accept_partial: false,
            use_v2_api: true,
            ..WriteOptions::default()
        })
        .await
    {
        Ok(_) => (),
        Err(err) => match err {
            Error::Server { code, message } => {
                assert_eq!(code, 400);
                assert!(message.eq("write buffer error: line protocol parse failed: Expected at least one space character, got end of input"));
            }
            _ => panic!("expected Server error, got: {err}"),
        },
    }

    Ok(())
}

#[tokio::test]
async fn write_error_api_v3() -> Result<(), Box<dyn std::error::Error>> {
    let Some(config) = testing_config() else {
        eprintln!("skipping e2e test: TESTING_INFLUXDB_* env vars are not set");
        return Ok(());
    };

    const POINTS: &str = "temperature,room=room1 value=18.94647\n\
     temperatureroom=room2value=20.268019\n\
     temperature,room=room3 value=24.064857\n\
     temperature,room=room4 value=43i";

    let client = Client::new(config).await?;
    match client
        .write(POINTS)
        .with_options(WriteOptions {
            accept_partial: false,
            use_v2_api: false,
            ..WriteOptions::default()
        })
        .await
    {
        Ok(_) => (),
        Err(err) => match err {
            Error::Server { code, message } => {
                assert_eq!(code, 400);
                assert!(message.contains("line protocol parsing error"));
            }
            _ => panic!("expected Server error, got: {err}"),
        },
    }

    Ok(())
}

#[tokio::test]
async fn partial_write_error() -> Result<(), Box<dyn std::error::Error>> {
    let Some(config) = testing_config() else {
        eprintln!("skipping e2e test: TESTING_INFLUXDB_* env vars are not set");
        return Ok(());
    };

    const POINTS: &str = "home,room=Sunroom temp=96 1735545600\n\
    home,room=Sunroom temp=\"hi\" 1735545610\n\
    home,room=Sunroom temp=88i 1735545620";

    let client = Client::new(config).await?;
    match client
        .write(POINTS)
        .with_options(WriteOptions {
            accept_partial: true,
            use_v2_api: false,
            ..WriteOptions::default()
        })
        .await
    {
        Ok(_) => (),
        Err(err) => match err {
            Error::PartialWrite(e) => {
                assert_eq!(e.message, "partial write of line protocol occurred:\n\tline 2: invalid column type for column 'temp', expected iox::column_type::field::float, got iox::column_type::field::string (home,room=Sunroom te)\n\tline 3: invalid column type for column 'temp', expected iox::column_type::field::float, got iox::column_type::field::integer (home,room=Sunroom te)");
                assert_eq!(e.line_errors.len(), 2);

                let mut line = e.line_errors.first().unwrap();
                assert_eq!(line.line, Some(2));
                assert_eq!(line.message, "invalid column type for column 'temp', expected iox::column_type::field::float, got iox::column_type::field::string");
                assert_eq!(
                    line.original_line.as_ref().unwrap().as_str(),
                    "home,room=Sunroom te"
                );

                line = e.line_errors.get(1).unwrap();
                assert_eq!(line.line, Some(3));
                assert_eq!(line.message, "invalid column type for column 'temp', expected iox::column_type::field::float, got iox::column_type::field::integer");
                assert_eq!(
                    line.original_line.as_ref().unwrap().as_str(),
                    "home,room=Sunroom te"
                );
            }
            _ => panic!("expected PartialWrite, got: {err}"),
        },
    }

    Ok(())
}

#[tokio::test]
async fn write_and_query_data_with_v2_precision() -> Result<(), Box<dyn std::error::Error>> {
    let Some(config) = testing_config() else {
        eprintln!("skipping e2e test: TESTING_INFLUXDB_* env vars are not set");
        return Ok(());
    };

    let v2_write_options = WriteOptions {
        precision: Precision::Second,
        default_tags: config.write_options.default_tags.clone(),
        gzip_threshold: config.write_options.gzip_threshold,
        no_sync: config.write_options.no_sync,
        accept_partial: config.write_options.accept_partial,
        use_v2_api: true,
        tag_order: config.write_options.tag_order,
        batch_size: config.write_options.batch_size,
        max_inflight: config.write_options.max_inflight,
    };

    let v2_config = ClientConfig::builder()
        .host(config.host.clone())
        .database(config.database.clone())
        .token(config.token.unwrap())
        .write_options(v2_write_options)
        .build()
        .expect("v2 config should be built correctly");

    let client = Client::new(v2_config).await?;
    let test_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos() as i64;

    let point = Point::new(MEASUREMENT)
        .tag("location", LOCATION)
        .field("temp", 15.5_f64)
        .field("index", 80_i64)
        .field("uindex", 800_u64)
        .field("valid", true)
        .field("testId", test_id)
        .field("text", "a1")
        .timestamp_nanos(test_id);

    client.write(vec![point]).await?;

    let row = query_written_point(&client, test_id)
        .await?
        .unwrap_or_else(|| panic!("expected to query back point with test_id={test_id}"));

    assert_eq!(row["location"].as_str(), Some(LOCATION));
    assert_eq!(row["temp"].as_f64(), Some(15.5));
    assert_eq!(row["index"].as_i64(), Some(80));
    assert_eq!(row["uindex"], Value::U64(800));
    assert_eq!(row["valid"].as_bool(), Some(true));
    assert_eq!(row["testId"].as_i64(), Some(test_id));
    assert_eq!(row["text"].as_str(), Some("a1"));
    assert_eq!(
        row["time"],
        Value::Timestamp((test_id / 1_000_000_000) * 1_000_000_000)
    );

    Ok(())
}

struct TlsTest {
    name: &'static str,
    server_cert_path: String,
    client_cert_path: Option<String>,
    client_key_path: Option<String>,
    require_client_auth: bool,
    expect_fail: bool,
}

#[tokio::test]
async fn write_with_secure_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = rustls::crypto::ring::default_provider().install_default(); // pick one provider explicitly

    let generated_certs = generate_certs();
    for test_case in get_tls_test_cases(&generated_certs) {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        listener.set_nonblocking(true)?;
        let port = listener.local_addr()?.port();

        let handler = start_secure_write_server(
            listener,
            &generated_certs.ca_cert_path,
            &generated_certs.server_cert_path,
            &generated_certs.server_key_path,
            test_case.require_client_auth,
        )
        .await;

        let mut builder = ClientConfig::builder()
            .host(format!("https://localhost:{port}"))
            .token("TEST_TOKEN")
            .database("db")
            .ssl_roots_path(test_case.server_cert_path)
            .query_timeout(Duration::from_secs(5));

        if let Some(path) = test_case.client_cert_path {
            builder = builder.client_cert_path(path);
            if let Some(path) = test_case.client_key_path {
                builder = builder.client_key_path(path);
            }
        }
        let client = Client::new(builder.build()?).await?;
        if test_case.expect_fail {
            let err = client.write("cpu usage=1.0").await.unwrap_err();
            let chain = error_chain(&err);
            assert!(
                chain.contains("invalid peer certificate")
                    || chain.contains("UnknownIssuer")
                    || chain.contains("alert")
                    || chain.contains("certificate"),
                "[{}] expected TLS/mTLS cert failure, chain: {chain}",
                test_case.name
            );
        } else {
            client.write("cpu usage=1.0").await?;
        }
        handler.abort();
    }
    generated_certs.remove_temp_dirs();
    Ok(())
}

#[tokio::test]
async fn query_with_secure_server() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let _ = rustls::crypto::ring::default_provider().install_default(); // pick one provider explicitly

    let generated_certs = generate_certs();
    for test_case in get_tls_test_cases(&generated_certs) {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let port = listener.local_addr()?.port();
        let incoming = TcpIncoming::from(listener);

        let do_get_calls = Arc::new(AtomicUsize::new(0));
        let service = CapturingFlightService::new(
            Arc::new(Mutex::new(None)),
            Arc::clone(&do_get_calls),
            Arc::new(AtomicUsize::new(0)),
        );
        let server = start_secure_query_server(
            &generated_certs.server_cert_path,
            &generated_certs.server_key_path,
            &generated_certs.ca_cert_path,
            incoming,
            service,
            test_case.require_client_auth,
        );

        let mut builder = ClientConfig::builder()
            .host(format!("https://localhost:{port}"))
            .token("TEST_TOKEN")
            .database("db")
            .ssl_roots_path(test_case.server_cert_path)
            .query_timeout(Duration::from_secs(5));

        if let Some(path) = test_case.client_cert_path {
            builder = builder.client_cert_path(path);
            if let Some(path) = test_case.client_key_path {
                builder = builder.client_key_path(path);
            }
        }
        let client = Client::new(builder.build()?).await?;
        if test_case.expect_fail {
            let err = client
                .sql("SELECT * FROM test")
                .stream()
                .await
                .err()
                .unwrap();
            let chain = error_chain(&err);
            assert!(
                chain.contains("invalid peer certificate")
                    || chain.contains("UnknownIssuer")
                    || chain.contains("alert")
                    || chain.contains("InvalidCertificate")
                    || chain.contains("certificate"),
                "[{}] expected TLS/mTLS cert failure, chain: {chain}",
                test_case.name
            );
        } else {
            let stream = client.sql("SELECT * FROM test").stream().await?;
            drop(stream);
            assert_eq!(do_get_calls.load(Ordering::SeqCst), 1);
        }

        server
            .shutdown()
            .await
            .expect("Shutdown Flight server failed");
    }
    generated_certs.remove_temp_dirs();
    Ok(())
}

fn get_tls_test_cases(test_certs: &TestCerts) -> Vec<TlsTest> {
    Vec::from([
        TlsTest {
            name: "tls success with correct server certificate",
            server_cert_path: test_certs.ca_cert_path.clone(),
            client_cert_path: None,
            client_key_path: None,
            require_client_auth: false,
            expect_fail: false,
        },
        TlsTest {
            name: "tls fail with incorrect server certificate",
            server_cert_path: test_certs.client_cert_path.clone(),
            client_cert_path: None,
            client_key_path: None,
            require_client_auth: false,
            expect_fail: true,
        },
        TlsTest {
            name: "mtls success with correct client and server certificates",
            server_cert_path: test_certs.ca_cert_path.clone(),
            client_cert_path: Some(test_certs.client_cert_path.clone()),
            client_key_path: Some(test_certs.client_key_path.clone()),
            require_client_auth: true,
            expect_fail: false,
        },
        TlsTest {
            name: "mtls success with correct client cert and key bundled inside .pem",
            server_cert_path: test_certs.ca_cert_path.clone(),
            client_cert_path: Some(test_certs.client_pem_path.clone()),
            client_key_path: Some(test_certs.client_pem_path.clone()),
            require_client_auth: true,
            expect_fail: false,
        },
        TlsTest {
            name: "mtls failed with incorrect client cert and key bundled inside .pem",
            server_cert_path: test_certs.ca_cert_path.clone(),
            client_cert_path: Some(test_certs.server_pem_path.clone()),
            client_key_path: Some(test_certs.server_pem_path.clone()),
            require_client_auth: true,
            expect_fail: true,
        },
        TlsTest {
            name: "mtls fail with incorrect client certificates",
            server_cert_path: test_certs.ca_cert_path.clone(),
            client_cert_path: Some(test_certs.server_cert_path.clone()),
            client_key_path: Some(test_certs.server_key_path.clone()),
            require_client_auth: true,
            expect_fail: true,
        },
        TlsTest {
            name: "mtls fail without a client certificate",
            server_cert_path: test_certs.ca_cert_path.clone(),
            client_cert_path: None,
            client_key_path: None,
            require_client_auth: true,
            expect_fail: true,
        },
    ])
}

async fn query_written_point(
    client: &Client,
    test_id: i64,
) -> influxdb3_client::Result<Option<Row>> {
    let query = format!(
        r#"
        SELECT *
        FROM "{MEASUREMENT}"
        WHERE
            time >= now() - interval '10 minute'
            AND "location" = $location
            AND "testId" = $test_id
        ORDER BY time
        "#
    );

    for _ in 0..10 {
        let rows = client
            .sql(&query)
            .param("location", LOCATION)
            .param("test_id", test_id)
            .await?
            .rows()?;
        if let Some(row) = rows.into_iter().next() {
            return Ok(Some(row));
        }

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
    }

    Ok(None)
}
