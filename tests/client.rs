use std::time::{SystemTime, UNIX_EPOCH};

use influxdb3_client::{Client, ClientConfig, Error, Point, Row, Value, WriteOptions};

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
