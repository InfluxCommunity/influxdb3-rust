/// ClientConfig construction and parsing.
use influxdb3_client::{ClientConfig, Precision};

#[test]
fn construction_and_connection_string() {
    // Builder validation.
    let err = ClientConfig::builder().database("db").build().unwrap_err();
    assert!(err.to_string().contains("host is required"), "got: {err}");
    let err = ClientConfig::builder()
        .host("http://localhost")
        .build()
        .unwrap_err();
    assert!(
        err.to_string().contains("database is required"),
        "got: {err}"
    );
    let err = ClientConfig::builder()
        .host("not a url!!")
        .database("db")
        .build()
        .unwrap_err();
    assert!(err.to_string().contains("invalid"), "got: {err}");

    // Connection string, full form.
    let cfg = ClientConfig::from_connection_string(
        "https://cluster.example.io/?token=TOK&database=DB&org=ORG",
    )
    .unwrap();
    assert_eq!(cfg.host_url(), "https://cluster.example.io");
    assert_eq!(cfg.token.as_deref(), Some("TOK"));
    assert_eq!(cfg.database, "DB");
    assert_eq!(cfg.org.as_deref(), Some("ORG"));

    // Connection string preserves explicit non-default port.
    let cfg = ClientConfig::from_connection_string(
        "https://cluster.example.io:8443/?token=TOK&database=DB",
    )
    .unwrap();
    assert_eq!(cfg.host_url(), "https://cluster.example.io:8443");
    assert_eq!(cfg.database, "DB");

    // Connection string userinfo is not part of the client host.
    let cfg = ClientConfig::from_connection_string(
        "https://user:pass@cluster.example.io:8443/?token=TOK&database=DB",
    )
    .unwrap();
    assert_eq!(cfg.host_url(), "https://cluster.example.io:8443");
    assert!(!cfg.host_url().contains("user:pass"));
    assert_eq!(cfg.token.as_deref(), Some("TOK"));

    // IPv6 hosts keep brackets and explicit ports.
    let cfg =
        ClientConfig::from_connection_string("http://[::1]:8181/?token=TOK&database=DB").unwrap();
    assert_eq!(cfg.host_url(), "http://[::1]:8181");
    assert_eq!(cfg.database, "DB");

    // `bucket` is an alias for `database` (v2 compat).
    let cfg = ClientConfig::from_connection_string("https://h/?token=T&bucket=mybucket").unwrap();
    assert_eq!(cfg.database, "mybucket");

    // Connection string supports common client config options.
    let cfg = ClientConfig::from_connection_string(
        "https://cluster.example.io/?token=TOK&database=DB&authScheme=Token\
         &precision=ms&gzipThreshold=64&writeNoSync=true\
         &writeAcceptPartial=false&writeUseV2Api=true",
    )
    .unwrap();
    assert_eq!(cfg.auth_scheme, "Token");
    assert_eq!(cfg.write_options.precision, Precision::Millisecond);
    assert_eq!(cfg.write_options.gzip_threshold, Some(64));
    assert!(cfg.write_options.no_sync);
    assert!(!cfg.write_options.accept_partial);
    assert!(cfg.write_options.use_v2_api);

    for precision in [
        "ns",
        "nanosecond",
        "us",
        "microsecond",
        "ms",
        "millisecond",
        "s",
        "second",
    ] {
        let cs = format!("https://h/?token=T&database=db&precision={precision}");
        ClientConfig::from_connection_string(&cs).unwrap();
    }

    for cs in [
        "https://h/?token=T&database=db&precision=invalid",
        "https://h/?token=T&database=db&gzipThreshold=abc",
        "https://h/?token=T&database=db&writeNoSync=invalid",
        "https://h/?token=T&database=db&writeAcceptPartial=invalid",
        "https://h/?token=T&database=db&writeUseV2Api=invalid",
    ] {
        let err = ClientConfig::from_connection_string(cs).unwrap_err();
        assert!(
            err.to_string().contains("invalid"),
            "expected invalid config error for {cs}, got: {err}"
        );
    }

    // Connection string with no database is an error.
    let err = ClientConfig::from_connection_string("http://localhost:8086/?token=T").unwrap_err();
    assert!(
        err.to_string().contains("database is required"),
        "got: {err}"
    );
}

#[test]
fn from_env() {
    // Errors when host/database missing.
    std::env::remove_var("INFLUX_HOST");
    assert!(ClientConfig::from_env()
        .unwrap_err()
        .to_string()
        .contains("INFLUX_HOST"));
    std::env::set_var("INFLUX_HOST", "https://env-host");
    std::env::remove_var("INFLUX_TOKEN");
    std::env::remove_var("INFLUX_DATABASE");
    std::env::remove_var("INFLUX_BUCKET");
    std::env::remove_var("INFLUX_ORG");
    assert!(ClientConfig::from_env()
        .unwrap_err()
        .to_string()
        .contains("INFLUX_DATABASE"));

    // Full happy path.
    std::env::set_var("INFLUX_TOKEN", "env-token");
    std::env::set_var("INFLUX_DATABASE", "env-db");
    std::env::set_var("INFLUX_AUTH_SCHEME", "Token");
    std::env::set_var("INFLUX_PRECISION", "ms");
    std::env::set_var("INFLUX_GZIP_THRESHOLD", "64");
    std::env::set_var("INFLUX_WRITE_NO_SYNC", "true");
    std::env::set_var("INFLUX_WRITE_ACCEPT_PARTIAL", "false");
    std::env::set_var("INFLUX_WRITE_USE_V2_API", "true");
    std::env::set_var("INFLUX_ORG", "env-org");
    let cfg = ClientConfig::from_env().unwrap();
    assert_eq!(cfg.host_url(), "https://env-host");
    assert_eq!(cfg.token.as_deref(), Some("env-token"));
    assert_eq!(cfg.database, "env-db");
    assert_eq!(cfg.org.as_deref(), Some("env-org"));
    assert_eq!(cfg.auth_scheme, "Token");
    assert_eq!(cfg.write_options.precision, Precision::Millisecond);
    assert_eq!(cfg.write_options.gzip_threshold, Some(64));
    assert!(cfg.write_options.no_sync);
    assert!(!cfg.write_options.accept_partial);
    assert!(cfg.write_options.use_v2_api);

    for (name, value) in [
        ("INFLUX_PRECISION", "invalid"),
        ("INFLUX_GZIP_THRESHOLD", "abc"),
        ("INFLUX_WRITE_NO_SYNC", "invalid"),
        ("INFLUX_WRITE_ACCEPT_PARTIAL", "invalid"),
        ("INFLUX_WRITE_USE_V2_API", "invalid"),
    ] {
        std::env::set_var(name, value);
        let err = ClientConfig::from_env().unwrap_err();
        assert!(
            err.to_string().contains("invalid"),
            "expected invalid config error for {name}={value}, got: {err}"
        );
        std::env::remove_var(name);
    }

    for v in [
        "INFLUX_HOST",
        "INFLUX_TOKEN",
        "INFLUX_DATABASE",
        "INFLUX_ORG",
        "INFLUX_AUTH_SCHEME",
        "INFLUX_PRECISION",
        "INFLUX_GZIP_THRESHOLD",
        "INFLUX_WRITE_NO_SYNC",
        "INFLUX_WRITE_ACCEPT_PARTIAL",
        "INFLUX_WRITE_USE_V2_API",
    ] {
        std::env::remove_var(v);
    }
}
