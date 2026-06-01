/// ClientConfig construction and parsing.
use influxdb3_client::ClientConfig;

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

    // `bucket` is an alias for `database` (v2 compat).
    let cfg = ClientConfig::from_connection_string("https://h/?token=T&bucket=mybucket").unwrap();
    assert_eq!(cfg.database, "mybucket");

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
    std::env::set_var("INFLUX_ORG", "env-org");
    let cfg = ClientConfig::from_env().unwrap();
    assert_eq!(cfg.host_url(), "https://env-host");
    assert_eq!(cfg.token.as_deref(), Some("env-token"));
    assert_eq!(cfg.database, "env-db");
    assert_eq!(cfg.org.as_deref(), Some("env-org"));

    for v in [
        "INFLUX_HOST",
        "INFLUX_TOKEN",
        "INFLUX_DATABASE",
        "INFLUX_ORG",
    ] {
        std::env::remove_var(v);
    }
}
