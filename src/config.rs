use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use url::Url;

use crate::{error::Error, precision::Precision, retry::RetryConfig, write::WriteOptions};

/// Configuration for the InfluxDB 3 client.
///
/// Construct with [`ClientConfig::builder()`] or parse from a connection string /
/// environment variables with [`ClientConfig::from_connection_string()`] /
/// [`ClientConfig::from_env()`].
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// InfluxDB host URL (e.g. `https://cluster.influxdata.io`).
    pub host: String,

    /// API token.
    pub token: Option<String>,

    /// Authentication scheme: `"Bearer"` (default) or `"Token"`.
    pub auth_scheme: String,

    /// Database for all operations. Required; validated at construction time.
    pub database: String,

    /// Organization name (used for v2 API compatibility).
    pub org: Option<String>,

    /// Default write options applied to every write call.
    pub write_options: WriteOptions,

    /// Default retry policy for transient write/query failures. Override per
    /// request with `WriteRequest`/`QueryRequest` `.retry()` / `.no_retry()`.
    pub retry: RetryConfig,

    /// Extra HTTP headers sent with every request.
    pub headers: HeaderMap,

    /// Path to a PEM file with additional CA roots for TLS verification.
    pub ssl_roots_path: Option<String>,

    /// HTTP proxy URL.
    pub proxy: Option<String>,

    /// Request timeout for write calls.
    pub write_timeout: Duration,

    /// Timeout for the Flight channel connect and for collected (`.await`)
    /// queries. Streaming queries (`.stream()`) are intentionally unbounded.
    pub query_timeout: Duration,

    /// Keep-alive idle connection timeout.
    pub idle_connection_timeout: Duration,

    /// Maximum number of idle connections in the pool.
    pub max_idle_connections: usize,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            host: String::new(),
            token: None,
            auth_scheme: "Bearer".to_string(),
            database: String::new(), // validated as non-empty in build()
            org: None,
            write_options: WriteOptions::default(),
            retry: RetryConfig::default(),
            headers: HeaderMap::new(),
            ssl_roots_path: None,
            proxy: None,
            write_timeout: Duration::from_secs(30),
            query_timeout: Duration::from_secs(60),
            idle_connection_timeout: Duration::from_secs(90),
            max_idle_connections: 100,
        }
    }
}

impl ClientConfig {
    /// Start building a config.
    pub fn builder() -> ClientConfigBuilder {
        ClientConfigBuilder::default()
    }

    /// Parse client configuration from process environment variables.
    ///
    /// Supported variables:
    /// - `INFLUX_HOST` - InfluxDB host URL (required).
    /// - `INFLUX_DATABASE` - database name (required).
    /// - `INFLUX_TOKEN` - authentication token.
    /// - `INFLUX_AUTH_SCHEME` - authentication scheme.
    /// - `INFLUX_ORG` - organization name.
    /// - `INFLUX_PRECISION` - write precision (`ns`, `us`, `ms`, `s`, or long form).
    /// - `INFLUX_GZIP_THRESHOLD` - gzip threshold in bytes.
    /// - `INFLUX_WRITE_NO_SYNC` - skip WAL synchronization for writes.
    /// - `INFLUX_WRITE_ACCEPT_PARTIAL` - accept partial writes.
    /// - `INFLUX_WRITE_USE_V2_API` - use the v2 write endpoint.
    pub fn from_env() -> Result<Self, Error> {
        let host = std::env::var("INFLUX_HOST").map_err(|_| Error::EnvVar("INFLUX_HOST".into()))?;
        let database = std::env::var("INFLUX_DATABASE")
            .map_err(|_| Error::EnvVar("INFLUX_DATABASE".into()))?;

        let token = std::env::var("INFLUX_TOKEN").ok();
        let auth_scheme = std::env::var("INFLUX_AUTH_SCHEME").ok();
        let org = std::env::var("INFLUX_ORG").ok();
        let mut write_options = WriteOptions::default();
        if let Ok(value) = std::env::var("INFLUX_PRECISION") {
            write_options.precision = parse_precision(&value)?;
        }
        if let Ok(value) = std::env::var("INFLUX_GZIP_THRESHOLD") {
            write_options.gzip_threshold = Some(parse_usize("INFLUX_GZIP_THRESHOLD", &value)?);
        }
        if let Ok(value) = std::env::var("INFLUX_WRITE_NO_SYNC") {
            write_options.no_sync = parse_bool("INFLUX_WRITE_NO_SYNC", &value)?;
        }
        if let Ok(value) = std::env::var("INFLUX_WRITE_ACCEPT_PARTIAL") {
            write_options.accept_partial = parse_bool("INFLUX_WRITE_ACCEPT_PARTIAL", &value)?;
        }
        if let Ok(value) = std::env::var("INFLUX_WRITE_USE_V2_API") {
            write_options.use_v2_api = parse_bool("INFLUX_WRITE_USE_V2_API", &value)?;
        }

        let mut builder = ClientConfig::builder()
            .host(host)
            .database(database)
            .token_opt(token)
            .org_opt(org)
            .write_options(write_options);
        if let Some(auth_scheme) = auth_scheme {
            builder = builder.auth_scheme(auth_scheme);
        }
        builder.build()
    }

    /// Parse a URL-formatted connection string, e.g.:
    ///
    /// ```text
    /// https://cluster.influxdata.io/?token=TOKEN&database=DB&org=ORG
    /// ```
    ///
    /// Supported query parameters:
    /// - `token` - authentication token.
    /// - `database` - database name (required).
    /// - `org` - organization name.
    /// - `authScheme` - authentication scheme.
    /// - `precision` - write precision (`ns`, `us`, `ms`, `s`, or long form).
    /// - `gzipThreshold` - gzip threshold in bytes.
    /// - `writeNoSync` - skip WAL synchronization for writes.
    /// - `writeAcceptPartial` - accept partial writes.
    /// - `writeUseV2Api` - use the v2 write endpoint.
    pub fn from_connection_string(cs: &str) -> Result<Self, Error> {
        let url = Url::parse(cs)?;
        let mut host_url = url.clone();
        host_url
            .set_password(None)
            .map_err(|_| Error::Config("invalid connection string host".into()))?;
        host_url
            .set_username("")
            .map_err(|_| Error::Config("invalid connection string host".into()))?;
        host_url.set_path("");
        host_url.set_query(None);
        host_url.set_fragment(None);
        let host = host_url.to_string();

        let mut builder = ClientConfig::builder().host(host);
        let mut write_options = WriteOptions::default();

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "token" => {
                    builder = builder.token(value.into_owned());
                }
                "database" => {
                    builder = builder.database(value.into_owned());
                }
                "org" => {
                    builder = builder.org(value.into_owned());
                }
                "authScheme" => {
                    builder = builder.auth_scheme(value.into_owned());
                }
                "precision" => {
                    write_options.precision = parse_precision(value.as_ref())?;
                }
                "gzipThreshold" => {
                    write_options.gzip_threshold =
                        Some(parse_usize("gzipThreshold", value.as_ref())?);
                }
                "writeNoSync" => {
                    write_options.no_sync = parse_bool("writeNoSync", value.as_ref())?;
                }
                "writeAcceptPartial" => {
                    write_options.accept_partial =
                        parse_bool("writeAcceptPartial", value.as_ref())?;
                }
                "writeUseV2Api" => {
                    write_options.use_v2_api = parse_bool("writeUseV2Api", value.as_ref())?;
                }
                _other => {}
            }
        }

        builder.write_options(write_options).build()
    }

    /// Return the normalised host URL (trailing slash stripped).
    pub fn host_url(&self) -> &str {
        self.host.trim_end_matches('/')
    }

    /// Build the `Authorization` header value (`"Bearer TOKEN"` etc.).
    ///
    /// Returns `Ok(None)` when no token is set. Returns an error if the token
    /// contains characters that are invalid in an HTTP header value.
    pub fn authorization_header(&self) -> Result<Option<HeaderValue>, Error> {
        match &self.token {
            None => Ok(None),
            Some(tok) => HeaderValue::from_str(&format!("{} {}", self.auth_scheme, tok))
                .map(Some)
                .map_err(|_| Error::Config("token contains invalid header characters".into())),
        }
    }
}

fn parse_precision(value: &str) -> Result<Precision, Error> {
    value
        .parse()
        .map_err(|e| Error::Config(format!("invalid precision '{value}': {e}")))
}

fn parse_usize(name: &str, value: &str) -> Result<usize, Error> {
    value
        .parse()
        .map_err(|e| Error::Config(format!("invalid {name} '{value}': {e}")))
}

fn parse_bool(name: &str, value: &str) -> Result<bool, Error> {
    value
        .parse()
        .map_err(|e| Error::Config(format!("invalid {name} '{value}': {e}")))
}

/// Fluent builder for [`ClientConfig`].
#[derive(Debug, Default)]
pub struct ClientConfigBuilder {
    cfg: ClientConfig,
    /// Validated when [`ClientConfigBuilder::build`] is called, so a malformed
    /// header surfaces as an error rather than a panic at insertion time.
    pending_headers: Vec<(String, String)>,
}

impl ClientConfigBuilder {
    /// Required: the InfluxDB host URL.
    pub fn host(mut self, host: impl Into<String>) -> Self {
        self.cfg.host = host.into();
        self
    }

    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.cfg.token = Some(token.into());
        self
    }

    pub fn token_opt(mut self, token: Option<String>) -> Self {
        self.cfg.token = token;
        self
    }

    /// `"Bearer"` (default) or `"Token"`.
    pub fn auth_scheme(mut self, scheme: impl Into<String>) -> Self {
        self.cfg.auth_scheme = scheme.into();
        self
    }

    pub fn database(mut self, db: impl Into<String>) -> Self {
        self.cfg.database = db.into();
        self
    }

    pub fn org(mut self, org: impl Into<String>) -> Self {
        self.cfg.org = Some(org.into());
        self
    }

    pub fn org_opt(mut self, org: Option<String>) -> Self {
        self.cfg.org = org;
        self
    }

    pub fn write_options(mut self, opts: WriteOptions) -> Self {
        self.cfg.write_options = opts;
        self
    }

    /// Set the default retry policy for transient write/query failures.
    pub fn retry(mut self, retry: RetryConfig) -> Self {
        self.cfg.retry = retry;
        self
    }

    /// Add a single extra HTTP header sent with every request.
    ///
    /// The name and value are validated in [`build`](Self::build), so an
    /// invalid header is reported as an error rather than panicking here.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.pending_headers.push((key.into(), value.into()));
        self
    }

    pub fn ssl_roots_path(mut self, path: impl Into<String>) -> Self {
        self.cfg.ssl_roots_path = Some(path.into());
        self
    }

    pub fn proxy(mut self, proxy: impl Into<String>) -> Self {
        self.cfg.proxy = Some(proxy.into());
        self
    }

    pub fn write_timeout(mut self, dur: Duration) -> Self {
        self.cfg.write_timeout = dur;
        self
    }

    pub fn query_timeout(mut self, dur: Duration) -> Self {
        self.cfg.query_timeout = dur;
        self
    }

    pub fn idle_connection_timeout(mut self, dur: Duration) -> Self {
        self.cfg.idle_connection_timeout = dur;
        self
    }

    pub fn max_idle_connections(mut self, n: usize) -> Self {
        self.cfg.max_idle_connections = n;
        self
    }

    /// Validate and produce the final [`ClientConfig`].
    ///
    /// Returns an error if `host` or `database` were not set.
    pub fn build(mut self) -> Result<ClientConfig, Error> {
        if self.cfg.host.is_empty() {
            return Err(Error::Config("host is required".into()));
        }
        Url::parse(&self.cfg.host)
            .map_err(|e| Error::Config(format!("invalid host URL '{}': {e}", self.cfg.host)))?;
        if self.cfg.database.is_empty() {
            return Err(Error::Config("database is required".into()));
        }

        for (key, value) in self.pending_headers {
            let name = HeaderName::from_bytes(key.as_bytes())
                .map_err(|e| Error::Config(format!("invalid header name '{key}': {e}")))?;
            let val = HeaderValue::from_str(&value)
                .map_err(|e| Error::Config(format!("invalid value for header '{key}': {e}")))?;
            self.cfg.headers.insert(name, val);
        }

        // Surface a malformed token now rather than on the first request.
        self.cfg.authorization_header()?;

        Ok(self.cfg)
    }
}
