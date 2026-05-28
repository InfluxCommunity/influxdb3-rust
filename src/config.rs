use std::time::Duration;

use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use url::Url;

use crate::{
    error::Error,
    write::WriteOptions,
};

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

    /// Authentication scheme — `"Bearer"` (default) or `"Token"`.
    pub auth_scheme: String,

    /// Database for all operations. Required — validated at construction time.
    pub database: String,

    /// Organization name (used for v2 API compatibility).
    pub org: Option<String>,

    /// Default write options applied to every write call.
    pub write_options: WriteOptions,

    /// Extra HTTP headers sent with every request.
    pub headers: HeaderMap,

    /// Path to a PEM file with additional CA roots for TLS verification.
    pub ssl_roots_path: Option<String>,

    /// HTTP proxy URL.
    pub proxy: Option<String>,

    /// Request timeout for write calls.
    pub write_timeout: Duration,

    /// Request timeout for HTTP query calls.
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

    /// Parse `INFLUX_HOST`, `INFLUX_TOKEN`, `INFLUX_DATABASE`, and `INFLUX_ORG`
    /// from the process environment. `INFLUX_HOST` and `INFLUX_DATABASE` are
    /// required; token and org are optional.
    pub fn from_env() -> Result<Self, Error> {
        let host = std::env::var("INFLUX_HOST")
            .map_err(|_| Error::EnvVar("INFLUX_HOST".into()))?;
        let database = std::env::var("INFLUX_DATABASE")
            .or_else(|_| std::env::var("INFLUX_BUCKET"))
            .map_err(|_| Error::EnvVar("INFLUX_DATABASE".into()))?;

        let token = std::env::var("INFLUX_TOKEN").ok();
        let org   = std::env::var("INFLUX_ORG").ok();

        ClientConfig::builder()
            .host(host)
            .database(database)
            .token_opt(token)
            .org_opt(org)
            .build()
    }

    /// Parse a URL-formatted connection string, e.g.:
    ///
    /// ```text
    /// https://cluster.influxdata.io/?token=TOKEN&database=DB&org=ORG
    /// ```
    ///
    /// `database` (or `bucket`) is required; returns an error if absent.
    pub fn from_connection_string(cs: &str) -> Result<Self, Error> {
        let url = Url::parse(cs)?;
        let host = format!("{}://{}", url.scheme(), url.host_str().unwrap_or_default());

        let mut builder = ClientConfig::builder().host(host);

        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "token"              => { builder = builder.token(value.into_owned()); }
                "database" | "bucket" => { builder = builder.database(value.into_owned()); }
                "org"                => { builder = builder.org(value.into_owned()); }
                _other               => {}
            }
        }

        builder.build()
    }

    /// Return the normalised host URL (trailing slash stripped).
    pub fn host_url(&self) -> &str {
        self.host.trim_end_matches('/')
    }

    /// Build the `Authorization` header value (`"Bearer TOKEN"` etc.).
    pub fn authorization_header(&self) -> Option<HeaderValue> {
        self.token.as_ref().map(|tok| {
            HeaderValue::from_str(&format!("{} {}", self.auth_scheme, tok))
                .expect("token contains invalid header characters")
        })
    }
}


/// Fluent builder for [`ClientConfig`].
#[derive(Debug, Default)]
pub struct ClientConfigBuilder {
    cfg: ClientConfig,
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

    /// Add a single extra HTTP header sent with every request.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let name = HeaderName::from_bytes(key.into().as_bytes())
            .expect("invalid header name");
        let val = HeaderValue::from_str(&value.into())
            .expect("invalid header value");
        self.cfg.headers.insert(name, val);
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
    pub fn build(self) -> Result<ClientConfig, Error> {
        if self.cfg.host.is_empty() {
            return Err(Error::Config("host is required".into()));
        }
        Url::parse(&self.cfg.host)
            .map_err(|e| Error::Config(format!("invalid host URL '{}': {e}", self.cfg.host)))?;
        if self.cfg.database.is_empty() {
            return Err(Error::Config("database is required".into()));
        }
        Ok(self.cfg)
    }
}

