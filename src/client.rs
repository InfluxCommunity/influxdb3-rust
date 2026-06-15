use std::collections::HashMap;
use std::future::IntoFuture;
use std::io::Write as IoWrite;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::stream::{self, StreamExt, TryStreamExt};
use reqwest::{
    header::{AUTHORIZATION, CONTENT_ENCODING, CONTENT_TYPE, RETRY_AFTER},
    Client as HttpClient, ClientBuilder, Response,
};
use tokio::sync::OnceCell;

use crate::{
    config::ClientConfig,
    error::{Error, LineError, PartialWriteError},
    flight::{BatchStream, FlightQueryClient},
    precision::Precision,
    query::{QueryOptions, QueryParameters, QueryResult, QueryType},
    retry::{self, RetryConfig},
    write::{WriteInput, WriteOptions},
    Result,
};

/// Async client for InfluxDB 3 Core and Enterprise.
///
/// See the crate-level docs for end-to-end examples.
pub struct Client {
    config: ClientConfig,
    http: HttpClient,
    /// Lazy: connected on first query.  `OnceCell` retries on init failure.
    flight: OnceCell<FlightQueryClient>,
}

impl Client {
    /// Create a client from a [`ClientConfig`].
    ///
    /// The Arrow Flight gRPC channel is opened lazily on the first query call,
    /// so this constructor never fails due to gRPC connectivity.
    pub async fn new(config: ClientConfig) -> Result<Self> {
        let http = build_http_client(&config)?;
        Ok(Client {
            config,
            http,
            flight: OnceCell::new(),
        })
    }

    /// Parse a connection string and create a client.
    pub async fn from_connection_string(cs: &str) -> Result<Self> {
        Client::new(ClientConfig::from_connection_string(cs)?).await
    }

    /// Read configuration from environment variables and create a client.
    ///
    /// See [`ClientConfig::from_env`] for the supported variables.
    pub async fn from_env() -> Result<Self> {
        Client::new(ClientConfig::from_env()?).await
    }

    /// Return a reference to the underlying config.
    pub fn config(&self) -> &ClientConfig {
        &self.config
    }

    /// Start a write request.
    ///
    /// `data` may be any [`WriteInput`]: a `&str` / `String` of pre-formatted
    /// line protocol, a `Vec<Point>` / `&[Point]`, a [`DataFrameWrite`] (polars
    /// feature), or your own type that implements the trait.
    ///
    /// Returns a [`WriteRequest`] builder; chain options, then `.await`.
    /// See the crate-level docs for examples.
    ///
    /// [`DataFrameWrite`]: crate::write_dataframe::DataFrameWrite
    pub fn write<W: WriteInput>(&self, data: W) -> WriteRequest<'_, W> {
        WriteRequest {
            client: self,
            data: Some(data),
            options: self.config.write_options.clone(),
            retry: None,
        }
    }

    /// Start a SQL query.  Sugar for `query(q, QueryType::Sql)`.
    pub fn sql(&self, q: impl Into<String>) -> QueryRequest<'_> {
        self.query(q, QueryType::Sql)
    }

    /// Start an InfluxQL query.  Sugar for `query(q, QueryType::InfluxQL)`.
    pub fn influxql(&self, q: impl Into<String>) -> QueryRequest<'_> {
        self.query(q, QueryType::InfluxQL)
    }

    /// Start a query.  Returns a [`QueryRequest`] builder.
    pub fn query(&self, q: impl Into<String>, language: QueryType) -> QueryRequest<'_> {
        QueryRequest {
            client: self,
            query: q.into(),
            query_type: language,
            params: QueryParameters::new(),
            headers: HashMap::new(),
            retry: None,
        }
    }

    /// Ping the server and return its version string.
    pub async fn ping(&self) -> Result<String> {
        let url = format!("{}/ping", self.config.host_url());
        let mut req = self.http.get(&url);
        if let Some(auth) = self.config.authorization_header()? {
            req = req.header(AUTHORIZATION, auth);
        }
        let resp = req.send().await?;
        let version = resp
            .headers()
            .get("x-influxdb-version")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("unknown")
            .to_owned();
        Ok(version)
    }

    /// Internal: resolve the Flight client (lazy-init on first call).
    async fn flight(&self) -> Result<&FlightQueryClient> {
        self.flight
            .get_or_try_init(|| async {
                FlightQueryClient::new(
                    &self.config.host,
                    self.config.token.as_deref(),
                    &self.config.auth_scheme,
                    self.config.ssl_roots_path.as_deref(),
                    self.config.query_timeout,
                )
                .await
            })
            .await
    }

    /// Internal: send one LP batch, retrying transient failures per `policy`.
    async fn send_lp(
        &self,
        body: Vec<u8>,
        opts: &WriteOptions,
        policy: &RetryConfig,
    ) -> Result<()> {
        let db = &self.config.database;

        let (url, mut params) = if opts.use_v2_api {
            let url = format!("{}/api/v2/write", self.config.host_url());
            let mut p = vec![("bucket", db.clone())];
            if let Some(org) = &self.config.org {
                p.push(("org", org.clone()));
            }
            (url, p)
        } else {
            let url = format!("{}/api/v3/write_lp", self.config.host_url());
            let mut p = vec![("db", db.clone())];
            p.push(("accept_partial", opts.accept_partial.to_string()));
            if opts.no_sync {
                p.push(("no_sync", "true".to_string()));
            }
            (url, p)
        };

        params.push(("precision", opts.precision.as_str().to_string()));

        // Compress once; each attempt re-sends the same (Arc-backed) Bytes.
        let (final_body, compressed) = maybe_gzip(body, opts.gzip_threshold).await?;

        let mut base = self
            .http
            .post(&url)
            .query(&params)
            .header(CONTENT_TYPE, "text/plain; charset=utf-8");
        if compressed {
            base = base.header(CONTENT_ENCODING, "gzip");
        }
        if let Some(auth) = self.config.authorization_header()? {
            base = base.header(AUTHORIZATION, auth);
        }
        for (k, v) in &self.config.headers {
            base = base.header(k, v);
        }
        let base = base.body(final_body);

        let start = Instant::now();
        let mut attempt: u32 = 0;
        loop {
            let req = base.try_clone().expect("write body is cloneable");
            match send_write_once(req).await {
                WriteAttempt::Ok => return Ok(()),
                WriteAttempt::Fatal(e) => return Err(e),
                WriteAttempt::Retry { after, last } => {
                    if attempt >= policy.max_retries {
                        return Err(last);
                    }
                    let delay = after
                        .filter(|_| policy.honor_retry_after)
                        .unwrap_or_else(|| policy.backoff(attempt));
                    if let Some(budget) = policy.max_elapsed {
                        if start.elapsed() + delay > budget {
                            return Err(last);
                        }
                    }
                    tokio::time::sleep(delay).await;
                    attempt += 1;
                }
            }
        }
    }
}

/// Builder produced by [`Client::write`]; chain options, then `.await`.
pub struct WriteRequest<'a, W: WriteInput> {
    client: &'a Client,
    data: Option<W>,
    options: WriteOptions,
    retry: Option<RetryConfig>,
}

impl<'a, W: WriteInput> WriteRequest<'a, W> {
    pub fn precision(mut self, p: Precision) -> Self {
        self.options.precision = p;
        self
    }
    pub fn no_sync(mut self) -> Self {
        self.options.no_sync = true;
        self
    }
    pub fn accept_partial(mut self, accept: bool) -> Self {
        self.options.accept_partial = accept;
        self
    }
    pub fn use_v2_api(mut self) -> Self {
        self.options.use_v2_api = true;
        self
    }
    pub fn batch_size(mut self, n: usize) -> Self {
        self.options.batch_size = n;
        self
    }
    pub fn max_inflight(mut self, n: usize) -> Self {
        self.options.max_inflight = n;
        self
    }
    pub fn default_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.options.default_tags.insert(key.into(), value.into());
        self
    }
    /// Compress bodies larger than `t` bytes; `None` disables compression.
    /// Disable or raise this for high-throughput ingest over a fast LAN where
    /// gzip CPU outweighs the bandwidth saved. See [`WriteOptions::gzip_threshold`].
    pub fn gzip_threshold(mut self, t: Option<usize>) -> Self {
        self.options.gzip_threshold = t;
        self
    }
    pub fn tag_order(mut self, order: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.options.tag_order = order.into_iter().map(Into::into).collect();
        self
    }

    /// Replace the underlying options wholesale.
    pub fn with_options(mut self, opts: WriteOptions) -> Self {
        self.options = opts;
        self
    }

    /// Override the retry policy for this write (defaults to the client's).
    pub fn retry(mut self, policy: RetryConfig) -> Self {
        self.retry = Some(policy);
        self
    }

    /// Disable retries for this write.
    pub fn no_retry(mut self) -> Self {
        self.retry = Some(RetryConfig::disabled());
        self
    }
}

impl<'a, W: WriteInput + Send + 'a> IntoFuture for WriteRequest<'a, W> {
    type Output = Result<()>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(mut self) -> Self::IntoFuture {
        let client = self.client;
        let data = self.data.take().expect("data already taken");
        let options = self.options;
        let policy = self.retry.unwrap_or_else(|| client.config.retry.clone());
        Box::pin(async move {
            options.validate()?;
            let max_inflight = options.max_inflight.max(1);
            let batches = data.into_lp_batches(&options);

            if max_inflight == 1 {
                for batch in batches {
                    let bytes = batch?;
                    client.send_lp(bytes, &options, &policy).await?;
                }
                return Ok(());
            }

            let options = Arc::new(options);
            let policy = Arc::new(policy);
            stream::iter(batches)
                .map(|b| b.map(|bytes| (bytes, Arc::clone(&options), Arc::clone(&policy))))
                .try_for_each_concurrent(Some(max_inflight), |(bytes, opts, pol)| async move {
                    client.send_lp(bytes, &opts, &pol).await
                })
                .await
        })
    }
}

/// Builder produced by [`Client::sql`], [`Client::influxql`], or
/// [`Client::query`]; chain options, then `.await` (for a collected
/// [`QueryResult`]) or `.stream()` (for a streaming [`BatchStream`]).
pub struct QueryRequest<'a> {
    client: &'a Client,
    query: String,
    query_type: QueryType,
    params: QueryParameters,
    headers: HashMap<String, String>,
    retry: Option<RetryConfig>,
}

impl<'a> QueryRequest<'a> {
    /// Add a single named parameter.
    pub fn param(mut self, key: impl Into<String>, value: impl Into<serde_json::Value>) -> Self {
        self.params.insert(key.into(), value.into());
        self
    }

    /// Add multiple named parameters from an iterable.
    pub fn params<K, V, I>(mut self, params: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<serde_json::Value>,
    {
        for (k, v) in params {
            self.params.insert(k.into(), v.into());
        }
        self
    }

    /// Add a gRPC metadata header sent with the Flight DoGet request.
    pub fn header(mut self, k: impl Into<String>, v: impl Into<String>) -> Self {
        self.headers.insert(k.into(), v.into());
        self
    }

    /// Override the retry policy for this query (defaults to the client's).
    pub fn retry(mut self, policy: RetryConfig) -> Self {
        self.retry = Some(policy);
        self
    }

    /// Disable retries for this query.
    pub fn no_retry(mut self) -> Self {
        self.retry = Some(RetryConfig::disabled());
        self
    }

    /// Open the query as a streaming [`BatchStream`] instead of collecting.
    /// Use this for results too large to materialise in memory.
    ///
    /// Only the connection setup is retried; once batches start arriving a
    /// stream can't be replayed, so mid-stream errors propagate to the caller.
    pub async fn stream(self) -> Result<BatchStream> {
        let policy = self
            .retry
            .clone()
            .unwrap_or_else(|| self.client.config.retry.clone());
        let opts = QueryOptions {
            query_type: self.query_type,
            headers: self.headers,
        };
        let params = (!self.params.is_empty()).then_some(self.params);

        let mut attempt: u32 = 0;
        loop {
            let result = async {
                let flight = self.client.flight().await?;
                flight
                    .stream(
                        &self.query,
                        &self.client.config.database,
                        &opts,
                        params.as_ref(),
                    )
                    .await
            }
            .await;

            match result {
                Ok(s) => return Ok(s),
                Err(e) => {
                    if attempt >= policy.max_retries || !is_retryable_query_err(&e) {
                        return Err(e);
                    }
                    tokio::time::sleep(policy.backoff(attempt)).await;
                    attempt += 1;
                }
            }
        }
    }
}

/// Whether a query error is a transient failure worth retrying.
fn is_retryable_query_err(e: &Error) -> bool {
    match e {
        Error::Flight(status) => retry::retryable_tonic(status.code()),
        Error::Transport(_) => true,
        Error::Timeout(_) => true,
        _ => false,
    }
}

impl<'a> IntoFuture for QueryRequest<'a> {
    type Output = Result<QueryResult>;
    type IntoFuture = Pin<Box<dyn std::future::Future<Output = Self::Output> + Send + 'a>>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move {
            let timeout = self.client.config.query_timeout;
            let policy = self
                .retry
                .clone()
                .unwrap_or_else(|| self.client.config.retry.clone());
            let opts = QueryOptions {
                query_type: self.query_type,
                headers: self.headers,
            };
            let params = (!self.params.is_empty()).then_some(self.params);

            let start = Instant::now();
            let mut attempt: u32 = 0;
            loop {
                // `timeout` is per attempt; collected queries are read-only and
                // safe to re-issue in full.
                let fut = async {
                    let flight = self.client.flight().await?;
                    flight
                        .query(
                            &self.query,
                            &self.client.config.database,
                            &opts,
                            params.as_ref(),
                        )
                        .await
                };
                let outcome = match tokio::time::timeout(timeout, fut).await {
                    Ok(inner) => inner,
                    Err(_) => Err(Error::Timeout(timeout)),
                };

                match outcome {
                    Ok(result) => return Ok(result),
                    Err(e) => {
                        if attempt >= policy.max_retries || !is_retryable_query_err(&e) {
                            return Err(e);
                        }
                        let delay = policy.backoff(attempt);
                        if let Some(budget) = policy.max_elapsed {
                            if start.elapsed() + delay > budget {
                                return Err(e);
                            }
                        }
                        tokio::time::sleep(delay).await;
                        attempt += 1;
                    }
                }
            }
        })
    }
}

fn build_http_client(config: &ClientConfig) -> Result<HttpClient> {
    let mut builder = ClientBuilder::new()
        .timeout(config.write_timeout)
        .pool_idle_timeout(config.idle_connection_timeout)
        .pool_max_idle_per_host(config.max_idle_connections)
        .gzip(true)
        .use_rustls_tls();

    if let Some(proxy_url) = &config.proxy {
        let proxy = reqwest::Proxy::all(proxy_url)
            .map_err(|e| Error::Config(format!("invalid proxy URL: {e}")))?;
        builder = builder.proxy(proxy);
    }

    if let Some(roots_path) = &config.ssl_roots_path {
        let pem = std::fs::read(roots_path)
            .map_err(|e| Error::Config(format!("cannot read SSL roots '{roots_path}': {e}")))?;
        let cert = reqwest::tls::Certificate::from_pem(&pem)
            .map_err(|e| Error::Config(format!("invalid SSL roots PEM: {e}")))?;
        builder = builder.add_root_certificate(cert);
    }

    Ok(builder.build()?)
}

/// Threshold above which gzip compression runs on a blocking thread pool.
/// For smaller bodies the spawn_blocking overhead dominates the compression cost.
const SPAWN_BLOCKING_GZIP_THRESHOLD: usize = 64 * 1024;

/// Maybe gzip-compress a body.  Returns `(body_bytes, was_compressed)`.
async fn maybe_gzip(data: Vec<u8>, threshold: Option<usize>) -> Result<(Bytes, bool)> {
    let should_compress = matches!(threshold, Some(t) if data.len() > t);
    if !should_compress {
        return Ok((Bytes::from(data), false));
    }

    if data.len() < SPAWN_BLOCKING_GZIP_THRESHOLD {
        let compressed = gzip_compress(data)?;
        return Ok((Bytes::from(compressed), true));
    }

    let compressed = tokio::task::spawn_blocking(move || gzip_compress(data))
        .await
        .map_err(|e| Error::Config(format!("gzip task join error: {e}")))??;
    Ok((Bytes::from(compressed), true))
}

fn gzip_compress(data: Vec<u8>) -> Result<Vec<u8>> {
    let mut encoder = flate2::write::GzEncoder::new(
        Vec::with_capacity(data.len() / 2),
        flate2::Compression::default(),
    );
    encoder
        .write_all(&data)
        .map_err(|e| Error::Config(format!("gzip encoding failed: {e}")))?;
    encoder
        .finish()
        .map_err(|e| Error::Config(format!("gzip finalization failed: {e}")))
}

/// Outcome of a single write attempt, before any backoff decision.
enum WriteAttempt {
    Ok,
    /// Transient, safe to retry. `last` is surfaced if retries are exhausted;
    /// `after` is the server-suggested delay from `Retry-After`, if any.
    Retry {
        after: Option<Duration>,
        last: Error,
    },
    /// Deterministic; do not retry (auth, bad request, partial write, etc.).
    Fatal(Error),
}

/// Send one write request and classify the result into a [`WriteAttempt`].
async fn send_write_once(req: reqwest::RequestBuilder) -> WriteAttempt {
    match req.send().await {
        Err(e) => {
            let retryable = retry::retryable_reqwest(&e);
            let err = Error::Http(e);
            if retryable {
                WriteAttempt::Retry {
                    after: None,
                    last: err,
                }
            } else {
                WriteAttempt::Fatal(err)
            }
        }
        Ok(resp) => {
            let status = resp.status();
            if status.is_success() {
                return WriteAttempt::Ok;
            }
            let code = status.as_u16();
            let retry_after = resp
                .headers()
                .get(RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(retry::parse_retry_after);
            // A partial write is a 400, so it falls through to Fatal here:
            // a deterministic data error, never a transient one.
            let err = parse_write_error(code, resp).await;
            if retry::retryable_status(code) {
                WriteAttempt::Retry {
                    after: retry_after,
                    last: err,
                }
            } else {
                WriteAttempt::Fatal(err)
            }
        }
    }
}

/// Parse a non-2xx write response body into an [`Error`] (partial write vs server).
async fn parse_write_error(code: u16, resp: Response) -> Error {
    let body = resp.text().await.unwrap_or_default();

    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
        let is_partial = v
            .get("error")
            .and_then(|e| e.as_str())
            .map(|s| s.contains("partial write"))
            .unwrap_or(false);

        if is_partial && v.get("data").and_then(|d| d.as_array()).is_some() {
            return Error::PartialWrite(PartialWriteError {
                line_errors: parse_line_errors(&v),
            });
        }

        let msg = v
            .get("error")
            .or_else(|| v.get("message"))
            .and_then(|m| m.as_str())
            .unwrap_or(&body)
            .to_owned();

        return Error::Server { code, message: msg };
    }

    Error::Server {
        code,
        message: body,
    }
}

fn parse_line_errors(v: &serde_json::Value) -> Vec<LineError> {
    v.get("data")
        .and_then(|d| d.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|e| {
                    Some(LineError {
                        line: e.get("line_number")?.as_u64()?,
                        message: e.get("error_message")?.as_str()?.to_owned(),
                        original_line: e
                            .get("original_line")
                            .and_then(|s| s.as_str())
                            .map(str::to_owned),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}
