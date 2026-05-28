use thiserror::Error;

/// Describes a single rejected line in a partial write response.
#[derive(Debug, Clone)]
pub struct LineError {
    /// 1-based line number in the submitted batch.
    pub line: u64,
    /// Error message from the server.
    pub message: String,
    /// The (possibly truncated) original line as echoed by the server.
    pub original_line: Option<String>,
}

impl std::fmt::Display for LineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "line {}: {}", self.line, self.message)
    }
}

/// Returned when `accept_partial=true` and the server rejected one or more lines.
///
/// The server accepts the valid lines and returns HTTP 400 with a JSON body
/// listing every rejected line. Check `line_errors` for details.
#[derive(Debug, Error)]
#[error(
    "partial write: {} line(s) rejected — first error: {}",
    line_errors.len(),
    line_errors.first().map(|e| e.message.as_str()).unwrap_or("unknown")
)]
pub struct PartialWriteError {
    pub line_errors: Vec<LineError>,
}

/// Top-level error type for the InfluxDB 3 client.
#[derive(Debug, Error)]
pub enum Error {
    /// HTTP transport error (connection refused, timeout, TLS, etc.)
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// Invalid URL (bad host format, etc.)
    #[error("invalid URL: {0}")]
    Url(#[from] url::ParseError),

    /// JSON serialization / deserialization failure
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),

    /// Arrow IPC or in-memory format error
    #[error("Arrow error: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// gRPC status returned by Arrow Flight
    #[error("Flight gRPC error: {0}")]
    Flight(#[from] tonic::Status),

    /// gRPC transport error (could not connect, TLS failure)
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    /// Server returned an error response (non-2xx HTTP)
    #[error("server error {code}: {message}")]
    Server { code: u16, message: String },

    /// Server accepted some lines and rejected others
    #[error(transparent)]
    PartialWrite(#[from] PartialWriteError),

    /// Bad client configuration (missing required field, etc.)
    #[error("configuration error: {0}")]
    Config(String),

    /// Required environment variable was not set
    #[error("environment variable '{0}' is not set")]
    EnvVar(String),
}
