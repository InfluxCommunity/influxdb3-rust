use thiserror::Error;

/// Describes a single rejected line in a partial write response.
#[derive(Debug, Clone)]
pub struct LineError {
    /// 1-based line number in the submitted batch.
    pub line: Option<u64>,
    /// Error message from the server.
    pub message: String,
    /// The (possibly truncated) original line as echoed by the server.
    pub original_line: Option<String>,
}

impl std::fmt::Display for LineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match (self.line, self.original_line.as_deref()) {
            (Some(line), Some(original_line)) => {
                write!(f, "line {line}: {} ({original_line})", self.message)
            }
            (Some(line), None) => write!(f, "line {line}: {}", self.message),
            (None, _) => write!(f, "{}", self.message),
        }
    }
}

/// Returned when `accept_partial=true` and the server rejected one or more lines.
///
/// The server accepts the valid lines and returns HTTP 400 with a JSON body
/// listing every rejected line. Check `line_errors` for details.
#[derive(Debug, Error)]
#[error("{message}")]
pub struct PartialWriteError {
    pub message: String,
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

    /// An operation exceeded its configured timeout
    #[error("operation timed out after {0:?}")]
    Timeout(std::time::Duration),

    /// Server accepted some lines and rejected others
    #[error(transparent)]
    PartialWrite(#[from] PartialWriteError),

    /// Bad client configuration (missing required field, etc.)
    #[error("configuration error: {0}")]
    Config(String),

    /// Required environment variable was not set
    #[error("environment variable '{0}' is not set")]
    EnvVar(String),

    /// Query result contained an Arrow data type this client cannot decode.
    #[error("unsupported Arrow data type in query result: {data_type}")]
    UnsupportedArrowType { data_type: String },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_line_error_display() {
        let err1 = LineError {
            line: Some(1),
            message: "parsing error".to_string(),
            original_line: Some("cpu x=1".to_string()),
        };
        assert_eq!(err1.to_string(), "line 1: parsing error (cpu x=1)");

        let err2 = LineError {
            line: Some(2),
            message: "type mismatch".to_string(),
            original_line: None,
        };
        assert_eq!(err2.to_string(), "line 2: type mismatch");

        let err3 = LineError {
            line: None,
            message: "generic error".to_string(),
            original_line: None,
        };
        assert_eq!(err3.to_string(), "generic error");

        let err4 = LineError {
            line: None,
            message: "generic error with line".to_string(),
            original_line: Some("cpu x=1".to_string()),
        };
        assert_eq!(err4.to_string(), "generic error with line");
    }
}

#[test]
fn test_partial_write_error_display() {
    let error = PartialWriteError {
        message: "partial write of line protocol occurred:\n\tline 2: bad value".to_string(),
        line_errors: vec![],
    };

    assert_eq!(
        error.to_string(),
        "partial write of line protocol occurred:\n\tline 2: bad value"
    );
}
