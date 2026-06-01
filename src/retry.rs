//! Retry policy and transient-failure classification.
//!
//! Retries apply only to *transient* conditions (`5xx`, `429`, and transport
//! errors) where the server never gave a definitive answer. Deterministic
//! failures (other `4xx`, and partial writes) are never retried: re-sending the
//! same bytes earns the same rejection.
//!
//! Re-sending is safe because line-protocol writes are idempotent: the same
//! `(series, timestamp, field)` written twice converges to one value, so a
//! batch the server partially applied before a dropped connection cannot be
//! double-counted on replay. Queries are read-only and likewise safe to
//! re-issue.

use std::time::Duration;

/// Controls automatic retries of transient write and query failures.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of *additional* attempts after the first. `0` disables
    /// retries entirely (the pre-0.2 behaviour).
    pub max_retries: u32,

    /// Initial backoff delay, before exponential growth and jitter.
    pub base_delay: Duration,

    /// Upper bound on any single backoff delay.
    pub max_delay: Duration,

    /// Exponential growth factor applied to `base_delay` each attempt.
    pub multiplier: f64,

    /// Honour the server's `Retry-After` header (delay-seconds form) when
    /// present, in preference to the computed backoff.
    pub honor_retry_after: bool,

    /// Optional cap on total time spent across all attempts. When the next
    /// backoff would exceed the budget, the last error is returned instead.
    pub max_elapsed: Option<Duration>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            max_retries: 3,
            base_delay: Duration::from_millis(250),
            max_delay: Duration::from_secs(30),
            multiplier: 2.0,
            honor_retry_after: true,
            max_elapsed: None,
        }
    }
}

impl RetryConfig {
    /// A policy that performs no retries.
    pub fn disabled() -> Self {
        RetryConfig {
            max_retries: 0,
            ..Default::default()
        }
    }

    /// Backoff delay for a zero-based `attempt` index, with full jitter:
    /// uniform random in `[0, min(max_delay, base_delay * multiplier^attempt)]`.
    ///
    /// Full jitter decorrelates the retries of concurrent in-flight batches so
    /// they don't stampede a recovering server in lock-step.
    pub(crate) fn backoff(&self, attempt: u32) -> Duration {
        let base = self.base_delay.as_millis() as f64;
        let cap = self.max_delay.as_millis() as f64;
        let ceiling = (base * self.multiplier.powi(attempt as i32))
            .min(cap)
            .max(0.0);
        Duration::from_millis((fastrand::f64() * ceiling) as u64)
    }
}

/// Whether an HTTP status code is a transient failure worth retrying.
pub(crate) fn retryable_status(code: u16) -> bool {
    matches!(code, 429 | 500 | 502 | 503 | 504)
}

/// Whether a reqwest transport error is transient (connect/timeout/send).
pub(crate) fn retryable_reqwest(e: &reqwest::Error) -> bool {
    e.is_timeout() || e.is_connect() || e.is_request()
}

/// Whether a tonic gRPC status code is transient.
pub(crate) fn retryable_tonic(code: tonic::Code) -> bool {
    use tonic::Code::*;
    matches!(
        code,
        Unavailable | ResourceExhausted | Internal | DeadlineExceeded | Aborted
    )
}

/// Parse a `Retry-After` header value in delay-seconds form. The HTTP-date form
/// is not honoured (returns `None`, falling back to computed backoff).
pub(crate) fn parse_retry_after(value: &str) -> Option<Duration> {
    value.trim().parse::<u64>().ok().map(Duration::from_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_bounded_by_max_delay() {
        let cfg = RetryConfig {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(5),
            multiplier: 2.0,
            ..Default::default()
        };
        for attempt in 0..12 {
            // Sample a few times since jitter is random.
            for _ in 0..16 {
                assert!(cfg.backoff(attempt) <= cfg.max_delay);
            }
        }
    }

    #[test]
    fn classification_table() {
        for code in [429, 500, 502, 503, 504] {
            assert!(retryable_status(code), "{code} should retry");
        }
        for code in [200, 400, 401, 403, 404, 422] {
            assert!(!retryable_status(code), "{code} should not retry");
        }
        assert!(retryable_tonic(tonic::Code::Unavailable));
        assert!(!retryable_tonic(tonic::Code::InvalidArgument));
        assert!(!retryable_tonic(tonic::Code::Unauthenticated));
    }

    #[test]
    fn retry_after_parsing() {
        assert_eq!(parse_retry_after("1"), Some(Duration::from_secs(1)));
        assert_eq!(parse_retry_after("  30 "), Some(Duration::from_secs(30)));
        assert_eq!(parse_retry_after("Wed, 21 Oct 2015 07:28:00 GMT"), None);
        assert_eq!(parse_retry_after("nonsense"), None);
    }
}
