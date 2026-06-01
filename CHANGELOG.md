# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/).

## [0.1.0] - Unreleased

Initial release.

### Added

- Async client for InfluxDB 3 Core and Enterprise over HTTP (writes) and Arrow
  Flight (queries).
- Write API: a builder accepting line-protocol strings, `Vec<Point>`, and (with
  the `polars` feature) a DataFrame. Options for timestamp precision, batching,
  in-flight concurrency, default tags, gzip, and WAL no-sync.
- Query API: SQL and InfluxQL, parameterised queries, row iteration, and
  streaming of results too large to hold in memory.
- Automatic retries with exponential backoff and full jitter for transient
  failures (transport errors, `429`, `5xx`), honouring `Retry-After`.
  Configurable per client or per request.
- Partial-write error reporting with per-line detail.
- Optional `polars` feature: DataFrame writes and query-to-DataFrame conversion.

### Notes

- Retries are enabled by default (`max_retries = 3`). Use `.no_retry()` or a
  custom `RetryConfig` to change this.
