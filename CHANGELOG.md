# Change Log

## 0.2.0 [unreleased]

### Features

1. [#12](https://github.com/InfluxCommunity/influxdb3-rust/pull/12): Expand client configuration support for environment variables and connection strings.
   Add auth scheme and write option support.
   Preserve explicit ports and strip userinfo from normalized hosts.
   Remove the legacy `bucket` and `INFLUX_BUCKET` aliases in favor of `database` and `INFLUX_DATABASE`.

### Bug Fixes

1. [#13](https://github.com/InfluxCommunity/influxdb3-rust/pull/13): Improve Arrow query result type support.
   Unsupported Arrow types now produce `Error::UnsupportedArrowType` instead of `Null`.

## 0.1.0 [2026-06-08]

Initial release.

### Features

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
