use std::collections::HashMap;

use crate::{error::Error, point::Point, precision::Precision};

/// Options controlling a single write operation.
///
/// Defaults live in [`crate::ClientConfig::write_options`]; individual writes
/// override values via the [`crate::Client::write`] builder.
#[derive(Debug, Clone)]
pub struct WriteOptions {
    /// Timestamp precision for this write.  Defaults to `Nanosecond`.
    pub precision: Precision,

    /// Tags merged into every point before serialisation.
    /// Point-level tags take precedence on collision.
    pub default_tags: HashMap<String, String>,

    /// If `Some(n)`, compress the body with gzip when it exceeds `n` bytes.
    /// `Some(0)` always compresses; `None` never compresses. Defaults to
    /// `Some(1024)`.
    ///
    /// Compression trades CPU for bandwidth. The default suits remote/cloud
    /// targets where bandwidth dominates. For high-throughput ingest over a
    /// fast LAN (flight-test, IIoT), gzip CPU can become the bottleneck. Set
    /// `gzip_threshold(None)` to disable it, or raise the threshold so only
    /// large batches are compressed.
    pub gzip_threshold: Option<usize>,

    /// When `true`, skip WAL synchronisation (faster, lower durability).
    pub no_sync: bool,

    /// When `true`, a batch is accepted even if some lines are invalid.
    pub accept_partial: bool,

    /// When `true`, use the V2 (`/api/v2/write`) endpoint instead of V3.
    pub use_v2_api: bool,

    /// Optional tag ordering for deterministic line-protocol output.
    pub tag_order: Vec<String>,

    /// Maximum number of points per HTTP request when calling `write`.
    /// Larger inputs are streamed as multiple sequential or pipelined requests.
    /// Defaults to `5_000`.
    ///
    /// This is a point count, not a byte size. The effective ceiling is the
    /// server's maximum request size (configurable on InfluxDB, 10 MB by
    /// default); if you raise `batch_size` into a `413`, raise that limit too.
    pub batch_size: usize,

    /// Maximum number of concurrent in-flight HTTP requests when writing
    /// multiple batches.  Defaults to `4`.  Set to `1` for strict ordering.
    pub max_inflight: usize,
}

/// Default maximum number of points per write request.
pub const DEFAULT_BATCH_SIZE: usize = 5_000;

/// Default maximum number of concurrent in-flight HTTP write requests.
pub const DEFAULT_MAX_INFLIGHT: usize = 4;

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            precision: Precision::Nanosecond,
            default_tags: HashMap::new(),
            gzip_threshold: Some(1024),
            no_sync: false,
            accept_partial: true,
            use_v2_api: true,
            tag_order: Vec::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            max_inflight: DEFAULT_MAX_INFLIGHT,
        }
    }
}

impl WriteOptions {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.use_v2_api && self.no_sync {
            return Err(Error::Config(
                "invalid write options: no_sync requires use_v2_api=false".into(),
            ));
        }
        Ok(())
    }
}

/// A type that can be lazily serialised to InfluxDB line protocol for writing.
///
/// Pass anything that implements this trait to [`crate::Client::write`].
///
/// | Type | Use case |
/// |---|---|
/// | `&str` / `String` | Pre-formatted line protocol (low-level escape hatch) |
/// | `Vec<Point>` | Point builder API (pass ownership; clone a slice with `.to_vec()` if you must keep it) |
/// | [`crate::write_dataframe::DataFrameWrite`] | polars DataFrame (`polars` feature) |
///
/// Implementations return an iterator that yields **one batch per HTTP
/// request**. The iterator is consumed lazily, so only one batch buffer lives
/// in memory at a time even for million-point writes.
pub trait WriteInput {
    /// Lazily produce line-protocol batches, one per HTTP request.
    ///
    /// Implementations should respect `opts.batch_size`.  Errors per batch are
    /// returned in the iterator so partially-valid inputs can still send what
    /// they can.
    fn into_lp_batches(
        self,
        opts: &WriteOptions,
    ) -> Box<dyn Iterator<Item = crate::Result<Vec<u8>>> + Send>;
}

impl WriteInput for &str {
    fn into_lp_batches(
        self,
        _opts: &WriteOptions,
    ) -> Box<dyn Iterator<Item = crate::Result<Vec<u8>>> + Send> {
        if self.is_empty() {
            Box::new(std::iter::empty())
        } else {
            Box::new(std::iter::once(Ok(self.as_bytes().to_vec())))
        }
    }
}

impl WriteInput for String {
    fn into_lp_batches(
        self,
        _opts: &WriteOptions,
    ) -> Box<dyn Iterator<Item = crate::Result<Vec<u8>>> + Send> {
        if self.is_empty() {
            Box::new(std::iter::empty())
        } else {
            Box::new(std::iter::once(Ok(self.into_bytes())))
        }
    }
}

/// Lazy iterator that serialises chunks of points into LP buffers on demand.
pub(crate) struct PointBatchIter {
    points: Vec<Point>,
    idx: usize,
    batch_size: usize,
    precision: Precision,
    default_tags: HashMap<String, String>,
    tag_order: Vec<String>,
}

impl Iterator for PointBatchIter {
    type Item = crate::Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.idx >= self.points.len() {
            return None;
        }
        let end = (self.idx + self.batch_size).min(self.points.len());
        // Pre-size at roughly 64 bytes per point.
        let mut buf = Vec::with_capacity((end - self.idx) * 64);
        let tag_order = if self.tag_order.is_empty() {
            None
        } else {
            Some(self.tag_order.as_slice())
        };
        // One scratch buffer reused for every point in the batch.
        let mut key_scratch = Vec::new();
        for point in &self.points[self.idx..end] {
            if let Err(e) = point.write_line_protocol(
                &mut buf,
                self.precision,
                &self.default_tags,
                tag_order,
                &mut key_scratch,
            ) {
                self.idx = self.points.len(); // stop iteration after error
                return Some(Err(e));
            }
            buf.push(b'\n');
        }
        // Drop the trailing newline.
        if buf.last() == Some(&b'\n') {
            buf.pop();
        }
        self.idx = end;
        Some(Ok(buf))
    }
}

impl WriteInput for Vec<Point> {
    fn into_lp_batches(
        self,
        opts: &WriteOptions,
    ) -> Box<dyn Iterator<Item = crate::Result<Vec<u8>>> + Send> {
        Box::new(PointBatchIter {
            points: self,
            idx: 0,
            batch_size: opts.batch_size.max(1),
            precision: opts.precision,
            default_tags: opts.default_tags.clone(),
            tag_order: opts.tag_order.clone(),
        })
    }
}
