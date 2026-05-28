use std::collections::HashMap;

use crate::{point::Point, precision::Precision};

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
    /// `Some(0)` always compresses; `None` never compresses.
    pub gzip_threshold: Option<usize>,

    /// When `true`, skip WAL synchronisation (faster, lower durability).
    pub no_sync: bool,

    /// When `true`, a batch is accepted even if some lines are invalid.
    pub accept_partial: bool,

    /// When `true`, use the v2 (`/api/v2/write`) endpoint instead of v3.
    pub use_v2_api: bool,

    /// Optional tag ordering for deterministic line-protocol output.
    pub tag_order: Vec<String>,

    /// Maximum number of points per HTTP request when calling `write`.
    /// Larger inputs are streamed as multiple sequential or pipelined requests.
    /// Defaults to `5_000`.
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
            use_v2_api: false,
            tag_order: Vec::new(),
            batch_size: DEFAULT_BATCH_SIZE,
            max_inflight: DEFAULT_MAX_INFLIGHT,
        }
    }
}

impl WriteOptions {
    /// Create a builder starting from defaults.
    pub fn builder() -> WriteOptionsBuilder {
        WriteOptionsBuilder::default()
    }
}


/// Fluent builder for [`WriteOptions`].
#[derive(Debug, Default)]
pub struct WriteOptionsBuilder(WriteOptions);

impl WriteOptionsBuilder {
    pub fn precision(mut self, p: Precision) -> Self {
        self.0.precision = p;
        self
    }

    pub fn default_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.0.default_tags.insert(key.into(), value.into());
        self
    }

    pub fn default_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.0.default_tags = tags;
        self
    }

    /// `None` disables compression.
    pub fn gzip_threshold(mut self, threshold: Option<usize>) -> Self {
        self.0.gzip_threshold = threshold;
        self
    }

    pub fn no_sync(mut self, no_sync: bool) -> Self {
        self.0.no_sync = no_sync;
        self
    }

    pub fn accept_partial(mut self, accept_partial: bool) -> Self {
        self.0.accept_partial = accept_partial;
        self
    }

    pub fn use_v2_api(mut self, use_v2: bool) -> Self {
        self.0.use_v2_api = use_v2;
        self
    }

    /// Define the order in which tags are serialised.  Tags not listed appear
    /// alphabetically after the explicitly ordered ones.
    pub fn tag_order(mut self, order: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.0.tag_order = order.into_iter().map(Into::into).collect();
        self
    }

    /// Override the maximum number of points per HTTP request.
    /// Defaults to [`DEFAULT_BATCH_SIZE`] (5 000).
    pub fn batch_size(mut self, n: usize) -> Self {
        self.0.batch_size = n;
        self
    }

    /// Override the maximum number of concurrent in-flight HTTP requests.
    /// Defaults to [`DEFAULT_MAX_INFLIGHT`] (4).
    pub fn max_inflight(mut self, n: usize) -> Self {
        self.0.max_inflight = n;
        self
    }

    pub fn build(self) -> WriteOptions {
        self.0
    }
}


/// A type that can be lazily serialised to InfluxDB line protocol for writing.
///
/// Pass anything that implements this trait to [`crate::Client::write`].
///
/// | Type | Use case |
/// |---|---|
/// | `&str` / `String` | Pre-formatted line protocol — low-level escape hatch |
/// | `Vec<Point>` / `&[Point]` | Point builder API |
/// | [`crate::write_dataframe::DataFrameWrite`] | polars DataFrame (`polars` feature) |
///
/// Implementations return an iterator that yields **one batch per HTTP
/// request**.  The iterator is consumed lazily — only one batch buffer lives
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
        // Approx 64 bytes/point — avoids most reallocations during serialisation.
        let mut buf = Vec::with_capacity((end - self.idx) * 64);
        let tag_order = if self.tag_order.is_empty() {
            None
        } else {
            Some(self.tag_order.as_slice())
        };
        for point in &self.points[self.idx..end] {
            if let Err(e) = point.write_line_protocol(
                &mut buf,
                self.precision,
                &self.default_tags,
                tag_order,
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

impl WriteInput for &[Point] {
    fn into_lp_batches(
        self,
        opts: &WriteOptions,
    ) -> Box<dyn Iterator<Item = crate::Result<Vec<u8>>> + Send> {
        // Clone the slice into an owned Vec so the iterator can outlive the call.
        // For zero-copy alternative, users can implement WriteInput on their own type.
        self.to_vec().into_lp_batches(opts)
    }
}
