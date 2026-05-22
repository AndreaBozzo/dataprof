/// Result of a quick row count operation.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RowCountEstimate {
    /// The estimated or exact row count.
    pub count: u64,
    /// Whether the count is exact or an estimate.
    pub exact: bool,
    /// How the count was obtained.
    pub method: CountMethod,
    /// Time taken in milliseconds.
    pub count_time_ms: u128,
}

/// Method used to obtain the row count.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum CountMethod {
    /// Read from Parquet file footer metadata (exact, zero row reading).
    ParquetMetadata,
    /// Full scan of the file (exact).
    FullScan,
    /// Sample-based estimation (approximate).
    Sampling,
    /// Full scan of a streaming source (no file metadata available).
    StreamFullScan,
}
