use crate::classification::DataType;

/// Result of fast schema inference — column names paired with inferred data types.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SchemaResult {
    /// Columns with their inferred types. For CSV/Parquet the order matches
    /// the source; for JSON/JSONL columns are sorted alphabetically.
    pub columns: Vec<ColumnSchema>,
    /// How many rows were sampled to infer the schema (0 for Parquet metadata).
    pub rows_sampled: usize,
    /// Time taken for inference in milliseconds.
    pub inference_time_ms: u128,
    /// `true` when the entire file was consumed or schema was read from
    /// metadata; `false` when inference stopped at the sample-size cap and
    /// the schema may not have fully stabilized.
    pub schema_stable: bool,
}

/// A single column's name and inferred data type.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub data_type: DataType,
}

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
