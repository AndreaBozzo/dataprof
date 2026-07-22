use crate::{classification::DataType, source::FileFormat};

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

/// Lightweight structural summary for a file.
///
/// This intentionally sits between `quick_row_count()` / `infer_schema()` and a
/// full profile: it reports shape and sampled per-column counters, not quality
/// scores, pattern detection, recommendations, or raw values.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StructureReport {
    /// Source identifier, currently the file path passed by the caller.
    pub source: String,
    /// Detected or explicitly provided file format.
    pub format: FileFormat,
    /// Exact or estimated row count for the source.
    pub row_count: RowCountEstimate,
    /// Number of rows consumed for the structural column summaries.
    pub rows_sampled: usize,
    /// True when the structural pass consumed the whole source.
    pub source_exhausted: bool,
    /// True when the structural pass stopped at the row sample cap.
    pub truncated: bool,
    /// Human-readable reason for truncation, if any.
    pub truncation_reason: Option<String>,
    /// CSV delimiter used for parsing, when applicable.
    pub delimiter: Option<String>,
    /// Per-column structural summaries.
    pub columns: Vec<StructureColumnSummary>,
    /// Non-fatal caveats, such as estimated row counts.
    pub warnings: Vec<String>,
}

/// Sample-derived or metadata-derived structural summary for one column.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct StructureColumnSummary {
    pub name: String,
    pub data_type: DataType,
    pub total_count: Option<usize>,
    pub null_count: Option<usize>,
    pub null_ratio: Option<f64>,
    pub unique_count: Option<usize>,
    pub uniqueness_ratio: Option<f64>,
    pub distinct_count_approximate: Option<bool>,
    /// `"sample"` for CSV/JSON/JSONL, `"metadata"` for Parquet.
    pub provenance: String,
}

/// Result of a quick row count operation.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RowCountEstimate {
    /// The estimated or exact row count.
    pub count: u64,
    /// Whether the count is exact or an estimate.
    pub exact: bool,
    /// How the count was obtained.
    pub method: CountMethod,
    /// Time taken in milliseconds.
    pub count_time_ms: u128,
    /// Approximate 1-sigma relative standard error of the estimate, expressed
    /// as a fraction of `count` (e.g. `0.02` ≈ ±2%). Populated only for
    /// sampled estimates where a variance across sampling windows is available;
    /// `None` for exact counts and when it cannot be estimated.
    #[serde(default)]
    pub relative_error: Option<f64>,
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
