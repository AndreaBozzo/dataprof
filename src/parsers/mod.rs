pub mod csv;
pub mod json;
#[cfg(feature = "parquet")]
pub mod parquet;
pub mod robust_csv;

pub use csv::{analyze_csv, analyze_csv_robust, analyze_csv_with_sampling, try_strict_csv_parsing};
pub use json::{analyze_json, analyze_json_with_quality};
#[cfg(feature = "parquet")]
pub use parquet::{ParquetConfig, analyze_parquet_with_config, analyze_parquet_with_quality};
pub use robust_csv::{CsvDiagnostics, RobustCsvParser};
