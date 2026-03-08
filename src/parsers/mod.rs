pub mod csv;
pub mod json;
#[cfg(feature = "parquet")]
pub mod parquet;
#[allow(dead_code)]
pub(crate) mod robust_csv;

// New config-based API (#181 + #218)
pub use csv::{CsvParserConfig, analyze_csv_file, analyze_csv_from_reader};

// New config-based JSON API (#218)
pub use json::{JsonFormat, JsonParserConfig, analyze_json_file, analyze_json_from_reader};
#[cfg(feature = "parquet")]
pub use parquet::{ParquetConfig, analyze_parquet_with_config, analyze_parquet_with_quality};

// CsvDiagnostics remains public even though RobustCsvParser is now pub(crate)
pub use robust_csv::CsvDiagnostics;
