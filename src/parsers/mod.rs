pub mod csv;
pub mod json;
pub mod parquet;
#[cfg(feature = "parquet-async")]
pub mod parquet_async;
#[allow(dead_code)]
pub(crate) mod robust_csv;

// New config-based API (#181 + #218)
pub use csv::{CsvParserConfig, analyze_csv_file, analyze_csv_from_reader};

// New config-based JSON API (#218)
pub use json::{JsonFormat, JsonParserConfig, analyze_json_file, analyze_json_from_reader};
pub use parquet::{ParquetConfig, analyze_parquet_with_config, analyze_parquet_with_quality};
#[cfg(feature = "parquet-async")]
pub use parquet_async::{HttpParquetReader, analyze_parquet_async_http};

// CsvDiagnostics remains public even though RobustCsvParser is now pub(crate)
pub use robust_csv::CsvDiagnostics;
