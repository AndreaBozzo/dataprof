pub mod csv {
    pub use dataprof_csv::*;
}
pub mod json {
    pub use dataprof_json::*;
}
#[cfg(feature = "parquet")]
pub mod parquet;
#[cfg(feature = "parquet-async")]
pub mod parquet_async;

// New config-based API (#181 + #218)
pub use csv::{CsvParserConfig, analyze_csv_file, analyze_csv_from_reader};

// New config-based JSON API (#218)
pub use json::{JsonFormat, JsonParserConfig, analyze_json_file, analyze_json_from_reader};
#[cfg(feature = "parquet")]
pub use parquet::{ParquetConfig, analyze_parquet_with_config, analyze_parquet_with_quality};
#[cfg(feature = "parquet-async")]
pub use parquet_async::{HttpParquetReader, analyze_parquet_async_http};

pub use dataprof_csv::CsvDiagnostics;
