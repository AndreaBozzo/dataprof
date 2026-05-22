#[cfg(feature = "arrow")]
pub mod record_batch_analyzer;

#[cfg(feature = "parquet")]
mod parser;

#[cfg(feature = "arrow")]
pub use record_batch_analyzer::RecordBatchAnalyzer;

#[cfg(feature = "parquet")]
pub use parser::{
    ParquetConfig, analyze_parquet_with_config, analyze_parquet_with_config_dims,
    analyze_parquet_with_quality, analyze_parquet_with_quality_dims, is_parquet_file,
};
