#[cfg(feature = "arrow")]
mod arrow_profiler;

#[cfg(feature = "parquet-async")]
mod async_http;

#[cfg(feature = "arrow")]
pub mod record_batch_analyzer;

#[cfg(feature = "parquet")]
mod parser;

#[cfg(feature = "arrow")]
pub use arrow_profiler::ArrowProfiler;

#[cfg(feature = "parquet-async")]
pub use async_http::{
    HttpParquetReader, analyze_parquet_async_http, analyze_parquet_async_http_dims,
};

#[cfg(feature = "arrow")]
pub use record_batch_analyzer::RecordBatchAnalyzer;

#[cfg(feature = "parquet")]
pub use parser::{
    ParquetConfig, analyze_parquet_with_config, analyze_parquet_with_config_dims,
    analyze_parquet_with_quality, analyze_parquet_with_quality_dims, is_parquet_file,
};
