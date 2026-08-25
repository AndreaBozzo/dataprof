//! Parquet and Arrow profiling for `dataprof`.
//!
//! This crate is an implementation detail of the `dataprof` facade, which
//! re-exports `ParquetConfig`, `is_parquet_file`, `analyze_parquet_with_config`,
//! and `analyze_parquet_bytes` under its own `parquet` feature. Depend on
//! `dataprof` unless you need Parquet support without the rest of the
//! workspace.
//!
//! # Features
//!
//! Nothing is enabled by default, and every entry point below is gated, so the
//! names here are written as code rather than as links: a link to a gated item
//! does not resolve when the docs are built without its feature.
//!
//! - `arrow` — `RecordBatchAnalyzer` and `ArrowProfiler`, which profile Arrow
//!   record batches and CSV read through Arrow. Neither is re-exported by the
//!   facade; they are the entry points for callers who already hold Arrow data.
//! - `parquet` — the file and byte-buffer entry points in this crate's root.
//!   Implies `arrow`.
//! - `parquet-async` — `analyze_parquet_async_http` and `HttpParquetReader`,
//!   for reading a remote file over HTTP range requests. Implies `parquet`.
//!
//! Where an example names a shared type — `TruncationReason`, say — it reaches
//! for `dataprof_core`, since that is the crate a doctest here can link against.
//! The facade re-exports the same items as `dataprof::TruncationReason`, and a
//! caller depending on `dataprof` should use those.

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
    analyze_parquet_async_http_dims_with_hints, analyze_parquet_async_http_with_options,
};

#[cfg(feature = "arrow")]
pub use record_batch_analyzer::RecordBatchAnalyzer;

#[cfg(feature = "parquet")]
pub use parser::{
    ParquetConfig, analyze_parquet_bytes, analyze_parquet_bytes_with_options,
    analyze_parquet_with_config, analyze_parquet_with_config_dims,
    analyze_parquet_with_config_dims_and_hints, analyze_parquet_with_options,
    analyze_parquet_with_quality, analyze_parquet_with_quality_dims,
    analyze_parquet_with_quality_dims_and_hints, is_parquet_file,
};
