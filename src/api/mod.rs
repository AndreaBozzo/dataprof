pub mod partial;

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::core::errors::DataProfilerError;
use crate::core::progress::{ProgressEvent, ProgressSink};
use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::core::stop_condition::StopCondition;
use crate::engines::adaptive::AdaptiveProfiler;
use crate::types::{DataSource, FileFormat, ProfileReport, RowCountEstimate, SchemaResult};

/// Which engine to use for profiling
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EngineType {
    /// Automatically select the best engine based on file characteristics (default)
    #[default]
    Auto,
    /// True streaming engine with bounded memory (online algorithms)
    Incremental,
    /// Arrow-based columnar engine for high-performance batch analysis
    Columnar,
}

/// Plain-data configuration for a profiler
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ProfilerConfig {
    pub engine: EngineType,
    pub chunk_size: ChunkSize,
    pub sampling: SamplingStrategy,
    pub memory_limit_mb: Option<usize>,
    pub format_override: Option<FileFormat>,
    pub stop_condition: StopCondition,
    pub progress_interval: Duration,
}

impl Default for ProfilerConfig {
    fn default() -> Self {
        Self {
            engine: EngineType::Auto,
            chunk_size: ChunkSize::Adaptive,
            sampling: SamplingStrategy::None,
            memory_limit_mb: None,
            format_override: None,
            stop_condition: StopCondition::Never,
            progress_interval: Duration::from_millis(500),
        }
    }
}

/// Unified profiler with builder pattern
///
/// This is the primary entry point for data profiling. It dispatches to the
/// appropriate internal engine based on the configured `EngineType`.
///
/// # Examples
///
/// ```no_run
/// use dataprof::{Profiler, EngineType};
///
/// // Simple usage with auto engine selection (recommended)
/// let report = Profiler::new().analyze_file("data.csv").unwrap();
///
/// // Explicit engine selection
/// let report = Profiler::new()
///     .engine(EngineType::Columnar)
///     .analyze_file("data.csv")
///     .unwrap();
/// ```
pub struct Profiler {
    config: ProfilerConfig,
    progress_sink: ProgressSink,
}

impl Profiler {
    /// Create a new profiler with default settings (Auto engine selection)
    pub fn new() -> Self {
        Self {
            config: ProfilerConfig::default(),
            progress_sink: ProgressSink::None,
        }
    }

    /// Create a profiler from an existing configuration
    pub fn with_config(config: ProfilerConfig) -> Self {
        Self {
            config,
            progress_sink: ProgressSink::None,
        }
    }

    /// Set the engine type
    pub fn engine(mut self, engine: EngineType) -> Self {
        self.config.engine = engine;
        self
    }

    /// Set the chunk size for streaming engines
    pub fn chunk_size(mut self, chunk_size: ChunkSize) -> Self {
        self.config.chunk_size = chunk_size;
        self
    }

    /// Set the sampling strategy
    pub fn sampling(mut self, strategy: SamplingStrategy) -> Self {
        self.config.sampling = strategy;
        self
    }

    /// Set the memory limit in megabytes (applies to Incremental and Columnar engines)
    pub fn memory_limit_mb(mut self, mb: usize) -> Self {
        self.config.memory_limit_mb = Some(mb);
        self
    }

    /// Set a stop condition for early termination.
    ///
    /// Only effective with `EngineType::Incremental`. Ignored for other engines.
    pub fn stop_when(mut self, condition: StopCondition) -> Self {
        self.config.stop_condition = condition;
        self
    }

    /// Override automatic format detection.
    ///
    /// By default the format is inferred from the file extension. Use this method
    /// when the extension is missing or misleading (e.g. a CSV file named `.dat`).
    pub fn format(mut self, format: FileFormat) -> Self {
        self.config.format_override = Some(format);
        self
    }

    /// Set the progress update interval (default: 500ms)
    pub fn progress_interval(mut self, interval: Duration) -> Self {
        self.config.progress_interval = interval;
        self
    }

    /// Set a progress sink for receiving structured progress events.
    ///
    /// Note: Progress events are only emitted by `EngineType::Incremental`.
    /// With `EngineType::Auto` and `EngineType::Columnar`, the sink is ignored.
    pub fn progress_sink(mut self, sink: ProgressSink) -> Self {
        self.progress_sink = sink;
        self
    }

    /// Set a synchronous callback for progress events (convenience method).
    ///
    /// Only effective with `EngineType::Incremental`. Ignored for other engines.
    pub fn on_progress<F>(mut self, callback: F) -> Self
    where
        F: Fn(ProgressEvent) + Send + Sync + 'static,
    {
        self.progress_sink = ProgressSink::Callback(Arc::new(callback));
        self
    }

    /// Set an async channel for progress events (requires `async-streaming` feature).
    #[cfg(feature = "async-streaming")]
    pub fn progress_channel(mut self, tx: tokio::sync::mpsc::Sender<ProgressEvent>) -> Self {
        self.progress_sink = ProgressSink::Channel(tx);
        self
    }

    /// Analyze a file and return a quality report
    pub fn analyze_file<P: AsRef<Path>>(
        &self,
        file_path: P,
    ) -> Result<ProfileReport, DataProfilerError> {
        let path = file_path.as_ref();
        let format = self
            .config
            .format_override
            .clone()
            .unwrap_or_else(|| Self::detect_format(path));

        match self.config.engine {
            EngineType::Auto => self.run_auto(path, format),
            EngineType::Incremental => self.run_incremental(path, format),
            EngineType::Columnar => self.run_columnar(path, format),
        }
    }

    /// Analyze a DataSource and return a quality report
    pub fn analyze_source(&self, source: &DataSource) -> Result<ProfileReport, DataProfilerError> {
        match source {
            DataSource::File { path, .. } => self.analyze_file(Path::new(path)),
            _ => Err(DataProfilerError::UnsupportedDataSource {
                message: "Only File DataSource is currently supported in synchronous API. \
                          Use async API for streams."
                    .to_string(),
            }),
        }
    }

    /// Infer the schema (column names + data types) of a file.
    ///
    /// Respects the builder's format override. This is much faster than a full
    /// `analyze_file` — it reads only a small sample (or just metadata for Parquet).
    pub fn infer_schema<P: AsRef<Path>>(&self, path: P) -> Result<SchemaResult, DataProfilerError> {
        let path = path.as_ref();
        let format = self
            .config
            .format_override
            .clone()
            .unwrap_or_else(|| Self::detect_format(path));
        partial::infer_schema_with_format(path, format)
    }

    /// Quick row count (exact or estimated) for a file.
    ///
    /// Respects the builder's format override. Returns an exact count for small
    /// files and Parquet; an estimate for large CSV/JSON files.
    pub fn quick_row_count<P: AsRef<Path>>(
        &self,
        path: P,
    ) -> Result<RowCountEstimate, DataProfilerError> {
        let path = path.as_ref();
        let format = self
            .config
            .format_override
            .clone()
            .unwrap_or_else(|| Self::detect_format(path));
        partial::quick_row_count_with_format(path, format)
    }

    /// Detect file format from extension
    pub(crate) fn detect_format(file_path: &Path) -> FileFormat {
        file_path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| match ext.to_ascii_lowercase().as_str() {
                "csv" | "tsv" | "txt" => FileFormat::Csv,
                "json" => FileFormat::Json,
                "jsonl" | "ndjson" => FileFormat::Jsonl,
                "parquet" => FileFormat::Parquet,
                other => FileFormat::Unknown(other.to_string()),
            })
            .unwrap_or(FileFormat::Csv) // default to CSV for extensionless files
    }

    /// Dispatch via AdaptiveProfiler, with format-aware routing for JSON
    fn run_auto(
        &self,
        file_path: &Path,
        format: FileFormat,
    ) -> Result<ProfileReport, DataProfilerError> {
        // AdaptiveProfiler handles Parquet and CSV natively, but not JSON
        match format {
            FileFormat::Json | FileFormat::Jsonl => crate::parsers::json::analyze_json_file(
                file_path,
                &crate::parsers::json::JsonParserConfig::default(),
            ),
            _ => AdaptiveProfiler::new().analyze_file(file_path),
        }
    }

    /// Dispatch to IncrementalProfiler with all configured options
    fn run_incremental(
        &self,
        file_path: &Path,
        format: FileFormat,
    ) -> Result<ProfileReport, DataProfilerError> {
        // IncrementalProfiler only supports CSV
        match format {
            FileFormat::Json | FileFormat::Jsonl => {
                return crate::parsers::json::analyze_json_file(
                    file_path,
                    &crate::parsers::json::JsonParserConfig::default(),
                );
            }
            FileFormat::Parquet => {
                return crate::parsers::parquet::analyze_parquet_with_quality(file_path);
            }
            _ => {}
        }

        use crate::engines::streaming::IncrementalProfiler;

        let profiler = IncrementalProfiler::new()
            .chunk_size(self.config.chunk_size.clone())
            .sampling(self.config.sampling.clone())
            .stop_condition(self.config.stop_condition.clone())
            .progress(self.progress_sink.clone(), self.config.progress_interval);

        profiler.analyze_file(file_path)
    }

    /// Dispatch to ArrowProfiler for CSV, or fall back to native parsers for other formats
    fn run_columnar(
        &self,
        file_path: &Path,
        format: FileFormat,
    ) -> Result<ProfileReport, DataProfilerError> {
        match format {
            FileFormat::Parquet => {
                return crate::parsers::parquet::analyze_parquet_with_quality(file_path);
            }
            FileFormat::Json | FileFormat::Jsonl => {
                return crate::parsers::json::analyze_json_file(
                    file_path,
                    &crate::parsers::json::JsonParserConfig::default(),
                );
            }
            _ => {}
        }

        // CSV — use ArrowProfiler
        use crate::engines::columnar::ArrowProfiler;
        let mut profiler = ArrowProfiler::new();
        if let Some(mb) = self.config.memory_limit_mb {
            profiler = profiler.memory_limit_mb(mb);
        }
        profiler.analyze_csv_file(file_path)
    }
}

impl Default for Profiler {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Async streaming API (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "async-streaming")]
impl Profiler {
    /// Profile data from any async byte stream.
    ///
    /// This is the primary async entry point for embedding dataprof in async
    /// services. The stream is consumed incrementally — memory usage is bounded
    /// regardless of total data size.
    ///
    /// Supports CSV, JSON, and JSONL formats. For Parquet, use [`profile_file`](Self::profile_file)
    /// or [`profile_url`](Self::profile_url) (requires `parquet-async` feature) since Parquet
    /// requires seeking to the file footer.
    ///
    /// Note: `EngineType` is ignored — async profiling always uses the streaming
    /// pipeline internally.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dataprof::{Profiler, AsyncSourceInfo, BytesSource, FileFormat};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let csv_data = b"name,age\nAlice,30\nBob,25\n";
    /// let source = BytesSource::new(
    ///     bytes::Bytes::from_static(csv_data),
    ///     AsyncSourceInfo {
    ///         label: "request-body".into(),
    ///         format: FileFormat::Csv,
    ///         size_hint: Some(csv_data.len() as u64),
    ///         source_system: None,
    ///     },
    /// );
    ///
    /// let report = Profiler::new().profile_stream(source).await?;
    /// println!("Columns: {}", report.execution.columns_detected);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn profile_stream(
        &self,
        source: impl crate::engines::streaming::AsyncDataSource,
    ) -> Result<ProfileReport, DataProfilerError> {
        use crate::engines::streaming::AsyncStreamingProfiler;

        let mut profiler = AsyncStreamingProfiler::new()
            .chunk_size(self.config.chunk_size.clone())
            .sampling(self.config.sampling.clone())
            .stop_condition(self.config.stop_condition.clone())
            .progress(self.progress_sink.clone(), self.config.progress_interval);

        if let Some(mb) = self.config.memory_limit_mb {
            profiler = profiler.memory_limit_mb(mb);
        }

        profiler.analyze_stream(source).await
    }

    /// Profile a local file asynchronously.
    ///
    /// Detects the format from the file extension (override with [`.format()`](Self::format)).
    /// All formats are supported, including Parquet (handled via `spawn_blocking`).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dataprof::Profiler;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let report = Profiler::new().profile_file("data.csv").await?;
    /// println!("Rows: {}", report.execution.rows_processed);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn profile_file<P: AsRef<Path>>(
        &self,
        file_path: P,
    ) -> Result<ProfileReport, DataProfilerError> {
        use crate::engines::streaming::async_source::AsyncSourceInfo;

        let path = file_path.as_ref();
        let format = self
            .config
            .format_override
            .clone()
            .unwrap_or_else(|| Self::detect_format(path));

        match format {
            FileFormat::Parquet => {
                // Parquet requires seeking — delegate to sync parser on a blocking thread.
                let path = path.to_path_buf();
                tokio::task::spawn_blocking(move || {
                    crate::parsers::parquet::analyze_parquet_with_quality(&path)
                })
                .await
                .map_err(|e| DataProfilerError::StreamingError {
                    message: format!("Blocking task failed: {e}"),
                })?
            }
            FileFormat::Csv | FileFormat::Json | FileFormat::Jsonl => {
                let metadata =
                    tokio::fs::metadata(path)
                        .await
                        .map_err(|e| DataProfilerError::IoError {
                            message: format!("{}: {e}", path.display()),
                        })?;
                let file =
                    tokio::fs::File::open(path)
                        .await
                        .map_err(|e| DataProfilerError::IoError {
                            message: format!("{}: {e}", path.display()),
                        })?;
                let info = AsyncSourceInfo {
                    label: path.display().to_string(),
                    format,
                    size_hint: Some(metadata.len()),
                    source_system: Some(crate::types::StreamSourceSystem::Custom("file".into())),
                };
                self.profile_stream((file, info)).await
            }
            FileFormat::Unknown(ref ext) => Err(DataProfilerError::UnsupportedDataSource {
                message: format!(
                    "Unknown file format '.{ext}'. Use .format() to override detection."
                ),
            }),
        }
    }

    /// Infer schema from any async byte stream.
    ///
    /// True async — no file path needed. Reads up to 1000 rows for
    /// CSV/JSON/JSONL. Parquet is not supported (requires seeking).
    pub async fn infer_schema_stream(
        &self,
        source: impl crate::engines::streaming::AsyncDataSource,
    ) -> Result<SchemaResult, DataProfilerError> {
        partial::infer_schema_stream(source).await
    }

    /// Quick row count from any async byte stream.
    ///
    /// True async — always a full scan (no sampling, since stream size is
    /// unknown). Parquet is not supported (requires seeking).
    pub async fn quick_row_count_stream(
        &self,
        source: impl crate::engines::streaming::AsyncDataSource,
    ) -> Result<RowCountEstimate, DataProfilerError> {
        partial::quick_row_count_stream(source).await
    }
}

#[cfg(feature = "parquet-async")]
impl Profiler {
    /// Profile data from a remote URL.
    ///
    /// Supports all formats. For Parquet, HTTP Range requests are used to read
    /// the footer without downloading the entire file. For CSV/JSON/JSONL, the
    /// response body is streamed incrementally.
    ///
    /// Format is detected from the URL path extension. Use [`.format()`](Self::format)
    /// to override when the URL has no extension (e.g., API endpoints).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dataprof::{Profiler, FileFormat};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let report = Profiler::new()
    ///     .format(FileFormat::Csv)
    ///     .profile_url("https://example.com/api/data")
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn profile_url(&self, url: &str) -> Result<ProfileReport, DataProfilerError> {
        use crate::engines::streaming::async_source::{AsyncSourceInfo, ReqwestSource};

        // Detect format from URL path, respecting format_override.
        // Extract the last path segment to avoid OS-specific Path parsing issues
        // (e.g., Windows treating "https:" as a drive prefix).
        let format = self.config.format_override.clone().unwrap_or_else(|| {
            let without_query = url.split('?').next().unwrap_or(url);
            let without_fragment = without_query.split('#').next().unwrap_or(without_query);
            let last_segment = without_fragment.rsplit('/').next().unwrap_or("");
            Self::detect_format(Path::new(last_segment))
        });

        match format {
            FileFormat::Parquet => {
                crate::parsers::parquet_async::analyze_parquet_async_http(
                    url,
                    &crate::parsers::parquet::ParquetConfig::default(),
                )
                .await
            }
            FileFormat::Csv | FileFormat::Json | FileFormat::Jsonl => {
                let response =
                    reqwest::get(url)
                        .await
                        .map_err(|e| DataProfilerError::StreamingError {
                            message: format!("HTTP request failed: {e}"),
                        })?;

                let status = response.status();
                if !status.is_success() {
                    return Err(DataProfilerError::StreamingError {
                        message: format!("HTTP {status} for {url}"),
                    });
                }

                let size_hint = response.content_length();
                let source = ReqwestSource::new(
                    response,
                    AsyncSourceInfo {
                        label: url.to_string(),
                        format,
                        size_hint,
                        source_system: Some(crate::types::StreamSourceSystem::Http),
                    },
                );
                self.profile_stream(source).await
            }
            FileFormat::Unknown(ref ext) => Err(DataProfilerError::UnsupportedDataSource {
                message: format!(
                    "Unknown format '.{ext}' in URL. Use .format() to specify the data format."
                ),
            }),
        }
    }
}

/// One-liner API for quick profiling with intelligent engine selection
pub fn quick_quality_check<P: AsRef<Path>>(file_path: P) -> Result<f64, DataProfilerError> {
    let profiler = Profiler::new();
    let report = profiler.analyze_file(file_path)?;
    Ok(report.quality_score().unwrap_or(0.0))
}

/// One-liner API for quick profiling from a DataSource
pub fn quick_quality_check_source(source: &DataSource) -> Result<f64, DataProfilerError> {
    let profiler = Profiler::new();
    let report = profiler.analyze_source(source)?;
    Ok(report.quality_score().unwrap_or(0.0))
}
