use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use dataprof_core::{
    ChunkSize, DataProfilerError, DataSource, FileFormat, MetricPack, ProgressEvent, ProgressSink,
    QualityDimension, RowCountEstimate, SamplingStrategy, SchemaResult, SemanticHints,
    StopCondition,
};
#[cfg(feature = "database")]
use dataprof_db::DatabaseConfig;
use dataprof_engines::adaptive::AdaptiveProfiler;
use dataprof_partial as partial;
use dataprof_runtime::ProfileReport;

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
    /// Custom CSV delimiter (None = auto-detect).
    pub csv_delimiter: Option<u8>,
    /// Allow ragged CSV rows (None = use parser default).
    pub csv_flexible: Option<bool>,
    /// Which quality dimensions to compute. `None` = all (default).
    pub quality_dimensions: Option<Vec<QualityDimension>>,
    /// Which metric packs to compute. `None` = all (default).
    /// Controls whether statistics, patterns, and quality are included.
    pub metric_packs: Option<Vec<MetricPack>>,
    /// ISO 3166-1 alpha-2 locale for pattern detection (e.g. "IT", "US", "GB").
    /// When set, locale-matching patterns get a confidence boost and non-matching
    /// locale patterns are suppressed (unless they have a very high match rate).
    /// `None` = no locale preference (default).
    ///
    /// Applies to file-based (CSV/Parquet) and DataFrame/Arrow profiling engines.
    /// Database-backed (`analyze_query`) and async streaming entry points do not
    /// currently forward this setting.
    pub locale: Option<String>,
    /// Columns whose numeric values are expected to be non-negative.
    pub positive_columns: Vec<String>,
    /// Columns that should be treated as semantic identifiers, not measures.
    pub identifier_columns: Vec<String>,
    /// Database connection configuration. Required for `analyze_query()`.
    #[cfg(feature = "database")]
    pub database_config: Option<DatabaseConfig>,
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
            csv_delimiter: None,
            csv_flexible: None,
            quality_dimensions: None,
            metric_packs: None,
            locale: None,
            positive_columns: Vec::new(),
            identifier_columns: Vec::new(),
            #[cfg(feature = "database")]
            database_config: None,
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

    /// Set a custom CSV delimiter (single byte). None = auto-detect.
    pub fn csv_delimiter(mut self, delimiter: u8) -> Self {
        self.config.csv_delimiter = Some(delimiter);
        self
    }

    /// Set whether to allow ragged CSV rows. None = use parser default.
    pub fn csv_flexible(mut self, flexible: bool) -> Self {
        self.config.csv_flexible = Some(flexible);
        self
    }

    /// Set database connection configuration for `analyze_query()`.
    #[cfg(feature = "database")]
    pub fn database(mut self, config: DatabaseConfig) -> Self {
        self.config.database_config = Some(config);
        self
    }

    /// Convenience: set a database connection string with default config.
    #[cfg(feature = "database")]
    pub fn connection_string(mut self, conn: &str) -> Self {
        self.config.database_config = Some(DatabaseConfig {
            connection_string: conn.to_string(),
            ..Default::default()
        });
        self
    }

    /// Select which ISO 25012 quality dimensions to compute.
    ///
    /// By default all dimensions are evaluated. Call this method with a subset
    /// to skip the rest — dimensions that are not requested will appear as
    /// `None` in the report.
    pub fn quality_dimensions(mut self, dims: Vec<QualityDimension>) -> Self {
        self.config.quality_dimensions = Some(dims);
        self
    }

    /// Select which metric packs to compute.
    ///
    /// Controls high-level categories: `Schema` (always included),
    /// `Statistics`, `Patterns`, `Quality`. `None` = all (default).
    pub fn metric_packs(mut self, packs: Vec<MetricPack>) -> Self {
        self.config.metric_packs = Some(packs);
        self
    }

    /// Set the locale for pattern detection (e.g. "IT", "US", "GB").
    ///
    /// Locale-matching patterns get a confidence boost; non-matching locale
    /// patterns are suppressed unless they have a very high match rate.
    pub fn locale(mut self, locale: impl Into<String>) -> Self {
        self.config.locale = Some(locale.into());
        self
    }

    /// Mark columns whose numeric values are expected to be non-negative.
    pub fn positive_columns(mut self, columns: Vec<String>) -> Self {
        self.config.positive_columns = columns;
        self
    }

    /// Mark numeric-looking columns that should be profiled as identifiers.
    pub fn identifier_columns(mut self, columns: Vec<String>) -> Self {
        self.config.identifier_columns = columns;
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

    /// Analyze a DataSource and return a quality report.
    ///
    /// Supports `DataSource::File` synchronously. For `DataSource::Query`, use
    /// `analyze_source_async()` or `analyze_query()` (requires `database` feature).
    pub fn analyze_source(&self, source: &DataSource) -> Result<ProfileReport, DataProfilerError> {
        match source {
            DataSource::File { path, .. } => self.analyze_file(Path::new(path)),
            _ => Err(DataProfilerError::UnsupportedDataSource {
                message: "Only File DataSource is supported in synchronous API. \
                          Use analyze_source_async() for Query/Stream sources."
                    .to_string(),
            }),
        }
    }

    /// Analyze a DataSource asynchronously.
    ///
    /// Supports `DataSource::File` and `DataSource::Query`. For queries, a database
    /// connection must be configured via `.database()` or `.connection_string()`
    /// (requires `database` feature).
    pub async fn analyze_source_async(
        &self,
        source: &DataSource,
    ) -> Result<ProfileReport, DataProfilerError> {
        match source {
            DataSource::File { path, .. } => self.analyze_file(Path::new(path)),
            #[cfg(feature = "database")]
            DataSource::Query { statement, .. } => self.analyze_query(statement).await,
            _ => Err(DataProfilerError::UnsupportedDataSource {
                message: "Unsupported DataSource variant for this configuration.".to_string(),
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
    pub fn detect_format(file_path: &Path) -> FileFormat {
        partial::detect_format(file_path)
    }

    /// Whether custom CSV config options are set.
    fn has_csv_config(&self) -> bool {
        self.config.csv_delimiter.is_some() || self.config.csv_flexible.is_some()
    }

    /// Build a `CsvParserConfig` from the profiler's CSV settings.
    fn csv_parser_config(&self) -> dataprof_csv::CsvParserConfig {
        let mut csv_config = dataprof_csv::CsvParserConfig::default();
        if let Some(d) = self.config.csv_delimiter {
            csv_config = csv_config.with_delimiter(d);
        }
        if let Some(f) = self.config.csv_flexible {
            csv_config.flexible = f;
        }
        csv_config
    }

    fn semantic_hints(&self) -> SemanticHints {
        SemanticHints::new(
            self.config.positive_columns.clone(),
            self.config.identifier_columns.clone(),
        )
    }

    /// Build a `CsvParserConfig` for a CSV file, auto-detecting the delimiter
    /// when none was explicitly configured.
    fn csv_config_for_file(&self, file_path: &Path) -> dataprof_csv::CsvParserConfig {
        if self.has_csv_config() {
            self.csv_parser_config()
        } else {
            let detected = dataprof_csv::detect_delimiter_from_path(file_path).unwrap_or(b',');
            dataprof_csv::CsvParserConfig::default().with_delimiter(detected)
        }
    }

    /// Dispatch via AdaptiveProfiler, with format-aware routing for JSON/Parquet.
    ///
    /// Format dispatch happens here (not inside the adaptive engine) so that an
    /// explicit `.format()` override is respected even when the path extension
    /// would otherwise pick a different parser.
    fn run_auto(
        &self,
        file_path: &Path,
        format: FileFormat,
    ) -> Result<ProfileReport, DataProfilerError> {
        let dims = self.config.quality_dimensions.as_deref();
        let semantic_hints = self.semantic_hints();
        match format {
            FileFormat::Json | FileFormat::Jsonl => {
                dataprof_json::analyze_json_file_with_dimensions_and_hints(
                    file_path,
                    &dataprof_json::JsonParserConfig::default(),
                    dims,
                    &semantic_hints,
                )
            }
            FileFormat::Parquet => {
                #[cfg(feature = "parquet")]
                {
                    dataprof_parquet::analyze_parquet_with_quality_dims_and_hints(
                        file_path,
                        dims,
                        &semantic_hints,
                    )
                }
                #[cfg(not(feature = "parquet"))]
                {
                    Err(DataProfilerError::UnsupportedFormat {
                        format: "parquet (enable the `parquet` feature)".to_string(),
                    })
                }
            }
            _ => {
                let mut profiler = AdaptiveProfiler::new();
                if let Some(d) = &self.config.quality_dimensions {
                    profiler = profiler.quality_dimensions(d.clone());
                }
                if let Some(p) = &self.config.metric_packs {
                    profiler = profiler.metric_packs(p.clone());
                }
                if let Some(l) = &self.config.locale {
                    profiler = profiler.locale(l.clone());
                }
                profiler = profiler.semantic_hints(semantic_hints);
                let csv_config = self.csv_config_for_file(file_path);
                profiler = profiler.csv_config(csv_config);
                // Format already resolved here; skip the engine's extension-based
                // Parquet redetection so explicit `.format()` overrides win.
                profiler.analyze_csv_file(file_path)
            }
        }
    }

    /// Dispatch to IncrementalProfiler with all configured options
    fn run_incremental(
        &self,
        file_path: &Path,
        format: FileFormat,
    ) -> Result<ProfileReport, DataProfilerError> {
        let dims = self.config.quality_dimensions.as_deref();
        let semantic_hints = self.semantic_hints();
        // IncrementalProfiler only supports CSV
        match format {
            FileFormat::Json | FileFormat::Jsonl => {
                return dataprof_json::analyze_json_file_with_dimensions_and_hints(
                    file_path,
                    &dataprof_json::JsonParserConfig::default(),
                    dims,
                    &semantic_hints,
                );
            }
            FileFormat::Parquet => {
                #[cfg(feature = "parquet")]
                return dataprof_parquet::analyze_parquet_with_quality_dims_and_hints(
                    file_path,
                    dims,
                    &semantic_hints,
                );
                #[cfg(not(feature = "parquet"))]
                return Err(DataProfilerError::UnsupportedFormat {
                    format: "parquet (enable the `parquet` feature)".to_string(),
                });
            }
            _ => {}
        }

        use dataprof_engines::streaming::IncrementalProfiler;

        let mut profiler = IncrementalProfiler::new()
            .chunk_size(self.config.chunk_size.clone())
            .sampling(self.config.sampling.clone())
            .stop_condition(self.config.stop_condition.clone())
            .progress(self.progress_sink.clone(), self.config.progress_interval);
        if let Some(d) = &self.config.quality_dimensions {
            profiler = profiler.quality_dimensions(d.clone());
        }
        if let Some(p) = &self.config.metric_packs {
            profiler = profiler.metric_packs(p.clone());
        }
        if let Some(l) = &self.config.locale {
            profiler = profiler.locale(l.clone());
        }
        profiler = profiler.semantic_hints(semantic_hints);
        let csv_config = self.csv_config_for_file(file_path);
        profiler = profiler.csv_config(csv_config);

        profiler.analyze_file(file_path)
    }

    /// Dispatch to ArrowProfiler for CSV, or fall back to native parsers for other formats
    fn run_columnar(
        &self,
        file_path: &Path,
        format: FileFormat,
    ) -> Result<ProfileReport, DataProfilerError> {
        let dims = self.config.quality_dimensions.as_deref();
        let semantic_hints = self.semantic_hints();
        match format {
            FileFormat::Parquet => {
                #[cfg(feature = "parquet")]
                return dataprof_parquet::analyze_parquet_with_quality_dims_and_hints(
                    file_path,
                    dims,
                    &semantic_hints,
                );
                #[cfg(not(feature = "parquet"))]
                return Err(DataProfilerError::UnsupportedFormat {
                    format: "parquet (enable the `parquet` feature)".to_string(),
                });
            }
            FileFormat::Json | FileFormat::Jsonl => {
                return dataprof_json::analyze_json_file_with_dimensions_and_hints(
                    file_path,
                    &dataprof_json::JsonParserConfig::default(),
                    dims,
                    &semantic_hints,
                );
            }
            _ => {}
        }

        // CSV — use ArrowProfiler
        #[cfg(feature = "arrow")]
        {
            use dataprof_engines::columnar::ArrowProfiler;
            let mut profiler = ArrowProfiler::new();
            if let Some(mb) = self.config.memory_limit_mb {
                profiler = profiler.memory_limit_mb(mb);
            }
            if let Some(d) = &self.config.quality_dimensions {
                profiler = profiler.quality_dimensions(d.clone());
            }
            if let Some(p) = &self.config.metric_packs {
                profiler = profiler.metric_packs(p.clone());
            }
            if let Some(l) = &self.config.locale {
                profiler = profiler.locale(l.clone());
            }
            profiler = profiler.semantic_hints(semantic_hints);
            let csv_config = self.csv_config_for_file(file_path);
            profiler = profiler.csv_config(csv_config);
            profiler.analyze_csv_file(file_path)
        }
        #[cfg(not(feature = "arrow"))]
        {
            let _ = file_path;
            Err(DataProfilerError::UnsupportedFormat {
                format: "columnar engine (enable the `arrow` or `parquet` feature)".to_string(),
            })
        }
    }
}

impl Default for Profiler {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// Database API (feature-gated)
// ---------------------------------------------------------------------------

#[cfg(feature = "database")]
impl Profiler {
    /// Profile a database query or table asynchronously.
    ///
    /// Requires a database connection configured via `.database()` or `.connection_string()`.
    ///
    /// Quality metrics are computed by default. Use [`.quality_dimensions()`](Self::quality_dimensions)
    /// to select a subset.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use dataprof::Profiler;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let report = Profiler::new()
    ///     .connection_string("sqlite:///tmp/test.db")
    ///     .analyze_query("SELECT * FROM users")
    ///     .await?;
    /// println!("Rows: {}", report.execution.rows_processed);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn analyze_query(&self, query: &str) -> Result<ProfileReport, DataProfilerError> {
        let semantic_hints = self.semantic_hints();
        if !semantic_hints.is_empty() {
            return Err(DataProfilerError::UnsupportedDataSource {
                message: "positive_columns and identifier_columns are not supported for database profiling yet".to_string(),
            });
        }
        let config = self
            .config
            .database_config
            .clone()
            .ok_or(DataProfilerError::DatabaseConfigError {
            message:
                "No database connection configured. Use .database() or .connection_string() first."
                    .to_string(),
        })?;

        dataprof_db::analyze_database(config, query, true, self.config.quality_dimensions.clone())
            .await
    }

    /// Profile a database query without computing quality metrics.
    ///
    /// Faster than `analyze_query()` since it skips
    /// ISO 25012 quality metric computation.
    pub async fn analyze_query_no_quality(
        &self,
        query: &str,
    ) -> Result<ProfileReport, DataProfilerError> {
        let semantic_hints = self.semantic_hints();
        if !semantic_hints.is_empty() {
            return Err(DataProfilerError::UnsupportedDataSource {
                message: "positive_columns and identifier_columns are not supported for database profiling yet".to_string(),
            });
        }
        let config = self
            .config
            .database_config
            .clone()
            .ok_or(DataProfilerError::DatabaseConfigError {
            message:
                "No database connection configured. Use .database() or .connection_string() first."
                    .to_string(),
        })?;

        dataprof_db::analyze_database(config, query, false, None).await
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
    ///     AsyncSourceInfo::new("request-body", FileFormat::Csv)
    ///         .size_hint(Some(csv_data.len() as u64)),
    /// );
    ///
    /// let report = Profiler::new().profile_stream(source).await?;
    /// println!("Columns: {}", report.execution.columns_detected);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn profile_stream(
        &self,
        source: impl dataprof_engines::streaming::AsyncDataSource,
    ) -> Result<ProfileReport, DataProfilerError> {
        use dataprof_engines::streaming::AsyncStreamingProfiler;

        let mut profiler = AsyncStreamingProfiler::new()
            .chunk_size(self.config.chunk_size.clone())
            .sampling(self.config.sampling.clone())
            .stop_condition(self.config.stop_condition.clone())
            .progress(self.progress_sink.clone(), self.config.progress_interval);

        if let Some(mb) = self.config.memory_limit_mb {
            profiler = profiler.memory_limit_mb(mb);
        }
        if let Some(ref d) = self.config.quality_dimensions {
            profiler = profiler.quality_dimensions(d.clone());
        }
        if let Some(ref p) = self.config.metric_packs {
            profiler = profiler.metric_packs(p.clone());
        }
        profiler = profiler.semantic_hints(self.semantic_hints());

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
        use dataprof_engines::streaming::async_source::AsyncSourceInfo;

        let path = file_path.as_ref();
        let format = self
            .config
            .format_override
            .clone()
            .unwrap_or_else(|| Self::detect_format(path));

        match format {
            FileFormat::Parquet => {
                #[cfg(feature = "parquet")]
                {
                    // Parquet requires seeking — delegate to sync parser on a blocking thread.
                    let path = path.to_path_buf();
                    let dims = self.config.quality_dimensions.clone();
                    let semantic_hints = self.semantic_hints();
                    tokio::task::spawn_blocking(move || {
                        dataprof_parquet::analyze_parquet_with_quality_dims_and_hints(
                            &path,
                            dims.as_deref(),
                            &semantic_hints,
                        )
                    })
                    .await
                    .map_err(|e| DataProfilerError::StreamingError {
                        message: format!("Blocking task failed: {e}"),
                    })?
                }
                #[cfg(not(feature = "parquet"))]
                {
                    Err(DataProfilerError::UnsupportedFormat {
                        format: "parquet (enable the `parquet` feature)".to_string(),
                    })
                }
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
                let info = AsyncSourceInfo::new(path.display().to_string(), format)
                    .size_hint(Some(metadata.len()))
                    .source_system(dataprof_core::StreamSourceSystem::Custom("file".into()));
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
        source: impl dataprof_engines::streaming::AsyncDataSource,
    ) -> Result<SchemaResult, DataProfilerError> {
        partial::infer_schema_stream(source).await
    }

    /// Quick row count from any async byte stream.
    ///
    /// True async — always a full scan (no sampling, since stream size is
    /// unknown). Parquet is not supported (requires seeking).
    pub async fn quick_row_count_stream(
        &self,
        source: impl dataprof_engines::streaming::AsyncDataSource,
    ) -> Result<RowCountEstimate, DataProfilerError> {
        partial::quick_row_count_stream(source).await
    }
}

#[cfg(feature = "async-streaming")]
impl Profiler {
    /// Profile data from a remote URL.
    ///
    /// Supports CSV, JSON, and JSONL when `async-streaming` is enabled. Remote
    /// Parquet additionally requires the `parquet-async` feature so the footer
    /// can be fetched via HTTP Range requests without downloading the entire
    /// file.
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
        use dataprof_engines::streaming::async_source::{AsyncSourceInfo, ReqwestSource};

        // Validate URL shape up-front so callers get a clear error instead of
        // reqwest's opaque "builder error" when the scheme is missing or wrong.
        if !(url.starts_with("http://") || url.starts_with("https://")) {
            return Err(DataProfilerError::UnsupportedDataSource {
                message: format!(
                    "Invalid URL '{url}': expected an http:// or https:// scheme. \
                     For local files use `analyze_file()` instead."
                ),
            });
        }

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
                #[cfg(feature = "parquet-async")]
                {
                    return dataprof_parquet::analyze_parquet_async_http_dims_with_hints(
                        url,
                        &dataprof_parquet::ParquetConfig::default(),
                        self.config.quality_dimensions.clone(),
                        &self.semantic_hints(),
                    )
                    .await;
                }

                #[cfg(not(feature = "parquet-async"))]
                {
                    return Err(DataProfilerError::UnsupportedDataSource {
                        message: "Remote Parquet profiling requires the 'parquet-async' feature. Rebuild with 'parquet-async' to read Parquet over HTTP.".to_string(),
                    });
                }
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
                    AsyncSourceInfo::new(url.to_string(), format)
                        .size_hint(size_hint)
                        .source_system(dataprof_core::StreamSourceSystem::Http),
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
