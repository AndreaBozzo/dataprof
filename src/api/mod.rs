use std::path::Path;
use std::sync::Arc;

use crate::core::errors::DataProfilerError;
use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::engines::streaming::ProgressInfo;
use crate::engines::{AdaptiveProfiler, ProcessingType};
use crate::types::{DataSource, FileFormat, QualityReport};

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
pub struct ProfilerConfig {
    pub engine: EngineType,
    pub chunk_size: ChunkSize,
    pub sampling: SamplingStrategy,
    pub memory_limit_mb: Option<usize>,
}

impl Default for ProfilerConfig {
    fn default() -> Self {
        Self {
            engine: EngineType::Auto,
            chunk_size: ChunkSize::Adaptive,
            sampling: SamplingStrategy::None,
            memory_limit_mb: None,
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
    progress_callback: Option<Arc<dyn Fn(ProgressInfo) + Send + Sync>>,
    enhanced_progress: Option<usize>,
}

impl Profiler {
    /// Create a new profiler with default settings (Auto engine selection)
    pub fn new() -> Self {
        Self {
            config: ProfilerConfig::default(),
            progress_callback: None,
            enhanced_progress: None,
        }
    }

    /// Create a profiler from an existing configuration
    pub fn with_config(config: ProfilerConfig) -> Self {
        Self {
            config,
            progress_callback: None,
            enhanced_progress: None,
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

    /// Set a progress callback for real-time updates
    ///
    /// Note: Progress callbacks are only effective with `EngineType::Incremental`.
    /// With `EngineType::Auto` and `EngineType::Columnar`, progress callbacks are
    /// ignored and will not be invoked.
    pub fn progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(ProgressInfo) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Arc::new(callback));
        self
    }

    /// Enable enhanced progress tracking with memory monitoring
    ///
    /// Only effective with `EngineType::Incremental`. Ignored for other engines.
    pub fn with_enhanced_progress(mut self, leak_threshold_mb: usize) -> Self {
        self.enhanced_progress = Some(leak_threshold_mb);
        self
    }

    /// Enable enhanced progress with smart defaults based on terminal context
    ///
    /// Only effective with `EngineType::Incremental`. Ignored for other engines.
    pub fn with_smart_progress(mut self) -> Self {
        use crate::output::supports_enhanced_output;

        if supports_enhanced_output() {
            self.enhanced_progress = Some(100); // 100MB threshold
        }
        self
    }

    /// Analyze a file and return a quality report
    pub fn analyze_file<P: AsRef<Path>>(
        &self,
        file_path: P,
    ) -> Result<QualityReport, DataProfilerError> {
        let path = file_path.as_ref();

        match self.config.engine {
            EngineType::Auto => self.run_auto(path),
            EngineType::Incremental => self.run_incremental(path),
            EngineType::Columnar => self.run_columnar(path),
        }
    }

    /// Analyze a DataSource and return a quality report
    pub fn analyze_source(&self, source: &DataSource) -> Result<QualityReport, DataProfilerError> {
        match source {
            DataSource::File { path, .. } => self.analyze_file(Path::new(path)),
            _ => Err(DataProfilerError::UnsupportedDataSource {
                message: "Only File DataSource is currently supported in synchronous API. \
                          Use async API for streams."
                    .to_string(),
            }),
        }
    }

    /// Detect file format from extension
    fn detect_format(file_path: &Path) -> FileFormat {
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
    fn run_auto(&self, file_path: &Path) -> Result<QualityReport, DataProfilerError> {
        let format = Self::detect_format(file_path);

        // AdaptiveProfiler handles Parquet and CSV natively, but not JSON
        match format {
            FileFormat::Json | FileFormat::Jsonl => {
                crate::parsers::json::analyze_json_with_quality(file_path)
            }
            _ => {
                let profiler = AdaptiveProfiler::new().with_logging(false);
                profiler.analyze_file_with_context(file_path, ProcessingType::QualityFocused)
            }
        }
    }

    /// Dispatch to IncrementalProfiler with all configured options
    fn run_incremental(&self, file_path: &Path) -> Result<QualityReport, DataProfilerError> {
        let format = Self::detect_format(file_path);

        // IncrementalProfiler only supports CSV
        match format {
            FileFormat::Json | FileFormat::Jsonl => {
                return crate::parsers::json::analyze_json_with_quality(file_path);
            }
            FileFormat::Parquet => {
                #[cfg(feature = "parquet")]
                {
                    return crate::parsers::parquet::analyze_parquet_with_quality(file_path);
                }
                #[cfg(not(feature = "parquet"))]
                {
                    return Err(DataProfilerError::FeatureNotEnabled {
                        feature: "parquet".to_string(),
                        message: "Parquet file detected but parquet feature is not enabled. \
                                  Recompile with --features parquet"
                            .to_string(),
                    });
                }
            }
            _ => {}
        }

        use crate::engines::streaming::IncrementalProfiler;

        let mut profiler = IncrementalProfiler::new()
            .chunk_size(self.config.chunk_size.clone())
            .sampling(self.config.sampling.clone());

        if let Some(ref callback) = self.progress_callback {
            let cb = Arc::clone(callback);
            profiler = profiler.progress_callback(move |info| cb(info));
        }

        profiler.analyze_file(file_path)
    }

    /// Dispatch to ArrowProfiler for CSV, or fall back to native parsers for other formats
    fn run_columnar(&self, file_path: &Path) -> Result<QualityReport, DataProfilerError> {
        let format = Self::detect_format(file_path);

        match format {
            FileFormat::Parquet => {
                #[cfg(feature = "parquet")]
                {
                    return crate::parsers::parquet::analyze_parquet_with_quality(file_path);
                }
                #[cfg(not(feature = "parquet"))]
                {
                    return Err(DataProfilerError::FeatureNotEnabled {
                        feature: "parquet".to_string(),
                        message: "Parquet file detected but parquet feature is not enabled. \
                                  Recompile with --features parquet"
                            .to_string(),
                    });
                }
            }
            FileFormat::Json | FileFormat::Jsonl => {
                return crate::parsers::json::analyze_json_with_quality(file_path);
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

    // --- Deprecated compatibility methods ---

    #[deprecated(since = "0.6.0", note = "Use `Profiler::new()` which defaults to Auto")]
    pub fn auto() -> Self {
        Self::new()
    }

    #[deprecated(
        since = "0.6.0",
        note = "Use `Profiler::new().engine(EngineType::Incremental)`"
    )]
    pub fn streaming() -> Self {
        Self::new().engine(EngineType::Incremental)
    }
}

impl Default for Profiler {
    fn default() -> Self {
        Self::new()
    }
}

/// Deprecated: Use [`Profiler`] instead.
#[deprecated(since = "0.6.0", note = "Use `Profiler` instead")]
pub type DataProfiler = Profiler;

/// One-liner API for quick profiling with intelligent engine selection
pub fn quick_quality_check<P: AsRef<Path>>(file_path: P) -> Result<f64, DataProfilerError> {
    let profiler = Profiler::new();
    let report = profiler.analyze_file(file_path)?;
    Ok(report.quality_score())
}

/// One-liner API for quick profiling from a DataSource
pub fn quick_quality_check_source(source: &DataSource) -> Result<f64, DataProfilerError> {
    let profiler = Profiler::new();
    let report = profiler.analyze_source(source)?;
    Ok(report.quality_score())
}
