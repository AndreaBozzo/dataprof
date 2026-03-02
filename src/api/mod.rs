use std::path::Path;
use std::sync::Arc;

use crate::core::errors::DataProfilerError;
use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::engines::streaming::ProgressInfo;
use crate::engines::{AdaptiveProfiler, ProcessingType};
use crate::types::{DataSource, QualityReport};

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
}

impl Default for ProfilerConfig {
    fn default() -> Self {
        Self {
            engine: EngineType::Auto,
            chunk_size: ChunkSize::Adaptive,
            sampling: SamplingStrategy::None,
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

    /// Set a progress callback for real-time updates
    ///
    /// Note: Progress callbacks are effective with `EngineType::Incremental`.
    /// With `EngineType::Auto`, callbacks may not be forwarded to all engines.
    pub fn progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(ProgressInfo) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Arc::new(callback));
        self
    }

    /// Enable enhanced progress tracking with memory monitoring
    pub fn with_enhanced_progress(mut self, leak_threshold_mb: usize) -> Self {
        self.enhanced_progress = Some(leak_threshold_mb);
        self
    }

    /// Enable enhanced progress with smart defaults based on terminal context
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
            EngineType::Auto => {
                let profiler = AdaptiveProfiler::new().with_logging(false);
                profiler.analyze_file_with_context(path, ProcessingType::QualityFocused)
            }
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

    /// Dispatch to IncrementalProfiler with all configured options
    fn run_incremental(&self, file_path: &Path) -> Result<QualityReport, DataProfilerError> {
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
        // Check file format — ArrowProfiler currently only supports CSV
        let is_parquet = file_path
            .extension()
            .map(|ext| ext.eq_ignore_ascii_case("parquet"))
            .unwrap_or(false);

        let is_json = file_path
            .extension()
            .map(|ext| ext.eq_ignore_ascii_case("json") || ext.eq_ignore_ascii_case("jsonl"))
            .unwrap_or(false);

        if is_parquet {
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

        if is_json {
            return crate::parsers::json::analyze_json_with_quality(file_path);
        }

        // CSV — use ArrowProfiler
        use crate::engines::columnar::ArrowProfiler;
        let profiler = ArrowProfiler::new();
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
