use crate::core::errors::DataProfilerError;
use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::engines::streaming::{BufferedProfiler, ProgressInfo};
use crate::engines::{AdaptiveProfiler, ProcessingType};
use crate::types::{DataSource, QualityReport};
use std::path::Path;

/// One-liner API for quick profiling with intelligent engine selection
pub fn quick_quality_check<P: AsRef<Path>>(file_path: P) -> Result<f64, DataProfilerError> {
    let profiler = AdaptiveProfiler::new().with_logging(false); // Disable logging for quick checks

    let report =
        profiler.analyze_file_with_context(file_path.as_ref(), ProcessingType::QualityFocused)?;

    // Use ISO 8000/25012 quality score
    Ok(report.quality_score())
}

/// One-liner API for quick profiling from a DataSource
pub fn quick_quality_check_source(source: &DataSource) -> Result<f64, DataProfilerError> {
    let profiler = AdaptiveProfiler::new().with_logging(false);

    let report = match source {
        DataSource::File { path, .. } => {
            profiler.analyze_file_with_context(Path::new(path), ProcessingType::QualityFocused)?
        }
        _ => {
            return Err(DataProfilerError::UnsupportedDataSource {
                message: "Only File DataSource is currently supported in synchronous API. Use async API for streams.".to_string(),
            })
        }
    };

    Ok(report.quality_score())
}

/// Simple builder API for profiling with flexible configuration
pub struct DataProfiler {
    inner: BufferedProfiler,
}

impl DataProfiler {
    /// Create an adaptive profiler with intelligent engine selection (RECOMMENDED)
    pub fn auto() -> AdaptiveProfiler {
        AdaptiveProfiler::new()
    }

    /// Create a streaming profiler
    pub fn streaming() -> Self {
        Self {
            inner: BufferedProfiler::new(),
        }
    }

    /// Create an Arrow-based columnar profiler for high performance on large files
    pub fn columnar() -> crate::engines::columnar::ArrowProfiler {
        crate::engines::columnar::ArrowProfiler::new()
    }

    /// Create from path with builder pattern
    pub fn from_path<P: AsRef<Path>>(path: P) -> DataProfilerBuilder<P> {
        DataProfilerBuilder {
            path,
            chunk_size: ChunkSize::Adaptive,
            sampling: SamplingStrategy::None,
            progress_callback: None,
        }
    }

    /// Create from DataSource with builder pattern
    pub fn from_data_source(source: DataSource) -> DataProfilerBuilder<DataSource> {
        DataProfilerBuilder {
            path: source,
            chunk_size: ChunkSize::Adaptive,
            sampling: SamplingStrategy::None,
            progress_callback: None,
        }
    }

    pub fn chunk_size(mut self, chunk_size: ChunkSize) -> Self {
        self.inner = self.inner.chunk_size(chunk_size);
        self
    }

    pub fn sampling(mut self, strategy: SamplingStrategy) -> Self {
        self.inner = self.inner.sampling(strategy);
        self
    }

    pub fn progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(ProgressInfo) + Send + Sync + 'static,
    {
        self.inner = self.inner.progress_callback(callback);
        self
    }

    /// Enable enhanced progress tracking with memory monitoring
    pub fn with_enhanced_progress(mut self, leak_threshold_mb: usize) -> Self {
        self.inner = self.inner.with_enhanced_progress(leak_threshold_mb);
        self
    }

    /// Enable enhanced progress with smart defaults based on terminal context
    pub fn with_smart_progress(mut self) -> Self {
        use crate::output::supports_enhanced_output;

        if supports_enhanced_output() {
            self.inner = self.inner.with_enhanced_progress(100); // 100MB threshold
        }
        self
    }

    pub fn analyze_file<P: AsRef<Path>>(
        &mut self,
        file_path: P,
    ) -> Result<QualityReport, DataProfilerError> {
        self.inner.analyze_file(file_path.as_ref())
    }

    pub fn analyze_source(
        &mut self,
        source: &DataSource,
    ) -> Result<QualityReport, DataProfilerError> {
        match source {
            DataSource::File { path, .. } => self.inner.analyze_file(Path::new(path)),
            _ => Err(DataProfilerError::UnsupportedDataSource {
                message: "Only File DataSource is currently supported in synchronous API. Use async API for streams.".to_string(),
            }),
        }
    }
}

/// Builder for flexible profiler configuration
pub struct DataProfilerBuilder<P> {
    path: P,
    chunk_size: ChunkSize,
    sampling: SamplingStrategy,
    progress_callback: Option<Box<dyn Fn(ProgressInfo) + Send + Sync>>,
}

impl<P: AsRef<Path>> DataProfilerBuilder<P> {
    pub fn analyze(self) -> Result<QualityReport, DataProfilerError> {
        let mut profiler = BufferedProfiler::new()
            .chunk_size(self.chunk_size)
            .sampling(self.sampling);

        if let Some(callback) = self.progress_callback {
            profiler = profiler.progress_callback(callback);
        }

        profiler.analyze_file(self.path.as_ref())
    }
}

impl DataProfilerBuilder<DataSource> {
    pub fn analyze_source(self) -> Result<QualityReport, DataProfilerError> {
        let mut profiler = BufferedProfiler::new()
            .chunk_size(self.chunk_size)
            .sampling(self.sampling);

        if let Some(callback) = self.progress_callback {
            profiler = profiler.progress_callback(callback);
        }

        match &self.path {
            DataSource::File { path, .. } => profiler.analyze_file(Path::new(path)),
            _ => Err(DataProfilerError::UnsupportedDataSource {
                message: "Only File DataSource is currently supported in synchronous API. Use async API for streams.".to_string(),
            }),
        }
    }
}
