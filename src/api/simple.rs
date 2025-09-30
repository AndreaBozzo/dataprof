use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::engines::streaming::{ProgressInfo, StreamingProfiler};
use crate::engines::{AdaptiveProfiler, ProcessingType};
use crate::types::QualityReport;
use anyhow::Result;
use std::path::Path;

/// One-liner API for quick profiling with intelligent engine selection
pub fn quick_quality_check<P: AsRef<Path>>(file_path: P) -> Result<f64> {
    let profiler = AdaptiveProfiler::new().with_logging(false); // Disable logging for quick checks

    let report =
        profiler.analyze_file_with_context(file_path.as_ref(), ProcessingType::QualityFocused)?;

    // Calculate a simple quality score based on issues
    let total_issues = report.issues.len();
    let quality_score = if total_issues == 0 {
        100.0
    } else {
        (100.0 - (total_issues as f64 * 10.0)).max(0.0)
    };

    Ok(quality_score)
}

/// Stream profiling with intelligent engine selection and progress logging
pub fn stream_profile<P, F>(file_path: P, _callback: F) -> Result<QualityReport>
where
    P: AsRef<Path>,
    F: Fn(QualityReport) + Send + Sync + 'static,
{
    let profiler = AdaptiveProfiler::new()
        .with_logging(true) // Enable progress logging
        .with_performance_logging(true);

    profiler.analyze_file_with_context(file_path.as_ref(), ProcessingType::StreamingRequired)
}

/// Simple builder API that maintains backward compatibility
pub struct DataProfiler {
    inner: StreamingProfiler,
}

impl DataProfiler {
    /// Create an adaptive profiler with intelligent engine selection (RECOMMENDED)
    pub fn auto() -> AdaptiveProfiler {
        AdaptiveProfiler::new()
    }

    /// Create a streaming profiler - API from roadmap
    pub fn streaming() -> Self {
        Self {
            inner: StreamingProfiler::new(),
        }
    }

    /// Create an Arrow-based columnar profiler for high performance on large files
    #[cfg(feature = "arrow")]
    pub fn columnar() -> crate::engines::columnar::ArrowProfiler {
        crate::engines::columnar::ArrowProfiler::new()
    }

    /// Create from path - backward compatibility
    pub fn from_path<P: AsRef<Path>>(path: P) -> DataProfilerBuilder<P> {
        DataProfilerBuilder {
            path,
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

    pub fn analyze_file<P: AsRef<Path>>(&mut self, file_path: P) -> Result<QualityReport> {
        self.inner.analyze_file(file_path.as_ref())
    }
}

/// Builder for backward compatibility
pub struct DataProfilerBuilder<P> {
    path: P,
    chunk_size: ChunkSize,
    sampling: SamplingStrategy,
    progress_callback: Option<Box<dyn Fn(ProgressInfo) + Send + Sync>>,
}

impl<P: AsRef<Path>> DataProfilerBuilder<P> {
    pub fn analyze(self) -> Result<QualityReport> {
        let mut profiler = StreamingProfiler::new()
            .chunk_size(self.chunk_size)
            .sampling(self.sampling);

        if let Some(callback) = self.progress_callback {
            profiler = profiler.progress_callback(callback);
        }

        profiler.analyze_file(self.path.as_ref())
    }
}
