use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::engines::streaming::{ProgressInfo, StreamingProfiler};
use crate::types::QualityReport;
use anyhow::Result;
use std::path::Path;

/// One-liner API for quick profiling - exactly as shown in the roadmap
pub fn quick_quality_check<P: AsRef<Path>>(file_path: P) -> Result<f64> {
    let report = StreamingProfiler::new()
        .sampling(SamplingStrategy::adaptive(None, 0.0))
        .analyze_file(file_path.as_ref())?;

    // Calculate a simple quality score based on issues
    let total_issues = report.issues.len();
    let quality_score = if total_issues == 0 {
        100.0
    } else {
        (100.0 - (total_issues as f64 * 10.0)).max(0.0)
    };

    Ok(quality_score)
}

/// Stream profiling with callback - as shown in the roadmap
pub fn stream_profile<P, F>(file_path: P, _callback: F) -> Result<QualityReport>
where
    P: AsRef<Path>,
    F: Fn(QualityReport) + Send + Sync + 'static,
{
    StreamingProfiler::new()
        .chunk_size(ChunkSize::Adaptive)
        .progress_callback(move |progress| {
            // For chunk-based callback, we'd need to modify the streaming profiler
            // to produce intermediate reports. For now, this is a placeholder.
            println!("Progress: {:.1}%", progress.percentage);
        })
        .analyze_file(file_path.as_ref())
}

/// Simple builder API that maintains backward compatibility
pub struct DataProfiler {
    inner: StreamingProfiler,
}

impl DataProfiler {
    /// Create a streaming profiler - API from roadmap
    pub fn streaming() -> Self {
        Self {
            inner: StreamingProfiler::new(),
        }
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

    pub fn progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(ProgressInfo) + Send + Sync + 'static,
    {
        self.inner = self.inner.progress_callback(callback);
        self
    }

    pub fn analyze_file<P: AsRef<Path>>(&self, file_path: P) -> Result<QualityReport> {
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
