use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::engines::streaming::{
    MemoryEfficientProfiler, ProgressInfo, StreamingProfiler, TrueStreamingProfiler,
};
use crate::types::QualityReport;
use anyhow::Result;
use std::path::Path;

/// One-liner API for quick profiling - exactly as shown in the roadmap
pub fn quick_quality_check<P: AsRef<Path>>(file_path: P) -> Result<f64> {
    let report = choose_best_profiler(file_path.as_ref())?
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

/// Helper function to choose the best profiler based on file size
fn choose_best_profiler(file_path: &Path) -> Result<ProfilerChoice> {
    let metadata = std::fs::metadata(file_path)?;
    let file_size_mb = metadata.len() as f64 / 1_048_576.0;

    if file_size_mb > 200.0 {
        // Use true streaming profiler for very large files (>200MB)
        Ok(ProfilerChoice::TrueStreaming(TrueStreamingProfiler::new()))
    } else if file_size_mb > 50.0 {
        // Use memory-efficient profiler for moderately large files (50-200MB)
        Ok(ProfilerChoice::MemoryEfficient(
            MemoryEfficientProfiler::new(),
        ))
    } else {
        // Use regular streaming profiler for smaller files (<50MB)
        Ok(ProfilerChoice::Streaming(StreamingProfiler::new()))
    }
}

enum ProfilerChoice {
    Streaming(StreamingProfiler),
    MemoryEfficient(MemoryEfficientProfiler),
    TrueStreaming(TrueStreamingProfiler),
}

impl ProfilerChoice {
    fn sampling(self, strategy: SamplingStrategy) -> Self {
        match self {
            ProfilerChoice::Streaming(profiler) => {
                ProfilerChoice::Streaming(profiler.sampling(strategy))
            }
            ProfilerChoice::MemoryEfficient(profiler) => {
                ProfilerChoice::MemoryEfficient(profiler.sampling(strategy))
            }
            ProfilerChoice::TrueStreaming(profiler) => {
                ProfilerChoice::TrueStreaming(profiler.sampling(strategy))
            }
        }
    }

    fn analyze_file(self, file_path: &Path) -> Result<QualityReport> {
        match self {
            ProfilerChoice::Streaming(profiler) => profiler.analyze_file(file_path),
            ProfilerChoice::MemoryEfficient(profiler) => profiler.analyze_file(file_path),
            ProfilerChoice::TrueStreaming(profiler) => profiler.analyze_file(file_path),
        }
    }
}

/// Stream profiling with callback - as shown in the roadmap
pub fn stream_profile<P, F>(file_path: P, _callback: F) -> Result<QualityReport>
where
    P: AsRef<Path>,
    F: Fn(QualityReport) + Send + Sync + 'static,
{
    // Choose best profiler and add progress callback
    let profiler_choice = choose_best_profiler(file_path.as_ref())?;

    match profiler_choice {
        ProfilerChoice::Streaming(profiler) => profiler
            .chunk_size(ChunkSize::Adaptive)
            .progress_callback(move |progress| {
                println!("Progress: {:.1}%", progress.percentage);
            })
            .analyze_file(file_path.as_ref()),
        ProfilerChoice::MemoryEfficient(profiler) => profiler
            .chunk_size(ChunkSize::Adaptive)
            .progress_callback(move |progress| {
                println!("Progress: {:.1}%", progress.percentage);
            })
            .analyze_file(file_path.as_ref()),
        ProfilerChoice::TrueStreaming(profiler) => profiler
            .chunk_size(ChunkSize::Adaptive)
            .progress_callback(move |progress| {
                println!("Progress: {:.1}%", progress.percentage);
            })
            .analyze_file(file_path.as_ref()),
    }
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
