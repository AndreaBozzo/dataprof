/// Smart defaults for CLI operations
///
/// Automatically adjusts behavior based on file characteristics and system resources.
/// Implements progressive disclosure: simple commands work great, advanced users can override.
pub struct SmartDefaults {
    /// File size threshold for auto-streaming (MB)
    pub auto_streaming_threshold_mb: f64,

    /// Number of files to trigger auto-parallel
    pub auto_parallel_threshold_files: usize,

    /// ML score threshold to suggest ML command
    pub auto_show_ml_threshold: f64,

    /// Sample size for large files (rows)
    pub large_file_sample_size: usize,

    /// Chunk size for streaming (rows)
    pub streaming_chunk_size: usize,
}

impl Default for SmartDefaults {
    fn default() -> Self {
        Self {
            auto_streaming_threshold_mb: 100.0,
            auto_parallel_threshold_files: 3,
            auto_show_ml_threshold: 60.0,
            large_file_sample_size: 100_000,
            streaming_chunk_size: 10_000,
        }
    }
}

impl SmartDefaults {
    /// Create with default settings
    pub fn new() -> Self {
        Self::default()
    }

    /// Should use streaming mode based on file size?
    pub fn should_use_streaming(&self, file_size_mb: f64) -> bool {
        file_size_mb > self.auto_streaming_threshold_mb
    }

    /// Should enable parallel processing based on file count?
    pub fn should_use_parallel(&self, file_count: usize) -> bool {
        file_count >= self.auto_parallel_threshold_files
    }

    /// Should show ML recommendations?
    pub fn should_show_ml(&self, has_numeric: bool, has_categorical: bool, ml_score: f64) -> bool {
        (has_numeric || has_categorical) && ml_score > self.auto_show_ml_threshold
    }

    /// Get recommended chunk size for streaming
    pub fn get_chunk_size(&self, file_size_mb: f64) -> usize {
        if file_size_mb > 1000.0 {
            // Very large files: smaller chunks
            5_000
        } else if file_size_mb > 500.0 {
            // Large files: medium chunks
            10_000
        } else {
            // Normal files: standard chunks
            self.streaming_chunk_size
        }
    }

    /// Get recommended sample size for large files
    pub fn get_sample_size(&self, total_rows: usize) -> Option<usize> {
        if total_rows > 1_000_000 {
            Some(self.large_file_sample_size)
        } else {
            None
        }
    }

    /// Suggest command based on context
    pub fn suggest_next_command(&self, has_issues: bool, ml_score: Option<f64>) -> Option<String> {
        if has_issues {
            Some("dataprof analyze <file> --detailed".to_string())
        } else if let Some(score) = ml_score {
            if score > self.auto_show_ml_threshold {
                Some("dataprof ml <file> --script preprocess.py".to_string())
            } else {
                None
            }
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_streaming_threshold() {
        let defaults = SmartDefaults::new();
        assert!(!defaults.should_use_streaming(50.0));
        assert!(defaults.should_use_streaming(150.0));
    }

    #[test]
    fn test_parallel_threshold() {
        let defaults = SmartDefaults::new();
        assert!(!defaults.should_use_parallel(2));
        assert!(defaults.should_use_parallel(5));
    }

    #[test]
    fn test_ml_recommendation() {
        let defaults = SmartDefaults::new();
        assert!(defaults.should_show_ml(true, false, 75.0));
        assert!(!defaults.should_show_ml(true, false, 50.0));
        assert!(!defaults.should_show_ml(false, false, 80.0));
    }

    #[test]
    fn test_chunk_size_scaling() {
        let defaults = SmartDefaults::new();
        assert_eq!(defaults.get_chunk_size(50.0), 10_000);
        assert_eq!(defaults.get_chunk_size(600.0), 10_000);
        assert_eq!(defaults.get_chunk_size(1500.0), 5_000);
    }
}
