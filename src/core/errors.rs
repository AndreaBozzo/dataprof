use std::fmt;
use thiserror::Error;

/// Enhanced error types with more descriptive messages for DataProfiler
#[derive(Error, Debug)]
pub enum DataProfilerError {
    #[error("CSV parsing failed: {message}\n💡 Suggestion: {suggestion}")]
    CsvParsingError { message: String, suggestion: String },

    #[error("File not found: {path}\n💡 Please check that the file exists and you have permission to read it")]
    FileNotFound { path: String },

    #[error("Unsupported file format: {format}\n💡 Supported formats: CSV, JSON, JSONL")]
    UnsupportedFormat { format: String },

    #[error("Memory limit exceeded while processing large file\n💡 Try using --streaming mode or increase available memory")]
    MemoryLimitExceeded,

    #[error("Invalid configuration: {message}\n💡 {suggestion}")]
    InvalidConfiguration { message: String, suggestion: String },

    #[error("Data quality issue detected: {issue}\n📊 Impact: {impact}\n💡 Recommendation: {recommendation}")]
    DataQualityIssue {
        issue: String,
        impact: String,
        recommendation: String,
    },

    #[error("Streaming processing failed: {message}\n💡 Try using --chunk-size with a smaller value or disable streaming")]
    StreamingError { message: String },

    #[error("SIMD acceleration not available: {reason}\n💡 Falling back to standard processing")]
    SimdUnavailable { reason: String },

    #[error("Sampling error: {message}\n💡 {suggestion}")]
    SamplingError { message: String, suggestion: String },

    #[error("I/O error: {message}\n💡 Check file permissions and disk space")]
    IoError { message: String },

    #[error("JSON parsing failed: {message}\n💡 Verify JSON format and encoding")]
    JsonParsingError { message: String },

    #[error("Column analysis failed for '{column}': {reason}\n💡 {suggestion}")]
    ColumnAnalysisError {
        column: String,
        reason: String,
        suggestion: String,
    },

    #[error("HTML report generation failed: {message}\n💡 Check output directory permissions and available disk space")]
    HtmlReportError { message: String },
}

impl DataProfilerError {
    /// Create a CSV parsing error with helpful suggestions
    pub fn csv_parsing(original_error: &str, file_path: &str) -> Self {
        let suggestion = if original_error.contains("field") && original_error.contains("record") {
            format!("The CSV file '{}' has inconsistent column counts. This often happens with:\n  • Text fields containing commas without proper quoting\n  • Mixed line endings (Windows/Unix)\n  • Embedded newlines in data\n\n  DataProfiler will attempt to parse it with flexible mode automatically.", file_path)
        } else if original_error.contains("UTF-8") {
            "The file contains non-UTF-8 characters. Try converting it to UTF-8 encoding."
                .to_string()
        } else if original_error.contains("permission") {
            "Check file permissions - you may not have read access to this file.".to_string()
        } else {
            "Try using a different CSV delimiter or check for data formatting issues.".to_string()
        };

        DataProfilerError::CsvParsingError {
            message: original_error.to_string(),
            suggestion,
        }
    }

    /// Create a file not found error with path context
    pub fn file_not_found<P: AsRef<str>>(path: P) -> Self {
        DataProfilerError::FileNotFound {
            path: path.as_ref().to_string(),
        }
    }

    /// Create unsupported format error with format detection
    pub fn unsupported_format(extension: &str) -> Self {
        DataProfilerError::UnsupportedFormat {
            format: extension.to_string(),
        }
    }

    /// Create configuration error with specific suggestion
    pub fn invalid_config(message: &str, suggestion: &str) -> Self {
        DataProfilerError::InvalidConfiguration {
            message: message.to_string(),
            suggestion: suggestion.to_string(),
        }
    }

    /// Create data quality issue with impact and recommendation
    pub fn data_quality_issue(issue: &str, impact: &str, recommendation: &str) -> Self {
        DataProfilerError::DataQualityIssue {
            issue: issue.to_string(),
            impact: impact.to_string(),
            recommendation: recommendation.to_string(),
        }
    }

    /// Create streaming error with context
    pub fn streaming_error(message: &str) -> Self {
        DataProfilerError::StreamingError {
            message: message.to_string(),
        }
    }

    /// Create SIMD error with fallback information
    pub fn simd_unavailable(reason: &str) -> Self {
        DataProfilerError::SimdUnavailable {
            reason: reason.to_string(),
        }
    }

    /// Create sampling error with suggestion
    pub fn sampling_error(message: &str, suggestion: &str) -> Self {
        DataProfilerError::SamplingError {
            message: message.to_string(),
            suggestion: suggestion.to_string(),
        }
    }

    /// Create I/O error with context
    pub fn io_error(original: &std::io::Error) -> Self {
        DataProfilerError::IoError {
            message: original.to_string(),
        }
    }

    /// Create JSON parsing error
    pub fn json_parsing_error(original: &str) -> Self {
        DataProfilerError::JsonParsingError {
            message: original.to_string(),
        }
    }

    /// Create column analysis error with specific suggestion
    pub fn column_analysis_error(column: &str, reason: &str, suggestion: &str) -> Self {
        DataProfilerError::ColumnAnalysisError {
            column: column.to_string(),
            reason: reason.to_string(),
            suggestion: suggestion.to_string(),
        }
    }

    /// Create HTML report generation error
    pub fn html_report_error(message: &str) -> Self {
        DataProfilerError::HtmlReportError {
            message: message.to_string(),
        }
    }

    /// Check if this error is recoverable (can continue processing)
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            DataProfilerError::SimdUnavailable { .. }
                | DataProfilerError::SamplingError { .. }
                | DataProfilerError::DataQualityIssue { .. }
        )
    }

    /// Get error category for logging/metrics
    pub fn category(&self) -> &'static str {
        match self {
            DataProfilerError::CsvParsingError { .. } => "csv_parsing",
            DataProfilerError::FileNotFound { .. } => "file_not_found",
            DataProfilerError::UnsupportedFormat { .. } => "unsupported_format",
            DataProfilerError::MemoryLimitExceeded => "memory_limit",
            DataProfilerError::InvalidConfiguration { .. } => "configuration",
            DataProfilerError::DataQualityIssue { .. } => "data_quality",
            DataProfilerError::StreamingError { .. } => "streaming",
            DataProfilerError::SimdUnavailable { .. } => "simd",
            DataProfilerError::SamplingError { .. } => "sampling",
            DataProfilerError::IoError { .. } => "io",
            DataProfilerError::JsonParsingError { .. } => "json_parsing",
            DataProfilerError::ColumnAnalysisError { .. } => "column_analysis",
            DataProfilerError::HtmlReportError { .. } => "html_report",
        }
    }

    /// Get severity level for this error
    pub fn severity(&self) -> ErrorSeverity {
        match self {
            DataProfilerError::FileNotFound { .. } => ErrorSeverity::Critical,
            DataProfilerError::UnsupportedFormat { .. } => ErrorSeverity::Critical,
            DataProfilerError::MemoryLimitExceeded => ErrorSeverity::Critical,
            DataProfilerError::IoError { .. } => ErrorSeverity::High,
            DataProfilerError::CsvParsingError { .. } => ErrorSeverity::High,
            DataProfilerError::JsonParsingError { .. } => ErrorSeverity::High,
            DataProfilerError::InvalidConfiguration { .. } => ErrorSeverity::Medium,
            DataProfilerError::StreamingError { .. } => ErrorSeverity::Medium,
            DataProfilerError::ColumnAnalysisError { .. } => ErrorSeverity::Medium,
            DataProfilerError::HtmlReportError { .. } => ErrorSeverity::Medium,
            DataProfilerError::SamplingError { .. } => ErrorSeverity::Low,
            DataProfilerError::DataQualityIssue { .. } => ErrorSeverity::Info,
            DataProfilerError::SimdUnavailable { .. } => ErrorSeverity::Info,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorSeverity {
    Critical, // Prevents execution
    High,     // Major functionality impacted
    Medium,   // Some features may not work
    Low,      // Minor issues, workarounds available
    Info,     // Informational, no impact
}

impl fmt::Display for ErrorSeverity {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let s = match self {
            ErrorSeverity::Critical => "CRITICAL",
            ErrorSeverity::High => "HIGH",
            ErrorSeverity::Medium => "MEDIUM",
            ErrorSeverity::Low => "LOW",
            ErrorSeverity::Info => "INFO",
        };
        write!(f, "{}", s)
    }
}

/// Convert from anyhow::Error to DataProfilerError with context
impl From<anyhow::Error> for DataProfilerError {
    fn from(err: anyhow::Error) -> Self {
        let error_str = err.to_string();

        // Try to categorize the error based on its message
        if error_str.contains("No such file") || error_str.contains("not found") {
            DataProfilerError::FileNotFound {
                path: "unknown".to_string(),
            }
        } else if error_str.contains("CSV") {
            DataProfilerError::CsvParsingError {
                message: error_str,
                suggestion: "Try using robust CSV parsing mode".to_string(),
            }
        } else if error_str.contains("JSON") {
            DataProfilerError::JsonParsingError { message: error_str }
        } else if error_str.contains("permission") {
            DataProfilerError::IoError { message: error_str }
        } else {
            // Generic error
            DataProfilerError::IoError { message: error_str }
        }
    }
}

/// Convert from std::io::Error to DataProfilerError
impl From<std::io::Error> for DataProfilerError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::NotFound => DataProfilerError::FileNotFound {
                path: "unknown".to_string(),
            },
            std::io::ErrorKind::PermissionDenied => DataProfilerError::IoError {
                message: "Permission denied - check file access rights".to_string(),
            },
            std::io::ErrorKind::InvalidData => DataProfilerError::CsvParsingError {
                message: "Invalid data format detected".to_string(),
                suggestion: "Check file encoding and format".to_string(),
            },
            _ => DataProfilerError::IoError {
                message: err.to_string(),
            },
        }
    }
}

/// Convert from csv::Error to DataProfilerError with enhanced context
impl From<csv::Error> for DataProfilerError {
    fn from(err: csv::Error) -> Self {
        DataProfilerError::csv_parsing(&err.to_string(), "unknown")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_categorization() {
        let csv_error = DataProfilerError::csv_parsing("field count mismatch", "test.csv");
        assert_eq!(csv_error.category(), "csv_parsing");
        assert_eq!(csv_error.severity(), ErrorSeverity::High);
        assert!(!csv_error.is_recoverable());
    }

    #[test]
    fn test_recoverable_errors() {
        let simd_error = DataProfilerError::simd_unavailable("CPU doesn't support SIMD");
        assert!(simd_error.is_recoverable());
        assert_eq!(simd_error.severity(), ErrorSeverity::Info);
    }

    #[test]
    fn test_error_suggestions() {
        let config_error = DataProfilerError::invalid_config(
            "Invalid chunk size",
            "Use a value between 1000 and 100000",
        );

        let error_string = config_error.to_string();
        assert!(error_string.contains("Invalid chunk size"));
        assert!(error_string.contains("💡"));
    }
}
