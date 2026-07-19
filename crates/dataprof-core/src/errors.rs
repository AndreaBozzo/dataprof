use thiserror::Error;

/// The file formats this build can actually read, reflecting compiled features.
///
/// The list must never over-claim: Parquet is only advertised when the `parquet`
/// feature is enabled, so an "unsupported format" error cannot point the user at
/// a format the installed build cannot open.
fn supported_formats_hint() -> &'static str {
    #[cfg(feature = "parquet")]
    {
        "CSV, JSON, JSONL, Parquet"
    }
    #[cfg(not(feature = "parquet"))]
    {
        "CSV, JSON, JSONL"
    }
}

/// Redact credentials from a string before it enters an error message.
///
/// Connection strings and driver errors can carry `scheme://user:password@host`.
/// We collapse the userinfo to `***` so passwords (and usernames) never reach a
/// log line or a surfaced diagnostic, while keeping the scheme/host for context.
pub fn redact_credentials(input: &str) -> String {
    // Match the `://[userinfo@]` segment of any URL-like token and drop userinfo.
    let mut out = String::with_capacity(input.len());
    let mut rest = input;
    while let Some(scheme_idx) = rest.find("://") {
        let after_scheme = scheme_idx + 3;
        out.push_str(&rest[..after_scheme]);
        let tail = &rest[after_scheme..];
        // Userinfo, if present, ends at the first '@' before the next authority
        // delimiter ('/', '?', '#', or whitespace).
        let authority_end = tail
            .find(['/', '?', '#', ' ', '\t', '\n'])
            .unwrap_or(tail.len());
        if let Some(at_rel) = tail[..authority_end].find('@') {
            out.push_str("***");
            rest = &tail[at_rel..]; // keep '@host...'
        } else {
            rest = tail;
        }
    }
    out.push_str(rest);
    out
}

/// Auto-retry configuration for error recovery
#[derive(Debug, Clone)]
pub struct RetryConfig {
    pub max_attempts: usize,
    pub enable_delimiter_detection: bool,
    pub enable_encoding_detection: bool,
    pub enable_flexible_parsing: bool,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 3,
            enable_delimiter_detection: true,
            enable_encoding_detection: true,
            enable_flexible_parsing: true,
        }
    }
}

/// Result of an auto-recovery attempt
#[derive(Debug, Clone)]
pub struct RecoveryAttempt {
    pub attempt_number: usize,
    pub strategy: RecoveryStrategy,
    pub success: bool,
    pub error_message: Option<String>,
}

/// Strategies for error recovery
#[derive(Debug, Clone)]
pub enum RecoveryStrategy {
    DelimiterDetection { delimiter: char },
    EncodingConversion { from: String, to: String },
    FlexibleParsing,
    ChunkSizeReduction { new_size: usize },
    MemoryOptimization,
}

/// Enhanced error types with more descriptive messages for DataProfiler
#[derive(Error, Debug, Clone)]
pub enum DataProfilerError {
    #[error("CSV parsing failed: {message}\nSuggestion: {suggestion}")]
    CsvParsingError { message: String, suggestion: String },

    #[error(
        "File not found: {path}\nPlease check that the file exists and you have permission to read it"
    )]
    FileNotFound { path: String },

    #[error(
        "Unsupported file format: {format}\nSupported formats: {}",
        supported_formats_hint()
    )]
    UnsupportedFormat { format: String },

    #[error(
        "Memory limit exceeded while processing large file\nTry using streaming mode or increase available memory"
    )]
    MemoryLimitExceeded,

    #[error("Invalid configuration: {message}\n{suggestion}")]
    InvalidConfiguration { message: String, suggestion: String },

    #[error("Invalid semantic hint: {message}\n{suggestion}")]
    InvalidSemanticHint { message: String, suggestion: String },

    #[error(
        "Data quality issue detected: {issue}\nImpact: {impact}\nRecommendation: {recommendation}"
    )]
    DataQualityIssue {
        issue: String,
        impact: String,
        recommendation: String,
    },

    #[error(
        "Streaming processing failed: {message}\nTry setting a smaller chunk size on the profiler builder (e.g. `.chunk_size(ChunkSize::Fixed(1000))`)"
    )]
    StreamingError { message: String },

    #[error("SIMD acceleration not available: {reason}\nFalling back to standard processing")]
    SimdUnavailable { reason: String },

    #[error("Sampling error: {message}\n{suggestion}")]
    SamplingError { message: String, suggestion: String },

    #[error("I/O error: {message}\nCheck file permissions and disk space")]
    IoError { message: String },

    #[error("JSON parsing failed: {message}\nVerify JSON format and encoding")]
    JsonParsingError { message: String },

    #[error("Column analysis failed for '{column}': {reason}\n{suggestion}")]
    ColumnAnalysisError {
        column: String,
        reason: String,
        suggestion: String,
    },

    #[error(
        "Recoverable error (attempt {attempt}/{max_attempts}): {message}\n{recovery_suggestion}"
    )]
    RecoverableError {
        message: String,
        recovery_suggestion: String,
        attempt: usize,
        max_attempts: usize,
        recovery_attempts: Vec<RecoveryAttempt>,
    },

    #[error(
        "Auto-recovery failed after {attempts} attempts\nLast strategy tried: {last_strategy}\nRecovery log: {recovery_log}"
    )]
    RecoveryFailed {
        attempts: usize,
        last_strategy: String,
        recovery_log: String,
        original_error: String,
    },

    #[error("Parquet processing failed: {message}")]
    ParquetError { message: String },

    #[error("Arrow processing failed: {message}")]
    ArrowError { message: String },

    #[error("Unsupported data source: {message}")]
    UnsupportedDataSource { message: String },

    #[error("All engines failed: {message}")]
    AllEnginesFailed { message: String },

    #[error("Metrics calculation failed: {message}")]
    MetricsCalculationError { message: String },

    #[error("Configuration validation failed: {message}")]
    ConfigValidationError { message: String },

    #[error("Database connection failed: {message}\n{suggestion}")]
    DatabaseConnectionError { message: String, suggestion: String },

    #[error("Database query failed: {message}")]
    DatabaseQueryError { message: String },

    #[error("Database configuration error: {message}")]
    DatabaseConfigError { message: String },

    #[error("Database feature not enabled: {message}\nRecompile with the appropriate feature flag")]
    DatabaseFeatureDisabled { message: String },

    #[error("SQL validation failed: {message}")]
    SqlValidationError { message: String },

    #[error("Database SSL/TLS error: {message}")]
    DatabaseSslError { message: String },

    #[error(
        "Database retry exhausted: operation '{operation}' failed after {attempts} attempts\nLast error: {last_error}"
    )]
    DatabaseRetryExhausted {
        operation: String,
        attempts: u32,
        last_error: String,
    },
}

impl DataProfilerError {
    /// Create a database connection error.
    ///
    /// The message is scrubbed of URL credentials before storage so a driver
    /// error that echoes the connection string cannot leak the password.
    pub fn database_connection(message: &str) -> Self {
        let message = redact_credentials(message);
        let m = message.to_lowercase();
        let suggestion = if m.contains("refused") {
            "Check that the database server is running and accepting connections."
        } else if m.contains("timeout") {
            "Increase the connection timeout or check network connectivity."
        } else if m.contains("authentication") || m.contains("password") {
            "Verify your credentials or use environment variables for authentication."
        } else {
            "Verify the connection string format and database server availability."
        };
        DataProfilerError::DatabaseConnectionError {
            message,
            suggestion: suggestion.to_string(),
        }
    }

    /// Create a database query error.
    ///
    /// Redacts URL credentials that a driver error might surface; the raw SQL
    /// text and bound parameter values are never embedded by callers.
    pub fn database_query(message: &str) -> Self {
        DataProfilerError::DatabaseQueryError {
            message: redact_credentials(message),
        }
    }

    /// Create a database config error
    pub fn database_config(message: &str) -> Self {
        DataProfilerError::DatabaseConfigError {
            message: message.to_string(),
        }
    }

    /// Create a feature-not-enabled error
    pub fn database_feature_disabled(db_name: &str, feature: &str) -> Self {
        DataProfilerError::DatabaseFeatureDisabled {
            message: format!(
                "{} support not compiled. Enable '{}' feature.",
                db_name, feature
            ),
        }
    }

    /// Create a SQL validation error
    pub fn sql_validation(message: &str) -> Self {
        DataProfilerError::SqlValidationError {
            message: message.to_string(),
        }
    }

    /// Create a database SSL error
    pub fn database_ssl(message: &str) -> Self {
        DataProfilerError::DatabaseSslError {
            message: message.to_string(),
        }
    }
    /// Create a CSV parsing error with helpful suggestions.
    ///
    /// `file_path` is `None` at boundaries that do not know the source (e.g. the
    /// `From<csv::Error>` conversion); pass `Some(path)` whenever it is known so
    /// the suggestion can name the offending file instead of a placeholder.
    pub fn csv_parsing(original_error: &str, file_path: Option<&str>) -> Self {
        let suggestion = if original_error.contains("field") && original_error.contains("record") {
            let file_clause = match file_path {
                Some(path) => format!("The CSV file '{}' has", path),
                None => "This CSV file has".to_string(),
            };
            format!(
                "{} inconsistent column counts. This often happens with:\n  • Text fields containing commas without proper quoting\n  • Mixed line endings (Windows/Unix)\n  • Embedded newlines in data\n\n  dataprof will attempt to parse it with flexible mode automatically.",
                file_clause
            )
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

    /// Create a recoverable error that can be auto-retried
    pub fn recoverable_error(
        message: &str,
        recovery_suggestion: &str,
        attempt: usize,
        max_attempts: usize,
    ) -> Self {
        DataProfilerError::RecoverableError {
            message: message.to_string(),
            recovery_suggestion: recovery_suggestion.to_string(),
            attempt,
            max_attempts,
            recovery_attempts: Vec::new(),
        }
    }

    /// Create a recovery failed error with attempt log
    pub fn recovery_failed(
        attempts: usize,
        last_strategy: &str,
        recovery_log: &str,
        original_error: &str,
    ) -> Self {
        DataProfilerError::RecoveryFailed {
            attempts,
            last_strategy: last_strategy.to_string(),
            recovery_log: recovery_log.to_string(),
            original_error: original_error.to_string(),
        }
    }

    /// Add a recovery attempt to the error
    pub fn add_recovery_attempt(&mut self, attempt: RecoveryAttempt) {
        if let DataProfilerError::RecoverableError {
            recovery_attempts, ..
        } = self
        {
            recovery_attempts.push(attempt);
        }
    }

    /// Check if error supports auto-recovery
    pub fn supports_auto_recovery(&self) -> bool {
        matches!(
            self,
            DataProfilerError::CsvParsingError { .. }
                | DataProfilerError::JsonParsingError { .. }
                | DataProfilerError::StreamingError { .. }
                | DataProfilerError::MemoryLimitExceeded
                | DataProfilerError::RecoverableError { .. }
        )
    }

    /// Get suggested recovery strategies for this error
    pub fn suggested_recovery_strategies(&self) -> Vec<RecoveryStrategy> {
        match self {
            DataProfilerError::CsvParsingError { .. } => vec![
                RecoveryStrategy::DelimiterDetection { delimiter: ',' },
                RecoveryStrategy::DelimiterDetection { delimiter: ';' },
                RecoveryStrategy::DelimiterDetection { delimiter: '\t' },
                RecoveryStrategy::DelimiterDetection { delimiter: '|' },
                RecoveryStrategy::EncodingConversion {
                    from: "latin1".to_string(),
                    to: "utf8".to_string(),
                },
                RecoveryStrategy::FlexibleParsing,
            ],
            DataProfilerError::MemoryLimitExceeded => vec![
                RecoveryStrategy::ChunkSizeReduction { new_size: 1000 },
                RecoveryStrategy::MemoryOptimization,
            ],
            DataProfilerError::JsonParsingError { .. } => {
                vec![RecoveryStrategy::EncodingConversion {
                    from: "latin1".to_string(),
                    to: "utf8".to_string(),
                }]
            }
            DataProfilerError::StreamingError { .. } => vec![
                RecoveryStrategy::ChunkSizeReduction { new_size: 500 },
                RecoveryStrategy::MemoryOptimization,
            ],
            _ => vec![],
        }
    }

    /// Check if this error is recoverable (can continue processing)
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            DataProfilerError::SimdUnavailable { .. }
                | DataProfilerError::SamplingError { .. }
                | DataProfilerError::DataQualityIssue { .. }
                | DataProfilerError::RecoverableError { .. }
        )
    }

    /// The actionable next step for this error, as structured context.
    ///
    /// Suggestions live in the variants and the `Display` string, but callers
    /// that want to react programmatically (or render the hint separately from
    /// the message) should read it here rather than parse the formatted output.
    pub fn suggestion(&self) -> Option<String> {
        match self {
            DataProfilerError::CsvParsingError { suggestion, .. }
            | DataProfilerError::InvalidConfiguration { suggestion, .. }
            | DataProfilerError::InvalidSemanticHint { suggestion, .. }
            | DataProfilerError::SamplingError { suggestion, .. }
            | DataProfilerError::DatabaseConnectionError { suggestion, .. } => {
                Some(suggestion.clone())
            }
            DataProfilerError::ColumnAnalysisError { suggestion, .. } => Some(suggestion.clone()),
            DataProfilerError::FileNotFound { .. } => {
                Some("Check that the file exists and you have permission to read it.".to_string())
            }
            DataProfilerError::UnsupportedFormat { .. } => Some(format!(
                "This build reads {}. Re-encode the input, or rebuild with the needed feature.",
                supported_formats_hint()
            )),
            DataProfilerError::MemoryLimitExceeded => {
                Some("Use streaming mode or increase available memory.".to_string())
            }
            DataProfilerError::StreamingError { .. } => Some(
                "Set a smaller chunk size on the profiler builder, e.g. `.chunk_size(ChunkSize::Fixed(1000))`."
                    .to_string(),
            ),
            DataProfilerError::DataQualityIssue { recommendation, .. } => {
                Some(recommendation.clone())
            }
            DataProfilerError::DatabaseFeatureDisabled { .. } => {
                Some("Recompile with the appropriate database feature flag.".to_string())
            }
            _ => None,
        }
    }

    /// Get error category for logging/metrics
    pub fn category(&self) -> &'static str {
        match self {
            DataProfilerError::CsvParsingError { .. } => "csv_parsing",
            DataProfilerError::FileNotFound { .. } => "file_not_found",
            DataProfilerError::UnsupportedFormat { .. } => "unsupported_format",
            DataProfilerError::MemoryLimitExceeded => "memory_limit",
            DataProfilerError::InvalidConfiguration { .. } => "configuration",
            DataProfilerError::InvalidSemanticHint { .. } => "semantic_hint",
            DataProfilerError::DataQualityIssue { .. } => "data_quality",
            DataProfilerError::StreamingError { .. } => "streaming",
            DataProfilerError::SimdUnavailable { .. } => "simd",
            DataProfilerError::SamplingError { .. } => "sampling",
            DataProfilerError::IoError { .. } => "io",
            DataProfilerError::JsonParsingError { .. } => "json_parsing",
            DataProfilerError::ColumnAnalysisError { .. } => "column_analysis",
            DataProfilerError::RecoverableError { .. } => "recoverable",
            DataProfilerError::RecoveryFailed { .. } => "recovery_failed",
            DataProfilerError::ParquetError { .. } => "parquet",
            DataProfilerError::ArrowError { .. } => "arrow",
            DataProfilerError::UnsupportedDataSource { .. } => "unsupported_data_source",
            DataProfilerError::AllEnginesFailed { .. } => "all_engines_failed",
            DataProfilerError::MetricsCalculationError { .. } => "metrics_calculation",
            DataProfilerError::ConfigValidationError { .. } => "config_validation",
            DataProfilerError::DatabaseConnectionError { .. } => "database_connection",
            DataProfilerError::DatabaseQueryError { .. } => "database_query",
            DataProfilerError::DatabaseConfigError { .. } => "database_config",
            DataProfilerError::DatabaseFeatureDisabled { .. } => "database_feature_disabled",
            DataProfilerError::SqlValidationError { .. } => "sql_validation",
            DataProfilerError::DatabaseSslError { .. } => "database_ssl",
            DataProfilerError::DatabaseRetryExhausted { .. } => "database_retry_exhausted",
        }
    }
}

/// Convert from anyhow::Error to DataProfilerError with context.
///
/// These conversions run where the source path/operation is *not* known, so they
/// never fabricate one: a path is only ever attached by the call sites that hold
/// it (see [`DataProfilerError::csv_parsing`], `map_io_error`, and the facade's
/// existence check). Preserving the original message keeps the source diagnostic.
impl From<anyhow::Error> for DataProfilerError {
    fn from(err: anyhow::Error) -> Self {
        let error_str = err.to_string();

        // Categorize by message only; do not invent a path we do not have.
        if error_str.contains("CSV") {
            DataProfilerError::CsvParsingError {
                message: error_str,
                suggestion: "Try using robust CSV parsing mode".to_string(),
            }
        } else if error_str.contains("JSON") {
            DataProfilerError::JsonParsingError { message: error_str }
        } else {
            // Generic I/O error — the original message carries the detail
            // (including "file not found" wording) without a fabricated path.
            DataProfilerError::IoError { message: error_str }
        }
    }
}

/// Convert from std::io::Error to DataProfilerError.
///
/// Path-less by design: callers that know the file use `map_io_error` to produce
/// a [`DataProfilerError::FileNotFound`] with the real path. Here we keep the OS
/// message (which already names the condition) rather than guessing a path.
impl From<std::io::Error> for DataProfilerError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::PermissionDenied => DataProfilerError::IoError {
                message: "Permission denied - check file access rights".to_string(),
            },
            _ => DataProfilerError::IoError {
                message: err.to_string(),
            },
        }
    }
}

/// Convert from csv::Error to DataProfilerError with enhanced context.
///
/// The source path is unknown at this boundary, so the suggestion is written
/// without a filename rather than the misleading `'unknown'` placeholder.
impl From<csv::Error> for DataProfilerError {
    fn from(err: csv::Error) -> Self {
        DataProfilerError::csv_parsing(&err.to_string(), None)
    }
}

/// Convert from arrow::error::ArrowError to DataProfilerError
#[cfg(feature = "arrow")]
impl From<arrow::error::ArrowError> for DataProfilerError {
    fn from(err: arrow::error::ArrowError) -> Self {
        DataProfilerError::ArrowError {
            message: err.to_string(),
        }
    }
}

/// Convert from serde_json::Error to DataProfilerError
impl From<serde_json::Error> for DataProfilerError {
    fn from(err: serde_json::Error) -> Self {
        DataProfilerError::JsonParsingError {
            message: err.to_string(),
        }
    }
}

/// Convert from glob::PatternError to DataProfilerError
impl From<glob::PatternError> for DataProfilerError {
    fn from(err: glob::PatternError) -> Self {
        DataProfilerError::InvalidConfiguration {
            message: format!("Invalid glob pattern: {}", err),
            suggestion: "Check the glob pattern syntax".to_string(),
        }
    }
}

/// Convert from toml::de::Error to DataProfilerError
impl From<toml::de::Error> for DataProfilerError {
    fn from(err: toml::de::Error) -> Self {
        DataProfilerError::InvalidConfiguration {
            message: format!("Failed to parse TOML configuration: {}", err),
            suggestion: "Check your configuration file syntax".to_string(),
        }
    }
}

/// Convert from toml::ser::Error to DataProfilerError
impl From<toml::ser::Error> for DataProfilerError {
    fn from(err: toml::ser::Error) -> Self {
        DataProfilerError::InvalidConfiguration {
            message: format!("Failed to serialize configuration: {}", err),
            suggestion: "Check configuration values for serialization issues".to_string(),
        }
    }
}

/// Auto-recovery manager for handling error recovery strategies
pub struct AutoRecoveryManager {
    config: RetryConfig,
    recovery_log: Vec<RecoveryAttempt>,
}

impl AutoRecoveryManager {
    pub fn new(config: RetryConfig) -> Self {
        Self {
            config,
            recovery_log: Vec::new(),
        }
    }

    /// Attempt auto-recovery for a given error
    pub fn attempt_recovery<F, T>(
        &mut self,
        error: &DataProfilerError,
        retry_fn: F,
    ) -> Result<T, DataProfilerError>
    where
        F: Fn(RecoveryStrategy) -> Result<T, DataProfilerError>,
    {
        if !error.supports_auto_recovery() {
            return Err(error.clone());
        }

        let strategies = error.suggested_recovery_strategies();
        let mut last_error: DataProfilerError = error.clone();

        for (attempt, strategy) in strategies.iter().enumerate() {
            if attempt >= self.config.max_attempts {
                break;
            }

            // Log attempt start
            log::info!(
                "Auto-recovery attempt {}/{}: {:?}",
                attempt + 1,
                self.config.max_attempts,
                strategy
            );

            match retry_fn(strategy.clone()) {
                Ok(result) => {
                    let recovery_attempt = RecoveryAttempt {
                        attempt_number: attempt + 1,
                        strategy: strategy.clone(),
                        success: true,
                        error_message: None,
                    };
                    self.recovery_log.push(recovery_attempt);

                    log::info!("Auto-recovery successful with strategy: {:?}", strategy);
                    return Ok(result);
                }
                Err(err) => {
                    let recovery_attempt = RecoveryAttempt {
                        attempt_number: attempt + 1,
                        strategy: strategy.clone(),
                        success: false,
                        error_message: Some(err.to_string()),
                    };
                    self.recovery_log.push(recovery_attempt);
                    last_error = err;

                    log::warn!("Auto-recovery attempt failed: {}", last_error);
                }
            }
        }

        // All recovery attempts failed
        let recovery_log_text = self
            .recovery_log
            .iter()
            .map(|attempt| {
                format!(
                    "Attempt {}: {:?} - {}",
                    attempt.attempt_number,
                    attempt.strategy,
                    if attempt.success { "Success" } else { "Failed" }
                )
            })
            .collect::<Vec<_>>()
            .join("; ");

        let last_strategy = self
            .recovery_log
            .last()
            .map(|attempt| format!("{:?}", attempt.strategy))
            .unwrap_or_else(|| "None".to_string());

        Err(DataProfilerError::recovery_failed(
            self.recovery_log.len(),
            &last_strategy,
            &recovery_log_text,
            &last_error.to_string(),
        ))
    }

    /// Get the recovery log
    pub fn get_recovery_log(&self) -> &[RecoveryAttempt] {
        &self.recovery_log
    }

    /// Clear the recovery log
    pub fn clear_log(&mut self) {
        self.recovery_log.clear();
    }
}

impl Default for AutoRecoveryManager {
    fn default() -> Self {
        Self::new(RetryConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_categorization() {
        let csv_error = DataProfilerError::csv_parsing("field count mismatch", Some("test.csv"));
        assert_eq!(csv_error.category(), "csv_parsing");
        assert!(!csv_error.is_recoverable());
    }

    #[test]
    fn test_recoverable_errors() {
        let simd_error = DataProfilerError::simd_unavailable("CPU doesn't support SIMD");
        assert!(simd_error.is_recoverable());
    }

    #[test]
    fn test_error_suggestions() {
        let config_error = DataProfilerError::invalid_config(
            "Invalid chunk size",
            "Use a value between 1000 and 100000",
        );

        let error_string = config_error.to_string();
        assert!(error_string.contains("Invalid chunk size"));
        assert!(error_string.contains("Use a value between"));
    }

    #[test]
    fn io_conversion_never_fabricates_a_path() {
        // A NotFound io::Error carries no path here; the conversion must not
        // invent one (the `'unknown'` placeholder the contract work removed).
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "no such file");
        let err = DataProfilerError::from(io_err);
        assert_eq!(err.category(), "io");
        assert!(!err.to_string().contains("unknown"));
    }

    #[test]
    fn csv_conversion_without_path_omits_the_filename() {
        // The `From<csv::Error>` boundary does not know the source file, so the
        // suggestion must not print `'unknown'` as if it were the filename.
        let err = DataProfilerError::csv_parsing("record has 5 fields but 3 expected", None);
        let msg = err.to_string();
        assert!(!msg.contains("unknown"));
        assert!(msg.contains("This CSV file has inconsistent column counts"));
    }

    #[test]
    fn csv_conversion_with_path_names_the_file() {
        let err =
            DataProfilerError::csv_parsing("record has 5 fields but 3 expected", Some("data.csv"));
        assert!(err.to_string().contains("The CSV file 'data.csv' has"));
    }

    #[test]
    fn file_not_found_keeps_the_real_path() {
        let err = DataProfilerError::file_not_found("/data/sales.csv");
        assert_eq!(err.category(), "file_not_found");
        assert!(err.to_string().contains("/data/sales.csv"));
    }

    #[test]
    fn unsupported_format_advertises_only_buildable_formats() {
        let err = DataProfilerError::unsupported_format("xlsx");
        let msg = err.to_string();
        assert!(msg.contains("CSV, JSON, JSONL"));
        // Parquet is only claimed when the build can actually read it.
        #[cfg(feature = "parquet")]
        assert!(msg.contains("Parquet"));
        #[cfg(not(feature = "parquet"))]
        assert!(!msg.contains("Parquet"));
    }

    #[test]
    fn suggestion_is_available_as_structured_context() {
        let err = DataProfilerError::unsupported_format("xlsx");
        assert!(err.suggestion().is_some());
        let hint = DataProfilerError::simd_unavailable("no avx2");
        assert!(hint.suggestion().is_none());
    }

    #[test]
    fn redact_credentials_scrubs_userinfo() {
        assert_eq!(
            redact_credentials("postgresql://admin:s3cret@db.internal:5432/sales"),
            "postgresql://***@db.internal:5432/sales"
        );
        // No userinfo → untouched.
        assert_eq!(
            redact_credentials("mysql://db.internal:3306/app"),
            "mysql://db.internal:3306/app"
        );
        // Embedded in a driver error message.
        let msg = "Failed to connect: error with url mysql://root:hunter2@localhost/db";
        let redacted = redact_credentials(msg);
        assert!(!redacted.contains("hunter2"));
        assert!(redacted.contains("mysql://***@localhost/db"));
    }

    #[test]
    fn database_connection_error_redacts_password() {
        let err = DataProfilerError::database_connection(
            "pool timed out connecting to postgres://user:topsecret@host/db",
        );
        assert!(!err.to_string().contains("topsecret"));
    }
}
