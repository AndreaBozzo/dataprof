use crate::binary_input::BinarySignature;
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

/// Render a remedy as its own line, or nothing at all when there is none.
///
/// Streaming failures are the one family where "no advice" is a real answer: a
/// panicked task has no remedy the caller can act on, and #550 removed the
/// hardcoded chunk-size line that used to fill the gap. Formatting the
/// suggestion unconditionally would end those messages in a bare newline.
fn suggestion_line(suggestion: &str) -> String {
    if suggestion.is_empty() {
        String::new()
    } else {
        format!("\n{suggestion}")
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

/// Boxed originating error retained by the variants that can carry a cause.
///
/// `Box` rather than `Arc` is deliberate. `thiserror` renders a boxed source
/// through `&**b`, so [`std::error::Error::source`] hands back the *concrete*
/// error and `downcast_ref::<std::io::Error>()` succeeds. Behind an `Arc` the
/// chain instead yields the `Arc`: the message still reads correctly while
/// every downcast returns `None`, which is the exact capability this type
/// exists to provide.
pub type ErrorSource = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Enhanced error types with more descriptive messages for DataProfiler
///
/// Not `Clone`: a retained [`ErrorSource`] is a boxed trait object, and the
/// originating errors (`std::io::Error`, `csv::Error`, ...) are not cloneable
/// themselves.
#[derive(Error, Debug)]
pub enum DataProfilerError {
    #[error("CSV parsing failed: {message}\nSuggestion: {suggestion}")]
    #[non_exhaustive]
    CsvParsingError {
        message: String,
        suggestion: String,
        #[source]
        source: Option<ErrorSource>,
    },

    #[error(
        "File not found: {path}\nPlease check that the file exists and you have permission to read it"
    )]
    FileNotFound { path: String },

    #[error(
        "Unsupported file format: {format}\nSupported formats: {}",
        supported_formats_hint()
    )]
    UnsupportedFormat { format: String },

    #[error("Invalid configuration: {message}\n{suggestion}")]
    #[non_exhaustive]
    InvalidConfiguration {
        message: String,
        suggestion: String,
        #[source]
        source: Option<ErrorSource>,
    },

    #[error("Invalid semantic hint: {message}\n{suggestion}")]
    InvalidSemanticHint { message: String, suggestion: String },

    #[error("Streaming processing failed: {message}{}", suggestion_line(.suggestion))]
    #[non_exhaustive]
    StreamingError {
        message: String,
        suggestion: String,
        #[source]
        source: Option<ErrorSource>,
    },

    #[error("I/O error: {message}\nCheck file permissions and disk space")]
    #[non_exhaustive]
    IoError {
        message: String,
        #[source]
        source: Option<ErrorSource>,
    },

    #[error(
        "Non-UTF-8 input in {path}: {detail}\nRe-encode the file as UTF-8 (e.g. `iconv -f {guess} -t UTF-8 '{path}'`) and profile the result"
    )]
    EncodingError {
        path: String,
        detail: String,
        guess: String,
    },

    /// A file whose signature bytes name a binary format (gzip, zstd, zip,
    /// Parquet, Arrow IPC, Feather V1) was about to reach a text reader
    /// (#893). Built with [`DataProfilerError::binary_input`].
    #[error("Binary input in {path}: the file is {detected}, not text\n{suggestion}")]
    #[non_exhaustive]
    BinaryInput {
        path: String,
        /// What the signature says the file is, e.g. `gzip-compressed`.
        detected: String,
        suggestion: String,
    },

    /// A remote object changed between the ranged reads of one profile, so
    /// the bytes read do not all come from one version of it (#865). Built
    /// with [`DataProfilerError::source_changed`].
    #[error("Source changed during the read: {path}: {detail}\n{suggestion}")]
    #[non_exhaustive]
    SourceChanged {
        path: String,
        /// What showed the change, e.g. the server answering 412 to `If-Match`.
        detail: String,
        suggestion: String,
    },

    #[error("JSON parsing failed: {message}\nVerify JSON format and encoding")]
    #[non_exhaustive]
    JsonParsingError {
        message: String,
        #[source]
        source: Option<ErrorSource>,
    },

    #[error("Parquet processing failed: {message}")]
    #[non_exhaustive]
    ParquetError {
        message: String,
        #[source]
        source: Option<ErrorSource>,
    },

    #[error("Arrow processing failed: {message}")]
    #[non_exhaustive]
    ArrowError {
        message: String,
        #[source]
        source: Option<ErrorSource>,
    },

    #[error("Unsupported data source: {message}")]
    UnsupportedDataSource { message: String },

    #[error("All engines failed: {message}")]
    AllEnginesFailed { message: String },

    #[error("Metrics calculation failed: {message}")]
    MetricsCalculationError { message: String },

    #[error(
        "Duplicate column name{plural}: {names}\nSource: {source_label}. Column names must be unique after normalization; rename or drop the duplicate column(s) before profiling."
    )]
    DuplicateColumnName {
        names: String,
        plural: String,
        source_label: String,
    },

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
    /// Create a duplicate-column-name error from the already-collected list of
    /// repeated names and a short transport label.
    ///
    /// Only the offending names and the source are embedded — never row values —
    /// so the diagnostic cannot leak cell data.
    pub fn duplicate_column_name(duplicates: &[String], source: &str) -> Self {
        let plural = if duplicates.len() == 1 { "" } else { "s" };
        let names = duplicates
            .iter()
            .map(|n| format!("'{}'", n))
            .collect::<Vec<_>>()
            .join(", ");
        DataProfilerError::DuplicateColumnName {
            names,
            plural: plural.to_string(),
            source_label: source.to_string(),
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
            source: None,
        }
    }

    /// Create a CSV parsing error that retains `original` as its source.
    ///
    /// The suggestion is derived from the same message text as
    /// [`DataProfilerError::csv_parsing`], so the two forms render identically;
    /// only the cause chain differs.
    pub fn csv_parsing_from<E>(original: E, file_path: Option<&str>) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        let mut err = Self::csv_parsing(&original.to_string(), file_path);
        if let DataProfilerError::CsvParsingError { source, .. } = &mut err {
            *source = Some(Box::new(original));
        }
        err
    }

    /// Create a file not found error with path context
    pub fn file_not_found<P: AsRef<str>>(path: P) -> Self {
        DataProfilerError::FileNotFound {
            path: path.as_ref().to_string(),
        }
    }

    /// A file at `path` whose bytes carry `signature`, refused before a text
    /// reader parses it.
    pub fn binary_input(path: &std::path::Path, signature: BinarySignature) -> Self {
        let path = path.display().to_string();
        DataProfilerError::BinaryInput {
            detected: signature.description().to_string(),
            suggestion: signature.suggestion(&path),
            path,
        }
    }

    /// The object at `path` changed while it was being read; `detail` says
    /// how that showed.
    pub fn source_changed(path: &str, detail: impl Into<String>) -> Self {
        DataProfilerError::SourceChanged {
            path: path.to_string(),
            detail: detail.into(),
            suggestion: "Profile it again once writes to it have finished, or profile a copy that is not being rewritten.".to_string(),
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
            source: None,
        }
    }

    /// Create a configuration error that retains `original` as its source.
    ///
    /// Used by the TOML and glob conversions, where the underlying parser
    /// reports a position worth reaching programmatically.
    pub fn invalid_config_with_source<E>(
        message: impl Into<String>,
        suggestion: impl Into<String>,
        original: E,
    ) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        DataProfilerError::InvalidConfiguration {
            message: message.into(),
            suggestion: suggestion.into(),
            source: Some(Box::new(original)),
        }
    }

    /// Create streaming error with context
    pub fn streaming_error(message: &str) -> Self {
        Self::streaming_with_suggestion(message, "")
    }

    /// Create a streaming error with an actionable suggestion and no source.
    ///
    /// An empty suggestion means there is no remedy to offer the caller.
    pub fn streaming_with_suggestion(
        message: impl Into<String>,
        suggestion: impl Into<String>,
    ) -> Self {
        DataProfilerError::StreamingError {
            message: message.into(),
            suggestion: suggestion.into(),
            source: None,
        }
    }

    /// Create a streaming error that retains `original` without changing its
    /// contextual message, suggestion, or streaming category.
    pub fn streaming_with_source<E>(
        message: impl Into<String>,
        suggestion: impl Into<String>,
        original: E,
    ) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        DataProfilerError::StreamingError {
            message: message.into(),
            suggestion: suggestion.into(),
            source: Some(Box::new(original)),
        }
    }

    /// Create an I/O error that retains `original` as its source.
    ///
    /// Takes the error by value: `std::io::Error` is not cloneable, so a
    /// borrowed one could only ever be stringified, which is what this issue
    /// set out to stop. Use `map_err(DataProfilerError::io_error)` at call
    /// sites.
    pub fn io_error(original: std::io::Error) -> Self {
        DataProfilerError::IoError {
            message: original.to_string(),
            source: Some(Box::new(original)),
        }
    }

    /// Create an I/O error from a message, for callers with no originating
    /// error to retain (a condition detected by dataprof itself rather than
    /// reported by the OS).
    pub fn io_message(message: &str) -> Self {
        DataProfilerError::IoError {
            message: message.to_string(),
            source: None,
        }
    }

    /// Create a JSON parsing error with no retained source.
    pub fn json_parsing_error(original: &str) -> Self {
        DataProfilerError::JsonParsingError {
            message: original.to_string(),
            source: None,
        }
    }

    /// Create a JSON parsing error that retains `original` as its source.
    pub fn json_parsing_from<E>(original: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        DataProfilerError::JsonParsingError {
            message: original.to_string(),
            source: Some(Box::new(original)),
        }
    }

    /// Create a CSV parsing error with a caller-supplied message and
    /// suggestion that still retains `original` as its source.
    ///
    /// For callers whose suggestion is specific to their own context and so
    /// cannot come from [`DataProfilerError::csv_parsing`]'s message analysis.
    pub fn csv_parsing_with_source<E>(
        message: impl Into<String>,
        suggestion: impl Into<String>,
        original: E,
    ) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        DataProfilerError::CsvParsingError {
            message: message.into(),
            suggestion: suggestion.into(),
            source: Some(Box::new(original)),
        }
    }

    /// Create a CSV parsing error for a structural problem the parser itself
    /// accepted, so there is no underlying error to keep as its source.
    pub fn csv_structure(message: impl Into<String>, suggestion: impl Into<String>) -> Self {
        DataProfilerError::CsvParsingError {
            message: message.into(),
            suggestion: suggestion.into(),
            source: None,
        }
    }

    /// Create a JSON parsing error with a caller-supplied message that still
    /// retains `original` as its source.
    ///
    /// Used where the message adds context the decoder does not have (a record
    /// position, a physical line number) and the decoder error is still worth
    /// keeping underneath it.
    pub fn json_parsing_with_source<E>(message: impl Into<String>, original: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        DataProfilerError::JsonParsingError {
            message: message.into(),
            source: Some(Box::new(original)),
        }
    }

    /// Create an Arrow error with a caller-supplied message that still retains
    /// `original` as its source.
    pub fn arrow_with_source<E>(message: impl Into<String>, original: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        DataProfilerError::ArrowError {
            message: message.into(),
            source: Some(Box::new(original)),
        }
    }

    /// Create a Parquet error with a caller-supplied message that still retains
    /// `original` as its source.
    pub fn parquet_with_source<E>(message: impl Into<String>, original: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        DataProfilerError::ParquetError {
            message: message.into(),
            source: Some(Box::new(original)),
        }
    }

    /// Create an I/O error with a caller-supplied message that still retains
    /// `original` as its source.
    pub fn io_with_source<E>(message: impl Into<String>, original: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        DataProfilerError::IoError {
            message: message.into(),
            source: Some(Box::new(original)),
        }
    }

    /// Create an Arrow error with no retained source.
    pub fn arrow_error(message: &str) -> Self {
        DataProfilerError::ArrowError {
            message: message.to_string(),
            source: None,
        }
    }

    /// Create an Arrow error that retains `original` as its source.
    pub fn arrow_error_from<E>(original: E) -> Self
    where
        E: std::error::Error + Send + Sync + 'static,
    {
        DataProfilerError::ArrowError {
            message: original.to_string(),
            source: Some(Box::new(original)),
        }
    }

    /// Create a Parquet error with no retained source.
    pub fn parquet_error(message: &str) -> Self {
        DataProfilerError::ParquetError {
            message: message.to_string(),
            source: None,
        }
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
            | DataProfilerError::BinaryInput { suggestion, .. }
            | DataProfilerError::SourceChanged { suggestion, .. }
            | DataProfilerError::DatabaseConnectionError { suggestion, .. } => {
                Some(suggestion.clone())
            }
            DataProfilerError::FileNotFound { .. } => {
                Some("Check that the file exists and you have permission to read it.".to_string())
            }
            DataProfilerError::UnsupportedFormat { .. } => Some(format!(
                "This build reads {}. Convert the input to one of these formats, or rebuild with the feature for the format you need.",
                supported_formats_hint()
            )),
            DataProfilerError::EncodingError { guess, .. } => Some(format!(
                "Re-encode the file as UTF-8 (e.g. `iconv -f {guess} -t UTF-8`) and profile the result."
            )),
            DataProfilerError::StreamingError { suggestion, .. } => {
                if suggestion.is_empty() {
                    None
                } else {
                    Some(suggestion.clone())
                }
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
            DataProfilerError::EncodingError { .. } => "encoding",
            DataProfilerError::BinaryInput { .. } => "binary_input",
            DataProfilerError::SourceChanged { .. } => "source_changed",
            DataProfilerError::InvalidConfiguration { .. } => "configuration",
            DataProfilerError::InvalidSemanticHint { .. } => "semantic_hint",
            DataProfilerError::StreamingError { .. } => "streaming",
            DataProfilerError::IoError { .. } => "io",
            DataProfilerError::JsonParsingError { .. } => "json_parsing",
            DataProfilerError::ParquetError { .. } => "parquet",
            DataProfilerError::ArrowError { .. } => "arrow",
            DataProfilerError::UnsupportedDataSource { .. } => "unsupported_data_source",
            DataProfilerError::AllEnginesFailed { .. } => "all_engines_failed",
            DataProfilerError::MetricsCalculationError { .. } => "metrics_calculation",
            DataProfilerError::DuplicateColumnName { .. } => "duplicate_column_name",
            DataProfilerError::ConfigValidationError { .. } => "config_validation",
            DataProfilerError::DatabaseConnectionError { .. } => "database_connection",
            DataProfilerError::DatabaseQueryError { .. } => "database_query",
            DataProfilerError::DatabaseConfigError { .. } => "database_config",
            DataProfilerError::DatabaseFeatureDisabled { .. } => "database_feature_disabled",
            DataProfilerError::SqlValidationError { .. } => "sql_validation",
            DataProfilerError::DatabaseSslError { .. } => "database_ssl",
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

        // The anyhow error is retained as the source in every branch, so a
        // caller can still reach the concrete error underneath it via
        // `source()` even though categorisation here is message-based.
        let source: Option<ErrorSource> = Some(err.into());

        // Categorize by message only; do not invent a path we do not have.
        if error_str.contains("CSV") {
            DataProfilerError::CsvParsingError {
                message: error_str,
                suggestion: "Try using robust CSV parsing mode".to_string(),
                source,
            }
        } else if error_str.contains("JSON") {
            DataProfilerError::JsonParsingError {
                message: error_str,
                source,
            }
        } else {
            // Generic I/O error — the original message carries the detail
            // (including "file not found" wording) without a fabricated path.
            DataProfilerError::IoError {
                message: error_str,
                source,
            }
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
        // The message is still rewritten for PermissionDenied, but the original
        // error is retained either way: `err.source()` now yields the
        // `std::io::Error`, so a caller can match on `kind()` instead of
        // pattern-matching the human-readable text.
        let message = match err.kind() {
            std::io::ErrorKind::PermissionDenied => {
                "Permission denied - check file access rights".to_string()
            }
            _ => err.to_string(),
        };
        DataProfilerError::IoError {
            message,
            source: Some(Box::new(err)),
        }
    }
}

/// Convert from csv::Error to DataProfilerError with enhanced context.
///
/// The source path is unknown at this boundary, so the suggestion is written
/// without a filename rather than the misleading `'unknown'` placeholder.
impl From<csv::Error> for DataProfilerError {
    fn from(err: csv::Error) -> Self {
        DataProfilerError::csv_parsing_from(err, None)
    }
}

/// Convert from arrow::error::ArrowError to DataProfilerError
#[cfg(feature = "arrow")]
impl From<arrow::error::ArrowError> for DataProfilerError {
    fn from(err: arrow::error::ArrowError) -> Self {
        DataProfilerError::arrow_error_from(err)
    }
}

/// Convert from serde_json::Error to DataProfilerError
impl From<serde_json::Error> for DataProfilerError {
    fn from(err: serde_json::Error) -> Self {
        DataProfilerError::json_parsing_from(err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_categorization() {
        let csv_error = DataProfilerError::csv_parsing("field count mismatch", Some("test.csv"));
        assert_eq!(csv_error.category(), "csv_parsing");
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
    fn streaming_error_without_suggestion_prints_no_stale_advice() {
        let err = DataProfilerError::streaming_error("Reader task panicked: boxed error");
        let msg = err.to_string();
        assert!(msg.contains("Reader task panicked"));
        assert!(!msg.contains("chunk size"), "stale remedy leaked: {msg}");
        assert!(err.suggestion().is_none());
        assert!(std::error::Error::source(&err).is_none());
        // No remedy means no line for one: an unconditional "\n{suggestion}"
        // leaves the message ending in a bare newline, which surfaces in Python
        // as a blank line under the error.
        assert!(
            !msg.ends_with('\n'),
            "message ends in a bare newline: {msg:?}"
        );
        assert!(
            !msg.contains('\n'),
            "empty remedy still opened a line: {msg:?}"
        );
    }

    #[test]
    fn streaming_error_with_suggestion_prints_the_remedy_exactly_once() {
        let err = DataProfilerError::streaming_with_suggestion(
            "Parquet schema inference requires random access",
            "Use infer_schema() with a file path instead",
        );
        let msg = err.to_string();
        assert!(msg.contains("random access"), "{msg}");
        assert_eq!(
            msg.matches("Use infer_schema() with a file path instead")
                .count(),
            1,
            "remedy must appear exactly once: {msg}"
        );
        assert!(!msg.contains("chunk size"), "stale remedy leaked: {msg}");
        // A remedy still gets its own line, and only one.
        assert_eq!(
            msg.matches('\n').count(),
            1,
            "remedy is not on its own line: {msg:?}"
        );
        assert!(
            !msg.ends_with('\n'),
            "trailing newline after the remedy: {msg:?}"
        );
        assert_eq!(
            err.suggestion(),
            Some("Use infer_schema() with a file path instead".to_string())
        );
    }

    #[test]
    fn encoding_error_names_file_and_gives_reencode_advice() {
        let err = DataProfilerError::EncodingError {
            path: "sales.csv".to_string(),
            detail: "first invalid UTF-8 byte at offset 12".to_string(),
            guess: "windows-1252".to_string(),
        };
        assert_eq!(err.category(), "encoding");
        let msg = err.to_string();
        assert!(msg.contains("sales.csv"), "names the file: {msg}");
        assert!(msg.contains("offset 12"), "reports the offset: {msg}");
        assert!(
            msg.to_lowercase().contains("utf-8"),
            "mentions UTF-8: {msg}"
        );
        assert!(msg.contains("iconv"), "gives a re-encode command: {msg}");
        // The old, misleading advice must not appear.
        assert!(
            !msg.to_lowercase().contains("check file permissions"),
            "{msg}"
        );
        assert!(!msg.contains("All engines failed"), "{msg}");
        assert!(err.suggestion().unwrap().contains("windows-1252"));
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
        let hint = DataProfilerError::streaming_error("reader task panicked");
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

    // --- source chain (#447) ---------------------------------------------
    //
    // The point of retaining a source is that a caller can recover the
    // *concrete* originating error, not just a string that reads like it. Each
    // test below therefore downcasts, rather than comparing rendered text.

    use std::error::Error as _;

    #[test]
    fn io_error_retains_the_os_error_and_its_kind() {
        let original = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied by acl");
        let err = DataProfilerError::io_error(original);

        let source = err.source().expect("io error must retain a source");
        let io: &std::io::Error = source
            .downcast_ref()
            .expect("source must downcast to the original std::io::Error");
        assert_eq!(io.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn from_io_error_retains_the_kind_even_when_the_message_is_rewritten() {
        // PermissionDenied is the one branch that replaces the OS text, so it is
        // the case where a flattened source would lose the most.
        let original = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "denied by acl");
        let err: DataProfilerError = original.into();

        assert!(err.to_string().contains("Permission denied"));
        let io: &std::io::Error = err
            .source()
            .and_then(|s| s.downcast_ref())
            .expect("rewritten message must still retain the io::Error");
        assert_eq!(io.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(io.to_string().contains("denied by acl"));
    }

    #[test]
    fn from_json_error_retains_line_and_column() {
        let original = serde_json::from_str::<serde_json::Value>("{oops}").unwrap_err();
        let (line, column) = (original.line(), original.column());
        let err: DataProfilerError = original.into();

        let json: &serde_json::Error = err
            .source()
            .and_then(|s| s.downcast_ref())
            .expect("source must downcast to serde_json::Error");
        assert_eq!((json.line(), json.column()), (line, column));
    }

    #[test]
    fn from_csv_error_retains_the_csv_error_kind() {
        let mut reader = csv::ReaderBuilder::new()
            .flexible(false)
            .from_reader("a,b\n1,2,3\n".as_bytes());
        let original = reader
            .records()
            .next()
            .expect("one record")
            .expect_err("ragged row must fail with flexible(false)");
        let err: DataProfilerError = original.into();

        let csv: &csv::Error = err
            .source()
            .and_then(|s| s.downcast_ref())
            .expect("source must downcast to csv::Error");
        assert!(matches!(csv.kind(), csv::ErrorKind::UnequalLengths { .. }));
    }

    // Gated exactly like the conversion it covers: without the feature there is
    // no `From<ArrowError>` impl to test.
    #[cfg(feature = "arrow")]
    #[test]
    fn from_arrow_error_retains_the_arrow_error() {
        let original = arrow::error::ArrowError::ComputeError("overflow in sum".to_string());
        let expected = original.to_string();
        let err: DataProfilerError = original.into();

        assert!(matches!(err, DataProfilerError::ArrowError { .. }));
        let arrow_err: &arrow::error::ArrowError = err
            .source()
            .and_then(|s| s.downcast_ref())
            .expect("source must downcast to arrow::error::ArrowError");
        assert_eq!(arrow_err.to_string(), expected);
        assert!(matches!(
            arrow_err,
            arrow::error::ArrowError::ComputeError(_)
        ));
    }

    // Parquet failures reach the user through `parquet_with_source`, which the
    // reader paths use; this covers that constructor's retention contract with
    // a stand-in error so the test needs no parquet dependency here.
    #[test]
    fn parquet_with_source_retains_the_reader_error() {
        let original = std::io::Error::new(std::io::ErrorKind::InvalidData, "Corrupt footer");
        let err =
            DataProfilerError::parquet_with_source("Failed to create Parquet reader", original);

        assert!(err.to_string().contains("Failed to create Parquet reader"));
        let io: &std::io::Error = err
            .source()
            .and_then(|s| s.downcast_ref())
            .expect("parquet errors must retain the reader failure");
        assert_eq!(io.kind(), std::io::ErrorKind::InvalidData);
    }

    #[test]
    fn errors_built_without_a_cause_report_no_source() {
        // "No source" must stay distinguishable from "source not retained":
        // these variants genuinely have no originating error underneath.
        assert!(
            DataProfilerError::io_message("disk budget exceeded")
                .source()
                .is_none()
        );
        assert!(
            DataProfilerError::json_parsing_error("not an object")
                .source()
                .is_none()
        );
        assert!(
            DataProfilerError::arrow_error("downcast failed")
                .source()
                .is_none()
        );
        assert!(
            DataProfilerError::file_not_found("/nope")
                .source()
                .is_none()
        );
    }

    #[test]
    fn retaining_a_source_does_not_change_the_rendered_message() {
        // The #373 message contract is independent of the cause chain: the two
        // constructions must be indistinguishable in output.
        let original = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
        let text = original.to_string();

        let with_source = DataProfilerError::io_error(original).to_string();
        let without_source = DataProfilerError::io_message(&text).to_string();
        assert_eq!(with_source, without_source);
    }

    #[test]
    fn the_whole_chain_is_walkable_to_the_root() {
        let original = std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated");
        let err = DataProfilerError::io_with_source("reading chunk 4", original);

        let mut depth = 0;
        let mut current: Option<&(dyn std::error::Error + 'static)> = Some(&err);
        while let Some(e) = current {
            current = e.source();
            depth += 1;
        }
        // Exactly two links: the DataProfilerError and the io::Error beneath it.
        // A wrapper type in between would make this 3 and break downcasting.
        assert_eq!(depth, 2);
    }
}
