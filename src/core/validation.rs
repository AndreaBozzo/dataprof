use anyhow::Result;
use std::path::{Path, PathBuf};

/// Enhanced input validation with helpful error messages and suggestions
pub struct InputValidator;

#[derive(Debug)]
pub struct ValidationError {
    pub message: String,
    pub suggestion: String,
    pub error_code: i32,
}

impl std::fmt::Display for ValidationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}\n💡 {}", self.message, self.suggestion)
    }
}

impl std::error::Error for ValidationError {}

impl InputValidator {
    /// Validate file input with helpful suggestions
    pub fn validate_file_input(file_path: &Path) -> Result<(), ValidationError> {
        // Check if file exists
        if !file_path.exists() {
            return Err(ValidationError {
                message: format!("File not found: {}", file_path.display()),
                suggestion: Self::generate_file_suggestions(file_path),
                error_code: 2, // ENOENT
            });
        }

        // Check if it's actually a file (not directory)
        if file_path.is_dir() {
            return Err(ValidationError {
                message: format!("Path is a directory, not a file: {}", file_path.display()),
                suggestion: "Use --recursive flag to process directories, or specify a file path"
                    .to_string(),
                error_code: 21, // EISDIR
            });
        }

        // Check file extension
        Self::validate_file_extension(file_path)?;

        // Check file permissions
        Self::validate_file_permissions(file_path)?;

        // Check file size (warn for very large files)
        Self::validate_file_size(file_path)?;

        Ok(())
    }

    /// Validate configuration file
    pub fn validate_config_file(config_path: &Path) -> Result<(), ValidationError> {
        if !config_path.exists() {
            return Err(ValidationError {
                message: format!("Configuration file not found: {}", config_path.display()),
                suggestion: format!(
                    "Create a config file at {} or use 'dataprof --help' to see configuration options",
                    config_path.display()
                ),
                error_code: 2,
            });
        }

        // Check if it's a TOML file
        if let Some(ext) = config_path.extension()
            && ext != "toml"
        {
            return Err(ValidationError {
                message: "Configuration file must have .toml extension".to_string(),
                suggestion:
                    "Rename your config file to have .toml extension (e.g., .dataprof.toml)"
                        .to_string(),
                error_code: 22, // EINVAL
            });
        }

        Ok(())
    }

    /// Validate output directory for HTML reports
    pub fn validate_output_directory(output_path: &Path) -> Result<(), ValidationError> {
        if let Some(parent) = output_path.parent() {
            // Skip validation if parent is current directory or empty
            if parent.as_os_str().is_empty() || parent == Path::new(".") {
                return Ok(());
            }

            if !parent.exists() {
                return Err(ValidationError {
                    message: format!("Output directory does not exist: {}", parent.display()),
                    suggestion: format!(
                        "Create the directory first: mkdir -p \"{}\"",
                        parent.display()
                    ),
                    error_code: 2,
                });
            }

            // Check write permissions on parent directory
            if parent
                .metadata()
                .map(|m| m.permissions().readonly())
                .unwrap_or(true)
            {
                return Err(ValidationError {
                    message: format!("Cannot write to directory: {}", parent.display()),
                    suggestion: "Check directory permissions or choose a different output location"
                        .to_string(),
                    error_code: 13, // EACCES
                });
            }
        }

        Ok(())
    }

    /// Validate chunk size parameter
    pub fn validate_chunk_size(chunk_size: usize) -> Result<(), ValidationError> {
        if chunk_size == 0 {
            return Err(ValidationError {
                message: "Chunk size cannot be zero".to_string(),
                suggestion: "Use a positive chunk size (e.g., --chunk-size 1000) or omit for adaptive sizing".to_string(),
                error_code: 22,
            });
        }

        if chunk_size < 10 {
            return Err(ValidationError {
                message: format!("Chunk size too small: {}", chunk_size),
                suggestion: "Use at least 10 rows per chunk for efficient processing".to_string(),
                error_code: 22,
            });
        }

        if chunk_size > 10_000_000 {
            return Err(ValidationError {
                message: format!("Chunk size very large: {}", chunk_size),
                suggestion: "Consider using smaller chunks (< 10M rows) to avoid memory issues"
                    .to_string(),
                error_code: 22,
            });
        }

        Ok(())
    }

    /// Validate sample size parameter
    pub fn validate_sample_size(sample_size: usize) -> Result<(), ValidationError> {
        if sample_size == 0 {
            return Err(ValidationError {
                message: "Sample size cannot be zero".to_string(),
                suggestion:
                    "Use a positive sample size (e.g., --sample 10000) or omit for full analysis"
                        .to_string(),
                error_code: 22,
            });
        }

        if sample_size < 100 {
            return Err(ValidationError {
                message: format!("Sample size very small: {}", sample_size),
                suggestion: "Use at least 100 samples for meaningful statistical analysis"
                    .to_string(),
                error_code: 22,
            });
        }

        Ok(())
    }

    /// Validate conflicting arguments
    pub fn validate_argument_combinations(
        streaming: bool,
        sample: Option<usize>,
        progress: bool,
        benchmark: bool,
    ) -> Result<(), ValidationError> {
        // Progress requires streaming
        if progress && !streaming {
            return Err(ValidationError {
                message: "Progress display requires streaming mode".to_string(),
                suggestion: "Add --streaming flag when using --progress".to_string(),
                error_code: 22,
            });
        }

        // Benchmark conflicts with other modes
        if benchmark && streaming {
            return Err(ValidationError {
                message: "Benchmark mode conflicts with streaming".to_string(),
                suggestion: "Use either --benchmark OR --streaming, not both".to_string(),
                error_code: 22,
            });
        }

        if benchmark && sample.is_some() {
            return Err(ValidationError {
                message: "Benchmark mode conflicts with sampling".to_string(),
                suggestion: "Use either --benchmark OR --sample, not both".to_string(),
                error_code: 22,
            });
        }

        Ok(())
    }

    /// Generate helpful file suggestions
    fn generate_file_suggestions(file_path: &Path) -> String {
        let mut suggestions = Vec::new();

        // Check parent directory
        if let Some(parent) = file_path.parent() {
            if parent.exists() {
                // Look for similar files
                if let Ok(entries) = std::fs::read_dir(parent) {
                    let similar_files: Vec<PathBuf> = entries
                        .filter_map(|entry| entry.ok())
                        .filter(|entry| {
                            if let Some(ext) = entry.path().extension() {
                                matches!(ext.to_str(), Some("csv") | Some("json") | Some("jsonl"))
                            } else {
                                false
                            }
                        })
                        .take(3)
                        .map(|entry| entry.path())
                        .collect();

                    if !similar_files.is_empty() {
                        suggestions.push(format!("Similar files found in {}:", parent.display()));
                        for file in similar_files {
                            suggestions.push(format!("  • {}", file.display()));
                        }
                    }
                }
            } else {
                suggestions.push(format!(
                    "Parent directory does not exist: {}",
                    parent.display()
                ));
            }
        }

        // Check current directory
        if file_path.is_relative() {
            suggestions
                .push("Try using an absolute path or check your current directory".to_string());
        }

        if suggestions.is_empty() {
            "Check the file path and make sure the file exists".to_string()
        } else {
            suggestions.join("\n")
        }
    }

    /// Validate file extension
    fn validate_file_extension(file_path: &Path) -> Result<(), ValidationError> {
        if let Some(ext) = file_path.extension().and_then(|e| e.to_str()) {
            match ext.to_lowercase().as_str() {
                "csv" | "json" | "jsonl" => Ok(()),
                _ => Err(ValidationError {
                    message: format!("Unsupported file format: .{}", ext),
                    suggestion: "Supported formats: .csv, .json, .jsonl".to_string(),
                    error_code: 22,
                }),
            }
        } else {
            Err(ValidationError {
                message: "File has no extension or unrecognizable format".to_string(),
                suggestion: "Use files with extensions: .csv, .json, or .jsonl".to_string(),
                error_code: 22,
            })
        }
    }

    /// Validate file permissions
    fn validate_file_permissions(file_path: &Path) -> Result<(), ValidationError> {
        match std::fs::metadata(file_path) {
            Ok(metadata) => {
                if metadata.permissions().readonly() {
                    // This is actually OK for reading, but warn if they might want to write
                    log::debug!("File is read-only: {}", file_path.display());
                }
                Ok(())
            }
            Err(e) => Err(ValidationError {
                message: format!("Cannot access file metadata: {}", e),
                suggestion: "Check file permissions and try again".to_string(),
                error_code: 13,
            }),
        }
    }

    /// Validate file size and provide warnings
    fn validate_file_size(file_path: &Path) -> Result<(), ValidationError> {
        match std::fs::metadata(file_path) {
            Ok(metadata) => {
                let size_mb = metadata.len() as f64 / 1_048_576.0;

                if size_mb > 1000.0 {
                    // Large file warning, not error
                    log::warn!(
                        "Large file detected ({:.1} MB). Consider using --streaming for better performance",
                        size_mb
                    );
                }

                if size_mb > 10_000.0 {
                    return Err(ValidationError {
                        message: format!("File very large: {:.1} GB", size_mb / 1024.0),
                        suggestion: "Use --streaming --sample for very large files, or ensure sufficient memory".to_string(),
                        error_code: 27, // EFBIG
                    });
                }

                Ok(())
            }
            Err(e) => Err(ValidationError {
                message: format!("Cannot check file size: {}", e),
                suggestion: "Ensure file is accessible and try again".to_string(),
                error_code: 13,
            }),
        }
    }

    /// Validate database connection string format
    #[cfg(feature = "database")]
    pub fn validate_database_connection(connection_string: &str) -> Result<(), ValidationError> {
        if connection_string.is_empty() {
            return Err(ValidationError {
                message: "Database connection string is empty".to_string(),
                suggestion:
                    "Provide a valid connection string (e.g., postgresql://user:pass@host/db)"
                        .to_string(),
                error_code: 22,
            });
        }

        // Basic format validation
        if !connection_string.contains("://") {
            return Err(ValidationError {
                message: "Invalid connection string format".to_string(),
                suggestion: "Use format: protocol://[user:password@]host[:port]/database"
                    .to_string(),
                error_code: 22,
            });
        }

        // Check for supported protocols
        let supported_protocols = ["postgresql", "postgres", "mysql", "sqlite"];
        let protocol = connection_string.split("://").next().unwrap_or("");

        if !supported_protocols.contains(&protocol) {
            return Err(ValidationError {
                message: format!("Unsupported database protocol: {}", protocol),
                suggestion: format!("Supported protocols: {}", supported_protocols.join(", ")),
                error_code: 22,
            });
        }

        Ok(())
    }

    /// Validate glob pattern
    pub fn validate_glob_pattern(pattern: &str) -> Result<(), ValidationError> {
        if pattern.is_empty() {
            return Err(ValidationError {
                message: "Glob pattern is empty".to_string(),
                suggestion: "Provide a valid glob pattern (e.g., \"data/**/*.csv\")".to_string(),
                error_code: 22,
            });
        }

        // Test if pattern compiles
        match glob::Pattern::new(pattern) {
            Ok(_) => Ok(()),
            Err(e) => Err(ValidationError {
                message: format!("Invalid glob pattern: {}", e),
                suggestion: "Use valid glob syntax with *, **, ?, [abc], etc.".to_string(),
                error_code: 22,
            }),
        }
    }

    /// Get appropriate exit code for validation error
    pub fn get_exit_code(error: &ValidationError) -> i32 {
        error.error_code
    }
}

/// Exit codes following Unix conventions
pub mod exit_codes {
    pub const SUCCESS: i32 = 0;
    pub const GENERAL_ERROR: i32 = 1;
    pub const FILE_NOT_FOUND: i32 = 2;
    pub const PERMISSION_DENIED: i32 = 13;
    pub const INVALID_ARGUMENT: i32 = 22;
    pub const FILE_TOO_LARGE: i32 = 27;
    pub const NO_SPACE_LEFT: i32 = 28;
    pub const BROKEN_PIPE: i32 = 32;

    // Custom application codes
    pub const INVALID_DATA_FORMAT: i32 = 65;
    pub const PROCESSING_ERROR: i32 = 66;
    pub const CONFIG_ERROR: i32 = 67;
    pub const DATABASE_ERROR: i32 = 68;
    pub const NETWORK_ERROR: i32 = 69;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    // -- chunk size validation --

    #[test]
    fn test_chunk_size_zero_rejected() {
        assert!(InputValidator::validate_chunk_size(0).is_err());
    }

    #[test]
    fn test_chunk_size_too_small_rejected() {
        assert!(InputValidator::validate_chunk_size(5).is_err());
    }

    #[test]
    fn test_chunk_size_too_large_rejected() {
        assert!(InputValidator::validate_chunk_size(20_000_000).is_err());
    }

    #[test]
    fn test_chunk_size_valid() {
        assert!(InputValidator::validate_chunk_size(1000).is_ok());
        assert!(InputValidator::validate_chunk_size(10).is_ok());
        assert!(InputValidator::validate_chunk_size(10_000_000).is_ok());
    }

    // -- sample size validation --

    #[test]
    fn test_sample_size_zero_rejected() {
        assert!(InputValidator::validate_sample_size(0).is_err());
    }

    #[test]
    fn test_sample_size_too_small_rejected() {
        assert!(InputValidator::validate_sample_size(50).is_err());
    }

    #[test]
    fn test_sample_size_valid() {
        assert!(InputValidator::validate_sample_size(100).is_ok());
        assert!(InputValidator::validate_sample_size(10_000).is_ok());
    }

    // -- argument combinations --

    #[test]
    fn test_progress_without_streaming_rejected() {
        let result = InputValidator::validate_argument_combinations(false, None, true, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_benchmark_with_streaming_rejected() {
        let result = InputValidator::validate_argument_combinations(true, None, false, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_benchmark_with_sample_rejected() {
        let result = InputValidator::validate_argument_combinations(false, Some(1000), false, true);
        assert!(result.is_err());
    }

    #[test]
    fn test_valid_argument_combinations() {
        // streaming + progress: OK
        assert!(InputValidator::validate_argument_combinations(true, None, true, false).is_ok());
        // no flags: OK
        assert!(InputValidator::validate_argument_combinations(false, None, false, false).is_ok());
        // benchmark alone: OK
        assert!(InputValidator::validate_argument_combinations(false, None, false, true).is_ok());
    }

    // -- file validation --

    #[test]
    fn test_validate_file_nonexistent() {
        let result = InputValidator::validate_file_input(Path::new("/nonexistent/file.csv"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_code, 2); // ENOENT
    }

    #[test]
    fn test_validate_file_directory_rejected() {
        let dir = tempfile::tempdir().unwrap();
        let result = InputValidator::validate_file_input(dir.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.error_code, 21); // EISDIR
    }

    #[test]
    fn test_validate_file_unsupported_extension() {
        let mut f = NamedTempFile::with_suffix(".xlsx").unwrap();
        write!(f, "data").unwrap();
        f.flush().unwrap();
        let result = InputValidator::validate_file_input(f.path());
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().error_code, 22); // EINVAL
    }

    #[test]
    fn test_validate_file_valid_csv() {
        let mut f = NamedTempFile::with_suffix(".csv").unwrap();
        write!(f, "a,b\n1,2\n").unwrap();
        f.flush().unwrap();
        assert!(InputValidator::validate_file_input(f.path()).is_ok());
    }

    #[test]
    fn test_validate_file_valid_json() {
        let mut f = NamedTempFile::with_suffix(".json").unwrap();
        write!(f, "[]").unwrap();
        f.flush().unwrap();
        assert!(InputValidator::validate_file_input(f.path()).is_ok());
    }

    // -- glob pattern validation --

    #[test]
    fn test_glob_pattern_empty_rejected() {
        assert!(InputValidator::validate_glob_pattern("").is_err());
    }

    #[test]
    fn test_glob_pattern_valid() {
        assert!(InputValidator::validate_glob_pattern("*.csv").is_ok());
        assert!(InputValidator::validate_glob_pattern("data/**/*.json").is_ok());
    }

    // -- output directory validation --

    #[test]
    fn test_output_directory_current_dir_ok() {
        assert!(InputValidator::validate_output_directory(Path::new("report.html")).is_ok());
    }

    #[test]
    fn test_output_directory_nonexistent_parent() {
        let result =
            InputValidator::validate_output_directory(Path::new("/no/such/dir/report.html"));
        assert!(result.is_err());
    }

    // -- config file validation --

    #[test]
    fn test_config_file_nonexistent() {
        assert!(InputValidator::validate_config_file(Path::new("/no/config.toml")).is_err());
    }

    #[test]
    fn test_config_file_wrong_extension() {
        let mut f = NamedTempFile::with_suffix(".yaml").unwrap();
        write!(f, "key: value").unwrap();
        f.flush().unwrap();
        assert!(InputValidator::validate_config_file(f.path()).is_err());
    }

    #[test]
    fn test_config_file_valid_toml() {
        let mut f = NamedTempFile::with_suffix(".toml").unwrap();
        write!(f, "[settings]").unwrap();
        f.flush().unwrap();
        assert!(InputValidator::validate_config_file(f.path()).is_ok());
    }
}
