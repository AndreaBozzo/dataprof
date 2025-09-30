use anyhow::Result;
use dataprof::core::{exit_codes, InputValidator, ValidationError};

use crate::cli::args::Cli;

pub fn validate_cli_inputs(cli: &Cli) -> Result<(), ValidationError> {
    // Validate file input (unless it's engine info, benchmark mode, database mode, or batch processing)
    if !cli.engine_info && !cli.benchmark {
        // Skip file validation for database mode
        #[cfg(feature = "database")]
        let is_database_mode = cli.database.is_some();
        #[cfg(not(feature = "database"))]
        let is_database_mode = false;

        if !is_database_mode {
            // Allow directories for batch processing (recursive or glob mode)
            if cli.file.is_dir() && (cli.recursive || cli.glob.is_some()) {
                // For batch processing, just validate directory exists and is readable
                if !cli.file.exists() {
                    return Err(ValidationError {
                        message: format!("Directory not found: {}", cli.file.display()),
                        suggestion: "Check the directory path and permissions".to_string(),
                        error_code: 2,
                    });
                }
            } else {
                // Standard file validation for single file processing
                InputValidator::validate_file_input(&cli.file)?;
            }
        }
    }

    // Validate HTML output path if specified
    if let Some(html_path) = &cli.html {
        InputValidator::validate_output_directory(html_path)?;

        // Ensure HTML extension
        if html_path.extension().and_then(|s| s.to_str()) != Some("html") {
            return Err(ValidationError {
                message: "HTML output file must have .html extension".to_string(),
                suggestion: format!("Use: {}.html", html_path.with_extension("").display()),
                error_code: exit_codes::INVALID_ARGUMENT,
            });
        }
    }

    // Validate chunk size
    if let Some(chunk_size) = cli.chunk_size {
        InputValidator::validate_chunk_size(chunk_size)?;
    }

    // Validate sample size
    if let Some(sample_size) = cli.sample {
        InputValidator::validate_sample_size(sample_size)?;
    }

    // Validate argument combinations
    InputValidator::validate_argument_combinations(
        cli.streaming,
        cli.sample,
        cli.progress,
        cli.benchmark,
    )?;

    // Validate database connection if provided
    #[cfg(feature = "database")]
    if let Some(connection_string) = &cli.database {
        InputValidator::validate_database_connection(connection_string)?;
    }

    // Validate glob pattern if provided
    if let Some(pattern) = &cli.glob {
        InputValidator::validate_glob_pattern(pattern)?;
    }

    // Validate concurrent settings
    if cli.max_concurrent > 100 {
        return Err(ValidationError {
            message: format!("Max concurrent value too high: {}", cli.max_concurrent),
            suggestion: "Use a reasonable value (1-100) to avoid system overload".to_string(),
            error_code: exit_codes::INVALID_ARGUMENT,
        });
    }

    // HTML requires quality mode
    if cli.html.is_some() && !cli.quality {
        return Err(ValidationError {
            message: "HTML report requires quality checking".to_string(),
            suggestion: "Add --quality flag when using --html".to_string(),
            error_code: exit_codes::INVALID_ARGUMENT,
        });
    }

    Ok(())
}
