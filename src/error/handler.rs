use anyhow::Error;
use colored::Colorize;
use dataprof::core::{ValidationError, exit_codes};
use dataprof::{DataProfilerError, ErrorSeverity};
use std::path::Path;

#[allow(dead_code)]
pub fn handle_error(error: &Error, file_path: &Path) {
    // Check if it's our custom error type
    if let Some(dp_error) = error.downcast_ref::<DataProfilerError>() {
        let severity_icon = match dp_error.severity() {
            ErrorSeverity::Critical => "🔴",
            ErrorSeverity::High => "🟠",
            ErrorSeverity::Medium => "🟡",
            ErrorSeverity::Low => "🔵",
            ErrorSeverity::Info => "ℹ️",
        };

        eprintln!(
            "\n{} {} Error: {}",
            severity_icon,
            dp_error.severity(),
            dp_error
        );
    } else {
        // Handle generic errors with enhanced suggestions
        let error_str = error.to_string();

        if error_str.contains("No such file") || error_str.contains("not found") {
            let file_error = DataProfilerError::file_not_found(file_path.display().to_string());
            eprintln!("\n🔴 CRITICAL Error: {}", file_error);

            // Provide additional context
            if let Some(parent) = file_path.parent() {
                eprintln!("📂 Looking in directory: {}", parent.display());
            }

            // Suggest similar files
            if let Ok(entries) =
                std::fs::read_dir(file_path.parent().unwrap_or(std::path::Path::new(".")))
            {
                let similar_files: Vec<_> = entries
                    .filter_map(|entry| entry.ok())
                    .filter(|entry| {
                        if let Some(ext) = entry.path().extension() {
                            matches!(ext.to_str(), Some("csv") | Some("json") | Some("jsonl"))
                        } else {
                            false
                        }
                    })
                    .take(3)
                    .collect();

                if !similar_files.is_empty() {
                    eprintln!("🔍 Similar files found:");
                    for entry in similar_files {
                        eprintln!("   • {}", entry.path().display());
                    }
                }
            }
        } else if error_str.contains("CSV") {
            let csv_error =
                DataProfilerError::csv_parsing(&error_str, &file_path.display().to_string());
            eprintln!("\n🟠 HIGH Error: {}", csv_error);
        } else {
            eprintln!("\n❌ Error: {}", error);
            eprintln!("💡 For help, run: {} --help", "dataprof".bright_blue());
        }
    }
}

#[allow(dead_code)]
pub fn determine_exit_code(error: &Error) -> i32 {
    // Check if it's our custom error type first
    if let Some(dp_error) = error.downcast_ref::<DataProfilerError>() {
        match dp_error {
            DataProfilerError::FileNotFound { .. } => exit_codes::FILE_NOT_FOUND,
            DataProfilerError::UnsupportedFormat { .. } => exit_codes::INVALID_DATA_FORMAT,
            DataProfilerError::MemoryLimitExceeded => exit_codes::NO_SPACE_LEFT,
            DataProfilerError::InvalidConfiguration { .. } => exit_codes::CONFIG_ERROR,
            DataProfilerError::StreamingError { .. } => exit_codes::PROCESSING_ERROR,
            DataProfilerError::IoError { .. } => exit_codes::GENERAL_ERROR,
            DataProfilerError::JsonParsingError { .. } => exit_codes::INVALID_DATA_FORMAT,
            DataProfilerError::CsvParsingError { .. } => exit_codes::INVALID_DATA_FORMAT,
            _ => exit_codes::PROCESSING_ERROR,
        }
    } else if let Some(validation_error) = error.downcast_ref::<ValidationError>() {
        validation_error.error_code
    } else {
        // Handle common error types
        let error_str = error.to_string().to_lowercase();
        if error_str.contains("no such file") || error_str.contains("not found") {
            exit_codes::FILE_NOT_FOUND
        } else if error_str.contains("permission denied") || error_str.contains("access") {
            exit_codes::PERMISSION_DENIED
        } else if error_str.contains("invalid") || error_str.contains("malformed") {
            exit_codes::INVALID_DATA_FORMAT
        } else if error_str.contains("database") || error_str.contains("connection") {
            exit_codes::DATABASE_ERROR
        } else if error_str.contains("network") || error_str.contains("timeout") {
            exit_codes::NETWORK_ERROR
        } else {
            exit_codes::GENERAL_ERROR
        }
    }
}
