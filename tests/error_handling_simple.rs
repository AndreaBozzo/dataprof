/// Simplified Error Handling Tests for DataProfiler
/// Tests critical error scenarios that should be properly handled
use anyhow::Result;
use dataprof::core::errors::{DataProfilerError, ErrorSeverity};
use dataprof::core::robust_csv::RobustCsvParser;
use std::io::Write;
use tempfile::NamedTempFile;

#[test]
fn test_malformed_csv_field_count_mismatch() -> Result<()> {
    let malformed_csv = "col1,col2,col3\nvalue1,value2\nvalue1,value2,value3,value4\n";
    let mut temp_file = NamedTempFile::new()?;
    write!(temp_file, "{}", malformed_csv)?;
    temp_file.flush()?;

    let parser = RobustCsvParser::default();
    let result = parser.parse_csv(temp_file.path());

    // Should either succeed with flexible parsing or fail gracefully
    match result {
        Ok((headers, records)) => {
            assert_eq!(headers.len(), 3);
            assert!(!records.is_empty());
        }
        Err(_) => {
            // Acceptable to error on malformed data
        }
    }
    Ok(())
}

#[test]
fn test_empty_csv_file() -> Result<()> {
    let empty_csv = "";
    let mut temp_file = NamedTempFile::new()?;
    write!(temp_file, "{}", empty_csv)?;
    temp_file.flush()?;

    let parser = RobustCsvParser::default();
    let result = parser.parse_csv(temp_file.path());

    // Should handle empty files gracefully
    match result {
        Ok((headers, records)) => {
            assert!(headers.is_empty() || records.is_empty());
        }
        Err(_) => {
            // Also acceptable to error on empty files
        }
    }
    Ok(())
}

#[test]
fn test_csv_with_unicode_content() -> Result<()> {
    let unicode_csv = "name,city,country\nJoão,São Paulo,Brasil\n田中,東京,日本\n";
    let mut temp_file = NamedTempFile::new()?;
    write!(temp_file, "{}", unicode_csv)?;
    temp_file.flush()?;

    let parser = RobustCsvParser::default();
    let result = parser.parse_csv(temp_file.path());

    assert!(result.is_ok(), "Should handle Unicode content");
    if let Ok((headers, records)) = result {
        assert_eq!(headers.len(), 3);
        assert_eq!(records.len(), 2);
        assert!(records[0][0].contains("João"));
        assert!(records[1][0].contains("田中"));
    }
    Ok(())
}

#[test]
fn test_csv_with_different_delimiters() -> Result<()> {
    let semicolon_csv = "col1;col2;col3\nvalue1;value2;value3\n";
    let mut temp_file = NamedTempFile::new()?;
    write!(temp_file, "{}", semicolon_csv)?;
    temp_file.flush()?;

    let mut parser = RobustCsvParser::default();
    parser.delimiter = Some(b';');

    let result = parser.parse_csv(temp_file.path());
    assert!(result.is_ok());

    if let Ok((headers, records)) = result {
        assert_eq!(headers.len(), 3);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].len(), 3);
    }
    Ok(())
}

#[test]
fn test_csv_with_quoted_fields() -> Result<()> {
    let quoted_csv = "name,description\n\"John Doe\",\"A person with a \"\"quoted\"\" name\"\n";
    let mut temp_file = NamedTempFile::new()?;
    write!(temp_file, "{}", quoted_csv)?;
    temp_file.flush()?;

    let parser = RobustCsvParser::default();
    let result = parser.parse_csv(temp_file.path());

    assert!(result.is_ok(), "Should handle quoted fields");
    if let Ok((headers, records)) = result {
        assert_eq!(headers.len(), 2);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0][0], "John Doe");
    }
    Ok(())
}

#[test]
fn test_error_categorization() {
    let csv_error = DataProfilerError::csv_parsing("field count mismatch", "test.csv");
    assert_eq!(csv_error.category(), "csv_parsing");
    assert_eq!(csv_error.severity(), ErrorSeverity::High);
    assert!(!csv_error.is_recoverable());
}

#[test]
fn test_error_suggestions() {
    let error = DataProfilerError::csv_parsing("field count mismatch", "test.csv");
    let error_msg = error.to_string();
    assert!(error_msg.contains("💡"));
    assert!(error_msg.contains("field count mismatch"));
}

#[test]
fn test_file_not_found_error() {
    let error = DataProfilerError::file_not_found("nonexistent.csv");
    assert_eq!(error.category(), "file_not_found");
    assert_eq!(error.severity(), ErrorSeverity::Critical);
    assert!(!error.is_recoverable());
}

#[test]
fn test_memory_limit_error() {
    let error = DataProfilerError::MemoryLimitExceeded;
    assert_eq!(error.category(), "memory_limit");
    assert_eq!(error.severity(), ErrorSeverity::Critical);
    assert!(!error.is_recoverable());

    let error_msg = error.to_string();
    assert!(error_msg.contains("💡"));
    assert!(error_msg.contains("streaming"));
}

#[test]
fn test_data_quality_error() {
    let error = DataProfilerError::data_quality_issue(
        "High null percentage",
        "90% of values are missing",
        "Consider data collection improvements",
    );

    assert_eq!(error.category(), "data_quality");
    assert_eq!(error.severity(), ErrorSeverity::Info);
    assert!(error.is_recoverable());

    let error_msg = error.to_string();
    assert!(error_msg.contains("📊"));
    assert!(error_msg.contains("💡"));
}

#[test]
fn test_column_analysis_error() {
    let error = DataProfilerError::column_analysis_error(
        "age",
        "Invalid numeric format",
        "Check for non-numeric values",
    );

    assert_eq!(error.category(), "column_analysis");
    assert_eq!(error.severity(), ErrorSeverity::Medium);

    let error_msg = error.to_string();
    assert!(error_msg.contains("age"));
    assert!(error_msg.contains("💡"));
}

#[test]
fn test_csv_with_mixed_line_endings() -> Result<()> {
    // Mix of \n and \r\n
    let mixed_csv = "col1,col2,col3\nvalue1,value2,value3\r\nvalue4,value5,value6\n";
    let mut temp_file = NamedTempFile::new()?;
    write!(temp_file, "{}", mixed_csv)?;
    temp_file.flush()?;

    let parser = RobustCsvParser::default();
    let result = parser.parse_csv(temp_file.path());

    // Should handle mixed line endings
    assert!(result.is_ok());
    if let Ok((headers, records)) = result {
        assert_eq!(headers.len(), 3);
        assert_eq!(records.len(), 2);
    }
    Ok(())
}

#[test]
fn test_csv_with_only_headers() -> Result<()> {
    let headers_only = "col1,col2,col3\n";
    let mut temp_file = NamedTempFile::new()?;
    write!(temp_file, "{}", headers_only)?;
    temp_file.flush()?;

    let parser = RobustCsvParser::default();
    let result = parser.parse_csv(temp_file.path());

    assert!(result.is_ok());
    if let Ok((headers, records)) = result {
        assert_eq!(headers.len(), 3);
        assert!(records.is_empty());
    }
    Ok(())
}

#[test]
fn test_very_long_fields() -> Result<()> {
    let long_field = "x".repeat(10000);
    let csv_data = format!("col1,col2\n{},short\n", long_field);
    let mut temp_file = NamedTempFile::new()?;
    write!(temp_file, "{}", csv_data)?;
    temp_file.flush()?;

    let parser = RobustCsvParser::default();
    let result = parser.parse_csv(temp_file.path());

    assert!(result.is_ok(), "Should handle very long fields");
    if let Ok((headers, records)) = result {
        assert_eq!(headers.len(), 2);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0][0].len(), 10000);
    }
    Ok(())
}
