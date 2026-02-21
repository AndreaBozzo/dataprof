use anyhow::Result;
use csv::ReaderBuilder;
use std::collections::HashMap;
use std::path::Path;

use crate::analysis::{analyze_column, analyze_column_fast};
use crate::core::sampling::SamplingStrategy;
use crate::parsers::robust_csv::RobustCsvParser;
use crate::types::{
    ColumnProfile, DataQualityMetrics, DataSource, FileFormat, QualityReport, ScanInfo,
};

// Default verbosity level for CSV analysis (1 = normal, suppresses fallback messages)
const DEFAULT_VERBOSITY: u8 = 1;

// ============================================================================
// HELPER FUNCTIONS - Reusable components to eliminate duplication
// ============================================================================

/// Initialize empty column vectors from headers
#[inline]
fn initialize_columns(headers: &[String]) -> HashMap<String, Vec<String>> {
    let mut columns = HashMap::new();
    for header in headers {
        columns.insert(header.to_string(), Vec::new());
    }
    columns
}

/// Process records into column-oriented format
#[inline]
fn process_records_to_columns(
    records: &[Vec<String>],
    headers: &[String],
    columns: &mut HashMap<String, Vec<String>>,
) {
    for record in records {
        for (i, header) in headers.iter().enumerate() {
            let value = record.get(i).map_or("", |v| v.as_str());
            if let Some(column_data) = columns.get_mut(header) {
                column_data.push(value.to_string());
            }
        }
    }
}

/// Analyze columns and return profiles
#[inline]
fn analyze_columns(columns: &HashMap<String, Vec<String>>) -> Vec<ColumnProfile> {
    let mut profiles = Vec::new();
    for (name, data) in columns {
        let profile = analyze_column(name, data);
        profiles.push(profile);
    }
    profiles
}

/// Analyze columns in fast mode (skips pattern detection)
#[inline]
fn analyze_columns_fast(columns: HashMap<String, Vec<String>>) -> Vec<ColumnProfile> {
    let mut profiles = Vec::new();
    for (name, data) in columns {
        let profile = analyze_column_fast(&name, &data);
        profiles.push(profile);
    }
    profiles
}

/// Process CSV records from a reader into columns (for streaming/sampling scenarios)
#[inline]
fn process_csv_record(
    record: &csv::StringRecord,
    header_names: &[String],
    columns: &mut HashMap<String, Vec<String>>,
) {
    for (i, field) in record.iter().enumerate() {
        if let Some(header_name) = header_names.get(i)
            && let Some(column_data) = columns.get_mut(header_name)
        {
            column_data.push(field.to_string());
        }
    }
}

// v0.3.0 Robust CSV analysis function - handles edge cases and malformed data
pub fn analyze_csv_robust(file_path: &Path) -> Result<QualityReport> {
    let metadata = std::fs::metadata(file_path)?;
    let _file_size_mb = metadata.len() as f64 / 1_048_576.0;
    let start = std::time::Instant::now();

    // Use robust CSV parser
    let parser = RobustCsvParser::new()
        .flexible(true)
        .allow_variable_columns(true);

    let (headers, records) = parser.parse_csv(file_path)?;

    if records.is_empty() {
        return Ok(QualityReport::new(
            DataSource::File {
                path: file_path.display().to_string(),
                format: FileFormat::Csv,
                size_bytes: metadata.len(),
                modified_at: None,
                parquet_metadata: None,
            },
            vec![],
            ScanInfo::new(0, headers.len(), 0, 1.0, start.elapsed().as_millis()),
            DataQualityMetrics::empty(),
        ));
    }

    // Convert records to column format using helper functions
    let mut columns = initialize_columns(&headers);
    process_records_to_columns(&records, &headers, &mut columns);

    // Analyze columns using helper function
    let column_profiles = analyze_columns(&columns);

    // Calculate comprehensive quality metrics using ISO 8000/25012 standards
    let data_quality_metrics = DataQualityMetrics::calculate_from_data(&columns, &column_profiles)
        .map_err(|e| anyhow::anyhow!("Quality metrics calculation failed: {}", e))?;
    let scan_time_ms = start.elapsed().as_millis();
    let num_rows = records.len();
    let num_columns = column_profiles.len();

    Ok(QualityReport::new(
        DataSource::File {
            path: file_path.display().to_string(),
            format: FileFormat::Csv,
            size_bytes: metadata.len(),
            modified_at: None,
            parquet_metadata: None,
        },
        column_profiles,
        ScanInfo::new(num_rows, num_columns, num_rows, 1.0, scan_time_ms),
        data_quality_metrics,
    ))
}

/// Enhanced function that uses robust parsing with adaptive sampling for large files
///
/// This function now uses the modern sampling system from `core::sampling`
/// instead of the legacy `utils::sampler` module.
pub fn analyze_csv_with_sampling(file_path: &Path) -> Result<QualityReport> {
    let metadata = std::fs::metadata(file_path)?;
    let file_size_mb = metadata.len() as f64 / 1_048_576.0;
    let start = std::time::Instant::now();

    // Use modern adaptive sampling strategy
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .from_path(file_path)?;
    let headers = reader.headers()?;
    let header_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();

    // Estimate total rows for adaptive sampling
    let file_size_bytes = metadata.len();
    let estimated_rows = if file_size_bytes > 1_000_000 {
        // Quick estimation: assume ~100 bytes per row
        Some((file_size_bytes / 100) as usize)
    } else {
        None
    };

    // Determine sampling strategy
    let sampling_strategy = SamplingStrategy::adaptive(estimated_rows, file_size_mb);

    // Calculate how many rows to sample based on strategy
    let (sample_size, should_sample) = match sampling_strategy {
        SamplingStrategy::None => (usize::MAX, false),
        SamplingStrategy::Random { size } => (size, true),
        SamplingStrategy::Progressive { initial_size, .. } => (initial_size, true),
        _ => (100_000, true), // Default fallback
    };

    // Initialize columns using helper function
    let mut columns = initialize_columns(&header_names);

    // Read and sample data
    let mut rows_read = 0;
    let mut total_rows = 0;

    for result in reader.records() {
        total_rows += 1;

        // Apply sampling if needed
        if should_sample && rows_read >= sample_size {
            break;
        }

        if let Ok(record) = result {
            rows_read += 1;
            process_csv_record(&record, &header_names, &mut columns);
        }
    }

    // Analyze columns using helper function
    let column_profiles = analyze_columns(&columns);

    // Calculate comprehensive quality metrics using ISO 8000/25012 standards
    let data_quality_metrics = DataQualityMetrics::calculate_from_data(&columns, &column_profiles)
        .map_err(|e| anyhow::anyhow!("Quality metrics calculation failed: {}", e))?;

    let scan_time_ms = start.elapsed().as_millis();
    let sampling_ratio = if total_rows > 0 {
        rows_read as f64 / total_rows as f64
    } else {
        1.0
    };
    let num_columns = column_profiles.len();

    Ok(QualityReport::new(
        DataSource::File {
            path: file_path.display().to_string(),
            format: FileFormat::Csv,
            size_bytes: metadata.len(),
            modified_at: None,
            parquet_metadata: None,
        },
        column_profiles,
        ScanInfo::new(
            total_rows,
            num_columns,
            rows_read,
            sampling_ratio,
            scan_time_ms,
        ),
        data_quality_metrics,
    ))
}

// Enhanced original function with robust parsing fallback for compatibility
pub fn analyze_csv(file_path: &Path) -> Result<Vec<ColumnProfile>> {
    analyze_csv_with_verbosity(file_path, DEFAULT_VERBOSITY)
}

/// Analyze CSV with verbosity control for fallback messages
///
/// Verbosity levels:
/// - 0: quiet (no fallback messages)
/// - 1: normal (no fallback messages)
/// - 2+: verbose/debug (show fallback info)
pub fn analyze_csv_with_verbosity(file_path: &Path, verbosity: u8) -> Result<Vec<ColumnProfile>> {
    // First try strict CSV parsing
    match try_strict_csv_parsing(file_path) {
        Ok(profiles) => return Ok(profiles),
        Err(e) => {
            // Only show fallback message at verbose level
            if verbosity >= 2 {
                log::info!("Using flexible CSV parsing (strict mode failed: {})", e);
            }
        }
    }

    // Fallback to robust parsing
    let parser = RobustCsvParser::new()
        .flexible(true)
        .allow_variable_columns(true)
        .verbosity(verbosity);

    let (headers, records) = parser.parse_csv(file_path)?;

    // Convert records to column format using helper functions
    let mut columns = initialize_columns(&headers);
    process_records_to_columns(&records, &headers, &mut columns);

    // Analyze columns using helper function
    Ok(analyze_columns(&columns))
}

// Fast version optimized for benchmarks - skips pattern detection and unique counts
pub fn analyze_csv_fast(file_path: &Path) -> Result<Vec<ColumnProfile>> {
    // Use only strict parsing for speed
    match try_strict_csv_parsing_fast(file_path) {
        Ok(profiles) => Ok(profiles),
        Err(_) => {
            // If strict fails, fallback to robust but still fast
            // Suppress fallback messages in fast mode (verbosity=0)
            let parser = RobustCsvParser::new()
                .flexible(true)
                .allow_variable_columns(true)
                .verbosity(0);

            let (headers, records) = parser.parse_csv(file_path)?;

            // Convert records to column format using helper functions
            let mut columns = initialize_columns(&headers);
            process_records_to_columns(&records, &headers, &mut columns);

            // Analyze in fast mode using helper function
            Ok(analyze_columns_fast(columns))
        }
    }
}

// Helper function for strict CSV parsing
pub fn try_strict_csv_parsing(file_path: &Path) -> Result<Vec<ColumnProfile>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(false) // Strict parsing
        .from_path(file_path)?;

    let headers = reader.headers()?;
    let header_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
    let mut columns = initialize_columns(&header_names);

    // Read records using helper function
    for result in reader.records() {
        let record = result?;
        process_csv_record(&record, &header_names, &mut columns);
    }

    // Analyze using helper function
    Ok(analyze_columns(&columns))
}

// Fast version of strict CSV parsing for benchmarks
pub fn try_strict_csv_parsing_fast(file_path: &Path) -> Result<Vec<ColumnProfile>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(false) // Strict parsing
        .from_path(file_path)?;

    let headers = reader.headers()?;
    let header_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
    let mut columns = initialize_columns(&header_names);

    // Read records using helper function
    for result in reader.records() {
        let record = result?;
        process_csv_record(&record, &header_names, &mut columns);
    }

    // Analyze in fast mode using helper function
    Ok(analyze_columns_fast(columns))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_csv(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "{}", content).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_analyze_csv_basic() {
        let csv = write_csv("name,age\nAlice,25\nBob,30\n");
        let profiles = analyze_csv(csv.path()).unwrap();
        assert_eq!(profiles.len(), 2);

        let names: Vec<&str> = profiles.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"name"));
        assert!(names.contains(&"age"));

        let age = profiles.iter().find(|p| p.name == "age").unwrap();
        assert_eq!(age.total_count, 2);
        assert_eq!(age.null_count, 0);
    }

    #[test]
    fn test_analyze_csv_with_nulls() {
        let csv = write_csv("name,age,email\nAlice,25,a@b.com\nBob,,\nCharlie,30,c@d.com\n");
        let profiles = analyze_csv(csv.path()).unwrap();

        let age = profiles.iter().find(|p| p.name == "age").unwrap();
        assert_eq!(age.total_count, 3);
        assert_eq!(age.null_count, 1);

        let email = profiles.iter().find(|p| p.name == "email").unwrap();
        assert_eq!(email.null_count, 1);
    }

    #[test]
    fn test_analyze_csv_robust_returns_quality_report() {
        let csv = write_csv("x,y\n1,a\n2,b\n3,c\n");
        let report = analyze_csv_robust(csv.path()).unwrap();

        assert_eq!(report.column_profiles.len(), 2);
        assert_eq!(report.scan_info.total_rows, 3);
        assert_eq!(report.scan_info.total_columns, 2);
        // Quality score should be computable
        assert!(report.quality_score() >= 0.0);
        assert!(report.quality_score() <= 100.0);
    }

    #[test]
    fn test_analyze_csv_robust_empty_file() {
        let csv = write_csv("name,age\n");
        let report = analyze_csv_robust(csv.path()).unwrap();

        assert_eq!(report.column_profiles.len(), 0);
        assert_eq!(report.scan_info.total_rows, 0);
    }

    #[test]
    fn test_analyze_csv_fast_produces_profiles() {
        let csv = write_csv("a,b,c\n1,x,true\n2,y,false\n3,z,true\n");
        let profiles = analyze_csv_fast(csv.path()).unwrap();
        assert_eq!(profiles.len(), 3);

        for p in &profiles {
            assert_eq!(p.total_count, 3);
        }
    }

    #[test]
    fn test_analyze_csv_with_sampling_small_file() {
        // Small files should not be sampled — all rows processed
        let csv = write_csv("val\n1\n2\n3\n4\n5\n");
        let report = analyze_csv_with_sampling(csv.path()).unwrap();

        assert_eq!(report.scan_info.total_rows, 5);
        assert_eq!(report.scan_info.rows_scanned, 5);
        assert!((report.scan_info.sampling_ratio - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_analyze_csv_numeric_types_detected() {
        let csv = write_csv("int_col,float_col,str_col\n1,1.5,hello\n2,2.5,world\n3,3.5,foo\n");
        let profiles = analyze_csv(csv.path()).unwrap();

        let int_col = profiles.iter().find(|p| p.name == "int_col").unwrap();
        assert!(
            matches!(int_col.data_type, DataType::Integer),
            "Expected Integer, got {:?}",
            int_col.data_type
        );

        let float_col = profiles.iter().find(|p| p.name == "float_col").unwrap();
        assert!(
            matches!(float_col.data_type, DataType::Float),
            "Expected Float, got {:?}",
            float_col.data_type
        );

        let str_col = profiles.iter().find(|p| p.name == "str_col").unwrap();
        assert!(
            matches!(str_col.data_type, DataType::String),
            "Expected String, got {:?}",
            str_col.data_type
        );
    }

    #[test]
    fn test_analyze_csv_strict_rejects_ragged_rows() {
        // Strict parsing should fail on rows with inconsistent column counts
        let csv = write_csv("a,b\n1,2\n3,4,5\n");
        assert!(try_strict_csv_parsing(csv.path()).is_err());
    }

    #[test]
    fn test_analyze_csv_robust_handles_ragged_rows() {
        // Robust parsing should handle rows with inconsistent column counts
        let csv = write_csv("a,b\n1,2\n3,4,5\n6\n");
        let report = analyze_csv_robust(csv.path()).unwrap();
        assert!(!report.column_profiles.is_empty());
    }

    #[test]
    fn test_analyze_csv_with_quality_metrics() {
        let csv = write_csv("name,age\nAlice,25\nBob,\nCharlie,30\n");
        let report = analyze_csv_robust(csv.path()).unwrap();

        // Missing values should affect completeness
        assert!(report.data_quality_metrics.missing_values_ratio > 0.0);
        assert!(report.data_quality_metrics.complete_records_ratio < 100.0);
    }
}
