use anyhow::Result;
use csv::ReaderBuilder;
use std::collections::HashMap;
use std::path::Path;

use crate::analysis::{analyze_column, analyze_column_fast};
use crate::core::sampling::SamplingStrategy;
use crate::parsers::robust_csv::RobustCsvParser;
use crate::types::{ColumnProfile, DataQualityMetrics, FileInfo, QualityReport, ScanInfo};

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
        if let Some(header_name) = header_names.get(i) {
            if let Some(column_data) = columns.get_mut(header_name) {
                column_data.push(field.to_string());
            }
        }
    }
}

// v0.3.0 Robust CSV analysis function - handles edge cases and malformed data
pub fn analyze_csv_robust(file_path: &Path) -> Result<QualityReport> {
    let metadata = std::fs::metadata(file_path)?;
    let file_size_mb = metadata.len() as f64 / 1_048_576.0;
    let start = std::time::Instant::now();

    // Use robust CSV parser
    let parser = RobustCsvParser::new()
        .flexible(true)
        .allow_variable_columns(true);

    let (headers, records) = parser.parse_csv(file_path)?;

    if records.is_empty() {
        return Ok(QualityReport {
            file_info: FileInfo {
                path: file_path.display().to_string(),
                total_rows: Some(0),
                total_columns: headers.len(),
                file_size_mb,
            },
            column_profiles: vec![],
            scan_info: ScanInfo {
                rows_scanned: 0,
                sampling_ratio: 1.0,
                scan_time_ms: start.elapsed().as_millis(),
            },
            data_quality_metrics: DataQualityMetrics::empty(),
        });
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

    Ok(QualityReport {
        file_info: FileInfo {
            path: file_path.display().to_string(),
            total_rows: Some(records.len()),
            total_columns: headers.len(),
            file_size_mb,
        },
        column_profiles,
        scan_info: ScanInfo {
            rows_scanned: records.len(),
            sampling_ratio: 1.0,
            scan_time_ms,
        },
        data_quality_metrics,
    })
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

    Ok(QualityReport {
        file_info: FileInfo {
            path: file_path.display().to_string(),
            total_rows: Some(total_rows),
            total_columns: column_profiles.len(),
            file_size_mb,
        },
        column_profiles,
        scan_info: ScanInfo {
            rows_scanned: rows_read,
            sampling_ratio,
            scan_time_ms,
        },
        data_quality_metrics,
    })
}

// Enhanced original function with robust parsing fallback for compatibility
pub fn analyze_csv(file_path: &Path) -> Result<Vec<ColumnProfile>> {
    // First try strict CSV parsing
    match try_strict_csv_parsing(file_path) {
        Ok(profiles) => return Ok(profiles),
        Err(e) => {
            eprintln!(
                "⚠️ Strict CSV parsing failed: {}. Using robust parsing...",
                e
            );
        }
    }

    // Fallback to robust parsing
    let parser = RobustCsvParser::new()
        .flexible(true)
        .allow_variable_columns(true);

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
            let parser = RobustCsvParser::new()
                .flexible(true)
                .allow_variable_columns(true);

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
