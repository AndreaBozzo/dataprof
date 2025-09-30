use anyhow::Result;
use csv::ReaderBuilder;
use std::collections::HashMap;
use std::path::Path;

use crate::analysis::{analyze_column, analyze_column_fast};
use crate::core::robust_csv::RobustCsvParser;
use crate::types::{ColumnProfile, FileInfo, QualityReport, ScanInfo};
use crate::utils::quality::QualityChecker;
use crate::utils::sampler::Sampler;

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
            issues: vec![],
            scan_info: ScanInfo {
                rows_scanned: 0,
                sampling_ratio: 1.0,
                scan_time_ms: start.elapsed().as_millis(),
            },
            data_quality_metrics: None,
        });
    }

    // Convert records to column format for analysis
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();

    // Initialize columns
    for header in &headers {
        columns.insert(header.to_string(), Vec::new());
    }

    // Add data from records
    for record in &records {
        for (i, header) in headers.iter().enumerate() {
            let value = record.get(i).map_or("", |v| v);
            if let Some(column_data) = columns.get_mut(header) {
                column_data.push(value.to_string());
            }
        }
    }

    // Analyze columns
    let mut column_profiles = Vec::new();
    for (name, data) in &columns {
        let profile = analyze_column(name, data);
        column_profiles.push(profile);
    }

    // Enhanced quality analysis: get both issues and comprehensive metrics
    let (issues, data_quality_metrics) =
        QualityChecker::enhanced_quality_analysis(&column_profiles, &columns)
            .map_err(|e| anyhow::anyhow!("Enhanced quality analysis failed: {}", e))?;
    let scan_time_ms = start.elapsed().as_millis();

    Ok(QualityReport {
        file_info: FileInfo {
            path: file_path.display().to_string(),
            total_rows: Some(records.len()),
            total_columns: headers.len(),
            file_size_mb,
        },
        column_profiles,
        issues,
        scan_info: ScanInfo {
            rows_scanned: records.len(),
            sampling_ratio: 1.0,
            scan_time_ms,
        },
        data_quality_metrics: Some(data_quality_metrics),
    })
}

// Enhanced function that uses robust parsing with sampling for large files
pub fn analyze_csv_with_sampling(file_path: &Path) -> Result<QualityReport> {
    let metadata = std::fs::metadata(file_path)?;
    let file_size_mb = metadata.len() as f64 / 1_048_576.0;

    let sampler = Sampler::new(file_size_mb);
    let start = std::time::Instant::now();

    let (records, sample_info) = sampler.sample_csv(file_path)?;

    // Converti i records in formato compatibile
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();

    if !records.is_empty() {
        // Usa gli header dal primo record (assumendo che ci siano)
        let mut reader = ReaderBuilder::new()
            .has_headers(true)
            .from_path(file_path)?;
        let headers = reader.headers()?;

        // Inizializza colonne usando iteratore direttamente senza clone
        let header_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
        for header_name in header_names.iter() {
            columns.insert(header_name.to_string(), Vec::new());
        }

        // Aggiungi i dati campionati
        for record in records {
            for (i, field) in record.iter().enumerate() {
                if let Some(header_name) = header_names.get(i) {
                    if let Some(column_data) = columns.get_mut(header_name) {
                        column_data.push(field.to_string());
                    }
                }
            }
        }
    }

    // Analizza le colonne
    let mut column_profiles = Vec::new();
    for (name, data) in &columns {
        let profile = analyze_column(name, data);
        column_profiles.push(profile);
    }

    // Enhanced quality analysis: get both issues and comprehensive metrics
    let (issues, data_quality_metrics) =
        QualityChecker::enhanced_quality_analysis(&column_profiles, &columns)
            .map_err(|e| anyhow::anyhow!("Enhanced quality analysis failed: {}", e))?;

    let scan_time_ms = start.elapsed().as_millis();

    Ok(QualityReport {
        file_info: FileInfo {
            path: file_path.display().to_string(),
            total_rows: sample_info.total_rows,
            total_columns: column_profiles.len(),
            file_size_mb,
        },
        column_profiles,
        issues,
        scan_info: ScanInfo {
            rows_scanned: sample_info.sampled_rows,
            sampling_ratio: sample_info.sampling_ratio,
            scan_time_ms,
        },
        data_quality_metrics: Some(data_quality_metrics),
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

    // Convert records to column format
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();

    // Initialize columns
    for header in &headers {
        columns.insert(header.to_string(), Vec::new());
    }

    // Add data from records
    for record in &records {
        for (i, header) in headers.iter().enumerate() {
            let value = record.get(i).map_or("", |v| v);
            if let Some(column_data) = columns.get_mut(header) {
                column_data.push(value.to_string());
            }
        }
    }

    // Analyze each column
    let mut profiles = Vec::new();
    for (name, data) in columns {
        let profile = analyze_column(&name, &data);
        profiles.push(profile);
    }

    Ok(profiles)
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

            // Convert records to column format
            let mut columns: HashMap<String, Vec<String>> = HashMap::new();

            // Initialize columns
            for header in &headers {
                columns.insert(header.to_string(), Vec::new());
            }

            // Add data from records
            for record in &records {
                for (i, header) in headers.iter().enumerate() {
                    let value = record.get(i).map_or("", |v| v);
                    if let Some(column_data) = columns.get_mut(header) {
                        column_data.push(value.to_string());
                    }
                }
            }

            // Analyze each column in fast mode
            let mut profiles = Vec::new();
            for (name, data) in columns {
                let profile = analyze_column_fast(&name, &data);
                profiles.push(profile);
            }

            Ok(profiles)
        }
    }
}

// Helper function for strict CSV parsing
pub fn try_strict_csv_parsing(file_path: &Path) -> Result<Vec<ColumnProfile>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(false) // Strict parsing
        .from_path(file_path)?;

    // Get headers without clone
    let headers = reader.headers()?;
    let header_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();

    // Initialize columns
    for header_name in header_names.iter() {
        columns.insert(header_name.to_string(), Vec::new());
    }

    // Read records
    for result in reader.records() {
        let record = result?;
        for (i, field) in record.iter().enumerate() {
            if let Some(header_name) = header_names.get(i) {
                if let Some(column_data) = columns.get_mut(header_name) {
                    column_data.push(field.to_string());
                }
            }
        }
    }

    // Analyze each column
    let mut profiles = Vec::new();
    for (name, data) in columns {
        let profile = analyze_column(&name, &data);
        profiles.push(profile);
    }

    Ok(profiles)
}

// Fast version of strict CSV parsing for benchmarks
pub fn try_strict_csv_parsing_fast(file_path: &Path) -> Result<Vec<ColumnProfile>> {
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .flexible(false) // Strict parsing
        .from_path(file_path)?;

    // Get headers without clone
    let headers = reader.headers()?;
    let header_names: Vec<String> = headers.iter().map(|h| h.to_string()).collect();
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();

    // Initialize columns
    for header_name in header_names.iter() {
        columns.insert(header_name.to_string(), Vec::new());
    }

    // Read records
    for result in reader.records() {
        let record = result?;
        for (i, field) in record.iter().enumerate() {
            if let Some(header_name) = header_names.get(i) {
                if let Some(column_data) = columns.get_mut(header_name) {
                    column_data.push(field.to_string());
                }
            }
        }
    }

    // Analyze each column in fast mode
    let mut profiles = Vec::new();
    for (name, data) in columns {
        let profile = analyze_column_fast(&name, &data);
        profiles.push(profile);
    }

    Ok(profiles)
}
