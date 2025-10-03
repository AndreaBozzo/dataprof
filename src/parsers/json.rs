use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

use crate::analysis::analyze_column;
use crate::types::{ColumnProfile, DataQualityMetrics, FileInfo, QualityReport, ScanInfo};

// Simple JSON/JSONL support
pub fn analyze_json(file_path: &Path) -> Result<Vec<ColumnProfile>> {
    let content = std::fs::read_to_string(file_path)?;

    // Try to detect format: JSON array vs JSONL
    let records: Vec<Value> = if content.trim_start().starts_with('[') {
        // JSON array
        serde_json::from_str(&content)?
    } else {
        // JSONL - one JSON object per line
        content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()?
    };

    if records.is_empty() {
        return Ok(vec![]);
    }

    // Convert JSON objects to flat string columns
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();

    for record in &records {
        if let Value::Object(obj) = record {
            for (key, value) in obj {
                let column_data = columns.entry(key.to_string()).or_default();

                // Convert JSON value to string representation
                let string_value = match value {
                    Value::Null => String::new(), // Treat as empty/null
                    Value::Bool(b) => b.to_string(),
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.to_string(),
                    Value::Array(_) | Value::Object(_) => {
                        // For complex types, serialize to JSON string
                        serde_json::to_string(value).unwrap_or_default()
                    }
                };

                column_data.push(string_value);
            }
        }
    }

    // Ensure all columns have the same length (fill missing with empty strings)
    let max_len = records.len();
    for values in columns.values_mut() {
        values.resize(max_len, String::new());
    }

    // Analyze columns using existing logic
    let mut profiles = Vec::new();
    for (name, data) in columns {
        let profile = analyze_column(&name, &data);
        profiles.push(profile);
    }

    Ok(profiles)
}

// JSON analysis with quality checking
pub fn analyze_json_with_quality(file_path: &Path) -> Result<QualityReport> {
    let metadata = std::fs::metadata(file_path)?;
    let file_size_mb = metadata.len() as f64 / 1_048_576.0;

    let start = std::time::Instant::now();

    // Use existing JSON parsing logic
    let content = std::fs::read_to_string(file_path)?;

    let records: Vec<Value> = if content.trim_start().starts_with('[') {
        serde_json::from_str(&content)?
    } else {
        content
            .lines()
            .filter(|line| !line.trim().is_empty())
            .map(serde_json::from_str)
            .collect::<Result<Vec<_>, _>>()?
    };

    if records.is_empty() {
        return Ok(QualityReport {
            file_info: FileInfo {
                path: file_path.display().to_string(),
                total_rows: Some(0),
                total_columns: 0,
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

    // Convert to columns
    let mut columns: HashMap<String, Vec<String>> = HashMap::new();

    for record in &records {
        if let Value::Object(obj) = record {
            for (key, value) in obj {
                let column_data = columns.entry(key.to_string()).or_default();

                let string_value = match value {
                    Value::Null => String::new(),
                    Value::Bool(b) => b.to_string(),
                    Value::Number(n) => n.to_string(),
                    Value::String(s) => s.to_string(),
                    Value::Array(_) | Value::Object(_) => {
                        serde_json::to_string(value).unwrap_or_default()
                    }
                };

                column_data.push(string_value);
            }
        }
    }

    let max_len = records.len();
    for values in columns.values_mut() {
        values.resize(max_len, String::new());
    }

    // Analyze columns
    let mut column_profiles = Vec::new();
    for (name, data) in &columns {
        let profile = analyze_column(name, data);
        column_profiles.push(profile);
    }

    // Calculate comprehensive ISO 8000/25012 quality metrics
    let data_quality_metrics = DataQualityMetrics::calculate_from_data(&columns, &column_profiles)
        .map_err(|e| anyhow::anyhow!("Quality metrics calculation failed: {}", e))?;

    let scan_time_ms = start.elapsed().as_millis();

    Ok(QualityReport {
        file_info: FileInfo {
            path: file_path.display().to_string(),
            total_rows: Some(records.len()),
            total_columns: column_profiles.len(),
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
