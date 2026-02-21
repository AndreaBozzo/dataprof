use anyhow::Result;
use serde_json::Value;
use std::collections::HashMap;
use std::path::Path;

use crate::analysis::analyze_column;
use crate::types::{
    ColumnProfile, DataQualityMetrics, DataSource, FileFormat, QualityReport, ScanInfo,
};

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
    let _file_size_mb = metadata.len() as f64 / 1_048_576.0;

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

    // Detect format
    let format = if content.trim_start().starts_with('[') {
        FileFormat::Json
    } else {
        FileFormat::Jsonl
    };

    if records.is_empty() {
        return Ok(QualityReport::new(
            DataSource::File {
                path: file_path.display().to_string(),
                format,
                size_bytes: metadata.len(),
                modified_at: None,
                parquet_metadata: None,
            },
            vec![],
            ScanInfo::new(0, 0, 0, 1.0, start.elapsed().as_millis()),
            DataQualityMetrics::empty(),
        ));
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
    let num_rows = records.len();
    let num_columns = column_profiles.len();

    Ok(QualityReport::new(
        DataSource::File {
            path: file_path.display().to_string(),
            format,
            size_bytes: metadata.len(),
            modified_at: None,
            parquet_metadata: None,
        },
        column_profiles,
        ScanInfo::new(num_rows, num_columns, num_rows, 1.0, scan_time_ms),
        data_quality_metrics,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_file(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "{}", content).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn test_analyze_json_array() {
        let json = write_file(r#"[{"name":"Alice","age":25},{"name":"Bob","age":30}]"#);
        let profiles = analyze_json(json.path()).unwrap();

        assert_eq!(profiles.len(), 2);
        let names: Vec<&str> = profiles.iter().map(|p| p.name.as_str()).collect();
        assert!(names.contains(&"name"));
        assert!(names.contains(&"age"));

        let age = profiles.iter().find(|p| p.name == "age").unwrap();
        assert_eq!(age.total_count, 2);
        assert_eq!(age.null_count, 0);
    }

    #[test]
    fn test_analyze_jsonl() {
        let jsonl = write_file("{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n");
        let profiles = analyze_json(jsonl.path()).unwrap();

        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].name, "x");
        assert_eq!(profiles[0].total_count, 3);
    }

    #[test]
    fn test_analyze_json_with_nulls() {
        let json = write_file(
            r#"[{"a":"hello","b":1},{"a":null,"b":2},{"a":"world","b":null}]"#,
        );
        let profiles = analyze_json(json.path()).unwrap();

        let col_a = profiles.iter().find(|p| p.name == "a").unwrap();
        assert_eq!(col_a.null_count, 1);

        let col_b = profiles.iter().find(|p| p.name == "b").unwrap();
        assert_eq!(col_b.null_count, 1);
    }

    #[test]
    fn test_analyze_json_with_missing_fields() {
        // Second record is missing "b" — should be padded with empty string
        let json = write_file(r#"[{"a":1,"b":2},{"a":3}]"#);
        let profiles = analyze_json(json.path()).unwrap();

        let col_b = profiles.iter().find(|p| p.name == "b").unwrap();
        assert_eq!(col_b.total_count, 2); // padded to match record count
    }

    #[test]
    fn test_analyze_json_empty_array() {
        let json = write_file("[]");
        let profiles = analyze_json(json.path()).unwrap();
        assert!(profiles.is_empty());
    }

    #[test]
    fn test_analyze_json_with_quality_detects_format() {
        let json_array = write_file(r#"[{"x":1}]"#);
        let report = analyze_json_with_quality(json_array.path()).unwrap();
        assert!(matches!(
            report.data_source,
            DataSource::File { format: FileFormat::Json, .. }
        ));

        let jsonl = write_file("{\"x\":1}\n{\"x\":2}\n");
        let report = analyze_json_with_quality(jsonl.path()).unwrap();
        assert!(matches!(
            report.data_source,
            DataSource::File { format: FileFormat::Jsonl, .. }
        ));
    }

    #[test]
    fn test_analyze_json_with_quality_empty() {
        let json = write_file("[]");
        let report = analyze_json_with_quality(json.path()).unwrap();
        assert_eq!(report.scan_info.total_rows, 0);
        assert!(report.column_profiles.is_empty());
    }

    #[test]
    fn test_analyze_json_boolean_and_nested() {
        let json = write_file(
            r#"[{"flag":true,"nested":{"a":1}},{"flag":false,"nested":{"b":2}}]"#,
        );
        let profiles = analyze_json(json.path()).unwrap();

        let flag = profiles.iter().find(|p| p.name == "flag").unwrap();
        assert_eq!(flag.total_count, 2);

        // Nested objects are serialized as JSON strings
        let nested = profiles.iter().find(|p| p.name == "nested").unwrap();
        assert_eq!(nested.total_count, 2);
    }
}
