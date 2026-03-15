use serde_json::Value;
use std::collections::HashSet;
use std::io::BufRead;
use std::path::Path;

use crate::core::errors::DataProfilerError;
use crate::core::profile_builder;
use crate::core::report_assembler::ReportAssembler;
use crate::core::streaming_stats::StreamingColumnCollection;
use crate::types::{
    ColumnProfile, DataSource, ExecutionMetadata, FileFormat, ProfileReport, QualityDimension,
};

// ============================================================================
// NEW CONFIG-BASED API (#218)
// ============================================================================

/// JSON/JSONL format hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonFormat {
    /// Standard JSON array of objects (`[{...}, {...}]`).
    JsonArray,
    /// JSON Lines — one JSON object per line.
    Jsonl,
}

/// Configuration for JSON/JSONL parsing and analysis.
#[derive(Debug, Clone, Default)]
pub struct JsonParserConfig {
    /// Force a specific format (None = auto-detect from content).
    pub format: Option<JsonFormat>,
    /// Maximum rows to process (None = all rows).
    pub max_rows: Option<usize>,
}

impl JsonParserConfig {
    /// Set the maximum number of rows to process.
    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = Some(max_rows);
        self
    }

    /// Force JSONL format.
    pub fn jsonl() -> Self {
        Self {
            format: Some(JsonFormat::Jsonl),
            ..Default::default()
        }
    }

    /// Force JSON array format.
    pub fn json_array() -> Self {
        Self {
            format: Some(JsonFormat::JsonArray),
            ..Default::default()
        }
    }
}

/// Convert a JSON [`Value`] to a flat string for column storage.
fn json_value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

/// Feed a JSON object's fields into a [`StreamingColumnCollection`].
///
/// Tracks which columns have been seen so far in `known_columns` to maintain
/// insertion order and fill missing fields with empty strings.
fn feed_json_object(
    obj: &serde_json::Map<String, Value>,
    known_columns: &mut Vec<String>,
    known_columns_set: &mut HashSet<String>,
    column_stats: &mut StreamingColumnCollection,
) {
    // Register any new columns (HashSet for O(1) lookup, Vec for insertion order)
    for key in obj.keys() {
        if known_columns_set.insert(key.clone()) {
            known_columns.push(key.clone());
        }
    }

    // Build values aligned to known_columns
    let values: Vec<String> = known_columns
        .iter()
        .map(|col| obj.get(col).map(json_value_to_string).unwrap_or_default())
        .collect();

    column_stats.process_record(known_columns, values);
}

/// Analyze JSON/JSONL data from a buffered reader using streaming statistics.
///
/// - **JSONL**: true streaming — reads line-by-line with bounded memory.
/// - **JSON array**: parsed as a streaming array of objects using
///   `serde_json::Deserializer::from_reader`, processing each element with
///   [`StreamingColumnCollection`] without buffering the entire array in memory.
///
/// Returns `(column_profiles, streaming_stats, rows_read, detected_format)`.
pub fn analyze_json_from_reader<R: BufRead>(
    mut reader: R,
    config: &JsonParserConfig,
) -> Result<
    (
        Vec<ColumnProfile>,
        StreamingColumnCollection,
        usize,
        FileFormat,
    ),
    DataProfilerError,
> {
    // Auto-detect format by peeking at the first non-whitespace byte
    let format = match config.format {
        Some(JsonFormat::JsonArray) => FileFormat::Json,
        Some(JsonFormat::Jsonl) => FileFormat::Jsonl,
        None => {
            let buf = reader.fill_buf().map_err(DataProfilerError::from)?;
            let first_char = buf.iter().find(|b| !b.is_ascii_whitespace());
            match first_char {
                Some(b'[') => FileFormat::Json,
                _ => FileFormat::Jsonl,
            }
        }
    };

    let mut column_stats = StreamingColumnCollection::new();
    let mut known_columns: Vec<String> = Vec::new();
    let mut known_columns_set: HashSet<String> = HashSet::new();
    let mut rows_read = 0;

    match format {
        FileFormat::Jsonl => {
            // True streaming: read line-by-line
            let mut line = String::new();
            loop {
                line.clear();
                let bytes = reader
                    .read_line(&mut line)
                    .map_err(DataProfilerError::from)?;
                if bytes == 0 {
                    break; // EOF
                }

                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }

                if let Some(max) = config.max_rows
                    && rows_read >= max
                {
                    break;
                }

                let value: Value = match serde_json::from_str(trimmed) {
                    Ok(v) => v,
                    Err(_) => continue,
                };
                if let Value::Object(ref obj) = value {
                    feed_json_object(
                        obj,
                        &mut known_columns,
                        &mut known_columns_set,
                        &mut column_stats,
                    );
                    rows_read += 1;
                }
            }
        }
        _ => {
            // JSON array: stream elements efficiently
            let mut found_array = false;
            loop {
                let mut consume = 0;
                {
                    let buf = reader.fill_buf().map_err(DataProfilerError::from)?;
                    if buf.is_empty() {
                        break;
                    }
                    for &b in buf {
                        consume += 1;
                        if b == b'[' {
                            found_array = true;
                            break;
                        } else if !b.is_ascii_whitespace() {
                            break;
                        }
                    }
                }
                reader.consume(consume);
                if found_array || consume == 0 {
                    break;
                }
            }

            if !found_array {
                // Format was forced to JsonArray but no '[' was found
                if config.format.is_some() {
                    return Err(DataProfilerError::JsonParsingError {
                        message: "Expected JSON array (starts with '[') but input does not match"
                            .to_string(),
                    });
                }
                // Auto-detected: fall through with 0 rows
            }

            if found_array {
                loop {
                    let mut consume = 0;
                    let mut found_value = false;
                    let mut end_of_array = false;

                    {
                        let buf = reader.fill_buf().map_err(DataProfilerError::from)?;
                        if buf.is_empty() {
                            break;
                        }
                        for &b in buf {
                            if b.is_ascii_whitespace() || b == b',' {
                                consume += 1;
                            } else if b == b']' {
                                end_of_array = true;
                                consume += 1;
                                break;
                            } else {
                                found_value = true;
                                break;
                            }
                        }
                    }

                    reader.consume(consume);
                    if end_of_array {
                        break;
                    }

                    if found_value {
                        if let Some(max) = config.max_rows
                            && rows_read >= max
                        {
                            break;
                        }

                        let mut de = serde_json::Deserializer::from_reader(&mut reader);
                        match serde::Deserialize::deserialize(&mut de) {
                            Ok(Value::Object(obj)) => {
                                feed_json_object(
                                    &obj,
                                    &mut known_columns,
                                    &mut known_columns_set,
                                    &mut column_stats,
                                );
                                rows_read += 1;
                            }
                            Ok(_) => { /* Skip non-object elements */ }
                            Err(_) => {
                                // If parsing fails (e.g., malformed object), stop array stream
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    let profiles = profile_builder::profiles_from_streaming(&column_stats);

    Ok((profiles, column_stats, rows_read, format))
}

/// Analyze a JSON/JSONL file, returning a full [`ProfileReport`].
pub fn analyze_json_file(
    file_path: &Path,
    config: &JsonParserConfig,
) -> Result<ProfileReport, DataProfilerError> {
    analyze_json_file_with_dimensions(file_path, config, None)
}

/// Like [`analyze_json_file`] but only computes the requested quality dimensions.
pub fn analyze_json_file_with_dimensions(
    file_path: &Path,
    config: &JsonParserConfig,
    quality_dimensions: Option<&[QualityDimension]>,
) -> Result<ProfileReport, DataProfilerError> {
    let metadata = std::fs::metadata(file_path).map_err(|e| map_io_error(file_path, e))?;
    let start = std::time::Instant::now();

    let file = std::fs::File::open(file_path).map_err(|e| map_io_error(file_path, e))?;
    let buf_reader = std::io::BufReader::new(file);

    let (column_profiles, column_stats, rows_read, format) =
        analyze_json_from_reader(buf_reader, config)?;

    let file_source = DataSource::File {
        path: file_path.display().to_string(),
        format,
        size_bytes: metadata.len(),
        modified_at: None,
        parquet_metadata: None,
    };

    if rows_read == 0 {
        return Ok(ReportAssembler::new(
            file_source,
            ExecutionMetadata::new(0, 0, start.elapsed().as_millis()),
        )
        .skip_quality()
        .build());
    }

    let sample_columns = profile_builder::quality_check_samples(&column_stats);
    let scan_time_ms = start.elapsed().as_millis();
    let num_columns = column_profiles.len();

    let mut assembler = ReportAssembler::new(
        file_source,
        ExecutionMetadata::new(rows_read, num_columns, scan_time_ms),
    )
    .columns(column_profiles)
    .with_quality_data(sample_columns);
    if let Some(dims) = quality_dimensions {
        assembler = assembler.with_requested_dimensions(dims.to_vec());
    }
    Ok(assembler.build())
}

fn map_io_error(file_path: &Path, e: std::io::Error) -> DataProfilerError {
    if e.kind() == std::io::ErrorKind::NotFound {
        DataProfilerError::FileNotFound {
            path: file_path.display().to_string(),
        }
    } else {
        DataProfilerError::from(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;

    fn write_file(content: &str) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        write!(f, "{}", content).unwrap();
        f.flush().unwrap();
        f
    }

    // --- New API tests ---

    #[test]
    fn test_analyze_json_from_reader_jsonl_streaming() {
        let data = b"{\"x\":1,\"y\":\"a\"}\n{\"x\":2,\"y\":\"b\"}\n{\"x\":3,\"y\":\"c\"}\n";
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::default();

        let (profiles, _stats, rows, format) = analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(format, FileFormat::Jsonl);
        assert_eq!(rows, 3);
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn test_analyze_json_from_reader_json_array() {
        let data = br#"[{"name":"Alice","age":25},{"name":"Bob","age":30}]"#;
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::default();

        let (profiles, _stats, rows, format) = analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(format, FileFormat::Json);
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn test_analyze_json_from_reader_max_rows() {
        let data = b"{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n{\"x\":4}\n{\"x\":5}\n";
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::default().with_max_rows(3);

        let (_profiles, _stats, rows, _format) = analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 3);
    }

    #[test]
    fn test_analyze_json_from_reader_missing_fields() {
        let data = b"{\"a\":1,\"b\":2}\n{\"a\":3}\n";
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::jsonl();

        let (profiles, _stats, rows, _format) = analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);

        let col_b = profiles.iter().find(|p| p.name == "b").unwrap();
        assert_eq!(col_b.total_count, 2); // 1 real + 1 empty fill
    }

    #[test]
    fn test_analyze_json_file_quality_report() {
        let f = write_file(r#"[{"x":1},{"x":2}]"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(f.path(), &config).unwrap();

        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.column_profiles.len(), 1);
        assert!(report.quality_score().unwrap() >= 0.0);
    }

    #[test]
    fn test_jsonl_skips_malformed_lines() {
        let data = b"{\"x\":1}\n{\"x\":,malformed}\n{\"x\":3}\n";
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::jsonl();

        let (profiles, _stats, rows, format) = analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(format, FileFormat::Jsonl);
        assert_eq!(rows, 2);
        assert_eq!(profiles[0].total_count, 2);
    }

    // --- Legacy API tests migrated to new API ---

    #[test]
    fn test_analyze_json_array() {
        let json = write_file(r#"[{"name":"Alice","age":25},{"name":"Bob","age":30}]"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json.path(), &config).unwrap();
        let profiles = &report.column_profiles;

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
        let config = JsonParserConfig::default();
        let report = analyze_json_file(jsonl.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        assert_eq!(profiles.len(), 1);
        assert_eq!(profiles[0].name, "x");
        assert_eq!(profiles[0].total_count, 3);
    }

    #[test]
    fn test_analyze_json_with_nulls() {
        let json = write_file(r#"[{"a":"hello","b":1},{"a":null,"b":2},{"a":"world","b":null}]"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        let col_a = profiles.iter().find(|p| p.name == "a").unwrap();
        assert_eq!(col_a.null_count, 1);

        let col_b = profiles.iter().find(|p| p.name == "b").unwrap();
        assert_eq!(col_b.null_count, 1);
    }

    #[test]
    fn test_analyze_json_with_missing_fields() {
        let json = write_file(r#"[{"a":1,"b":2},{"a":3}]"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        let col_b = profiles.iter().find(|p| p.name == "b").unwrap();
        assert_eq!(col_b.total_count, 2);
    }

    #[test]
    fn test_analyze_json_empty_array() {
        let json = write_file("[]");
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json.path(), &config).unwrap();
        assert!(report.column_profiles.is_empty());
    }

    #[test]
    fn test_analyze_json_file_detects_format() {
        let json_array = write_file(r#"[{"x":1}]"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json_array.path(), &config).unwrap();
        assert!(matches!(
            report.data_source,
            DataSource::File {
                format: FileFormat::Json,
                ..
            }
        ));

        let jsonl = write_file("{\"x\":1}\n{\"x\":2}\n");
        let report = analyze_json_file(jsonl.path(), &config).unwrap();
        assert!(matches!(
            report.data_source,
            DataSource::File {
                format: FileFormat::Jsonl,
                ..
            }
        ));
    }

    #[test]
    fn test_analyze_json_file_empty() {
        let json = write_file("[]");
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json.path(), &config).unwrap();
        assert_eq!(report.execution.rows_processed, 0);
        assert!(report.column_profiles.is_empty());
    }

    #[test]
    fn test_analyze_json_boolean_and_nested() {
        let json =
            write_file(r#"[{"flag":true,"nested":{"a":1}},{"flag":false,"nested":{"b":2}}]"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        let flag = profiles.iter().find(|p| p.name == "flag").unwrap();
        assert_eq!(flag.total_count, 2);

        let nested = profiles.iter().find(|p| p.name == "nested").unwrap();
        assert_eq!(nested.total_count, 2);
    }
}
