use std::collections::HashSet;
use std::io::BufRead;
use std::path::Path;

use dataprof_core::{
    ColumnProfile, DataProfilerError, DataSource, ExecutionMetadata, FileFormat, QualityDimension,
    SemanticHints,
};
use dataprof_runtime::{
    ProfileReport, ReportAssembler, StreamingColumnCollection, profile_builder,
};
use serde::Deserialize;
use serde_json::Value;

/// JSON/JSONL format hint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonFormat {
    /// Standard JSON array of objects (`[{...}, {...}]`).
    JsonArray,
    /// JSON Lines — one JSON object per line.
    Jsonl,
}

/// Configuration for JSON/JSONL parsing and scanning.
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

/// Borrowed JSON object callback payload.
pub type JsonObject = serde_json::Map<String, Value>;

/// Summary of a JSON/JSONL scan.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JsonScanSummary {
    pub rows_read: usize,
    pub malformed_lines: usize,
    pub format: FileFormat,
}

/// Scan JSON or JSONL input and invoke `on_object` for each object record.
///
/// - **JSONL**: scans one top-level JSON value at a time from the reader.
/// - **JSON array**: streams array elements without buffering the entire input.
pub fn scan_json_from_reader<R, F>(
    mut reader: R,
    config: &JsonParserConfig,
    mut on_object: F,
) -> Result<JsonScanSummary, DataProfilerError>
where
    R: BufRead,
    F: FnMut(&JsonObject),
{
    let format = match config.format {
        Some(JsonFormat::JsonArray) => FileFormat::Json,
        Some(JsonFormat::Jsonl) => FileFormat::Jsonl,
        None => match consume_leading_whitespace(&mut reader)? {
            Some(b'[') => FileFormat::Json,
            _ => FileFormat::Jsonl,
        },
    };

    let mut rows_read = 0;
    let mut malformed_lines = 0;

    match format {
        FileFormat::Jsonl => loop {
            if let Some(max) = config.max_rows
                && rows_read >= max
            {
                break;
            }

            let mut deserializer = serde_json::Deserializer::from_reader(&mut reader);
            let value: Value = match Value::deserialize(&mut deserializer) {
                Ok(v) => v,
                Err(err) if err.classify() == serde_json::error::Category::Eof => break,
                Err(_) => {
                    malformed_lines += 1;
                    skip_to_next_line(&mut reader)?;
                    continue;
                }
            };

            if let Value::Object(ref obj) = value {
                on_object(obj);
                rows_read += 1;
            }
        },
        FileFormat::Json => {
            let mut found_array = false;
            loop {
                let mut consume = 0;
                {
                    let buf = reader.fill_buf().map_err(DataProfilerError::from)?;
                    if buf.is_empty() {
                        break;
                    }
                    for &byte in buf {
                        consume += 1;
                        if byte == b'[' {
                            found_array = true;
                            break;
                        } else if !byte.is_ascii_whitespace() {
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
                if config.format.is_some() {
                    return Err(DataProfilerError::JsonParsingError {
                        message: "Expected JSON array (starts with '[') but input does not match"
                            .to_string(),
                    });
                }
            } else {
                loop {
                    let mut consume = 0;
                    let mut found_value = false;
                    let mut end_of_array = false;

                    {
                        let buf = reader.fill_buf().map_err(DataProfilerError::from)?;
                        if buf.is_empty() {
                            break;
                        }
                        for &byte in buf {
                            if byte.is_ascii_whitespace() || byte == b',' {
                                consume += 1;
                            } else if byte == b']' {
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

                        let mut deserializer = serde_json::Deserializer::from_reader(&mut reader);
                        match Value::deserialize(&mut deserializer) {
                            Ok(Value::Object(obj)) => {
                                on_object(&obj);
                                rows_read += 1;
                            }
                            Ok(_) => {}
                            Err(_) => {
                                malformed_lines += 1;
                                break;
                            }
                        }
                    }
                }
            }
        }
        _ => unreachable!("json scanner only returns json or jsonl formats"),
    }

    Ok(JsonScanSummary {
        rows_read,
        malformed_lines,
        format,
    })
}

fn consume_leading_whitespace<R: BufRead>(reader: &mut R) -> Result<Option<u8>, DataProfilerError> {
    loop {
        let mut bytes_to_consume = 0;
        let first_non_whitespace = {
            let buf = reader.fill_buf().map_err(DataProfilerError::from)?;
            if buf.is_empty() {
                return Ok(None);
            }

            let first_non_whitespace = buf.iter().find(|byte| !byte.is_ascii_whitespace()).copied();
            if first_non_whitespace.is_none() {
                bytes_to_consume = buf.len();
            }
            first_non_whitespace
        };

        if first_non_whitespace.is_some() {
            return Ok(first_non_whitespace);
        }

        reader.consume(bytes_to_consume);
    }
}

fn skip_to_next_line<R: BufRead>(reader: &mut R) -> Result<(), DataProfilerError> {
    let mut discarded = Vec::new();
    reader
        .read_until(b'\n', &mut discarded)
        .map_err(DataProfilerError::from)?;
    Ok(())
}

/// Convert a JSON [`Value`] to a flat string for column storage.
fn json_value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(string) => string.to_string(),
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_default(),
    }
}

/// Feed a JSON object's fields into a [`StreamingColumnCollection`].
fn feed_json_object(
    obj: &JsonObject,
    known_columns: &mut Vec<String>,
    known_columns_set: &mut HashSet<String>,
    column_stats: &mut StreamingColumnCollection,
) {
    for key in obj.keys() {
        if known_columns_set.insert(key.clone()) {
            known_columns.push(key.clone());
        }
    }

    let values: Vec<String> = known_columns
        .iter()
        .map(|column| {
            obj.get(column)
                .map(json_value_to_string)
                .unwrap_or_default()
        })
        .collect();

    column_stats.process_record(known_columns, values);
}

/// Analyze JSON/JSONL data from a buffered reader using streaming statistics.
///
/// Returns `(column_profiles, streaming_stats, rows_read, malformed_lines, detected_format)`.
pub fn analyze_json_from_reader<R: BufRead>(
    reader: R,
    config: &JsonParserConfig,
) -> Result<
    (
        Vec<ColumnProfile>,
        StreamingColumnCollection,
        usize,
        usize,
        FileFormat,
    ),
    DataProfilerError,
> {
    analyze_json_from_reader_with_hints(reader, config, &SemanticHints::default())
}

pub fn analyze_json_from_reader_with_hints<R: BufRead>(
    reader: R,
    config: &JsonParserConfig,
    semantic_hints: &SemanticHints,
) -> Result<
    (
        Vec<ColumnProfile>,
        StreamingColumnCollection,
        usize,
        usize,
        FileFormat,
    ),
    DataProfilerError,
> {
    let mut column_stats = StreamingColumnCollection::new();
    let mut known_columns = Vec::new();
    let mut known_columns_set = HashSet::new();

    let summary = scan_json_from_reader(reader, config, |obj| {
        feed_json_object(
            obj,
            &mut known_columns,
            &mut known_columns_set,
            &mut column_stats,
        );
    })?;

    let profiles = profile_builder::profiles_from_streaming_with_hints(
        &column_stats,
        false,
        false,
        None,
        semantic_hints,
    );

    Ok((
        profiles,
        column_stats,
        summary.rows_read,
        summary.malformed_lines,
        summary.format,
    ))
}

/// Analyze a JSON or JSONL file, returning a full [`ProfileReport`].
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
    analyze_json_file_with_dimensions_and_hints(
        file_path,
        config,
        quality_dimensions,
        &SemanticHints::default(),
    )
}

pub fn analyze_json_file_with_dimensions_and_hints(
    file_path: &Path,
    config: &JsonParserConfig,
    quality_dimensions: Option<&[QualityDimension]>,
    semantic_hints: &SemanticHints,
) -> Result<ProfileReport, DataProfilerError> {
    let metadata = std::fs::metadata(file_path).map_err(|error| map_io_error(file_path, error))?;
    let start = std::time::Instant::now();

    let file = std::fs::File::open(file_path).map_err(|error| map_io_error(file_path, error))?;
    let buf_reader = std::io::BufReader::new(file);

    let (column_profiles, column_stats, rows_read, malformed_lines, format) =
        analyze_json_from_reader_with_hints(buf_reader, config, semantic_hints)?;

    let file_source = DataSource::File {
        path: file_path.display().to_string(),
        format,
        size_bytes: metadata.len(),
        modified_at: None,
        parquet_metadata: None,
    };

    if rows_read == 0 {
        if malformed_lines > 0 {
            return Err(DataProfilerError::JsonParsingError {
                message: "No valid JSON records found in file (malformed JSON encountered)"
                    .to_string(),
            });
        }
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
    .with_quality_data(sample_columns)
    .with_semantic_hints(semantic_hints.clone());
    if let Some(dimensions) = quality_dimensions {
        assembler = assembler.with_requested_dimensions(dimensions.to_vec());
    }
    Ok(assembler.build())
}

fn map_io_error(file_path: &Path, error: std::io::Error) -> DataProfilerError {
    if error.kind() == std::io::ErrorKind::NotFound {
        DataProfilerError::FileNotFound {
            path: file_path.display().to_string(),
        }
    } else {
        DataProfilerError::from(error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;

    fn write_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{}", content).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_scan_jsonl_detects_format_and_rows() {
        let data = b"{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n";
        let mut rows = 0;

        let summary = scan_json_from_reader(
            Cursor::new(data.as_ref()),
            &JsonParserConfig::default(),
            |_| {
                rows += 1;
            },
        )
        .unwrap();

        assert_eq!(summary.format, FileFormat::Jsonl);
        assert_eq!(summary.rows_read, 3);
        assert_eq!(summary.malformed_lines, 0);
        assert_eq!(rows, 3);
    }

    #[test]
    fn test_scan_json_array_detects_format_and_rows() {
        let data = br#"[{"name":"Alice"},{"name":"Bob"}]"#;
        let mut keys = Vec::new();

        let summary = scan_json_from_reader(
            Cursor::new(data.as_ref()),
            &JsonParserConfig::default(),
            |obj| keys.push(obj.keys().cloned().collect::<Vec<_>>()),
        )
        .unwrap();

        assert_eq!(summary.format, FileFormat::Json);
        assert_eq!(summary.rows_read, 2);
        assert_eq!(keys.len(), 2);
        assert_eq!(keys[0], vec!["name".to_string()]);
    }

    #[test]
    fn test_scan_respects_max_rows() {
        let data = b"{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n{\"x\":4}\n";
        let config = JsonParserConfig::default().with_max_rows(2);
        let mut rows = 0;

        let summary =
            scan_json_from_reader(Cursor::new(data.as_ref()), &config, |_| rows += 1).unwrap();

        assert_eq!(summary.rows_read, 2);
        assert_eq!(rows, 2);
    }

    #[test]
    fn test_scan_skips_malformed_jsonl_lines() {
        let data = b"{\"x\":1}\n{\"x\":,bad}\n{\"x\":3}\n";
        let mut rows = 0;

        let summary = scan_json_from_reader(
            Cursor::new(data.as_ref()),
            &JsonParserConfig::jsonl(),
            |_| {
                rows += 1;
            },
        )
        .unwrap();

        assert_eq!(summary.format, FileFormat::Jsonl);
        assert_eq!(summary.rows_read, 2);
        assert_eq!(summary.malformed_lines, 1);
        assert_eq!(rows, 2);
    }

    #[test]
    fn test_scan_pretty_printed_root_object_is_one_jsonl_row() {
        let data = br#"{
    "type": "FeatureCollection",
    "features": [
        {"id": 1},
        {"id": 2}
  ]
}"#;
        let mut object_field_counts = Vec::new();

        let summary = scan_json_from_reader(
            Cursor::new(data.as_ref()),
            &JsonParserConfig::default(),
            |obj| object_field_counts.push(obj.len()),
        )
        .unwrap();

        assert_eq!(summary.format, FileFormat::Jsonl);
        assert_eq!(summary.rows_read, 1);
        assert_eq!(summary.malformed_lines, 0);
        assert_eq!(object_field_counts, vec![2]);
    }

    #[test]
    fn test_forced_json_array_requires_opening_bracket() {
        let data = br#"{"x":1}"#;
        let err = scan_json_from_reader(
            Cursor::new(data.as_ref()),
            &JsonParserConfig::json_array(),
            |_| {},
        )
        .expect_err("forced json array should reject non-array input");

        assert!(err.to_string().contains("Expected JSON array"));
    }

    #[test]
    fn test_analyze_json_from_reader_jsonl_streaming() {
        let data = b"{\"x\":1,\"y\":\"a\"}\n{\"x\":2,\"y\":\"b\"}\n{\"x\":3,\"y\":\"c\"}\n";
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::default();

        let (profiles, _stats, rows, _malformed, format) =
            analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(format, FileFormat::Jsonl);
        assert_eq!(rows, 3);
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn test_analyze_json_from_reader_json_array() {
        let data = br#"[{"name":"Alice","age":25},{"name":"Bob","age":30}]"#;
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::default();

        let (profiles, _stats, rows, _malformed, format) =
            analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(format, FileFormat::Json);
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn test_analyze_json_from_reader_max_rows() {
        let data = b"{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n{\"x\":4}\n{\"x\":5}\n";
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::default().with_max_rows(3);

        let (_profiles, _stats, rows, _malformed, _format) =
            analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 3);
    }

    #[test]
    fn test_analyze_json_from_reader_missing_fields() {
        let data = b"{\"a\":1,\"b\":2}\n{\"a\":3}\n";
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::jsonl();

        let (profiles, _stats, rows, _malformed, _format) =
            analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);

        let col_b = profiles.iter().find(|profile| profile.name == "b").unwrap();
        assert_eq!(col_b.total_count, 2);
    }

    #[test]
    fn test_analyze_json_file_quality_report() {
        let file = write_file(r#"[{"x":1},{"x":2}]"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(file.path(), &config).unwrap();

        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.column_profiles.len(), 1);
        assert!(report.quality_score().unwrap() >= 0.0);
    }

    #[test]
    fn test_jsonl_skips_malformed_lines() {
        let data = b"{\"x\":1}\n{\"x\":,malformed}\n{\"x\":3}\n";
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::jsonl();

        let (profiles, _stats, rows, _malformed, format) =
            analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(format, FileFormat::Jsonl);
        assert_eq!(rows, 2);
        assert_eq!(profiles[0].total_count, 2);
    }

    #[test]
    fn test_analyze_json_with_large_leading_whitespace() {
        let data = format!("{}[{{\"x\":1}}]", " ".repeat(10_000));
        let cursor = Cursor::new(data.into_bytes());
        let config = JsonParserConfig::default();

        let (_profiles, _stats, rows, malformed, format) =
            analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(format, FileFormat::Json);
        assert_eq!(rows, 1);
        assert_eq!(malformed, 0);
    }

    #[test]
    fn test_analyze_json_array() {
        let json = write_file(r#"[{"name":"Alice","age":25},{"name":"Bob","age":30}]"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        assert_eq!(profiles.len(), 2);
        let names: Vec<&str> = profiles
            .iter()
            .map(|profile| profile.name.as_str())
            .collect();
        assert!(names.contains(&"name"));
        assert!(names.contains(&"age"));

        let age = profiles
            .iter()
            .find(|profile| profile.name == "age")
            .unwrap();
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

        let col_a = profiles.iter().find(|profile| profile.name == "a").unwrap();
        assert_eq!(col_a.null_count, 1);

        let col_b = profiles.iter().find(|profile| profile.name == "b").unwrap();
        assert_eq!(col_b.null_count, 1);
    }

    #[test]
    fn test_analyze_json_with_missing_fields() {
        let json = write_file(r#"[{"a":1,"b":2},{"a":3}]"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        let col_b = profiles.iter().find(|profile| profile.name == "b").unwrap();
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
    fn test_analyze_json_malformed_returns_error() {
        let json = write_file("this is entirely invalid json");
        let config = JsonParserConfig::default();
        let err = analyze_json_file(json.path(), &config)
            .expect_err("malformed JSON should return an error");

        let message = err.to_string().to_lowercase();
        assert!(message.contains("malformed") && message.contains("json"));
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
        let json = write_file("");
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

        let flag = profiles
            .iter()
            .find(|profile| profile.name == "flag")
            .unwrap();
        assert_eq!(flag.total_count, 2);

        let nested = profiles
            .iter()
            .find(|profile| profile.name == "nested")
            .unwrap();
        assert_eq!(nested.total_count, 2);
    }

    #[test]
    fn test_single_root_object_compact_yields_one_row() {
        let data = br#"{"type":"FeatureCollection","features":[1,2,3]}"#;
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::default();

        let (profiles, _stats, rows, malformed, _format) =
            analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 1);
        assert_eq!(malformed, 0);
        assert_eq!(profiles.len(), 2);

        let names: Vec<&str> = profiles
            .iter()
            .map(|profile| profile.name.as_str())
            .collect();
        assert!(names.contains(&"type"));
        assert!(names.contains(&"features"));
    }

    #[test]
    fn test_jsonl_multi_object_still_works() {
        let data = b"{\"x\":1}\n{\"x\":2}\n{\"x\":3}\n";
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::default();

        let (_profiles, _stats, rows, _malformed, format) =
            analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(format, FileFormat::Jsonl);
        assert_eq!(rows, 3);
    }

    #[test]
    fn test_single_root_object_via_analyze_json_file() {
        let file = write_file(r#"{"type":"FeatureCollection","features":[{"id":1},{"id":2}]}"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(file.path(), &config).unwrap();

        assert_eq!(report.execution.rows_processed, 1);
        assert_eq!(report.column_profiles.len(), 2);
    }

    #[test]
    fn test_single_root_object_pretty_printed_yields_one_row() {
        let data = br#"{
  "type": "FeatureCollection",
  "features": [
    {"id": 1},
    {"id": 2}
  ]
}"#;
        let cursor = Cursor::new(data.as_ref());
        let config = JsonParserConfig::default();

        let (profiles, _stats, rows, malformed, format) =
            analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(format, FileFormat::Jsonl);
        assert_eq!(rows, 1);
        assert_eq!(malformed, 0);
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn test_single_root_object_pretty_printed_via_analyze_json_file() {
        let file = write_file(
            "{\n  \"type\": \"FeatureCollection\",\n  \"features\": [\n    {\"id\": 1},\n    {\"id\": 2}\n  ]\n}\n",
        );
        let config = JsonParserConfig::default();
        let report = analyze_json_file(file.path(), &config).unwrap();

        assert_eq!(report.execution.rows_processed, 1);
        assert_eq!(report.column_profiles.len(), 2);
    }
}
