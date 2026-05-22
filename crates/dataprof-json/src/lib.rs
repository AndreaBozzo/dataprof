use std::io::BufRead;

use dataprof_core::{DataProfilerError, FileFormat};
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

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
}
