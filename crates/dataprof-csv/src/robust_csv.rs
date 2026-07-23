use anyhow::{Context, Result};
use csv::{ReaderBuilder, Trim};
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;

use dataprof_core::errors::AutoRecoveryManager;
use dataprof_core::{DataProfilerError, RecoveryStrategy, RetryConfig};

/// Result type for robust CSV parsing operations.
pub type RobustParseResult = (Option<Vec<String>>, Vec<Vec<String>>);

/// What [`diagnose_non_utf8`] found about a file that failed UTF-8 decoding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodingDiagnostic {
    /// Best-effort encoding guess (e.g. `"latin-1"`, `"windows-1252"`, `"utf-16le"`).
    pub guess: &'static str,
    /// Byte offset of the first invalid UTF-8 sequence, when found within the
    /// sampled prefix. `None` means the sampled prefix was valid UTF-8 and the
    /// offending byte lies further into the file.
    pub first_invalid_byte: Option<usize>,
}

impl EncodingDiagnostic {
    /// Human-readable detail for an error message, without echoing file contents.
    pub fn detail(&self) -> String {
        // Plain ASCII only: this string ends up in an encoding-error message that
        // may be printed to a non-UTF-8 terminal, so it must never add mojibake.
        match self.first_invalid_byte {
            Some(offset) => format!(
                "looks like {}; first invalid UTF-8 byte at offset {offset}",
                self.guess
            ),
            None => format!(
                "looks like {}; contains bytes that are not valid UTF-8",
                self.guess
            ),
        }
    }
}

/// How many leading bytes [`diagnose_non_utf8`] inspects. Large enough to locate
/// the offending byte in the common case, bounded so the error path never reads
/// an entire large file.
const ENCODING_SNIFF_LIMIT: usize = 1 << 20; // 1 MiB

/// Inspect a file that failed UTF-8 decoding and guess its encoding.
///
/// Returns `None` when the sampled prefix is valid UTF-8 (so a failure that is
/// *not* an encoding problem is never mislabeled as one). BOM-prefixed UTF-16 is
/// reported directly; otherwise a byte in `0x80..=0x9F` points at windows-1252
/// and anything else at latin-1 — a best-effort guess, not a definitive verdict.
pub fn diagnose_non_utf8(path: &Path) -> Option<EncodingDiagnostic> {
    use std::io::Read;

    // decode-audit: unknown — this runs only to enrich an error the caller
    // already has. If the file cannot be reopened/read for sniffing, returning
    // None keeps that original engine error, so the failure is surfaced, not lost.
    let file = std::fs::File::open(path).ok()?;
    let mut sample = Vec::new();
    // decode-audit: unknown — as above; a read failure abandons the guess and
    // preserves the caller's original error rather than swallowing it.
    file.take(ENCODING_SNIFF_LIMIT as u64)
        .read_to_end(&mut sample)
        .ok()?;

    if sample.starts_with(&[0xFF, 0xFE]) {
        return Some(EncodingDiagnostic {
            guess: "utf-16le",
            first_invalid_byte: Some(0),
        });
    }
    if sample.starts_with(&[0xFE, 0xFF]) {
        return Some(EncodingDiagnostic {
            guess: "utf-16be",
            first_invalid_byte: Some(0),
        });
    }

    // A valid-UTF-8 prefix means the fault, if any, is beyond the sample — do not
    // claim an encoding problem we cannot see.
    let first_invalid_byte = match std::str::from_utf8(&sample) {
        Ok(_) => return None,
        Err(err) => Some(err.valid_up_to()),
    };

    let has_c1_controls = sample.iter().any(|&b| (0x80..=0x9F).contains(&b));
    let guess = if has_c1_controls {
        "windows-1252"
    } else {
        "latin-1"
    };

    Some(EncodingDiagnostic {
        guess,
        first_invalid_byte,
    })
}

/// Robust CSV parser that handles edge cases and malformed data.
pub struct RobustCsvParser {
    pub flexible: bool,
    pub quote_char: Option<u8>,
    pub delimiter: Option<u8>,
    pub allow_variable_columns: bool,
    pub trim_whitespace: bool,
    pub auto_recovery: bool,
    pub retry_config: RetryConfig,
    pub verbosity: u8,
}

impl Default for RobustCsvParser {
    fn default() -> Self {
        Self {
            flexible: true,
            quote_char: Some(b'"'),
            delimiter: None,
            allow_variable_columns: true,
            trim_whitespace: true,
            auto_recovery: true,
            retry_config: RetryConfig::default(),
            verbosity: 1,
        }
    }
}

impl RobustCsvParser {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn flexible(mut self, flexible: bool) -> Self {
        self.flexible = flexible;
        self
    }

    pub fn allow_variable_columns(mut self, allow: bool) -> Self {
        self.allow_variable_columns = allow;
        self
    }

    /// Set verbosity level for logging.
    /// 0=quiet, 1=normal, 2=verbose, 3=debug
    pub fn verbosity(mut self, level: u8) -> Self {
        self.verbosity = level;
        self
    }

    /// Enable or disable auto-recovery features.
    pub fn with_auto_recovery(mut self, enabled: bool) -> Self {
        self.auto_recovery = enabled;
        self
    }

    /// Set custom retry configuration.
    pub fn with_retry_config(mut self, config: RetryConfig) -> Self {
        self.retry_config = config;
        self
    }

    /// Detect encoding by analyzing file bytes.
    pub fn detect_encoding(&self, file_path: &Path) -> Result<&'static str> {
        let mut file = std::fs::File::open(file_path)?;
        let mut buffer = [0; 4096];
        let bytes_read = std::io::Read::read(&mut file, &mut buffer)?;

        let sample = &buffer[..bytes_read];

        if sample.starts_with(&[0xEF, 0xBB, 0xBF]) {
            return Ok("utf-8");
        }
        if sample.starts_with(&[0xFF, 0xFE]) {
            return Ok("utf-16le");
        }
        if sample.starts_with(&[0xFE, 0xFF]) {
            return Ok("utf-16be");
        }

        if String::from_utf8(sample.to_vec()).is_ok() {
            return Ok("utf-8");
        }

        let latin1_indicators = [
            0x80, 0x81, 0x82, 0x83, 0x84, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8A, 0x8B, 0x8C, 0x8D,
            0x8E, 0x8F,
        ];
        let has_latin1_chars = sample.iter().any(|&byte| latin1_indicators.contains(&byte));

        if has_latin1_chars {
            Ok("latin1")
        } else {
            Ok("utf-8")
        }
    }

    /// Enhanced parsing with auto-recovery.
    pub fn parse_csv_with_recovery(
        &self,
        file_path: &Path,
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        if !self.auto_recovery {
            return self.parse_csv(file_path);
        }

        let mut recovery_manager = AutoRecoveryManager::new(self.retry_config.clone());

        match self.parse_csv(file_path) {
            Ok(result) => Ok(result),
            Err(initial_error) => {
                log::warn!(
                    "Initial CSV parsing failed: {}. Attempting auto-recovery...",
                    initial_error
                );

                let initial_error_string = initial_error.to_string();
                let dp_error = if let Ok(dp_err) = initial_error.downcast::<DataProfilerError>() {
                    dp_err
                } else {
                    DataProfilerError::csv_parsing(
                        &initial_error_string,
                        Some(file_path.to_string_lossy().as_ref()),
                    )
                };

                // Duplicate headers are deterministic across strategies; recovery
                // cannot resolve them, so surface the clear error instead of
                // burying it under "auto-recovery failed".
                if matches!(dp_error, DataProfilerError::DuplicateColumnName { .. }) {
                    return Err(dp_error.into());
                }

                recovery_manager
                    .attempt_recovery(&dp_error, |strategy| {
                        self.try_recovery_strategy(file_path, strategy)
                    })
                    .map_err(|error| error.into())
            }
        }
    }

    fn try_recovery_strategy(
        &self,
        file_path: &Path,
        strategy: RecoveryStrategy,
    ) -> Result<(Vec<String>, Vec<Vec<String>>), DataProfilerError> {
        match strategy {
            RecoveryStrategy::DelimiterDetection { delimiter } => {
                log::debug!("Trying delimiter: '{}'", delimiter);
                self.parse_with_override(file_path, Some(delimiter as u8), None, None)
                    .map_err(|error| {
                        DataProfilerError::csv_parsing(
                            &error.to_string(),
                            Some(file_path.to_string_lossy().as_ref()),
                        )
                    })
            }
            RecoveryStrategy::EncodingConversion { from: _, to: _ } => {
                log::debug!(
                    "Attempting encoding conversion (placeholder - full implementation needed)"
                );
                self.parse_with_override(file_path, None, Some(true), Some(true))
                    .map_err(|error| {
                        DataProfilerError::csv_parsing(
                            &error.to_string(),
                            Some(file_path.to_string_lossy().as_ref()),
                        )
                    })
            }
            RecoveryStrategy::FlexibleParsing => {
                log::debug!("Enabling flexible parsing");
                self.parse_with_override(file_path, None, Some(true), Some(true))
                    .map_err(|error| {
                        DataProfilerError::csv_parsing(
                            &error.to_string(),
                            Some(file_path.to_string_lossy().as_ref()),
                        )
                    })
            }
            _ => Err(DataProfilerError::csv_parsing(
                "Unsupported recovery strategy for CSV parsing",
                Some(file_path.to_string_lossy().as_ref()),
            )),
        }
    }

    fn parse_with_override(
        &self,
        file_path: &Path,
        delimiter_override: Option<u8>,
        flexible_override: Option<bool>,
        allow_variable_override: Option<bool>,
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let delimiter = delimiter_override
            .or(self.delimiter)
            .unwrap_or_else(|| self.detect_delimiter(file_path).unwrap_or(b','));

        let flexible = flexible_override.unwrap_or(self.flexible);
        let allow_variable = allow_variable_override.unwrap_or(self.allow_variable_columns);

        self.parse_with_config(file_path, delimiter, flexible, allow_variable)
    }

    fn parse_with_config(
        &self,
        file_path: &Path,
        delimiter: u8,
        flexible: bool,
        _allow_variable: bool,
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let mut reader = ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(true)
            .flexible(flexible)
            .quote(self.quote_char.unwrap_or(b'"'))
            .trim(if self.trim_whitespace {
                Trim::All
            } else {
                Trim::None
            })
            .from_path(file_path)?;

        let headers: Vec<String> = reader
            .headers()?
            .iter()
            .map(|value| value.to_string())
            .collect();
        let expected_field_count = headers.len();
        let mut records = Vec::new();

        if flexible {
            for result in reader.records() {
                match result {
                    Ok(record) => {
                        let mut row: Vec<String> =
                            record.iter().map(|value| value.to_string()).collect();
                        if row.len() < expected_field_count {
                            row.resize(expected_field_count, String::new());
                        } else if row.len() > expected_field_count {
                            row.truncate(expected_field_count);
                        }
                        records.push(row);
                    }
                    Err(_) => continue,
                }
            }
        } else {
            for result in reader.records() {
                let record = result?;
                let row: Vec<String> = record.iter().map(|value| value.to_string()).collect();
                records.push(row);
            }
        }

        Ok((headers, records))
    }

    /// Detect delimiter by sampling the file.
    ///
    /// Delegates to the shared quote-aware sniffer so both parsers agree on
    /// the delimiter for the same file.
    pub fn detect_delimiter(&self, file_path: &Path) -> Result<u8> {
        Ok(crate::detect_delimiter_from_path(file_path)?)
    }

    /// Parse CSV with robust error handling.
    pub fn parse_csv(&self, file_path: &Path) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let delimiter = if let Some(delimiter) = self.delimiter {
            delimiter
        } else {
            self.detect_delimiter(file_path)
                .context("Failed to detect delimiter")?
        };

        let result = match self.try_strict_parsing(file_path, delimiter) {
            Ok(result) => result,
            Err(error) => {
                if self.verbosity >= 2 {
                    log::info!("Using flexible CSV parsing (strict mode failed: {})", error);
                }
                self.try_flexible_parsing(file_path, delimiter)
                    .context("Both strict and flexible CSV parsing failed")?
            }
        };

        // Duplicate headers are a property of the file, identical across parse
        // strategies, so reject once here rather than in each leaf parser.
        dataprof_core::validate_unique_column_names(&result.0, "CSV header")?;
        Ok(result)
    }

    fn try_strict_parsing(
        &self,
        file_path: &Path,
        delimiter: u8,
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let mut reader = ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(true)
            .flexible(false)
            .quote(self.quote_char.unwrap_or(b'"'))
            .trim(if self.trim_whitespace {
                Trim::All
            } else {
                Trim::None
            })
            .from_path(file_path)?;

        let headers: Vec<String> = reader
            .headers()?
            .iter()
            .map(|value| value.to_string())
            .collect();
        let mut records = Vec::new();

        for result in reader.records() {
            let record = result?;
            let row: Vec<String> = record.iter().map(|value| value.to_string()).collect();
            records.push(row);
        }

        Ok((headers, records))
    }

    fn try_flexible_parsing(
        &self,
        file_path: &Path,
        delimiter: u8,
    ) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let mut reader = ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(true)
            .flexible(true)
            .quote(self.quote_char.unwrap_or(b'"'))
            .trim(if self.trim_whitespace {
                Trim::All
            } else {
                Trim::None
            })
            .from_path(file_path)?;

        let headers: Vec<String> = reader
            .headers()?
            .iter()
            .map(|value| value.to_string())
            .collect();
        let expected_field_count = headers.len();
        let mut records = Vec::new();
        let mut error_count = 0;
        let max_errors = 100;

        for (row_index, result) in reader.records().enumerate() {
            match result {
                Ok(record) => {
                    let mut row: Vec<String> =
                        record.iter().map(|value| value.to_string()).collect();

                    if self.allow_variable_columns {
                        if row.len() < expected_field_count {
                            row.resize(expected_field_count, String::new());
                        } else if row.len() > expected_field_count {
                            if row.len() == expected_field_count + 1
                                && let (Some(second_last), Some(last)) = (
                                    row.get(expected_field_count - 1),
                                    row.get(expected_field_count),
                                )
                            {
                                let combined =
                                    format!("{}{}{}", second_last, delimiter as char, last);
                                row[expected_field_count - 1] = combined;
                            }
                            row.truncate(expected_field_count);
                        }
                    }

                    records.push(row);
                }
                Err(error) => {
                    error_count += 1;

                    let file_path_str = file_path.to_string_lossy();
                    let enhanced_error = DataProfilerError::csv_parsing(
                        &error.to_string(),
                        Some(file_path_str.as_ref()),
                    );
                    log::warn!("Row {}: {}", row_index + 2, enhanced_error);

                    if error_count > max_errors {
                        return Err(DataProfilerError::csv_parsing(
                            &format!("Too many parsing errors ({})", error_count),
                            Some(file_path_str.as_ref()),
                        )
                        .into());
                    }
                }
            }
        }

        if error_count > 0 {
            log::warn!(
                "Successfully parsed {} rows with {} errors skipped.",
                records.len(),
                error_count
            );
        }

        Ok((headers, records))
    }

    /// Parse a specific chunk of CSV data.
    pub fn parse_csv_chunk(
        &self,
        data: &str,
        delimiter: u8,
        has_headers: bool,
    ) -> Result<RobustParseResult> {
        let mut reader = ReaderBuilder::new()
            .delimiter(delimiter)
            .has_headers(has_headers)
            .flexible(self.flexible)
            .quote(self.quote_char.unwrap_or(b'"'))
            .trim(if self.trim_whitespace {
                Trim::All
            } else {
                Trim::None
            })
            .from_reader(Cursor::new(data));

        let headers: Option<Vec<String>> = if has_headers {
            let names: Vec<String> = reader
                .headers()?
                .iter()
                .map(|value| value.to_string())
                .collect();
            dataprof_core::validate_unique_column_names(&names, "CSV header")?;
            Some(names)
        } else {
            None
        };

        let mut records = Vec::new();
        let mut error_count = 0;

        for (row_index, result) in reader.records().enumerate() {
            match result {
                Ok(record) => {
                    let row: Vec<String> = record.iter().map(|value| value.to_string()).collect();
                    records.push(row);
                }
                Err(error) => {
                    error_count += 1;
                    if error_count <= 10 {
                        log::warn!("Chunk row {}: {}", row_index + 1, error);
                    }
                }
            }
        }

        Ok((headers, records))
    }

    /// Validate CSV structure and provide diagnostic information.
    pub fn validate_csv(&self, file_path: &Path) -> Result<CsvDiagnostics> {
        let file = std::fs::File::open(file_path)?;
        let reader = BufReader::new(file);
        let mut diagnostics = CsvDiagnostics::default();

        let mut field_counts = std::collections::HashMap::new();

        for line_result in reader.lines() {
            let line = line_result?;

            if line.trim().is_empty() {
                diagnostics.empty_lines += 1;
                continue;
            }

            if line.contains('\0') {
                diagnostics.null_bytes += 1;
            }

            let quote_count = line.chars().filter(|&ch| ch == '"').count();
            if quote_count % 2 != 0 {
                diagnostics.unmatched_quotes += 1;
            }

            let delimiter = self.delimiter.unwrap_or(b',') as char;
            let field_count = crate::count_fields(&line, delimiter);
            *field_counts.entry(field_count).or_insert(0) += 1;

            diagnostics.total_lines += 1;
        }

        if let Some((&most_common_fields, &count)) =
            field_counts.iter().max_by_key(|(_, count)| *count)
        {
            diagnostics.most_common_field_count = most_common_fields;
            diagnostics.consistent_field_rows = count;
        }

        diagnostics.field_count_variations = field_counts.len();
        diagnostics.inconsistent_field_rows =
            diagnostics.total_lines - diagnostics.consistent_field_rows;

        Ok(diagnostics)
    }
}

#[derive(Debug, Default)]
pub struct CsvDiagnostics {
    pub total_lines: usize,
    pub empty_lines: usize,
    pub most_common_field_count: usize,
    pub consistent_field_rows: usize,
    pub inconsistent_field_rows: usize,
    pub field_count_variations: usize,
    pub unmatched_quotes: usize,
    pub null_bytes: usize,
}

impl CsvDiagnostics {
    pub fn print_summary(&self) {
        println!("CSV File Diagnostics:");
        println!("  Total lines: {}", self.total_lines);
        println!("  Empty lines: {}", self.empty_lines);
        println!(
            "  Most common field count: {}",
            self.most_common_field_count
        );
        println!(
            "  Consistent rows: {} ({:.1}%)",
            self.consistent_field_rows,
            self.consistent_field_rows as f64 / self.total_lines as f64 * 100.0
        );

        if self.inconsistent_field_rows > 0 {
            println!(
                "  WARNING: Inconsistent rows: {} ({:.1}%)",
                self.inconsistent_field_rows,
                self.inconsistent_field_rows as f64 / self.total_lines as f64 * 100.0
            );
        }

        if self.field_count_variations > 1 {
            println!(
                "  WARNING: Different field counts detected: {}",
                self.field_count_variations
            );
        }

        if self.unmatched_quotes > 0 {
            println!(
                "  WARNING: Lines with unmatched quotes: {}",
                self.unmatched_quotes
            );
        }

        if self.null_bytes > 0 {
            println!("  WARNING: Lines with null bytes: {}", self.null_bytes);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    fn write_bytes(bytes: &[u8]) -> NamedTempFile {
        let mut f = NamedTempFile::new().unwrap();
        f.write_all(bytes).unwrap();
        f.flush().unwrap();
        f
    }

    #[test]
    fn diagnose_non_utf8_flags_latin1() {
        // "café" in latin-1: the 0xE9 (é) is an invalid lone continuation byte.
        let f = write_bytes(b"name\ncaf\xE9\n");
        let d = diagnose_non_utf8(f.path()).expect("latin-1 should be flagged");
        assert_eq!(d.guess, "latin-1");
        assert_eq!(d.first_invalid_byte, Some(8));
    }

    #[test]
    fn diagnose_non_utf8_flags_windows1252() {
        // 0x93/0x94 are smart quotes in windows-1252 (C1 range), invalid UTF-8.
        let f = write_bytes(b"quote\n\x93hi\x94\n");
        let d = diagnose_non_utf8(f.path()).expect("windows-1252 should be flagged");
        assert_eq!(d.guess, "windows-1252");
        assert_eq!(d.first_invalid_byte, Some(6));
    }

    #[test]
    fn diagnose_non_utf8_flags_utf16_bom() {
        let f = write_bytes(&[0xFF, 0xFE, b'a', 0x00]);
        let d = diagnose_non_utf8(f.path()).expect("utf-16 BOM should be flagged");
        assert_eq!(d.guess, "utf-16le");
    }

    #[test]
    fn diagnose_non_utf8_ignores_clean_utf8() {
        let f = write_bytes("name\ncafé\n".as_bytes());
        assert!(diagnose_non_utf8(f.path()).is_none());
    }

    #[test]
    fn encoding_diagnostic_detail_never_leaks_bytes() {
        let d = EncodingDiagnostic {
            guess: "latin-1",
            first_invalid_byte: Some(8),
        };
        let detail = d.detail();
        assert!(detail.contains("latin-1"));
        assert!(detail.contains("offset 8"));
    }

    #[test]
    fn test_robust_csv_parser_basic() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,New York")?;
        writeln!(temp_file, "Bob,30,London")?;
        temp_file.flush()?;

        let parser = RobustCsvParser::new();
        let (headers, records) = parser.parse_csv(temp_file.path())?;

        assert_eq!(headers, vec!["name", "age", "city"]);
        assert_eq!(records.len(), 2);
        assert_eq!(records[0], vec!["Alice", "25", "New York"]);

        Ok(())
    }

    #[test]
    fn test_flexible_field_counts() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,New York")?;
        writeln!(temp_file, "Bob,30")?;
        writeln!(temp_file, "Charlie,35,London,UK")?;
        temp_file.flush()?;

        let mut parser = RobustCsvParser::new();
        parser.delimiter = Some(b',');
        let parser = parser.allow_variable_columns(true);
        let (headers, records) = parser.parse_csv(temp_file.path())?;

        assert_eq!(headers, vec!["name", "age", "city"]);
        assert_eq!(records.len(), 3);
        assert_eq!(records[1], vec!["Bob", "30", ""]);
        assert_eq!(records[2], vec!["Charlie", "35", "London,UK"]);

        Ok(())
    }

    #[test]
    fn test_delimiter_detection() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name;age;city")?;
        writeln!(temp_file, "Alice;25;New York")?;
        writeln!(temp_file, "Bob;30;London")?;
        temp_file.flush()?;

        let parser = RobustCsvParser::new();
        let delimiter = parser.detect_delimiter(temp_file.path())?;
        assert!(delimiter == b';' || delimiter == b',');

        Ok(())
    }

    #[test]
    fn test_csv_diagnostics() -> Result<()> {
        let mut temp_file = NamedTempFile::new()?;
        writeln!(temp_file, "name,age,city")?;
        writeln!(temp_file, "Alice,25,New York")?;
        writeln!(temp_file, "Bob,30")?;
        writeln!(temp_file)?;
        writeln!(temp_file, "Charlie,35,London")?;
        temp_file.flush()?;

        let parser = RobustCsvParser::new();
        let diagnostics = parser.validate_csv(temp_file.path())?;

        assert_eq!(diagnostics.total_lines, 4);
        assert_eq!(diagnostics.empty_lines, 1);
        assert_eq!(diagnostics.most_common_field_count, 3);
        assert_eq!(diagnostics.inconsistent_field_rows, 1);

        Ok(())
    }
}
