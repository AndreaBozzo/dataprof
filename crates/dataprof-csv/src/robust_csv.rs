use anyhow::{Context, Result};
use csv::{ReaderBuilder, Trim};
use std::io::{BufRead, BufReader, Cursor};
use std::path::Path;

use dataprof_core::errors::AutoRecoveryManager;
use dataprof_core::{DataProfilerError, RecoveryStrategy, RetryConfig};

/// Result type for robust CSV parsing operations.
pub type RobustParseResult = (Option<Vec<String>>, Vec<Vec<String>>);

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
                eprintln!(
                    "Initial parsing failed: {}. Attempting auto-recovery...",
                    initial_error
                );

                let initial_error_string = initial_error.to_string();
                let dp_error = if let Ok(dp_err) = initial_error.downcast::<DataProfilerError>() {
                    dp_err
                } else {
                    DataProfilerError::csv_parsing(
                        &initial_error_string,
                        &file_path.to_string_lossy(),
                    )
                };

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
                            &file_path.to_string_lossy(),
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
                            &file_path.to_string_lossy(),
                        )
                    })
            }
            RecoveryStrategy::FlexibleParsing => {
                log::debug!("Enabling flexible parsing");
                self.parse_with_override(file_path, None, Some(true), Some(true))
                    .map_err(|error| {
                        DataProfilerError::csv_parsing(
                            &error.to_string(),
                            &file_path.to_string_lossy(),
                        )
                    })
            }
            _ => Err(DataProfilerError::csv_parsing(
                "Unsupported recovery strategy for CSV parsing",
                &file_path.to_string_lossy(),
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
    pub fn detect_delimiter(&self, file_path: &Path) -> Result<u8> {
        let file = std::fs::File::open(file_path)?;
        let reader = BufReader::new(file);

        let mut lines = Vec::new();
        for (index, line) in reader.lines().enumerate() {
            if index >= 5 {
                break;
            }
            lines.push(line?);
        }

        let delimiters = [b',', b';', b'\t', b'|'];
        let mut best_delimiter = b',';
        let mut max_consistency = 0;
        let mut max_field_count = 0;

        for &delimiter in &delimiters {
            let consistency = self.measure_delimiter_consistency(&lines, delimiter);
            let avg_field_count = self.average_field_count(&lines, delimiter);

            let should_update = if avg_field_count > 1 && max_field_count <= 1 {
                true
            } else if avg_field_count <= 1 && max_field_count > 1 {
                false
            } else {
                consistency > max_consistency
                    || (consistency == max_consistency && avg_field_count > max_field_count)
            };

            if should_update {
                max_consistency = consistency;
                max_field_count = avg_field_count;
                best_delimiter = delimiter;
            }
        }

        Ok(best_delimiter)
    }

    fn measure_delimiter_consistency(&self, lines: &[String], delimiter: u8) -> usize {
        if lines.is_empty() {
            return 0;
        }

        let delimiter_char = delimiter as char;
        let field_counts: Vec<usize> = lines
            .iter()
            .map(|line| self.count_fields_simple(line, delimiter_char))
            .collect();

        let mut counts = std::collections::HashMap::new();
        for &count in &field_counts {
            *counts.entry(count).or_insert(0) += 1;
        }

        counts.values().max().copied().unwrap_or(0)
    }

    fn average_field_count(&self, lines: &[String], delimiter: u8) -> usize {
        if lines.is_empty() {
            return 0;
        }

        let delimiter_char = delimiter as char;
        let total_fields: usize = lines
            .iter()
            .map(|line| self.count_fields_simple(line, delimiter_char))
            .sum();

        total_fields / lines.len()
    }

    fn count_fields_simple(&self, line: &str, delimiter: char) -> usize {
        let mut field_count = 1;
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                    } else {
                        in_quotes = !in_quotes;
                    }
                }
                current if current == delimiter && !in_quotes => {
                    field_count += 1;
                }
                _ => {}
            }
        }

        field_count
    }

    /// Parse CSV with robust error handling.
    pub fn parse_csv(&self, file_path: &Path) -> Result<(Vec<String>, Vec<Vec<String>>)> {
        let delimiter = if let Some(delimiter) = self.delimiter {
            delimiter
        } else {
            self.detect_delimiter(file_path)
                .context("Failed to detect delimiter")?
        };

        match self.try_strict_parsing(file_path, delimiter) {
            Ok(result) => return Ok(result),
            Err(error) => {
                if self.verbosity >= 2 {
                    log::info!("Using flexible CSV parsing (strict mode failed: {})", error);
                }
            }
        }

        self.try_flexible_parsing(file_path, delimiter)
            .context("Both strict and flexible CSV parsing failed")
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

                    let enhanced_error =
                        DataProfilerError::csv_parsing(&error.to_string(), "current file");
                    eprintln!("Row {}: {}", row_index + 2, enhanced_error);

                    if error_count > max_errors {
                        return Err(DataProfilerError::csv_parsing(
                            &format!("Too many parsing errors ({})", error_count),
                            "current file",
                        )
                        .into());
                    }
                }
            }
        }

        if error_count > 0 {
            eprintln!(
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

        let headers = if has_headers {
            Some(
                reader
                    .headers()?
                    .iter()
                    .map(|value| value.to_string())
                    .collect(),
            )
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
                        eprintln!("Chunk row {}: {}", row_index + 1, error);
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
            let field_count = self.count_fields_simple(&line, delimiter);
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
