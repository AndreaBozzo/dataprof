use csv::ReaderBuilder;
use std::io::Read;
use std::path::Path;

use dataprof_core::{
    ColumnProfile, DataProfilerError, DataSource, ExecutionMetadata, FileFormat, QualityDimension,
    SemanticHints,
};
use dataprof_runtime::{
    ProfileReport, ReportAssembler, StreamingColumnCollection, profile_builder,
};

mod memmap_reader;
mod robust_csv;

pub use memmap_reader::MemoryMappedCsvReader;
pub use robust_csv::{CsvDiagnostics, RobustCsvParser, RobustParseResult};

/// Configuration for CSV parsing and analysis.
///
/// Replaces the 7 separate `analyze_csv_*` functions with a single config struct.
/// Supports both file-based and reader-based (streaming) entry points.
#[derive(Debug, Clone)]
pub struct CsvParserConfig {
    /// Allow ragged rows (flexible field counts).
    pub flexible: bool,
    /// Custom delimiter (None = comma).
    pub delimiter: Option<u8>,
    /// Quote character.
    pub quote_char: u8,
    /// Trim whitespace from fields.
    pub trim_whitespace: bool,
    /// Whether the first row contains column headers.
    /// When `false`, columns are named `column_0`, `column_1`, etc.
    pub has_header: bool,
    /// Maximum rows to process (None = all rows).
    pub max_rows: Option<usize>,
}

impl Default for CsvParserConfig {
    fn default() -> Self {
        Self {
            flexible: true,
            delimiter: None,
            quote_char: b'"',
            trim_whitespace: false,
            has_header: true,
            max_rows: None,
        }
    }
}

impl CsvParserConfig {
    /// Create a strict config that rejects ragged rows.
    pub fn strict() -> Self {
        Self {
            flexible: false,
            ..Default::default()
        }
    }

    /// Set the delimiter.
    pub fn with_delimiter(mut self, delimiter: u8) -> Self {
        self.delimiter = Some(delimiter);
        self
    }

    /// Set whether the first row contains column headers.
    pub fn has_header(mut self, yes: bool) -> Self {
        self.has_header = yes;
        self
    }

    /// Set maximum number of rows to evaluate.
    pub fn max_rows(mut self, max: Option<usize>) -> Self {
        self.max_rows = max;
        self
    }
}

/// Count fields in a line for a given delimiter, respecting quoted sections.
fn count_fields(line: &str, delimiter: char) -> usize {
    let mut count = 1;
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '"' {
            if chars.peek() == Some(&'"') {
                chars.next();
            } else {
                in_quotes = !in_quotes;
            }
        } else if ch == delimiter && !in_quotes {
            count += 1;
        }
    }
    count
}

/// Detect the most likely CSV delimiter by sampling up to 4 KB of data.
///
/// Tests comma, semicolon, tab, and pipe. Returns the delimiter that produces
/// the most consistent field counts across the first 5 sample lines, preferring
/// delimiters that yield more than one field.
///
/// Falls back to comma if no clear winner is found.
pub fn detect_delimiter<R: Read>(reader: R) -> std::io::Result<u8> {
    let mut preamble = Vec::new();
    let mut take = reader.take(4096);
    take.read_to_end(&mut preamble)?;

    let preamble_str = String::from_utf8_lossy(&preamble);
    let lines: Vec<String> = preamble_str.lines().map(|line| line.to_string()).collect();

    let delimiters = *b",;\t|";
    let mut best_delimiter = b',';
    let mut max_consistency = 0;
    let mut max_fields = 0;

    for &delimiter in &delimiters {
        let delimiter_char = delimiter as char;
        let mut counts = std::collections::HashMap::new();
        let mut total_fields = 0;

        let sample_lines = std::cmp::min(lines.len(), 5);
        for line in lines.iter().take(sample_lines) {
            let count = count_fields(line, delimiter_char);
            *counts.entry(count).or_insert(0) += 1;
            total_fields += count;
        }

        let consistency = counts.values().max().copied().unwrap_or(0);
        let avg_fields = total_fields.checked_div(sample_lines).unwrap_or(0);

        let should_update = if avg_fields > 1 && max_fields <= 1 {
            true
        } else if avg_fields <= 1 && max_fields > 1 {
            false
        } else {
            consistency > max_consistency
                || (consistency == max_consistency && avg_fields > max_fields)
        };

        if should_update {
            max_consistency = consistency;
            max_fields = avg_fields;
            best_delimiter = delimiter;
        }
    }

    Ok(best_delimiter)
}

/// Detect the most likely CSV delimiter from a file path.
pub fn detect_delimiter_from_path(path: &Path) -> std::io::Result<u8> {
    let file = std::fs::File::open(path)?;
    detect_delimiter(std::io::BufReader::new(file))
}

/// Analyze CSV data from any `Read` source using streaming statistics.
///
/// This is the primary generic entry point for both file-based and stream-based
/// CSV profiling. Data is processed incrementally through [`StreamingColumnCollection`]
/// with bounded memory, rather than accumulating all rows in a `HashMap`.
///
/// Returns `(column_profiles, streaming_stats, rows_read)`.
pub fn analyze_csv_from_reader<R: Read>(
    reader: R,
    config: &CsvParserConfig,
) -> Result<
    (
        Vec<ColumnProfile>,
        StreamingColumnCollection,
        usize,
        Vec<String>,
    ),
    DataProfilerError,
> {
    analyze_csv_from_reader_with_hints(reader, config, &SemanticHints::default())
}

/// Analyze CSV data from any `Read` source while applying semantic hints.
pub fn analyze_csv_from_reader_with_hints<R: Read>(
    reader: R,
    config: &CsvParserConfig,
    semantic_hints: &SemanticHints,
) -> Result<
    (
        Vec<ColumnProfile>,
        StreamingColumnCollection,
        usize,
        Vec<String>,
    ),
    DataProfilerError,
> {
    let mut csv_builder = ReaderBuilder::new();
    csv_builder.has_headers(config.has_header);
    csv_builder.flexible(config.flexible);
    csv_builder.quote(config.quote_char);
    if config.trim_whitespace {
        csv_builder.trim(csv::Trim::All);
    }
    let mut actual_delimiter = config.delimiter;

    let boxed_reader: Box<dyn std::io::Read + '_> = if config.delimiter.is_none() {
        let mut preamble = Vec::new();
        let mut take = reader.take(4096);
        take.read_to_end(&mut preamble)?;

        let detected = detect_delimiter(std::io::Cursor::new(&preamble)).unwrap_or(b',');
        actual_delimiter = Some(detected);

        Box::new(std::io::Cursor::new(preamble).chain(take.into_inner()))
    } else {
        Box::new(reader)
    };

    if let Some(delimiter) = actual_delimiter {
        csv_builder.delimiter(delimiter);
    }

    let mut csv_reader = csv_builder.from_reader(boxed_reader);

    let header_names: Vec<String> = if config.has_header {
        let headers = csv_reader.headers()?;
        headers.iter().map(|header| header.to_string()).collect()
    } else {
        let headers = csv_reader.headers()?;
        (0..headers.len())
            .map(|index| format!("column_{index}"))
            .collect()
    };

    let mut column_stats = StreamingColumnCollection::new();
    let mut rows_read = 0;

    for result in csv_reader.records() {
        if let Some(max_rows) = config.max_rows
            && rows_read >= max_rows
        {
            break;
        }

        let record = result?;
        let mut values: Vec<String> = record.iter().map(|value| value.to_string()).collect();

        let header_len = header_names.len();
        if values.len() < header_len {
            values.resize(header_len, String::new());
        } else if values.len() > header_len {
            values.truncate(header_len);
        }

        column_stats.process_record(&header_names, values);
        rows_read += 1;
    }

    let profiles = profile_builder::profiles_from_streaming_with_hints(
        &column_stats,
        false,
        false,
        None,
        semantic_hints,
    );

    Ok((profiles, column_stats, rows_read, header_names))
}

/// Analyze a CSV file, returning a full [`ProfileReport`].
///
/// Opens the file, delegates to [`analyze_csv_from_reader`], and wraps the
/// result with file metadata and ISO 8000/25012 data quality metrics.
pub fn analyze_csv_file(
    file_path: &Path,
    config: &CsvParserConfig,
) -> Result<ProfileReport, DataProfilerError> {
    analyze_csv_file_with_dimensions(file_path, config, None)
}

/// Like [`analyze_csv_file`] but only computes the requested quality dimensions.
pub fn analyze_csv_file_with_dimensions(
    file_path: &Path,
    config: &CsvParserConfig,
    quality_dimensions: Option<&[QualityDimension]>,
) -> Result<ProfileReport, DataProfilerError> {
    analyze_csv_file_with_dimensions_and_hints(
        file_path,
        config,
        quality_dimensions,
        &SemanticHints::default(),
    )
}

pub fn analyze_csv_file_with_dimensions_and_hints(
    file_path: &Path,
    config: &CsvParserConfig,
    quality_dimensions: Option<&[QualityDimension]>,
    semantic_hints: &SemanticHints,
) -> Result<ProfileReport, DataProfilerError> {
    let metadata = std::fs::metadata(file_path).map_err(|error| map_io_error(file_path, error))?;
    let start = std::time::Instant::now();

    let file = std::fs::File::open(file_path).map_err(|error| map_io_error(file_path, error))?;
    let buf_reader = std::io::BufReader::new(file);

    let (column_profiles, column_stats, rows_read, header_names) =
        analyze_csv_from_reader_with_hints(buf_reader, config, semantic_hints)?;

    let file_source = DataSource::File {
        path: file_path.display().to_string(),
        format: FileFormat::Csv,
        size_bytes: metadata.len(),
        modified_at: None,
        parquet_metadata: None,
    };

    if column_profiles.is_empty() && rows_read == 0 {
        return Ok(ReportAssembler::new(
            file_source,
            ExecutionMetadata::new(0, header_names.len(), start.elapsed().as_millis()),
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
    use dataprof_core::DataType;
    use std::io::{Cursor, Write};
    use tempfile::NamedTempFile;

    fn write_csv(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{}", content).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_analyze_csv_from_reader_basic() {
        let data = b"name,age\nAlice,25\nBob,30\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default();

        let (profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);

        let age = profiles
            .iter()
            .find(|profile| profile.name == "age")
            .unwrap();
        assert_eq!(age.total_count, 2);
        assert_eq!(age.data_type, DataType::Integer);
    }

    #[test]
    fn test_analyze_csv_from_reader_with_max_rows() {
        let data = b"val\n1\n2\n3\n4\n5\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default().max_rows(Some(3));

        let (_profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 3);
    }

    #[test]
    fn test_analyze_csv_from_reader_strict_rejects_ragged() {
        let data = b"a,b\n1,2\n3,4,5\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::strict();

        assert!(analyze_csv_from_reader(cursor, &config).is_err());
    }

    #[test]
    fn test_analyze_csv_from_reader_flexible_handles_ragged() {
        let data = b"a,b\n1,2\n3,4,5\n6\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default();

        let (profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 3);
        assert!(!profiles.is_empty());
    }

    #[test]
    fn test_analyze_csv_from_reader_custom_delimiter() {
        let data = b"a;b\n1;2\n3;4\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default().with_delimiter(b';');

        let (profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn test_analyze_csv_file_returns_quality_report() {
        let csv = write_csv("x,y\n1,a\n2,b\n3,c\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.column_profiles.len(), 2);
        assert_eq!(report.execution.rows_processed, 3);
        assert!(report.quality_score().unwrap() >= 0.0);
        assert!(report.quality_score().unwrap() <= 100.0);
    }

    #[test]
    fn test_csv_auto_detects_delimiter_from_reader() {
        let data = b"name;age\nAlice;25\nBob;30\n";
        let cursor = Cursor::new(data.as_ref());
        let config = CsvParserConfig::default();

        let (profiles, _stats, rows, _) = analyze_csv_from_reader(cursor, &config).unwrap();
        assert_eq!(rows, 2);
        assert_eq!(profiles.len(), 2);

        let age = profiles
            .iter()
            .find(|profile| profile.name == "age")
            .unwrap();
        assert_eq!(age.total_count, 2);
    }

    #[test]
    fn test_analyze_csv_basic() {
        let csv = write_csv("name,age\nAlice,25\nBob,30\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
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
    fn test_analyze_csv_with_nulls() {
        let csv = write_csv("name,age,email\nAlice,25,a@b.com\nBob,,\nCharlie,30,c@d.com\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        let age = profiles
            .iter()
            .find(|profile| profile.name == "age")
            .unwrap();
        assert_eq!(age.total_count, 3);
        assert_eq!(age.null_count, 1);

        let email = profiles
            .iter()
            .find(|profile| profile.name == "email")
            .unwrap();
        assert_eq!(email.null_count, 1);
    }

    #[test]
    fn test_analyze_csv_file_returns_quality_report_legacy() {
        let csv = write_csv("x,y\n1,a\n2,b\n3,c\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.column_profiles.len(), 2);
        assert_eq!(report.execution.rows_processed, 3);
        assert_eq!(report.execution.columns_detected, 2);
        assert!(report.quality_score().unwrap() >= 0.0);
        assert!(report.quality_score().unwrap() <= 100.0);
    }

    #[test]
    fn test_analyze_csv_file_empty_file() {
        let csv = write_csv("name,age\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.column_profiles.len(), 0);
        assert_eq!(report.execution.rows_processed, 0);
    }

    #[test]
    fn test_analyze_csv_with_max_rows_small_file() {
        let csv = write_csv("val\n1\n2\n3\n4\n5\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        assert_eq!(report.execution.rows_processed, 5);
        assert_eq!(report.execution.columns_detected, 1);
        assert!((report.execution.sampling_ratio.unwrap_or(1.0) - 1.0).abs() < 0.01);
    }

    #[test]
    fn test_analyze_csv_numeric_types_detected() {
        let csv = write_csv("int_col,float_col,str_col\n1,1.5,hello\n2,2.5,world\n3,3.5,foo\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        let profiles = &report.column_profiles;

        let int_col = profiles
            .iter()
            .find(|profile| profile.name == "int_col")
            .unwrap();
        assert!(matches!(int_col.data_type, DataType::Integer));

        let float_col = profiles
            .iter()
            .find(|profile| profile.name == "float_col")
            .unwrap();
        assert!(matches!(float_col.data_type, DataType::Float));

        let str_col = profiles
            .iter()
            .find(|profile| profile.name == "str_col")
            .unwrap();
        assert!(matches!(str_col.data_type, DataType::String));
    }

    #[test]
    fn test_analyze_csv_strict_rejects_ragged_rows() {
        let csv = write_csv("a,b\n1,2\n3,4,5\n");
        let config = CsvParserConfig::strict();
        assert!(analyze_csv_file(csv.path(), &config).is_err());
    }

    #[test]
    fn test_analyze_csv_file_handles_ragged_rows() {
        let csv = write_csv("a,b\n1,2\n3,4,5\n6\n");
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();
        assert!(!report.column_profiles.is_empty());
    }

    #[test]
    fn test_analyze_csv_with_quality_metrics() {
        let csv = write_csv(
            "name,age\n\
             Alice,25\nBob,\nCharlie,30\nDave,\nEve,28\n\
             Frank,\nGrace,35\nHeidi,40\nIvan,\nJudy,22\n",
        );
        let config = CsvParserConfig::default();
        let report = analyze_csv_file(csv.path(), &config).unwrap();

        let age = report
            .column_profiles
            .iter()
            .find(|profile| profile.name == "age")
            .unwrap();
        assert_eq!(age.null_count, 4);
        assert_eq!(age.total_count, 10);
        assert!(report.quality_score().unwrap() >= 0.0);
    }

    #[test]
    fn test_detect_delimiter_comma() {
        let data = b"name,age,city\nAlice,25,NYC\nBob,30,London\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
    }

    #[test]
    fn test_detect_delimiter_semicolon() {
        let data = b"name;age;city\nAlice;25;NYC\nBob;30;London\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b';');
    }

    #[test]
    fn test_detect_delimiter_pipe() {
        let data = b"name|age|city\nAlice|25|NYC\nBob|30|London\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b'|');
    }

    #[test]
    fn test_detect_delimiter_tab() {
        let data = b"name\tage\tcity\nAlice\t25\tNYC\nBob\t30\tLondon\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b'\t');
    }

    #[test]
    fn test_detect_delimiter_from_path_semicolon() {
        let csv = write_csv("id;name;salary\n1;Alice;50000\n2;Bob;60000\n");
        assert_eq!(detect_delimiter_from_path(csv.path()).unwrap(), b';');
    }

    #[test]
    fn test_detect_delimiter_quoted_fields() {
        let data = b"name;desc;val\n\"hello;world\";foo;1\nbar;baz;2\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b';');
    }
}
