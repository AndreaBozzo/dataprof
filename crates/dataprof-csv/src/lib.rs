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
pub(crate) fn count_fields(line: &str, delimiter: char) -> usize {
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

/// Split raw CSV sample text into logical records, honoring quoted sections
/// so an embedded newline inside quotes does not start a new record.
///
/// The returned flag is true when the final record was not terminated by a
/// newline outside quotes — i.e. the text ends mid-record (or exactly at a
/// record's end with no trailing newline, which a caller sampling a fixed
/// byte budget cannot distinguish from a cut).
fn split_sample_records(text: &str) -> (Vec<&str>, bool) {
    let bytes = text.as_bytes();
    let mut records = Vec::new();
    let mut in_quotes = false;
    let mut start = 0usize;
    let mut i = 0usize;
    while i < bytes.len() {
        match bytes[i] {
            b'"' => {
                if in_quotes && bytes.get(i + 1) == Some(&b'"') {
                    i += 1; // escaped quote inside a quoted section
                } else {
                    in_quotes = !in_quotes;
                }
            }
            b'\n' if !in_quotes => {
                let end = if i > start && bytes[i - 1] == b'\r' {
                    i - 1
                } else {
                    i
                };
                records.push(&text[start..end]);
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    let last_unterminated = start < text.len();
    if last_unterminated {
        records.push(text[start..].trim_end_matches('\r'));
    }
    (records, last_unterminated)
}

/// Detect the most likely CSV delimiter by sampling up to 4 KB of data.
///
/// The sample is split into logical records with quote handling, so quoted
/// fields containing embedded newlines or delimiter characters do not skew
/// detection. Tests comma, semicolon, tab, and pipe over the first 5 records:
/// a delimiter whose modal field count is greater than one always beats one
/// that only ever yields single-field records, then higher record agreement
/// and higher field counts win.
///
/// Falls back to comma if no candidate splits the sample.
pub fn detect_delimiter<R: Read>(reader: R) -> std::io::Result<u8> {
    const SAMPLE_BYTES: u64 = 4096;
    let mut preamble = Vec::new();
    let mut take = reader.take(SAMPLE_BYTES);
    take.read_to_end(&mut preamble)?;
    let truncated = preamble.len() as u64 == SAMPLE_BYTES;

    let preamble_str = String::from_utf8_lossy(&preamble);
    let (mut records, last_unterminated) = split_sample_records(&preamble_str);
    // A full sample buffer that ends mid-record leaves a fragment; never score
    // it. A sample ending on a clean newline keeps all its records.
    if truncated && last_unterminated && records.len() > 1 {
        records.pop();
    }

    let delimiters = *b",;\t|";
    let mut best_delimiter = b',';
    // (splits records into >1 field, records agreeing on the modal count,
    //  modal field count) — compared lexicographically, first wins ties.
    let mut best_score = (false, 0usize, 0usize);

    for &delimiter in &delimiters {
        let mut counts = std::collections::HashMap::new();
        for record in records.iter().take(5) {
            let fields = count_fields(record, delimiter as char);
            *counts.entry(fields).or_insert(0usize) += 1;
        }

        // decode-audit: no-data — an empty sample scores 0 for this delimiter
        // candidate; nothing is fabricated.
        let (modal_fields, consistency) = counts
            .into_iter()
            .max_by_key(|&(fields, count)| (count, fields))
            .unwrap_or((0, 0));

        let score = (modal_fields > 1, consistency, modal_fields);
        if score > best_score {
            best_score = score;
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
        let names: Vec<String> = headers.iter().map(|header| header.to_string()).collect();
        // Reject duplicate headers before profiling: process_record keys on name,
        // so a repeat would merge two source columns into one inflated profile.
        dataprof_core::validate_unique_column_names(&names, "CSV header")?;
        names
    } else {
        let headers = csv_reader.headers()?;
        (0..headers.len())
            .map(|index| format!("column_{index}"))
            .collect()
    };

    let mut column_stats = StreamingColumnCollection::new().with_semantic_hints(semantic_hints);
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
            ExecutionMetadata::new(0, header_names.len(), start.elapsed().as_millis())
                .with_engine("csv"),
        )
        .skip_quality()
        .build());
    }

    let sample_columns = profile_builder::quality_check_samples(&column_stats);
    let scan_time_ms = start.elapsed().as_millis();
    let num_columns = column_profiles.len();

    let mut assembler = ReportAssembler::new(
        file_source,
        ExecutionMetadata::new(rows_read, num_columns, scan_time_ms).with_engine("csv"),
    )
    .columns(column_profiles)
    .with_quality_data(sample_columns)
    .with_row_duplicates(column_stats.row_duplicate_summary())
    .with_exact_value_hint_bindings(column_stats.semantic_hint_bindings())
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

    #[test]
    fn test_detect_delimiter_quoted_embedded_newline() {
        // A quoted cell containing a newline must not break record splitting:
        // before the quote-aware sniffer this file was detected as semicolon
        // (zero occurrences, "perfectly consistent" single-field records) and
        // profiled as a single column named "id,comment".
        let data = b"id,comment\n1,\"line one\nline two\"\n2,\"normal\"\n3,\"has \"\"quotes\"\" inside\"\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
    }

    #[test]
    fn test_detect_delimiter_absent_candidate_never_beats_real_one() {
        // One ragged record makes comma imperfect (consistency 3/4); an absent
        // delimiter is uniformly single-field (4/4) and must still lose.
        let data = b"a,b,c\n1,2,3\nragged line without delimiters\n4,5,6\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
    }

    #[test]
    fn test_detect_delimiter_single_column_falls_back_to_comma() {
        let data = b"value\n1\n2\n3\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b',');
    }

    #[test]
    fn test_detect_delimiter_two_column_semicolon_with_commas_in_text() {
        // Free text containing commas must not outvote the true delimiter.
        let data = b"id;note\n1;hello, world\n2;foo, bar, baz\n3;plain\n";
        assert_eq!(detect_delimiter(Cursor::new(data.as_ref())).unwrap(), b';');
    }

    #[test]
    fn test_split_sample_records_quote_aware() {
        let (records, last_unterminated) = split_sample_records("a,b\r\n1,\"x\ny\"\n2,z\n");
        assert_eq!(records, vec!["a,b", "1,\"x\ny\"", "2,z"]);
        assert!(!last_unterminated);

        let (records, last_unterminated) = split_sample_records("a,b\n1,\"cut mid-fie");
        assert_eq!(records, vec!["a,b", "1,\"cut mid-fie"]);
        assert!(last_unterminated);
    }

    #[test]
    fn test_detect_delimiter_full_sample_ending_on_record_boundary() {
        // Exactly 4096 bytes ending with a clean newline: the final record is
        // complete and must be scored, not dropped as a fragment.
        let mut data = Vec::from(&b"a;b;c\n"[..]);
        while data.len() < 4090 {
            data.extend_from_slice(b"1;2;3\n");
        }
        while data.len() < 4095 {
            data.push(b'#');
        }
        data.push(b'\n');
        assert_eq!(data.len(), 4096);
        assert_eq!(
            detect_delimiter(Cursor::new(data.as_slice())).unwrap(),
            b';'
        );
    }
}
