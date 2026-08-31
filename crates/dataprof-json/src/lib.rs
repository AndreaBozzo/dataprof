//! JSON and JSONL scanning and profiling for `dataprof`.
//!
//! This crate is an implementation detail of the `dataprof` facade, which
//! re-exports [`JsonParserConfig`], [`analyze_json_from_reader`], and
//! [`analyze_json_file`]. Depend on `dataprof` unless you need JSON support
//! without the rest of the workspace.
//!
//! Two entry shapes are offered. [`scan_json_from_reader`] hands each record to
//! a callback and reports what it read; [`analyze_json_from_reader`] and
//! [`analyze_json_file`] build column profiles and a full report on top of it.
//!
//! ```
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! use std::io::Cursor;
//!
//! use dataprof_json::{JsonParserConfig, analyze_json_from_reader};
//!
//! let jsonl = "{\"id\": 1, \"city\": \"Rome\"}\n{\"id\": 2, \"city\": \"Milan\"}\n";
//! let (profiles, _stats, rows_read, malformed_lines, _format) =
//!     analyze_json_from_reader(Cursor::new(jsonl), &JsonParserConfig::jsonl())?;
//!
//! assert_eq!(rows_read, 2);
//! assert_eq!(malformed_lines, 0);
//! assert_eq!(profiles[0].name, "id");
//! # Ok(())
//! # }
//! ```

use std::collections::{HashMap, HashSet};
use std::io::BufRead;
use std::path::Path;

pub use dataprof_core::JsonErrorPolicy;
use dataprof_core::{
    AnalysisOptions, ColumnProfile, DataProfilerError, DataSource, ExecutionMetadata, FileFormat,
    QualityDimension, SemanticHints, TruncationReason, Utf8BomReader,
};
use dataprof_runtime::{
    ProfileReport, ReportAssembler, StreamingColumnCollection, profile_builder,
};
use serde::Deserialize;
use serde_json::Value;

/// Which grammar a JSON source is read with.
///
/// The two are deliberately distinct: whitespace is insignificant inside a
/// standard JSON document and load-bearing in JSONL, so one input cannot be
/// valid under both readings and mean the same thing. Callers pick with
/// `format="json"` / `format="jsonl"`, or by file extension.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JsonFormat {
    /// A standard JSON document, in either of the two shapes that carry
    /// records: an array of objects (`[{...}, {...}]`), or a single object
    /// (`{...}`) as one record. Values may span lines and be pretty-printed,
    /// and exactly one document must fill the whole input.
    Json,
    /// JSON Lines — one record per physical line. A record may not span lines,
    /// and a line may not hold more than one value.
    Jsonl,
}

/// Configuration for JSON/JSONL parsing and scanning.
///
/// The default detects the grammar from the input and reads every record,
/// skipping malformed ones. The two format constructors say which grammar to
/// read with, which is what a caller who knows should do: detection cannot tell
/// a JSON object document from a JSONL file, since both open with `{`.
///
/// # Examples
///
/// The same two records under each grammar:
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::io::Cursor;
///
/// use dataprof_json::{JsonParserConfig, scan_json_from_reader};
///
/// let document = r#"[{"id": 1}, {"id": 2}]"#;
/// let lines = "{\"id\": 1}\n{\"id\": 2}\n";
///
/// let from_document = scan_json_from_reader(
///     Cursor::new(document),
///     &JsonParserConfig::json_document(),
///     |_| {},
/// )?;
/// let from_lines =
///     scan_json_from_reader(Cursor::new(lines), &JsonParserConfig::jsonl(), |_| {})?;
///
/// assert_eq!(from_document.rows_read, 2);
/// assert_eq!(from_lines.rows_read, 2);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct JsonParserConfig {
    /// The grammar to read with. `None` detects it from the first byte, which
    /// resolves `[` to [`JsonFormat::Json`] and everything else to
    /// [`JsonFormat::Jsonl`] — a leading `{` opens both a JSON object document
    /// and a JSONL file, so callers that know which they have (from the file
    /// extension or an explicit `format=`) should say so rather than rely on
    /// detection.
    pub format: Option<JsonFormat>,
    /// Maximum rows to process (None = all rows).
    pub max_rows: Option<usize>,
    /// How to react to a malformed record (default: skip).
    pub error_policy: JsonErrorPolicy,
}

impl JsonParserConfig {
    /// Set the maximum number of rows to process.
    ///
    /// The cap counts profileable records, not lines: blank lines and records
    /// skipped as malformed do not consume it.
    ///
    /// A cap is only *truncation* when a record still remained once it was
    /// reached, so a source holding exactly `max_rows` records reads as
    /// complete. [`JsonScanSummary::truncated`] carries that distinction, and
    /// [`analyze_json_file`] turns it into a [`TruncationReason::MaxRows`] on
    /// the report.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::io::Cursor;
    ///
    /// use dataprof_json::{JsonParserConfig, scan_json_from_reader};
    ///
    /// let lines = "{\"id\": 1}\n{\"id\": 2}\n{\"id\": 3}\n";
    ///
    /// // Two of three records: a third remained, so the scan was cut short.
    /// let capped = JsonParserConfig::jsonl().with_max_rows(2);
    /// let summary = scan_json_from_reader(Cursor::new(lines), &capped, |_| {})?;
    /// assert_eq!(summary.rows_read, 2);
    /// assert!(summary.truncated);
    ///
    /// // A cap the source never reaches past is a complete read, not a
    /// // truncated one.
    /// let exact = JsonParserConfig::jsonl().with_max_rows(3);
    /// let summary = scan_json_from_reader(Cursor::new(lines), &exact, |_| {})?;
    /// assert_eq!(summary.rows_read, 3);
    /// assert!(!summary.truncated);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_max_rows(mut self, max_rows: usize) -> Self {
        self.max_rows = Some(max_rows);
        self
    }

    /// Set how malformed records are handled.
    ///
    /// Under the default [`JsonErrorPolicy::Skip`] a malformed record is
    /// counted in [`JsonScanSummary::malformed_lines`] and scanning continues,
    /// so a partial profile never looks like a clean one. Under
    /// [`JsonErrorPolicy::Strict`] the first one fails the scan.
    ///
    /// # Examples
    ///
    /// ```
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::io::Cursor;
    ///
    /// use dataprof_json::{JsonErrorPolicy, JsonParserConfig, scan_json_from_reader};
    ///
    /// let lines = "{\"id\": 1}\nnot json\n{\"id\": 2}\n";
    ///
    /// let skipping = JsonParserConfig::jsonl();
    /// let summary = scan_json_from_reader(Cursor::new(lines), &skipping, |_| {})?;
    /// assert_eq!(summary.rows_read, 2);
    /// assert_eq!(summary.malformed_lines, 1);
    ///
    /// let strict = JsonParserConfig::jsonl().with_error_policy(JsonErrorPolicy::Strict);
    /// assert!(scan_json_from_reader(Cursor::new(lines), &strict, |_| {}).is_err());
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_error_policy(mut self, policy: JsonErrorPolicy) -> Self {
        self.error_policy = policy;
        self
    }

    /// Read as JSON Lines: one record per physical line.
    pub fn jsonl() -> Self {
        Self {
            format: Some(JsonFormat::Jsonl),
            ..Default::default()
        }
    }

    /// Read as a standard JSON document: an array of records, or a single
    /// object as one record.
    pub fn json_document() -> Self {
        Self {
            format: Some(JsonFormat::Json),
            ..Default::default()
        }
    }

    /// Set the grammar explicitly.
    pub fn with_format(mut self, format: JsonFormat) -> Self {
        self.format = Some(format);
        self
    }
}

/// Borrowed JSON object callback payload.
///
/// The workspace enables `serde_json/preserve_order`, so iterating this map
/// yields fields in the order they appear in the source document rather than
/// alphabetically. That is what makes the column-ordering contract on
/// [`analyze_json_from_reader`] possible.
pub type JsonObject = serde_json::Map<String, Value>;

/// Summary of a JSON/JSONL scan.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct JsonScanSummary {
    pub rows_read: usize,
    /// Records that were read but not profiled: malformed JSON, and valid JSON
    /// values that are not objects and therefore carry no fields. Both are
    /// counted so a tolerant scan never looks like a clean one.
    pub malformed_lines: usize,
    pub format: FileFormat,
    /// The scan stopped at `max_rows` while records still remained. A source
    /// holding exactly `max_rows` records was read in full, so this stays false.
    pub truncated: bool,
}

/// Scan JSON or JSONL input and invoke `on_object` for each object record.
///
/// - **JSONL**: one record per physical line. A record may not span lines and a
///   line may not hold more than one value, so a concatenation whose delimiter
///   was lost is reported rather than read as several clean records. Blank lines
///   are separators: neither records nor errors.
/// - **JSON**: one standard document. An array of objects streams element by
///   element without buffering the whole input; a single object is read whole
///   and is one record, which is why it may be pretty-printed across lines.
///
/// # Record policy
///
/// Only JSON objects are profileable records — they are the only JSON value
/// with named fields to turn into columns. A record that is valid JSON but not
/// an object (a scalar, an array, `null`) is never silently discarded: under
/// [`JsonErrorPolicy::Skip`] it is counted in
/// [`JsonScanSummary::malformed_lines`] and scanning continues with the next
/// record, and under [`JsonErrorPolicy::Strict`] the first one fails the scan.
///
/// # Examples
///
/// Reading JSONL, keeping one field per record:
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::io::Cursor;
///
/// use dataprof_json::{JsonParserConfig, scan_json_from_reader};
///
/// let lines = "{\"city\": \"Rome\", \"pop\": 2800000}\n\
///              {\"city\": \"Milan\", \"pop\": 1400000}\n";
///
/// let mut cities = Vec::new();
/// let summary = scan_json_from_reader(
///     Cursor::new(lines),
///     &JsonParserConfig::jsonl(),
///     |object| {
///         if let Some(city) = object.get("city").and_then(|value| value.as_str()) {
///             cities.push(city.to_string());
///         }
///     },
/// )?;
///
/// assert_eq!(cities, ["Rome", "Milan"]);
/// assert_eq!(summary.rows_read, 2);
/// assert!(!summary.truncated);
/// # Ok(())
/// # }
/// ```
///
/// The same records as a JSON array. The array streams element by element, and
/// a record may be pretty-printed across lines because whitespace carries no
/// meaning under this grammar:
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::io::Cursor;
///
/// use dataprof_json::{JsonParserConfig, scan_json_from_reader};
///
/// let document = r#"[
///     {"city": "Rome", "pop": 2800000},
///     {"city": "Milan", "pop": 1400000}
/// ]"#;
///
/// let mut populations = Vec::new();
/// let summary = scan_json_from_reader(
///     Cursor::new(document),
///     &JsonParserConfig::json_document(),
///     |object| {
///         // A field that is absent, or present but not a number, stays
///         // `None`. Reading it as 0 instead would turn a missing value into
///         // a real measurement, which is the one thing a profile must never
///         // do.
///         populations.push(object.get("pop").and_then(|value| value.as_u64()));
///     },
/// )?;
///
/// assert_eq!(populations, [Some(2_800_000), Some(1_400_000)]);
/// assert_eq!(summary.rows_read, 2);
/// assert_eq!(summary.malformed_lines, 0);
/// # Ok(())
/// # }
/// ```
pub fn scan_json_from_reader<R, F>(
    reader: R,
    config: &JsonParserConfig,
    mut on_object: F,
) -> Result<JsonScanSummary, DataProfilerError>
where
    R: BufRead,
    F: FnMut(&JsonObject),
{
    let mut reader = Utf8BomReader::new(reader).map_err(DataProfilerError::from)?;
    let format = match config.format {
        Some(JsonFormat::Json) => FileFormat::Json,
        Some(JsonFormat::Jsonl) => FileFormat::Jsonl,
        // A leading `{` is the first byte of both a JSON object document and a
        // JSONL file, so detection can only resolve the array case and has to
        // fall back to JSONL. Callers that know better pass `format`.
        None => match consume_leading_whitespace(&mut reader)? {
            Some(b'[') => FileFormat::Json,
            _ => FileFormat::Jsonl,
        },
    };

    let mut rows_read = 0;
    let mut malformed_lines = 0;
    let mut truncated = false;

    match format {
        // JSONL is line-delimited: exactly one record per non-blank physical
        // line. Reading it as a whitespace-delimited stream of values instead
        // made `{"a":1}{"b":2}` — a concatenation with the delimiter lost —
        // profile as two clean rows, which is precisely the corruption a
        // profiler exists to surface.
        FileFormat::Jsonl => {
            let mut line = String::new();
            let mut line_number = 0;
            loop {
                if let Some(max) = config.max_rows
                    && rows_read >= max
                {
                    // Reaching the cap is not truncation unless a record
                    // actually remains: a file with exactly `max_rows` records
                    // was read in full. Blank lines are not records.
                    truncated = next_record_line(&mut reader, &mut line, &mut line_number)?;
                    break;
                }

                if !next_record_line(&mut reader, &mut line, &mut line_number)? {
                    break;
                }

                match read_jsonl_line(&line) {
                    JsonRecord::Object(obj) => {
                        on_object(&obj);
                        rows_read += 1;
                    }
                    JsonRecord::NonObject(kind) => {
                        if config.error_policy == JsonErrorPolicy::Strict {
                            return Err(non_object_record_error(
                                kind,
                                rows_read + malformed_lines + 1,
                            ));
                        }
                        malformed_lines += 1;
                    }
                    JsonRecord::Malformed(err) => {
                        if config.error_policy == JsonErrorPolicy::Strict {
                            return Err(malformed_jsonl_record_error(line_number, err));
                        }
                        malformed_lines += 1;
                    }
                }
            }
        }
        FileFormat::Json => {
            // Which of the two record-carrying shapes this document is. The
            // first non-whitespace byte decides, and is left unconsumed so the
            // object branch still sees the value it opens.
            let found_array = consume_leading_whitespace(&mut reader)? == Some(b'[');

            if !found_array {
                // The other shape a standard JSON document takes: a single
                // object, which is one record. It may be pretty-printed across
                // lines, which is why this is not the JSONL grammar.
                let (object_rows, object_malformed, object_truncated) =
                    scan_single_json_object(&mut reader, config, &mut on_object)?;
                rows_read += object_rows;
                malformed_lines += object_malformed;
                truncated |= object_truncated;
            } else {
                // The array loop starts after the opening bracket.
                consume_peeked(&mut reader)?;

                let mut expect_value = true;
                let mut allow_end = true;
                let mut array_closed = false;
                let mut drain_remainder = false;

                loop {
                    let Some(next) = consume_leading_whitespace(&mut reader)? else {
                        if config.error_policy == JsonErrorPolicy::Strict {
                            return Err(malformed_array_error(
                                "unexpected end of input before closing ']'",
                            ));
                        }
                        malformed_lines += 1;
                        drain_remainder = true;
                        break;
                    };

                    if expect_value {
                        if next == b']' {
                            if !allow_end {
                                if config.error_policy == JsonErrorPolicy::Strict {
                                    return Err(malformed_array_error(
                                        "trailing comma before closing ']'",
                                    ));
                                }
                                malformed_lines += 1;
                                drain_remainder = true;
                                break;
                            }
                            consume_peeked(&mut reader)?;
                            array_closed = true;
                            break;
                        }

                        if let Some(max) = config.max_rows
                            && rows_read >= max
                        {
                            // A value remains unread. Do not validate the unread
                            // suffix of an intentionally bounded scan.
                            truncated = true;
                            break;
                        }

                        if next == b',' {
                            if config.error_policy == JsonErrorPolicy::Strict {
                                return Err(malformed_array_error(
                                    "unexpected comma where an array value was required",
                                ));
                            }
                            malformed_lines += 1;
                            drain_remainder = true;
                            break;
                        }

                        match read_json_record(&mut reader, next)? {
                            JsonRecord::Object(obj) => {
                                on_object(&obj);
                                rows_read += 1;
                            }
                            JsonRecord::NonObject(kind) => {
                                if config.error_policy == JsonErrorPolicy::Strict {
                                    return Err(non_object_record_error(
                                        kind,
                                        rows_read + malformed_lines + 1,
                                    ));
                                }
                                // The element was consumed in full, so the array
                                // grammar is still intact and the objects after
                                // it are still profileable.
                                malformed_lines += 1;
                            }
                            JsonRecord::Malformed(err) => {
                                if config.error_policy == JsonErrorPolicy::Strict {
                                    return Err(malformed_record_error(err));
                                }
                                malformed_lines += 1;
                                drain_remainder = true;
                                break;
                            }
                        }
                        expect_value = false;
                    } else {
                        match next {
                            b',' => {
                                consume_peeked(&mut reader)?;
                                expect_value = true;
                                allow_end = false;
                            }
                            b']' => {
                                consume_peeked(&mut reader)?;
                                array_closed = true;
                                break;
                            }
                            _ => {
                                if config.error_policy == JsonErrorPolicy::Strict {
                                    return Err(malformed_array_error(
                                        "expected ',' or ']' after an array value",
                                    ));
                                }
                                malformed_lines += 1;
                                drain_remainder = true;
                                break;
                            }
                        }
                    }
                }

                if drain_remainder {
                    drain_to_end(&mut reader)?;
                } else if array_closed && consume_leading_whitespace(&mut reader)?.is_some() {
                    if config.error_policy == JsonErrorPolicy::Strict {
                        return Err(malformed_array_error(
                            "non-whitespace content follows the closing ']'",
                        ));
                    }
                    malformed_lines += 1;
                    drain_to_end(&mut reader)?;
                }
            }
        }
        _ => unreachable!("json scanner only returns json or jsonl formats"),
    }

    Ok(JsonScanSummary {
        rows_read,
        malformed_lines,
        format,
        truncated,
    })
}

/// Read the next line that holds a record into `line`, skipping blank ones.
///
/// Returns whether a record line was found. Blank lines are separators, not
/// records: they are skipped without counting as rows or as errors. `line_number`
/// counts every physical line read, blank ones included, so a diagnostic points
/// at the line the user would open the file to.
fn next_record_line<R: BufRead>(
    reader: &mut R,
    line: &mut String,
    line_number: &mut usize,
) -> Result<bool, DataProfilerError> {
    loop {
        line.clear();
        let read = reader.read_line(line).map_err(DataProfilerError::from)?;
        if read == 0 {
            return Ok(false);
        }
        *line_number += 1;
        if !line.trim().is_empty() {
            return Ok(true);
        }
    }
}

/// Parse one JSONL line as exactly one JSON value.
///
/// `serde_json::from_str` requires the value to fill the whole input, so a line
/// carrying two adjacent or space-separated values is a malformed record rather
/// than two clean ones — the boundary rule that separates JSONL from a stream of
/// JSON values.
fn read_jsonl_line(line: &str) -> JsonRecord {
    match serde_json::from_str::<Value>(line.trim()) {
        Ok(Value::Object(obj)) => JsonRecord::Object(obj),
        Ok(value) => JsonRecord::NonObject(json_value_kind(&value)),
        Err(err) => JsonRecord::Malformed(err),
    }
}

/// Read a whole-input single JSON object as one record.
///
/// Returns `(rows_read, malformed, truncated)`. Anything that is not exactly one object
/// filling the input is an error under [`JsonErrorPolicy::Strict`] and a counted
/// malformed record otherwise, so a truncated or concatenated document never
/// profiles as clean.
fn scan_single_json_object<R, F>(
    reader: &mut R,
    config: &JsonParserConfig,
    on_object: &mut F,
) -> Result<(usize, usize, bool), DataProfilerError>
where
    R: BufRead,
    F: FnMut(&JsonObject),
{
    let mut text = String::new();
    reader
        .read_to_string(&mut text)
        .map_err(DataProfilerError::from)?;

    if text.trim().is_empty() {
        return Ok((0, 0, false));
    }

    // A row cap of zero asks for no records, so the document is not read. The
    // record it holds is left unread, which is a truncation and has to be
    // reported as one — an unread record and an empty source must not produce
    // the same report.
    if config.max_rows == Some(0) {
        return Ok((0, 0, true));
    }

    match serde_json::from_str::<Value>(text.trim()) {
        Ok(Value::Object(obj)) => {
            on_object(&obj);
            Ok((1, 0, false))
        }
        Ok(value) => {
            if config.error_policy == JsonErrorPolicy::Strict {
                return Err(non_object_record_error(json_value_kind(&value), 1));
            }
            Ok((0, 1, false))
        }
        Err(err) => {
            if config.error_policy == JsonErrorPolicy::Strict {
                return Err(json_document_error(err));
            }
            Ok((0, 1, false))
        }
    }
}

/// One record read from a JSON or JSONL source.
enum JsonRecord {
    /// A JSON object — the only value with named fields to profile as a row.
    Object(JsonObject),
    /// Valid JSON that is not an object, carrying the value's kind for the
    /// error message.
    NonObject(&'static str),
    /// The bytes are not valid JSON.
    Malformed(serde_json::Error),
}

/// Read one JSON value and leave the reader positioned immediately after it.
///
/// `first` is the value's first byte, already located by
/// [`consume_leading_whitespace`] but not yet consumed.
fn read_json_record<R: BufRead>(
    reader: &mut R,
    first: u8,
) -> Result<JsonRecord, DataProfilerError> {
    // A number is the only JSON value that is not self-delimiting: serde finds
    // its end by peeking one byte past it and then drops that byte along with
    // the deserializer. Inside an array that byte is the `,` or `]` the scanner
    // needs next, so numbers are read byte-wise instead.
    if first == b'-' || first.is_ascii_digit() {
        let token = read_number_token(reader)?;
        return Ok(match serde_json::from_str::<Value>(&token) {
            Ok(_) => JsonRecord::NonObject("number"),
            Err(err) => JsonRecord::Malformed(err),
        });
    }

    let mut deserializer = serde_json::Deserializer::from_reader(reader);
    Ok(match Value::deserialize(&mut deserializer) {
        Ok(Value::Object(obj)) => JsonRecord::Object(obj),
        Ok(value) => JsonRecord::NonObject(json_value_kind(&value)),
        Err(err) => JsonRecord::Malformed(err),
    })
}

/// Read the bytes that can spell a JSON number, stopping at the first byte that
/// cannot. The token is returned unvalidated — the caller decides whether it
/// actually parses, so `1.2.3` stays a malformed record rather than a number.
///
/// Leading whitespace is consumed first: [`consume_leading_whitespace`] reports
/// the next value's first byte without consuming what precedes it, because
/// serde needs that whitespace to keep its line and column context.
fn read_number_token<R: BufRead>(reader: &mut R) -> Result<String, DataProfilerError> {
    skip_ascii_whitespace(reader)?;
    let mut token = String::new();
    loop {
        let (consume, token_ended) = {
            let buf = reader.fill_buf().map_err(DataProfilerError::from)?;
            if buf.is_empty() {
                break;
            }
            let consume = buf
                .iter()
                .take_while(|byte| is_json_number_byte(**byte))
                .count();
            // decode-audit: impossible — every byte accepted above is ASCII.
            token.push_str(
                std::str::from_utf8(&buf[..consume]).expect("JSON number bytes are ASCII"),
            );
            (consume, consume < buf.len())
        };
        reader.consume(consume);
        if token_ended {
            break;
        }
    }
    Ok(token)
}

fn is_json_number_byte(byte: u8) -> bool {
    byte.is_ascii_digit() || matches!(byte, b'-' | b'+' | b'.' | b'e' | b'E')
}

/// Consume the byte [`consume_leading_whitespace`] just reported, along with the
/// whitespace it looked past.
///
/// That function only peeks — it leaves the whitespace queued so serde keeps its
/// line and column context — so consuming one byte would eat a space rather than
/// the delimiter. Getting this wrong made a pretty-printed JSON array report a
/// phantom malformed record, because the `]` survived the consume and then
/// looked like content after the closing bracket.
fn consume_peeked<R: BufRead>(reader: &mut R) -> Result<(), DataProfilerError> {
    skip_ascii_whitespace(reader)?;
    reader.consume(1);
    Ok(())
}

/// Advance the reader past any ASCII whitespace at its current position.
fn skip_ascii_whitespace<R: BufRead>(reader: &mut R) -> Result<(), DataProfilerError> {
    loop {
        let (consume, done) = {
            let buf = reader.fill_buf().map_err(DataProfilerError::from)?;
            if buf.is_empty() {
                return Ok(());
            }
            let consume = buf
                .iter()
                .take_while(|byte| byte.is_ascii_whitespace())
                .count();
            (consume, consume < buf.len())
        };
        reader.consume(consume);
        if done {
            return Ok(());
        }
    }
}

fn json_value_kind(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

/// Build a strict-mode error for valid JSON that is not a row object. This is a
/// distinct category from a syntax failure: the document parsed, it just does
/// not hold records. `position` is 1-based over the records scanned so far.
fn non_object_record_error(kind: &str, position: usize) -> DataProfilerError {
    // No source: the document parsed cleanly, it simply does not hold records,
    // so there is no decoder error underneath to retain.
    DataProfilerError::json_parsing_error(&format!(
        "non-object JSON record at position {position}: \
         expected an object with fields to profile, found {kind}"
    ))
}

/// Build a strict-mode parse error from a serde failure. The serde message
/// carries line/column context but never the record contents, so it is safe to
/// surface directly.
fn malformed_record_error(err: serde_json::Error) -> DataProfilerError {
    let message = format!("malformed JSON record: {err}");
    DataProfilerError::json_parsing_with_source(message, err)
}

/// Build a strict-mode parse error for a JSONL record, located by its physical
/// line.
///
/// Each line is parsed on its own, so the decoder's own line number is always 1
/// and would be misleading; the column it reports is within the line and so is
/// also the column in the file. The record's text is never included. Matches the
/// phrasing the Python bytes reader uses for the same failure.
fn malformed_jsonl_record_error(line: usize, err: serde_json::Error) -> DataProfilerError {
    let message = format!(
        "malformed JSON record on line {line}, column {}: a JSONL record must be one complete JSON value on one line",
        err.column()
    );
    DataProfilerError::json_parsing_with_source(message, err)
}

fn malformed_array_error(message: &str) -> DataProfilerError {
    // Callers here hold only a message, not the decoder error, so there is
    // nothing to retain.
    DataProfilerError::json_parsing_error(&format!("malformed JSON array: {message}"))
}

/// Build a strict-mode error for a standard JSON document that is not exactly
/// one value.
///
/// The most common cause is JSONL read with the JSON grammar — several objects
/// back to back parse as one value plus trailing characters — so the message
/// names the option that would read it correctly.
fn json_document_error(err: serde_json::Error) -> DataProfilerError {
    let message = format!(
        // One source line on purpose: rustfmt may join a `\`-continued
        // literal and leave its indentation inside the string, which silently
        // corrupted this very message once.
        "malformed JSON document: {err}. A JSON source must hold exactly one array or object; for one record per line use format=\"jsonl\""
    );
    DataProfilerError::json_parsing_with_source(message, err)
}

fn drain_to_end<R: BufRead>(reader: &mut R) -> Result<(), DataProfilerError> {
    loop {
        let available = reader.fill_buf().map_err(DataProfilerError::from)?.len();
        if available == 0 {
            return Ok(());
        }
        reader.consume(available);
    }
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

/// Convert a JSON [`Value`] to a flat string for column storage.
fn json_value_to_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(string) => string.to_string(),
        // decode-audit: impossible — serializing an in-memory Value back to a
        // string cannot fail; a panic here beats folding "" (= null) into stats.
        Value::Array(_) | Value::Object(_) => {
            serde_json::to_string(value).expect("re-serializing a parsed JSON value cannot fail")
        }
    }
}

/// Feed a JSON object's fields into a [`StreamingColumnCollection`].
///
/// Columns are registered in the order the fields appear in each record, so the
/// resulting column order is the first record's field order with later-only
/// fields appended where they were first seen.
fn feed_json_object(
    obj: &JsonObject,
    prior_rows: usize,
    known_columns: &mut Vec<String>,
    known_columns_set: &mut HashSet<String>,
    column_stats: &mut StreamingColumnCollection,
) {
    for key in obj.keys() {
        if known_columns_set.insert(key.clone()) {
            known_columns.push(key.clone());
            column_stats.init_column_with_missing(key, prior_rows);
        }
    }

    let values: Vec<String> = known_columns
        .iter()
        .map(|column| {
            // decode-audit: no-data — a key absent from this object is a
            // missing field, and "" is the profiler's textual null.
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
///
/// # Column order
///
/// Columns are returned in source order — the first record's field order, with
/// fields that only appear in later records appended in first-seen order. This
/// matches CSV (header order) and Parquet (schema order), so the same logical
/// dataset profiles to the same column order in every format.
///
/// # Examples
///
/// The second record lists its fields in a different order and adds one. The
/// first record still decides the order, and the new field is appended:
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::io::Cursor;
///
/// use dataprof_json::{JsonParserConfig, analyze_json_from_reader};
///
/// let lines = "{\"id\": 1, \"city\": \"Rome\"}\n\
///              {\"city\": \"Milan\", \"id\": 2, \"region\": \"Lombardy\"}\n";
///
/// let (profiles, _stats, rows_read, malformed_lines, format) =
///     analyze_json_from_reader(Cursor::new(lines), &JsonParserConfig::jsonl())?;
///
/// let names: Vec<&str> = profiles.iter().map(|profile| profile.name.as_str()).collect();
/// assert_eq!(names, ["id", "city", "region"]);
/// assert_eq!(rows_read, 2);
/// assert_eq!(malformed_lines, 0);
/// assert_eq!(format, dataprof_core::FileFormat::Jsonl);
///
/// // The first record has no `region`, which counts as a missing value.
/// let region = &profiles[2];
/// assert_eq!(region.total_count, 2);
/// assert_eq!(region.null_count, 1);
/// # Ok(())
/// # }
/// ```
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
    let options = AnalysisOptions::default().with_semantic_hints(semantic_hints.clone());
    let (profiles, stats, rows_read, malformed_lines, format, _truncated) =
        analyze_json_from_reader_full(reader, config, &options)?;
    Ok((profiles, stats, rows_read, malformed_lines, format))
}

/// As [`analyze_json_from_reader_with_hints`], but also reports whether the scan
/// stopped early. Kept private so the public tuple stays stable.
#[allow(clippy::type_complexity)]
fn analyze_json_from_reader_full<R: BufRead>(
    reader: R,
    config: &JsonParserConfig,
    options: &AnalysisOptions,
) -> Result<
    (
        Vec<ColumnProfile>,
        StreamingColumnCollection,
        usize,
        usize,
        FileFormat,
        bool,
    ),
    DataProfilerError,
> {
    let semantic_hints = options.semantic_hints();
    let mut column_stats = StreamingColumnCollection::new().with_semantic_hints(semantic_hints);
    let mut known_columns = Vec::new();
    let mut known_columns_set = HashSet::new();
    let mut rows_seen = 0;

    let summary = scan_json_from_reader(reader, config, |obj| {
        feed_json_object(
            obj,
            rows_seen,
            &mut known_columns,
            &mut known_columns_set,
            &mut column_stats,
        );
        rows_seen += 1;
    })?;

    if let Some(indices) = options.column_indices(&column_stats.column_names())? {
        let available = column_stats.column_names();
        let selected = indices
            .into_iter()
            .map(|index| available[index].clone())
            .collect::<Vec<_>>();
        column_stats.retain_columns(&selected);
    }

    let profiles = profile_builder::profiles_from_streaming_with_hints(
        &column_stats,
        !options.include_statistics(),
        !options.include_patterns(),
        options.locale(),
        semantic_hints,
    );

    Ok((
        profiles,
        column_stats,
        summary.rows_read,
        summary.malformed_lines,
        summary.format,
        summary.truncated,
    ))
}

/// Analyze a JSON or JSONL file, returning a full [`ProfileReport`].
///
/// With no explicit [`JsonParserConfig::format`] the file extension decides:
/// `.json` reads as a standard document, `.jsonl` and `.ndjson` as JSON Lines,
/// and any other name falls back to sniffing the first byte.
///
/// # Examples
///
/// ```
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use std::io::Write;
///
/// use dataprof_json::{JsonParserConfig, analyze_json_file};
///
/// let mut file = tempfile::NamedTempFile::with_suffix(".jsonl")?;
/// writeln!(file, "{{\"id\": 1, \"score\": 9.5}}")?;
/// writeln!(file, "{{\"id\": 2, \"score\": 7.0}}")?;
/// file.flush()?;
///
/// let report = analyze_json_file(file.path(), &JsonParserConfig::default())?;
///
/// assert_eq!(report.execution.rows_processed, 2);
/// assert_eq!(report.execution.columns_detected, 2);
/// assert!(report.execution.truncation_reason.is_none());
/// # Ok(())
/// # }
/// ```
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
    let options = AnalysisOptions::default()
        .with_quality_dimensions(quality_dimensions.map(<[_]>::to_vec))
        .with_semantic_hints(semantic_hints.clone());
    analyze_json_file_with_options(file_path, config, &options)
}

/// Analyze a JSON or JSONL file, honouring the caller's full analysis selection.
///
/// This is the entry point that carries metric packs and locale as well as
/// dimensions and hints, so a JSON profile reports exactly the analysis the
/// caller asked for — the same selection the CSV engines apply.
pub fn analyze_json_file_with_options(
    file_path: &Path,
    config: &JsonParserConfig,
    options: &AnalysisOptions,
) -> Result<ProfileReport, DataProfilerError> {
    // An explicit grammar wins; otherwise the file name gets a say before
    // content sniffing, which cannot tell a JSON object from a JSONL stream.
    let mut config = config.clone();
    if config.format.is_none() {
        config.format = grammar_for_extension(file_path);
    }
    let config = &config;
    let metadata = std::fs::metadata(file_path).map_err(|error| map_io_error(file_path, error))?;
    let start = std::time::Instant::now();

    let file = std::fs::File::open(file_path).map_err(|error| map_io_error(file_path, error))?;
    let buf_reader = std::io::BufReader::new(file);

    let (column_profiles, column_stats, rows_read, malformed_lines, format, truncated) =
        analyze_json_from_reader_full(buf_reader, config, options)?;
    let read_as_json_document = format == FileFormat::Json;

    let file_source = DataSource::File {
        path: file_path.display().to_string(),
        format,
        size_bytes: metadata.len(),
        modified_at: None,
        parquet_metadata: None,
    };

    if rows_read == 0 {
        if malformed_lines > 0 {
            // Nothing parsed under the JSON grammar most often means the source
            // is JSONL, so say so here too: under the default skip policy this
            // is the only error the caller sees.
            let hint = if read_as_json_document {
                ". A JSON source must hold exactly one array or object; for one record per line use format=\"jsonl\""
            } else {
                ""
            };
            // Aggregate over many discarded records: no single decoder error
            // is the cause, so none is retained.
            return Err(DataProfilerError::json_parsing_error(&format!(
                "No valid JSON records found in file \
                 (every record was malformed or not a JSON object){hint}"
            )));
        }
        // A row cap that stopped the scan before the first record still has to
        // be disclosed. Without this the report reads exactly like a complete
        // profile of an empty source, which is the one thing a truncated scan
        // must never be mistaken for.
        let mut execution =
            ExecutionMetadata::new(0, 0, start.elapsed().as_millis()).with_engine("json");
        if truncated && let Some(max) = config.max_rows {
            execution = execution.with_truncation(TruncationReason::MaxRows(max as u64));
        }
        return Ok(ReportAssembler::new(file_source, execution)
            .columns(column_profiles)
            .with_quality_data(HashMap::new())
            .with_analysis_options(options)
            .build());
    }

    let sample_columns = profile_builder::quality_check_samples(&column_stats);
    let scan_time_ms = start.elapsed().as_millis();
    let num_columns = column_profiles.len();

    let mut execution = ExecutionMetadata::new(rows_read, num_columns, scan_time_ms)
        .with_engine("json")
        // Tolerant scans surface skipped malformed records here so callers can
        // tell a partial profile from a clean one.
        .with_error_count(malformed_lines);
    // `truncated` is set only when a record still remained at the cap, so a source
    // holding exactly `max_rows` records is reported as fully read, not cut short.
    if truncated && let Some(max) = config.max_rows {
        execution = execution.with_truncation(TruncationReason::MaxRows(max as u64));
    }

    Ok(ReportAssembler::new(file_source, execution)
        .columns(column_profiles)
        .with_quality_data(sample_columns)
        .with_row_duplicates(column_stats.row_duplicate_summary())
        .with_row_completeness(column_stats.row_completeness_summary())
        .with_exact_value_hint_bindings(column_stats.semantic_hint_bindings())
        .with_analysis_options(options)
        .build())
}

/// The grammar a file name advertises, if it advertises one.
///
/// Content sniffing resolves a leading `[` on its own, but not a leading `{`:
/// that is the first byte of both a JSON object document and a JSONL file. The
/// extension is the evidence that separates them, and a `.json` file holding one
/// pretty-printed object is an ordinary thing to profile. `None` means the name
/// says nothing and the content decides.
fn grammar_for_extension(file_path: &Path) -> Option<JsonFormat> {
    let extension = file_path.extension().and_then(|ext| ext.to_str())?;
    if extension.eq_ignore_ascii_case("json") {
        Some(JsonFormat::Json)
    } else if extension.eq_ignore_ascii_case("jsonl") || extension.eq_ignore_ascii_case("ndjson") {
        Some(JsonFormat::Jsonl)
    } else {
        None
    }
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

    /// A temp file with no extension, so `analyze_json_file` falls back to the
    /// JSONL grammar the way an unnamed stream does.
    fn write_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        write!(file, "{}", content).unwrap();
        file.flush().unwrap();
        file
    }

    /// A temp file named `.json`, so `analyze_json_file` reads it as a standard
    /// JSON document — the grammar the extension advertises.
    fn write_json_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::with_suffix(".json").unwrap();
        write!(file, "{}", content).unwrap();
        file.flush().unwrap();
        file
    }

    fn write_bytes(content: &[u8]) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content).unwrap();
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
    fn test_scan_accepts_one_leading_utf8_bom_for_json_and_jsonl() {
        for (data, config, expected_format) in [
            (
                b"\xEF\xBB\xBF[{\"x\":1},{\"x\":2}]".as_slice(),
                JsonParserConfig::default(),
                FileFormat::Json,
            ),
            (
                b"\xEF\xBB\xBF{\"x\":1}\n{\"x\":2}\n".as_slice(),
                JsonParserConfig::default(),
                FileFormat::Jsonl,
            ),
        ] {
            let mut rows = 0;
            let summary = scan_json_from_reader(Cursor::new(data), &config, |_| rows += 1)
                .expect("leading UTF-8 BOM should be accepted");

            assert_eq!(rows, 2);
            assert_eq!(summary.rows_read, 2);
            assert_eq!(summary.malformed_lines, 0);
            assert_eq!(summary.format, expected_format);
        }
    }

    #[test]
    fn test_scan_does_not_strip_nonleading_or_second_utf8_bom() {
        let strict = JsonParserConfig::jsonl().with_error_policy(JsonErrorPolicy::Strict);

        for data in [
            b" \xEF\xBB\xBF{\"x\":1}\n".as_slice(),
            b"\xEF\xBB\xBF\xEF\xBB\xBF{\"x\":1}\n".as_slice(),
            b"{\"x\":1}\n\xEF\xBB\xBF{\"x\":2}\n".as_slice(),
        ] {
            scan_json_from_reader(Cursor::new(data), &strict, |_| {})
                .expect_err("only the BOM at absolute byte offset zero may be stripped");
        }
    }

    #[test]
    fn test_bom_only_input_matches_empty_input_policy() {
        for config in [JsonParserConfig::default(), JsonParserConfig::jsonl()] {
            let empty = scan_json_from_reader(Cursor::new(b""), &config, |_| {}).unwrap();
            let bom =
                scan_json_from_reader(Cursor::new(b"\xEF\xBB\xBF".as_slice()), &config, |_| {})
                    .unwrap();
            assert_eq!(bom, empty);
        }
    }

    #[test]
    fn test_json_array_container_grammar_obeys_error_policy() {
        let cases = [
            (br#"[{"x":1}"#.as_ref(), 1, "missing closing bracket"),
            (br#"[{"x":1},"#.as_ref(), 1, "comma followed by EOF"),
            (br#"[{"x":1} {"x":2}]"#.as_ref(), 1, "missing comma"),
            (br#"[{"x":1},,{"x":2}]"#.as_ref(), 1, "doubled comma"),
            (br#"[,{"x":1}]"#.as_ref(), 0, "leading comma"),
            (br#"[{"x":1},]"#.as_ref(), 1, "trailing comma"),
            (br#"[{"x":1}] trailing"#.as_ref(), 1, "trailing content"),
            (
                br#"[{"x":1}][{"x":2}]"#.as_ref(),
                1,
                "second top-level value",
            ),
        ];

        for (data, expected_rows, case) in cases {
            let skip = JsonParserConfig::json_document();
            let summary =
                scan_json_from_reader(Cursor::new(data), &skip, |_| {}).expect("tolerant scan");
            assert_eq!(summary.rows_read, expected_rows, "{case}");
            assert_eq!(summary.malformed_lines, 1, "{case}");

            let strict =
                JsonParserConfig::json_document().with_error_policy(JsonErrorPolicy::Strict);
            let err = scan_json_from_reader(Cursor::new(data), &strict, |_| {})
                .expect_err("strict mode must reject invalid array grammar");
            assert!(
                err.to_string()
                    .to_lowercase()
                    .contains("malformed json array"),
                "{case}: {err}"
            );
        }
    }

    #[test]
    fn test_json_array_max_rows_does_not_validate_unread_values() {
        let data = br#"[{"x":1},not-valid-json"#;
        let config = JsonParserConfig::json_document().with_max_rows(1);
        let summary = scan_json_from_reader(Cursor::new(data.as_ref()), &config, |_| {}).unwrap();

        assert_eq!(summary.rows_read, 1);
        assert_eq!(summary.malformed_lines, 0);
        assert!(summary.truncated);
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

    /// A pretty-printed root object is one record under the JSON grammar and a
    /// malformed record under JSONL (#486). It cannot be both: whitespace is
    /// insignificant in a JSON document and is the record delimiter in JSONL, so
    /// reading a multi-line object as JSONL would mean each of its lines is a
    /// record — none of which is valid JSON on its own.
    #[test]
    fn test_pretty_printed_root_object_is_one_row_under_the_json_grammar() {
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
            &JsonParserConfig::json_document(),
            |obj| object_field_counts.push(obj.len()),
        )
        .unwrap();

        assert_eq!(summary.format, FileFormat::Json);
        assert_eq!(summary.rows_read, 1);
        assert_eq!(summary.malformed_lines, 0);
        assert_eq!(object_field_counts, vec![2]);
    }

    #[test]
    fn test_pretty_printed_root_object_is_not_valid_jsonl() {
        let data = br#"{
    "type": "FeatureCollection"
}"#;
        let err = scan_json_from_reader(
            Cursor::new(data.as_ref()),
            &JsonParserConfig::jsonl().with_error_policy(JsonErrorPolicy::Strict),
            |_| {},
        )
        .expect_err("a record spanning lines is not JSONL");

        assert!(
            err.to_string().contains("malformed JSON record"),
            "{err}, expected the malformed category"
        );
    }

    /// Every JSON value that is not an object, so none of them can be a row.
    /// Numbers are listed twice on purpose: they are the one value serde reads
    /// from a reader by peeking one byte past the end.
    const NON_OBJECT_VALUES: &[(&str, &str)] = &[
        ("null", "null"),
        ("boolean", "true"),
        ("number", "42"),
        ("number", "-1.5e3"),
        ("string", r#""text""#),
        ("array", "[1, 2]"),
    ];

    #[test]
    fn test_jsonl_counts_non_object_records_and_keeps_scanning() {
        for (kind, value) in NON_OBJECT_VALUES {
            let data = format!("{{\"id\":1}}\n{value}\n{{\"id\":2}}\n");
            let mut rows = 0;

            let summary = scan_json_from_reader(
                Cursor::new(data.as_bytes()),
                &JsonParserConfig::jsonl(),
                |_| rows += 1,
            )
            .unwrap();

            assert_eq!(summary.rows_read, 2, "{kind}: record after it was lost");
            assert_eq!(summary.malformed_lines, 1, "{kind}: was silently dropped");
            assert_eq!(rows, 2, "{kind}");
        }
    }

    #[test]
    fn test_json_array_counts_non_object_elements_and_keeps_scanning() {
        for (kind, value) in NON_OBJECT_VALUES {
            let data = format!("[{{\"id\":1}}, {value}, {{\"id\":2}}]");
            let mut rows = 0;

            let summary = scan_json_from_reader(
                Cursor::new(data.as_bytes()),
                &JsonParserConfig::json_document(),
                |_| rows += 1,
            )
            .unwrap();

            assert_eq!(summary.rows_read, 2, "{kind}: element after it was lost");
            assert_eq!(summary.malformed_lines, 1, "{kind}: was silently dropped");
            assert_eq!(rows, 2, "{kind}");
        }
    }

    #[test]
    fn test_strict_policy_rejects_the_first_non_object_record() {
        for (kind, value) in NON_OBJECT_VALUES {
            for (config, data) in [
                (
                    JsonParserConfig::jsonl(),
                    format!("{{\"id\":1}}\n{value}\n{{\"id\":2}}\n"),
                ),
                (
                    JsonParserConfig::json_document(),
                    format!("[{{\"id\":1}}, {value}, {{\"id\":2}}]"),
                ),
            ] {
                let config = config.with_error_policy(JsonErrorPolicy::Strict);
                let err = scan_json_from_reader(Cursor::new(data.as_bytes()), &config, |_| {})
                    .expect_err("{kind}: strict mode must reject a non-object record");

                let message = err.to_string();
                assert!(message.contains("non-object JSON record"), "{message}");
                assert!(message.contains("at position 2"), "{message}");
                assert!(message.contains(&format!("found {kind}")), "{message}");
            }
        }
    }

    #[test]
    fn test_number_element_does_not_swallow_the_array_delimiter() {
        // serde ends a number by peeking one byte past it and drops that byte
        // with the deserializer; here it is the `,` the array scanner needs.
        let data = br#"[{"id":1}, 1, 2, 3, {"id":2}, 4, {"id":3}]"#;
        let mut rows = 0;

        let summary = scan_json_from_reader(
            Cursor::new(data.as_ref()),
            &JsonParserConfig::json_document(),
            |_| rows += 1,
        )
        .unwrap();

        assert_eq!(summary.rows_read, 3);
        assert_eq!(summary.malformed_lines, 4);
        assert_eq!(rows, 3);
    }

    #[test]
    fn test_number_shaped_garbage_stays_a_malformed_record() {
        // `1.2.3` is read by the byte-wise number scanner but is not a number,
        // so it must not be reported as a valid non-object record.
        let data = b"{\"id\":1}\n1.2.3\n";
        let config = JsonParserConfig::jsonl().with_error_policy(JsonErrorPolicy::Strict);
        let err = scan_json_from_reader(Cursor::new(data.as_ref()), &config, |_| {})
            .expect_err("number-shaped garbage should be malformed");

        assert!(
            err.to_string().contains("malformed JSON record"),
            "{err}, expected the malformed category"
        );
    }

    #[test]
    fn test_input_of_only_non_object_records_fails() {
        let file = write_file("\"just a string\"\n1\n[2]\n");
        let err = analyze_json_file(file.path(), &JsonParserConfig::jsonl())
            .expect_err("nothing profileable should not return a clean empty profile");

        assert!(
            err.to_string().contains("No valid JSON records found"),
            "{err}"
        );
    }

    #[test]
    /// The JSON grammar accepts both shapes that carry records, so a bare object
    /// is one row rather than an error (#486).
    fn test_json_grammar_reads_a_bare_object_as_one_row() {
        let data = br#"{"x":1}"#;
        let mut rows = 0;
        let summary = scan_json_from_reader(
            Cursor::new(data.as_ref()),
            &JsonParserConfig::json_document(),
            |_| rows += 1,
        )
        .unwrap();

        assert_eq!(summary.rows_read, 1);
        assert_eq!(summary.malformed_lines, 0);
        assert_eq!(rows, 1);
    }

    #[test]
    /// Several objects back to back are not a JSON document. The error names
    /// the option that reads them, because this is what a JSONL file looks like
    /// when it is read with the wrong grammar.
    fn test_json_grammar_rejects_concatenated_objects() {
        let data = br#"{"x":1}{"x":2}"#;
        let err = scan_json_from_reader(
            Cursor::new(data.as_ref()),
            &JsonParserConfig::json_document().with_error_policy(JsonErrorPolicy::Strict),
            |_| {},
        )
        .expect_err("two objects are not one JSON document");

        let message = err.to_string();
        assert!(message.contains("malformed JSON document"), "{message}");
        assert!(message.contains("format=\"jsonl\""), "{message}");
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
    fn test_strict_policy_errors_on_malformed_jsonl() {
        // Malformed in first / middle / last position all abort under Strict.
        for data in [
            b"not-json\n{\"x\":1}\n{\"x\":2}\n".as_ref(),
            b"{\"x\":1}\nnot-json\n{\"x\":2}\n".as_ref(),
            b"{\"x\":1}\n{\"x\":2}\nnot-json\n".as_ref(),
        ] {
            let config = JsonParserConfig::jsonl().with_error_policy(JsonErrorPolicy::Strict);
            let err = scan_json_from_reader(Cursor::new(data), &config, |_| {})
                .expect_err("strict mode must reject malformed records");
            let message = err.to_string().to_lowercase();
            assert!(message.contains("malformed json record"), "got: {message}");
            // The offending record text must never be echoed back.
            assert!(
                !err.to_string().contains("not-json"),
                "leaked record: {err}"
            );
        }
    }

    #[test]
    fn test_tolerant_policy_counts_skipped_records() {
        let data = b"{\"x\":1}\nnot-json\n{\"x\":2}\n";
        let config = JsonParserConfig::jsonl(); // default Skip
        let summary = scan_json_from_reader(Cursor::new(data.as_ref()), &config, |_| {}).unwrap();
        assert_eq!(summary.rows_read, 2);
        assert_eq!(summary.malformed_lines, 1);
    }

    #[test]
    fn test_blank_lines_are_not_counted_as_malformed() {
        let data = b"{\"x\":1}\n\n   \n{\"x\":2}\n";
        for policy in [JsonErrorPolicy::Skip, JsonErrorPolicy::Strict] {
            let config = JsonParserConfig::jsonl().with_error_policy(policy);
            let summary =
                scan_json_from_reader(Cursor::new(data.as_ref()), &config, |_| {}).unwrap();
            assert_eq!(summary.rows_read, 2, "policy {policy:?}");
            assert_eq!(summary.malformed_lines, 0, "policy {policy:?}");
        }
    }

    #[test]
    fn test_truncated_final_record_obeys_error_policy() {
        let data = b"{\"x\":1}\n{\"x\":2";

        let skip = JsonParserConfig::jsonl().with_error_policy(JsonErrorPolicy::Skip);
        let summary = scan_json_from_reader(Cursor::new(data.as_ref()), &skip, |_| {}).unwrap();
        assert_eq!(summary.rows_read, 1);
        assert_eq!(summary.malformed_lines, 1);

        let strict = JsonParserConfig::jsonl().with_error_policy(JsonErrorPolicy::Strict);
        let err = scan_json_from_reader(Cursor::new(data.as_ref()), &strict, |_| {})
            .expect_err("strict mode must reject an incomplete trailing record");
        assert!(
            err.to_string()
                .to_lowercase()
                .contains("malformed json record")
        );
    }

    #[test]
    fn test_all_malformed_file_fails() {
        let file = write_file("not-json\nalso-bad\n");
        let config = JsonParserConfig::jsonl();
        let err = analyze_json_file(file.path(), &config)
            .expect_err("a file with no valid records must fail");
        let message = err.to_string().to_lowercase();
        assert!(message.contains("no valid json records"), "got: {message}");
    }

    #[test]
    fn test_tolerant_file_reports_error_count() {
        let file = write_file("{\"x\":1}\nnot-json\n{\"x\":2}\n");
        let config = JsonParserConfig::jsonl();
        let report = analyze_json_file(file.path(), &config).unwrap();
        assert_eq!(report.execution.rows_processed, 2);
        assert_eq!(report.execution.error_count, 1);
    }

    #[test]
    fn test_strict_file_errors_on_malformed() {
        let file = write_file("{\"x\":1}\nnot-json\n{\"x\":2}\n");
        let config = JsonParserConfig::jsonl().with_error_policy(JsonErrorPolicy::Strict);
        let err = analyze_json_file(file.path(), &config)
            .expect_err("strict mode must reject the malformed record");
        assert!(
            err.to_string()
                .to_lowercase()
                .contains("malformed json record")
        );
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

    /// Deliberately non-alphabetical field order: sorting would give
    /// `active, amount, date, id`, so any re-sort is visible.
    const UNSORTED_FIELDS: &str = r#"{"id":1,"amount":12.5,"active":true,"date":"2026-07-23"}"#;
    const UNSORTED_ORDER: [&str; 4] = ["id", "amount", "active", "date"];

    fn profile_names(report: &ProfileReport) -> Vec<&str> {
        report
            .column_profiles
            .iter()
            .map(|profile| profile.name.as_str())
            .collect()
    }

    #[test]
    fn test_json_column_order_follows_source_not_alphabet() {
        let array = write_file(&format!("[{UNSORTED_FIELDS},{UNSORTED_FIELDS}]"));
        let jsonl = write_file(&format!("{UNSORTED_FIELDS}\n{UNSORTED_FIELDS}\n"));

        for file in [&array, &jsonl] {
            let report = analyze_json_file(file.path(), &JsonParserConfig::default()).unwrap();
            assert_eq!(profile_names(&report), UNSORTED_ORDER);
        }
    }

    #[test]
    fn test_json_column_order_appends_later_keys_in_first_seen_order() {
        // Each record introduces two keys in reverse-alphabetical order, so
        // sorting would reshuffle both the leading pair and the appended pair.
        let data = b"{\"zulu\":1,\"mike\":2}\n{\"zulu\":3,\"mike\":4,\"delta\":5,\"alpha\":6}\n";
        let file = write_bytes(data);
        let report = analyze_json_file(file.path(), &JsonParserConfig::jsonl()).unwrap();

        assert_eq!(profile_names(&report), ["zulu", "mike", "delta", "alpha"]);
    }

    #[test]
    fn test_json_column_order_survives_report_serialization() {
        let file = write_file(&format!("[{UNSORTED_FIELDS}]"));
        let report = analyze_json_file(file.path(), &JsonParserConfig::default()).unwrap();

        let json = serde_json::to_string(&report).unwrap();
        let restored: Value = serde_json::from_str(&json).unwrap();
        let names: Vec<&str> = restored["column_profiles"]
            .as_array()
            .unwrap()
            .iter()
            .map(|profile| profile["name"].as_str().unwrap())
            .collect();

        assert_eq!(names, UNSORTED_ORDER);
    }

    #[test]
    fn test_analyze_json_backfills_columns_discovered_after_first_row() {
        let json = write_file(r#"[{"a":1},{"a":2,"b":3}]"#);
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json.path(), &config).unwrap();
        let col_b = report
            .column_profiles
            .iter()
            .find(|profile| profile.name == "b")
            .unwrap();

        assert_eq!(col_b.total_count, 2);
        assert_eq!(col_b.null_count, 1);
        assert_eq!(col_b.unique_count, Some(1));
    }

    #[test]
    fn test_analyze_json_empty_array() {
        let json = write_file("[]");
        let config = JsonParserConfig::default();
        let report = analyze_json_file(json.path(), &config).unwrap();
        assert!(report.column_profiles.is_empty());
        // An empty dataset has nothing to assess: no score is fabricated.
        assert_eq!(report.quality_score(), None);
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
    fn test_bom_prefixed_file_preserves_source_size() {
        let data = b"\xEF\xBB\xBF[{\"x\":1}]";
        let json = write_bytes(data);
        let report = analyze_json_file(json.path(), &JsonParserConfig::json_document()).unwrap();

        assert_eq!(report.execution.rows_processed, 1);
        assert!(matches!(
            report.data_source,
            DataSource::File {
                format: FileFormat::Json,
                size_bytes,
                ..
            } if size_bytes == data.len() as u64
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
        let file =
            write_json_file(r#"{"type":"FeatureCollection","features":[{"id":1},{"id":2}]}"#);
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
        // A reader has no file name, so the grammar cannot be inferred and is
        // named explicitly; `analyze_json_file` takes it from the extension.
        let config = JsonParserConfig::json_document();

        let (profiles, _stats, rows, malformed, format) =
            analyze_json_from_reader(cursor, &config).unwrap();
        assert_eq!(format, FileFormat::Json);
        assert_eq!(rows, 1);
        assert_eq!(malformed, 0);
        assert_eq!(profiles.len(), 2);
    }

    #[test]
    fn test_single_root_object_pretty_printed_via_analyze_json_file() {
        let file = write_json_file(
            "{\n  \"type\": \"FeatureCollection\",\n  \"features\": [\n    {\"id\": 1},\n    {\"id\": 2}\n  ]\n}\n",
        );
        let config = JsonParserConfig::default();
        let report = analyze_json_file(file.path(), &config).unwrap();

        assert_eq!(report.execution.rows_processed, 1);
        assert_eq!(report.column_profiles.len(), 2);
    }
}
