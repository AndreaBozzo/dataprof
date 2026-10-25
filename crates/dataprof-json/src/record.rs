//! The JSON record tokenizer shared by the file reader and the async stream
//! reader, so a payload profiles identically whichever way it arrives.

use std::io::{BufRead, Read};

use dataprof_core::DataProfilerError;
use serde::Deserialize;
use serde_json::Value;

use crate::JsonObject;

/// One record read from a JSON or JSONL source.
pub enum JsonRecord {
    /// A JSON object — the only value with named fields to profile as a row.
    Object(JsonObject),
    /// Valid JSON that is not an object, carrying the value's kind for the
    /// error message.
    NonObject(&'static str),
    /// The bytes are not valid JSON.
    Malformed(serde_json::Error),
    /// An object that holds an integer literal outside the 64-bit range.
    /// serde_json keeps such an integer as the nearest `f64`, so distinct
    /// values would round to one before they are counted (#1103). The record
    /// was read in full, so the caller can skip it and keep reading.
    WideInteger,
}

/// Read one JSON value and leave the reader positioned immediately after it.
///
/// `first` is the value's first byte, already located by
/// the caller but not yet consumed.
pub fn read_json_record<R: BufRead>(
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

    // The source bytes are kept so an integer serde rounded to f64 can be
    // recognised from its literal; the parsed value no longer tells.
    let mut recorded = RecordingReader {
        inner: reader,
        bytes: Vec::new(),
    };
    let parsed = {
        let mut deserializer = serde_json::Deserializer::from_reader(&mut recorded);
        Value::deserialize(&mut deserializer)
    };
    Ok(match parsed {
        Ok(Value::Object(obj)) if holds_wide_integer(&obj, &recorded.bytes) => {
            JsonRecord::WideInteger
        }
        Ok(Value::Object(obj)) => JsonRecord::Object(obj),
        Ok(value) => JsonRecord::NonObject(json_value_kind(&value)),
        Err(err) if err.is_io() => {
            // A failed read is not a malformed record that the caller can
            // skip. Recover the original I/O wrapper: serde's Error::source
            // skips it, hiding concrete transport causes from Python.
            let message = format!("JSON record read failed: {err}");
            return Err(DataProfilerError::json_parsing_with_source(
                message,
                std::io::Error::from(err),
            ));
        }
        Err(err) => JsonRecord::Malformed(err),
    })
}

/// A reader that keeps a copy of every byte read through it.
struct RecordingReader<'a, R> {
    inner: &'a mut R,
    bytes: Vec<u8>,
}

impl<R: Read> Read for RecordingReader<'_, R> {
    fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
        let read = self.inner.read(buffer)?;
        self.bytes.extend_from_slice(&buffer[..read]);
        Ok(read)
    }
}

/// Whether a parsed object came from source text holding an integer literal
/// outside the 64-bit range.
///
/// An integer within `i64` or `u64` parses exactly; one outside becomes an
/// `f64` of magnitude at least 2^63. Only an object holding such a float can
/// hide one, so `raw` is scanned only then: a float literal like `1e19` is
/// read as written and stays accepted.
pub fn holds_wide_integer(obj: &JsonObject, raw: &[u8]) -> bool {
    obj.values().any(may_hold_wide_integer) && has_wide_integer_literal(raw)
}

/// Whether `value` holds a float large enough to have been read from an
/// integer literal outside the 64-bit range; only the source text can tell.
pub fn may_hold_wide_integer(value: &Value) -> bool {
    match value {
        Value::Number(number) => {
            number.as_i64().is_none()
                && number.as_u64().is_none()
                && number
                    .as_f64()
                    .is_some_and(|float| float.abs() >= 2f64.powi(63))
        }
        Value::Array(items) => items.iter().any(may_hold_wide_integer),
        Value::Object(fields) => fields.values().any(may_hold_wide_integer),
        _ => false,
    }
}

/// Whether `raw` JSON text holds an integer literal (no fraction, no exponent)
/// that fits neither `i64` nor `u64`. String contents are skipped.
pub fn has_wide_integer_literal(raw: &[u8]) -> bool {
    let mut index = 0;
    while index < raw.len() {
        match raw[index] {
            b'"' => {
                index += 1;
                while index < raw.len() && raw[index] != b'"' {
                    index += if raw[index] == b'\\' { 2 } else { 1 };
                }
                index += 1;
            }
            byte if byte == b'-' || byte.is_ascii_digit() => {
                let start = index;
                while index < raw.len() && is_json_number_byte(raw[index]) {
                    index += 1;
                }
                let token = &raw[start..index];
                if !token.iter().any(|byte| matches!(byte, b'.' | b'e' | b'E')) {
                    // decode-audit: impossible — every number byte is ASCII.
                    let text = std::str::from_utf8(token).expect("JSON number bytes are ASCII");
                    if text.parse::<i64>().is_err() && text.parse::<u64>().is_err() {
                        return true;
                    }
                }
            }
            _ => index += 1,
        }
    }
    false
}

/// Build the error for a record refused as [`JsonRecord::WideInteger`].
/// `location` names the record ("at position 3", "on line 7"); the value is
/// never included, as with every record error.
pub fn wide_integer_record_error(location: &str) -> DataProfilerError {
    DataProfilerError::json_parsing_error(&format!(
        "JSON record {location} holds an integer outside the 64-bit range, which would be rounded to a float and could merge distinct values; write such integers as strings to profile them"
    ))
}

/// Read the bytes that can spell a JSON number, stopping at the first byte that
/// cannot. The token is returned unvalidated — the caller decides whether it
/// actually parses, so `1.2.3` stays a malformed record rather than a number.
///
/// Leading whitespace is consumed first: callers locate the next value's first
/// byte without consuming what precedes it, because serde needs that whitespace
/// to keep its line and column context.
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

/// Advance the reader past any ASCII whitespace at its current position.
pub(crate) fn skip_ascii_whitespace<R: BufRead>(reader: &mut R) -> Result<(), DataProfilerError> {
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

/// The text a JSON value is profiled as: scalars as themselves, `null` as the
/// empty string, and an array or object as compact JSON with every object's
/// keys sorted.
///
/// The sort makes the text canonical. With `preserve_order` an object keeps
/// its source key order, so `{"x":1,"y":2}` and `{"y":2,"x":1}` would render
/// differently and two equal rows would not count as duplicates (#767). Keys
/// sort by code point, as Python's `json.dumps(..., sort_keys=True)` does for
/// in-memory inputs.
pub fn json_value_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(string) => string.to_string(),
        Value::Array(_) | Value::Object(_) => {
            let mut text = String::new();
            write_canonical_json(value, &mut text);
            text
        }
    }
}

fn write_canonical_json(value: &Value, out: &mut String) {
    match value {
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                write_canonical_json(item, out);
            }
            out.push(']');
        }
        Value::Object(fields) => {
            let mut entries: Vec<_> = fields.iter().collect();
            entries.sort_unstable_by_key(|(key, _)| *key);
            out.push('{');
            for (index, (key, field)) in entries.into_iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                out.push_str(&scalar_json(key));
                out.push(':');
                write_canonical_json(field, out);
            }
            out.push('}');
        }
        scalar => out.push_str(&scalar_json(scalar)),
    }
}

fn scalar_json<T: serde::Serialize + ?Sized>(value: &T) -> String {
    // decode-audit: impossible — serializing an in-memory string or scalar
    // Value cannot fail; a panic here beats folding "" (= null) into stats.
    serde_json::to_string(value).expect("re-serializing a parsed JSON value cannot fail")
}

pub fn json_value_kind(value: &Value) -> &'static str {
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
pub fn non_object_record_error(kind: &str, position: usize) -> DataProfilerError {
    // No source: the document parsed cleanly, it simply does not hold records,
    // so there is no decoder error underneath to retain.
    DataProfilerError::json_parsing_error(&format!(
        "non-object JSON record at position {position}: \
         expected an object with fields to profile, found {kind}"
    ))
}

pub fn malformed_array_error(message: &str) -> DataProfilerError {
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
pub fn json_document_error(err: serde_json::Error) -> DataProfilerError {
    let message = format!(
        // One source line on purpose: rustfmt may join a `\`-continued
        // literal and leave its indentation inside the string, which silently
        // corrupted this very message once.
        "malformed JSON document: {err}. A JSON source must hold exactly one array or object; for one record per line use format=\"jsonl\""
    );
    DataProfilerError::json_parsing_with_source(message, err)
}

/// Consume the rest of the input, returning how many bytes that was.
pub fn drain_to_end<R: BufRead>(reader: &mut R) -> Result<usize, DataProfilerError> {
    let mut consumed = 0;
    loop {
        let available = reader.fill_buf().map_err(DataProfilerError::from)?.len();
        if available == 0 {
            return Ok(consumed);
        }
        reader.consume(available);
        consumed += available;
    }
}

/// Parse one JSONL line as exactly one JSON value.
///
/// `serde_json::from_str` requires the value to fill the whole input, so a line
/// carrying two adjacent or space-separated values is a malformed record rather
/// than two clean ones — the boundary rule that separates JSONL from a stream of
/// JSON values.
pub fn read_jsonl_line(line: &str) -> JsonRecord {
    match serde_json::from_str::<Value>(line.trim()) {
        Ok(Value::Object(obj)) if holds_wide_integer(&obj, line.as_bytes()) => {
            JsonRecord::WideInteger
        }
        Ok(Value::Object(obj)) => JsonRecord::Object(obj),
        Ok(value) => JsonRecord::NonObject(json_value_kind(&value)),
        Err(err) => JsonRecord::Malformed(err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;
    use std::io::{self, BufReader, Cursor, Read};

    #[derive(Debug)]
    struct LocalReadFailure;

    impl std::fmt::Display for LocalReadFailure {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("local read failure")
        }
    }

    impl std::error::Error for LocalReadFailure {}

    impl Read for LocalReadFailure {
        fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::new(io::ErrorKind::PermissionDenied, Self))
        }
    }

    #[test]
    fn json_record_read_failure_keeps_the_original_io_source() {
        let mut reader = BufReader::new(Cursor::new(b"{\"x\":\"").chain(LocalReadFailure));

        let error = read_json_record(&mut reader, b'{')
            .err()
            .expect("a read failure must abort before a caller can skip a malformed record");

        assert!(
            error
                .to_string()
                .contains("JSON record read failed: local read failure")
        );
        let source = error
            .source()
            .and_then(|source| source.downcast_ref::<io::Error>())
            .expect("the original I/O error must survive serde's wrapper");
        assert_eq!(source.kind(), io::ErrorKind::PermissionDenied);
        assert!(source.get_ref().unwrap().is::<LocalReadFailure>());
    }
}
