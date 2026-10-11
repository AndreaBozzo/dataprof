//! The JSON record tokenizer shared by the file reader and the async stream
//! reader, so a payload profiles identically whichever way it arrives.

use std::io::BufRead;

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
        Ok(Value::Object(obj)) => JsonRecord::Object(obj),
        Ok(value) => JsonRecord::NonObject(json_value_kind(&value)),
        Err(err) => JsonRecord::Malformed(err),
    }
}
