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
    /// An object whose source text holds an integer literal outside the
    /// 64-bit range. serde_json keeps such an integer as the nearest `f64`, so
    /// distinct values would round to one before they are counted (#1103).
    /// The text decides, not the parsed value, so a literal a later duplicate
    /// key overwrites counts too, as it does for the Python bytes reader,
    /// which sees every integer token. The record was read in full, so the
    /// caller can skip it and keep reading.
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
        Ok(Value::Object(obj)) => object_record(obj, &recorded.bytes),
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

/// Whether `value` holds a float large enough to have been read from an
/// integer literal outside the 64-bit range; only the source text can tell.
///
/// An integer within `i64` or `u64` parses exactly; one outside becomes an
/// `f64` of magnitude at least 2^63. A float literal like `1e19` does too, so
/// this alone refuses nothing.
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
/// that fits neither `i64` nor `u64`. String contents are skipped, and a token
/// shorter than 19 digits, which always fits, is not parsed.
pub fn has_wide_integer_literal(raw: &[u8]) -> bool {
    integer_literals(raw).any(|(_, token)| is_wide_integer(token))
}

/// The integer literals (no fraction, no exponent) of `raw` JSON text, each
/// with the offset it starts at. String contents are skipped.
fn integer_literals(raw: &[u8]) -> impl Iterator<Item = (usize, &[u8])> {
    number_literals(raw)
        .filter(|literal| !is_float_literal(literal.token))
        .map(|literal| (literal.start, literal.token))
}

/// A number literal in JSON text.
struct NumberLiteral<'a> {
    /// The offset the literal starts at.
    start: usize,
    /// How many arrays and objects enclose it: a field of a root object is at
    /// depth 1.
    depth: usize,
    token: &'a [u8],
}

/// The number literals of `raw` JSON text. String contents are skipped.
fn number_literals(raw: &[u8]) -> impl Iterator<Item = NumberLiteral<'_>> {
    let mut index = 0;
    let mut depth = 0usize;
    std::iter::from_fn(move || {
        while index < raw.len() {
            match raw[index] {
                b'"' => {
                    index += 1;
                    while index < raw.len() && raw[index] != b'"' {
                        index += if raw[index] == b'\\' { 2 } else { 1 };
                    }
                    index += 1;
                }
                b'{' | b'[' => {
                    depth += 1;
                    index += 1;
                }
                b'}' | b']' => {
                    depth = depth.saturating_sub(1);
                    index += 1;
                }
                byte if byte == b'-' || byte.is_ascii_digit() => {
                    let start = index;
                    while index < raw.len() && is_json_number_byte(raw[index]) {
                        index += 1;
                    }
                    return Some(NumberLiteral {
                        start,
                        depth,
                        token: &raw[start..index],
                    });
                }
                _ => index += 1,
            }
        }
        None
    })
}

fn is_float_literal(token: &[u8]) -> bool {
    token.iter().any(|byte| matches!(byte, b'.' | b'e' | b'E'))
}

/// Whether a float literal names a value its `f64` does not hold, so that two
/// such literals can read as one number (#1156).
///
/// The test is on the decimal value, not the spelling: `1.50` and `1e2` are
/// held exactly and stay numbers, while `1000000000000000.01` reads as
/// `1000000000000000` and `1e-400` as `0`.
fn is_lossy_float_literal(token: &[u8]) -> bool {
    // A zero literal is held exactly.
    let Some(literal) = Decimal::parse(token) else {
        return false;
    };
    // Up to 15 significant digits inside the normal range always survive the
    // round trip, which spares most literals the parse and the format below.
    if literal.digits <= 15 && (-290..=290).contains(&(literal.exponent + literal.digits as i64)) {
        return false;
    }
    // decode-audit: impossible — every number byte is ASCII.
    let text = std::str::from_utf8(token).expect("JSON number bytes are ASCII");
    // A literal serde_json accepted always parses; an infinite one was
    // refused as out of range before the record got here.
    let Ok(value) = text.parse::<f64>() else {
        return false;
    };
    // `{:e}` writes the shortest digits that read back as the same `f64`.
    let mut shortest = ShortBuffer::default();
    // decode-audit: impossible — `{:e}` of an f64 fits in the buffer.
    std::fmt::Write::write_fmt(&mut shortest, format_args!("{value:e}"))
        .expect("an f64 in exponent form fits in 32 bytes");
    Decimal::parse(shortest.as_bytes()).is_none_or(|held| !literal.same_value(&held))
}

/// A nonzero decimal number in JSON number syntax, located in its text
/// without copying, so that equal values compare equal however they are
/// spelled.
struct Decimal<'a> {
    negative: bool,
    /// The mantissa from its first to its last nonzero digit, which may hold
    /// the decimal point.
    significant: &'a [u8],
    /// How many digits `significant` holds.
    digits: usize,
    /// The power of ten of the last significant digit.
    exponent: i64,
}

impl<'a> Decimal<'a> {
    /// `None` for a zero.
    fn parse(text: &'a [u8]) -> Option<Self> {
        let (negative, unsigned) = match text.split_first() {
            Some((b'-', rest)) => (true, rest),
            _ => (false, text),
        };
        let (mantissa, exponent) = match unsigned.iter().position(|b| matches!(b, b'e' | b'E')) {
            Some(at) => {
                let exponent = &unsigned[at + 1..];
                // decode-audit: impossible — every number byte is ASCII.
                let parsed = std::str::from_utf8(exponent)
                    .expect("JSON number bytes are ASCII")
                    .parse::<i64>()
                    // An exponent too long for i64 lies far outside the f64
                    // range either way, so saturating keeps the comparison right.
                    .unwrap_or(if exponent.first() == Some(&b'-') {
                        i64::MIN / 2
                    } else {
                        i64::MAX / 2
                    });
                (&unsigned[..at], parsed)
            }
            None => (unsigned, 0),
        };
        let point = mantissa
            .iter()
            .position(|&b| b == b'.')
            .unwrap_or(mantissa.len());
        let first = mantissa.iter().position(|b| matches!(b, b'1'..=b'9'))?;
        // decode-audit: impossible — `first` found a nonzero digit.
        let last = mantissa
            .iter()
            .rposition(|b| matches!(b, b'1'..=b'9'))
            .expect("a nonzero digit was found");
        let last_power = if last < point {
            (point - last - 1) as i64
        } else {
            -((last - point) as i64)
        };
        let digits = last - first + 1 - usize::from(first < point && point < last);
        Some(Self {
            negative,
            significant: &mantissa[first..=last],
            digits,
            exponent: exponent.saturating_add(last_power),
        })
    }

    fn same_value(&self, other: &Decimal<'_>) -> bool {
        self.negative == other.negative
            && self.exponent == other.exponent
            && self.digits == other.digits
            && self
                .significant
                .iter()
                .filter(|&&b| b != b'.')
                .eq(other.significant.iter().filter(|&&b| b != b'.'))
    }
}

/// A stack buffer for the exponent form of one `f64`.
#[derive(Default)]
struct ShortBuffer {
    bytes: [u8; 32],
    len: usize,
}

impl ShortBuffer {
    fn as_bytes(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}

impl std::fmt::Write for ShortBuffer {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        let end = self.len + text.len();
        self.bytes
            .get_mut(self.len..end)
            .ok_or(std::fmt::Error)?
            .copy_from_slice(text.as_bytes());
        self.len = end;
        Ok(())
    }
}

fn is_wide_integer(token: &[u8]) -> bool {
    let digits = token.len() - usize::from(token.first() == Some(&b'-'));
    if digits < 19 {
        return false;
    }
    // decode-audit: impossible — every number byte is ASCII.
    let text = std::str::from_utf8(token).expect("JSON number bytes are ASCII");
    text.parse::<i64>().is_err() && text.parse::<u64>().is_err()
}

/// The record an object parsed from `raw` profiles as: always
/// [`JsonRecord::Object`] or [`JsonRecord::WideInteger`].
///
/// An object whose text holds an integer outside the 64-bit range is
/// [`JsonRecord::WideInteger`]. serde_json reads the integer literal `-0` as
/// the float `-0.0`, while Python's `json` reads it as the integer `0`, so
/// `{"v":0}` and `{"v":-0}` were duplicates over Python bytes and distinct
/// floats over the file and async readers. Such an object is read again with
/// each `-0` as `0`, which keeps its type and its duplicates the same on every
/// route. A float literal like `-0.0` keeps its sign on every route already.
///
/// A field whose float literal names a value its `f64` does not hold, like
/// `1000000000000000.01`, is given as the literal's text instead of the
/// rounded number, so distinct values stay distinct and the field profiles
/// as the same digits in a CSV do (#1156). Values inside an array or object
/// keep the `f64`: they are profiled as the container's text.
pub fn object_record(obj: JsonObject, raw: &[u8]) -> JsonRecord {
    let mut negative_zeros = Vec::new();
    let mut lossy_fields = Vec::new();
    for literal in number_literals(raw) {
        if is_float_literal(literal.token) {
            if literal.depth == 1 && is_lossy_float_literal(literal.token) {
                lossy_fields.push((literal.start, literal.token.len()));
            }
        } else if is_wide_integer(literal.token) {
            return JsonRecord::WideInteger;
        } else if literal.token == b"-0" {
            negative_zeros.push(literal.start);
        }
    }
    if negative_zeros.is_empty() && lossy_fields.is_empty() {
        return JsonRecord::Object(obj);
    }
    // Blanking the sign leaves `0` where `-0` stood; whitespace is legal
    // between any two JSON tokens, so the text stays the same document.
    let mut rewritten = raw.to_vec();
    for start in negative_zeros {
        rewritten[start] = b' ';
    }
    // Quoting a literal makes it a string of the same digits. Last first, so
    // the earlier offsets still hold.
    for (start, len) in lossy_fields.into_iter().rev() {
        rewritten.insert(start + len, b'"');
        rewritten.insert(start, b'"');
    }
    // decode-audit: impossible — `raw` parsed as this object, and only signs
    // were replaced by whitespace and number literals quoted.
    JsonRecord::Object(
        serde_json::from_slice::<JsonObject>(&rewritten)
            .expect("an object with its `-0` signs blanked and literals quoted is still an object"),
    )
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
        Ok(Value::Object(obj)) => object_record(obj, line.as_bytes()),
        Ok(value) => JsonRecord::NonObject(json_value_kind(&value)),
        // An integer too long even for f64 fails to parse at all; the line is
        // consumed either way, so name it as what it holds.
        Err(_) if has_wide_integer_literal(line.as_bytes()) => JsonRecord::WideInteger,
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
    fn integer_negative_zero_reads_as_zero_and_float_negative_zero_keeps_its_sign() {
        let text = r#"{"v":-0,"w":[-0,{"k":-0}],"s":"-0","-0":1,"f":-0.0,"e":-0e0}"#;
        let from_line = match read_jsonl_line(text) {
            JsonRecord::Object(obj) => obj,
            _ => panic!("a JSONL object"),
        };
        let mut reader = Cursor::new(text.as_bytes());
        let from_array = match read_json_record(&mut reader, b'{').unwrap() {
            JsonRecord::Object(obj) => obj,
            _ => panic!("an array element object"),
        };

        for obj in [from_line, from_array] {
            assert_eq!(json_value_text(&obj["v"]), "0");
            assert_eq!(json_value_text(&obj["w"]), r#"[0,{"k":0}]"#);
            assert_eq!(json_value_text(&obj["s"]), "-0");
            assert!(obj.contains_key("-0"));
            assert_eq!(json_value_text(&obj["f"]), "-0.0");
            assert_eq!(json_value_text(&obj["e"]), "-0.0");
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

    #[test]
    fn a_float_literal_is_lossy_only_when_its_value_changes() {
        let held = [
            "0.1", "1.50", "1e2", "1E+2", "-0.0", "0e999", "2.5e-3", "1e308",
        ];
        let lossy = [
            "1000000000000000.01",
            "1e-400",
            "0.30000000000000000001",
            "12345678901234567890.0",
            "1e-99999999999999999999",
            "9007199254740993.0",
            "-1.00000000000000001",
        ];
        for token in held {
            assert!(!is_lossy_float_literal(token.as_bytes()), "{token}");
        }
        for token in lossy {
            assert!(is_lossy_float_literal(token.as_bytes()), "{token}");
        }
    }

    /// The shortcut for 15 significant digits in the normal range must agree
    /// with the full comparison it skips, at both ends of that range.
    #[test]
    fn fifteen_digit_literals_in_the_normal_range_are_held() {
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        for exponent in (-305..=275).step_by(5) {
            for _ in 0..200 {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                let mantissa = 100_000_000_000_000 + state % 900_000_000_000_000;
                let token = format!("{mantissa}e{exponent}");
                let literal = Decimal::parse(token.as_bytes()).unwrap();
                let value: f64 = token.parse().unwrap();
                let mut shortest = ShortBuffer::default();
                std::fmt::Write::write_fmt(&mut shortest, format_args!("{value:e}")).unwrap();
                let held = Decimal::parse(shortest.as_bytes()).unwrap();
                assert!(literal.same_value(&held), "{token}");
            }
        }
    }

    #[test]
    fn only_a_lossy_field_of_the_root_object_becomes_text() {
        let raw = br#"{"a":1000000000000000.01,"b":[1000000000000000.01],"c":-0,"d":1.5}"#;
        let obj = serde_json::from_slice(raw).unwrap();
        let JsonRecord::Object(obj) = object_record(obj, raw) else {
            panic!("not an object record");
        };
        assert_eq!(obj["a"], Value::String("1000000000000000.01".into()));
        assert_eq!(json_value_text(&obj["b"]), "[1000000000000000.0]");
        assert_eq!(json_value_text(&obj["c"]), "0");
        assert_eq!(json_value_text(&obj["d"]), "1.5");
    }
}
