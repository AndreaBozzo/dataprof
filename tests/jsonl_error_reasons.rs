//! A strict JSONL error names the decoder's reason, and the record-boundary
//! rule only for the faults it describes (#1143).
//!
//! The file and async readers gave every decode fault the same text, "a JSONL
//! record must be one complete JSON value on one line", which sent the reader
//! looking for a line-splitting problem in an out-of-range number or a bad
//! escape. The reason now comes from serde; the boundary rule is kept for data
//! after a complete value and a value the line break cuts short. The column
//! counts characters of the line as the source has it: white space before the
//! record counts, and a record that ends early is located just past its last
//! character, as the Python bytes reader locates it.
//!
//! `python/tests/test_jsonl_error_reasons.py` is the Python twin, and adds the
//! bytes route.

use std::io::Write;

use dataprof::{JsonErrorPolicy, Profiler};

/// (label, line 2 of the input, what the strict error says after
/// "malformed JSON record on line 2, ")
const CASES: [(&str, &str, &str); 9] = [
    (
        "number out of range",
        r#"{"v":1e400}"#,
        "column 10: number out of range",
    ),
    (
        "invalid escape",
        r#"{"v":"a\qb"}"#,
        "column 9: invalid escape",
    ),
    (
        "control character",
        "{\"v\":\"a\u{1}b\"}",
        "column 8: control character (\\u0000-\\u001F) found while parsing a string",
    ),
    (
        "missing comma",
        r#"{"v":1 "w":2}"#,
        "column 8: expected `,` or `}`",
    ),
    (
        "trailing value",
        r#"{"v":1} {"v":2}"#,
        "column 9: trailing characters; a JSONL record must be one complete JSON value on one line",
    ),
    (
        "cut short",
        r#"{"v":1"#,
        "column 7: EOF while parsing an object; a JSONL record must be one complete JSON value on one line",
    ),
    (
        "cut short after white space",
        r#"{"x":   "#,
        "column 6: EOF while parsing a value; a JSONL record must be one complete JSON value on one line",
    ),
    (
        "leading white space",
        "  {bad}",
        "column 4: key must be a string",
    ),
    (
        // serde counts bytes; the column counts characters.
        "non-ASCII before the fault",
        "{\"\u{e9}\u{e9}\":1 \"w\":2}",
        "column 9: expected `,` or `}`",
    ),
];

fn input(line: &str) -> String {
    format!("{{\"v\":0}}\n{line}\n{{\"v\":2}}\n")
}

fn expected(tail: &str) -> String {
    format!("malformed JSON record on line 2, {tail}")
}

/// Whether `error` carries exactly the expected message: the reason ends the
/// sentence, so a reason with the boundary rule appended does not pass for one
/// without it.
fn matches(error: &str, expected: &str) -> bool {
    error
        .match_indices(expected)
        .any(|(at, _)| !error[at + expected.len()..].starts_with(';'))
}

/// Collects every wrong case before failing.
#[test]
fn file_reader_names_the_decoders_reason() {
    let mut wrong = Vec::new();
    for (label, line, tail) in CASES {
        let mut file = tempfile::Builder::new()
            .suffix(".jsonl")
            .tempfile()
            .unwrap();
        file.write_all(input(line).as_bytes()).unwrap();
        let result = Profiler::new()
            .json_error_policy(JsonErrorPolicy::Strict)
            .analyze_file(file.path());
        match result {
            Err(error) if matches(&error.to_string(), &expected(tail)) => {}
            Err(error) => wrong.push(format!("[{label}] file: {error}")),
            Ok(_) => wrong.push(format!("[{label}] file: profiled instead of refusing")),
        }
    }
    assert!(
        wrong.is_empty(),
        "wrong strict JSONL errors:\n{}",
        wrong.join("\n")
    );
}

#[cfg(feature = "async-streaming")]
mod async_route {
    use super::{CASES, expected, input, matches};
    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat, JsonErrorPolicy, Profiler};

    #[tokio::test]
    async fn async_reader_names_the_decoders_reason() {
        let mut wrong = Vec::new();
        for (label, line, tail) in CASES {
            let data = input(line);
            let size = data.len() as u64;
            let source = BytesSource::new(
                bytes::Bytes::from(data),
                AsyncSourceInfo::new("jsonl-reasons", FileFormat::Jsonl).size_hint(Some(size)),
            );
            let result = Profiler::new()
                .json_error_policy(JsonErrorPolicy::Strict)
                .profile_stream(source)
                .await;
            match result {
                Err(error) if matches(&error.to_string(), &expected(tail)) => {}
                Err(error) => wrong.push(format!("[{label}] async: {error}")),
                Ok(_) => wrong.push(format!("[{label}] async: profiled instead of refusing")),
            }
        }
        assert!(
            wrong.is_empty(),
            "wrong strict JSONL errors:\n{}",
            wrong.join("\n")
        );
    }
}
