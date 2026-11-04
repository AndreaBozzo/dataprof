//! Unicode White_Space around a JSON record is accepted for all three shapes,
//! a single object, an array and a JSONL line, on every route (#1146).
//!
//! The single-object and JSONL readers decode `str::trim`ed text, and Rust's
//! `trim` removes the Unicode White_Space set, so no-break space, NEL, LS, PS,
//! vertical tab and the rest were already accepted around them. The array
//! branch skipped only ASCII whitespace before `[` and allowed only JSON's four
//! after `]`, so the same characters around an array were refused. Characters
//! outside the set (U+001C to U+001F) stay refused around every shape.
//! `quick_row_count` parsed the document with serde_json, which allows only
//! JSON's four, so it refused both a single object and an array.
//!
//! `python/tests/test_json_unicode_whitespace.py` is the Python twin.

use std::io::{Cursor, Write};

use dataprof::{JsonErrorPolicy, Profiler};
use dataprof_json::{JsonParserConfig, scan_json_from_reader};

/// The Unicode White_Space characters outside JSON's own space, tab, LF, CR.
const WHITE_SPACE: [(&str, char); 21] = [
    ("VT", '\u{0B}'),
    ("FF", '\u{0C}'),
    ("NEL", '\u{85}'),
    ("NBSP", '\u{A0}'),
    ("OGHAM", '\u{1680}'),
    ("EN QUAD", '\u{2000}'),
    ("EM QUAD", '\u{2001}'),
    ("EN SPACE", '\u{2002}'),
    ("EM SPACE", '\u{2003}'),
    ("3/EM", '\u{2004}'),
    ("4/EM", '\u{2005}'),
    ("6/EM", '\u{2006}'),
    ("FIGURE", '\u{2007}'),
    ("PUNCT", '\u{2008}'),
    ("THIN", '\u{2009}'),
    ("HAIR", '\u{200A}'),
    ("LS", '\u{2028}'),
    ("PS", '\u{2029}'),
    ("NNBSP", '\u{202F}'),
    ("MMSP", '\u{205F}'),
    ("IDEOGRAPHIC", '\u{3000}'),
];

/// Control characters outside the White_Space set, which `str::trim` keeps.
const NOT_WHITE_SPACE: [(&str, char); 2] = [("FS", '\u{1C}'), ("US", '\u{1F}')];

/// `(shape, extension, text with the character before, with it after, rows)`.
fn shapes(c: char) -> [(&'static str, &'static str, String, String, usize); 3] {
    [
        (
            "object",
            "json",
            format!("{c}{{\"a\": 1}}"),
            format!("{{\"a\": 1}}{c}"),
            1,
        ),
        (
            "array",
            "json",
            format!("{c}[{{\"a\": 1}}]"),
            format!("[{{\"a\": 1}}]{c}"),
            1,
        ),
        (
            "jsonl",
            "jsonl",
            format!("{c}{{\"a\": 1}}\n{{\"a\": 2}}\n"),
            format!("{{\"a\": 1}}{c}\n{{\"a\": 2}}\n"),
            2,
        ),
    ]
}

/// Rows each sync route read from `text`, or the error.
fn sync_routes(extension: &str, text: &str) -> Vec<(&'static str, Result<usize, String>)> {
    let mut file = tempfile::Builder::new()
        .suffix(&format!(".{extension}"))
        .tempfile()
        .unwrap();
    file.write_all(text.as_bytes()).unwrap();
    file.flush().unwrap();
    let file_route = Profiler::new()
        .json_error_policy(JsonErrorPolicy::Strict)
        .analyze_file(file.path())
        .map(|report| report.execution.rows_processed)
        .map_err(|e| e.to_string());
    // No format: detection reads past the white space to tell an array from
    // the JSONL fallback.
    let detected = scan_json_from_reader(
        Cursor::new(text.as_bytes()),
        &JsonParserConfig::default().with_error_policy(JsonErrorPolicy::Strict),
        |_| {},
    )
    .map(|summary| summary.rows_read)
    .map_err(|e| e.to_string());
    // The partial-analysis count parses the document with serde_json, apart
    // from the scanner, and must count the same document.
    let counted = dataprof::quick_row_count(file.path())
        .map(|estimate| estimate.count as usize)
        .map_err(|e| e.to_string());
    vec![
        ("file", file_route),
        ("reader, format detected", detected),
        ("quick_row_count", counted),
    ]
}

#[test]
fn unicode_white_space_around_a_record_reads_on_every_sync_route() {
    let mut wrong = Vec::new();
    for (name, c) in WHITE_SPACE {
        for (shape, extension, before, after, rows) in shapes(c) {
            for (side, text) in [("before", &before), ("after", &after)] {
                for (route, outcome) in sync_routes(extension, text) {
                    // Detection cannot tell a single object from JSONL, and
                    // falls back to JSONL: a one-line object still reads.
                    if outcome != Ok(rows) {
                        wrong.push(format!("{route}, {shape}, {name} {side}: {outcome:?}"));
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn characters_outside_white_space_stay_refused_around_every_shape() {
    let mut wrong = Vec::new();
    for (name, c) in NOT_WHITE_SPACE {
        for (shape, extension, before, after, _) in shapes(c) {
            // A JSONL line's control character is refused as a malformed line;
            // only the line it sits on is in question, so the document shapes
            // are the ones this guards.
            if shape == "jsonl" {
                continue;
            }
            for (side, text) in [("before", &before), ("after", &after)] {
                for (route, outcome) in sync_routes(extension, text) {
                    if outcome.is_ok() {
                        wrong.push(format!("{route}, {shape}, {name} {side}: {outcome:?}"));
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "async-streaming")]
mod async_bytes {
    use super::{NOT_WHITE_SPACE, WHITE_SPACE, shapes};

    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat, JsonErrorPolicy, Profiler};

    async fn rows(extension: &str, text: &str) -> Result<usize, String> {
        let format = if extension == "jsonl" {
            FileFormat::Jsonl
        } else {
            FileFormat::Json
        };
        let source = BytesSource::new(
            bytes::Bytes::from(text.to_string()),
            AsyncSourceInfo::new("white-space", format).size_hint(Some(text.len() as u64)),
        );
        Profiler::new()
            .json_error_policy(JsonErrorPolicy::Strict)
            .profile_stream(source)
            .await
            .map(|report| report.execution.rows_processed)
            .map_err(|e| e.to_string())
    }

    /// The stream count parses an array with serde_json, apart from the
    /// scanner, and must count the same array; it counts arrays only.
    #[tokio::test]
    async fn the_stream_row_count_counts_an_array_between_unicode_white_space() {
        let count = |text: String| async move {
            let source = BytesSource::new(
                bytes::Bytes::from(text),
                AsyncSourceInfo::new("white-space", FileFormat::Json),
            );
            Profiler::new()
                .quick_row_count_stream(source)
                .await
                .map(|estimate| estimate.count)
                .map_err(|e| e.to_string())
        };
        let mut wrong = Vec::new();
        for (name, c) in WHITE_SPACE {
            for (side, text) in [
                ("before", format!("{c}[{{\"a\": 1}}, {{\"a\": 2}}]")),
                ("after", format!("[{{\"a\": 1}}, {{\"a\": 2}}]{c}")),
            ] {
                let outcome = count(text).await;
                if outcome != Ok(2) {
                    wrong.push(format!("{name} {side}: {outcome:?}"));
                }
            }
        }
        for (name, c) in NOT_WHITE_SPACE {
            let outcome = count(format!("[{{\"a\": 1}}]{c}")).await;
            if outcome.is_ok() {
                wrong.push(format!("{name} after: counted {outcome:?}"));
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }

    #[tokio::test]
    async fn unicode_white_space_around_a_record_reads_on_async_bytes() {
        let mut wrong = Vec::new();
        for (name, c) in WHITE_SPACE {
            for (shape, extension, before, after, expected) in shapes(c) {
                for (side, text) in [("before", &before), ("after", &after)] {
                    let outcome = rows(extension, text).await;
                    if outcome != Ok(expected) {
                        wrong.push(format!("{shape}, {name} {side}: {outcome:?}"));
                    }
                }
            }
        }
        for (name, c) in NOT_WHITE_SPACE {
            for (shape, extension, before, after, _) in shapes(c) {
                if shape == "jsonl" {
                    continue;
                }
                for (side, text) in [("before", &before), ("after", &after)] {
                    let outcome = rows(extension, text).await;
                    if outcome.is_ok() {
                        wrong.push(format!("{shape}, {name} {side}: read {outcome:?}"));
                    }
                }
            }
        }
        assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    }
}

/// After a fault the scanners ask whether input is left unread, which sets
/// `source_exhausted`. A no-break space there is no more input than a space.
#[test]
fn white_space_after_a_malformed_array_is_not_unread_input() {
    let mut wrong = Vec::new();
    for c in [' ', '\u{A0}', '\u{3000}'] {
        let text = format!("[{{\"a\": 1}}{c}");
        let mut file = tempfile::Builder::new().suffix(".json").tempfile().unwrap();
        file.write_all(text.as_bytes()).unwrap();
        file.flush().unwrap();
        let report = Profiler::new().analyze_file(file.path()).unwrap();
        let execution = &report.execution;
        let seen = (
            execution.rows_processed,
            execution.source_exhausted,
            execution.error_count,
        );
        if seen != (1, true, 1) {
            wrong.push(format!("file, U+{:04X}: {seen:?}", u32::from(c)));
        }
        #[cfg(feature = "async-streaming")]
        {
            let source = dataprof::BytesSource::new(
                bytes::Bytes::from(text.clone()),
                dataprof::AsyncSourceInfo::new("truncated", dataprof::FileFormat::Json),
            );
            let report = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(Profiler::new().profile_stream(source))
                .unwrap();
            let execution = &report.execution;
            let seen = (
                execution.rows_processed,
                execution.source_exhausted,
                execution.error_count,
            );
            if seen != (1, true, 1) {
                wrong.push(format!("async bytes, U+{:04X}: {seen:?}", u32::from(c)));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
