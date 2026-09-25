//! A CSV quote that is never closed must not read as a clean file (#782).
//!
//! The CSV parser accepts end of input inside a quoted field as the end of that
//! field, so the unclosed quote swallows every following row into one value.
//! `execution.unterminated_quote` is the only sign of it: `Some(true)` for such
//! a source on every engine, `Some(false)` for a clean one, and `None` where the
//! scan never reached the end of the source, which is where that quote sits.

use std::io::Write;

use dataprof::{EngineType, Profiler, StopCondition};

/// The issue's example: rows 2 and 3 end up inside row 1's `text` value.
const UNCLOSED: &str = "id,text\n1,\"never closed\n2,x\n3,y\n";
/// A quoted field that spans lines and is closed: one legitimate record.
const CLOSED_ACROSS_LINES: &str = "id,text\n1,\"line one\nline two\"\n2,x\n";

const ENGINES: [EngineType; 3] = [
    EngineType::Auto,
    EngineType::Incremental,
    EngineType::Columnar,
];

fn write_csv(contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    write!(file, "{contents}").unwrap();
    file.flush().unwrap();
    file
}

#[test]
fn an_unclosed_quote_is_reported_on_every_engine() {
    let csv = write_csv(UNCLOSED);
    for engine in ENGINES {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap();
        assert_eq!(report.execution.rows_processed, 1, "{engine:?}");
        assert_eq!(
            report.execution.unterminated_quote,
            Some(true),
            "{engine:?} must not describe the swallowed rows as a clean file"
        );
    }
}

#[test]
fn a_closed_quote_across_lines_is_clean_on_every_engine() {
    let csv = write_csv(CLOSED_ACROSS_LINES);
    for engine in ENGINES {
        let report = Profiler::new()
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap();
        assert_eq!(report.execution.rows_processed, 2, "{engine:?}");
        assert_eq!(
            report.execution.unterminated_quote,
            Some(false),
            "{engine:?}"
        );
    }
}

#[test]
fn strict_parsing_refuses_an_unclosed_quote_on_every_engine() {
    let csv = write_csv(UNCLOSED);
    for engine in ENGINES {
        let err = Profiler::new()
            .engine(engine)
            .csv_flexible(false)
            .analyze_file(csv.path())
            .expect_err("strict parsing must refuse the file");
        let message = err.to_string();
        assert!(
            message.contains("ends inside a quoted field"),
            "{engine:?}: {message}"
        );
    }
}

#[test]
fn a_scan_stopped_before_the_end_does_not_answer() {
    // The unclosed quote is on the last record, past the cap.
    let csv = write_csv("id,text\n1,a\n2,b\n3,\"open\n4,x\n");
    for engine in ENGINES {
        let report = Profiler::new()
            .engine(engine)
            .stop_when(StopCondition::MaxRows(1))
            .analyze_file(csv.path())
            .unwrap();
        assert!(!report.execution.source_exhausted, "{engine:?}");
        assert_eq!(report.execution.unterminated_quote, None, "{engine:?}");
    }
}

#[test]
fn a_cap_the_file_does_not_exceed_still_answers() {
    // A cap equal to the record count reads the whole file, so the answer
    // stands. The file is far larger than a read buffer, so a decoder that
    // stops at the cap need not have reached the end of the file itself.
    let mut contents = String::from(
        "id,text
",
    );
    for id in 0..20_000 {
        contents.push_str(&format!(
            "{id},value {id}
"
        ));
    }
    contents.push_str(
        "20000,\"open
20001,x
",
    );
    let csv = write_csv(&contents);
    for engine in ENGINES {
        let report = Profiler::new()
            .engine(engine)
            .stop_when(StopCondition::MaxRows(20_001))
            .analyze_file(csv.path())
            .unwrap();
        assert_eq!(report.execution.rows_processed, 20_001, "{engine:?}");
        assert!(report.execution.source_exhausted, "{engine:?}");
        assert_eq!(
            report.execution.unterminated_quote,
            Some(true),
            "{engine:?}"
        );
    }

    // A clean file under the same cap answers too. The trailing blank lines
    // hold no record, so the cap reads the whole file, but a decoder that
    // stops at the cap leaves them unread and never sees the end itself.
    let mut contents = String::from(
        "id,text
",
    );
    for id in 0..20_000 {
        contents.push_str(&format!(
            "{id},value {id}
"
        ));
    }
    contents.push_str(
        "


",
    );
    let csv = write_csv(&contents);
    for engine in ENGINES {
        let report = Profiler::new()
            .engine(engine)
            .stop_when(StopCondition::MaxRows(20_000))
            .analyze_file(csv.path())
            .unwrap();
        assert!(report.execution.source_exhausted, "{engine:?}");
        assert_eq!(
            report.execution.unterminated_quote,
            Some(false),
            "{engine:?}"
        );
    }
}

#[test]
fn non_csv_input_is_not_checked() {
    let mut file = tempfile::Builder::new()
        .suffix(".jsonl")
        .tempfile()
        .unwrap();
    writeln!(file, "{{\"text\": \"a \\\"quoted\\\" value\"}}").unwrap();
    file.flush().unwrap();
    let report = Profiler::new().analyze_file(file.path()).unwrap();
    assert_eq!(report.execution.unterminated_quote, None);
}

#[test]
fn the_finding_survives_a_saved_report() {
    let csv = write_csv(UNCLOSED);
    let report = Profiler::new().analyze_file(csv.path()).unwrap();
    let saved = serde_json::to_string(&report).unwrap();
    assert!(saved.contains("\"unterminated_quote\":true"));
    let loaded: dataprof::ProfileReport = serde_json::from_str(&saved).unwrap();
    assert_eq!(loaded.execution.unterminated_quote, Some(true));
    for findings in [report.findings(), loaded.findings()] {
        let json = serde_json::to_value(&findings).unwrap();
        assert!(
            json.to_string().contains("\"unterminated_quote\""),
            "no finding in {json}"
        );
    }

    // A clean report does not carry the finding; an unchecked one omits the field.
    let clean = Profiler::new()
        .analyze_file(write_csv(CLOSED_ACROSS_LINES).path())
        .unwrap();
    let json = serde_json::to_value(clean.findings()).unwrap();
    assert!(!json.to_string().contains("unterminated_quote"));
}

#[cfg(feature = "async-streaming")]
mod stream {
    use dataprof::{AsyncSourceInfo, BytesSource, FileFormat, Profiler};

    fn source(csv: &'static str) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(csv.as_bytes()),
            AsyncSourceInfo::new("quotes", FileFormat::Csv).size_hint(Some(csv.len() as u64)),
        )
    }

    #[tokio::test]
    async fn a_stream_reports_the_same_as_a_file() {
        let report = Profiler::new()
            .profile_stream(source(super::UNCLOSED))
            .await
            .unwrap();
        assert_eq!(report.execution.rows_processed, 1);
        assert_eq!(report.execution.unterminated_quote, Some(true));

        let report = Profiler::new()
            .profile_stream(source(super::CLOSED_ACROSS_LINES))
            .await
            .unwrap();
        assert_eq!(report.execution.unterminated_quote, Some(false));
    }

    #[tokio::test]
    async fn a_strict_stream_refuses_an_unclosed_quote() {
        let err = Profiler::new()
            .csv_flexible(false)
            .profile_stream(source(super::UNCLOSED))
            .await
            .expect_err("strict parsing must refuse the stream");
        assert!(
            err.to_string().contains("ends inside a quoted field"),
            "{err}"
        );
    }
}
