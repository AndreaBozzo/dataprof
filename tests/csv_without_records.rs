//! A CSV file that holds no record at all: empty, or nothing but blank lines.
//!
//! The `csv` crate skips blank lines, so such a file has no header and no rows,
//! and every route reports an empty profile. The columnar engine used to refuse
//! the blank-lines-only file, because Arrow decodes it against a schema of zero
//! fields and fails on the first line it reads (#964).
//!
//! `python/tests/test_csv_without_records.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CsvParserConfig, EngineType, ProfileReport, Profiler, StopCondition, analyze_csv_file,
};
use tempfile::NamedTempFile;

/// Files with no record, by label. The first is the empty file every route
/// already agreed on; the others hold only blank lines, the shape #964 found.
const FILES: [(&str, &[u8]); 6] = [
    ("empty", b""),
    ("BOM then a blank line", b"\xEF\xBB\xBF\n"),
    ("LF blank lines", b"\n\n\n"),
    ("CRLF blank lines", b"\r\n\r\n"),
    ("CR blank lines", b"\r\r"),
    ("one blank line", b"\n"),
];

/// Each way a caller can shape the read: the cap and flexible parsing take
/// their own paths through the columnar engine.
#[derive(Clone, Copy, Debug)]
enum Read {
    Default,
    RowCap,
    Flexible,
}

const READS: [Read; 3] = [Read::Default, Read::RowCap, Read::Flexible];

fn standard(path: &std::path::Path, read: Read) -> Result<ProfileReport, String> {
    let config = match read {
        Read::Default => CsvParserConfig::default(),
        Read::RowCap => CsvParserConfig {
            max_rows: Some(10),
            ..CsvParserConfig::default()
        },
        Read::Flexible => CsvParserConfig {
            flexible: true,
            ..CsvParserConfig::default()
        },
    };
    analyze_csv_file(path, &config).map_err(|error| error.to_string())
}

fn engine(path: &std::path::Path, engine: EngineType, read: Read) -> Result<ProfileReport, String> {
    let profiler = Profiler::new().engine(engine);
    let profiler = match read {
        Read::Default => profiler,
        Read::RowCap => profiler.stop_when(StopCondition::MaxRows(10)),
        Read::Flexible => profiler.csv_flexible(true),
    };
    profiler
        .analyze_file(path)
        .map_err(|error| error.to_string())
}

/// What the routes must agree on: the serialized profiles and quality, and the
/// row and column counts. Engine provenance is compared separately, if at all.
fn summary(report: &ProfileReport) -> serde_json::Value {
    let serialized = serde_json::to_value(report).expect("serialize report");
    serde_json::json!({
        "column_profiles": serialized["column_profiles"],
        "quality": serialized["quality"],
        "quality_status": serialized["quality_status"],
        "rows_processed": report.execution.rows_processed,
        "columns_detected": report.execution.columns_detected,
        "truncation_reason": serialized["execution"]["truncation_reason"],
    })
}

/// Collects every disagreeing (file, read, route) before failing, so one run
/// shows each route the fix reaches.
#[test]
fn a_csv_without_records_profiles_empty_on_every_route() {
    let mut wrong = Vec::new();
    for (label, bytes) in FILES {
        let mut file = NamedTempFile::with_suffix(".csv").unwrap();
        file.write_all(bytes).unwrap();
        file.flush().unwrap();
        let path = file.path();

        for read in READS {
            let routes = [
                ("standard", standard(path, read)),
                ("auto", engine(path, EngineType::Auto, read)),
                ("incremental", engine(path, EngineType::Incremental, read)),
                ("columnar", engine(path, EngineType::Columnar, read)),
            ];
            let expected = match &routes[0].1 {
                Ok(report) => summary(report),
                Err(error) => {
                    wrong.push(format!("[{label}, {read:?}] standard failed: {error}"));
                    continue;
                }
            };
            if expected["rows_processed"] != 0 || expected["columns_detected"] != 0 {
                wrong.push(format!(
                    "[{label}, {read:?}] standard is not empty: {expected}"
                ));
            }
            for (route, outcome) in &routes[1..] {
                match outcome {
                    Ok(report) if summary(report) == expected => {}
                    Ok(report) => wrong.push(format!(
                        "[{label}, {read:?}] {route}: {} != standard {expected}",
                        summary(report)
                    )),
                    Err(error) => {
                        wrong.push(format!("[{label}, {read:?}] {route} failed: {error}"))
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
