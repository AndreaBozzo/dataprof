//! `csv_flexible` sets parsing strictness and nothing else: with no explicit
//! delimiter, a file profile still detects it.
//!
//! Setting the flag to either value used to skip detection on every
//! synchronous file engine, so a semicolon file was split on commas into one
//! column and the null in it was never counted (#1091).
//!
//! `python/tests/test_csv_flexible_delimiter.py` is the Python twin.

use std::io::Write;

use dataprof::{EngineType, Profiler};
use tempfile::NamedTempFile;

const ENGINES: [EngineType; 3] = [
    EngineType::Auto,
    EngineType::Incremental,
    EngineType::Columnar,
];

/// Collects every wrong (engine, flag) before failing, so one run shows each
/// route the fix reaches.
#[test]
fn csv_flexible_keeps_delimiter_detection_on_every_engine() {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    file.write_all(b"id;value\n1;10\n2;\n").unwrap();
    file.flush().unwrap();

    let mut wrong = Vec::new();
    for engine in ENGINES {
        for flexible in [None, Some(true), Some(false)] {
            let profiler = Profiler::new().engine(engine);
            let profiler = match flexible {
                Some(flag) => profiler.csv_flexible(flag),
                None => profiler,
            };
            let report = match profiler.analyze_file(file.path()) {
                Ok(report) => report,
                Err(error) => {
                    wrong.push(format!("[{engine:?}, {flexible:?}] failed: {error}"));
                    continue;
                }
            };
            let columns: Vec<(&str, usize)> = report
                .column_profiles
                .iter()
                .map(|column| (column.name.as_str(), column.null_count))
                .collect();
            if columns != [("id", 0), ("value", 1)] {
                wrong.push(format!("[{engine:?}, {flexible:?}] columns {columns:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
