//! Delimiter detection never chooses a delimiter the header does not use.
//!
//! Every reader cuts a record to the header's width. When detection picked a
//! delimiter that split the data rows but left the header as one field, a
//! one-column file of composite keys was profiled as the keys' first parts:
//! `123|456` and `123|789` became two `123`, one distinct value and a
//! duplicate row the input does not contain (#1092).
//!
//! `python/tests/test_csv_delimiter_header.py` is the Python twin, and adds the
//! async routes.

use std::io::Write;

use dataprof::{CsvParserConfig, EngineType, ProfileReport, Profiler, analyze_csv_file};
use tempfile::NamedTempFile;

fn engine(path: &std::path::Path, engine: EngineType) -> Result<ProfileReport, String> {
    Profiler::new()
        .engine(engine)
        .analyze_file(path)
        .map_err(|error| error.to_string())
}

/// Collects every wrong route before failing, so one run shows each route the
/// fix reaches.
#[test]
fn composite_keys_keep_their_full_value_on_every_route() {
    let mut file = NamedTempFile::with_suffix(".csv").unwrap();
    file.write_all(b"key\n123|456\n123|789\n").unwrap();
    file.flush().unwrap();
    let path = file.path();

    let routes = [
        (
            "standard",
            analyze_csv_file(path, &CsvParserConfig::default()).map_err(|e| e.to_string()),
        ),
        ("auto", engine(path, EngineType::Auto)),
        ("incremental", engine(path, EngineType::Incremental)),
        ("columnar", engine(path, EngineType::Columnar)),
    ];

    let mut wrong = Vec::new();
    for (route, outcome) in routes {
        let report = match outcome {
            Ok(report) => report,
            Err(error) => {
                wrong.push(format!("{route} failed: {error}"));
                continue;
            }
        };
        let serialized = serde_json::to_value(&report).expect("serialize report");
        let columns = &serialized["column_profiles"];
        let shape = serde_json::json!({
            "columns": columns.as_array().map(Vec::len),
            "name": columns[0]["name"],
            "unique_count": columns[0]["unique_count"],
            "duplicate_rows": serialized["quality"]["metrics"]["uniqueness"]["duplicate_rows"],
            "ragged_row_count": serialized["execution"]["ragged_row_count"],
        });
        let expected = serde_json::json!({
            "columns": 1,
            "name": "key",
            "unique_count": 2,
            "duplicate_rows": 0,
            "ragged_row_count": 0,
        });
        if shape != expected {
            wrong.push(format!("{route}: {shape}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
