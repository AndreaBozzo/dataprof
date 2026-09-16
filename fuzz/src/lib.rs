//! Bounded, deterministic invariants shared by libFuzzer and stable corpus replay.
//! Parsing errors are expected outcomes; panics and failed invariants propagate.

use std::collections::HashSet;
use std::io::Cursor;

use dataprof_core::{ColumnProfile, DataProfilerError, JsonErrorPolicy};
use dataprof_csv::{CsvParserConfig, analyze_csv_from_reader, detect_delimiter};
use dataprof_json::{
    JsonFormat, JsonParserConfig, analyze_json_from_reader, scan_json_from_reader,
};
use dataprof_runtime::{ProfileReport, QualityAnalysisStatus, REPORT_SCHEMA_VERSION};
use serde_json::Value;

pub const MAX_INPUT_BYTES: usize = 4096;
pub const MAX_ROWS: usize = 32;

fn check_columns(columns: &[ColumnProfile], rows: usize) {
    assert!(rows <= MAX_ROWS);
    let names: HashSet<_> = columns.iter().map(|column| &column.name).collect();
    assert_eq!(names.len(), columns.len(), "duplicate output columns");
    for column in columns {
        assert_eq!(column.total_count, rows);
        assert!(column.null_count <= column.total_count);
        if let Some(unique_count) = column.unique_count {
            assert!(unique_count <= column.total_count);
        }
    }
    // A successful parse must produce valid JSON, even for extreme numbers.
    let encoded = serde_json::to_vec(columns).expect("serialize parsed columns");
    let _: Value = serde_json::from_slice(&encoded).expect("columns are valid JSON");
}

fn check_error(error: &DataProfilerError) {
    assert!(
        !error.to_string().is_empty(),
        "typed error lost its context"
    );
}

pub fn csv(data: &[u8]) {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }
    let delimiter = detect_delimiter(Cursor::new(data)).expect("in-memory delimiter detection");
    assert!(b",;\t|".contains(&delimiter));
    for has_header in [false, true] {
        for flexible in [false, true] {
            // Exercise both detected delimiters and the explicit comma path.
            for delimiter in [None, Some(b',')] {
                let config = CsvParserConfig {
                    has_header,
                    flexible,
                    delimiter,
                    max_rows: Some(MAX_ROWS),
                    ..Default::default()
                };
                match analyze_csv_from_reader(Cursor::new(data), &config) {
                    Ok((columns, _, rows, headers)) => {
                        check_columns(&columns, rows);
                        assert_eq!(
                            columns
                                .iter()
                                .map(|column| &column.name)
                                .collect::<Vec<_>>(),
                            headers.iter().collect::<Vec<_>>()
                        );
                    }
                    Err(error) => check_error(&error),
                }
            }
        }
    }
}

pub fn json(data: &[u8]) {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }
    for format in [None, Some(JsonFormat::Json), Some(JsonFormat::Jsonl)] {
        for error_policy in [JsonErrorPolicy::Strict, JsonErrorPolicy::Skip] {
            let config = JsonParserConfig {
                format,
                error_policy,
                max_rows: Some(MAX_ROWS),
            };
            let mut observed_rows = 0;
            let mut names = Vec::new();
            let mut seen = HashSet::new();
            let scan = scan_json_from_reader(Cursor::new(data), &config, |object| {
                observed_rows += 1;
                assert!(observed_rows <= MAX_ROWS);
                for name in object.keys() {
                    if seen.insert(name.clone()) {
                        names.push(name.clone());
                    }
                }
            });
            let analyzed = analyze_json_from_reader(Cursor::new(data), &config);
            match (scan, analyzed) {
                (Ok(summary), Ok((columns, _, rows, skipped, detected_format))) => {
                    check_columns(&columns, rows);
                    assert_eq!(rows, observed_rows);
                    assert_eq!(rows, summary.rows_read);
                    assert_eq!(skipped, summary.malformed_lines);
                    assert_eq!(detected_format, summary.format);
                    if error_policy == JsonErrorPolicy::Strict {
                        assert_eq!(skipped, 0);
                    }
                    assert_eq!(
                        columns
                            .iter()
                            .map(|column| &column.name)
                            .collect::<Vec<_>>(),
                        names.iter().collect::<Vec<_>>()
                    );
                }
                (Err(scan_error), Err(analyze_error)) => {
                    check_error(&scan_error);
                    check_error(&analyze_error);
                }
                _ => panic!("JSON scanner and profiler disagree on parse success"),
            }
        }
    }
}

/// The recorded row selection, read through the legacy `scan_info` alias as
/// well so a legacy document is compared against its canonical re-emission.
/// An explicit null is the absence serde cannot tell it apart from; an empty
/// list stays distinct from both.
fn sampled_row_ranges(document: &Value) -> Option<&Value> {
    [
        "/execution/sampled_row_ranges",
        "/scan_info/sampled_row_ranges",
    ]
    .into_iter()
    .find_map(|pointer| document.pointer(pointer))
    .filter(|ranges| !ranges.is_null())
}

pub fn report(data: &[u8]) {
    if data.len() > MAX_INPUT_BYTES {
        return;
    }
    let parsed: Result<ProfileReport, serde_json::Error> = serde_json::from_slice(data);
    let Ok(input) = serde_json::from_slice::<Value>(data) else {
        assert!(parsed.is_err(), "non-JSON input was accepted as a report");
        return;
    };

    // Documents a reader must refuse outright. Each mirrors a deliberate guard:
    // repairing any of them would invent a fact the document never recorded.
    if input
        .get("schema_version")
        .and_then(Value::as_u64)
        .is_some_and(|version| version > u64::from(REPORT_SCHEMA_VERSION))
    {
        assert!(parsed.is_err(), "future report version was accepted");
    }
    if input.get("quality_status") == Some(&Value::Null) {
        assert!(parsed.is_err(), "explicit null quality_status was accepted");
    }
    if let Some(state) = input
        .get("quality_status")
        .and_then(|status| status.get("state"))
        .and_then(Value::as_str)
    {
        // The status and the assessment are two halves of one fact. A document
        // pairing them any other way is malformed, whichever half is missing.
        let assessed = input
            .get("quality")
            .is_some_and(|quality| !quality.is_null());
        if (state == "computed") != assessed {
            assert!(
                parsed.is_err(),
                "contradictory quality pairing was accepted"
            );
        }
    }

    let Ok(report) = parsed else {
        return;
    };
    assert!(report.schema_version <= REPORT_SCHEMA_VERSION);
    assert_eq!(
        matches!(report.quality_status, QualityAnalysisStatus::Computed),
        report.quality.is_some(),
        "quality presence contradicts its computation status"
    );
    match input.get("schema_version") {
        None => assert_eq!(report.schema_version, 0),
        Some(version) => assert_eq!(version.as_u64(), Some(u64::from(report.schema_version))),
    }
    let encoded = serde_json::to_vec(&report).expect("serialize accepted report");
    let document: Value = serde_json::from_slice(&encoded).expect("report is valid JSON");
    let restored: ProfileReport = serde_json::from_slice(&encoded).expect("reload accepted report");
    assert_eq!(restored.schema_version, report.schema_version);
    assert_eq!(restored.quality.is_none(), report.quality.is_none());
    assert_eq!(restored.quality_status, report.quality_status);
    // "Not analyzed" and "analyzed, nothing found" are different answers, and
    // only the input says which one this was.
    assert_eq!(
        sampled_row_ranges(&input),
        sampled_row_ranges(&document),
        "row selection lost the difference between absent and empty"
    );
    // Compare the entire emitted document: absent, null and empty values
    // remain distinct, including optional statistics and evidence.
    assert_eq!(serde_json::to_value(&restored).unwrap(), document);
}
