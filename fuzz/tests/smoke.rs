use std::fs;
use std::io::Cursor;
use std::path::Path;

use dataprof_core::JsonErrorPolicy;
use dataprof_csv::{CsvParserConfig, analyze_csv_from_reader};
use dataprof_fuzz::{MAX_INPUT_BYTES, MAX_ROWS};
use dataprof_json::{JsonParserConfig, scan_json_from_reader};
use dataprof_runtime::{ProfileReport, REPORT_SCHEMA_VERSION};

fn replay(target: &str, run: fn(&[u8])) {
    let directory = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("seeds")
        .join(target);
    let mut paths = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect::<Vec<_>>();
    paths.sort();
    assert!(!paths.is_empty(), "missing {target} corpus");
    for path in paths {
        let data = fs::read(&path).unwrap();
        assert!(data.len() <= MAX_INPUT_BYTES, "oversized seed: {path:?}");
        eprintln!("replaying {}", path.display());
        run(&data);
    }
}

#[test]
fn csv_corpus() {
    replay("csv", dataprof_fuzz::csv);
}

#[test]
fn json_corpus() {
    replay("json", dataprof_fuzz::json);
}

#[test]
fn report_corpus() {
    replay("report", dataprof_fuzz::report);
}

#[test]
fn csv_decode_failures_do_not_become_defaults() {
    for flexible in [false, true] {
        let config = CsvParserConfig {
            flexible,
            max_rows: Some(MAX_ROWS),
            ..Default::default()
        };
        for bytes in [b"a,a\n1,2\n".as_slice(), b"a\n\xff\n"] {
            assert!(analyze_csv_from_reader(Cursor::new(bytes), &config).is_err());
        }
    }
    let ragged = b"a,b\n1\n2,3,4\n";
    assert!(analyze_csv_from_reader(Cursor::new(ragged), &CsvParserConfig::strict()).is_err());
    let (columns, _, rows, _) =
        analyze_csv_from_reader(Cursor::new(ragged), &CsvParserConfig::default()).unwrap();
    assert_eq!(rows, 2);
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[1].null_count, 1);
}

#[test]
fn json_tolerance_is_explicit_and_zero_field_rows_count() {
    let input = b"{}\nnot json\n{\"z\":1,\"a\":null}\n[]\n{}\n";
    let config = JsonParserConfig::jsonl().with_max_rows(MAX_ROWS);
    let summary = scan_json_from_reader(Cursor::new(input), &config, |_| {}).unwrap();
    assert_eq!(summary.rows_read, 3);
    assert_eq!(summary.malformed_lines, 2);
    let strict = config.with_error_policy(JsonErrorPolicy::Strict);
    assert!(scan_json_from_reader(Cursor::new(input), &strict, |_| {}).is_err());
}

#[test]
fn report_seeds_exercise_success_absence_and_version_rejection() {
    let bytes = include_bytes!("../seeds/report/current.json");
    let current: ProfileReport = serde_json::from_slice(bytes).unwrap();
    assert_eq!(current.schema_version, REPORT_SCHEMA_VERSION);
    assert!(current.quality.is_none());
    let mut document: serde_json::Value = serde_json::from_slice(bytes).unwrap();
    document.as_object_mut().unwrap().remove("schema_version");
    let legacy: ProfileReport = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(legacy.schema_version, 0);
    document["schema_version"] = serde_json::json!(REPORT_SCHEMA_VERSION + 1);
    assert!(serde_json::from_value::<ProfileReport>(document).is_err());

    let assessed: ProfileReport =
        serde_json::from_slice(include_bytes!("../seeds/report/empty-quality.json")).unwrap();
    assert!(assessed.quality.is_some());
    assert!(assessed.column_profiles.is_empty());

    let evidence: ProfileReport =
        serde_json::from_slice(include_bytes!("../seeds/report/absent-and-empty.json")).unwrap();
    assert!(evidence.column_profiles[0].patterns.is_none());
    assert_eq!(evidence.column_profiles[0].unique_count, None);
    assert!(
        evidence.column_profiles[1]
            .patterns
            .as_ref()
            .is_some_and(Vec::is_empty)
    );
    assert_eq!(evidence.column_profiles[1].unique_count, Some(0));
}
