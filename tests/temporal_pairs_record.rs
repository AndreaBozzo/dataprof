//! Which column pairs with which in `temporal_pairs`, recorded in
//! `analysis_settings` (#1029).
//!
//! A report recorded the columns `temporal_pairs` named, as `temporal_pair`
//! hint bindings, but not which column paired with which. Two runs that pair
//! the same columns differently score timeliness differently on the same
//! data, so the baseline gate read any non-empty `temporal_pairs` as
//! `setting_unrecorded` and left every timeliness and overall drop rule
//! between two such reports unevaluated. `analysis_settings.temporal_pairs`
//! now records the pairs, `compare()` lists them under `configuration`, and
//! the gate compares them.
//!
//! `python/tests/test_temporal_pairs_record.py` is the Python twin.

use std::io::Write;

use dataprof::{
    EngineType, ProfileReport, Profiler, QualityDimension, QualityPolicy, ReportDelta, ReportSide,
};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

/// Three date columns whose names carry no role word, so only a declared
/// pair compares them. `alpha` precedes `beta` on every row; `gamma` falls
/// before both on the last row.
const ROWS: [[&str; 3]; 3] = [
    ["2024-01-01", "2024-02-01", "2024-03-01"],
    ["2024-01-05", "2024-02-05", "2024-03-05"],
    ["2024-01-10", "2024-02-10", "2024-01-02"],
];
const NAMES: [&str; 3] = ["alpha", "beta", "gamma"];

fn fixture(suffix: &str, contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn csv_text() -> String {
    let mut text = NAMES.join(",") + "\n";
    for row in ROWS {
        text.push_str(&(row.join(",") + "\n"));
    }
    text
}

fn records() -> Vec<String> {
    ROWS.iter()
        .map(|row| {
            let fields: Vec<String> = NAMES
                .iter()
                .zip(row)
                .map(|(name, value)| format!(r#""{name}":"{value}""#))
                .collect();
            format!("{{{}}}", fields.join(","))
        })
        .collect()
}

#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{ArrayRef, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(
        NAMES
            .iter()
            .map(|name| Field::new(*name, DataType::Utf8, false))
            .collect::<Vec<_>>(),
    ));
    let columns: Vec<ArrayRef> = (0..NAMES.len())
        .map(|i| {
            Arc::new(StringArray::from(
                ROWS.iter().map(|row| row[i]).collect::<Vec<_>>(),
            )) as ArrayRef
        })
        .collect();
    let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

fn pairs(list: &[(&str, &str)]) -> Vec<(String, String)> {
    list.iter()
        .map(|(start, end)| (start.to_string(), end.to_string()))
        .collect()
}

/// A declaration as the caller makes it, and the pairs the report records.
struct Declaration {
    label: &'static str,
    pairs: Vec<(String, String)>,
    recorded: Value,
}

fn declarations() -> Vec<Declaration> {
    vec![
        Declaration {
            label: "none",
            pairs: vec![],
            recorded: json!([]),
        },
        Declaration {
            label: "one",
            pairs: pairs(&[("beta", "alpha")]),
            recorded: json!([{"start": "beta", "end": "alpha"}]),
        },
        Declaration {
            label: "repeated, out of order",
            pairs: pairs(&[("beta", "gamma"), ("alpha", "beta"), ("beta", "gamma")]),
            recorded: json!([
                {"start": "alpha", "end": "beta"},
                {"start": "beta", "end": "gamma"},
            ]),
        },
    ]
}

fn profiler(pairs: &[(String, String)]) -> Profiler {
    Profiler::new().temporal_pairs(pairs.to_vec())
}

/// Every engine over the CSV, then every other format, with `pairs` declared.
fn reports(pairs: &[(String, String)]) -> Vec<(String, ProfileReport)> {
    let csv = fixture(".csv", &csv_text());
    let mut reports = Vec::new();
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = profiler(pairs)
            .engine(engine)
            .analyze_file(csv.path())
            .unwrap_or_else(|e| panic!("[{engine:?}] {e}"));
        reports.push((format!("{engine:?}"), report));
    }
    let mut files = vec![
        (
            "json",
            fixture(".json", &format!("[{}]", records().join(","))),
        ),
        ("jsonl", fixture(".jsonl", &(records().join("\n") + "\n"))),
    ];
    #[cfg(feature = "parquet")]
    files.push(("parquet", parquet_fixture()));
    for (label, file) in files {
        let report = profiler(pairs)
            .analyze_file(file.path())
            .unwrap_or_else(|e| panic!("[{label}] {e}"));
        reports.push((label.to_string(), report));
    }
    #[cfg(feature = "async-streaming")]
    {
        use dataprof::{AsyncSourceInfo, BytesSource, FileFormat};
        let runtime = tokio::runtime::Runtime::new().unwrap();
        for (label, format, body) in [
            ("async csv", FileFormat::Csv, csv_text()),
            (
                "async jsonl",
                FileFormat::Jsonl,
                records().join("\n") + "\n",
            ),
        ] {
            let source = BytesSource::new(
                bytes::Bytes::from(body),
                AsyncSourceInfo::new("upload", format),
            );
            let report = runtime
                .block_on(profiler(pairs).profile_stream(source))
                .unwrap_or_else(|e| panic!("[{label}] {e}"));
            reports.push((label.to_string(), report));
        }
    }
    reports
}

/// The pairs as each form of `report` carries them: the report, its
/// canonical document read back, and its flat summary.
fn recorded_forms(report: &ProfileReport) -> Vec<(&'static str, Value)> {
    let canonical: ProfileReport = serde_json::from_str(&report.to_json().unwrap()).unwrap();
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    let pairs = |report: &ProfileReport| {
        serde_json::to_value(
            report
                .analysis_settings
                .as_ref()
                .and_then(|settings| settings.temporal_pairs.clone()),
        )
        .unwrap()
    };
    vec![
        ("report", pairs(report)),
        ("canonical", pairs(&canonical)),
        (
            "summary",
            summary["analysis_settings"]["temporal_pairs"].clone(),
        ),
    ]
}

#[test]
fn every_route_records_the_declared_pairs() {
    let mut wrong = Vec::new();
    for declaration in declarations() {
        for (route, report) in reports(&declaration.pairs) {
            for (form, recorded) in recorded_forms(&report) {
                if recorded != declaration.recorded {
                    wrong.push(format!(
                        "[{}] {route} {form}: {recorded} != {}",
                        declaration.label, declaration.recorded
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

fn profile_csv(list: &[(&str, &str)]) -> ProfileReport {
    profiler(&pairs(list))
        .analyze_file(fixture(".csv", &csv_text()).path())
        .unwrap()
}

/// A canonical document as a release before 0.14 wrote it: settings with no
/// `temporal_pairs`.
fn without_pairs(report: &ProfileReport) -> ProfileReport {
    let mut document: Value = serde_json::from_str(&report.to_json().unwrap()).unwrap();
    let removed = document["analysis_settings"]
        .as_object_mut()
        .unwrap()
        .remove("temporal_pairs");
    assert!(removed.is_some());
    serde_json::from_value(document).unwrap()
}

#[test]
fn a_document_without_pairs_loads_them_as_unrecorded() {
    let report = without_pairs(&profile_csv(&[("alpha", "beta")]));
    let settings = report.analysis_settings.as_ref().unwrap();
    assert_eq!(settings.temporal_pairs, None);
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    assert!(
        summary["analysis_settings"]
            .as_object()
            .unwrap()
            .get("temporal_pairs")
            .is_none()
    );
}

#[test]
fn a_null_or_malformed_pair_list_is_rejected() {
    let document: Value =
        serde_json::from_str(&profile_csv(&[("alpha", "beta")]).to_json().unwrap()).unwrap();
    for (label, pairs) in [
        ("null", Value::Null),
        ("not a list", json!({"start": "alpha", "end": "beta"})),
        ("a tuple", json!([["alpha", "beta"]])),
        ("no end", json!([{"start": "alpha"}])),
        ("a null end", json!([{"start": "alpha", "end": null}])),
    ] {
        let mut document = document.clone();
        document["analysis_settings"]["temporal_pairs"] = pairs;
        let read: Result<ProfileReport, _> = serde_json::from_value(document);
        assert!(read.is_err(), "{label} loaded");
    }
}

/// `configuration.temporal_pairs` in every pairing of report and summary.
fn configurations(a: &ProfileReport, b: &ProfileReport) -> Vec<(&'static str, Value)> {
    let native = |report: &ProfileReport| ReportSide::from_report(report);
    let flat = |report: &ProfileReport| {
        ReportSide::from_summary_json(&report.summary_json().unwrap()).unwrap()
    };
    let pairs = |a: ReportSide, b: ReportSide| {
        serde_json::to_value(ReportDelta::between_sides(&a, &b)).unwrap()["configuration"]
            ["temporal_pairs"]
            .clone()
    };
    vec![
        ("report/report", pairs(native(a), native(b))),
        ("summary/summary", pairs(flat(a), flat(b))),
        ("report/summary", pairs(native(a), flat(b))),
        ("summary/report", pairs(flat(a), native(b))),
    ]
}

#[test]
fn compare_lists_each_sides_pairs() {
    let forward = profile_csv(&[("alpha", "beta")]);
    let reversed = profile_csv(&[("beta", "alpha")]);
    let chained = profile_csv(&[("alpha", "beta"), ("beta", "gamma")]);
    let shuffled = profile_csv(&[("beta", "gamma"), ("alpha", "beta"), ("alpha", "beta")]);
    let none = profile_csv(&[]);
    let ab = json!([{"start": "alpha", "end": "beta"}]);
    let ba = json!([{"start": "beta", "end": "alpha"}]);
    let chain = json!([
        {"start": "alpha", "end": "beta"},
        {"start": "beta", "end": "gamma"},
    ]);
    let cases = [
        (
            "reversed",
            &forward,
            &reversed,
            json!({"a": ab, "b": ba, "same": false}),
        ),
        (
            "same pairs, other order and repetition",
            &chained,
            &shuffled,
            json!({"a": chain, "b": chain, "same": true}),
        ),
        (
            "none declared on one side",
            &none,
            &forward,
            json!({"a": [], "b": ab, "same": false}),
        ),
        (
            "baseline before 0.14",
            &without_pairs(&forward),
            &forward,
            json!({"a": null, "b": ab, "same": null}),
        ),
    ];
    let mut wrong = Vec::new();
    for (label, a, b, want) in cases {
        for (route, got) in configurations(a, b) {
            if got != want {
                wrong.push(format!("[{label}] {route}: {got} != {want}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn compare_reads_a_hand_written_list_as_a_set_of_directed_pairs() {
    let report = profile_csv(&[("alpha", "beta"), ("beta", "gamma")]);
    let mut document: Value = serde_json::from_str(&report.to_json().unwrap()).unwrap();
    document["analysis_settings"]["temporal_pairs"] = json!([
        {"start": "beta", "end": "gamma"},
        {"start": "alpha", "end": "beta"},
        {"start": "beta", "end": "gamma"},
    ]);
    let edited: ProfileReport = serde_json::from_value(document).unwrap();
    let delta = ReportDelta::between_sides(
        &ReportSide::from_report(&report),
        &ReportSide::from_report(&edited),
    );
    assert_eq!(delta.configuration.temporal_pairs.same, Some(true));
}

/// The outcome of each score rule, overall first, then timeliness: its
/// status, and its reason and setting when it was not evaluated.
fn gate(report: &ProfileReport, baseline: &ProfileReport) -> Vec<String> {
    let result = QualityPolicy::new()
        .max_quality_score_drop(0.0)
        .max_dimension_score_drop(QualityDimension::Timeliness, 0.0)
        .evaluate_against(report, &ReportSide::from_report(baseline))
        .unwrap();
    serde_json::to_value(&result).unwrap()["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|check| {
            [&check["status"], &check["reason"], &check["setting"]]
                .into_iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

const CHANGED: &str = "not_evaluated setting_changed temporal_pairs";
const UNRECORDED: &str = "not_evaluated setting_unrecorded temporal_pairs";

#[test]
fn the_gate_compares_the_recorded_pairs() {
    let forward = profile_csv(&[("alpha", "beta")]);
    let reversed = profile_csv(&[("beta", "alpha")]);
    let chained = profile_csv(&[("alpha", "beta"), ("beta", "gamma")]);
    let shuffled = profile_csv(&[("beta", "gamma"), ("alpha", "beta"), ("alpha", "beta")]);
    // The same three columns, paired otherwise.
    let regrouped = profile_csv(&[("alpha", "gamma"), ("beta", "gamma")]);
    let none = profile_csv(&[]);
    let cases = [
        (
            "same pairs, other order and repetition",
            &shuffled,
            &chained,
            vec!["passed", "passed"],
        ),
        ("reversed", &reversed, &forward, vec![CHANGED, CHANGED]),
        (
            "same columns, other pairs",
            &regrouped,
            &chained,
            vec![CHANGED, CHANGED],
        ),
        (
            "declared on one side",
            &forward,
            &none,
            vec![CHANGED, CHANGED],
        ),
        // A baseline before 0.14 keeps the rule over the pair columns.
        (
            "baseline before 0.14",
            &chained,
            &without_pairs(&chained),
            vec![UNRECORDED, UNRECORDED],
        ),
        (
            "baseline before 0.14, no pairs",
            &none,
            &without_pairs(&none),
            vec!["passed", "passed"],
        ),
    ];
    let mut wrong = Vec::new();
    for (label, report, baseline, want) in cases {
        let got = gate(report, baseline);
        if got != want {
            wrong.push(format!("[{label}] {got:?} != {want:?}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[cfg(feature = "sqlite")]
#[test]
fn a_database_query_records_no_pairs() {
    use dataprof::DatabaseConfig;
    use sqlx::sqlite::SqlitePoolOptions;

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("pairs.db");
    std::fs::File::create(&db_path).unwrap();
    let db_path = db_path.display().to_string();
    runtime.block_on(async {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{db_path}"))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (alpha TEXT, beta TEXT, gamma TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        for row in ROWS {
            sqlx::query("INSERT INTO t VALUES (?, ?, ?)")
                .bind(row[0])
                .bind(row[1])
                .bind(row[2])
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;
    });
    // Database profiling takes no semantic hints yet, so a query records
    // that it declared none: an empty list, never an absent one.
    let report = runtime
        .block_on(
            Profiler::new()
                .database(DatabaseConfig {
                    connection_string: db_path.clone(),
                    load_credentials_from_env: false,
                    ..Default::default()
                })
                .analyze_query("SELECT * FROM t"),
        )
        .unwrap();
    for (form, recorded) in recorded_forms(&report) {
        assert_eq!(recorded, json!([]), "{form}");
    }
}
