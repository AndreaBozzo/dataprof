//! The metric packs a run computed, recorded in `analysis_settings` (#1030).
//!
//! Deselecting a pack moves a score on unchanged data: without `patterns` no
//! column has detected patterns, so validity is not assessed and the overall
//! score averages the other dimensions. The report did not say which packs
//! ran, so two runs of the same bytes with different `metrics=` read as a
//! score change. `analysis_settings.metric_packs` now records them,
//! `compare()` lists them under `configuration`, and the baseline gate names
//! a `patterns` difference as the cause of a validity or overall change.
//!
//! `python/tests/test_metric_packs_settings.py` is the Python twin.

use std::io::Write;

use dataprof::{
    CsvParserConfig, EngineType, MetricPack, ProfileReport, Profiler, QualityDimension,
    QualityPolicy, ReportDelta, ReportSide, analyze_csv_file,
};
use serde_json::{Value, json};
use tempfile::NamedTempFile;

/// Six of seven emails are valid, so validity is assessed when patterns run.
const ROWS: [(u32, &str); 7] = [
    (1, "ada@example.com"),
    (2, "bob@example.com"),
    (3, "cy@example.com"),
    (4, "di@example.com"),
    (5, "ed@example.com"),
    (6, "fay@example.com"),
    (7, "not an email"),
];

fn fixture(suffix: &str, contents: &str) -> NamedTempFile {
    let mut file = NamedTempFile::with_suffix(suffix).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    file
}

fn csv_text() -> String {
    let mut text = String::from("id,email\n");
    for (id, email) in ROWS {
        text.push_str(&format!("{id},{email}\n"));
    }
    text
}

fn records() -> Vec<String> {
    ROWS.iter()
        .map(|(id, email)| format!(r#"{{"id":{id},"email":"{email}"}}"#))
        .collect()
}

#[cfg(feature = "parquet")]
fn parquet_fixture() -> NamedTempFile {
    use std::sync::Arc;

    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("email", DataType::Utf8, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                ROWS.iter().map(|row| i64::from(row.0)).collect::<Vec<_>>(),
            )),
            Arc::new(StringArray::from(
                ROWS.iter().map(|row| row.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let file = NamedTempFile::with_suffix(".parquet").unwrap();
    let mut writer = ArrowWriter::try_new(file.reopen().unwrap(), schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    file
}

/// A selection as the caller makes it, and the packs the report must record.
struct Selection {
    label: &'static str,
    packs: Option<Vec<MetricPack>>,
    no_dimensions: bool,
    recorded: Vec<&'static str>,
}

fn selections() -> Vec<Selection> {
    use MetricPack::{Patterns, Quality, Statistics};
    vec![
        Selection {
            label: "default",
            packs: None,
            no_dimensions: false,
            recorded: vec!["schema", "statistics", "patterns", "quality"],
        },
        Selection {
            label: "quality only",
            packs: Some(vec![Quality]),
            no_dimensions: false,
            recorded: vec!["schema", "quality"],
        },
        Selection {
            label: "repeated, out of order",
            packs: Some(vec![Patterns, Statistics, Patterns]),
            no_dimensions: false,
            recorded: vec!["schema", "statistics", "patterns"],
        },
        Selection {
            label: "none",
            packs: Some(vec![]),
            no_dimensions: false,
            recorded: vec!["schema"],
        },
        Selection {
            label: "no quality dimensions",
            packs: None,
            no_dimensions: true,
            recorded: vec!["schema", "statistics", "patterns"],
        },
    ]
}

fn profiler(selection: &Selection) -> Profiler {
    let mut profiler = Profiler::new();
    if let Some(packs) = &selection.packs {
        profiler = profiler.metric_packs(packs.clone());
    }
    if selection.no_dimensions {
        profiler = profiler.quality_dimensions(vec![]);
    }
    profiler
}

/// Every engine over the CSV, then every other format, under `selection`.
fn reports(selection: &Selection) -> Vec<(String, ProfileReport)> {
    let csv = fixture(".csv", &csv_text());
    let mut reports = Vec::new();
    // The standard CSV entry point takes no selection: it computes every pack.
    if selection.packs.is_none() && !selection.no_dimensions {
        reports.push((
            "standard".to_string(),
            analyze_csv_file(csv.path(), &CsvParserConfig::default()).expect("standard CSV"),
        ));
    }
    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let report = profiler(selection)
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
        let report = profiler(selection)
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
                .block_on(profiler(selection).profile_stream(source))
                .unwrap_or_else(|e| panic!("[{label}] {e}"));
            reports.push((label.to_string(), report));
        }
    }
    reports
}

/// The packs as each form of `report` carries them: the report, its
/// canonical document read back, and its flat summary.
fn recorded_forms(report: &ProfileReport) -> Vec<(&'static str, Value)> {
    let canonical: ProfileReport = serde_json::from_str(&report.to_json().unwrap()).unwrap();
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    let packs = |report: &ProfileReport| {
        serde_json::to_value(
            report
                .analysis_settings
                .as_ref()
                .and_then(|settings| settings.metric_packs.clone()),
        )
        .unwrap()
    };
    vec![
        ("report", packs(report)),
        ("canonical", packs(&canonical)),
        (
            "summary",
            summary["analysis_settings"]["metric_packs"].clone(),
        ),
    ]
}

#[test]
fn every_route_records_the_packs_it_computed() {
    let mut wrong = Vec::new();
    for selection in selections() {
        let want = json!(selection.recorded);
        for (route, report) in reports(&selection) {
            for (form, packs) in recorded_forms(&report) {
                if packs != want {
                    wrong.push(format!(
                        "[{}] {route} {form}: {packs} != {want}",
                        selection.label
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

fn profile_csv(packs: Option<Vec<MetricPack>>) -> ProfileReport {
    let mut profiler = Profiler::new();
    if let Some(packs) = packs {
        profiler = profiler.metric_packs(packs);
    }
    profiler
        .analyze_file(fixture(".csv", &csv_text()).path())
        .unwrap()
}

/// A canonical document as a release before 0.14 wrote it: settings with no
/// `metric_packs`.
fn without_packs(report: &ProfileReport) -> ProfileReport {
    let mut document: Value = serde_json::from_str(&report.to_json().unwrap()).unwrap();
    let removed = document["analysis_settings"]
        .as_object_mut()
        .unwrap()
        .remove("metric_packs");
    assert!(removed.is_some());
    serde_json::from_value(document).unwrap()
}

#[test]
fn a_document_without_packs_loads_them_as_unrecorded() {
    let report = without_packs(&profile_csv(None));
    let settings = report.analysis_settings.as_ref().unwrap();
    assert_eq!(settings.metric_packs, None);
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    assert!(
        summary["analysis_settings"]
            .as_object()
            .unwrap()
            .get("metric_packs")
            .is_none()
    );
}

#[test]
fn a_null_or_unknown_pack_list_is_malformed() {
    let document: Value = serde_json::from_str(&profile_csv(None).to_json().unwrap()).unwrap();
    for (label, packs) in [
        ("null", Value::Null),
        ("unknown name", json!(["schema", "lineage"])),
        ("not a list", json!("schema")),
    ] {
        let mut document = document.clone();
        document["analysis_settings"]["metric_packs"] = packs;
        let read: Result<ProfileReport, _> = serde_json::from_value(document);
        assert!(read.is_err(), "{label} loaded");
    }
}

/// `configuration.metric_packs` in every pairing of report and summary.
fn configurations(a: &ProfileReport, b: &ProfileReport) -> Vec<(&'static str, Value)> {
    let native = |report: &ProfileReport| ReportSide::from_report(report);
    let flat = |report: &ProfileReport| {
        ReportSide::from_summary_json(&report.summary_json().unwrap()).unwrap()
    };
    let packs = |a: ReportSide, b: ReportSide| {
        serde_json::to_value(ReportDelta::between_sides(&a, &b)).unwrap()["configuration"]
            ["metric_packs"]
            .clone()
    };
    vec![
        ("report/report", packs(native(a), native(b))),
        ("summary/summary", packs(flat(a), flat(b))),
        ("report/summary", packs(native(a), flat(b))),
        ("summary/report", packs(flat(a), native(b))),
    ]
}

#[test]
fn compare_lists_each_sides_packs() {
    use MetricPack::{Patterns, Quality, Statistics};
    let all = profile_csv(None);
    let quality = profile_csv(Some(vec![Quality]));
    let reordered = profile_csv(Some(vec![Quality, Patterns, Statistics]));
    let legacy = without_packs(&all);
    let every = json!(["schema", "statistics", "patterns", "quality"]);
    let cases = [
        (
            "differ",
            &all,
            &quality,
            json!({"a": every, "b": ["schema", "quality"], "same": false}),
        ),
        (
            "same packs, other order",
            &all,
            &reordered,
            json!({"a": every, "b": every, "same": true}),
        ),
        (
            "baseline before 0.14",
            &legacy,
            &all,
            json!({"a": null, "b": every, "same": null}),
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

/// The outcome of each score rule, overall first, then validity: its status,
/// and its reason and setting when it was not evaluated.
fn gate(report: &ProfileReport, baseline: &ProfileReport) -> Vec<String> {
    let result = QualityPolicy::new()
        .max_quality_score_drop(0.0)
        .max_dimension_score_drop(QualityDimension::Validity, 0.0)
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

const CHANGED: &str = "not_evaluated setting_changed metric_packs";

#[test]
fn the_gate_names_a_patterns_difference_as_the_cause() {
    use MetricPack::{Patterns, Quality, Statistics};
    let all = profile_csv(None);
    let no_patterns = profile_csv(Some(vec![Statistics, Quality]));
    let no_statistics = profile_csv(Some(vec![Patterns, Quality]));
    let cases = [
        // Same bytes, patterns on one side only: validity is assessed on one
        // side, and the overall score averages it there.
        (
            "patterns dropped",
            &no_patterns,
            &all,
            vec![CHANGED, CHANGED],
        ),
        ("patterns added", &all, &no_patterns, vec![CHANGED, CHANGED]),
        // `statistics` moves no score, so the rules decide.
        (
            "statistics dropped",
            &no_statistics,
            &all,
            vec!["passed", "passed"],
        ),
        // A baseline before 0.14 leaves the reasons that name the effect.
        (
            "baseline before 0.14",
            &no_patterns,
            &without_packs(&all),
            vec![
                "not_evaluated dimensions_changed",
                "not_evaluated not_assessed",
            ],
        ),
        (
            "baseline before 0.14, same packs",
            &all,
            &without_packs(&all),
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

#[test]
fn packs_that_differ_without_moving_validity_leave_the_overall_rule_decided() {
    use MetricPack::{Quality, Statistics};
    // No column holds a pattern, so validity is assessed on neither side and
    // the overall score averages the same dimensions.
    let text = "id,amount\n1,10\n2,20\n3,30\n";
    let profile = |packs: Option<Vec<MetricPack>>| {
        let mut profiler = Profiler::new();
        if let Some(packs) = packs {
            profiler = profiler.metric_packs(packs);
        }
        profiler.analyze_file(fixture(".csv", text).path()).unwrap()
    };
    let all = profile(None);
    let no_patterns = profile(Some(vec![Statistics, Quality]));
    let statuses = gate(&no_patterns, &all);
    assert_eq!(statuses, vec!["passed", CHANGED]);
}

#[cfg(feature = "sqlite")]
#[test]
fn a_database_query_records_its_packs() {
    use dataprof::DatabaseConfig;
    use sqlx::sqlite::SqlitePoolOptions;

    let runtime = tokio::runtime::Runtime::new().unwrap();
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("packs.db");
    std::fs::File::create(&db_path).unwrap();
    let db_path = db_path.display().to_string();
    runtime.block_on(async {
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect(&format!("sqlite://{db_path}"))
            .await
            .unwrap();
        sqlx::query("CREATE TABLE t (id INTEGER, email TEXT)")
            .execute(&pool)
            .await
            .unwrap();
        for (id, email) in ROWS {
            sqlx::query("INSERT INTO t VALUES (?, ?)")
                .bind(i64::from(id))
                .bind(email)
                .execute(&pool)
                .await
                .unwrap();
        }
        pool.close().await;
    });
    let profiler = |packs: Option<Vec<MetricPack>>| {
        let mut profiler = Profiler::new().database(DatabaseConfig {
            connection_string: db_path.clone(),
            load_credentials_from_env: false,
            ..Default::default()
        });
        if let Some(packs) = packs {
            profiler = profiler.metric_packs(packs);
        }
        profiler
    };
    let query = "SELECT * FROM t";
    let cases = [
        (
            "default",
            runtime.block_on(profiler(None).analyze_query(query)),
            json!(["schema", "statistics", "patterns", "quality"]),
        ),
        (
            "quality only",
            runtime.block_on(profiler(Some(vec![MetricPack::Quality])).analyze_query(query)),
            json!(["schema", "quality"]),
        ),
        (
            "without quality",
            runtime.block_on(profiler(None).analyze_query_no_quality(query)),
            json!(["schema", "statistics", "patterns"]),
        ),
    ];
    let mut wrong = Vec::new();
    for (label, report, want) in cases {
        for (form, packs) in recorded_forms(&report.unwrap()) {
            if packs != want {
                wrong.push(format!("[{label}] {form}: {packs} != {want}"));
            }
        }
    }
    assert!(
        wrong.is_empty(),
        "{}",
        wrong.join(
            "
"
        )
    );
}
