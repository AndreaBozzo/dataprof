//! Reports record the columns they were profiled with, so a baseline gate can
//! tell a column the source lacks from one that was not selected (#749).
//!
//! `python/tests/test_column_selection_record.py` is the Python twin; it also
//! covers the in-memory, DataFrame, Arrow and Parquet routes.

use std::io::Write;

use dataprof::{
    ColumnSelection, DiagnosticCode, EngineType, GateResult, MetricPack, ProfileReport, Profiler,
    QualityPolicy, ReportSide, SettingDifference, analyze_csv_file,
};
use serde_json::{Value, json};

const ORDERS: &str = "id,amount,note\n1,10,a\n2,20,b\n3,30,c\n";

fn file(suffix: &str, contents: &str) -> tempfile::NamedTempFile {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file
}

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|name| (*name).to_string()).collect()
}

fn recorded(report: &ProfileReport) -> Option<ColumnSelection> {
    report
        .analysis_settings
        .as_ref()
        .map(|settings| settings.columns.clone())
}

fn listed(values: &[&str]) -> ColumnSelection {
    serde_json::from_value(json!({"selection": "listed", "names": values})).unwrap()
}

#[test]
fn every_engine_and_file_format_records_the_selection() {
    let csv = file(".csv", ORDERS);
    let json = file(
        ".json",
        r#"[{"id": 1, "amount": 10, "note": "a"}, {"id": 2, "amount": 20, "note": "b"}]"#,
    );
    let jsonl = file(
        ".jsonl",
        "{\"id\": 1, \"amount\": 10, \"note\": \"a\"}\n{\"id\": 2, \"amount\": 20, \"note\": \"b\"}\n",
    );
    let routes: Vec<(&str, &std::path::Path, Option<EngineType>)> = vec![
        ("csv auto", csv.path(), Some(EngineType::Auto)),
        ("csv incremental", csv.path(), Some(EngineType::Incremental)),
        ("csv columnar", csv.path(), Some(EngineType::Columnar)),
        ("json", json.path(), None),
        ("jsonl", jsonl.path(), None),
    ];
    let mut wrong = Vec::new();
    // Without the quality pack the engines take another branch, and with a
    // projection quality is withheld; each must record the selection.
    let packs = [None, Some(vec![MetricPack::Schema])];
    for ((name, path, engine), packs) in routes
        .iter()
        .flat_map(|route| packs.iter().map(move |packs| (route, packs)))
    {
        // As given, not in source order.
        for (columns, expected) in [
            (None, ColumnSelection::All),
            (Some(names(&["note", "id"])), listed(&["note", "id"])),
        ] {
            let mut profiler = Profiler::new();
            if let Some(packs) = packs {
                profiler = profiler.metric_packs(packs.clone());
            }
            if let Some(engine) = engine {
                profiler = profiler.engine(*engine);
            }
            if let Some(columns) = &columns {
                profiler = profiler.columns(columns.clone());
            }
            let got = profiler.analyze_file(path).map(|report| recorded(&report));
            if got.as_ref().ok() != Some(&Some(expected.clone())) {
                wrong.push(format!("{name} {packs:?} with {columns:?}: {got:?}"));
            }
        }
    }
    // The legacy entry point takes no options and profiles every column.
    let legacy = analyze_csv_file(csv.path(), &Default::default()).unwrap();
    if recorded(&legacy) != Some(ColumnSelection::All) {
        wrong.push(format!("analyze_csv_file: {:?}", recorded(&legacy)));
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn a_document_keeps_the_selection() {
    let csv = file(".csv", ORDERS);
    let report = Profiler::new()
        .columns(names(&["note", "id"]))
        .analyze_file(csv.path())
        .unwrap();
    let selection = json!({"selection": "listed", "names": ["note", "id"]});
    let document = serde_json::to_value(&report).unwrap();
    assert_eq!(document["analysis_settings"]["columns"], selection);
    let reloaded: ProfileReport = serde_json::from_value(document.clone()).unwrap();
    assert_eq!(recorded(&reloaded), Some(listed(&["note", "id"])));
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    assert_eq!(summary["analysis_settings"]["columns"], selection);

    for malformed in [
        Value::Null,
        json!({}),
        json!({"selection": "listed"}),
        json!({"selection": "some"}),
        json!({"selection": "listed", "names": "id"}),
    ] {
        let mut broken = document.clone();
        broken["analysis_settings"]["columns"] = malformed.clone();
        assert!(
            serde_json::from_value::<ProfileReport>(broken).is_err(),
            "{malformed} decoded"
        );
    }
}

fn removed_or_added(result: &GateResult) -> Vec<(DiagnosticCode, &str, Option<&str>)> {
    result
        .baseline
        .as_ref()
        .unwrap()
        .diagnostics
        .iter()
        .map(|diagnostic| {
            let reason = diagnostic.setting.as_ref().map(|setting| match setting {
                SettingDifference::SettingChanged { setting, .. } => {
                    assert_eq!(setting, "columns");
                    "setting_changed"
                }
                SettingDifference::SettingUnrecorded { setting, .. } => {
                    assert_eq!(setting, "columns");
                    "setting_unrecorded"
                }
                other => panic!("{other:?}"),
            });
            (diagnostic.code, diagnostic.column.as_str(), reason)
        })
        .collect()
}

#[test]
fn the_side_without_a_column_says_whether_it_selected_it() {
    let full = file(".csv", ORDERS);
    let without_note = file(".csv", "id,amount\n1,10\n2,20\n3,30\n");
    let profile = |path: &std::path::Path, columns: Option<&[&str]>| {
        let mut profiler = Profiler::new();
        if let Some(columns) = columns {
            profiler = profiler.columns(names(columns));
        }
        profiler.analyze_file(path).unwrap()
    };
    let policy = QualityPolicy::new().max_null_percentage_increase("id", 0.0);
    let against = |report: &ProfileReport, baseline: &ReportSide| {
        policy.evaluate_against(report, baseline).unwrap()
    };
    let side = ReportSide::from_report;
    let baseline = profile(full.path(), None);

    // Every column selected and `note` missing: not in the source read.
    let gone = against(&profile(without_note.path(), None), &side(&baseline));
    assert_eq!(
        removed_or_added(&gone),
        [(DiagnosticCode::ColumnRemoved, "note", None)]
    );
    // Same source, `note` left out of the selection.
    let projected = profile(full.path(), Some(&["id", "amount"]));
    assert_eq!(
        removed_or_added(&against(&projected, &side(&baseline))),
        [(
            DiagnosticCode::ColumnRemoved,
            "note",
            Some("setting_changed")
        )]
    );
    // Both selections listed: the report's own selection decides. It names
    // `note` and the source still lacks it.
    let both_listed = against(
        &profile(without_note.path(), None),
        &side(&profile(full.path(), Some(&["id", "note"]))),
    );
    assert_eq!(
        removed_or_added(&both_listed),
        [
            (DiagnosticCode::ColumnRemoved, "note", None),
            (
                DiagnosticCode::ColumnAdded,
                "amount",
                Some("setting_changed")
            ),
        ]
    );
    // A report saved before the record: the selection cannot be shown.
    let mut legacy = serde_json::to_value(&projected).unwrap();
    legacy.as_object_mut().unwrap().remove("analysis_settings");
    let legacy: ProfileReport = serde_json::from_value(legacy).unwrap();
    assert_eq!(
        removed_or_added(&against(&legacy, &side(&baseline))),
        [(
            DiagnosticCode::ColumnRemoved,
            "note",
            Some("setting_unrecorded")
        )]
    );
    // A flat summary baseline carries it too.
    let flat = ReportSide::from_summary_json(&projected.summary_json().unwrap()).unwrap();
    assert_eq!(
        removed_or_added(&against(&baseline, &flat)),
        [(DiagnosticCode::ColumnAdded, "note", Some("setting_changed"))]
    );
}
