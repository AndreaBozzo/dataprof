//! The findings contract, asserted against the shared fixture (#375).
//!
//! Rust and Python each derive findings through their own code, and a finding
//! that appeared in one layer and not the other would make the same dataset
//! look clean in one pipeline and not in the next.
//! `tests/fixtures/findings_parity.json` states the inputs, profiling options,
//! thresholds, and the exact findings document for each case; this file
//! asserts the Rust rules against it and
//! `python/tests/test_findings_parity.py` asserts the Python ones against the
//! same file, so changing either implementation alone fails that layer's test.
//!
//! Every case is also derived from the report read back from its own JSON
//! document. Findings are not stored, so a saved report has to reproduce them
//! from what it serialized.

use std::collections::HashMap;
use std::fs;
use std::path::PathBuf;

use dataprof::{FindingPolicy, MetricPack, ProfileReport, Profiler, StopCondition};
use serde_json::Value;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("findings_parity.json");
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    serde_json::from_str(&text).expect("fixture is valid JSON")
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .expect("a list")
        .iter()
        .map(|item| item.as_str().expect("string entry").to_string())
        .collect()
}

/// Build the profiler the case describes. The option names are the fixture's
/// own; each layer maps them onto its own spelling of the same request.
fn profile(paths: &HashMap<String, PathBuf>, options: &Value) -> ProfileReport {
    let input = options
        .get("input")
        .and_then(Value::as_str)
        .unwrap_or("orders.csv");
    let path = paths.get(input).expect("the fixture names a known input");
    let mut profiler = Profiler::new();
    if let Some(metrics) = options.get("metrics") {
        profiler = profiler.metric_packs(
            strings(metrics)
                .iter()
                .map(|name| name.parse::<MetricPack>().expect("known metric pack"))
                .collect(),
        );
    }
    if let Some(columns) = options.get("columns") {
        profiler = profiler.columns(strings(columns));
    }
    if let Some(columns) = options.get("identifier_columns") {
        profiler = profiler.identifier_columns(strings(columns));
    }
    if let Some(max_rows) = options.get("max_rows") {
        profiler = profiler.stop_when(StopCondition::MaxRows(
            max_rows.as_u64().expect("max_rows is a whole number"),
        ));
    }
    profiler
        .analyze_file(path)
        .expect("the fixture input profiles")
}

fn policy_from(spec: &Value) -> FindingPolicy {
    let mut policy = FindingPolicy::new();
    if let Some(value) = spec.get("null_heavy_percentage") {
        policy = policy.null_heavy_percentage(value.as_f64().expect("a number"));
    }
    if let Some(value) = spec.get("mixed_types_percentage") {
        policy = policy.mixed_types_percentage(value.as_f64().expect("a number"));
    }
    policy
}

#[test]
fn rust_findings_match_the_shared_fixture() {
    let fixture = fixture();
    let directory = tempfile::tempdir().expect("temp dir");
    let paths: HashMap<String, PathBuf> = fixture["inputs"]
        .as_object()
        .expect("fixture carries its inputs")
        .iter()
        .map(|(name, text)| {
            let path = directory.path().join(name);
            fs::write(&path, text.as_str().expect("input is text")).expect("write input");
            (name.clone(), path)
        })
        .collect();

    let cases = fixture["cases"].as_array().expect("fixture has cases");
    assert!(!cases.is_empty(), "fixture states no case");

    for case in cases {
        let name = case["name"].as_str().expect("case has a name");
        let why = case["why"].as_str().unwrap_or_default();
        let policy = policy_from(&case["policy"]);
        let report = profile(&paths, &case["options"]);

        let fresh = policy
            .evaluate(&report)
            .unwrap_or_else(|e| panic!("{name}: policy is unevaluable: {e}"));
        assert_eq!(
            serde_json::to_value(&fresh).expect("the result serializes"),
            case["expected"],
            "{name}: {why}"
        );

        let document = report.to_json().expect("the report serializes");
        let restored: ProfileReport =
            serde_json::from_str(&document).expect("the report reads back");
        let reloaded = policy.evaluate(&restored).expect("same policy");
        assert_eq!(
            serde_json::to_value(&reloaded).expect("the result serializes"),
            case["expected"],
            "{name}: a report read back from its document derived different findings"
        );
    }
}

#[test]
fn default_findings_are_the_default_policy() {
    let fixture = fixture();
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("orders.csv");
    fs::write(
        &path,
        fixture["inputs"]["orders.csv"].as_str().expect("CSV"),
    )
    .expect("write");
    let report = Profiler::new().analyze_file(&path).expect("profiles");

    assert_eq!(
        report.findings(),
        FindingPolicy::default()
            .evaluate(&report)
            .expect("in range")
    );
}

#[test]
fn out_of_range_thresholds_are_rejected_before_evaluation() {
    for value in [0.0, -1.0, 100.5, f64::NAN, f64::INFINITY] {
        let error = FindingPolicy::new()
            .null_heavy_percentage(value)
            .validate()
            .expect_err("threshold outside (0, 100]");
        assert!(
            error
                .to_string()
                .starts_with("null_heavy_percentage must be"),
            "{error}"
        );
    }
    let error = FindingPolicy::new()
        .mixed_types_percentage(0.5)
        .mixed_types_percentage(0.0)
        .validate()
        .expect_err("zero reports every column");
    assert!(
        error
            .to_string()
            .starts_with("mixed_types_percentage must be"),
        "{error}"
    );
    assert!(
        FindingPolicy::new()
            .null_heavy_percentage(100.0)
            .validate()
            .is_ok()
    );
}
