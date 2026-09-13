//! The quality-gate contract, asserted against the shared fixture (#376).
//!
//! Rust and Python each evaluate a policy through their own code, and a gate
//! that disagreed between them would let the same dataset pass in one pipeline
//! and fail in the next. `tests/fixtures/quality_gate_parity.json` states one
//! CSV, a set of profiling options, a policy, and the exact result document
//! for each case; this file asserts the Rust evaluator against it and
//! `python/tests/test_quality_gate_parity.py` asserts the Python one against
//! the same file, so changing either implementation alone fails that layer's
//! test.
//!
//! The cases cover what the ticket's evidence section calls for: a complete
//! input, a capped one, unavailable metrics, projected columns, and a
//! conclusive violation witnessed under incomplete coverage.

use std::fs;
use std::path::PathBuf;

use dataprof::{
    MetricPack, ProfileReport, Profiler, QualityDimension, QualityPolicy, StopCondition,
};
use serde_json::Value;

fn fixture() -> Value {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join("quality_gate_parity.json");
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    serde_json::from_str(&text).expect("fixture is valid JSON")
}

fn strings(value: Option<&Value>) -> Vec<String> {
    value
        .and_then(Value::as_array)
        .map(|items| {
            items
                .iter()
                .map(|item| item.as_str().expect("string entry").to_string())
                .collect()
        })
        .unwrap_or_default()
}

/// Build the profiler the case describes. The option names are the fixture's
/// own; each layer maps them onto its own spelling of the same request.
fn profile(path: &PathBuf, options: &Value) -> ProfileReport {
    let mut profiler = Profiler::new();
    if let Some(metrics) = options.get("metrics") {
        profiler = profiler.metric_packs(
            strings(Some(metrics))
                .iter()
                .map(|name| name.parse::<MetricPack>().expect("known metric pack"))
                .collect(),
        );
    }
    if let Some(columns) = options.get("columns") {
        profiler = profiler.columns(strings(Some(columns)));
    }
    if let Some(max_rows) = options.get("max_rows") {
        profiler = profiler.stop_when(StopCondition::MaxRows(
            max_rows.as_u64().expect("max_rows is a whole number"),
        ));
    }
    profiler
        .analyze_file(path)
        .expect("the fixture CSV profiles")
}

fn policy_from(spec: &Value) -> QualityPolicy {
    let mut policy = QualityPolicy::new();
    if let Some(min) = spec.get("min_quality_score") {
        policy = policy.min_quality_score(min.as_f64().expect("a number"));
    }
    if let Some(scores) = spec.get("min_dimension_scores").and_then(Value::as_object) {
        for (dimension, min) in scores {
            policy = policy.min_dimension_score(
                dimension
                    .parse::<QualityDimension>()
                    .expect("known dimension"),
                min.as_f64().expect("a number"),
            );
        }
    }
    if let Some(limits) = spec.get("max_null_percentage").and_then(Value::as_object) {
        for (column, max) in limits {
            let max = max.as_f64().expect("a number");
            policy = if column == "*" {
                policy.max_null_percentage_any(max)
            } else {
                policy.max_null_percentage(column, max)
            };
        }
    }
    if let Some(max) = spec.get("max_duplicate_rows") {
        policy = policy.max_duplicate_rows(max.as_u64().expect("a whole number") as usize);
    }
    for metric in strings(spec.get("require_metrics")) {
        policy = match metric.as_str() {
            "quality" => policy.require_quality(),
            dimension => policy.require_dimension(dimension.parse().expect("known dimension")),
        };
    }
    if let Some(scope) = spec.get("scope").and_then(Value::as_str) {
        policy = policy.scope(scope.parse().expect("known scope"));
    }
    policy
}

#[test]
fn rust_gate_matches_the_shared_fixture() {
    let fixture = fixture();
    let directory = tempfile::tempdir().expect("temp dir");
    let path = directory.path().join("orders.csv");
    fs::write(
        &path,
        fixture["csv"].as_str().expect("fixture carries the CSV"),
    )
    .expect("write the fixture CSV");

    let cases = fixture["cases"].as_array().expect("fixture has cases");
    assert!(!cases.is_empty(), "fixture states no case");

    for case in cases {
        let name = case["name"].as_str().expect("case has a name");
        let report = profile(&path, &case["options"]);
        let result = policy_from(&case["policy"])
            .evaluate(&report)
            .unwrap_or_else(|e| panic!("{name}: policy is unevaluable: {e}"));

        assert_eq!(
            serde_json::to_value(&result).expect("the result serializes"),
            case["expected"],
            "{name}: {}",
            case["why"].as_str().unwrap_or_default()
        );
    }
}
