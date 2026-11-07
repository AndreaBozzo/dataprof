//! A flat summary baseline that does not record `execution.sampling_applied`
//! reads as `coverage_unrecorded`, so a gate against it is inconclusive; a
//! recorded `false` decides and a recorded `true` names the sampling (#1236).
//!
//! `python/tests/test_summary_sampling_applied.py` is the Python twin, where
//! the summary also crosses `from_dict()` and the `python -m dataprof.check`
//! entrypoint before the gate reads it.

use std::io::Write;

use dataprof::{CheckCode, ProfileReport, Profiler, QualityPolicy, ReportSide};
use serde_json::Value;

fn profile(contents: &str) -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.flush().unwrap();
    Profiler::new().analyze_file(file.path()).unwrap()
}

/// The verdict and the null-increase check's `status`, `reason:gap` when it
/// was not evaluated.
fn outcome(current: &ProfileReport, baseline: &Value) -> (String, String) {
    let baseline = ReportSide::from_summary_json(&baseline.to_string()).unwrap();
    let result = QualityPolicy::new()
        .max_null_percentage_increase("x", 0.0)
        .evaluate_against(current, &baseline)
        .unwrap();
    let check = result
        .checks
        .iter()
        .find(|check| check.code == CheckCode::MaxNullPercentageIncrease)
        .unwrap();
    let check = serde_json::to_value(check).unwrap();
    let text = |key: &str| check[key].as_str().unwrap_or_default().to_string();
    let status = match text("status").as_str() {
        "not_evaluated" => format!("{}:{}", text("reason"), text("gap")),
        status => status.to_string(),
    };
    let verdict = serde_json::to_value(result.verdict).unwrap();
    (verdict.as_str().unwrap().to_string(), status)
}

#[test]
fn a_summary_baseline_decides_only_on_a_recorded_sampling_applied() {
    let current = profile("x\n1\n2\n3\n4\n");
    let summary: Value = serde_json::from_str(&current.summary_json().unwrap()).unwrap();
    assert_eq!(summary["execution"]["sampling_applied"], Value::Bool(false));
    let cases = [
        (
            "missing",
            None,
            ("inconclusive", "evidence_incomplete:coverage_unrecorded"),
        ),
        ("false", Some(false), ("pass", "passed")),
        (
            "true",
            Some(true),
            ("inconclusive", "evidence_incomplete:sampled"),
        ),
    ];
    let mut wrong = Vec::new();
    for (case, recorded, (verdict, status)) in cases {
        let mut document = summary.clone();
        let execution = document["execution"].as_object_mut().unwrap();
        match recorded {
            Some(value) => {
                execution.insert("sampling_applied".into(), Value::Bool(value));
            }
            None => {
                execution.remove("sampling_applied");
            }
        }
        let observed = outcome(&current, &document);
        if observed != (verdict.to_string(), status.to_string()) {
            wrong.push((case, observed));
        }
    }
    assert!(wrong.is_empty(), "{wrong:?}");
}
