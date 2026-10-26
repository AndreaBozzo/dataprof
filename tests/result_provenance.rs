//! Result provenance stays separate from metrics and verdicts (#1063).

use dataprof::{ProfileReport, Profiler, QualityPolicy, ReportDelta, ReportSide};
use serde_json::{Value, json};
use std::io::Write;

fn profile() -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(b"id,value\n1,10\n2,20\n").unwrap();
    Profiler::new().analyze_file(file.path()).unwrap()
}

fn with_generator(report: &ProfileReport, generator: &Value) -> ProfileReport {
    let mut report = report.clone();
    report.generator = if generator.is_null() {
        None
    } else {
        Some(serde_json::from_value(generator.clone()).unwrap())
    };
    report
}

fn sides(report: &ProfileReport) -> Vec<ReportSide> {
    let loaded = serde_json::from_str(&serde_json::to_string(report).unwrap()).unwrap();
    vec![
        ReportSide::from_report(report),
        ReportSide::from_report(&loaded),
        ReportSide::from_summary_json(&report.summary_json().unwrap()).unwrap(),
    ]
}

#[test]
fn compare_records_builds_without_changing_metrics_on_every_route() {
    let report = profile();
    let original = serde_json::to_value(ReportDelta::between(&report, &report)).unwrap();
    let cases: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/result_provenance.json")).unwrap();
    for case in cases {
        let a = with_generator(&report, &case["a"]);
        let b = with_generator(&report, &case["b"]);
        for a in sides(&a) {
            for b in sides(&b) {
                let delta = serde_json::to_value(ReportDelta::between_sides(&a, &b)).unwrap();
                assert_eq!(delta.get("same_build"), Some(&case["same_build"]), "{case}");
                for side in ["a", "b"] {
                    assert_eq!(delta["reports"][side].get("generator"), Some(&case[side]));
                }
                for key in [
                    "quality_score",
                    "dimensions",
                    "columns",
                    "schema",
                    "metric_semantics",
                    "configuration",
                ] {
                    assert_eq!(delta[key], original[key], "{key}: {case}");
                }
            }
        }
    }
}

#[test]
fn gates_name_the_evaluated_report_build_not_the_baseline_or_current_build() {
    let report = profile();
    let policy = QualityPolicy::new().min_rows(1);
    let old = json!({"version": "old", "commit": "historical", "dirty": false});
    let baseline = with_generator(&report, &json!({"version": "baseline"}));
    for generator in [old, Value::Null] {
        let report = with_generator(&report, &generator);
        let loaded: ProfileReport =
            serde_json::from_str(&serde_json::to_string(&report).unwrap()).unwrap();
        for report in [&report, &loaded] {
            let result = policy.evaluate(report).unwrap();
            let expected_checks = serde_json::to_value(&result.checks).unwrap();
            let result = serde_json::to_value(result).unwrap();
            assert_eq!(result.get("generator"), Some(&generator));
            assert_eq!(result["verdict"], "pass");
            for baseline in sides(&baseline) {
                let result =
                    serde_json::to_value(policy.evaluate_against(report, &baseline).unwrap())
                        .unwrap();
                assert_eq!(result.get("generator"), Some(&generator));
                assert_eq!(result["baseline"]["reports"]["b"]["generator"], generator);
                assert_eq!(result["checks"], expected_checks);
                assert_eq!(result["verdict"], "pass");
            }
        }
    }
}

#[test]
fn malformed_generators_are_not_silently_ignored_in_flat_comparisons() {
    let report = profile();
    for generator in [
        Value::Null,
        json!("version"),
        json!({}),
        json!({"version": 1}),
        json!({"version": "v", "commit": null}),
        json!({"version": "v", "commit": false}),
        json!({"version": "v", "dirty": null}),
        json!({"version": "v", "dirty": 1}),
    ] {
        let mut summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
        summary["generator"] = generator.clone();
        assert!(
            ReportSide::from_summary_json(&summary.to_string()).is_err(),
            "accepted {generator}"
        );
        let mut canonical = serde_json::to_value(&report).unwrap();
        canonical["generator"] = generator.clone();
        assert!(
            serde_json::from_value::<ProfileReport>(canonical).is_err(),
            "accepted {generator}"
        );
    }
}
