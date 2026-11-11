//! `compare()` and the gate result name the build behind each report, and the
//! caller's version of its source (#1063).
//!
//! `python/tests/test_result_provenance.py` is the Python twin; it also covers
//! `python -m dataprof.check --json`.

use std::io::Write;

use dataprof::{
    ProfileReport, Profiler, QualityPolicy, ReportDelta, ReportGenerator, ReportSide,
    SourceIdentity,
};
use serde_json::{Value, json};

fn profile(contents: &str) -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    Profiler::new().analyze_file(file.path()).unwrap()
}

const CLEAN: &str = "id,amount\n1,10\n2,20\n3,30\n4,40\n";

fn generator(value: Value) -> ReportGenerator {
    serde_json::from_value(value).unwrap()
}

fn with(generator: Option<Value>, version: Option<&str>) -> ProfileReport {
    let mut report = profile(CLEAN);
    report.generator = generator.map(self::generator);
    report.source_identity = version.map(SourceIdentity::with_version);
    report
}

fn delta(a: &ProfileReport, b: &ProfileReport) -> Value {
    serde_json::to_value(ReportDelta::between(a, b)).unwrap()
}

fn summary_side(report: &ProfileReport) -> ReportSide {
    ReportSide::from_summary_json(&report.summary_json().unwrap()).unwrap()
}

const COMMIT_A: &str = "0123456789abcdef0123456789abcdef01234567";
const COMMIT_B: &str = "89abcdef0123456789abcdef0123456789abcdef";

#[test]
fn same_build_reads_version_commit_and_dirty_flag() {
    let build = |version: &str, commit: Option<&str>, dirty: Option<bool>| {
        let mut value = json!({"version": version});
        if let Some(commit) = commit {
            value["commit"] = json!(commit);
        }
        if let Some(dirty) = dirty {
            value["dirty"] = json!(dirty);
        }
        Some(value)
    };
    let clean = build("0.14.0", Some(COMMIT_A), Some(false));
    let cases: Vec<(&str, Option<Value>, Option<Value>, Value)> = vec![
        (
            "same clean build",
            clean.clone(),
            clean.clone(),
            json!(true),
        ),
        (
            "other version",
            clean.clone(),
            build("0.13.0", Some(COMMIT_A), Some(false)),
            json!(false),
        ),
        (
            "other version, no commit",
            build("0.14.0", None, None),
            build("0.13.0", None, None),
            json!(false),
        ),
        (
            "other commit",
            clean.clone(),
            build("0.14.0", Some(COMMIT_B), Some(false)),
            json!(false),
        ),
        (
            "other commit, both dirty",
            build("0.14.0", Some(COMMIT_A), Some(true)),
            build("0.14.0", Some(COMMIT_B), Some(true)),
            json!(false),
        ),
        (
            "one commit clean and dirty",
            clean.clone(),
            build("0.14.0", Some(COMMIT_A), Some(true)),
            json!(false),
        ),
        (
            "one commit dirty twice",
            build("0.14.0", Some(COMMIT_A), Some(true)),
            build("0.14.0", Some(COMMIT_A), Some(true)),
            Value::Null,
        ),
        (
            "commit unrecorded on one side",
            clean.clone(),
            build("0.14.0", None, None),
            Value::Null,
        ),
        (
            "dirty flag unrecorded",
            build("0.14.0", Some(COMMIT_A), None),
            build("0.14.0", Some(COMMIT_A), None),
            Value::Null,
        ),
        ("no build on one side", clean.clone(), None, Value::Null),
        ("no build on either side", None, None, Value::Null),
    ];

    let mut wrong = Vec::new();
    for (name, a, b, expected) in cases {
        let (a, b) = (with(a, None), with(b, None));
        for (route, document) in [
            ("native", delta(&a, &b)),
            (
                "summary",
                serde_json::to_value(ReportDelta::between_sides(
                    &summary_side(&a),
                    &summary_side(&b),
                ))
                .unwrap(),
            ),
        ] {
            let same = &document["reports"]["same_build"];
            if *same != expected {
                wrong.push(format!(
                    "{name} ({route}): same_build {same}, expected {expected}"
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn compare_names_each_sides_build_and_version_on_every_route() {
    let a = with(
        Some(json!({"version": "0.13.0", "commit": COMMIT_A, "dirty": false})),
        Some("snapshot 41"),
    );
    let b = with(Some(json!({"version": "0.14.0"})), Some("snapshot 42"));
    let expected = json!({
        "a": [{"version": "0.13.0", "commit": COMMIT_A, "dirty": false}, {"version": "snapshot 41"}],
        "b": [{"version": "0.14.0"}, {"version": "snapshot 42"}],
    });

    let mut wrong = Vec::new();
    let routes = [
        (
            "native/native",
            ReportSide::from_report(&a),
            ReportSide::from_report(&b),
        ),
        (
            "summary/native",
            summary_side(&a),
            ReportSide::from_report(&b),
        ),
        ("summary/summary", summary_side(&a), summary_side(&b)),
    ];
    for (route, side_a, side_b) in routes {
        let document = serde_json::to_value(ReportDelta::between_sides(&side_a, &side_b)).unwrap();
        for name in ["a", "b"] {
            let side = &document["reports"][name];
            let got = json!([side["generator"], side["source_identity"]]);
            if got != expected[name] {
                wrong.push(format!("{route} {name}: {got}"));
            }
        }
        if document["reports"]["same_build"] != json!(false) {
            wrong.push(format!(
                "{route}: same_build {}",
                document["reports"]["same_build"]
            ));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn an_unrecorded_build_or_version_reads_as_null() {
    let a = with(None, None);
    let b = with(None, None);
    let mut wrong = Vec::new();
    for (route, document) in [
        ("native", delta(&a, &b)),
        (
            "summary",
            serde_json::to_value(ReportDelta::between_sides(
                &summary_side(&a),
                &summary_side(&b),
            ))
            .unwrap(),
        ),
    ] {
        for name in ["a", "b"] {
            for key in ["generator", "source_identity"] {
                // Present and null: the key is part of the shape.
                if document["reports"][name].get(key) != Some(&Value::Null) {
                    wrong.push(format!(
                        "{route} {name}.{key}: {:?}",
                        document["reports"][name].get(key)
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn a_summary_with_a_null_generator_or_identity_is_malformed() {
    let report = with(Some(json!({"version": "0.14.0"})), Some("v1"));
    let summary: Value = serde_json::from_str(&report.summary_json().unwrap()).unwrap();
    let mut wrong = Vec::new();
    for key in ["generator", "source_identity"] {
        let mut document = summary.clone();
        document[key] = Value::Null;
        match ReportSide::from_summary_json(&document.to_string()) {
            Ok(_) => wrong.push(format!("{key}: null accepted")),
            Err(error) if !error.to_string().contains(key) => {
                wrong.push(format!("{key}: error does not name the key: {error}"))
            }
            Err(_) => {}
        }
        let mut document = summary.clone();
        document[key] = json!("not an object");
        if ReportSide::from_summary_json(&document.to_string()).is_ok() {
            wrong.push(format!("{key}: a string accepted"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}

#[test]
fn the_gate_result_names_the_evaluated_reports_build_and_version() {
    let report = with(
        Some(json!({"version": "0.14.0", "commit": COMMIT_B, "dirty": false})),
        Some("snapshot 42"),
    );
    let baseline = with(Some(json!({"version": "0.13.0"})), Some("snapshot 41"));
    let unrecorded = with(None, None);

    let absolute = QualityPolicy::new().min_rows(1);
    let relative = QualityPolicy::new().max_quality_score_drop(5.0);
    let mut wrong = Vec::new();
    fn expect(
        wrong: &mut Vec<String>,
        name: &str,
        result: Value,
        generator: &Value,
        version: &Value,
    ) -> Value {
        if result.get("generator") != Some(generator) {
            wrong.push(format!("{name}: generator {:?}", result.get("generator")));
        }
        if result.get("source_identity") != Some(version) {
            wrong.push(format!(
                "{name}: source_identity {:?}",
                result.get("source_identity")
            ));
        }
        result
    }

    let recorded_generator = json!({"version": "0.14.0", "commit": COMMIT_B, "dirty": false});
    let recorded_version = json!({"version": "snapshot 42"});
    expect(
        &mut wrong,
        "absolute",
        serde_json::to_value(absolute.evaluate(&report).unwrap()).unwrap(),
        &recorded_generator,
        &recorded_version,
    );
    expect(
        &mut wrong,
        "absolute, unrecorded",
        serde_json::to_value(absolute.evaluate(&unrecorded).unwrap()).unwrap(),
        &Value::Null,
        &Value::Null,
    );
    for (route, side) in [
        ("native baseline", ReportSide::from_report(&baseline)),
        ("summary baseline", summary_side(&baseline)),
    ] {
        let result = expect(
            &mut wrong,
            route,
            serde_json::to_value(relative.evaluate_against(&report, &side).unwrap()).unwrap(),
            &recorded_generator,
            &recorded_version,
        );
        let reports = &result["baseline"]["reports"];
        let got = json!([
            reports["a"]["generator"],
            reports["a"]["source_identity"],
            reports["b"]["generator"],
            reports["same_build"]
        ]);
        let want = json!([
            {"version": "0.13.0"},
            {"version": "snapshot 41"},
            recorded_generator,
            false
        ]);
        if got != want {
            wrong.push(format!("{route}: baseline.reports {got}"));
        }
    }
    assert!(wrong.is_empty(), "{wrong:#?}");
}
