//! Rust and Python must emit the same rounded numbers for the same data (#513).
//!
//! `tests/fixtures/report_rounding_parity.json` records the rounded floats that
//! profiling `tests/fixtures/rounding_parity.csv` must produce. This file
//! asserts the Rust report against it; `python/tests/test_rounding_parity.py`
//! asserts `ProfileReport.to_dict()` against the same file. Because both layers
//! check the same expectations, a precision or tie-breaking change on either
//! side fails that side's test rather than drifting unnoticed.
//!
//! The fixture data is chosen so the two layers' old disagreements are visible:
//! `amount` carries five decimals so `min`/`max`/`median` differ between 2dp
//! and 4dp, `label` has a non-round `avg_length`, and `coefficient_of_variation`
//! is a percentage that must stay at 2dp while the statistics around it do not.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use dataprof::{CsvParserConfig, analyze_csv_file};
use serde_json::Value;

fn fixture_path(name: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures")
        .join(name)
}

fn expectations() -> Value {
    let path = fixture_path("report_rounding_parity.json");
    let text =
        fs::read_to_string(&path).unwrap_or_else(|e| panic!("cannot read {}: {e}", path.display()));
    serde_json::from_str(&text).expect("fixture is valid JSON")
}

/// Every float the Rust report emits, keyed `column.field` (and
/// `column.quartiles.q1` for the nested ones).
fn actual_column_floats(report: &Value) -> BTreeMap<String, f64> {
    let mut found = BTreeMap::new();
    for column in report["column_profiles"]
        .as_array()
        .expect("report has column profiles")
    {
        let name = column["name"].as_str().expect("column has a name");
        // `stats` is an externally tagged enum: {"Numeric": {..}} / {"Text": {..}}.
        let Some(stats) = column["stats"].as_object().and_then(|o| o.values().next()) else {
            continue;
        };
        let Some(stats) = stats.as_object() else {
            continue;
        };
        for (field, value) in stats {
            if let Some(number) = value.as_f64() {
                found.insert(format!("{name}.{field}"), number);
            } else if field == "quartiles" {
                for (quartile, value) in value.as_object().expect("quartiles is an object") {
                    let number = value.as_f64().expect("quartile is a number");
                    found.insert(format!("{name}.quartiles.{quartile}"), number);
                }
            }
        }
    }
    found
}

fn expected_column_floats(fixture: &Value) -> BTreeMap<String, f64> {
    let mut wanted = BTreeMap::new();
    for (name, fields) in fixture["columns"].as_object().expect("fixture has columns") {
        for (field, value) in fields.as_object().expect("column entry is an object") {
            if let Some(number) = value.as_f64() {
                wanted.insert(format!("{name}.{field}"), number);
            } else if field == "quartiles" {
                for (quartile, value) in value.as_object().expect("quartiles is an object") {
                    let number = value.as_f64().expect("quartile is a number");
                    wanted.insert(format!("{name}.quartiles.{quartile}"), number);
                }
            }
        }
    }
    wanted
}

#[test]
fn rust_report_matches_the_shared_rounding_expectations() {
    let report = analyze_csv_file(
        &fixture_path("rounding_parity.csv"),
        &CsvParserConfig::default(),
    )
    .expect("fixture profiles cleanly");
    let report = serde_json::to_value(&report).expect("report serializes");

    let actual = actual_column_floats(&report);
    let expected = expected_column_floats(&expectations());
    assert!(
        !expected.is_empty(),
        "fixture lists no column floats, so this test would assert nothing"
    );

    let mut failures = Vec::new();
    for (field, want) in &expected {
        match actual.get(field) {
            Some(got) if got == want => {}
            Some(got) => failures.push(format!("{field}: expected {want:?}, Rust emitted {got:?}")),
            None => failures.push(format!("{field}: expected {want:?}, Rust emitted nothing")),
        }
    }
    assert!(
        failures.is_empty(),
        "{} of {} shared fields disagree between the Rust report and the fixture \
         that Python is also held to:\n{}",
        failures.len(),
        expected.len(),
        failures.join("\n")
    );
}

#[test]
fn rust_quality_metrics_match_the_shared_rounding_expectations() {
    let report = analyze_csv_file(
        &fixture_path("rounding_parity.csv"),
        &CsvParserConfig::default(),
    )
    .expect("fixture profiles cleanly");
    let report = serde_json::to_value(&report).expect("report serializes");
    let metrics = &report["quality"]["metrics"];

    let fixture = expectations();
    let expected = fixture["quality"]
        .as_object()
        .expect("fixture has quality expectations");
    assert!(
        !expected.is_empty(),
        "fixture lists no quality floats, so this test would assert nothing"
    );

    let mut failures = Vec::new();
    for (dimension, fields) in expected {
        for (field, value) in fields.as_object().expect("dimension entry is an object") {
            let want = value.as_f64().expect("expectation is a number");
            match metrics[dimension][field].as_f64() {
                Some(got) if got == want => {}
                Some(got) => failures.push(format!(
                    "{dimension}.{field}: expected {want:?}, Rust emitted {got:?}"
                )),
                None => failures.push(format!(
                    "{dimension}.{field}: expected {want:?}, Rust emitted nothing"
                )),
            }
        }
    }
    assert!(
        failures.is_empty(),
        "{} quality fields disagree between the Rust report and the fixture \
         that Python is also held to:\n{}",
        failures.len(),
        failures.join("\n")
    );
}

#[test]
fn fixture_data_would_expose_a_precision_change() {
    // Guard the guard: if the fixture CSV ever became all round numbers, both
    // layers would agree at any precision and these tests would pass while
    // pinning nothing. Require values that only survive at 4dp.
    let path = fixture_path("rounding_parity.csv");
    let text = fs::read_to_string(&path).expect("fixture CSV is readable");
    let with_four_decimals = text
        .lines()
        .skip(1)
        .filter(|line| {
            line.split(',').any(|cell| {
                cell.split_once('.')
                    .is_some_and(|(_, frac)| frac.trim_end_matches('0').len() > 2)
            })
        })
        .count();
    assert!(
        with_four_decimals >= 10,
        "only {with_four_decimals} fixture rows carry more than two decimals; \
         the fixture no longer distinguishes 2dp from 4dp"
    );
}
