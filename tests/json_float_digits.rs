//! A JSON float literal with more digits than `f64` holds profiles as its
//! digits, not as the rounded number.
//!
//! serde_json reads `1000000000000000.01` as `1000000000000000.0`, so three
//! distinct amounts became one value with zero variance, while a CSV of the
//! same digits reported three values and their spread (#1156). A field whose
//! literal does not survive the round trip is now profiled as its text, as the
//! CSV cell is; a literal the `f64` holds exactly, like `1.50`, stays a number.
//!
//! `python/tests/test_json_float_digits.py` is the Python twin, and adds the
//! bytes and async routes.

use std::io::Write;

use dataprof::{ProfileReport, Profiler};
use serde_json::Value;

fn profile(contents: &str, suffix: &str) -> ProfileReport {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    Profiler::new()
        .analyze_file(file.path())
        .unwrap_or_else(|error| panic!("{suffix}: {error}"))
}

/// The serialized profile of column `v`, without its patterns: a literal the
/// `f64` holds is written in its shortest form, so `1e5` reads as `100000.0`
/// and does not match the scientific-notation pattern its CSV cell does.
fn column(report: &ProfileReport) -> Value {
    let column = report
        .column_profiles
        .iter()
        .find(|column| column.name == "v")
        .expect("column v");
    let mut value = serde_json::to_value(column).unwrap();
    value.as_object_mut().unwrap().remove("patterns");
    value
}

/// (label, the `v` literals in row order)
const CASES: [(&str, &[&str]); 5] = [
    (
        "more digits than f64",
        &[
            "1000000000000000.01",
            "1000000000000000.05",
            "1000000000000000.02",
        ],
    ),
    ("below the f64 range", &["1e-400", "2e-400", "3e-400"]),
    (
        "beside held literals",
        &["0.1", "1000000000000000.01", "1.50", "1e2", "-0.0"],
    ),
    ("exponent form", &["1.00000000000000000001e5", "1e5", "2.5"]),
    (
        "large integer-valued float",
        &["12345678901234567890.0", "12345678901234567891.0", "7"],
    ),
];

/// Collects every wrong (case, path) before failing.
#[test]
fn every_json_path_profiles_float_digits_as_csv_does() {
    let mut wrong = Vec::new();
    for (label, values) in CASES {
        let csv = format!(
            "id,v\n{}\n",
            rows(values, |id, v| format!("{id},{v}")).join("\n")
        );
        let expected = column(&profile(&csv, ".csv"));

        let records = rows(values, |id, v| format!(r#"{{"id":{id},"v":{v}}}"#));
        let paths = [
            ("json array", format!("[{}]", records.join(",")), ".json"),
            ("jsonl", format!("{}\n", records.join("\n")), ".jsonl"),
            (
                // The last key wins, so the literal is the value profiled.
                "jsonl, duplicate key",
                rows(values, |id, v| format!(r#"{{"id":{id},"v":0.5,"v":{v}}}"#)).join("\n"),
                ".jsonl",
            ),
        ];
        for (path, contents, suffix) in paths {
            let actual = column(&profile(&contents, suffix));
            if actual != expected {
                wrong.push(format!(
                    "[{label}] {path}: unique {} variance {} (csv: unique {} variance {})",
                    actual["unique_count"],
                    actual["stats"]["variance"],
                    expected["unique_count"],
                    expected["stats"]["variance"],
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn a_single_object_keeps_the_digits() {
    let report = profile(r#"{"id":1,"v":1e-400}"#, ".json");
    let expected = profile("id,v\n1,1e-400\n", ".csv");
    assert_eq!(column(&report), column(&expected));
}

/// A float inside an array or object is profiled as the container's text,
/// which keeps the `f64`; the records still read.
#[test]
fn nested_floats_keep_the_container_text() {
    let report = profile(
        r#"[{"v":[1000000000000000.01]},{"v":{"k":1000000000000000.05}}]"#,
        ".json",
    );
    assert_eq!(report.execution.rows_processed, 2);
    assert_eq!(report.execution.error_count, 0);
}

fn rows(values: &[&str], row: impl Fn(usize, &str) -> String) -> Vec<String> {
    values
        .iter()
        .enumerate()
        .map(|(id, value)| row(id + 1, value))
        .collect()
}
