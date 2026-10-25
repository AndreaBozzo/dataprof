//! A JSON integer outside the 64-bit range is refused, not rounded.
//!
//! serde_json holds such an integer as the nearest `f64`, so
//! `18446744073709551616` and `18446744073709551617` became one value, one
//! distinct count and a fabricated duplicate row, with no error recorded
//! (#1103). The record is now a decode error the JSON error policy governs:
//! skipped and counted, or refused under strict. Integers at the 64-bit limits,
//! float literals of the same size and digits inside strings stay accepted.
//!
//! `python/tests/test_json_wide_integers.py` is the Python twin, and adds the
//! bytes and async routes.

use std::io::Write;

use dataprof::{JsonErrorPolicy, ProfileReport, Profiler};

fn profile(contents: &str, suffix: &str, policy: JsonErrorPolicy) -> Result<ProfileReport, String> {
    let mut file = tempfile::Builder::new().suffix(suffix).tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    Profiler::new()
        .json_error_policy(policy)
        .analyze_file(file.path())
        .map_err(|error| error.to_string())
}

/// (label, file suffix, contents, where a strict error names the record)
const WIDE: [(&str, &str, &str, &str); 5] = [
    (
        "array, above u64",
        ".json",
        r#"[{"id":1,"v":5},{"id":2,"v":18446744073709551616},{"id":3,"v":6}]"#,
        "at position 2",
    ),
    (
        "array, below i64",
        ".json",
        r#"[{"id":1,"v":5},{"id":2,"v":-9223372036854775809},{"id":3,"v":6}]"#,
        "at position 2",
    ),
    (
        "array, nested",
        ".json",
        r#"[{"id":1,"v":5},{"id":2,"v":{"k":[18446744073709551617]}},{"id":3,"v":6}]"#,
        "at position 2",
    ),
    (
        "jsonl",
        ".jsonl",
        "{\"id\":1,\"v\":5}\n{\"id\":2,\"v\":18446744073709551616}\n{\"id\":3,\"v\":6}\n",
        "on line 2",
    ),
    (
        "single object",
        ".json",
        r#"{"id":2,"v":18446744073709551616}"#,
        "at position 1",
    ),
];

/// Collects every wrong (case, policy) before failing.
#[test]
fn wide_integers_are_refused_under_either_policy() {
    let mut wrong = Vec::new();
    for (label, suffix, contents, location) in WIDE {
        match profile(contents, suffix, JsonErrorPolicy::Strict) {
            Err(error)
                if error.contains("outside the 64-bit range") && error.contains(location) => {}
            Err(error) => wrong.push(format!("[{label}] strict: unexpected error {error}")),
            Ok(report) => wrong.push(format!(
                "[{label}] strict: profiled {} rows instead of refusing",
                report.execution.rows_processed
            )),
        }
        // The refused record sits between two others: reading continues past it.
        let expected_rows = if label == "single object" { 0 } else { 2 };
        match profile(contents, suffix, JsonErrorPolicy::Skip) {
            Ok(report)
                if report.execution.rows_processed == expected_rows
                    && report.execution.error_count == 1 => {}
            Ok(report) => wrong.push(format!(
                "[{label}] skip: rows {} errors {}",
                report.execution.rows_processed, report.execution.error_count
            )),
            // A source whose only record is refused has nothing to profile.
            Err(error) if expected_rows == 0 && error.contains("No valid JSON records") => {}
            Err(error) => wrong.push(format!("[{label}] skip: failed {error}")),
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[test]
fn in_range_integers_floats_and_strings_stay_accepted() {
    let contents = r#"[{"v":18446744073709551615,"s":"18446744073709551616"},{"v":-9223372036854775808,"s":"a"},{"v":1e19,"s":"b"},{"v":1.8446744073709552e19,"s":"c"}]"#;
    let report =
        profile(contents, ".json", JsonErrorPolicy::Strict).expect("every record is in range");
    assert_eq!(report.execution.rows_processed, 4);
    assert_eq!(report.execution.error_count, 0);
}
