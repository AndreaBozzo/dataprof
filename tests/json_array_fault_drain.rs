//! A JSON array read that stops at a fault does not report the source as read.
//!
//! Under the skip policy, the array scanner cannot read past an element it
//! failed to decode, such as `1e400` or a syntax error, so it drains the rest.
//! The records after the fault were never read, yet the report said the source
//! was exhausted with no truncation, so a partial profile read as a complete
//! one (#1106). Anything left unread after the fault now marks the source as
//! not exhausted; a fault with nothing after it leaves it exhausted.
//!
//! `python/tests/test_json_array_fault_drain.py` is the Python twin, and adds
//! the async route and the gate's evidence.

use std::io::Write;

use dataprof::Profiler;

fn profile(contents: &str) -> (usize, usize, bool) {
    let mut file = tempfile::Builder::new().suffix(".json").tempfile().unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    let report = Profiler::new().analyze_file(file.path()).expect("profile");
    let execution = &report.execution;
    (
        execution.rows_processed,
        execution.error_count,
        execution.source_exhausted,
    )
}

/// (label, file, expected (rows, errors, source exhausted))
const CASES: [(&str, &str, (usize, usize, bool)); 6] = [
    (
        "number out of f64 range mid-array",
        r#"[{"a":1},{"a":1e400},{"a":2},{"a":3}]"#,
        (1, 1, false),
    ),
    (
        "syntax error mid-array",
        r#"[{"a":1},{"a":},{"a":2}]"#,
        (1, 1, false),
    ),
    (
        // The faulty element's own tail is unread too; whether it held more
        // records is not known, so the read is not called complete.
        "number out of f64 range in the last element",
        r#"[{"a":1},{"a":1e400}]"#,
        (1, 1, false),
    ),
    (
        "input ends before the closing bracket",
        r#"[{"a":1},{"a":2}"#,
        (2, 1, true),
    ),
    (
        "integer outside 64 bits mid-array, skipped without draining",
        r#"[{"a":1},{"a":18446744073709551616},{"a":2}]"#,
        (2, 1, true),
    ),
    ("clean array", r#"[{"a":1},{"a":2}]"#, (2, 0, true)),
];

#[test]
fn a_read_stopped_at_a_fault_is_not_exhausted() {
    let wrong: Vec<String> = CASES
        .iter()
        .filter_map(|(label, contents, expected)| {
            let found = profile(contents);
            (found != *expected).then(|| format!("[{label}] (rows, errors, exhausted) = {found:?}"))
        })
        .collect();
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
