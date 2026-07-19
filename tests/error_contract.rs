//! Facade-level error contract: the same input classes must surface a stable
//! category, a real source path (never `'unknown'`), and an honest
//! supported-format claim. Guards the #373 product-error work.

use std::io::Write;

use dataprof::{DataProfilerError, Profiler};

#[test]
fn missing_file_reports_the_real_path_not_unknown() {
    let path = "definitely/does/not/exist/sales.csv";
    let err = Profiler::new()
        .analyze_file(path)
        .expect_err("a missing file must be an error");

    assert_eq!(err.category(), "file_not_found");
    let msg = err.to_string();
    assert!(msg.contains(path), "message should name the file: {msg}");
    assert!(
        !msg.contains("unknown"),
        "path must not be 'unknown': {msg}"
    );
}

#[test]
fn unsupported_format_does_not_overclaim() {
    let mut file = tempfile::Builder::new().suffix(".xlsx").tempfile().unwrap();
    write!(file, "not really a spreadsheet").unwrap();
    file.flush().unwrap();

    // A genuinely unknown extension is refused by the lightweight entry points.
    let err = Profiler::new()
        .infer_schema(file.path())
        .expect_err("xlsx is not a supported format");

    assert_eq!(err.category(), "unsupported_format");
    // The surfaced format list must match what this build can actually read.
    let msg = err.to_string();
    assert!(
        msg.contains("CSV"),
        "message should list real formats: {msg}"
    );
    #[cfg(feature = "parquet")]
    assert!(
        msg.contains("Parquet"),
        "a parquet build should advertise Parquet: {msg}"
    );
    #[cfg(not(feature = "parquet"))]
    assert!(
        !msg.contains("Parquet"),
        "must not advertise Parquet without the feature: {msg}"
    );
}

#[test]
fn undecodable_csv_error_never_prints_unknown() {
    // Non-UTF-8 input (latin-1 `é` = 0xE9) is a deterministic hard failure that
    // every engine refuses — the exact case where the old error path printed the
    // `'unknown'` placeholder (see #426). The contract: it errors, and neither
    // the message nor its suggestion names the file as `'unknown'`.
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(b"name,city\nJos\xE9,Z\xFCrich\n").unwrap();
    file.flush().unwrap();

    let err = Profiler::new()
        .analyze_file(file.path())
        .expect_err("non-UTF-8 CSV must fail rather than silently mis-decode");

    let rendered = err.to_string();
    assert!(
        !rendered.contains("unknown"),
        "undecodable-file error must not print 'unknown': {rendered}"
    );
}

#[test]
fn error_suggestion_is_reachable_without_parsing_the_message() {
    let err = DataProfilerError::unsupported_format("xlsx");
    assert!(
        err.suggestion().is_some(),
        "actionable errors expose a structured suggestion"
    );
}
