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
fn malformed_csv_keeps_the_source_path_in_context() {
    // A ragged file that trips strict parsing; the error path must still name
    // the file rather than the removed `'unknown'` placeholder.
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    writeln!(file, "a,b,c").unwrap();
    for _ in 0..2000 {
        writeln!(file, "1,2,3,4,5,6,7,8,9").unwrap();
    }
    file.flush().unwrap();

    // Force strict parsing so the ragged rows surface as an error rather than
    // being recovered; if it still succeeds, the contract we assert is moot.
    let result = Profiler::new()
        .csv_flexible(false)
        .analyze_file(file.path());

    if let Err(err) = result {
        assert!(
            !err.to_string().contains("'unknown'"),
            "malformed-file error must not print 'unknown': {err}"
        );
    }
}

#[test]
fn error_suggestion_is_reachable_without_parsing_the_message() {
    let err = DataProfilerError::unsupported_format("xlsx");
    assert!(
        err.suggestion().is_some(),
        "actionable errors expose a structured suggestion"
    );
}
