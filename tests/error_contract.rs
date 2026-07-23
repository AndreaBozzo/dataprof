//! Facade-level error contract: the same input classes must surface a stable
//! category, a real source path (never `'unknown'`), and an honest
//! supported-format claim. Guards the #373 product-error work.

use std::io::Write;

use dataprof::{DataProfilerError, EngineType, Profiler};

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
fn non_utf8_csv_reports_one_clean_encoding_diagnostic() {
    // Non-UTF-8 input (latin-1 `é` = 0xE9) is a deterministic hard failure that
    // every engine refuses. Instead of stacking two engine errors with wrong
    // advice, the profiler surfaces a single encoding diagnostic (#426).
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    file.write_all(b"name,city\nJos\xE9,Z\xFCrich\n").unwrap();
    file.flush().unwrap();

    for engine in [
        EngineType::Auto,
        EngineType::Incremental,
        EngineType::Columnar,
    ] {
        let err = Profiler::new()
            .engine(engine)
            .analyze_file(file.path())
            .expect_err("non-UTF-8 CSV must fail rather than silently mis-decode");

        assert_eq!(err.category(), "encoding");
        let rendered = err.to_string();
        // Names the real file, the encoding guess, the offset, and a re-encode fix...
        assert!(!rendered.contains("unknown"), "{rendered}");
        assert!(rendered.to_lowercase().contains("utf-8"), "{rendered}");
        assert!(
            rendered.contains("iconv"),
            "gives a re-encode command: {rendered}"
        );
        // ...and drops the old misleading, stacked messaging.
        assert!(!rendered.contains("All engines failed"), "{rendered}");
        assert!(
            !rendered.to_lowercase().contains("check file permissions"),
            "encoding failure must not blame permissions: {rendered}"
        );
    }
}

#[test]
fn all_engines_failed_message_is_not_doubled() {
    // The `AllEnginesFailed` variant prefixes "All engines failed: "; the wrapped
    // message must not repeat it (#426).
    let err = DataProfilerError::AllEnginesFailed {
        message: "Primary (Incremental): boom. Fallback (Arrow): boom.".to_string(),
    };
    let rendered = err.to_string();
    assert_eq!(
        rendered.matches("All engines failed").count(),
        1,
        "{rendered}"
    );
}

#[test]
fn ragged_csv_is_not_mislabeled_as_encoding() {
    // A structural (ragged-row) problem in valid UTF-8 must keep its own category,
    // never be swept into the encoding path.
    let mut file = tempfile::Builder::new().suffix(".csv").tempfile().unwrap();
    // Force a hard parse error the engines cannot recover: an unterminated quote.
    file.write_all(b"a,b\n\"unterminated,2\n").unwrap();
    file.flush().unwrap();

    if let Err(err) = Profiler::new().analyze_file(file.path()) {
        assert_ne!(
            err.category(),
            "encoding",
            "valid UTF-8 is not an encoding fault"
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
