//! Partial APIs retain I/O causes. Python twin: test_partial_io_causes.py.

use dataprof::{DataProfilerError, analyze_structure, infer_schema, quick_row_count};
use std::error::Error;

#[test]
fn partial_apis_preserve_non_missing_io_causes() {
    let dir = tempfile::tempdir().unwrap();
    let mut wrong = Vec::new();
    let mut formats = vec!["csv", "json", "jsonl"];
    if cfg!(feature = "parquet") {
        formats.push("parquet");
    }
    for format in formats {
        let path = dir.path().join(format!("directory.{format}"));
        std::fs::create_dir(&path).unwrap();
        for (api, result) in [
            ("count", quick_row_count(&path).map(|_| ())),
            ("schema", infer_schema(&path).map(|_| ())),
            ("structure", analyze_structure(&path, None).map(|_| ())),
        ] {
            match result {
                Err(error) if !matches!(error, DataProfilerError::FileNotFound { .. }) => {
                    let mut cause: Option<&(dyn Error + 'static)> = Some(&error);
                    let mut has_io = false;
                    while let Some(error) = cause {
                        has_io |= error.downcast_ref::<std::io::Error>().is_some();
                        cause = error.source();
                    }
                    if !has_io {
                        wrong.push(format!("[{api}] {format}: lost I/O cause: {error}"));
                    }
                }
                result => wrong.push(format!("[{api}] {format}: {result:?}")),
            }
        }
        let missing = dir.path().join(format!("missing.{format}"));
        for (api, result) in [
            ("count", quick_row_count(&missing).map(|_| ())),
            ("schema", infer_schema(&missing).map(|_| ())),
            ("structure", analyze_structure(&missing, None).map(|_| ())),
        ] {
            if !matches!(result, Err(DataProfilerError::FileNotFound { .. })) {
                wrong.push(format!("[{api}] missing {format}: {result:?}"));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
