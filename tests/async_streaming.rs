#![cfg(feature = "async-streaming")]

use std::io::Write;

use dataprof::{AsyncSourceInfo, AsyncStreamingProfiler, BytesSource, FileFormat};

/// Compare async profiling of a CSV file against the sync IncrementalProfiler.
/// Column counts and row counts must match; data types should agree.
#[tokio::test]
async fn test_async_vs_sync_parity() {
    // Build a test CSV
    let mut tmp = tempfile::NamedTempFile::new().unwrap();
    writeln!(tmp, "name,age,salary").unwrap();
    for i in 1..=500 {
        writeln!(tmp, "Person{},{},{}", i, 20 + i % 40, 30000 + i * 10).unwrap();
    }
    tmp.flush().unwrap();

    // --- Sync baseline ---
    let sync_report = dataprof::engines::streaming::IncrementalProfiler::new()
        .memory_limit_mb(16)
        .analyze_file(tmp.path())
        .unwrap();

    // --- Async ---
    let file = tokio::fs::File::open(tmp.path()).await.unwrap();
    let meta = std::fs::metadata(tmp.path()).unwrap();
    let info = AsyncSourceInfo {
        label: tmp.path().display().to_string(),
        format: FileFormat::Csv,
        size_hint: Some(meta.len()),
        source_system: None,
    };

    let async_report = AsyncStreamingProfiler::new()
        .memory_limit_mb(16)
        .analyze_stream((file, info))
        .await
        .unwrap();

    // Column count must match
    assert_eq!(
        sync_report.column_profiles.len(),
        async_report.column_profiles.len(),
        "Column count mismatch"
    );

    // Row counts must match
    assert_eq!(
        sync_report.scan_info.rows_scanned, async_report.scan_info.rows_scanned,
        "Row count mismatch: sync={} async={}",
        sync_report.scan_info.rows_scanned, async_report.scan_info.rows_scanned,
    );

    // Data types should match for each column
    for sync_col in &sync_report.column_profiles {
        let async_col = async_report
            .column_profiles
            .iter()
            .find(|c| c.name == sync_col.name)
            .unwrap_or_else(|| panic!("Missing column '{}' in async report", sync_col.name));

        assert_eq!(
            sync_col.data_type, async_col.data_type,
            "Data type mismatch for column '{}'",
            sync_col.name
        );

        assert_eq!(
            sync_col.total_count, async_col.total_count,
            "Total count mismatch for column '{}'",
            sync_col.name
        );
    }

    // The async report should have a Stream data source
    assert!(async_report.data_source.is_stream());
}

/// Verify that profiling a simple in-memory buffer works end-to-end.
#[tokio::test]
async fn test_bytes_source_end_to_end() {
    let csv = b"color,count\nred,10\nblue,20\ngreen,30\n";
    let source = BytesSource::new(
        bytes::Bytes::from_static(csv),
        AsyncSourceInfo {
            label: "colors".into(),
            format: FileFormat::Csv,
            size_hint: Some(csv.len() as u64),
            source_system: None,
        },
    );

    let report = AsyncStreamingProfiler::new()
        .analyze_stream(source)
        .await
        .unwrap();

    assert_eq!(report.column_profiles.len(), 2);
    assert_eq!(report.scan_info.rows_scanned, 3);

    let count_col = report
        .column_profiles
        .iter()
        .find(|c| c.name == "count")
        .expect("count column");
    assert_eq!(count_col.data_type, dataprof::DataType::Integer);
}
