#![cfg(feature = "async-streaming")]

use std::io::Write;

use dataprof::{
    AsyncSourceInfo, AsyncStreamingProfiler, BytesSource, EngineType, FileFormat, Profiler,
    StopCondition,
};

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

    // --- Sync baseline via Profiler API ---
    let sync_report = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(tmp.path())
        .unwrap();

    // --- Async ---
    let file = tokio::fs::File::open(tmp.path()).await.unwrap();
    let meta = std::fs::metadata(tmp.path()).unwrap();
    let info = AsyncSourceInfo::new(tmp.path().display().to_string(), FileFormat::Csv)
        .size_hint(Some(meta.len()));

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
        sync_report.execution.rows_processed, async_report.execution.rows_processed,
        "Row count mismatch: sync={} async={}",
        sync_report.execution.rows_processed, async_report.execution.rows_processed,
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
        AsyncSourceInfo::new("colors", FileFormat::Csv).size_hint(Some(csv.len() as u64)),
    );

    let report = AsyncStreamingProfiler::new()
        .analyze_stream(source)
        .await
        .unwrap();

    assert_eq!(report.column_profiles.len(), 2);
    assert_eq!(report.execution.rows_processed, 3);

    let count_col = report
        .column_profiles
        .iter()
        .find(|c| c.name == "count")
        .expect("count column");
    assert_eq!(count_col.data_type, dataprof::DataType::Integer);
}

// ---------------------------------------------------------------------------
// Profiler async facade tests (#219)
// ---------------------------------------------------------------------------

/// Verify `Profiler::profile_stream()` delegates correctly for CSV.
#[tokio::test]
async fn test_profiler_profile_stream_csv() {
    let csv = b"name,age\nAlice,30\nBob,25\nCarol,28\n";
    let source = BytesSource::new(
        bytes::Bytes::from_static(csv),
        AsyncSourceInfo::new("test-csv", FileFormat::Csv).size_hint(Some(csv.len() as u64)),
    );

    let report = Profiler::new().profile_stream(source).await.unwrap();

    assert_eq!(report.column_profiles.len(), 2);
    assert_eq!(report.execution.rows_processed, 3);
    assert!(report.execution.source_exhausted);
}

/// Verify `Profiler::profile_stream()` works with JSON format.
#[tokio::test]
async fn test_profiler_profile_stream_json() {
    let json =
        br#"[{"city":"Rome","pop":2873},{"city":"Milan","pop":1352},{"city":"Naples","pop":967}]"#;
    let source = BytesSource::new(
        bytes::Bytes::from_static(json),
        AsyncSourceInfo::new("test-json", FileFormat::Json).size_hint(Some(json.len() as u64)),
    );

    let report = Profiler::new().profile_stream(source).await.unwrap();

    assert_eq!(report.column_profiles.len(), 2);
    assert_eq!(report.execution.rows_processed, 3);
}

/// Verify `Profiler::profile_file()` on a temp CSV matches sync results.
#[tokio::test]
async fn test_profiler_profile_file_csv() {
    let mut tmp = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(tmp, "x,y,z").unwrap();
    for i in 0..100 {
        writeln!(tmp, "{},{},{}", i, i * 2, i * 3).unwrap();
    }
    tmp.flush().unwrap();

    let sync_report = Profiler::new()
        .engine(EngineType::Incremental)
        .analyze_file(tmp.path())
        .unwrap();

    let async_report = Profiler::new().profile_file(tmp.path()).await.unwrap();

    assert_eq!(
        sync_report.execution.rows_processed,
        async_report.execution.rows_processed,
    );
    assert_eq!(
        sync_report.column_profiles.len(),
        async_report.column_profiles.len(),
    );
}

/// Verify `Profiler::profile_file()` handles Parquet via spawn_blocking.
#[cfg(feature = "parquet")]
#[tokio::test]
async fn test_profiler_profile_file_parquet() {
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "b", "c"])),
        ],
    )
    .unwrap();

    let tmp = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let file = std::fs::File::create(tmp.path()).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();

    let report = Profiler::new().profile_file(tmp.path()).await.unwrap();

    assert_eq!(report.execution.rows_processed, 3);
    assert_eq!(report.column_profiles.len(), 2);
}

/// Verify that config options (stop_condition) are forwarded through the async facade.
#[tokio::test]
async fn test_profiler_profile_stream_with_stop_condition() {
    // Generate enough data so stop condition can trigger before all rows are read.
    let mut csv_data = String::from("id,value\n");
    for i in 0..10_000 {
        csv_data.push_str(&format!("{},val_{}\n", i, i));
    }

    let source = BytesSource::new(
        bytes::Bytes::from(csv_data.clone()),
        AsyncSourceInfo::new("big-csv", FileFormat::Csv).size_hint(Some(csv_data.len() as u64)),
    );

    let report = Profiler::new()
        .stop_when(StopCondition::MaxRows(100))
        .profile_stream(source)
        .await
        .unwrap();

    // Stop condition is evaluated per-chunk, so the engine may slightly overshoot.
    assert!(
        report.execution.rows_processed < 10_000,
        "Expected early stop but processed {} rows",
        report.execution.rows_processed,
    );
    assert!(!report.execution.source_exhausted);
    assert!(report.execution.truncation_reason.is_some());
}

/// Verify that Parquet format is rejected by `profile_stream`.
#[tokio::test]
async fn test_profiler_profile_stream_rejects_parquet() {
    let source = BytesSource::new(
        bytes::Bytes::from_static(b"not-real-parquet"),
        AsyncSourceInfo::new("fake.parquet", FileFormat::Parquet),
    );

    let result = Profiler::new().profile_stream(source).await;
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(
        err.contains("Parquet"),
        "Error should mention Parquet, got: {err}"
    );
}
