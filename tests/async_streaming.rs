#![cfg(feature = "async-streaming")]

use std::io::Write;
use std::net::{TcpListener, TcpStream};
use std::thread::JoinHandle;
use std::time::Duration;

use dataprof::{
    AsyncSourceInfo, AsyncStreamingProfiler, BytesSource, EngineType, FileFormat, Profiler,
    StopCondition,
};

struct MockHttpServer {
    url: String,
    handle: JoinHandle<()>,
}

impl MockHttpServer {
    fn url(&self) -> &str {
        &self.url
    }

    fn join(self) {
        self.handle.join().unwrap();
    }
}

fn read_http_request(stream: &mut TcpStream) -> std::io::Result<String> {
    use std::io::Read;

    stream.set_read_timeout(Some(Duration::from_secs(2)))?;

    let mut request = Vec::new();
    let mut buffer = [0; 1024];
    loop {
        let bytes_read = stream.read(&mut buffer)?;
        if bytes_read == 0 {
            break;
        }

        request.extend_from_slice(&buffer[..bytes_read]);
        if request.windows(4).any(|window| window == b"\r\n\r\n") {
            break;
        }
    }

    Ok(String::from_utf8_lossy(&request).into_owned())
}

fn spawn_csv_server(body: &'static [u8]) -> MockHttpServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();

    let handle = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("mock server accept failed");
        let request = read_http_request(&mut stream).expect("mock server read failed");
        assert!(
            request.starts_with("GET /data.csv "),
            "unexpected request: {request}"
        );

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream
            .write_all(response.as_bytes())
            .expect("mock server response headers failed");
        stream
            .write_all(body)
            .expect("mock server response body failed");
    });

    MockHttpServer {
        url: format!("http://127.0.0.1:{port}/data.csv"),
        handle,
    }
}

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

#[tokio::test]
async fn test_profiler_profile_stream_header_only_csv_preserves_schema() {
    let csv = b"name,age,salary\n";
    let source = BytesSource::new(
        bytes::Bytes::from_static(csv),
        AsyncSourceInfo::new("header-only-csv", FileFormat::Csv).size_hint(Some(csv.len() as u64)),
    );

    let report = Profiler::new().profile_stream(source).await.unwrap();

    let names: Vec<&str> = report
        .column_profiles
        .iter()
        .map(|profile| profile.name.as_str())
        .collect();
    assert_eq!(names, vec!["name", "age", "salary"]);
    assert!(report.column_profiles.iter().all(|profile| {
        profile.data_type == dataprof::DataType::String && profile.total_count == 0
    }));
    assert_eq!(report.execution.rows_processed, 0);
}

#[tokio::test]
async fn test_profiler_profile_stream_rejects_duplicate_csv_headers() {
    let csv = b"x,x\n1,2\n";
    let source = BytesSource::new(
        bytes::Bytes::from_static(csv),
        AsyncSourceInfo::new("duplicate-header-csv", FileFormat::Csv),
    );

    let error = Profiler::new()
        .profile_stream(source)
        .await
        .expect_err("async CSV profiling must reject duplicate headers");

    assert_eq!(error.category(), "duplicate_column_name");
    assert!(error.to_string().contains("'x'"));
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

#[tokio::test]
async fn test_profiler_profile_url_csv() {
    let server = spawn_csv_server(b"city,pop\nRome,2873\nMilan,1352\n");

    let report = Profiler::new().profile_url(server.url()).await.unwrap();

    assert_eq!(report.execution.rows_processed, 2);
    assert_eq!(report.column_profiles.len(), 2);
    assert!(report.data_source.is_stream());

    server.join();
}

/// The delimiter is a property of the data, not of the transport: a
/// semicolon-separated source must profile the same read from disk or off a
/// stream. Previously the async reader always assumed a comma, collapsing such
/// a source into a single column. #462.
#[tokio::test]
async fn test_async_delimiter_matches_the_file_path() {
    let body = "name;age;city\nAlice;25;NYC\nBob;30;LA\n";

    let mut tmp = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
    write!(tmp, "{body}").unwrap();
    tmp.flush().unwrap();
    let from_file = Profiler::new().analyze_file(tmp.path()).unwrap();

    let source = BytesSource::new(
        bytes::Bytes::from(body),
        AsyncSourceInfo::new("semicolon", FileFormat::Csv),
    );
    let from_stream = Profiler::new().profile_stream(source).await.unwrap();

    assert_eq!(from_file.column_profiles.len(), 3);
    assert_eq!(
        from_stream.column_profiles.len(),
        from_file.column_profiles.len(),
        "async must detect the same delimiter as the file path"
    );
    assert_eq!(
        from_stream.execution.rows_processed,
        from_file.execution.rows_processed
    );
}

/// An explicit delimiter was accepted and ignored on the async path.
#[tokio::test]
async fn test_async_explicit_delimiter_is_honored() {
    let source = BytesSource::new(
        bytes::Bytes::from_static(b"a|b|c\n1|2|3\n"),
        AsyncSourceInfo::new("piped", FileFormat::Csv),
    );

    let report = Profiler::new()
        .csv_delimiter(b'|')
        .profile_stream(source)
        .await
        .unwrap();

    assert_eq!(report.column_profiles.len(), 3);
    assert_eq!(report.execution.rows_processed, 1);
}

/// A remote CSV rides the same async reader as byte streams, so a ragged
/// download must carry the same structural signal rather than looking clean
/// because it arrived over HTTP. #462.
#[tokio::test]
async fn test_profiler_profile_url_counts_ragged_rows() {
    let server = spawn_csv_server(b"city,pop\nRome,2873\nMilan\nTurin,886,EXTRA\n");

    let report = Profiler::new().profile_url(server.url()).await.unwrap();

    assert_eq!(report.execution.rows_processed, 3);
    assert_eq!(
        report.execution.ragged_row_count, 2,
        "a ragged remote CSV must not profile as clean"
    );

    server.join();
}

#[cfg(not(feature = "parquet-async"))]
#[tokio::test]
async fn test_profiler_profile_url_rejects_parquet_without_feature() {
    let error = Profiler::new()
        .format(FileFormat::Parquet)
        .profile_url("https://example.com/data.parquet")
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("parquet-async"), "unexpected error: {error}");
}

/// A Parquet source that is *not* a materialized buffer must keep its source and
/// reach the streaming profiler, which refuses it by name. This is what
/// `take_bytes` handing the source back buys: answering "not materialized" by
/// consuming the source would leave nothing to stream (gh #551).
#[tokio::test]
async fn test_profile_stream_refuses_parquet_that_is_not_a_buffer() {
    let tmp = tempfile::NamedTempFile::with_suffix(".parquet").unwrap();
    let file = tokio::fs::File::open(tmp.path()).await.unwrap();
    let info = AsyncSourceInfo::new("streamed.parquet", FileFormat::Parquet);

    let error = Profiler::new()
        .profile_stream((file, info))
        .await
        .unwrap_err()
        .to_string();

    assert!(error.contains("does not support"), "{error}");
    assert!(error.contains("synchronous profiler"), "{error}");
}

/// The `Finished` progress event must count the rows of the chunk the row cap
/// stops in, so it agrees with `rows_processed` like the other engines (gh #824).
#[tokio::test]
async fn test_profiler_profile_stream_finished_event_counts_capped_chunk() {
    use std::sync::{Arc, Mutex};

    let mut csv_data = String::from("id,value\n");
    for i in 0..10_000 {
        csv_data.push_str(&format!("{},val_{}\n", i, i));
    }

    let source = BytesSource::new(
        bytes::Bytes::from(csv_data.clone()),
        AsyncSourceInfo::new("capped-csv", FileFormat::Csv).size_hint(Some(csv_data.len() as u64)),
    );

    let finished = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&finished);
    let sink = dataprof::ProgressSink::Callback(Arc::new(move |event| {
        if let dataprof::ProgressEvent::Finished { total_rows, .. } = event {
            *captured.lock().unwrap() = Some(total_rows);
        }
    }));

    let report = Profiler::new()
        .stop_when(StopCondition::MaxRows(150))
        .progress_interval(Duration::ZERO)
        .progress_sink(sink)
        .profile_stream(source)
        .await
        .unwrap();

    assert_eq!(report.execution.rows_processed, 150);
    assert_eq!(*finished.lock().unwrap(), Some(150));
}

/// The `Finished` progress event must count the rows of the chunk when a stop condition
/// other than row cap (e.g. MaxBytes) stops an async stream (gh #969).
#[tokio::test]
async fn test_profiler_profile_stream_finished_event_counts_bytes_stop_condition() {
    use std::sync::{Arc, Mutex};

    let mut csv_data = String::from("id,value\n");
    for i in 0..10_000 {
        csv_data.push_str(&format!("{},val_{}\n", i, i));
    }

    let source = BytesSource::new(
        bytes::Bytes::from(csv_data.clone()),
        AsyncSourceInfo::new("bytes-capped-csv", FileFormat::Csv)
            .size_hint(Some(csv_data.len() as u64)),
    );

    let finished = Arc::new(Mutex::new(None));
    let captured = Arc::clone(&finished);
    let sink = dataprof::ProgressSink::Callback(Arc::new(move |event| {
        if let dataprof::ProgressEvent::Finished { total_rows, .. } = event {
            *captured.lock().unwrap() = Some(total_rows);
        }
    }));

    let report = Profiler::new()
        .stop_when(StopCondition::MaxBytes(50_000))
        .progress_interval(Duration::ZERO)
        .progress_sink(sink)
        .profile_stream(source)
        .await
        .unwrap();

    assert!(report.execution.rows_processed > 0);
    assert_eq!(
        *finished.lock().unwrap(),
        Some(report.execution.rows_processed),
        "finished event total_rows must match execution.rows_processed"
    );
}

/// A progress channel that is full when profiling ends must still deliver the
/// `Finished` event to a consumer draining the channel (gh #974).
#[tokio::test(flavor = "current_thread")]
async fn test_progress_channel_capacity_one_receives_finished_event() {
    for stop_cond in [
        None,
        Some(StopCondition::MaxRows(150)),
        Some(StopCondition::MaxBytes(50_000)),
    ] {
        let mut csv_data = String::from("id,value\n");
        for i in 0..10_000 {
            csv_data.push_str(&format!("{},val_{}\n", i, i));
        }

        let source = BytesSource::new(
            bytes::Bytes::from(csv_data.clone()),
            AsyncSourceInfo::new("bytes-csv", FileFormat::Csv)
                .size_hint(Some(csv_data.len() as u64)),
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel(1);
        let consumer_task = tokio::spawn(async move {
            let mut finished_received = false;
            while let Some(event) = rx.recv().await {
                if let dataprof::ProgressEvent::Finished { .. } = event {
                    finished_received = true;
                    break;
                }
            }
            finished_received
        });

        let mut profiler = Profiler::new()
            .progress_interval(Duration::ZERO)
            .progress_channel(tx);

        if let Some(ref cond) = stop_cond {
            profiler = profiler.stop_when(cond.clone());
        }

        let report = profiler.profile_stream(source).await.unwrap();
        assert!(report.execution.rows_processed > 0);

        drop(profiler);

        let finished_received = consumer_task.await.unwrap();
        assert!(
            finished_received,
            "Finished event must be received by channel consumer for stop_condition {:?}",
            stop_cond
        );
    }
}

/// A stream that fails during profiling must never emit `ProgressEvent::Finished`.
#[tokio::test]
async fn test_progress_channel_does_not_emit_finished_on_error() {
    // 1. Duplicate CSV headers error
    {
        let csv = b"x,x\n1,2\n";
        let source = BytesSource::new(
            bytes::Bytes::from_static(csv),
            AsyncSourceInfo::new("duplicate-header-csv", FileFormat::Csv),
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let consumer = tokio::spawn(async move {
            let mut got_finished = false;
            while let Some(event) = rx.recv().await {
                if let dataprof::ProgressEvent::Finished { .. } = event {
                    got_finished = true;
                }
            }
            got_finished
        });

        let res = Profiler::new()
            .progress_channel(tx)
            .profile_stream(source)
            .await;

        assert!(
            res.is_err(),
            "profiling should fail due to duplicate headers"
        );
        let got_finished = consumer.await.unwrap();
        assert!(
            !got_finished,
            "Finished event must not be emitted when profiling fails on duplicate headers"
        );
    }

    // 2. Unterminated quote error in strict CSV mode
    {
        let csv = b"id,val\n1,\"unterminated\n2,normal\n";
        let source = BytesSource::new(
            bytes::Bytes::from_static(csv),
            AsyncSourceInfo::new("unterminated-quote-csv", FileFormat::Csv),
        );

        let (tx, mut rx) = tokio::sync::mpsc::channel(10);
        let consumer = tokio::spawn(async move {
            let mut got_finished = false;
            while let Some(event) = rx.recv().await {
                if let dataprof::ProgressEvent::Finished { .. } = event {
                    got_finished = true;
                }
            }
            got_finished
        });

        let res = Profiler::new()
            .csv_flexible(false)
            .progress_channel(tx)
            .profile_stream(source)
            .await;

        assert!(
            res.is_err(),
            "profiling should fail due to unterminated quote in strict mode"
        );
        let got_finished = consumer.await.unwrap();
        assert!(
            !got_finished,
            "Finished event must not be emitted when profiling fails on unterminated quote"
        );
    }
}

/// A progress channel whose capacity is 1 must deliver `Finished` without blocking
/// or timing out even when the caller drains the channel only after profiling completes (gh #974).
#[tokio::test]
async fn test_progress_channel_drained_after_profile_under_timeout() {
    let mut csv_data = String::from("id,value\n");
    for i in 0..10_000 {
        csv_data.push_str(&format!("{},val_{}\n", i, i));
    }

    let source = BytesSource::new(
        bytes::Bytes::from(csv_data.clone()),
        AsyncSourceInfo::new("drained-after-csv", FileFormat::Csv)
            .size_hint(Some(csv_data.len() as u64)),
    );

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let profiler = Profiler::new()
        .progress_interval(Duration::ZERO)
        .progress_channel(tx);

    let report = tokio::time::timeout(Duration::from_secs(2), profiler.profile_stream(source))
        .await
        .expect("profiling must not deadlock or time out while channel is not yet drained")
        .unwrap();

    assert!(report.execution.rows_processed > 0);

    let mut finished_received = false;
    while let Ok(event) = rx.try_recv() {
        if let dataprof::ProgressEvent::Finished { .. } = event {
            finished_received = true;
            break;
        }
    }
    assert!(
        finished_received,
        "Finished event must be received by consumer draining after profiling completes"
    );
}

/// A facade-bracketed columnar file route must also deliver `Finished` using the reserved permit.
#[test]
fn test_progress_channel_file_route_receives_finished_event() {
    use std::io::Write;

    let mut tmp = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(tmp, "id,value").unwrap();
    for i in 0..100 {
        writeln!(tmp, "{},val_{}", i, i).unwrap();
    }
    tmp.flush().unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let report = Profiler::new()
        .engine(dataprof::EngineType::Columnar)
        .progress_interval(Duration::ZERO)
        .progress_channel(tx)
        .analyze_file(tmp.path())
        .unwrap();

    assert_eq!(report.execution.rows_processed, 100);

    let mut finished_received = false;
    while let Ok(event) = rx.try_recv() {
        if let dataprof::ProgressEvent::Finished { .. } = event {
            finished_received = true;
            break;
        }
    }
    assert!(
        finished_received,
        "Finished event must be delivered on file routes via reserved permit"
    );
}

/// Reusing a profiler must reserve fresh terminal capacity for every run.
#[test]
fn test_progress_channel_reused_profiler_receives_finished_each_run() {
    use std::io::Write;

    let mut tmp = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(tmp, "id,value").unwrap();
    for i in 0..100 {
        writeln!(tmp, "{},val_{}", i, i).unwrap();
    }
    tmp.flush().unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let profiler = Profiler::new()
        .progress_interval(Duration::ZERO)
        .progress_channel(tx);

    for run in 1..=2 {
        let report = profiler.analyze_file(tmp.path()).unwrap();
        assert_eq!(report.execution.rows_processed, 100);

        let events = std::iter::from_fn(|| rx.try_recv().ok()).collect::<Vec<_>>();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, dataprof::ProgressEvent::Finished { .. })),
            "run {run} must receive a freshly reserved Finished event; got {events:?}"
        );
    }
}

/// Reusing a facade-bracketed file route must reserve fresh terminal capacity for every run.
#[test]
fn test_progress_channel_reused_columnar_file_route_receives_finished_each_run() {
    use std::io::Write;

    let mut tmp = tempfile::NamedTempFile::with_suffix(".csv").unwrap();
    writeln!(tmp, "id,value").unwrap();
    for i in 0..100 {
        writeln!(tmp, "{},val_{}", i, i).unwrap();
    }
    tmp.flush().unwrap();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    let profiler = Profiler::new()
        .engine(dataprof::EngineType::Columnar)
        .progress_interval(Duration::ZERO)
        .progress_channel(tx);

    for run in 1..=2 {
        let report = profiler.analyze_file(tmp.path()).unwrap();
        assert_eq!(report.execution.rows_processed, 100);

        let events = std::iter::from_fn(|| rx.try_recv().ok()).collect::<Vec<_>>();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, dataprof::ProgressEvent::Finished { .. })),
            "columnar run {run} must receive a freshly reserved Finished event; got {events:?}"
        );
    }
}

/// Preserve the public enum constructor accepted before gh #974.
#[test]
fn test_progress_sink_channel_variant_accepts_sender() {
    let (tx, _rx) = tokio::sync::mpsc::channel(1);
    let _sink = dataprof::ProgressSink::Channel(tx);
}
