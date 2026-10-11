//! Pin the existing headerless-CSV gap until the reader pipelines converge (#884).
//! `AsyncSourceInfo::has_header(false)` is currently ignored by both Rust entry points.
#![cfg(feature = "async-streaming")]

use dataprof::{AsyncSourceInfo, AsyncStreamingProfiler, BytesSource, FileFormat, Profiler};

#[tokio::test]
async fn headerless_async_source_still_treats_first_row_as_headers() {
    let bytes = b"1\n2\n3\n";
    let info = AsyncSourceInfo::new("headerless", FileFormat::Csv).has_header(false);
    let direct = AsyncStreamingProfiler::new()
        .analyze_stream(BytesSource::new(bytes.as_slice().into(), info.clone()))
        .await
        .unwrap();
    let facade = Profiler::new()
        .profile_stream(BytesSource::new(bytes.as_slice().into(), info))
        .await
        .unwrap();
    for (route, report) in [("direct", direct), ("facade", facade)] {
        assert_eq!(report.execution.rows_processed, 2, "{route}: #884");
        assert_eq!(report.column_profiles[0].name, "1", "{route}: #884");
    }
}
