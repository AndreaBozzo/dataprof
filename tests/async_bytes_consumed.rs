//! A complete async scan reports `bytes_consumed` equal to the source's length,
//! with or without a size hint (#1223). Without one the report summed the
//! readers' per-chunk tallies, and the JSON readers derived theirs from the
//! records they parsed: a JSONL body of 17 bytes reported 26, a JSON array of
//! 22 bytes reported 18. The CSV reader left out the blank lines after its last
//! record.
//!
//! `python/tests/test_async_bytes_consumed.py` is the Python twin; it covers a
//! URL served without `Content-Length`.

#![cfg(feature = "async-streaming")]

use std::io::Write;

use dataprof::{AsyncSourceInfo, BytesSource, ChunkSize, FileFormat, Profiler, StopCondition};

/// `(case, format, source)`. Each holds bytes no record carries: blank lines,
/// white space between elements, brackets, a BOM, records that yield no row.
const SOURCES: [(&str, FileFormat, &[u8]); 11] = [
    (
        "JSONL, blank line",
        FileFormat::Jsonl,
        b"{\"a\":1}\n\n{\"a\":2}\n",
    ),
    (
        "JSONL, BOM",
        FileFormat::Jsonl,
        b"\xef\xbb\xbf{\"a\":1}\n{\"a\":2}\n",
    ),
    (
        "JSONL, malformed line",
        FileFormat::Jsonl,
        b"{\"a\":1}\n{bad\n{\"a\":2}\n",
    ),
    (
        "JSONL, no final newline",
        FileFormat::Jsonl,
        b"{\"a\":1}\n{\"a\":2}",
    ),
    (
        "JSON array, white space",
        FileFormat::Json,
        b"[ {\"a\":1} , {\"a\":2} ]\n",
    ),
    (
        "JSON array, non-object element",
        FileFormat::Json,
        b"[1, {\"a\":2}, {\"a\":3}]",
    ),
    (
        "JSON array, BOM",
        FileFormat::Json,
        b"\xef\xbb\xbf[{\"a\":1},{\"a\":2}]",
    ),
    ("JSON object", FileFormat::Json, b"  {\"a\": 1}  \n"),
    ("CSV", FileFormat::Csv, b"a,b\n1,2\n3,4\n"),
    (
        "CSV, trailing blank lines",
        FileFormat::Csv,
        b"a,b\n1,2\n3,4\n\n\n",
    ),
    ("CSV, BOM", FileFormat::Csv, b"\xef\xbb\xbfa,b\n1,2\n3,4\n"),
];

/// Default chunks, and chunks of one byte, so every record ends a chunk and
/// the bytes after the last one arrive in a chunk of their own.
const CHUNK_SIZES: [(&str, ChunkSize); 2] = [
    ("default chunks", ChunkSize::Adaptive),
    ("one-byte chunks", ChunkSize::Fixed(1)),
];

fn buffer(data: &'static [u8], format: FileFormat, hinted: bool) -> BytesSource {
    let info = AsyncSourceInfo::new("upload", format);
    let info = if hinted {
        info.size_hint(Some(data.len() as u64))
    } else {
        info
    };
    BytesSource::new(bytes::Bytes::from_static(data), info)
}

/// Rows each source holds, for a row cap that the scan meets at its end.
fn rows(case: &str) -> u64 {
    if case == "JSON object" { 1 } else { 2 }
}

#[tokio::test]
async fn a_complete_async_scan_consumes_the_whole_source() {
    let mut wrong = Vec::new();
    for (case, format, data) in SOURCES {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        file.write_all(data).unwrap();
        file.flush().unwrap();
        for (chunks, chunk_size) in CHUNK_SIZES {
            for (cap, stop) in [
                ("no cap", StopCondition::Never),
                ("a cap met at the end", StopCondition::MaxRows(rows(case))),
            ] {
                let profiler = Profiler::new()
                    .chunk_size(chunk_size.clone())
                    .stop_when(stop.clone());
                let handle = tokio::fs::File::open(file.path()).await.unwrap();
                let routes = [
                    (
                        "buffer, hinted",
                        profiler
                            .profile_stream(buffer(data, format.clone(), true))
                            .await,
                    ),
                    (
                        "buffer, no hint",
                        profiler
                            .profile_stream(buffer(data, format.clone(), false))
                            .await,
                    ),
                    (
                        "file stream, no hint",
                        profiler
                            .profile_stream((handle, AsyncSourceInfo::new("pipe", format.clone())))
                            .await,
                    ),
                ];
                for (route, outcome) in routes {
                    let route = format!("{case}, {chunks}, {cap}, {route}");
                    match outcome {
                        Ok(report) => {
                            let execution = &report.execution;
                            if execution.bytes_consumed != Some(data.len() as u64)
                                || !execution.source_exhausted
                            {
                                wrong.push(format!(
                                    "{route}: bytes_consumed {:?}, source_exhausted {}, \
                                     expected Some({}) and true",
                                    execution.bytes_consumed,
                                    execution.source_exhausted,
                                    data.len()
                                ));
                            }
                        }
                        Err(error) => wrong.push(format!("{route}: {error}")),
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A row cap that leaves a JSONL line unread ends the count at the last row
/// profiled, under any chunk size: neither the blank line after it nor the
/// record past the cap is counted. One-byte chunks put the end of the row at a
/// chunk boundary, where the bytes after it could otherwise arrive in a chunk of
/// their own and read as the end of the source.
#[tokio::test]
async fn a_capped_jsonl_scan_counts_no_line_past_the_cap() {
    let data: &'static [u8] = b"{\"a\":1}\n\n{\"a\":2}\n";
    let mut wrong = Vec::new();
    for (chunks, chunk_size) in CHUNK_SIZES {
        let report = Profiler::new()
            .chunk_size(chunk_size)
            .stop_when(StopCondition::MaxRows(1))
            .profile_stream(buffer(data, FileFormat::Jsonl, false))
            .await
            .unwrap();
        if report.execution.bytes_consumed != Some(8) || report.execution.source_exhausted {
            wrong.push(format!(
                "{chunks}: bytes_consumed {:?}, source_exhausted {}, expected Some(8) and false",
                report.execution.bytes_consumed, report.execution.source_exhausted
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// The JSON array twin of the test above: the count ends after the last
/// element profiled, before the comma and the element past the cap.
#[tokio::test]
async fn a_capped_json_array_scan_counts_through_the_last_row() {
    let data: &'static [u8] = b"[{\"a\":1},{\"a\":2}]";
    let mut wrong = Vec::new();
    for (chunks, chunk_size) in CHUNK_SIZES {
        let report = Profiler::new()
            .chunk_size(chunk_size)
            .stop_when(StopCondition::MaxRows(1))
            .profile_stream(buffer(data, FileFormat::Json, false))
            .await
            .unwrap();
        if report.execution.bytes_consumed != Some(8) || report.execution.source_exhausted {
            wrong.push(format!(
                "{chunks}: bytes_consumed {:?}, source_exhausted {}, expected Some(8) and false",
                report.execution.bytes_consumed, report.execution.source_exhausted
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// A row cap that leaves a row unread is a truncated scan, with or without a
/// size hint, and counts no more than the source holds. Counting the bytes
/// after the last row must not put a chunk of bytes alone ahead of the reader's
/// marker for a record past the cap, which the processor would take for the
/// end of the source.
#[tokio::test]
async fn a_cap_that_leaves_a_row_unread_is_a_truncation() {
    let mut wrong = Vec::new();
    for (case, format, data) in SOURCES {
        for (chunks, chunk_size) in CHUNK_SIZES {
            for cap in 0..rows(case) {
                for hinted in [true, false] {
                    let route = format!("{case}, {chunks}, cap {cap}, hinted {hinted}");
                    match Profiler::new()
                        .chunk_size(chunk_size.clone())
                        .stop_when(StopCondition::MaxRows(cap))
                        .profile_stream(buffer(data, format.clone(), hinted))
                        .await
                    {
                        Ok(report) => {
                            let execution = &report.execution;
                            let within = execution
                                .bytes_consumed
                                .is_some_and(|bytes| bytes <= data.len() as u64);
                            if execution.source_exhausted || !within {
                                wrong.push(format!(
                                    "{route}: bytes_consumed {:?}, source_exhausted {}",
                                    execution.bytes_consumed, execution.source_exhausted
                                ));
                            }
                        }
                        Err(error) => wrong.push(format!("{route}: {error}")),
                    }
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

/// Stop conditions met at the end of the source, found in review of #1226. A
/// stop whose look one chunk past it finds no row left is not a truncation,
/// on every format, and progress counts the bytes the report counts. A
/// progress sink makes async CSV count its rows before the scan, and that
/// count would answer for it, so CSV runs without one; CSV also leaves the
/// header out of progress, which predates #1223.
#[tokio::test]
async fn a_stop_at_the_end_of_the_source_is_a_complete_scan() {
    use std::sync::{Arc, Mutex};

    use dataprof::ProgressEvent;

    let cases: [(&str, &'static [u8], FileFormat, ChunkSize, StopCondition); 7] = [
        (
            "JSON array, one-byte chunks, a cap met at the end",
            b"[ {\"a\":1} , {\"a\":2} ]\n",
            FileFormat::Json,
            ChunkSize::Fixed(1),
            StopCondition::MaxRows(2),
        ),
        (
            "empty JSON array, a cap of zero",
            b"[]",
            FileFormat::Json,
            ChunkSize::Adaptive,
            StopCondition::MaxRows(0),
        ),
        (
            "JSON array, one-byte chunks, a byte budget met before the bracket",
            b"[{\"a\":1}]",
            FileFormat::Json,
            ChunkSize::Fixed(1),
            StopCondition::MaxBytes(8),
        ),
        (
            "CSV, trailing blank lines, a cap of zero",
            b"a,b\n\n\n",
            FileFormat::Csv,
            ChunkSize::Adaptive,
            StopCondition::MaxRows(0),
        ),
        (
            "CSV, one-byte chunks, a byte budget met by the trailing blank lines",
            b"a,b\n1,2\n\n\n",
            FileFormat::Csv,
            ChunkSize::Fixed(1),
            StopCondition::MaxBytes(10),
        ),
        (
            "CSV, a byte budget met by the last chunk",
            b"a,b\n1,2\n\n\n",
            FileFormat::Csv,
            ChunkSize::Adaptive,
            StopCondition::MaxBytes(9),
        ),
        (
            "JSONL, a byte budget met by the last chunk",
            b"{\"a\":1}\n\n\n",
            FileFormat::Jsonl,
            ChunkSize::Adaptive,
            StopCondition::MaxBytes(9),
        ),
    ];
    let mut wrong = Vec::new();
    for (case, data, format, chunk_size, stop) in cases {
        for hinted in [true, false] {
            let events = Arc::new(Mutex::new(Vec::new()));
            let sink = Arc::clone(&events);
            let route = format!("{case}, hinted {hinted}");
            let profiler = Profiler::new()
                .chunk_size(chunk_size.clone())
                .stop_when(stop.clone());
            let profiler = if format == FileFormat::Csv {
                profiler
            } else {
                profiler.on_progress(move |event| sink.lock().unwrap().push(event))
            };
            let report = match profiler
                .profile_stream(buffer(data, format.clone(), hinted))
                .await
            {
                Ok(report) => report,
                Err(error) => {
                    wrong.push(format!("{route}: {error}"));
                    continue;
                }
            };
            let finished = events
                .lock()
                .unwrap()
                .iter()
                .rev()
                .find_map(|event| match event {
                    ProgressEvent::Finished { total_bytes, .. } => Some(*total_bytes),
                    _ => None,
                });
            let execution = &report.execution;
            let progress_agrees = format == FileFormat::Csv || finished == execution.bytes_consumed;
            if execution.bytes_consumed != Some(data.len() as u64)
                || !execution.source_exhausted
                || !progress_agrees
            {
                wrong.push(format!(
                    "{route}: bytes_consumed {:?}, source_exhausted {}, progress total {finished:?}",
                    execution.bytes_consumed, execution.source_exhausted
                ));
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}
