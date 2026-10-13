//! A remote Parquet object replaced between ranged reads (#865).
//!
//! The remote reader learns the size from one request and then reads the
//! footer and each column chunk with separate ranged GETs. When the object was
//! rewritten in between with the same physical layout, the profile decoded the
//! footer of one version and pages of the other, and returned the statistics of
//! a dataset that never existed. The reader now pins the object's `ETag`: every
//! range is requested with `If-Match`, and every `206` answer's `ETag` is
//! compared with the pinned one.
//!
//! `python/tests/test_remote_parquet_versions.py` is the Python twin.

#![cfg(feature = "parquet-async")]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use arrow::array::{Float64Array, Int64Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use dataprof::{DataProfilerError, ParquetConfig, ProfileReport, analyze_parquet_async_http};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, Encoding};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

const ROWS: i64 = 10_000;

/// A profile makes this many requests: HEAD, footer length, footer, then two
/// column chunks in each of four row groups.
const REQUESTS: usize = 11;

/// `a` and `b` hold `offset..offset + ROWS`, uncompressed and PLAIN in four
/// row groups, so the two versions share every footer offset.
fn version(offset: i64) -> Vec<u8> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("a", DataType::Int64, false),
        Field::new("b", DataType::Float64, false),
    ]));
    let values: Vec<i64> = (offset..offset + ROWS).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(values.clone())),
            Arc::new(Float64Array::from_iter_values(
                values.iter().map(|&v| v as f64),
            )),
        ],
    )
    .unwrap();
    let properties = WriterProperties::builder()
        .set_compression(Compression::UNCOMPRESSED)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .set_statistics_enabled(EnabledStatistics::None)
        .set_max_row_group_row_count(Some(ROWS as usize / 4))
        .build();
    let mut buffer = Vec::new();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(properties)).unwrap();
    writer.write(&batch).unwrap();
    writer.close().unwrap();
    buffer
}

#[derive(Clone, Copy, Debug)]
enum Validators {
    /// Sends an `ETag` on HEAD only, and answers 412 when `If-Match` names
    /// another version. Only `If-Match` can catch the change.
    IfMatchOnly,
    /// Sends an `ETag` on every answer but serves the current version
    /// whatever `If-Match` says. Only comparing the answer's `ETag` can
    /// catch the change.
    EtagOnly,
    /// Sends no `ETag`, so there is nothing to pin.
    None,
}

struct SwapServer {
    url: String,
    requests: Arc<AtomicUsize>,
}

fn read_request(stream: &mut TcpStream) -> String {
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    let mut request = Vec::new();
    let mut buffer = [0; 1024];
    while !request.windows(4).any(|window| window == b"\r\n\r\n") {
        let read = stream.read(&mut buffer).unwrap();
        if read == 0 {
            break;
        }
        request.extend_from_slice(&buffer[..read]);
    }
    String::from_utf8(request).unwrap()
}

fn header<'a>(request: &'a str, name: &str) -> Option<&'a str> {
    request.lines().find_map(|line| {
        let (key, value) = line.split_once(':')?;
        key.eq_ignore_ascii_case(name).then(|| value.trim())
    })
}

/// Serves version A for the first `swap_after` requests and version B after.
fn spawn(
    a: Arc<Vec<u8>>,
    b: Arc<Vec<u8>>,
    swap_after: usize,
    validators: Validators,
) -> SwapServer {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let url = format!(
        "http://127.0.0.1:{}/data.parquet",
        listener.local_addr().unwrap().port()
    );
    let requests = Arc::new(AtomicUsize::new(0));
    let served = requests.clone();

    std::thread::spawn(move || {
        let mut idle = 0;
        loop {
            let mut stream = match listener.accept() {
                Ok((stream, _)) => stream,
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    idle += 1;
                    if idle > 50 {
                        return;
                    }
                    std::thread::sleep(Duration::from_millis(20));
                    continue;
                }
                Err(error) => panic!("accept failed: {error}"),
            };
            idle = 0;
            // The accepted stream inherits non-blocking mode on Windows.
            stream.set_nonblocking(false).unwrap();
            let request = read_request(&mut stream);
            let index = served.fetch_add(1, Ordering::SeqCst);
            let (data, etag) = if index < swap_after {
                (&a, "\"v-A\"")
            } else {
                (&b, "\"v-B\"")
            };
            let head = request.starts_with("HEAD");
            let etag_line = match validators {
                Validators::None => String::new(),
                Validators::IfMatchOnly if !head => String::new(),
                _ => format!("ETag: {etag}\r\n"),
            };

            if head {
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n{etag_line}Accept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                    data.len()
                );
                stream.write_all(response.as_bytes()).unwrap();
                continue;
            }
            if matches!(validators, Validators::IfMatchOnly)
                && header(&request, "If-Match").is_some_and(|tag| tag != etag)
            {
                stream
                    .write_all(b"HTTP/1.1 412 Precondition Failed\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                    .unwrap();
                continue;
            }
            let range = header(&request, "Range")
                .and_then(|value| value.strip_prefix("bytes="))
                .expect("a ranged GET");
            let (start, end) = range.split_once('-').unwrap();
            let (start, end): (usize, usize) = (start.parse().unwrap(), end.parse().unwrap());
            let chunk = &data[start..=end];
            let response = format!(
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {start}-{end}/{}\r\nContent-Length: {}\r\n{etag_line}Connection: close\r\n\r\n",
                data.len(),
                chunk.len()
            );
            stream.write_all(response.as_bytes()).unwrap();
            stream.write_all(chunk).unwrap();
        }
    });

    SwapServer { url, requests }
}

/// `a`'s min, max and mean.
fn summary(report: &ProfileReport) -> String {
    let a = serde_json::to_value(&report.column_profiles[0]).unwrap();
    let stats = &a["stats"]["Numeric"];
    format!("{} {} {}", stats["min"], stats["max"], stats["mean"])
}

async fn profile(server: &SwapServer) -> Result<ProfileReport, DataProfilerError> {
    analyze_parquet_async_http(&server.url, &ParquetConfig::default()).await
}

#[tokio::test]
async fn a_replaced_object_fails_the_read_at_every_mixing_swap_point() {
    let a = Arc::new(version(0));
    let b = Arc::new(version(1_000_000));
    assert_eq!(a.len(), b.len(), "the versions must share a layout");

    let mut wrong = Vec::new();
    for validators in [Validators::IfMatchOnly, Validators::EtagOnly] {
        // 0: every request sees B. REQUESTS: every request sees A. Between
        // them the reads mix versions.
        for swap_after in 0..=REQUESTS {
            let server = spawn(a.clone(), b.clone(), swap_after, validators);
            let result = profile(&server).await;
            let case = format!("{validators:?}, replaced after request {swap_after}");
            match (swap_after, result) {
                (0 | REQUESTS, Ok(report)) => {
                    let expected = if swap_after == 0 {
                        "1000000.0 1009999.0 1004999.5"
                    } else {
                        "0.0 9999.0 4999.5"
                    };
                    if summary(&report) != expected {
                        wrong.push(format!("{case}: {} for one version", summary(&report)));
                    }
                }
                (0 | REQUESTS, Err(error)) => wrong.push(format!("{case}: {error}")),
                (_, Ok(report)) => {
                    wrong.push(format!("{case}: profiled as {}", summary(&report)));
                }
                (_, Err(DataProfilerError::SourceChanged { path, .. })) if path == server.url => {}
                (_, Err(error)) => wrong.push(format!("{case}: untyped error {error}")),
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[tokio::test]
async fn an_unchanged_object_takes_the_same_requests() {
    let a = Arc::new(version(0));
    for validators in [
        Validators::IfMatchOnly,
        Validators::EtagOnly,
        Validators::None,
    ] {
        let server = spawn(a.clone(), a.clone(), usize::MAX, validators);
        let report = profile(&server)
            .await
            .unwrap_or_else(|error| panic!("{validators:?}: {error}"));
        assert_eq!(summary(&report), "0.0 9999.0 4999.5", "{validators:?}");
        assert_eq!(
            server.requests.load(Ordering::SeqCst),
            REQUESTS,
            "{validators:?}"
        );
    }
}
