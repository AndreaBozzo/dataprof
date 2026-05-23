//! Asynchronous Parquet HTTP reading module
use bytes::Bytes;
use dataprof_core::{
    DataProfilerError, DataSource, ExecutionMetadata, FileFormat, ParquetMetadata, QualityDimension,
};
use dataprof_runtime::{ProfileReport, ReportAssembler};
use futures::future::BoxFuture;
use parquet::arrow::async_reader::AsyncFileReader;
use reqwest::{Client, header};
use std::ops::Range;

use crate::{ParquetConfig, RecordBatchAnalyzer};

/// An asynchronous reader that fetches byte ranges from an HTTP server
/// using HTTP Range requests. Designed specifically for remote Parquet parsing.
#[derive(Clone)]
pub struct HttpParquetReader {
    client: Client,
    url: String,
    content_length: usize,
}

impl HttpParquetReader {
    /// Creates a new `HttpParquetReader` by performing a HEAD request
    /// to ascertain the target file's size, returning the initialized reader.
    pub async fn try_new(url: &str) -> Result<Self, DataProfilerError> {
        let client = Client::builder()
            .connect_timeout(std::time::Duration::from_secs(10))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|e| DataProfilerError::ParquetError {
                message: format!("Reqwest client builder failed: {}", e),
            })?;

        let content_length = Self::discover_content_length(&client, url).await?;

        Ok(Self {
            client,
            url: url.to_string(),
            content_length,
        })
    }

    async fn discover_content_length(
        client: &Client,
        url: &str,
    ) -> Result<usize, DataProfilerError> {
        let head_failure = match client.head(url).send().await {
            Ok(head_res) if head_res.status().is_success() => {
                if let Some(content_length) = Self::content_length_from_headers(head_res.headers())
                {
                    return Ok(content_length);
                }
                "Server did not provide Content-Length header on HEAD".to_string()
            }
            Ok(head_res) => {
                format!("HTTP HEAD request returned status {}", head_res.status())
            }
            Err(error) => {
                format!("Failed to HEAD url: {}", error)
            }
        };

        Self::probe_content_length_with_range(client, url)
            .await
            .map_err(|probe_error| {
                let mut message = head_failure;
                message.push_str("; range probe fallback failed: ");
                message.push_str(&probe_error);

                DataProfilerError::ParquetError { message }
            })
    }

    fn content_length_from_headers(headers: &header::HeaderMap) -> Option<usize> {
        headers
            .get(header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<usize>().ok())
    }

    fn content_length_from_content_range(headers: &header::HeaderMap) -> Option<usize> {
        let content_range = headers.get(header::CONTENT_RANGE)?.to_str().ok()?;
        let (_, total_size) = content_range.split_once('/')?;
        if total_size == "*" {
            return None;
        }

        total_size.parse::<usize>().ok()
    }

    async fn probe_content_length_with_range(client: &Client, url: &str) -> Result<usize, String> {
        let response = client
            .get(url)
            .header(reqwest::header::RANGE, "bytes=0-0")
            .send()
            .await
            .map_err(|error| format!("failed to fetch byte-range probe: {}", error))?;

        if response.status() != reqwest::StatusCode::PARTIAL_CONTENT {
            return Err(match response.status() {
                reqwest::StatusCode::OK => {
                    "server ignored Range header during size probe and returned 200 OK".to_string()
                }
                status => format!(
                    "expected 206 Partial Content from size probe, got {}",
                    status
                ),
            });
        }

        Self::content_length_from_content_range(response.headers()).ok_or_else(|| {
            "server did not provide a parseable Content-Range total during size probe".to_string()
        })
    }
}

impl AsyncFileReader for HttpParquetReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, parquet::errors::Result<Bytes>> {
        Box::pin(async move {
            let limit = self.content_length as u64;
            if range.start > limit {
                return Err(parquet::errors::ParquetError::General(format!(
                    "Out of bounds request start {} vs limit {}",
                    range.start, limit
                )));
            }
            if range.start >= range.end {
                return Ok(Bytes::new());
            }
            let req = self
                .client
                .get(&self.url)
                .header(
                    reqwest::header::RANGE,
                    format!("bytes={}-{}", range.start, range.end - 1),
                )
                .build()
                .map_err(|e| {
                    parquet::errors::ParquetError::General(format!("invalid request: {}", e))
                })?;

            let res = self.client.execute(req).await.map_err(|e| {
                parquet::errors::ParquetError::General(format!("network err: {}", e))
            })?;

            if res.status() != reqwest::StatusCode::PARTIAL_CONTENT {
                if res.status() == reqwest::StatusCode::OK {
                    return Err(parquet::errors::ParquetError::General(
                        "Server ignored Range header and returned 200 OK. Aborting to prevent full file download.".to_string(),
                    ));
                }
                return Err(parquet::errors::ParquetError::General(format!(
                    "Expected 206 Partial Content, got {}",
                    res.status()
                )));
            }

            let bytes = res.bytes().await.map_err(|e| {
                parquet::errors::ParquetError::General(format!(
                    "failed to fetch byte body from http stream: {}",
                    e
                ))
            })?;

            Ok(bytes)
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a parquet::arrow::arrow_reader::ArrowReaderOptions>,
    ) -> BoxFuture<
        'a,
        parquet::errors::Result<std::sync::Arc<parquet::file::metadata::ParquetMetaData>>,
    > {
        use futures::FutureExt;
        use parquet::file::metadata::{PageIndexPolicy, ParquetMetaDataReader};

        async move {
            let limit = self.content_length as u64;
            let metadata_opts = options.map(|o| o.metadata_options().clone());
            let metadata_reader = ParquetMetaDataReader::new()
                .with_page_index_policy(PageIndexPolicy::from(
                    options.is_some_and(|o| o.page_index()),
                ))
                .with_metadata_options(metadata_opts);

            let parquet_metadata = metadata_reader.load_and_finish(self, limit).await?;
            Ok(std::sync::Arc::new(parquet_metadata))
        }
        .boxed()
    }
}

use futures::StreamExt;
use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;

/// Analyzes a remote Parquet file by fetching it via HTTP Range requests.
pub async fn analyze_parquet_async_http(
    url: &str,
    config: &ParquetConfig,
) -> Result<ProfileReport, DataProfilerError> {
    analyze_parquet_async_http_dims(url, config, None).await
}

/// Like [`analyze_parquet_async_http`] but only computes the requested quality dimensions.
pub async fn analyze_parquet_async_http_dims(
    url: &str,
    config: &ParquetConfig,
    quality_dimensions: Option<Vec<QualityDimension>>,
) -> Result<ProfileReport, DataProfilerError> {
    let start = std::time::Instant::now();

    let reader = HttpParquetReader::try_new(url).await?;
    let content_length = reader.content_length as u64;

    let builder = ParquetRecordBatchStreamBuilder::new(reader)
        .await
        .map_err(|e| DataProfilerError::ParquetError {
            message: format!("Failed to create Parquet stream builder: {}", e),
        })?;

    let parquet_meta = builder.metadata().clone();
    let file_metadata = parquet_meta.file_metadata();

    let num_row_groups = parquet_meta.num_row_groups();
    let version = file_metadata.version();

    let compression = if num_row_groups > 0 && parquet_meta.row_group(0).num_columns() > 0 {
        format!("{:?}", parquet_meta.row_group(0).column(0).compression())
    } else {
        "UNKNOWN".to_string()
    };

    let compressed_size_bytes: u64 = (0..num_row_groups)
        .map(|i| parquet_meta.row_group(i).compressed_size() as u64)
        .sum();

    let schema_summary = format!("{}", builder.schema());

    let mut stream = builder
        .with_batch_size(config.batch_size)
        .build()
        .map_err(|e| DataProfilerError::ParquetError {
            message: format!("Failed to build Parquet stream: {}", e),
        })?;

    let mut analyzer = RecordBatchAnalyzer::new();

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| DataProfilerError::ParquetError {
            message: format!("Failed to read Parquet async batch: {}", e),
        })?;
        analyzer.process_batch(&batch)?;
    }

    let column_profiles = analyzer.to_profiles(false, false, None);
    let total_rows = analyzer.total_rows();

    let sample_columns = analyzer.create_sample_columns();

    let scan_time_ms = start.elapsed().as_millis();

    let parquet_metadata = Some(ParquetMetadata {
        num_row_groups,
        compression,
        version,
        schema_summary,
        compressed_size_bytes,
        uncompressed_size_bytes: None,
    });

    let num_columns = column_profiles.len();

    let mut assembler = ReportAssembler::new(
        DataSource::File {
            path: url.to_string(),
            format: FileFormat::Parquet,
            size_bytes: content_length,
            modified_at: None,
            parquet_metadata,
        },
        ExecutionMetadata::new(total_rows, num_columns, scan_time_ms),
    )
    .columns(column_profiles)
    .with_quality_data(sample_columns);
    if let Some(dims) = quality_dimensions {
        assembler = assembler.with_requested_dimensions(dims);
    }
    Ok(assembler.build())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use parquet::arrow::ArrowWriter;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::sync::Arc;
    use std::thread::JoinHandle;
    use std::time::Duration;

    #[derive(Clone, Copy)]
    enum HeadBehavior {
        WithContentLength,
        MissingContentLength,
        MethodNotAllowed,
    }

    struct MockServer {
        url: String,
        handle: JoinHandle<()>,
    }

    impl MockServer {
        fn url(&self) -> &str {
            &self.url
        }

        fn join(self) {
            self.handle.join().unwrap();
        }
    }

    fn read_http_request(stream: &mut TcpStream) -> std::io::Result<String> {
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

            if request.len() > 16 * 1024 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "request headers exceeded test limit",
                ));
            }
        }

        Ok(String::from_utf8_lossy(&request).into_owned())
    }

    fn parse_range_header(request: &str) -> Option<(usize, usize)> {
        let range_line = request
            .lines()
            .find(|line| line.to_ascii_lowercase().starts_with("range: bytes="))?;
        let range_value = range_line.split_once(':')?.1.trim();
        let byte_range = range_value.strip_prefix("bytes=")?;
        let (start, end) = byte_range.split_once('-')?;

        Some((start.parse().ok()?, end.parse().ok()?))
    }

    fn spawn_mock_server(data: Vec<u8>, head_behavior: HeadBehavior) -> MockServer {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let port = listener.local_addr().unwrap().port();
        let data = Arc::new(data);

        let handle = std::thread::spawn(move || {
            let mut idle_loops = 0;
            loop {
                match listener.accept() {
                    Ok((mut stream, _)) => {
                        idle_loops = 0;
                        let request =
                            read_http_request(&mut stream).expect("mock server read failed");

                        if request.starts_with("HEAD") {
                            let response = match head_behavior {
                                HeadBehavior::WithContentLength => format!(
                                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n",
                                    data.len()
                                ),
                                HeadBehavior::MissingContentLength => {
                                    "HTTP/1.1 200 OK\r\nAccept-Ranges: bytes\r\nConnection: close\r\n\r\n".to_string()
                                }
                                HeadBehavior::MethodNotAllowed => {
                                    "HTTP/1.1 405 Method Not Allowed\r\nAllow: GET\r\nConnection: close\r\n\r\n".to_string()
                                }
                            };
                            stream
                                .write_all(response.as_bytes())
                                .expect("mock server HEAD response failed");
                        } else if request.starts_with("GET") {
                            let (start, end) = parse_range_header(&request)
                                .expect("mock server missing or invalid Range header");
                            let chunk = data
                                .get(start..=end)
                                .expect("mock server requested invalid byte range");
                            let response = format!(
                                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                                start,
                                end,
                                data.len(),
                                chunk.len()
                            );
                            stream
                                .write_all(response.as_bytes())
                                .expect("mock server GET response headers failed");
                            stream
                                .write_all(chunk)
                                .expect("mock server GET response body failed");
                        } else {
                            panic!("mock server received unexpected request: {request}");
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        idle_loops += 1;
                        if idle_loops > 50 {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(20));
                    }
                    Err(error) => panic!("mock server accept failed: {error}"),
                }
            }
        });

        MockServer {
            url: format!("http://127.0.0.1:{}", port),
            handle,
        }
    }

    fn test_parquet_bytes() -> Vec<u8> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["rome", "milan", "turin"])),
            ],
        )
        .unwrap();

        let mut buffer = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut buffer, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        buffer
    }

    #[tokio::test]
    async fn test_http_parquet_reader_byte_ranges() {
        let dummy_data: Vec<u8> = (0..255).map(|x| x as u8).collect();
        let server = spawn_mock_server(dummy_data.clone(), HeadBehavior::WithContentLength);

        let mut reader = HttpParquetReader::try_new(server.url()).await.unwrap();
        assert_eq!(reader.content_length, 255);

        let bytes = reader.get_bytes(10..20).await.unwrap();
        assert_eq!(bytes.len(), 10);
        assert_eq!(&bytes[..], &dummy_data[10..20]);

        server.join();
    }

    #[tokio::test]
    async fn test_http_parquet_reader_falls_back_without_head_content_length() {
        let dummy_data: Vec<u8> = (0..255).map(|x| x as u8).collect();
        let server = spawn_mock_server(dummy_data.clone(), HeadBehavior::MissingContentLength);

        let mut reader = HttpParquetReader::try_new(server.url()).await.unwrap();
        assert_eq!(reader.content_length, 255);

        let bytes = reader.get_bytes(10..20).await.unwrap();
        assert_eq!(&bytes[..], &dummy_data[10..20]);

        server.join();
    }

    #[tokio::test]
    async fn test_analyze_parquet_async_http_without_head_support() {
        let server = spawn_mock_server(test_parquet_bytes(), HeadBehavior::MethodNotAllowed);

        let report = analyze_parquet_async_http(server.url(), &ParquetConfig::default())
            .await
            .unwrap();

        assert_eq!(report.execution.rows_processed, 3);
        assert_eq!(report.column_profiles.len(), 2);

        server.join();
    }
}
