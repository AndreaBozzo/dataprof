//! Asynchronous Parquet HTTP reading module
use crate::core::errors::DataProfilerError;
use bytes::Bytes;
use futures::future::BoxFuture;
use parquet::arrow::async_reader::AsyncFileReader;
use reqwest::{Client, header};
use std::ops::Range;

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

        let head_res =
            client
                .head(url)
                .send()
                .await
                .map_err(|e| DataProfilerError::ParquetError {
                    message: format!("Failed to HEAD url: {}", e),
                })?;

        if !head_res.status().is_success() {
            return Err(DataProfilerError::ParquetError {
                message: format!("HTTP HEAD request returned status {}", head_res.status()),
            });
        }

        let content_length = head_res
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|h| h.to_str().ok())
            .and_then(|h| h.parse::<usize>().ok())
            .ok_or_else(|| DataProfilerError::ParquetError {
                message: "Server did not provide Content-Length header".to_string(),
            })?;

        Ok(Self {
            client,
            url: url.to_string(),
            content_length,
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
                .with_column_index_policy(
                    options
                        .map(|o| o.column_index_policy())
                        .unwrap_or(PageIndexPolicy::Skip),
                )
                .with_offset_index_policy(
                    options
                        .map(|o| o.offset_index_policy())
                        .unwrap_or(PageIndexPolicy::Skip),
                )
                .with_metadata_options(metadata_opts);

            let parquet_metadata = metadata_reader.load_and_finish(self, limit).await?;
            Ok(std::sync::Arc::new(parquet_metadata))
        }
        .boxed()
    }
}

use crate::core::report_assembler::ReportAssembler;
use crate::engines::columnar::record_batch_analyzer::RecordBatchAnalyzer;
use crate::parsers::parquet::ParquetConfig;
use crate::types::{
    DataSource, ExecutionMetadata, FileFormat, ParquetMetadata, ProfileReport, QualityDimension,
};
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

    let column_profiles = analyzer.to_profiles();
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
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::sync::Arc;

    fn spawn_mock_server(data: Vec<u8>) -> String {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let data = Arc::new(data);

        std::thread::spawn(move || {
            for mut stream in listener.incoming().flatten() {
                let mut buffer = [0; 512];
                if let Ok(bytes_read) = stream.read(&mut buffer) {
                    let request = String::from_utf8_lossy(&buffer[..bytes_read]);

                    if request.starts_with("HEAD") {
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nAccept-Ranges: bytes\r\n\r\n",
                            data.len()
                        );
                        let _ = stream.write_all(response.as_bytes());
                    } else if request.starts_with("GET") {
                        // "Range: bytes=0-10"
                        if let Some(range_line) = request
                            .lines()
                            .find(|l| l.to_lowercase().starts_with("range: bytes="))
                        {
                            let parts: Vec<&str> = range_line
                                .trim_start_matches("Range: bytes=")
                                .trim_start_matches("range: bytes=")
                                .split('-')
                                .collect();
                            if parts.len() == 2 {
                                let start: usize = parts[0].parse().unwrap();
                                let end: usize = parts[1].parse().unwrap(); // inclusive
                                let chunk = &data[start..=end];
                                let response = format!(
                                    "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes {}-{}/{}\r\nContent-Length: {}\r\n\r\n",
                                    start,
                                    end,
                                    data.len(),
                                    chunk.len()
                                );
                                let _ = stream.write_all(response.as_bytes());
                                let _ = stream.write_all(chunk);
                            }
                        }
                    }
                }
            }
        });
        format!("http://127.0.0.1:{}", port)
    }

    #[tokio::test]
    async fn test_http_parquet_reader_byte_ranges() {
        let dummy_data: Vec<u8> = (0..255).map(|x| x as u8).collect();
        let url = spawn_mock_server(dummy_data.clone());

        let mut reader = HttpParquetReader::try_new(&url).await.unwrap();
        assert_eq!(reader.content_length, 255);

        let bytes = reader.get_bytes(10..20).await.unwrap();
        assert_eq!(bytes.len(), 10);
        assert_eq!(&bytes[..], &dummy_data[10..20]);
    }
}
