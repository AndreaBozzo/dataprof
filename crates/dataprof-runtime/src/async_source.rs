use std::pin::Pin;

use dataprof_core::{DataProfilerError, FileFormat, StreamSourceSystem};
use tokio::io::AsyncRead;

/// Metadata about an async data source for report construction and progress tracking.
///
/// Use struct update syntax with `Default` to avoid breaking when new fields are added:
/// ```ignore
/// AsyncSourceInfo { label: "...".into(), format: FileFormat::Csv, ..Default::default() }
/// ```
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct AsyncSourceInfo {
    /// Human-readable label (e.g., URL, topic name, filename)
    pub label: String,
    /// Format of the incoming data. Async streaming currently supports CSV.
    pub format: FileFormat,
    /// Optional total size in bytes — enables progress percentage calculation
    pub size_hint: Option<u64>,
    /// Optional source system for the report's `DataSource::Stream` variant.
    /// Defaults to `StreamSourceSystem::Http` when `None`.
    pub source_system: Option<StreamSourceSystem>,
    /// Whether the first row of a CSV contains column headers.
    /// `None` (default) assumes headers are present. Ignored for non-CSV formats.
    pub has_header: Option<bool>,
}

impl AsyncSourceInfo {
    /// Create a new source info with the required fields; optional fields use defaults.
    pub fn new(label: impl Into<String>, format: FileFormat) -> Self {
        Self {
            label: label.into(),
            format,
            size_hint: None,
            source_system: None,
            has_header: None,
        }
    }

    pub fn size_hint(mut self, size: Option<u64>) -> Self {
        self.size_hint = size;
        self
    }

    pub fn source_system(mut self, system: StreamSourceSystem) -> Self {
        self.source_system = Some(system);
        self
    }

    pub fn has_header(mut self, has: bool) -> Self {
        self.has_header = Some(has);
        self
    }
}

impl Default for AsyncSourceInfo {
    fn default() -> Self {
        Self {
            label: String::new(),
            format: FileFormat::Unknown(String::new()),
            size_hint: None,
            source_system: None,
            has_header: None,
        }
    }
}

/// A source of raw bytes that can be consumed asynchronously.
///
/// Implementors include HTTP response bodies, file streams, in-memory buffers,
/// and gRPC streams. The returned `AsyncRead` is typically consumed by the
/// async streaming profiler to produce a `ProfileReport`.
#[async_trait::async_trait]
pub trait AsyncDataSource: Send {
    /// Consume this source into an async byte reader.
    ///
    /// The returned reader must be `Unpin + Send` so it can be wrapped in
    /// `tokio::io::BufReader` and moved across `.await` points.
    async fn into_async_read(
        self,
    ) -> Result<Pin<Box<dyn AsyncRead + Send + Unpin>>, DataProfilerError>;

    /// Metadata about this source (label, format, optional size).
    fn source_info(&self) -> AsyncSourceInfo;
}

/// An in-memory byte buffer that implements `AsyncDataSource`.
///
/// Useful for testing and for services that already hold the request body in
/// memory.
#[derive(Debug, Clone)]
pub struct BytesSource {
    data: bytes::Bytes,
    info: AsyncSourceInfo,
}

impl BytesSource {
    pub fn new(data: bytes::Bytes, info: AsyncSourceInfo) -> Self {
        Self { data, info }
    }
}

#[async_trait::async_trait]
impl AsyncDataSource for BytesSource {
    async fn into_async_read(
        self,
    ) -> Result<Pin<Box<dyn AsyncRead + Send + Unpin>>, DataProfilerError> {
        let cursor = std::io::Cursor::new(self.data);
        Ok(Box::pin(cursor))
    }

    fn source_info(&self) -> AsyncSourceInfo {
        self.info.clone()
    }
}

/// `AsyncDataSource` implementation for `tokio::fs::File`.
///
/// Provides async file I/O parity with the sync engines — primarily useful for
/// testing and benchmarking the async pipeline against sync baselines.
#[async_trait::async_trait]
impl AsyncDataSource for (tokio::fs::File, AsyncSourceInfo) {
    async fn into_async_read(
        self,
    ) -> Result<Pin<Box<dyn AsyncRead + Send + Unpin>>, DataProfilerError> {
        Ok(Box::pin(self.0))
    }

    fn source_info(&self) -> AsyncSourceInfo {
        self.1.clone()
    }
}

/// An HTTP response body that implements `AsyncDataSource`.
///
/// Wraps a `reqwest::Response` so that remote CSV, JSON, and JSONL streams can
/// be profiled via the public async facade.
#[cfg(feature = "parquet-async")]
pub struct ReqwestSource {
    response: reqwest::Response,
    info: AsyncSourceInfo,
}

#[cfg(feature = "parquet-async")]
impl ReqwestSource {
    /// Create a new source from an HTTP response and its metadata.
    pub fn new(response: reqwest::Response, info: AsyncSourceInfo) -> Self {
        Self { response, info }
    }
}

#[cfg(feature = "parquet-async")]
#[async_trait::async_trait]
impl AsyncDataSource for ReqwestSource {
    async fn into_async_read(
        self,
    ) -> Result<Pin<Box<dyn AsyncRead + Send + Unpin>>, DataProfilerError> {
        use futures::TryStreamExt;
        use tokio_util::io::StreamReader;

        let byte_stream = self.response.bytes_stream().map_err(std::io::Error::other);

        Ok(Box::pin(StreamReader::new(byte_stream)))
    }

    fn source_info(&self) -> AsyncSourceInfo {
        self.info.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_bytes_source_roundtrip() {
        let csv_data = b"name,age\nAlice,30\nBob,25\n";
        let source = BytesSource::new(
            bytes::Bytes::from_static(csv_data),
            AsyncSourceInfo {
                label: "test-buffer".into(),
                format: FileFormat::Csv,
                size_hint: Some(csv_data.len() as u64),
                source_system: None,
                has_header: None,
            },
        );

        let info = source.source_info();
        assert_eq!(info.label, "test-buffer");
        assert_eq!(info.size_hint, Some(csv_data.len() as u64));

        let mut reader = source.into_async_read().await.unwrap();
        let mut buf = String::new();
        reader.read_to_string(&mut buf).await.unwrap();
        assert_eq!(buf, "name,age\nAlice,30\nBob,25\n");
    }

    #[tokio::test]
    async fn test_file_source() {
        use std::io::Write;

        let mut tmp = tempfile::NamedTempFile::new().unwrap();
        writeln!(tmp, "x,y").unwrap();
        writeln!(tmp, "1,2").unwrap();
        tmp.flush().unwrap();

        let file = tokio::fs::File::open(tmp.path()).await.unwrap();
        let info = AsyncSourceInfo {
            label: tmp.path().display().to_string(),
            format: FileFormat::Csv,
            size_hint: Some(std::fs::metadata(tmp.path()).unwrap().len()),
            source_system: None,
            has_header: None,
        };

        let source = (file, info);
        let mut reader = source.into_async_read().await.unwrap();
        let mut buf = String::new();
        reader.read_to_string(&mut buf).await.unwrap();
        assert!(buf.contains("x,y"));
        assert!(buf.contains("1,2"));
    }
}
