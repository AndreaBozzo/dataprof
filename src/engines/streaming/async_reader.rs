use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc;

use crate::analysis::patterns::looks_like_date;
use crate::core::errors::DataProfilerError;
use crate::core::sampling::{ChunkSize, SamplingStrategy};
use crate::core::streaming_stats::{StreamingColumnCollection, StreamingStatistics};
use crate::engines::streaming::{ProgressCallback, ProgressTracker};
use crate::types::{
    ColumnProfile, ColumnStats, DataQualityMetrics, DataSource, DataType, FileFormat,
    QualityReport, ScanInfo, StreamSourceSystem,
};

use super::async_source::AsyncDataSource;

/// A chunk of parsed CSV records sent through the bounded channel.
struct ParsedChunk {
    /// Rows of field values (each inner Vec is one row).
    records: Vec<Vec<String>>,
    /// Number of raw bytes consumed to produce this chunk (for progress).
    bytes_read: u64,
}

/// Async streaming profiler that accepts [`AsyncDataSource`] instead of file paths.
///
/// Uses a bounded channel between an async reader task and a sync processing loop
/// to provide natural backpressure: when the processor falls behind, the reader
/// pauses, which propagates TCP pressure back to the data source.
///
/// # Architecture
///
/// ```text
/// AsyncDataSource ──► [tokio::spawn reader task] ──► bounded mpsc ──► [processor loop]
///                          reads bytes, parses CSV       capacity N       StreamingColumnCollection
///                                                                        ──► QualityReport
/// ```
pub struct AsyncStreamingProfiler {
    chunk_size: ChunkSize,
    sampling_strategy: SamplingStrategy,
    memory_limit_mb: usize,
    channel_capacity: usize,
    progress_callback: Option<ProgressCallback>,
}

impl AsyncStreamingProfiler {
    pub fn new() -> Self {
        Self {
            chunk_size: ChunkSize::default(),
            sampling_strategy: SamplingStrategy::None,
            memory_limit_mb: 256,
            channel_capacity: 4,
            progress_callback: None,
        }
    }

    pub fn chunk_size(mut self, chunk_size: ChunkSize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn sampling(mut self, strategy: SamplingStrategy) -> Self {
        self.sampling_strategy = strategy;
        self
    }

    pub fn memory_limit_mb(mut self, limit: usize) -> Self {
        self.memory_limit_mb = limit;
        self
    }

    pub fn channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity.max(1);
        self
    }

    pub fn progress_callback<F>(mut self, callback: F) -> Self
    where
        F: Fn(super::ProgressInfo) + Send + Sync + 'static,
    {
        self.progress_callback = Some(Arc::new(callback));
        self
    }

    /// Profile data from an async source, returning a [`QualityReport`].
    ///
    /// Only CSV format is supported in this issue; JSON/Parquet support is #218.
    pub async fn analyze_stream(
        &self,
        source: impl AsyncDataSource,
    ) -> Result<QualityReport, DataProfilerError> {
        let source_info = source.source_info();

        if source_info.format != FileFormat::Csv {
            return Err(DataProfilerError::UnsupportedFormat {
                format: format!("{:?}", source_info.format),
            });
        }

        let start = std::time::Instant::now();
        let reader = source.into_async_read().await?;
        let buf_reader = BufReader::new(reader);

        let rows_per_chunk = self.rows_per_chunk();
        let (tx, rx) = mpsc::channel::<ParsedChunk>(self.channel_capacity);

        // Spawn the async reader task
        let reader_handle = tokio::spawn(Self::reader_task(buf_reader, tx, rows_per_chunk));

        // Process chunks on the current task
        let (_headers, column_stats, processed_rows, _total_bytes) =
            self.process_chunks(rx, source_info.size_hint).await?;

        // Wait for the reader task to finish and propagate any errors
        match reader_handle.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => return Err(e),
            Err(join_err) => {
                return Err(DataProfilerError::StreamingError {
                    message: format!("Reader task panicked: {}", join_err),
                });
            }
        }

        // Build the report
        let column_profiles = self.convert_to_profiles(&column_stats);
        let sample_columns = self.create_quality_check_samples(&column_stats);
        let data_quality_metrics =
            DataQualityMetrics::calculate_from_data(&sample_columns, &column_profiles)
                .unwrap_or_else(|_| DataQualityMetrics::empty());

        let scan_time_ms = start.elapsed().as_millis();
        let num_columns = column_profiles.len();

        let data_source = DataSource::Stream {
            topic: source_info.label,
            batch_id: uuid::Uuid::new_v4().to_string(),
            partition: None,
            consumer_group: None,
            source_system: StreamSourceSystem::Http,
            session_id: None,
            first_record_at: None,
            last_record_at: None,
        };

        let sampling_ratio = if processed_rows > 0 { 1.0 } else { 0.0 };

        Ok(QualityReport::new(
            data_source,
            column_profiles,
            ScanInfo {
                total_rows: processed_rows,
                total_columns: num_columns,
                rows_scanned: processed_rows,
                sampling_ratio,
                scan_time_ms,
                throughput_rows_sec: if scan_time_ms > 0 {
                    Some(processed_rows as f64 / (scan_time_ms as f64 / 1000.0))
                } else {
                    None
                },
                memory_peak_mb: None,
                error_count: 0,
            },
            data_quality_metrics,
        ))
    }

    /// Async reader task: reads lines from the `AsyncRead`, parses CSV, sends chunks.
    async fn reader_task(
        buf_reader: BufReader<std::pin::Pin<Box<dyn tokio::io::AsyncRead + Send + Unpin>>>,
        tx: mpsc::Sender<ParsedChunk>,
        rows_per_chunk: usize,
    ) -> Result<(), DataProfilerError> {
        let mut lines = buf_reader.lines();
        let mut current_chunk: Vec<Vec<String>> = Vec::with_capacity(rows_per_chunk);
        let mut bytes_in_chunk: u64 = 0;
        let mut is_first_line = true;

        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| DataProfilerError::IoError {
                message: e.to_string(),
            })?
        {
            let line_bytes = line.len() as u64 + 1; // +1 for the newline
            bytes_in_chunk += line_bytes;

            // Parse CSV fields from this line
            let mut csv_reader = csv::ReaderBuilder::new()
                .has_headers(false)
                .from_reader(line.as_bytes());

            if let Some(result) = csv_reader.records().next() {
                let record = result.map_err(|e| DataProfilerError::CsvParsingError {
                    message: e.to_string(),
                    suggestion: "Check CSV formatting in the stream data".to_string(),
                })?;

                let fields: Vec<String> = record.iter().map(|f| f.to_string()).collect();

                if is_first_line {
                    // First line is sent as a single-record chunk (headers)
                    let header_chunk = ParsedChunk {
                        records: vec![fields],
                        bytes_read: bytes_in_chunk,
                    };
                    if tx.send(header_chunk).await.is_err() {
                        // Receiver dropped — processing stopped early
                        return Ok(());
                    }
                    bytes_in_chunk = 0;
                    is_first_line = false;
                    continue;
                }

                current_chunk.push(fields);
            }

            if current_chunk.len() >= rows_per_chunk {
                let chunk = ParsedChunk {
                    records: std::mem::replace(
                        &mut current_chunk,
                        Vec::with_capacity(rows_per_chunk),
                    ),
                    bytes_read: bytes_in_chunk,
                };
                bytes_in_chunk = 0;

                if tx.send(chunk).await.is_err() {
                    return Ok(());
                }
            }
        }

        // Send remaining records
        if !current_chunk.is_empty() {
            let chunk = ParsedChunk {
                records: current_chunk,
                bytes_read: bytes_in_chunk,
            };
            let _ = tx.send(chunk).await;
        }

        Ok(())
    }

    /// Receive parsed chunks and feed them into StreamingColumnCollection.
    ///
    /// Returns (headers, column_stats, processed_rows, total_bytes_read).
    async fn process_chunks(
        &self,
        mut rx: mpsc::Receiver<ParsedChunk>,
        size_hint: Option<u64>,
    ) -> Result<(Vec<String>, StreamingColumnCollection, usize, u64), DataProfilerError> {
        let mut column_stats = StreamingColumnCollection::memory_limit(self.memory_limit_mb);
        let mut progress_tracker = ProgressTracker::new(self.progress_callback.clone());
        let mut processed_rows: usize = 0;
        let mut total_bytes: u64 = 0;
        let mut chunk_count: usize = 0;

        // First chunk is always headers
        let header_chunk = rx
            .recv()
            .await
            .ok_or_else(|| DataProfilerError::StreamingError {
                message: "Stream ended before any data was received".to_string(),
            })?;

        total_bytes += header_chunk.bytes_read;

        if header_chunk.records.is_empty() {
            return Err(DataProfilerError::StreamingError {
                message: "Stream header chunk was empty".to_string(),
            });
        }

        let headers: Vec<String> = header_chunk.records.into_iter().next().unwrap_or_default();

        if headers.is_empty() {
            return Err(DataProfilerError::StreamingError {
                message: "No column headers found in stream".to_string(),
            });
        }

        // Estimate total rows for progress (if we know the total size and have seen some bytes)
        let estimated_total_rows = size_hint.map(|total| {
            // Very rough estimate — will improve as we see more data
            (total as usize) / 50 // assume ~50 bytes per row as starting guess
        });

        // Process data chunks
        while let Some(chunk) = rx.recv().await {
            total_bytes += chunk.bytes_read;
            chunk_count += 1;
            let chunk_rows = chunk.records.len();

            for (row_idx, values) in chunk.records.into_iter().enumerate() {
                let global_row_idx = processed_rows + row_idx;

                // Apply sampling strategy
                if !self
                    .sampling_strategy
                    .should_include(global_row_idx, global_row_idx + 1)
                {
                    continue;
                }

                column_stats.process_record(&headers, values);
            }

            processed_rows += chunk_rows;

            // Check memory pressure
            if column_stats.is_memory_pressure() {
                column_stats.reduce_memory_usage();
            }

            // Update progress
            progress_tracker.update(
                processed_rows,
                estimated_total_rows.or(Some(processed_rows + 1000)),
                chunk_count,
            );
        }

        progress_tracker.finish(processed_rows);

        Ok((headers, column_stats, processed_rows, total_bytes))
    }

    /// Calculate rows per chunk based on ChunkSize config.
    fn rows_per_chunk(&self) -> usize {
        match self.chunk_size {
            ChunkSize::Adaptive => 8192,
            ChunkSize::Fixed(bytes) => {
                // Estimate ~50 bytes per row on average
                (bytes / 50).max(100)
            }
            ChunkSize::Custom(f) => {
                // Use the custom function with a dummy file size (streams don't know size upfront)
                f(0).max(100)
            }
        }
    }

    // -----------------------------------------------------------------------
    // Profile conversion methods — duplicated from IncrementalProfiler.
    // TODO(#228): Extract to a shared module during engine dedup.
    // -----------------------------------------------------------------------

    fn convert_to_profiles(&self, column_stats: &StreamingColumnCollection) -> Vec<ColumnProfile> {
        let mut profiles = Vec::new();

        for column_name in column_stats.column_names() {
            if let Some(stats) = column_stats.get_column_stats(&column_name) {
                let profile = self.convert_single_column_profile(&column_name, stats);
                profiles.push(profile);
            }
        }

        profiles
    }

    fn convert_single_column_profile(
        &self,
        name: &str,
        stats: &StreamingStatistics,
    ) -> ColumnProfile {
        let data_type = self.infer_data_type(stats);

        let column_stats = match data_type {
            DataType::Integer | DataType::Float => {
                let sample_strs: Vec<String> = stats.sample_values().to_vec();
                crate::stats::numeric::calculate_numeric_stats(&sample_strs)
            }
            DataType::String | DataType::Date => {
                let text_stats = stats.text_length_stats();
                ColumnStats::Text {
                    min_length: text_stats.min_length,
                    max_length: text_stats.max_length,
                    avg_length: text_stats.avg_length,
                    most_frequent: None,
                    least_frequent: None,
                }
            }
        };

        let patterns = crate::detect_patterns(stats.sample_values());

        ColumnProfile {
            name: name.to_string(),
            data_type,
            null_count: stats.null_count,
            total_count: stats.count,
            unique_count: Some(stats.unique_count()),
            stats: column_stats,
            patterns,
        }
    }

    fn infer_data_type(&self, stats: &StreamingStatistics) -> DataType {
        if stats.min.is_finite() && stats.max.is_finite() && stats.sum.is_finite() {
            let sample_values = stats.sample_values();
            let non_empty: Vec<&String> = sample_values.iter().filter(|s| !s.is_empty()).collect();

            if !non_empty.is_empty() {
                let all_integers = non_empty.iter().all(|s| s.parse::<i64>().is_ok());
                if all_integers {
                    return DataType::Integer;
                }

                let numeric_count = non_empty
                    .iter()
                    .filter(|s| s.parse::<f64>().is_ok())
                    .count();
                if numeric_count as f64 / non_empty.len() as f64 > 0.8 {
                    return DataType::Float;
                }
            }
        }

        let sample_values = stats.sample_values();
        let non_empty: Vec<&String> = sample_values.iter().filter(|s| !s.is_empty()).collect();

        if !non_empty.is_empty() {
            let date_like_count = non_empty
                .iter()
                .take(100)
                .filter(|s| looks_like_date(s))
                .count();

            if date_like_count as f64 / non_empty.len().min(100) as f64 > 0.7 {
                return DataType::Date;
            }
        }

        DataType::String
    }

    fn create_quality_check_samples(
        &self,
        column_stats: &StreamingColumnCollection,
    ) -> std::collections::HashMap<String, Vec<String>> {
        let mut samples = std::collections::HashMap::new();

        for column_name in column_stats.column_names() {
            if let Some(stats) = column_stats.get_column_stats(&column_name) {
                let sample_values: Vec<String> = stats.sample_values().to_vec();
                samples.insert(column_name, sample_values);
            }
        }

        samples
    }
}

impl Default for AsyncStreamingProfiler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engines::streaming::async_source::{AsyncSourceInfo, BytesSource};

    fn csv_source(data: &'static [u8]) -> BytesSource {
        BytesSource::new(
            bytes::Bytes::from_static(data),
            AsyncSourceInfo {
                label: "test".into(),
                format: FileFormat::Csv,
                size_hint: Some(data.len() as u64),
            },
        )
    }

    #[tokio::test]
    async fn test_basic_csv_profiling() {
        let source = csv_source(b"name,age,salary\nAlice,30,50000\nBob,25,60000\nCarol,35,55000\n");
        let profiler = AsyncStreamingProfiler::new();
        let report = profiler.analyze_stream(source).await.unwrap();

        assert_eq!(report.column_profiles.len(), 3);
        assert_eq!(report.scan_info.total_columns, 3);

        let age_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "age")
            .expect("age column");
        assert_eq!(age_col.data_type, DataType::Integer);
        assert_eq!(age_col.total_count, 3);
        assert_eq!(age_col.null_count, 0);
    }

    #[tokio::test]
    async fn test_empty_input() {
        let source = csv_source(b"");
        let profiler = AsyncStreamingProfiler::new();
        let result = profiler.analyze_stream(source).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_headers_only() {
        let source = csv_source(b"name,age,salary\n");
        let profiler = AsyncStreamingProfiler::new();
        let report = profiler.analyze_stream(source).await.unwrap();

        assert_eq!(report.column_profiles.len(), 0);
        assert_eq!(report.scan_info.rows_scanned, 0);
    }

    #[tokio::test]
    async fn test_large_synthetic_stream() {
        let mut data = String::from("id,value,label\n");
        for i in 0..10_000 {
            data.push_str(&format!("{},{},item_{}\n", i, i * 10 + 5, i));
        }

        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo {
                label: "large-test".into(),
                format: FileFormat::Csv,
                size_hint: None,
            },
        );

        let profiler = AsyncStreamingProfiler::new().memory_limit_mb(16);
        let report = profiler.analyze_stream(source).await.unwrap();

        assert_eq!(report.column_profiles.len(), 3);

        let id_col = report
            .column_profiles
            .iter()
            .find(|p| p.name == "id")
            .expect("id column");
        assert_eq!(id_col.data_type, DataType::Integer);
    }

    #[tokio::test]
    async fn test_channel_capacity_one() {
        let source =
            csv_source(b"a,b\n1,2\n3,4\n5,6\n7,8\n9,10\n11,12\n13,14\n15,16\n17,18\n19,20\n");
        let profiler = AsyncStreamingProfiler::new().channel_capacity(1);
        let report = profiler.analyze_stream(source).await.unwrap();
        assert_eq!(report.column_profiles.len(), 2);
    }

    #[tokio::test]
    async fn test_progress_callback_fires() {
        let progress_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let count_clone = progress_count.clone();

        let mut data = String::from("x,y\n");
        for i in 0..1000 {
            data.push_str(&format!("{},{}\n", i, i * 2));
        }

        let source = BytesSource::new(
            bytes::Bytes::from(data),
            AsyncSourceInfo {
                label: "progress-test".into(),
                format: FileFormat::Csv,
                size_hint: None,
            },
        );

        let profiler = AsyncStreamingProfiler::new().progress_callback(move |_info| {
            count_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        });

        let _report = profiler.analyze_stream(source).await.unwrap();
        // Progress should have fired at least once (finish call)
        assert!(progress_count.load(std::sync::atomic::Ordering::Relaxed) >= 1);
    }

    #[tokio::test]
    async fn test_unsupported_format_rejected() {
        let source = BytesSource::new(
            bytes::Bytes::from_static(b"{}"),
            AsyncSourceInfo {
                label: "json-test".into(),
                format: FileFormat::Json,
                size_hint: None,
            },
        );
        let profiler = AsyncStreamingProfiler::new();
        let result = profiler.analyze_stream(source).await;
        assert!(result.is_err());
    }
}
