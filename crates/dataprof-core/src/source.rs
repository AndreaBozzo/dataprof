/// Supported file formats for data profiling
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
#[non_exhaustive]
pub enum FileFormat {
    Csv,
    Json,
    Jsonl,
    Parquet,
    /// An Arrow C stream, whose schema lists every column before the first
    /// batch. Only the Arrow stream route records it: no file, bytes or URL
    /// route detects or reads this format.
    Arrow,
    #[serde(untagged)]
    Unknown(String),
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Csv => write!(f, "csv"),
            Self::Json => write!(f, "json"),
            Self::Jsonl => write!(f, "jsonl"),
            Self::Parquet => write!(f, "parquet"),
            Self::Arrow => write!(f, "arrow"),
            Self::Unknown(s) => write!(f, "{}", s),
        }
    }
}

/// How JSON/JSONL parsing reacts to a malformed record.
///
/// Shared across the file, in-memory, and async byte input paths so callers get
/// the same recovery behavior regardless of transport.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JsonErrorPolicy {
    /// Skip the malformed record, count it, and keep scanning. The profile is
    /// reported as partial via `execution.error_count`.
    #[default]
    Skip,
    /// Abort on the first malformed record with a `JsonParsingError` carrying
    /// line/column context (never the record contents).
    Strict,
}

/// Supported query engines for SQL-based profiling
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum QueryEngine {
    Postgres,
    MySql,
    Sqlite,
    Snowflake,
    BigQuery,
    #[serde(untagged)]
    Custom(String),
}

impl std::fmt::Display for QueryEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Postgres => write!(f, "postgres"),
            Self::MySql => write!(f, "mysql"),
            Self::Sqlite => write!(f, "sqlite"),
            Self::Snowflake => write!(f, "snowflake"),
            Self::BigQuery => write!(f, "bigquery"),
            Self::Custom(s) => write!(f, "{}", s),
        }
    }
}

/// Source library for in-memory DataFrame profiling
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum DataFrameLibrary {
    Pandas,
    Polars,
    PyArrow,
    #[serde(untagged)]
    Custom(String),
}

impl std::fmt::Display for DataFrameLibrary {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pandas => write!(f, "pandas"),
            Self::Polars => write!(f, "polars"),
            Self::PyArrow => write!(f, "pyarrow"),
            Self::Custom(s) => write!(f, "{}", s),
        }
    }
}

/// Supported stream source systems
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[serde(rename_all = "lowercase")]
pub enum StreamSourceSystem {
    Kafka,
    Kinesis,
    Pulsar,
    Http,
    WebSocket,
    #[serde(rename = "object_store")]
    ObjectStore,
    #[serde(rename = "message_queue")]
    MessageQueue,
    Database,
    #[serde(untagged)]
    Custom(String),
}

impl std::fmt::Display for StreamSourceSystem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Kafka => write!(f, "kafka"),
            Self::Kinesis => write!(f, "kinesis"),
            Self::Pulsar => write!(f, "pulsar"),
            Self::Http => write!(f, "http"),
            Self::WebSocket => write!(f, "websocket"),
            Self::ObjectStore => write!(f, "object_store"),
            Self::MessageQueue => write!(f, "message_queue"),
            Self::Database => write!(f, "database"),
            Self::Custom(s) => write!(f, "{}", s),
        }
    }
}

/// Metadata specific to Parquet files
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
pub struct ParquetMetadata {
    /// Number of row groups in the Parquet file
    pub num_row_groups: usize,
    /// Compression codec used (e.g., "SNAPPY", "GZIP", "ZSTD", "UNCOMPRESSED")
    pub compression: String,
    /// Parquet file version (e.g., "1.0", "2.0")
    pub version: i32,
    /// Arrow schema as string representation
    pub schema_summary: String,
    /// Total compressed size in bytes
    pub compressed_size_bytes: u64,
    /// Estimated uncompressed size if available
    pub uncompressed_size_bytes: Option<u64>,
}

/// How a CSV source was split into fields.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[non_exhaustive]
pub struct CsvSourceMetadata {
    /// The delimiter the reader split records on, as one character.
    pub delimiter: String,
    /// How detection chose the delimiter. `None` when the delimiter was set
    /// explicitly, so detection did not run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delimiter_detection: Option<DelimiterDetection>,
}

impl CsvSourceMetadata {
    /// A delimiter the caller set explicitly.
    pub fn configured(delimiter: u8) -> Self {
        Self {
            delimiter: char::from(delimiter).to_string(),
            delimiter_detection: None,
        }
    }

    /// A delimiter character the caller set, for a reader that splits on
    /// characters rather than bytes: Python's `csv` module, which reads
    /// synchronous CSV bytes, takes any one character, `§` included.
    pub fn configured_char(delimiter: char) -> Self {
        Self {
            delimiter: delimiter.to_string(),
            delimiter_detection: None,
        }
    }

    /// A delimiter detection chose.
    pub fn detected(detection: DelimiterDetection) -> Self {
        Self {
            delimiter: detection.delimiter.clone(),
            delimiter_detection: Some(detection),
        }
    }
}

/// What delimiter detection saw when it chose a delimiter.
///
/// Detection scores comma, semicolon, tab and pipe over the first records of
/// the source. Some sources read equally well with two of them, and some with
/// none: the record says so, and `findings()` warns about both.
#[derive(
    Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize, schemars::JsonSchema,
)]
#[non_exhaustive]
pub struct DelimiterDetection {
    /// The delimiter detection chose, as one character.
    pub delimiter: String,
    /// Fields the chosen delimiter splits the header into: 1 when no
    /// candidate splits it, 0 when the sampled source holds no header record.
    pub header_fields: usize,
    /// Another candidate that splits the header and keeps as many sampled
    /// records at the header's width as the chosen one, so that structure
    /// alone does not tell them apart. `None` when the choice was clear.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runner_up: Option<String>,
    /// When no candidate splits the header, a punctuation character outside
    /// the candidates that occurs as often in the header as in every sampled
    /// record, so it likely delimits the source. `None` when a candidate
    /// splits the header, and for an ordinary one-column source.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unlisted_delimiter: Option<String>,
}

impl DelimiterDetection {
    /// A detection outcome; `runner_up` is the tied candidate, if any.
    pub fn new(delimiter: u8, header_fields: usize, runner_up: Option<u8>) -> Self {
        Self {
            delimiter: char::from(delimiter).to_string(),
            header_fields,
            runner_up: runner_up.map(|byte| char::from(byte).to_string()),
            unlisted_delimiter: None,
        }
    }

    /// Record a likely delimiter detection does not try.
    pub fn with_unlisted_delimiter(mut self, unlisted: Option<u8>) -> Self {
        self.unlisted_delimiter = unlisted.map(|byte| char::from(byte).to_string());
        self
    }
}

/// Source-agnostic data source metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, schemars::JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DataSource {
    /// File-based data source (CSV, JSON, Parquet, etc.)
    File {
        /// Absolute or relative path to the file
        path: String,
        /// Detected or specified file format
        format: FileFormat,
        /// File size in bytes
        size_bytes: u64,
        /// Last modification timestamp (ISO 8601 / RFC 3339)
        #[serde(skip_serializing_if = "Option::is_none")]
        modified_at: Option<String>,
        /// Parquet-specific metadata (only present for Parquet files)
        #[serde(skip_serializing_if = "Option::is_none")]
        parquet_metadata: Option<ParquetMetadata>,
        /// How a CSV source was split into fields (only present for CSV)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        csv_metadata: Option<CsvSourceMetadata>,
    },
    /// SQL query-based data source
    Query {
        /// Database engine used for the query
        engine: QueryEngine,
        /// SQL statement executed
        statement: String,
        /// Target database name (if applicable)
        #[serde(skip_serializing_if = "Option::is_none")]
        database: Option<String>,
        /// Unique execution identifier for tracing
        #[serde(skip_serializing_if = "Option::is_none")]
        execution_id: Option<String>,
    },
    /// In-memory DataFrame source (pandas/polars via PyCapsule)
    #[serde(rename = "dataframe")]
    DataFrame {
        /// User-provided name for identification
        name: String,
        /// Source library (pandas, polars, pyarrow)
        source_library: DataFrameLibrary,
        /// Number of rows at profiling time
        row_count: usize,
        /// Number of columns
        column_count: usize,
        /// Memory usage in bytes (if available)
        #[serde(skip_serializing_if = "Option::is_none")]
        memory_bytes: Option<u64>,
    },
    /// In-memory byte buffer profiled without a path behind it. The buffer may
    /// be text (CSV/JSON/JSONL) or binary (Parquet); `format` says which.
    /// Distinguished from [`DataSource::DataFrame`]: the caller handed over
    /// bytes, not a schema-bearing dataframe, so provenance must say so.
    Bytes {
        /// Human-readable name for identification
        name: String,
        /// Format the buffer was parsed as
        format: FileFormat,
        /// Length of the buffer handed over, not of the values decoded from it
        size_bytes: u64,
        /// How a CSV source was split into fields (only present for CSV)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        csv_metadata: Option<CsvSourceMetadata>,
    },
    /// Streaming data source
    Stream {
        /// Stream identifier (e.g., Kafka topic, Kinesis stream name)
        topic: String,
        /// Batch identifier for ordering and deduplication
        batch_id: String,
        /// Partition for parallel processing (optional)
        #[serde(skip_serializing_if = "Option::is_none")]
        partition: Option<u32>,
        /// Consumer group for Kafka-style coordination (optional)
        #[serde(skip_serializing_if = "Option::is_none")]
        consumer_group: Option<String>,
        /// Source system identifier (kafka, kinesis, pulsar, http, etc.), as
        /// the caller declared it. Absent when the caller declared none: the
        /// report does not guess a transport (#1219).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        source_system: Option<StreamSourceSystem>,
        /// Format the stream was parsed as: `arrow` for an Arrow C stream
        /// (#1230). Absent in reports written before 0.14 (#1042).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        format: Option<FileFormat>,
        /// Session ID for multi-tenant scenarios
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        /// Timestamp of first record in batch (ISO 8601)
        #[serde(skip_serializing_if = "Option::is_none")]
        first_record_at: Option<String>,
        /// Timestamp of last record in batch (ISO 8601)
        #[serde(skip_serializing_if = "Option::is_none")]
        last_record_at: Option<String>,
        /// How a CSV source was split into fields (only present for CSV)
        #[serde(default, skip_serializing_if = "Option::is_none")]
        csv_metadata: Option<CsvSourceMetadata>,
    },
}

impl DataSource {
    /// Get a human-readable identifier for this data source.
    pub fn identifier(&self) -> String {
        match self {
            Self::File { path, .. } => path.clone(),
            Self::Query {
                engine, statement, ..
            } => {
                let truncated = if statement.chars().count() > 50 {
                    let mut prefix: String = statement.chars().take(47).collect();
                    prefix.push_str("...");
                    prefix
                } else {
                    statement.clone()
                };
                format!("{}: {}", engine, truncated)
            }
            Self::DataFrame {
                name,
                source_library,
                ..
            } => format!("{}[{}]", source_library, name),
            Self::Bytes { name, .. } => format!("bytes[{name}]"),
            Self::Stream {
                source_system,
                topic,
                batch_id,
                ..
            } => match source_system {
                Some(system) => format!("{system}[{topic}]-batch:{batch_id}"),
                None => format!("stream[{topic}]-batch:{batch_id}"),
            },
        }
    }

    /// Get file size in megabytes if this is a file-based source or dataframe.
    pub fn size_mb(&self) -> Option<f64> {
        match self {
            Self::File { size_bytes, .. } => Some(*size_bytes as f64 / 1_048_576.0),
            Self::DataFrame { memory_bytes, .. } => memory_bytes.map(|b| b as f64 / 1_048_576.0),
            Self::Bytes { size_bytes, .. } => Some(*size_bytes as f64 / 1_048_576.0),
            Self::Query { .. } | Self::Stream { .. } => None,
        }
    }

    /// Check if this is a file-based source.
    pub fn is_file(&self) -> bool {
        matches!(self, Self::File { .. })
    }

    /// Check if this is a query-based source.
    pub fn is_query(&self) -> bool {
        matches!(self, Self::Query { .. })
    }

    /// Check if this is a DataFrame-based source.
    pub fn is_dataframe(&self) -> bool {
        matches!(self, Self::DataFrame { .. })
    }

    /// Check if this is a Stream-based source.
    pub fn is_stream(&self) -> bool {
        matches!(self, Self::Stream { .. })
    }

    /// Check if this is an in-memory byte-buffer source.
    pub fn is_bytes(&self) -> bool {
        matches!(self, Self::Bytes { .. })
    }

    /// Get the file path if this is a file-based source.
    pub fn file_path(&self) -> Option<&str> {
        match self {
            Self::File { path, .. } => Some(path),
            _ => None,
        }
    }

    /// How a CSV source was split into fields, when the source records it.
    pub fn csv_metadata(&self) -> Option<&CsvSourceMetadata> {
        match self {
            Self::File { csv_metadata, .. }
            | Self::Bytes { csv_metadata, .. }
            | Self::Stream { csv_metadata, .. } => csv_metadata.as_ref(),
            Self::Query { .. } | Self::DataFrame { .. } => None,
        }
    }

    /// Record how a CSV source was split into fields. A query or dataframe
    /// source has no delimiter, so it is returned unchanged.
    pub fn with_csv_metadata(mut self, metadata: CsvSourceMetadata) -> Self {
        match &mut self {
            Self::File { csv_metadata, .. }
            | Self::Bytes { csv_metadata, .. }
            | Self::Stream { csv_metadata, .. } => *csv_metadata = Some(metadata),
            Self::Query { .. } | Self::DataFrame { .. } => {}
        }
        self
    }

    /// Get the stream topic if this is a stream-based source.
    pub fn stream_topic(&self) -> Option<&str> {
        match self {
            Self::Stream { topic, .. } => Some(topic),
            _ => None,
        }
    }

    /// Get the batch ID if this is a stream-based source.
    pub fn batch_id(&self) -> Option<&str> {
        match self {
            Self::Stream { batch_id, .. } => Some(batch_id),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_source_file_identifier() {
        let ds = DataSource::File {
            path: "/path/to/data.csv".to_string(),
            format: FileFormat::Csv,
            size_bytes: 0,
            modified_at: None,
            parquet_metadata: None,
            csv_metadata: None,
        };

        assert_eq!(ds.identifier(), "/path/to/data.csv");
        assert!(ds.is_file());
        assert!(!ds.is_query());
        assert!(!ds.is_dataframe());
        assert!(!ds.is_stream());
    }

    #[test]
    fn test_data_source_bytes_identifier_and_helpers() {
        let ds = DataSource::Bytes {
            name: "csv_bytes".to_string(),
            format: FileFormat::Csv,
            size_bytes: 42,
            csv_metadata: None,
        };

        assert_eq!(ds.identifier(), "bytes[csv_bytes]");
        assert!(ds.is_bytes());
        assert!(!ds.is_file());
        assert!(!ds.is_dataframe());
        assert!(!ds.is_stream());
        assert_eq!(ds.size_mb(), Some(42.0 / 1_048_576.0));
    }

    #[test]
    fn test_data_source_stream_identifier_and_helpers() {
        let ds = DataSource::Stream {
            topic: "events".to_string(),
            batch_id: "b1".to_string(),
            partition: Some(0),
            consumer_group: None,
            source_system: Some(StreamSourceSystem::Kafka),
            format: None,
            session_id: None,
            first_record_at: None,
            last_record_at: None,
            csv_metadata: None,
        };

        assert_eq!(ds.identifier(), "kafka[events]-batch:b1");
        assert!(ds.is_stream());
        assert_eq!(ds.stream_topic(), Some("events"));
        assert_eq!(ds.batch_id(), Some("b1"));
        assert!(!ds.is_file());
        assert!(!ds.is_query());
        assert!(ds.size_mb().is_none());
    }

    #[test]
    fn test_stream_without_a_declared_system_records_none() {
        let ds = DataSource::Stream {
            topic: "events".to_string(),
            batch_id: "b1".to_string(),
            partition: None,
            consumer_group: None,
            source_system: None,
            format: None,
            session_id: None,
            first_record_at: None,
            last_record_at: None,
            csv_metadata: None,
        };

        assert_eq!(ds.identifier(), "stream[events]-batch:b1");
        let json = serde_json::to_value(&ds).unwrap();
        assert!(json.get("source_system").is_none(), "{json}");
        let reloaded: DataSource = serde_json::from_value(json.clone()).unwrap();
        assert_eq!(serde_json::to_value(&reloaded).unwrap(), json);
    }

    #[test]
    fn test_stream_json_serialization() {
        let ds = DataSource::Stream {
            topic: "sensor-data".to_string(),
            batch_id: "batch-789".to_string(),
            partition: Some(2),
            consumer_group: Some("processing-group".to_string()),
            source_system: Some(StreamSourceSystem::Kinesis),
            format: None,
            session_id: Some("session-1".to_string()),
            first_record_at: Some("2023-01-01T10:00:00Z".to_string()),
            last_record_at: Some("2023-01-01T10:05:00Z".to_string()),
            csv_metadata: None,
        };

        let json = serde_json::to_string(&ds).unwrap();
        assert!(json.contains(r#""type":"stream""#));
        assert!(json.contains(r#""source_system":"kinesis""#));
        assert!(json.contains(r#""topic":"sensor-data""#));

        let deserialized: DataSource = serde_json::from_str(&json).unwrap();
        assert!(deserialized.is_stream());
        assert_eq!(deserialized.stream_topic(), Some("sensor-data"));
    }

    #[test]
    fn test_stream_source_system_serialization_names() {
        let object_store = serde_json::to_string(&StreamSourceSystem::ObjectStore).unwrap();
        let message_queue = serde_json::to_string(&StreamSourceSystem::MessageQueue).unwrap();
        let database = serde_json::to_string(&StreamSourceSystem::Database).unwrap();

        assert_eq!(object_store, r#""object_store""#);
        assert_eq!(message_queue, r#""message_queue""#);
        assert_eq!(database, r#""database""#);

        let object_store: StreamSourceSystem = serde_json::from_str(r#""object_store""#).unwrap();
        let message_queue: StreamSourceSystem = serde_json::from_str(r#""message_queue""#).unwrap();
        let database: StreamSourceSystem = serde_json::from_str(r#""database""#).unwrap();

        assert_eq!(object_store, StreamSourceSystem::ObjectStore);
        assert_eq!(message_queue, StreamSourceSystem::MessageQueue);
        assert_eq!(database, StreamSourceSystem::Database);
    }
}
