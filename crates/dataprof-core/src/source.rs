/// Supported file formats for data profiling
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Csv,
    Json,
    Jsonl,
    Parquet,
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
            Self::Unknown(s) => write!(f, "{}", s),
        }
    }
}

/// Supported query engines for SQL-based profiling
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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

/// Source-agnostic data source metadata.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
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
        /// Source system identifier (kafka, kinesis, pulsar, http, etc.)
        source_system: StreamSourceSystem,
        /// Session ID for multi-tenant scenarios
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        /// Timestamp of first record in batch (ISO 8601)
        #[serde(skip_serializing_if = "Option::is_none")]
        first_record_at: Option<String>,
        /// Timestamp of last record in batch (ISO 8601)
        #[serde(skip_serializing_if = "Option::is_none")]
        last_record_at: Option<String>,
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
                let truncated = if statement.len() > 50 {
                    format!("{}...", &statement[..47])
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
            Self::Stream {
                source_system,
                topic,
                batch_id,
                ..
            } => format!("{}[{}]-batch:{}", source_system, topic, batch_id),
        }
    }

    /// Get file size in megabytes if this is a file-based source or dataframe.
    pub fn size_mb(&self) -> Option<f64> {
        match self {
            Self::File { size_bytes, .. } => Some(*size_bytes as f64 / 1_048_576.0),
            Self::DataFrame { memory_bytes, .. } => memory_bytes.map(|b| b as f64 / 1_048_576.0),
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

    /// Get the file path if this is a file-based source.
    pub fn file_path(&self) -> Option<&str> {
        match self {
            Self::File { path, .. } => Some(path),
            _ => None,
        }
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
        };

        assert_eq!(ds.identifier(), "/path/to/data.csv");
        assert!(ds.is_file());
        assert!(!ds.is_query());
        assert!(!ds.is_dataframe());
        assert!(!ds.is_stream());
    }

    #[test]
    fn test_data_source_stream_identifier_and_helpers() {
        let ds = DataSource::Stream {
            topic: "events".to_string(),
            batch_id: "b1".to_string(),
            partition: Some(0),
            consumer_group: None,
            source_system: StreamSourceSystem::Kafka,
            session_id: None,
            first_record_at: None,
            last_record_at: None,
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
    fn test_stream_json_serialization() {
        let ds = DataSource::Stream {
            topic: "sensor-data".to_string(),
            batch_id: "batch-789".to_string(),
            partition: Some(2),
            consumer_group: Some("processing-group".to_string()),
            source_system: StreamSourceSystem::Kinesis,
            session_id: Some("session-1".to_string()),
            first_record_at: Some("2023-01-01T10:00:00Z".to_string()),
            last_record_at: Some("2023-01-01T10:05:00Z".to_string()),
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
