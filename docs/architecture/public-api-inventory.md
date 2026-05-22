# Public API Inventory

This inventory records the facade API currently exposed from `src/lib.rs`.
It is a migration guide for the crate redesign: the public `dataprof` package
can keep stable paths while implementation moves into smaller workspace crates.

## Must Stay Stable

These are the primary paths users are expected to depend on. Moving their
implementation is fine; removing or renaming them should be treated as a
breaking public API change.

| Surface | Exports |
| --- | --- |
| Main profiler API | `Profiler`, `ProfilerConfig`, `EngineType`, `quick_quality_check`, `quick_quality_check_source` |
| Partial file APIs | `infer_schema`, `quick_row_count`, `ColumnSchema`, `CountMethod`, `RowCountEstimate`, `SchemaResult` |
| Error and config primitives | `DataProfilerError`, `DataprofConfig`, `DataprofConfigBuilder`, `InputValidator`, `ValidationError` |
| Execution controls | `ChunkSize`, `SamplingStrategy`, `StopCondition`, `StopEvaluator`, `ProgressEvent`, `ProgressSink` |
| Core report model | `ProfileReport`, `ColumnProfile`, `ColumnStats`, `DataSource`, `FileFormat`, `DataType`, `ExecutionMetadata`, `MetricPack`, `OutputFormat`, `Pattern`, `PatternCategory`, `QualityAssessment`, `QualityDimension`, `QualityMetrics`, `QueryEngine`, `TruncationReason` |
| Quality metric DTOs | `AccuracyMetrics`, `CompletenessMetrics`, `ConsistencyMetrics`, `MetricConfidence`, `TimelinessMetrics`, `UniquenessMetrics` |
| Format entry points | `CsvParserConfig`, `CsvDiagnostics`, `analyze_csv_file`, `analyze_csv_from_reader`, `JsonFormat`, `JsonParserConfig`, `analyze_json_file`, `analyze_json_from_reader` |
| Analysis helpers | `MetricsCalculator`, `analyze_column_fast`, `detect_patterns`, `infer_type`, `calculate_numeric_stats`, `calculate_text_stats` |

## Feature-Gated Stable Surface

These paths should remain stable when their features are enabled, even as their
implementation crates change.

| Feature | Exports |
| --- | --- |
| `parquet` | `ParquetConfig`, `analyze_parquet_with_config`, `analyze_parquet_with_quality`, `is_parquet_file` |
| `database` | `DatabaseConfig`, `DatabaseConnector`, `DatabaseCredentials`, `MySqlConnector`, `PostgresConnector`, `RetryConfig`, `SamplingConfig`, `DbSamplingStrategy`, `SqliteConnector`, `SslConfig`, `analyze_database`, `create_connector` |
| `database` compatibility | `process_rows_to_columns`, `streaming_profile_loop` |
| `async-streaming` | `AsyncDataSource`, `AsyncSourceInfo`, `AsyncStreamingProfiler`, `BytesSource` |
| `parquet-async` | `ReqwestSource` |
| `python` | `python` module for PyO3 extension builds |

## Migration Re-Exports

These modules preserve old paths while implementation lives elsewhere. They are
valuable during the redesign, but should be documented as compatibility layers
rather than the preferred ownership boundary.

| Facade path | Current owner |
| --- | --- |
| `dataprof::types::*` | `dataprof-core`, `dataprof-metrics`, `dataprof-runtime` |
| `dataprof::analysis::*` | `dataprof-metrics` |
| `dataprof::stats::*` | `dataprof-metrics` |
| `dataprof::parsers::csv::*` | `dataprof-csv` |
| `dataprof::parsers::json::*` | `dataprof-json` |
| `dataprof::parsers::parquet::*` | `dataprof-parquet` |
| `dataprof::database::*` | `dataprof-db` |
| `dataprof::core::profile_builder::*` | `dataprof-runtime` |
| `dataprof::core::report_assembler::*` | `dataprof-runtime` |
| `dataprof::core::streaming_stats::*` | `dataprof-runtime` |
| `dataprof::core::memory_tracker::*` | `dataprof-core` |

## Internal-Only Candidates

These are public today because the crate historically exposed broad module
access. During facade polish, decide whether to keep them documented, gate them,
or move them behind narrower public entry points.

| Surface | Reason to review |
| --- | --- |
| `acceleration` module | Low-level implementation detail around SIMD helpers. |
| `engines` module | Users should normally go through `Profiler` and `EngineType`. |
| `serde_helpers` module | Serialization plumbing, not a user workflow. |
| `check_memory_leaks`, `get_memory_usage_stats` | Diagnostics helpers with unclear facade-level stability contract. |
| Database row-processing macros | Useful for compatibility, but not an ideal long-term public API. |

## Coverage Expectations

Public API compile coverage should exercise:

- default facade imports and builder methods
- parser and metrics re-export paths
- `--no-default-features --features minimal`
- `--features parquet`
- `--features database`
- `--features all-db`
- `--features async-streaming`
- deprecated compatibility aliases such as `--features cli` and
  `--features full-cli`

The coverage should prove that paths compile. Behavioral tests can remain
focused on the owning crates and the end-to-end facade workflows.
