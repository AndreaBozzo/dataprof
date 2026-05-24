# Public API Inventory

This inventory records the facade API currently exposed from
`crates/dataprof/src/lib.rs`. It is a release guide for the 0.8 crate redesign:
the public `dataprof` package keeps a compact top-level API while implementation
ownership lives in smaller workspace crates.

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
| `async-streaming` | `AsyncDataSource`, `AsyncSourceInfo`, `AsyncStreamingProfiler`, `BytesSource`, `ReqwestSource` |
| `parquet-async` | `HttpParquetReader`, `analyze_parquet_async_http` |

## Internal-Only Candidates

These surfaces intentionally do not belong to the 0.8 facade. They remain
available through their owning workspace crates when needed.

| Surface | Reason to review |
| --- | --- |
| Engine implementation modules | Users should normally go through `Profiler` and `EngineType`; direct engine work belongs in `dataprof-engines`. |
| Parser submodules | Format implementations belong in `dataprof-csv`, `dataprof-json`, and `dataprof-parquet`; the facade exposes only common entry points. |
| Runtime assembly helpers | Report assembly and streaming stats belong in `dataprof-runtime`. |
| Low-level acceleration and serialization helpers | Implementation details owned by `dataprof-metrics` and `dataprof-core`. |
| Database row-processing helpers | Internal database pipeline details owned by `dataprof-db`. |

## Coverage Expectations

Public API compile coverage should exercise:

- default facade imports and builder methods
- parser and metrics top-level re-exports
- `--no-default-features`
- `--no-default-features --features async-streaming`
- `--features parquet`
- `--features database`
- `--features all-db`
- `--features async-streaming`
- `--features parquet-async`

The coverage should prove that paths compile. Behavioral tests can remain
focused on the owning crates and the end-to-end facade workflows.

CI now enforces the lean facade combinations with `cargo check`,
`public_api_facade` integration-test runs, and owning-crate unit tests for
`dataprof-engines`, `dataprof-parquet`, and `dataprof-partial` so the facade
cannot accidentally grow implementation ownership again.
