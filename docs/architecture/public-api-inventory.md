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

## Error Type Compatibility

`DataProfilerError` is a public enum, and two of its properties are
compatibility-sensitive.

**It is not `Clone`.** The variants that can carry a cause hold an
`ErrorSource` (a boxed `dyn Error`), and neither that nor the errors it wraps
(`std::io::Error`, `csv::Error`, ...) are cloneable. `Box` is used rather than
`Arc` deliberately: only a boxed source lets `Error::source()` hand back the
concrete error, so `downcast_ref::<std::io::Error>()` resolves. Behind an `Arc`
the chain yields the `Arc` instead, every message still reads correctly, and
every downcast silently returns `None`.

**The cause-carrying variants are `#[non_exhaustive]`.** `CsvParsingError`,
`IoError`, `JsonParsingError`, `ParquetError`, and `ArrowError` cannot be built
with a struct expression from outside `dataprof-core`; use the constructors
(`DataProfilerError::io_error`, `::json_parsing_with_source`,
`::parquet_with_source`, and so on). Matching on them still works and must use
`..`. This keeps adding a field to those variants a non-breaking change.

Downstream effects to expect when upgrading:

| If you… | What to do |
| --- | --- |
| clone a `DataProfilerError` | Propagate it by value, or clone `to_string()` if only the text is needed. |
| construct one of the five variants directly | Call the matching constructor instead. |
| call `AutoRecoveryManager::attempt_recovery` | It takes the error by value now rather than by reference. |
| call `DataProfilerError::io_error(&err)` | Pass the error by value: `map_err(DataProfilerError::io_error)`. |

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

## Python Package Surface

The Python package is held to the same rule as the Rust facade: what
`__all__` declares is what is reachable. `python/tests/test_public_surface.py`
enforces it for every module below, and carries the same names as
`EXPECTED_SURFACE`, so growing the API is a deliberate edit rather than a side
effect.

| Module | Exports |
| --- | --- |
| `dataprof` | `Capabilities`, `capabilities`, `REPORT_SCHEMA_VERSION`, `profile`, `profile_file`, `Profiler`, `ProfileReport`, `ProfilerConfig`, `ColumnProfile`, `DataQualityMetrics`, `SamplingStrategy`, `StopCondition`, `ProgressEvent`, `list_patterns`, `infer_schema`, `quick_row_count`, `analyze_structure`, `SchemaResult`, `RowCountEstimate`, `StructureColumnSummary`, `StructureReport`, `RecordBatch`, `column_to_dict`, `asyncio`, `__version__`, `analyze_database_async`, `count_table_rows_async`, `get_table_schema_async`, `test_connection_async` |
| `dataprof.agent` | `AgentGuard`, `AgentSecurityError`, `AgentTimeoutError`, `PathNotAllowedError`, `ResourceLimitExceededError`, `SandboxPolicy` |
| `dataprof.asyncio` | `profile_bytes`, `profile_file`, `profile_url`, `infer_schema_stream`, `quick_row_count_stream` |
| `dataprof.interop` | `analyze_file`, `profile_dataframe`, `profile_arrow`, `analyze_csv_to_arrow`, `analyze_parquet_to_arrow`, `column_to_dict`, `ProfilerConfig`, `ProfileReport`, `ColumnProfile`, `DataQualityMetrics`, `RecordBatch` |

### Not Public

Everything the implementation imports for its own use is bound under a private
alias — `import os as _os`, `from typing import Any as _Any` — so it does not
land in the package namespace. Before this was enforced, `dataprof.os`,
`dataprof.json` and `dataprof.pathlib` were all importable, and
`dataprof.asyncio` declared no `__all__` at all.

The package's own submodules (`dataprof.agent`, `dataprof.interop`) become
attributes of `dataprof` once imported anywhere in the process. That is normal
package behaviour rather than a leak, and the guard ignores it.
