# dataprof API reference for agents

The surface an agent needs. Full documentation lives in the project's
`docs/guides/`; this file is the working subset.

## Contents

- Entry points
- Profiler builder (when keyword arguments are not enough)
- ProfileReport: execution metadata
- ProfileReport: output formats
- ColumnProfile fields
- DataQualityMetrics fields
- StructureReport fields
- Capabilities fields
- Persisting reports
- AgentGuard and SandboxPolicy

## Entry points

```python
import dataprof as dp

dp.capabilities()                          # -> Capabilities
dp.analyze_structure(path, max_rows=None)  # -> StructureReport
dp.profile(source, ...)                    # -> ProfileReport
dp.profile_file(path, ...)                 # -> ProfileReport
dp.list_patterns(locale=None)              # -> list[dict]
dp.infer_schema(path)                      # -> SchemaResult
dp.quick_row_count(path)                   # -> RowCountEstimate
```

`dp.profile()` accepts a path, a URL, bytes, a pandas/polars DataFrame, or an
Arrow table, subject to `capabilities()`.

Keyword arguments: `engine`, `chunk_size`, `memory_limit_mb`, `format`,
`max_rows`, `name`, `csv_delimiter`, `csv_flexible`, `jsonl_on_error`,
`sampling`, `stop_condition`, `on_progress`, `progress_interval_ms`,
`quality_dimensions`, `metrics`, `locale`, `positive_columns`,
`identifier_columns`, `temporal_columns`.

## Profiler builder (when keyword arguments are not enough)

```python
report = (
    dp.Profiler()
    .engine("auto")
    .max_rows(100_000)
    .metrics(["schema", "quality"])
    .quality_dimensions(["completeness", "uniqueness"])
    .identifier_columns(["customer_id"])
    .profile("data.csv")
)
```

Every builder method returns the profiler, so calls chain. Other methods:
`chunk_size`, `memory_limit_mb`, `format`, `name`, `csv_delimiter`,
`csv_flexible`, `sampling`, `stop_condition`, `stop_when`, `on_progress`,
`progress_interval_ms`, `locale`, `positive_columns`, `temporal_columns`.

## ProfileReport: execution metadata

Read these before reporting any number. See the trust-signal step in SKILL.md.

| Field | Meaning |
| --- | --- |
| `report.source` / `report.source_type` | what was profiled |
| `report.engine` | engine that produced the numbers |
| `report.rows` / `report.columns` | shape actually analyzed |
| `report.sampling_applied` / `report.sampling_ratio` | whether numbers describe a sample |
| `report.truncation_reason` | non-`None` means the read stopped early |
| `report.source_exhausted` | `False` means the source was not read to the end |
| `report.low_sample_warning` | too few rows to trust distributions |
| `report.error_count` | rows that failed to parse |
| `report.ragged_row_count` | rows whose field count did not match the header |
| `report.execution_time_ms` / `report.throughput` / `report.memory_peak_mb` | cost |
| `report.semantic_hint_bindings` | which columns the semantic policies bound to |

## ProfileReport: output formats

```python
report.to_llm_context(max_tokens=1000, include_samples=False)  # redacting; prefer this
report.to_markdown()                                           # column table
report.quality_summary()                                       # single-row dict
report.to_dict()                                               # full; "columns" grows with width
report.to_json(indent=2)
report.to_html()
report.compare(other)                                          # -> dict of deltas
```

Interop, subject to `capabilities()`: `report.to_dataframe()`,
`report.to_polars()`, `report.to_arrow()`, `report.describe()`.

Per column: `report["email"]`, `report.column_profiles()`, `report.profiles()`,
`"email" in report`, `len(report)`, and iteration yields column names.

## ColumnProfile fields

`name`, `data_type`, `total_count`, `null_count`, `null_percentage`,
`unique_count`, `uniqueness_ratio`, `unique_count_is_approximate`,
`is_approximate`, `patterns`, `min`, `max`, `mean`, `median`, `mode`, `std_dev`,
`variance`, `quartiles`, `skewness`, `kurtosis`, `coefficient_of_variation`,
`outlier_count`, `invalid_count`, `min_length`, `max_length`, `avg_length`,
`true_count`, `false_count`, `true_ratio`.

`is_approximate` and `unique_count_is_approximate` are provenance, not noise:
when set, the number came from an estimator rather than an exact count. Say so.

## DataQualityMetrics fields

Dimension scores: `completeness`, `consistency`, `uniqueness`, `accuracy`,
`timeliness`, `validity`, `precision`, plus `overall_quality_score`.

`assessed_dimensions` and `dimension_scores` tell you which were actually
computed — a dimension absent from `assessed_dimensions` was not assessed, and
its score is `None` rather than zero.

Supporting evidence: `missing_values_ratio`, `complete_records_ratio`,
`null_columns`, `duplicate_rows`, `key_uniqueness`, `high_cardinality_warning`,
`data_type_consistency`, `format_violations`, `encoding_issues`,
`outlier_ratio`, `range_violations`, `negative_values_in_positive`,
`future_dates_count`, `invalid_date_values`, `stale_data_ratio`,
`temporal_violations`, `low_sample_warning`, `score_weights`.

## StructureReport fields

`source`, `format`, `delimiter`, `row_count`, `rows_sampled`, `columns`,
`truncated`, `truncation_reason`, `source_exhausted`, `warnings`.

Each entry in `columns` is a `StructureColumnSummary`: `name`, `data_type`,
`total_count`, `null_count`, `null_ratio`, `unique_count`, `uniqueness_ratio`,
`distinct_count_approximate`, `provenance`.

## Capabilities fields

`version`, `local_csv`, `local_json`, `local_jsonl`, `local_parquet`,
`pandas_interop`, `pandas_installed`, `polars_interop`, `polars_installed`,
`arrow_interop`, `pyarrow_installed`, `async_streaming`, `url_profiling`,
`remote_parquet`, `database`, `database_connectors`.

The `*_interop` flags mean dataprof was compiled with the bridge; the
`*_installed` flags mean the third-party package is importable. Both must be
true for that path to work.

## Persisting reports

```python
report.save("profile.json")
restored = dp.ProfileReport.load("profile.json")
```

Also `dp.ProfileReport.from_dict(d)` and `dp.ProfileReport.from_json(text)`.
Saved reports carry `dp.REPORT_SCHEMA_VERSION`; a report written by a newer
schema version is rejected rather than silently misread.

## AgentGuard and SandboxPolicy

```python
from dataprof.agent import AgentGuard, SandboxPolicy

guard = AgentGuard(SandboxPolicy(roots=["/srv/data"]))
guard.analyze_structure("customers.csv")
report = guard.profile("customers.csv")
guard.llm_context(report, max_tokens=1000, include_samples=False)
```

`SandboxPolicy` fields and defaults: `roots` (required), `max_file_bytes`
(256 MiB), `max_rows` (1,000,000), `max_bytes` (256 MiB), `timeout_seconds`
(30.0), `follow_symlinks` (`False`), `allow_network` (`False`), `allow_samples`
(`False`).

Rejections raise `AgentSecurityError` subclasses — `PathNotAllowedError`,
`ResourceLimitExceededError`, `AgentTimeoutError` — whose messages are safe to
return to a model verbatim.
