# dataprof-core

Shared data model and configuration types for the `dataprof` workspace.

This crate owns format-independent contracts: profile and schema types, execution
metadata, errors, configuration, sampling and stop conditions, semantic hints,
source descriptions, validation, progress, and memory tracking. Parsing,
statistical analysis, report assembly, and engine orchestration belong to their
specialized crates.

## Public surface

Common entry points include `DataprofConfig`, `DataProfilerError`,
`ExecutionMetadata`, `ColumnProfile`, `SamplingStrategy`, `StopCondition`,
`SemanticHints`, and `DataSource`.

## Features

- `arrow`: Arrow-backed core types.
- `async-streaming`: async coordination primitives.
- `database`: database-specific configuration.
- `parquet`: Parquet-specific source metadata.

The default feature set is empty.

## Development

```bash
cargo test -p dataprof-core
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/master/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/master/docs/architecture/crate-redesign.md)
for crate ownership details.
