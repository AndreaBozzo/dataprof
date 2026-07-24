# dataprof-partial

Fast schema inference, structure inspection, and row-count estimation for the
`dataprof` workspace.

This crate owns partial-analysis operations that avoid building a full profile,
with explicit file-format and async-stream variants. Full metric calculation,
report assembly, and engine orchestration belong elsewhere.

## Public surface

The primary entry points are `infer_schema`, `quick_row_count`,
`analyze_structure`, their explicit-format variants, and, when enabled,
`infer_schema_stream` and `quick_row_count_stream`.

## Features

- `parquet`: Parquet schema and row-count support.
- `async-streaming`: async file and byte-stream partial analysis.

The default feature set is empty.

## Development

```bash
cargo test -p dataprof-partial --all-features
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/HEAD/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/architecture/crate-redesign.md)
for crate ownership details.
