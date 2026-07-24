# dataprof-parquet

Parquet and Arrow-backed profiling support for the `dataprof` workspace.

This crate owns synchronous Parquet analysis, shared Arrow record-batch
analysis, and optional asynchronous HTTP Range reading. It does not choose the
overall profiling engine or own format-independent report contracts.

## Public surface

Depending on enabled features, the main entry points are `ArrowProfiler`,
`RecordBatchAnalyzer`, `ParquetConfig`, `analyze_parquet_with_config`, and
`HttpParquetReader`.

## Features

- `arrow`: Arrow record-batch analysis.
- `parquet`: synchronous Parquet profiling; enables `arrow`.
- `parquet-async`: asynchronous HTTP Parquet profiling; enables `parquet`.

The default feature set is empty.

## Development

```bash
cargo test -p dataprof-parquet --all-features
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/HEAD/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/architecture/crate-redesign.md)
for crate ownership details.
