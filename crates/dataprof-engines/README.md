# dataprof-engines

Adaptive, incremental, and asynchronous profiling engines for the `dataprof`
workspace.

This crate owns engine selection and execution strategies, including
memory-mapped, incremental, async-streaming, and optional columnar paths. File
format parsing, shared report types, and metric calculations remain in their
own crates.

## Public surface

The principal engine types are `AdaptiveProfiler`, `IncrementalProfiler`, and,
when enabled, `AsyncStreamingProfiler`.

## Features

- `arrow`: Arrow-backed columnar profiling.
- `parquet`: synchronous Parquet engine support; enables `arrow`.
- `async-streaming`: async sources and streaming analysis.
- `parquet-async`: async Parquet integration; enables `async-streaming`.

The default feature set is empty.

## Development

```bash
cargo test -p dataprof-engines --all-features
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/HEAD/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/architecture/crate-redesign.md)
for crate ownership details.
