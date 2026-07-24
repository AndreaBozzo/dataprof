# dataprof-runtime

Shared report assembly and engine-facing runtime helpers for the `dataprof`
workspace.

This crate owns `ProfileReport`, column-profile construction, report assembly,
streaming statistics, reservoir and uniqueness tracking, memory configuration,
and optional async source abstractions. It does not parse formats or select
engines.

## Public surface

Common entry points include `ProfileReport`, `ReportAssembler`,
`StreamingStatistics`, `StreamingColumnCollection`, `build_column_profile`,
and, when enabled, `AsyncDataSource` and `BytesSource`.

## Features

- `async-streaming`: async data-source abstractions, including byte and HTTP
  sources.
- `parquet-async`: async Parquet runtime integration; enables
  `async-streaming`.

The default feature set is empty.

## Development

```bash
cargo test -p dataprof-runtime --all-features
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/master/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/master/docs/architecture/crate-redesign.md)
for crate ownership details.
