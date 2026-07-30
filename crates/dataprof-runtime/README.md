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

## Serialized report contract

`ProfileReport` serialization is versioned independently from the crate
release through `REPORT_SCHEMA_VERSION`. The generated
[JSON Schema 2020-12 artifact](../../docs/schema/profile-report.v1.schema.json)
describes every supported v1 serialization dialect and is the contract for
saved reports and downstream validators. Unknown additive object fields remain
accepted, matching the reader compatibility policy.

Regenerate and verify it from the workspace root:

```bash
cargo run --example generate_profile_schema
cargo test --test profile_report_schema
```

## Development

```bash
cargo test -p dataprof-runtime --all-features
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/HEAD/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/architecture/crate-redesign.md)
for crate ownership details.
