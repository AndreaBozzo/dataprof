# Profile report JSON Schema

The versioned
[`profile-report.v1.schema.json`](profile-report.v1.schema.json) document is the
machine-readable contract for serialized `ProfileReport` values. It uses JSON
Schema 2020-12, carries a stable project-owned `$id`, and keeps every `$ref`
self-contained under `$defs`.

Schema versions are independent of package versions. The filename and `$id`
must use the same version as Rust's `REPORT_SCHEMA_VERSION` and Python's
`dataprof.REPORT_SCHEMA_VERSION`.

Version 1 covers both existing serialization dialects:

- Rust's complete runtime document (`data_source`, `column_profiles`, and the
  confidence-wrapped quality assessment).
- Python's high-level export document (`source`, `source_type`, `columns`, and
  its flattened quality summary).

Both dialects accept unknown additive object properties. This matches the v1
reader policy and lets compatible fields be added without invalidating stored
reports. Required fields, schema version, known enum values, and primitive
types remain enforced.

## Regenerate and verify

Run from the repository root:

```bash
cargo run --example generate_profile_schema
cargo test --test profile_report_schema
```

The generator explicitly selects `SchemaSettings::draft2020_12()` and the
Serde serialization contract. CI reruns it and fails if the committed artifact
drifts. Rust tests validate the artifact against the bundled Draft 2020-12
meta-schema, compile it without network resolution, and validate representative
reports. Python tests validate `to_dict()`, `to_json()`, and JSON `save()`
output against the same file.

When changing a serialized report field, review compatibility first. Additive
fields with reader defaults stay in the current schema version; an incompatible
change requires incrementing `REPORT_SCHEMA_VERSION` and committing a new
versioned schema without deleting schemas that supported releases still read.
