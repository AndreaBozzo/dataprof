# dataprof-json

Streaming JSON and JSON Lines scanning for the `dataprof` workspace.

This crate owns JSON format selection, tolerant and strict error handling,
row-wise scanning, and reader- and file-based report assembly. General metrics,
source orchestration, and Python-facing APIs belong elsewhere.

## Public surface

The main entry points are `JsonFormat`, `JsonParserConfig`,
`scan_json_from_reader`, `analyze_json_from_reader`, and
`analyze_json_file`.

## Column order

Columns are reported in source order: the first record's field order, with
fields that only appear in later records appended where they were first seen.
This matches CSV header order and Parquet schema order, so the same logical
dataset profiles to the same column order in every format. The contract relies
on the workspace enabling `serde_json/preserve_order`.

## Features

This crate has no feature flags.

## Development

```bash
cargo test -p dataprof-json
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/HEAD/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/HEAD/docs/architecture/crate-redesign.md)
for crate ownership details.
