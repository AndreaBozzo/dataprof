# dataprof-json

Streaming JSON and JSON Lines scanning for the `dataprof` workspace.

This crate owns JSON format selection, tolerant and strict error handling,
row-wise scanning, and reader- and file-based report assembly. General metrics,
source orchestration, and Python-facing APIs belong elsewhere.

## Public surface

The main entry points are `JsonFormat`, `JsonParserConfig`,
`scan_json_from_reader`, `analyze_json_from_reader`, and
`analyze_json_file`.

## Features

This crate has no feature flags.

## Development

```bash
cargo test -p dataprof-json
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/master/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/master/docs/architecture/crate-redesign.md)
for crate ownership details.
