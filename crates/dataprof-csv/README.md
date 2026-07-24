# dataprof-csv

CSV parsing and report assembly for the `dataprof` workspace.

This crate owns delimiter detection, parser configuration, robust CSV recovery
and diagnostics, reader- and file-based CSV analysis, and memory-mapped CSV
reading. Format-independent metrics and engine selection belong elsewhere.

## Public surface

The main entry points are `CsvParserConfig`, `detect_delimiter`,
`analyze_csv_from_reader`, `analyze_csv_file`, and
`MemoryMappedCsvReader`.

## Features

This crate has no feature flags.

## Development

```bash
cargo test -p dataprof-csv
```

Most users should depend on the high-level
[`dataprof` facade](https://github.com/AndreaBozzo/dataprof/blob/master/README.md).
See the
[workspace architecture](https://github.com/AndreaBozzo/dataprof/blob/master/docs/architecture/crate-redesign.md)
for crate ownership details.
