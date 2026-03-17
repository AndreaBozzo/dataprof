# CLI Usage Guide

Complete reference for the `dataprof` command-line interface (v0.6.0).

## Installation

```bash
cargo install dataprof                        # default features
cargo install dataprof --features full-cli    # includes Parquet + all databases
```

## Subcommands

### `analyze` -- Full profiling with quality metrics

Runs comprehensive ISO 8000/25012 quality analysis on a file.

```
dataprof analyze <FILE> [OPTIONS]
```

| Option | Description |
|---|---|
| `--detailed` | Show detailed metrics for all quality dimensions |
| `--format <FMT>` | Output format: `text` (default), `json`, `csv`, `plain` |
| `-o, --output <PATH>` | Save output to file |
| `--threshold-profile <PROFILE>` | ISO quality threshold: `default`, `strict`, `lenient` |
| `--sample <SIZE>` | Sample first N rows (useful for large files) |
| `--progress` | Show real-time progress bar |
| `--chunk-size <SIZE>` | Custom chunk size for streaming (e.g. `10000`) |
| `--config <PATH>` | Load TOML configuration file |
| `-v, -vv, -vvv` | Increase verbosity (info, debug, trace) |

**Examples:**

```bash
# Basic analysis
dataprof analyze data.csv

# Detailed quality report
dataprof analyze data.csv --detailed

# JSON output for CI pipelines
dataprof analyze data.csv --format json -o report.json

# Strict quality thresholds
dataprof analyze data.csv --detailed --threshold-profile strict

# Large file with sampling and progress
dataprof analyze huge.csv --sample 100000 --progress --chunk-size 5000
```

**Sample text output:**

```
File: data.csv
Size: 15.3 MB
Columns: 8
Rows: 50000
Scan time: 342 ms

Column Profiles
  customer_id (Integer)
    Records: 50000 | Nulls: 0 (0.0%)
    Min: 1.00 | Max: 10000.00 | Mean: 5000.50
    Std Dev: 2886.90 | Median: 5000.00

  email (String)
    Records: 50000 | Nulls: 234 (0.5%)
    Min Length: 8 | Max Length: 45 | Avg Length: 22.3
    Patterns: email (98.2%)

Quality Assessment (ISO 8000/25012)
  Completeness:  95.2%
  Consistency:   98.7%
  Uniqueness:    99.1%
  Accuracy:      96.5%
  Timeliness:    100.0%
  Overall Score: 97.9%
```

### `schema` -- Fast schema inference

Infers column names and data types by reading a small sample of rows. For Parquet files, reads metadata only (zero row scanning).

```
dataprof schema <FILE> [OPTIONS]
```

| Option | Description |
|---|---|
| `--format <FMT>` | Output format: `text` (default), `json` |
| `-o, --output <PATH>` | Save output to file |

**Examples:**

```bash
dataprof schema data.csv
dataprof schema data.parquet --format json
```

**Sample output:**

```
Schema for data.csv (100 rows sampled, 12ms):
  customer_id   Integer
  name          String
  email         String
  age           Integer
  salary        Float
  signup_date   Date
```

### `count` -- Quick row count

Returns exact counts for small files and Parquet (via metadata), estimates for large files.

```
dataprof count <FILE> [OPTIONS]
```

| Option | Description |
|---|---|
| `--format <FMT>` | Output format: `text` (default), `json` |

**Examples:**

```bash
dataprof count data.csv
dataprof count data.parquet
```

**Sample output:**

```
Row count: 50000 (exact, full scan, 45ms)
```

### `database` -- Database profiling

> Requires compilation with database feature flags (`--features postgres`, `--features mysql`, `--features sqlite`, or `--features all-db`).

Profile data directly from a database connection.

```
dataprof database <CONNECTION_STRING> [OPTIONS]
```

| Option | Description |
|---|---|
| `--table <NAME>` | Table name to profile (generates `SELECT * FROM <table>`) |
| `--query <SQL>` | Custom SQL query to profile |
| `--batch-size <N>` | Streaming batch size (default: `10000`) |
| `--quality` | Compute ISO 8000/25012 quality metrics |

You must provide either `--table` or `--query`.

**Connection string formats:**

```
postgres://user:password@host:5432/database
mysql://user:password@host:3306/database
sqlite:///path/to/database.db
```

**Examples:**

```bash
# Profile a PostgreSQL table
dataprof database postgres://user:pass@localhost/mydb --table users --quality

# Custom query on MySQL
dataprof database mysql://root:pass@localhost/shop --query "SELECT * FROM orders WHERE date > '2024-01-01'"

# SQLite
dataprof database sqlite:///data.db --table events
```

## Output Formats

All subcommands that support `--format` accept these values:

| Format | Description | Use case |
|---|---|---|
| `text` | Human-readable with colors | Terminal, interactive use |
| `json` | Machine-readable JSON | CI/CD, pipelines, APIs |
| `csv` | Comma-separated values | Spreadsheets, data pipelines |
| `plain` | Text without ANSI colors | Piping, file redirection |

The CLI automatically adapts output when it detects a non-interactive context (piped output, file redirection).

## Configuration File

dataprof loads settings from a TOML configuration file. Use `--config <PATH>` to specify one explicitly, or place a `.dataprof.toml` in your working directory.

```toml
[output]
format = "json"
colored = true
show_progress = true
verbosity = 1

[quality]
enabled = true

[engine]
# Engine settings are applied automatically
```

**Auto-discovery paths** (checked in order):

1. `.dataprof.toml` in current directory
2. `.dataprof.toml` in parent directories (up to root)
3. `.dataprof.toml` in home directory

## Supported File Formats

| Format | Extensions | Notes |
|---|---|---|
| CSV | `.csv`, `.tsv`, `.txt` | Auto-detects delimiter (`,` `;` `\|` `\t`) |
| JSON | `.json` | Array of objects |
| JSONL | `.jsonl` | Newline-delimited JSON (one object per line) |
| Parquet | `.parquet` | Schema/count read from metadata without scanning rows |

## Environment Variables

| Variable | Effect |
|---|---|
| `NO_COLOR=1` | Disable colored output |
| `RUST_LOG=dataprof=debug` | Enable debug logging via env_logger |

## Getting Help

```bash
dataprof --help
dataprof analyze --help
dataprof schema --help
dataprof count --help
dataprof database --help    # if compiled with database features
```
