# DataProfiler CLI - Project Documentation

## Overview

Fast data profiling and quality checking tool for large datasets built in Rust with Polars.

**RULES**

- Start simple, then iterate.
- Best practices,always.

## Architecture

### Core Components

- **CLI Interface**: Clap-based with subcommands (check, analyze, diff)
- **Smart Sampling**: Adaptive sampling based on file size for performance
- **Quality Analysis**: Automated detection of data quality issues
- **Terminal Output**: Rich colored output with progress bars

### Project Structure

```
src/
├── main.rs              # Entry point & CLI handling
├── types.rs             # Core data structures & enums
├── analysis/            # Data analysis modules
│   ├── mod.rs
│   ├── analyzer.rs      # Column profiling & pattern detection
│   └── quality.rs       # Quality issue detection
├── input/               # Data input & sampling
│   ├── mod.rs
│   └── sampler.rs       # Smart sampling strategies
└── output/              # Output & reporting
    ├── mod.rs
    └── reporter.rs      # Terminal reporting & progress bars
```

### Key Dependencies

- **polars**: Data processing engine (v0.38)
- **clap**: CLI argument parsing (v4.5)
- **colored**: Terminal styling (v2.1)
- **indicatif**: Progress bars (v0.17)
- **anyhow**: Error handling (v1.0)
- **regex**: Pattern matching (v1.10)

## Commands

### check

Quick quality check with smart sampling

```bash
dataprof check data.csv [--max-rows 1000] [--fast]
```

### analyze

Deep analysis with multiple output formats

```bash
dataprof analyze data.csv [--output terminal|json|html]
```

### diff (Coming Soon)

Compare two datasets

```bash
dataprof diff file1.csv file2.csv
```

## Data Types Detected

- **String**: Text data with pattern detection (email, phone, fiscal codes)
- **Integer/Float**: Numeric data with statistical analysis
- **Date/DateTime**: Temporal data with format detection
- **Boolean**: True/false values

## Quality Issues Detected

- **Null Values**: Missing data detection with percentages
- **Duplicates**: Duplicate value identification
- **Outliers**: Statistical outliers using 3-sigma rule
- **Mixed Date Formats**: Inconsistent date formatting
- **Mixed Types**: Type inconsistencies in columns

## Sampling Strategy

- **Small files (<100MB)**: 10K rows minimum
- **Large files (>10GB)**: 1M rows maximum
- **Logarithmic scaling**: Adaptive between min/max based on file size
- **Stratified sampling**: Future enhancement for balanced sampling

## Performance Features

- **Lazy evaluation**: Polars lazy API for memory efficiency
- **Parallel processing**: Rayon for multi-threading
- **Smart sampling**: Avoid loading entire datasets
- **Fast hashmaps**: AHash for better performance
- **Progress tracking**: Real-time progress indicators

## Output Features

- **Rich terminal output**: Colored, formatted reports
- **Issue prioritization**: Severity-based sorting (High/Medium/Low)
- **Quick fix suggestions**: Automated command suggestions
- **Column summaries**: Top 5 columns with statistics
- **Export recommendations**: DuckDB integration hints

## Build & Test

```bash
cargo build --release    # Optimized build
cargo test               # Run tests
cargo run -- check data.csv
```

## Development Notes

- Uses Rust 2024 edition
- All warnings suppressed with #[allow(dead_code)] for future features
- Modular architecture for easy extension
- Error handling with Result<T> throughout
