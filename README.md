# DataProfiler CLI

[![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
[![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

A fast, lightweight CLI tool for CSV and JSON data profiling and quality analysis written in Rust.

![DataProfiler HTML Report](assets/animations/HTML.gif)
*Interactive HTML reports with professional data quality insights*

## ✨ Features

- **⚡ Fast Analysis**: Lightning-fast profiling of CSV, JSON, and JSONL files
- **🔍 Smart Detection**: Auto-detects data types, patterns (emails, phones), and quality issues
- **📊 Quality Insights**: Finds null values, duplicates, outliers, and format inconsistencies  
- **📈 Scales Up**: Handles large files (GB+) with intelligent sampling
- **🎨 Beautiful Output**: Colored terminal display and professional HTML reports

## Quick Start

### CLI Tool
```bash
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof
cargo build --release

# Analyze your data
./target/release/dataprof data.csv
./target/release/dataprof --quality --html report.html data.csv
```

### Rust Library
Add to your `Cargo.toml`:
```toml
[dependencies]
dataprof = { git = "https://github.com/AndreaBozzo/dataprof.git" }
```

```rust
use dataprof::{analyze_csv, analyze_json};
use std::path::Path;

let profiles = analyze_csv(Path::new("data.csv"))?;
for profile in profiles {
    println!("{}: {:?}", profile.name, profile.data_type);
}
```

## Usage

```bash
# Basic analysis
dataprof data.csv

# Quality checking with issues detection  
dataprof --quality data.csv

# Generate HTML report
dataprof --quality --html report.html data.csv

# Help
dataprof --help
```

## Example Output

### Basic Analysis
```
📊 DataProfiler - Analyzing CSV...

Column: email
  Type: String
  Records: 10
  Nulls: 2 (20.0%)
  Min Length: 6
  Max Length: 21
  Avg Length: 18.6
  Patterns:
    Email - 6 matches (75.0%)

Column: amount
  Type: Float
  Records: 10
  Nulls: 1 (10.0%)
  Min: 0.00
  Max: 999999.99
  Mean: 111196.30
```

### Quality Analysis
```
📊 DataProfiler - Analyzing CSV...

📁 sales_data_problematic.csv | 0.0 MB | 9 columns

⚠️  QUALITY ISSUES FOUND: (15)

1. 🔴 CRITICAL [email]: 2 null values (20.0%)
2. 🔴 CRITICAL [order_date]: Mixed date formats
     - DD-MM-YYYY: 1 rows
     - YYYY/MM/DD: 1 rows
     - YYYY-MM-DD: 5 rows
     - DD/MM/YYYY: 2 rows
3. 🟡 WARNING [phone]: 1 null values (10.0%)
4. 🟡 WARNING [amount]: 1 duplicate values

📊 Summary: 2 critical 13 warnings
```

## Supported Formats

- **CSV**: Comma-separated values with auto-delimiter detection
- **JSON**: JSON arrays with object records
- **JSONL**: Line-delimited JSON (one object per line)

## Performance

- **Small files** (<10MB): Analysis in milliseconds
- **Large files** (100MB+): Smart sampling maintains accuracy
- **Example**: 115MB file analyzed in 2.9s with 99.6% accuracy

## Development

Requirements: Rust 1.70+

```bash
cargo build    # Build
cargo test     # Test
cargo fmt      # Format
cargo clippy   # Lint
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.