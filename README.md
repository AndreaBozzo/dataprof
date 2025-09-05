# DataProfiler 📊 

[![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
[![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

**Fast CSV data profiler with quality checking - v0.3.0 Streaming Edition**

⚡ **10x faster** with SIMD acceleration • 🌊 **Streams** files of any size • 🔍 **Detects** quality issues automatically

![DataProfiler HTML Report](assets/animations/HTML.gif)

## 🚀 Quick Start

```bash
# Install
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof && cargo build --release

# Basic analysis
./target/release/dataprof data.csv

# Quality checking
./target/release/dataprof data.csv --quality

# Large files (streaming)
./target/release/dataprof huge_file.csv --streaming --progress

# Generate HTML report  
./target/release/dataprof data.csv --quality --html report.html
```

## 📊 Example Output

### Basic Analysis
```
📊 DataProfiler - Standard Analysis

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

### Quality Issues Detection
```
📊 DataProfiler - Standard Analysis

📁 sales_data_problematic.csv | 0.0 MB | 9 columns

⚠️  QUALITY ISSUES FOUND: (15)

1. 🔴 CRITICAL [email]: 2 null values (20.0%)
2. 🔴 CRITICAL [order_date]: Mixed date formats
     - DD/MM/YYYY: 2 rows
     - YYYY-MM-DD: 5 rows  
     - YYYY/MM/DD: 1 rows
     - DD-MM-YYYY: 1 rows
3. 🟡 WARNING [phone]: 1 null values (10.0%)
4. 🟡 WARNING [amount]: 1 duplicate values

📊 Summary: 2 critical 13 warnings
```

## ⚡ Features

### v0.3.0 New Features
- **🚀 SIMD Acceleration**: 10x speedup on numeric computations
- **🌊 Streaming Processing**: Handle files larger than available RAM  
- **💾 Memory Efficient**: Adaptive engines for optimal performance

### Core Features
- **🔍 Fast Analysis**: Lightning-fast profiling of CSV, JSON, and JSONL files
- **🧠 Smart Detection**: Auto-detects data types, patterns (emails, phones), and quality issues
- **⚠️ Quality Insights**: Finds null values, duplicates, outliers, and format inconsistencies
- **📊 Scales Up**: Handles large files (GB+) with intelligent sampling
- **📄 HTML Reports**: Professional documentation with interactive charts
- **🎨 Beautiful Output**: Colored terminal display and professional formatting

## 📋 All Options

```bash
Fast CSV data profiler with quality checking - v0.3.0 Streaming Edition

Usage: dataprof [OPTIONS] <FILE>

Arguments:
  <FILE>  CSV file to analyze

Options:
  -q, --quality                  Enable quality checking (shows data issues)
      --html <HTML>              Generate HTML report (requires --quality)
      --streaming                Use streaming engine for large files (v0.3.0)
      --progress                 Show progress during processing (requires --streaming)
      --chunk-size <CHUNK_SIZE>  Override chunk size for streaming (default: adaptive)
      --sample <SAMPLE>          Enable sampling for very large datasets
  -h, --help                     Print help
```

## 🛠️ As a Library

Add to your `Cargo.toml`:
```toml
[dependencies]
dataprof = { git = "https://github.com/AndreaBozzo/dataprof.git" }
```

```rust
use dataprof::analyze_csv;

let profiles = analyze_csv("data.csv")?;
for profile in profiles {
    println!("{}: {:?} ({}% nulls)", 
             profile.name, 
             profile.data_type,
             profile.null_count as f32 / profile.total_count as f32 * 100.0);
}
```

## 🎯 Supported Formats

- **CSV**: Comma-separated values with auto-delimiter detection
- **JSON**: JSON arrays with object records
- **JSONL**: Line-delimited JSON (one object per line)

## ⚡ Performance

- **Small files** (<10MB): Analysis in milliseconds
- **Large files** (100MB+): Smart sampling maintains accuracy
- **SIMD optimized**: 10x faster numeric computations on modern CPUs
- **Memory bounded**: Process files larger than available RAM
- **Example**: 115MB file analyzed in 2.9s with 99.6% accuracy

## 🧪 Development

Requirements: Rust 1.70+

```bash
cargo build --release    # Build optimized
cargo test               # Run all tests
cargo test --test v03_comprehensive  # Run v0.3.0 integration tests
cargo fmt                # Format code
cargo clippy             # Lint code
```

## 🤝 Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development guidelines.

## 📄 License

This project is licensed under the GNU General Public License v3.0 - see the [LICENSE](LICENSE) file for details.