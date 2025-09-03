# DataProfiler CLI

[![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
[![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

A fast, lightweight CLI tool for CSV and JSON data profiling and quality analysis written in Rust.

![DataProfiler HTML Report](assets/animations/HTML.gif)

## ✨ Features

- **⚡ Fast Data Analysis**: Quick statistical analysis of CSV, JSON, and JSONL files with smart sampling
- **🔍 Type Detection**: Automatically detects String, Integer, Float, and Date columns  
- **🎯 Pattern Recognition**: Identifies common patterns like emails, phone numbers (IT/US)
- **📊 Quality Checking**: Advanced data quality issues detection
  - Mixed date formats detection
  - Null values analysis
  - Duplicate detection  
  - Statistical outliers (3-sigma rule)
- **📈 Smart Sampling**: Efficiently handles large files (MB to GB) with intelligent sampling
- **🎨 Colored Terminal Output**: Beautiful, easy-to-read results with syntax highlighting
- **⚠️ Issue Severity Classification**: Critical, Warning, and Info level issues
- **📄 HTML Reports**: Generate professional web reports with interactive visualizations

## Installation

### CLI Tool
```bash
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof
cargo build --release
```

### Rust Library
Add to your `Cargo.toml`:
```toml
[dependencies]
dataprof = { git = "https://github.com/AndreaBozzo/dataprof.git" }
```

```rust
use dataprof::{analyze_csv, analyze_json, DataType};
use std::path::Path;

let profiles = analyze_csv(Path::new("data.csv"))?;
for profile in profiles {
    println!("{}: {:?}", profile.name, profile.data_type);
}
```

## 🚀 Usage

### Basic Analysis
```bash
# Quick column analysis (backwards compatible)
./target/release/dataprof data.csv
./target/release/dataprof users.json
./target/release/dataprof logs.jsonl
```

### Quality Checking Mode
```bash
# Advanced analysis with quality issues detection
./target/release/dataprof --quality data.csv
./target/release/dataprof -q users.json
./target/release/dataprof --quality logs.jsonl
```

### HTML Report Generation
```bash
# Generate beautiful HTML reports (requires --quality)
./target/release/dataprof --quality --html report.html data.csv
./target/release/dataprof -q --html users_report.html users.json
```

### Help
```bash
./target/release/dataprof --help
```

## 🎯 Key Features Demo

### Terminal Output
- **Real-time analysis** with colored, structured output
- **Progress indicators** for large files with sampling information  
- **Quality issues** highlighted with severity icons (🔴 Critical, 🟡 Warning, 🔵 Info)
- **Pattern detection** for emails, phone numbers, UUIDs, and more

### HTML Reports
- **Professional web interface** with responsive design
- **Interactive quality dashboard** with issue breakdown
- **Column-by-column analysis** with statistics and patterns
- **Export and share** reports easily

## 📊 Example Output

### Basic Mode - CSV
```
📊 DataProfiler - Analyzing CSV...

Column: email
  Type: String
  Records: 100
  Nulls: 0
  Min Length: 10
  Max Length: 25
  Avg Length: 18.5
  Patterns:
    Email - 98 matches (98.0%)

Column: age  
  Type: Integer
  Records: 100
  Nulls: 2 (2.0%)
  Min: 18.00
  Max: 65.00
  Mean: 35.20
```

### Basic Mode - JSON/JSONL
```
📊 DataProfiler - Analyzing CSV...

Column: timestamp
  Type: String
  Records: 5
  Nulls: 0
  Min Length: 19
  Max Length: 20
  Avg Length: 19.8

Column: level
  Type: String  
  Records: 5
  Nulls: 0
  Min Length: 4
  Max Length: 5
  Avg Length: 4.6
```

### Quality Mode
```
📊 DataProfiler - Analyzing CSV...

📁 users.json | 0.0 MB | 8 columns

⚠️  QUALITY ISSUES FOUND: (4)

1. 🔴 CRITICAL [email]: 1 null values (25.0%)
2. 🔴 CRITICAL [signup_date]: Mixed date formats
     - YYYY-MM-DD: 3 rows
     - DD/MM/YYYY: 1 rows
3. 🔴 CRITICAL [salary]: 1 outliers detected (>3σ)
     - Row 4: 999999.99
4. 🟡 WARNING [age]: 1 null values (25.0%)

📊 Summary: 3 critical 1 warnings

[Column details follow...]
```

## 🗂️ Supported File Formats

- **CSV files**: Standard comma-separated values with automatic delimiter detection
- **JSON files**: JSON arrays with object records  
- **JSONL files**: Line-delimited JSON (one JSON object per line)

The tool automatically detects the file format based on the file extension (`.csv`, `.json`, `.jsonl`).

## 🚀 Performance

DataProfiler is optimized for speed and memory efficiency:

- **Small files** (<10MB): Complete analysis in milliseconds
- **Medium files** (10-100MB): Smart sampling with <2s processing 
- **Large files** (100MB+): Intelligent sampling maintaining accuracy
  - 115MB file (1M rows): **2.9s** with 0.4% sampling
  - Quality issues detection remains highly accurate

### Benchmark Results
```
File Size: 115MB (1,000,000 rows, 11 columns)
Basic Mode:    8.08s (complete analysis)
Quality Mode:  2.90s (smart sampling: 4,330 rows analyzed)
Accuracy:      12 quality issues detected with perfect precision
```

## Supported Patterns

- **Email addresses** (RFC compliant)
- **Phone numbers** (US and Italian formats)
- **URLs** (HTTP/HTTPS)
- **UUIDs**
- **Date formats** (YYYY-MM-DD, DD/MM/YYYY, DD-MM-YYYY)

## Development

### Prerequisites
- Rust 1.70 or later
- Cargo

### Build
```bash
cargo build
```

### Test  
```bash
cargo test
```

### Format
```bash
cargo fmt
```

### Lint
```bash
cargo clippy
```

## 🛣️ Roadmap

### Phase 1 (Completed ✅)
- [x] CSV parsing with type inference
- [x] Basic statistics (min, max, mean)
- [x] Pattern detection for emails, phones, dates
- [x] Terminal output with colors
- [x] Missing data analysis
- [x] **Quality issues detection**
- [x] **Smart sampling for large files**
- [x] **Mixed date formats detection**
- [x] **Outlier detection with 3-sigma rule**
- [x] **Duplicate detection**
- [x] **Issue severity classification**

### Phase 2 (Completed ✅)
- [x] JSON/JSONL support
- [x] HTML report generation

### Phase 3 (Next)
- [ ] Data quality scoring with recommendations
- [ ] Advanced pattern detection (Italian fiscal codes, VAT numbers)
- [ ] Custom pattern definitions
- [ ] Export to different formats (JSON, Parquet)

## 🧪 Testing

DataProfiler includes comprehensive test coverage:

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test --test integration_tests
cargo test unit_tests
```

**Test Coverage:**
- 14 integration tests covering all major features
- Unit tests for core algorithms
- Performance benchmarks for large files
- Error handling and edge cases

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

**Development Guidelines:**
- Follow "start simple, then iterate" principle
- Best practices always
- Comprehensive test coverage for new features
- Performance considerations for large datasets

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.