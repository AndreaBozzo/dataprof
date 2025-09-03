# DataProfiler CLI

[![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
[![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

A fast, lightweight CLI tool for CSV data profiling and quality analysis written in Rust.

## ✨ Features

- **⚡ Fast CSV Analysis**: Quick statistical analysis of CSV files with smart sampling
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

## Installation

### From Source
```bash
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof
cargo build --release
```

## 🚀 Usage

### Basic Analysis
```bash
# Quick column analysis (backwards compatible)
./target/release/dataprof data.csv
```

### Quality Checking Mode
```bash
# Advanced analysis with quality issues detection
./target/release/dataprof --quality data.csv
./target/release/dataprof -q data.csv
```

### Help
```bash
./target/release/dataprof --help
```

## 📊 Example Output

### Basic Mode
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

### Quality Mode
```
📊 DataProfiler - Analyzing CSV...

📁 data.csv | 2.1 MB | 8 columns

⚠️  QUALITY ISSUES FOUND: (3)

1. 🔴 CRITICAL [email]: 15 null values (15.0%)
2. 🔴 CRITICAL [order_date]: Mixed date formats
     - YYYY-MM-DD: 45 rows
     - DD/MM/YYYY: 23 rows  
     - MM-DD-YYYY: 12 rows
3. 🟡 WARNING [amount]: 2 outliers detected (>3σ)
     - Row 23: 999999.99
     - Row 67: -5000.00

📊 Summary: 2 critical 1 warnings

[Column details follow...]
```

## Example Output

```
📊 DataProfiler - Analyzing CSV...

Column: email
  Type: String
  Records: 1000
  Nulls: 5 (0.5%)
  Min Length: 12
  Max Length: 35
  Avg Length: 23.4
  Patterns:
    Email - 995 matches (99.5%)

Column: age  
  Type: Integer
  Records: 1000
  Nulls: 0
  Min: 18.00
  Max: 65.00
  Mean: 42.35

Column: signup_date
  Type: Date
  Records: 1000
  Nulls: 2 (0.2%)
  Min Length: 10
  Max Length: 10
  Avg Length: 10.0
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

### Phase 2 (Next)
- [ ] JSON/JSONL support
- [ ] HTML report generation  
- [ ] Data quality scoring with recommendations
- [ ] Advanced pattern detection (Italian fiscal codes, VAT numbers)
- [ ] Custom pattern definitions
- [ ] Export to different formats (JSON, Parquet)

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.