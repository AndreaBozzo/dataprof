# DataProfiler CLI

[![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
[![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)

A fast, lightweight CLI tool for CSV data profiling and analysis written in Rust.

## Features

- **Fast CSV Analysis**: Quick statistical analysis of CSV files
- **Type Detection**: Automatically detects String, Integer, Float, and Date columns  
- **Pattern Recognition**: Identifies common patterns like emails, phone numbers, URLs
- **Missing Data Analysis**: Reports null values and percentages
- **Colored Terminal Output**: Easy-to-read results with syntax highlighting

## Installation

### From Source
```bash
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof
cargo build --release
```

### Usage
```bash
# Analyze a CSV file
./target/release/dataprof data.csv

# Or with cargo
cargo run -- data.csv
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

## Roadmap

### Phase 1 (Completed ✅)
- [x] CSV parsing with type inference
- [x] Basic statistics (min, max, mean)
- [x] Pattern detection for emails, phones, dates
- [x] Terminal output with colors
- [x] Missing data analysis

### Phase 2 (Next)
- [ ] JSON/JSONL support
- [ ] HTML report generation  
- [ ] Data quality scoring
- [ ] Performance optimizations for large files

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.