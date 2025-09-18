# DataProfiler 📊

[![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
[![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/crates/v/dataprof.svg)](https://crates.io/crates/dataprof)
[![PyPI](https://img.shields.io/pypi/v/dataprof.svg)](https://pypi.org/project/dataprof/)

**High-performance data quality library for production pipelines**

DataProfiler analyzes datasets with 20x better memory efficiency than pandas, handles unlimited file sizes through streaming, and detects 30+ quality issues automatically. Built in Rust with Python bindings and direct database connectivity.

![DataProfiler HTML Report](assets/animations/HTML.gif)

![DataProfiler HTML ML Report](assets/screenshots/MLfeatshtml.png)

## ✨ Key Features

- **⚡ High Performance**: 20x more memory efficient than pandas with Apache Arrow integration
- **🌊 Scalable**: Stream processing for files larger than RAM (tested up to 100GB)
- **🔍 Smart Quality Detection**: Automatically finds nulls, duplicates, outliers, format issues
- **🗃️ Database Connectivity**: Direct profiling of PostgreSQL, MySQL, SQLite, DuckDB
- **🐍 Python & Rust APIs**: Library-first design with comprehensive bindings

## 🚀 Quick Start

### Python
```bash
pip install dataprof
```

```python
import dataprof

# Analyze CSV with quality assessment
report = dataprof.analyze_csv_with_quality("data.csv")
print(f"Quality score: {report.quality_score():.1f}%")

# Profile database table directly
profiles = dataprof.analyze_database("postgresql://user:pass@host/db", "users")
```

### Rust
```bash
cargo add dataprof --features arrow
```

```rust
use dataprof::*;

// High-performance Arrow processing
let profiler = DataProfiler::columnar();
let report = profiler.analyze_csv_file("large_dataset.csv")?;
```

### CLI
```bash
# Basic profiling
dataprof data.csv --quality --html report.html

# Database profiling
dataprof users --database "postgresql://user:pass@host:5432/db" --quality

# Large files with progress
dataprof huge_file.csv --streaming --progress
```

## 📊 Performance

| Tool | 100MB CSV | Memory | Quality Checks | >RAM Support |
|------|-----------|--------|----------------|--------------|
| **DataProfiler (Arrow)** | **0.5s** | 30MB | ✅ 30+ checks | ✅ |
| DataProfiler (Standard) | 2.1s | 45MB | ✅ 30+ checks | ✅ |
| pandas.describe() | 8.4s | 380MB | ❌ Basic stats | ❌ |
| Great Expectations | 12.1s | 290MB | ✅ Rule-based | ❌ |

## 💡 Real-World Examples

**Production Quality Gate**
```python
from dataprof import quick_quality_check

def validate_pipeline_data(file_path):
    quality_score = quick_quality_check(file_path)
    if quality_score < 85.0:
        raise Exception(f"Data quality too low: {quality_score:.1f}%")
    return quality_score
```

**Jupyter Data Exploration**
```python
report = dataprof.analyze_csv_with_quality("experiment_data.csv")

# Quick overview
print(f"📊 Quality: {report.quality_score():.1f}% | Rows: {report.scan_info.rows_scanned:,}")

# Identify issues
for issue in report.issues:
    print(f"⚠️ {issue.severity}: {issue.description}")
```

**Database Monitoring**
```bash
# Monitor daily data loads
dataprof daily_sales --database "mysql://user:pass@prod-db/warehouse" \
  --query "SELECT * FROM sales WHERE date = CURRENT_DATE" \
  --quality --json | jq '.quality_score'
```

## 📖 Documentation

| Guide | Description |
|-------|-------------|
| **[Python Overview](https://github.com/AndreaBozzo/dataprof/wiki/Python-Overview)** | Getting started with Python bindings |
| **[Python API Reference](https://github.com/AndreaBozzo/dataprof/wiki/Python-API-Reference)** | Complete function and class reference |
| **[ML Features Guide](https://github.com/AndreaBozzo/dataprof/wiki/Python-ML-Features)** | ML readiness assessment and recommendations |
| **[Python Integrations](https://github.com/AndreaBozzo/dataprof/wiki/Python-Integrations)** | Pandas, scikit-learn, Jupyter, Airflow integration |
| **[Database Connectors](https://github.com/AndreaBozzo/dataprof/wiki/Database-Connectors)** | PostgreSQL, MySQL, SQLite, DuckDB integration |
| **[Apache Arrow Integration](https://github.com/AndreaBozzo/dataprof/wiki/Apache-Arrow-Integration)** | High-performance columnar processing guide |
| **[Performance Guide](https://github.com/AndreaBozzo/dataprof/wiki/Performance-Guide)** | Benchmarks, optimization tips, and tool selection |
| **[CLI Guide](https://github.com/AndreaBozzo/dataprof/wiki/CLI-Usage-Guide)** | General CLI usage

Additional resources: [CHANGELOG](CHANGELOG.md) • [CONTRIBUTING](CONTRIBUTING.md) • [LICENSE](LICENSE)

## 🛠️ Development

```bash
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof

# Quick setup
bash scripts/setup-dev.sh    # Linux/macOS
pwsh scripts/setup-dev.ps1   # Windows

# Build and test
cargo build --release
cargo test --all
```

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

- 🐛 [Report bugs](https://github.com/AndreaBozzo/dataprof/issues)
- ✨ [Request features](https://github.com/AndreaBozzo/dataprof/issues)
- 📖 [Improve docs](https://github.com/AndreaBozzo/dataprof/wiki)

## 📄 License

Licensed under [GPL-3.0](LICENSE) • Commercial use allowed with source disclosure
