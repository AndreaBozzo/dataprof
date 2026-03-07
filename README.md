<div align="center">
  <img src="assets/images/logo.png" alt="dataprof logo" width="800" height="auto" />
  <h1>dataprof</h1>
  <p>
    <strong>High-Performance Data Profiler with ISO 8000/25012 Quality Metrics</strong>
  </p>

  [![Crates.io](https://img.shields.io/crates/v/dataprof.svg)](https://crates.io/crates/dataprof)
  [![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
  [![PyPI Downloads](https://static.pepy.tech/personalized-badge/dataprof?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/dataprof)

  <p>Profile datasets larger than your RAM in seconds on your laptop with Rust and SIMD.</p>
</div>

## 💡 Key Features

- **Blazing Fast & Low Memory**: Written in Rust, using optional SIMD acceleration and streaming to process huge files with `< 100MB` memory usage.
- **Smart Adaptive Engines**: Automatically chooses between Batch and Streaming engines based on your system capabilities and data size.
- **ISO 8000/25012 Metrics**: Robust data quality scoring alongside type distributions and value frequency.
- **PII & Semantic Detection**: Automatically identifies Emails, IPs, IBANs, Credit Cards, and more.
- **Connects Everywhere**: CSV, Parquet, JSON/L, and SQL Databases (Postgres, MySQL, SQLite).
- **Privacy First**: Local execution; data never leaves your machine.

## 🚀 Quick Start

**Python (via pip)**
```bash
pip install dataprof
```
```python
import dataprof

# Connect, profile, and export an HTML dashboard
dataprof.profile("huge_dataset.csv").save("report.html")
```

**CLI (via cargo - Advanced/Rust users)**
```bash
cargo install dataprof

# Profile directly from your terminal
dataprof-cli report huge_data.parquet -o report.html
```

## 📊 Visual Reports

`dataprof` generates rich, interactive HTML reports for single files or batches, visualizing data quality distributions, anomalies, and insights.

<details>
<summary>Click to view Report Dashboards</summary>

<br/>

**Interactive Demo**  
<img src="assets/animations/dataprof_demo_minimal.gif" alt="DataProf Demo" width="100%" />

**Single File Analysis**  
<img src="assets/images/dataprofhtml2026.png" alt="Single Report Dashboard" width="100%" />

**Batch Processing Dashboard**  
<img src="assets/images/dataprofbatch2026.png" alt="Batch Dashboard" width="100%" />

</details>

## 📚 Documentation

- [Python API Reference](docs/python/README.md)
- [CLI Reference Guide](docs/guides/CLI_USAGE_GUIDE.md)
- [Database Connectors](docs/guides/database-connectors.md)

## 🛠️ Advanced Usage (Python)

**Batch Processing**
```python
# Process an entire folder natively in parallel
result = dataprof.batch_analyze_directory("/data_folder", recursive=True)
print(f"Processed at {result.files_per_second:.1f} files/sec")
```

**Database Integration (Async)**
```python
# Profile a SQL query directly
await dataprof.analyze_database_async(
    "postgresql://user:pass@localhost/db",
    "SELECT * FROM sales_data_2024"
)
```

## 💻 Development & Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) and our [Code of Conduct](CODE_OF_CONDUCT.md).

```bash
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof

# Default build (CSV/JSON only)
cargo build --release

# With extensive formats and DB functionality
cargo build --release --features "arrow parquet postgres mysql sqlite"

# To develop the Python API locally
maturin develop --features "python-async database postgres"
```

## ⚖️ License

Dual-licensed under either the [MIT License](LICENSE) or the [Apache License, Version 2.0](LICENSE-APACHE).
