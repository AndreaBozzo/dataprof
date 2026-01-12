<div align="center">
  <img src="assets/images/logo.png" alt="dataprof logo" width="400" height="auto" />
  <h1>dataprof</h1>
  <p>
    <strong>The High-Performance Profiler for Large Datasets</strong>
  </p>

  [![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
  [![codecov](https://codecov.io/gh/AndreaBozzo/dataprof/branch/master/graph/badge.svg)](https://codecov.io/gh/AndreaBozzo/dataprof)
  [![Crates.io](https://img.shields.io/crates/v/dataprof.svg)](https://crates.io/crates/dataprof)
   [![License: MIT OR Apache-2.0](https://img.shields.io/badge/license-MIT%20OR%20Apache--2.0-blue.svg)](LICENSE)
  [![PyPI Downloads](https://static.pepy.tech/personalized-badge/dataprof?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/dataprof)
</div>

<br />

> **Profile 50GB datasets in seconds on your laptop.**

DataProf is built for Data Scientists and Engineers who need to understand their data *fast*. No more `MemoryError` when trying to profile a CSV larger than your RAM.

**Pandas-Profiling vs DataProf on a 10GB CSV:**
| Feature | Pandas-Profiling / YData | DataProf |
|---------|--------------------------|----------|
| **Memory Usage** | 12GB+ (Crashes) | **< 100MB (Streaming)** |
| **Speed** | 15+ minutes | **45 seconds** |
| **Implementation** | Python (Slow) | **Rust (Blazing Fast)** |

**🔒 Privacy First:** 100% local processing, no telemetry. [See what dataprof analyzes →](docs/WHAT_DATAPROF_DOES.md)

## 🚀 Quick Start

### Installation

The easiest way to get started is via pip:

```bash
pip install dataprof
```

### Python Usage

Forget complex configurations. Just point to your file:

```python
import dataprof

# Analyze a huge file without crashing memory
# Generates a report.html with quality metrics and distributions
dataprof.profile("huge_dataset.csv").save("report.html")
```

### CLI & Rust Usage (Advanced)

If you prefer the command line or are a Rust developer:

```bash
# Install via cargo
cargo install dataprof

# Generate report from CLI
dataprof-cli report huge_data.csv -o report.html
```

**More options:** `dataprof-cli --help` | [Full CLI Guide](docs/guides/CLI_USAGE_GUIDE.md)

## 💡 Key Features

- **No Size Limits**: Profiles files larger than RAM using streaming and memory mapping.
- **Blazing Fast**: Written in Rust with SIMD acceleration.
- **Privacy Guaranteed**: Data never leaves your machine.
- **Format Support**: CSV, Parquet, JSON/L, and Databases (Postgres, MySQL, etc.).
- **Smart Detection**: Automatically identifies Emails, IPs, IBANs, Credit Cards, and more.

## 📊 Beautiful Reports

<div align="center">
  <p><strong>Interactive Demo</strong><br/>
  <em>Animated walkthrough of dataprof features and dashboards</em></p>
  <img src="assets/animations/dataprof_demo_minimal.gif" alt="DataProf Demo" width="100%" />
  
  <br/><br/>
  
  <p><strong>Single File Analysis</strong><br/>
  <em>Interactive dashboards with quality scoring and distributions</em></p>
  <img src="assets/images/dataprofhtml2026.png" alt="Single Report Dashboard" width="100%" />
  
  <br/><br/>
  
  <p><strong>Batch Processing Dashboard</strong><br/>
  <em>Aggregate metrics from hundreds of files in one view</em></p>
  <img src="assets/images/dataprofbatch2026.png" alt="Batch Dashboard" width="100%" />
</div>

## Documentation

- **[Python API Reference](docs/python/README.md)**
- **[CLI Guide](docs/guides/CLI_USAGE_GUIDE.md)**
- **[Performance & Benchmarks](docs/guides/performance-guide.md)**

### Advanced Examples

**Batch Processing (Python)**
```python
# Process a whole directory of files in parallel
result = dataprof.batch_analyze_directory("/data_folder", recursive=True)
print(f"Processed {result.processed_files} files at {result.files_per_second:.1f} files/sec")
```

**Database Integration (Python)**
```python
# Profile a SQL query directly
await dataprof.profile_database_async(
    "postgresql://user:pass@localhost/db",
    "SELECT * FROM sales_data_2024"
)
```

**Rust Library Usage**
```rust
use dataprof::*;

let profiler = DataProfiler::auto();
let report = profiler.analyze_file("dataset.csv")?;
println!("Quality Score: {}", report.quality_score());
```

## Development

```bash
# Setup
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof
cargo build --release

# Test databases (optional)
docker-compose -f .devcontainer/docker-compose.yml up -d

# Common tasks
cargo test          # Run tests
cargo bench         # Benchmarks
cargo clippy        # Linting
```

**[Performance Guide](docs/guides/performance-guide.md)**

### Feature Flags

```bash
# Minimal (CSV/JSON only)
cargo build --release

# With Apache Arrow (large files >100MB)
cargo build --release --features arrow

# With Parquet support
cargo build --release --features parquet

# With databases
cargo build --release --features postgres,mysql,sqlite

# Python async support
maturin develop --features python-async,database,postgres

# All features
cargo build --release --all-features
```

**When to use Arrow:** Large files (>100MB), many columns (>20), uniform types
**When to use Parquet:** Analytics, data lakes, Spark/Pandas integration


## Documentation

**User Guides:**
[CLI Reference](docs/guides/CLI_USAGE_GUIDE.md) | [Python API](docs/python/API_REFERENCE.md) | [Python Integrations](docs/python/INTEGRATIONS.md) | [Database Connectors](docs/guides/database-connectors.md) | [Apache Arrow](docs/guides/apache-arrow-integration.md) | [Performance Guide](docs/guides/performance-guide.md)

**Privacy:**
[What DataProf Does](docs/WHAT_DATAPROF_DOES.md) - Complete transparency with source verification

## 🤝 Contributing

We welcome contributions from everyone! Whether you want to:
- **Fix a bug** 🐛
- **Add a feature** ✨
- **Improve documentation** 📚
- **Report an issue** 📝

### Quick Start for Contributors

1. **Fork & clone:**
   ```bash
   git clone https://github.com/YOUR-USERNAME/dataprof.git
   cd dataprof
   ```

2. **Build & test:**
   ```bash
   cargo build
   cargo test
   ```

3. **Create a feature branch:**
   ```bash
   git checkout -b feature/your-feature-name
   ```

4. **Before submitting PR:**
   ```bash
   cargo fmt --all
   cargo clippy --all --all-targets
   cargo test --all
   ```

5. **Submit a Pull Request** with clear description


All contributions are welcome. Please read [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines and our [Code of Conduct](CODE_OF_CONDUCT.md).

## License

Dual-licensed under either:
- [MIT License](LICENSE)
- [Apache License, Version 2.0](LICENSE-APACHE)

You may use this project under the terms of either license.

