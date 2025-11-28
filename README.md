<div align="center">
  <img src="assets/images/logo.png" alt="dataprof logo" width="400" height="auto" />
  <h1>dataprof</h1>
  <p>
    <strong>Fast, reliable data quality assessment for CSV, Parquet, and databases</strong>
  </p>

  [![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
  [![Crates.io](https://img.shields.io/crates/v/dataprof.svg)](https://crates.io/crates/dataprof)
  [![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
  [![PyPI Downloads](https://static.pepy.tech/personalized-badge/dataprof?period=total&units=INTERNATIONAL_SYSTEM&left_color=BLACK&right_color=GREEN&left_text=downloads)](https://pepy.tech/projects/dataprof)
</div>

<br />

**20x faster** than pandas with **unlimited streaming** for large files. ISO 8000/25012 compliant quality metrics, automatic pattern detection (emails, IPs, IBANs, etc.), and comprehensive statistics (mean, median, skewness, kurtosis). Available as CLI, Rust library, or Python package.

**🔒 Privacy First:** 100% local processing, no telemetry, read-only DB access. [See what dataprof analyzes →](docs/WHAT_DATAPROF_DOES.md)

## Quick Start

### CLI Installation

```bash
# Install from crates.io
cargo install dataprof

# Or use Python
pip install dataprof
```

### CLI Usage

```bash
# Analyze a file
dataprof-cli analyze data.csv

# Generate HTML report
dataprof-cli report data.csv -o report.html

# Batch process directories
dataprof-cli batch /data/folder --recursive --parallel

# Database profiling
dataprof-cli database postgres://user:pass@host/db --table users
```

**More options:** `dataprof-cli --help` | [Full CLI Guide](docs/guides/CLI_USAGE_GUIDE.md)

### Python API

```python
import dataprof

# Quality analysis (ISO 8000/25012 compliant)
report = dataprof.analyze_csv_with_quality("data.csv")
print(f"Quality score: {report.quality_score():.1f}%")

# Batch processing
result = dataprof.batch_analyze_directory("/data", recursive=True)

# Async database profiling
async def profile_db():
    result = await dataprof.profile_database_async(
        "postgresql://user:pass@localhost/db",
        "SELECT * FROM users",
        batch_size=1000,
        calculate_quality=True
    )
    return result
```

**[Python Documentation](docs/python/README.md)** | **[Integrations](docs/python/INTEGRATIONS.md)** (Pandas, scikit-learn, Jupyter, Airflow, dbt)

### Rust Library

```rust
use dataprof::*;

// Adaptive profiling (recommended)
let profiler = DataProfiler::auto();
let report = profiler.analyze_file("dataset.csv")?;

// Arrow for large files (>100MB, requires --features arrow)
let profiler = DataProfiler::columnar();
let report = profiler.analyze_csv_file("large_dataset.csv")?;
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

**[Development Guide](docs/DEVELOPMENT.md)** | **[Performance Guide](docs/guides/performance-guide.md)**

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
[CLI Reference](docs/guides/CLI_USAGE_GUIDE.md) | [Python API](docs/python/API_REFERENCE.md) | [Python Integrations](docs/python/INTEGRATIONS.md) | [Database Connectors](docs/guides/database-connectors.md) | [Apache Arrow](docs/guides/apache-arrow-integration.md)

**Developer:**
[Development Guide](docs/DEVELOPMENT.md) | [Performance Guide](docs/guides/performance-guide.md) | [Benchmarks](docs/project/benchmarking.md)

**Privacy:**
[What DataProf Does](docs/WHAT_DATAPROF_DOES.md) - Complete transparency with source verification

## License

MIT License - See [LICENSE](LICENSE) for details.
