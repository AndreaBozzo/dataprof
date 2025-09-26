# dataprof

[![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
[![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/crates/v/dataprof.svg)](https://crates.io/crates/dataprof)
[![ML Ready](https://csv-mlready-3hvb2ny6j-andrea-bozzos-projects.vercel.app/api/badge?score=93&label=Try%20Online)](https://csv-mlready-3hvb2ny6j-andrea-bozzos-projects.vercel.app)

**DISCLAIMER FOR HUMAN READERS**

dataprof, even if working, is in early-stage development, therefore you might encounter bugs, minor or even major ones during your data-quality exploration journey.

Report them appropriately by opening an issue or by mailing the maintainer for security issues.

Thanks for your time here!

High-performance data quality and ML readiness assessment library built in Rust. Delivers 20x better memory efficiency than pandas with unlimited file streaming, 30+ automated quality checks, and comprehensive ML readiness assessment. **NEW in v0.4.6: Generates ready-to-use Python code snippets** for each ML recommendation. Full Python bindings and production database connectivity included.

## 🌐 Try Online

**No installation required!** Test dataprof instantly with our web interface:

**[📊 CSV ML Readiness API →](https://csv-mlready-3hvb2ny6j-andrea-bozzos-projects.vercel.app)**

- Drag & drop your CSV (up to 50MB)
- Get ML readiness score in ~10 seconds
- Powered by dataprof v0.4.6 core engine
- Embeddable badges for your README

## Quick Start

### Python
```bash
pip install dataprof
```

```python
import dataprof

# ML readiness assessment with actionable code snippets
ml_score = dataprof.ml_readiness_score("data.csv")
print(f"ML Readiness: {ml_score.readiness_level} ({ml_score.overall_score:.1f}%)")

# NEW: Get ready-to-use preprocessing code
for rec in ml_score.recommendations:
    if rec.code_snippet:
        print(f"📦 {rec.framework} code for {rec.category}")
        print(rec.code_snippet)

# Quality analysis with detailed reporting
report = dataprof.analyze_csv_with_quality("data.csv")
print(f"Quality score: {report.quality_score():.1f}%")

# Production database profiling
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

### CLI with Code Generation
```bash
# Generate ML readiness report with actionable code snippets
dataprof data.csv --ml-score --ml-code

# Generate complete Python preprocessing script
dataprof data.csv --ml-score --output-script preprocess.py
```

## Development

### Prerequisites
- Rust (latest stable via [rustup](https://rustup.rs/))
- Docker (for database testing)

### Setup
```bash
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof
cargo build --release  # Build project
docker-compose -f .devcontainer/docker-compose.yml up -d  # Start databases
```

### Common Tasks
```bash
cargo test          # Run all tests
cargo bench         # Performance benchmarks
cargo fmt           # Format code
cargo clippy        # Code quality checks
```

## Documentation

- [Development Guide](docs/DEVELOPMENT.md) - Complete setup and contribution guide
- [Python API Reference](docs/python/API_REFERENCE.md) - Full Python API documentation
- [ML Features](docs/python/ML_FEATURES.md) - Machine learning readiness assessment
- [Python Integrations](docs/python/INTEGRATIONS.md) - Pandas, scikit-learn, Jupyter, Airflow, dbt
- [Database Connectors](docs/guides/database-connectors.md) - Production database connectivity
- [Performance Guide](docs/guides/performance-guide.md) - Optimization and benchmarking
- [Apache Arrow Integration](docs/guides/apache-arrow-integration.md) - Columnar processing guide
- [CLI Usage Guide](docs/guides/CLI_USAGE_GUIDE.md) - Complete CLI reference
- [Performance Benchmarks](docs/project/benchmarking.md) - Benchmark results and methodology

## License

Licensed under GPL-3.0. See [LICENSE](LICENSE) for details.
