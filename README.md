# dataprof

[![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
[![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/crates/v/dataprof.svg)](https://crates.io/crates/dataprof)
[![Try Online](https://img.shields.io/badge/Try%20Online-CSV%20ML%20Readiness-blue?style=flat&logo=vercel)](https://csv-mlready-api.vercel.app)

**DISCLAIMER FOR HUMAN READERS**

dataprof, even if working, is in early-stage development, therefore you might encounter bugs, minor or even major ones during your data-quality exploration journey.

Report them appropriately by opening an issue or by mailing the maintainer for security issues.

Thanks for your time here!

A fast, reliable data quality and ML readiness assessment tool built in Rust. Analyze datasets with 20x better memory efficiency than pandas, unlimited file streaming, and 30+ automated quality checks. **NEW in v0.4.6: Generate ready-to-use Python code snippets** for each ML recommendation. Full Python bindings and production database connectivity included.

Perfect for data scientists, ML engineers, and anyone working with data who needs quick, reliable quality insights.

## Try Online

**No installation required!** Test dataprof instantly with our web interface:

**[CSV ML Readiness API →](https://csv-mlready-api.vercel.app)**

- Drag & drop your CSV (up to 50MB)
- Get ML readiness score in ~10 seconds
- Powered by dataprof v0.4.6 core engine
- Embeddable badges for your README

## CI/CD Integration

Automate data quality checks in your workflows with our GitHub Action:

```yaml
- name: DataProf ML Readiness Check
  uses: AndreaBozzo/dataprof-actions@v1
  with:
    file: 'data/dataset.csv'
    ml-threshold: 80
    fail-on-issues: true
```

**[Get the Action →](https://github.com/AndreaBozzo/dataprof-actions)**

- **Zero setup** - works out of the box
- **Smart analysis** - ML readiness scoring with actionable insights
- **Flexible** - customizable thresholds and output formats
- **Fast** - typically completes in under 2 minutes

Perfect for validating datasets before training, ensuring data quality in pipelines, or generating automated quality reports.

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
cargo add dataprof
```

```rust
use dataprof::*;

// High-performance Arrow processing
let profiler = DataProfiler::columnar();
let report = profiler.analyze_csv_file("large_dataset.csv")?;
```

### CLI Usage
```bash
# Generate ML readiness report with actionable code snippets
dataprof data.csv --quality --ml-score --ml-code

# Generate complete Python preprocessing script
dataprof data.csv --quality --ml-score --output-script preprocess.py

# Quick analysis with streaming for large files
dataprof large_dataset.csv --streaming --sample 10000
```

**Note**: On Windows, the binary is named `dataprof-cli.exe`. Use `cargo build --release` to build from source.

## Development

Want to contribute or build from source? Here's what you need:

### Prerequisites
- Rust (latest stable via [rustup](https://rustup.rs/))
- Docker (for database testing)

### Quick Setup
```bash
git clone https://github.com/AndreaBozzo/dataprof.git
cd dataprof
cargo build --release  # Build the project
docker-compose -f .devcontainer/docker-compose.yml up -d  # Start test databases
```

### Common Development Tasks
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
