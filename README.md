# dataprof

[![CI](https://github.com/AndreaBozzo/dataprof/workflows/CI/badge.svg)](https://github.com/AndreaBozzo/dataprof/actions)
[![License](https://img.shields.io/github/license/AndreaBozzo/dataprof)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.70%2B-orange.svg)](https://www.rust-lang.org)
[![Crates.io](https://img.shields.io/crates/v/dataprof.svg)](https://crates.io/crates/dataprof)

**DISCLAIMER FOR HUMAN READERS**

dataprof, even if working, is in early-stage development, therefore you might encounter bugs, minor or even major ones during your data-quality exploration journey.

Report them appropriately by opening an issue or by mailing the maintainer for security issues.

Thanks for your time here!

High-performance data quality and ML readiness assessment library built in Rust. Delivers 20x better memory efficiency than pandas with unlimited file streaming, 30+ automated quality checks, and comprehensive ML readiness assessment. Full Python bindings and production database connectivity included.

## Quick Start

### Python
```bash
pip install dataprof
```

```python
import dataprof

# ML readiness assessment
ml_score = dataprof.ml_readiness_score("data.csv")
print(f"ML Readiness: {ml_score.readiness_level} ({ml_score.overall_score:.1f}%)")

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
- [Database Integration](docs/python/INTEGRATIONS.md) - Production database connectivity
- [Performance Benchmarks](docs/project/benchmarking.md) - Benchmark results and methodology

## License

Licensed under GPL-3.0. See [LICENSE](LICENSE) for details.
