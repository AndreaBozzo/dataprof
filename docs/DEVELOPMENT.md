# DataProfiler Development Guide

A comprehensive guide for setting up and developing with DataProfiler.

## 🚀 Quick Start

```bash
# Clone and setup in one command
git clone https://github.com/username/dataprof.git
cd dataprof
cargo build --release  # Build project
docker-compose -f .devcontainer/docker-compose.yml up -d  # Start databases
```

## 📋 Prerequisites

### Required Tools
- **Rust** (latest stable) - Install via [rustup](https://rustup.rs/)
- **Docker** - For database testing and development containers
- **Git** - Version control

### Optional but Recommended
- **VS Code** - Pre-configured workspace available
- **pre-commit** - Git hooks for code quality
- **cargo-tarpaulin** - Code coverage: `cargo install cargo-tarpaulin`
- **cargo-machete** - Unused dependencies: `cargo install cargo-machete`

## 🏗️ Development Environment Options

### Option 1: Native Development
```bash
# Quick setup
just setup

# Complete setup with databases
just setup-complete
```

### Option 2: VS Code Dev Containers (Recommended)
1. Install [VS Code](https://code.visualstudio.com/) and [Remote-Containers extension](https://marketplace.visualstudio.com/items?itemName=ms-vscode-remote.remote-containers)
2. Open project in VS Code
3. Click "Reopen in Container" when prompted
4. Everything is automatically configured!

### Option 3: Manual Setup
```bash
# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Install components
rustup component add rustfmt clippy
cargo install cargo-tarpaulin cargo-machete

# Setup development tools
pip install pre-commit
pre-commit install
```

## 🔧 Project Architecture

### Core Components
```
src/
├── lib.rs              # Library entry point
├── cli/                # Command-line interface
├── engines/            # Data processing engines
│   ├── streaming.rs    # Streaming processor
│   ├── memory.rs       # Memory-efficient processor
│   ├── arrow.rs        # Apache Arrow integration
│   └── mod.rs
├── connectors/         # Database connectors
├── profiling/          # Data profiling logic
├── quality/            # Data quality analysis
└── utils/              # Shared utilities
```

### Engine Selection Logic
DataProfiler automatically selects the optimal processing engine based on:
- **File size** and available memory
- **Data format** (CSV, Parquet, JSON, etc.)
- **System capabilities** (SIMD, memory bandwidth)
- **Feature requirements** (streaming vs batch)

## 🛠️ Common Development Tasks

### Daily Development Workflow
```bash
# Start working on a feature
git checkout staging && git pull origin staging
git checkout -b feature/my-feature

# Development cycle
cargo fmt && cargo build && cargo test  # format + build + test
cargo fmt && cargo clippy && cargo test  # comprehensive checks

# Commit and push
git add .
git commit -m "feat: implement my feature"
git push origin feature/my-feature
```

### Code Quality Checks
```bash
# Quick checks
cargo fmt         # Format code
cargo clippy      # Run clippy
cargo test        # Run unit tests

# Comprehensive checks
cargo fmt && cargo clippy && cargo test  # All quality checks
cargo test        # All tests including integration
pre-commit run --all-files  # Pre-commit hooks
```

### Database Development
```bash
# Start databases
docker-compose -f .devcontainer/docker-compose.yml up -d postgres mysql redis  # PostgreSQL, MySQL, Redis
docker-compose -f .devcontainer/docker-compose.yml --profile admin up -d       # + admin tools

# Test specific databases
cargo test --features postgres  # PostgreSQL tests
cargo test --features mysql     # MySQL tests
cargo test --features sqlite    # SQLite tests
cargo test --features duckdb    # DuckDB tests

# Connect to databases
docker exec -it dataprof-postgres-dev psql -U dataprof -d dataprof_test  # psql console
docker exec -it dataprof-mysql-dev mysql -u dataprof -pdev_password_123  # mysql console

# Cleanup
docker-compose -f .devcontainer/docker-compose.yml down        # Stop all databases
docker-compose -f .devcontainer/docker-compose.yml down -v     # Reset all data
```

### Performance Analysis
```bash
# Benchmarks
cargo bench                     # Run cargo bench
cargo run -- large_file.csv --benchmark

# Memory profiling (Linux)
cargo build --release && valgrind --tool=massif ./target/release/dataprof-cli examples/large.csv

# Code coverage
cargo tarpaulin --out Html --output-dir coverage  # Generate HTML report
open coverage/tarpaulin-report.html
```

## 🧪 Testing Strategy

### Test Categories
1. **Unit Tests** (`cargo test --lib`) - Fast, isolated component tests
2. **Integration Tests** (`cargo test`) - Feature integration tests
3. **CLI Tests** (`cargo test-cli`) - End-to-end CLI testing
4. **Database Tests** (`cargo test-db`) - Database connector tests
5. **Performance Tests** (`cargo bench`) - Performance regression tests

### Running Tests
```bash
# Quick feedback loop
cargo test                       # Unit tests only

# Comprehensive testing
cargo test-all                   # All tests
cargo test-all-db               # All tests + database setup

# Specific test suites
cargo test-arrow                # Apache Arrow integration
cargo test-security             # Security-focused tests
cargo test engine_selection    # Specific test case
cargo test --features postgres # Feature-specific tests
```

### Test Data
- Standard datasets in `tests/fixtures/standard_datasets/`
- Database fixtures in `.devcontainer/test-data/`
- Large file testing: `just debug-run examples/large.csv`

## 🔍 Debugging

### VS Code Debugging
Pre-configured debug configurations:
- **Debug unit tests** - Debug specific test cases
- **Debug CLI** - Debug CLI with sample data
- **Debug with custom args** - Modify launch.json as needed

### Command Line Debugging
```bash
# Debug build
cargo build
RUST_LOG=debug ./target/debug/dataprof-cli file.csv --quality

# Memory debugging (Linux)
RUSTFLAGS="-Zsanitizer=address" cargo test

# Performance debugging
cargo build --release
time ./target/release/dataprof-cli file.csv --benchmark
```

### Common Debug Scenarios
```bash
# Engine selection debugging
RUST_LOG=dataprof::engines=debug cargo run -- file.csv

# Memory usage tracking
RUST_LOG=dataprof::memory=debug cargo run -- file.csv

# Database connection debugging
RUST_LOG=dataprof::connectors=debug cargo test test_postgres_connection
```

## 📦 Dependency Management

### Adding Dependencies
```bash
# Add runtime dependency
cargo add serde --features derive

# Add development dependency
cargo add --dev tempfile

# Add optional feature dependency
cargo add postgres --optional
```

### Maintenance
```bash
# Update dependencies
just update                     # cargo update
cargo outdated                 # Check for updates

# Clean unused dependencies
just check-deps                 # cargo machete
cargo tree                     # Dependency tree
```

### Feature Flags
- `database` - Database connector support
- `arrow` - Apache Arrow integration
- `postgres` - PostgreSQL connector
- `mysql` - MySQL connector
- `sqlite` - SQLite connector
- `duckdb` - DuckDB connector
- `all-db` - All database connectors

## 🚀 Performance Optimization

### Profiling Commands
```bash
# CPU profiling
cargo build --release
perf record --call-graph=dwarf ./target/release/dataprof-cli large.csv
perf report

# Memory profiling
valgrind --tool=massif ./target/release/dataprof-cli large.csv
ms_print massif.out.* > memory_profile.txt
```

### Optimization Guidelines
- Use `cargo build --release` for performance testing
- Profile before optimizing
- Prefer streaming over loading entire files
- Use SIMD operations where applicable
- Avoid unnecessary string allocations
- Use `Arc<str>` for shared string data

## 🔒 Security Best Practices

### Code Security
```bash
# Security audit
cargo audit                     # Check for vulnerabilities
cargo test-security             # Security-focused tests

# Safe code patterns
grep -r "unsafe" src/          # Minimize unsafe blocks
grep -r "unwrap()" src/        # Use proper error handling
```

### Database Security
- Use parameterized queries
- Validate all inputs
- Use connection pooling
- Implement proper authentication

## 📝 Documentation

### Code Documentation
```bash
# Generate docs
cargo doc --all-features --no-deps                      # Generate documentation
cargo doc --all-features --no-deps-open                # Open in browser

# Documentation tests
cargo test --doc              # Test code examples in docs
```

### Documentation Standards
- Document all public APIs
- Include examples in doc comments
- Keep README.md updated
- Update CHANGELOG.md for releases

## 🎯 Project Statistics

```bash
just stats                     # Project statistics
tokei                         # Lines of code (if installed)
cargo tree                    # Dependency tree
```

## 🤝 Contributing

### Before Submitting PR
1. `just quality` - All checks pass
2. `cargo test-all` - All tests pass
3. Update documentation if needed
4. Follow commit message conventions
5. Ensure no `unwrap()` calls in new code

### Code Review Checklist
- [ ] Code follows project conventions
- [ ] Tests added for new functionality
- [ ] Documentation updated
- [ ] Performance impact considered
- [ ] Security implications reviewed
- [ ] Error handling implemented properly

## 📚 Additional Resources

- [Testing Guide](./TESTING.md) - Detailed testing strategies
- [IDE Setup](./IDE_SETUP.md) - IDE-specific configurations
- [Troubleshooting](./TROUBLESHOOTING.md) - Common issues and solutions
- [Project Documentation](./project/) - Architecture and design decisions
- [API Reference](./python/API_REFERENCE.md) - API documentation
