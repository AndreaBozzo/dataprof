# DataProfiler Development Task Runner
# Install 'just' command runner: https://github.com/casey/just
# Usage: just <task>

# Default task - show available commands
default:
    @just --list

# Complete development environment setup
setup:
    @echo "🔧 Setting up DataProfiler development environment..."
    ./scripts/setup-dev.sh

# Format all code
fmt:
    @echo "🎨 Formatting code..."
    cargo fmt --all

# Check code formatting without changing files
fmt-check:
    @echo "🎨 Checking code formatting..."
    cargo fmt --all --check

# Run linter (clippy)
lint:
    @echo "🔍 Running clippy linter..."
    cargo clippy --all-targets --all-features -- -D warnings

# Run all library tests
test:
    @echo "🧪 Running library tests..."
    cargo test --lib

# Run all tests including integration tests
test-all:
    @echo "🧪 Running all tests..."
    cargo test

# Run database tests (requires database feature)
test-db:
    @echo "🗃️ Running database integration tests..."
    cargo test --features database --test database_integration

# Run security tests
test-security:
    @echo "🛡️ Running security tests..."
    cargo test --features database --test security_tests

# Run CLI tests (slow)
test-cli:
    @echo "⚡ Running CLI tests..."
    cargo test --test cli_basic_tests

# Run tests with specific feature flags
test-arrow:
    @echo "🏹 Running Arrow integration tests..."
    cargo test --features arrow --test arrow_integration_test

# Build in release mode
build:
    @echo "🔨 Building in release mode..."
    cargo build --release

# Build with all features
build-all:
    @echo "🔨 Building with all features..."
    cargo build --all-features

# Clean build artifacts
clean:
    @echo "🧹 Cleaning build artifacts..."
    cargo clean

# Run pre-commit hooks on all files
precommit:
    @echo "🔍 Running pre-commit hooks..."
    pre-commit run --all-files

# Check for unused dependencies
check-deps:
    @echo "📦 Checking for unused dependencies..."
    cargo machete

# Update dependencies
update:
    @echo "📦 Updating dependencies..."
    cargo update

# Generate documentation
docs:
    @echo "📚 Generating documentation..."
    cargo doc --all-features --no-deps

# Open documentation in browser
docs-open:
    @echo "📚 Opening documentation..."
    cargo doc --all-features --no-deps --open

# Run benchmarks
bench:
    @echo "⚡ Running benchmarks..."
    cargo bench

# Check code coverage (requires cargo-tarpaulin)
coverage:
    @echo "📊 Checking code coverage..."
    cargo tarpaulin --out Html --output-dir coverage/

# Full quality check pipeline
quality: fmt-check lint test-all
    @echo "✅ All quality checks passed!"

# Development cycle: format, build, test
dev: fmt build test
    @echo "✅ Development cycle complete!"

# Release preparation pipeline
release-prep: clean fmt lint test-all build docs
    @echo "🚀 Release preparation complete!"

# Show project statistics
stats:
    @echo "📈 DataProfiler Project Statistics:"
    @echo "Source files:"
    @find src -name "*.rs" | wc -l
    @echo "Test files:"
    @find tests -name "*.rs" | wc -l
    @echo "Lines of code (src/):"
    @find src -name "*.rs" -exec wc -l {} + | tail -1
    @echo "Lines of tests:"
    @find tests -name "*.rs" -exec wc -l {} + | tail -1

# Debug build and run with example data
debug-run file="examples/sample_data.csv":
    @echo "🐛 Debug run with {{file}}..."
    cargo run --bin dataprof-cli -- {{file}} --quality --progress

# Profile memory usage (requires valgrind on Linux)
profile-memory file="examples/sample_data.csv":
    @echo "🔍 Profiling memory usage..."
    cargo build --release
    valgrind --tool=massif ./target/release/dataprof-cli {{file}} --quality

# Database development setup (requires Docker)
db-setup:
    @echo "🗃️ Setting up development databases..."
    docker-compose -f docker/dev-compose.yml up -d

# Database teardown
db-teardown:
    @echo "🗃️ Tearing down development databases..."
    docker-compose -f docker/dev-compose.yml down

# Install development tools
install-tools:
    @echo "🛠️ Installing development tools..."
    cargo install cargo-tarpaulin cargo-machete just
    rustup component add rustfmt clippy

# Create a new release (maintainer only)
release version:
    @echo "🚀 Creating release {{version}}..."
    git tag -a "v{{version}}" -m "Release v{{version}}"
    git push origin "v{{version}}"
