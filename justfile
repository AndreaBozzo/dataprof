# DataProfiler Development Task Runner
# Install 'just' command runner: https://github.com/casey/just
# Usage: just <task>

# Default task - show available commands
default:
    @just --list

# Complete development environment setup
setup mode="full":
    @echo "🔧 Setting up DataProfiler development environment..."
    #!/usr/bin/env bash
    if command -v pwsh >/dev/null 2>&1; then
        pwsh -ExecutionPolicy Bypass -File ./scripts/setup-dev.ps1 -Mode {{mode}}
    elif [[ "$OSTYPE" == "msys" ]] || [[ "$OSTYPE" == "cygwin" ]]; then
        powershell -ExecutionPolicy Bypass -File ./scripts/setup-dev.ps1 -Mode {{mode}}
    else
        ./scripts/setup-dev.sh {{mode}}
    fi

# Legacy setup script (basic version)
setup-legacy:
    @echo "🔧 Legacy DataProfiler development environment setup..."
    ./scripts/setup-dev-legacy.sh

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
    docker-compose -f .devcontainer/docker-compose.yml up -d postgres mysql redis

# Database teardown
db-teardown:
    @echo "🗃️ Tearing down development databases..."
    docker-compose -f .devcontainer/docker-compose.yml down

# Start all development services including admin tools
db-setup-all:
    @echo "🗃️ Setting up all development services..."
    docker-compose -f .devcontainer/docker-compose.yml --profile admin up -d

# Database status check
db-status:
    @echo "🔍 Checking database services status..."
    docker-compose -f .devcontainer/docker-compose.yml ps

# Database logs
db-logs service="":
    @echo "📋 Showing database logs for {{service}}..."
    #!/usr/bin/env bash
    if [ "{{service}}" = "" ]; then
        docker-compose -f .devcontainer/docker-compose.yml logs -f
    else
        docker-compose -f .devcontainer/docker-compose.yml logs -f {{service}}
    fi

# Connect to PostgreSQL
db-connect-postgres:
    @echo "🐘 Connecting to PostgreSQL..."
    docker exec -it dataprof-postgres-dev psql -U dataprof -d dataprof_test

# Connect to MySQL
db-connect-mysql:
    @echo "🐬 Connecting to MySQL..."
    docker exec -it dataprof-mysql-dev mysql -u dataprof -pdev_password_123 dataprof_test

# Reset database data (careful!)
db-reset:
    @echo "⚠️ Resetting all database data..."
    docker-compose -f .devcontainer/docker-compose.yml down -v
    docker-compose -f .devcontainer/docker-compose.yml up -d postgres mysql redis

# Test specific database features
test-postgres:
    @echo "🐘 Running PostgreSQL-specific tests..."
    cargo test --features postgres --test postgres_integration

test-mysql:
    @echo "🐬 Running MySQL-specific tests..."
    cargo test --features mysql --test mysql_integration

test-sqlite:
    @echo "💿 Running SQLite-specific tests..."
    cargo test --features sqlite --test sqlite_integration

test-duckdb:
    @echo "🦆 Running DuckDB-specific tests..."
    cargo test --features duckdb --test duckdb_integration

# Test all database features with databases running
test-all-db:
    @echo "🗃️ Running all database tests..."
    just db-setup
    sleep 10  # Wait for databases to be ready
    cargo test --features all-db
    just db-teardown

# Complete development environment setup with databases
setup-complete:
    @echo "🚀 Complete development environment setup..."
    just setup full
    @echo "🗃️ Setting up databases..."
    just db-setup
    @echo "🧪 Running verification tests..."
    sleep 15  # Wait for databases to be fully ready
    just quality
    @echo "✅ Complete development environment setup finished!"
    @echo "💡 Use 'just --list' to see all available commands"

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
