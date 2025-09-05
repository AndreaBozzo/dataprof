# DataProfiler v0.3.0 Development Tasks
# Install just: cargo install just
# Run: just <task-name>

# Show available tasks
default:
    @just --list

# Development setup
setup:
    @echo "🔧 Setting up development environment..."
    @if [ -f "scripts/setup-dev.sh" ]; then bash scripts/setup-dev.sh; else pwsh scripts/setup-dev.ps1; fi

# Code formatting
fmt:
    @echo "🎨 Formatting code..."
    cargo fmt --all

# Check formatting without fixing
fmt-check:
    @echo "🔍 Checking code formatting..."
    cargo fmt --all --check

# Run clippy linter
lint:
    @echo "🔍 Running clippy linter..."
    cargo clippy --all-targets --all-features -- -D warnings

# Run clippy with fixes
lint-fix:
    @echo "🔧 Running clippy with automatic fixes..."
    cargo clippy --all-targets --all-features --fix -- -D warnings

# Run all tests
test:
    @echo "🧪 Running all tests..."
    cargo test

# Run only library tests (faster)
test-lib:
    @echo "🧪 Running library tests..."
    cargo test --lib

# Run integration tests
test-integration:
    @echo "🧪 Running integration tests..."
    cargo test --test integration_tests

# Run specific test
test-one name:
    @echo "🧪 Running test: {{name}}"
    cargo test {{name}}

# Build project
build:
    @echo "🔨 Building project..."
    cargo build

# Build release
build-release:
    @echo "🔨 Building release..."
    cargo build --release

# Run pre-commit hooks on all files
precommit:
    @echo "🔄 Running pre-commit hooks..."
    pre-commit run --all-files

# Run specific pre-commit hook
precommit-hook hook:
    @echo "🔄 Running pre-commit hook: {{hook}}"
    pre-commit run {{hook}}

# Full quality check (format + lint + test)
check: fmt lint test-lib
    @echo "✅ All quality checks passed!"

# Clean build artifacts
clean:
    @echo "🧹 Cleaning build artifacts..."
    cargo clean
    @rm -rf target/ || true

# Generate documentation
docs:
    @echo "📚 Generating documentation..."
    cargo doc --no-deps --open

# Run benchmarks
bench:
    @echo "⚡ Running benchmarks..."
    cargo bench

# Profile with flamegraph (requires flamegraph: cargo install flamegraph)
profile file:
    @echo "🔥 Profiling with flamegraph..."
    cargo flamegraph --bin dataprof -- {{file}} --quality

# Update dependencies
update:
    @echo "📦 Updating dependencies..."
    cargo update

# Check for outdated dependencies (requires cargo-outdated)
outdated:
    @echo "📦 Checking for outdated dependencies..."
    cargo outdated

# Security audit (requires cargo-audit)
audit:
    @echo "🔐 Running security audit..."
    cargo audit

# Run comprehensive CI-like checks
ci: fmt-check lint test
    @echo "🎯 All CI checks passed!"

# Development workflow: format, lint, test
dev: fmt lint test-lib
    @echo "🚀 Development checks complete!"

# Release preparation
release: clean fmt lint test build-release
    @echo "📦 Release build ready!"

# View project statistics
stats:
    @echo "📊 Project statistics:"
    @find src -name "*.rs" | xargs wc -l | tail -1
    @echo "Tests:"
    @find tests -name "*.rs" | xargs wc -l | tail -1 || echo "No tests directory"
    @echo "Examples:"
    @find examples -name "*" | wc -l || echo "No examples directory"

# Run example analysis
example file:
    @echo "🎯 Running example analysis on {{file}}"
    cargo run -- {{file}} --quality

# Run streaming example
example-streaming file:
    @echo "🌊 Running streaming analysis on {{file}}"
    cargo run -- {{file}} --quality --streaming --progress

# Generate HTML report example
example-html file output:
    @echo "📄 Generating HTML report for {{file}}"
    cargo run -- {{file}} --quality --html {{output}}

# Install development dependencies
install-dev-deps:
    @echo "📦 Installing development dependencies..."
    cargo install cargo-outdated cargo-audit flamegraph
    @echo "Consider also installing: just, pre-commit"

# Show version information
version:
    @echo "DataProfiler v0.3.0 Development Environment"
    @echo "Rust version:"
    @rustc --version
    @echo "Cargo version:"
    @cargo --version
    @echo "Git version:"
    @git --version || echo "Git not available"
