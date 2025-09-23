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

# Comprehensive security scanning (all security tools)
security-scan:
    @echo "🔒 Running comprehensive security scan..."
    @echo "1️⃣  Dependency vulnerability scan..."
    cargo audit
    @echo "2️⃣  Security policy validation..."
    cargo deny check
    @echo "3️⃣  Security tests..."
    cargo test --test security_tests --features database
    @echo "4️⃣  Security-focused clippy..."
    cargo clippy --all-targets --all-features -- \
        -D clippy::suspicious \
        -D clippy::unwrap_used \
        -D clippy::panic \
        -D clippy::expect_used
    @echo "✅ Comprehensive security scan completed"

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

# Check for outdated dependencies
check-outdated:
    @echo "📦 Checking for outdated dependencies..."
    cargo outdated --root-deps-only

# Show dependency tree
deps-tree:
    @echo "🌳 Showing dependency tree..."
    cargo tree

# Show dependency tree with duplicates highlighted
deps-tree-duplicates:
    @echo "🌳 Showing dependency tree with duplicate detection..."
    cargo tree --duplicates

# Show dependency tree with features
deps-tree-features:
    @echo "🌳 Showing dependency tree with features..."
    cargo tree --format "{p} {f}"

# Comprehensive dependency audit
audit:
    @echo "🔍 Running comprehensive security audit..."
    cargo audit --color always
    @echo "✅ Security audit completed"

# Audit with detailed vulnerability information
audit-detailed:
    @echo "🔍 Running detailed security audit..."
    cargo audit --color always --json > security_audit.json
    cargo audit --color always
    @echo "📄 Detailed audit saved to security_audit.json"

# Check dependency licenses
check-licenses:
    @echo "📜 Checking dependency licenses..."
    #!/usr/bin/env bash
    if command -v cargo-license >/dev/null 2>&1; then
        cargo license --color always
    else
        echo "⚠️  cargo-license not installed. Install with: cargo install cargo-license"
        echo "📋 Checking licenses via Cargo.toml..."
        cargo metadata --format-version 1 | jq -r '.packages[] | select(.source != null) | "\(.name) \(.version): \(.license // "Unknown")"' | sort
    fi

# Update dependencies with safety checks
update:
    @echo "📦 Updating dependencies with safety checks..."
    @echo "🔍 Current dependency status:"
    cargo outdated --root-deps-only || echo "cargo-outdated not installed"
    @echo "🔄 Updating dependencies..."
    cargo update
    @echo "🧪 Running tests to verify updates..."
    cargo test --lib
    @echo "🔍 Checking for security issues after update..."
    cargo audit
    @echo "✅ Dependencies updated successfully"

# Update specific dependency
update-dep package:
    @echo "📦 Updating specific dependency: {{package}}..."
    cargo update -p {{package}}
    @echo "🧪 Running tests after updating {{package}}..."
    cargo test --lib
    @echo "✅ {{package}} updated successfully"

# Smart dependency update with backup
update-smart:
    @echo "🤖 Smart dependency update with backup..."
    cp Cargo.lock Cargo.lock.backup
    @echo "💾 Backed up Cargo.lock"
    -just update
    #!/usr/bin/env bash
    if [ $? -ne 0 ]; then
        echo "❌ Update failed, restoring backup..."
        mv Cargo.lock.backup Cargo.lock
        exit 1
    else
        rm Cargo.lock.backup
        echo "✅ Smart update completed successfully"
    fi

# Check for yanked dependencies
check-yanked:
    @echo "⚠️  Checking for yanked dependencies..."
    #!/usr/bin/env bash
    cargo tree --format "{p}" | sort -u | while read -r dep; do
        if [ ! -z "$dep" ]; then
            cargo search --limit 1 "$dep" 2>/dev/null | grep -q "yanked" && echo "⚠️  YANKED: $dep"
        fi
    done || echo "✅ No yanked dependencies found"

# Generate dependency report
deps-report:
    @echo "📊 Generating comprehensive dependency report..."
    @echo "=== DEPENDENCY REPORT ===" > dependency_report.txt
    @echo "Generated: $(date)" >> dependency_report.txt
    @echo "" >> dependency_report.txt
    @echo "=== DIRECT DEPENDENCIES ===" >> dependency_report.txt
    cargo tree --depth 1 >> dependency_report.txt
    @echo "" >> dependency_report.txt
    @echo "=== OUTDATED DEPENDENCIES ===" >> dependency_report.txt
    cargo outdated --root-deps-only >> dependency_report.txt 2>/dev/null || echo "cargo-outdated not available" >> dependency_report.txt
    @echo "" >> dependency_report.txt
    @echo "=== SECURITY AUDIT ===" >> dependency_report.txt
    cargo audit >> dependency_report.txt 2>&1
    @echo "" >> dependency_report.txt
    @echo "=== UNUSED DEPENDENCIES ===" >> dependency_report.txt
    cargo machete >> dependency_report.txt 2>&1
    @echo "📄 Report saved to dependency_report.txt"

# Clean up dependency cache
deps-clean:
    @echo "🧹 Cleaning dependency cache..."
    cargo clean
    rm -rf ~/.cargo/registry/cache/
    rm -rf ~/.cargo/git/
    @echo "✅ Dependency cache cleaned"

# Minimal dependency check (fast)
deps-check-minimal:
    @echo "⚡ Quick dependency check..."
    cargo check --locked
    @echo "✅ Dependencies are consistent with lock file"

# Full dependency health check
deps-health:
    @echo "🏥 Running full dependency health check..."
    @echo "1️⃣  Checking for unused dependencies..."
    cargo machete
    @echo "2️⃣  Checking for outdated dependencies..."
    cargo outdated --root-deps-only || echo "⚠️  cargo-outdated not installed"
    @echo "3️⃣  Running security audit..."
    cargo audit
    @echo "4️⃣  Checking for duplicate dependencies..."
    cargo tree --duplicates
    @echo "5️⃣  Verifying lock file consistency..."
    cargo check --locked
    @echo "✅ Dependency health check completed"

# Advanced dependency analysis with cargo-deny
deps-deny:
    @echo "🛡️ Running advanced dependency analysis with cargo-deny..."
    #!/usr/bin/env bash
    if command -v cargo-deny >/dev/null 2>&1; then
        cargo deny check
    else
        echo "⚠️  cargo-deny not installed. Install with: cargo install cargo-deny"
        echo "📋 Falling back to basic checks..."
        just deps-health
    fi

# Check dependency licenses compliance
deps-licenses-compliance:
    @echo "📜 Checking dependency license compliance..."
    #!/usr/bin/env bash
    if command -v cargo-deny >/dev/null 2>&1; then
        cargo deny check licenses
    else
        echo "⚠️  cargo-deny not installed. Using alternative license check..."
        just check-licenses
    fi

# Complete dependency management workflow
deps-workflow:
    @echo "🔄 Running complete dependency management workflow..."
    @echo "1️⃣  Checking for security advisories..."
    cargo audit
    @echo "2️⃣  Checking for unused dependencies..."
    cargo machete
    @echo "3️⃣  Checking for outdated dependencies..."
    cargo outdated --root-deps-only || echo "⚠️  cargo-outdated not installed"
    @echo "4️⃣  Running advanced analysis..."
    #!/usr/bin/env bash
    if command -v cargo-deny >/dev/null 2>&1; then
        cargo deny check
    else
        echo "⚠️  cargo-deny not available, skipping advanced analysis"
    fi
    @echo "5️⃣  Generating dependency report..."
    just deps-report
    @echo "✅ Complete dependency workflow finished"

# Add new dependency with safety checks
add-dep package *flags:
    @echo "📦 Adding dependency: {{package}} {{flags}}..."
    #!/usr/bin/env bash
    if command -v cargo-edit >/dev/null 2>&1; then
        cargo add {{package}} {{flags}}
    else
        echo "⚠️  cargo-edit not installed. Install with: cargo install cargo-edit"
        echo "🔧 Please add dependency manually to Cargo.toml"
        exit 1
    fi
    @echo "🧪 Running tests after adding {{package}}..."
    cargo test --lib
    @echo "🔍 Running security audit..."
    cargo audit
    @echo "✅ {{package}} added successfully"

# Remove dependency with cleanup
remove-dep package:
    @echo "🗑️  Removing dependency: {{package}}..."
    #!/usr/bin/env bash
    if command -v cargo-edit >/dev/null 2>&1; then
        cargo remove {{package}}
    else
        echo "⚠️  cargo-edit not installed. Install with: cargo install cargo-edit"
        echo "🔧 Please remove dependency manually from Cargo.toml"
        exit 1
    fi
    @echo "🧹 Checking for unused dependencies after removal..."
    cargo machete
    @echo "🧪 Running tests after removing {{package}}..."
    cargo test --lib
    @echo "✅ {{package}} removed successfully"

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
    cargo install cargo-tarpaulin cargo-machete cargo-outdated cargo-audit cargo-license just
    rustup component add rustfmt clippy rust-src rust-analyzer
    @echo "✅ Development tools installed successfully"

# Install extended development tools (optional but recommended)
install-tools-extended:
    @echo "🔧 Installing extended development tools..."
    cargo install cargo-deny cargo-update cargo-edit cargo-expand cargo-watch
    @echo "📋 Extended tools installed:"
    @echo "  - cargo-deny: Advanced dependency checking"
    @echo "  - cargo-update: Update installed cargo binaries"
    @echo "  - cargo-edit: Add/remove dependencies from CLI"
    @echo "  - cargo-expand: Show macro expansions"
    @echo "  - cargo-watch: Auto-rebuild on file changes"
    @echo "✅ Extended development tools installed successfully"

# Create a new release (maintainer only)
release version:
    @echo "🚀 Creating release {{version}}..."
    git tag -a "v{{version}}" -m "Release v{{version}}"
    git push origin "v{{version}}"
