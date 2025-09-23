# DataProfiler Testing Guide

Comprehensive testing strategies and guidelines for DataProfiler development.

## 🎯 Testing Philosophy

DataProfiler employs a multi-layered testing approach:
- **Fast feedback** with unit tests
- **Integration confidence** with component tests
- **Real-world validation** with end-to-end tests
- **Performance assurance** with benchmarks
- **Security validation** with specialized tests

## 📊 Test Structure Overview

```
tests/
├── fixtures/                    # Test data and utilities
│   ├── standard_datasets/       # Standard CSV/JSON test files
│   ├── dataset_generator.rs     # Dynamic test data generation
│   └── domain_datasets.rs       # Domain-specific test data
├── *_test.rs                   # Integration tests
├── *_tests.rs                  # Test suites
└── data/                       # Large test datasets

benches/
├── unified_benchmarks.rs        # General performance tests
├── domain_benchmarks.rs         # Domain-specific benchmarks
└── statistical_benchmark.rs     # Statistical operations benchmarks

src/
└── **/*_test.rs                # Unit tests (co-located with source)
```

## 🧪 Test Categories

### 1. Unit Tests
**Location**: Inline with source code (`#[cfg(test)]` modules)
**Purpose**: Test individual functions and methods in isolation

```bash
# Run unit tests only
just test                       # Fast unit tests
cargo test --lib               # Library unit tests only
cargo test --bin dataprof-cli  # CLI unit tests only

# Run specific unit tests
cargo test engine_selection     # Test specific functionality
cargo test --package dataprof quality  # Test specific module
```

**Example Unit Test Structure**:
```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_selection_small_file() {
        // Test automatic engine selection logic
    }

    #[test]
    fn test_memory_estimation() {
        // Test memory usage calculations
    }
}
```

### 2. Integration Tests
**Location**: `tests/` directory
**Purpose**: Test component interactions and feature integration

```bash
# Run all integration tests
just test-all                  # All tests including integration
cargo test                     # All tests

# Run specific integration test files
cargo test --test integration_tests
cargo test --test data_quality_simple
cargo test --test error_handling_simple
```

**Key Integration Test Files**:
- `integration_tests.rs` - Core functionality integration
- `data_quality_simple.rs` - Data quality analysis tests
- `error_handling_simple.rs` - Error handling and recovery
- `v03_comprehensive.rs` - Version 0.3 feature validation
- `adaptive_engine_tests.rs` - Engine selection and fallback

### 3. Database Integration Tests
**Location**: `tests/database_integration.rs`
**Purpose**: Test database connectors and data operations

```bash
# Setup databases first
just db-setup                  # Start PostgreSQL, MySQL, Redis

# Run database tests
just test-db                   # All database tests
just test-postgres             # PostgreSQL-specific tests
just test-mysql                # MySQL-specific tests
just test-sqlite               # SQLite tests
just test-duckdb               # DuckDB tests

# Run all database tests with setup
just test-all-db               # Setup + test + teardown
```

**Database Test Requirements**:
- Docker must be running
- Test databases are automatically configured
- Connection pooling and cleanup tested
- SQL injection prevention validated

### 4. CLI End-to-End Tests
**Location**: `tests/cli_basic_tests.rs`
**Purpose**: Test CLI interface and user workflows

```bash
# Run CLI tests (slower)
just test-cli                  # CLI integration tests
cargo test --test cli_basic_tests

# Debug CLI behavior
just debug-run examples/sample.csv
cargo run -- --help           # Test help output
```

**CLI Test Coverage**:
- Command-line argument parsing
- File format detection
- Output formatting
- Error message clarity
- Performance metrics display

### 5. Security Tests
**Location**: `tests/security_tests.rs`
**Purpose**: Validate security properties and prevent vulnerabilities

```bash
# Run security tests
just test-security             # Security-focused tests
cargo audit                    # Dependency vulnerability scan

# Memory safety tests
RUSTFLAGS="-Zsanitizer=address" cargo test
```

**Security Test Areas**:
- Input validation and sanitization
- SQL injection prevention
- Memory safety verification
- Unsafe code block validation
- Dependency vulnerability checks

### 6. Memory and Performance Tests
**Location**: `tests/memory_leak_tests.rs`
**Purpose**: Ensure memory efficiency and detect leaks

```bash
# Memory tests
cargo test --test memory_leak_tests
just profile-memory examples/large.csv

# Performance tests
cargo test --test arrow_performance_test
```

### 7. Feature-Specific Tests
**Location**: Various test files
**Purpose**: Test specific features and integrations

```bash
# Apache Arrow integration
just test-arrow                # Arrow feature tests
cargo test --features arrow --test arrow_integration_test

# Feature flag combinations
cargo test --features database
cargo test --features all-db
cargo test --no-default-features
```

## ⚡ Benchmarks

### Running Benchmarks
```bash
# Run all benchmarks
just bench                     # cargo bench
cargo bench                   # Direct cargo command

# Run specific benchmarks
cargo bench unified            # General performance
cargo bench domain            # Domain-specific operations
cargo bench statistical       # Statistical computations
```

### Benchmark Categories

#### 1. Unified Benchmarks (`benches/unified_benchmarks.rs`)
- File processing performance
- Engine selection overhead
- Memory allocation patterns
- Cross-platform performance

#### 2. Domain Benchmarks (`benches/domain_benchmarks.rs`)
- Financial data processing
- Scientific dataset analysis
- Log file analysis
- Geospatial data handling

#### 3. Statistical Benchmarks (`benches/statistical_benchmark.rs`)
- Statistical computation performance
- Large dataset processing
- Memory-efficient algorithms
- SIMD optimization validation

### Performance Regression Detection
```bash
# Baseline performance measurement
cargo bench > baseline.txt

# After changes, compare performance
cargo bench > current.txt
# Compare baseline.txt vs current.txt
```

## 🔍 Test Data Management

### Standard Test Datasets
**Location**: `tests/fixtures/standard_datasets/`

```bash
# View available test datasets
ls tests/fixtures/standard_datasets/
cat tests/fixtures/standard_datasets/README.md
```

**Standard Datasets Include**:
- Small CSV files (< 1MB) for unit tests
- Medium files (1-10MB) for integration tests
- Large files (10-100MB) for performance tests
- Malformed files for error handling tests
- Unicode and special character files

### Dynamic Test Data Generation
**Location**: `tests/fixtures/dataset_generator.rs`

```rust
// Generate test data programmatically
use tests::fixtures::dataset_generator::*;

let csv_data = generate_csv(1000, vec!["name", "age", "salary"]);
let json_data = generate_json_array(500, field_types);
```

### Domain-Specific Test Data
**Location**: `tests/fixtures/domain_datasets.rs`

Pre-generated datasets for specific domains:
- Financial data (stock prices, transactions)
- Scientific data (measurements, experiments)
- Web logs (access logs, error logs)
- Geospatial data (coordinates, boundaries)

## 🏃‍♂️ Test Execution Strategies

### Development Workflow
```bash
# Fast feedback loop (< 30 seconds)
just test                      # Unit tests only

# Pre-commit validation (< 2 minutes)
just quality                   # Format + lint + test

# Full validation (< 10 minutes)
just test-all                  # All tests including integration
```

### Continuous Integration
```bash
# CI test matrix
cargo test --all-features     # All features enabled
cargo test --no-default-features  # Minimal features
cargo test --features database    # Database features only
```

### Pre-Release Testing
```bash
# Comprehensive validation
just test-all-db              # All tests with databases
just bench                    # Performance validation
cargo audit                   # Security audit
just coverage                 # Code coverage report
```

## 📈 Code Coverage

### Generating Coverage Reports
```bash
# Install coverage tool
cargo install cargo-tarpaulin

# Generate HTML coverage report
just coverage                  # Uses tarpaulin
open coverage/tarpaulin-report.html

# Generate different format reports
cargo tarpaulin --out Xml      # For CI systems
cargo tarpaulin --out Json     # For tooling
```

### Coverage Targets
- **Unit tests**: >90% line coverage
- **Integration tests**: >80% feature coverage
- **Critical paths**: 100% coverage required
- **Error paths**: >70% coverage

### Coverage Exclusions
```rust
// Exclude from coverage
#[cfg(not(tarpaulin_include))]
fn platform_specific_function() {
    // Platform-specific code
}
```

## 🛠️ Testing Tools and Utilities

### Property-Based Testing
```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn test_column_analysis_properties(
        data in prop::collection::vec(any::<String>(), 0..1000)
    ) {
        // Test properties that should always hold
        let analysis = analyze_column(&data);
        assert!(analysis.count >= 0);
        assert!(analysis.null_count <= analysis.count);
    }
}
```

### Test Fixtures and Helpers
```rust
// Common test utilities
use tests::fixtures::*;

#[test]
fn test_with_standard_data() {
    let test_data = load_standard_dataset("financial_sample.csv");
    let result = process_data(test_data);
    assert_validation_rules(result);
}
```

### Custom Test Attributes
```rust
// Slow tests (excluded from default test run)
#[test]
#[ignore = "slow"]
fn test_large_file_processing() {
    // Test with multi-GB files
}

// Database tests (require database setup)
#[test]
#[cfg(feature = "database")]
fn test_database_connection() {
    // Database-specific tests
}
```

## 🐛 Debugging Tests

### Test Debugging in VS Code
1. Set breakpoints in test code
2. Use "Debug unit tests" configuration
3. Run specific test with debugger attached

### Command Line Test Debugging
```bash
# Debug specific test
cargo test test_name -- --nocapture

# Show test output
cargo test -- --show-output

# Run single test with logging
RUST_LOG=debug cargo test test_name -- --nocapture

# Test with memory debugging
RUSTFLAGS="-Zsanitizer=address" cargo test test_name
```

### Test Environment Variables
```bash
# Useful environment variables for testing
export RUST_LOG=debug              # Enable debug logging
export RUST_BACKTRACE=1           # Show backtraces on panic
export DATAPROF_TEST_DB_URL=...   # Override test database
export DATAPROF_TEST_DATA_DIR=... # Override test data location
```

## 📋 Testing Best Practices

### Test Organization
1. **Arrange-Act-Assert** pattern in tests
2. **One assertion per test** when possible
3. **Descriptive test names** that explain the scenario
4. **Test both success and failure paths**

### Test Data Principles
1. **Deterministic test data** for consistent results
2. **Representative data** that matches real usage
3. **Edge cases coverage** (empty, null, malformed)
4. **Size-appropriate data** for test performance

### Performance Testing Guidelines
1. **Baseline measurements** before optimization
2. **Consistent test environment** for comparisons
3. **Multiple iterations** to account for variance
4. **Memory profiling** alongside CPU profiling

### Database Testing Strategy
1. **Isolated test databases** per test run
2. **Automatic cleanup** after tests
3. **Transaction rollback** for test isolation
4. **Connection pool testing** under load

## 🔧 Test Configuration

### Feature Flag Testing
```bash
# Test different feature combinations
cargo test --features database
cargo test --features arrow
cargo test --features all-db
cargo test --no-default-features --features minimal
```

### Platform-Specific Testing
```bash
# Cross-platform test validation
cargo test --target x86_64-unknown-linux-gnu
cargo test --target x86_64-pc-windows-msvc
cargo test --target x86_64-apple-darwin
```

### Custom Test Runners
```toml
# In Cargo.toml
[[test]]
name = "integration_tests"
path = "tests/integration_tests.rs"
required-features = ["database"]
```

## 📊 Test Metrics and Monitoring

### Key Testing Metrics
- **Test execution time**: Monitor for performance regression
- **Test coverage percentage**: Maintain >80% overall coverage
- **Test failure rate**: Target <1% flaky tests
- **Database test isolation**: Ensure no cross-test contamination

### Automated Test Reporting
```bash
# Generate test reports for CI
cargo test --message-format json > test_results.json
cargo tarpaulin --out Json > coverage.json
```

## 🚀 Advanced Testing Techniques

### Fuzzing
```rust
// Fuzz testing with arbitrary data
#[cfg(fuzzing)]
mod fuzz_tests {
    use super::*;

    #[test]
    fn fuzz_csv_parser() {
        // Generate random CSV-like data and test parsing
    }
}
```

### Load Testing
```rust
#[test]
#[ignore = "load_test"]
fn test_concurrent_processing() {
    // Test system under concurrent load
    use std::thread;
    let handles: Vec<_> = (0..10)
        .map(|i| thread::spawn(move || process_large_file(i)))
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }
}
```

### Integration with External Systems
```bash
# Test against real databases
export DATABASE_URL=postgresql://real_server/test_db
cargo test --features postgres test_real_database_integration
```

## 🔒 Security Testing

### Overview
DataProfiler implements comprehensive security testing to prevent SQL injection, credential exposure, and other vulnerabilities. Security tests are automatically run in CI/CD and should be executed before any database-related changes.

### Security Test Categories

#### SQL Injection Prevention
```bash
# Run all security tests
cargo test --test security_tests --features database

# Run specific security categories
cargo test sql_injection_tests
cargo test error_sanitization_tests
cargo test integration_security_tests
```

**Test Coverage Areas:**
- Union-based injection attacks
- Boolean-based blind attacks
- Time-based blind attacks
- Error-based attacks
- Stacked queries
- Comment injection

#### Error Sanitization Tests
```rust
#[test]
fn test_credential_sanitization() {
    let error_with_creds = "Connection failed: postgresql://user:secret@host/db";
    let sanitized = sanitize_error_message(error_with_creds);

    assert!(!sanitized.contains("secret"));
    assert!(sanitized.contains("[REDACTED]"));
}
```

#### Input Validation Tests
```rust
#[test]
fn test_sql_identifier_validation() {
    // Valid identifiers
    assert!(validate_sql_identifier("users").is_ok());
    assert!(validate_sql_identifier("\"quoted table\"").is_ok());

    // Malicious attempts
    assert!(validate_sql_identifier("users; DROP TABLE").is_err());
    assert!(validate_sql_identifier("users' OR 1=1--").is_err());
}
```

### Security Testing Best Practices

#### 1. Comprehensive Attack Vectors
Test against realistic attack patterns:
```rust
let attack_patterns = vec![
    "users'; DROP TABLE users; --",
    "products' UNION SELECT password FROM admin",
    "orders' AND (SELECT SLEEP(5))",
    "customers'; EXEC xp_cmdshell('rm -rf /')",
];

for pattern in attack_patterns {
    assert!(validate_sql_identifier(pattern).is_err());
}
```

#### 2. Environment Security Validation
```bash
# Test credential loading from environment
export POSTGRES_USER=testuser
export POSTGRES_PASSWORD=testpass
cargo test test_env_credential_loading
```

#### 3. SSL/TLS Configuration Testing
```rust
#[test]
fn test_ssl_enforcement() {
    let config = SslConfig::production();
    assert!(config.require_ssl);
    assert!(config.verify_server_cert);
}
```

### Security Test Data

#### Safe Test Credentials
```rust
// Use non-sensitive test credentials
const TEST_USER: &str = "dataprof_test";
const TEST_PASS: &str = "test_password_123";
const TEST_DB: &str = "dataprof_test_db";
```

#### Malicious Input Patterns
```rust
const INJECTION_PATTERNS: &[&str] = &[
    "'; DROP TABLE users; --",
    "' UNION SELECT * FROM passwords --",
    "'; WAITFOR DELAY '00:00:05'; --",
    "' AND 1=1 --",
    "'; EXEC sp_configure; --",
];
```

### Security Test Environment

#### Isolated Test Database
```bash
# Setup isolated test environment
docker run -d --name dataprof-security-test \
  -e POSTGRES_DB=security_test \
  -e POSTGRES_USER=test_user \
  -e POSTGRES_PASSWORD=test_pass \
  -p 5433:5432 postgres:15

# Run security tests against isolated instance
export TEST_DATABASE_URL=postgresql://test_user:test_pass@localhost:5433/security_test
cargo test --test security_tests
```

#### CI/CD Security Integration
Security tests are automatically run in multiple CI workflows:
- **Basic CI**: `cargo audit` for dependency vulnerabilities
- **Advanced Security**: Comprehensive scanning with multiple tools
- **Production Pipeline**: Enhanced validation before deployment

### Security Monitoring Tests

#### Audit Trail Validation
```rust
#[test]
fn test_security_audit_logging() {
    let result = profile_database(config, "sensitive_table").await?;

    // Verify security warnings are captured
    assert!(result.security_warnings.len() > 0);

    // Verify no sensitive data in logs
    for warning in &result.security_warnings {
        assert!(!warning.contains("password"));
        assert!(!warning.contains("secret"));
    }
}
```

#### Connection Security Tests
```rust
#[test]
fn test_connection_security_validation() {
    let warnings = validate_connection_security(
        "postgresql://user:pass@localhost:5432/db",
        &SslConfig::default(),
        "postgresql"
    ).unwrap();

    assert!(warnings.iter().any(|w| w.contains("Password embedded")));
    assert!(warnings.iter().any(|w| w.contains("localhost")));
}
```

### Performance Impact Testing

Security features should not significantly impact performance:

```rust
#[test]
fn test_validation_performance() {
    let start = Instant::now();

    for _ in 0..1000 {
        validate_sql_identifier("test_table").unwrap();
    }

    let duration = start.elapsed();
    assert!(duration.as_millis() < 100); // Should be very fast
}
```

### Security Test Reporting

#### Coverage Requirements
- **Security functions**: 100% test coverage required
- **SQL validation**: All injection patterns tested
- **Error sanitization**: All sensitive patterns covered
- **SSL configuration**: All modes validated

#### Automated Security Scanning
```bash
# Run comprehensive security scan
just security-scan

# Individual security tools
cargo audit                    # Dependency vulnerabilities
cargo deny check              # License and security policy
semgrep --config=p/security   # Static analysis
```

## 📚 Further Reading

- [Rust Testing Documentation](https://doc.rust-lang.org/book/ch11-00-testing.html)
- [Property-Based Testing with Proptest](https://docs.rs/proptest/)
- [Benchmarking with Criterion](https://docs.rs/criterion/)
- [Security Testing in Rust](https://rust-secure-code.github.io/)
- [DataProfiler Development Guide](./DEVELOPMENT.md)
- [IDE Setup Guide](./IDE_SETUP.md)