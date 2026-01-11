# GitHub Actions Workflows 🤖

This directory contains the optimized CI/CD workflows for DataProfiler, built with composite actions for maximum efficiency and maintainability.

## 📋 Workflow Architecture

| Workflow | Trigger | Purpose | Duration | Composite Actions |
|----------|---------|---------|----------|-------------------|
| `ci.yml` | Push/PR to `main`, `master` | Core testing & quality | ~5 min | setup-rust, setup-system-deps, run-tests |
| `staging-dev.yml` | Push/PR to `staging` | Development feedback | ~15 min | setup-rust, setup-system-deps, run-tests |
| `quick-benchmarks.yml` | Push/PR (code paths) | PR performance validation | ~8 min | setup-rust, benchmark-runner |
| `benchmarks.yml` | Push to `main`/`master`, scheduled | Comprehensive performance | ~90 min | setup-rust, setup-python-deps, benchmark-runner |
| `staging-to-master.yml` | PR `staging`→`master` | Production release gate | ~10 min | Validation only |
| `release.yml` | Version tags (`v*`) | Release automation | ~25 min | Cross-platform builds |

## 🏗️ Composite Actions

### Core Infrastructure
- **`setup-rust/`**: Unified Rust toolchain setup with caching and fallback
- **`setup-python-deps/`**: Python dependencies for benchmark comparisons
- **`setup-system-deps/`**: DuckDB, SQLite system dependencies
- **`run-tests/`**: Configurable test execution (core, integration, all)
- **`benchmark-runner/`**: Benchmark execution with quick/full modes

### Key Benefits
- **🔄 Eliminated 24+ duplicate Rust setups** across workflows
- **⚡ Intelligent caching** with 80%+ hit rates
- **🛡️ Network retry logic** and fallback installations
- **🎯 Consistent naming** and error handling patterns
- **📊 Modular performance tracking** with external templates

## 🎯 Workflow Specialization

### `ci.yml` - Core Testing (main/master)
**Philosophy**: Essential quality gates for production branches

**Jobs**:
- **test**: Parallel core and integration testing with matrix strategy
- **quality-checks**: Formatting, clippy, documentation validation
- **security-audit**: Dependency vulnerability scanning

**Features**:
- Parallel test execution for faster feedback
- Security audit with cargo-audit
- Cross-platform system dependency support

### `staging-dev.yml` - Development Workflow (staging)
**Philosophy**: Fast feedback for development iteration

**Jobs**:
- **quick-validation**: Build checks, core tests, format/lint (8 min)
- **integration-tests**: Full integration suite when pushing (20 min)
- **staging-summary**: Development progress reporting

**Features**:
- Quick validation for all PRs
- Comprehensive testing only on push
- Conditional job execution for efficiency

### `quick-benchmarks.yml` - PR Performance Check
**Philosophy**: Catch performance regressions early without blocking development

**Jobs**:
- **quick-performance-check**: Micro/small dataset benchmarks (8 min)

**Features**:
- Triggers only on code changes (`src/`, `benches/`, `Cargo.toml`)
- Manual workflow_dispatch for testing
- Focused on speed over comprehensiveness

### `benchmarks.yml` - Comprehensive Performance Suite
**Philosophy**: Scientific performance validation with statistical rigor

**Jobs**:
- **build-check**: Benchmark compilation validation
- **comprehensive-benchmarks**: Full benchmark suite with external comparisons
- **manual-benchmarks**: On-demand benchmark execution with configurable types
- **performance-dashboard**: GitHub Pages deployment with trend analysis

**Features**:
- Scheduled runs (Monday/Thursday 2 AM UTC)
- External tool comparison (pandas, polars, great-expectations)
- Performance regression analysis with baseline comparison
- Interactive dashboard with real-time metrics

### `staging-to-master.yml` - Production Gate
**Philosophy**: Lightweight validation for staging→master promotion

**Jobs**:
- **validate-staging-pr**: Enforce proper workflow (staging branch only)
- **production-validation**: Additional production-specific checks

**Features**:
- Strict branch validation
- Lightweight by design (staging already tested comprehensively)

### `release.yml` - Release Automation
**Philosophy**: Multi-platform release with Python bindings

**Jobs**:
- **create-release**: GitHub release generation with changelog
- **build-***: Cross-platform binary builds (Linux, macOS, Windows)
- **python-release**: PyPI publishing with wheel generation
- **crates-release**: Crates.io publishing

**Features**:
- Automated changelog generation
- Cross-platform binary distribution
- Python package automation

## 🔧 Composite Action Details

### `setup-rust/` - Unified Rust Setup
```yaml
- uses: ./.github/actions/setup-rust
  with:
    cache-prefix: 'benchmark'  # Workflow-specific caching
    components: 'rustfmt,clippy'  # Additional components
```

**Features**:
- Automatic retry with `RUSTUP_MAX_RETRIES: 3`
- Fallback to manual rustup installation
- Intelligent caching with workflow-specific keys
- Component verification and version reporting

### `benchmark-runner/` - Benchmark Execution
```yaml
- uses: ./.github/actions/benchmark-runner
  with:
    benchmark-type: 'unified'    # unified, domain, statistical, all
    mode: 'quick'               # quick, full
    sample-size: '10'           # Quick mode samples
    measurement-time: '3'       # Quick mode timing
    timeout: '25'               # Per-benchmark timeout
```

**Features**:
- Configurable benchmark types and execution modes
- Intelligent parameter handling for quick vs full modes
- Timeout controls for reliability
- Structured result collection

### `run-tests/` - Test Execution
```yaml
- uses: ./.github/actions/run-tests
  with:
    test-type: 'core'          # core, integration, all
    features: 'database'       # Optional cargo features
    timeout: '15'              # Test timeout in minutes
```

**Features**:
- Granular test type selection
- Feature flag support
- Timeout controls
- Comprehensive reporting

## ⚡ Performance Optimizations

### Cache Strategy
```yaml
# Workflow-specific cache keys prevent conflicts
benchmark: "benchmark-{os}-{cargo.lock}-v3"
ci: "ci-{os}-{cargo.lock}-v3"
staging: "staging-{os}-{cargo.lock}-v3"
quick-benchmark: "quick-benchmark-{os}-{cargo.lock}-v2"
```

### Parallel Execution
- **CI**: Parallel test matrix (core + integration)
- **Staging**: Conditional parallelism based on trigger
- **Benchmarks**: Sequential execution for accurate measurements
- **Quick Benchmarks**: Minimal parallelism for speed

### Resource Efficiency
- **Shared Dependencies**: System deps cached between runs
- **Smart Triggers**: Path-based workflow activation
- **Timeout Controls**: Prevent runaway processes
- **Artifact Management**: 30-90 day retention policies

## 📊 GitHub Pages Dashboard

### Automated Performance Tracking
- **Real-time Metrics**: Performance vs pandas comparison
- **Historical Trends**: 90-day artifact retention
- **Statistical Confidence**: 95% confidence intervals
- **Engine Accuracy**: AdaptiveProfiler selection validation

### Dashboard Features
- **External Template**: Maintainable HTML separated from workflow
- **Dynamic Loading**: Fetches latest benchmark results
- **Mobile Responsive**: Optimized for all devices
- **Performance Categories**: Outstanding, Excellent, Competitive ratings

## 🚨 Reliability Features

### Network Resilience
- **Retry Logic**: `RUSTUP_MAX_RETRIES: 3` for toolchain installation
- **Fallback Installation**: Manual rustup if primary fails
- **Timeout Controls**: Prevent hanging processes
- **Health Checks**: Environment verification before execution

### Error Handling
- **Graceful Degradation**: Continue with warnings when possible
- **Detailed Logging**: Comprehensive error reporting
- **Status Validation**: Verify successful completion
- **Artifact Preservation**: Results saved even on partial failure

## 🔄 Development Workflow

### Feature Development
```bash
# 1. Create feature branch
git checkout -b feature/new-feature

# 2. Push to staging for testing
git push origin staging
# Triggers: staging-dev.yml (quick validation)

# 3. Create PR to staging
gh pr create --base staging
# Triggers: staging-dev.yml (full validation)

# 4. Merge to staging
# Triggers: quick-benchmarks.yml (if code changed)

# 5. Create staging→master PR
gh pr create --base master --head staging
# Triggers: staging-to-master.yml (production validation)

# 6. Merge to master
# Triggers: ci.yml, benchmarks.yml (comprehensive)
```

### Manual Workflows
```bash
# Trigger quick benchmarks manually
gh workflow run quick-benchmarks.yml --ref staging

# Run specific benchmark type
gh workflow run benchmarks.yml --ref main \
  -f benchmark_type=unified

# Check workflow status
gh run list --workflow=benchmarks.yml --limit=5
```

## 🎯 Quality Metrics

### Performance Targets
- **Quick Benchmarks**: <8 minutes (95% target)
- **CI Testing**: <5 minutes (99% target)
- **Staging Development**: <15 minutes (90% target)
- **Comprehensive Benchmarks**: <90 minutes (95% target)

### Success Rate Targets
- **CI**: >99% (critical path protection)
- **Staging Development**: >90% (development feedback)
- **Production Validation**: >95% (release readiness)
- **Benchmark Suite**: >90% (performance tracking)

### Cache Efficiency
- **Hit Rate Target**: >80% across all workflows
- **Cache Size**: Optimized for <500MB per workflow
- **Invalidation**: Smart key generation prevents stale caches

## 🔧 Troubleshooting

### Common Issues

**Q: Composite action not found error**
```
Error: ./.github/actions/setup-rust/action.yml not found
```
- Ensure you're using the correct ref in workflow triggers
- Composite actions are only available within the same repository

**Q: Cache permission warnings**
```
Warning: Failed to restore: Permission denied
```
- System dependencies use file existence checks instead of caching
- Warnings are normal and don't affect functionality

**Q: Benchmark timeout issues**
```
Error: Process completed with exit code 124
```
- Increase timeout in benchmark-runner parameters
- Check for infinite loops or hanging processes

### Performance Debugging

**Q: Why are benchmarks slower than expected?**
1. Check runner resource allocation
2. Verify no background processes interfering
3. Review sample size and measurement time parameters
4. Check for network latency in external comparisons

**Q: Cache misses causing slow builds**
1. Verify cache key generation is consistent
2. Check for Cargo.lock file changes
3. Review cache size limits and eviction policies

## 🔮 Future Enhancements

### Planned Improvements
- **Smart Testing**: Only test changed components
- **Progressive Deployment**: Canary releases with rollback
- **Advanced Metrics**: Memory profiling, CPU analysis
- **Security Scanning**: SAST/DAST integration

### Monitoring Integration
- **Performance Alerts**: Regression detection notifications
- **Resource Tracking**: CI minute usage optimization
- **Success Rate Monitoring**: Workflow reliability metrics
- **Dependency Analysis**: Automated security updates

---

## 📚 Quick Reference

```bash
# View all workflows
gh workflow list

# Monitor staging development
gh run watch $(gh run list -w staging-dev.yml -L 1 --json databaseId -q '.[0].databaseId')

# Check benchmark results
gh run download $(gh run list -w benchmarks.yml -L 1 --json databaseId -q '.[0].databaseId')

# Manual benchmark execution
gh workflow run benchmarks.yml -f benchmark_type=all
```

For detailed development guidance, see [DEVELOPMENT.md](../DEVELOPMENT.md).
For performance information, see [Performance Guide](../../docs/guides/performance-guide.md).
