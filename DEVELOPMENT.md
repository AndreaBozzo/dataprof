# Development Workflow 🛠️

This document describes the development workflow for DataProfiler using the **staging** branch for development and **master** for production releases.

## 🌳 Branch Strategy

```
master (production)
  ↑
  └── staging (development/integration)
        ↑
        ├── feature/database-connectors
        ├── feature/arrow-integration
        ├── bugfix/unwrap-cleanup
        └── ... (other feature branches)
```

### Branch Purposes

- **`master`**: Production-ready code, stable releases only
- **`staging`**: Integration branch for development, pre-production testing
- **`feature/*`**: Individual feature development branches
- **`bugfix/*`**: Bug fix branches

## 🔄 Development Workflow

### 1. Starting New Work

```bash
# Always start from latest staging
git checkout staging
git pull origin staging

# Create feature branch
git checkout -b feature/your-feature-name

# Work on your feature...
git add .
git commit -m "feat: implement your feature"
git push -u origin feature/your-feature-name
```

### 2. Pull Request to Staging

1. **Create PR**: `feature/your-feature` → `staging`
2. **Automated checks run**: Comprehensive staging workflow
3. **Code review**: Team reviews the changes
4. **Merge**: Once approved and checks pass

### 3. Staging Integration

- All features are integrated and tested in `staging`
- Staging has comprehensive CI/CD with:
  - ✅ Code quality checks (format, lint, unwrap detection)
  - ✅ Multi-platform testing (Linux, Windows, macOS)
  - ✅ Performance monitoring
  - ✅ Memory leak detection
  - ✅ Security audits
  - ✅ Integration testing

### 4. Production Release

When staging is ready for release:

```bash
# Create production release PR
git checkout staging
git pull origin staging
gh pr create --base master --title "Release v0.3.2" --body "Production release from staging"
```

**Production PR includes**:
- 🔒 **Security audit**: Vulnerability scanning, secret detection
- 🚀 **Performance validation**: Benchmark tests, memory usage
- 📚 **Documentation checks**: Version consistency, changelog
- 🧪 **Comprehensive testing**: All platforms, all features
- ✅ **Production readiness gate**: Final approval process

## 🤖 GitHub Actions Workflows

### `staging-dev.yml` - Development Workflow
**Triggers**: Push/PR to `staging`

**Jobs**:
- **quick-check**: Fast feedback (format, lint, basic checks)
- **test-suite**: Comprehensive testing across platforms/Rust versions
- **performance-check**: Memory analysis, basic performance validation
- **security-audit**: Dependency vulnerabilities, unsafe code detection
- **dev-environment**: Development tools validation
- **integration-check**: CLI integration testing

### `staging-to-master.yml` - Production Release
**Triggers**: PR from `staging` to `master`

**Jobs**:
- **validate-pr**: Ensures PR comes from staging branch
- **production-tests**: Strict cross-platform testing
- **security-production-audit**: Enhanced security scanning
- **performance-validation**: Production performance benchmarks
- **documentation-check**: Release documentation validation
- **production-ready**: Final approval gate

### `ci.yml` - Master Protection
**Triggers**: Push/PR to `master` (direct PRs blocked)

Lightweight checks for master branch protection.

## 📋 Development Guidelines

### Code Quality Standards

```bash
# Before committing, run:
cargo fmt                           # Format code
cargo clippy -- -D warnings       # Lint with no warnings
cargo test                         # All tests pass

# Avoid in production code:
.unwrap()                          # Use proper error handling
.expect()                          # Return Result<T, Error>
panic!()                           # Graceful error handling

# Check before PR:
grep -r "unwrap()" src/           # Should return nothing
grep -r "TODO\|FIXME" src/        # Address before merge
```

### Commit Message Format

```
type(scope): description

Types: feat, fix, docs, style, refactor, test, chore
Scope: module/component affected
Description: imperative mood, lowercase

Examples:
feat(connectors): add PostgreSQL database connector
fix(parsing): handle malformed CSV headers gracefully
docs(api): update streaming API documentation
refactor(lib): extract column analysis to separate module
```

### Testing Requirements

- **Unit tests**: New code must include unit tests
- **Integration tests**: Major features need integration tests
- **Error path testing**: Test failure scenarios
- **Performance tests**: Benchmark critical paths
- **Documentation tests**: Ensure doc examples work

### Performance Considerations

- Profile memory usage for large file processing
- Benchmark performance-critical changes
- Use SIMD optimizations where applicable
- Avoid unnecessary cloning in hot paths

## 🚀 Release Process

### Preparing a Release

1. **Update version** in `Cargo.toml`
2. **Update CHANGELOG.md** with changes
3. **Test thoroughly** in staging environment
4. **Create release PR**: `staging` → `master`
5. **Production checks pass**
6. **Merge to master**
7. **Tag release**: `git tag v0.3.2 && git push origin v0.3.2`

### Version Numbering

Follow [Semantic Versioning](https://semver.org/):
- `MAJOR.MINOR.PATCH`
- **MAJOR**: Breaking changes
- **MINOR**: New features (backward compatible)
- **PATCH**: Bug fixes (backward compatible)

## 🛡️ Branch Protection Rules

### Master Branch Protection
- ✅ Require PR reviews
- ✅ Require status checks to pass
- ✅ Require branches to be up to date
- ✅ Restrict pushes to admins only
- ✅ Require PRs from staging branch only

### Staging Branch Protection
- ✅ Require status checks to pass
- ✅ Allow development team push access
- ✅ Require PR reviews for external contributors

## 🐛 Troubleshooting

### Common Issues

**Q: My PR to staging failed the unwrap() check**
```bash
# Find and fix unwrap calls
grep -r "\.unwrap()" src/
# Replace with proper error handling
```

**Q: Performance check failed**
```bash
# Profile your changes
cargo build --release
time ./target/release/dataprof-cli large_file.csv --quality
# Compare with baseline performance
```

**Q: Memory leak detected**
```bash
# Test with AddressSanitizer
RUSTFLAGS="-Zsanitizer=address" cargo test
# Check unsafe code blocks for proper cleanup
```

### Getting Help

- 💬 **Discussions**: GitHub Discussions for questions
- 🐛 **Issues**: GitHub Issues for bugs/feature requests
- 📧 **Maintainers**: Tag @AndreaBozzo for urgent issues
- 📖 **Documentation**: Check docs/ directory

## 📊 Monitoring & Metrics

### CI/CD Metrics
- ✅ **Build success rate**: Target >95%
- 🚀 **Build time**: Target <10 minutes for full suite
- 🧪 **Test coverage**: Target >80% on core modules
- 🔒 **Security scan**: Zero high-severity vulnerabilities

### Performance Metrics
- ⚡ **Processing speed**: Monitor throughput trends
- 💾 **Memory usage**: Track memory efficiency
- 📈 **Regression detection**: Alert on >10% performance drops

---

## 🎯 Quick Reference

```bash
# Start new feature
git checkout staging && git pull origin staging
git checkout -b feature/my-feature

# Daily development
cargo fmt && cargo clippy && cargo test
git add . && git commit -m "feat: my changes"
git push origin feature/my-feature

# Create PR to staging
gh pr create --base staging --title "Add my feature"

# Prepare production release
git checkout staging && git pull origin staging
gh pr create --base master --title "Release v0.3.x"
```

Happy coding! 🦀✨
