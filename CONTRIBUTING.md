# Contributing to dataprof

Thank you for considering contributing to dataprof! We welcome contributions from everyone. This document provides guidelines for contributing to the project.

## Quick Start for Contributors

### Prerequisites

- **Rust 1.80** or later ([install](https://rustup.rs/))
- **Cargo** (comes with Rust)
- **Git**

### Fork & Clone

1. Fork the repository on GitHub
2. Clone your fork:

   ```bash
   git clone https://github.com/YOUR-USERNAME/dataprof.git
   cd dataprof
   ```

3. Add upstream remote:

   ```bash
   git remote add upstream https://github.com/AndreaBozzo/dataprof.git
   ```

### Build & Test

```bash
# Build the project
cargo build

# Run tests
cargo test

# Check everything locally before submitting PR
cargo fmt --all
cargo clippy --all --all-targets
```

## Development Workflow

### Create a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

Use descriptive branch names:
- `feature/add-json-support` - for new features
- `fix/handle-empty-files` - for bug fixes
- `docs/update-api-reference` - for documentation
- `perf/optimize-memory-usage` - for performance improvements

### Code Changes

1. **Make your changes** - keep commits focused and logical
2. **Write tests** - new features should have corresponding tests
3. **Test locally**:
   ```bash
   cargo test --all
   ```
4. **Format code**:
   ```bash
   cargo fmt --all
   ```
5. **Lint checking**:
   ```bash
   cargo clippy --all --all-targets -- -D warnings
   ```
6. **Build release** (verify no warnings):
   ```bash
   cargo build --release
   ```

### Commit Messages

Write clear, descriptive commit messages:

```
Short summary (50 chars max)

More detailed explanation if needed (wrap at 72 chars).
Explain what changed and why, not how.

Closes #123
```

### Submit Pull Request

1. **Push to your fork**:
   ```bash
   git push origin feature/your-feature-name
   ```

2. **Create PR on GitHub** with:
   - Clear title describing the change
   - Description of what changed and why
   - Reference to related issues (e.g., "Closes #123")
   - Screenshots/examples for user-visible changes

3. **CI will automatically run**:
   - Tests
   - Code formatting checks
   - Lint checks
   - Build verification

4. **Address feedback** - we may request changes before merging

## Pull Request Guidelines

### Before Submitting

✅ **Checklist:**
- [ ] Code builds without warnings
- [ ] All tests pass (`cargo test`)
- [ ] Code is formatted (`cargo fmt --all`)
- [ ] No linting errors (`cargo clippy`)
- [ ] Added tests for new functionality
- [ ] Updated documentation if needed
- [ ] Commit messages are clear and descriptive

### PR Description Template

```markdown
## Description
Brief summary of changes

## Related Issues
Closes #123

## Changes Made
- Change 1
- Change 2
- Change 3

## Testing
How did you test this?

## Breaking Changes
Any breaking changes? Document them here.
```

### Review Process

1. **Automated checks** run first (format, lint, tests)
2. **Code review** - maintainers will review your changes
3. **Feedback loop** - we may ask for adjustments
4. **Merge** - once approved and checks pass

## Feature Requests and Bug Reports

### 🐛 Bug Reports

Before submitting, check if the issue already exists.

Include:
- **Rust version**: `rustc --version`
- **OS and version**: `uname -a` (Linux/Mac) or Windows version
- **Steps to reproduce** the issue
- **Expected vs actual behavior**
- **Sample data** (anonymized if needed)
- **Error messages** or stack traces

### ✨ Feature Requests

Describe:
- What feature you need
- Why you need it
- Potential use cases
- Any implementation ideas you have

### Questions?

- Check [DEVELOPMENT.md](docs/DEVELOPMENT.md) for architecture details
- Read [docs/](docs/) for detailed documentation
- Open a discussion issue if you're unsure

## Code Guidelines

### Architecture & Design

- **Modularity**: Keep modules focused and single-purpose
- **Composition**: Prefer composition over inheritance
- **Error Handling**: Handle errors gracefully with meaningful messages
- **Performance**: Consider memory usage for large files and streaming
- **Compatibility**: Maintain backward compatibility when possible

### Testing Standards

- **Unit tests**: Test individual functions and modules
- **Integration tests**: Test end-to-end workflows
- **Edge cases**: Test with:
  - Empty files
  - Very large files
  - Malformed data
  - Different encodings
  - Special characters

Example:
```rust
#[test]
fn test_handles_empty_file() {
    let result = process_csv("");
    assert!(result.is_err());
}
```

### Documentation

- Document public APIs with doc comments
- Include examples in doc comments
- Update README.md for user-facing changes
- Update [docs/](docs/) for architectural changes

## Development Tips

### Useful Commands

```bash
# Run tests in watch mode
cargo watch -x test

# Generate documentation
cargo doc --open

# Check for unused dependencies
cargo clippy -- -W clippy::all

# Run benchmarks
cargo bench

# Profile memory usage
cargo build --release && valgrind ./target/release/dataprof-cli analyze data.csv
```

### Project Structure

- `src/` - Main Rust library code
- `src/cli/` - Command-line interface
- `src/database/` - Database integration
- `src/analysis/` - Core analysis logic
- `python/` - Python bindings
- `tests/` - Integration tests
- `docs/` - Documentation

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
