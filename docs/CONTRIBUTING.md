# Contributing to dataprof

Thank you for considering contributing to dataprof! We welcome contributions from everyone. This document provides guidelines for contributing to the project.

## Quick Start for Contributors

### Prerequisites

- **Rust 1.96** or later ([install](https://rustup.rs/))
- **Cargo** (comes with Rust)
- **Python 3.10** or later
- **uv** for Python dependency and test commands ([install](https://docs.astral.sh/uv/))
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
# Install Python dev dependencies
uv sync

# Build and install the local Python extension into the uv environment
uv run maturin develop

# Run focused Python API tests
uv run pytest python/tests/test_python_api.py -q

# Run focused Rust tests
cargo test -p dataprof-core
cargo test -p dataprof-metrics

# Check everything locally before submitting PR
cargo fmt --all
uv run ruff check python/dataprof python/tests
```

You do not need to run every expensive workspace check for a small docs or
Python-only PR. Prefer a focused test command that matches your change, and
list exactly what you ran in the PR body.

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
   uv run pytest python/tests/test_python_api.py -q
   cargo test -p dataprof-core
   ```
4. **Format code**:
   ```bash
   cargo fmt --all
   ```
5. **Lint checking**:
   ```bash
   uv run ruff check python/dataprof python/tests
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

### Picking an Issue

If you are new to the project, start with issues labelled `good first issue`,
`difficulty: beginner`, or `mentor available`. These are expected to have a
small scope and enough context for a first PR.

Before opening a PR, leave a short comment on the issue saying which slice you
want to take. Some larger issues are intentionally split into independent
subtasks, so a focused partial PR is welcome.

Good first issues in this repository usually mean:

- no deep knowledge of the Rust engine internals is required
- the desired behavior is described in the issue body
- maintainers are happy to answer API or setup questions
- a small PR is better than a sweeping rewrite

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
   - A short list of the commands you ran

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
- [ ] Relevant tests pass
- [ ] Code is formatted (`cargo fmt --all`)
- [ ] No linting errors in the area you touched
- [ ] Added tests for new functionality
- [ ] Updated documentation if needed
- [ ] Commit messages are clear and descriptive

### PR Size

Small PRs are easier to review and merge. A good default is one behavior change
or one documentation/example slice per PR. If an issue lists several scenarios,
it is fine to implement one scenario and leave the rest for follow-ups.

Avoid mixing unrelated cleanup with feature work. If you notice something
nearby, open a follow-up issue or separate PR.

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
3. **Feedback loop** - we may ask for adjustments, but we try to explain the
   project constraint behind each request
4. **Merge** - once approved and checks pass

Maintainer review style is direct but collaborative: expect requests to keep
the public API small, preserve backward compatibility when possible, and add
tests only where they protect real behavior.

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
# Install/update the Python dev environment
uv sync

# Rebuild the local Python extension after Rust/PyO3 changes
uv run maturin develop

# Run the main Python API regression file
uv run pytest python/tests/test_python_api.py -q

# Run a focused Python test
uv run pytest python/tests/test_python_api.py::TestToLlmContext -v -ra

# Generate documentation
cargo doc --open

# Run targeted Rust package tests
cargo test -p dataprof-core
cargo test -p dataprof-python

# Run lint checks
uv run ruff check python/dataprof python/tests
cargo clippy --all --all-targets -- -D warnings

# Run benchmarks
cargo bench

# Run library checks
cargo check --workspace --all-targets
```

### Project Structure

- `crates/dataprof/src/` - Public `dataprof` facade source
- `crates/` - Workspace crates that own core, parser, engine, database, metrics, and runtime code
- `crates/dataprof-db/` - Database connectors
- `crates/dataprof-metrics/` - Core analysis and statistics logic
- `crates/dataprof-python/` - PyO3 extension crate
- `python/` - Python package sources and tests
- `tests/` - Integration tests
- `docs/` - Documentation

## License

By contributing, you agree that your contributions will be licensed under the
same dual license as the project: MIT OR Apache-2.0.
