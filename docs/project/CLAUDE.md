# DataProfiler CLI - Project Documentation

## Overview

Fast data profiling and quality checking library and CLI tool for large datasets built in Rust.

## Rules

Start simple, then iterate.
Best practices, always.
TDD, write tests before implementation
Clean code, code must be readable and maintainable by humans
SOLID principles, guarantee modular scalable architectures
Proven solutions to recurring problems
Security is non negotable, OWASP

## Essential Commands

```bash
# Build & Test
cargo test --lib                    # Quick unit tests
cargo clippy -- -D warnings         # Code quality
cargo fmt                           # Format code

# Development
cargo run -- --engine-info          # Show system info
cargo run -- data.csv --benchmark   # Performance test
pre-commit run --all-files          # Full QA

# Quality Checks
grep -r "unwrap()" src/             # Should return nothing
cargo test --features arrow         # Test with Arrow
```

## Architecture

- **Engines**: Streaming, MemoryEfficient, TrueStreaming, Arrow
- **Selection**: Intelligent auto-selection based on file/system characteristics
- **Fallback**: Transparent recovery with logging
- **API**: Zero breaking changes, backward compatible
