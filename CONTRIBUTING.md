# Contributing to DataProfiler

Thank you for considering contributing to DataProfiler CLI! This document provides guidelines for contributing to the project.

## Development Setup

### Prerequisites

- Rust 1.70 or later
- Cargo (comes with Rust)
- Git

### Getting Started

1. Fork the repository on GitHub
2. Clone your fork locally:

   ```bash
   git clone https://github.com/your-username/dataprof.git
   cd dataprof
   ```

3. Build the project:

   ```bash
   cargo build
   ```

4. Run tests to ensure everything works:

   ```bash
   cargo test
   ```

## Development Workflow

### Code Style

- Use `cargo fmt` to format your code
- Run `cargo clippy` to catch common mistakes
- Follow Rust naming conventions

### Testing

- Write tests for new features
- Run the full test suite: `cargo test`
- Test with sample data files to ensure real-world functionality

### Making Changes

1. Create a feature branch: `git checkout -b feature/your-feature-name`
2. Make your changes
3. Add tests for new functionality
4. Ensure all tests pass: `cargo test`
5. Format code: `cargo fmt`
6. Check for lints: `cargo clippy`
7. Commit your changes with clear, descriptive messages
8. Push to your fork and create a Pull Request

## Pull Request Guidelines

### Before Submitting

- Ensure your code builds without warnings
- All tests pass
- Code is properly formatted
- Include documentation for new features

### PR Description

Please include:

- Clear description of the changes
- Motivation for the changes
- Any breaking changes
- Screenshots/examples for UI changes

## Feature Requests and Bug Reports

### Bug Reports

Please include:

- Rust version (`rustc --version`)
- Operating system and version
- Steps to reproduce
- Expected vs actual behavior
- Sample data (if applicable, anonymized)

### Feature Requests

- Clear description of the feature
- Use case/motivation
- Any implementation ideas

## Code Guidelines

### Architecture Principles

- Keep modules focused and small
- Prefer composition over inheritance
- Handle errors gracefully
- Performance matters - consider memory usage for large files
- Maintain backward compatibility when possible

### Testing

- Unit tests for individual functions
- Integration tests for end-to-end functionality
- Performance tests for large file handling

## Getting Help

- Open an issue for questions
- Check existing issues before creating new ones
- Be respectful and patient

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
