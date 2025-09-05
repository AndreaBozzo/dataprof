#!/bin/bash
# Development setup script for DataProfiler v0.3.0

set -e

echo "🔧 Setting up development environment for DataProfiler v0.3.0..."

# Check if pre-commit is available
if ! command -v pre-commit &> /dev/null; then
    echo "⚠️ pre-commit not found. Installing..."

    # Try different installation methods
    if command -v pip &> /dev/null; then
        pip install pre-commit
    elif command -v pip3 &> /dev/null; then
        pip3 install pre-commit
    elif command -v python -m pip &> /dev/null; then
        python -m pip install pre-commit
    elif command -v python3 -m pip &> /dev/null; then
        python3 -m pip install pre-commit
    else
        echo "❌ Could not install pre-commit. Please install it manually:"
        echo "   pip install pre-commit"
        echo "   Or visit: https://pre-commit.com/#installation"
        exit 1
    fi
fi

# Install pre-commit hooks
echo "📦 Installing pre-commit hooks..."
pre-commit install

# Install commit-msg hook for conventional commits
echo "📝 Installing commit-msg hook..."
pre-commit install --hook-type commit-msg

# Check Rust toolchain
echo "🦀 Checking Rust toolchain..."
if ! command -v cargo &> /dev/null; then
    echo "❌ Rust/Cargo not found. Please install Rust first:"
    echo "   https://rustup.rs/"
    exit 1
fi

# Install additional Rust tools for development
echo "🔧 Installing Rust development tools..."

# Install rustfmt if not present
if ! rustup component list --installed | grep -q rustfmt; then
    echo "  📦 Installing rustfmt..."
    rustup component add rustfmt
fi

# Install clippy if not present
if ! rustup component list --installed | grep -q clippy; then
    echo "  📦 Installing clippy..."
    rustup component add clippy
fi

# Run initial checks
echo "✅ Running initial code quality checks..."

echo "  🎨 Running cargo fmt..."
cargo fmt --all --check || {
    echo "  ⚠️ Code formatting issues found. Auto-fixing..."
    cargo fmt --all
    echo "  ✅ Code formatted successfully"
}

echo "  🔍 Running cargo clippy..."
cargo clippy --all-targets --all-features -- -D warnings || {
    echo "  ⚠️ Linting issues found. Please fix the warnings above."
    exit 1
}

echo "  🧪 Running cargo test (lib only)..."
cargo test --lib || {
    echo "  ❌ Tests failed. Please fix failing tests."
    exit 1
}

echo ""
echo "🎉 Development environment setup complete!"
echo ""
echo "📋 Available development commands:"
echo "   cargo fmt                    # Format code"
echo "   cargo clippy                 # Run linter"
echo "   cargo test                   # Run all tests"
echo "   cargo test --lib             # Run library tests only"
echo "   pre-commit run --all-files   # Run all pre-commit hooks"
echo "   pre-commit run cargo-fmt     # Run formatting only"
echo "   pre-commit run cargo-clippy  # Run linting only"
echo ""
echo "💡 Pre-commit hooks will now run automatically on each commit!"
echo "   To skip hooks temporarily: git commit --no-verify"
echo ""
