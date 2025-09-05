# Development setup script for DataProfiler v0.3.0 (PowerShell)

Write-Host "🔧 Setting up development environment for DataProfiler v0.3.0..." -ForegroundColor Blue

# Check if pre-commit is available
if (-not (Get-Command pre-commit -ErrorAction SilentlyContinue)) {
    Write-Host "⚠️ pre-commit not found. Installing..." -ForegroundColor Yellow

    # Try to install via pip
    if (Get-Command pip -ErrorAction SilentlyContinue) {
        pip install pre-commit
    } elseif (Get-Command pip3 -ErrorAction SilentlyContinue) {
        pip3 install pre-commit
    } elseif (Get-Command python -ErrorAction SilentlyContinue) {
        python -m pip install pre-commit
    } elseif (Get-Command python3 -ErrorAction SilentlyContinue) {
        python3 -m pip install pre-commit
    } else {
        Write-Host "❌ Could not install pre-commit. Please install it manually:" -ForegroundColor Red
        Write-Host "   pip install pre-commit"
        Write-Host "   Or visit: https://pre-commit.com/#installation"
        exit 1
    }
}

# Install pre-commit hooks
Write-Host "📦 Installing pre-commit hooks..." -ForegroundColor Blue
pre-commit install

# Install commit-msg hook for conventional commits
Write-Host "📝 Installing commit-msg hook..." -ForegroundColor Blue
pre-commit install --hook-type commit-msg

# Check Rust toolchain
Write-Host "🦀 Checking Rust toolchain..." -ForegroundColor Blue
if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
    Write-Host "❌ Rust/Cargo not found. Please install Rust first:" -ForegroundColor Red
    Write-Host "   https://rustup.rs/"
    exit 1
}

# Install additional Rust tools for development
Write-Host "🔧 Installing Rust development tools..." -ForegroundColor Blue

# Install rustfmt if not present
$rustfmtInstalled = rustup component list --installed | Select-String "rustfmt"
if (-not $rustfmtInstalled) {
    Write-Host "  📦 Installing rustfmt..." -ForegroundColor Blue
    rustup component add rustfmt
}

# Install clippy if not present
$clippyInstalled = rustup component list --installed | Select-String "clippy"
if (-not $clippyInstalled) {
    Write-Host "  📦 Installing clippy..." -ForegroundColor Blue
    rustup component add clippy
}

# Run initial checks
Write-Host "✅ Running initial code quality checks..." -ForegroundColor Green

Write-Host "  🎨 Running cargo fmt..." -ForegroundColor Blue
$fmtResult = cargo fmt --all --check 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ⚠️ Code formatting issues found. Auto-fixing..." -ForegroundColor Yellow
    cargo fmt --all
    Write-Host "  ✅ Code formatted successfully" -ForegroundColor Green
}

Write-Host "  🔍 Running cargo clippy..." -ForegroundColor Blue
$clippyResult = cargo clippy --all-targets --all-features -- -D warnings 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ⚠️ Linting issues found. Please fix the warnings above." -ForegroundColor Yellow
    Write-Host $clippyResult -ForegroundColor Red
}

Write-Host "  🧪 Running cargo test (lib only)..." -ForegroundColor Blue
$testResult = cargo test --lib 2>&1
if ($LASTEXITCODE -ne 0) {
    Write-Host "  ❌ Tests failed. Please fix failing tests." -ForegroundColor Red
    Write-Host $testResult -ForegroundColor Red
}

Write-Host ""
Write-Host "🎉 Development environment setup complete!" -ForegroundColor Green
Write-Host ""
Write-Host "📋 Available development commands:" -ForegroundColor Cyan
Write-Host "   cargo fmt                    # Format code"
Write-Host "   cargo clippy                 # Run linter"
Write-Host "   cargo test                   # Run all tests"
Write-Host "   cargo test --lib             # Run library tests only"
Write-Host "   pre-commit run --all-files   # Run all pre-commit hooks"
Write-Host "   pre-commit run cargo-fmt     # Run formatting only"
Write-Host "   pre-commit run cargo-clippy  # Run linting only"
Write-Host ""
Write-Host "💡 Pre-commit hooks will now run automatically on each commit!" -ForegroundColor Yellow
Write-Host "   To skip hooks temporarily: git commit --no-verify"
Write-Host ""
