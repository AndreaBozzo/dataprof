# DataProfiler Troubleshooting Guide

Common issues and solutions for DataProfiler development and usage.

## 🚀 Quick Diagnostics

When encountering issues, start with these diagnostic commands:

```bash
# Check system information
cargo run -- --engine-info

# Verify development setup
just setup

# Run comprehensive health check
just quality

# Check database connectivity
just db-status
```

## 🏗️ Setup and Installation Issues

### Rust Installation Problems

#### Issue: `rustc` command not found
```
error: command not found: rustc
```

**Solutions**:
```bash
# Install Rust via rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source ~/.cargo/env

# Verify installation
rustc --version
cargo --version

# Update PATH if needed (add to ~/.bashrc or ~/.zshrc)
export PATH="$HOME/.cargo/bin:$PATH"
```

#### Issue: Outdated Rust version
```
error: package requires Rust 1.70+ but you have 1.65
```

**Solutions**:
```bash
# Update Rust to latest stable
rustup update stable
rustup default stable

# Verify version
rustc --version
```

#### Issue: Missing Rust components
```
error: component 'rustfmt' is not available
```

**Solutions**:
```bash
# Install missing components
rustup component add rustfmt clippy rust-src rust-analyzer

# Verify components
rustup component list --installed
```

### Platform-Specific Issues

#### Windows: PowerShell Execution Policy
```
cannot be loaded because running scripts is disabled on this system
```

**Solutions**:
```powershell
# Option 1: Allow execution for current user
Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser

# Option 2: Bypass for specific script
PowerShell -ExecutionPolicy Bypass -File scripts/setup-dev.ps1

# Option 3: Use Git Bash instead (recommended)
# Install Git for Windows and use Git Bash terminal
```

#### Windows: WSL2 Issues
```
error: could not create directory '/mnt/c/.../.cargo'
```

**Solutions**:
```bash
# Use WSL2 filesystem instead of Windows filesystem
cd ~/
git clone <repo-url>
cd dataprof

# Alternative: Fix permissions on Windows filesystem
sudo chmod -R 755 /mnt/c/path/to/project
```

#### macOS: Command Line Tools Missing
```
error: Microsoft Visual C++ 14.0 is required
xcrun: error: invalid active developer path
```

**Solutions**:
```bash
# Install Xcode Command Line Tools
xcode-select --install

# Verify installation
xcode-select -p
```

#### Linux: Missing System Dependencies
```
error: failed to run custom build command for 'openssl-sys'
```

**Solutions**:
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install build-essential pkg-config libssl-dev

# CentOS/RHEL/Fedora
sudo yum groupinstall "Development Tools"
sudo yum install pkgconfig openssl-devel

# Or use dnf on newer versions
sudo dnf groupinstall "Development Tools"
sudo dnf install pkgconfig openssl-devel
```

## 🛠️ Build and Compilation Issues

### Dependency Issues

#### Issue: Cargo lock conflicts
```
error: failed to select a version for the requirement `serde = "^1.0"`
```

**Solutions**:
```bash
# Update Cargo.lock
cargo update

# Clean and rebuild
cargo clean
cargo build

# If still failing, remove lock file and regenerate
rm Cargo.lock
cargo build
```

#### Issue: Feature flag conflicts
```
error: Package X doesn't have feature Y
```

**Solutions**:
```bash
# Check available features
cargo metadata --format-version 1 | jq '.packages[0].features'

# Build with specific features
cargo build --features "database,arrow"
cargo build --no-default-features --features minimal

# Check feature dependencies
cargo tree --format "{p} {f}"
```

#### Issue: Outdated dependencies
```
warning: package X has been superseded by Y
```

**Solutions**:
```bash
# Check for outdated dependencies
cargo outdated

# Update specific dependency
cargo update -p package_name

# Update all dependencies (be careful)
cargo update

# Check for unused dependencies
cargo machete
```

### Memory and Performance Issues

#### Issue: Out of memory during compilation
```
error: could not compile due to previous error: out of memory
```

**Solutions**:
```bash
# Reduce parallel compilation jobs
export CARGO_BUILD_JOBS=1
cargo build

# Or set in .cargo/config.toml
mkdir -p ~/.cargo
echo "[build]" >> ~/.cargo/config.toml
echo "jobs = 2" >> ~/.cargo/config.toml

# Use release mode for less memory usage
cargo build --release
```

#### Issue: Slow compilation times
```
# Compilation takes extremely long
```

**Solutions**:
```bash
# Enable incremental compilation
export CARGO_INCREMENTAL=1

# Use faster linker (Linux)
sudo apt-get install lld
export RUSTFLAGS="-C link-arg=-fuse-ld=lld"

# Use faster linker (macOS)
export RUSTFLAGS="-C link-arg=-fuse-ld=/usr/bin/ld"

# Parallel builds
export CARGO_BUILD_JOBS=$(nproc)

# Use sccache for cached builds
cargo install sccache
export RUSTC_WRAPPER=sccache
```

## 🐳 Container and Database Issues

### Docker Issues

#### Issue: Docker daemon not running
```
error: Cannot connect to the Docker daemon
```

**Solutions**:
```bash
# Start Docker service (Linux)
sudo systemctl start docker
sudo systemctl enable docker

# Start Docker Desktop (macOS/Windows)
# Open Docker Desktop application

# Check Docker status
docker version
docker ps
```

#### Issue: Permission denied for Docker
```
error: permission denied while trying to connect to Docker daemon
```

**Solutions**:
```bash
# Add user to docker group (Linux)
sudo usermod -aG docker $USER
newgrp docker

# Verify permission
docker ps

# Alternative: use sudo (not recommended for development)
sudo docker ps
```

#### Issue: Dev container fails to build
```
error: failed to build dev container
```

**Solutions**:
```bash
# Rebuild container without cache
docker system prune -f
code .
# Command palette: "Dev Containers: Rebuild Container"

# Check container logs
docker logs <container_id>

# Manual container build
cd .devcontainer
docker build -t dataprof-dev .
```

### Database Issues

#### Issue: Database connection refused
```
error: connection refused at localhost:5432
```

**Solutions**:
```bash
# Check if databases are running
just db-status
docker ps

# Start databases
just db-setup

# Check database logs
just db-logs postgres
just db-logs mysql

# Reset database containers
just db-reset
```

#### Issue: Database authentication failed
```
error: authentication failed for user "dataprof"
```

**Solutions**:
```bash
# Verify database configuration
cat .devcontainer/docker-compose.yml

# Reset database with fresh credentials
just db-teardown
just db-setup

# Manual connection test
docker exec -it dataprof-postgres-dev psql -U dataprof -d dataprof_test
```

#### Issue: Database port conflicts
```
error: port 5432 already in use
```

**Solutions**:
```bash
# Check what's using the port
lsof -i :5432  # Linux/macOS
netstat -an | grep :5432  # Windows

# Stop conflicting service
sudo systemctl stop postgresql  # Linux system PostgreSQL

# Use different ports in development
# Edit .devcontainer/docker-compose.yml
ports:
  - "15432:5432"  # PostgreSQL on 15432
  - "13306:3306"  # MySQL on 13306
```

## 🧪 Testing Issues

### Test Failures

#### Issue: Tests fail with file not found
```
error: No such file or directory: 'tests/data/sample.csv'
```

**Solutions**:
```bash
# Generate test data
cargo run --bin generate_test_data

# Check test data location
ls tests/fixtures/standard_datasets/

# Run from project root
cd /path/to/dataprof
cargo test

# Verify working directory in tests
pwd
```

#### Issue: Database tests fail
```
error: connection to server at "localhost" (127.0.0.1), port 5432 failed
```

**Solutions**:
```bash
# Ensure databases are running for tests
just db-setup
sleep 10  # Wait for startup

# Run database tests specifically
just test-db

# Run all tests with database setup
just test-all-db

# Check database connectivity
just db-connect-postgres
\q  # Exit if successful
```

#### Issue: Performance tests are flaky
```
error: benchmark exceeded maximum time
```

**Solutions**:
```bash
# Run benchmarks on dedicated hardware
cargo bench --bench unified_benchmarks

# Increase benchmark timeout
export BENCH_TIMEOUT=300  # 5 minutes

# Run benchmarks multiple times for average
for i in {1..5}; do cargo bench --bench statistical_benchmark; done

# Check system load during benchmarks
htop
```

### Memory and Resource Issues

#### Issue: Tests run out of memory
```
error: memory allocation failed
```

**Solutions**:
```bash
# Run tests sequentially
cargo test -- --test-threads=1

# Increase available memory
export RUST_MIN_STACK=8388608  # 8MB stack

# Skip memory-intensive tests in CI
cargo test --exclude slow_tests

# Run specific test suites
just test  # Unit tests only
cargo test --lib  # Library tests only
```

#### Issue: File handle exhaustion
```
error: Too many open files
```

**Solutions**:
```bash
# Check current limits
ulimit -n

# Increase file handle limit
ulimit -n 4096

# Make permanent (Linux)
echo "* soft nofile 4096" | sudo tee -a /etc/security/limits.conf
echo "* hard nofile 4096" | sudo tee -a /etc/security/limits.conf

# Restart terminal/session for changes to take effect
```

## 🔧 IDE and Editor Issues

### VS Code Issues

#### Issue: rust-analyzer not working
```
error: rust-analyzer server crashed
```

**Solutions**:
```bash
# Restart rust-analyzer
# Command Palette: "Rust Analyzer: Restart Server"

# Update rust-analyzer
rustup component add rust-analyzer

# Clear rust-analyzer cache
# Command Palette: "Rust Analyzer: Clear Cache and Restart"

# Check rust-analyzer logs
# Command Palette: "Rust Analyzer: Show Logs"

# Reinstall extension
code --uninstall-extension rust-lang.rust-analyzer
code --install-extension rust-lang.rust-analyzer
```

#### Issue: Dev container won't start
```
error: Failed to create container
```

**Solutions**:
```bash
# Check Docker is running
docker version

# Clear container cache
docker system prune -f

# Rebuild container
# Command Palette: "Dev Containers: Rebuild Container"

# Check container requirements
cat .devcontainer/devcontainer.json

# Use fallback setup
just setup  # Native development setup
```

#### Issue: Debugging not working
```
error: could not launch debugger
```

**Solutions**:
```bash
# Install CodeLLDB extension
code --install-extension vadimcn.vscode-lldb

# Build debug version
cargo build

# Check launch configuration
cat .vscode/dataprof.code-workspace

# Manual debug
gdb ./target/debug/dataprof-cli
```

### LSP Issues

#### Issue: Language server features missing
```
warning: no hover information available
```

**Solutions**:
```bash
# Ensure rust-src component is installed
rustup component add rust-src

# Regenerate Cargo.lock
cargo check

# Clear language server cache
rm -rf target/
cargo check

# IDE-specific restart
# VS Code: Restart rust-analyzer
# Vim: :LspRestart
# Emacs: lsp-workspace-restart
```

## 🚀 Performance Issues

### Runtime Performance

#### Issue: Slow file processing
```
warning: processing large files is slow
```

**Solutions**:
```bash
# Use release build for performance testing
cargo build --release
./target/release/dataprof-cli large_file.csv

# Check available memory
free -h  # Linux
vm_stat  # macOS

# Profile memory usage
just profile-memory large_file.csv

# Use streaming engine explicitly
cargo run -- large_file.csv --engine streaming

# Check for memory leaks
RUSTFLAGS="-Zsanitizer=address" cargo run -- file.csv
```

#### Issue: High memory usage
```
error: process exceeded memory limit
```

**Solutions**:
```bash
# Use memory-efficient engine
cargo run -- file.csv --engine memory-efficient

# Process in chunks
cargo run -- file.csv --chunk-size 1000000

# Monitor memory usage
valgrind --tool=massif ./target/release/dataprof-cli file.csv

# Reduce concurrent operations
export RAYON_NUM_THREADS=2
cargo run -- file.csv
```

### Build Performance

#### Issue: Long compile times
```
# Builds take too long
```

**Solutions**:
```bash
# Use parallel builds
export CARGO_BUILD_JOBS=$(nproc)

# Enable incremental compilation
export CARGO_INCREMENTAL=1

# Use faster linker
# Linux
sudo apt-get install lld clang
export RUSTFLAGS="-C link-arg=-fuse-ld=lld"

# Cache builds with sccache
cargo install sccache
export RUSTC_WRAPPER=sccache
sccache --show-stats
```

## 🔒 Security and Authentication

### Dependency Security

#### Issue: Security vulnerabilities in dependencies
```
error: security advisory found
```

**Solutions**:
```bash
# Audit dependencies
cargo audit

# Update vulnerable dependencies
cargo update

# Check for specific advisories
cargo audit --db advisory-db

# Fix or accept specific vulnerabilities
cargo audit --ignore RUSTSEC-YYYY-NNNN
```

### Database Security

#### Issue: Database connection security warnings
```
warning: SSL connection not established
```

**Solutions**:
```bash
# Use SSL connections in production
export DATABASE_URL="postgresql://user:pass@host/db?sslmode=require"

# Configure SSL in development
# Edit database connection strings in .env

# Check certificate validity
openssl s_client -connect hostname:5432 -starttls postgres
```

## 🌐 Network and Connectivity

### Network Issues

#### Issue: Cannot download dependencies
```
error: failed to download packages
```

**Solutions**:
```bash
# Check internet connectivity
ping crates.io

# Configure proxy if needed
export https_proxy=http://proxy.company.com:8080
export http_proxy=http://proxy.company.com:8080

# Use alternative registry
echo '[source.crates-io]' >> ~/.cargo/config.toml
echo 'replace-with = "mirror"' >> ~/.cargo/config.toml
echo '[source.mirror]' >> ~/.cargo/config.toml
echo 'registry = "https://mirror.example.com/git/index"' >> ~/.cargo/config.toml

# Retry with verbose output
cargo build --verbose
```

#### Issue: Slow dependency downloads
```
# Downloads are extremely slow
```

**Solutions**:
```bash
# Use sparse registry (Rust 1.68+)
export CARGO_REGISTRIES_CRATES_IO_PROTOCOL=sparse

# Configure in Cargo config
mkdir -p ~/.cargo
echo '[registries.crates-io]' >> ~/.cargo/config.toml
echo 'protocol = "sparse"' >> ~/.cargo/config.toml

# Use regional mirror
# Add to ~/.cargo/config.toml
[source.crates-io]
replace-with = "tuna"

[source.tuna]
registry = "https://mirrors.tuna.tsinghua.edu.cn/git/crates.io-index.git"
```

## 🔍 Debugging Tips

### General Debugging

#### Enable verbose logging
```bash
# Environment variables for debugging
export RUST_LOG=debug
export RUST_BACKTRACE=1
export RUST_BACKTRACE=full  # Even more detail

# Application-specific logging
export RUST_LOG=dataprof=debug
export RUST_LOG=dataprof::engines=trace

# Run with debugging
cargo run -- file.csv --quality
```

#### Memory debugging
```bash
# Address sanitizer (detects memory errors)
RUSTFLAGS="-Zsanitizer=address" cargo run

# Memory leak detection
RUSTFLAGS="-Zsanitizer=leak" cargo run

# Thread sanitizer (detects data races)
RUSTFLAGS="-Zsanitizer=thread" cargo run

# Valgrind (Linux)
valgrind --leak-check=full ./target/debug/dataprof-cli file.csv
```

#### Performance profiling
```bash
# CPU profiling with perf (Linux)
perf record --call-graph=dwarf ./target/release/dataprof-cli file.csv
perf report

# Time profiling
time ./target/release/dataprof-cli file.csv

# Memory profiling
/usr/bin/time -v ./target/release/dataprof-cli file.csv
```

## 📞 Getting Help

### Self-Help Resources

1. **Check the logs**: Most issues have helpful error messages
2. **Search existing issues**: [GitHub Issues](https://github.com/user/dataprof/issues)
3. **Read documentation**: Start with [Development Guide](./DEVELOPMENT.md)
4. **Run diagnostics**: Use `just quality` for comprehensive checks

### Reporting Issues

When reporting issues, include:

```bash
# System information
cargo run -- --engine-info

# Rust version
rustc --version
cargo --version

# Operating system
uname -a  # Linux/macOS
systeminfo  # Windows

# Error reproduction
# Minimal example that reproduces the issue
# Complete error message
# Steps to reproduce
```

### Community Support

- **GitHub Discussions**: For questions and general discussion
- **GitHub Issues**: For bugs and feature requests
- **Documentation**: Comprehensive guides in `docs/` directory

### Emergency Debugging

If nothing works, try the nuclear option:

```bash
# Complete clean slate
cargo clean
rm -rf target/ Cargo.lock
rm -rf ~/.cargo/registry/cache/
rustup update

# Fresh clone
cd ..
rm -rf dataprof/
git clone <repo-url>
cd dataprof/
just setup-complete
```

## 📚 Additional Resources

- [Rust Troubleshooting Guide](https://doc.rust-lang.org/book/appendix-04-useful-development-tools.html)
- [Cargo Troubleshooting](https://doc.rust-lang.org/cargo/reference/troubleshooting.html)
- [Docker Troubleshooting](https://docs.docker.com/engine/troubleshooting/)
- [VS Code Troubleshooting](https://code.visualstudio.com/docs/supporting/troubleshooting)
- [DataProfiler Development Guide](./DEVELOPMENT.md)
- [Testing Guide](./TESTING.md)
- [IDE Setup Guide](./IDE_SETUP.md)