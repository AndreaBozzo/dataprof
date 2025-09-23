#!/bin/bash
# Enhanced Development setup script for DataProfiler v0.4.0
# Provides robust error handling, logging, and flexible setup options

set -euo pipefail  # Exit on error, undefined vars, pipe failures

# Configuration
SCRIPT_VERSION="0.4.0"
LOG_FILE="/tmp/dataprof-setup-$(date +%Y%m%d_%H%M%S).log"
SETUP_MODE="${1:-full}"  # full, minimal, or update
FORCE_REINSTALL="${FORCE_REINSTALL:-false}"

# Colors for output
readonly RED='\033[0;31m'
readonly GREEN='\033[0;32m'
readonly YELLOW='\033[0;33m'
readonly BLUE='\033[0;34m'
readonly PURPLE='\033[0;35m'
readonly CYAN='\033[0;36m'
readonly NC='\033[0m' # No Color

# Logging functions
log() {
    local level="$1"
    shift
    local timestamp=$(date '+%Y-%m-%d %H:%M:%S')
    echo -e "${timestamp} [${level}] $*" | tee -a "$LOG_FILE"
}

log_info() { log "INFO" "${BLUE}$*${NC}"; }
log_warn() { log "WARN" "${YELLOW}$*${NC}"; }
log_error() { log "ERROR" "${RED}$*${NC}"; }
log_success() { log "SUCCESS" "${GREEN}$*${NC}"; }
log_debug() { [[ "${DEBUG:-false}" == "true" ]] && log "DEBUG" "${PURPLE}$*${NC}"; }

# Error handling
cleanup_on_error() {
    local exit_code=$?
    log_error "Setup failed with exit code $exit_code"
    log_info "Cleanup operations..."

    # Stop any running containers that might have been started
    if command -v docker-compose &> /dev/null; then
        docker-compose -f .devcontainer/docker-compose.yml down &> /dev/null || true
    fi

    log_info "Log file saved to: $LOG_FILE"
    exit $exit_code
}

trap cleanup_on_error ERR

# Utility functions
command_exists() {
    command -v "$1" &> /dev/null
}

version_ge() {
    # Compare versions: returns 0 if $1 >= $2
    printf '%s\n%s\n' "$2" "$1" | sort -V -C
}

get_version() {
    local cmd="$1"
    case "$cmd" in
        "docker")
            docker --version | sed 's/Docker version //' | cut -d',' -f1
            ;;
        "docker-compose")
            docker-compose --version | sed 's/docker-compose version //' | cut -d',' -f1
            ;;
        "rust")
            rustc --version | cut -d' ' -f2
            ;;
        "just")
            just --version | cut -d' ' -f2
            ;;
        *)
            echo "unknown"
            ;;
    esac
}

check_prerequisites() {
    log_info "🔍 Checking system prerequisites..."

    local missing_deps=()
    local os_type=$(uname -s)

    # Check OS compatibility
    case "$os_type" in
        "Linux"|"Darwin")
            log_success "✅ Operating system: $os_type (supported)"
            ;;
        "MINGW"*|"CYGWIN"*|"MSYS"*)
            log_success "✅ Operating system: Windows with Unix environment (supported)"
            ;;
        *)
            log_warn "⚠️ Operating system: $os_type (untested, may have issues)"
            ;;
    esac

    # Check essential commands
    local essential_commands=("git" "curl" "wget")
    for cmd in "${essential_commands[@]}"; do
        if command_exists "$cmd"; then
            log_success "✅ $cmd: available"
        else
            missing_deps+=("$cmd")
            log_error "❌ $cmd: not found"
        fi
    done

    # Check Python (required for pre-commit)
    if command_exists python3 || command_exists python; then
        local python_cmd=$(command_exists python3 && echo "python3" || echo "python")
        local python_version=$($python_cmd --version 2>&1 | cut -d' ' -f2)
        log_success "✅ Python: $python_version"

        # Check pip
        if $python_cmd -m pip --version &> /dev/null; then
            log_success "✅ pip: available"
        else
            log_warn "⚠️ pip: not available, will try alternative installation methods"
        fi
    else
        missing_deps+=("python3")
        log_error "❌ Python: not found"
    fi

    # Report missing dependencies
    if [[ ${#missing_deps[@]} -gt 0 ]]; then
        log_error "❌ Missing essential dependencies: ${missing_deps[*]}"
        log_info "Please install the missing dependencies and run the script again."

        case "$os_type" in
            "Linux")
                log_info "Ubuntu/Debian: sudo apt-get install ${missing_deps[*]}"
                log_info "CentOS/RHEL: sudo yum install ${missing_deps[*]}"
                ;;
            "Darwin")
                log_info "macOS: brew install ${missing_deps[*]}"
                ;;
        esac
        exit 1
    fi
}

install_rust() {
    log_info "🦀 Setting up Rust toolchain..."

    if command_exists rustc && command_exists cargo; then
        local rust_version=$(get_version rust)
        log_success "✅ Rust $rust_version already installed"

        # Update if requested
        if [[ "$FORCE_REINSTALL" == "true" ]] || [[ "$SETUP_MODE" == "update" ]]; then
            log_info "🔄 Updating Rust toolchain..."
            rustup update
        fi
    else
        log_info "📦 Installing Rust via rustup..."
        curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
        source "$HOME/.cargo/env" || export PATH="$HOME/.cargo/bin:$PATH"
    fi

    # Install required components
    log_info "🔧 Installing Rust components..."
    rustup component add rustfmt clippy

    # Install development tools
    local rust_tools=("cargo-tarpaulin" "cargo-machete" "just")
    for tool in "${rust_tools[@]}"; do
        if [[ "$FORCE_REINSTALL" == "true" ]] || ! command_exists "$tool"; then
            log_info "📦 Installing $tool..."
            cargo install "$tool" --force
        else
            log_success "✅ $tool: already installed"
        fi
    done
}

install_pre_commit() {
    log_info "🪝 Setting up pre-commit hooks..."

    local python_cmd="python3"
    command_exists python3 || python_cmd="python"

    # Install pre-commit if not available
    if ! command_exists pre-commit; then
        log_info "📦 Installing pre-commit..."

        # Try different installation methods
        if $python_cmd -m pip install pre-commit &> /dev/null; then
            log_success "✅ pre-commit installed via pip"
        elif pip3 install pre-commit &> /dev/null; then
            log_success "✅ pre-commit installed via pip3"
        elif pip install pre-commit &> /dev/null; then
            log_success "✅ pre-commit installed via pip"
        else
            log_error "❌ Failed to install pre-commit"
            log_info "Please install pre-commit manually:"
            log_info "  pip install pre-commit"
            log_info "  Or visit: https://pre-commit.com/#installation"
            exit 1
        fi
    else
        log_success "✅ pre-commit: already installed"
    fi

    # Install hooks
    log_info "🔗 Installing pre-commit hooks..."
    pre-commit install --install-hooks
    pre-commit install --hook-type commit-msg

    log_success "✅ Pre-commit hooks installed"
}

setup_docker() {
    if [[ "$SETUP_MODE" == "minimal" ]]; then
        log_info "⏭️ Skipping Docker setup in minimal mode"
        return 0
    fi

    log_info "🐳 Checking Docker setup..."

    if command_exists docker; then
        local docker_version=$(get_version docker)
        log_success "✅ Docker $docker_version found"

        # Check if Docker daemon is running
        if docker info &> /dev/null; then
            log_success "✅ Docker daemon is running"
        else
            log_warn "⚠️ Docker daemon is not running. Please start Docker."
            return 1
        fi
    else
        log_warn "⚠️ Docker not found. Database features will not be available."
        log_info "Install Docker from: https://docs.docker.com/get-docker/"
        return 1
    fi

    if command_exists docker-compose; then
        local compose_version=$(get_version docker-compose)
        log_success "✅ Docker Compose $compose_version found"
    else
        log_warn "⚠️ Docker Compose not found. Installing..."

        # Install Docker Compose
        local compose_url="https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)"
        sudo curl -L "$compose_url" -o /usr/local/bin/docker-compose
        sudo chmod +x /usr/local/bin/docker-compose

        log_success "✅ Docker Compose installed"
    fi
}

run_quality_checks() {
    log_info "🧪 Running initial quality checks..."

    # Format check and auto-fix
    log_info "  🎨 Checking code formatting..."
    if cargo fmt --all --check &> /dev/null; then
        log_success "  ✅ Code formatting: OK"
    else
        log_warn "  ⚠️ Code formatting issues found. Auto-fixing..."
        cargo fmt --all
        log_success "  ✅ Code formatting: fixed"
    fi

    # Clippy check
    log_info "  🔍 Running clippy linter..."
    if cargo clippy --all-targets --all-features -- -D warnings &> /dev/null; then
        log_success "  ✅ Clippy: no issues found"
    else
        log_error "  ❌ Clippy found issues. Please fix the warnings above."
        log_info "  Run 'cargo clippy --all-targets --all-features' for details"
        exit 1
    fi

    # Basic tests
    log_info "  🧪 Running basic tests..."
    if cargo test --lib &> /dev/null; then
        log_success "  ✅ Basic tests: passed"
    else
        log_error "  ❌ Tests failed. Please fix failing tests."
        log_info "  Run 'cargo test --lib' for details"
        exit 1
    fi

    log_success "✅ All quality checks passed!"
}

create_dev_config() {
    log_info "⚙️ Creating development configuration..."

    # Create .vscode settings if .vscode directory exists or user wants it
    if [[ -d ".vscode" ]] || [[ "${CREATE_VSCODE_CONFIG:-true}" == "true" ]]; then
        mkdir -p .vscode

        cat > .vscode/settings.json << 'EOF'
{
    "rust-analyzer.checkOnSave.command": "clippy",
    "rust-analyzer.checkOnSave.extraArgs": ["--all-targets", "--all-features"],
    "rust-analyzer.cargo.features": "all",
    "rust-analyzer.procMacro.enable": true,
    "rust-analyzer.cargo.loadOutDirsFromCheck": true,
    "editor.formatOnSave": true,
    "editor.codeActionsOnSave": {
        "source.fixAll.rust-analyzer": true
    },
    "files.watcherExclude": {
        "**/target/**": true
    }
}
EOF
        log_success "✅ VS Code settings created"
    fi

    # Create cargo config for faster builds
    mkdir -p .cargo
    cat > .cargo/config.toml << 'EOF'
[build]
target-dir = "target"
jobs = 0

[profile.dev]
opt-level = 0
debug = true
incremental = true
codegen-units = 256

[profile.dev.package."*"]
opt-level = 1
EOF
    log_success "✅ Cargo configuration created"
}

display_summary() {
    log_success "🎉 Development environment setup complete!"
    echo ""
    log_info "📋 Available commands:"
    echo "   just --list                  # Show all available commands"
    echo "   just setup-complete          # Complete environment setup with databases"
    echo "   just dev                     # Quick development cycle (fmt, build, test)"
    echo "   just quality                 # Full quality check pipeline"
    echo "   just db-setup                # Start development databases"
    echo "   just test-all                # Run comprehensive tests"
    echo ""
    log_info "🗃️ Database commands:"
    echo "   just db-setup                # Start PostgreSQL, MySQL, Redis"
    echo "   just db-setup-all            # Start all services including admin tools"
    echo "   just db-connect-postgres     # Connect to PostgreSQL"
    echo "   just db-connect-mysql        # Connect to MySQL"
    echo "   just db-status               # Check database service status"
    echo ""
    log_info "💡 Tips:"
    echo "   - Pre-commit hooks run automatically on commits"
    echo "   - Use 'git commit --no-verify' to skip hooks temporarily"
    echo "   - Development containers are available in .devcontainer/"
    echo "   - Log file saved to: $LOG_FILE"
    echo ""

    if [[ "$SETUP_MODE" != "minimal" ]] && command_exists docker; then
        log_info "🚀 Next steps:"
        echo "   1. Run 'just db-setup' to start development databases"
        echo "   2. Run 'just test-all-db' to verify database integration"
        echo "   3. Open the project in VS Code with the Dev Containers extension"
    fi
}

# Main execution
main() {
    log_info "🔧 DataProfiler Development Environment Setup v$SCRIPT_VERSION"
    log_info "📝 Log file: $LOG_FILE"
    log_info "🎯 Setup mode: $SETUP_MODE"
    echo ""

    # Check command line arguments
    case "$SETUP_MODE" in
        "minimal"|"full"|"update")
            log_info "✅ Setup mode '$SETUP_MODE' selected"
            ;;
        *)
            log_error "❌ Invalid setup mode: $SETUP_MODE"
            log_info "Valid modes: minimal, full, update"
            log_info "Usage: $0 [minimal|full|update]"
            exit 1
            ;;
    esac

    # Step 1: Prerequisites
    check_prerequisites

    # Step 2: Rust toolchain
    install_rust

    # Step 3: Pre-commit hooks
    install_pre_commit

    # Step 4: Docker (if not minimal)
    setup_docker || log_warn "⚠️ Docker setup failed, continuing without database features"

    # Step 5: Development configuration
    create_dev_config

    # Step 6: Quality checks
    run_quality_checks

    # Step 7: Summary
    display_summary

    log_success "✅ Setup completed successfully!"
}

# Show help
if [[ "${1:-}" == "--help" ]] || [[ "${1:-}" == "-h" ]]; then
    echo "DataProfiler Development Environment Setup"
    echo ""
    echo "Usage: $0 [MODE] [OPTIONS]"
    echo ""
    echo "Modes:"
    echo "  minimal    Setup basic Rust toolchain and pre-commit hooks only"
    echo "  full       Complete setup including Docker and databases (default)"
    echo "  update     Update existing installation"
    echo ""
    echo "Options:"
    echo "  --help, -h              Show this help"
    echo ""
    echo "Environment variables:"
    echo "  FORCE_REINSTALL=true    Force reinstallation of tools"
    echo "  DEBUG=true              Enable debug logging"
    echo "  CREATE_VSCODE_CONFIG=false  Skip VS Code configuration"
    echo ""
    echo "Examples:"
    echo "  $0                      # Full setup"
    echo "  $0 minimal              # Minimal setup"
    echo "  $0 update               # Update existing installation"
    echo "  FORCE_REINSTALL=true $0 # Force reinstall all tools"
    exit 0
fi

# Run main function
main "$@"