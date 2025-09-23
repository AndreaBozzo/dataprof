# Enhanced Development setup script for DataProfiler v0.4.0 (PowerShell)
# Provides robust error handling, logging, and flexible setup options

param(
    [string]$Mode = "full",
    [switch]$ForceReinstall = $false,
    [switch]$Debug = $false,
    [switch]$Help = $false
)

# Configuration
$ScriptVersion = "0.4.0"
$LogFile = "$env:TEMP\dataprof-setup-$(Get-Date -Format 'yyyyMMdd_HHmmss').log"

# Error handling
$ErrorActionPreference = "Stop"

# Colors
$ColorInfo = "Blue"
$ColorWarn = "Yellow"
$ColorError = "Red"
$ColorSuccess = "Green"
$ColorDebug = "Magenta"

# Logging functions
function Write-Log {
    param($Level, $Message)
    $Timestamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
    $LogEntry = "$Timestamp [$Level] $Message"
    Add-Content -Path $LogFile -Value $LogEntry

    switch ($Level) {
        "INFO" { Write-Host $Message -ForegroundColor $ColorInfo }
        "WARN" { Write-Host $Message -ForegroundColor $ColorWarn }
        "ERROR" { Write-Host $Message -ForegroundColor $ColorError }
        "SUCCESS" { Write-Host $Message -ForegroundColor $ColorSuccess }
        "DEBUG" { if ($Debug) { Write-Host $Message -ForegroundColor $ColorDebug } }
    }
}

function Write-LogInfo { param($Message) Write-Log "INFO" $Message }
function Write-LogWarn { param($Message) Write-Log "WARN" $Message }
function Write-LogError { param($Message) Write-Log "ERROR" $Message }
function Write-LogSuccess { param($Message) Write-Log "SUCCESS" $Message }
function Write-LogDebug { param($Message) Write-Log "DEBUG" $Message }

# Show help
if ($Help) {
    Write-Host "DataProfiler Development Environment Setup (PowerShell)" -ForegroundColor Cyan
    Write-Host ""
    Write-Host "Usage: .\setup-dev.ps1 [-Mode <MODE>] [-ForceReinstall] [-Debug] [-Help]"
    Write-Host ""
    Write-Host "Parameters:"
    Write-Host "  -Mode          Setup mode: minimal, full, update (default: full)"
    Write-Host "  -ForceReinstall Force reinstallation of tools"
    Write-Host "  -Debug         Enable debug logging"
    Write-Host "  -Help          Show this help"
    Write-Host ""
    Write-Host "Examples:"
    Write-Host "  .\setup-dev.ps1                    # Full setup"
    Write-Host "  .\setup-dev.ps1 -Mode minimal      # Minimal setup"
    Write-Host "  .\setup-dev.ps1 -Mode update       # Update existing installation"
    Write-Host "  .\setup-dev.ps1 -ForceReinstall    # Force reinstall all tools"
    exit 0
}

# Validate mode
if ($Mode -notin @("minimal", "full", "update")) {
    Write-LogError "Invalid setup mode: $Mode"
    Write-LogInfo "Valid modes: minimal, full, update"
    exit 1
}

# Error cleanup
function Cleanup-OnError {
    Write-LogError "Setup failed"
    Write-LogInfo "Cleanup operations..."

    # Stop any running containers that might have been started
    if (Get-Command docker-compose -ErrorAction SilentlyContinue) {
        try {
            docker-compose -f .devcontainer/docker-compose.yml down 2>$null
        } catch {}
    }

    Write-LogInfo "Log file saved to: $LogFile"
}

# Utility functions
function Test-CommandExists {
    param($Command)
    return $null -ne (Get-Command $Command -ErrorAction SilentlyContinue)
}

function Get-ToolVersion {
    param($Tool)
    try {
        switch ($Tool) {
            "docker" { return (docker --version).Split(" ")[2].TrimEnd(",") }
            "docker-compose" { return (docker-compose --version).Split(" ")[2].TrimEnd(",") }
            "rust" { return (rustc --version).Split(" ")[1] }
            "just" { return (just --version).Split(" ")[1] }
            default { return "unknown" }
        }
    } catch {
        return "unknown"
    }
}

function Test-Prerequisites {
    Write-LogInfo "🔍 Checking system prerequisites..."

    $MissingDeps = @()

    # Check OS
    $OSInfo = Get-CimInstance Win32_OperatingSystem
    Write-LogSuccess "✅ Operating system: $($OSInfo.Caption) (Windows supported)"

    # Check essential commands
    $EssentialCommands = @("git")
    foreach ($cmd in $EssentialCommands) {
        if (Test-CommandExists $cmd) {
            Write-LogSuccess "✅ $cmd: available"
        } else {
            $MissingDeps += $cmd
            Write-LogError "❌ $cmd: not found"
        }
    }

    # Check Python
    if ((Test-CommandExists "python") -or (Test-CommandExists "python3")) {
        $PythonCmd = if (Test-CommandExists "python3") { "python3" } else { "python" }
        $PythonVersion = & $PythonCmd --version 2>&1
        Write-LogSuccess "✅ Python: $PythonVersion"

        # Check pip
        try {
            & $PythonCmd -m pip --version | Out-Null
            Write-LogSuccess "✅ pip: available"
        } catch {
            Write-LogWarn "⚠️ pip: not available, will try alternative installation methods"
        }
    } else {
        $MissingDeps += "python"
        Write-LogError "❌ Python: not found"
    }

    # Report missing dependencies
    if ($MissingDeps.Count -gt 0) {
        Write-LogError "❌ Missing essential dependencies: $($MissingDeps -join ', ')"
        Write-LogInfo "Please install the missing dependencies and run the script again."
        Write-LogInfo "Windows: Use chocolatey (choco install $($MissingDeps -join ' ')) or manual installation"
        exit 1
    }
}

function Install-Rust {
    Write-LogInfo "🦀 Setting up Rust toolchain..."

    if ((Test-CommandExists "rustc") -and (Test-CommandExists "cargo")) {
        $RustVersion = Get-ToolVersion "rust"
        Write-LogSuccess "✅ Rust $RustVersion already installed"

        # Update if requested
        if ($ForceReinstall -or ($Mode -eq "update")) {
            Write-LogInfo "🔄 Updating Rust toolchain..."
            rustup update
        }
    } else {
        Write-LogInfo "📦 Installing Rust via rustup..."
        $RustupUrl = "https://win.rustup.rs/x86_64"
        $RustupPath = "$env:TEMP\rustup-init.exe"

        Invoke-WebRequest -Uri $RustupUrl -OutFile $RustupPath
        & $RustupPath -y --default-toolchain stable

        # Refresh environment
        $env:PATH = "$env:USERPROFILE\.cargo\bin;$env:PATH"
    }

    # Install required components
    Write-LogInfo "🔧 Installing Rust components..."
    rustup component add rustfmt clippy

    # Install development tools
    $RustTools = @("cargo-tarpaulin", "cargo-machete", "just")
    foreach ($tool in $RustTools) {
        if ($ForceReinstall -or (-not (Test-CommandExists $tool))) {
            Write-LogInfo "📦 Installing $tool..."
            cargo install $tool --force
        } else {
            Write-LogSuccess "✅ $tool: already installed"
        }
    }
}

function Install-PreCommit {
    Write-LogInfo "🪝 Setting up pre-commit hooks..."

    $PythonCmd = if (Test-CommandExists "python3") { "python3" } else { "python" }

    # Install pre-commit if not available
    if (-not (Test-CommandExists "pre-commit")) {
        Write-LogInfo "📦 Installing pre-commit..."

        try {
            & $PythonCmd -m pip install pre-commit
            Write-LogSuccess "✅ pre-commit installed via pip"
        } catch {
            try {
                pip install pre-commit
                Write-LogSuccess "✅ pre-commit installed via pip"
            } catch {
                Write-LogError "❌ Failed to install pre-commit"
                Write-LogInfo "Please install pre-commit manually: pip install pre-commit"
                exit 1
            }
        }
    } else {
        Write-LogSuccess "✅ pre-commit: already installed"
    }

    # Install hooks
    Write-LogInfo "🔗 Installing pre-commit hooks..."
    pre-commit install --install-hooks
    pre-commit install --hook-type commit-msg

    Write-LogSuccess "✅ Pre-commit hooks installed"
}

function Setup-Docker {
    if ($Mode -eq "minimal") {
        Write-LogInfo "⏭️ Skipping Docker setup in minimal mode"
        return $true
    }

    Write-LogInfo "🐳 Checking Docker setup..."

    if (Test-CommandExists "docker") {
        $DockerVersion = Get-ToolVersion "docker"
        Write-LogSuccess "✅ Docker $DockerVersion found"

        # Check if Docker daemon is running
        try {
            docker info | Out-Null
            Write-LogSuccess "✅ Docker daemon is running"
        } catch {
            Write-LogWarn "⚠️ Docker daemon is not running. Please start Docker Desktop."
            return $false
        }
    } else {
        Write-LogWarn "⚠️ Docker not found. Database features will not be available."
        Write-LogInfo "Install Docker Desktop from: https://docs.docker.com/desktop/windows/"
        return $false
    }

    if (Test-CommandExists "docker-compose") {
        $ComposeVersion = Get-ToolVersion "docker-compose"
        Write-LogSuccess "✅ Docker Compose $ComposeVersion found"
    } else {
        Write-LogWarn "⚠️ Docker Compose not found. It should be included with Docker Desktop."
        return $false
    }

    return $true
}

function Invoke-QualityChecks {
    Write-LogInfo "🧪 Running initial quality checks..."

    # Format check and auto-fix
    Write-LogInfo "  🎨 Checking code formatting..."
    try {
        cargo fmt --all --check | Out-Null
        Write-LogSuccess "  ✅ Code formatting: OK"
    } catch {
        Write-LogWarn "  ⚠️ Code formatting issues found. Auto-fixing..."
        cargo fmt --all
        Write-LogSuccess "  ✅ Code formatting: fixed"
    }

    # Clippy check
    Write-LogInfo "  🔍 Running clippy linter..."
    try {
        cargo clippy --all-targets --all-features -- -D warnings | Out-Null
        Write-LogSuccess "  ✅ Clippy: no issues found"
    } catch {
        Write-LogError "  ❌ Clippy found issues. Please fix the warnings above."
        Write-LogInfo "  Run 'cargo clippy --all-targets --all-features' for details"
        exit 1
    }

    # Basic tests
    Write-LogInfo "  🧪 Running basic tests..."
    try {
        cargo test --lib | Out-Null
        Write-LogSuccess "  ✅ Basic tests: passed"
    } catch {
        Write-LogError "  ❌ Tests failed. Please fix failing tests."
        Write-LogInfo "  Run 'cargo test --lib' for details"
        exit 1
    }

    Write-LogSuccess "✅ All quality checks passed!"
}

function New-DevConfig {
    Write-LogInfo "⚙️ Creating development configuration..."

    # Create .vscode settings
    if ((Test-Path ".vscode") -or $true) {
        New-Item -ItemType Directory -Path ".vscode" -Force | Out-Null

        $VSCodeSettings = @'
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
'@
        Set-Content -Path ".vscode\settings.json" -Value $VSCodeSettings
        Write-LogSuccess "✅ VS Code settings created"
    }

    # Create cargo config for faster builds
    New-Item -ItemType Directory -Path ".cargo" -Force | Out-Null
    $CargoConfig = @'
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
'@
    Set-Content -Path ".cargo\config.toml" -Value $CargoConfig
    Write-LogSuccess "✅ Cargo configuration created"
}

function Show-Summary {
    Write-LogSuccess "🎉 Development environment setup complete!"
    Write-Host ""
    Write-LogInfo "📋 Available commands:"
    Write-Host "   just --list                  # Show all available commands"
    Write-Host "   just setup-complete          # Complete environment setup with databases"
    Write-Host "   just dev                     # Quick development cycle (fmt, build, test)"
    Write-Host "   just quality                 # Full quality check pipeline"
    Write-Host "   just db-setup                # Start development databases"
    Write-Host "   just test-all                # Run comprehensive tests"
    Write-Host ""
    Write-LogInfo "🗃️ Database commands:"
    Write-Host "   just db-setup                # Start PostgreSQL, MySQL, Redis"
    Write-Host "   just db-setup-all            # Start all services including admin tools"
    Write-Host "   just db-connect-postgres     # Connect to PostgreSQL"
    Write-Host "   just db-connect-mysql        # Connect to MySQL"
    Write-Host "   just db-status               # Check database service status"
    Write-Host ""
    Write-LogInfo "💡 Tips:"
    Write-Host "   - Pre-commit hooks run automatically on commits"
    Write-Host "   - Use 'git commit --no-verify' to skip hooks temporarily"
    Write-Host "   - Development containers are available in .devcontainer/"
    Write-Host "   - Log file saved to: $LogFile"
    Write-Host ""

    if (($Mode -ne "minimal") -and (Test-CommandExists "docker")) {
        Write-LogInfo "🚀 Next steps:"
        Write-Host "   1. Run 'just db-setup' to start development databases"
        Write-Host "   2. Run 'just test-all-db' to verify database integration"
        Write-Host "   3. Open the project in VS Code with the Dev Containers extension"
    }
}

# Main execution
try {
    Write-LogInfo "🔧 DataProfiler Development Environment Setup v$ScriptVersion"
    Write-LogInfo "📝 Log file: $LogFile"
    Write-LogInfo "🎯 Setup mode: $Mode"
    Write-Host ""

    # Step 1: Prerequisites
    Test-Prerequisites

    # Step 2: Rust toolchain
    Install-Rust

    # Step 3: Pre-commit hooks
    Install-PreCommit

    # Step 4: Docker (if not minimal)
    $DockerOK = Setup-Docker
    if (-not $DockerOK) {
        Write-LogWarn "⚠️ Docker setup failed, continuing without database features"
    }

    # Step 5: Development configuration
    New-DevConfig

    # Step 6: Quality checks
    Invoke-QualityChecks

    # Step 7: Summary
    Show-Summary

    Write-LogSuccess "✅ Setup completed successfully!"

} catch {
    Cleanup-OnError
    Write-LogError "Setup failed: $($_.Exception.Message)"
    exit 1
}