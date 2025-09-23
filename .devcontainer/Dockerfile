# DataProfiler Development Container
# Multi-stage Dockerfile optimized for development experience

# Base stage with common dependencies
FROM mcr.microsoft.com/devcontainers/rust:1-1-bullseye AS base

# Install system dependencies for development
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    # Development tools
    build-essential \
    pkg-config \
    libssl-dev \
    ca-certificates \
    curl \
    wget \
    git \
    vim \
    nano \
    htop \
    tree \
    jq \
    unzip \
    # Database clients and development libraries
    postgresql-client \
    libpq-dev \
    mysql-client \
    libmysqlclient-dev \
    sqlite3 \
    libsqlite3-dev \
    # Python for scripts and utilities
    python3 \
    python3-pip \
    python3-venv \
    # Performance and debugging tools
    valgrind \
    strace \
    ltrace \
    gdb \
    # Network tools
    net-tools \
    iputils-ping \
    telnet \
    # Additional utilities
    sudo \
    zsh \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Install Python packages for development
RUN pip3 install --no-cache-dir \
    pre-commit \
    black \
    flake8 \
    mypy \
    pytest \
    pandas \
    numpy \
    matplotlib \
    jupyter

# Development stage - optimized for fast iteration
FROM base AS development

# Set up the development user
USER vscode

# Configure shell
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" || true

# Install Rust development tools
RUN rustup component add rustfmt clippy \
    && rustup target add x86_64-unknown-linux-musl \
    && cargo install cargo-tarpaulin cargo-machete just cargo-audit cargo-outdated

# Set up cargo configuration for faster builds
RUN mkdir -p ~/.cargo \
    && echo '[build]\ntarget-dir = "/tmp/target"\njobs = 0\n\n[profile.dev]\nopt-level = 0\ndebug = true\nincremental = true\ncodegen-units = 256\n\n[profile.dev.package."*"]\nopt-level = 1' > ~/.cargo/config.toml

# Configure git for development
RUN git config --global init.defaultBranch main \
    && git config --global pull.rebase false \
    && git config --global core.autocrlf input

# Set working directory
WORKDIR /workspace

# Copy configuration files for caching
COPY --chown=vscode:vscode Cargo.toml Cargo.lock ./
COPY --chown=vscode:vscode justfile ./

# Pre-build dependencies for faster development cycles
RUN mkdir -p src && echo "fn main() {}" > src/main.rs \
    && cargo build --release \
    && cargo build \
    && rm -rf src

# Set environment variables for development
ENV RUST_LOG=debug
ENV RUST_BACKTRACE=1
ENV CARGO_TARGET_DIR=/tmp/target

# Expose common development ports
EXPOSE 8000 8080 5432 3306

# Testing stage - includes database services
FROM development AS testing

# Switch to root for service installation
USER root

# Install Docker for database containers
RUN curl -fsSL https://get.docker.com | sh \
    && usermod -aG docker vscode

# Install Docker Compose
RUN curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose \
    && chmod +x /usr/local/bin/docker-compose

# Create directories for test data and databases
RUN mkdir -p /opt/test-data /opt/db-data \
    && chown -R vscode:vscode /opt/test-data /opt/db-data

# Switch back to development user
USER vscode

# Production stage - minimal runtime environment
FROM debian:bullseye-slim AS production

# Install minimal runtime dependencies
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    ca-certificates \
    libssl1.1 \
    libpq5 \
    libmysqlclient21 \
    libsqlite3-0 \
    && apt-get autoremove -y && apt-get clean -y && rm -rf /var/lib/apt/lists/*

# Create app user
RUN groupadd --gid 1000 app \
    && useradd --uid 1000 --gid app --shell /bin/bash --create-home app

# Set working directory
WORKDIR /app

# Copy binary from a previous build stage (to be used in CI/CD)
# COPY --from=builder /workspace/target/release/dataprof-cli /usr/local/bin/

USER app

# Default stage is development
FROM development