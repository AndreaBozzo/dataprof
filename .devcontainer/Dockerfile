FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

# Install Python 3.12 and database client tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3.12 \
    python3.12-dev \
    python3-pip \
    postgresql-client \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*

# Install uv and maturin
RUN pip install --break-system-packages uv maturin
