FROM mcr.microsoft.com/devcontainers/rust:1-bookworm

# Install database client tools (Python is installed via devcontainer feature)
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    default-mysql-client \
    && rm -rf /var/lib/apt/lists/*
