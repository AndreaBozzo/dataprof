# DataProfiler Development Environment

This directory contains the development environment configuration for DataProfiler, implementing **Phase 1 & 2** of [Issue #58](https://github.com/AndreaBozzo/dataprof/issues/58).

## 🚀 Quick Start

### Option 1: VS Code Dev Containers (Recommended)
```bash
# Open in VS Code with Dev Containers extension
code .
# VS Code will prompt to "Reopen in Container"
```

### Option 2: Manual Setup
```bash
# Enhanced setup script with error handling
just setup full

# Or use legacy script
just setup-legacy

# Start databases
just db-setup

# Complete setup with databases and tests
just setup-complete
```

## 📁 Directory Structure

```
.devcontainer/
├── devcontainer.json         # VS Code dev container configuration
├── Dockerfile               # Multi-stage development container
├── docker-compose.yml       # Database services
├── init-scripts/            # Database initialization
│   ├── postgres/
│   │   └── 01-init-schema.sql
│   └── mysql/
│       └── 01-init-schema.sql
├── test-data/               # Sample data for testing
│   ├── sample.csv
│   └── large-sample.csv
└── README.md               # This file
```

## 🐳 Available Services

### Core Databases
- **PostgreSQL 15** (`localhost:5432`)
  - Database: `dataprof_test`
  - User: `dataprof` / Password: `dev_password_123`
  - Pre-loaded with sample data and schemas

- **MySQL 8.0** (`localhost:3306`)
  - Database: `dataprof_test`
  - User: `dataprof` / Password: `dev_password_123`
  - Pre-loaded with sample data and schemas

### Supporting Services
- **Redis** (`localhost:6379`) - For caching tests
- **MinIO** (`localhost:9000`) - S3-compatible storage
- **DuckDB** - File-based analytical database

### Admin Tools (Optional)
```bash
# Start with admin tools
just db-setup-all

# Access web interfaces
# pgAdmin: http://localhost:8080 (admin@dataprof.dev / admin123)
# phpMyAdmin: http://localhost:8081
# MinIO Console: http://localhost:9090 (minioadmin / minioadmin123)
```

## 🔧 Database Commands

```bash
# Start core databases
just db-setup

# Start all services including admin tools
just db-setup-all

# Check database status
just db-status

# View logs
just db-logs postgres  # or mysql, redis, etc.

# Connect to databases
just db-connect-postgres
just db-connect-mysql

# Reset all data (⚠️ destructive)
just db-reset
```

## 🧪 Testing Commands

```bash
# Test specific databases
just test-postgres
just test-mysql
just test-sqlite
just test-duckdb

# Test all databases with services running
just test-all-db

# Full quality check
just quality
```

## 📊 Sample Data

The development environment includes pre-loaded test data:

### PostgreSQL/MySQL Tables
- `sample_data` - Employee data with various data types
- `data_types_test` - Comprehensive data type testing
- `employee_summary` - View for aggregated data

### Test Data Files
- `.devcontainer/test-data/sample.csv` - Basic CSV with 8 records
- `.devcontainer/test-data/large-sample.csv` - Product catalog data

### Example Queries
```sql
-- PostgreSQL
SELECT * FROM dataprof_test.employee_summary;
SELECT name, salary FROM dataprof_test.sample_data WHERE salary > 70000;

-- MySQL
SELECT * FROM employee_summary;
SELECT name, salary FROM sample_data WHERE salary > 70000;
```

## 🛠️ Development Tools

### VS Code Configuration
- **Extensions**: Rust Analyzer, Docker, Database tools
- **Settings**: Auto-format on save, clippy integration
- **Tasks**: Build, test, quality checks
- **Debugging**: LLDB configuration for Rust

### Build Optimization
- Cargo configuration for faster builds
- Volume caching for dependencies
- Incremental compilation enabled

## 🔍 Features Implemented

✅ **Development Containers**
- Multi-stage Dockerfile (development/testing/production)
- VS Code integration with full extension setup
- Volume caching for performance

✅ **Database Services**
- PostgreSQL, MySQL, Redis, MinIO, DuckDB
- Health checks and proper initialization
- Pre-loaded schemas and test data

✅ **Task Automation**
- Enhanced `justfile` with database commands
- Cross-platform setup scripts (Bash + PowerShell)
- One-command environment setup

✅ **Cross-Platform Support**
- Linux, macOS, Windows (WSL2/Git Bash)
- Automatic platform detection
- Robust error handling and logging

## 🐛 Troubleshooting

### Common Issues

**Docker not running**
```bash
# Start Docker Desktop
# Wait for Docker to be fully started
docker ps  # Should work without errors
```

**Database initialization failed**
```bash
# Check logs
just db-logs mysql
just db-logs postgres

# Reset if needed
just db-reset
```

**Permission errors on Windows**
```bash
# Use PowerShell as Administrator
# Or use WSL2 environment
```

**Build cache issues**
```bash
# Clear cargo cache
cargo clean
# Or rebuild container
docker-compose -f .devcontainer/docker-compose.yml build --no-cache
```

### Performance Tips

**Faster Rust builds**
- Use volume caching (already configured)
- Increase Docker memory allocation (8GB+ recommended)
- Use `cargo build` in dev mode for iteration

**Database performance**
- Allocate more memory to Docker
- Use local volumes for persistence
- Consider database-specific tuning

## 📚 Related Documentation

- [DEVELOPMENT.md](../DEVELOPMENT.md) - Complete development workflow
- [justfile](../justfile) - All available commands
- [Docker Compose](docker-compose.yml) - Service configuration
- [Issue #58](https://github.com/AndreaBozzo/dataprof/issues/58) - Original requirements

## 🎯 Next Steps (Phase 3 & 4)

Planned for future implementation:
- [ ] Comprehensive development documentation
- [ ] IDE-specific setup guides
- [ ] Enhanced test data management
- [ ] Code snippets and templates
- [ ] Automated dependency management

---

**Quick Commands Reference:**
```bash
just setup              # Setup development environment
just db-setup           # Start databases
just test-all-db        # Test with databases
just quality            # Full quality checks
just --list             # Show all commands
```