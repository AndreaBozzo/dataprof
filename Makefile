.PHONY: build dev build-db dev-db test test-rust test-python test-python-db test-db test-db-all test-db-live lint lint-rust lint-python fmt fmt-rust fmt-python clippy check clean

# ── Build ────────────────────────────────────────

build:  ## Build the Python extension (release)
	uv run maturin develop --release

dev:  ## Build the Python extension (debug, fast)
	uv run maturin develop

build-db:  ## Build the Python extension with database support (release)
	uv run maturin develop --release --features "python,python-async,database,sqlite"

dev-db:  ## Build the Python extension with database support (debug, fast)
	uv run maturin develop --features "python,python-async,database,sqlite"

# ── Test ─────────────────────────────────────────

test: test-rust test-python  ## Run all tests

test-rust:  ## Run Rust tests
	cargo test

test-python: dev  ## Build and run Python tests
	uv run pytest

test-python-db: dev-db  ## Build with DB features and run Python database tests
	uv run pytest python/tests/test_database_api.py -v -ra

test-db:  ## Run Rust database integration tests (SQLite only)
	cargo test --test database_integration --features "database,sqlite"

test-db-all:  ## Run Rust database integration tests (all DBs, needs Docker)
	cargo test --test database_integration --features "all-db"

test-db-live:  ## Run live database tests with Docker containers
	cd .devcontainer && bash test_live_databases.sh

# ── Lint ─────────────────────────────────────────

lint: lint-rust lint-python  ## Run all linters

lint-rust: clippy  ## Alias for clippy

clippy:  ## Run clippy with strict warnings
	cargo clippy --lib --tests --all-features -- -D warnings

lint-python:  ## Run ruff linter + type check
	uv run ruff check python/

# ── Format ───────────────────────────────────────

fmt: fmt-rust fmt-python  ## Format all code

fmt-rust:  ## Format Rust code
	cargo fmt

fmt-python:  ## Format Python code
	uv run ruff format python/
	uv run ruff check --fix python/

# ── Check (CI-like) ─────────────────────────────

check: fmt lint test  ## Format, lint, and test everything

# ── Clean ────────────────────────────────────────

clean:  ## Remove build artifacts
	cargo clean
	rm -rf target/ .pytest_cache/ python/dataprof/__pycache__/

# ── Help ─────────────────────────────────────────

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-16s\033[0m %s\n", $$1, $$2}'

.DEFAULT_GOAL := help
