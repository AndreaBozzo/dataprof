.PHONY: build dev test test-rust test-python lint lint-rust lint-python fmt fmt-rust fmt-python clippy check clean

# ── Build ────────────────────────────────────────

build:  ## Build the Python extension (release)
	uv run maturin develop --release

dev:  ## Build the Python extension (debug, fast)
	uv run maturin develop

# ── Test ─────────────────────────────────────────

test: test-rust test-python  ## Run all tests

test-rust:  ## Run Rust tests
	cargo test

test-python: dev  ## Build and run Python tests
	uv run pytest

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
