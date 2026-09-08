"""Shared dataset paths and the CSV report fixture for Python API tests."""

from pathlib import Path

import pytest

# Resolve fixture paths relative to repo root
REPO_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURES = REPO_ROOT / "examples" / "test_datasets"
CSV_FILE = str(FIXTURES / "small_comma.csv")
CSV_LARGE_FILE = str(FIXTURES / "large_dataset.csv")
JSON_FILE = str(FIXTURES / "users.json")
JSONL_FILE = str(FIXTURES / "logs.jsonl")
PARQUET_FILE = str(REPO_ROOT / "examples" / "test_data" / "simple.parquet")
SEMICOLON_FILE = str(FIXTURES / "employees_semicolon.csv")


@pytest.fixture()
def report():
    import dataprof

    return dataprof.profile(CSV_FILE)
