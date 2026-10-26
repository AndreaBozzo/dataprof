"""Flat baselines retain the schema declaration of file and bytes formats (#1028)."""

import json
from pathlib import Path

import dataprof as dp
import pyarrow as pa
import pyarrow.parquet as pq
import pytest


def _missing_column_outcome(baseline: dp.ProfileReport) -> tuple[str, str | None]:
    current = dp.profile({"id": [1, 2, 3], "amount": [10, 20, 30], "note": ["a", "b", "c"]})
    result = current.check(baseline=baseline, forbid_added_columns=True)
    check = next(item for item in result.checks if item.column == "note")
    return check.status, check.reason["reason"] if check.reason else None


@pytest.fixture(
    params=[("csv", "file"), ("csv", "bytes"), ("parquet", "file"), ("parquet", "bytes")]
)
def capped_report(tmp_path: Path, request) -> tuple[dp.ProfileReport, str, str]:
    fmt, source_type = request.param
    path = tmp_path / f"capped.{fmt}"
    if fmt == "csv":
        path.write_text("id,amount\n1,10\n2,20\n3,30\n")
    else:
        pq.write_table(pa.table({"id": [1, 2, 3], "amount": [10, 20, 30]}), path)
    source = path if source_type == "file" else path.read_bytes()
    return dp.profile(source, format=fmt, max_rows=1), fmt, source_type


def test_capped_summary_retains_its_declared_columns(capped_report, tmp_path: Path) -> None:
    report, fmt, source_type = capped_report
    assert not report.source_exhausted
    summary = report.to_dict()
    assert summary["source_type"] == source_type
    assert summary["source_format"] == fmt
    assert _missing_column_outcome(report) == ("failed", None)

    restored = dp.ProfileReport.from_dict(summary)
    assert restored.to_dict()["source_format"] == fmt
    assert _missing_column_outcome(restored) == ("failed", None)
    destination = tmp_path / "summary.json"
    restored.save(destination)
    assert json.loads(destination.read_text())["source_format"] == fmt
    loaded = dp.ProfileReport.load(destination)
    assert loaded.to_dict()["source_format"] == fmt
    assert _missing_column_outcome(loaded) == ("failed", None)


def test_legacy_summary_does_not_invent_a_source_format(capped_report, tmp_path: Path) -> None:
    report, _, _ = capped_report
    summary = report.to_dict()
    summary.pop("source_format", None)
    legacy = dp.ProfileReport.from_dict(summary)
    assert "source_format" not in legacy.to_dict()
    assert _missing_column_outcome(legacy) == ("not_evaluated", "evidence_incomplete")
    destination = tmp_path / "legacy.json"
    legacy.save(destination)
    loaded = dp.ProfileReport.load(destination)
    assert "source_format" not in loaded.to_dict()
    assert _missing_column_outcome(loaded) == ("not_evaluated", "evidence_incomplete")


@pytest.mark.parametrize("fmt", ["json", "jsonl", "future-format"])
def test_nondeclaring_formats_remain_conservative(capped_report, fmt: str) -> None:
    report, _, _ = capped_report
    summary = report.to_dict()
    summary["source_format"] = fmt
    restored = dp.ProfileReport.from_dict(summary)
    assert restored.to_dict()["source_format"] == fmt
    assert _missing_column_outcome(restored) == ("not_evaluated", "evidence_incomplete")


@pytest.mark.parametrize("malformed", [None, True, 42, [], {}])
def test_present_source_format_must_be_a_string(malformed) -> None:
    summary = dp.profile({"id": [1, 2, 3]}).to_dict()
    summary["source_format"] = malformed
    with pytest.raises(ValueError, match="source_format"):
        dp.ProfileReport.from_dict(summary)


def test_sources_without_a_format_do_not_gain_one() -> None:
    report = dp.profile({"id": [1, 2, 3]})
    assert "source_format" not in report.to_dict()
    assert "source_format" not in dp.ProfileReport.from_dict(report.to_dict()).to_dict()
