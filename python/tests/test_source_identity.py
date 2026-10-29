"""A report can record which data it describes (#866).

``tests/source_identity.rs`` is the Rust twin. Here the routes are the Python
entry points that take ``source_version=``, the flat ``to_dict()`` summary,
loading, and ``python -m dataprof.check --source-version``.
"""

import asyncio
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

import dataprof as dp
import dataprof.asyncio as dpa
import pytest

ROWS = {"id": [1, 2, 3]}
VERSION = "snapshot 7883168378056080943 "


def _routes(tmp_path: Path) -> dict[str, Any]:
    csv = tmp_path / "ids.csv"
    csv.write_text("id\n1\n2\n3\n")
    jsonl = tmp_path / "ids.jsonl"
    jsonl.write_text('{"id": 1}\n{"id": 2}\n')
    routes: dict[str, Any] = {
        "dict": ROWS,
        "rows": [{"id": 1}, {"id": 2}],
        "csv auto": (str(csv), "auto"),
        "csv incremental": (str(csv), "incremental"),
        "csv columnar": (str(csv), "columnar"),
        "jsonl": str(jsonl),
        "csv bytes": (csv.read_bytes(), "csv"),
    }
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pass
    else:
        routes["arrow table"] = pa.table(ROWS)
        routes["arrow stream"] = lambda: pa.table(ROWS).to_reader()
        parquet = tmp_path / "ids.parquet"
        pq.write_table(pa.table(ROWS), parquet)
        routes["parquet"] = str(parquet)
        routes["parquet bytes"] = (parquet.read_bytes(), "parquet")
    try:
        import pandas as pd
    except ImportError:
        pass
    else:
        routes["pandas"] = pd.DataFrame(ROWS)
    return routes


def _profile(source: Any, **kwargs: Any) -> dp.ProfileReport:
    if callable(source):
        source = source()
    if isinstance(source, tuple) and isinstance(source[0], bytes):
        data, fmt = source
        return dp.profile(data, format=fmt, **kwargs)
    if isinstance(source, tuple):
        path, engine = source
        return dp.profile(path, engine=engine, **kwargs)
    return dp.profile(source, **kwargs)


def _recorded(report: dp.ProfileReport) -> list[Any]:
    """The identity as the property, the canonical document and the summary hold it."""
    return [
        report.source_identity,
        json.loads(report.to_json()).get("source_identity"),
        report.to_dict().get("source_identity"),
    ]


def test_every_input_path_records_the_version(tmp_path: Path):
    wrong = []
    for name, source in _routes(tmp_path).items():
        report = _profile(source, source_version=VERSION)
        if _recorded(report) != [{"version": VERSION}] * 3:
            wrong.append((name, _recorded(report)))
    assert wrong == []


def test_without_a_version_nothing_is_recorded(tmp_path: Path):
    wrong = []
    for name, source in _routes(tmp_path).items():
        report = _profile(source)
        if _recorded(report) != [None] * 3:
            wrong.append((name, _recorded(report)))
    assert wrong == []


def test_profile_file_and_the_builder_record_the_version(tmp_path: Path):
    csv = tmp_path / "ids.csv"
    csv.write_text("id\n1\n2\n")
    assert dp.profile_file(csv, source_version=VERSION).source_identity == {"version": VERSION}
    builder = dp.Profiler().source_version(VERSION)
    assert builder.profile(str(csv)).source_identity == {"version": VERSION}
    assert builder.profile(ROWS).source_identity == {"version": VERSION}


@pytest.mark.skipif(not dpa._HAS_ASYNC, reason="async support not compiled")
def test_async_entry_points_record_the_version(tmp_path: Path):
    csv = tmp_path / "ids.csv"
    csv.write_text("id\n1\n2\n")
    from_bytes = asyncio.run(
        dpa.profile_bytes(csv.read_bytes(), format="csv", source_version=VERSION)
    )
    from_file = asyncio.run(dpa.profile_file(csv, source_version=VERSION, max_rows=1))
    assert from_bytes.source_identity == {"version": VERSION}
    assert from_file.source_identity == {"version": VERSION}
    assert asyncio.run(dpa.profile_file(csv)).source_identity is None


@pytest.mark.parametrize(
    ("value", "error"),
    [("", ValueError), ("  \t", ValueError), (42, TypeError), (b"v1", TypeError)],
)
def test_a_version_that_identifies_nothing_is_refused(
    tmp_path: Path, value: Any, error: type[Exception]
):
    csv = tmp_path / "ids.csv"
    csv.write_text("id\n1\n")
    for source in (str(csv), ROWS):
        with pytest.raises(error, match="source_version"):
            dp.profile(source, source_version=value)


def test_a_recorded_version_survives_saving_and_loading(tmp_path: Path):
    report = dp.profile(ROWS, source_version=VERSION)
    saved = tmp_path / "report.json"
    report.save(saved)
    reloaded = {
        "load": dp.ProfileReport.load(saved),
        "from_json": dp.ProfileReport.from_json(report.to_json()),
        "from_dict(to_dict())": dp.ProfileReport.from_dict(report.to_dict()),
    }
    wrong = {
        name: _recorded(loaded)
        for name, loaded in reloaded.items()
        if _recorded(loaded) != [{"version": VERSION}] * 3
    }
    assert wrong == {}


def test_a_stored_identity_is_read_as_the_rust_reader_reads_it():
    summary = dp.profile(ROWS).to_dict()
    canonical = json.loads(dp.profile(ROWS).to_json())
    accepted = {"version": "v7", "content_sha256": "ab"}
    accepted_by: dict[str, Any] = {}
    refused_by: dict[str, list[Any]] = {}
    for name, document in (("summary", summary), ("canonical", canonical)):
        accepted_by[name] = [
            dp.ProfileReport.from_dict({**document, "source_identity": value}).source_identity
            for value in (accepted, {})
        ]
        refused_by[name] = []
        for malformed in (None, "v7", {"version": None}, {"version": 7}):
            try:
                dp.ProfileReport.from_dict({**document, "source_identity": malformed})
            except ValueError:
                refused_by[name].append(malformed)
    assert accepted_by == {"summary": [{"version": "v7"}, {}], "canonical": [{"version": "v7"}, {}]}
    every = [None, "v7", {"version": None}, {"version": 7}]
    assert refused_by == {"summary": every, "canonical": every}


def test_check_records_the_version_in_its_report(tmp_path: Path):
    csv = tmp_path / "ids.csv"
    csv.write_text("id\n1\n2\n")
    written = {}
    for name, extra in (("with", ["--source-version", VERSION]), ("without", [])):
        target = tmp_path / f"{name}.json"
        completed = subprocess.run(
            [
                sys.executable,
                "-m",
                "dataprof.check",
                str(csv),
                "--min-rows",
                "1",
                "--report",
                str(target),
                *extra,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        assert completed.returncode == 0, completed.stderr
        written[name] = dp.ProfileReport.load(target).source_identity
    assert written == {"with": {"version": VERSION}, "without": None}


def test_check_refuses_an_empty_version_as_an_input_error(tmp_path: Path):
    csv = tmp_path / "ids.csv"
    csv.write_text("id\n1\n")
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "dataprof.check",
            str(csv),
            "--min-rows",
            "1",
            "--source-version",
            "",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 3
    assert "source_version" in completed.stderr
