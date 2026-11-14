"""Every Python route resolves a request the same way (#717).

The analysis options used to be forwarded field by field at each layer of the
Rust pipeline, and a missed copy is a route that silently ignores an option.
This test sets every analysis option at once and profiles the same records
through each Python route: file paths on every engine and format (including a
wide numeric CSV, which the adaptive selector sends to the Arrow engine),
byte buffers, in-memory data and the asyncio entry points. Each route must
show the effect of every option.

``tests/request_resolution.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
    import dataprof.asyncio as dpa
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

#: Postal codes make the locale observable: without one, both ``CAP (IT)`` and
#: ``ZIP Code (US)`` match a five-digit string.
CAPS = ["20121", "00184", "10121", "80132", "50122"]
CREATED = ["2026-01-05", "2026-02-10", "2026-03-15", "2026-04-20", "2026-05-25"]
#: Enough numeric columns past ``n0`` for the adaptive selector to pick Arrow.
WIDE_EXTRA = 19
SELECTED = ["cap", "created", "n0"]
REFERENCE = "2026-06-30"

OPTIONS: dict[str, Any] = {
    "columns": SELECTED,
    "metrics": ["schema", "patterns", "quality"],
    "quality_dimensions": ["consistency", "timeliness"],
    "locale": "it",
    "reference_date": REFERENCE,
    "temporal_columns": ["created"],
    "identifier_columns": ["n0"],
}


def _records() -> list[dict[str, Any]]:
    return [
        {"id": row + 1, "cap": cap, "created": created, "n0": row * 7 + 3}
        for row, (cap, created) in enumerate(zip(CAPS, CREATED, strict=True))
    ]


def _csv(wide: bool) -> str:
    names = ["id", "cap", "created", "n0"]
    if wide:
        names += [f"n{i}" for i in range(1, WIDE_EXTRA + 1)]
    lines = [",".join(names)]
    for row, record in enumerate(_records()):
        fields = [str(record[name]) for name in ("id", "cap", "created", "n0")]
        if wide:
            fields += [str(row * i) for i in range(1, WIDE_EXTRA + 1)]
        lines.append(",".join(fields))
    return "\n".join(lines) + "\n"


def _parquet() -> bytes:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")
    table = pa.Table.from_pylist(_records())
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink)
    return sink.getvalue().to_pybytes()


@pytest.fixture
def sources(tmp_path: Path) -> dict[str, tuple[Path, str]]:
    """Each fixture as a file whose extension detects its format."""
    contents: dict[str, tuple[str, str, bytes]] = {
        "csv": ("data.csv", "csv", _csv(False).encode()),
        "csv-wide": ("wide.csv", "csv", _csv(True).encode()),
        "json": ("data.json", "json", json.dumps(_records()).encode()),
        "jsonl": (
            "data.jsonl",
            "jsonl",
            ("\n".join(json.dumps(r) for r in _records()) + "\n").encode(),
        ),
        "parquet": ("data.parquet", "parquet", _parquet()),
    }
    paths = {}
    for label, (name, fmt, data) in contents.items():
        path = tmp_path / name
        path.write_bytes(data)
        paths[label] = (path, fmt)
    return paths


def _missed(report: dp.ProfileReport) -> list[str]:
    """Every option the report does not reflect."""
    summary = report.to_dict()
    missed = []
    names = [column["name"] for column in summary["columns"]]
    if names != SELECTED:
        missed.append(f"columns: reported {names}")
    with_stats = [c["name"] for c in summary["columns"] if "stats" in c]
    if with_stats:
        missed.append(f"metrics: statistics on {with_stats}")
    cap = next((c for c in summary["columns"] if c["name"] == "cap"), {})
    patterns = [p["name"] for p in cap.get("patterns") or []]
    if "CAP (IT)" not in patterns or any("ZIP" in p for p in patterns):
        missed.append(f"locale: cap patterns {patterns}")
    quality = summary.get("quality") or {}
    scores = quality.get("dimension_scores", {})
    for dimension, requested in (
        ("consistency", True),
        ("timeliness", True),
        ("accuracy", False),
    ):
        if (scores.get(dimension) is None) == requested:
            state = "missing" if requested else "assessed"
            missed.append(f"quality_dimensions: {dimension} {state}")
    clock = (quality.get("timeliness") or {}).get("clock") or {}
    if clock.get("reference_date") != REFERENCE:
        missed.append(f"reference_date: clock {clock}")
    bindings = sorted((b["column"], b["kind"]) for b in summary["semantic_hint_bindings"])
    if bindings != [("created", "temporal"), ("n0", "identifier")]:
        missed.append(f"semantic hints: bindings {bindings}")
    return missed


def _check(wrong: list[str], route: str, run: Any) -> None:
    try:
        report = run()
    except Exception as error:  # noqa: BLE001 - every failure is a finding here
        wrong.append(f"{route}: failed: {error}")
        return
    wrong.extend(f"{route}: {missed}" for missed in _missed(report))


def test_every_file_route_honours_every_analysis_option(
    sources: dict[str, tuple[Path, str]],
) -> None:
    wrong: list[str] = []
    for label, (path, _) in sources.items():
        for engine in ("auto", "incremental", "columnar"):
            _check(
                wrong,
                f"profile(path, engine={engine}) {label}",
                lambda engine=engine, path=path: dp.profile(path, engine=engine, **OPTIONS),
            )
        # A row cap sends an auto CSV request to the incremental engine
        # directly, not through the selector.
        _check(
            wrong,
            f"profile(path, max_rows) {label}",
            lambda path=path: dp.profile(path, max_rows=1_000, **OPTIONS),
        )
    assert not wrong, "\n".join(wrong)


def test_the_wide_csv_reaches_the_arrow_engine_through_the_selector(
    sources: dict[str, tuple[Path, str]],
) -> None:
    def engine(label: str) -> str:
        report = dp.profile(sources[label][0], **OPTIONS)
        return report.to_dict()["execution"]["engine"]

    assert engine("csv-wide") == "columnar"
    assert engine("csv") == "incremental"


def test_every_bytes_and_memory_route_honours_every_analysis_option(
    sources: dict[str, tuple[Path, str]],
) -> None:
    wrong: list[str] = []
    for label, (path, fmt) in sources.items():
        data = path.read_bytes()
        _check(
            wrong,
            f"profile(bytes) {label}",
            lambda data=data, fmt=fmt: dp.profile(data, format=fmt, **OPTIONS),
        )
        _check(
            wrong,
            f"asyncio.profile_bytes {label}",
            lambda data=data, fmt=fmt: asyncio.run(dpa.profile_bytes(data, format=fmt, **OPTIONS)),
        )
        _check(
            wrong,
            f"asyncio.profile_file {label}",
            lambda path=path: asyncio.run(dpa.profile_file(path, **OPTIONS)),
        )
    _check(
        wrong,
        "profile(records)",
        lambda: dp.profile(_records(), **OPTIONS),
    )
    assert not wrong, "\n".join(wrong)
