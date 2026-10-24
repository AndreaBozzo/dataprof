"""Reports name the dataprof build that produced them (#907).

``tests/report_generator.rs`` is the Rust twin. Here the extra routes are the
Python ones, the flat ``to_dict()`` summary, and the commit the extension's
``build.rs`` read from git.
"""

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

ROWS = {"id": [1, 2, 3]}
REPO = Path(__file__).resolve().parents[2]


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
        "csv bytes": csv.read_bytes(),
    }
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pass
    else:
        routes["arrow"] = pa.table(ROWS)
        parquet = tmp_path / "ids.parquet"
        pq.write_table(pa.table(ROWS), parquet)
        routes["parquet"] = str(parquet)
    try:
        import pandas as pd
    except ImportError:
        pass
    else:
        routes["pandas"] = pd.DataFrame(ROWS)
    return routes


def _profile(source: Any) -> dp.ProfileReport:
    if isinstance(source, tuple):
        path, engine = source
        return dp.profile(path, engine=engine)
    if isinstance(source, bytes):
        return dp.profile(source, format="csv")
    return dp.profile(source)


def _installed() -> dict[str, Any]:
    """The generator this installation writes, from capabilities()."""
    build = dp.capabilities()
    expected: dict[str, Any] = {"version": dp.__version__}
    if build.commit is not None:
        expected["commit"] = build.commit
    if build.dirty is not None:
        expected["dirty"] = build.dirty
    return expected


def test_every_input_path_records_the_build(tmp_path: Path):
    wrong = []
    for name, source in _routes(tmp_path).items():
        report = _profile(source)
        if report.generator != _installed() or report.to_dict().get("generator") != _installed():
            wrong.append((name, report.generator))
    assert wrong == []


def test_an_extension_built_from_this_checkout_names_its_commit():
    """`build.rs` reads git where the workspace root is the checkout's top.

    An editable install from this repository is such a build, so it must
    carry a commit of this repository. The commit is checked as an ancestor
    of HEAD rather than equal to it, so a commit made after the build does
    not fail the test.
    """
    if Path(dp.__file__).resolve().parents[2] != REPO:
        pytest.skip("dataprof is not installed from this checkout")
    git = shutil.which("git")
    if git is None or not (REPO / ".git").exists():
        pytest.skip("not a git checkout with git available")
    build = dp.capabilities()
    assert build.commit is not None, "an extension built from a checkout records its commit"
    assert len(build.commit) == 40
    ancestor = subprocess.run(
        [git, "-C", str(REPO), "merge-base", "--is-ancestor", build.commit, "HEAD"],
        check=False,
    )
    assert ancestor.returncode == 0, f"{build.commit} is not in this checkout's history"
    assert isinstance(build.dirty, bool)


def test_a_document_keeps_its_generator_and_its_absence(tmp_path: Path):
    report = dp.profile(ROWS)
    older = {"version": "0.11.0"}

    saved = tmp_path / "report.json"
    report.save(saved)
    assert dp.ProfileReport.load(saved).generator == _installed()

    summary = report.to_dict()
    assert dp.ProfileReport.from_dict(summary).generator == _installed()

    # A stored report names the build that wrote it, not the reader's.
    summary["generator"] = older
    assert dp.ProfileReport.from_dict(summary).generator == older
    assert dp.ProfileReport.from_dict(summary).to_dict()["generator"] == older
    canonical = json.loads(report.to_json())
    canonical["generator"] = older
    assert dp.ProfileReport.from_json(json.dumps(canonical)).generator == older

    # Written before the field: unknown.
    del summary["generator"]
    legacy = dp.ProfileReport.from_dict(summary)
    assert legacy.generator is None
    assert "generator" not in legacy.to_dict()
    del canonical["generator"]
    assert dp.ProfileReport.from_json(json.dumps(canonical)).generator is None

    # A field a later release adds is ignored.
    later = report.to_dict()
    later["generator"]["profile"] = "release"
    assert dp.ProfileReport.from_dict(later).generator == _installed()


@pytest.mark.parametrize(
    "malformed",
    [
        None,
        "0.13.0",
        {},
        {"version": 13},
        {"version": "0.13.0", "commit": None},
        {"version": "0.13.0", "commit": 7},
        {"version": "0.13.0", "dirty": None},
        {"version": "0.13.0", "dirty": "yes"},
    ],
)
def test_a_malformed_generator_is_refused_by_both_readers(malformed: Any):
    report = dp.profile(ROWS)
    summary = report.to_dict()
    summary["generator"] = malformed
    with pytest.raises(ValueError):
        dp.ProfileReport.from_dict(summary)
    canonical = json.loads(report.to_json())
    canonical["generator"] = malformed
    with pytest.raises(ValueError):
        dp.ProfileReport.from_json(json.dumps(canonical))
