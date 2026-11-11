"""compare() and the gate result name the build behind each report (#1063).

``tests/result_provenance.rs`` is the Rust twin. Here the extra routes are the
Python ones: a report loaded from its canonical JSON or from a flat
``to_dict()`` summary, ``QualityGateResult.to_dict()``, and
``python -m dataprof.check --json``.
"""

import json
import subprocess
import sys
from collections.abc import Callable
from pathlib import Path
from typing import Any

import dataprof as dp
import pytest

ROWS = {"id": [1, 2, 3, 4], "amount": [10, 20, 30, 40]}
COMMIT_A = "0123456789abcdef0123456789abcdef01234567"
COMMIT_B = "89abcdef0123456789abcdef0123456789abcdef"
_UNSET = object()


def _with(
    generator: Any = _UNSET, version: str | None = None, *, flat: bool = False
) -> dp.ProfileReport:
    """A report with the given build and source version, `None` dropping it."""
    report = dp.profile(ROWS, source_version=version)
    document = report.to_dict() if flat else json.loads(report.to_json())
    if generator is None:
        document.pop("generator", None)
    elif generator is not _UNSET:
        document["generator"] = generator
    return dp.ProfileReport.from_dict(document)


def _canonical(generator: Any = _UNSET, version: str | None = None) -> dp.ProfileReport:
    return _with(generator, version)


def _flat(generator: Any = _UNSET, version: str | None = None) -> dp.ProfileReport:
    return _with(generator, version, flat=True)


ROUTES: dict[str, Callable[..., dp.ProfileReport]] = {
    "canonical": _canonical,
    "flat": _flat,
}


def _build(version: str, commit: str | None = None, dirty: bool | None = None) -> dict[str, Any]:
    build: dict[str, Any] = {"version": version}
    if commit is not None:
        build["commit"] = commit
    if dirty is not None:
        build["dirty"] = dirty
    return build


CLEAN_A = _build("0.14.0", COMMIT_A, False)
SAME_BUILD_CASES = [
    ("same clean build", CLEAN_A, CLEAN_A, True),
    ("other version", CLEAN_A, _build("0.13.0", COMMIT_A, False), False),
    ("other version, no commit", _build("0.14.0"), _build("0.13.0"), False),
    ("other commit", CLEAN_A, _build("0.14.0", COMMIT_B, False), False),
    (
        "other commit, both dirty",
        _build("0.14.0", COMMIT_A, True),
        _build("0.14.0", COMMIT_B, True),
        False,
    ),
    ("one commit clean and dirty", CLEAN_A, _build("0.14.0", COMMIT_A, True), False),
    (
        "one commit dirty twice",
        _build("0.14.0", COMMIT_A, True),
        _build("0.14.0", COMMIT_A, True),
        None,
    ),
    ("commit unrecorded on one side", CLEAN_A, _build("0.14.0"), None),
    (
        "dirty flag unrecorded",
        _build("0.14.0", COMMIT_A),
        _build("0.14.0", COMMIT_A),
        None,
    ),
    ("no build on one side", CLEAN_A, None, None),
    ("no build on either side", None, None, None),
]


def test_same_build_reads_version_commit_and_dirty_flag() -> None:
    wrong = []
    for name, a, b, expected in SAME_BUILD_CASES:
        for route, make in ROUTES.items():
            same = make(a).compare(make(b))["reports"]["same_build"]
            if same is not expected:
                wrong.append(f"{name} ({route}): same_build {same!r}, expected {expected!r}")
    assert not wrong, wrong


def test_compare_names_each_sides_build_and_version_on_every_route() -> None:
    build_a = _build("0.13.0", COMMIT_A, False)
    build_b = _build("0.14.0")
    expected = {
        "a": [build_a, {"version": "snapshot 41"}],
        "b": [build_b, {"version": "snapshot 42"}],
    }
    wrong = []
    for route_a, make_a in ROUTES.items():
        for route_b, make_b in ROUTES.items():
            delta = make_a(build_a, "snapshot 41").compare(make_b(build_b, "snapshot 42"))
            for side in ("a", "b"):
                got = [
                    delta["reports"][side]["generator"],
                    delta["reports"][side]["source_identity"],
                ]
                if got != expected[side]:
                    wrong.append(f"{route_a}/{route_b} {side}: {got}")
            if delta["reports"]["same_build"] is not False:
                wrong.append(f"{route_a}/{route_b}: same_build {delta['reports']['same_build']!r}")
    # A profiled report names the build that is running.
    live = dp.profile(ROWS).compare(dp.profile(ROWS))
    if live["reports"]["a"]["generator"] != dp.profile(ROWS).generator:
        wrong.append(f"live: {live['reports']['a']['generator']}")
    assert not wrong, wrong


def test_an_unrecorded_build_or_version_reads_as_none() -> None:
    wrong = []
    for route, make in ROUTES.items():
        delta = make(None).compare(make(None))
        for side in ("a", "b"):
            for key in ("generator", "source_identity"):
                if key not in delta["reports"][side] or delta["reports"][side][key] is not None:
                    wrong.append(
                        f"{route} {side}.{key}: {delta['reports'][side].get(key, 'absent')!r}"
                    )
    assert not wrong, wrong


@pytest.mark.parametrize("key", ["generator", "source_identity"])
def test_a_summary_with_a_null_generator_or_identity_is_refused(key: str) -> None:
    document = dp.profile(ROWS, source_version="v1").to_dict()
    document[key] = None
    with pytest.raises(ValueError, match=key):
        dp.ProfileReport.from_dict(document)


def test_the_gate_result_names_the_evaluated_reports_build_and_version() -> None:
    build = _build("0.14.0", COMMIT_B, False)
    report = _canonical(build, "snapshot 42")
    wrong = []

    def expect(name: str, result: dp.QualityGateResult, generator: Any, version: Any) -> None:
        document = json.loads(result.to_json())
        for key, value in (("generator", generator), ("source_identity", version)):
            if getattr(result, key) != value:
                wrong.append(f"{name}: result.{key} {getattr(result, key)!r}")
            if key not in document or document[key] != value:
                wrong.append(f"{name}: to_dict()[{key!r}] {document.get(key, 'absent')!r}")

    expect("absolute", report.check(min_rows=1), build, {"version": "snapshot 42"})
    expect("absolute, unrecorded", _canonical(None).check(min_rows=1), None, None)
    for route, make in ROUTES.items():
        baseline = make(_build("0.13.0"), "snapshot 41")
        result = report.check(baseline=baseline, max_quality_score_drop=5.0)
        expect(f"{route} baseline", result, build, {"version": "snapshot 42"})
        assert result.baseline is not None
        reports = result.baseline["reports"]
        got = [
            reports["a"]["generator"],
            reports["a"]["source_identity"],
            reports["b"]["generator"],
            reports["same_build"],
        ]
        if got != [_build("0.13.0"), {"version": "snapshot 41"}, build, False]:
            wrong.append(f"{route} baseline: reports {got}")
    assert not wrong, wrong


def test_the_check_entrypoint_prints_the_build_and_version(tmp_path: Path) -> None:
    source = tmp_path / "rows.csv"
    source.write_text("id,amount\n1,10\n2,20\n")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "dataprof.check",
            str(source),
            "--min-rows",
            "1",
            "--source-version",
            "load 7",
            "--json",
        ],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    verdict = json.loads(result.stdout)
    assert verdict["generator"] == dp.profile(ROWS).generator
    assert verdict["generator"]["version"] == dp.__version__
    assert verdict["source_identity"] == {"version": "load 7"}
