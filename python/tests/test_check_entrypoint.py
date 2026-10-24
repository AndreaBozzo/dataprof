"""Exercise the installed module as a CI process, including its output streams."""

from __future__ import annotations

import codecs
import json
import subprocess
import sys
from pathlib import Path
from typing import Any
from unittest.mock import Mock

import dataprof as dp
import pytest
from dataprof.check import __main__ as entrypoint


def run_check(*args: str | Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", "dataprof.check", *(str(arg) for arg in args)],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
        check=False,
    )


@pytest.fixture
def source(tmp_path: Path) -> Path:
    path = tmp_path / "orders.csv"
    path.write_text("id,amount\n1,10\n2,\n3,30\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("limit,code", [(100, 0), (0, 1)])
def test_verdict_and_json_match_the_library(source: Path, limit: int, code: int):
    result = run_check(source, "--max-null", f"*={limit}", "--json")
    expected = dp.profile_file(source).check(max_null_percentage={"*": limit})
    assert result.returncode == code, result.stderr
    assert json.loads(result.stdout) == expected.to_dict()
    assert result.stderr.startswith(f"dataprof {dp.__version__}: {expected.verdict}:")
    if code == 1:
        assert "max_null_percentage [amount]" in result.stderr


def test_human_output_keeps_stdout_empty(source: Path):
    result = run_check(source, "--max-null", "*=100")
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert "pass:" in result.stderr


@pytest.mark.parametrize("existing_report", [False, True])
@pytest.mark.parametrize("json_output", [False, True])
@pytest.mark.parametrize(
    "flags,code,verdict",
    [
        (["--max-null", "*=100"], 0, "pass"),
        (["--max-null", "*=0"], 1, "fail"),
        (["--metric", "schema", "--min-quality", "90"], 2, "inconclusive"),
    ],
)
def test_report_is_reloadable_for_every_verdict(
    source: Path,
    tmp_path: Path,
    flags: list[str],
    code: int,
    verdict: str,
    json_output: bool,
    existing_report: bool,
):
    path = tmp_path / "quality report.JSON"
    if existing_report:
        path.write_text("previous report", encoding="utf-8")
    output_flags = ["--json"] if json_output else []
    result = run_check(source, *flags, "--report", path, *output_flags)
    assert result.returncode == code, result.stderr
    restored = dp.ProfileReport.load(path)
    assert restored.rows == 3
    assert restored.columns == 2
    assert json.loads(restored.to_json()) == json.loads(path.read_text(encoding="utf-8"))
    if verdict == "inconclusive":
        assert restored.quality_score is None
        expected = restored.check(min_quality_score=90)
    else:
        expected = restored.check(max_null_percentage={"*": 100 if verdict == "pass" else 0})
    assert expected.verdict == verdict
    if json_output:
        assert json.loads(result.stdout) == expected.to_dict()
    else:
        assert result.stdout == ""
    assert result.stderr.startswith(f"dataprof {dp.__version__}: {verdict}:")


def test_report_preserves_the_original_profile_without_profiling_again(
    source: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    report = dp.profile_file(source)
    expected = json.loads(report.to_json())
    profiler = Mock(return_value=report)
    monkeypatch.setattr(entrypoint, "profile_file", profiler)
    path = tmp_path / "report.json"
    code = entrypoint.main([str(source), "--max-null", "*=100", "--report", str(path)])
    assert code == 0
    assert profiler.call_count == 1
    assert json.loads(path.read_text(encoding="utf-8")) == expected
    assert json.loads(dp.ProfileReport.load(path).to_json()) == expected


@pytest.mark.parametrize("existing_report", [False, True])
def test_profiling_failure_does_not_write_a_report(tmp_path: Path, existing_report: bool):
    path = tmp_path / "report.json"
    if existing_report:
        path.write_text("previous report", encoding="utf-8")
    result = run_check(tmp_path / "missing.csv", "--min-quality", "0", "--report", path, "--json")
    assert result.returncode == 3
    assert json.loads(result.stdout)["error"]["kind"] == "input"
    if existing_report:
        assert path.read_text(encoding="utf-8") == "previous report"
    else:
        assert not path.exists()


@pytest.mark.parametrize("json_output", [False, True])
@pytest.mark.parametrize("destination", ["missing/report.json", "directory.json"])
@pytest.mark.parametrize(
    "flags",
    [
        ["--max-null", "*=100"],
        ["--max-null", "*=0"],
        ["--metric", "schema", "--min-quality", "90"],
    ],
)
def test_report_write_failure_is_an_output_error(
    source: Path, tmp_path: Path, destination: str, json_output: bool, flags: list[str]
):
    path = tmp_path / destination
    if destination == "directory.json":
        path.mkdir()
    output_flags = ["--json"] if json_output else []
    result = run_check(source, *flags, "--report", path, *output_flags)
    assert result.returncode == 3
    if json_output:
        document = json.loads(result.stdout)
        assert "verdict" not in document
        error = document["error"]
        assert error["kind"] == "output"
        assert error["path"] == str(path)
        assert str(path) in error["message"]
        assert result.stderr == ""
    else:
        assert result.stdout == ""
        assert str(path) in result.stderr
        assert "could not write report" in result.stderr
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize("protected", ["source", "--policy", "--baseline"])
@pytest.mark.parametrize("alias", ["direct", "parent", "symlink", "hardlink", "case"])
@pytest.mark.parametrize("json_output", [False, True])
def test_report_cannot_overwrite_inputs(
    tmp_path: Path, protected: str, alias: str, json_output: bool
):
    source = tmp_path / "events.json"
    source.write_text('[{"id": 1, "amount": 10}, {"id": 2, "amount": 20}]', encoding="utf-8")
    policy = tmp_path / "policy.json"
    policy.write_text('{"max_null_percentage": {"*": 100}}', encoding="utf-8")
    # A report is written whatever the verdict: over the baseline, a failing
    # run would become the next run's baseline.
    baseline = tmp_path / "baseline.json"
    dp.profile_file(source).save(baseline)
    before = {path: path.read_bytes() for path in (source, policy, baseline)}
    target = {"source": source, "--policy": policy, "--baseline": baseline}[protected]
    if alias == "parent":
        subdir = tmp_path / "subdir"
        subdir.mkdir()
        target = subdir / ".." / target.name
    elif alias == "symlink":
        link = tmp_path / "report.json"
        try:
            link.symlink_to(target)
        except OSError:
            pytest.skip("symlink creation is not available on this host")
        target = link
    elif alias == "hardlink":
        link = tmp_path / "report.json"
        try:
            link.hardlink_to(target)
        except OSError:
            pytest.skip("hard link creation is not available on this host")
        target = link
    elif alias == "case":
        target = target.with_name(target.name.upper())
        if not target.exists():
            pytest.skip("filesystem is case-sensitive")

    output_flags = ["--json"] if json_output else []
    result = run_check(
        source, "--policy", policy, "--baseline", baseline, "--report", target, *output_flags
    )
    for path, original in before.items():
        assert path.read_bytes() == original
    assert result.returncode == 3
    message = f"--report would overwrite the {protected} file: {target}"
    if json_output:
        error = json.loads(result.stdout)["error"]
        assert error["kind"] == "argument"
        assert error["message"] == message
        assert result.stderr == ""
    else:
        assert result.stdout == ""
        assert message in result.stderr


@pytest.mark.parametrize("suffix", [".csv", ".parquet", ".html", ".md", ""])
def test_report_rejects_formats_that_cannot_be_reloaded(source: Path, tmp_path: Path, suffix: str):
    path = tmp_path / f"report{suffix}"
    result = run_check(source, "--max-null", "*=100", "--report", path, "--json")
    assert result.returncode == 3
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "argument"
    assert ".json" in error["message"]
    assert str(path) in error["message"]
    assert not path.exists()


def test_missing_quality_threshold_is_inconclusive(source: Path):
    result = run_check(source, "--metric", "schema", "--min-quality", "90", "--json")
    assert result.returncode == 2, result.stderr
    document = json.loads(result.stdout)
    assert document["verdict"] == "inconclusive"
    assert document["checks"][0]["reason"] == "quality_unavailable"
    assert "observed" not in document["checks"][0]


def test_explicit_availability_requirement_preserves_library_failure(source: Path):
    result = run_check(
        source,
        "--metric",
        "schema",
        "--min-quality",
        "90",
        "--require-metric",
        "quality",
        "--json",
    )
    assert result.returncode == 1, result.stderr
    expected = dp.profile_file(source, metrics=["schema"]).check(
        min_quality_score=90,
        require_metrics=["quality"],
    )
    assert json.loads(result.stdout) == expected.to_dict()
    assert len(expected.unevaluated) == 1


def test_empty_source_has_no_fabricated_score(source: Path):
    source.write_text("id,amount\n", encoding="utf-8")
    result = run_check(source, "--min-quality", "0", "--json")
    assert result.returncode == 2, result.stderr
    assert json.loads(result.stdout)["verdict"] == "inconclusive"


@pytest.mark.parametrize("scope,code", [("full_source", 2), ("observed", 0)])
def test_capped_input_respects_evidence_scope(source: Path, scope: str, code: int):
    result = run_check(
        source,
        "--max-rows",
        "1",
        "--max-null",
        "*=100",
        "--scope",
        scope,
        "--json",
    )
    assert result.returncode == code, result.stderr
    expected = dp.profile_file(source, max_rows=1).check(max_null_percentage=100, scope=scope)
    assert json.loads(result.stdout) == expected.to_dict()


def test_policy_file_supports_all_library_keywords(source: Path, tmp_path: Path):
    policy: dict[str, Any] = {
        "min_rows": 1,
        "min_quality_score": 0,
        "min_dimension_scores": {"completeness": 0},
        "max_null_percentage": {"*": 100, "amount": 50},
        "max_duplicate_rows": 0,
        "require_metrics": ["quality", "completeness"],
        "scope": "observed",
    }
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(policy), encoding="utf-8")
    result = run_check(source, "--policy", path, "--json")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == dp.profile_file(source).check(**policy).to_dict()


@pytest.mark.parametrize(
    "flags,code",
    [
        (("--min-rows", "3"), 0),
        (("--min-rows", "4"), 1),
        # A capped scan short of the minimum may be short only because of the cap.
        (("--min-rows", "2", "--max-rows", "1"), 2),
        (("--min-rows", "2", "--max-rows", "1", "--scope", "observed"), 1),
    ],
    ids=["enough", "too-few", "capped-full-source", "capped-observed"],
)
def test_min_rows_flag(source: Path, flags: tuple[str, ...], code: int):
    result = run_check(source, *flags, "--json")
    assert result.returncode == code, result.stderr
    (check,) = json.loads(result.stdout)["checks"]
    assert check["code"] == "min_rows"


def test_flags_replace_matching_policy_keys(source: Path, tmp_path: Path):
    path = tmp_path / "policy.json"
    path.write_text('{"max_null_percentage": {"amount": 0}}', encoding="utf-8")
    result = run_check(source, "--policy", path, "--max-null", "id=0", "--json")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == (
        dp.profile_file(source).check(max_null_percentage={"id": 0}).to_dict()
    )


def test_policy_file_may_start_with_a_utf8_bom(source: Path, tmp_path: Path):
    path = tmp_path / "policy.json"
    path.write_bytes(codecs.BOM_UTF8 + b'{"min_quality_score": 0}')
    result = run_check(source, "--policy", path, "--json")
    assert result.returncode == 0, result.stderr
    assert json.loads(result.stdout) == (
        dp.profile_file(source).check(min_quality_score=0).to_dict()
    )


def test_repeated_policy_flags_match_library(source: Path):
    result = run_check(
        source,
        "--min-quality",
        "0",
        "--min-dimension",
        "completeness=0",
        "--min-dimension",
        "consistency=0",
        "--max-null",
        "*=100",
        "--max-null",
        "id=0",
        "--max-duplicate-rows",
        "0",
        "--require-metric",
        "quality",
        "--require-metric",
        "completeness",
        "--scope",
        "observed",
        "--json",
    )
    assert result.returncode == 0, result.stderr
    expected = dp.profile_file(source).check(
        min_quality_score=0,
        min_dimension_scores={"completeness": 0, "consistency": 0},
        max_null_percentage={"*": 100, "id": 0},
        max_duplicate_rows=0,
        require_metrics=["quality", "completeness"],
        scope="observed",
    )
    assert json.loads(result.stdout) == expected.to_dict()


@pytest.mark.parametrize(
    "policy",
    [
        "{",
        "[]",
        "{}",
        '{"typo": 90}',
        '{"min_quality_score": 101}',
        '{"min_dimension_scores": []}',
        '{"require_metrics": "quality"}',
        '{"require_metrics": [[]]}',
        '{"scope": "unknown", "min_quality_score": 0}',
        '{"max_duplicate_rows": -1}',
        '{"min_quality_score": NaN}',
        json.dumps({"min_quality_score": 10**400}),
        # A quoted number is text, not a threshold (#771).
        '{"min_quality_score": "90"}',
        '{"max_null_percentage": {"*": "20"}}',
        # Rules about change, refused for their value before the missing baseline.
        '{"max_quality_score_drop": 101}',
        '{"max_dimension_score_drops": []}',
        '{"max_dimension_score_drops": [5]}',
        '{"max_null_percentage_increase": [5]}',
        '{"max_null_percentage_increase": "5"}',
        '{"forbid_added_columns": "yes"}',
    ],
)
def test_invalid_policy_is_a_policy_error(source: Path, tmp_path: Path, policy: str):
    path = tmp_path / "policy.json"
    path.write_text(policy, encoding="utf-8")
    result = run_check(source, "--policy", path, "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "policy"
    assert error["path"] == str(path)
    assert error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    ("policy_json", "flags", "expected_error", "file_origin"),
    [
        (
            '{"min_quality_score": 150}',
            (),
            "min_quality_score threshold",
            True,
        ),
        (
            '{"min_dimension_scores": {"not_a_dimension": 90}}',
            (),
            "not_a_dimension",
            True,
        ),
        (
            '{"scope": "invalid_scope", "min_quality_score": 90}',
            (),
            "invalid_scope",
            True,
        ),
        (
            '{"min_quality_score": 90}',
            ("--min-quality", "150"),
            "min_quality_score threshold",
            False,
        ),
        (
            '{"max_null_percentage": {"*": 150}}',
            (),
            "max_null_percentage threshold",
            True,
        ),
        (
            '{"max_duplicate_rows": -1}',
            (),
            "max_duplicate_rows",
            True,
        ),
        (
            '{"require_metrics": ["not_a_dimension"]}',
            (),
            "not_a_dimension",
            True,
        ),
        (
            '{"min_quality_score": 150}',
            ("--max-duplicate-rows", "0"),
            "min_quality_score threshold",
            True,
        ),
    ],
    ids=[
        "file-quality",
        "file-dimension",
        "file-scope",
        "flag-quality",
        "file-max-null",
        "file-max-duplicates",
        "file-required-metric",
        "file-error-with-valid-flag",
    ],
)
def test_policy_value_error_attribution(
    source: Path,
    tmp_path: Path,
    policy_json: str,
    flags: tuple[str, ...],
    expected_error: str,
    file_origin: bool,
):
    path = tmp_path / "policy.json"
    path.write_text(policy_json, encoding="utf-8")

    result = run_check(source, "--policy", path, *flags)

    assert result.returncode == 3
    assert result.stdout == ""
    assert expected_error in result.stderr
    if file_origin:
        assert f"policy file {path}:" in result.stderr
    else:
        assert str(path) not in result.stderr
    assert "Traceback" not in result.stderr

    json_result = run_check(source, "--policy", path, *flags, "--json")
    assert json_result.returncode == 3
    error = json.loads(json_result.stdout)["error"]
    assert error["kind"] == "policy"
    assert expected_error in error["message"]
    if file_origin:
        assert error["path"] == str(path)
    else:
        assert "path" not in error
    assert json_result.stderr == ""


def test_valid_cli_override_prevents_file_value_error(
    source: Path,
    tmp_path: Path,
):
    path = tmp_path / "policy.json"
    path.write_text('{"min_quality_score": 150}', encoding="utf-8")

    result = run_check(
        source,
        "--policy",
        path,
        "--min-quality",
        "0",
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert "pass:" in result.stderr


def test_scope_only_policy_file_combines_with_cli_requirement(
    source: Path,
    tmp_path: Path,
):
    path = tmp_path / "policy.json"
    path.write_text('{"scope": "observed"}', encoding="utf-8")

    result = run_check(
        source,
        "--policy",
        path,
        "--min-quality",
        "0",
        "--json",
    )

    assert result.returncode == 0, result.stderr

    output = json.loads(result.stdout)

    assert output["scope"] == "observed"
    assert output["verdict"] == "pass"


def test_invalid_json_policy_error_names_the_file(source: Path, tmp_path: Path):
    path = tmp_path / "policy.json"
    path.write_text("{", encoding="utf-8")
    result = run_check(source, "--policy", path, "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "policy"
    assert error["message"].startswith(f"policy file {path} is not valid JSON: ")


def test_absent_policy_file_is_a_policy_error(source: Path, tmp_path: Path):
    missing = tmp_path / "missing.json"
    result = run_check(source, "--policy", missing, "--min-quality", "0", "--json")
    assert result.returncode == 3
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "policy"
    assert error["path"] == str(missing)
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    "content,stage",
    [
        (None, "read"),
        ("{not json", "read"),
        ("[]", "read"),
        ('{"a": 1}', "read"),
        # load() accepts the shape of a flat summary; the gate cannot decode it.
        ('{"source": null, "execution": {}, "columns": []}', "compare with"),
    ],
    ids=["missing", "invalid-json", "array", "not-a-report", "undecodable-summary"],
)
@pytest.mark.parametrize("json_output", [False, True])
def test_an_unreadable_baseline_is_a_baseline_error(
    source: Path, tmp_path: Path, content: str | None, stage: str, json_output: bool
):
    baseline = tmp_path / "baseline.json"
    if content is not None:
        baseline.write_text(content, encoding="utf-8")
    flags = ["--json"] if json_output else []
    result = run_check(source, "--baseline", baseline, "--max-quality-drop", "5", *flags)
    assert result.returncode == 3
    assert "Traceback" not in result.stderr
    if json_output:
        error = json.loads(result.stdout)["error"]
        assert error["kind"] == "baseline"
        assert error["path"] == str(baseline)
        assert error["message"].startswith(f"cannot {stage} baseline {baseline}: ")
        assert result.stderr == ""
    else:
        assert result.stdout == ""
        assert f"cannot {stage} baseline {baseline}: " in result.stderr


@pytest.mark.parametrize(
    "policy",
    [
        '{"max_null_percentage": 0, "max_null_percentage": 100}',
        '{"max_null_percentage": {"amount": 0, "amount": 100}}',
    ],
)
def test_duplicate_policy_keys_cannot_silently_relax_a_gate(
    source: Path,
    tmp_path: Path,
    policy: str,
):
    path = tmp_path / "policy.json"
    path.write_text(policy, encoding="utf-8")
    result = run_check(source, "--policy", path, "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "policy"
    assert "duplicate policy key" in error["message"]
    assert str(path) in error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


def test_non_utf8_policy_file_is_reported_clearly(source: Path, tmp_path: Path):
    path = tmp_path / "policy_utf16.json"
    path.write_bytes('{"min_quality_score": 90}'.encode("utf-16"))
    result = run_check(source, "--policy", path, "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "policy"
    assert f"policy file {path} is not UTF-8: " in error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


def test_non_utf8_policy_file_in_text_mode_names_file_on_stderr(source: Path, tmp_path: Path):
    path = tmp_path / "policy_utf16.json"
    path.write_bytes('{"min_quality_score": 90}'.encode("utf-16"))
    result = run_check(source, "--policy", path)
    assert result.returncode == 3
    assert result.stdout == ""
    assert f"policy file {path} is not UTF-8: " in result.stderr


def test_non_object_policy_file_in_text_mode_names_file_on_stderr(source: Path, tmp_path: Path):
    path = tmp_path / "policy_array.json"
    path.write_text("[]", encoding="utf-8")
    result = run_check(source, "--policy", path)
    assert result.returncode == 3
    assert result.stdout == ""
    assert f"policy file {path} must contain a JSON object of check() keywords" in result.stderr


def test_unknown_policy_keys_in_text_mode_names_file_on_stderr(source: Path, tmp_path: Path):
    path = tmp_path / "policy_unknown.json"
    path.write_text('{"typo": 90}', encoding="utf-8")
    result = run_check(source, "--policy", path)
    assert result.returncode == 3
    assert result.stdout == ""
    assert f"policy file {path} has unknown policy keys: typo" in result.stderr


def _clean_baseline(tmp_path: Path, text: str = "id,amount\n1,10\n2,20\n3,30\n") -> Path:
    """Yesterday's report: the same columns as `source`, without its null."""
    clean = tmp_path / "yesterday.csv"
    clean.write_text(text, encoding="utf-8")
    baseline = tmp_path / "baseline.json"
    dp.profile_file(clean).save(baseline)
    return baseline


def _same_result(document: dict[str, Any]) -> dict[str, Any]:
    """The result without the identity of this run's report, new on every profile."""
    reports = (document.get("baseline") or {}).get("reports") or {}
    for key in ("id", "timestamp"):
        (reports.get("b") or {}).pop(key, None)
    return document


@pytest.mark.parametrize(
    "flags,rules,code",
    [
        (["--max-null-increase", "amount=50"], {"max_null_percentage_increase": {"amount": 50}}, 0),
        (["--max-null-increase", "amount=10"], {"max_null_percentage_increase": {"amount": 10}}, 1),
        (["--max-null-increase", "*=10"], {"max_null_percentage_increase": {"*": 10}}, 1),
        (["--max-quality-drop", "100"], {"max_quality_score_drop": 100}, 0),
        (
            ["--max-dimension-drop", "completeness=0"],
            {"max_dimension_score_drops": {"completeness": 0}},
            1,
        ),
    ],
)
def test_baseline_verdict_matches_the_library(
    source: Path, tmp_path: Path, flags: list[str], rules: dict[str, Any], code: int
):
    baseline = _clean_baseline(tmp_path)
    result = run_check(source, "--baseline", baseline, *flags, "--json")
    assert result.returncode == code, result.stdout + result.stderr
    expected = dp.profile_file(source).check(baseline=dp.ProfileReport.load(baseline), **rules)
    assert _same_result(json.loads(result.stdout)) == _same_result(expected.to_dict())
    assert json.loads(result.stdout)["baseline"]["reports"]["a"]["source"].endswith("yesterday.csv")


def test_an_incomparable_baseline_is_inconclusive(source: Path, tmp_path: Path):
    # A baseline that read one row of three does not cover its source, so a
    # full-source rule about change cannot be decided against it.
    clean = tmp_path / "yesterday.csv"
    clean.write_text("id,amount\n1,10\n2,20\n3,30\n", encoding="utf-8")
    baseline = tmp_path / "baseline.json"
    dp.profile_file(clean, max_rows=1).save(baseline)
    result = run_check(source, "--baseline", baseline, "--max-null-increase", "amount=0", "--json")
    assert result.returncode == 2, result.stdout + result.stderr
    document = json.loads(result.stdout)
    assert document["verdict"] == "inconclusive"
    (check,) = [c for c in document["checks"] if c["code"] == "max_null_percentage_increase"]
    assert check["status"] == "not_evaluated"
    assert check["reason"] is not None


def test_a_flat_summary_baseline_matches_the_library(source: Path, tmp_path: Path):
    clean = tmp_path / "yesterday.csv"
    clean.write_text("id,amount\n1,10\n2,20\n3,30\n", encoding="utf-8")
    baseline = tmp_path / "summary.json"
    baseline.write_text(json.dumps(dp.profile_file(clean).to_dict()), encoding="utf-8")
    rules: dict[str, Any] = {
        "max_null_percentage_increase": {"amount": 10},
        "max_quality_score_drop": 100,
    }
    result = run_check(
        source,
        "--baseline",
        baseline,
        "--max-null-increase",
        "amount=10",
        "--max-quality-drop",
        "100",
        "--json",
    )
    expected = dp.profile_file(source).check(baseline=dp.ProfileReport.load(baseline), **rules)
    assert result.returncode == {"pass": 0, "fail": 1, "inconclusive": 2}[expected.verdict]
    assert _same_result(json.loads(result.stdout)) == _same_result(expected.to_dict())


def test_policy_file_supports_the_rules_about_change(source: Path, tmp_path: Path):
    baseline = _clean_baseline(tmp_path)
    policy: dict[str, Any] = {
        "max_quality_score_drop": 100,
        "max_dimension_score_drops": {"completeness": 100},
        "max_null_percentage_increase": {"*": 100, "amount": 50},
        "forbid_removed_columns": True,
        "forbid_added_columns": True,
    }
    path = tmp_path / "policy.json"
    path.write_text(json.dumps(policy), encoding="utf-8")
    result = run_check(source, "--policy", path, "--baseline", baseline, "--json")
    assert result.returncode == 0, result.stdout + result.stderr
    expected = dp.profile_file(source).check(baseline=dp.ProfileReport.load(baseline), **policy)
    assert _same_result(json.loads(result.stdout)) == _same_result(expected.to_dict())

    # A flag replaces the matching key in full, as for the absolute rules.
    tightened = run_check(
        source, "--policy", path, "--baseline", baseline, "--max-null-increase", "amount=0"
    )
    assert tightened.returncode == 1, tightened.stderr


@pytest.mark.parametrize(
    "baseline_text,flag",
    [
        ("id,amount,note\n1,10,a\n2,20,b\n3,30,c\n", "--forbid-removed-columns"),
        ("id\n1\n2\n3\n", "--forbid-added-columns"),
    ],
)
def test_column_rules_against_a_baseline(
    source: Path, tmp_path: Path, baseline_text: str, flag: str
):
    baseline = _clean_baseline(tmp_path, baseline_text)
    result = run_check(source, "--baseline", baseline, flag, "--json")
    assert result.returncode == 1, result.stdout + result.stderr
    rules: dict[str, Any] = {flag.removeprefix("--").replace("-", "_"): True}
    expected = dp.profile_file(source).check(baseline=dp.ProfileReport.load(baseline), **rules)
    assert _same_result(json.loads(result.stdout)) == _same_result(expected.to_dict())


@pytest.mark.parametrize("from_file", [False, True])
@pytest.mark.parametrize("json_output", [False, True])
def test_a_rule_about_change_needs_a_baseline(
    source: Path, tmp_path: Path, from_file: bool, json_output: bool
):
    policy = tmp_path / "policy.json"
    policy.write_text('{"max_quality_score_drop": 5}', encoding="utf-8")
    rule: list[str | Path] = ["--policy", policy] if from_file else ["--max-quality-drop", "5"]
    flags = ["--json"] if json_output else []
    result = run_check(source, *rule, *flags)
    assert result.returncode == 3
    assert "Traceback" not in result.stderr
    if json_output:
        error = json.loads(result.stdout)["error"]
        assert error["kind"] == "policy"
        assert error["message"].endswith(
            "needs a baseline report to compare against: pass --baseline PATH"
        )
        # No file supplied the missing baseline, so none is named.
        assert "path" not in error
    else:
        assert result.stdout == ""
        assert "pass --baseline PATH" in result.stderr
        assert "policy file" not in result.stderr


def test_human_summary_names_the_baseline(source: Path, tmp_path: Path):
    baseline = _clean_baseline(tmp_path)
    result = run_check(source, "--baseline", baseline, "--max-quality-drop", "100")
    assert result.returncode == 0, result.stderr
    assert result.stdout == ""
    assert f"  baseline: {baseline}" in result.stderr


def test_deeply_nested_policy_is_a_policy_error(source: Path, tmp_path: Path):
    path = tmp_path / "policy.json"
    # Deep enough that json.loads raises RecursionError (a RuntimeError), which
    # the pre-fix except clause let escape as a traceback with exit 1. From 3.14
    # the C decoder's limit is the thread's stack, not sys.getrecursionlimit():
    # an 8 MB stack held 20000 and 50000 levels on CI, so stay far beyond it.
    depth = 1_000_000
    path.write_text("[" * depth + "]" * depth, encoding="utf-8")
    result = run_check(source, "--policy", path, "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "policy"
    assert error["path"] == str(path)
    assert f"policy file {path} is nested too deeply: " in error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr

    text_result = run_check(source, "--policy", path)
    assert text_result.returncode == 3, text_result.stderr
    assert text_result.stdout == ""
    assert f"policy file {path} is nested too deeply: " in text_result.stderr
    assert "Traceback" not in text_result.stderr


@pytest.mark.parametrize("flag", ["--engine", "--format"])
def test_bogus_engine_and_format_are_argument_errors(source: Path, flag: str):
    result = run_check(source, flag, "bogus", "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "argument"
    assert "bogus" in error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


def test_engine_alias_still_accepted(source: Path):
    result = run_check(source, "--engine", "streaming", "--max-null", "*=100")
    assert result.returncode == 0, result.stderr


def test_bogus_metric_is_an_argument_error(source: Path):
    result = run_check(source, "--metric", "bogus", "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "argument"
    assert "path" not in error
    assert "bogus" in error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


def test_metric_pack_name_is_case_insensitive(source: Path):
    result = run_check(source, "--metric", "SCHEMA", "--max-null", "*=100")
    assert result.returncode == 0, result.stderr


def test_negative_max_rows_is_an_argument_error(source: Path):
    result = run_check(source, "--max-rows", "-1", "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "argument"
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


def test_flag_only_policy_mistake_has_no_path(source: Path):
    result = run_check(source, "--min-quality", "150", "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "policy"
    assert "path" not in error
    assert result.stderr == ""


def test_missing_source_is_an_input_error(tmp_path: Path):
    missing = tmp_path / "missing.csv"
    result = run_check(missing, "--min-quality", "0", "--json")
    assert result.returncode == 3
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "input"
    assert error["path"] == str(missing)
    assert error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


def test_unknown_flag_is_an_argument_error(source: Path):
    result = run_check(source, "--bogus-flag", "--json")
    assert result.returncode == 3
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "argument"
    assert "--bogus-flag" in error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


def test_abbreviated_json_flag_still_selects_json_errors(source: Path):
    result = run_check(source, "--js", "--bogus-flag")
    assert result.returncode == 3
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "argument"
    assert "--bogus-flag" in error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


def test_unknown_flag_without_json_prints_usage_on_stderr(source: Path):
    result = run_check(source, "--bogus-flag")
    assert result.returncode == 3
    assert result.stdout == ""
    assert "usage:" in result.stderr
    assert "error: unrecognized arguments: --bogus-flag" in result.stderr


def test_invalid_flag_value_is_an_argument_error(source: Path):
    result = run_check(source, "--max-null", "not-a-pair", "--json")
    assert result.returncode == 3
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "argument"
    assert "NAME=PERCENT" in error["message"]
    assert result.stderr == ""


def test_inconclusive_still_exits_2(source: Path):
    result = run_check(source, "--metric", "schema", "--min-quality", "90", "--json")
    assert result.returncode == 2, result.stderr
    assert json.loads(result.stdout)["verdict"] == "inconclusive"


def test_help_documents_flags_and_exit_codes():
    result = run_check("--help")
    assert result.returncode == 0
    assert result.stderr == ""
    for text in (
        "--policy PATH",
        "--report PATH",
        "--json",
        "--min-quality",
        "--baseline PATH",
        "--max-quality-drop POINTS",
        "--max-dimension-drop NAME=POINTS",
        "--max-null-increase COLUMN=POINTS",
        "--forbid-removed-columns",
        "--forbid-added-columns",
        "Exit codes:",
        "0 =",
        "1 =",
        "2 =",
        "3 =",
    ):
        assert text in result.stdout


def test_version_flag():
    result = run_check("--version")
    assert result.returncode == 0
    assert result.stderr == ""
    # A development build carries the version of the release it follows, so
    # the line names the commit where the build recorded one.
    build = dp.capabilities()
    if build.commit is None:
        assert result.stdout.strip() == f"dataprof {dp.__version__}"
    else:
        dirty = ", dirty" if build.dirty else ""
        assert result.stdout.strip() == f"dataprof {dp.__version__} ({build.commit[:12]}{dirty})"


def test_verdict_summary_includes_version(source: Path):
    result = run_check(source, "--max-null", "*=100")
    assert f"dataprof {dp.__version__}" in result.stderr


def test_a_required_column_is_a_policy_key_and_a_flag(source: Path, tmp_path: Path):
    # `note` is not in the source, and a CSV header lists every column.
    policy = tmp_path / "columns.json"
    policy.write_text(json.dumps({"require_columns": ["id", "note"]}), encoding="utf-8")
    from_file = run_check(source, "--policy", policy, "--json")
    from_flags = run_check(source, "--require-column", "id", "--require-column", "note", "--json")
    expected = dp.profile_file(source).check(require_columns=["id", "note"]).to_dict()
    for result in (from_file, from_flags):
        assert result.returncode == 1, result.stderr
        assert json.loads(result.stdout) == expected

    policy.write_text(json.dumps({"require_columns": "id"}), encoding="utf-8")
    refused = run_check(source, "--policy", policy, "--json")
    assert refused.returncode == 3
    assert "require_columns must be a JSON array of names" in refused.stdout
    # A rule about change needs --baseline, which this run does not pass.
    policy.write_text(json.dumps({"forbid_added_columns": True}), encoding="utf-8")
    assert run_check(source, "--policy", policy, "--json").returncode == 3
