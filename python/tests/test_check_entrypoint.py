"""Exercise the installed module as a CI process, including its output streams."""

from __future__ import annotations

import codecs
import json
import subprocess
import sys
from pathlib import Path
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
):
    path = tmp_path / "quality report.JSON"
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


@pytest.mark.parametrize("protected", ["source", "--policy"])
@pytest.mark.parametrize("alias", ["direct", "parent", "symlink"])
@pytest.mark.parametrize("json_output", [False, True])
def test_report_cannot_overwrite_inputs(
    tmp_path: Path, protected: str, alias: str, json_output: bool
):
    source = tmp_path / "events.json"
    source.write_text('[{"id": 1, "amount": 10}, {"id": 2, "amount": 20}]', encoding="utf-8")
    policy = tmp_path / "policy.json"
    policy.write_text('{"max_null_percentage": {"*": 100}}', encoding="utf-8")
    before = {path: path.read_bytes() for path in (source, policy)}
    target = source if protected == "source" else policy
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

    output_flags = ["--json"] if json_output else []
    result = run_check(source, "--policy", policy, "--report", target, *output_flags)
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
    policy = {
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


def test_absent_baseline_is_an_argument_error(source: Path, tmp_path: Path):
    result = run_check(source, "--baseline", tmp_path / "missing.json", "--json")
    assert result.returncode == 3
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "argument"
    assert "--baseline is not supported by the quality-gate API yet" in error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


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
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


def test_existing_baseline_is_explicitly_unsupported(source: Path, tmp_path: Path):
    baseline = tmp_path / "baseline.json"
    dp.profile_file(source).save(baseline)
    result = run_check(source, "--baseline", baseline, "--min-quality", "0", "--json")
    assert result.returncode == 3
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "argument"
    assert "--baseline is not supported by the quality-gate API yet" in error["message"]
    assert result.stderr == ""


def test_unsupported_baseline_without_json_explains_on_stderr(source: Path, tmp_path: Path):
    baseline = tmp_path / "baseline.json"
    dp.profile_file(source).save(baseline)
    result = run_check(source, "--baseline", baseline, "--min-quality", "0")
    assert result.returncode == 3
    assert result.stdout == ""
    assert "--baseline is not supported by the quality-gate API yet" in result.stderr


def test_deeply_nested_policy_is_a_policy_error(source: Path, tmp_path: Path):
    path = tmp_path / "policy.json"
    # Deep enough that json.loads raises RecursionError (a RuntimeError), which
    # the pre-fix except clause let escape as a traceback with exit 1.
    path.write_text("[" * 20000 + "]" * 20000, encoding="utf-8")
    result = run_check(source, "--policy", path, "--json")
    assert result.returncode == 3, result.stderr
    error = json.loads(result.stdout)["error"]
    assert error["kind"] == "policy"
    assert error["path"] == str(path)
    assert error["message"]
    assert result.stderr == ""
    assert "Traceback" not in result.stderr


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
        "--baseline",
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
    assert result.stdout.strip() == f"dataprof {dp.__version__}"
    assert result.stderr == ""


def test_verdict_summary_includes_version(source: Path):
    result = run_check(source, "--max-null", "*=100")
    assert f"dataprof {dp.__version__}" in result.stderr
