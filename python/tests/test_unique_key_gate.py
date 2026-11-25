"""Gate decisions and CLI profiling for ordered key groups."""

import json

import dataprof as dp
import pytest
from dataprof.check.__main__ import main

KEY = ["country", "year"]
POLICY = [KEY]


@pytest.fixture
def drops(tmp_path):
    good = tmp_path / "good.csv"
    good.write_text('country,year,"row,id"\nIT,2024,1\nIT,2025,2\n', encoding="utf-8")
    bad = tmp_path / "bad.csv"
    bad.write_text('country,year,"row,id"\nIT,2024,1\nIT,2025,2\nIT,2024,3\n', encoding="utf-8")
    return good, bad


def test_single_cli_requests_groups_and_keeps_literal_column_names(drops, tmp_path, capsys):
    _, bad = drops
    saved = tmp_path / "report.json"
    status = main(
        [
            str(bad),
            "--unique-column",
            "row,id",
            "--unique-key",
            "country,year",
            "--unique-key",
            "year,country",
            "--report",
            str(saved),
            "--json",
        ]
    )
    out = capsys.readouterr()
    assert status == 1
    checks = json.loads(out.out)["checks"]
    assert [(check["code"], check["status"]) for check in checks] == [
        ("unique_column", "passed"),
        ("unique_key", "failed"),
        ("unique_key", "failed"),
    ]
    assert checks[0]["column"] == "row,id"
    assert [check["key"]["columns"] for check in checks[1:]] == [KEY, KEY[::-1]]
    assert [check["key"]["repeats"]["sample_repeated_keys"] for check in checks[1:]] == [
        [["IT", "2024"]],
        [["2024", "IT"]],
    ]
    assert "1 repeated row, 1 distinct repeated key" in out.err
    assert '[["IT", "2024"]]' in out.err
    assert dp.ProfileReport.load(saved).check(unique_columns=POLICY).verdict == "fail"


def test_multi_cli_requests_policy_file_groups_for_every_source(drops, tmp_path, capsys):
    good, bad = drops
    policy = tmp_path / "policy.json"
    columns = ["country", "year", "row,id"]
    policy.write_text(json.dumps({"unique_columns": [KEY, columns]}), encoding="utf-8")
    assert main([str(good), str(bad), "--policy", str(policy), "--json"]) == 1
    output = capsys.readouterr()
    sources = json.loads(output.out)["sources"]
    assert [source["result"]["verdict"] for source in sources] == ["pass", "fail"]
    assert all(
        [check["key"]["columns"] for check in source["result"]["checks"]] == [KEY, columns]
        for source in sources
    )
    assert '[["IT", "2024"]]' in output.err


def test_cli_key_limit_changes_the_verdict_and_reports_the_hint(drops, tmp_path, capsys):
    good, bad = drops
    policy = tmp_path / "policy.json"
    policy.write_text(json.dumps({"unique_columns": [["absent"]]}), encoding="utf-8")
    args = ["--policy", str(policy), "--unique-key", "country,year", "--json"]
    assert main([str(good), *args, "--unique-keys-exact-limit", "1"]) == 2
    output = capsys.readouterr()
    check = json.loads(output.out)["checks"][0]
    assert check["reason"] == "approximate_count"
    assert "unique_keys_exact_limit" in check["message"]
    assert "--unique-keys-exact-limit 2" in output.err
    assert main([str(good), *args, "--unique-keys-exact-limit", "2"]) == 0
    assert main([str(bad), *args, "--unique-keys-exact-limit", "1"]) == 1
    assert "at least 1 repeated row" in capsys.readouterr().err


def test_cli_invalid_key_flag_and_policy_are_usage_errors(drops, tmp_path, capsys):
    good, _ = drops
    for value in ("", "country,", "country,country"):
        with pytest.raises(SystemExit) as error:
            main([str(good), "--unique-key", value, "--json"])
        assert error.value.code == 3
    capsys.readouterr()
    policy = tmp_path / "policy.json"
    policy.write_text('{"unique_columns": [["country", "country"]]}', encoding="utf-8")
    assert main([str(good), "--policy", str(policy), "--json"]) == 3
    assert "distinct column names" in json.loads(capsys.readouterr().out)["error"]["message"]


def test_explicit_singleton_uses_group_evidence(drops):
    good, _ = drops
    report = dp.profile_file(good, unique_keys=[["year"]], unique_keys_exact_limit=1)
    result = report.check(unique_columns=["year", ["year"]])
    assert result.verdict == "inconclusive"
    column, group = result.checks
    assert (column.code, column.status, column.observed) == ("unique_column", "passed", 0)
    assert group.code == "unique_key"
    assert group.reason == {"reason": "approximate_count"}
    assert group.key == {"columns": ["year"]}
    assert "unique_keys_exact_limit" in group.message


def test_key_group_without_recorded_evidence_is_inconclusive(drops):
    good, _ = drops
    plain = dp.profile_file(good)
    assert plain.composite_keys is None and "composite_keys" not in plain.to_dict()
    result = plain.check(unique_columns=POLICY)
    assert result.verdict == "inconclusive"
    assert result.checks[0].reason == {"reason": "not_assessed"}
    assert "unique_keys" in result.checks[0].message


def test_key_gate_fails_on_a_witness_after_the_limit(drops):
    _, bad = drops
    report = dp.profile_file(bad, unique_keys=POLICY, unique_keys_exact_limit=1)
    result = report.check(unique_columns=POLICY)
    assert result.verdict == "fail"
    (check,) = result.checks
    assert check.observed == 1
    assert check.key == {
        "columns": KEY,
        "repeats": {
            "distinct_repeated_keys": 1,
            "sample_repeated_keys": [["IT", "2024"]],
            "is_exact": False,
        },
    }


def test_key_gate_needs_complete_evidence(drops):
    _, bad = drops
    report = dp.profile_file(bad, unique_keys=POLICY, max_rows=2)
    for scope in ("observed", "full_source"):
        result = report.check(unique_columns=POLICY, scope=scope)
        assert result.verdict == "inconclusive"
        (check,) = result.checks
        assert check.reason == {"reason": "evidence_incomplete", "gap": "truncated"}


def test_key_gate_survives_canonical_round_trips_and_flat_baselines(drops):
    good, bad = drops
    baseline = dp.profile_file(good)
    report = dp.profile_file(bad, unique_keys=POLICY, unique_keys_exact_limit=2)
    expected = report.check(unique_columns=POLICY, baseline=baseline, max_row_count_drop=0)
    assert expected.verdict == "fail"
    assert expected.violations[0].code == "unique_key"
    for restored in (
        dp.ProfileReport.from_json(report.to_json()),
        dp.ProfileReport.from_dict(json.loads(report.to_json())),
    ):
        assert (
            restored.check(unique_columns=POLICY, baseline=baseline, max_row_count_drop=0).to_dict()
            == expected.to_dict()
        )
    flat = dp.ProfileReport.from_dict(report.to_dict())
    assert flat.composite_keys == report.composite_keys
    assert baseline.compare(flat)["configuration"]["unique_keys"]["b"] == {
        "groups": POLICY,
        "exact_limit": 2,
    }
    flat_baseline = dp.ProfileReport.from_dict(baseline.to_dict())
    assert (
        report.check(unique_columns=POLICY, baseline=flat_baseline, max_row_count_drop=0).checks
        == expected.checks
    )
