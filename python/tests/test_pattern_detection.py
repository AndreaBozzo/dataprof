"""Pattern semantics across input routes. Rust twin: tests/pattern_detection.rs."""

import json
import math
from pathlib import Path

import dataprof as dp
from _profile_routes import _routes

CASES = json.loads(
    (Path(__file__).parents[2] / "tests/fixtures/pattern_detection.json").read_text(
        encoding="utf-8"
    )
)


def check(group: str, tmp_path: Path):
    wrong = []
    for index, case in enumerate(CASES[group]):
        catalogue = {p["name"]: p for p in dp.list_patterns(case.get("locale"))}
        if group == "complete_iso_datetime" and not catalogue["DateTime (ISO)"]["regex"].endswith(
            "$"
        ):
            wrong.append("ISO datetime catalogue exposes an unanchored regex")
        serialized = None
        for route, run in _routes(tmp_path, locale=case.get("locale")).items():
            report = run(f"{group}-{index}", case["values"])
            patterns = {p.name: p for p in report["event"].patterns}
            for name, count in case["expected"].items():
                if name not in patterns:
                    wrong.append(f"[{route}] {group}/{index}: missing {name}: {patterns}")
                    continue
                pattern = patterns[name]
                if pattern.regex != catalogue[name]["regex"]:
                    wrong.append(f"[{route}] {group}/{index}: catalogue regex differs for {name}")
                percentage = count / len(case["values"]) * 100
                if pattern.match_count != count or not math.isclose(
                    pattern.match_percentage, percentage, rel_tol=1e-9, abs_tol=1e-12
                ):
                    got = f"{pattern.match_count}/{pattern.match_percentage}"
                    wrong.append(
                        f"[{route}] {group}/{index} {name}: {got}, expected {count}/{percentage}"
                    )
                if name in case.get("confidence", {}) and not math.isclose(
                    pattern.confidence, case["confidence"][name], rel_tol=1e-9, abs_tol=1e-12
                ):
                    wrong.append(
                        f"[{route}] {group}/{index} {name}: confidence {pattern.confidence}"
                    )
            for name in case.get("absent", []):
                if name in patterns:
                    wrong.append(f"[{route}] {group}/{index}: unexpected {name}")
            if "sensitive" in case:
                sensitive = any(f.code == "sensitive_pattern" for f in report.findings())
                expected = case["sensitive"]
                if sensitive != expected:
                    wrong.append(
                        f"[{route}] {group}/{index}: sensitive={sensitive}, expected {expected}"
                    )
            context = report.to_llm_context(max_tokens=4000)
            for name in case["expected"]:
                if name in patterns and patterns[name].confidence >= 0.5 and name not in context:
                    wrong.append(f"[{route}] {group}/{index}: {name} absent from LLM context")
            if "Date (EU slash)" in context:
                wrong.append(f"[{route}] {group}/{index}: stale slash label in LLM context")
            actual = (
                report.to_dict()["columns"][0]["patterns"],
                json.loads(report.to_json())["column_profiles"][0]["patterns"],
            )
            if serialized is None:
                serialized = actual
            elif actual != serialized:
                wrong.append(f"[{route}] {group}/{index}: serialized patterns differ")
    assert not wrong, "\n".join(wrong)


def test_configured_locale_patterns_cannot_be_suppressed_by_foreign_locales(tmp_path):
    check("configured_locale", tmp_path)


def test_unconfigured_postcodes_preserve_their_ambiguity(tmp_path):
    check("postcode_ambiguity", tmp_path)


def test_uppercase_uuids_match(tmp_path):
    check("uuid_case", tmp_path)


def test_pattern_counts_use_semantically_validated_values(tmp_path):
    check("validated_counts", tmp_path)


def test_printed_and_lowercase_ibans_match(tmp_path):
    check("printed_iban", tmp_path)


def test_domestic_italian_phones_require_formatting(tmp_path):
    check("domestic_phone", tmp_path)


def test_decimal_numbers_are_not_domestic_italian_phones(tmp_path):
    check("decimals_are_not_phones", tmp_path)


def test_coordinates_require_decimal_points_in_both_components(tmp_path):
    check("coordinate_decimal_points", tmp_path)


def test_iso_datetime_requires_complete_valid_values(tmp_path):
    check("complete_iso_datetime", tmp_path)


def test_slash_date_labels_do_not_assert_an_order(tmp_path):
    check("slash_date_label", tmp_path)
