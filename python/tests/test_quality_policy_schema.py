"""Test that docs/schema/quality-policy.v1.schema.json matches Python check gate contracts."""

import json
from pathlib import Path
from typing import Any

import pytest
from dataprof._gate import _COUNT_MAX, _SCOPES
from dataprof._report_schema import _QUALITY_DIMENSIONS
from dataprof.check.__main__ import _POLICY_DEFAULTS, _parser, _policy
from jsonschema import Draft202012Validator


def _get_schema_and_validator() -> tuple[dict[str, Any], Draft202012Validator]:
    """Load and validate the quality policy draft 2020-12 JSON Schema."""
    schema_path = (
        Path(__file__).resolve().parents[2] / "docs" / "schema" / "quality-policy.v1.schema.json"
    )
    assert schema_path.is_file(), f"Missing schema at {schema_path}"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return schema, Draft202012Validator(schema)


class _DummyBaseline:
    """Minimal baseline report mock implementing _delta_input for change-rule gate tests."""

    def _delta_input(self) -> str:
        """Return dummy serialized JSON string."""
        return "{}"


def _evaluate_gate(doc: dict[str, Any], with_baseline: bool, tmp_path: Path) -> tuple[bool, str]:
    """Evaluate a candidate policy dictionary through the dataprof check policy parser."""
    policy_file = tmp_path / "policy.json"
    policy_file.write_text(json.dumps(doc), encoding="utf-8")

    parser = _parser(["data.csv", "--policy", str(policy_file)])
    args = parser.parse_args(["data.csv", "--policy", str(policy_file)])
    baseline = _DummyBaseline() if with_baseline else None

    try:
        _policy(args, baseline)
        return True, ""
    except ValueError as exc:
        return False, str(exc)


def test_quality_policy_schema_structure_and_constants():
    """Verify schema properties, enums, $id, and definitions align with gate constants."""
    schema, _ = _get_schema_and_validator()

    assert (
        schema["$id"]
        == "https://andreabozzo.github.io/dataprof/schema/quality-policy.v1.schema.json"
    )
    assert schema["additionalProperties"] is False

    # Assert properties match _POLICY_DEFAULTS keys
    schema_props = set(schema["properties"].keys())
    policy_keys = set(_POLICY_DEFAULTS.keys())
    assert schema_props == policy_keys, (
        f"Schema properties mismatch.\nMissing in schema: {policy_keys - schema_props}\n"
        f"Extra in schema: {schema_props - policy_keys}"
    )

    # Assert scope enum matches _SCOPES
    assert set(schema["properties"]["scope"]["enum"]) == set(_SCOPES)

    # Assert require_metrics enum matches ("quality", *_QUALITY_DIMENSIONS)
    expected_metrics = set(("quality", *_QUALITY_DIMENSIONS))
    require_metrics_prop = schema["properties"]["require_metrics"]
    enum_values = None
    for option in require_metrics_prop.get("oneOf", [require_metrics_prop]):
        if option.get("type") == "array":
            enum_values = set(option["items"]["enum"])
            break
    assert enum_values == expected_metrics, (
        f"require_metrics enum mismatch.\nExpected: {expected_metrics}\nGot: {enum_values}"
    )

    # Assert dimension properties in DimensionScoreMap match _QUALITY_DIMENSIONS
    dim_props = set(schema["$defs"]["DimensionScoreMap"]["properties"].keys())
    assert dim_props == set(_QUALITY_DIMENSIONS)

    # Assert count definition bounds match _COUNT_MAX
    assert schema["$defs"]["Count"]["maximum"] == _COUNT_MAX
    assert schema["$defs"]["Count"]["minimum"] == 0


VALID_CASES = [
    # README example policy verbatim
    (
        {
            "min_rows": 1,
            "min_quality_score": 90,
            "max_null_percentage": {"customer_id": 0, "*": 20},
            "max_duplicate_rows": 0,
            "require_metrics": ["quality"],
            "scope": "full_source",
        },
        False,
    ),
    # Explicit nulls for keys whose default is None
    (
        {
            "min_rows": 10,
            "min_quality_score": None,
            "min_dimension_scores": None,
            "max_null_percentage": None,
            "max_duplicate_rows": None,
            "require_metrics": None,
            "require_columns": None,
            "max_quality_score_drop": None,
            "max_dimension_score_drops": None,
            "max_null_percentage_increase": None,
        },
        False,
    ),
    # Per-column null percentage increase with baseline
    (
        {"max_null_percentage_increase": {"email": 2.5, "*": 10.0}},
        True,
    ),
    # Bare number for max_null_percentage
    (
        {"max_null_percentage": 15.0},
        False,
    ),
    # Bare number for max_null_percentage_increase with baseline
    (
        {"max_null_percentage_increase": 5.0},
        True,
    ),
    # Duplicate entries in arrays (gate deduplicates silently)
    (
        {
            "min_rows": 5,
            "require_columns": ["col_a", "col_a"],
            "require_metrics": ["quality", "quality"],
        },
        False,
    ),
    # All dimensions specified in min_dimension_scores
    (
        {
            "min_dimension_scores": {
                "completeness": 95.0,
                "consistency": 90.0,
                "uniqueness": 95.0,
                "accuracy": 90.0,
                "timeliness": 85.0,
                "validity": 90.0,
                "precision": 90.0,
            }
        },
        False,
    ),
    # Maximum 64-bit integer count boundary (2^64 - 1)
    (
        {
            "min_rows": _COUNT_MAX,
            "max_duplicate_rows": _COUNT_MAX,
        },
        False,
    ),
    # Scope observed with valid constraint
    (
        {
            "min_rows": 1,
            "scope": "observed",
        },
        False,
    ),
    # Forbid columns false with valid constraint
    (
        {
            "min_rows": 1,
            "forbid_removed_columns": False,
            "forbid_added_columns": False,
        },
        False,
    ),
]


@pytest.mark.parametrize("doc,with_baseline", VALID_CASES)
def test_valid_policy_fixtures_pass_both_schema_and_gate(doc, with_baseline, tmp_path):
    """Verify that every valid policy fixture is accepted by both schema and gate."""
    _, validator = _get_schema_and_validator()
    assert validator.is_valid(doc), f"Schema rejected valid doc: {doc}"

    gate_ok, gate_err = _evaluate_gate(doc, with_baseline, tmp_path)
    assert gate_ok, f"Gate rejected valid doc: {gate_err}"


INVALID_CASES = [
    # Unknown key (rejected by additionalProperties and _policy)
    ({"min_rows": 10, "unknown_field": 123}, False),
    # Percentage above 100
    ({"min_quality_score": 100.1}, False),
    # Negative percentage
    ({"min_quality_score": -0.1}, False),
    # Count exceeds 2^64 - 1
    ({"min_rows": _COUNT_MAX + 1}, False),
    # Negative count
    ({"min_rows": -1}, False),
    # Invalid metric name not in ("quality", *_QUALITY_DIMENSIONS)
    ({"require_metrics": ["schema"]}, False),
    ({"require_metrics": ["statistics"]}, False),
    # Unknown quality dimension
    ({"min_dimension_scores": {"non_existent_dim": 50.0}}, False),
    # Null for non-nullable scope
    ({"min_rows": 1, "scope": None}, False),
    # Null for non-nullable forbid_removed_columns
    ({"min_rows": 1, "forbid_removed_columns": None}, False),
    # Invalid scope enum value
    ({"min_rows": 1, "scope": "invalid_scope"}, False),
    # Boolean passed for count
    ({"min_rows": True}, False),
    # String passed instead of array for require_columns
    ({"require_columns": "customer_id"}, False),
]


@pytest.mark.parametrize("doc,with_baseline", INVALID_CASES)
def test_invalid_policy_fixtures_fail_both_schema_and_gate(doc, with_baseline, tmp_path):
    """Verify that every invalid policy fixture is rejected by both schema and gate."""
    _, validator = _get_schema_and_validator()
    assert not validator.is_valid(doc), f"Schema unexpectedly accepted invalid doc: {doc}"

    gate_ok, _ = _evaluate_gate(doc, with_baseline, tmp_path)
    assert not gate_ok, f"Gate unexpectedly accepted invalid doc: {doc}"


RUNTIME_ONLY_DISAGREEMENTS = [
    # Empty policy: states no requirement, rejected at runtime by check()
    ({}, False, "states no requirement"),
    # Non-constraining scope: states no requirement
    ({"scope": "observed"}, False, "states no requirement"),
    # Non-constraining forbid flags: states no requirement
    ({"forbid_added_columns": False}, False, "states no requirement"),
    # Change rule without baseline: requires baseline report
    ({"max_quality_score_drop": 5.0}, False, "needs a baseline report"),
    ({"forbid_removed_columns": True}, False, "needs a baseline report"),
    # Float count: JSON Schema accepts 1.0 as integer, but json.loads returns float
    ({"min_rows": 1.0, "min_quality_score": 90.0}, False, "must be a whole number"),
]


@pytest.mark.parametrize(
    "doc,with_baseline,expected_error_fragment",
    RUNTIME_ONLY_DISAGREEMENTS,
)
def test_runtime_only_rules_documented_in_schema_description(
    doc, with_baseline, expected_error_fragment, tmp_path
):
    """Verify expected runtime-only rejections documented in schema description."""
    _, validator = _get_schema_and_validator()
    assert validator.is_valid(doc), f"Schema should accept syntactically conformant doc: {doc}"

    gate_ok, gate_err = _evaluate_gate(doc, with_baseline, tmp_path)
    assert not gate_ok, f"Gate should have rejected runtime-only invalid doc: {doc}"
    assert expected_error_fragment in gate_err, (
        f"Expected '{expected_error_fragment}' in error, got: {gate_err}"
    )
