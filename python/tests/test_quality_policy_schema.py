"""Test that docs/schema/quality-policy.v1.schema.json matches Python check gate contracts."""

import json
from pathlib import Path

import pytest

from dataprof._gate import _QUALITY_DIMENSIONS, _SCOPES
from dataprof.check.__main__.py import _POLICY_DEFAULTS


def test_quality_policy_schema_matches_policy_defaults():
    schema_path = (
        Path(__file__).resolve().parents[2]
        / "docs"
        / "schema"
        / "quality-policy.v1.schema.json"
    )
    assert schema_path.is_file(), f"Missing schema at {schema_path}"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    # Assert properties match _POLICY_DEFAULTS keys
    schema_props = set(schema["properties"].keys())
    policy_keys = set(_POLICY_DEFAULTS.keys())
    assert schema_props == policy_keys, (
        f"Schema properties mismatch.\nMissing in schema: {policy_keys - schema_props}\n"
        f"Extra in schema: {schema_props - policy_keys}"
    )

    # Assert scope enum matches _SCOPES
    assert set(schema["properties"]["scope"]["enum"]) == set(_SCOPES)

    # Assert dimension properties match _QUALITY_DIMENSIONS
    dim_props = set(schema["properties"]["min_dimension_scores"]["properties"].keys())
    assert dim_props == set(_QUALITY_DIMENSIONS)
    drop_dim_props = set(schema["properties"]["max_dimension_score_drops"]["properties"].keys())
    assert drop_dim_props == set(_QUALITY_DIMENSIONS)
