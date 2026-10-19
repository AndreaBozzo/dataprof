"""Duplicate hints do not weight metrics or duplicate bindings (#1004).

``tests/semantic_hint_duplicates.rs`` is the Rust twin.
"""

import json

import dataprof as dp
import pytest

DATA = {
    "z_old": ["2001-01-01", "2002-01-01", "2003-01-01", "2026-10-01"],
    "a_new": ["2026-10-01", "2026-10-02", "2026-10-03", "2099-01-01"],
    "value": [1, -2, 3, 4],
    "code": ["A", "B", "C", "D"],
}


@pytest.mark.parametrize(
    "route",
    [
        "dict",
        "rows",
        "auto",
        "incremental",
        "columnar",
        "builder",
        "json",
        "jsonl",
        "arrow",
        "parquet",
        "pandas",
        "polars",
    ],
)
def test_duplicate_hints_preserve_scores_and_bindings(tmp_path, route):
    path = tmp_path / "hints.csv"
    path.write_text(
        ",".join(DATA)
        + "\n"
        + "\n".join(",".join(map(str, row)) for row in zip(*DATA.values(), strict=True))
        + "\n"
    )

    def profile(temporal, copies):
        hints = dict(
            temporal_columns=temporal,
            positive_columns=["value"] * copies,
            identifier_columns=["code"] * copies,
        )
        if route == "builder":
            return (
                dp.Profiler()
                .temporal_columns(temporal)
                .positive_columns(hints["positive_columns"])
                .identifier_columns(hints["identifier_columns"])
                .profile(str(path))
            )
        if route == "dict":
            return dp.profile(DATA, **hints)
        if route == "rows":
            rows = [dict(zip(DATA, row, strict=True)) for row in zip(*DATA.values(), strict=True)]
            return dp.profile(rows, **hints)
        if route in ("json", "jsonl"):
            rows = [dict(zip(DATA, row, strict=True)) for row in zip(*DATA.values(), strict=True)]
            source = tmp_path / f"hints.{route}"
            source.write_text(
                json.dumps(rows) if route == "json" else "\n".join(map(json.dumps, rows))
            )
            return dp.profile(str(source), **hints)
        if route in ("arrow", "parquet"):
            pa = pytest.importorskip("pyarrow")
            table = pa.table(DATA)
            if route == "arrow":
                return dp.profile(table, **hints)
            import pyarrow.parquet as pq

            source = tmp_path / "hints.parquet"
            pq.write_table(table, source)
            return dp.profile(str(source), **hints)
        if route == "pandas":
            pd = pytest.importorskip("pandas")
            return dp.profile(pd.DataFrame(DATA), **hints)
        if route == "polars":
            pl = pytest.importorskip("polars")
            return dp.profile(pl.DataFrame(DATA), **hints)
        return dp.profile(str(path), engine=route, **hints)

    base = profile(["z_old", "a_new"], 1)
    for temporal in (
        ["z_old", "z_old", "a_new"],
        ["z_old", "a_new", "a_new"],
        ["z_old", "z_old", "a_new", "z_old", "a_new", "a_new"],
    ):
        repeated = profile(temporal, 3)
        assert repeated.to_dict()["quality"] == base.to_dict()["quality"]
        assert repeated.semantic_hint_bindings == base.semantic_hint_bindings
        assert [
            b["column"] for b in repeated.semantic_hint_bindings if b["kind"] == "temporal"
        ] == ["z_old", "a_new"]


def test_empty_and_unknown_hints_keep_validation():
    assert dp.profile(DATA, temporal_columns=[]).semantic_hint_bindings == []
    with pytest.raises(ValueError, match="missing"):
        dp.profile(DATA, temporal_columns=["missing", "missing"])
