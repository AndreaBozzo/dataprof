"""Acceptance smoke test for the published dataprof wheel.

This runs against an *installed wheel*, not an editable build, and asserts the
base-wheel contract: everything here must work with zero Python dependencies
and no source build. Anything that needs pandas, a compiled async extension, or
a database connector belongs in the regular pytest suite, not here.

Run it from a directory outside the repository, so that `import dataprof`
resolves to the installed package rather than the `python/dataprof` sources::

    python .github/scripts/wheel_smoke.py

Exits non-zero on the first failed check.
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import dataprof as dp


def check(condition: bool, label: str) -> None:
    if not condition:
        raise AssertionError(label)
    print(f"  ok  {label}")


def assert_no_optional_deps() -> None:
    """The base wheel must not silently depend on pandas being importable."""
    print("base wheel imports cleanly")
    check("pandas" not in sys.modules, "importing dataprof does not import pandas")
    check(hasattr(dp, "profile"), "dp.profile is exported")
    check(hasattr(dp, "analyze_structure"), "dp.analyze_structure is exported")
    check(hasattr(dp, "ProfileReport"), "dp.ProfileReport is exported")


def acceptance_first_profile() -> None:
    """Snippet 1: profile an ad-hoc input and render it, with no files involved."""
    print("acceptance: first profile")
    r = dp.profile({"age": [31, 42], "city": ["Rome", "Milan"]})
    check(r.rows == 2, "rows == 2")
    check(r.columns == 2, "columns == 2")
    check(r.quality_score is not None, "quality_score is computed")

    check(isinstance(r.to_json(), str), "to_json() returns str")
    check(isinstance(r.to_markdown(), str), "to_markdown() returns str")
    check(isinstance(r.quality_summary(), dict), "quality_summary() returns dict")
    check(isinstance(r.to_llm_context(), str), "to_llm_context() returns str")

    # Without pandas, describe() degrades to a dict-of-dicts rather than failing.
    check(isinstance(r.describe(), dict), "describe() degrades to dict without pandas")

    check(r["age"].data_type == "integer", "column access infers age as integer")


def acceptance_pandas_paths_fail_loudly() -> None:
    """Base-wheel contract: pandas-only exports must raise a directive error.

    They must not raise a bare `ModuleNotFoundError` from deep inside the call,
    and they must not silently return something useless.
    """
    print("acceptance: pandas-only exports")
    try:
        dp.profile({"a": [1, 2]}).to_dataframe()
    except ImportError as exc:
        check("pandas is required" in str(exc), "to_dataframe() names the missing dependency")
        check("install" in str(exc).lower(), "to_dataframe() says how to fix it")
    else:
        raise AssertionError("to_dataframe() must raise ImportError without pandas")


def acceptance_export_reload_compare(workdir: Path) -> None:
    """Snippet 2: the profile -> export -> reload -> compare loop."""
    print("acceptance: export / reload / compare")
    raw = workdir / "raw.csv"
    clean = workdir / "clean.csv"
    raw.write_text("id,score\n1,10\n2,\n3,30\n", encoding="utf-8")
    clean.write_text("id,score\n1,10\n3,30\n", encoding="utf-8")

    before = dp.profile(str(raw))
    after = dp.profile(str(clean))

    delta = before.compare(after)
    check(isinstance(delta, dict), "compare() returns a dict")

    saved = workdir / "before.json"
    before.save(str(saved))
    check(saved.exists(), "save() writes the report")
    check(isinstance(json.loads(saved.read_text(encoding="utf-8")), dict), "saved file is JSON")

    loaded = dp.ProfileReport.load(str(saved))
    check(loaded.quality_score == before.quality_score, "load() round-trips quality_score")
    check(loaded.rows == before.rows, "load() round-trips rows")

    structure = dp.analyze_structure(str(raw))
    check(len(structure.columns) == 2, "analyze_structure() sees 2 columns")
    check([c.name for c in structure.columns] == ["id", "score"], "structure names columns")


def acceptance_agent_output_is_redacted(workdir: Path) -> None:
    """Agent-facing output must never echo sensitive values. Release-critical."""
    print("acceptance: agent output redaction")
    cards = workdir / "cards.csv"
    rows = ["card"] + [f"411111111111{1000 + i}" for i in range(10)]
    cards.write_text("\n".join(rows) + "\n", encoding="utf-8")

    full = dp.profile(str(cards))
    check(
        "4111111111111000" not in full.to_llm_context(include_samples=True),
        "detected sensitive pattern suppresses extrema",
    )

    # Pattern detection skipped -> sensitivity unknown -> must fail closed.
    partial = dp.profile(str(cards), metrics=["schema", "statistics"])
    check(
        "4111111111111000" not in partial.to_llm_context(include_samples=True),
        "unscanned column suppresses extrema (fail closed)",
    )


def main() -> int:
    print(f"dataprof {dp.__version__} from {Path(dp.__file__).parent}")
    if Path(dp.__file__).resolve().parents[1].name == "python":
        print("FAIL: imported the source tree, not the installed wheel", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory() as tmp:
        workdir = Path(tmp)
        assert_no_optional_deps()
        acceptance_first_profile()
        acceptance_pandas_paths_fail_loudly()
        acceptance_export_reload_compare(workdir)
        acceptance_agent_output_is_redacted(workdir)

    print("\nwheel smoke passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
