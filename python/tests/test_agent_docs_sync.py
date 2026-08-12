"""Keep agent-facing docs honest about the API they describe.

The skill, the Cursor rule, and the agent guide are the files an agent actually
loads before touching a dataset. They are hand-maintained copies of the same
knowledge, so they drift: at the time this test was written all three had gone
out of sync with the installed package, and nothing in CI noticed.

This test is the thing that notices. Every ``dp.foo`` / ``report.bar`` an agent
doc names must resolve against the installed package, and every keyword argument
shown in a Python snippet must be one the function actually accepts. A renamed
or removed API now fails the build instead of quietly teaching an agent to call
something that no longer exists.

Run after building the extension:
    uv run maturin develop
    uv run pytest python/tests/test_agent_docs_sync.py -v
"""

from __future__ import annotations

import ast
import inspect
import json
import re
import warnings
from pathlib import Path
from typing import Any

import dataprof
import dataprof.agent
import pytest
from dataprof import Profiler, ProfileReport, StructureReport
from dataprof.agent import AgentGuard, SandboxPolicy

REPO_ROOT = Path(__file__).resolve().parents[2]
EVALS = REPO_ROOT / ".claude/skills/dataprof/evals"

# Every file an agent runner loads as instructions. A new agent-facing doc
# belongs here; that is the point of the list.
AGENT_DOCS = (
    "AGENTS.md",
    ".claude/skills/dataprof/SKILL.md",
    ".claude/skills/dataprof/reference/api.md",
    ".claude/skills/dataprof/reference/interpretation.md",
    ".cursor/rules/dataprof.mdc",
    "docs/guides/agent-workflows.md",
)

# Docs that must offer the redacting summary. Reference files describe the wider
# surface, so they are exempt; the instruction files an agent follows are not.
INSTRUCTION_DOCS = (
    ".claude/skills/dataprof/SKILL.md",
    ".cursor/rules/dataprof.mdc",
    "docs/guides/agent-workflows.md",
)

# Receiver name -> the object its attributes must exist on. Docs use conventional
# variable names; anything not registered here is ignored, so a mention like
# `data.csv` is not mistaken for an API call. Adding a new receiver name to a doc
# means adding it here, which is the intended friction.
RECEIVERS: dict[str, Any] = {
    "dp": dataprof,
    "dataprof": dataprof,
    "report": ProfileReport,
    "before": ProfileReport,
    "after": ProfileReport,
    "other": ProfileReport,
    "structure": StructureReport,
    "profiler": Profiler,
    "guard": AgentGuard,
    "policy": SandboxPolicy,
    "caps": dataprof.Capabilities,
}

# File extensions, not attributes: agent docs cite paths like
# `.cursor/rules/dataprof.mdc`, and `dataprof.mdc` is not an API call.
_FILE_SUFFIXES = frozenset(
    ("md", "mdc", "csv", "json", "jsonl", "parquet", "py", "pyi", "rs", "toml", "html")
)

# The lookbehind keeps a receiver name from matching mid-path (`rules/dataprof.mdc`)
# or mid-identifier. The attribute chain stops at the first call, so
# `report.to_dict()["columns"]` contributes `report.to_dict` and nothing else.
_MENTION = re.compile(
    r"(?<![\w./-])(" + "|".join(sorted(RECEIVERS)) + r")\.([A-Za-z_][A-Za-z0-9_]*)"
)
_PYTHON_FENCE = re.compile(r"```python\n(.*?)```", re.DOTALL)

# Field-list sections in the reference name attributes bare, without a receiver,
# so the mention regex cannot see them. Map the section heading to the type(s)
# those names must exist on; a name may satisfy any type in the tuple.
FIELD_SECTIONS: dict[str, tuple[Any, ...]] = {
    "ColumnProfile fields": (dataprof.ColumnProfile,),
    "StructureReport fields": (
        dataprof.StructureReport,
        dataprof.StructureColumnSummary,
    ),
    "Capabilities fields": (dataprof.Capabilities,),
}

# Quality evidence is nested one level down — `quality.completeness` is a dict,
# not a score — so its keys are checked against the live dicts instead of
# against attributes. The reference documents them as a markdown table:
#   | `completeness` | `missing_values_ratio`, `complete_records_ratio`, ... |
QUALITY_TABLE_SECTION = "DataQualityMetrics fields"
_TABLE_ROW = re.compile(r"^\|\s*`([a-z_]+)`\s*\|(.+?)\|\s*$", re.MULTILINE)

# Attribute-style access to a quality accessor, as a doc would write it.
_QUALITY_ATTR = re.compile(r"\b(?:quality|q)\.([a-z_][a-z0-9_]*)")

# A bare backticked lowercase identifier inside a field-list section. Excludes
# `None`/`False` (capitalized) and glob shorthand like `*_interop`.
_BARE_FIELD = re.compile(r"`([a-z_][a-z0-9_]*)`")
_HEADING = re.compile(r"^#{2,}\s+(.+?)\s*$", re.MULTILINE)


def _docs() -> list[tuple[str, str]]:
    found = []
    for rel in AGENT_DOCS:
        path = REPO_ROOT / rel
        assert path.is_file(), f"agent doc {rel} is listed but missing"
        found.append((rel, path.read_text(encoding="utf-8")))
    return found


def _resolve(receiver: str, attribute: str) -> Any:
    """Return the documented attribute, or raise AttributeError."""
    return getattr(RECEIVERS[receiver], attribute)


def _dimension(report: ProfileReport, name: str) -> dict[str, Any]:
    """Return an assessed quality dimension, failing loudly if it is absent.

    Both ``quality`` and each dimension are optional, and ``None`` means "not
    assessed". A fixture that stopped producing a dimension would otherwise
    surface as an opaque TypeError halfway through a comparison.
    """
    quality = report.quality
    assert quality is not None, f"{report.source}: no quality metrics were computed"
    dimension = getattr(quality, name)
    assert isinstance(dimension, dict), (
        f"{report.source}: quality dimension {name!r} was not assessed, so the "
        "eval fixture no longer exercises what its rubric claims"
    )
    return dimension


@pytest.mark.parametrize("rel,text", _docs(), ids=[rel for rel, _ in _docs()])
def test_documented_attributes_exist(rel: str, text: str) -> None:
    """Every API name an agent doc mentions resolves on the installed package."""
    missing = []
    for receiver, attribute in _MENTION.findall(text):
        if attribute in _FILE_SUFFIXES:
            continue
        try:
            _resolve(receiver, attribute)
        except AttributeError:
            missing.append(f"{receiver}.{attribute}")

    assert not missing, (
        f"{rel} names APIs that do not exist on the installed package: "
        f"{sorted(set(missing))}. Fix the doc, or the rename that broke it."
    )


@pytest.mark.parametrize("rel,text", _docs(), ids=[rel for rel, _ in _docs()])
def test_documented_keyword_arguments_are_accepted(rel: str, text: str) -> None:
    """Every keyword shown in a Python snippet is one the callee accepts.

    Catches the drift a name-only check misses: the function survives a rename
    of its parameters, so the doc keeps resolving while every snippet in it
    raises TypeError.
    """
    bad = []
    for snippet in _PYTHON_FENCE.findall(text):
        try:
            tree = ast.parse(snippet)
        except SyntaxError:  # pragma: no cover - a fence that is not real code
            continue

        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            func = node.func
            if not isinstance(func, ast.Attribute) or not isinstance(func.value, ast.Name):
                continue
            if func.value.id not in RECEIVERS:
                continue

            try:
                target = _resolve(func.value.id, func.attr)
            except AttributeError:
                continue  # reported by the attribute test

            try:
                signature = inspect.signature(target)
            except (TypeError, ValueError):
                continue  # native callable without an introspectable signature

            accepts_any = any(
                p.kind is inspect.Parameter.VAR_KEYWORD for p in signature.parameters.values()
            )
            if accepts_any:
                continue

            call = f"{func.value.id}.{func.attr}"
            for keyword in node.keywords:
                if keyword.arg is None:  # **kwargs at the call site
                    continue
                if keyword.arg not in signature.parameters:
                    bad.append(f"{call}({keyword.arg}=...)")

    assert not bad, (
        f"{rel} passes keyword arguments the callee does not accept: {sorted(set(bad))}."
    )


def _sections(text: str) -> dict[str, str]:
    """Split markdown into {heading: body} at heading level 2 and deeper."""
    headings = list(_HEADING.finditer(text))
    return {
        m.group(1): text[
            m.end() : (headings[i + 1].start() if i + 1 < len(headings) else len(text))
        ]
        for i, m in enumerate(headings)
    }


@pytest.mark.parametrize("rel,text", _docs(), ids=[rel for rel, _ in _docs()])
def test_documented_fields_exist(rel: str, text: str) -> None:
    """Bare field names listed under a typed section exist on that type.

    The reference enumerates ~60 attributes with no receiver to anchor them, so
    the mention check cannot see them. Without this they are the largest patch
    of unverified surface in the skill — exactly where drift hides.
    """
    missing = []
    for heading, body in _sections(text).items():
        types = FIELD_SECTIONS.get(heading)
        if types is None:
            continue
        for field in _BARE_FIELD.findall(body):
            if not any(hasattr(t, field) for t in types):
                names = "/".join(t.__name__ for t in types)
                missing.append(f"{field} (not on {names})")

    assert not missing, f"{rel} lists fields that do not exist: {sorted(set(missing))}."


def test_field_sections_are_reachable() -> None:
    """Every registered field section is actually present in the reference.

    Renaming a heading would otherwise silently switch its field list off.
    """
    headings: set[str] = set()
    for _, text in _docs():
        headings |= set(_sections(text))

    unreachable = sorted(set(FIELD_SECTIONS) - headings)
    assert not unreachable, (
        f"FIELD_SECTIONS names headings no agent doc contains: {unreachable}. "
        "A renamed heading disables its field check silently."
    )


def test_agent_docs_name_the_redacting_summary() -> None:
    """The agent-safe summary must be reachable from the files agents load.

    ``to_llm_context()`` is the only export that enforces redaction (#332). A
    doc that teaches an agent to summarize a dataset without naming it is
    steering that agent toward the surfaces that do not redact.
    """
    unnamed = [
        rel for rel, text in _docs() if rel in INSTRUCTION_DOCS and "to_llm_context" not in text
    ]
    assert not unnamed, (
        f"agent docs omit to_llm_context(): {unnamed}. It is the redaction-"
        "enforcing summary; agent instructions must offer it."
    )


def test_documented_quality_dimension_keys_exist() -> None:
    """Every key in the quality dimension table exists in the live dimension dict.

    The dimensions are dicts, so their contents are invisible to both the
    attribute check and the type stubs. Profile the eval fixtures and take the
    union of keys each dimension actually produces — one fixture alone does not
    exercise every dimension.
    """
    observed: dict[str, set[str]] = {}
    for fixture in sorted((EVALS / "fixtures").glob("*.csv")):
        quality = dataprof.profile(str(fixture)).quality
        for dimension in (
            "completeness",
            "consistency",
            "uniqueness",
            "accuracy",
            "timeliness",
            "validity",
            "precision",
        ):
            value = getattr(quality, dimension)
            if isinstance(value, dict):
                observed.setdefault(dimension, set()).update(value)

    reference = (REPO_ROOT / ".claude/skills/dataprof/reference/api.md").read_text(encoding="utf-8")
    table = _sections(reference)[QUALITY_TABLE_SECTION]

    rows = _TABLE_ROW.findall(table)
    assert rows, (
        f"no dimension table found under '{QUALITY_TABLE_SECTION}'; the check "
        "silently covers nothing if the table is reformatted"
    )

    wrong = []
    for dimension, keys in rows:
        if dimension not in observed:
            wrong.append(f"{dimension} is not a quality dimension")
            continue
        wrong += [
            f"{dimension}[{key!r}] not produced by dataprof"
            for key in _BARE_FIELD.findall(keys)
            if key not in observed[dimension]
        ]

    assert not wrong, f"reference/api.md documents quality keys that do not exist: {wrong}"


def test_agent_docs_do_not_teach_deprecated_accessors() -> None:
    """No agent doc may name a DataQualityMetrics accessor that warns on access.

    Existence is not enough. The flat quality accessors were deprecated in 0.9
    but still resolve, still appear in ``dir()``, and are unmarked in the stubs,
    so a reference written by introspection documents them without noticing
    (#509). Access each one and let the warning decide.
    """
    quality = dataprof.profile(
        str(REPO_ROOT / ".claude/skills/dataprof/evals/fixtures/inventory_before.csv")
    ).quality

    deprecated = set()
    for name in dir(type(quality)):
        if name.startswith("_"):
            continue
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            try:
                getattr(quality, name)
            except Exception:  # pragma: no cover - not an accessor we can probe
                continue
        if any(issubclass(w.category, DeprecationWarning) for w in caught):
            deprecated.add(name)

    assert deprecated, (
        "probe found no deprecated quality accessors; if the deprecation was "
        "lifted or removed, delete this test rather than letting it pass vacuously"
    )

    # Only attribute-style access counts. `missing_values_ratio` named as a key
    # of the completeness dict is the correct way to reach the same evidence;
    # `quality.missing_values_ratio` is the deprecated way.
    taught = []
    for rel, text in _docs():
        body = re.sub(r"<details>.*?</details>", "", text, flags=re.DOTALL)
        taught += [
            f"{rel}: quality.{name}" for name in _QUALITY_ATTR.findall(body) if name in deprecated
        ]

    assert not taught, (
        f"agent docs name deprecated quality accessors: {sorted(set(taught))}. "
        "Document the nested dimension key instead; see #509."
    )


def test_eval_scenarios_reference_real_fixtures() -> None:
    """Every file a scenario names exists, relative to the evals directory."""
    scenarios = json.loads((EVALS / "scenarios.json").read_text(encoding="utf-8"))
    missing = [
        f"{s['id']}: {f}" for s in scenarios for f in s["files"] if not (EVALS / f).is_file()
    ]
    assert not missing, f"eval scenarios name fixtures that do not exist: {missing}"


def test_eval_rubrics_cite_current_fixture_values() -> None:
    """The numbers the rubrics grade against still match what dataprof produces.

    The scenarios and their README quote concrete values — ragged row counts,
    quality scores, missing-value ratios. If profiling changes and these are not
    updated, the rubrics grade an agent against fiction, and a passing eval
    means nothing.
    """
    ragged = dataprof.profile(str(EVALS / "fixtures/ragged_orders.csv"))
    before = dataprof.profile(str(EVALS / "fixtures/inventory_before.csv"))
    after = dataprof.profile(str(EVALS / "fixtures/inventory_after.csv"))
    pii = dataprof.profile(str(EVALS / "fixtures/customers_pii.csv"))
    payments = dataprof.profile(str(EVALS / "fixtures/payments_mixed_amount.csv"))

    quoted = (EVALS / "scenarios.json").read_text(encoding="utf-8") + (
        EVALS / "README.md"
    ).read_text(encoding="utf-8")

    observed = {
        "ragged_row_count": (ragged.ragged_row_count, 2),
        "future_dates_count": (_dimension(ragged, "timeliness")["future_dates_count"], 1),
        "duplicate_rows_before": (_dimension(before, "uniqueness")["duplicate_rows"], 1),
        "duplicate_rows_after": (_dimension(after, "uniqueness")["duplicate_rows"], 0),
    }
    stale = [
        f"{name}: rubrics say {expected}, profiling now gives {actual}"
        for name, (actual, expected) in observed.items()
        if actual != expected
    ]

    # Scores are quoted to one decimal; compare at that precision.
    for label, actual, expected in (
        ("before quality score", before.quality_score, 80.9),
        ("after quality score", after.quality_score, 97.9),
        # The high-score-is-not-clean rubric grades against this one. It went
        # stale unnoticed when #544 changed string-column consistency, because
        # the fixture was quoted here but never profiled.
        ("payments quality score", payments.quality_score, 97.9),
        (
            "before missing ratio",
            _dimension(before, "completeness")["missing_values_ratio"],
            22.2,
        ),
    ):
        if actual is None or round(actual, 1) != expected:
            stale.append(f"{label}: rubrics say {expected}, profiling now gives {actual}")

    # The redaction claim the PII rubric grades against, verified rather than assumed.
    context = pii.to_llm_context(max_tokens=800, include_samples=True)
    leaked = [
        value
        for value in ("alice@example.com", "IT60X0542811101000000123456", "+39 320 1234567")
        if value in context
    ]
    if leaked:
        stale.append(f"to_llm_context leaked sensitive values the rubric forbids: {leaked}")

    for pattern in ("Email", "IBAN"):
        if pattern not in quoted:
            stale.append(f"README/scenarios no longer name the {pattern} pattern")

    # The high-score-is-not-clean rubric now grades against a per-column flag
    # rather than the score alone, and quotes its wording. A flag that changed
    # shape would leave the rubric grading against a line dataprof no longer
    # emits — the same way the score went stale under #544.
    payments_flag = "amount_eur: mixed types (60% numeric, 40% text)"
    if payments_flag not in payments.to_llm_context():
        stale.append(f"to_llm_context no longer emits the quoted flag {payments_flag!r}")
    if payments_flag not in quoted:
        stale.append("README/scenarios no longer quote the amount_eur mixed-types flag")

    assert not stale, (
        "eval rubrics are out of date with what dataprof produces: "
        + "; ".join(stale)
        + ". Update .claude/skills/dataprof/evals/ before trusting an eval run."
    )
