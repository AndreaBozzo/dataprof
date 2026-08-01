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
import re
from pathlib import Path
from typing import Any

import dataprof
import dataprof.agent
import pytest
from dataprof import Profiler, ProfileReport, StructureReport
from dataprof.agent import AgentGuard, SandboxPolicy

REPO_ROOT = Path(__file__).resolve().parents[2]

# Every file an agent runner loads as instructions. A new agent-facing doc
# belongs here; that is the point of the list.
AGENT_DOCS = (
    "AGENTS.md",
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


def test_agent_docs_name_the_redacting_summary() -> None:
    """The agent-safe summary must be reachable from the files agents load.

    ``to_llm_context()`` is the only export that enforces redaction (#332). A
    doc that teaches an agent to summarize a dataset without naming it is
    steering that agent toward the surfaces that do not redact.
    """
    unnamed = [rel for rel, text in _docs() if rel != "AGENTS.md" and "to_llm_context" not in text]
    assert not unnamed, (
        f"agent docs omit to_llm_context(): {unnamed}. It is the redaction-"
        "enforcing summary; agent instructions must offer it."
    )
