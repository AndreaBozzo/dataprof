"""The Python public surface is what ``__all__`` says it is (#514).

The Rust facade has had this guard for a while — ``tests/public_api_facade.rs``
pins its surface and ``docs/architecture/public-api-inventory.md`` records it,
with zero drift across 81 names. Python had no equivalent, and it showed:
``dataprof.__all__`` declared 28 names while 40 were reachable, because the
whole package lives in one module namespace and every ``import os`` landed in
it. ``dataprof.os``, ``dataprof.json`` and ``dataprof.pathlib`` were importable;
``dataprof.asyncio`` had no ``__all__`` at all.

Two checks, deliberately overlapping:

* the **structural** one — reachable public names equal ``__all__`` — fails the
  moment a new incidental import is added, without anyone editing this file;
* the **inventory** — the literal expected names below — makes adding to or
  removing from the public API a deliberate edit here, so it cannot happen as a
  side effect of an unrelated change.

Names beginning with an underscore are internal by convention and are not part
of the surface. ``__version__`` is the exception: it is a dunder that ``__all__``
declares on purpose, so it is checked for existence rather than for reachability.
"""

from __future__ import annotations

import importlib
from types import ModuleType

import pytest

EXPECTED_SURFACE: dict[str, frozenset[str]] = {
    "dataprof": frozenset(
        {
            "Capabilities",
            "ColumnProfile",
            "DataQualityMetrics",
            "ProgressEvent",
            "ProfileReport",
            "Profiler",
            "ProfilerConfig",
            "REPORT_SCHEMA_VERSION",
            "RecordBatch",
            "RowCountEstimate",
            "SamplingStrategy",
            "SchemaResult",
            "StopCondition",
            "StructureColumnSummary",
            "StructureReport",
            "__version__",
            "analyze_database_async",
            "analyze_structure",
            "asyncio",
            "capabilities",
            "column_to_dict",
            "count_table_rows_async",
            "get_table_schema_async",
            "infer_schema",
            "list_patterns",
            "profile",
            "profile_file",
            "quick_row_count",
            "test_connection_async",
        }
    ),
    "dataprof.agent": frozenset(
        {
            "AgentGuard",
            "AgentSecurityError",
            "AgentTimeoutError",
            "PathNotAllowedError",
            "ResourceLimitExceededError",
            "SandboxPolicy",
        }
    ),
    "dataprof.asyncio": frozenset(
        {
            "infer_schema_stream",
            "profile_bytes",
            "profile_file",
            "profile_url",
            "quick_row_count_stream",
        }
    ),
    "dataprof.interop": frozenset(
        {
            "ColumnProfile",
            "DataQualityMetrics",
            "ProfileReport",
            "ProfilerConfig",
            "RecordBatch",
            "analyze_csv_to_arrow",
            "analyze_file",
            "analyze_parquet_to_arrow",
            "column_to_dict",
            "profile_arrow",
            "profile_dataframe",
        }
    ),
}


def _module(name: str) -> ModuleType:
    return importlib.import_module(name)


def _reachable(module: ModuleType) -> set[str]:
    """Public attribute names, ignoring the package's own submodules.

    Importing ``dataprof.interop`` anywhere in the process binds ``interop`` as
    an attribute of ``dataprof`` — that is how Python packages work, not an
    incidental import, and counting it would make this check depend on which
    tests ran first.
    """
    names = set()
    for name in dir(module):
        if name.startswith("_"):
            continue
        value = getattr(module, name, None)
        if isinstance(value, ModuleType) and value.__name__.startswith(f"{module.__name__}."):
            continue
        names.add(name)
    return names


def _declared(module: ModuleType) -> set[str]:
    declared = getattr(module, "__all__", None)
    assert declared is not None, f"{module.__name__} declares no __all__"
    return set(declared)


@pytest.mark.parametrize("name", sorted(EXPECTED_SURFACE))
def test_reachable_names_equal_all(name):
    """Nothing is reachable that ``__all__`` does not declare.

    This is the check that catches a new ``import os`` at the top of the module
    without anyone remembering to update a list.
    """
    module = _module(name)
    declared = _declared(module)
    # __all__ may declare dunders (``__version__``); those are never reachable
    # under the non-underscore rule, so compare only the public-name half.
    public_declared = {n for n in declared if not n.startswith("_")}
    reachable = _reachable(module)

    leaked = sorted(reachable - public_declared)
    assert not leaked, (
        f"{name}: reachable but not declared in __all__: {leaked}. "
        "Import it under a private alias (import os as _os) or add it to __all__ "
        "if it is genuinely public."
    )


@pytest.mark.parametrize("name", sorted(EXPECTED_SURFACE))
def test_every_declared_name_exists(name):
    """``__all__`` cannot promise a name the module does not have.

    A stale entry makes ``from dataprof import *`` raise, and silently breaks
    anything reading ``__all__`` to build documentation.
    """
    module = _module(name)
    missing = sorted(n for n in _declared(module) if not hasattr(module, n))
    assert not missing, f"{name}: declared in __all__ but not defined: {missing}"


@pytest.mark.parametrize("name", sorted(EXPECTED_SURFACE))
def test_surface_matches_the_recorded_inventory(name):
    """Adding to or removing from the public API is a deliberate edit.

    The structural check above would happily accept a new public name; this one
    will not, so growth of the API surface shows up in review as a change to
    this file.
    """
    declared = _declared(_module(name))
    expected = EXPECTED_SURFACE[name]
    added = sorted(declared - expected)
    removed = sorted(expected - declared)
    assert not added and not removed, (
        f"{name}: public surface changed.\n"
        f"  added:   {added}\n"
        f"  removed: {removed}\n"
        "If this is intended, update EXPECTED_SURFACE in this file. Removing a "
        "documented name also needs a release note."
    )


def test_stdlib_modules_are_not_package_attributes():
    """The specific leak this issue was filed for.

    ``dataprof.os`` and friends worked purely because the implementation lives
    in one module namespace. Stated directly so the regression is named, not
    just implied by a set difference.
    """
    import dataprof

    for leaked in ("os", "json", "math", "pathlib", "warnings", "functools", "csv", "io"):
        assert not hasattr(dataprof, leaked), (
            f"dataprof.{leaked} is importable; bind it as _{leaked} instead"
        )


def test_typing_helpers_are_not_package_attributes():
    """Typing names leak the same way and are equally not API."""
    import dataprof

    for leaked in ("Any", "Callable", "Iterator", "TYPE_CHECKING", "cast", "annotations"):
        assert not hasattr(dataprof, leaked), (
            f"dataprof.{leaked} is importable; bind it as _{leaked} instead"
        )


def test_column_to_dict_is_public_and_consistent():
    """``column_to_dict`` was documented but undeclared; it is now exported.

    The stub declared it, ``docs/guides/examples.md`` uses it, and the changelog
    announced it — so the consistent resolution was to export it rather than
    make a documented name private.
    """
    import dataprof

    assert "column_to_dict" in dataprof.__all__
    assert callable(dataprof.column_to_dict)


def test_star_import_yields_exactly_the_declared_surface():
    """``from dataprof import *`` is the user-facing form of this contract."""
    namespace: dict[str, object] = {}
    exec("from dataprof import *", namespace)  # noqa: S102
    imported = {n for n in namespace if not n.startswith("__")}
    expected = {n for n in EXPECTED_SURFACE["dataprof"] if not n.startswith("_")}
    assert imported == expected, (
        f"star-import surface differs: unexpected {sorted(imported - expected)}, "
        f"missing {sorted(expected - imported)}"
    )
