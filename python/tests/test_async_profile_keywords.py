"""Keyword arguments of the async profile functions.

``profile_bytes``, ``profile_file`` and ``profile_url`` accept the same
keywords as :func:`dataprof.profile_file`. The stubs spell them, so a typo is
visible to a type checker, and at runtime the interpreter raises the same
``TypeError`` suggestion ``profile()`` does.
"""

from __future__ import annotations

import ast
import asyncio
import inspect
import sys
from collections.abc import Callable
from pathlib import Path

import pytest

_PACKAGE = Path(__file__).resolve().parents[1] / "dataprof"
_PROFILE_FUNCTIONS = ("profile_bytes", "profile_file", "profile_url")
# ``format`` is the bytes/URL transport argument, not a ProfilerConfig field.
_NOT_A_CONFIG_KEYWORD = {
    "profile_bytes": frozenset({"format", "source_version"}),
    "profile_file": frozenset({"source_version"}),
    "profile_url": frozenset({"format", "source_version"}),
}


def _async_functions(tree: ast.Module) -> dict[str, ast.AsyncFunctionDef]:
    return {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name in _PROFILE_FUNCTIONS
    }


def _kwonly(function: ast.AsyncFunctionDef) -> dict[str, object]:
    """Keyword-only parameters, with ``Parameter.empty`` where there is no default."""
    spelled: dict[str, object] = {}
    for arg, default in zip(function.args.kwonlyargs, function.args.kw_defaults, strict=True):
        if default is None:
            spelled[arg.arg] = inspect.Parameter.empty
        elif isinstance(default, ast.Constant):
            spelled[arg.arg] = default.value
        else:
            raise AssertionError(f"{function.name} has a non-literal default for {arg.arg}")
    return spelled


def _forwarded_keywords(function: ast.AsyncFunctionDef) -> set[str]:
    forwarded: set[str] = set()
    for node in ast.walk(function):
        if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Name):
            continue
        if node.func.id != "_config_from_keywords" or not node.args:
            continue
        argument = node.args[0]
        if not isinstance(argument, ast.Dict):
            continue
        for key in argument.keys:
            if isinstance(key, ast.Constant) and isinstance(key.value, str):
                forwarded.add(key.value)
    return forwarded


def test_the_stub_spells_the_keywords_the_functions_accept() -> None:
    """The stub keyword list stays aligned with the implementation."""
    source = _async_functions(ast.parse((_PACKAGE / "asyncio.py").read_text(encoding="utf-8")))
    stub = _async_functions(ast.parse((_PACKAGE / "asyncio.pyi").read_text(encoding="utf-8")))
    assert set(source) == set(_PROFILE_FUNCTIONS)
    assert set(stub) == set(_PROFILE_FUNCTIONS)
    for name in _PROFILE_FUNCTIONS:
        assert _kwonly(stub[name]) == _kwonly(source[name])
        spelled = set(_kwonly(source[name]))
        assert _forwarded_keywords(source[name]) == spelled - _NOT_A_CONFIG_KEYWORD[name]


def _suggestion(message: str) -> str | None:
    marker = ". Did you mean "
    if marker not in message:
        return None
    return message.split(marker, 1)[1]


def _error(call: Callable[[], object]) -> str:
    with pytest.raises(TypeError) as caught:
        call()
    return str(caught.value)


def test_a_misspelled_keyword_raises_the_sync_suggestion() -> None:
    """A typo raises TypeError with the suggestion ``profile()`` gives."""
    dataprof = pytest.importorskip("dataprof")
    from dataprof.asyncio import profile_bytes, profile_file, profile_url

    sync_near = _error(lambda: dataprof.profile("missing.csv", max_row=1))
    assert "unexpected keyword argument 'max_row'" in sync_near
    if sys.version_info >= (3, 13):
        assert _suggestion(sync_near) == "'max_rows'?"

    sync_far = _error(lambda: dataprof.profile("missing.csv", not_an_option=1))
    assert "unexpected keyword argument 'not_an_option'" in sync_far
    assert _suggestion(sync_far) is None

    near = (
        lambda: asyncio.run(
            profile_file("missing.csv", max_row=1)  # ty: ignore[unknown-argument]
        ),
        lambda: asyncio.run(
            profile_bytes(b"id\n1\n", format="csv", max_row=1)  # ty: ignore[unknown-argument]
        ),
        lambda: asyncio.run(
            profile_url("http://127.0.0.1:9/x.csv", max_row=1)  # ty: ignore[unknown-argument]
        ),
    )
    far = (
        lambda: asyncio.run(
            profile_file("missing.csv", not_an_option=1)  # ty: ignore[unknown-argument]
        ),
        lambda: asyncio.run(
            profile_bytes(b"id\n1\n", format="csv", not_an_option=1)  # ty: ignore[unknown-argument]
        ),
        lambda: asyncio.run(
            profile_url("http://127.0.0.1:9/x.csv", not_an_option=1)  # ty: ignore[unknown-argument]
        ),
    )
    for call in near:
        message = _error(call)
        assert "unexpected keyword argument 'max_row'" in message
        assert _suggestion(message) == _suggestion(sync_near)
    for call in far:
        message = _error(call)
        assert "unexpected keyword argument 'not_an_option'" in message
        assert _suggestion(message) == _suggestion(sync_far)


def test_async_profile_keywords_match_profile_file() -> None:
    """The three async functions accept the keywords ``profile_file`` accepts."""
    dataprof = pytest.importorskip("dataprof")
    from dataprof.asyncio import profile_bytes, profile_file, profile_url

    def keywords(function: Callable[..., object]) -> dict[str, object]:
        return {
            name: parameter.default
            for name, parameter in inspect.signature(function).parameters.items()
            if parameter.kind is inspect.Parameter.KEYWORD_ONLY
        }

    sync = keywords(dataprof.profile_file)
    assert keywords(profile_file) == sync
    assert keywords(profile_url) == sync
    spelled = keywords(profile_bytes)
    assert spelled["format"] is inspect.Parameter.empty
    spelled["format"] = None
    assert spelled == sync
