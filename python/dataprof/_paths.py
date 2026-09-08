"""Path normalization shared by profiling and report persistence."""

from __future__ import annotations as _annotations

import errno as _errno
import os as _os
import pathlib as _pathlib


def _normalize_pathlike(path: str | _os.PathLike[str], *, arg_name: str = "path") -> str:
    """Normalize Python path-like input to the string form expected by Rust."""
    if isinstance(path, str):
        return path
    if isinstance(path, _os.PathLike):
        normalized = _os.fspath(path)
        if isinstance(normalized, str):
            return normalized
    raise TypeError(
        f"argument '{arg_name}': expected str or path-like object, "
        f"got {type(path).__module__}.{type(path).__name__}"
    )


def _normalize_existing_file(path: str | _os.PathLike[str], *, arg_name: str = "path") -> str:
    normalized = _normalize_pathlike(path, arg_name=arg_name)
    try:
        _pathlib.Path(normalized).stat()
    except FileNotFoundError:
        raise FileNotFoundError(_errno.ENOENT, _os.strerror(_errno.ENOENT), normalized) from None
    return normalized
