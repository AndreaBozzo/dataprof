"""Binary files bound for a text reader fail with what they are (#893).

A gzip file named ``data.csv.gz``, or a Parquet file without a ``.parquet``
extension, used to reach the CSV engines and fail with advice about delimiters
and column counts. Every file route now checks the signature bytes first and
raises ``ValueError`` naming the format and the way forward.

``tests/binary_input.rs`` is the Rust twin.
"""

from __future__ import annotations

import asyncio
import gzip
import io
import zipfile
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

try:
    import dataprof as dp
    import dataprof.asyncio as dpa
    from dataprof import interop
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")

CSV_TEXT = b"id,city\n1,Rome\n2,\n3,Milan\n"


def _zstd(payload: bytes) -> bytes:
    """Real zstd where the standard library has it (3.14+), else its signature."""
    try:
        from compression import zstd  # ty: ignore[unresolved-import]
    except ImportError:
        return b"\x28\xb5\x2f\xfd" + b"\x00\xff\x8b\x02"
    return zstd.compress(payload)


def _zip(payload: bytes) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr("data.csv", payload)
    return buffer.getvalue()


def _parquet() -> bytes:
    table = pa.table({"id": [1, 2, 3], "city": ["Rome", None, "Milan"]})
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    return buffer.getvalue()


def _binary_files(tmp_path: Path) -> list[tuple[Path, str]]:
    gz, zst, zp, parquet = gzip.compress(CSV_TEXT), _zstd(CSV_TEXT), _zip(CSV_TEXT), _parquet()
    files = [
        ("data.csv.gz", gz, "gzip-compressed"),
        ("gzip.csv", gz, "gzip-compressed"),
        ("gzip.jsonl", gz, "gzip-compressed"),
        ("data.csv.zst", zst, "zstd-compressed"),
        ("data.zip", zp, "a zip archive"),
        ("zip.csv", zp, "a zip archive"),
        ("parquet.csv", parquet, "a Parquet file"),
        ("parquet_no_extension", parquet, "a Parquet file"),
    ]
    written = []
    for name, payload, detected in files:
        path = tmp_path / name
        path.write_bytes(payload)
        written.append((path, detected))
    return written


ROUTES: list[tuple[str, Callable[[Path], Any]]] = [
    ("profile", lambda p: dp.profile(p)),
    ("profile columnar", lambda p: dp.profile(p, engine="columnar")),
    ("profile_file", lambda p: dp.profile_file(p)),
    ("interop.analyze_file", lambda p: interop.analyze_file(str(p))),
    ("infer_schema", lambda p: dp.infer_schema(p)),
    ("quick_row_count", lambda p: dp.quick_row_count(p)),
    ("analyze_structure", lambda p: dp.analyze_structure(p)),
    ("asyncio.profile_file", lambda p: asyncio.run(dpa.profile_file(p))),
]


def test_binary_files_fail_with_what_they_are_on_every_file_route(tmp_path: Path) -> None:
    wrong = []
    for path, detected in _binary_files(tmp_path):
        for route, run in ROUTES:
            try:
                run(path)
            except ValueError as err:
                message = str(err)
                if detected not in message:
                    wrong.append(f"{route} / {path.name}: does not say {detected!r}: {message}")
                elif "column count" in message or "delimiter" in message.lower():
                    wrong.append(f"{route} / {path.name}: parsing advice: {message}")
            except Exception as err:  # noqa: BLE001 - the type is what is checked
                wrong.append(f"{route} / {path.name}: {type(err).__name__}: {err}")
            else:
                wrong.append(f"{route} / {path.name}: profiled a binary file as text")
    assert not wrong, "\n".join(wrong)


def test_a_misnamed_parquet_file_profiles_once_the_format_is_selected(tmp_path: Path) -> None:
    path = tmp_path / "export.csv"
    path.write_bytes(_parquet())

    with pytest.raises(ValueError, match=r'format="parquet"'):
        dp.profile(path)

    report = dp.profile(path, format="parquet")
    assert report.rows == 3


def test_text_that_starts_like_a_signature_is_not_refused(tmp_path: Path) -> None:
    path = tmp_path / "par1.csv"
    path.write_bytes(b"PAR1,PAR2\n1,2\n3,4\n")
    report = dp.profile(path)
    assert report.rows == 2
