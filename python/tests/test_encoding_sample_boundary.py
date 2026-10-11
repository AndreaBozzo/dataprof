"""A valid boundary cannot replace a later UTF-8 error. Rust twin: encoding_sample_boundary.rs."""

from pathlib import Path

import dataprof as dp


def test_valid_utf8_at_the_sample_boundary_preserves_the_later_error(tmp_path: Path):
    path = tmp_path / "late-utf8.csv"
    prefix = b"value\n"
    path.write_bytes(prefix + b"a" * (1024 * 1024 - 1 - len(prefix)) + "é\n".encode() + b"\xff\n")
    wrong = []
    for engine in ("auto", "incremental", "columnar"):
        try:
            dp.profile(path, engine=engine)
        except Exception as error:
            message = str(error).lower()
            if "latin" in message or "windows-1252" in message:
                wrong.append(f"[{engine}] {error}")
        else:
            wrong.append(f"[{engine}] invalid UTF-8 accepted")
    assert not wrong, "\n".join(wrong)
