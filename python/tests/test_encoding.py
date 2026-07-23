"""Non-UTF-8 CSV input surfaces one clean encoding diagnostic (#426).

A latin-1 / windows-1252 CSV used to fail with stacked engine errors and
misleading advice. It should now raise a single ValueError that names the file,
guesses the encoding, and tells the caller to re-encode as UTF-8.

Run after building the extension:
    maturin develop --features python
    pytest python/tests/test_encoding.py -v
"""

from __future__ import annotations

import dataprof
import pytest


def test_latin1_csv_raises_clean_value_error(tmp_path):
    path = tmp_path / "latin1.csv"
    # "José" / "Málaga" encoded as latin-1 — invalid UTF-8.
    path.write_bytes("name,city\nJosé,Málaga\n".encode("latin-1"))

    try:
        dataprof.profile(str(path))
    except ValueError as exc:
        message = str(exc)
        assert "utf-8" in message.lower()
        # Actionable re-encode guidance, not the old stacked-engine noise.
        assert "iconv" in message
        assert "All engines failed" not in message
        assert "check file permissions" not in message.lower()
    else:
        raise AssertionError("non-UTF-8 CSV must raise, not profile silently")


def test_windows1252_csv_is_flagged(tmp_path):
    path = tmp_path / "cp1252.csv"
    # 0x93/0x94 are windows-1252 smart quotes (invalid UTF-8).
    path.write_bytes(b"quote\n\x93hi\x94\n")

    with pytest.raises(ValueError) as excinfo:
        dataprof.profile(str(path))
    assert "utf-8" in str(excinfo.value).lower()


def test_valid_utf8_csv_still_profiles(tmp_path):
    path = tmp_path / "utf8.csv"
    path.write_text("name,city\nJosé,Málaga\n", encoding="utf-8")
    report = dataprof.profile(str(path))
    assert report.rows == 1
