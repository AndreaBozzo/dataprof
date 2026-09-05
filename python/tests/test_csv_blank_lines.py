"""CSV byte buffers share the file readers' newline and empty-record policy."""

import asyncio

import dataprof
import pytest


@pytest.mark.parametrize("newline", [b"\n", b"\r", b"\r\n"])
@pytest.mark.parametrize("blank_lines", [False, True])
def test_csv_bytes_match_file_readers(tmp_path, newline, blank_lines):
    lines = [b"x,y", b"1,a", b"2,b"]
    if blank_lines:
        lines = [b"", lines[0], b"", lines[1], b"", lines[2], b""]
    data = newline.join(lines) + newline
    path = tmp_path / "records.csv"
    path.write_bytes(data)

    reference = dataprof.profile(path, engine="incremental").to_dict()["columns"]
    assert dataprof.profile(path, engine="columnar").to_dict()["columns"] == reference
    assert dataprof.profile(data, format="csv").to_dict()["columns"] == reference
    if dataprof.capabilities().async_streaming:
        from dataprof.asyncio import profile_bytes

        report = asyncio.run(profile_bytes(data, format="csv"))
        assert report.to_dict()["columns"] == reference


def test_quoted_empty_csv_record_is_a_null_row():
    report = dataprof.profile(b'x\n\n""\n1\n', format="csv")
    assert report.rows == 2
    assert report["x"].null_count == 1
