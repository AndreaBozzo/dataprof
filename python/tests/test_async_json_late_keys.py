"""A JSON key first seen after the async reader's first chunk is a column (#1020).

``tests/async_json_late_keys.rs`` is the Rust twin; here the async route is
``dataprof.asyncio.profile_bytes`` and the reference is the sync bytes path.
"""

import asyncio
import json
from typing import Any

import dataprof as dp
import pytest
from dataprof import asyncio as dpa

pytestmark = pytest.mark.skipif(not dpa._HAS_ASYNC, reason="async support not compiled")


def _jsonl(records: list[dict[str, Any]]) -> bytes:
    return b"".join(json.dumps(record).encode() + b"\n" for record in records)


def _measured(report: dp.ProfileReport) -> dict[str, Any]:
    document = report.to_dict()
    return {"columns": document["columns"], "quality": document["quality"]}


CASES = {
    "second record": _jsonl([{"id": 1}, {"id": 2, "note": "a"}]),
    "mid-stream, absent again after": _jsonl(
        [{"id": 1}, {"id": 2}, {"id": 3, "note": "a"}, {"id": 4}]
    ),
    "two keys in different chunks": _jsonl([{"a": 1}, {"a": 2, "b": 3}, {"a": 4, "c": 5}]),
    "a late container": _jsonl([{"id": 1}, {"id": 2, "tags": [1, 2]}]),
    "duplicates either side of the key": _jsonl([{"id": 1}, {"id": 1, "x": 2}, {"id": 1}]),
}


def test_a_late_key_profiles_as_on_the_sync_path_at_every_chunk_size():
    wrong = []
    for name, data in CASES.items():
        expected = _measured(dp.profile(data, format="jsonl"))
        for chunk in ({"chunk_size": 1}, {"chunk_size": 64}, {}):
            report = asyncio.run(dpa.profile_bytes(data, format="jsonl", **chunk))
            if _measured(report) != expected:
                columns = [column["name"] for column in report.to_dict()["columns"]]
                wrong.append((name, chunk, columns))
    array = json.dumps([{"id": 1}, {"id": 2, "note": "a"}]).encode()
    expected = _measured(dp.profile(array, format="json"))
    for chunk in ({"chunk_size": 1}, {}):
        if _measured(asyncio.run(dpa.profile_bytes(array, format="json", **chunk))) != expected:
            wrong.append(("json array", chunk, None))
    assert wrong == []


def test_a_key_in_a_later_default_chunk_is_a_column():
    # 50,000 records cross the default 512 KiB chunk. The async path computes
    # quality over its retained sample and the sync bytes path over every row,
    # so only the column counts are compared.
    data = _jsonl([{"id": i, **({"late": 1} if i == 49999 else {})} for i in range(50000)])

    def counts(report: dp.ProfileReport) -> list[tuple[str, int, int]]:
        columns = report.to_dict()["columns"]
        return [(c["name"], c["total_count"], c["null_count"]) for c in columns]

    report = asyncio.run(dpa.profile_bytes(data, format="jsonl"))
    assert counts(report) == [("id", 50000, 0), ("late", 50000, 49999)]
    assert counts(report) == counts(dp.profile(data, format="jsonl"))
    assert report.to_dict()["execution"]["source_exhausted"] is True


def test_a_late_key_counts_the_earlier_records_as_missing():
    data = _jsonl([{"id": 1}, {"id": 2}, {"id": 3, "note": "a"}])
    report = asyncio.run(dpa.profile_bytes(data, format="jsonl", chunk_size=1)).to_dict()
    note = [column for column in report["columns"] if column["name"] == "note"]
    assert [(column["total_count"], column["null_count"]) for column in note] == [(3, 2)]
    assert report["execution"]["source_exhausted"] is True
