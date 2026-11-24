"""Arrow PyCapsule sources from arro3 profile without PyArrow installed.

arro3 is a minimal Arrow implementation that does not depend on PyArrow. The
sources are profiled in a child process where importing ``pyarrow`` fails, so
the test proves that the capsule paths need nothing from PyArrow. The parent
then compares each report with the profile of the same values as a PyArrow
table, which the other Arrow tests already pin.
"""

import json
import subprocess
import sys

import dataprof
import pytest

pytest.importorskip("arro3.core")
pa = pytest.importorskip("pyarrow")

_CHILD = r"""
import json
import sys

sys.modules["pyarrow"] = None  # any `import pyarrow` now raises ImportError

from arro3.core import Array, DataType, RecordBatch, RecordBatchReader, Table

import dataprof


def batch(z, a):
    return RecordBatch.from_pydict(
        {"z": Array(z, type=DataType.int64()), "a": Array(a, type=DataType.string())}
    )


batches = [batch([2**60, None], ["Roma", None]), batch([2**60 + 1, 4], ["東京", "Roma"])]
single = batch([2**60, None, 2**60 + 1, 4], ["Roma", None, "東京", "Roma"])
sources = {
    "reader": lambda: RecordBatchReader.from_batches(batches[0].schema, batches),
    "table": lambda: Table.from_batches(batches),
    "batch": lambda: single,
    "reader_capped": lambda: RecordBatchReader.from_batches(batches[0].schema, batches),
}

out = {}
for key, make in sources.items():
    kwargs = {"max_rows": 3} if key == "reader_capped" else {}
    report = dataprof.profile(make(), **kwargs)
    out[key] = {
        "columns": report.to_dict()["columns"],
        "rows": report.rows,
        "source_type": report.source_type,
        "engine": report.engine,
        "truncated": report.truncation_reason is not None,
        "quality_score": report.quality_score,
    }
out["pyarrow_blocked"] = sys.modules.get("pyarrow", "missing") is None
print(json.dumps(out))
"""


@pytest.fixture(scope="module")
def child_reports():
    result = subprocess.run(
        [sys.executable, "-c", _CHILD],
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=120,
    )
    assert result.returncode == 0, result.stderr
    return json.loads(result.stdout)


def _reference(**kwargs):
    table = pa.table(
        {
            "z": pa.array([2**60, None, 2**60 + 1, 4], type=pa.int64()),
            "a": pa.array(["Roma", None, "東京", "Roma"], type=pa.string()),
        }
    )
    return dataprof.profile(table, **kwargs)


def test_pyarrow_stays_blocked_in_child(child_reports):
    assert child_reports["pyarrow_blocked"] is True


@pytest.mark.parametrize(
    "key, source_type",
    # A batch takes the C Array path, labelled like a PyArrow RecordBatch. An
    # arro3 Table exports only a C stream, so it is profiled as one.
    [("reader", "stream"), ("table", "stream"), ("batch", "dataframe")],
)
def test_arro3_source_matches_pyarrow_table(child_reports, key, source_type):
    got = child_reports[key]
    reference = _reference()
    assert got["columns"] == reference.to_dict()["columns"]
    assert got["rows"] == 4
    assert got["quality_score"] == reference.quality_score
    assert got["truncated"] is False
    assert got["source_type"] == source_type
    assert got["engine"] == "columnar"


def test_arro3_reader_row_cap_crosses_batch_boundary(child_reports):
    got = child_reports["reader_capped"]
    reference = _reference(max_rows=3)
    assert got["rows"] == 3
    assert got["truncated"] is True
    assert got["columns"] == reference.to_dict()["columns"]
