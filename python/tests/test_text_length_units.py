"""Text lengths count Unicode scalar values, on every input path.

`min_length`, `max_length` and `avg_length` used to report UTF-8 byte counts
under names that say nothing about encoding, so `"東京"` measured 6 and `"🙂"`
measured 4 (#627). Every engine agreed, and every engine was surprising for
anything but ASCII.

Scalar values are the unit; grapheme clusters would need a segmentation policy
and a dependency, and encoded size is a property of the encoding rather than of
the value. A combining sequence therefore counts each scalar, which is pinned
below rather than left to be discovered.
"""

from __future__ import annotations

import json

import dataprof
import pytest

# UTF-8 widths are 1, 2, 6, 4; scalar counts are 1, 1, 2, 1.
SAMPLE = ["a", "é", "東京", "🙂"]
EXPECTED_MIN = 1
EXPECTED_MAX = 2
EXPECTED_AVG = pytest.approx(1.25)

ASCII_SAMPLE = ["a", "bb", "ccc", "dddd"]


def lengths(column):
    return column.min_length, column.max_length, column.avg_length


def write_csv(tmp_path, name: str, values: list[str]):
    path = tmp_path / name
    path.write_text("text\n" + "\n".join(values) + "\n", encoding="utf-8", newline="")
    return path


def test_in_memory_records_count_scalars() -> None:
    column = dataprof.profile({"text": SAMPLE})["text"]
    assert lengths(column) == (EXPECTED_MIN, EXPECTED_MAX, EXPECTED_AVG)


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar", "streaming", "arrow"])
def test_every_csv_engine_counts_scalars(tmp_path, engine: str) -> None:
    path = write_csv(tmp_path, f"unicode_{engine}.csv", SAMPLE)
    column = dataprof.profile(path, engine=engine)["text"]
    assert lengths(column) == (EXPECTED_MIN, EXPECTED_MAX, EXPECTED_AVG)


def test_json_and_jsonl_count_scalars(tmp_path) -> None:
    records = [{"text": value} for value in SAMPLE]

    array = tmp_path / "unicode.json"
    array.write_text(json.dumps(records, ensure_ascii=False), encoding="utf-8", newline="")

    lines = tmp_path / "unicode.jsonl"
    lines.write_text(
        "\n".join(json.dumps(record, ensure_ascii=False) for record in records) + "\n",
        encoding="utf-8",
        newline="",
    )

    for path in (array, lines):
        column = dataprof.profile(path)["text"]
        assert lengths(column) == (EXPECTED_MIN, EXPECTED_MAX, EXPECTED_AVG), path.name


def test_parquet_file_and_bytes_count_scalars(tmp_path) -> None:
    pa = pytest.importorskip("pyarrow")
    pq = pytest.importorskip("pyarrow.parquet")

    path = tmp_path / "unicode.parquet"
    pq.write_table(pa.table({"text": SAMPLE}), path)

    from_file = dataprof.profile(path)["text"]
    assert lengths(from_file) == (EXPECTED_MIN, EXPECTED_MAX, EXPECTED_AVG)

    from_bytes = dataprof.profile(path.read_bytes(), format="parquet")["text"]
    assert lengths(from_bytes) == (EXPECTED_MIN, EXPECTED_MAX, EXPECTED_AVG)


def test_arrow_input_counts_scalars() -> None:
    pa = pytest.importorskip("pyarrow")

    column = dataprof.profile(pa.table({"text": SAMPLE}))["text"]
    assert lengths(column) == (EXPECTED_MIN, EXPECTED_MAX, EXPECTED_AVG)


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_ascii_results_are_unchanged(tmp_path, engine: str) -> None:
    # The unit only moves for non-ASCII text, so an ASCII corpus must report
    # exactly what it did when lengths were byte counts.
    path = write_csv(tmp_path, f"ascii_{engine}.csv", ASCII_SAMPLE)
    column = dataprof.profile(path, engine=engine)["text"]
    assert lengths(column) == (1, 4, pytest.approx(2.5))


def test_a_combining_sequence_counts_each_scalar(tmp_path) -> None:
    # One grapheme, two scalars. Documented behaviour, not an accident: the
    # precomposed and decomposed spellings of the same word differ in length.
    path = write_csv(tmp_path, "combining.csv", ["é", "é"])
    column = dataprof.profile(path)["text"]
    assert lengths(column) == (1, 2, pytest.approx(1.5))


def test_the_serialized_report_carries_the_same_lengths(tmp_path) -> None:
    path = write_csv(tmp_path, "serialized.csv", SAMPLE)
    report = dataprof.profile(path)
    emitted = report.to_dict()["columns"][0]["stats"]

    assert emitted["min_length"] == EXPECTED_MIN
    assert emitted["max_length"] == EXPECTED_MAX
    assert emitted["avg_length"] == EXPECTED_AVG
    assert json.loads(report.to_json())["columns"][0]["stats"]["max_length"] == EXPECTED_MAX
