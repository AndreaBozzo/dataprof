"""Partial analysis, schema inference, and row-count estimation."""

from __future__ import annotations

from pathlib import Path

import pytest
from conftest import CSV_FILE, FIXTURES

try:
    import dataprof
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )


class TestPartialAnalysis:
    def test_infer_schema(self):
        result = dataprof.infer_schema(CSV_FILE)
        assert result.num_columns > 0
        assert len(result.column_names) == result.num_columns
        assert result.rows_sampled > 0

    def test_header_only_csv_surfaces_agree_on_declared_schema(self, tmp_path):
        path = tmp_path / "header_only.csv"
        path.write_text("name,age\n")

        profile = dataprof.profile(path)
        schema = dataprof.infer_schema(path)
        structure = dataprof.analyze_structure(path)

        expected_names = ["name", "age"]
        assert profile.rows == schema.rows_sampled == structure.rows_sampled == 0
        assert list(profile) == list(schema.column_names) == expected_names
        assert [column.name for column in structure.columns] == expected_names
        assert [column["data_type"] for column in schema.columns] == ["string", "string"]
        assert [profile[name].data_type for name in expected_names] == ["string", "string"]
        assert [column.data_type for column in structure.columns] == ["string", "string"]
        assert schema.schema_stable is True

    def test_infer_schema_path_object(self):
        result = dataprof.infer_schema(Path(CSV_FILE))
        assert result.num_columns > 0

    def test_quick_row_count(self):
        result = dataprof.quick_row_count(CSV_FILE)
        assert result.count > 0
        assert isinstance(result.exact, bool)
        assert isinstance(result.method, str)

    def test_quick_row_count_exact_has_no_relative_error(self):
        # Small files are counted exactly; the confidence hint only applies to
        # sampled estimates, so it must be absent (not 0.0) here.
        result = dataprof.quick_row_count(CSV_FILE)
        assert result.exact is True
        assert result.relative_error is None

    def test_quick_row_count_path_object(self):
        result = dataprof.quick_row_count(Path(CSV_FILE))
        assert result.count > 0

    def test_analyze_structure_path_object(self):
        result = dataprof.analyze_structure(Path(CSV_FILE))
        assert result.source.endswith("small_comma.csv")
        assert result.format == "csv"
        assert result.row_count.count > 0
        assert result.rows_sampled > 0
        assert result.source_exhausted is True
        assert result.truncated is False
        assert result.delimiter == ","
        assert result.columns
        first = result.columns[0]
        assert first.name
        assert first.data_type in {
            "integer",
            "float",
            "string",
            "identifier",
            "date",
            "boolean",
        }
        assert first.provenance == "sample"

    def test_analyze_structure_default_max_rows(self, tmp_path):
        path = tmp_path / "many.csv"
        path.write_text("x\n" + "\n".join(str(i) for i in range(1001)) + "\n")

        result = dataprof.analyze_structure(path)
        assert result.row_count.count == 1001
        assert result.rows_sampled == 1000
        assert result.source_exhausted is False
        assert result.truncated is True
        assert result.truncation_reason == "max_rows(1000)"
        assert "structure_sample_truncated" in result.warnings

    def test_analyze_structure_none_max_rows_uses_default(self, tmp_path):
        path = tmp_path / "many.csv"
        path.write_text("x\n" + "\n".join(str(i) for i in range(1001)) + "\n")

        result = dataprof.analyze_structure(path, max_rows=None)
        assert result.rows_sampled == 1000
        assert result.truncated is True
        assert result.truncation_reason == "max_rows(1000)"

    def test_analyze_structure_column_summaries(self, tmp_path):
        path = tmp_path / "summary.csv"
        path.write_text("name,age\nAlice,30\nBob,\nCharlie,40\n")

        result = dataprof.analyze_structure(path, max_rows=10)
        age = next(col for col in result.columns if col.name == "age")
        assert age.data_type == "integer"
        assert age.total_count == 3
        assert age.null_count == 1
        assert age.null_ratio is not None
        assert abs(age.null_ratio - (1 / 3)) < 0.001
        assert age.unique_count is not None
        assert age.uniqueness_ratio is not None
        assert age.distinct_count_approximate is False

    def test_parquet_schema_agrees_with_profile_and_with_csv(self, tmp_path):
        """One dataset, two formats, one answer (#693).

        A Parquet writer that did not type its input leaves dates, integers and
        booleans in string columns. The schema used to be read from the file
        metadata alone, so ``infer_schema`` reported ``string`` for three of
        these five columns while ``profile`` on the same file reported the
        types the values carry.
        """
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        rows = 50
        columns = {
            "when": ["2026-01-02T03:04:05Z"] * rows,
            "ident": [f"ORD-{i}" for i in range(rows)],
            "numish": [str(i * 3) for i in range(rows)],
            "boolish": ["true", "false"] * (rows // 2),
            "real_int": [str(i) for i in range(rows)],
        }
        parquet_path = tmp_path / "typed.parquet"
        pq.write_table(pa.table({**columns, "real_int": list(range(rows))}), parquet_path)

        # The identical data as CSV. No value needs quoting, so a plain join is
        # the whole writer.
        csv_path = tmp_path / "typed.csv"
        lines = [",".join(columns)]
        lines += [",".join(values[row] for values in columns.values()) for row in range(rows)]
        csv_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        expected = {
            "when": "date",
            "ident": "string",
            "numish": "integer",
            "boolish": "boolean",
            "real_int": "integer",
        }
        for path in (parquet_path, csv_path):
            profile = dataprof.profile(path)
            schema = dataprof.infer_schema(path)
            structure = dataprof.analyze_structure(path)

            assert {name: profile[name].data_type for name in expected} == expected, path
            assert {c["name"]: c["data_type"] for c in schema.columns} == expected, path
            assert {c.name: c.data_type for c in structure.columns} == expected, path

    def test_parquet_structure_provenance_separates_metadata_from_sample(self, tmp_path):
        """A Parquet file is typed from two sources, and says which is which."""
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        path = tmp_path / "mixed.parquet"
        pq.write_table(pa.table({"id": [1, 2, 3], "label": ["a", "b", "c"]}), path)

        structure = dataprof.analyze_structure(path)
        provenance = {column.name: column.provenance for column in structure.columns}
        assert provenance == {"id": "metadata", "label": "sample"}
        # Only the text column needed values, and the file is short enough that
        # every row of it was read.
        assert structure.rows_sampled == 3
        assert structure.source_exhausted is True
        # The counts come from the footer, not from the rows read to type the
        # text column: they describe every row of the file (#700).
        assert [(c.name, c.total_count, c.null_count) for c in structure.columns] == [
            ("id", 3, 0),
            ("label", 3, 0),
        ]
        # No whole-file source for a distinct count, so none is claimed.
        assert all(column.unique_count is None for column in structure.columns)
        assert dataprof.infer_schema(path).schema_stable is True

    def test_parquet_structure_counts_describe_the_whole_file(self, tmp_path):
        """Counts come from the footer, so a truncated type sample does not
        narrow them -- and a float column, whose nulls the footer undercounts,
        is counted from its values instead (#700)."""
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        path = tmp_path / "counts.parquet"
        pq.write_table(
            pa.table(
                {
                    "id": [1, None, 3, 4],
                    "label": ["a", "b", None, None],
                    "score": [1.0, float("nan"), None, 4.0],
                }
            ),
            path,
        )

        structure = dataprof.analyze_structure(path)
        profile = dataprof.profile(path)

        for column in structure.columns:
            assert column.total_count == profile.rows, column.name
            assert column.null_count == profile[column.name].null_count, column.name

        # The float column's NaN counts as null, as it does in a full profile;
        # the footer alone would have said 1.
        counts = {column.name: column.null_count for column in structure.columns}
        assert counts == {"id": 1, "label": 2, "score": 2}

    def test_fully_typed_parquet_still_reads_no_rows(self, tmp_path):
        """The metadata fast path survives where the metadata is the answer."""
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")

        path = tmp_path / "typed_only.parquet"
        pq.write_table(pa.table({"id": [1, 2, 3], "score": [1.5, 2.5, 3.5]}), path)

        schema = dataprof.infer_schema(path)
        assert schema.rows_sampled == 0
        assert schema.schema_stable is True
        assert [c["data_type"] for c in schema.columns] == ["integer", "float"]

    def test_missing_file_raises_file_not_found(self):
        missing = str(FIXTURES / "does_not_exist.csv")
        with pytest.raises(FileNotFoundError, match="does_not_exist.csv") as excinfo:
            dataprof.infer_schema(missing)
        assert excinfo.value.filename == missing
        with pytest.raises(FileNotFoundError, match="does_not_exist.csv") as excinfo:
            dataprof.quick_row_count(missing)
        assert excinfo.value.filename == missing
        with pytest.raises(FileNotFoundError, match="does_not_exist.csv") as excinfo:
            dataprof.analyze_structure(missing)
        assert excinfo.value.filename == missing
