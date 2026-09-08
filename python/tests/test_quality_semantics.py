"""Quality monotonicity, evidence, boolean columns, and absent or invalid values."""

from __future__ import annotations

import io
import json

import pytest
from conftest import CSV_FILE

try:
    import dataprof
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )


def _numeric_with_junk(junk: int, rows: int = 200) -> io.BytesIO:
    """One column of `rows` values, `junk` of them non-numeric."""
    values = [str(1000 + i) for i in range(rows - junk)]
    values += [f"junk{i}" for i in range(junk)]
    return io.BytesIO(("v\n" + "\n".join(values) + "\n").encode())


def _type_consistency(report: dataprof.ProfileReport) -> float:
    """``data_type_consistency``, failing loudly when it was not assessed.

    Both ``quality`` and each dimension are optional, and ``None`` means "not
    assessed" rather than "nothing wrong"; letting that through would make a
    dimension that stopped being computed look like a passing comparison.
    """
    quality = report.quality
    assert quality is not None, "no quality metrics were computed"
    consistency = quality.consistency
    assert isinstance(consistency, dict), "the consistency dimension was not assessed"
    return consistency["data_type_consistency"]


def _quality_score(report: dataprof.ProfileReport) -> float:
    score = report.quality_score
    assert score is not None, "no quality score was computed"
    return score


class TestQualityScoreMonotonicity:
    """Adding junk to a column must never raise its quality score (#544).

    A column that lost its numeric majority fell back to ``string``; every value
    conforms to ``string``, so consistency reported a perfect 100 however mixed
    the column actually was. 20% junk scored 100.0 while 18% junk scored 95.5.
    """

    def test_mixed_column_never_scores_perfect(self):
        for junk in range(10, 200, 10):
            report = dataprof.profile(_numeric_with_junk(junk), format="csv")
            consistency = _type_consistency(report)
            score = _quality_score(report)
            assert consistency < 100.0, (
                f"{junk}/200 junk values reported {consistency} type consistency"
            )
            assert score < 100.0, f"{junk}/200 junk values scored a perfect {score}"

    def test_score_never_rises_as_junk_replaces_numbers(self):
        previous = None
        for junk in range(0, 101, 10):  # 0% up to the 50/50 point
            score = _quality_score(dataprof.profile(_numeric_with_junk(junk), format="csv"))
            if previous is not None:
                assert score <= previous, (
                    f"score rose from {previous} to {score} at {junk}/200 junk values"
                )
            previous = score

    def test_wholly_textual_column_is_not_penalised(self):
        # The far end of the sweep is an ordinary text column, not a mixture,
        # and must still read as consistent.
        report = dataprof.profile(_numeric_with_junk(200), format="csv")
        assert report.profiles[0].data_type == "string"
        assert _type_consistency(report) == 100.0

    def test_engines_agree_on_type_consistency(self, tmp_path):
        # Consistency is measured over reservoir samples, so the engines have to
        # agree on the sample as well as on the arithmetic. Engine selection
        # only applies to file sources, so this cannot use a buffer.
        path = tmp_path / "mixed.csv"
        path.write_bytes(_numeric_with_junk(50).getvalue())
        scores = {
            engine: _type_consistency(
                dataprof.Profiler().engine(engine).profile(path),
            )
            for engine in ("auto", "incremental", "columnar")
        }
        assert set(scores.values()) == {75.0}, scores


class TestQualitySummary:
    def test_quality_summary_keys(self):
        r = dataprof.profile(CSV_FILE)
        qs = r.quality_summary()
        assert isinstance(qs, dict)
        expected_keys = {
            "source",
            "rows",
            "quality_score",
            "completeness",
            "consistency",
            "uniqueness",
            "accuracy",
            "timeliness",
            "validity",
            "precision",
            "execution_time_ms",
        }
        assert expected_keys == set(qs.keys())

    def test_quality_summary_values(self):
        r = dataprof.profile(CSV_FILE)
        qs = r.quality_summary()
        assert qs["rows"] == r.rows
        assert isinstance(qs["execution_time_ms"], int)
        assert qs["execution_time_ms"] >= 0

    def test_quality_summary_score_ranges(self):
        r = dataprof.profile(CSV_FILE)
        qs = r.quality_summary()
        score_keys = (
            "quality_score",
            "completeness",
            "consistency",
            "uniqueness",
            "accuracy",
            "timeliness",
            "validity",
            "precision",
        )
        for key in score_keys:
            v = qs[key]
            if v is not None:
                assert isinstance(v, (int, float)), f"{key} should be numeric"
                assert 0 <= v <= 100, f"{key}={v} should be in the 0-100 range"


class TestBooleanColumns:
    @pytest.fixture()
    def boolean_csv(self, tmp_path):
        p = tmp_path / "booleans.csv"
        lines = ["active,verified,count"]
        for i in range(100):
            active = "true" if i % 2 == 0 else "false"
            verified = "True" if i % 3 == 0 else "False"
            lines.append(f"{active},{verified},{i}")
        p.write_text("\n".join(lines))
        return str(p)

    @pytest.fixture()
    def report(self, boolean_csv):
        return dataprof.profile(boolean_csv)

    def test_boolean_detection(self, report):
        active = report["active"]
        assert active.data_type == "boolean"

    def test_boolean_stats_properties(self, report):
        active = report["active"]
        assert active.true_count == 50
        assert active.false_count == 50
        assert active.true_ratio is not None
        assert abs(active.true_ratio - 0.5) < 0.01

    def test_boolean_in_to_dict(self, report):
        d = report.to_dict()
        active_col = next(c for c in d["columns"] if c["name"] == "active")
        assert "stats" in active_col
        assert active_col["stats"]["true_count"] == 50
        assert active_col["stats"]["false_count"] == 50
        assert active_col["stats"]["true_ratio"] is not None

    def test_boolean_in_to_json(self, report):
        j = json.loads(report.to_json())
        active_col = next(c for c in j["columns"] if c["name"] == "active")
        assert "stats" in active_col
        assert "true_count" in active_col["stats"]

    def test_boolean_in_column_record(self, report):
        df = report.to_dataframe()
        active_row = df[df["name"] == "active"].iloc[0]
        assert active_row["true_count"] == 50
        assert active_row["false_count"] == 50

    def test_boolean_repr_shows_stats(self, report):
        r = repr(report)
        assert "true=" in r

    def test_integer_not_boolean(self, report):
        """Pure integer column should NOT be detected as boolean."""
        count_col = report["count"]
        assert count_col.data_type == "integer"

    def test_yes_no_stays_string(self, tmp_path):
        path = tmp_path / "yes_no.csv"
        path.write_text("subscribed\nyes\nno\nyes\nno\n")
        r = dataprof.profile(str(path))
        assert r["subscribed"].data_type == "string"

    def test_mixed_case_boolean_with_null_like_tokens(self, tmp_path):
        path = tmp_path / "booleans_nulls.csv"
        path.write_text("flag,label\ntrue,a\nFALSE,b\nTRUE,c\nfalse,d\nnull,e\nNULL,f\nnan,g\n,h\n")
        r = dataprof.profile(str(path))
        flag = r["flag"]
        assert flag.data_type == "boolean"
        assert flag.null_count == 4
        assert flag.true_count == 2
        assert flag.false_count == 2


class TestColumnLevelOutliers:
    """Regression: per-column `outlier_count` should surface IQR outliers."""

    def test_outlier_count_flags_spike(self, tmp_path):
        path = tmp_path / "spiky.csv"
        # 9 baseline rows around 22 + one obvious spike at 999.9
        path.write_text(
            "value\n"
            + "\n".join(["22.5", "23.1", "22.8", "23.2", "22.9", "23.0", "22.7", "23.1", "999.9"])
            + "\n"
        )
        r = dataprof.profile(str(path))
        assert r["value"].outlier_count is not None
        assert r["value"].outlier_count >= 1

    def test_outlier_count_zero_on_uniform_column(self, tmp_path):
        path = tmp_path / "flat.csv"
        path.write_text("value\n" + "\n".join(["10.0"] * 12) + "\n")
        r = dataprof.profile(str(path))
        assert r["value"].outlier_count == 0


class TestLowSampleWarning:
    def test_low_sample_warning_set_on_tiny_csv(self, tmp_path):
        path = tmp_path / "tiny.csv"
        path.write_text("x\n1\n2\n3\n")
        r = dataprof.profile(str(path))
        assert r.low_sample_warning is True
        assert r.to_dict()["quality"].get("low_sample_warning") is True

    def test_low_sample_warning_clear_on_normal_csv(self, tmp_path):
        path = tmp_path / "fine.csv"
        path.write_text("x\n" + "\n".join(str(i) for i in range(50)) + "\n")
        r = dataprof.profile(str(path))
        assert r.low_sample_warning is False
        # Emitted as an explicit False (non-optional bool), not omitted.
        assert r.to_dict()["quality"]["low_sample_warning"] is False


class TestZeroRowSemantics:
    """Header-only inputs have no values to measure, so ratios and aggregates
    are absent (None), never a fabricated 0.0 (#435 item 3). Raw counts stay 0
    -- that is "analyzed, found zero", which the counts genuinely are."""

    @pytest.fixture()
    def report(self, tmp_path):
        path = tmp_path / "header_only.csv"
        path.write_text("a,b,c\n")
        return dataprof.profile(str(path))

    def test_shape(self, report):
        assert report.rows == 0
        assert report.columns == 3

    def test_derived_stats_absent_counts_zero(self, report):
        for col in report.profiles:
            # Counts are legitimately zero ("analyzed, found none").
            assert col.total_count == 0
            assert col.null_count == 0
            # Ratios / aggregates were never measured -> absent, not 0.0.
            assert col.null_percentage is None
            assert col.uniqueness_ratio is None
            assert col.avg_length is None
            assert col.min_length is None
            assert col.max_length is None

    def test_to_dict_reports_absent(self, report):
        col = report.to_dict()["columns"][0]
        assert col["total_count"] == 0
        assert col["null_percentage"] is None
        assert col["uniqueness_ratio"] is None
        # Length stats were never measured, so they are omitted entirely.
        assert "avg_length" not in col.get("stats", {})

    def test_renders_do_not_crash(self, report):
        assert "ProfileReport" in repr(report)
        assert "<table" in report.to_html()
        md = report.to_markdown()
        assert "Column" in md
        # An em dash marks the absent percentage — never a misleading "0.0%".
        assert "—" in md

    def test_round_trips(self, report):
        reloaded = dataprof.ProfileReport.from_dict(report.to_dict())
        assert reloaded.to_dict() == report.to_dict()

    def test_arrow_export_null_not_zero(self, tmp_path):
        pa = pytest.importorskip("pyarrow")
        import dataprof.interop as interop

        path = tmp_path / "header_only.csv"
        path.write_text("a,b\n")
        batch = pa.record_batch(interop.analyze_csv_to_arrow(str(path)))
        assert batch.num_rows == 2  # one row per column profile
        # The Arrow surface must agree with the report API: absent, not 0.0.
        assert batch.column("null_percentage").to_pylist() == [None, None]
        assert batch.column("uniqueness_ratio").to_pylist() == [None, None]

    def test_zero_row_parquet_preserves_schema(self, tmp_path):
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        path = tmp_path / "empty.parquet"
        pq.write_table(
            pa.table(
                {
                    "id": pa.array([], type=pa.int64()),
                    "label": pa.array([], type=pa.string()),
                }
            ),
            path,
        )

        report = dataprof.profile(path)
        assert report.rows == 0
        assert report.columns == 2
        assert list(report) == ["id", "label"]
        assert report["id"].total_count == 0
        assert report["id"].null_percentage is None
        assert report.quality_score is None


class TestInvalidCount:
    """Per-column invalid_count: non-null values excluded from numeric stats
    must be disclosed, never silently dropped (#425)."""

    def test_unparseable_numeric_value_is_counted(self, tmp_path):
        path = tmp_path / "amounts.csv"
        path.write_text('amount,label\n1.5,a\n2.5,b\n3.5,c\n4.5,d\n5.5,e\n"12,50",f\n,g\n')
        r = dataprof.profile(str(path))

        amount = r["amount"]
        assert amount.data_type == "float"
        assert amount.null_count == 1
        assert amount.invalid_count == 1

        d = next(c for c in r.to_dict()["columns"] if c["name"] == "amount")
        assert d["invalid_count"] == 1

    def test_clean_numeric_column_reports_zero_not_none(self, tmp_path):
        path = tmp_path / "clean.csv"
        path.write_text("x\n" + "\n".join(str(i) for i in range(20)) + "\n")
        r = dataprof.profile(str(path))
        assert r["x"].invalid_count == 0

    def test_non_numeric_column_not_checked(self, tmp_path):
        path = tmp_path / "text.csv"
        path.write_text("name\nalice\nbob\ncarol\n")
        r = dataprof.profile(str(path))
        assert r["name"].invalid_count is None
        d = r.to_dict()["columns"][0]
        assert "invalid_count" not in d

    def test_in_memory_sources_match_file_semantics(self):
        r = dataprof.profile({"v": ["1", "2", "3", "4", "5", "12,50", None]})
        assert r["v"].invalid_count == 1

    def test_arrow_non_finite_values_do_not_poison_statistics(self):
        pa = pytest.importorskip("pyarrow")
        r = dataprof.profile(
            pa.table({"v": [1.5, float("nan"), float("inf"), float("-inf"), 3.25]})
        )
        v = r["v"]

        assert v.data_type == "float"
        assert v.null_count == 1
        assert v.unique_count == 4
        assert v.invalid_count == 2
        assert v.min == 1.5
        assert v.max == 3.25
        assert v.mean == pytest.approx(2.375)
        assert v.std_dev is not None and v.std_dev > 0.0

    def test_roundtrip_preserves_invalid_count(self, tmp_path):
        path = tmp_path / "amounts.csv"
        path.write_text('amount\n1.5\n2.5\n3.5\n4.5\n5.5\n"12,50"\n')
        r = dataprof.profile(str(path))
        r2 = dataprof.ProfileReport.from_json(r.to_json())
        assert r2["amount"].invalid_count == 1
        assert r2.to_dict() == r.to_dict()
