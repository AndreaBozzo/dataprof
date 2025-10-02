"""Test timeliness metrics in Python bindings."""
import os
import tempfile
import dataprof


def test_timeliness_metrics_present():
    """Verify that timeliness metrics are exposed in PyDataQualityMetrics."""
    # Create a simple CSV with date column
    csv_content = """date,value
2020-01-01,100
2021-01-01,200
2025-12-31,300
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_content)
        temp_path = f.name

    try:
        # Analyze CSV with quality metrics
        report = dataprof.analyze_csv_with_quality(temp_path)

        # Check that data_quality_metrics exists
        assert report.data_quality_metrics is not None, "data_quality_metrics should not be None"

        metrics = report.data_quality_metrics

        # Check that timeliness fields exist
        assert hasattr(metrics, 'future_dates_count'), "Missing future_dates_count"
        assert hasattr(metrics, 'stale_data_ratio'), "Missing stale_data_ratio"
        assert hasattr(metrics, 'temporal_violations'), "Missing temporal_violations"

        # Check types
        assert isinstance(metrics.future_dates_count, int), "future_dates_count should be int"
        assert isinstance(metrics.stale_data_ratio, float), "stale_data_ratio should be float"
        assert isinstance(metrics.temporal_violations, int), "temporal_violations should be int"

        print(f"✅ Timeliness metrics present:")
        print(f"   Future dates: {metrics.future_dates_count}")
        print(f"   Stale data ratio: {metrics.stale_data_ratio:.4f}")
        print(f"   Temporal violations: {metrics.temporal_violations}")

    finally:
        os.unlink(temp_path)


def test_quality_score_includes_timeliness():
    """Verify that overall_quality_score() uses ISO formula with timeliness."""
    csv_content = """id,name,value
1,Alice,100
2,Bob,200
3,Charlie,300
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_content)
        temp_path = f.name

    try:
        report = dataprof.analyze_csv_with_quality(temp_path)
        metrics = report.data_quality_metrics

        assert metrics is not None

        # Calculate score
        score = metrics.overall_quality_score()

        # Manual calculation using ISO formula
        completeness = metrics.complete_records_ratio * 0.3
        consistency = metrics.data_type_consistency * 0.25
        uniqueness = metrics.key_uniqueness * 0.2
        accuracy = (100.0 - metrics.outlier_ratio * 100.0) * 0.15
        timeliness = (100.0 - metrics.stale_data_ratio * 100.0) * 0.1

        expected_score = completeness + consistency + uniqueness + accuracy + timeliness

        # Allow small floating point difference
        assert abs(score - expected_score) < 0.01, \
            f"Score mismatch: {score} vs {expected_score}"

        print(f"✅ Quality score calculation correct: {score:.2f}%")
        print(f"   Breakdown: C={completeness:.1f} + Co={consistency:.1f} + U={uniqueness:.1f} + A={accuracy:.1f} + T={timeliness:.1f}")

    finally:
        os.unlink(temp_path)


def test_timeliness_summary_method():
    """Verify that timeliness_summary() method exists and works."""
    csv_content = """id,value
1,100
2,200
"""

    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
        f.write(csv_content)
        temp_path = f.name

    try:
        report = dataprof.analyze_csv_with_quality(temp_path)
        metrics = report.data_quality_metrics

        assert metrics is not None
        assert hasattr(metrics, 'timeliness_summary'), "Missing timeliness_summary method"

        summary = metrics.timeliness_summary()
        assert isinstance(summary, str), "timeliness_summary should return string"
        assert len(summary) > 0, "timeliness_summary should not be empty"

        print(f"✅ Timeliness summary: {summary}")

    finally:
        os.unlink(temp_path)


if __name__ == '__main__':
    print("Testing timeliness metrics in Python bindings...\n")
    test_timeliness_metrics_present()
    print()
    test_quality_score_includes_timeliness()
    print()
    test_timeliness_summary_method()
    print("\n✅ All timeliness tests passed!")
