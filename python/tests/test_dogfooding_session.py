"""Dogfooding checks against fresh, repo-local datasets.

These fixtures intentionally do not reuse examples/test_datasets so the Python
API gets exercised against shapes that are not already covered by the public
example resources.
"""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop --features python",
        allow_module_level=True,
    )


FIXTURES = Path(__file__).resolve().parent / "fixtures" / "dogfood"


class TestDogfoodingSession:
    def test_support_incident_csv_session(self):
        report = dp.profile(FIXTURES / "incidents.csv", metrics=["schema", "statistics", "quality"])

        assert report.rows == 30
        assert report.columns == 12
        assert "email" in report
        assert report["email"].null_count == 3
        assert report["response_minutes"].null_count == 3
        assert report["response_minutes"].max == 1440
        assert report["sla_breached"].true_count == 12
        assert report["opened_at"].data_type == "date"
        assert report.quality_score is not None
        assert report.quality_score < 95

        column_repr = repr(report["response_minutes"])
        assert "ColumnProfile" in column_repr
        assert "response_minutes" in column_repr

        quality_repr = repr(report.quality)
        assert "DataQualityMetrics" in quality_repr
        assert "dimensions=" in quality_repr

    def test_checkout_jsonl_session(self):
        report = dp.profile(FIXTURES / "checkout_events.jsonl", format="jsonl")

        assert report.rows == 25
        assert report.columns == 10
        assert report["event_id"].unique_count == 25
        assert report["successful"].true_count == 19
        assert report["coupon_code"].null_count == 14
        assert report["amount"].min == -12.5
        assert report["amount"].max == 2500
        assert report["latency_ms"].max == 12000
        assert report["risk_score"].max == 0.98
        assert report.quality_score is not None
        assert report.quality_score < 90
