"""Ten-digit integers are not US phone numbers (#812).

Any ten digits matched ``Phone (US)`` at 0.525 confidence without a locale,
above the 0.5 bar of the ``sensitive_pattern`` finding and of the pattern shown
in ``to_llm_context()``. ``tests/us_phone_pattern.rs`` is the Rust twin.
"""

from __future__ import annotations

from pathlib import Path

import pytest

try:
    import dataprof as dp
except ImportError:
    pytest.skip(
        "dataprof native extension not built. Run: maturin develop",
        allow_module_level=True,
    )

ROWS = 60
COLUMNS = {
    "order_id": [str(2_000_000_000 + i) for i in range(ROWS)],
    "created_epoch": [str(1_705_312_200 + i * 86_400) for i in range(ROWS)],
    "legacy_id": [str(1_100_000_000 + i) for i in range(ROWS)],
    "phone": [f"(212) 555-{100 + i % 100:04d}" for i in range(ROWS)],
    "phone_digits": [f"312555{100 + i % 100:04d}" for i in range(ROWS)],
}
PHONES = ["phone", "phone_digits"]


def _sensitive(report: dp.ProfileReport) -> list[str | None]:
    return [f.column for f in report.findings() if f.code == "sensitive_pattern"]


@pytest.fixture
def csv_path(tmp_path: Path) -> Path:
    path = tmp_path / "ids.csv"
    names = list(COLUMNS)
    lines = [",".join(names)]
    lines += [",".join(f'"{COLUMNS[n][i]}"' for n in names) for i in range(ROWS)]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.parametrize("engine", ["auto", "incremental", "columnar"])
def test_only_phone_columns_are_sensitive(csv_path: Path, engine: str):
    assert _sensitive(dp.profile(str(csv_path), engine=engine)) == PHONES


def test_pandas():
    pd = pytest.importorskip("pandas")
    assert _sensitive(dp.profile(pd.DataFrame(COLUMNS))) == PHONES


def test_the_llm_context_does_not_call_ids_phone_numbers(csv_path: Path):
    lines = dp.profile(str(csv_path)).to_llm_context().splitlines()
    phone_lines = sorted(line.split(":", 1)[0] for line in lines if "Phone (US)" in line)
    assert phone_lines == ["- phone", "- phone_digits"]
