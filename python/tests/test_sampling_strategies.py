"""Every sampling strategy actually samples, on every path that accepts one (#459).

Previously the documented default call — ``dp.profile(path, sampling=...)`` —
dropped the strategy and profiled everything, while forcing the incremental or
async engine returned all rows, no rows, or an arbitrary subset depending on the
strategy. Two contracts are checked here:

1. **No silent ignore.** A path applies the strategy or refuses it before
   reading. A full profile returned under a sampling request is the worst
   outcome: it looks complete.
2. **Provenance matches.** ``sampling_applied`` and ``sampling_ratio`` describe
   the rows that reached the profile, and sampling never marks a fully read
   source as unexhausted.

Run after building the extension:
    maturin develop --features python,python-async,async-streaming
    pytest python/tests/test_sampling_strategies.py -v
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import dataprof as dp
import pytest

ROWS = 100
GROUPS = 5

_HAS_ASYNC = dp.capabilities().async_streaming
requires_async = pytest.mark.skipif(
    not _HAS_ASYNC,
    reason="Async streaming not compiled. Build with --features "
    "'python,python-async,async-streaming'.",
)

S = dp.SamplingStrategy


def _csv_text() -> str:
    rows = [f"g{i % GROUPS},{i % 10},{100 + i}" for i in range(ROWS)]
    return "group,weight,value\n" + "\n".join(rows) + "\n"


@pytest.fixture
def csv_path(tmp_path: Path) -> str:
    target = tmp_path / "sampling.csv"
    target.write_text(_csv_text())
    return str(target)


def _strategies():
    """(name, factory, expected sampled rows) for every exposed constructor."""
    return [
        ("random", lambda: S.random(10), 10),
        ("reservoir", lambda: S.reservoir(10), 10),
        ("stratified", lambda: S.stratified(["group"], 2), GROUPS * 2),
        ("progressive", lambda: S.progressive(5, 0.95, 10), 10),
        ("systematic", lambda: S.systematic(10), 10),
        ("importance", lambda: S.importance("weight", 8.0), 20),
        (
            "multi_stage",
            lambda: S.multi_stage([S.systematic(2), S.reservoir(10)]),
            10,
        ),
    ]


def _assert_sampled(report, expected_rows: int, label: str) -> None:
    assert report.rows == expected_rows, f"{label}: sampled row count"
    assert report.sampling_applied, f"{label}: {expected_rows} of {ROWS} rows must be reported"
    assert report.sampling_ratio == pytest.approx(expected_rows / ROWS), f"{label}: ratio"
    # Sampling is not truncation — the source was still read to the end.
    assert report.source_exhausted, f"{label}: sampling must not mark the source unexhausted"
    assert report.truncation_reason is None, f"{label}"


def _async_bytes(**kwargs):
    from dataprof.asyncio import profile_bytes

    async def _inner():
        return await profile_bytes(_csv_text().encode(), format="csv", **kwargs)

    return asyncio.run(_inner())


# --- Every strategy samples, on every engine that accepts one ---------------


@pytest.mark.parametrize(("name", "factory", "expected"), _strategies())
@pytest.mark.parametrize("engine", ["auto", "incremental"])
def test_strategy_samples(csv_path, engine, name, factory, expected):
    report = dp.profile(csv_path, engine=engine, sampling=factory())
    _assert_sampled(report, expected, f"{engine}/{name}")


@pytest.mark.parametrize(("name", "factory", "expected"), _strategies())
def test_engines_agree(csv_path, name, factory, expected):
    auto = dp.profile(csv_path, sampling=factory())
    incremental = dp.profile(csv_path, engine="incremental", sampling=factory())
    assert auto.rows == incremental.rows == expected, name
    assert auto.sampling_ratio == incremental.sampling_ratio, name


@requires_async
@pytest.mark.parametrize(("name", "factory", "expected"), _strategies())
def test_async_bytes_sample(name, factory, expected):
    report = _async_bytes(sampling=factory())
    _assert_sampled(report, expected, f"async/{name}")


@requires_async
@pytest.mark.parametrize(("name", "factory", "expected"), _strategies())
def test_async_agrees_with_file(csv_path, name, factory, expected):
    from_file = dp.profile(csv_path, sampling=factory())
    from_async = _async_bytes(sampling=factory())
    assert from_async.rows == from_file.rows == expected, name


# --- Provenance -------------------------------------------------------------


def test_no_sampling_reports_none(csv_path):
    report = dp.profile(csv_path)
    assert report.rows == ROWS
    assert not report.sampling_applied
    assert report.sampling_ratio is None


def test_a_strategy_that_keeps_every_row_is_not_sampling(csv_path):
    report = dp.profile(csv_path, sampling=S.systematic(1))
    assert report.rows == ROWS
    assert not report.sampling_applied


def test_fixed_size_larger_than_source_keeps_everything(csv_path):
    report = dp.profile(csv_path, sampling=S.reservoir(ROWS * 10))
    assert report.rows == ROWS
    assert not report.sampling_applied


def test_statistics_cover_only_the_final_sample(csv_path):
    # If provisionally selected rows were folded in as they arrived, evicted
    # rows would leave values behind and the counts would exceed the sample.
    report = dp.profile(csv_path, sampling=S.reservoir(10))
    assert report.rows == 10
    for name in ("group", "weight", "value"):
        assert report[name].total_count == 10, name


# --- Redefined strategies ---------------------------------------------------


def test_importance_filters_on_the_named_column(csv_path):
    # weight cycles 0..9; only the 9s clear a threshold of 9.
    report = dp.profile(csv_path, sampling=S.importance("weight", 9.0))
    assert report.rows == 10
    assert report["weight"].max == 9


def test_importance_requires_a_column_name():
    # The pre-0.10 signature took only a threshold. Calling it that way must
    # fail loudly rather than silently weighing some default column.
    with pytest.raises(TypeError):
        S.importance(0.5)  # ty: ignore[invalid-argument-type, missing-argument]


def test_importance_on_an_unknown_column_samples_nothing(csv_path):
    # A row whose weight cannot be read has no stated importance. The scan still
    # succeeds and reports an empty sample rather than inventing one.
    report = dp.profile(csv_path, sampling=S.importance("absent", 0.0))
    assert report.rows == 0


def test_progressive_stops_early_on_low_variance_data(tmp_path):
    # A constant column reaches any precision target immediately, so the
    # strategy must stop at initial_size instead of running to max_size.
    target = tmp_path / "constant.csv"
    target.write_text("value\n" + "42\n" * 5_000)

    report = dp.profile(str(target), sampling=S.progressive(10, 0.95, 4_000))
    assert report.rows == 10


def test_progressive_grows_when_the_data_is_volatile(tmp_path):
    # Wide spread: precision arrives slowly, so the sample runs to the cap.
    target = tmp_path / "volatile.csv"
    rows = "\n".join(str((i * 7919) % 100_000) for i in range(20_000))
    target.write_text("value\n" + rows + "\n")

    report = dp.profile(str(target), sampling=S.progressive(10, 0.999, 500))
    assert report.rows == 500


# --- Composed with a row cap ------------------------------------------------


@pytest.mark.parametrize(("name", "factory", "expected"), _strategies())
@pytest.mark.parametrize("limit", [1, 5, 20])
def test_row_cap_bounds_every_strategy(csv_path, limit, name, factory, expected):
    # A row cap is a hard ceiling whatever the strategy. A fixed-size sample
    # folds nothing in until the scan ends, so a cap checked against folded rows
    # never fired and random(100) under max_rows(20) returned 100 rows.
    report = dp.profile(
        csv_path,
        engine="incremental",
        sampling=factory(),
        stop_condition=dp.StopCondition.max_rows(limit),
    )
    assert report.rows <= limit, f"{name} under max_rows({limit}) produced {report.rows}"


def test_row_cap_below_a_fixed_size_sample_wins(csv_path):
    report = dp.profile(
        csv_path,
        sampling=S.reservoir(50),
        stop_condition=dp.StopCondition.max_rows(10),
    )
    assert report.rows == 10
    assert not report.source_exhausted
    assert report.truncation_reason is not None


# --- Paths that cannot sample must refuse -----------------------------------


def test_columnar_engine_rejects_sampling(csv_path):
    with pytest.raises(ValueError) as excinfo:
        dp.profile(csv_path, engine="columnar", sampling=S.reservoir(10))
    message = str(excinfo.value)
    assert "sampling" in message
    assert "incremental" in message, "the error must name a path that can sample"


def test_json_parser_rejects_sampling(tmp_path):
    target = tmp_path / "data.jsonl"
    target.write_text("".join(f'{{"id":{i}}}\n' for i in range(50)))
    with pytest.raises(ValueError) as excinfo:
        dp.profile(str(target), sampling=S.reservoir(10))
    assert "sampling" in str(excinfo.value)


def test_sync_bytes_reject_sampling():
    with pytest.raises(ValueError) as excinfo:
        dp.profile(_csv_text().encode(), format="csv", sampling=S.reservoir(10))
    message = str(excinfo.value)
    assert "sampling" in message
    assert "asyncio" in message, "the error must point at the path that samples bytes"


def test_sync_bytes_accept_an_explicit_no_op_strategy():
    # none() asks for nothing, so a caller that always passes a strategy object
    # is not treated as having requested sampling.
    report = dp.profile(_csv_text().encode(), format="csv", sampling=S.none())
    assert report.rows == ROWS
    assert not report.sampling_applied


def test_an_explicit_no_op_strategy_is_accepted_everywhere(csv_path, tmp_path):
    assert dp.profile(csv_path, engine="columnar", sampling=S.none()).rows == ROWS

    jsonl = tmp_path / "data.jsonl"
    jsonl.write_text("".join(f'{{"id":{i}}}\n' for i in range(50)))
    assert dp.profile(str(jsonl), sampling=S.none()).rows == 50


# --- Compositions that have no meaning must be refused ----------------------


def test_two_fixed_size_stages_are_refused(csv_path):
    with pytest.raises(ValueError) as excinfo:
        dp.profile(csv_path, sampling=S.multi_stage([S.reservoir(10), S.random(5)]))
    assert "at most one fixed-size stage" in str(excinfo.value)


def test_a_filter_after_the_sample_is_refused(csv_path):
    with pytest.raises(ValueError) as excinfo:
        dp.profile(csv_path, sampling=S.multi_stage([S.reservoir(10), S.systematic(2)]))
    assert "must be the last stage" in str(excinfo.value)
