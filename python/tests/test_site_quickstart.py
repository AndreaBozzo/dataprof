"""The project page's quickstart figures are read from the page and checked against a run."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest
from test_wheel_feature_single_source import _job_body

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "check_site_quickstart", ROOT / ".github/scripts/check_site_quickstart.py"
)
assert SPEC is not None and SPEC.loader is not None
site = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = site
SPEC.loader.exec_module(site)

PAGE = (ROOT / "site/index.html").read_text(encoding="utf-8")
CI = (ROOT / ".github/workflows/ci.yml").read_text(encoding="utf-8")
_RUST_EXAMPLE = "\n".join(
    (
        'println!("Rows: {}", report.execution.rows_processed);',
        'println!("Quality: {score:.2}");',
        'println!("{} {:?} nulls={}", col.name, col.data_type, col.null_count);',
    )
)


def test_the_script_is_registered_before_its_dataclasses_run():
    """3.11 to 3.14 look a postponed dataclass up in sys.modules."""

    source = Path(__file__).read_text(encoding="utf-8")
    loader = (
        "site = importlib.util.module_from_spec(SPEC)\n"
        "sys.modules[SPEC.name] = site\n"
        "SPEC.loader.exec_module(site)\n"
    )
    assert loader in source
    assert SPEC is not None
    assert sys.modules[SPEC.name] is site


def _run(**overrides: object) -> site.Run:
    columns = (
        site.Column("order_id", "integer", 0, 0.0),
        site.Column("amount", "integer", 0, 0.0),
        site.Column("region", "string", 2, 25.0),
    )
    values: dict[str, object] = {
        "version": "9.9.9",
        "rows": 8,
        "columns": columns,
        "quality_score": 93.19,
        "verdict": "fail",
        "exit_code": 1,
    }
    values.update(overrides)
    return site.Run(**values)


def _rust_text(run: site.Run, example: str = _RUST_EXAMPLE) -> str:
    """Replay ``example`` the way the site check does, without a full page."""

    holder = site.Page(
        versions=(),
        quality_scores=(),
        null_percents=(),
        verdicts=(),
        exit_code="",
        python_output="",
        rust_output="",
        quickstart=site.Quickstart((), {}),
        rust_prints=site._rust_prints(example),
        command_flags=(),
    )
    return site.rust_output(holder, run)


def _page(
    run: site.Run,
    *,
    column: str = "region",
    unmarked: str = "",
    rust_example: str = _RUST_EXAMPLE,
) -> str:
    """A page whose marked figures are exactly what ``run`` produces."""

    score = site._number_text(run.quality_score)
    nulls = "\n".join(
        f'<td data-site-figure="null-percent" data-site-column="{item.name}">'
        f"{site._percent_text(item.null_percentage)}</td>"
        for item in run.columns
    )
    printed = next(item for item in run.columns if item.name == column)
    example = "\n".join(
        (
            "import dataprof as dp",
            'report = dp.profile("orders.csv")',
            "print(report.quality_score)",
            f'print(report["{column}"].null_percentage)',
            "result = report.check(",
            "    min_quality_score=90,",
            '    max_null_percentage={"*": 20},',
            ")",
            "print(result.verdict)",
        )
    )
    output = "\n".join((score, site._number_text(printed.null_percentage), run.verdict))
    command = 'python -m dataprof.check orders.csv --min-quality 90 --max-null "*=20"'
    rust_output = _rust_text(run, rust_example)
    return f"""
    <p>{unmarked}</p>
    <span data-site-figure="version">{run.version}</span>
    <span data-site-figure="quality-score">{score}</span>
    {nulls}
    <span data-site-figure="verdict">{run.verdict}</span>
    <span data-site-figure="exit-code">{run.exit_code}</span>
    <code data-site-figure="python-example">{example}</code>
    <code data-site-figure="python-output">{output}</code>
    <code data-site-figure="rust-example">{rust_example}</code>
    <code data-site-figure="rust-output">{rust_output}</code>
    <code data-site-figure="ci-command">{command}</code>
    """


def test_a_page_that_matches_the_run_passes():
    run = _run()
    assert site.differences(site.parse_page(_page(run)), run) == []


def test_an_unmarked_number_is_not_a_figure():
    run = _run()
    page = _page(run, unmarked="the draft score was 1.00 and then it changed")
    assert site.differences(site.parse_page(page), run) == []


def test_a_quality_score_that_differs_from_the_run_fails():
    run = _run()
    page = _page(run).replace(">93.19<", ">10.00<", 1)
    failures = site.differences(site.parse_page(page), run)
    assert len(failures) == 1
    assert failures[0].startswith("quality-score:")
    assert "10.00" in failures[0]
    assert "93.19" in failures[0]


def test_the_version_mark_must_be_the_package_version():
    run = _run(version="1.2.3")
    page = _page(run).replace(">1.2.3<", ">9.9.9<", 1)
    failures = site.differences(site.parse_page(page), run)
    expected = "version: page shows '9.9.9', the run shows '1.2.3' (edit site/index.html)"
    assert failures == [expected]


def test_the_exit_code_mark_must_be_the_command_result():
    run = _run(exit_code=1)
    page = _page(run).replace(">1<", ">0<", 1)
    failures = site.differences(site.parse_page(page), run)
    assert any(item.startswith("exit-code:") for item in failures)


def test_a_null_percentage_rounded_on_the_page_fails():
    """25% is a claim that the percentage is 25, not that it rounds to 25."""

    run = _run(
        columns=(
            site.Column("order_id", "integer", 0, 0.0),
            site.Column("amount", "integer", 0, 0.0),
            site.Column("region", "string", 2, 25.4),
        )
    )
    page = _page(_run())
    failures = site.differences(site.parse_page(page), run)
    assert any(item.startswith("null-percent[region]:") for item in failures)


def test_python_output_follows_the_column_the_example_prints():
    run = _run()
    page = site.parse_page(_page(run, column="amount"))
    assert site.python_output(page, run).splitlines()[1] == "0.0"


def test_rust_output_uses_the_example_spellings():
    run = _run()
    text = site.rust_output(site.parse_page(_page(run)), run)
    assert text.splitlines() == [
        "Rows: 8",
        "Quality: 93.19",
        "order_id Integer nulls=0",
        "amount Integer nulls=0",
        "region String nulls=2",
    ]


def test_rust_output_follows_the_prints_on_the_page():
    run = _run()
    example = "\n".join(
        (
            'println!("Quality: {score:.2}");',
            'println!("Rows: {}", report.execution.rows_processed);',
        )
    )
    page = site.parse_page(_page(run, rust_example=example))
    assert site.rust_output(page, run).splitlines() == ["Quality: 93.19", "Rows: 8"]


def test_a_rust_print_this_check_cannot_replay_fails():
    example = 'println!("rows: {}", report.execution.rows_processed);'
    page = _page(_run()).replace(_RUST_EXAMPLE, example, 1)
    with pytest.raises(site.PageError, match="cannot replay"):
        site.parse_page(page)


def test_a_break_inside_a_marked_output_is_recorded():
    run = _run()
    page = _page(run).replace(
        'data-site-figure="python-output">',
        'data-site-figure="python-output"><br>',
        1,
    )
    parsed = site.parse_page(page)
    assert parsed.python_output == site.python_output(parsed, run)


def test_a_traceback_is_not_a_passing_exit():
    with pytest.raises(site.PageError, match="Traceback"):
        site._command_exit("Traceback (most recent call last):\n", 1, "0.14.0", "fail")


def test_the_command_summary_keeps_its_exit_code():
    exit_code = site._command_exit("dataprof 0.14.0: fail: 4 checks\n", 1, "0.14.0", "fail")
    assert exit_code == 1


def test_a_missing_mark_is_an_error():
    run = _run()
    page = _page(run).replace('data-site-figure="version"', 'data-site-figure="unused"', 1)
    with pytest.raises(site.PageError, match="version"):
        site.parse_page(page)


def test_the_live_page_marks_the_quickstart_figures():
    page = site.parse_page(PAGE)
    assert page.versions
    assert page.quality_scores
    assert page.null_percents
    assert page.verdicts
    assert page.exit_code
    assert [step.kind for step in page.quickstart.prints] == [
        "quality-score",
        "null-percentage",
        "verdict",
    ]
    assert page.quickstart.prints[1].column
    assert "min_quality_score" in page.quickstart.check_arguments
    assert "max_null_percentage" in page.quickstart.check_arguments
    assert [step.kind for step in page.rust_prints] == ["rows", "quality", "column"]
    assert all(not flag.endswith(".csv") for flag in page.command_flags)
    assert any(flag.startswith("--") for flag in page.command_flags)


def test_the_script_does_not_copy_the_page_figures():
    source = (ROOT / ".github/scripts/check_site_quickstart.py").read_text(encoding="utf-8")
    for figure in ("93.19", "25.0", "0.14.0"):
        assert figure not in source


def test_the_wheel_smoke_job_runs_the_check():
    body = _job_body(CI, "python-wheel-smoke")
    assert ".github/scripts/check_site_quickstart.py" in body
    assert "maturin build --out dist" in body


def test_orders_csv_matches_the_page():
    import dataprof

    assert dataprof.__version__
    failures = site.check_repository(ROOT)
    assert failures == []
