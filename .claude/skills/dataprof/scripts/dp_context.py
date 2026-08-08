#!/usr/bin/env python3
"""Bounded, redaction-safe dataprof summaries for an agent to execute.

This is NOT a dataprof CLI. dataprof deliberately ships no binary: the release
surface is Rust crates plus Python wheels. This file lives inside the Claude
skill, is not packaged, is not installed, and is not registered as a console
script. It exists because an agent following the skill would otherwise rewrite
the same twenty lines of Python every session — differently each time, and
occasionally printing a whole report dict into the conversation.

Executed rather than read, so it costs no context beyond its output.

    python scripts/dp_context.py data.csv
    python scripts/dp_context.py data.csv --max-tokens 500
    python scripts/dp_context.py data.csv --column email
    python scripts/dp_context.py data.csv --structure-only
    python scripts/dp_context.py before.csv --compare after.csv
    python scripts/dp_context.py data.csv --root /srv/data   # sandboxed

Exit codes: 0 success, 1 profiling or policy error, 2 bad usage.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

# Dimensions in the order the reference documents them, so output ordering is
# stable across runs and diffable between two profiles.
DIMENSIONS = (
    "completeness",
    "consistency",
    "uniqueness",
    "accuracy",
    "timeliness",
    "validity",
    "precision",
)

# Execution fields that change how the numbers should be read. Printed only
# when set, so a clean profile stays quiet and a caveated one cannot be missed.
TRUST_SIGNALS = (
    "sampling_applied",
    "sampling_ratio",
    "truncation_reason",
    "low_sample_warning",
    "error_count",
    "ragged_row_count",
)


def _import_dataprof() -> Any:
    try:
        import dataprof
    except ImportError:
        sys.exit(
            "dataprof is not installed in this interpreter.\n"
            "Install it with: pip install dataprof\n"
            "In the dataprof repo itself: uv run maturin develop"
        )
    return dataprof


def _caveats(report: Any) -> list[str]:
    """Trust signals worth reporting, as human-readable lines.

    A truncated or sampled read still produces confident-looking statistics.
    Surfacing these next to the numbers is the whole point of the script.
    """
    lines = []
    for name in TRUST_SIGNALS:
        value = getattr(report, name, None)
        if value in (None, False, 0):
            continue
        lines.append(f"  {name}: {value}")
    if not report.source_exhausted:
        lines.append("  source_exhausted: False (the source was not read to the end)")
    return lines


def _quality_block(report: Any) -> list[str]:
    """Dimension scores plus their evidence, skipping what was not assessed."""
    quality = report.quality
    if quality is None:
        return ["quality: not analyzed"]

    scores = quality.dimension_scores()
    lines = [f"quality score: {quality.overall_quality_score()}"]
    for dimension in DIMENSIONS:
        score = scores.get(dimension)
        if score is None:
            # Absent means not assessed. Saying so beats omitting the row,
            # which reads as a clean result.
            lines.append(f"  {dimension}: not assessed")
            continue
        evidence = getattr(quality, dimension, None)
        detail = ""
        if isinstance(evidence, dict):
            interesting = {
                k: v for k, v in sorted(evidence.items()) if v not in (0, 0.0, False, [], None)
            }
            if interesting:
                detail = "  " + json.dumps(interesting, default=str)
        lines.append(f"  {dimension}: {round(score, 2)}{detail}")
    return lines


def _profiler(args: argparse.Namespace, dataprof: Any) -> Any:
    """Return something with .profile() and .analyze_structure(): guard or module.

    With --root, every read goes through AgentGuard, which resolves paths inside
    the sandbox and bounds the work. Without it, the caller is trusted.
    """
    if args.root is None:
        return dataprof

    from dataprof.agent import AgentGuard, SandboxPolicy

    return AgentGuard(SandboxPolicy(roots=[args.root]))


def _sanitize(runner: Any, exc: BaseException) -> str:
    """Render an exception as a line that is safe to put in front of a model.

    An arbitrary error from the engine or the filesystem may carry an absolute
    host path or a fragment of the row that failed to parse — a script that
    promises never to print cell values cannot echo those. Guard rejections are
    already written to be safe and pass through intact.

    Delegates to ``AgentGuard.sanitize_error`` when a guard is in play so the
    two surfaces cannot drift apart, and applies the same policy when it is not.
    """
    if hasattr(runner, "sanitize_error"):
        message = runner.sanitize_error(exc)
    else:
        from dataprof.agent import AgentSecurityError

        message = (
            str(exc)
            if isinstance(exc, AgentSecurityError)
            else "request failed (details withheld at the agent boundary)"
        )

    # sanitize_error already prefixes the type for non-guard errors but not for
    # guard rejections; prefix uniformly so the caller always sees what failed.
    name = type(exc).__name__
    return message if message.startswith(name) else f"{name}: {message}"


def _describe_column(report: Any, name: str) -> int:
    if name not in report:
        print(f"column {name!r} not found. Columns: {', '.join(report)}", file=sys.stderr)
        return 1

    column = report[name]
    patterns = [p.name for p in (column.patterns or [])]
    summary = {
        "name": column.name,
        "data_type": str(column.data_type),
        "total_count": column.total_count,
        "null_count": column.null_count,
        "null_percentage": column.null_percentage,
        "unique_count": column.unique_count,
        "uniqueness_ratio": column.uniqueness_ratio,
        "approximate": bool(column.is_approximate or column.unique_count_is_approximate),
        "patterns": patterns,
    }
    # Pattern names only. Values are withheld unconditionally: this script has
    # no opt-in for samples, because an agent surface is the wrong place for one.
    print(json.dumps(summary, indent=2, default=str))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="dp_context.py",
        description="Bounded dataprof summaries for agent use. Never prints raw cell values.",
    )
    parser.add_argument("source", help="path to a CSV, JSON, JSONL, or Parquet file")
    parser.add_argument(
        "--max-tokens",
        type=int,
        default=1000,
        help="token ceiling for the summary (default: 1000)",
    )
    parser.add_argument("--column", help="print detail for one column instead of the summary")
    parser.add_argument(
        "--compare",
        metavar="OTHER",
        help="profile OTHER too and print the deltas between the two",
    )
    parser.add_argument(
        "--structure-only",
        action="store_true",
        help="cheap structural pass; skips the full metrics",
    )
    parser.add_argument("--max-rows", type=int, default=None, help="stop after this many rows")
    parser.add_argument(
        "--root",
        default=None,
        help="sandbox root; use when the path came from a model or untrusted caller",
    )
    args = parser.parse_args(argv)

    if args.column and args.compare:
        parser.error("--column and --compare are mutually exclusive")

    dataprof = _import_dataprof()

    # SandboxPolicy validates --root, so building the guard can fail — and its
    # message names the resolved directory. Sanitize it like any other error
    # rather than letting a traceback carry a host path into the transcript.
    try:
        runner = _profiler(args, dataprof)
    except Exception as exc:
        print(_sanitize(None, exc), file=sys.stderr)
        return 1

    # AgentGuard rejects a per-call max_rows outright, including an explicit
    # None, because the policy owns that ceiling. Only forward the flag when the
    # caller actually set it, so --root works without it.
    limit = {} if args.max_rows is None else {"max_rows": args.max_rows}

    try:
        if args.structure_only:
            structure = runner.analyze_structure(args.source, **limit)
            rows = structure.row_count
            # RowCountEstimate carries whether the count is exact; its repr is
            # not something to put in front of a user.
            count = f"{rows.count}" + ("" if rows.exact else " (estimated)")
            print(f"source: {structure.source}")
            print(f"format: {structure.format}  rows: {count}")
            # The row count comes from a full scan while the per-column numbers
            # come from a bounded head sample. Printing them adjacently without
            # saying so invites reading a sample's distinct count as the
            # dataset's — off by 5x on the first file this was tried on.
            sampled = structure.truncated or structure.source_exhausted is False
            if sampled:
                print(
                    f"  NOTE: per-column numbers below describe the first "
                    f"{structure.rows_sampled} rows, not all {rows.count}. "
                    f"Counts are lower bounds; do not report them as dataset totals."
                )
            for column in structure.columns:
                # null_ratio is 0..1, not a percentage. Scaling it here rather
                # than labelling a ratio with a % sign, which is wrong by 100x.
                nulls = f"{column.null_ratio * 100:.1f}%" if column.null_ratio is not None else "?"
                # "~" marks a distinct count the profiler itself calls approximate,
                # which is a different caveat from the sampling one above.
                approx = "~" if column.distinct_count_approximate else ""
                scope = " (of sample)" if sampled else ""
                print(
                    f"  {column.name}: {column.data_type} "
                    f"nulls={nulls} unique={approx}{column.unique_count}{scope}"
                )
            for warning in structure.warnings or []:
                print(f"  warning: {warning}")
            return 0

        report = runner.profile(args.source, **limit)

        if args.column:
            return _describe_column(report, args.column)

        if args.compare:
            other = runner.profile(args.compare, **limit)
            print(json.dumps(report.compare(other), indent=2, default=str))
            return 0

        print(report.to_llm_context(max_tokens=args.max_tokens))
        print()
        print("\n".join(_quality_block(report)))

        caveats = _caveats(report)
        if caveats:
            print("\ncaveats (these numbers are not the whole dataset):")
            print("\n".join(caveats))
        return 0

    except FileNotFoundError as exc:
        # Echoes back only the path the caller just supplied, which tells them
        # nothing they did not already know.
        print(f"{exc.filename}: no such file", file=sys.stderr)
        return 1
    except Exception as exc:
        print(_sanitize(runner, exc), file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
