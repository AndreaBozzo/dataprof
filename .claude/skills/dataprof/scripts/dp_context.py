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
    runner = _profiler(args, dataprof)

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
            for column in structure.columns:
                # null_ratio is 0..1, not a percentage. Scaling it here rather
                # than labelling a ratio with a % sign, which is wrong by 100x.
                nulls = f"{column.null_ratio * 100:.1f}%" if column.null_ratio is not None else "?"
                print(
                    f"  {column.name}: {column.data_type} "
                    f"nulls={nulls} unique={column.unique_count}"
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
        print(f"{exc.filename}: no such file", file=sys.stderr)
        return 1
    except Exception as exc:
        # AgentSecurityError messages are written to be safe to show a model;
        # everything else is reported by type and message, without a traceback,
        # so a stack trace never lands in a conversation.
        print(f"{type(exc).__name__}: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
