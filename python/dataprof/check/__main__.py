"""Argument and exit-code adapter over the batch quality-gate API."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from dataprof import profile_file
from dataprof._gate import _Policy

_EXIT_CODES = {"pass": 0, "fail": 1, "inconclusive": 2}
_POLICY_DEFAULTS: dict[str, Any] = {
    "min_quality_score": None,
    "min_dimension_scores": None,
    "max_null_percentage": None,
    "max_duplicate_rows": None,
    "require_metrics": None,
    "scope": "full_source",
}


def _named_percentage(text: str) -> tuple[str, float]:
    name, separator, value = text.rpartition("=")
    if not separator or not name:
        raise argparse.ArgumentTypeError("expected NAME=PERCENT")
    try:
        return name, float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected NAME=PERCENT") from exc


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m dataprof.check",
        description="Profile a local file and evaluate a quality policy.",
        epilog=(
            "Exit codes: 0 = pass (also --help); 1 = a proven policy violation; "
            "2 = inconclusive, invalid arguments/policy, or input error. "
            "A proven violation takes precedence over unevaluated checks. "
            "Baseline comparison is not yet supported by the gate API."
        ),
    )
    parser.add_argument("source", type=Path, help="CSV, JSON, JSONL, or Parquet file")
    parser.add_argument(
        "--policy",
        type=Path,
        help="UTF-8 JSON object of check() keywords; flags replace matching keys",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="write the QualityGateResult JSON to stdout",
    )
    parser.add_argument(
        "--min-quality",
        dest="min_quality_score",
        type=float,
        help="minimum overall quality score (0-100)",
    )
    parser.add_argument(
        "--min-dimension",
        dest="min_dimension_scores",
        type=_named_percentage,
        action="append",
        metavar="NAME=PERCENT",
        help="minimum dimension score (0-100); repeat for multiple dimensions",
    )
    parser.add_argument(
        "--max-null",
        dest="max_null_percentage",
        type=_named_percentage,
        action="append",
        metavar="COLUMN=PERCENT",
        help="maximum null percentage (0-100); use *=PERCENT for all columns; repeatable",
    )
    parser.add_argument("--max-duplicate-rows", type=int, help="maximum duplicate-row count")
    parser.add_argument(
        "--require-metric",
        dest="require_metrics",
        action="append",
        metavar="NAME",
        help="require quality or a dimension to be analyzed; absence fails; repeatable",
    )
    parser.add_argument(
        "--scope",
        choices=("full_source", "observed"),
        help="require evidence about the full source (default) or the observed population",
    )
    parser.add_argument("--engine", default="auto", help="profiling engine (default: auto)")
    parser.add_argument("--format", help="explicit input format; inferred from the path by default")
    parser.add_argument("--max-rows", type=int, help="cap the number of profiled rows")
    parser.add_argument(
        "--metric",
        dest="metrics",
        action="append",
        metavar="PACK",
        help="select a profiling metric pack; repeatable; default: all packs",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        metavar="PATH",
        help="unsupported: baseline comparison awaits support in the gate API (exits 2)",
    )
    return parser


def _unique_policy_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Refuse ambiguous JSON instead of silently replacing a requirement."""
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate policy key: {key!r}")
        result[key] = value
    return result


def _policy(args: argparse.Namespace) -> dict[str, Any]:
    policy = dict(_POLICY_DEFAULTS)
    if args.policy is not None:
        document = json.loads(
            args.policy.read_text(encoding="utf-8"),
            object_pairs_hook=_unique_policy_keys,
        )
        if not isinstance(document, dict):
            raise ValueError("policy file must contain a JSON object of check() keywords")
        unknown = document.keys() - policy.keys()
        if unknown:
            raise ValueError(f"unknown policy keys: {', '.join(sorted(unknown))}")
        policy.update(document)
    for key in policy:
        value = getattr(args, key)
        if value is not None:
            if key in ("min_dimension_scores", "max_null_percentage"):
                value = dict(value)
            policy[key] = value

    # Validate JSON container shapes before handing values to the same policy
    # validator the public check() method uses. Thresholds, dimension names,
    # scope, and empty-policy semantics belong to that validator.
    dimensions = policy["min_dimension_scores"]
    if dimensions is not None and not isinstance(dimensions, dict):
        raise ValueError("min_dimension_scores must be a JSON object")
    required = policy["require_metrics"]
    if required is not None and (
        not isinstance(required, list) or not all(isinstance(name, str) for name in required)
    ):
        raise ValueError("require_metrics must be a JSON array of names")
    _Policy(**policy)
    return policy


def main(argv: list[str] | None = None) -> int:
    """Run a gate; return 0 for pass, 1 for fail, or 2 when inconclusive/error."""
    parser = _parser()
    args = parser.parse_args(argv)
    if args.baseline is not None:
        parser.error("--baseline is not supported by the quality-gate API yet")
    try:
        policy = _policy(args)
        report = profile_file(
            args.source,
            engine=args.engine,
            format=args.format,
            max_rows=args.max_rows,
            metrics=args.metrics,
        )
        result = report.check(**policy)
    except (OSError, ValueError, TypeError, RuntimeError, OverflowError) as exc:
        print(f"dataprof.check: {exc}", file=sys.stderr)
        return 2

    if args.json:
        print(result.to_json())
    print(
        f"{result.verdict}: {len(result.checks)} checks, "
        f"{len(result.violations)} violations, {len(result.unevaluated)} unevaluated "
        f"(scope: {result.scope})",
        file=sys.stderr,
    )
    for check in result.checks:
        if check.status != "passed":
            subject = check.column if check.column is not None else check.dimension
            label = f" [{subject}]" if subject is not None else ""
            print(f"  {check.status}: {check.code}{label}: {check.message}", file=sys.stderr)
    return _EXIT_CODES[result.verdict]


if __name__ == "__main__":
    raise SystemExit(main())
