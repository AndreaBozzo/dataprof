"""Argument and exit-code adapter over the batch quality-gate API."""

from __future__ import annotations

import argparse
import json
import sys
from argparse import ArgumentParser
from collections.abc import Sequence
from pathlib import Path
from typing import Any, NoReturn

import dataprof
from dataprof import profile_file
from dataprof._gate import _Policy

_EXIT_CODES = {"pass": 0, "fail": 1, "inconclusive": 2}
_ERROR_EXIT_CODE = 3
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


# The names profile_file() accepts for --engine/--format/--metric, mirroring
# the case-insensitive parsing (and aliases) on the Rust side. argparse
# choices= would reject the documented aliases ("streaming", "arrow", "ndjson")
# and case variants the API accepts, so validate explicitly: bogus names still
# fail here as argument errors instead of surfacing later as input errors.
_ENGINES = ("auto", "incremental", "columnar", "streaming", "arrow")
_FORMATS = ("csv", "json", "jsonl", "ndjson", "parquet")
_METRIC_PACKS = ("schema", "statistics", "patterns", "quality")


def _engine_name(value: str) -> str:
    if value.lower() not in _ENGINES:
        raise argparse.ArgumentTypeError(
            f"invalid engine: {value!r} (choose from {', '.join(_ENGINES)})"
        )
    return value


def _format_name(value: str) -> str:
    if value.lower() not in _FORMATS:
        raise argparse.ArgumentTypeError(
            f"invalid format: {value!r} (choose from {', '.join(_FORMATS)})"
        )
    return value


def _metric_pack(value: str) -> str:
    if value.lower() not in _METRIC_PACKS:
        raise argparse.ArgumentTypeError(
            f"invalid metric pack: {value!r} (choose from {', '.join(_METRIC_PACKS)})"
        )
    return value


def _non_negative_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid non-negative integer: {value!r}") from None
    if result < 0:
        raise argparse.ArgumentTypeError(f"invalid non-negative integer: {value!r}")
    return result


def _json_requested(parser: ArgumentParser, argv: Sequence[str]) -> bool:
    """Detect --json (or an unambiguous abbreviation) using argparse's own rules.

    argparse accepts unambiguous long-option abbreviations (``allow_abbrev``),
    so ``--js`` selects ``--json``. Mirror that here so usage errors honor the
    JSON error contract no matter how the flag was spelled.
    """
    for arg in argv:
        if arg == "--":
            break
        if not arg.startswith("--"):
            continue
        name = arg.split("=", 1)[0]
        # _option_string_actions is private argparse API; the abbreviated---json
        # test pins this behavior so an upstream rename fails loudly, not silently.
        matches = [opt for opt in parser._option_string_actions if opt.startswith(name)]
        if matches == ["--json"]:
            return True
    return False


class _CheckArgumentParser(ArgumentParser):
    """An ArgumentParser whose usage errors exit 3, not argparse's default 2.

    With --json, a usage error writes ``{"error": {"kind": "argument", ...}}``
    to stdout instead of printing usage to stderr, so machine consumers never
    need to parse stderr.
    """

    def __init__(self, *args: Any, argv: Sequence[str] | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._argv = list(sys.argv[1:] if argv is None else argv)

    def error(self, message: str) -> NoReturn:
        if _json_requested(self, self._argv):
            print(json.dumps({"error": {"kind": "argument", "message": message}}))
        else:
            self.print_usage(sys.stderr)
            sys.stderr.write(f"{self.prog}: error: {message}\n")
        raise SystemExit(_ERROR_EXIT_CODE)


def _report_error(kind: str, message: str, *, path: Path | None, json_output: bool) -> int:
    """Report an input, policy, argument, or output error; always returns exit code 3."""
    if json_output:
        error: dict[str, Any] = {"kind": kind, "message": message}
        if path is not None:
            error["path"] = str(path)
        print(json.dumps({"error": error}))
    else:
        print(f"dataprof.check: error: {message}", file=sys.stderr)
    return _ERROR_EXIT_CODE


def _parser(argv: list[str] | None = None) -> _CheckArgumentParser:
    parser = _CheckArgumentParser(
        prog="python -m dataprof.check",
        argv=argv,
        description="Profile a local file and evaluate a quality policy.",
        epilog=(
            "Exit codes: 0 = pass (also --help and --version); 1 = a proven policy violation; "
            "2 = inconclusive (a threshold could not be evaluated); "
            "3 = an argument, policy, or source could not be read or used, "
            "or the report could not be written. "
            "A proven violation takes precedence over unevaluated checks. "
            "Baseline comparison is not yet supported by the gate API."
        ),
    )
    parser.add_argument("source", type=Path, help="CSV, JSON, JSONL, or Parquet file")
    parser.add_argument(
        "--policy",
        type=Path,
        metavar="PATH",
        help="path to a UTF-8 JSON file of check() keywords; flags replace matching keys",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="write the QualityGateResult JSON to stdout",
    )
    parser.add_argument(
        "--report",
        type=Path,
        metavar="PATH",
        help="save the full profile to a .json file, including on fail or inconclusive",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"dataprof {dataprof.__version__}",
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
    parser.add_argument(
        "--engine",
        default="auto",
        type=_engine_name,
        help="profiling engine (default: auto)",
    )
    parser.add_argument(
        "--format",
        type=_format_name,
        help="explicit input format; inferred from the path by default",
    )
    parser.add_argument(
        "--max-rows",
        type=_non_negative_int,
        help="cap the number of profiled rows",
    )
    parser.add_argument(
        "--metric",
        dest="metrics",
        action="append",
        metavar="PACK",
        type=_metric_pack,
        help="select a profiling metric pack; repeatable; default: all packs",
    )
    parser.add_argument(
        "--baseline",
        type=Path,
        metavar="PATH",
        help="unsupported: baseline comparison awaits support in the gate API (exits 3)",
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
        try:
            document = json.loads(
                args.policy.read_text(encoding="utf-8-sig"),
                object_pairs_hook=_unique_policy_keys,
            )
        except json.JSONDecodeError as exc:
            raise ValueError(f"policy file {args.policy} is not valid JSON: {exc}") from exc
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
    """Run a gate; return 0 for pass, 1 for fail, 2 when inconclusive, 3 on error.

    Raises:
        SystemExit: Argument parsing exits with 0 for ``--help``, or 3 for
            invalid arguments or the unsupported ``--baseline`` option.
    """
    parser = _parser(argv)
    args = parser.parse_args(argv)
    if args.baseline is not None:
        parser.error("--baseline is not supported by the quality-gate API yet")
    if args.report is not None and args.report.suffix.lower() != ".json":
        parser.error(f"--report path must end in .json for a reloadable report: {args.report}")
    if args.report is not None:
        target = args.report.resolve()
        for flag, other in (("source", args.source), ("--policy", args.policy)):
            if other is not None and (
                target == Path(other).resolve()
                or (target.exists() and Path(other).exists() and target.samefile(other))
            ):
                parser.error(f"--report would overwrite the {flag} file: {args.report}")
    try:
        policy = _policy(args)
    except (OSError, ValueError, RuntimeError) as exc:
        return _report_error("policy", str(exc), path=args.policy, json_output=args.json)
    try:
        report = profile_file(
            args.source,
            engine=args.engine,
            format=args.format,
            max_rows=args.max_rows,
            metrics=args.metrics,
        )
        result = report.check(**policy)
    except (OSError, ValueError, TypeError, RuntimeError, OverflowError) as exc:
        return _report_error("input", str(exc), path=args.source, json_output=args.json)

    if args.report is not None:
        try:
            report.save(args.report)
        except (OSError, ValueError, TypeError, RuntimeError, OverflowError) as exc:
            return _report_error(
                "output",
                f"could not write report {args.report}: {exc}",
                path=args.report,
                json_output=args.json,
            )

    if args.json:
        print(result.to_json())
    print(
        f"dataprof {dataprof.__version__}: "
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
