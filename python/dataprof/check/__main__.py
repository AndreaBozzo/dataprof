"""Argument and exit-code adapter over the batch quality-gate API."""

from __future__ import annotations

import argparse
import errno
import json
import os
import stat
import sys
from argparse import ArgumentParser
from collections.abc import Sequence
from pathlib import Path
from typing import Any, NoReturn

import dataprof
from dataprof import profile_file
from dataprof._gate import _Policy, _PolicyFieldError, _summary_lines

_EXIT_CODES = {"pass": 0, "fail": 1, "inconclusive": 2}
_ERROR_EXIT_CODE = 3
# The extensions detect_format() in dataprof-partial maps to a format; keep the
# two lists in step. Files with any other extension (or none) are reported as
# skipped unless --format is supplied explicitly.
_INFERRED_EXTENSIONS = frozenset({".csv", ".tsv", ".txt", ".json", ".jsonl", ".ndjson", ".parquet"})
_POLICY_DEFAULTS: dict[str, Any] = {
    "min_rows": None,
    "min_quality_score": None,
    "min_dimension_scores": None,
    "max_null_percentage": None,
    "max_null_count": None,
    "max_duplicate_rows": None,
    "unique_columns": None,
    "max_date_age": None,
    "max_date_gap": None,
    "require_metrics": None,
    "require_columns": None,
    "scope": "full_source",
    "max_quality_score_drop": None,
    "max_dimension_score_drops": None,
    "max_null_percentage_increase": None,
    "max_null_count_increase": None,
    "max_distinct_drop": None,
    "max_row_count_drop": None,
    "max_row_count_growth": None,
    "forbid_removed_columns": False,
    "forbid_added_columns": False,
    "forbid_findings": None,
    "timeliness_clock": "fixed",
}


def _named_percentage(text: str) -> tuple[str, float]:
    name, separator, value = text.rpartition("=")
    if not separator or not name:
        raise argparse.ArgumentTypeError("expected NAME=PERCENT")
    try:
        return name, float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected NAME=PERCENT") from exc


def _named_days(text: str) -> tuple[str, int]:
    name, separator, value = text.rpartition("=")
    if not separator or not name:
        raise argparse.ArgumentTypeError("expected COLUMN=DAYS")
    try:
        return name, int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected COLUMN=DAYS") from exc


def _named_count(text: str) -> tuple[str, int]:
    name, separator, value = text.rpartition("=")
    if not separator or not name:
        raise argparse.ArgumentTypeError("expected NAME=COUNT")
    try:
        return name, int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected NAME=COUNT") from exc


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


def _positive_int(value: str) -> int:
    try:
        result = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid positive integer: {value!r}") from None
    if result < 1:
        raise argparse.ArgumentTypeError(f"invalid positive integer: {value!r}")
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


# The errors Path.exists() treats as "not there" on Python 3.11 to 3.13.
_ABSENT_ERRNOS = frozenset({errno.ENOENT, errno.ENOTDIR, errno.EBADF, errno.ELOOP})


def _stat_if_present(path: Path) -> os.stat_result | None:
    """Stat ``path``, or return None where ``Path.exists()`` would be False.

    Any other OSError, such as EACCES, is raised for the caller to report
    against this path.
    """
    try:
        return path.stat()
    except OSError as exc:
        if exc.errno in _ABSENT_ERRNOS:
            return None
        raise


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


def _version() -> str:
    """The version, with the commit where the build recorded one: a development
    build carries the version of the release it follows."""
    build = dataprof.capabilities()
    if build.commit is None:
        return f"dataprof {dataprof.__version__}"
    dirty = ", dirty" if build.dirty else ""
    return f"dataprof {dataprof.__version__} ({build.commit[:12]}{dirty})"


def _parser(argv: list[str] | None = None) -> _CheckArgumentParser:
    parser = _CheckArgumentParser(
        prog="python -m dataprof.check",
        argv=argv,
        description="Profile one or more local files and evaluate a quality policy.",
        epilog=(
            "Exit codes: 0 = pass (every source passed; also --help and --version); "
            "1 = a proven policy violation (at least one source failed); "
            "2 = inconclusive (nothing failed, and at least one threshold could not be evaluated); "
            "3 = an argument, policy, source, or baseline could not be read or used, "
            "or the report could not be written (including any unreadable source or empty "
            "directory in a multi-source run). "
            "A proven violation takes precedence over unevaluated checks. "
            "In a multi-source run, unreadable sources are reported without stopping the "
            "remaining sources, and files in a directory whose format cannot be inferred "
            "are reported as skipped. "
            "Rules about change need --baseline: a report an earlier run saved with --report "
            "(single source file only)."
        ),
    )
    parser.add_argument(
        "sources",
        type=Path,
        nargs="+",
        metavar="SOURCE",
        help="CSV, JSON, JSONL, or Parquet file, or a directory containing them",
    )
    parser.add_argument(
        "-r",
        "--recursive",
        action="store_true",
        help="recurse into subdirectories of directory arguments",
    )
    parser.add_argument(
        "--policy",
        type=Path,
        metavar="PATH",
        help="path to a UTF-8 JSON file of check() keywords; flags replace matching keys",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="write the QualityGateResult JSON (or multi-source document) to stdout",
    )
    parser.add_argument(
        "--report",
        type=Path,
        metavar="PATH",
        help=(
            "save the full profile to a .json file, including on fail or inconclusive "
            "(single source file only)"
        ),
    )
    parser.add_argument(
        "--source-version",
        metavar="VERSION",
        help=(
            "the version of the source being checked (a table snapshot, commit or load ID), "
            "recorded verbatim in the --report document and QualityGateResult.source_identity"
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=_version(),
    )
    parser.add_argument(
        "--min-rows",
        type=int,
        help="minimum number of rows the scan must read",
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
    parser.add_argument(
        "--max-null-count",
        dest="max_null_count",
        type=_named_count,
        action="append",
        metavar="COLUMN=COUNT",
        help="maximum null count, decided on the exact count (COLUMN=0 means no nulls); "
        "use *=COUNT for all columns; repeatable",
    )
    parser.add_argument("--max-duplicate-rows", type=int, help="maximum duplicate-row count")
    parser.add_argument(
        "--max-date-age",
        dest="max_date_age",
        type=_named_days,
        action="append",
        metavar="COLUMN=DAYS",
        help="how many days the column's newest date may lie before today; repeatable",
    )
    parser.add_argument(
        "--max-date-gap",
        dest="max_date_gap",
        type=_named_days,
        action="append",
        metavar="COLUMN=DAYS",
        help="the longest gap allowed between the column's consecutive dates "
        "(1 allows no missing day); repeatable",
    )
    parser.add_argument(
        "--require-metric",
        dest="require_metrics",
        action="append",
        metavar="NAME",
        help="require quality or a dimension to be analyzed; absence fails; repeatable",
    )
    parser.add_argument(
        "--require-column",
        dest="require_columns",
        action="append",
        metavar="NAME",
        help="require a column to be profiled; fails when the source lacks it; repeatable",
    )
    parser.add_argument(
        "--unique-column",
        dest="unique_columns",
        action="append",
        metavar="NAME",
        help="fail when a non-null value of the column occurs more than once; repeatable",
    )
    parser.add_argument(
        "--forbid-finding",
        dest="forbid_findings",
        action="append",
        metavar="CODE",
        help="fail when findings() reports this code, such as locale_numbers; repeatable",
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
        "--duplicate-rows-exact-limit",
        type=_positive_int,
        metavar="ROWS",
        help=(
            "count duplicate rows exactly up to ROWS distinct rows (default: 1000000), so "
            "--max-duplicate-rows can decide on a larger source; costs 10-20 bytes per row"
        ),
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
        help=(
            "a .json report an earlier run saved (--report or save()) to measure change from "
            "(single source file only)"
        ),
    )
    parser.add_argument(
        "--max-quality-drop",
        dest="max_quality_score_drop",
        type=float,
        metavar="POINTS",
        help="how far the overall quality score may fall below the baseline's (0-100)",
    )
    parser.add_argument(
        "--max-dimension-drop",
        dest="max_dimension_score_drops",
        type=_named_percentage,
        action="append",
        metavar="NAME=POINTS",
        help="how far a dimension score may fall below the baseline's (0-100); repeatable",
    )
    parser.add_argument(
        "--max-row-drop",
        dest="max_row_count_drop",
        type=float,
        metavar="PERCENT",
        help="how far the row count may fall, in percent of the baseline's (0-100)",
    )
    parser.add_argument(
        "--max-row-growth",
        dest="max_row_count_growth",
        type=float,
        metavar="PERCENT",
        help="how far the row count may grow, in percent of the baseline's (0 or more)",
    )
    parser.add_argument(
        "--max-null-increase",
        dest="max_null_percentage_increase",
        type=_named_percentage,
        action="append",
        metavar="COLUMN=POINTS",
        help=(
            "how far a column's null percentage may rise above the baseline's (0-100); "
            "use *=POINTS for every column both reports profiled; repeatable"
        ),
    )
    parser.add_argument(
        "--max-null-count-increase",
        dest="max_null_count_increase",
        type=_named_count,
        action="append",
        metavar="COLUMN=N",
        help=(
            "how many nulls a column may gain over the baseline's, decided on the exact counts; "
            "use *=N for every column both reports profiled; repeatable"
        ),
    )
    parser.add_argument(
        "--max-distinct-drop",
        dest="max_distinct_drop",
        type=_named_percentage,
        action="append",
        metavar="COLUMN=PERCENT",
        help=(
            "how far a column's distinct count may fall, in percent of the baseline's (0-100), "
            "failing only when its distinct share of non-null values fell as far; "
            "use *=PERCENT for every column both reports profiled; repeatable"
        ),
    )
    parser.add_argument(
        "--forbid-removed-columns",
        action="store_const",
        const=True,
        help="fail when a column the baseline profiled is gone",
    )
    parser.add_argument(
        "--forbid-added-columns",
        action="store_const",
        const=True,
        help="fail when a column the baseline did not profile appears (a closed schema)",
    )
    parser.add_argument(
        "--timeliness-clock",
        choices=("fixed", "rolling"),
        help=(
            "how the timeliness reference dates may differ from the baseline's: fixed "
            "(default) needs the same date, rolling lets each run measure against its own day"
        ),
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


def _policy(args: argparse.Namespace, baseline: dataprof.ProfileReport | None) -> dict[str, Any]:
    policy = dict(_POLICY_DEFAULTS)
    if args.policy is not None:
        try:
            raw_bytes = args.policy.read_bytes()
        except OSError as exc:
            raise ValueError(f"cannot read policy file {args.policy}: {exc.strerror}") from exc
        try:
            text = raw_bytes.decode("utf-8-sig")
        except UnicodeDecodeError as exc:
            raise ValueError(f"policy file {args.policy} is not UTF-8: {exc}") from exc
        try:
            document = json.loads(
                text,
                object_pairs_hook=_unique_policy_keys,
            )
        except json.JSONDecodeError as exc:
            raise ValueError(f"policy file {args.policy} is not valid JSON: {exc}") from exc
        except RecursionError as exc:
            raise ValueError(f"policy file {args.policy} is nested too deeply: {exc}") from exc
        except ValueError as exc:
            raise ValueError(f"policy file {args.policy}: {exc}") from exc
        if not isinstance(document, dict):
            raise ValueError(
                f"policy file {args.policy} must contain a JSON object of check() keywords"
            )
        unknown = document.keys() - policy.keys()
        if unknown:
            raise ValueError(
                f"policy file {args.policy} has unknown policy keys: {', '.join(sorted(unknown))}"
            )
        policy.update(document)
    for key in policy:
        value = getattr(args, key)
        if value is not None:
            if key in (
                "min_dimension_scores",
                "max_null_percentage",
                "max_null_count",
                "max_date_age",
                "max_date_gap",
                "max_dimension_score_drops",
                "max_null_percentage_increase",
                "max_null_count_increase",
                "max_distinct_drop",
            ):
                value = dict(value)
            policy[key] = value

    # Validate JSON container shapes before handing values to the same policy
    # validator the public check() method uses. Thresholds, dimension names,
    # scope, and empty-policy semantics belong to that validator.
    for key in ("min_dimension_scores", "max_dimension_score_drops"):
        if policy[key] is not None and not isinstance(policy[key], dict):
            raise ValueError(f"policy file {args.policy}: {key} must be a JSON object")
    required = policy["require_metrics"]
    if required is not None and (
        not isinstance(required, list) or not all(isinstance(name, str) for name in required)
    ):
        raise ValueError(
            f"policy file {args.policy}: require_metrics must be a JSON array of names"
        )
    for key in ("require_columns", "forbid_findings"):
        names = policy[key]
        if names is not None and (
            not isinstance(names, list) or not all(isinstance(name, str) for name in names)
        ):
            raise ValueError(f"policy file {args.policy}: {key} must be a JSON array of names")
    _Policy(**policy, baseline=None if baseline is None else baseline._delta_input())
    return policy


def _is_directory(path: Path) -> bool:
    """Return True when ``path`` resolves to an existing directory."""
    try:
        st = _stat_if_present(path)
    except OSError:
        return False
    return st is not None and stat.S_ISDIR(st.st_mode)


def _expand_directory(
    directory: Path,
    *,
    recursive: bool,
    explicit_format: str | None,
) -> tuple[list[tuple[Path, str | None]], list[Path]]:
    """Expand a directory into ``(path, pre_error)`` items and skipped paths.

    Supported files are returned in deterministic sorted order. Files whose
    format cannot be inferred (when ``explicit_format`` is None) are returned
    in ``skipped`` rather than dropped silently. Symlinked directories are not
    traversed so directory cycles cannot loop infinitely.
    """
    items: list[tuple[Path, str | None]] = []
    skipped: list[Path] = []

    def _walk(current: Path) -> None:
        try:
            entries = sorted(current.iterdir(), key=lambda p: p.name)
        except OSError as exc:
            detail = exc.strerror or str(exc)
            items.append((current, f"cannot list directory {current}: {detail}"))
            return
        for entry in entries:
            if entry.is_symlink():
                try:
                    target_st = entry.stat()
                except OSError:
                    if explicit_format is not None or entry.suffix.lower() in _INFERRED_EXTENSIONS:
                        items.append((entry, None))
                    else:
                        skipped.append(entry)
                    continue
                if stat.S_ISDIR(target_st.st_mode):
                    continue
                if explicit_format is not None or entry.suffix.lower() in _INFERRED_EXTENSIONS:
                    items.append((entry, None))
                else:
                    skipped.append(entry)
                continue
            try:
                entry_st = entry.lstat()
            except OSError as exc:
                detail = exc.strerror or str(exc)
                items.append((entry, f"cannot stat {entry}: {detail}"))
                continue
            if stat.S_ISDIR(entry_st.st_mode):
                if recursive:
                    _walk(entry)
                continue
            if explicit_format is not None or entry.suffix.lower() in _INFERRED_EXTENSIONS:
                items.append((entry, None))
            else:
                skipped.append(entry)

    _walk(directory)
    if not items:
        if skipped:
            suffix = f" ({len(skipped)} file(s) whose format could not be inferred)"
        else:
            suffix = ""
        items.append((directory, f"no supported files found in directory {directory}{suffix}"))
    return items, skipped


def _expand_sources(
    sources: Sequence[Path],
    *,
    recursive: bool,
    explicit_format: str | None,
) -> tuple[list[tuple[Path, str | None]], list[Path]]:
    """Expand positional arguments, turning directory arguments into their files."""
    items: list[tuple[Path, str | None]] = []
    skipped: list[Path] = []
    for source in sources:
        if _is_directory(source):
            dir_items, dir_skipped = _expand_directory(
                source,
                recursive=recursive,
                explicit_format=explicit_format,
            )
            items.extend(dir_items)
            skipped.extend(dir_skipped)
        else:
            items.append((source, None))
    return items, skipped


def _load_policy(
    args: argparse.Namespace,
    baseline: dataprof.ProfileReport | None,
) -> tuple[dict[str, Any] | None, int | None]:
    """Validate and load policy configuration, reporting policy errors on failure."""
    try:
        return _policy(args, baseline), None
    except (OSError, ValueError, RuntimeError) as exc:
        message = str(exc)
        if isinstance(exc, _PolicyFieldError) and exc.key == "baseline":
            message = f"{message}: pass --baseline PATH"
        # A value error names the file only when the file supplied that key
        # and no flag replaced it. A missing baseline is no file's key.
        from_flag = isinstance(exc, _PolicyFieldError) and (
            exc.key == "baseline" or getattr(args, exc.key, None) is not None
        )

        if (
            not args.json
            and args.policy is not None
            and isinstance(exc, _PolicyFieldError)
            and not from_flag
        ):
            message = f"policy file {args.policy}: {message}"

        code = _report_error(
            "policy",
            message,
            path=None if from_flag else args.policy,
            json_output=args.json,
        )
        return None, code


def _multi_summary_lines(
    verdict: str,
    outcomes: Sequence[tuple[Path, dataprof.QualityGateResult | None, str | None]],
    skipped: Sequence[Path],
    *,
    version: str,
) -> list[str]:
    """Format the human-readable stderr summary for a multi-source run."""
    passed = sum(1 for _, res, _ in outcomes if res is not None and res.verdict == "pass")
    failed = sum(1 for _, res, _ in outcomes if res is not None and res.verdict == "fail")
    inconclusive = sum(
        1 for _, res, _ in outcomes if res is not None and res.verdict == "inconclusive"
    )
    errors = sum(1 for _, _, err in outcomes if err is not None)
    lines = [
        f"dataprof {version}: {verdict}: {len(outcomes)} sources "
        f"({passed} passed, {failed} failed, {inconclusive} inconclusive, {errors} errors)"
    ]
    for path, result, error_message in outcomes:
        if error_message is not None:
            lines.append(f"  error: {path}: {error_message}")
            continue
        assert result is not None
        lines.append(
            f"  {result.verdict}: {path} "
            f"({len(result.checks)} checks, {len(result.violations)} violations, "
            f"{len(result.unevaluated)} unevaluated)"
        )
        for check in result.checks:
            if check.status != "passed":
                subject = next(
                    (s for s in (check.column, check.dimension, check.finding) if s is not None),
                    None,
                )
                label = f" [{subject}]" if subject is not None else ""
                lines.append(f"    {check.status}: {check.code}{label}: {check.message}")
    for path in skipped:
        lines.append(f"  skipped: {path}: format could not be inferred")
    return lines


def _run_single(parser: _CheckArgumentParser, args: argparse.Namespace, source: Path) -> int:
    if args.report is not None and args.report.suffix.lower() != ".json":
        parser.error(f"--report path must end in .json for a reloadable report: {args.report}")
    if args.report is not None:
        # Up to Python 3.12, resolve() raises RuntimeError on a symlink loop
        # even when not strict; report that like any other bad path. The
        # report is stat'ed here too, so an error reading it (EACCES, say)
        # is reported against the report and not the path compared with it.
        try:
            target = args.report.resolve()
            target_stat = _stat_if_present(target)
        except (OSError, RuntimeError) as exc:
            return _report_error(
                "output",
                f"could not resolve report path {args.report}: {exc}",
                path=args.report,
                json_output=args.json,
            )
        # A report is written whatever the verdict, so writing it over the
        # baseline would make a failing run the next run's baseline.
        for flag, kind, other in (
            ("source", "input", source),
            ("--policy", "policy", args.policy),
            ("--baseline", "baseline", args.baseline),
        ):
            if other is None:
                continue
            try:
                other_path = Path(other)
                overwrites = target == other_path.resolve()
                if not overwrites and target_stat is not None:
                    other_stat = _stat_if_present(other_path)
                    overwrites = other_stat is not None and os.path.samestat(
                        target_stat, other_stat
                    )
            except (OSError, RuntimeError) as exc:
                return _report_error(
                    kind,
                    f"could not resolve {flag} path {other}: {exc}",
                    path=other,
                    json_output=args.json,
                )
            if overwrites:
                parser.error(f"--report would overwrite the {flag} file: {args.report}")
    baseline = None
    if args.baseline is not None:
        try:
            baseline = dataprof.ProfileReport.load(args.baseline)
        except (OSError, ValueError, TypeError, RuntimeError) as exc:
            detail = exc.strerror if isinstance(exc, OSError) and exc.strerror else exc
            return _report_error(
                "baseline",
                f"cannot read baseline {args.baseline}: {detail}",
                path=args.baseline,
                json_output=args.json,
            )
    policy, policy_err = _load_policy(args, baseline)
    if policy is None:
        assert policy_err is not None
        return policy_err
    try:
        report = profile_file(
            source,
            engine=args.engine,
            format=args.format,
            max_rows=args.max_rows,
            metrics=args.metrics,
            duplicate_rows_exact_limit=args.duplicate_rows_exact_limit,
            source_version=args.source_version,
        )
    except (OSError, ValueError, TypeError, RuntimeError, OverflowError) as exc:
        return _report_error("input", str(exc), path=source, json_output=args.json)
    try:
        result = report.check(**policy, baseline=baseline)
    except (OSError, ValueError, TypeError, RuntimeError, OverflowError) as exc:
        # The policy was validated and today's report is native, so a
        # comparison that cannot start is the baseline's: a summary load()
        # accepts can still be one the gate cannot decode.
        if baseline is not None:
            return _report_error(
                "baseline",
                f"cannot compare with baseline {args.baseline}: {exc}",
                path=args.baseline,
                json_output=args.json,
            )
        return _report_error("input", str(exc), path=source, json_output=args.json)

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
    for line in _summary_lines(result, version=dataprof.__version__, baseline_path=args.baseline):
        print(line, file=sys.stderr)
    return _EXIT_CODES[result.verdict]


def _run_multi(parser: _CheckArgumentParser, args: argparse.Namespace) -> int:
    if args.baseline is not None:
        parser.error("--baseline is only supported with a single source file")
    if args.report is not None:
        parser.error("--report is only supported with a single source file")
    policy, policy_err = _load_policy(args, baseline=None)
    if policy is None:
        assert policy_err is not None
        return policy_err

    items, skipped = _expand_sources(
        args.sources,
        recursive=args.recursive,
        explicit_format=args.format,
    )
    results: list[dataprof.QualityGateResult] = []
    source_documents: list[dict[str, Any]] = []
    outcomes: list[tuple[Path, dataprof.QualityGateResult | None, str | None]] = []
    error_count = 0

    for path, pre_error in items:
        if pre_error is not None:
            source_documents.append(
                {"path": str(path), "error": {"kind": "input", "message": pre_error}}
            )
            outcomes.append((path, None, pre_error))
            error_count += 1
            continue
        try:
            report = profile_file(
                path,
                engine=args.engine,
                format=args.format,
                max_rows=args.max_rows,
                metrics=args.metrics,
                duplicate_rows_exact_limit=args.duplicate_rows_exact_limit,
                source_version=args.source_version,
            )
            result = report.check(**policy)
        except (OSError, ValueError, TypeError, RuntimeError, OverflowError) as exc:
            message = str(exc)
            source_documents.append(
                {"path": str(path), "error": {"kind": "input", "message": message}}
            )
            outcomes.append((path, None, message))
            error_count += 1
            continue
        results.append(result)
        source_documents.append({"path": str(path), "result": result.to_dict()})
        outcomes.append((path, result, None))

    if error_count > 0:
        verdict = "error"
        exit_code = _ERROR_EXIT_CODE
    else:
        verdict = dataprof.combine_verdicts(results)
        exit_code = _EXIT_CODES[verdict]

    if args.json:
        document = {
            "verdict": verdict,
            "sources": source_documents,
            "skipped": [{"path": str(path), "reason": "format_not_inferred"} for path in skipped],
        }
        print(json.dumps(document, indent=2))
    for line in _multi_summary_lines(verdict, outcomes, skipped, version=dataprof.__version__):
        print(line, file=sys.stderr)
    return exit_code


def main(argv: list[str] | None = None) -> int:
    """Run a gate; return 0 for pass, 1 for fail, 2 when inconclusive, 3 on error.

    Raises:
        SystemExit: Argument parsing exits with 0 for ``--help``, or 3 for
            invalid arguments.
    """
    parser = _parser(argv)
    args = parser.parse_args(argv)
    if len(args.sources) == 1 and not _is_directory(args.sources[0]):
        return _run_single(parser, args, args.sources[0])
    return _run_multi(parser, args)


if __name__ == "__main__":
    raise SystemExit(main())
