"""Check the project site's quickstart figures against one profile of the sample.

``site/index.html`` prints the version, quality score, null percentages, CI
verdict and the Python and Rust quickstart output for ``site/assets/orders.csv``.
Those figures are the text of elements marked ``data-site-figure``. This script
profiles the sample with the dataprof it was run with — the built wheel, in
CI — and fails when a marked figure disagrees. It does not keep a second copy
of the numbers, so correcting the page is the edit that updates them. The
Python and Rust examples are read the same way: a print this check cannot
replay is an error.
"""

from __future__ import annotations

import html.parser
import math
import re
import shlex
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

PAGE = Path("site/index.html")
SAMPLE = Path("site/assets/orders.csv")

_CHECK_START = "report.check("
_COMMAND_PREFIX = ("python", "-m", "dataprof.check")
_NUMBER = r"[0-9]+(?:\.[0-9]+)?"
_PRINT_CALL = re.compile(r"print\s*\(")
_PRINT_QUALITY = re.compile(r"print\(\s*report\.quality_score\s*\)")
_PRINT_NULL = re.compile(
    r"""print\(\s*report\[\s*(['"])(?P<column>[^'"]+)\1\s*\]\.null_percentage\s*\)"""
)
_PRINT_VERDICT = re.compile(r"print\(\s*result\.verdict\s*\)")
_KW_DICT = re.compile(
    rf"""([A-Za-z_][A-Za-z0-9_]*)\s*=\s*\{{\s*(['"])([^'"]+)\2\s*:\s*({_NUMBER})\s*\}}"""
)
_KW_NUMBER = re.compile(rf"""([A-Za-z_][A-Za-z0-9_]*)\s*=\s*({_NUMBER})""")
_RUST_ROWS = re.compile(
    r"""println!\(\s*"Rows: \{\}"\s*,\s*report\.execution\.rows_processed\s*,?\s*\)"""
)
_RUST_QUALITY = re.compile(r"""println!\(\s*"Quality: \{score:\.2\}"\s*\)""")
_RUST_COLUMN = re.compile(
    r"""println!\(\s*"\{\} \{:\?\} nulls=\{\}"\s*,\s*"""
    r"""col\.name\s*,\s*col\.data_type\s*,\s*col\.null_count\s*,?\s*\)"""
)
# html.parser does not treat these as empty, so counting one leaves the mark open.
_VOID_TAGS = frozenset({"br", "hr", "img", "input", "wbr"})


class PageError(Exception):
    """The page is missing a mark, or a marked example cannot be read."""


@dataclass(frozen=True)
class _Print:
    kind: str
    column: str | None = None


@dataclass(frozen=True)
class Quickstart:
    """The prints and check() arguments read from the Python example."""

    prints: tuple[_Print, ...]
    check_arguments: dict[str, float | dict[str, float]]


@dataclass(frozen=True)
class Page:
    """Figures and examples read from the project page."""

    versions: tuple[str, ...]
    quality_scores: tuple[str, ...]
    null_percents: tuple[tuple[str, str], ...]
    verdicts: tuple[str, ...]
    exit_code: str
    python_output: str
    rust_output: str
    quickstart: Quickstart
    rust_prints: tuple[_Print, ...]
    command_flags: tuple[str, ...]


@dataclass(frozen=True)
class Column:
    name: str
    data_type: str
    null_count: int
    null_percentage: float


@dataclass(frozen=True)
class Run:
    """One profile of the sample and the quickstart check made from it."""

    version: str
    rows: int
    columns: tuple[Column, ...]
    quality_score: float
    verdict: str
    exit_code: int


@dataclass
class _Frame:
    tag: str
    kind: str
    column: str | None
    parts: list[str]
    depth: int = 0


class _Marks(html.parser.HTMLParser):
    """Collect the text of each ``data-site-figure`` element, including descendants."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._open: list[_Frame] = []
        self.found: list[tuple[str, str | None, str]] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _VOID_TAGS:
            return
        if self._open:
            self._open[-1].depth += 1
        attr = {key: value or "" for key, value in attrs}
        kind = attr.get("data-site-figure")
        if kind:
            self._open.append(_Frame(tag, kind, attr.get("data-site-column") or None, []))

    def handle_startendtag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in _VOID_TAGS:
            return
        super().handle_startendtag(tag, attrs)

    def handle_endtag(self, tag: str) -> None:
        if not self._open:
            return
        frame = self._open[-1]
        if frame.depth:
            frame.depth -= 1
            return
        if frame.tag != tag:
            return
        self._open.pop()
        self.found.append((frame.kind, frame.column, "".join(frame.parts).strip()))
        if self._open:
            self._open[-1].depth -= 1

    def handle_data(self, data: str) -> None:
        for frame in self._open:
            frame.parts.append(data)


def _one(found: list[tuple[str | None, str]], kind: str) -> str:
    if len(found) != 1:
        raise PageError(f"expected one {kind} element, found {len(found)}")
    return found[0][1]


def _prints(source: str) -> tuple[_Print, ...]:
    prints: list[_Print] = []
    for line in source.splitlines():
        text = line.strip()
        if _PRINT_CALL.match(text) is None:
            continue
        if _PRINT_QUALITY.fullmatch(text):
            prints.append(_Print("quality-score"))
        elif match := _PRINT_NULL.fullmatch(text):
            prints.append(_Print("null-percentage", match.group("column")))
        elif _PRINT_VERDICT.fullmatch(text):
            prints.append(_Print("verdict"))
        else:
            raise PageError(f"the Python example has a print this check cannot replay: {text}")
    if not prints:
        raise PageError("the Python example does not print a figure")
    return tuple(prints)


def _matching_paren(source: str, open_paren: int) -> int:
    depth = 0
    for end, character in enumerate(source[open_paren:], start=open_paren):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                return end
    raise PageError("the Rust example's println! does not close")


def _rust_prints(source: str) -> tuple[_Print, ...]:
    """The println! lines the Rust example shows, in order.

    Each one has to be a line ``rust_output`` can reproduce. Anything else,
    including a ``print!`` call, is reported on its own, the way ``_prints``
    reports a Python print.
    """

    prints: list[_Print] = []
    index = 0
    while True:
        printed = source.find("print!", index)
        start = source.find("println!", index)
        if printed >= 0 and (start < 0 or printed < start):
            snippet = source[printed:].splitlines()[0].strip()
            raise PageError(f"the Rust example has a print this check cannot replay: {snippet}")
        if start < 0:
            break
        open_paren = start + len("println!")
        while open_paren < len(source) and source[open_paren].isspace():
            open_paren += 1
        if open_paren >= len(source) or source[open_paren] != "(":
            snippet = source[start:].splitlines()[0].strip()
            raise PageError(f"the Rust example has a print this check cannot replay: {snippet}")
        end = _matching_paren(source, open_paren)
        call = source[start : end + 1]
        if _RUST_ROWS.fullmatch(call):
            prints.append(_Print("rows"))
        elif _RUST_QUALITY.fullmatch(call):
            prints.append(_Print("quality"))
        elif _RUST_COLUMN.fullmatch(call):
            prints.append(_Print("column"))
        else:
            shown = " ".join(call.split())
            raise PageError(f"the Rust example has a print this check cannot replay: {shown}")
        index = end + 1
    if not prints:
        raise PageError("the Rust example does not print a figure")
    return tuple(prints)


def _check_arguments(source: str) -> dict[str, float | dict[str, float]]:
    start = source.find(_CHECK_START)
    if start < 0:
        raise PageError("the Python example does not call report.check()")
    index = start + len(_CHECK_START)
    depth = 1
    for end, character in enumerate(source[index:], start=index):
        if character == "(":
            depth += 1
        elif character == ")":
            depth -= 1
            if depth == 0:
                body = source[index:end]
                break
    else:
        raise PageError("the Python example's report.check() call does not close")

    arguments: dict[str, float | dict[str, float]] = {}
    consumed = body
    for match in _KW_DICT.finditer(body):
        name, _quote, key, number = match.groups()
        if name in arguments:
            raise PageError(f"the Python example sets {name} twice")
        arguments[name] = {key: float(number)}
        consumed = consumed.replace(match.group(0), " ", 1)
    for match in _KW_NUMBER.finditer(consumed):
        name, number = match.groups()
        if name in arguments:
            raise PageError(f"the Python example sets {name} twice")
        arguments[name] = float(number)
        consumed = consumed.replace(match.group(0), " ", 1)
    shown = " ".join(consumed.replace(",", " ").split())
    if shown:
        raise PageError(
            "the Python example's report.check() call has an argument this check cannot read: "
            + shown
        )
    if not arguments:
        raise PageError("the Python example's report.check() call has no arguments")
    return arguments


def _command_flags(command: str) -> tuple[str, ...]:
    try:
        tokens = tuple(shlex.split(command, posix=True))
    except ValueError as exc:
        raise PageError(f"the CI command cannot be parsed: {exc}") from exc
    if len(tokens) < 4 or tokens[:3] != _COMMAND_PREFIX or tokens[3].startswith("-"):
        raise PageError(
            "the CI command must be `python -m dataprof.check <file>` followed by its flags"
        )
    return tokens[4:]


def parse_page(html: str) -> Page:
    """Read the marked figures and the two quickstart examples from the page."""

    parser = _Marks()
    parser.feed(html)
    grouped: dict[str, list[tuple[str | None, str]]] = {}
    for kind, column, text in parser.found:
        grouped.setdefault(kind, []).append((column, text))

    def require(kind: str) -> list[tuple[str | None, str]]:
        found = grouped.get(kind)
        if not found:
            raise PageError(f"the page has no {kind} element")
        return found

    null_percents: list[tuple[str, str]] = []
    for column, text in require("null-percent"):
        if not column:
            raise PageError("a null-percent element needs data-site-column")
        null_percents.append((column, text))

    example = _one(require("python-example"), "python-example")
    rust_example = _one(require("rust-example"), "rust-example")
    return Page(
        versions=tuple(text for _column, text in require("version")),
        quality_scores=tuple(text for _column, text in require("quality-score")),
        null_percents=tuple(null_percents),
        verdicts=tuple(text for _column, text in require("verdict")),
        exit_code=_one(require("exit-code"), "exit-code"),
        python_output=_one(require("python-output"), "python-output"),
        rust_output=_one(require("rust-output"), "rust-output"),
        quickstart=Quickstart(_prints(example), _check_arguments(example)),
        rust_prints=_rust_prints(rust_example),
        command_flags=_command_flags(_one(require("ci-command"), "ci-command")),
    )


def _number_text(value: float) -> str:
    """The text ``print`` gives a float, which is what the Python example shows."""

    return str(value)


def _percent_text(value: float) -> str:
    """A whole percentage as ``25%``; any fraction keeps the digits ``print`` would."""

    if value == math.trunc(value):
        return f"{int(value)}%"
    return f"{_number_text(value)}%"


def _rust_type(data_type: str) -> str:
    """The Debug spelling the Rust example prints, from the lowercase Python name.

    ``DataType``'s Debug form is the variant (``Integer``) and Python reports
    ``DataType::name`` (``integer``). The two differ by that capital.
    """

    if data_type.isascii() and data_type.isidentifier() and data_type == data_type.lower():
        return data_type.capitalize()
    raise PageError(f"cannot print a Rust type for {data_type!r}")


def python_output(page: Page, run: Run) -> str:
    """The lines the marked Python example prints for this run."""

    columns = {column.name: column for column in run.columns}
    lines: list[str] = []
    for step in page.quickstart.prints:
        if step.kind == "quality-score":
            lines.append(_number_text(run.quality_score))
        elif step.kind == "null-percentage":
            column = columns.get(step.column or "")
            if column is None:
                raise PageError(
                    f"the Python example prints {step.column!r}, which the profile does not have"
                )
            lines.append(_number_text(column.null_percentage))
        else:
            lines.append(run.verdict)
    return "\n".join(lines)


def rust_output(page: Page, run: Run) -> str:
    """The lines the marked Rust example prints for this run."""

    lines: list[str] = []
    for step in page.rust_prints:
        if step.kind == "rows":
            lines.append(f"Rows: {run.rows}")
        elif step.kind == "quality":
            lines.append(f"Quality: {run.quality_score:.2f}")
        elif step.kind == "column":
            for column in run.columns:
                rust_type = _rust_type(column.data_type)
                lines.append(f"{column.name} {rust_type} nulls={column.null_count}")
        else:
            raise PageError(f"the Rust example has a print this check cannot replay: {step.kind}")
    return "\n".join(lines)


def differences(page: Page, run: Run) -> list[str]:
    """Each marked figure that disagrees with ``run``, as a line naming both texts."""

    found: list[str] = []

    def differ(kind: str, shown: str, expected: str) -> None:
        if shown != expected:
            hint = f" (edit {PAGE.as_posix()})" if kind == "version" else ""
            found.append(f"{kind}: page shows {shown!r}, the run shows {expected!r}{hint}")

    for shown in page.versions:
        differ("version", shown, run.version)
    for shown in page.quality_scores:
        differ("quality-score", shown, _number_text(run.quality_score))
    for shown in page.verdicts:
        differ("verdict", shown, run.verdict)
    differ("exit-code", page.exit_code, str(run.exit_code))

    columns = {column.name: column for column in run.columns}
    marked = {column for column, _text in page.null_percents}
    for name in columns:
        if name not in marked:
            found.append(f"null-percent: the page does not show {name}")
    for name, shown in page.null_percents:
        column = columns.get(name)
        if column is None:
            found.append(f"null-percent: the page shows {name}, which the profile does not have")
            continue
        differ(f"null-percent[{name}]", shown, _percent_text(column.null_percentage))

    try:
        differ("python-output", page.python_output, python_output(page, run))
        differ("rust-output", page.rust_output, rust_output(page, run))
    except PageError as exc:
        found.append(str(exc))
    return found


def _profile(root: Path, page: Page) -> Run:
    try:
        import dataprof
    except ImportError as exc:
        raise PageError(f"dataprof is not installed in this interpreter: {exc}") from exc

    sample = root / SAMPLE
    report = dataprof.profile(sample)
    if report.quality_score is None:
        raise PageError(f"{sample} produced no quality score")
    columns: list[Column] = []
    for column in report.profiles:
        if column.null_percentage is None:
            raise PageError(f"{column.name} has no null percentage")
        columns.append(
            Column(
                name=column.name,
                data_type=str(column.data_type),
                null_count=int(column.null_count),
                null_percentage=float(column.null_percentage),
            )
        )
    names = [column.name for column in columns]
    if len(names) != len(set(names)):
        raise PageError("the profile has duplicate column names")

    try:
        verdict = str(report.check(**page.quickstart.check_arguments).verdict)
    except TypeError as exc:
        raise PageError(f"the Python example's check() arguments were rejected: {exc}") from exc

    completed = subprocess.run(
        [sys.executable, "-m", "dataprof.check", str(sample), *page.command_flags],
        cwd=root,
        check=False,
        capture_output=True,
        text=True,
    )
    return Run(
        version=dataprof.__version__,
        rows=int(report.rows),
        columns=tuple(columns),
        quality_score=float(report.quality_score),
        verdict=verdict,
        exit_code=_command_exit(
            completed.stderr, completed.returncode, dataprof.__version__, verdict
        ),
    )


def _command_exit(stderr: str, returncode: int, version: str, verdict: str) -> int:
    """The process exit, once stderr shows the command actually reported ``verdict``.

    A crash can exit 1, which is also the exit for a failing gate. The summary
    line is how ``python -m dataprof.check`` announces the verdict it decided.
    """

    summary = f"dataprof {version}: {verdict}:"
    if not stderr.startswith(summary):
        detail = stderr.strip() or "no output"
        raise PageError(f"the CI command did not report {verdict!r} ({detail})")
    return returncode


def check_repository(root: Path) -> list[str]:
    """Profile the sample and return every figure on the page that disagrees."""

    try:
        page = parse_page((root / PAGE).read_text(encoding="utf-8"))
        return differences(page, _profile(root, page))
    except PageError as exc:
        return [str(exc)]
    except OSError as exc:
        return [f"could not read the sample: {exc}"]


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    failures = check_repository(root)
    if failures:
        print("site quickstart figures disagree with the profile run:", file=sys.stderr)
        for failure in failures:
            print(f"- {failure}", file=sys.stderr)
        return 1
    print("site quickstart figures match the profile run")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
