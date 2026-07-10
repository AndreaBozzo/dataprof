"""Guard against silently swallowed errors on decode paths (issue #364).

Every `.ok()`, `.unwrap_or_default()` or `.unwrap_or(0...)` in the audited
decode modules must carry a `decode-audit:` classification comment on the same
line or within the preceding lines, naming the case it falls into:

- ``impossible`` — the error cannot happen (prefer ``expect()`` with a reason)
- ``no-data``    — the absent value genuinely means "no data", not an error
- ``unknown``    — the failure is surfaced (logged/propagated), never a default

A new unclassified instance in these modules fails CI. See AGENTS.md
("Error handling") and issue #364 for the rationale.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# Decode boundaries: the modules where an error mistaken for "missing data"
# becomes a silently wrong profile. Keep this list narrow and intentional.
AUDITED_PATHS = [
    "crates/dataprof-db/src/connectors",
    "crates/dataprof-db/src/streaming.rs",
    "crates/dataprof-db/src/lib.rs",
    "crates/dataprof-parquet/src",
    "crates/dataprof-json/src",
    "crates/dataprof-csv/src",
    "crates/dataprof-engines/src/streaming",
    "crates/dataprof-python/src/arrow_export.rs",
    "crates/dataprof-metrics/src/stats",
]

SWALLOW_PATTERN = re.compile(r"\.ok\(\)|\.unwrap_or_default\(\)|\.unwrap_or\(0")
AUDIT_TAG = "decode-audit:"
# How many lines above a match the classification comment may sit.
TAG_WINDOW = 10


def rust_files(root: Path) -> list[Path]:
    files: list[Path] = []
    for entry in AUDITED_PATHS:
        path = root / entry
        if path.is_file():
            files.append(path)
        elif path.is_dir():
            files.extend(sorted(path.rglob("*.rs")))
        else:
            print(f"error: audited path does not exist: {entry}", file=sys.stderr)
            sys.exit(2)
    return files


def check_file(path: Path, root: Path) -> list[str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    violations: list[str] = []
    for lineno, line in enumerate(lines, start=1):
        if not SWALLOW_PATTERN.search(line):
            continue
        window = lines[max(0, lineno - 1 - TAG_WINDOW) : lineno]
        if any(AUDIT_TAG in context_line for context_line in window):
            continue
        rel = path.relative_to(root).as_posix()
        violations.append(f"{rel}:{lineno}: {line.strip()}")
    return violations


def main() -> int:
    root = Path(__file__).resolve().parents[2]
    violations: list[str] = []
    for path in rust_files(root):
        violations.extend(check_file(path, root))

    if violations:
        print(
            "Unclassified error-swallowing pattern(s) on audited decode paths.\n"
            "Each .ok() / .unwrap_or_default() / .unwrap_or(0...) here must have\n"
            f"a `{AUDIT_TAG}` comment within the {TAG_WINDOW} lines above it\n"
            "(cases: impossible | no-data | unknown — see issue #364).\n",
            file=sys.stderr,
        )
        for violation in violations:
            print(violation, file=sys.stderr)
        return 1

    print("decode-audit check passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
