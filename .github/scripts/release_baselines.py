"""Write the release-baseline corpus and the reports a published release wrote
for it (#1245).

``tests/release_baselines.rs`` and ``python/tests/test_release_baselines.py``
profile each input with the current build and gate it against the report the
last release saved, with a zero drop allowed on every score. A fix that moves a
score on these unchanged bytes without a ``metric_semantics`` definition fails
there, in its own PR, instead of failing every user's gate after the upgrade.

The inputs carry the values recent fixes changed the reading of: wide integers,
decimals, long float literals in JSON, and values on and past the range-rule
bounds. Run this after each release, from the repository root::

    uv run --no-sync python .github/scripts/release_baselines.py 0.14.0

It rewrites the inputs from the definitions below, then profiles each with the
published wheel in an isolated environment (never the local build: with
matching versions, ``--with dataprof==X`` can resolve to the repository's own
package, so the child refuses to run unless it imported the PyPI wheel).
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from decimal import Decimal
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
CORPUS = ROOT / "tests" / "fixtures" / "release_baselines"

# The child: profiles every input in the corpus with the installed wheel and
# saves each report beside it. It runs inside the corpus and profiles each file
# by its name, so a report records `"path": "<name>"`, never this machine's.
CHILD = """
import sys
from pathlib import Path

import dataprof as dp

version, root = sys.argv[1], Path(sys.argv[2])
module = Path(dp.__file__).resolve()
if dp.__version__ != version or module.is_relative_to(root):
    sys.exit(f"imported dataprof {dp.__version__} from {module}, wanted the {version} wheel")
for path in sorted(Path().iterdir()):
    if path.name == "RELEASE" or path.name.endswith(".report.json"):
        continue
    dp.profile(path.name).save(path.name + ".report.json")
    print(f"{path.name}: saved with {version}")
"""

#: 64-bit integers past 2^53, one about 6 below the lower Tukey fence (#1199).
WIDE = [
    1759700000123456512,
    1759700000123456916,
    1759700000123456940,
    1759700000123456945,
    1759700000123457061,
    1759700000123457175,
    1759700000123457320,
    1759700000123457409,
]
#: A decimal column on 10^13 whose lowest value lies below the lower fence by
#: less than float fences resolve (#1199).
CENTS = [
    Decimal("9999999999999") + Decimal(c) / 100 for c in (80, 173, 195, 209, 222, 255, 284, 331)
]


def _lines(header: str, rows: list[str]) -> str:
    return "\n".join([header, *rows]) + "\n"


TEXT = {
    # Range and sign rules on and past their bounds, read exactly (#1237).
    "range_bounds.csv": _lines(
        "discount_percent,age,price",
        [
            "100.000000000000000001,0,-1e-400",
            "100,120,0",
            "0,35,19.99",
            "50,-1,1e400",
            "25.5,64,4.5",
        ],
    ),
    "wide_integers.csv": _lines(
        "id,event_ns,balance",
        [f"{i},{w},{c}" for i, (w, c) in enumerate(zip(WIDE, CENTS, strict=True), start=1)],
    ),
    # Decimals, slash and ISO dates, a pattern column and nulls: the CSV text
    # inferrer, precision, timeliness and validity (#1305, the #1245 comment).
    "decimals.csv": _lines(
        "id,price,rate,email,signup,shipped",
        [
            "1,1.5,0.125,a@example.com,03/04/2024,2024-03-05",
            "2,2.25,0.1,b@example.com,13/04/2024,2024-04-14",
            "3,3.5,,c@example.com,25/12/2023,",
            "4,4.75,0.33333,not-an-email,01/01/2024,2024-01-02",
            "5,,0.5,e@example.com,,2024-06-30",
            "6,10.0,1.0,f@example.com,30/06/2024,2024-07-01",
        ],
    ),
    # Long float literals an f64 does not hold, and numbers past f64's range
    # (#1166, #1243).
    "long_floats.json": (
        '[{"id":1,"v":1000000000000000.01},{"id":2,"v":1000000000000000.05},'
        '{"id":3,"v":1000000000000000.02},{"id":4,"v":1e-400},{"id":5,"v":2e-400},'
        '{"id":6,"v":1.50},{"id":7,"v":[1000000000000000.01]}]\n'
    ),
    # Spaced ISO datetimes with a fraction or an offset, which 0.14 reads as
    # dates (#1335): beside words in a text column, and alone in a column the
    # release typed as text (#1340).
    "spaced_datetimes.csv": _lines(
        "id,event,logged_at,day",
        [
            "1,heartbeat,2026-06-01 12:00:00+00:00,2026-06-01",
            "2,2026-06-01 12:05:00+00:00,2026-06-01 12:05:00.250Z,2026-06-02",
            "3,restart,2026-06-02 08:30:00+02:00,2026-06-03",
            "4,2026-06-02 09:00:00.5,2026-06-02 23:59:59-05:00,2026-06-04",
        ],
    ),
    "json_digits.jsonl": (
        '{"amount":1000000000000000.01,"n":1,"w":100000000000000000000.0001}\n'
        '{"amount":1000000000000000.02,"n":1,"w":100000000000000000000.0001}\n'
        '{"amount":1000000000000000.03,"n":1,"w":100000000000000000000.0001}\n'
        '{"amount":7,"n":2,"w":"1,2"}\n'
        '{"amount":-0.000000000000000000001,"n":3,"w":null}\n'
    ),
}


def _write_parquet(path: Path) -> None:
    """Typed columns: wide int64, a declared decimal, float, date, text."""
    import datetime

    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.table(
        {
            "event_ns": pa.array(WIDE, pa.int64()),
            "balance": pa.array(CENTS, pa.decimal128(16, 2)),
            "discount_percent": pa.array(
                [100.0, 0.0, 50.0, 101.5, 25.0, 75.0, None, 10.0], pa.float64()
            ),
            "day": pa.array(
                [datetime.date(2024, 1, d) for d in (1, 2, 3, 5, 8, 13, 21, 30)],
                pa.date32(),
            ),
            "code": pa.array(["A1", "A2", "A3", "B1", None, "B2", "B3", "C1"], pa.string()),
        }
    )
    pq.write_table(table, path, compression="none")


def write_inputs() -> None:
    CORPUS.mkdir(parents=True, exist_ok=True)
    for name, text in TEXT.items():
        (CORPUS / name).write_bytes(text.encode("utf-8"))
    _write_parquet(CORPUS / "typed.parquet")


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("version", help="the published dataprof release, e.g. 0.14.0")
    parser.add_argument("--python", default="3.13", help="interpreter for the isolated wheel run")
    args = parser.parse_args()

    write_inputs()
    subprocess.run(
        [
            "uv",
            "run",
            "--isolated",
            "--no-project",
            "--no-sources",
            "--python",
            args.python,
            "--with",
            f"dataprof=={args.version}",
            "python",
            "-I",
            "-c",
            CHILD,
            args.version,
            str(ROOT),
        ],
        check=True,
        cwd=CORPUS,
    )
    (CORPUS / "RELEASE").write_bytes(f"{args.version}\n".encode())
    return 0


if __name__ == "__main__":
    sys.exit(main())
