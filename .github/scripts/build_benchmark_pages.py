#!/usr/bin/env python3
"""Build the /benchmarks/ section of the dataprof site from Criterion output.

Usage:
    python scripts/build_benchmark_pages.py <benchmarks_dir>
    python scripts/build_benchmark_pages.py <benchmarks_dir> --placeholder

<benchmarks_dir> is a directory already containing the raw Criterion report
tree (the contents of target/criterion). The script adds:

  - index.html              a designed landing page for the section
  - benchmark-summary.json  machine-readable summary of the run
  - <group>/report/index.html redirect aliases for single-benchmark groups,
    so stable deep links keep working

With --placeholder it only writes an index.html explaining that no benchmark
data is available yet (used when the CI artifact has expired).
"""

from __future__ import annotations

import argparse
import html
import json
import sys
from collections import defaultdict
from pathlib import Path

GROUP_DESCRIPTIONS = {
    "csv_parsing": "Parser and facade throughput across file sizes",
    "throughput_metrics": "Rows and bytes per second on representative datasets",
    "full_analysis": "End-to-end profiling pipeline timings",
    "scaling_behavior": "Scaling behavior as dataset size increases",
    "large_scale": "Stress tests for larger benchmark datasets",
}

PAGE_SHELL = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{title}</title>
  <meta name="description"
        content="Criterion benchmark reports for dataprof, from the latest successful CI run.">
  <link rel="icon" type="image/png" sizes="32x32" href="../assets/favicon-32.png">
  <link rel="stylesheet" href="../style.css">
  <style>
    .page-head {{ padding: 56px 0 8px; }}
    .page-head h1 {{
      font-size: clamp(1.9rem, 3.6vw, 2.7rem); margin: 12px 0 14px; font-weight: 800;
    }}
    .page-head p {{ color: var(--muted); font-size: 1.06rem; max-width: 72ch; margin: 0 0 20px; }}
    .page-actions {{ display: flex; flex-wrap: wrap; gap: 12px; }}
    .stats {{
      display: grid; grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
      gap: 14px; margin-top: 34px;
    }}
    .stat-card {{ padding: 18px 18px 20px; }}
    .stat-label {{ color: var(--muted); font-size: 0.86rem; font-weight: 600; }}
    .stat-value {{ margin-top: 6px; font-size: 1.7rem; font-weight: 780; letter-spacing: -0.02em; }}
    .stat-subtle {{ color: var(--muted); font-size: 0.88rem; margin-top: 5px; }}
    .insights {{ margin-top: 26px; padding: 22px 26px; }}
    .insights h2 {{ margin: 0 0 10px; font-size: 1.15rem; }}
    .insights ul {{ margin: 0; padding-left: 1.2rem; color: var(--muted); line-height: 1.7; }}
    .groups {{ display: grid; gap: 18px; margin-top: 30px; }}
    .group-card {{ padding: 24px 26px; }}
    .group-header {{
      display: flex; justify-content: space-between; gap: 14px; align-items: start;
    }}
    .group-card h2 {{ margin: 8px 0; font-size: 1.4rem; }}
    .group-card h2 a {{ color: inherit; text-decoration: none; }}
    .group-card h2 a:hover {{ color: var(--accent-ink); }}
    .group-card > .group-header p {{ margin: 0; color: var(--muted); }}
    .group-preview {{
      margin-top: 16px; border-radius: 12px; overflow: hidden;
      border: 1px solid var(--line); background: #fff;
    }}
    .group-preview img {{ display: block; width: 100%; height: auto; }}
    .bench-table {{ overflow-x: auto; margin-top: 14px; }}
    .bench-table table {{ min-width: 480px; }}
    .empty-card {{ margin-top: 36px; padding: 40px; text-align: center; }}
    .empty-card p {{ color: var(--muted); max-width: 56ch; margin: 10px auto 0; }}
    @media (max-width: 700px) {{ .group-header {{ flex-direction: column; }} }}
  </style>
</head>
<body>

<header class="site-header">
  <div class="wrap">
    <a class="brand" href="../">
      <img src="../assets/logo-mark.png" alt="" width="30" height="30">
      dataprof
    </a>
    <nav class="site-nav" aria-label="Site">
      <a href="../#quickstart">Quickstart</a>
      <a href="../#quality">Quality model</a>
      <a href="./" aria-current="page">Benchmarks</a>
      <a class="nav-cta" href="https://github.com/AndreaBozzo/dataprof">GitHub&nbsp;&rarr;</a>
    </nav>
  </div>
</header>

<main class="wrap">
{body}
</main>

<footer class="site-footer">
  <div class="wrap">
    <span>dataprof &mdash; dual-licensed MIT or Apache-2.0</span>
    <span class="spacer"></span>
    <a href="../">Home</a>
    <a href="https://github.com/AndreaBozzo/dataprof">GitHub</a>
    <a href="https://github.com/AndreaBozzo/dataprof/actions/workflows/benchmarks.yml">
      Benchmark CI runs</a>
  </div>
</footer>

</body>
</html>
"""

PLACEHOLDER_BODY = """
  <section class="page-head">
    <span class="eyebrow">Continuous benchmarks</span>
    <h1>Benchmark reports</h1>
    <p>No benchmark data is published right now &mdash; the most recent CI artifact has expired.
      Reports will reappear automatically after the next benchmark run on <code>master</code>.</p>
  </section>
  <div class="card empty-card">
    <span class="eyebrow">Nothing here yet</span>
    <p>Benchmarks run on every push that touches the Rust crates. You can trigger or inspect runs
      in the <a href="https://github.com/AndreaBozzo/dataprof/actions/workflows/benchmarks.yml">
      Benchmarks workflow</a>.</p>
  </div>
"""


def pretty_group_name(name: str) -> str:
    return name.replace("_", " ").title()


def fmt_time(ms: float) -> str:
    if ms >= 1000:
        return f"{ms / 1000:.2f} s"
    return f"{ms:.2f} ms"


def fmt_throughput(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f} MiB/s"


def write_redirect(path: Path, target: str, title: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta http-equiv="refresh" content="0; url={target}">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>{html.escape(title)}</title>
  <link rel="canonical" href="{target}">
</head>
<body>
  <p>Redirecting to <a href="{target}">{html.escape(title)}</a>...</p>
</body>
</html>
""",
        encoding="utf-8",
    )


def add_group_aliases(pages: Path) -> None:
    """Criterion only creates <group>/report/index.html for grouped benchmarks.

    For single-benchmark groups, create a stable alias so deep links stay valid.
    """
    for group_dir in sorted(p for p in pages.iterdir() if p.is_dir() and p.name != "report"):
        report = group_dir / "report" / "index.html"
        if report.exists():
            continue
        child_reports = sorted(group_dir.glob("*/report/index.html")) + sorted(
            group_dir.glob("*/*/report/index.html")
        )
        if len(child_reports) == 1:
            rel = child_reports[0].relative_to(group_dir).as_posix()
            write_redirect(report, f"../{rel}", pretty_group_name(group_dir.name))


def collect_benchmarks(pages: Path) -> list[dict]:
    benchmarks = []
    for benchmark_json in pages.glob("**/new/benchmark.json"):
        estimates_json = benchmark_json.with_name("estimates.json")
        if not estimates_json.exists():
            continue

        meta = json.loads(benchmark_json.read_text())
        estimates = json.loads(estimates_json.read_text())
        mean_ns = estimates["mean"]["point_estimate"]
        mean_ms = mean_ns / 1_000_000.0
        throughput_bytes = (meta.get("throughput") or {}).get("Bytes")
        throughput_mib_s = None
        if throughput_bytes and mean_ns > 0:
            throughput_mib_s = throughput_bytes / (mean_ns / 1_000_000_000.0) / (1024 * 1024)

        directory_name = meta["directory_name"]
        report_dir = directory_name + "/report"
        benchmarks.append(
            {
                "group": meta["group_id"],
                "title": meta["title"],
                "function": meta.get("function_id"),
                "value": meta.get("value_str"),
                "directory": directory_name,
                "report": report_dir + "/index.html",
                "mean_ms": mean_ms,
                "throughput_mib_s": throughput_mib_s,
                "bytes": throughput_bytes,
                "preview": next(
                    (
                        candidate
                        for candidate in [
                            report_dir + "/pdf_small.svg",
                            report_dir + "/regression_small.svg",
                            report_dir + "/iteration_times_small.svg",
                        ]
                        if (pages / candidate).exists()
                    ),
                    None,
                ),
            }
        )
    return benchmarks


def build_groups(pages: Path, benchmarks: list[dict]) -> list[dict]:
    grouped = defaultdict(list)
    for benchmark in benchmarks:
        grouped[benchmark["group"]].append(benchmark)

    groups = []
    for group_name in sorted(grouped):
        items = sorted(
            grouped[group_name],
            key=lambda item: (item["function"] or "", item["value"] or "", item["title"]),
        )
        preview = None
        for candidate in [
            f"{group_name}/report/lines_throughput.svg",
            f"{group_name}/report/lines.svg",
            f"{group_name}/report/violin.svg",
        ]:
            if (pages / candidate).exists():
                preview = candidate
                break
        if preview is None and items:
            preview = items[0]["preview"]

        groups.append(
            {
                "name": group_name,
                "label": pretty_group_name(group_name),
                "description": GROUP_DESCRIPTIONS.get(group_name, "Criterion benchmark group"),
                "report": f"{group_name}/report/index.html",
                "preview": preview,
                "benchmarks": items,
            }
        )
    return groups


def build_observations(grouped: dict[str, list[dict]]) -> list[str]:
    observations = []
    csv_items = {item["value"]: item for item in grouped.get("csv_parsing", [])}
    if all(
        key in csv_items and csv_items[key]["throughput_mib_s"] is not None
        for key in ["tiny", "small", "medium"]
    ):
        tiny = csv_items["tiny"]["throughput_mib_s"]
        medium = csv_items["medium"]["throughput_mib_s"]
        observations.append(
            f"csv_parsing climbs from {tiny:.2f} MiB/s on tiny inputs to {medium:.2f} MiB/s on "
            "medium inputs, which suggests fixed overhead dominates the smallest case."
        )

    throughput_items = grouped.get("throughput_metrics", [])
    medium_parse = csv_items.get("medium")
    if (
        throughput_items
        and medium_parse
        and throughput_items[0]["throughput_mib_s"] is not None
        and medium_parse["throughput_mib_s"] is not None
        and throughput_items[0]["bytes"] == medium_parse["bytes"]
    ):
        delta = (
            abs(throughput_items[0]["throughput_mib_s"] - medium_parse["throughput_mib_s"])
            / medium_parse["throughput_mib_s"]
        )
        observations.append(
            f"throughput_metrics is currently within {delta * 100:.1f}% of "
            "csv_parsing/parse/medium on the same dataset size, so it adds confidence "
            "more than distinct coverage right now."
        )
    return observations


def render_index(
    pages: Path, benchmarks: list[dict], groups: list[dict], observations: list[str]
) -> str:
    fastest = max(
        (item for item in benchmarks if item["throughput_mib_s"] is not None),
        key=lambda item: item["throughput_mib_s"],
        default=None,
    )
    slowest = max(benchmarks, key=lambda item: item["mean_ms"])
    fastest_value = fmt_throughput(fastest["throughput_mib_s"]) if fastest else "n/a"
    fastest_label = html.escape(fastest["title"]) if fastest else "No throughput data"

    group_cards = []
    for group in groups:
        rows = []
        for item in group["benchmarks"]:
            label_bits = [bit for bit in [item["function"], item["value"]] if bit]
            label = " / ".join(label_bits) if label_bits else item["title"]
            rows.append(
                f"""<tr>
                  <td><a href="{item["report"]}">{html.escape(label)}</a></td>
                  <td>{fmt_time(item["mean_ms"])}</td>
                  <td>{fmt_throughput(item["throughput_mib_s"])}</td>
                </tr>"""
            )

        preview = ""
        if group["preview"]:
            preview = f"""<div class="group-preview">
              <a href="{group["report"]}"><img src="{group["preview"]}"
                alt="{html.escape(group["label"])} preview" loading="lazy"></a>
            </div>"""

        count = len(group["benchmarks"])
        group_cards.append(
            f"""<article class="card group-card" id="{group["name"]}">
              <div class="group-header">
                <div>
                  <span class="eyebrow">Benchmark group</span>
                  <h2><a href="{group["report"]}">{html.escape(group["label"])}</a></h2>
                  <p>{html.escape(group["description"])}</p>
                </div>
                <span class="pill">{count} benchmark{"s" if count != 1 else ""}</span>
              </div>
              {preview}
              <div class="bench-table">
                <table>
                  <thead><tr><th>Benchmark</th><th>Mean</th><th>Throughput</th></tr></thead>
                  <tbody>{"".join(rows)}</tbody>
                </table>
              </div>
            </article>"""
        )

    observation_items = "".join(f"<li>{html.escape(item)}</li>" for item in observations) or (
        "<li>No automatic observations were generated for this run yet.</li>"
    )

    full_index_link = ""
    if (pages / "report" / "index.html").exists():
        full_index_link = (
            '<a class="btn btn-primary" href="report/index.html">Open full Criterion index</a>'
        )

    body = f"""
  <section class="page-head">
    <span class="eyebrow">Continuous benchmarks</span>
    <h1>Benchmark reports, readable at a glance</h1>
    <p>Criterion artifacts from the latest successful run on <code>master</code>. This page is
      generated from live benchmark metadata, so links, metrics, and previews stay aligned with
      the actual report tree.</p>
    <div class="page-actions">
      {full_index_link}
      <a class="btn btn-ghost" href="benchmark-summary.json">Summary JSON</a>
    </div>
  </section>

  <section class="stats">
    <article class="card stat-card">
      <div class="stat-label">Benchmark groups</div>
      <div class="stat-value">{len(groups)}</div>
      <div class="stat-subtle">Top-level Criterion sections published</div>
    </article>
    <article class="card stat-card">
      <div class="stat-label">Benchmarks in this run</div>
      <div class="stat-value">{len(benchmarks)}</div>
      <div class="stat-subtle">Function/input combinations with live estimates</div>
    </article>
    <article class="card stat-card">
      <div class="stat-label">Fastest throughput</div>
      <div class="stat-value">{fastest_value}</div>
      <div class="stat-subtle">{fastest_label}</div>
    </article>
    <article class="card stat-card">
      <div class="stat-label">Slowest mean time</div>
      <div class="stat-value">{fmt_time(slowest["mean_ms"])}</div>
      <div class="stat-subtle">{html.escape(slowest["title"])}</div>
    </article>
  </section>

  <section class="card insights">
    <h2>Current read of this run</h2>
    <ul>{observation_items}</ul>
  </section>

  <section class="groups">{"".join(group_cards)}</section>
"""
    return PAGE_SHELL.format(title="dataprof benchmarks", body=body)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("benchmarks_dir", type=Path)
    parser.add_argument(
        "--placeholder",
        action="store_true",
        help="write a placeholder index instead of reading Criterion data",
    )
    args = parser.parse_args()

    pages: Path = args.benchmarks_dir

    if args.placeholder:
        pages.mkdir(parents=True, exist_ok=True)
        (pages / "index.html").write_text(
            PAGE_SHELL.format(title="dataprof benchmarks", body=PLACEHOLDER_BODY),
            encoding="utf-8",
        )
        print(f"Wrote placeholder index to {pages / 'index.html'}")
        return 0

    if not pages.is_dir():
        print(f"Error: {pages} is not a directory", file=sys.stderr)
        return 1

    add_group_aliases(pages)

    benchmarks = collect_benchmarks(pages)
    if not benchmarks:
        print("Error: no Criterion benchmark metadata found", file=sys.stderr)
        return 1

    groups = build_groups(pages, benchmarks)

    grouped = defaultdict(list)
    for benchmark in benchmarks:
        grouped[benchmark["group"]].append(benchmark)
    observations = build_observations(grouped)

    fastest = max(
        (item for item in benchmarks if item["throughput_mib_s"] is not None),
        key=lambda item: item["throughput_mib_s"],
        default=None,
    )
    slowest = max(benchmarks, key=lambda item: item["mean_ms"])
    summary = {
        "group_count": len(groups),
        "benchmark_count": len(benchmarks),
        "fastest": fastest,
        "slowest": slowest,
        "groups": groups,
        "observations": observations,
    }
    (pages / "benchmark-summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    (pages / "index.html").write_text(
        render_index(pages, benchmarks, groups, observations), encoding="utf-8"
    )
    print(f"Wrote {pages / 'index.html'} ({len(benchmarks)} benchmarks, {len(groups)} groups)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
