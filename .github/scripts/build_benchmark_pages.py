#!/usr/bin/env python3
"""Build /benchmarks/ from the shared benchmark artifact (see benches/README.md).

Usage:
    python .github/scripts/build_benchmark_pages.py <benchmarks_dir>
    python .github/scripts/build_benchmark_pages.py <benchmarks_dir> --placeholder

<benchmarks_dir> is a directory already containing the raw Criterion report
tree (the contents of target/criterion). The script adds:

  - index.html              a designed landing page for the section
  - benchmark-summary.json  machine-readable summary of the run
  - <group>/report/index.html redirect aliases for single-benchmark groups,
    so stable deep links keep working

When comparison/results.json is present, the same landing page also presents
the Python comparison suite. Older Criterion-only artifacts remain supported.

With --placeholder it only writes an index.html explaining that no benchmark
data is available yet (used when the CI artifact has expired).
"""

from __future__ import annotations

import argparse
import html
import json
import math
import sys
from collections import defaultdict
from pathlib import Path

GROUP_DESCRIPTIONS = {
    "csv_parsing": "CSV scan and column profiling across file sizes",
    "throughput_metrics": "Fast row-count estimation across input sizes",
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
        content="Benchmark suites for dataprof, from the latest successful CI run.">
  <link rel="icon" type="image/png" sizes="32x32" href="../assets/favicon-32.png">
  <link rel="stylesheet" href="../style.css">
  <link rel="stylesheet" href="../benchmarks.css">
  <script src="../benchmarks.js" defer></script>
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
    return name.replace("_", " ").title().replace("Csv", "CSV")


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
        throughput_elements = (meta.get("throughput") or {}).get("Elements")
        confidence = estimates["mean"].get("confidence_interval")
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
                "throughput_rows_s": (
                    throughput_elements / (mean_ns / 1_000_000_000.0)
                    if throughput_elements and mean_ns > 0
                    else None
                ),
                "mean_confidence_interval": (
                    {
                        "level": confidence["confidence_level"],
                        "lower_ms": confidence["lower_bound"] / 1_000_000,
                        "upper_ms": confidence["upper_bound"] / 1_000_000,
                    }
                    if confidence
                    else None
                ),
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
    size_order = {"tiny": 0, "small": 1, "medium": 2, "large": 3}
    for group_name in sorted(grouped):
        items = sorted(
            grouped[group_name],
            key=lambda item: (
                item["function"] or "",
                size_order.get(item["value"], 4),
                item["value"] or "",
                item["title"],
            ),
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
            "medium inputs. Compare the full distributions before attributing the difference."
        )

    return observations


def load_comparison(pages: Path) -> dict | None:
    # Failed artifacts are retained for diagnosis, never treated as old artifacts
    # that merely lack a comparison suite.
    for status_path in (pages / "run-status.json", pages / "comparison" / "progress.json"):
        if status_path.exists():
            status = json.loads(status_path.read_text(encoding="utf-8"))
            if status.get("status") != "complete":
                raise ValueError(f"incomplete benchmark artifact: {status_path.name}")
    path = pages / "comparison" / "results.json"
    if not path.exists():
        return None
    document = json.loads(path.read_text(encoding="utf-8"))
    if document.get("status", "complete") != "complete":
        raise ValueError("incomplete comparison results")
    if document["schema_version"] != 1:
        raise ValueError("unsupported comparison result schema")
    if not document["results"]:
        raise ValueError("comparison contains no results")
    if document["results"].keys() != document["config"]["workloads"].keys():
        raise ValueError("comparison is missing requested tools")
    for tool, modes in document["results"].items():
        if tool not in document["config"]["workloads"]:
            raise ValueError(f"comparison has no workload description for {tool}")
        for mode in ("cold", "warm"):
            cell = modes[mode]
            for key in ("median_seconds", "iqr_seconds", "q1_seconds", "q3_seconds"):
                if not math.isfinite(cell[key]) or cell[key] < 0:
                    raise ValueError(f"invalid comparison timing: {tool}.{mode}.{key}")
            if not cell["q1_seconds"] <= cell["median_seconds"] <= cell["q3_seconds"]:
                raise ValueError(f"invalid comparison quartiles: {tool}.{mode}")
            if len(cell["samples_seconds"]) != document["config"]["iterations"] * document[
                "config"
            ].get("blocks", 1):
                raise ValueError(f"comparison sample count mismatch: {tool}.{mode}")
    return document


def render_timing_chart(document: dict, mode: str) -> str:
    maximum = max(cell[mode]["q3_seconds"] for cell in document["results"].values())
    scale = maximum * 1.08 or 1
    rows = []
    for tool, modes in document["results"].items():
        cell = modes[mode]
        lower, upper, median = (cell[key] for key in ("q1_seconds", "q3_seconds", "median_seconds"))
        accent = " is-dataprof" if tool == "dataprof" else ""
        rows.append(f"""<div class="timing-row{accent}">
          <span class="timing-name">{html.escape(tool)}</span>
          <div class="timing-track" aria-hidden="true">
            <span class="timing-iqr"
              style="left:{lower / scale * 100:.4f}%;
                width:{(upper - lower) / scale * 100:.4f}%"></span>
            <span class="timing-median" style="left:{median / scale * 100:.4f}%"></span>
          </div>
          <span class="timing-value">{fmt_time(median * 1000)}</span>
        </div>""")
    description = (
        "Repeated CSV reads and fresh summaries after warmup. Adapter import/setup excluded."
        if mode == "warm"
        else "A fresh process per sample. Startup, imports, operation and exit included."
    )
    return f"""<div class="timing-panel" id="timing-{mode}" role="tabpanel"
      aria-labelledby="tab-{mode}">
      <h3>{"Warm operations" if mode == "warm" else "Fresh-process operations"}</h3>
      <p>{description}</p>{"".join(rows)}
      <div class="chart-legend"><span>● Median &nbsp; ▰ Middle 50% of samples (IQR)</span>
        <span>Linear scale from 0 to {fmt_time(scale * 1000)} · Lower is faster</span></div>
    </div>"""


def render_comparison(document: dict | None) -> str:
    if document is None:
        return ""
    rows = []
    for tool, modes in document["results"].items():
        cells = "".join(
            f"<td>{modes[mode]['median_seconds']:.6f} [{modes[mode]['iqr_seconds']:.6f}]</td>"
            for mode in ("cold", "warm")
        )
        workload = html.escape(document["config"]["workloads"][tool])
        rows.append(f"<tr><td>{html.escape(tool)}</td>{cells}<td>{workload}</td></tr>")
    config = document["config"]
    environment = document["environment"]
    blocks = config.get("blocks", 1)
    sample_count = config["iterations"] * blocks
    details = {
        "Measured at": document["created_at"],
        "Operating system": environment["os"],
        "Processor": environment["cpu"],
        "Samples / warmups per block": f"{config['iterations']} / {config['warmups']}",
        "Process blocks": str(blocks),
        "Experiment mode": config.get("mode", "routine"),
        "Declared host controls": config.get("host_description") or "Not declared",
        "Library cache": config.get("library_cache", "Not controlled / not recorded"),
        "Import preflight": config.get("preflight", "Not recorded"),
        "Thread request": str(config["threads"]),
        "Cold file cache": config["cold_cache"] + " (residency / eviction unverified)",
        "Fixture SHA-256": document["fixture"]["sha256"],
        "Checkout": environment["git_commit"],
        "Working tree": "modified" if environment["git_status"] else "clean",
        "Installed tools": ", ".join(
            f"{tool} {environment['versions'][tool]}" for tool in document["results"]
        ),
    }
    provenance = "".join(
        f"<dt>{html.escape(key)}</dt><dd>{html.escape(value)}</dd>"
        for key, value in details.items()
    )
    return f"""
  <section id="comparison">
    <div class="section-head"><div><span class="eyebrow">01 / Python tool comparison</span>
      <h2>One input. Explicit workloads.</h2>
      <p>File-to-summary timings for {document["fixture"]["expected"]["rows"]:,} rows of
        numeric, text and null data. Every sample checks row counts, column order and nulls.</p>
    </div><span class="pill">{sample_count} samples per condition · {blocks} process blocks</span>
    </div>
    <div class="card comparison-card">
    <div class="chart-controls">
      <div class="mode-switch" role="tablist" aria-label="Timing condition">
        <button id="tab-warm" role="tab" aria-controls="timing-warm"
          aria-selected="true" data-timing-mode="warm">Warm operation</button>
        <button id="tab-cold" role="tab" aria-controls="timing-cold"
          aria-selected="false" data-timing-mode="cold">Fresh process</button>
      </div><span class="chart-unit">Wall time · median with interquartile range</span>
    </div>
    {render_timing_chart(document, "warm")}
    {render_timing_chart(document, "cold")}
    <div class="scope-note"><strong>Read the workload before the ratio.</strong>
      Each tool computes a different summary; metric equivalence is not established.
      Fresh process does not imply cold storage. These measurements describe this run,
      not a universal ranking. IQR shows variation, not a confidence interval.</div>
    <p><strong>Diagnostic evidence — no established baseline.</strong>
      Publication mode increases the experiment budget; it does not certify precision.
      Warm samples share a process within each block. Separate blocks use fresh processes,
      but share host and OS caches. Within-run intervals do not demonstrate across-run stability;
      repeat-run IQR overlap is a diagnostic, not a significance test.</p>
    {render_ordered_samples(document)}
    <details class="evidence-details"><summary>Exact workloads and timing table</summary>
    <div class="bench-table"><table><caption>Seconds: median [IQR]</caption>
      <thead><tr><th>Tool</th><th>Cold median [IQR]</th><th>Warm median [IQR]</th>
        <th>Workload</th></tr></thead>
      <tbody>{"".join(rows)}</tbody>
    </table></div></details>
    <details class="evidence-details"><summary>Machine, versions and fixture identity</summary>
      <dl>{provenance}</dl></details>
    <div class="download-links"><a href="comparison/results.json">Download raw evidence ↗</a>
      <a href="comparison/comparison.md">Reference ratios &amp; table ↗</a>
      <a href="#reproduce">Reproduce this run ↓</a></div>
    </div>
  </section>
"""


def render_ordered_samples(document: dict) -> str:
    """Expose actual measurement order and boundaries, including first use and controls."""

    def seconds(value):
        return "Not recorded" if value is None else f"{value:.6f}"

    rows = []
    for index, run in enumerate(document.get("runs", []), 1):
        values = [
            str(index),
            str(run.get("block", 1)),
            run["tool"],
            run["mode"],
            run.get("invocation", "Not recorded"),
            seconds(run.get("process_seconds")),
            seconds(run.get("import_setup_seconds")),
            seconds(run.get("first_operation_seconds")),
            ", ".join(seconds(value) for value in run["operation_seconds"]),
        ]
        rows.append(
            "<tr>" + "".join(f"<td>{html.escape(value)}</td>" for value in values) + "</tr>"
        )
    if not rows:
        # Older artifacts lack boundary metadata; preserve their ordered observations.
        for tool, modes in document["results"].items():
            for mode, cell in modes.items():
                values = ", ".join(seconds(value) for value in cell["samples_seconds"])
                rows.append(
                    f'<tr><td colspan="9">{html.escape(tool)} / {html.escape(mode)}: '
                    f"{values}</td></tr>"
                )
    controls = (
        "; ".join(
            f"block {run['block']}, round {run['iteration']}: {seconds(run['process_seconds'])}"
            for run in document.get("controls", [])
        )
        or "Not recorded"
    )
    preflight = (
        "; ".join(
            f"{run['tool']}: {seconds(run.get('import_setup_seconds'))}"
            for run in document.get("preflight", [])
        )
        or "Not recorded"
    )
    return f"""<details class="evidence-details">
      <summary>Ordered samples and timing boundaries</summary>
      <p>Seconds in execution order; no first sample is discarded. Process time includes
        interpreter startup, harness imports, adapter import/setup, operations, validation,
        garbage collection, IPC and exit. Import/setup is timed directly around the adapter;
        deferred imports remain inside operations. First operation includes initialization and,
        for warm workers, is the first warmup. Warmup samples are retained in the raw JSON.</p>
      <p>First adapter import after environment preparation (preflight): {html.escape(preflight)}.
        Earlier installs or workflow preflights may have warmed library pages;
        this is not disk-cold.</p>
      <div class="bench-table"><table><caption>Ordered worker observations (seconds)</caption>
        <thead><tr><th>Order</th><th>Block</th><th>Tool</th><th>Mode</th><th>Invocation</th>
          <th>Process</th><th>Import/setup</th><th>First operation</th><th>Measured operations</th>
        </tr></thead><tbody>{"".join(rows)}</tbody></table></div>
      <p>Minimal-worker controls, before each round (seconds): {html.escape(controls)}.</p>
      <p>Control scope: interpreter, minimal JSON worker, IPC and exit, without harness or tool
        imports. It is context for combined startup costs, not a subtractable import estimate.</p>
    </details>"""


def render_index(
    pages: Path,
    benchmarks: list[dict],
    groups: list[dict],
    observations: list[str],
    comparison: dict | None = None,
) -> str:
    group_cards = []
    for group in groups:
        rows = []
        for item in group["benchmarks"]:
            label_bits = [bit for bit in [item["function"], item["value"]] if bit]
            label = " / ".join(label_bits) if label_bits else item["title"]
            confidence = item["mean_confidence_interval"]
            interval = (
                f"{confidence['level']:.0%}: {fmt_time(confidence['lower_ms'])}–"
                f"{fmt_time(confidence['upper_ms'])}"
                if confidence
                else "Not recorded"
            )
            throughput = (
                f"{item['throughput_rows_s']:,.0f} rows/s"
                if item["throughput_rows_s"] is not None
                else fmt_throughput(item["throughput_mib_s"])
            )
            rows.append(
                f"""<tr>
                  <td><a href="{item["report"]}">{html.escape(label)}</a></td>
                  <td>{fmt_time(item["mean_ms"])}</td>
                  <td>{interval}</td>
                  <td>{throughput}</td>
                </tr>"""
            )

        preview = ""
        if group["preview"]:
            preview = f"""<div class="group-preview">
              <a href="{group["report"]}"><img src="{group["preview"]}"
                alt="{html.escape(group["label"])} preview" loading="lazy"></a>
            </div>"""

        count = len(group["benchmarks"])
        search_text = html.escape((group["name"] + " " + group["description"]).lower(), quote=True)
        group_cards.append(
            f"""<article class="card group-card" id="{group["name"]}"
              data-scenario="{search_text}">
              <div class="group-header">
                <div>
                  <span class="eyebrow">Criterion scenario</span>
                  <h2><a href="{group["report"]}">{html.escape(group["label"])}</a></h2>
                  <p>{html.escape(group["description"])}</p>
                </div>
                <span class="pill">{count} benchmark{"s" if count != 1 else ""}</span>
              </div>
              {preview}
              <div class="bench-table">
                <table>
                  <thead><tr><th>Benchmark</th><th>Mean</th>
                    <th>Confidence interval</th><th>Throughput</th></tr></thead>
                  <tbody>{"".join(rows)}</tbody>
                </table>
              </div>
            </article>"""
        )

    observation_items = "".join(f"<li>{html.escape(item)}</li>" for item in observations)
    observation_section = (
        f'<details class="card insights"><summary>Observations within the Rust suite</summary>'
        f"<ul>{observation_items}</ul></details>"
        if observations
        else ""
    )

    full_index_link = ""
    if (pages / "report" / "index.html").exists():
        full_index_link = '<a href="report/index.html">Full Criterion index ↗</a>'

    comparison_nav = '<a href="#comparison">Tool comparison</a>' if comparison else ""
    evidence = (
        f"<dt>Comparison run</dt><dd>{html.escape(comparison['created_at'][:10])}</dd>"
        f"<dt>Fixture</dt><dd>{comparison['fixture']['expected']['rows']:,} rows · 3 columns</dd>"
        f"<dt>Repetitions</dt><dd>{comparison['config']['iterations']} per block × "
        f"{comparison['config'].get('blocks', 1)} process blocks</dd>"
        f"<dt>Tool versions</dt><dd>{len(comparison['results'])} pinned tools</dd>"
        if comparison
        else "<dt>Comparison suite</dt><dd>Not included in this artifact</dd>"
    )

    body = f"""
  <section class="bench-hero">
    <div><span class="eyebrow">dataprof / performance lab</span>
    <h1>See how dataprof performs.</h1>
    <p class="lede">From a CSV scan to a complete profile. Explore measured workloads,
      inspect their variation, and run the same experiments on your own data infrastructure.</p>
    <div class="page-actions">
      <a class="btn btn-primary" href="{"#comparison" if comparison else "#rust-scenarios"}">
        Explore the results ↓</a>
      <a class="btn btn-ghost" href="benchmark-summary.json">Summary JSON</a>
    </div></div>
    <aside class="evidence-card" aria-label="Published evidence">
      <span class="eyebrow">Open measurements</span><h2>The evidence travels with the result.</h2>
      <dl><dt>Rust coverage</dt><dd>{len(benchmarks)} cases / {len(groups)} scenario groups</dd>
        {evidence}<dt>Source &amp; protocol</dt><dd>
          <a href="https://github.com/AndreaBozzo/dataprof/blob/master/benches/README.md">
            Inspect the benchmark suite ↗</a></dd>
      </dl>
    </aside>
  </section>
  <nav class="suite-nav" aria-label="Benchmark sections">
    {comparison_nav}<a href="#rust-scenarios">Rust scenarios</a>
    <a href="#methodology">Methodology</a><a href="#reproduce">Reproduce</a>{full_index_link}
  </nav>
  {render_comparison(comparison)}
  <section id="rust-scenarios">
    <div class="section-head"><div><span class="eyebrow">
      {"02" if comparison else "01"} / Rust profiling</span>
      <h2>Explore the pipeline.</h2>
      <p>Repeated in-process measurements, from scan and column profiling to full report assembly.
        Each case links to its Criterion distributions and estimates.</p></div>
      <div class="filter-control"><label for="scenario-search">Find a scenario</label>
        <input id="scenario-search" type="search" placeholder="Try CSV or scaling"></div>
    </div>
    <p id="scenario-count" class="sr-only" aria-live="polite">
      {len(groups)} scenario groups shown</p>
    <p class="section-note">Only scenarios measured in this artifact appear here.
      Routine CI runs the CSV and full-analysis groups; full runs include row counting,
      scaling and larger inputs. Confidence intervals below describe estimated means.</p>
    <div class="groups">{"".join(group_cards)}</div>{observation_section}
  </section>
  <section class="repro-section">
    <article class="card" id="methodology"><span class="eyebrow">Read the evidence</span>
      <h2>A number needs its context.</h2>
      <ul class="protocol-list">
        <li><strong>Different measurement boundaries.</strong> Criterion measures Rust operations;
          Python comparisons also cover file reads and, in fresh processes, imports and startup.
        </li>
        <li><strong>Variation stays visible.</strong> Python shows median and IQR. Criterion shows
          mean confidence intervals and links to its full distributions.</li>
        <li><strong>Shared CI is a signal.</strong> Runner load and hardware vary. Confirm changes
          on an idle, controlled host before making performance claims.</li>
        <li><strong>Scope stays explicit.</strong> Tool summaries differ. Input-integrity checks
          do not establish complete cross-tool metric parity.</li>
      </ul>
    </article>
    <article class="card" id="reproduce"><span class="eyebrow">Run it yourself</span>
      <h2>From checkout to evidence.</h2>
      <p>Use the repository's Rust toolchain and uv. The comparison environment is locked
        and separate from the dependency-free Python wheel.</p>
      <pre><code># Existing Rust scenarios
cargo bench --bench benchmarks

# Four-tool comparison (from repository root)
uv run --project benches --locked \\
  --reinstall-package dataprof python \\
  .github/scripts/benchmark_comparison.py</code></pre>
      <a href="https://github.com/AndreaBozzo/dataprof/blob/master/benches/README.md">
        Full protocol, cache controls &amp; repeatability checks ↗</a>
    </article>
  </section>
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
    comparison = load_comparison(pages)

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
    if comparison is not None:
        summary["comparison"] = comparison
    (pages / "benchmark-summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

    (pages / "index.html").write_text(
        render_index(pages, benchmarks, groups, observations, comparison), encoding="utf-8"
    )
    print(f"Wrote {pages / 'index.html'} ({len(benchmarks)} benchmarks, {len(groups)} groups)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
