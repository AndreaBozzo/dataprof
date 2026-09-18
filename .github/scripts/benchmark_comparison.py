"""Repeated end-to-end CSV workloads; see benches/README.md for the protocol."""

from __future__ import annotations

import argparse
import contextlib
import csv
import gc
import hashlib
import importlib.metadata
import json
import math
import os
import platform
import random
import statistics
import subprocess
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
TOOLS = ("dataprof", "pandas", "polars", "ydata-profiling")
THREAD_ENV = (
    "OMP_NUM_THREADS",
    "OPENBLAS_NUM_THREADS",
    "MKL_NUM_THREADS",
    "NUMEXPR_NUM_THREADS",
    "POLARS_MAX_THREADS",
    "RAYON_NUM_THREADS",
)
WORKLOADS = {
    "dataprof": 'profile(engine="auto", metrics=["schema", "statistics"])',
    "pandas": 'read_csv + describe(include="all") + null counts',
    "polars": "read_csv + describe + null counts",
    "ydata-profiling": "pandas.read_csv + ProfileReport(minimal=True).description_set",
}
PREFLIGHT_POLICY = (
    "imports only, in disposable workers before measurements; no fixture operations; "
    "may warm OS library pages and on-disk library caches, which are not evicted; "
    "fresh-process samples are subsequent to preflight, not first host invocations"
)


def checkpoint(output: Path, document: dict) -> None:
    """Atomically retain completed observations without publishing a comparison."""
    temporary = output / "progress.json.tmp"
    temporary.write_text(json.dumps(document, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(output / "progress.json")


class WorkerError(RuntimeError):
    def __init__(self, message: str, *, stdout="", stderr="", returncode=None):
        super().__init__(message)
        # TimeoutExpired may carry bytes even when subprocess.run uses text=True.
        self.diagnostics = {
            "stdout": stdout.decode("utf-8", errors="replace")
            if isinstance(stdout, bytes)
            else stdout,
            "stderr": stderr.decode("utf-8", errors="replace")
            if isinstance(stderr, bytes)
            else stderr,
            "returncode": returncode,
        }


def fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def make_fixture(path: Path, rows: int) -> dict:
    """Versioned, deterministic UTF-8 CSV with numeric, string and null data."""
    with path.open("w", encoding="utf-8", newline="") as stream:
        writer = csv.writer(stream, lineterminator="\n")
        writer.writerow(["id", "amount", "category"])
        for i in range(rows):
            writer.writerow([i, "" if i % 11 == 0 else (i % 2001) - 1000, f"group_{i % 7}_東京"])
    return {
        "generator": "mixed-csv-v1",
        "path": str(path),
        "sha256": fingerprint(path),
        "bytes": path.stat().st_size,
        "expected": {
            "rows": rows,
            "columns": ["id", "amount", "category"],
            "null_counts": [0, (rows + 10) // 11, 0],
        },
    }


def operation(tool: str, path: Path, threads: int):
    """Import once; return a callable that constructs a fresh summary each time."""
    if tool == "dataprof":
        import dataprof

        def run():
            report = dataprof.profile(path, engine="auto", metrics=["schema", "statistics"])
            return {
                "rows": report.rows,
                "columns": [column.name for column in report.profiles],
                "null_counts": [column.null_count for column in report.profiles],
            }

    elif tool == "pandas":
        import pandas as pd

        def run():
            frame = pd.read_csv(path)
            frame.describe(include="all")
            return {
                "rows": len(frame),
                "columns": list(frame.columns),
                "null_counts": [int(n) for n in frame.isna().sum()],
            }

    elif tool == "polars":
        import polars as pl

        def run():
            frame = pl.read_csv(path)
            frame.describe()
            return {
                "rows": frame.height,
                "columns": frame.columns,
                "null_counts": list(frame.null_count().row(0)),
            }

    elif tool == "ydata-profiling":
        import pandas as pd
        from ydata_profiling import ProfileReport

        def run():
            report = ProfileReport(
                pd.read_csv(path), minimal=True, progress_bar=False, pool_size=threads
            ).description_set
            return {
                "rows": int(report.table["n"]),
                "columns": list(report.variables),
                "null_counts": [int(value["n_missing"]) for value in report.variables.values()],
            }

    else:
        raise ValueError(f"unknown tool: {tool}")
    return run


def worker(request: dict) -> dict:
    run = operation(request["tool"], Path(request["path"]), request["threads"])
    if request.get("preflight"):
        return {"pid": os.getpid(), "status": "complete"}
    for _ in range(request["warmups"]):
        observed = run()
        if observed != request["expected"]:
            raise ValueError(f"fixture mismatch during warmup: {observed!r}")
    samples = []
    for _ in range(request["iterations"]):
        gc.collect()  # Outside the operation timer; automatic GC remains enabled.
        start = time.perf_counter_ns()
        observed = run()
        seconds = (time.perf_counter_ns() - start) / 1e9
        if observed != request["expected"]:
            raise ValueError(
                f"fixture mismatch: expected {request['expected']!r}, got {observed!r}"
            )
        samples.append(seconds)
    return {"pid": os.getpid(), "operation_seconds": samples, "observed": observed}


def prime_file(path: Path) -> None:
    with path.open("rb") as stream:
        while stream.read(1024 * 1024):
            pass


def prepare_cache(path: Path, mode: str) -> str:
    if mode == "warm":
        prime_file(path)
        return "fixture read immediately before worker; residency unverified"
    if not hasattr(os, "posix_fadvise") or not hasattr(os, "POSIX_FADV_DONTNEED"):
        raise RuntimeError("--cold-cache evict requires POSIX_FADV_DONTNEED on this host")
    with path.open("rb") as stream:
        os.fsync(stream.fileno())
        os.posix_fadvise(stream.fileno(), 0, 0, os.POSIX_FADV_DONTNEED)
    return "file eviction requested with POSIX_FADV_DONTNEED; eviction unverified"


def run_worker(request: dict, timeout: float) -> dict:
    env = {**os.environ, **dict.fromkeys(THREAD_ENV, str(request["threads"]))}
    env["PYTHONHASHSEED"] = "0"
    env["PYTHONIOENCODING"] = "utf-8"
    start = time.perf_counter_ns()
    try:
        completed = subprocess.run(
            [sys.executable, str(Path(__file__).resolve()), "--worker"],
            input=json.dumps(request),
            text=True,
            encoding="utf-8",
            capture_output=True,
            env=env,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        raise WorkerError(
            f"{request['tool']} worker timed out after {timeout}s",
            stdout=exc.stdout,
            stderr=exc.stderr,
        ) from exc
    elapsed = (time.perf_counter_ns() - start) / 1e9
    if completed.returncode:
        raise WorkerError(
            f"{request['tool']} worker failed:\n{completed.stderr}",
            stdout=completed.stdout,
            stderr=completed.stderr,
            returncode=completed.returncode,
        )
    try:
        result = json.loads(completed.stdout)
        if not isinstance(result, dict):
            raise ValueError("worker response must be an object")
    except ValueError as exc:
        raise WorkerError(
            f"{request['tool']} worker returned invalid JSON: {exc}",
            stdout=completed.stdout,
            stderr=completed.stderr,
            returncode=completed.returncode,
        ) from exc
    result["process_seconds"] = elapsed
    result["stderr"] = completed.stderr
    return result


def summarize(samples: list[float]) -> dict:
    if len(samples) < 2:
        raise ValueError("at least two samples are required")
    q1, _, q3 = statistics.quantiles(samples, n=4, method="inclusive")
    return {
        "samples_seconds": samples,
        "median_seconds": statistics.median(samples),
        "q1_seconds": q1,
        "q3_seconds": q3,
        "iqr_seconds": q3 - q1,
        "min_seconds": min(samples),
        "max_seconds": max(samples),
    }


def relative_time(reference: float, measured: float) -> str:
    ratio = reference / measured
    if ratio >= 1:
        return f"{ratio:.2f}x speedup"
    return f"{1 / ratio:.2f}x slowdown"


def render_table(document: dict) -> str:
    reference = document["config"]["reference"]
    cells = document["results"]
    lines = [
        f"Reference: {reference}. Different workloads; metric equivalence is not established.",
        "Cold = fresh process (startup/import/exit included); warm = operation after warmup.",
        "Times in seconds: median [IQR]. Cache treatment is recorded in results.json.",
        "",
        "| Tool | Cold median [IQR] | Warm median [IQR] | Cold vs reference | Warm vs reference |",
        "| --- | ---: | ---: | --- | --- |",
    ]
    for tool, modes in cells.items():
        values = [
            f"{modes[mode]['median_seconds']:.6f} [{modes[mode]['iqr_seconds']:.6f}]"
            for mode in ("cold", "warm")
        ]
        ratios = [
            relative_time(cells[reference][mode]["median_seconds"], modes[mode]["median_seconds"])
            for mode in ("cold", "warm")
        ]
        lines.append(f"| {tool} | {' | '.join(values + ratios)} |")
    if "repeatability" in document:
        lines.extend(["", "Repeat-run IQR overlap (diagnostic, not a confidence interval):"])
        for tool, modes in document["repeatability"].items():
            lines.append(
                f"- {tool}: "
                + ", ".join(
                    f"{mode} {'overlaps' if overlaps else 'DISJOINT — rerun on an idle host'}"
                    for mode, overlaps in modes.items()
                )
            )
    return "\n".join(lines) + "\n"


def compare_runs(previous: dict, current: dict) -> dict:
    """Refuse mismatched experiments before reporting repeat-run dispersion."""
    if previous.get("status", "complete") != "complete":
        raise ValueError("repeatability comparison requires a complete previous run")
    if previous["fixture"]["sha256"] != current["fixture"]["sha256"]:
        raise ValueError("repeatability comparison requires the same fixture")
    if previous["config"] != current["config"]:
        raise ValueError("repeatability comparison requires the same benchmark configuration")
    for key in (
        "python",
        "os",
        "architecture",
        "cpu",
        "hostname",
        "logical_cpus",
        "ram_bytes",
        "versions",
        "git_commit",
        "git_status",
        "native_extensions",
        "benchmark_script_sha256",
        "benchmark_lock_sha256",
        "cargo_lock_sha256",
        "build_environment",
    ):
        if previous["environment"][key] != current["environment"][key]:
            raise ValueError(f"repeatability comparison has different environment.{key}")
    return {
        tool: {
            mode: max(cell["q1_seconds"], previous["results"][tool][mode]["q1_seconds"])
            <= min(cell["q3_seconds"], previous["results"][tool][mode]["q3_seconds"])
            for mode, cell in modes.items()
        }
        for tool, modes in current["results"].items()
    }


def git_output(*args: str) -> str:
    return subprocess.check_output(["git", *args], cwd=ROOT, text=True).strip()


def environment_metadata() -> dict:
    import psutil
    import tomllib

    versions = dict(
        sorted((dist.metadata["Name"], dist.version) for dist in importlib.metadata.distributions())
    )
    manifest = tomllib.loads((ROOT / "benches/pyproject.toml").read_text(encoding="utf-8"))
    for requirement in manifest["project"]["dependencies"]:
        if "==" in requirement:
            name, expected = requirement.split("==")
            if importlib.metadata.version(name) != expected:
                raise ValueError(
                    f"{name} must match benchmark pin {expected}; use --project benches"
                )
    expected_python = (ROOT / "benches/.python-version").read_text().strip()
    if platform.python_version() != expected_python:
        raise ValueError(f"benchmark Python must be {expected_python}; use --project benches")
    # Record installed distributions, including transitive libraries, in addition
    # to the committed lockfile. Hash the actual native extension being timed.
    distribution = importlib.metadata.distribution("dataprof")
    native = [
        distribution.locate_file(entry)
        for entry in distribution.files or []
        if str(entry).endswith((".pyd", ".so"))
    ]
    if not native:
        raise RuntimeError("cannot fingerprint the installed dataprof extension")
    cpu = platform.processor()
    if not cpu and Path("/proc/cpuinfo").exists():
        cpu = next(
            (
                line.split(":", 1)[1].strip()
                for line in Path("/proc/cpuinfo").read_text().splitlines()
                if line.startswith("model name")
            ),
            "unknown",
        )
    return {
        "python": sys.version,
        "executable": sys.executable,
        "os": platform.platform(),
        "hostname": platform.node(),
        "architecture": platform.machine(),
        "cpu": cpu or "unknown",
        "logical_cpus": psutil.cpu_count(),
        "physical_cpus": psutil.cpu_count(logical=False),
        "ram_bytes": psutil.virtual_memory().total,
        "versions": versions,
        "native_extensions": {str(path): fingerprint(path) for path in native},
        "git_commit": git_output("rev-parse", "HEAD"),
        "git_status": git_output("status", "--porcelain"),
        "benchmark_script_sha256": fingerprint(Path(__file__)),
        "benchmark_lock_sha256": fingerprint(ROOT / "benches/uv.lock"),
        "cargo_lock_sha256": fingerprint(ROOT / "Cargo.lock"),
        "rustc_available": subprocess.check_output(["rustc", "--version"], text=True).strip(),
        "build_environment": {
            key: os.environ.get(key)
            for key in (
                "RUSTFLAGS",
                "CARGO_ENCODED_RUSTFLAGS",
                "RUSTUP_TOOLCHAIN",
                "CARGO_BUILD_TARGET",
                "CARGO_PROFILE_RELEASE_OPT_LEVEL",
                "CARGO_PROFILE_RELEASE_DEBUG",
            )
        },
    }


def new_document(args: argparse.Namespace) -> dict:
    return {
        "schema_version": 1,
        "status": "incomplete",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "config": {
            "iterations": args.iterations,
            "warmups": args.warmups,
            "threads": args.threads,
            "thread_environment": dict.fromkeys(THREAD_ENV, str(args.threads)),
            "order_seed": args.seed,
            "reference": args.reference,
            "cold_cache": args.cold_cache,
            "cold_timer": "parent wall time: process startup, imports, operation, IPC, exit",
            "warm_timer": "worker wall time: CSV read, summary, observation; imports excluded",
            "preflight": PREFLIGHT_POLICY,
            "workloads": {tool: WORKLOADS[tool] for tool in args.tools},
        },
        "runs": [],
        "preflight": [],
        "results": {},
    }


def stage(args: argparse.Namespace, document: dict, name: str, **context) -> None:
    document["active_stage"] = {"stage": name, **context}
    checkpoint(args.output, document)


def preflight(args: argparse.Namespace, document: dict) -> None:
    for tool in args.tools:
        stage(args, document, "preflight", tool=tool)
        result = run_worker(
            {"tool": tool, "path": "unused", "threads": args.threads, "preflight": True},
            args.timeout,
        )
        document["preflight"].append({"tool": tool, **result})
        checkpoint(args.output, document)


def benchmark(args: argparse.Namespace, document: dict) -> dict:
    stage(args, document, "fixture")
    fixture = make_fixture((args.output / "fixture.csv").resolve(), args.rows)
    document["fixture"] = fixture
    path = Path(fixture["path"])
    rng = random.Random(args.seed)
    base = {
        "path": str(path),
        "expected": fixture["expected"],
        "threads": args.threads,
    }
    # Interleave cold cells by round to avoid giving one tool all the idle-host
    # samples and another all the busy-host samples. Never run tools concurrently.
    for iteration in range(args.iterations):
        order = list(args.tools)
        rng.shuffle(order)
        for tool in order:
            stage(args, document, "cold", tool=tool, iteration=iteration + 1)
            cache = prepare_cache(path, args.cold_cache)
            print(f"cold {iteration + 1}/{args.iterations}: {tool}", file=sys.stderr)
            result = run_worker({**base, "tool": tool, "warmups": 0, "iterations": 1}, args.timeout)
            document["runs"].append({"tool": tool, "mode": "cold", "cache": cache, **result})
            checkpoint(args.output, document)
    order = list(args.tools)
    rng.shuffle(order)
    for tool in order:
        stage(args, document, "warm", tool=tool)
        cache = prepare_cache(path, "warm")
        print(f"warm: {tool}", file=sys.stderr)
        result = run_worker(
            {**base, "tool": tool, "warmups": args.warmups, "iterations": args.iterations},
            args.timeout,
        )
        document["runs"].append({"tool": tool, "mode": "warm", "cache": cache, **result})
        checkpoint(args.output, document)
    stage(args, document, "validation")
    if fingerprint(path) != fixture["sha256"]:
        raise RuntimeError("fixture changed during the benchmark")
    for tool in args.tools:
        runs = [run for run in document["runs"] if run["tool"] == tool]
        document["results"][tool] = {
            "cold": summarize([run["process_seconds"] for run in runs if run["mode"] == "cold"]),
            "warm": summarize(
                next(run["operation_seconds"] for run in runs if run["mode"] == "warm")
            ),
        }
    return document


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "benchmark-results/comparison")
    parser.add_argument(
        "--preflight-only",
        action="store_true",
        help="validate pins and imports without measurements",
    )
    parser.add_argument("--rows", type=int, default=10_000, help="generated fixture rows (>= 100)")
    parser.add_argument(
        "--iterations", type=int, default=7, help="measured samples per cell (>= 2)"
    )
    parser.add_argument("--warmups", type=int, default=2, help="unmeasured warm operations (>= 1)")
    parser.add_argument("--threads", type=int, default=1, help="requested library thread limits")
    parser.add_argument("--tools", nargs="+", choices=TOOLS, default=list(TOOLS))
    parser.add_argument("--reference", choices=TOOLS, default="pandas")
    parser.add_argument("--seed", type=int, default=401, help="tool-order shuffle seed")
    parser.add_argument(
        "--compare", type=Path, help="prior results.json for repeat-run IQR overlap"
    )
    parser.add_argument("--timeout", type=float, default=600, help="seconds allowed per worker")
    parser.add_argument(
        "--cold-cache",
        choices=("warm", "evict"),
        default="warm",
        help="pre-read fixture, or request POSIX file eviction (unverified)",
    )
    args = parser.parse_args(argv)
    if args.rows < 100 or args.iterations < 2 or args.warmups < 1 or args.threads < 1:
        parser.error("require rows >= 100, iterations >= 2, warmups >= 1, threads >= 1")
    if (
        not math.isfinite(args.timeout)
        or args.timeout <= 0
        or args.reference not in args.tools
        or len(set(args.tools)) != len(args.tools)
    ):
        parser.error("require positive timeout, unique tools, and reference in tools")
    # Fail before doing work rather than silently retaining a table from an older run.
    if args.output.exists() and any(args.output.iterdir()):
        parser.error("output directory must be empty; choose a new --output for each run")
    args.output.mkdir(parents=True, exist_ok=True)
    document = new_document(args)
    try:
        previous = None
        if args.compare and not args.preflight_only:
            stage(args, document, "compare_input")
            previous = json.loads(args.compare.read_text(encoding="utf-8"))
        stage(args, document, "environment")
        document["environment"] = environment_metadata()
        preflight(args, document)
        if not args.preflight_only:
            benchmark(args, document)
            if previous is not None:
                stage(args, document, "compare")
                document["repeatability"] = compare_runs(previous, document)
            stage(args, document, "export")
            table = render_table(document)
            # The progress checkpoint stays incomplete until both exports exist.
            exported = {key: value for key, value in document.items() if key != "active_stage"}
            exported["status"] = "complete"
            (args.output / "comparison.md").write_text(table, encoding="utf-8")
            (args.output / "results.json").write_text(
                json.dumps(exported, indent=2, allow_nan=False) + "\n",
                encoding="utf-8",
            )
            print(table, end="")
        document["status"] = "complete"
        document.pop("active_stage", None)
        checkpoint(args.output, document)
    except Exception as exc:
        # Preserve evidence, then fail the run. No failed observation becomes a timing.
        document["status"] = "incomplete"
        document["failure"] = {
            **document.get("active_stage", {}),
            "type": type(exc).__name__,
            "message": str(exc),
            "traceback": traceback.format_exc(),
            **(exc.diagnostics if isinstance(exc, WorkerError) else {}),
        }
        checkpoint(args.output, document)
        # An export error may leave one file behind. Keep raw evidence only in
        # progress.json, so an incomplete run cannot be reused as a baseline.
        (args.output / "results.json").unlink(missing_ok=True)
        (args.output / "comparison.md").unlink(missing_ok=True)
        print(f"benchmark failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    if sys.argv[1:] == ["--worker"]:
        # Third-party progress or warnings must not corrupt the worker protocol.
        with contextlib.redirect_stdout(sys.stderr):
            result = worker(json.load(sys.stdin))
        print(json.dumps(result, allow_nan=False))
    else:
        raise SystemExit(main())
