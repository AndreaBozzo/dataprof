"""Repeated end-to-end CSV workloads; see benches/README.md for the protocol."""

from __future__ import annotations

import argparse
import contextlib
import csv
import functools
import gc
import hashlib
import importlib.metadata
import importlib.util
import json
import math
import os
import platform
import random
import re
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
CONTROL_CODE = (
    "import json,os,sys; json.load(sys.stdin); "
    "print(json.dumps({'pid': os.getpid(), 'status': 'complete'}))"
)
CONTROL_SCOPE = "interpreter, minimal JSON worker, IPC and exit; no harness or tool imports"


@functools.cache
def resource_collector():
    """Load and cache the optional collector for resource-enabled workers."""
    # Load only for opted-in runs, including when this script is imported by tests.
    spec = importlib.util.spec_from_file_location(
        "benchmark_resources", Path(__file__).with_name("benchmark_resources.py")
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def checkpoint(output: Path, document: dict) -> None:
    """Atomically retain completed observations without publishing a comparison."""
    temporary = output / "progress.json.tmp"
    temporary.write_text(json.dumps(document, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    temporary.replace(output / "progress.json")


class WorkerError(RuntimeError):
    def __init__(self, message: str, *, stdout="", stderr="", returncode=None, resources=None):
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
        if resources is not None:
            self.diagnostics["resources"] = resources


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
    start = time.perf_counter_ns()
    run = operation(request["tool"], Path(request["path"]), request["threads"])
    import_setup_seconds = (time.perf_counter_ns() - start) / 1e9
    if request.get("preflight"):
        return {
            "pid": os.getpid(),
            "status": "complete",
            "import_setup_seconds": import_setup_seconds,
        }
    samples, warmups = [], []
    for index in range(request["warmups"] + request["iterations"]):
        gc.collect()  # Outside the operation timer; automatic GC remains enabled.
        start = time.perf_counter_ns()
        observed = run()
        seconds = (time.perf_counter_ns() - start) / 1e9
        if observed != request["expected"]:
            raise ValueError(
                f"fixture mismatch: expected {request['expected']!r}, got {observed!r}"
            )
        (warmups if index < request["warmups"] else samples).append(seconds)
    result = {
        "pid": os.getpid(),
        "import_setup_seconds": import_setup_seconds,
        "first_operation_seconds": (warmups or samples)[0],
        "warmup_seconds": warmups,
        "operation_seconds": samples,
        "observed": observed,
    }
    if request.get("resources"):
        result["peak_rss"] = resource_collector().peak_rss()
    return result


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


def run_worker(request: dict, timeout: float, *, script: Path | None = None) -> dict:
    """Run an isolated worker, retaining output and resource evidence on failure."""
    env = {**os.environ, **dict.fromkeys(THREAD_ENV, str(request["threads"]))}
    env["PYTHONHASHSEED"] = "0"
    env["PYTHONIOENCODING"] = "utf-8"
    measurement = (
        resource_collector().ResourceMeasurement(request["resources"])
        if request.get("resources")
        else None
    )
    if measurement is not None:
        measurement.start()
    failure = None
    start = time.perf_counter_ns()
    try:
        completed = subprocess.run(
            [sys.executable, "-c", CONTROL_CODE]
            if request.get("control")
            else [sys.executable, str(script or Path(__file__).resolve()), "--worker"],
            input=json.dumps(request),
            text=True,
            encoding="utf-8",
            capture_output=True,
            env=env,
            timeout=timeout,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        failure = WorkerError(
            f"{request['tool']} worker timed out after {timeout}s",
            stdout=exc.stdout,
            stderr=exc.stderr,
        )
        raise failure from exc
    except OSError as exc:
        failure = WorkerError(f"{request['tool']} worker could not start: {exc}")
        raise failure from exc
    finally:
        elapsed = (time.perf_counter_ns() - start) / 1e9
        resources = measurement.finish() if measurement is not None else None
        if failure is not None and resources is not None:
            failure.diagnostics["resources"] = resources
    if completed.returncode:
        raise WorkerError(
            f"{request['tool']} worker failed:\n{completed.stderr}",
            stdout=completed.stdout,
            stderr=completed.stderr,
            returncode=completed.returncode,
            resources=resources,
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
            resources=resources,
        ) from exc
    result["process_seconds"] = elapsed
    result["stderr"] = completed.stderr
    if resources is not None:
        resources["peak_rss"] = result.pop("peak_rss")
        result["resources"] = resources
    return result


def summarize(samples: list[float]) -> dict:
    if len(samples) < 2:
        raise ValueError("at least two samples are required")
    q1, _, q3 = statistics.quantiles(samples, n=4, method="inclusive")
    return {
        "sample_count": len(samples),
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
        f"Evidence: diagnostic; {document['config']['blocks']} process blocks, "
        f"{document['config']['iterations']} samples per block. "
        "Publication mode is not an established baseline.",
        "Import/setup and first-operation boundaries and ordered controls are retained in JSON; "
        "no independent medians are subtracted.",
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
    if "resource_results" in document:
        lines.extend(["", *resource_collector().render_summary(document["resource_results"])])
    return "\n".join(lines) + "\n"


def compare_runs(previous: dict, current: dict) -> dict:
    """Refuse mismatched experiments before reporting repeat-run dispersion."""
    if previous.get("status", "complete") != "complete":
        raise ValueError("repeatability comparison requires a complete previous run")
    if previous["fixture"]["sha256"] != current["fixture"]["sha256"]:
        raise ValueError("repeatability comparison requires the same fixture")
    if previous["config"] != current["config"]:
        raise ValueError("repeatability comparison requires the same benchmark configuration")
    if "resources" in current["config"]:
        for key in ("resource_script_sha256", "resource_host"):
            if previous["environment"].get(key) != current["environment"].get(key):
                raise ValueError(f"repeatability comparison has different environment.{key}")
    if any(not known_cpu(doc["environment"]["cpu"]) for doc in (previous, current)):
        raise ValueError("repeatability comparison requires a known CPU model")
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


def known_cpu(value: str) -> bool:
    """Reject unknown identifiers and architecture aliases, including cross-host aliases."""
    model = value.strip().lower()
    architecture = re.fullmatch(
        r"x86(?:_64(?:h|_v[234])?)?|i[3-6]86|amd64|x64|ia64|"
        r"aarch64(?:_be)?|arm(?:64e?|v\d+(?:[a-z]+|[-_][a-z0-9]+)?)?|"
        r"(?:ppc|powerpc)(?:64)?(?:le|el)?|mips(?:32|64)?(?:el|le)?|"
        r"s390x?|riscv(?:32|64)|sparc(?:32|64|v9)?|loongarch64|alpha",
        model,
    )
    return (
        model not in {"", "unknown", platform.machine().strip().lower()}
        and not model.startswith(("intel64 family", "amd64 family"))
        and architecture is None
    )


def cpu_model() -> str:
    """Prefer a model identifier to platform.processor's architecture placeholder."""
    candidate = platform.processor()
    if known_cpu(candidate):
        return candidate
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            key, separator, value = line.partition(":")
            if (
                separator
                and key.strip() in ("model name", "Hardware", "Model")
                and known_cpu(value)
            ):
                return value.strip()
    if sys.platform == "win32":
        import winreg

        with winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE, r"HARDWARE\DESCRIPTION\System\CentralProcessor\0"
        ) as key:
            candidate = winreg.QueryValueEx(key, "ProcessorNameString")[0]
    elif sys.platform == "darwin":
        candidate = subprocess.check_output(
            ["sysctl", "-n", "machdep.cpu.brand_string"], text=True
        ).strip()
    return candidate if known_cpu(candidate) else "unknown"


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
    return {
        "python": sys.version,
        "executable": sys.executable,
        "os": platform.platform(),
        "hostname": platform.node(),
        "architecture": platform.machine(),
        "cpu": cpu_model(),
        "logical_cpus": psutil.cpu_count(),
        "physical_cpus": psutil.cpu_count(logical=False),
        "ram_bytes": psutil.virtual_memory().total,
        "versions": versions,
        "native_extensions": {str(path): fingerprint(path) for path in native},
        "git_commit": git_output("rev-parse", "HEAD"),
        "git_status": git_output("status", "--porcelain"),
        "benchmark_script_sha256": fingerprint(Path(__file__)),
        "resource_script_sha256": fingerprint(Path(__file__).with_name("benchmark_resources.py")),
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
            "protocol_version": 2,
            "mode": "publication" if args.publication else "routine",
            "blocks": args.blocks,
            "host_description": args.host_description,
            "iterations": args.iterations,
            "warmups": args.warmups,
            "threads": args.threads,
            "thread_environment": dict.fromkeys(THREAD_ENV, str(args.threads)),
            "order_seed": args.seed,
            "reference": args.reference,
            "cold_cache": args.cold_cache,
            "cold_timer": "parent wall time: process startup, imports, operation, IPC, exit",
            "warm_timer": "worker wall time: CSV read, summary, observation; "
            "adapter import/setup excluded",
            "preflight": PREFLIGHT_POLICY,
            "import_timer": "worker wall time around adapter import and callable setup; "
            "deferred imports remain in operations",
            "first_operation_timer": "first CSV read, summary and observation in each worker, "
            "before any repeated operation",
            "control_timer": CONTROL_SCOPE,
            "library_cache": "not evicted; environment preparation and preflight "
            "may warm library pages and caches",
            "workloads": {tool: WORKLOADS[tool] for tool in args.tools},
            **(
                {
                    "resources": {
                        "protocol_version": 1,
                        "powercap_root": str(args.powercap_root.resolve()),
                        "poll_seconds": args.energy_poll_seconds,
                        "idle_seconds": args.idle_seconds,
                        "max_zone_watts": args.max_zone_watts,
                        "background_load_policy": args.background_load_policy,
                        "power_source": args.power_source,
                        "scope": "per worker; cold: one operation; "
                        "warm: warmups plus all iterations",
                        "counter_assumptions": "no resets; each zone stays below max_zone_watts; "
                        "overlapping zones are never summed",
                    }
                }
                if args.resources
                else {}
            ),
        },
        "runs": [],
        "preflight": [],
        "controls": [],
        "evidence_status": "diagnostic",
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
        document["preflight"].append(
            {
                "tool": tool,
                "invocation": "first_adapter_import_after_environment_preparation",
                **result,
            }
        )
        checkpoint(args.output, document)


def summarize_runs(runs: list[dict], tools: list[str]) -> dict:
    results = {}
    for tool in tools:
        selected = [run for run in runs if run["tool"] == tool]
        results[tool] = {
            "cold": summarize(
                [run["process_seconds"] for run in selected if run["mode"] == "cold"]
            ),
            "warm": summarize(
                [
                    sample
                    for run in selected
                    if run["mode"] == "warm"
                    for sample in run["operation_seconds"]
                ]
            ),
        }
    return results


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
        **({"resources": document["config"]["resources"]} if args.resources else {}),
    }
    # Interleave cold cells by round to avoid giving one tool all the idle-host
    # samples and another all the busy-host samples. Never run tools concurrently.
    for block in range(1, args.blocks + 1):
        for iteration in range(1, args.iterations + 1):
            stage(args, document, "control", block=block, iteration=iteration)
            control = run_worker(
                {"tool": "minimal-worker", "threads": args.threads, "control": True}, args.timeout
            )
            document["controls"].append({"block": block, "iteration": iteration, **control})
            checkpoint(args.output, document)
            order = list(args.tools)
            rng.shuffle(order)
            for tool in order:
                stage(args, document, "cold", tool=tool, block=block, iteration=iteration)
                cache = prepare_cache(path, args.cold_cache)
                print(f"block {block}, cold {iteration}/{args.iterations}: {tool}", file=sys.stderr)
                result = run_worker(
                    {**base, "tool": tool, "warmups": 0, "iterations": 1}, args.timeout
                )
                document["runs"].append(
                    {
                        "tool": tool,
                        "mode": "cold",
                        "block": block,
                        "iteration": iteration,
                        "invocation": "first_fixture_operation_after_preflight"
                        if block == iteration == 1
                        else "subsequent_fresh_process",
                        "cache": cache,
                        **result,
                    }
                )
                checkpoint(args.output, document)
        order = list(args.tools)
        rng.shuffle(order)
        for tool in order:
            stage(args, document, "warm", tool=tool, block=block)
            cache = prepare_cache(path, "warm")
            print(f"block {block}, warm: {tool}", file=sys.stderr)
            result = run_worker(
                {**base, "tool": tool, "warmups": args.warmups, "iterations": args.iterations},
                args.timeout,
            )
            document["runs"].append(
                {
                    "tool": tool,
                    "mode": "warm",
                    "block": block,
                    "invocation": "subsequent_fresh_process",
                    "cache": cache,
                    **result,
                }
            )
            checkpoint(args.output, document)
    stage(args, document, "validation")
    if fingerprint(path) != fixture["sha256"]:
        raise RuntimeError("fixture changed during the benchmark")
    document["results"] = summarize_runs(document["runs"], args.tools)
    if args.resources:
        document["resource_results"] = resource_collector().summarize_resources(
            document["runs"], args.tools
        )
    document["control_summary"] = summarize(
        [run["process_seconds"] for run in document["controls"]]
    )
    # Blocks remain individually assessable; pooled operations are not independent hosts.
    document["block_results"] = [
        {
            "block": block,
            "results": summarize_runs(
                [r for r in document["runs"] if r["block"] == block], args.tools
            ),
        }
        for block in range(1, args.blocks + 1)
    ]
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
        "--iterations", type=int, help="samples per block (routine: 7, publication: 21)"
    )
    parser.add_argument(
        "--publication",
        action="store_true",
        help="larger experiment budget; still diagnostic until independently assessed",
    )
    parser.add_argument(
        "--blocks", type=int, help="independent process blocks (routine: 1, publication: 3)"
    )
    parser.add_argument(
        "--host-description",
        default="",
        help="host identity and load/power controls; required for publication mode",
    )
    parser.add_argument(
        "--warmups",
        type=int,
        default=2,
        help="timed warmups excluded from steady-state aggregates (>= 1)",
    )
    parser.add_argument("--threads", type=int, default=1, help="requested library thread limits")
    parser.add_argument("--tools", nargs="+", choices=TOOLS, default=list(TOOLS))
    parser.add_argument("--reference", choices=TOOLS, default="pandas")
    parser.add_argument("--seed", type=int, default=401, help="tool-order shuffle seed")
    parser.add_argument(
        "--compare", type=Path, help="prior results.json for repeat-run IQR overlap"
    )
    parser.add_argument("--timeout", type=float, default=600, help="seconds allowed per worker")
    parser.add_argument(
        "--resources", action="store_true", help="collect host energy and worker peak RSS"
    )
    parser.add_argument(
        "--powercap-root",
        type=Path,
        default=Path("/sys/class/powercap"),
        help="Linux powercap sysfs root; missing/inaccessible counters are unavailable",
    )
    parser.add_argument(
        "--energy-poll-seconds",
        type=float,
        default=0.05,
        help="parent counter sampling interval (default: 0.05 seconds)",
    )
    parser.add_argument(
        "--idle-seconds",
        type=float,
        default=0.25,
        help="paired idle baseline before each worker (default: 0.25 seconds)",
    )
    parser.add_argument(
        "--max-zone-watts",
        type=float,
        default=10000,
        help="assumed upper power bound per zone for detecting ambiguous wraps",
    )
    parser.add_argument(
        "--background-load-policy",
        default="uncontrolled",
        help="declared host background-load controls; observations do not enforce them",
    )
    parser.add_argument(
        "--power-source",
        default="unknown",
        help="declared power source (for example AC or battery)",
    )
    parser.add_argument(
        "--cold-cache",
        choices=("warm", "evict"),
        default="warm",
        help="pre-read fixture, or request POSIX file eviction (unverified)",
    )
    args = parser.parse_args(argv)
    args.iterations = (
        args.iterations if args.iterations is not None else (21 if args.publication else 7)
    )
    args.blocks = args.blocks if args.blocks is not None else (3 if args.publication else 1)
    args.host_description = args.host_description.strip()
    if any(
        not math.isfinite(v) or v <= 0
        for v in (args.energy_poll_seconds, args.idle_seconds, args.max_zone_watts)
    ):
        parser.error("resource intervals and power bound must be positive and finite")
    if (
        args.resources
        and args.publication
        and (
            args.background_load_policy.strip() in ("", "uncontrolled")
            or args.power_source.strip() in ("", "unknown")
        )
    ):
        parser.error("resource publication requires --background-load-policy and --power-source")
    if args.blocks < 1:
        parser.error("require blocks >= 1")
    if args.publication and (args.iterations < 7 or args.blocks < 2 or not args.host_description):
        parser.error(
            "publication requires iterations >= 7, blocks >= 2 and --host-description; "
            "this is a budget, not a precision guarantee"
        )
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
        if args.resources:
            document["environment"]["resource_host"] = resource_collector().host_metadata()
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
