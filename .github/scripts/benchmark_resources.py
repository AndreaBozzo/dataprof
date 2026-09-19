"""Optional host-energy and worker peak-RSS evidence for the benchmark harness."""

from __future__ import annotations

import math
import platform
import statistics
import threading
import time
from pathlib import Path

POWERCAP = Path("/sys/class/powercap")
ENERGY_SCOPE = "host powercap zone during worker startup, imports, all operations, IPC and exit"
RSS_SCOPE = (
    "worker process lifetime through final operation; imports and warmups included; no children"
)


def unavailable(reason: str) -> dict:
    return {"status": "unavailable", "reason": reason}


def discover_zones(root: Path = POWERCAP) -> dict:
    """Resolve sysfs class symlinks and keep overlapping zones separate."""
    try:
        # Class entries are symlinks; rglob alone does not traverse them.
        paths = sorted(
            {p.resolve() for entry in root.iterdir() for p in entry.resolve().rglob("energy_uj")}
        )
    except OSError as exc:
        return {**unavailable(str(exc)), "zones": []}
    zones = []
    for path in paths:
        zone = {"path": str(path), "unit": "microjoules"}
        try:
            zone["name"] = (path.parent / "name").read_text().strip()
            zone["max_energy_range_uj"] = int((path.parent / "max_energy_range_uj").read_text())
            value = int(path.read_text())
            if not 0 <= value < zone["max_energy_range_uj"]:
                raise ValueError("energy counter outside its positive range")
            zone["status"] = "available"
        except (OSError, ValueError) as exc:
            zone.update(unavailable(str(exc)))
        zones.append(zone)
    if not any(z["status"] == "available" for z in zones):
        return {**unavailable("no readable powercap energy counters"), "zones": zones}
    return {"status": "available", "zones": zones}


def energy_delta(before: int, after: int, maximum: int, seconds: float, max_watts: float) -> int:
    """Modulo delta, rejecting intervals that could hide a full counter cycle.

    max_watts is an explicit conservative *assumption*, not a measured power
    limit. A counter reset is indistinguishable from wraparound in this ABI.
    """
    if maximum <= 0 or not (0 <= before < maximum and 0 <= after < maximum):
        raise ValueError("energy counter outside its positive range")
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("counter interval must be positive and finite")
    if not math.isfinite(max_watts) or max_watts <= 0:
        raise ValueError("power bound must be positive and finite")
    bound_uj = seconds * max_watts * 1_000_000
    if bound_uj >= maximum:
        raise ValueError("counter interval could hide a full wrap under the declared power bound")
    delta = (after - before) % maximum
    if delta > bound_uj:
        raise ValueError("counter delta exceeds declared power bound (or counter reset)")
    return delta


class EnergyMeter:
    """Read counters on a parent thread; preserve every reading and read failure."""

    def __init__(self, discovery: dict, interval: float, max_watts: float):
        self.discovery = discovery
        self.interval = interval
        self.max_watts = max_watts
        self.readings: dict[str, list[dict]] = {
            z["path"]: [] for z in discovery["zones"] if z["status"] == "available"
        }
        self.stop_event = threading.Event()
        self.thread: threading.Thread | None = None

    def read(self) -> None:
        for path, samples in self.readings.items():
            start = time.perf_counter_ns()
            try:
                value = int(Path(path).read_text())
                sample = {"energy_uj": value}
            except (OSError, ValueError) as exc:
                sample = {"error": str(exc)}
            samples.append(
                {"read_start_ns": start, "read_end_ns": time.perf_counter_ns(), **sample}
            )

    def poll(self) -> None:
        while not self.stop_event.wait(self.interval):
            self.read()

    def start(self) -> None:
        self.read()
        if self.readings:
            self.thread = threading.Thread(target=self.poll, daemon=True)
            self.thread.start()

    def finish(self) -> dict:
        self.stop_event.set()
        if self.thread is not None:
            self.thread.join()
        self.read()
        zones = []
        for zone in self.discovery["zones"]:
            result = dict(zone)
            samples = self.readings.get(zone["path"], [])
            result["readings"] = samples
            if samples:
                try:
                    if any("error" in s for s in samples):
                        raise ValueError("counter read failed; see raw readings")
                    total = 0
                    for before, after in zip(samples, samples[1:]):
                        # Outer read bounds conservatively include syscall latency.
                        seconds = (after["read_end_ns"] - before["read_start_ns"]) / 1e9
                        total += energy_delta(
                            before["energy_uj"],
                            after["energy_uj"],
                            zone["max_energy_range_uj"],
                            seconds,
                            self.max_watts,
                        )
                    result.update(
                        status="available",
                        energy_uj=total,
                        seconds=(samples[-1]["read_end_ns"] - samples[0]["read_start_ns"]) / 1e9,
                    )
                except ValueError as exc:
                    result.update(unavailable(str(exc)))
            zones.append(result)
        return {
            "status": "available"
            if any(z["status"] == "available" for z in zones)
            else "unavailable",
            "reason": self.discovery.get("reason"),
            "zones": zones,
        }


def subtract_baseline(measured: dict, idle: dict) -> dict:
    """Keep gross energy and a signed idle-adjusted estimate, per zone."""
    baselines = {z["path"]: z for z in idle["zones"]}
    for zone in measured["zones"]:
        baseline = baselines.get(zone["path"])
        if zone["status"] != "available" or not baseline or baseline["status"] != "available":
            zone["idle_adjusted"] = unavailable("measurement or paired idle baseline unavailable")
            continue
        watts = baseline["energy_uj"] / baseline["seconds"] / 1_000_000
        zone["idle_adjusted"] = {
            "status": "available",
            "baseline_watts": watts,
            "energy_uj": zone["energy_uj"] - watts * zone["seconds"] * 1_000_000,
        }
    return measured


class ResourceMeasurement:
    def __init__(self, config: dict):
        self.config = config
        self.discovery = discover_zones(Path(config["powercap_root"]))
        self.meter = self.new_meter()

    def new_meter(self) -> EnergyMeter:
        return EnergyMeter(
            self.discovery, self.config["poll_seconds"], self.config["max_zone_watts"]
        )

    def start(self) -> None:
        baseline = self.new_meter()
        baseline.start()
        try:
            if self.discovery["status"] == "available":
                time.sleep(self.config["idle_seconds"])
        finally:
            self.idle = baseline.finish()
        self.meter.start()

    def finish(self) -> dict:
        return {
            "energy_scope": ENERGY_SCOPE,
            "idle": self.idle,
            "energy": subtract_baseline(self.meter.finish(), self.idle),
        }


def peak_rss() -> dict:
    """OS high-water mark, not Python allocations or sampled instantaneous RSS."""
    system = platform.system()
    try:
        if system in ("Linux", "Darwin"):
            import resource

            raw = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
            value = int(raw * (1024 if system == "Linux" else 1))
            source = "getrusage(RUSAGE_SELF).ru_maxrss"
        elif system == "Windows":
            import psutil

            try:
                memory = psutil.Process().memory_info()
            except psutil.Error as exc:
                return {**unavailable(str(exc)), "scope": RSS_SCOPE}
            value = int(memory.peak_wset)
            source = "GetProcessMemoryInfo.PeakWorkingSetSize (psutil peak_wset)"
        else:
            return {**unavailable(f"no peak-RSS collector for {system}"), "scope": RSS_SCOPE}
        return {"status": "available", "bytes": value, "source": source, "scope": RSS_SCOPE}
    except (ImportError, OSError, AttributeError) as exc:
        return {**unavailable(str(exc)), "scope": RSS_SCOPE}


def host_metadata() -> dict:
    """Read policy observations; absence is unknown, never a claimed host control."""

    def read(path: Path) -> dict:
        try:
            return {"status": "available", "value": path.read_text().strip()}
        except OSError as exc:
            return unavailable(str(exc))

    return {
        "kernel": platform.release(),
        "power_supply": {
            str(p): read(p) for p in sorted(Path("/sys/class/power_supply").glob("*/online"))
        },
        "cpu_frequency_policy": {
            str(p): read(p)
            for name in ("scaling_governor", "scaling_min_freq", "scaling_max_freq")
            for p in sorted(Path("/sys/devices/system/cpu/cpufreq").glob(f"policy*/{name}"))
        },
        "missing_policy": "empty mappings mean unavailable; record controls in host_description",
    }


def distribution(values: list[float], expected: int, unit: str) -> dict:
    """Do not publish a median from an incomplete set of measurements."""
    result = {
        "unit": unit,
        "sample_count": len(values),
        "expected_count": expected,
        "samples": values,
    }
    if len(values) != expected or not values:
        return {**result, **unavailable("one or more measurements unavailable")}
    if len(values) < 2:
        return {**result, "status": "insufficient_samples", "reason": "need repeated workers"}
    q1, _, q3 = statistics.quantiles(values, n=4, method="inclusive")
    return {
        **result,
        "status": "available",
        "median": statistics.median(values),
        "q1": q1,
        "q3": q3,
        "iqr": q3 - q1,
        "min": min(values),
        "max": max(values),
    }


def summarize_resources(runs: list[dict], tools: list[str]) -> dict:
    results = {}
    for tool in tools:
        results[tool] = {}
        for mode in ("cold", "warm"):
            selected = [r["resources"] for r in runs if r["tool"] == tool and r["mode"] == mode]
            peak = [
                r["peak_rss"]["bytes"] for r in selected if r["peak_rss"]["status"] == "available"
            ]
            paths = sorted({z["path"] for r in selected for z in r["energy"]["zones"]})
            energy = {}
            for path in paths:
                zones = [z for r in selected for z in r["energy"]["zones"] if z["path"] == path]
                energy[path] = {
                    "gross": distribution(
                        [z["energy_uj"] for z in zones if z["status"] == "available"],
                        len(selected),
                        "microjoules",
                    ),
                    "idle_adjusted": distribution(
                        [
                            z["idle_adjusted"]["energy_uj"]
                            for z in zones
                            if z["idle_adjusted"]["status"] == "available"
                        ],
                        len(selected),
                        "microjoules",
                    ),
                }
            results[tool][mode] = {
                "peak_rss": distribution(peak, len(selected), "bytes"),
                "energy": energy or unavailable("no powercap zones recorded"),
            }
    return results


def render_summary(results: dict) -> list[str]:
    def cell(summary: dict, scale: float = 1) -> str:
        if summary["status"] != "available":
            return f"{summary['status']} ({summary['sample_count']}/{summary['expected_count']})"
        return f"{summary['median'] / scale:.6f} [{summary['iqr'] / scale:.6f}]"

    lines = [
        "Resource evidence: diagnostic only; median [IQR] across worker processes.",
        "Energy covers the host zone, including background load and the collector; "
        "it is not process or whole-system energy. Zones are never summed.",
        "Cold workers run once; warm workers include imports, warmups and every iteration. "
        "Warm resource values are block totals, not per-operation costs.",
        "RSS is the worker lifetime high-water mark through its last operation, "
        "excluding children.",
        "Idle-adjusted energy is a signed estimate; negative values are retained. "
        "Raw readings, paired baselines and unavailable reasons are in results.json.",
        "",
        "| Tool | Worker condition | Peak RSS MiB [IQR] |",
        "| --- | --- | ---: |",
    ]
    for tool, modes in results.items():
        for mode, result in modes.items():
            lines.append(f"| {tool} | {mode} | {cell(result['peak_rss'], 1024**2)} |")
    lines.extend(
        [
            "",
            "| Tool | Worker condition | Energy zone | Gross J [IQR] | Idle-adjusted J [IQR] |",
            "| --- | --- | --- | ---: | ---: |",
        ]
    )
    for tool, modes in results.items():
        for mode, result in modes.items():
            energy = result["energy"]
            if energy.get("status") == "unavailable":
                lines.append(f"| {tool} | {mode} | unavailable | unavailable | unavailable |")
                continue
            for path, zone in energy.items():
                # sysfs paths are labels, not executable markup.
                label = path.replace("|", "\\|").replace("\n", " ")
                lines.append(
                    f"| {tool} | {mode} | {label} | {cell(zone['gross'], 1e6)} | "
                    f"{cell(zone['idle_adjusted'], 1e6)} |"
                )
    return lines
