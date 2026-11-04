"""The local build keeps the profile's own optimization level.

rustflags in `.cargo/config.toml` reach rustc after the profile's flags, and
the last `-C opt-level` wins. A level there overrides every profile, and at a
non-zero level a dev build, the one `maturin develop` makes for the Python
suite, loses its debug assertions and overflow checks: cargo leaves them to
rustc's default at the profile's level 0, and rustc derives that default from
the final level.
"""

from __future__ import annotations

import re
import tomllib
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_cargo_config_rustflags_set_no_opt_level():
    config = tomllib.loads((ROOT / ".cargo/config.toml").read_text(encoding="utf-8"))
    flags = {
        triple: " ".join(table.get("rustflags", []))
        for triple, table in config.get("target", {}).items()
    }
    assert flags, "expected the per-target CPU flags in .cargo/config.toml"
    leaking = {triple: f for triple, f in flags.items() if re.search(r"opt-level", f)}
    assert leaking == {}


def test_dev_profile_keeps_debug_assertions():
    profiles = tomllib.loads((ROOT / "Cargo.toml").read_text(encoding="utf-8"))["profile"]
    for name in ("dev", "test"):
        assert profiles[name].get("debug-assertions", True) is True, name
        assert profiles[name].get("overflow-checks", True) is True, name
