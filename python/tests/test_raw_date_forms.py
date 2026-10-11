"""The date parser fast paths keep every raw date count. Rust twin: tests/raw_date_forms.rs."""

import json
from pathlib import Path

from _profile_routes import _routes


def test_every_accepted_date_form_keeps_its_count_on_every_route(tmp_path):
    forms = json.loads(
        (Path(__file__).parents[2] / "tests/fixtures/raw_date_forms.json").read_text(
            encoding="utf-8"
        )
    )
    wrong = []
    for valid in forms["valid"]:
        for invalid in forms["invalid"]:
            for route, run in _routes(tmp_path).items():
                report = run("dates", [valid] * 20 + [invalid])
                metrics = (report.to_dict().get("quality") or {}).get("timeliness") or {}
                actual = metrics.get("invalid_date_values")
                if actual != 1:
                    wrong.append(f"[{route}] {valid!r}/{invalid!r}: invalid dates={actual}")
    assert not wrong, "\n".join(wrong)
