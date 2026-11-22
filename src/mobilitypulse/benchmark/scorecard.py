"""Evaluation scorecard: compares results/measurements.json with configs/targets.yaml -> results/evaluation_scorecard.md."""
from __future__ import annotations

import json
import platform
import xml.etree.ElementTree as ET
from datetime import datetime, timezone

from ..common.config import Config, load_config, load_targets
from ..common.utils import get_logger, load_measurements, record_measurements

log = get_logger(__name__)

OPS = {
    "<=": lambda v, t: v <= t,
    ">=": lambda v, t: v >= t,
    "==": lambda v, t: abs(v - t) < 1e-9,
    "<": lambda v, t: v < t,
    ">": lambda v, t: v > t,
}


def evaluate(targets: list[dict], measurements: dict) -> list[dict]:
    rows = []
    for t in targets:
        m = measurements.get(t["key"])
        value = None if m is None else m["value"]
        if value is None:
            status = "NOT RUN"
        elif t["op"] == "info":
            status = "INFO"
        else:
            status = "PASS" if OPS[t["op"]](float(value), float(t["threshold"])) else "FAIL"
        rows.append({**t, "measured": value, "status": status,
                     "recorded_at": None if m is None else m.get("recorded_at")})
    return rows


def ingest_test_reports(cfg: Config) -> None:
    """Turn pytest junit + coverage json (written by `make test`) into D5 measurements."""
    tests = cfg.path("results") / "tests"
    values = {}
    junit = tests / "junit.xml"
    if junit.exists():
        root = ET.parse(junit).getroot()
        suites = [root] if root.tag == "testsuite" else list(root)
        total = sum(int(s.get("tests", 0)) for s in suites)
        bad = sum(int(s.get("failures", 0)) + int(s.get("errors", 0)) for s in suites)
        skipped = sum(int(s.get("skipped", 0)) for s in suites)
        run = total - skipped
        values["tests.pass_pct"] = 100.0 * (run - bad) / run if run else None
    cov = tests / "coverage.json"
    if cov.exists():
        values["tests.coverage_pct"] = json.loads(cov.read_text(encoding="utf-8"))["totals"]["percent_covered"]
    if values:
        record_measurements(cfg, values, {"source": "pytest"})


def _fmt(v) -> str:
    if v is None:
        return "-"
    if isinstance(v, float):
        return f"{v:,.3f}" if abs(v) < 100 else f"{v:,.1f}"
    return str(v)


def render(rows: list[dict]) -> str:
    counts = {s: sum(r["status"] == s for r in rows) for s in ["PASS", "FAIL", "NOT RUN", "INFO"]}
    lines = [
        "# NYC MobilityPulse - Evaluation Scorecard",
        "",
        f"Generated {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC} on {platform.node()} "
        f"({platform.system()} {platform.machine()}, Python {platform.python_version()}).",
        "Targets: `configs/targets.yaml` (fixed before results). Measurements: `results/measurements.json`.",
        "",
        f"**PASS {counts['PASS']} | FAIL {counts['FAIL']} | NOT RUN {counts['NOT RUN']} | INFO {counts['INFO']}**",
        "",
        "| ID | Area | Metric | Target | Measured | Status |",
        "|---|---|---|---|---|---|",
    ]
    for r in rows:
        target = "-" if r["op"] == "info" else f"{r['op']} {_fmt(r['threshold'])} {r.get('unit') or ''}".strip()
        lines.append(f"| {r['id']} | {r['area']} | {r['metric']} | {target} | {_fmt(r['measured'])} | **{r['status']}** |")
    lines += ["", "Every FAIL must be explained in the report with the measured value, root cause (Spark UI / metrics "
              "evidence) and remediation attempted.", ""]
    return "\n".join(lines)


def build_scorecard(cfg: Config) -> list[dict]:
    ingest_test_reports(cfg)
    rows = evaluate(load_targets(), load_measurements(cfg))
    out = cfg.path("results") / "evaluation_scorecard.md"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render(rows), encoding="utf-8")
    log.info("scorecard written to %s", out)
    return rows


def main() -> None:
    rows = build_scorecard(load_config())
    for r in rows:
        print(f"{r['id']:<5} {r['status']:<8} {r['metric']}: {_fmt(r['measured'])}")


if __name__ == "__main__":
    main()
