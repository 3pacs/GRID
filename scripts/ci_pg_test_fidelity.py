"""Discover PostgreSQL tests and account for every JUnit run's skipped tests.

Discovery only collects tests; it never runs them or configures a database.
Example commands, after the database/bootstrap decision has been made elsewhere::

    python scripts/ci_pg_test_fidelity.py discover --tests-root tests --output pg-tests.json
    python -m pytest tests/ -p scripts.ci_pytest_report \
      --ci-report=exact.json --junitxml=junit.xml
    python scripts/ci_pg_test_fidelity.py check-skips --junit junit.xml \
      --report exact.json --allowlist allowed.json --output skip-check.json

The allowlist is JSON: {"version": 1, "allowed_skips": [{"nodeid": "...",
"reason": "..."}]}. Both fields match exactly; no globs. JUnit validates the run
and skip count, while the controller report preserves exact pytest node IDs,
xfail and XPASS outcomes that JUnit alone cannot represent reliably.
"""

from __future__ import annotations

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path


class _Collector:
    def __init__(self) -> None:
        self.items: list[object] = []

    def pytest_collection_finish(self, session: object) -> None:
        self.items = list(session.items)


def discover(tests_root: Path) -> dict[str, object]:
    """Collect all tests and select PostgreSQL candidates without executing any."""
    import pytest

    collector = _Collector()
    status = pytest.main(
        [str(tests_root), "--collect-only", "-q", "-p", "no:cacheprovider",
         f"--rootdir={Path.cwd().resolve()}"],
        plugins=[collector],
    )
    if status not in (pytest.ExitCode.OK, pytest.ExitCode.NO_TESTS_COLLECTED):
        raise ValueError(f"pytest collection failed with exit code {int(status)}")
    if not collector.items:
        raise ValueError("pytest collection produced no test IDs")

    selected: list[dict[str, object]] = []
    for item in collector.items:
        signals = []
        if str(item.path).endswith("_pg.py"):
            signals.append("pg_filename")
        if "pg_engine" in item.fixturenames:
            signals.append("pg_engine_fixture")
        marker = item.get_closest_marker("xdist_group")
        if marker and marker.args and marker.args[0] == "postgres":
            signals.append("postgres_xdist_group")
        if signals:
            selected.append({"nodeid": item.nodeid, "signals": sorted(signals)})

    return {
        "version": 1,
        "collected_count": len(collector.items),
        "pg_test_count": len(selected),
        "pg_tests": sorted(selected, key=lambda entry: str(entry["nodeid"])),
    }


def _allowed(allowlist_path: Path) -> set[tuple[str, str]]:
    data = json.loads(allowlist_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict) or set(data) != {"version", "allowed_skips"} or data["version"] != 1:
        raise ValueError("allowlist must have version 1 and allowed_skips only")
    if not isinstance(data["allowed_skips"], list):
        raise ValueError("allowed_skips must be a list")
    allowed: set[tuple[str, str]] = set()
    for entry in data["allowed_skips"]:
        if not isinstance(entry, dict) or set(entry) != {"nodeid", "reason"}:
            raise ValueError("each allowlist entry needs exact nodeid and reason")
        pair = (entry["nodeid"], entry["reason"])
        if not all(isinstance(part, str) and part for part in pair) or pair in allowed:
            raise ValueError("allowlist entries must be unique nonempty strings")
        allowed.add(pair)
    return allowed


def check_skips(junit_path: Path, report_path: Path, allowlist_path: Path) -> dict[str, object]:
    """Fail closed on unknown, changed, duplicate, or stale exact-ID skips."""
    root = ET.parse(junit_path).getroot()
    cases = root.findall(".//testcase")
    if not cases:
        raise ValueError("JUnit XML has no testcases")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict) or set(report) != {"version", "complete", "exitstatus", "events", "collect_errors"}:
        raise ValueError("exact-ID report has missing or unexpected fields")
    if report["version"] != 1 or report["complete"] is not True:
        raise ValueError("exact-ID report is incomplete or has unsupported version")
    if not isinstance(report["exitstatus"], int) or not isinstance(report["collect_errors"], list):
        raise ValueError("exact-ID report has malformed status or collection errors")
    if report["collect_errors"]:
        raise ValueError("exact-ID report contains collection errors")
    if not isinstance(report["events"], list):
        raise ValueError("exact-ID report events must be a list")
    observed: set[tuple[str, str]] = set()
    xpasses: list[dict[str, str]] = []
    for event in report["events"]:
        if not isinstance(event, dict) or set(event) != {"nodeid", "outcome", "reason"}:
            raise ValueError("malformed exact-ID event")
        nodeid, outcome, reason = event["nodeid"], event["outcome"], event["reason"]
        if not all(isinstance(value, str) and value for value in (nodeid, outcome, reason)):
            raise ValueError("exact-ID event needs nonempty strings")
        if outcome == "xpass":
            xpasses.append(event)
            continue
        if outcome not in ("skip", "xfail"):
            raise ValueError(f"unknown exact-ID outcome {outcome!r}")
        pair = (nodeid, reason)
        if pair in observed:
            raise ValueError(f"duplicate skipped testcase {nodeid}: {reason}")
        observed.add(pair)
    junit_skips = sum(case.find("skipped") is not None for case in cases)
    if junit_skips != len(observed):
        raise ValueError("JUnit skip count differs from exact-ID report")
    allowed = _allowed(allowlist_path)
    unexpected = sorted(observed - allowed)
    stale = sorted(allowed - observed)
    return {
        "version": 1,
        "testcase_count": len(cases),
        "skipped_count": len(observed),
        "unexpected": [{"nodeid": n, "reason": r} for n, r in unexpected],
        "stale_allowlist": [{"nodeid": n, "reason": r} for n, r in stale],
        "xpasses": xpasses,
        "ok": not unexpected and not stale and not xpasses,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    discover_parser = sub.add_parser("discover")
    discover_parser.add_argument("--tests-root", type=Path, default=Path("tests"))
    discover_parser.add_argument("--output", type=Path, required=True)
    check_parser = sub.add_parser("check-skips")
    check_parser.add_argument("--junit", type=Path, required=True)
    check_parser.add_argument("--report", type=Path, required=True)
    check_parser.add_argument("--allowlist", type=Path, required=True)
    check_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "discover":
            result = discover(args.tests_root)
        else:
            result = check_skips(args.junit, args.report, args.allowlist)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return 0 if result.get("ok", True) else 1
    except (OSError, ValueError, TypeError, ET.ParseError, json.JSONDecodeError) as exc:
        print(f"CI PostgreSQL test accounting failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
