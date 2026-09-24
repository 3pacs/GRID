"""Discover PostgreSQL tests and account for every JUnit run's skipped tests.

Discovery only collects tests; it never runs them or configures a database.
Example commands, after the database/bootstrap decision has been made elsewhere::

    python scripts/ci_pg_test_fidelity.py discover --tests-root tests --output pg-tests.json
    python -m pytest tests/ -p scripts.ci_pytest_report \
      --ci-report=exact.json --junitxml=junit.xml
    python scripts/ci_pg_test_fidelity.py check-skips --junit junit.xml \
      --report exact.json --discovery pg-tests.json --allowlist allowed.json \
      --output skip-check.json

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
        self.collection_skips: list[str] = []

    def pytest_collectreport(self, report: object) -> None:
        if report.skipped:
            self.collection_skips.append(str(report.nodeid))

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
    if not collector.items and not collector.collection_skips:
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
        "all_collected_nodeids": sorted(item.nodeid for item in collector.items),
        "collection_skips": sorted(collector.collection_skips),
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


def _nodeid_from_junit(case: ET.Element, collection_skips: set[str]) -> str:
    properties = case.find("properties")
    values = [] if properties is None else [
        prop.get("value") for prop in properties.findall("property")
        if prop.get("name") == "nodeid"
    ]
    if len(values) == 1 and isinstance(values[0], str) and values[0]:
        return values[0]
    if values:
        raise ValueError("JUnit testcase has duplicate or empty nodeid properties")
    # pytest's JUnit plugin writes a collection-level skip without properties:
    # classname="", name=<module stem>. Match it only when unambiguous.
    if case.find("skipped") is not None and case.get("classname") == "":
        matches = [nodeid for nodeid in collection_skips
                   if Path(nodeid).stem == case.get("name")]
        if len(matches) == 1:
            return matches[0]
    raise ValueError("JUnit testcase lacks an unambiguous exact nodeid")


def _nodeid_set(value: object, label: str) -> set[str]:
    if not isinstance(value, list) or any(not isinstance(n, str) or not n for n in value):
        raise ValueError(f"{label} must be a list of nonempty nodeids")
    if len(value) != len(set(value)):
        raise ValueError(f"{label} contains duplicate nodeids")
    return set(value)


def check_skips(junit_path: Path, report_path: Path, discovery_path: Path,
                allowlist_path: Path) -> dict[str, object]:
    """Fail closed on unknown, changed, duplicate, or stale exact-ID skips."""
    root = ET.parse(junit_path).getroot()
    cases = root.findall(".//testcase")
    if not cases:
        raise ValueError("JUnit XML has no testcases")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    if not isinstance(report, dict) or set(report) != {"version", "complete", "exitstatus", "events", "collect_errors", "executed_nodeids"}:
        raise ValueError("exact-ID report has missing or unexpected fields")
    if report["version"] != 1 or report["complete"] is not True:
        raise ValueError("exact-ID report is incomplete or has unsupported version")
    if not isinstance(report["exitstatus"], int) or not isinstance(report["collect_errors"], list):
        raise ValueError("exact-ID report has malformed status or collection errors")
    if report["collect_errors"]:
        raise ValueError("exact-ID report contains collection errors")
    if not isinstance(report["events"], list):
        raise ValueError("exact-ID report events must be a list")
    executed = _nodeid_set(report["executed_nodeids"], "executed_nodeids")
    discovery = json.loads(discovery_path.read_text(encoding="utf-8"))
    if not isinstance(discovery, dict) or set(discovery) != {
        "version", "collected_count", "all_collected_nodeids", "collection_skips",
        "pg_test_count", "pg_tests",
    } or discovery["version"] != 1:
        raise ValueError("discovery manifest is malformed or unsupported")
    collected = _nodeid_set(discovery["all_collected_nodeids"], "all_collected_nodeids")
    collection_skips = _nodeid_set(discovery["collection_skips"], "collection_skips")
    if discovery["collected_count"] != len(collected):
        raise ValueError("discovery collected_count differs from IDs")
    if not isinstance(discovery["pg_tests"], list) or discovery["pg_test_count"] != len(discovery["pg_tests"]):
        raise ValueError("discovery PostgreSQL test count differs from entries")
    pg_ids = _nodeid_set([entry.get("nodeid") for entry in discovery["pg_tests"]
                          if isinstance(entry, dict)], "pg_tests nodeids")
    if len(pg_ids) != len(discovery["pg_tests"]) or not pg_ids <= collected:
        raise ValueError("discovery PostgreSQL IDs are malformed or not collected")
    if executed != collected:
        raise ValueError("executed test IDs differ from discovery manifest")
    observed: set[tuple[str, str]] = set()
    observed_nodes: set[str] = set()
    xpasses: list[dict[str, str]] = []
    event_collection_skips: set[str] = set()
    for event in report["events"]:
        if not isinstance(event, dict) or set(event) != {"nodeid", "outcome", "reason"}:
            raise ValueError("malformed exact-ID event")
        nodeid, outcome, reason = event["nodeid"], event["outcome"], event["reason"]
        if not all(isinstance(value, str) and value for value in (nodeid, outcome, reason)):
            raise ValueError("exact-ID event needs nonempty strings")
        if nodeid in observed_nodes:
            raise ValueError(f"duplicate exact-ID event for {nodeid}")
        observed_nodes.add(nodeid)
        if outcome == "xpass":
            if nodeid not in executed:
                raise ValueError("XPASS event nodeid was not executed")
            xpasses.append(event)
            continue
        if outcome not in ("skip", "xfail"):
            raise ValueError(f"unknown exact-ID outcome {outcome!r}")
        if nodeid not in executed:
            event_collection_skips.add(nodeid)
        pair = (nodeid, reason)
        if pair in observed:
            raise ValueError(f"duplicate skipped testcase {nodeid}: {reason}")
        observed.add(pair)
    if event_collection_skips != collection_skips:
        raise ValueError("collection-level skips differ from discovery manifest")
    junit_ids = [_nodeid_from_junit(case, collection_skips) for case in cases]
    if len(junit_ids) != len(set(junit_ids)):
        raise ValueError("JUnit has duplicate testcase nodeids")
    if set(junit_ids) != executed | collection_skips:
        raise ValueError("JUnit testcase IDs differ from exact-ID report")
    junit_skipped_ids = {nodeid for case, nodeid in zip(cases, junit_ids)
                         if case.find("skipped") is not None}
    reported_skipped_ids = {nodeid for nodeid, _ in observed}
    if junit_skipped_ids != reported_skipped_ids:
        raise ValueError("JUnit skipped IDs differ from exact-ID report")
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
        "pytest_exitstatus": report["exitstatus"],
        "ok": report["exitstatus"] == 0 and not unexpected and not stale and not xpasses,
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
    check_parser.add_argument("--discovery", type=Path, required=True)
    check_parser.add_argument("--allowlist", type=Path, required=True)
    check_parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "discover":
            result = discover(args.tests_root)
        else:
            result = check_skips(args.junit, args.report, args.discovery, args.allowlist)
        args.output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        return 0 if result.get("ok", True) else 1
    except (OSError, ValueError, TypeError, ET.ParseError, json.JSONDecodeError) as exc:
        print(f"CI PostgreSQL test accounting failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
