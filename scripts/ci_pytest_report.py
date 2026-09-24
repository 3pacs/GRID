"""Controller-side pytest report with exact node IDs for CI skip accounting.

Load with ``-p scripts.ci_pytest_report --ci-report=path.json``. JUnit XML alone
does not preserve exact pytest node IDs or distinguish an XPASS reliably.
"""

from __future__ import annotations

import json
import re
from pathlib import Path

import pytest


def pytest_addoption(parser: object) -> None:
    parser.addoption("--ci-report", default=None, help="write exact pytest outcome JSON")


def _nodeid(nodeid: str) -> str:
    # xdist loadgroup appends @postgres or @tests/file.py to worker node IDs.
    return re.sub(r"@(?:postgres|tests/[^@]+\.py)$", "", nodeid)


def _reason(report: object) -> str:
    if hasattr(report, "wasxfail"):
        return str(report.wasxfail)
    longrepr = report.longrepr
    if isinstance(longrepr, tuple) and len(longrepr) == 3:
        return str(longrepr[2])
    return str(getattr(report, "longreprtext", "") or longrepr)


class _Reporter:
    def __init__(self, path: Path, controller: bool) -> None:
        self.path = path
        self.controller = controller
        self.events: list[dict[str, str]] = []
        self.collect_errors: list[str] = []
        self.executed_nodeids: set[str] = set()

    @pytest.hookimpl(hookwrapper=True)
    def pytest_runtest_makereport(self, item: object, call: object):
        outcome = yield
        report = outcome.get_result()
        report.user_properties.append(("nodeid", _nodeid(item.nodeid)))

    @pytest.hookimpl
    def pytest_collectreport(self, report: object) -> None:
        if not self.controller:
            return
        if report.skipped:
            self.events.append({"nodeid": _nodeid(report.nodeid),
                                "outcome": "skip", "reason": _reason(report)})
        elif report.failed:
            self.collect_errors.append(_nodeid(report.nodeid))

    @pytest.hookimpl
    def pytest_runtest_logreport(self, report: object) -> None:
        if not self.controller:
            return
        self.executed_nodeids.add(_nodeid(report.nodeid))
        if report.skipped:
            outcome = "xfail" if hasattr(report, "wasxfail") else "skip"
        elif report.passed and hasattr(report, "wasxfail"):
            outcome = "xpass"
        elif report.failed and str(report.longrepr).startswith("[XPASS(strict)]"):
            outcome = "xpass"
        else:
            return
        self.events.append({"nodeid": _nodeid(report.nodeid),
                            "outcome": outcome, "reason": _reason(report)})

    @pytest.hookimpl
    def pytest_sessionfinish(self, session: object, exitstatus: int) -> None:
        if not self.controller:
            return
        complete = not (session.shouldstop or session.shouldfail) and exitstatus not in (
            pytest.ExitCode.INTERRUPTED,
            pytest.ExitCode.INTERNAL_ERROR,
            pytest.ExitCode.USAGE_ERROR,
        )
        payload = {
            "version": 1,
            "complete": bool(complete),
            "exitstatus": int(exitstatus),
            "events": self.events,
            "collect_errors": self.collect_errors,
            "executed_nodeids": sorted(self.executed_nodeids),
        }
        self.path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def pytest_configure(config: object) -> None:
    output = config.getoption("--ci-report")
    if output:
        config.pluginmanager.register(
            _Reporter(Path(output), controller=not hasattr(config, "workerinput")),
            "ci-pytest-report-controller",
        )
