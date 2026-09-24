"""No-database contracts for CI PostgreSQL discovery and skip accounting."""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

import pytest

from scripts.ci_pytest_report import _nodeid

SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "ci_pg_test_fidelity.py"
ROOT = SCRIPT.parent.parent


def _run(tmp_path: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, str(SCRIPT), *args],
        cwd=tmp_path,
        text=True,
        capture_output=True,
        check=False,
    )


def test_discovery_includes_new_pg_file_without_hand_maintained_list(tmp_path: Path) -> None:
    (tmp_path / "test_plain.py").write_text("def test_plain(): pass\n", encoding="utf-8")
    (tmp_path / "test_new_contract_pg.py").write_text(
        "def test_new_postgres_contract(): pass\n", encoding="utf-8"
    )
    output = tmp_path / "discovery.json"

    result = _run(tmp_path, "discover", "--tests-root", ".", "--output", str(output))

    assert result.returncode == 0, result.stderr
    data = json.loads(output.read_text(encoding="utf-8"))
    assert data["collected_count"] == 2
    assert data["pg_tests"] == [{
        "nodeid": "test_new_contract_pg.py::test_new_postgres_contract",
        "signals": ["pg_filename"],
    }]


def _pytest_run(tmp_path: Path, source: str, *extra: str) -> tuple[Path, Path, subprocess.CompletedProcess[str]]:
    test_file = tmp_path / "test_new_contract_pg.py"
    test_file.write_text(source, encoding="utf-8")
    junit = tmp_path / "junit.xml"
    report = tmp_path / "exact.json"
    env = os.environ.copy()
    env["PYTHONPATH"] = str(ROOT) + os.pathsep + env.get("PYTHONPATH", "")
    run = subprocess.run(
        [sys.executable, "-m", "pytest", "-q", str(test_file),
         "-p", "no:cacheprovider", "-p", "scripts.ci_pytest_report",
         f"--junitxml={junit}", f"--ci-report={report}", f"--rootdir={tmp_path}",
         f"--basetemp={tmp_path / '.pytest-internal'}",
         *extra],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )
    return junit, report, run


def _check(tmp_path: Path, junit: Path, report: Path, allowlist: Path) -> tuple[subprocess.CompletedProcess[str], dict[str, object]]:
    output = tmp_path / "check.json"
    discovery = tmp_path / "discovery.json"
    collected = _run(tmp_path, "discover", "--tests-root", ".",
                     "--output", str(discovery))
    assert collected.returncode == 0, collected.stdout + collected.stderr
    result = _run(tmp_path, "check-skips", "--junit", str(junit),
                  "--report", str(report), "--discovery", str(discovery),
                  "--allowlist", str(allowlist),
                  "--output", str(output))
    return result, json.loads(output.read_text(encoding="utf-8")) if output.exists() else {}


def test_unknown_junit_skip_fails_and_exact_allowlist_passes(tmp_path: Path) -> None:
    junit, report, run = _pytest_run(tmp_path,
        "import pytest\n"
        "def test_new_contract(): pytest.skip('database bootstrap unavailable')\n")
    assert run.returncode == 0, run.stdout + run.stderr
    case = ET.parse(junit).find(".//testcase")
    assert case is not None
    assert [(p.get("name"), p.get("value")) for p in case.findall("./properties/property")]
    assert ("nodeid", "test_new_contract_pg.py::test_new_contract") in [
        (p.get("name"), p.get("value")) for p in case.findall("./properties/property")]
    allowlist = tmp_path / "allowed.json"
    allowlist.write_text('{"version":1,"allowed_skips":[]}', encoding="utf-8")

    unknown, data = _check(tmp_path, junit, report, allowlist)
    assert unknown.returncode == 1, unknown.stderr
    assert data["unexpected"] == [{
        "nodeid": "test_new_contract_pg.py::test_new_contract",
        "reason": "Skipped: database bootstrap unavailable",
    }]

    allowlist.write_text(json.dumps({"version": 1, "allowed_skips": data["unexpected"]}),
                         encoding="utf-8")
    accepted, data = _check(tmp_path, junit, report, allowlist)
    assert accepted.returncode == 0, accepted.stderr
    assert data["ok"] is True

    allowlist.write_text(json.dumps({"version": 1, "allowed_skips": [{
        "nodeid": "test_new_contract_pg.py::test_new_contract",
        "reason": "different reason",
    }]}), encoding="utf-8")
    changed, data = _check(tmp_path, junit, report, allowlist)
    assert changed.returncode == 1, changed.stderr
    assert data["stale_allowlist"]


def test_module_skip_and_xfail_are_exact_events(tmp_path: Path) -> None:
    junit, report, run = _pytest_run(tmp_path,
        "import pytest\n"
        "pytestmark = pytest.mark.skip(reason='module hold')\n"
        "def test_module(): pass\n")
    assert run.returncode == 0, run.stdout + run.stderr
    events = json.loads(report.read_text(encoding="utf-8"))["events"]
    assert events == [{"nodeid": "test_new_contract_pg.py::test_module",
                       "outcome": "skip", "reason": "Skipped: module hold"}]

    junit, report, run = _pytest_run(tmp_path,
        "import pytest\n"
        "@pytest.mark.xfail(reason='known issue')\n"
        "def test_xfail(): assert False\n")
    assert run.returncode == 0, run.stdout + run.stderr
    events = json.loads(report.read_text(encoding="utf-8"))["events"]
    assert events == [{"nodeid": "test_new_contract_pg.py::test_xfail",
                       "outcome": "xfail", "reason": "known issue"}]


def test_collection_level_skip_is_reported_with_module_id(tmp_path: Path) -> None:
    junit, report, run = _pytest_run(tmp_path,
        "import pytest\n"
        "pytest.skip('module bootstrap hold', allow_module_level=True)\n")
    assert run.returncode == 5, run.stdout + run.stderr
    case = ET.parse(junit).find(".//testcase")
    assert case is not None
    assert case.attrib
    events = json.loads(report.read_text(encoding="utf-8"))["events"]
    assert events == [{"nodeid": "test_new_contract_pg.py",
                       "outcome": "skip", "reason": "Skipped: module bootstrap hold"}]
    allowlist = tmp_path / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed_skips": [{
        "nodeid": "test_new_contract_pg.py", "reason": "Skipped: module bootstrap hold",
    }]}), encoding="utf-8")
    checked, data = _check(tmp_path, junit, report, allowlist)
    assert checked.returncode == 1, checked.stderr
    assert data["skipped_count"] == 1
    assert data["pytest_exitstatus"] == 5


def test_xdist_controller_reports_one_exact_skip(tmp_path: Path) -> None:
    junit, report, run = _pytest_run(tmp_path,
        "import pytest\n"
        "def test_one(): pytest.skip('grouped hold')\n"
        "def test_two(): pass\n",
        "-n", "2", "--dist", "loadgroup")
    assert run.returncode == 0, run.stdout + run.stderr
    events = json.loads(report.read_text(encoding="utf-8"))["events"]
    assert events == [{"nodeid": "test_new_contract_pg.py::test_one",
                       "outcome": "skip", "reason": "Skipped: grouped hold"}]
    allowlist = tmp_path / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed_skips": [{
        "nodeid": "test_new_contract_pg.py::test_one", "reason": "Skipped: grouped hold",
    }]}), encoding="utf-8")
    checked, data = _check(tmp_path, junit, report, allowlist)
    assert checked.returncode == 0, checked.stderr
    assert data["ok"] is True


def test_xpass_and_missing_reports_fail_closed(tmp_path: Path) -> None:
    junit, report, run = _pytest_run(tmp_path,
        "import pytest\n"
        "@pytest.mark.xfail(reason='should fail')\n"
        "def test_xpass(): pass\n")
    assert run.returncode == 0, run.stdout + run.stderr
    allowlist = tmp_path / "allowed.json"
    allowlist.write_text('{"version":1,"allowed_skips":[]}', encoding="utf-8")
    checked, data = _check(tmp_path, junit, report, allowlist)
    assert checked.returncode == 1, checked.stderr
    assert data["xpasses"][0]["nodeid"] == "test_new_contract_pg.py::test_xpass"

    strict_junit, strict_report, strict_run = _pytest_run(tmp_path,
        "import pytest\n"
        "@pytest.mark.xfail(reason='strict unexpected pass', strict=True)\n"
        "def test_strict_xpass(): pass\n")
    assert strict_run.returncode == 1, strict_run.stdout + strict_run.stderr
    strict_checked, strict_data = _check(tmp_path, strict_junit, strict_report, allowlist)
    assert strict_checked.returncode == 1, strict_checked.stderr
    assert strict_data["xpasses"][0]["nodeid"] == "test_new_contract_pg.py::test_strict_xpass"

    strict_report.unlink()
    missing, _ = _check(tmp_path, strict_junit, strict_report, allowlist)
    assert missing.returncode == 2
    strict_report.write_text("{bad json", encoding="utf-8")
    malformed, _ = _check(tmp_path, strict_junit, strict_report, allowlist)
    assert malformed.returncode == 2
    strict_report.unlink()
    strict_junit.unlink()
    missing_junit, _ = _check(tmp_path, strict_junit, strict_report, allowlist)
    assert missing_junit.returncode == 2
    strict_junit.write_text("<broken", encoding="utf-8")
    malformed_junit, _ = _check(tmp_path, strict_junit, strict_report, allowlist)
    assert malformed_junit.returncode == 2


def test_xdist_group_suffix_normalization() -> None:
    assert _nodeid("tests/test_new_pg.py::test_case@param@postgres") == "tests/test_new_pg.py::test_case@param"
    assert _nodeid("tests/test_new_pg.py::test_case@tests/test_new_pg.py") == "tests/test_new_pg.py::test_case"
    assert _nodeid("tests/test_new_pg.py::test_case[email@example.com]") == "tests/test_new_pg.py::test_case[email@example.com]"


def test_same_skip_count_with_different_junit_and_report_ids_fails(tmp_path: Path) -> None:
    junit, report, run = _pytest_run(tmp_path,
        "import pytest\n"
        "def test_a(): pytest.skip('hold')\n"
        "def test_b(): pass\n")
    assert run.returncode == 0, run.stdout + run.stderr
    payload = json.loads(report.read_text(encoding="utf-8"))
    payload["events"][0]["nodeid"] = "test_new_contract_pg.py::test_b"
    report.write_text(json.dumps(payload), encoding="utf-8")
    allowlist = tmp_path / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed_skips": [{
        "nodeid": "test_new_contract_pg.py::test_b", "reason": "Skipped: hold",
    }]}), encoding="utf-8")
    checked, _ = _check(tmp_path, junit, report, allowlist)
    assert checked.returncode == 2
    assert "JUnit skipped IDs differ" in checked.stderr


def test_nonzero_status_and_discovery_mismatch_fail(tmp_path: Path) -> None:
    junit, report, run = _pytest_run(tmp_path,
        "import pytest\n"
        "def test_a(): pytest.skip('hold')\n")
    assert run.returncode == 0, run.stdout + run.stderr
    allowlist = tmp_path / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed_skips": [{
        "nodeid": "test_new_contract_pg.py::test_a", "reason": "Skipped: hold",
    }]}), encoding="utf-8")
    payload = json.loads(report.read_text(encoding="utf-8"))
    payload["exitstatus"] = 1
    report.write_text(json.dumps(payload), encoding="utf-8")
    checked, data = _check(tmp_path, junit, report, allowlist)
    assert checked.returncode == 1
    assert data["pytest_exitstatus"] == 1
    assert data["ok"] is False

    discovery = tmp_path / "wrong-discovery.json"
    discovery.write_text(json.dumps({
        "version": 1, "collected_count": 1,
        "all_collected_nodeids": ["test_new_contract_pg.py::test_b"],
        "collection_skips": [], "pg_test_count": 1,
        "pg_tests": [{"nodeid": "test_new_contract_pg.py::test_b",
                      "signals": ["pg_filename"]}],
    }), encoding="utf-8")
    wrong = _run(tmp_path, "check-skips", "--junit", str(junit),
                 "--report", str(report), "--discovery", str(discovery),
                 "--allowlist", str(allowlist), "--output", str(tmp_path / "wrong.json"))
    assert wrong.returncode == 2
    assert "executed test IDs differ" in wrong.stderr


def test_parametrized_junit_properties_bind_exact_nodeids(tmp_path: Path) -> None:
    junit, report, run = _pytest_run(tmp_path,
        "import pytest\n"
        "@pytest.mark.parametrize('value', [0, 1])\n"
        "def test_parameter(value):\n"
        "    if value == 0: pytest.skip('zero hold')\n")
    assert run.returncode == 0, run.stdout + run.stderr
    cases = ET.parse(junit).findall(".//testcase")
    ids = [p.get("value") for case in cases for p in case.findall("./properties/property")
           if p.get("name") == "nodeid"]
    assert set(ids) == {"test_new_contract_pg.py::test_parameter[0]",
                        "test_new_contract_pg.py::test_parameter[1]"}
    allowlist = tmp_path / "allowed.json"
    allowlist.write_text(json.dumps({"version": 1, "allowed_skips": [{
        "nodeid": "test_new_contract_pg.py::test_parameter[0]",
        "reason": "Skipped: zero hold",
    }]}), encoding="utf-8")
    checked, data = _check(tmp_path, junit, report, allowlist)
    assert checked.returncode == 0, checked.stderr
    assert data["ok"] is True


@pytest.mark.parametrize("outcome,source", [
    ("failure", "def test_case(): assert False\n"),
    ("error", "\n".join([
        "import pytest",
        "@pytest.fixture",
        "def broken(): raise RuntimeError('setup broke')",
        "def test_case(broken): pass",
        "",
    ])),
])
def test_junit_failure_or_error_rejects_zero_exit_report(
    tmp_path: Path, outcome: str, source: str,
) -> None:
    junit, report, run = _pytest_run(tmp_path, source)
    assert run.returncode == 1, run.stdout + run.stderr
    root = ET.parse(junit).getroot()
    case = root.find(".//testcase")
    assert case is not None
    assert case.find(outcome) is not None
    payload = json.loads(report.read_text(encoding="utf-8"))
    payload["exitstatus"] = 0
    report.write_text(json.dumps(payload), encoding="utf-8")
    allowlist = tmp_path / "allowed.json"
    allowlist.write_text('{"version":1,"allowed_skips":[]}', encoding="utf-8")

    checked, data = _check(tmp_path, junit, report, allowlist)
    assert checked.returncode == 1, checked.stderr
    assert data["ok"] is False
    assert data["pytest_exitstatus"] == 0
    assert data[f"junit_{outcome}s"] == 1


@pytest.mark.parametrize("field,value", [
    ("tests", "2"), ("skipped", "1"), ("failures", "1"),
    ("errors", "1"), ("tests", "bad"),
])
def test_junit_suite_totals_fail_closed(tmp_path: Path, field: str, value: str) -> None:
    junit, report, run = _pytest_run(tmp_path, "def test_case(): pass\n")
    assert run.returncode == 0, run.stdout + run.stderr
    root = ET.parse(junit).getroot()
    suite = root.find(".//testsuite")
    assert suite is not None
    suite.set(field, value)
    ET.ElementTree(root).write(junit, encoding="unicode")
    allowlist = tmp_path / "allowed.json"
    allowlist.write_text('{"version":1,"allowed_skips":[]}', encoding="utf-8")

    checked, _ = _check(tmp_path, junit, report, allowlist)
    assert checked.returncode == 2
    assert "JUnit suite" in checked.stderr
