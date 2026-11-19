"""The nightly Desk receipt job: its unit templates and its run script (#1164).

* The timer fires after the close in New York time, with one later retry.
* The service runs the script under a lock, from a pinned checkout, and no CI
  workflow installs it.
* The script, with a stub recorder and a stub hub reporter: posts the new head
  once when a receipt is written, posts nothing on a no-op retry, and reports
  a crash or a broken log with a nonzero exit.
"""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SYSTEMD = REPO / "deploy" / "systemd"
SCRIPT = REPO / "scripts" / "desk_receipts_run.sh"

RECORD_WRITTEN = ('{"ok": true, "written": true, "unknown_sources": ["thesis_log"], '
                  '"session": "2026-11-18", "evidence_cutoff": "2026-11-18T21:00:00+00:00"}')
RECORD_NOOP = RECORD_WRITTEN.replace('"written": true', '"written": false')
VERIFY_OK = '{"ok": true, "records": 2, "receipts": 1, "head_sha256": "abc123", "detail": null}'
VERIFY_BAD = '{"ok": false, "records": 2, "receipts": 0, "head_sha256": null, "detail": "record 1: edited"}'


def test_timer_fires_after_the_close_in_new_york_time_with_one_retry():
    timer = (SYSTEMD / "grid-desk-receipts.timer.template").read_text(encoding="utf-8")

    assert "OnCalendar=Mon..Fri *-*-* 16:20:00 America/New_York" in timer
    assert "OnCalendar=Mon..Fri *-*-* 19:20:00 America/New_York" in timer
    assert "Persistent=true" in timer and "Unit=grid-desk-receipts.service" in timer


def test_service_runs_the_script_under_a_lock_from_a_pinned_checkout():
    service = (SYSTEMD / "grid-desk-receipts.service.template").read_text(encoding="utf-8")

    assert "ExecStart=/usr/bin/flock -n /tmp/grid-desk-receipts.lock /bin/bash @CODE@/scripts/desk_receipts_run.sh" in service
    assert "WorkingDirectory=@CODE@" in service
    assert "DESK_RECEIPTS_DIR=/data/grid_ledgers/desk-receipts" in service
    assert "User=" not in service  # a user unit


def test_no_workflow_installs_the_job():
    for workflow in (REPO / ".github" / "workflows").glob("*.yml"):
        assert "grid-desk-receipts" not in workflow.read_text(encoding="utf-8"), workflow.name


@pytest.fixture
def run(tmp_path):
    """Run the script with a stub interpreter and a stub hub reporter."""

    def go(record_out: str, record_rc: int = 0, verify_out: str = VERIFY_OK, verify_rc: int = 0,
           reporter_on: bool = True):
        posts = tmp_path / "posts"
        posts.mkdir(exist_ok=True)
        stub_py = tmp_path / "py"
        # `-c` calls are the script's own JSON field reads: hand them to the real python.
        stub_py.write_text(
            "#!/usr/bin/env bash\n"
            'if [ "$1" = "-c" ]; then exec python3 "$@"; fi\n'
            'case "$3" in\n'
            f"  record) printf '%s\\n' '{record_out}'; exit {record_rc};;\n"
            f"  verify) printf '%s\\n' '{verify_out}'; exit {verify_rc};;\n"
            "esac\n", encoding="utf-8")
        reporter = tmp_path / "agent-report"
        reporter.write_text(f'#!/usr/bin/env bash\ncp "$3" "{posts}/$2.md"\n', encoding="utf-8")
        for path in (stub_py, reporter):
            path.chmod(path.stat().st_mode | stat.S_IEXEC)
        env = {**os.environ, "DESK_RECEIPTS_DIR": str(tmp_path / "log"),
               "DESK_FACTORY_LEDGER": "l", "DESK_FACTORY_ANCHOR": "a",
               "DESK_RECEIPTS_PYTHON": str(stub_py),
               "DESK_RECEIPTS_REPORT": str(reporter) if reporter_on else ""}
        done = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True,
                              timeout=60, check=False)
        return done, {p.name: p.read_text(encoding="utf-8") for p in posts.iterdir()}

    return go


def test_a_written_receipt_posts_its_head_and_unknown_sources_once(run):
    done, posts = run(RECORD_WRITTEN)

    assert done.returncode == 0, done.stdout + done.stderr
    assert list(posts) == ["desk-receipt-2026-11-18.md"]
    body = posts["desk-receipt-2026-11-18.md"]
    assert "Log head sha256: `abc123`" in body
    assert "Sources recorded as unknown: thesis_log" in body
    assert "Evidence cutoff: 2026-11-18T21:00:00+00:00" in body


def test_a_retry_that_writes_nothing_posts_nothing(run):
    done, posts = run(RECORD_NOOP)

    assert done.returncode == 0 and posts == {}
    assert "already recorded" in done.stdout


def test_a_refusal_exits_2_without_a_post(run):
    done, posts = run('{"ok": false, "refused": "session 2026-11-19 has not closed"}', record_rc=2)

    assert done.returncode == 2 and posts == {}
    assert "has not closed" in done.stdout


def test_a_crash_is_posted_and_exits_3(run):
    done, posts = run("Traceback (most recent call last)", record_rc=1)

    assert done.returncode == 3
    assert len(posts) == 1 and next(iter(posts)).startswith("desk-receipts-crash-")


def test_a_broken_log_is_posted_and_exits_4(run):
    done, posts = run(RECORD_NOOP, verify_out=VERIFY_BAD, verify_rc=1)

    assert done.returncode == 4
    name, body = next(iter(posts.items()))
    assert name.startswith("desk-receipts-broken-") and "record 1: edited" in body


def test_a_missing_hub_reporter_does_not_fail_the_run(run):
    done, posts = run(RECORD_WRITTEN, reporter_on=False)

    assert done.returncode == 0 and posts == {}
    assert "not posted" in done.stdout and "recorded 2026-11-18" in done.stdout
