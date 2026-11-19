"""The nightly Desk receipt job: its unit templates and its run script (#1164).

* The timer fires after the close in New York time, with one later retry.
* The service runs the script under a lock, from a pinned checkout, and no CI
  workflow installs it.
* The script, with a stub recorder and a stub hub reporter: posts each pending
  head and notes the delivery only when the post succeeded; a failed post
  exits 5 and the same session and head are posted on the retry; a head once
  delivered is not posted again; a crash or a broken log is a nonzero exit.
"""

from __future__ import annotations

import os
import stat
import subprocess
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parent.parent
SYSTEMD = REPO / "deploy" / "systemd-user"
SCRIPT = REPO / "scripts" / "desk_receipts_run.sh"

RECORD_WRITTEN = ('{"ok": true, "written": true, "recovered_anchor": false, '
                  '"unknown_sources": ["thesis_log"], '
                  '"session": "2026-11-18", "evidence_cutoff": "2026-11-18T21:00:00+00:00"}')
RECORD_NOOP = RECORD_WRITTEN.replace('"written": true', '"written": false')
VERIFY_OK = '{"ok": true, "records": 2, "receipts": 1, "head_sha256": "abc123", "detail": null}'
VERIFY_BAD = '{"ok": false, "records": 2, "receipts": 0, "head_sha256": null, "detail": "record 1: edited"}'
HEAD_18 = "a" * 64
HEAD_19 = "b" * 64
ROW_18 = f"2026-11-18\t{HEAD_18}\t2\t2026-11-18T21:00:00+00:00\tthesis_log"
ROW_19 = f"2026-11-19\t{HEAD_19}\t3\t2026-11-19T21:00:00+00:00\t-"


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
    """Run the script with a stub interpreter and a stub hub reporter.

    The stubs keep state in ``tmp_path`` across calls, so a second ``go`` is a
    restart of the job against the same log: ``witness-mark`` writes a note
    file, ``witness-status`` lists the receipts that have none, and the
    reporter fails for as many calls as ``reporter_fails`` says.
    """
    posts = tmp_path / "posts"
    notes = tmp_path / "notes"
    calls = tmp_path / "reporter-calls"
    for folder in (posts, notes):
        folder.mkdir()

    def go(record_out: str, record_rc: int = 0, verify_out: str = VERIFY_OK, verify_rc: int = 0,
           reporter_on: bool = True, receipts: tuple[str, ...] = (ROW_18,),
           reporter_fails: int = 0, reporter: str | None = None, status_rc: int = 0,
           mark_rc: int = 0):
        rows = tmp_path / "rows"
        rows.write_text("".join(row + "\n" for row in receipts), encoding="utf-8")
        stub_py = tmp_path / "py"
        # `-c` calls are the script's own JSON field reads: hand them to the real python.
        stub_py.write_text(
            "#!/usr/bin/env bash\n"
            'if [ "$1" = "-c" ]; then exec python3 "$@"; fi\n'
            'case "$3" in\n'
            f"  record) printf '%s\\n' '{record_out}'; exit {record_rc};;\n"
            f"  verify) printf '%s\\n' '{verify_out}'; exit {verify_rc};;\n"
            "  witness-status)\n"
            f'    [ {status_rc} -eq 0 ] || exit {status_rc}\n'
            f"    while IFS=$'\\t' read -r session head rest; do\n"
            f'      [ -e "{notes}/$session.$head" ] || printf \'%s\\t%s\\t%s\\n\' "$session" "$head" "$rest"\n'
            f'    done < "{rows}"; exit 0;;\n'
            "  witness-mark)\n"
            f'    [ {mark_rc} -eq 0 ] || exit {mark_rc}\n'
            f'    : > "{notes}/$7.$9"; exit 0;;\n'
            "esac\n", encoding="utf-8")
        stub_reporter = tmp_path / "agent-report"
        stub_reporter.write_text(
            "#!/usr/bin/env bash\n"
            f'echo "$2" >> "{calls}"\n'
            f'[ "$(wc -l < "{calls}")" -gt {reporter_fails} ] || exit 1\n'
            f'cp "$3" "{posts}/$2.md"\n', encoding="utf-8")
        for path in (stub_py, stub_reporter):
            path.chmod(path.stat().st_mode | stat.S_IEXEC)
        if reporter is None:
            reporter = str(stub_reporter) if reporter_on else ""
        env = {**os.environ, "DESK_RECEIPTS_DIR": str(tmp_path / "log"),
               "DESK_FACTORY_LEDGER": "l", "DESK_FACTORY_ANCHOR": "a",
               "DESK_RECEIPTS_PYTHON": str(stub_py),
               "DESK_RECEIPTS_REPORT": reporter}
        done = subprocess.run(["bash", str(SCRIPT)], env=env, capture_output=True, text=True,
                              timeout=60, check=False)
        return done, {p.name: p.read_text(encoding="utf-8") for p in posts.iterdir()}

    go.notes = lambda: sorted(p.name for p in notes.iterdir())
    go.calls = lambda: calls.read_text(encoding="utf-8").split() if calls.exists() else []
    return go


def test_a_written_receipt_posts_its_head_and_unknown_sources_once(run):
    done, posts = run(RECORD_WRITTEN)

    assert done.returncode == 0, done.stdout + done.stderr
    assert list(posts) == ["desk-receipt-2026-11-18.md"]
    body = posts["desk-receipt-2026-11-18.md"]
    assert f"Receipt head sha256: `{HEAD_18}`" in body
    assert "Records in the log up to this receipt: 2" in body
    assert "Sources recorded as unknown: thesis_log" in body
    assert "Session close (evidence cutoff): 2026-11-18T21:00:00+00:00" in body
    assert run.notes() == [f"2026-11-18.{HEAD_18}"]


def test_a_retry_with_nothing_pending_posts_nothing(run):
    run(RECORD_WRITTEN)

    done, posts = run(RECORD_NOOP)

    assert done.returncode == 0 and list(posts) == ["desk-receipt-2026-11-18.md"]
    assert "already recorded" in done.stdout and "nothing pending" in done.stdout
    assert run.calls() == ["desk-receipt-2026-11-18"]       # one post, ever


def test_a_failed_hub_post_is_not_a_success_and_the_retry_posts_the_same_head(run):
    """The defect: first post fails, exit 0; the retry finds the session
    recorded and never posts. Now the head stays pending until it is posted."""
    first, posts = run(RECORD_WRITTEN, reporter_fails=1)

    assert first.returncode == 5 and posts == {} and run.notes() == []
    assert "FAILED" in first.stdout and f"head {HEAD_18} stays pending" in first.stdout
    assert "recorded 2026-11-18" in first.stdout            # the receipt itself is safe

    retry, posts = run(RECORD_NOOP, reporter_fails=1)       # same session, nothing written

    assert retry.returncode == 0, retry.stdout + retry.stderr
    assert f"Receipt head sha256: `{HEAD_18}`" in posts["desk-receipt-2026-11-18.md"]
    assert run.notes() == [f"2026-11-18.{HEAD_18}"]
    assert run.calls() == ["desk-receipt-2026-11-18"] * 2   # the failed try and the good one

    third, _ = run(RECORD_NOOP, reporter_fails=1)
    assert third.returncode == 0 and len(run.calls()) == 2  # delivered: not posted again


def test_the_post_for_one_session_and_head_is_the_same_text_on_every_try(run):
    """Nothing in the post depends on when it was sent, so a repeated delivery
    of one (session, head) repeats the same bytes."""
    _, failed_then = run(RECORD_WRITTEN, mark_rc=2)         # posted, note refused: still pending
    first = failed_then["desk-receipt-2026-11-18.md"]

    _, again = run(RECORD_NOOP)

    assert again["desk-receipt-2026-11-18.md"] == first
    assert [line.split(":")[0] for line in first.splitlines() if line.startswith("- ")] == [
        "- Host", "- Session close (evidence cutoff)", "- Receipt head sha256",
        "- Records in the log up to this receipt", "- Sources recorded as unknown"]


def test_an_earlier_pending_head_is_delivered_with_tonights_oldest_first(run):
    done, posts = run(RECORD_WRITTEN.replace("2026-11-18", "2026-11-19"),
                      receipts=(ROW_18, ROW_19))

    assert done.returncode == 0, done.stdout + done.stderr
    assert run.calls() == ["desk-receipt-2026-11-18", "desk-receipt-2026-11-19"]
    assert f"`{HEAD_19}`" in posts["desk-receipt-2026-11-19.md"]
    assert "Sources recorded as unknown: none" in posts["desk-receipt-2026-11-19.md"]
    assert run.notes() == [f"2026-11-18.{HEAD_18}", f"2026-11-19.{HEAD_19}"]


def test_one_failed_post_does_not_stop_the_others_and_still_exits_5(run):
    done, posts = run(RECORD_WRITTEN, receipts=(ROW_18, ROW_19), reporter_fails=1)

    assert done.returncode == 5
    assert list(posts) == ["desk-receipt-2026-11-19.md"]
    assert run.notes() == [f"2026-11-19.{HEAD_19}"]         # only the delivered one is noted


def test_a_post_whose_delivery_note_is_refused_stays_pending(run):
    done, posts = run(RECORD_WRITTEN, mark_rc=2)

    assert done.returncode == 5 and run.notes() == []
    assert "delivery note was refused" in done.stdout


def test_unreadable_witness_status_is_exit_5_not_a_quiet_pass(run):
    done, posts = run(RECORD_WRITTEN, status_rc=2)

    assert done.returncode == 5 and posts == {}
    assert "delivery not attempted" in done.stdout


def test_a_reporter_that_is_not_installed_is_a_failed_delivery(run):
    done, posts = run(RECORD_WRITTEN, reporter="no-such-hub-reporter-anywhere")

    assert done.returncode == 5 and posts == {} and run.notes() == []
    assert "not found" in done.stdout and "stays pending" in done.stdout


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
    assert run.notes() == []                                # nothing is witnessed from a broken log


def test_delivery_turned_off_leaves_the_head_pending_and_says_so(run):
    done, posts = run(RECORD_WRITTEN, reporter_on=False)

    assert done.returncode == 0 and posts == {} and run.notes() == []
    assert "delivery is off" in done.stdout and "pending: 2026-11-18" in done.stdout
    assert "recorded 2026-11-18" in done.stdout


def test_an_anchor_finished_after_an_interruption_is_reported(run):
    done, _ = run(RECORD_NOOP.replace('"recovered_anchor": false', '"recovered_anchor": true'))

    assert done.returncode == 0
    assert "finished the anchor an interrupted run left out" in done.stdout
