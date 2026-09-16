"""A deploy that writes code grid-realtime never loads is not a deploy.

`grid-realtime`'s unit runs ``ExecStart=/usr/bin/python3 -m
ingestion.realtime.ws_listener`` -- module invocation, but `-m` still resolves
against ``sys.path`` with the process's cwd prepended, which systemd sets from
``WorkingDirectory``. That is pinned to ``/home/grid/grid_v4/grid_repo``, a
checkout this workflow never writes to -- the exact same bug class already
fixed for grid-hermes (#487/#490) and grid-scheduler (#499). Before this
change, `deploy.yml` had no restart step for grid-realtime at all.

Confirmed live on 2026-09-16: grid-realtime's process (PID 16196) had been
running since 2026-07-29 with zero restarts, and its checkout at
`/home/grid/grid_v4/grid_repo` was last fast-forwarded around 2026-09-11
(commit 5facbdf0) -- both predate every dependency/bug-fix PR merged around
2026-09-15/16 (#366, #367, #371, #374, #503).

grid-realtime's restart is gated the SAME WAY as grid-scheduler -- behind
an explicit `activate_realtime` input plus
`acknowledge_realtime_interruption`, never on a routine push or on
`do_restart` (which only covers grid-api/grid-hermes). This is a deliberate
narrowing from an earlier version of this fix, which put grid-realtime on
the unconditional gate reasoning that `Restart=always` was sufficient
justification on its own. Closer review found a real, PRE-EXISTING risk
that reasoning glossed over: every candle write is `INSERT ... ON CONFLICT
(symbol, interval, ts) DO NOTHING` (`INSERT_SQL`, flusher.py) -- unchanged
by this PR, same as `main` -- which is idempotent (no duplicate/corrupt
row) but does NOT mean the most complete candle wins. A truncated
shutdown-flush candle and a later, complete candle for the same bucket
share the identical primary key, and whichever lands first (always the
truncated one) permanently blocks the other -- proven against real
Postgres in tests/test_realtime_shutdown_semantics.py::
test_truncated_candle_blocks_a_later_complete_candle_for_the_same_bucket.
A source-aware merge (`DO UPDATE`) was explored in an earlier round of
this same PR and reverted after review found real, unresolved correctness
gaps in it -- see docs/TODO-REALTIME-CANDLE-CORRECTNESS.md for that
history and docs/realtime_candle_merge_proposal_tests.py for the reverted
draft. This PR does not change candle persistence semantics at all -- the
truncation risk above happens on every unplanned crash-restart today too,
not something this PR introduces, but it is real and this PR does not fix
it, so -- combined with this being the first-ever automated restart of a
daemon otherwise untouched for months -- activation requires the same
explicit human acknowledgment grid-scheduler's gate does
(scripts/realtime_activation_gate.sh), rather than being treated as
already safe.

What IS unconditionally true regardless of the above, from this same round:
SIGTERM triggers a graceful shutdown that flushes the in-progress candle
(ws_listener.py::main), the Binance feed already reconnects with backoff on
any disconnect (feeds/binance.py), the Yahoo feed is a stateless HTTP poll
with no per-connection state (feeds/yahoo.py), and
ingestion/realtime/db_writer.py's DB-connection-concurrency bound closes a
real gap (a cancelled coroutine could release its slot while its executor
thread was still mid-write) found and fixed alongside this.

So these guard three things: the restart step exists and is gated
identically to grid-scheduler's activation (not to grid-api/grid-hermes's
unconditional restart), the activation gate script is actually consulted,
and the restart -- once it does run -- is verified by reading the running
process's working directory, not just `systemctl is-active` (which cannot
distinguish "running" from "running what we deployed", exactly how
grid-hermes and grid-scheduler went unnoticed for days).
"""

from __future__ import annotations

import os

import pytest

# A plain import, deliberately, not `pytest.importorskip("yaml")` -- see
# test_deploy_restarts_hermes.py for why an importorskip here would let every
# guard in this file vanish silently.
import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEPLOY_YML = os.path.join(REPO_ROOT, ".github", "workflows", "deploy.yml")


def _deploy_steps() -> list[dict]:
    with open(DEPLOY_YML, encoding="utf-8") as handle:
        workflow = yaml.safe_load(handle)
    return workflow["jobs"]["deploy"]["steps"]


def _step_named(steps: list[dict], needle: str) -> dict | None:
    for step in steps:
        if needle.lower() in (step.get("name") or "").lower():
            return step
    return None


@pytest.mark.unit
def test_deploy_restarts_grid_realtime():
    """Without this the daemon keeps running whatever it started with."""
    step = _step_named(_deploy_steps(), "restart grid-realtime")

    assert step is not None, (
        "deploy.yml has no step restarting grid-realtime. Its ExecStart is "
        "`python3 -m ingestion.realtime.ws_listener`, resolved against "
        "WorkingDirectory via sys.path, so a deploy that does not repoint "
        "and restart the daemon leaves it on whatever tree it booted with."
    )
    run = step.get("run", "")
    assert "systemctl restart grid-realtime" in run, (
        "the grid-realtime step does not actually restart the service"
    )


@pytest.mark.unit
def test_restart_repoints_realtime_at_the_deployed_tree():
    """Restarting alone changes nothing -- it reloads the same stale tree."""
    run = _step_named(_deploy_steps(), "restart grid-realtime").get("run", "")

    assert "grid-realtime.service.d" in run, (
        "no systemd drop-in directory for grid-realtime -- the restart would "
        "reload the same stale WorkingDirectory"
    )
    # The drop-in write itself moved into scripts/realtime_dropin_backup.sh
    # (backup-before-overwrite, unit-tested in
    # tests/test_realtime_dropin_backup.py) -- this step now only needs to
    # call that script with $DEPLOY_PATH as the working directory to hand to.
    assert "realtime_dropin_backup.sh" in run, (
        "the restart step no longer calls the drop-in backup script -- "
        "either it was removed or written inline again, losing the "
        "backup-before-overwrite behavior"
    )
    assert '"$DEPLOY_PATH"' in run, (
        "the drop-in backup script is not being told to point "
        "WorkingDirectory at $DEPLOY_PATH, so the daemon would still "
        "resolve ingestion.realtime.ws_listener against the old tree"
    )
    assert "daemon-reload" in run, (
        "systemd will not pick up a new drop-in without daemon-reload"
    )


@pytest.mark.unit
def test_realtime_verification_reads_the_running_cwd_not_just_liveness():
    """`is-active` cannot distinguish "running" from "running stale code"."""
    step = _step_named(_deploy_steps(), "verify grid-realtime")
    assert step is not None, "deploy.yml does not verify grid-realtime after restarting it"

    run = step.get("run", "")
    assert "/proc/" in run and "cwd" in run, (
        "the verify step does not read the running process's cwd -- checking "
        "only `systemctl is-active` reproduces the grid-hermes/grid-scheduler "
        "bug class this mirrors"
    )
    assert "DEPLOY_PATH" in run, "the verify step never compares cwd against $DEPLOY_PATH"
    assert "exit 1" in run, "the verify step cannot fail the deploy"


@pytest.mark.unit
def test_realtime_restart_happens_after_hermes_is_verified():
    """Don't bounce a third service when an earlier one did not come back."""
    names = [(s.get("name") or "") for s in _deploy_steps()]
    lowered = [n.lower() for n in names]

    hermes_verify = next(i for i, n in enumerate(lowered) if "verify grid-hermes" in n)
    realtime_restart = next(i for i, n in enumerate(lowered) if "restart grid-realtime" in n)

    assert realtime_restart > hermes_verify, (
        f"grid-realtime is restarted at step {realtime_restart}, before the "
        f"grid-hermes verify at step {hermes_verify}. A failed earlier "
        "restart should stop the job before bouncing another service."
    )


@pytest.mark.unit
def test_realtime_steps_are_gated_like_the_scheduler_activation():
    """Same opt-in shape as grid-scheduler -- never the unconditional api/hermes gate.

    Candle-merge correctness across a restart has known, tested gaps (see
    the module docstring and docs/TODO-REALTIME-CANDLE-CORRECTNESS.md), so
    grid-realtime activation requires the same explicit acknowledgment
    grid-scheduler's does, not the routine push/do_restart gate.
    """
    steps = _deploy_steps()
    scheduler_restart = _step_named(steps, "restart grid-scheduler")
    api_restart = _step_named(steps, "restart grid-api")
    for label in ("restart grid-realtime", "verify grid-realtime runs the deployed tree",
                  "verify grid-realtime is delivering fresh data"):
        step = _step_named(steps, label)
        assert step is not None, f"missing step: {label}"
        assert step.get("if") != api_restart.get("if"), (
            f"'{label}' has gate {step.get('if')!r}, matching 'Restart grid-api' "
            f"({api_restart.get('if')!r}) -- grid-realtime must not be on the "
            "unconditional push/do_restart gate."
        )
        assert "activate_realtime" in str(step.get("if")), (
            f"'{label}' has gate {step.get('if')!r}, which does not reference "
            "inputs.activate_realtime"
        )
    # Same shape as grid-scheduler's own activation gate (single explicit
    # input, not folded into do_restart).
    assert "activate_scheduler" in str(scheduler_restart.get("if"))


@pytest.mark.unit
def test_activate_realtime_input_exists_and_requires_acknowledgment():
    """Mirrors activate_scheduler/acknowledge_scheduler_interruption exactly."""
    with open(DEPLOY_YML, encoding="utf-8") as handle:
        workflow = yaml.safe_load(handle)
    # PyYAML's default (YAML 1.1) resolver parses the bare `on:` key as the
    # boolean `True`, not the string "on" -- a well-known GitHub Actions/
    # PyYAML gotcha. Every other test in this file only reaches into
    # `jobs.deploy.steps` and never hits this; this is the first assertion in
    # this repo's deploy tests to read the workflow_dispatch inputs block.
    workflow_dispatch = workflow[True]["workflow_dispatch"]
    inputs = workflow_dispatch["inputs"]

    assert "activate_realtime" in inputs, "no activate_realtime workflow_dispatch input"
    assert inputs["activate_realtime"].get("default") is False, (
        "activate_realtime must default to false -- never implied"
    )

    assert "acknowledge_realtime_interruption" in inputs, (
        "no acknowledge_realtime_interruption workflow_dispatch input"
    )
    assert inputs["acknowledge_realtime_interruption"].get("default") is False

    do_restart_description = inputs["do_restart"]["description"]
    assert "grid-realtime" not in do_restart_description, (
        "do_restart's description still claims to restart grid-realtime, but "
        "restart is now gated separately behind activate_realtime"
    )


@pytest.mark.unit
def test_realtime_activation_gate_script_is_actually_consulted():
    """The acknowledgment input must be enforced by the gate script, not just
    read -- otherwise a caller could set activate_realtime=true without
    acknowledge_realtime_interruption and nothing would stop the restart.
    """
    steps = _deploy_steps()
    gate_step = _step_named(steps, "require explicit interruption acknowledgment (grid-realtime)")
    assert gate_step is not None, "no gate step for grid-realtime activation"
    assert "activate_realtime" in str(gate_step.get("if"))
    run = gate_step.get("run", "")
    assert "realtime_activation_gate.sh" in run
    assert "acknowledge_realtime_interruption" in run

    restart_step = _step_named(steps, "restart grid-realtime")
    steps_by_name = [(s.get("name") or "") for s in steps]
    gate_index = next(i for i, n in enumerate(steps_by_name) if "require explicit interruption acknowledgment (grid-realtime)" in n.lower())
    restart_index = next(i for i, n in enumerate(steps_by_name) if "restart grid-realtime" in n.lower())
    assert gate_index < restart_index, (
        "the acknowledgment gate must run BEFORE the restart, not after"
    )


@pytest.mark.unit
def test_guard_fails_against_the_pre_fix_workflow():
    """Red/green, against a stable fixture rather than git history.

    Originally read deploy.yml via `git show HEAD~1:...` -- fragile, because
    HEAD~1 is only "the commit before this change" for the very first commit
    built on top of the true pre-fix baseline. Any later, ordinary commit on
    this branch (a fixup, a rebase, a second PR stacked on this one) shifts
    HEAD~1 to point at an already-partially-fixed intermediate state instead,
    silently breaking this test's red/green premise without changing
    anything this test is actually supposed to be guarding.

    tests/fixtures/deploy_pre_grid_realtime_fix.yml is a small, permanent,
    hand-written snippet with the same shape as deploy.yml before grid-realtime
    had any restart/verify step at all -- not a copy of history, just "the
    known-bad shape the guards below must catch". It never needs to change
    again regardless of how this branch's commit history evolves.
    """
    fixture_path = os.path.join(REPO_ROOT, "tests", "fixtures", "deploy_pre_grid_realtime_fix.yml")
    with open(fixture_path, encoding="utf-8") as handle:
        old_steps = yaml.safe_load(handle)["jobs"]["deploy"]["steps"]

    failures: list[str] = []

    restart = _step_named(old_steps, "restart grid-realtime")
    if restart is None:
        failures.append("no 'Restart grid-realtime' step")
    else:  # pragma: no cover - only if a future edit changes history
        run = restart.get("run", "")
        if "WorkingDirectory=$DEPLOY_PATH" not in run:
            failures.append("restart step does not repoint WorkingDirectory")

    verify = _step_named(old_steps, "verify grid-realtime")
    if verify is None:
        failures.append("no 'Verify grid-realtime' step")
    elif "/proc/" not in verify.get("run", ""):  # pragma: no cover
        failures.append("verify step does not read the running cwd")

    assert failures, (
        "the pre-fix deploy.yml satisfies every guard in this file, so none of "
        "them can be catching the regression they claim to. Re-derive what "
        "actually changed before trusting these tests."
    )

    # Both halves were missing, which is exactly the shipped-but-inert bug.
    assert len(failures) == 2, f"expected both guards to fail on the old file, got: {failures}"


@pytest.mark.unit
def test_freshness_probe_query_is_bounded_by_an_indexed_column():
    """The freshness probe's `WHERE created_at > :restart_ts` has no index
    to use on its own (realtime_candles has no index on created_at) and
    forces a full sequential scan -- confirmed in production to exceed
    Postgres's statement_timeout against the real 2M+-row table (only ever
    exercised against small CI tables before that). Guards against silently
    dropping the `ts > :ts_floor` bound that makes this use
    idx_rt_candles_ts instead (validated via EXPLAIN ANALYZE directly
    against production: Parallel Seq Scan, cost ~39911 -> Index Scan using
    idx_rt_candles_ts, <1ms -- no new index, no settings change).
    """
    step = _step_named(_deploy_steps(), "verify grid-realtime is delivering fresh data")
    assert step is not None, "deploy.yml does not verify grid-realtime data freshness"

    run = step.get("run", "")
    assert "ts_floor" in run, (
        "the freshness probe no longer bounds its query by ts -- this will "
        "force a full sequential scan on realtime_candles again (no index "
        "on created_at), which times out against the real production table"
    )
    assert "idx_rt_candles_ts" in run or "WHERE ts >" in run, (
        "the freshness probe's query no longer filters on ts before "
        "created_at"
    )
