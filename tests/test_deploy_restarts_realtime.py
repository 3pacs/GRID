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
2026-09-15/16 (#366, #367, #371, #374, #503). Unlike grid-scheduler,
grid-realtime is restarted unconditionally on the same gate as
grid-api/grid-hermes rather than behind a separate opt-in input: its unit
already carries `Restart=always`/`RestartSec=10`, SIGTERM triggers a graceful
shutdown that flushes the in-progress candle (ws_listener.py::main), every
candle write is an idempotent `INSERT ... ON CONFLICT DO NOTHING`
(flusher.py), the Binance feed already reconnects with backoff on any
disconnect (feeds/binance.py), and the Yahoo feed is a stateless HTTP poll
with no per-connection state (feeds/yahoo.py) -- a deploy-triggered restart
is the same bounded, self-healing event this daemon already tolerates on
every unplanned crash.

So these guard both halves: the restart happens, on the same gate as
grid-api/grid-hermes, and it is verified by reading the running process's
working directory -- `systemctl is-active` alone cannot distinguish "running"
from "running what we deployed", which is exactly how grid-hermes and
grid-scheduler went unnoticed for days.
"""

from __future__ import annotations

import os
import subprocess

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
def test_realtime_steps_are_gated_like_the_api_restart():
    """Same gate as grid-api/grid-hermes -- never the scheduler's opt-in gate.

    grid-realtime does not carry grid-scheduler's interrupted-long-pass risk
    (see the module docstring), so it belongs on the unconditional
    push/do_restart gate, not behind a separate activation input.
    """
    steps = _deploy_steps()
    api = _step_named(steps, "restart grid-api")
    for label in ("restart grid-realtime", "verify grid-realtime"):
        step = _step_named(steps, label)
        assert step.get("if") == api.get("if"), (
            f"'{label}' has gate {step.get('if')!r}, but 'Restart grid-api' has "
            f"{api.get('if')!r}. A workflow_dispatch with do_restart=false must "
            "not restart grid-realtime either, and a routine push must."
        )


@pytest.mark.unit
def test_do_restart_input_description_mentions_realtime():
    """The workflow_dispatch help text should not undersell what do_restart does."""
    with open(DEPLOY_YML, encoding="utf-8") as handle:
        workflow = yaml.safe_load(handle)
    # PyYAML's default (YAML 1.1) resolver parses the bare `on:` key as the
    # boolean `True`, not the string "on" -- a well-known GitHub Actions/
    # PyYAML gotcha. Every other test in this file only reaches into
    # `jobs.deploy.steps` and never hits this; this is the first assertion in
    # this repo's deploy tests to read the workflow_dispatch inputs block.
    workflow_dispatch = workflow[True]["workflow_dispatch"]
    description = workflow_dispatch["inputs"]["do_restart"]["description"]
    assert "grid-realtime" in description, (
        "do_restart now also restarts grid-realtime -- its description should say so"
    )


@pytest.mark.unit
def test_guard_fails_against_the_pre_fix_workflow():
    """Red/green, against the real previous file rather than a hand-made one.

    Reads deploy.yml as of the commit before this change and asserts the two
    load-bearing guards would have failed on it (there was no grid-realtime
    restart step at all). Skips where git history is not available (shallow
    clones without the parent, exported trees).
    """
    prev = subprocess.run(
        ["git", "show", "HEAD~1:.github/workflows/deploy.yml"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if prev.returncode != 0:
        pytest.skip("previous revision of deploy.yml not available in this checkout")

    old_steps = yaml.safe_load(prev.stdout)["jobs"]["deploy"]["steps"]

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
