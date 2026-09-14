"""A deploy that writes code grid-hermes never loads is not a deploy.

`grid-hermes`'s unit runs ``ExecStart=/usr/bin/python3 scripts/hermes_operator.py``
-- a RELATIVE path, resolved against ``WorkingDirectory``. That is pinned to the
Hermes tree, which no deploy writes to. `deploy.yml` writes `grid_release` and
restarted only `grid-api`, so every Hermes-side change shipped and then sat
inert.

Measured on 2026-09-14: the live daemon (MainPID 3899061, started 2026-09-11
15:11) had cwd `/data/grid_v4/grid_repo` at `5facbdf0`, with **zero**
occurrences of `DISTINCT_SCAN_SLICE_HOURS` (#487, merged 2026-09-13) or
`RESOLUTION_SCAN_BUDGET_SECONDS` (#490, merged 2026-09-14). Both PRs were green
through their deploys and neither was running. The ~400s unsliced scan that
#487 exists to remove was still what executed every cycle.

What makes this worth a test rather than a one-time server fix: nothing about
the green deploy was false. The code *had* reached `grid_release`. The gap was
between "deployed" and "running", and no signal in the pipeline distinguished
them -- which is why it went unnoticed for a day across four deploys.

So these guard both halves: the restart happens, and it is verified by reading
the running process's working directory. `systemctl is-active` alone is not
enough -- it reported green throughout the entire period the daemon was running
three-day-old code.
"""

from __future__ import annotations

import os
import subprocess

import pytest

# A plain import, deliberately, not `pytest.importorskip("yaml")`.
#
# PyYAML reaches CI only transitively (prefect depends on it) and is not
# declared by this repo, so an importorskip here would make every guard in this
# file vanish silently the day that chain shifts -- green CI, zero coverage,
# and no signal that the deploy-integrity checks stopped running. That is the
# same shape as the bug the file exists to catch, so it fails loudly instead.
# requirements.txt now declares PyYAML for exactly this reason.
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
def test_deploy_yml_is_valid_yaml():
    """A syntax error here fails every deploy, so check it before anything else."""
    steps = _deploy_steps()
    assert steps, "deploy job has no steps"


@pytest.mark.unit
def test_deploy_restarts_grid_hermes():
    """Without this the daemon keeps running whatever it started with."""
    step = _step_named(_deploy_steps(), "restart grid-hermes")

    assert step is not None, (
        "deploy.yml has no step restarting grid-hermes. The unit's ExecStart is "
        "a relative path resolved against WorkingDirectory, so a deploy that "
        "does not repoint and restart the daemon leaves it on whatever tree it "
        "booted with -- how #487 and #490 both shipped green and stayed inert."
    )
    run = step.get("run", "")
    assert "systemctl restart grid-hermes" in run, (
        "the grid-hermes step does not actually restart the service"
    )


@pytest.mark.unit
def test_restart_repoints_hermes_at_the_deployed_tree():
    """Restarting alone changes nothing -- it reloads the same stale tree.

    This is the half I got wrong when first recommending a fix: a bare
    ``systemctl restart grid-hermes`` brings the daemon back on exactly the
    code it was already running. The drop-in is what makes the restart mean
    something.
    """
    run = _step_named(_deploy_steps(), "restart grid-hermes").get("run", "")

    assert "grid-hermes.service.d" in run, (
        "no systemd drop-in directory for grid-hermes -- the restart would "
        "reload the same stale WorkingDirectory"
    )
    assert "WorkingDirectory=$DEPLOY_PATH" in run, (
        "the drop-in does not point WorkingDirectory at $DEPLOY_PATH, so the "
        "daemon still resolves scripts/hermes_operator.py against the old tree"
    )
    assert "daemon-reload" in run, (
        "systemd will not pick up a new drop-in without daemon-reload"
    )


@pytest.mark.unit
def test_hermes_verification_reads_the_running_cwd_not_just_liveness():
    """`is-active` was green for three days while the code was stale.

    The only signal that distinguishes "running" from "running what we
    deployed" is the process's own working directory, so the verify step has to
    read it and compare.
    """
    step = _step_named(_deploy_steps(), "verify grid-hermes")
    assert step is not None, "deploy.yml does not verify grid-hermes after restarting it"

    run = step.get("run", "")
    assert "/proc/" in run and "cwd" in run, (
        "the verify step does not read the running process's cwd. Checking only "
        "`systemctl is-active` reproduces the bug: it reported active for three "
        "days while grid-hermes ran code from 2026-09-11."
    )
    assert "DEPLOY_PATH" in run, "the verify step never compares cwd against $DEPLOY_PATH"
    assert "exit 1" in run, "the verify step cannot fail the deploy"


@pytest.mark.unit
def test_hermes_restart_happens_after_the_api_is_verified_healthy():
    """Don't bounce a second service when the first one did not come back."""
    names = [(s.get("name") or "") for s in _deploy_steps()]
    lowered = [n.lower() for n in names]

    api_verify = next(i for i, n in enumerate(lowered) if "verify running api" in n)
    hermes_restart = next(i for i, n in enumerate(lowered) if "restart grid-hermes" in n)

    assert hermes_restart > api_verify, (
        f"grid-hermes is restarted at step {hermes_restart}, before the API "
        f"health verify at step {api_verify}. A failed API deploy should stop "
        "before taking a second service down."
    )


@pytest.mark.unit
def test_hermes_steps_are_gated_like_the_api_restart():
    """Same gate, or a no-restart dispatch would still bounce the daemon."""
    steps = _deploy_steps()
    api = _step_named(steps, "restart grid-api")
    for label in ("restart grid-hermes", "verify grid-hermes"):
        step = _step_named(steps, label)
        assert step.get("if") == api.get("if"), (
            f"'{label}' has gate {step.get('if')!r}, but 'Restart grid-api' has "
            f"{api.get('if')!r}. A workflow_dispatch with do_restart=false must "
            "not restart grid-hermes either."
        )


@pytest.mark.unit
def test_guard_fails_against_the_pre_fix_workflow():
    """Red/green, against the real previous file rather than a hand-made one.

    Reads deploy.yml as of the commit before this change and asserts the two
    load-bearing guards would have failed on it. Skips where git history is not
    available (shallow clones without the parent, exported trees).
    """
    prev = subprocess.run(
        ["git", "show", "HEAD~1:.github/workflows/deploy.yml"],
        cwd=REPO_ROOT, capture_output=True, text=True,
    )
    if prev.returncode != 0:
        pytest.skip("previous revision of deploy.yml not available in this checkout")

    old_steps = yaml.safe_load(prev.stdout)["jobs"]["deploy"]["steps"]

    # Run the real predicates this file enforces, against the real old content.
    # Asserting merely that the step names are absent would be a weaker claim
    # than the guards actually make.
    failures: list[str] = []

    restart = _step_named(old_steps, "restart grid-hermes")
    if restart is None:
        failures.append("no 'Restart grid-hermes' step")
    else:  # pragma: no cover - only if a future edit changes history
        run = restart.get("run", "")
        if "WorkingDirectory=$DEPLOY_PATH" not in run:
            failures.append("restart step does not repoint WorkingDirectory")

    verify = _step_named(old_steps, "verify grid-hermes")
    if verify is None:
        failures.append("no 'Verify grid-hermes' step")
    elif "/proc/" not in verify.get("run", ""):  # pragma: no cover
        failures.append("verify step does not read the running cwd")

    assert failures, (
        "the pre-fix deploy.yml satisfies every guard in this file, so none of "
        "them can be catching the regression they claim to. Re-derive what "
        "actually changed before trusting these tests."
    )

    # Both halves were missing, which is exactly the shipped-but-inert bug.
    assert len(failures) == 2, f"expected both guards to fail on the old file, got: {failures}"
