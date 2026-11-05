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

So these guard both halves for an already active/enabled daemon: the conditional
restart happens, and it is verified by reading
the running process's working directory. `systemctl is-active` alone is not
enough -- it reported green throughout the entire period the daemon was running
three-day-old code.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

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


def _effective_run(step: dict) -> str:
    """The step's own `run:` text, plus scripts/deploy_verify_release_tree.sh's
    content when the step delegates to it.

    2026-09-22: the cwd-vs-deploy-path check this file guards moved out of
    each verify step's inline `run:` block and into one shared script (also
    fixing a real bug -- the old inline check compared the literal
    $DEPLOY_PATH string, which can never match once it's a symlink). The
    logic these guards care about still runs, just not inline any more, so
    the guards read the delegated script's content too rather than being
    fooled by the refactor into passing on a step that no longer checks
    anything.
    """
    run = step.get("run", "")
    if "deploy_verify_release_tree.sh" in run:
        script_path = os.path.join(
            REPO_ROOT, "scripts", "deploy_verify_release_tree.sh"
        )
        with open(script_path, encoding="utf-8") as handle:
            run += "\n" + handle.read()
    return run


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
    assert "systemctl try-restart grid-hermes" in run, (
        "the grid-hermes step does not conditionally restart an existing service"
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
    assert step is not None, (
        "deploy.yml does not verify grid-hermes after restarting it"
    )

    run = _effective_run(step)
    assert "/proc/" in run and "cwd" in run, (
        "the verify step does not read the running process's cwd. Checking only "
        "`systemctl is-active` reproduces the bug: it reported active for three "
        "days while grid-hermes ran code from 2026-09-11."
    )
    assert "DEPLOY_PATH" in run, (
        "the verify step never compares cwd against $DEPLOY_PATH"
    )
    assert "exit 1" in run, "the verify step cannot fail the deploy"


@pytest.mark.unit
def test_hermes_restart_happens_after_the_api_is_verified_healthy():
    """Don't bounce a second service when the first one did not come back."""
    names = [(s.get("name") or "") for s in _deploy_steps()]
    lowered = [n.lower() for n in names]

    api_verify = next(i for i, n in enumerate(lowered) if "verify running api" in n)
    hermes_restart = next(
        i for i, n in enumerate(lowered) if "restart grid-hermes" in n
    )

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
    restart = _step_named(steps, "restart grid-hermes")
    assert restart.get("if") == api.get("if")
    assert restart.get("id") == "hermes_restart"
    verify = _step_named(steps, "verify grid-hermes")
    assert verify.get("if") == (
        "${{ (github.event_name == 'push' || inputs.do_restart == true) "
        "&& steps.hermes_restart.outputs.verify == 'true' }}"
    ), "Hermes verification must use the same restart gate and observed outcome"


@pytest.mark.unit
def test_guard_fails_against_the_pre_fix_workflow():
    """Red/green, against a stable fixture rather than git history.

    Originally read deploy.yml via `git show HEAD~1:...` -- fragile, because
    HEAD~1 is only "the commit before this change" for the very first commit
    built on top of the true pre-fix baseline. Any later, ordinary commit on
    this branch (a fixup, a rebase, a second PR stacked on this one, or --
    concretely, what actually broke this -- 2026-09-22's cwd-check refactor,
    which left grid-hermes's own check correct but landed as a new commit on
    top of one where it already was) shifts HEAD~1 to point at an
    already-fixed state instead, silently breaking this test's red/green
    premise without changing anything this test is actually supposed to be
    guarding. tests/fixtures/deploy_pre_grid_hermes_fix.yml is a small,
    permanent, hand-written snippet with the same shape as deploy.yml before
    grid-hermes had any restart/verify step at all -- not a copy of history,
    just "the known-bad shape the guards below must catch". It never needs to
    change again regardless of how this branch's commit history evolves --
    the same fix already applied to the equivalent grid-realtime guard in
    tests/test_deploy_restarts_realtime.py, for exactly this reason.
    """
    fixture_path = os.path.join(
        REPO_ROOT, "tests", "fixtures", "deploy_pre_grid_hermes_fix.yml"
    )
    with open(fixture_path, encoding="utf-8") as handle:
        old_steps = yaml.safe_load(handle)["jobs"]["deploy"]["steps"]

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
    assert len(failures) == 2, (
        f"expected both guards to fail on the old file, got: {failures}"
    )


# All shell commands with service/file effects resolve to this rejecting fake.
# It models systemctl's try-restart semantics (including inactive no-op), not
# an invented restart authority based on UnitFileState. Absolute /etc paths
# are recorded in a fixture JSON file; they are never opened or mutated.
_FAKE_COMMAND = r"""
import json
import os
from pathlib import Path
import sys

path = Path(os.environ["FAKE_STATE"])
state = json.loads(path.read_text())
cmd = Path(sys.argv[0]).name
args = sys.argv[1:]
if cmd == "sudo":
    if args and args[0] == "-n":
        args = args[1:]
    if not args:
        raise SystemExit("fake sudo needs a command")
    cmd, args = args[0], args[1:]
state["calls"].append([cmd, *args])
result = 0
out = ""
if cmd == "systemctl":
    if args[0] == "show" and "--property=LoadState" in args:
        index = state["query_count"]
        state["query_count"] += 1
        item = state["states"][min(index, len(state["states"]) - 1)]
        if isinstance(item, dict):
            result = item.get("status", 0)
            out = item.get("raw", "")
        else:
            load, active, sub, unit = item.split("|")
            out = (f"UnitFileState={unit}\nActiveState={active}\n"
                   f"LoadState={load}\nSubState={sub}\n")
    elif args[0] in ("try-restart", "restart") and args[1:] == ["grid-hermes"]:
        index = min(max(state["query_count"] - 1, 0), len(state["states"]) - 1)
        current = state.get("at_action", state["states"][index])
        active = isinstance(current, str) and current.split("|")[1] == "active"
        if args[0] == "restart" or active:
            state["activations"] += 1
        else:
            state["noop"] = True
        result = state.get("action_status", 0)
    elif args == ["daemon-reload"]:
        state["mutations"].append("daemon-reload")
    elif args == ["show", "-p", "MainPID", "--value", "grid-hermes"]:
        out = str(state["pid"]) + "\n"
    elif args == ["is-active", "grid-hermes"]:
        out = "active\n"
    elif args[0] == "status":
        out = "fake status\n"
    else:
        raise SystemExit(f"unrecognized fake systemctl: {args!r}")
elif cmd in ("mkdir", "tee"):
    allowed = {"/etc/systemd/system/grid-hermes.service.d",
               "/etc/systemd/system/grid-hermes.service.d/zz-release-worktree.conf"}
    target = args[-1]
    if target not in allowed:
        raise SystemExit(f"unexpected mutation target: {args!r}")
    value = sys.stdin.read() if cmd == "tee" else ""
    state["mutations"].append([cmd, target, value])
elif cmd == "chmod":
    if args != ["+x", "scripts/deploy_verify_release_tree.sh"]:
        raise SystemExit(f"unexpected chmod: {args!r}")
elif cmd == "readlink":
    if args == [f"/proc/{state['pid']}/cwd"] and str(state["pid"]) == "12345":
        out = state["cwd"] + "\n"
    elif len(args) == 2 and args[0] == "-f":
        out = args[1] + "\n"
    else:
        result = 1
elif cmd == "git":
    if args == ["-C", state["deploy"], "rev-parse", "HEAD"]:
        out = state["sha"] + "\n"
    else:
        raise SystemExit(f"unexpected git: {args!r}")
elif cmd in ("sleep", "journalctl"):
    raise SystemExit(f"unexpected wait/diagnostic: {cmd}")
else:
    raise SystemExit(f"unexpected command (never forwarded): {cmd} {args!r}")
path.write_text(json.dumps(state))
sys.stdout.write(out)
raise SystemExit(result)
"""


_ELIGIBLE = "loaded|active|running|enabled"
_HELD = "loaded|inactive|dead|disabled"
_EXPECTED_SHA = "0123456789abcdef0123456789abcdef01234567"


def _run_shell(
    tmp_path,
    *,
    states=None,
    restart_run=None,
    verify_run=None,
    event="push",
    do_restart=False,
    at_action=None,
    action_status=0,
    pid=12345,
    cwd=None,
    sha=_EXPECTED_SHA,
):
    """Execute exact YAML blocks/helper with only rejecting fake commands."""
    tmp_path.mkdir(parents=True, exist_ok=True)
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    command = f"#!{sys.executable}\n" + _FAKE_COMMAND
    for name in (
        "sudo",
        "systemctl",
        "mkdir",
        "tee",
        "chmod",
        "readlink",
        "git",
        "sleep",
        "journalctl",
    ):
        leaf = fake_bin / name
        leaf.write_text(command)
        leaf.chmod(0o700)
    (fake_bin / "bash").symlink_to("/bin/bash")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    helper = scripts / "deploy_verify_release_tree.sh"
    helper.write_bytes(
        (Path(REPO_ROOT) / "scripts/deploy_verify_release_tree.sh").read_bytes()
    )
    helper.chmod(0o700)
    deploy = str(tmp_path / "release")
    state = {
        "states": states or [_ELIGIBLE],
        "query_count": 0,
        "calls": [],
        "activations": 0,
        "mutations": [],
        "pid": pid,
        "cwd": cwd if cwd is not None else deploy,
        "deploy": deploy,
        "sha": sha,
        "action_status": action_status,
    }
    if at_action is not None:
        state["at_action"] = at_action
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(state))
    output = tmp_path / "output"
    output.write_text("")
    steps = _deploy_steps()
    restart = _step_named(steps, "restart grid-hermes")
    verify = _step_named(steps, "verify grid-hermes")
    env = {
        "PATH": str(fake_bin),
        "FAKE_STATE": str(state_path),
        "DEPLOY_PATH": deploy,
        "GITHUB_OUTPUT": str(output),
        "LC_ALL": "C",
    }
    results = []
    if event == "push" or do_restart:
        run = restart_run if restart_run is not None else restart["run"]
        results.append(
            subprocess.run(
                ["/bin/bash", "-e", "-o", "pipefail", "-c", run],
                cwd=tmp_path,
                env=env,
                capture_output=True,
                text=True,
                timeout=15,
            )
        )
        if results[-1].returncode == 0 and output.read_text().splitlines()[-1:] == [
            "verify=true"
        ]:
            run = verify_run if verify_run is not None else verify["run"]
            run = run.replace("${{ github.sha }}", _EXPECTED_SHA)
            results.append(
                subprocess.run(
                    ["/bin/bash", "-e", "-o", "pipefail", "-c", run],
                    cwd=tmp_path,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=15,
                )
            )
    state = json.loads(state_path.read_text())
    state["output"] = output.read_text()
    state["results"] = results
    return state


@pytest.mark.unit
def test_active_enabled_hermes_restarts_and_verifies_real_helper(tmp_path):
    state = _run_shell(tmp_path)
    assert [r.returncode for r in state["results"]] == [0, 0]
    assert state["activations"] == 1
    assert "WorkingDirectory=" + state["deploy"] in state["mutations"][1][2]
    assert state["output"].splitlines() == ["verify=false", "verify=true"]
    assert "exactly matches expected" in state["results"][-1].stdout
    assert ["systemctl", "try-restart", "grid-hermes"] in state["calls"]
    assert ["git", "-C", state["deploy"], "rev-parse", "HEAD"] in state["calls"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "state",
    [
        _HELD,
        "loaded|inactive|dead|enabled",
        "loaded|active|running|disabled",
        "masked|inactive|dead|masked",
        "masked|active|running|masked",
        "loaded|active|running|masked-runtime",
        "loaded|inactive|dead|enabled-runtime",
        "loaded|active|running|static",
    ],
)
def test_held_hermes_never_writes_dropin_or_activates(tmp_path, state):
    observed = _run_shell(tmp_path, states=[state])
    assert [r.returncode for r in observed["results"]] == [0]
    assert observed["activations"] == 0
    assert observed["mutations"] == []
    assert observed["output"] == "verify=false\n"
    assert "state preserved" in observed["results"][0].stdout


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad",
    [
        {
            "status": 1,
            "raw": (
                "LoadState=loaded\nActiveState=inactive\n"
                "SubState=dead\nUnitFileState=disabled\n"
            ),
        },
        {"raw": ""},
        {"raw": "LoadState=loaded\nActiveState=active\nSubState=running\n"},
        {
            "raw": (
                "LoadState=loaded\nActiveState=active\nSubState=running\n"
                "UnitFileState=enabled\nUnexpected=yes\n"
            )
        },
        {
            "raw": (
                "LoadState=loaded\nActiveState=active\nActiveState=active\n"
                "SubState=running\nUnitFileState=enabled\n"
            )
        },
        {
            "raw": (
                "LoadState=loaded\nActiveState=\n"
                "SubState=running\nUnitFileState=enabled\n"
            )
        },
        "not-found|inactive|dead|disabled",
        "loaded|failed|failed|enabled",
        "loaded|activating|start|enabled",
        "loaded|active|exited|enabled",
        "loaded|active|running|unknown",
        "masked|active|running|enabled",
    ],
)
def test_uncertain_state_fails_without_mutation(tmp_path, bad):
    state = _run_shell(tmp_path, states=[bad])
    assert state["results"][0].returncode != 0
    assert state["activations"] == 0
    assert state["mutations"] == []
    assert state["output"] == "verify=false\n"


@pytest.mark.unit
@pytest.mark.parametrize("held", [_HELD, "loaded|active|running|disabled"])
@pytest.mark.parametrize("query", [1, 2, 3, 4])
def test_late_stop_or_disable_is_preserved(tmp_path, held, query):
    state = _run_shell(tmp_path, states=[_ELIGIBLE] * query + [held])
    assert all(r.returncode == 0 for r in state["results"])
    assert state["activations"] == (1 if query >= 3 else 0)
    assert bool(state["mutations"]) == (query >= 2)
    assert not any(call[0] == "git" for call in state["calls"])
    assert "preserved" in state["results"][-1].stdout
    assert "exactly matches expected" not in state["results"][-1].stdout


@pytest.mark.unit
@pytest.mark.parametrize("query", [1, 2, 3, 4])
def test_late_query_failure_never_claims_verified_tree(tmp_path, query):
    state = _run_shell(tmp_path, states=[_ELIGIBLE] * query + [{"status": 1}])
    assert state["results"][-1].returncode != 0
    assert not any(call[0] == "git" for call in state["calls"])
    assert state["activations"] == (1 if query >= 3 else 0)


@pytest.mark.unit
def test_inactive_race_at_try_restart_is_a_noop_and_tree_unverified(tmp_path):
    state = _run_shell(tmp_path, states=[_ELIGIBLE] * 3 + [_HELD], at_action=_HELD)
    assert state["results"][0].returncode == 0
    assert state["activations"] == 0
    assert state["noop"] is True
    assert state["output"] == "verify=false\n"
    assert "tree unverified" in state["results"][0].stdout


@pytest.mark.unit
def test_failed_try_restart_fails_and_never_verifies(tmp_path):
    state = _run_shell(tmp_path, action_status=1)
    assert state["results"][0].returncode != 0
    assert len(state["results"]) == 1
    assert state["output"] == "verify=false\n"


@pytest.mark.unit
@pytest.mark.parametrize(
    "wrong",
    [
        {"pid": ""},
        {"pid": 0},
        {"pid": 99999},
        {"cwd": "/different/fixture/tree"},
        {"sha": "fedcba9876543210fedcba9876543210fedcba98"},
    ],
)
def test_actual_helper_rejects_wrong_pid_cwd_or_exact_sha(tmp_path, wrong):
    state = _run_shell(tmp_path, **wrong)
    assert state["results"][0].returncode == 0
    assert state["results"][-1].returncode != 0
    assert "::error::deploy_verify_release_tree" in state["results"][-1].stderr


@pytest.mark.unit
def test_no_restart_dispatch_has_zero_service_commands(tmp_path):
    state = _run_shell(tmp_path, event="workflow_dispatch", do_restart=False)
    assert state["calls"] == []
    assert state["results"] == []
    assert state["mutations"] == []
    assert state["activations"] == 0


@pytest.mark.unit
def test_restart_dispatch_uses_the_same_state_gate(tmp_path):
    state = _run_shell(
        tmp_path, event="workflow_dispatch", do_restart=True, states=[_HELD]
    )
    assert [r.returncode for r in state["results"]] == [0]
    assert state["activations"] == 0
    assert state["mutations"] == []


@pytest.mark.unit
def test_unconditional_restart_regression_activates_a_stopped_daemon(tmp_path):
    # This is the exact previously shipped command shape, executed by the
    # same fake systemctl. It demonstrates the forbidden transition rather
    # than relying only on finding the word "restart" in YAML.
    old_run = "set -euo pipefail\nsudo systemctl restart grid-hermes\n"
    old = _run_shell(tmp_path / "old", states=[_HELD], restart_run=old_run)
    fixed = _run_shell(tmp_path / "fixed", states=[_HELD])
    assert old["activations"] == 1
    assert fixed["activations"] == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    "mutation",
    [
        "initial_hold",
        "write_recheck",
        "action_recheck",
        "try_restart",
        "post_action",
        "verify_hold",
    ],
)
def test_functional_policy_corruptions_are_detected(tmp_path, mutation):
    restart = _step_named(_deploy_steps(), "restart grid-hermes")["run"]
    verify = _step_named(_deploy_steps(), "verify grid-hermes")["run"]
    guard = "if [ \"$state\" != 'loaded|active|running|enabled' ]; then"
    if mutation == "initial_hold":
        restart = restart.replace(guard, "if false; then", 2)
        states = [_HELD]
    elif mutation == "write_recheck":
        start = restart.index("# Recheck immediately")
        restart = restart[:start] + restart[start:].replace(guard, "if false; then", 1)
        states = [_ELIGIBLE, _HELD]
    elif mutation == "action_recheck":
        start = restart.index("# No start/enable authority")
        restart = restart[:start] + restart[start:].replace(guard, "if false; then", 1)
        states = [_ELIGIBLE, _ELIGIBLE, "loaded|active|running|disabled"]
    elif mutation == "try_restart":
        restart = restart.replace(
            "systemctl try-restart grid-hermes", "systemctl restart grid-hermes"
        )
        states = [_ELIGIBLE] * 3 + [_HELD]
    elif mutation == "post_action":
        start = restart.index("sudo systemctl try-restart grid-hermes")
        restart = restart[:start] + restart[start:].replace(guard, "if false; then", 1)
        states = [_ELIGIBLE] * 3 + [_HELD]
    else:
        verify = verify.replace(guard, "if false; then", 1)
        states = [_ELIGIBLE] * 4 + [_HELD]
    state = _run_shell(
        tmp_path,
        states=states,
        restart_run=restart,
        verify_run=verify,
        at_action=_HELD if mutation == "try_restart" else None,
    )
    if mutation == "initial_hold":
        assert state["mutations"], "corruption must expose held-state drop-in writes"
    elif mutation == "write_recheck":
        assert state["mutations"], "corruption must expose late-hold drop-in writes"
    elif mutation in ("action_recheck", "try_restart"):
        assert state["activations"] == 1, "corruption must expose forbidden activation"
    elif mutation == "post_action":
        assert "verify=true" in state["output"], (
            "corruption must expose false verification eligibility"
        )
    else:
        assert any(call[0] == "git" for call in state["calls"]), (
            "corruption must expose verification of held service"
        )
