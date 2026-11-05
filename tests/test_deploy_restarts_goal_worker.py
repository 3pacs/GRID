"""Deploy-integrity guards for grid-goal-worker@grid-svr.

The goal worker is long-running and keeps the release it booted in; it ran the
frozen /home/grid/grid_v4/grid_repo checkout on py3.10 until 2026-11-05. These
tests run the exact deploy.yml blocks against rejecting fake commands (no real
systemctl/sudo is ever reached) and pin the policy: restart only an active,
running, enabled instance already pointed at $DEPLOY_PATH, never write a
drop-in, never start a stopped unit, fail a crash-loop, and verify the running
tree afterwards.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest
import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEPLOY_YML = os.path.join(REPO_ROOT, ".github", "workflows", "deploy.yml")
_EXPECTED_SHA = "0123456789abcdef0123456789abcdef01234567"
_UNIT = "grid-goal-worker@grid-svr"


def _steps() -> list[dict]:
    with open(DEPLOY_YML, encoding="utf-8") as handle:
        return yaml.safe_load(handle)["jobs"]["deploy"]["steps"]


def _step(needle: str) -> dict:
    for step in _steps():
        if needle.lower() in (step.get("name") or "").lower():
            return step
    raise AssertionError(f"deploy.yml has no step named like {needle!r}")


_FAKE = r"""
import json, os, sys
from pathlib import Path

UNIT = "grid-goal-worker@grid-svr"
path = Path(os.environ["FAKE_STATE"])
state = json.loads(path.read_text())
cmd, args = Path(sys.argv[0]).name, sys.argv[1:]
if cmd == "sudo":
    cmd, args = args[0], args[1:]
state["calls"].append([cmd, *args])
out, rc = "", 0
if cmd == "systemctl":
    if args[:2] == ["show", UNIT]:
        i = state["queries"]; state["queries"] += 1
        item = state["states"][min(i, len(state["states"]) - 1)]
        if isinstance(item, dict):
            out, rc = item.get("raw", ""), item.get("status", 0)
        else:
            active, sub, unit, wd = item.split("|")
            out = (f"WorkingDirectory={wd}\nActiveState={active}\n"
                   f"SubState={sub}\nUnitFileState={unit}\n")
    elif args == ["try-restart", UNIT]:
        state["restarts"] += 1
    elif args == ["show", "-p", "MainPID", "--value", UNIT]:
        pids = state["pids"]
        j = state["pid_queries"]; state["pid_queries"] += 1
        out = str(pids[min(j, len(pids) - 1)]) + "\n"
    elif args[0] == "status":
        out = "fake status\n"
    else:
        raise SystemExit(f"unexpected systemctl {args!r}")
elif cmd == "journalctl":
    out = "fake journal\n"
elif cmd == "sleep":
    pass
elif cmd == "chmod":
    if args != ["+x", "scripts/deploy_verify_release_tree.sh"]:
        raise SystemExit(f"unexpected chmod {args!r}")
elif cmd == "readlink":
    if len(args) == 1 and args[0].startswith("/proc/") and args[0].endswith("/cwd"):
        out = state["cwd"] + "\n"
    elif len(args) == 2 and args[0] == "-f":
        out = args[1] + "\n"
    else:
        rc = 1
elif cmd == "git":
    if args != ["-C", state["deploy"], "rev-parse", "HEAD"]:
        raise SystemExit(f"unexpected git {args!r}")
    out = state["sha"] + "\n"
else:
    raise SystemExit(f"unexpected command (never forwarded): {cmd} {args!r}")
path.write_text(json.dumps(state))
sys.stdout.write(out)
raise SystemExit(rc)
"""


def _run(tmp_path, *, states, pids=(4242,), cwd=None, sha=_EXPECTED_SHA):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    for name in ("sudo", "systemctl", "journalctl", "sleep", "chmod",
                 "readlink", "git", "mkdir", "tee", "curl"):
        leaf = fake_bin / name
        leaf.write_text(f"#!{sys.executable}\n" + _FAKE)
        leaf.chmod(0o700)
    (fake_bin / "bash").symlink_to("/bin/bash")
    scripts = tmp_path / "scripts"
    scripts.mkdir()
    helper = scripts / "deploy_verify_release_tree.sh"
    helper.write_bytes((Path(REPO_ROOT) / "scripts/deploy_verify_release_tree.sh").read_bytes())
    helper.chmod(0o700)
    deploy = str(tmp_path / "release")
    states = [s.replace("{DEPLOY}", deploy) if isinstance(s, str) else s for s in states]
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps({
        "states": states, "queries": 0, "calls": [], "restarts": 0,
        "pids": list(pids), "pid_queries": 0,
        "cwd": cwd or deploy, "deploy": deploy, "sha": sha,
    }))
    output = tmp_path / "output"
    output.write_text("")
    env = {"PATH": str(fake_bin), "FAKE_STATE": str(state_path),
           "DEPLOY_PATH": deploy, "GITHUB_OUTPUT": str(output), "LC_ALL": "C"}

    def sh(run):
        return subprocess.run(["/bin/bash", "-e", "-o", "pipefail", "-c", run],
                              cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30,
                              check=False)

    results = [sh(_step("restart grid-goal-worker")["run"])]
    if results[0].returncode == 0 and output.read_text().splitlines()[-1:] == ["verify=true"]:
        verify = _step("verify grid-goal-worker")["run"].replace("${{ github.sha }}", _EXPECTED_SHA)
        results.append(sh(verify))
    state = json.loads(state_path.read_text())
    state["results"] = results
    state["output"] = output.read_text().splitlines()
    return state


ELIGIBLE = "active|running|enabled|{DEPLOY}"


@pytest.mark.unit
def test_restart_step_is_gated_like_the_api_and_never_writes_a_dropin():
    step = _step("restart grid-goal-worker")
    run = step["run"]
    assert "github.event_name == 'push' || inputs.do_restart == true" in step["if"]
    assert 'systemctl try-restart "$unit"' in run
    assert "systemctl restart" not in run
    assert "systemctl start" not in run
    assert "service.d" not in run and "tee" not in run and "daemon-reload" not in run
    names = [s.get("name") for s in _steps()]
    assert names.index("Restart grid-goal-worker") > names.index(
        "Verify grid-coordinator runs the deployed tree")


@pytest.mark.unit
def test_eligible_worker_restarts_and_verifies_running_tree(tmp_path):
    state = _run(tmp_path, states=[ELIGIBLE])
    assert [r.returncode for r in state["results"]] == [0, 0], state["results"][-1].stderr
    assert state["restarts"] == 1
    assert state["output"] == ["verify=false", "verify=true"]
    assert "exactly matches expected" in state["results"][-1].stdout


@pytest.mark.unit
def test_worker_that_comes_back_after_a_few_polls_is_accepted(tmp_path):
    state = _run(tmp_path, states=[
        ELIGIBLE, "activating|start|enabled|{DEPLOY}", "activating|start|enabled|{DEPLOY}", ELIGIBLE,
    ])
    assert [r.returncode for r in state["results"]] == [0, 0]
    assert state["restarts"] == 1


@pytest.mark.unit
@pytest.mark.parametrize("held", [
    "inactive|dead|enabled|{DEPLOY}",
    "active|running|disabled|{DEPLOY}",
    "failed|failed|enabled|{DEPLOY}",
    "activating|auto-restart|enabled|{DEPLOY}",
    "active|running|enabled|/home/grid/grid_v4/grid_repo",
    "active|running|enabled|",
    "active|running|masked|{DEPLOY}",
])
def test_held_or_unmoved_worker_is_never_restarted(tmp_path, held):
    state = _run(tmp_path, states=[held])
    assert [r.returncode for r in state["results"]] == [0]
    assert state["restarts"] == 0
    assert state["output"] == ["verify=false"]
    assert all(c[0] == "systemctl" and c[1] == "show" for c in state["calls"])


@pytest.mark.unit
@pytest.mark.parametrize("bad", [
    {"status": 1, "raw": ""},
    {"raw": "ActiveState=active\nSubState=running\nUnitFileState=enabled\n"},
    {"raw": "ActiveState=active\nActiveState=active\nSubState=running\nUnitFileState=enabled\nWorkingDirectory=/x\n"},
    {"raw": "ActiveState=active\nSubState=running\nUnitFileState=enabled\nWorkingDirectory=/x\nBogus=1\n"},
])
def test_unreadable_state_fails_before_any_restart(tmp_path, bad):
    state = _run(tmp_path, states=[bad])
    assert state["results"][0].returncode != 0
    assert state["restarts"] == 0


@pytest.mark.unit
def test_never_running_after_restart_fails_the_deploy(tmp_path):
    state = _run(tmp_path, states=[ELIGIBLE, "activating|auto-restart|enabled|{DEPLOY}"])
    assert state["results"][0].returncode == 1
    assert "not running after restart" in state["results"][0].stdout
    assert len(state["results"]) == 1


@pytest.mark.unit
def test_crash_loop_detected_by_pid_change_fails_the_deploy(tmp_path):
    state = _run(tmp_path, states=[ELIGIBLE], pids=(4242, 4300))
    assert state["results"][0].returncode == 1
    assert "did not stay up" in state["results"][0].stdout
    assert len(state["results"]) == 1


@pytest.mark.unit
def test_crashed_during_settle_window_fails_the_deploy(tmp_path):
    state = _run(tmp_path, states=[ELIGIBLE, ELIGIBLE, "failed|failed|enabled|{DEPLOY}"])
    assert state["results"][0].returncode == 1
    assert "did not stay up" in state["results"][0].stdout


@pytest.mark.unit
@pytest.mark.parametrize("stopped_at", [1, 2])
def test_operator_stop_racing_the_restart_leaves_tree_unverified(tmp_path, stopped_at):
    states = [ELIGIBLE] * stopped_at + ["inactive|dead|enabled|{DEPLOY}"]
    state = _run(tmp_path, states=states)
    assert [r.returncode for r in state["results"]] == [0]
    assert state["output"] == ["verify=false"]
    assert "tree unverified" in state["results"][0].stdout


@pytest.mark.unit
@pytest.mark.parametrize("wrong", ["cwd", "sha"])
def test_verify_rejects_wrong_cwd_or_sha(tmp_path, wrong):
    kw = {"cwd": "/home/grid/grid_v4/grid_repo"} if wrong == "cwd" else {"sha": "f" * 40}
    state = _run(tmp_path, states=[ELIGIBLE], **kw)
    assert state["results"][0].returncode == 0
    assert state["results"][-1].returncode != 0
