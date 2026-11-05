"""Deploy-integrity guards for grid-coordinator.

The coordinator ran from the frozen /home/grid/grid_v4/grid_repo until
2026-10-14, so #836 merged green and changed nothing live. These tests run
the exact deploy.yml blocks against rejecting fake commands (no real
systemctl/curl/sudo is ever reached) and pin the policy: restart only an
active, enabled unit already pointed at $DEPLOY_PATH, never write a drop-in,
never start a stopped unit, and verify the running tree afterwards.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest
import yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEPLOY_YML = os.path.join(REPO_ROOT, ".github", "workflows", "deploy.yml")
_EXPECTED_SHA = "0123456789abcdef0123456789abcdef01234567"


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

path = Path(os.environ["FAKE_STATE"])
state = json.loads(path.read_text())
cmd, args = Path(sys.argv[0]).name, sys.argv[1:]
if cmd == "sudo":
    cmd, args = args[0], args[1:]
state["calls"].append([cmd, *args])
out, rc = "", 0
if cmd == "systemctl":
    if args[:2] == ["show", "grid-coordinator"]:
        i = state["queries"]; state["queries"] += 1
        item = state["states"][min(i, len(state["states"]) - 1)]
        if isinstance(item, dict):
            out, rc = item.get("raw", ""), item.get("status", 0)
        else:
            active, unit, wd = item.split("|")
            out = f"WorkingDirectory={wd}\nActiveState={active}\nUnitFileState={unit}\n"
    elif args == ["try-restart", "grid-coordinator"]:
        state["restarts"] += 1
    elif args == ["show", "-p", "MainPID", "--value", "grid-coordinator"]:
        out = "12345\n"
    elif args[0] == "status":
        out = "fake status\n"
    else:
        raise SystemExit(f"unexpected systemctl {args!r}")
elif cmd == "curl":
    rc = 0 if state["healthy"] else 22
elif cmd == "journalctl":
    out = "fake journal\n"
elif cmd == "sleep":
    pass
elif cmd == "chmod":
    if args != ["+x", "scripts/deploy_verify_release_tree.sh"]:
        raise SystemExit(f"unexpected chmod {args!r}")
elif cmd == "readlink":
    if args == ["/proc/12345/cwd"]:
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


def _run(tmp_path, *, states, healthy=True, cwd=None, sha=_EXPECTED_SHA):
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    for name in ("sudo", "systemctl", "curl", "journalctl", "sleep", "chmod",
                 "readlink", "git", "mkdir", "tee"):
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
        "healthy": healthy, "cwd": cwd or deploy, "deploy": deploy, "sha": sha,
    }))
    output = tmp_path / "output"
    output.write_text("")
    env = {"PATH": str(fake_bin), "FAKE_STATE": str(state_path),
           "DEPLOY_PATH": deploy, "GITHUB_OUTPUT": str(output), "LC_ALL": "C"}

    def sh(run):
        return subprocess.run(["/bin/bash", "-e", "-o", "pipefail", "-c", run],
                              cwd=tmp_path, env=env, capture_output=True, text=True, timeout=30)

    results = [sh(_step("restart grid-coordinator")["run"])]
    if results[0].returncode == 0 and output.read_text().splitlines()[-1:] == ["verify=true"]:
        verify = _step("verify grid-coordinator")["run"].replace("${{ github.sha }}", _EXPECTED_SHA)
        results.append(sh(verify))
    state = json.loads(state_path.read_text())
    state["results"] = results
    state["output"] = output.read_text().splitlines()
    return state


ELIGIBLE = "active|enabled|{DEPLOY}"


@pytest.mark.unit
def test_restart_step_is_gated_like_the_api_and_never_writes_a_dropin():
    step = _step("restart grid-coordinator")
    run = step["run"]
    assert "github.event_name == 'push' || inputs.do_restart == true" in step["if"]
    assert "systemctl try-restart grid-coordinator" in run
    assert "systemctl restart grid-coordinator" not in run
    assert "systemctl start" not in run
    assert "service.d" not in run and "tee" not in run and "daemon-reload" not in run
    names = [s.get("name") for s in _steps()]
    assert names.index("Restart grid-coordinator") > names.index("Verify grid-hermes runs the deployed tree")


@pytest.mark.unit
def test_eligible_coordinator_restarts_and_verifies_running_tree(tmp_path):
    state = _run(tmp_path, states=[ELIGIBLE])
    assert [r.returncode for r in state["results"]] == [0, 0], state["results"][-1].stderr
    assert state["restarts"] == 1
    assert state["output"] == ["verify=false", "verify=true"]
    assert "exactly matches expected" in state["results"][-1].stdout


@pytest.mark.unit
@pytest.mark.parametrize("held", [
    "inactive|enabled|{DEPLOY}",
    "active|disabled|{DEPLOY}",
    "failed|enabled|{DEPLOY}",
    "active|enabled|/home/grid/grid_v4/grid_repo",
    "active|enabled|",
    "active|masked|{DEPLOY}",
])
def test_held_or_unmoved_coordinator_is_never_restarted(tmp_path, held):
    state = _run(tmp_path, states=[held])
    assert [r.returncode for r in state["results"]] == [0]
    assert state["restarts"] == 0
    assert state["output"] == ["verify=false"]
    assert all(c[0] in ("systemctl",) and c[1] == "show" for c in state["calls"])


@pytest.mark.unit
@pytest.mark.parametrize("bad", [
    {"status": 1, "raw": ""},
    {"raw": "ActiveState=active\nUnitFileState=enabled\n"},
    {"raw": "ActiveState=active\nActiveState=active\nUnitFileState=enabled\nWorkingDirectory=/x\n"},
    {"raw": "ActiveState=active\nUnitFileState=enabled\nWorkingDirectory=/x\nBogus=1\n"},
])
def test_unreadable_state_fails_before_any_restart(tmp_path, bad):
    state = _run(tmp_path, states=[bad])
    assert state["results"][0].returncode != 0
    assert state["restarts"] == 0


@pytest.mark.unit
def test_unhealthy_after_restart_fails_the_deploy(tmp_path):
    state = _run(tmp_path, states=[ELIGIBLE], healthy=False)
    assert state["results"][0].returncode == 1
    assert "/health failed" in state["results"][0].stdout
    assert len(state["results"]) == 1


@pytest.mark.unit
def test_stop_racing_the_restart_leaves_tree_unverified(tmp_path):
    state = _run(tmp_path, states=[ELIGIBLE, "inactive|enabled|{DEPLOY}"])
    assert [r.returncode for r in state["results"]] == [0]
    assert state["output"] == ["verify=false"]


@pytest.mark.unit
@pytest.mark.parametrize("wrong", ["cwd", "sha"])
def test_verify_rejects_wrong_cwd_or_sha(tmp_path, wrong):
    kw = {"cwd": "/home/grid/grid_v4/grid_repo"} if wrong == "cwd" else {"sha": "f" * 40}
    state = _run(tmp_path, states=[ELIGIBLE], **kw)
    assert state["results"][0].returncode == 0
    assert state["results"][-1].returncode != 0
