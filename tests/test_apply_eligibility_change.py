"""scripts/apply_eligibility_change.py without a database (#1301, #1383): the prerequisite gate, the deployed-code
evidence (entrypoints and restart receipts included), the owner receipts, the plan and the SQL shape.

systemctl, /proc and git are faked by ``tests/eligibility_fakes.FakeHost``. The database behaviour (authority,
apply, rollback, divergence, the receipt's planned rows) is proved on real PostgreSQL in
tests/test_eligibility_core_migration_pg.py.
"""

from __future__ import annotations

import ast
import hashlib
import importlib
import json
import shutil
import uuid
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scripts import apply_eligibility_change as elig
from tests.eligibility_fakes import BOOTED_AT as BOOTED
from tests.eligibility_fakes import (
    HYSTERESIS, NEXT_SHA, PINNED_SHA, POLARITY, FakeHost, write_tree,
)

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "versions" / "eligibility_core_20261122.py"
DATABASE = "postgresql://grid_eligibility_owner:***@127.0.0.1:5432/griddb"
PLANNED = [{"feature_id": 11, "name": "vix_spot", "old_eligible": False, "new_eligible": True}]
BACKEND = {"system_identifier": "7311842825918795820", "timeline_id": 1, "database_name": "griddb",
           "database_oid": 16384, "postmaster_started_at": "2026-11-20T03:00:00.000000Z", "server_addr": "127.0.0.1",
           "server_port": 5432, "in_recovery": False, "server_version_num": "150010"}
CHANGE_SET = "6f1b6c1e-6a0e-4f0e-9a51-1d6a3c2f7b10"


# ── the literals ────────────────────────────────────────────────────────────────────────────────────────────


def test_the_gate_refuses_on_this_tree_today():
    """#1307 has not landed, so --apply must refuse. When it adds its constant, this test is the one to update."""
    gate = elig.check_prerequisites([("repo", ROOT)])
    assert not gate["ok"]
    assert {(c["file"], c["constant"], c["found"], c["ok"]) for c in gate["checks"]} == {
        ("scripts/auto_regime.py", "REGIME_HYSTERESIS_RULE", None, False)}
    assert any("REGIME_HYSTERESIS_RULE is not defined" in r for r in gate["refusals"])


def test_the_oracle_prerequisite_is_1350s_merge_commit_not_a_literal():
    """#1350 landed without the SIGNAL_POLARITY_RULE literal, so the gate is bound to its merge commit."""
    assert [(p.name, p.commit) for p in elig.ANCESTOR_PREREQUISITES] == [
        ("oracle_feature_polarity", "9d777445a23eb90b92b6cab812aece7b9bcf99c8")]
    assert "#1350" in POLARITY.provided_by
    assert all(p.constant != "SIGNAL_POLARITY_RULE" for p in elig.PREREQUISITES)


def test_the_gate_passes_with_the_exact_literal(tmp_path):
    gate = elig.check_prerequisites([("t", write_tree(tmp_path))])
    assert gate["ok"] and gate["refusals"] == []
    assert [c["found"] for c in gate["checks"]] == [HYSTERESIS.expected]


@pytest.mark.parametrize("regime, problem", [
    (None, "cannot read"),
    ("import os\n", "is not defined at module level"),
    (f'{HYSTERESIS.constant} = "yes"\n', "is 'yes', not"),
    (f'{HYSTERESIS.constant} = "{HYSTERESIS.expected}".lower()\n', "is not a string literal"),
    (f'{HYSTERESIS.constant} = f"{HYSTERESIS.expected}"\n', "is not a string literal"),
    (f'if True:\n    {HYSTERESIS.constant} = "{HYSTERESIS.expected}"\n', "is not defined at module level"),
    (f'{HYSTERESIS.constant} = "{HYSTERESIS.expected}"\n{HYSTERESIS.constant} = ""\n', "is '', not"),
    (f'{HYSTERESIS.constant} = "{HYSTERESIS.expected}"\ndef (:\n', "does not parse"),
])
def test_the_gate_refuses_anything_but_the_exact_literal(tmp_path, regime, problem):
    gate = elig.check_prerequisites([("t", write_tree(tmp_path, regime))])
    assert not gate["ok"]
    assert len(gate["refusals"]) == 1 and problem in gate["refusals"][0]
    assert "#1307" in gate["refusals"][0]


def test_the_gate_reads_without_importing(tmp_path):
    regime = f'raise SystemExit("imported")\n{HYSTERESIS.constant} = "{HYSTERESIS.expected}"\n'
    assert elig.check_prerequisites([("t", write_tree(tmp_path, regime))])["ok"]


# ── deployed-code evidence ──────────────────────────────────────────────────────────────────────────────────


@pytest.fixture()
def host(tmp_path, monkeypatch) -> FakeHost:
    fake = FakeHost(tmp_path)
    monkeypatch.setattr(elig, "ROOT", fake.current)
    return fake


def _gate(host: FakeHost, extra=()):
    return elig.deployed_prerequisites(extra, probes=host.probes())


def _only(gate, text: str) -> None:
    assert not gate["ok"]
    assert len(gate["refusals"]) == 1, gate["refusals"]
    assert text in gate["refusals"][0], gate["refusals"][0]


def test_the_full_inventory_running_its_configured_trees_passes(host):
    gate = _gate(host)
    assert gate["ok"] and gate["refusals"] == []
    evidence = gate["evidence"]
    assert set(evidence["units"]) == set(elig.SERVICE_UNITS)
    assert {u["evidence"] for u in evidence["units"].values()} == {"running_process"}
    api = evidence["units"]["grid-api.service"]
    assert api["working_directory"] == str(host.live_link) and api["process_cwd"] == str(host.current)
    assert api["main_pid"] == 2001 and api["process_start"] == "200100"
    trees = evidence["trees"]
    assert set(trees) == {str(host.current), str(host.pinned)}
    assert trees[str(host.pinned)]["used_by"] == ["grid-realtime.service", "grid-scheduler.service"]
    assert trees[str(host.pinned)]["head"] == trees[str(host.pinned)]["release_label"] == PINNED_SHA
    assert trees[str(host.current)]["prerequisites"] == {HYSTERESIS.constant: HYSTERESIS.expected}
    assert trees[str(host.current)]["contains"] == {POLARITY.name: {"commit": POLARITY.commit, "ancestor": True}}
    assert evidence["host"] == "grid-svr" and evidence["boot_id"] == "boot-1"
    assert evidence["deploy_locks"] == [host.deploy_lock_path]
    assert api["entrypoint"] == api["process_entrypoint"] | {"python_env": None}
    assert (api["process_entrypoint"]["kind"], api["process_entrypoint"]["entry"],
            api["process_entrypoint"]["entry_file"]) == ("app", "api.main", str(host.current / "api" / "main.py"))
    assert api["restart_receipt"] == {"process_started_at": "2026-11-01T00:33:21+00:00",
                                      "code_changed_at": "2026-10-31T23:00:00+00:00", "started_after_code": True}


def test_a_repinned_unit_whose_process_still_runs_the_old_tree_refuses(host):
    """The 08:03Z case: the scheduler's configuration was repinned but its running process was not."""
    host.units["grid-scheduler.service"]["WorkingDirectory"] = str(host.current)
    _only(_gate(host), f"grid-scheduler.service's running process (PID 2003) runs from {host.pinned}, but its "
                       f"WorkingDirectory {host.current} resolves to {host.current}")


def test_a_repointed_release_symlink_with_unrestarted_processes_refuses(host):
    """A deploy repoints grid_release; until each unit restarts, its process runs the previous release."""
    nxt = write_tree(host.releases / NEXT_SHA)
    host.live_link.unlink()
    host.live_link.symlink_to(nxt)
    gate = _gate(host)
    assert not gate["ok"] and len(gate["refusals"]) == 5      # api, hermes, coordinator, worker, intelligence
    assert all(f"resolves to {nxt}" in r for r in gate["refusals"])


@pytest.mark.parametrize("error, text", [
    (PermissionError(13, "Permission denied"), "Permission denied; run as the user the GRID units run as"),
    (FileNotFoundError(2, "No such file or directory"), "No such file or directory"),
])
def test_an_unreadable_process_refuses(host, error, text):
    host.cwds[2003] = error
    _only(_gate(host), f"cannot read grid-scheduler.service's running process (PID 2003): {text}")


def test_a_process_whose_directory_was_deleted_refuses(host):
    host.cwds[2003] = f"{host.pinned} (deleted)"
    _only(_gate(host), "runs from a deleted directory")


def test_a_restart_while_the_process_is_read_refuses(host):
    host.starts[2003] = ["200300", "999999"]
    _only(_gate(host), "grid-scheduler.service's PID 2003 changed while it was read")


def test_an_active_unit_without_a_main_pid_refuses(host):
    host.units["grid-worker.service"]["MainPID"] = "0"
    _only(_gate(host), "grid-worker.service is active but systemd names no MainPID")


@pytest.mark.parametrize("state", ["activating", "deactivating", "reloading"])
def test_a_unit_in_transition_refuses(host, state):
    host.units["grid-hermes.service"]["ActiveState"] = state
    _only(_gate(host), f"grid-hermes.service is {state}; re-run when it is active or stopped")


def test_a_unit_whose_files_changed_since_systemd_loaded_them_refuses(host):
    host.units["grid-realtime.service"]["NeedDaemonReload"] = "yes"
    _only(_gate(host), "grid-realtime.service's unit files changed on disk since systemd loaded them")


def test_a_stopped_unit_is_checked_by_its_configuration(host):
    """No process runs, so nothing reads flags now; its next start uses the WorkingDirectory checked here."""
    host.stop("grid-hermes.service")
    gate = _gate(host)
    assert gate["ok"]
    hermes = gate["evidence"]["units"]["grid-hermes.service"]
    assert hermes["evidence"] == "config_only" and hermes["process_cwd"] is None
    assert hermes["tree"] == str(host.current)

    old = write_tree(host.releases / NEXT_SHA, regime="import os\n")
    host.units["grid-hermes.service"]["WorkingDirectory"] = str(old)  # a stopped unit on a tree without the literal
    _only(_gate(host), f"REGIME_HYSTERESIS_RULE is not defined at module level in {old / HYSTERESIS.path}")


@pytest.mark.parametrize("override, text", [
    ({"head": NEXT_SHA}, f"is named {PINNED_SHA} but its HEAD is {NEXT_SHA}"),
    ({"clean": False}, "tracked files differ from its HEAD"),
    (RuntimeError("git rev-parse exited 128: fatal: detected dubious ownership"), "cannot read the git identity"),
])
def test_the_pinned_tree_needs_an_exact_git_identity(host, override, text):
    host.git_overrides[str(host.pinned)] = override
    _only(_gate(host), text)


def test_a_tree_without_a_full_commit_id_refuses(host):
    host.git_overrides[str(host.pinned)] = {"head": "HEAD"}
    gate = _gate(host)
    assert gate["refusals"] == [f"{host.pinned} has no full commit id at HEAD ('HEAD')",
                                f"release directory {host.pinned} is named {PINNED_SHA} but its HEAD is HEAD"]


def test_a_tree_that_is_not_the_top_of_its_checkout_refuses(host):
    host.git_overrides[str(host.current)] = {"toplevel": str(host.releases)}
    _only(_gate(host), f"{host.current} is not the top of its git checkout")


def test_a_pinned_tree_without_the_literals_refuses(host):
    (host.pinned / HYSTERESIS.path).write_text("import os\n")
    _only(_gate(host), f"REGIME_HYSTERESIS_RULE is not defined at module level in {host.pinned / HYSTERESIS.path}")


# ── code identity: entrypoints, PYTHONPATH, restart receipts (#1383) ─────────────────────────────────────────

PY = "/data/grid_v4/grid_venv-314/bin/python"


def _launch(host: FakeHost, unit: str, *argv: str, running=None) -> int:
    """Configure ``unit`` with ExecStart ``argv`` (and its process with ``running``, default the same)."""
    from tests.eligibility_fakes import exec_start

    pid = int(host.units[unit]["MainPID"])
    host.units[unit]["ExecStart"] = exec_start(list(argv))
    host.argvs[pid] = list(running or argv)
    return pid


def test_an_absolute_entrypoint_outside_the_working_directory_refuses(host, tmp_path):
    """Codex (6011901469): the cwd check did not cover an absolute ExecStart outside cwd."""
    other = write_tree(tmp_path / "grid_repo")
    _launch(host, "grid-worker.service", PY, str(other / "scripts" / "worker.py"))
    gate = _gate(host)
    assert len(gate["refusals"]) == 2 and all("outside its tree" in r for r in gate["refusals"])
    assert {r.split(" runs ")[0] for r in gate["refusals"]} == {
        "grid-worker.service's ExecStart", "grid-worker.service's running process (PID 2005)"}


@pytest.mark.parametrize("argv, text", [
    ((PY, "-m", "ingestion.nowhere"), "runs module ingestion.nowhere, which is not a file in"),
    ((PY, "-m", "uvicorn", "api.nowhere:app"), "runs app api.nowhere, which is not a file in"),
    ((PY, "scripts/missing.py"), "runs scripts/missing.py, which is not a file under"),
    ((PY, "-I", "-m", "ingestion.scheduler"), "runs python with -I, which this check does not model"),
    (("/usr/bin/flock", "-n", "/tmp/x.lock", PY, "-m", "ingestion.scheduler"), "not a python interpreter"),
    ((PY, "-m", "uvicorn", "api.main:app", "--app-dir", "/elsewhere"), "with --app-dir"),
    ((PY, "-m", "uvicorn", "--port", "8000", "api.main:app"), "without its app as the first argument"),
    ((PY, "-m"), "names no script or module"),
])
def test_an_entrypoint_python_would_not_load_from_the_tree_refuses(host, argv, text):
    _launch(host, "grid-scheduler.service", *argv)
    gate = _gate(host)
    assert gate["refusals"] and all(text in r for r in gate["refusals"]), gate["refusals"]


def test_a_process_started_with_another_command_line_than_its_execstart_refuses(host):
    _launch(host, "grid-intelligence.service", PY, "-m", "intelligence.scheduler",
            running=[PY, "-m", "ingestion.scheduler"])
    _only(_gate(host), "grid-intelligence.service's running process (PID 2006) was started as")


def test_multiple_execstart_lines_refuse(host):
    from tests.eligibility_fakes import exec_start

    host.units["grid-api.service"]["ExecStart"] = exec_start([PY, "-m", "x"]) + " " + exec_start([PY, "-m", "y"])
    gate = _gate(host)
    assert any("has 2 ExecStart command lines, not one" in r for r in gate["refusals"])


@pytest.mark.parametrize("env, text", [
    ({"PYTHONPATH": "/home/grid/grid_v4/grid_repo"}, "has PYTHONPATH entry /home/grid/grid_v4/grid_repo outside"),
    ({"PYTHONSAFEPATH": "1"}, "sets PYTHONSAFEPATH"),
])
def test_a_python_environment_that_can_load_other_code_refuses(host, env, text):
    host.envs[2003] = env
    _only(_gate(host), text)


def test_a_pythonpath_inside_the_tree_is_recorded_and_passes(host):
    host.envs[2003] = {"PYTHONPATH": str(host.pinned)}
    gate = _gate(host)
    assert gate["ok"]
    assert gate["evidence"]["units"]["grid-scheduler.service"]["process_entrypoint"]["python_env"] == {
        "PYTHONPATH": str(host.pinned)}


def test_code_that_changed_after_its_process_started_refuses(host):
    """Codex (6011901469): code loaded before an in-place checkout change. The restart receipt names it."""
    host.code_at[str(host.pinned)] = BOOTED + timedelta(hours=1)      # after every fake process started
    gate = _gate(host)
    assert len(gate["refusals"]) == 2                                  # the scheduler and grid-realtime run it
    assert all("before the code in" in r and "restart the unit" in r for r in gate["refusals"])
    receipt = gate["evidence"]["units"]["grid-scheduler.service"]["restart_receipt"]
    assert receipt == {"process_started_at": "2026-11-01T00:33:23+00:00",
                       "code_changed_at": "2026-11-01T01:00:00+00:00", "started_after_code": False}


def test_a_stopped_unit_has_no_restart_receipt(host):
    host.stop("grid-hermes.service")
    host.code_at[str(host.current)] = BOOTED + timedelta(seconds=2001.5)  # after api (2001 s), before the rest
    gate = _gate(host)
    assert gate["evidence"]["units"]["grid-hermes.service"]["restart_receipt"] is None
    assert {r.split("'s ")[0] for r in gate["refusals"]} == {"grid-api.service"}


def test_a_tree_that_cannot_be_dated_refuses(host):
    host.code_at[str(host.pinned)] = RuntimeError("git ls-files exited 128")
    _only(_gate(host), f"cannot date the code in {host.pinned}")


# ── the #1350 prerequisite: its merge commit in every tree's history ────────────────────────────────────────


def test_a_tree_whose_head_lacks_1350_refuses(host):
    host.ancestors[str(host.pinned)] = False
    _only(_gate(host), f"{host.pinned}'s HEAD {PINNED_SHA} does not contain {POLARITY.commit} (needs the Oracle")


def test_a_tree_that_predates_1350_or_lacks_history_refuses(host):
    host.ancestors[str(host.current)] = RuntimeError("git merge-base --is-ancestor exited 128: fatal: Not a valid "
                                                     "commit name 9d777445")
    _only(_gate(host), "the tree predates it or its history is missing")


def test_the_receipt_records_each_trees_ancestor_check(host, tmp_path):
    path, _ = _receipt(host, tmp_path)
    receipt = json.loads(path.read_bytes())
    assert {t["contains"][POLARITY.name]["ancestor"] for t in receipt["trees"].values()} == {True}


def test_extra_code_roots_are_checked_too(host, tmp_path):
    gate = _gate(host, [tmp_path / "missing"])
    _only(gate, "--code-root")


def test_a_host_without_systemd_refuses(host):
    """Codex (6011670010): no systemctl was a note, so a laptop with compliant literals could certify the database."""
    host_probes = host.probes()
    gate = elig.deployed_prerequisites(probes=elig.Probes(**{**host_probes.__dict__, "show": lambda unit: None}))
    _only(gate, "systemctl is not available on this host")


@pytest.mark.parametrize("unit", elig.SERVICE_UNITS)
def test_a_missing_inventory_unit_refuses(host, unit):
    host.units[unit] = {"LoadState": "not-found"}
    _only(_gate(host), f"{unit} is in the deployed service inventory but is not-found here")


@pytest.mark.parametrize("props, text", [
    ({"LoadState": "loaded", "ActiveState": "active", "WorkingDirectory": ""}, "has no absolute WorkingDirectory"),
    ({"LoadState": "loaded", "ActiveState": "active", "WorkingDirectory": "~"}, "has no absolute WorkingDirectory"),
    (RuntimeError("systemctl show exited 1"), "cannot ask systemd about grid-worker.service"),
])
def test_a_unit_whose_tree_cannot_be_named_refuses(host, props, text):
    host.units["grid-worker.service"] = props
    _only(_gate(host), text)


def _git(*args, cwd):
    import subprocess

    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@t", *args], cwd=cwd, check=True,
                          capture_output=True, text=True)  # nosec B603 B607 - test fixture


@pytest.mark.skipif(not shutil.which("git"), reason="needs git")
def test_the_real_git_probe_reads_the_identity_and_writes_nothing(tmp_path):
    repo = write_tree(tmp_path / "repo")
    _git("init", "-q", cwd=repo)
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", "x", cwd=repo)
    head = _git("rev-parse", "HEAD", cwd=repo).stdout.strip()
    (repo / HYSTERESIS.path).touch()                          # stale stat info, same content: still clean
    index = repo / ".git" / "index"
    stamp = (index.stat().st_mtime_ns, index.read_bytes())
    identity = elig.git_identity(repo)
    assert identity == {"toplevel": str(repo.resolve()), "head": head,
                        "head_tree": _git("rev-parse", "HEAD^{tree}", cwd=repo).stdout.strip(), "clean": True}
    assert (index.stat().st_mtime_ns, index.read_bytes()) == stamp and not (repo / ".git" / "index.lock").exists()
    (repo / HYSTERESIS.path).write_text("changed\n")
    assert elig.git_identity(repo)["clean"] is False
    with pytest.raises(RuntimeError, match="git rev-parse"):
        elig.git_identity(tmp_path / "not-a-repo")


@pytest.mark.skipif(not shutil.which("git"), reason="needs git")
def test_the_real_ancestor_and_code_date_probes(tmp_path):
    import os
    import time

    repo = write_tree(tmp_path / "repo")
    _git("init", "-q", cwd=repo)
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", "first", cwd=repo)
    first = _git("rev-parse", "HEAD", cwd=repo).stdout.strip()
    (repo / "later.py").write_text("x = 1\n")
    _git("add", "-A", cwd=repo)
    _git("commit", "-q", "-m", "second", cwd=repo)
    second = _git("rev-parse", "HEAD", cwd=repo).stdout.strip()
    assert elig.git_is_ancestor(repo, first) is True
    _git("checkout", "-q", first, cwd=repo)
    assert elig.git_is_ancestor(repo, second) is False
    with pytest.raises(RuntimeError, match="merge-base --is-ancestor exited 128"):
        elig.git_is_ancestor(repo, "f" * 40)                  # a commit this tree has never seen
    old = time.time() - 86400
    for rel in elig_files(repo):
        os.utime(repo / rel, (old, old))
    dated = elig.git_code_changed_at(repo)
    assert dated.timestamp() >= time.time() - 120             # the checkout above moved HEAD: its reflog dates it
    (repo / HYSTERESIS.path).write_text("edited\n")
    assert elig.git_code_changed_at(repo) >= dated            # an edited tracked file is newer still


def elig_files(repo: Path) -> list[str]:
    return [p for p in _git("ls-files", cwd=repo).stdout.split("\n") if p]


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="needs /proc")
def test_the_real_proc_probes_read_this_process():
    import os

    assert Path(elig.proc_cwd(os.getpid())) == Path.cwd().resolve()
    assert elig.proc_start(os.getpid()).isdigit() and elig.proc_start(os.getpid()) == elig.proc_start(os.getpid())
    assert len(elig.boot_id()) == 36
    import subprocess
    import sys
    from datetime import datetime, timedelta, timezone

    child = subprocess.Popen(  # nosec B603 - test fixture, fixed argv
        [sys.executable, "-c", "import time; time.sleep(30)"],
        env={"PATH": os.environ.get("PATH", ""), "PYTHONPATH": "/srv/x", "ELIG_TEST_SECRET": "must-not-be-read"})
    try:
        import time

        argv = [sys.executable, "-c", "import time; time.sleep(30)"]
        deadline = time.monotonic() + 10
        while elig.proc_cmdline(child.pid) != argv and time.monotonic() < deadline:
            time.sleep(0.02)                                  # /proc shows the argv once the child has exec'd
        assert elig.proc_cmdline(child.pid) == argv
        assert elig.proc_python_env(child.pid) == {"PYTHONPATH": "/srv/x"}     # the secret is never kept
        started = elig.proc_clock(elig.proc_start(child.pid))
        assert datetime.now(timezone.utc) - timedelta(minutes=1) < started <= datetime.now(timezone.utc)
    finally:
        child.kill()
        child.wait()


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="needs /proc")
def test_the_real_deploy_lock_refuses_while_another_holder_has_it(tmp_path):
    import fcntl
    import os

    lock = tmp_path / ".lock"
    lock.write_text("")
    with elig.flock_deploy_lock(lock):
        other = os.open(lock, os.O_RDONLY)
        try:                                                  # a deploy's flock -w would wait here
            with pytest.raises(BlockingIOError):
                fcntl.flock(other, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            os.close(other)
    holder = os.open(lock, os.O_RDONLY)
    fcntl.flock(holder, fcntl.LOCK_EX)                        # a deploy holds it
    try:
        with pytest.raises(BlockingIOError):
            with elig.flock_deploy_lock(lock):
                pass
    finally:
        os.close(holder)
    with pytest.raises(FileNotFoundError):
        with elig.flock_deploy_lock(tmp_path / "missing.lock"):
            pass
    assert lock.read_text() == ""                             # opened read-only: nothing written


def test_the_inventory_includes_the_pinned_units():
    assert {"grid-scheduler.service", "grid-realtime.service"} <= set(elig.SERVICE_UNITS)
    assert "MainPID" in elig.UNIT_PROPERTIES and "NeedDaemonReload" in elig.UNIT_PROPERTIES


# ── the owner receipt ───────────────────────────────────────────────────────────────────────────────────────


def _receipt(host: FakeHost, tmp_path: Path, planned=PLANNED, database=DATABASE, backend=BACKEND,
             change_set=CHANGE_SET):
    """A dry run's receipt, saved as --receipt-out would; returns (path, sha256)."""
    body = elig.canonical(elig.build_receipt(_gate(host), planned, database, backend=backend, change_set=change_set))
    path = tmp_path / "receipt.json"
    path.write_bytes(body)
    return path, hashlib.sha256(body).hexdigest()


def _verify(host: FakeHost, path, sha, database=DATABASE):
    return elig.verify_receipt(path, sha, _gate(host), database, host.now)


def test_a_receipt_that_matches_the_live_evidence_passes(host, tmp_path):
    path, sha = _receipt(host, tmp_path)
    host.now += timedelta(minutes=29)
    receipt, refusals = _verify(host, path, sha)
    assert refusals == [] and receipt["planned_changes"] == PLANNED
    assert receipt["created_at"] == "2026-11-22T08:00:00Z" and receipt["host"] == "grid-svr"


def test_apply_needs_both_receipt_flags(host, tmp_path):
    path, sha = _receipt(host, tmp_path)
    for args in ((None, sha), (path, None)):
        _, refusals = _verify(host, *args)
        assert refusals and "--apply needs --approved-receipt and --approved-receipt-sha256" in refusals[0]


def test_a_receipt_whose_sha256_is_not_the_approved_one_refuses(host, tmp_path):
    path, sha = _receipt(host, tmp_path)
    other = hashlib.sha256(b"something else").hexdigest()
    _, refusals = _verify(host, path, other)
    assert refusals == [f"the approved receipt {path} has sha256 {sha}, not the approved {other}"]
    _, refusals = _verify(host, path, "not-hex")
    assert "is not a sha256 hex digest" in refusals[0]


def test_an_edited_receipt_refuses(host, tmp_path):
    path, sha = _receipt(host, tmp_path)
    body = path.read_bytes().replace(b'"main_pid":2003', b'"main_pid":2004')
    assert body != path.read_bytes()
    path.write_bytes(body)
    _, refusals = _verify(host, path, sha)                  # the approved sha no longer matches
    assert "has sha256" in refusals[0]
    _, refusals = _verify(host, path, hashlib.sha256(body).hexdigest())  # an edited receipt "approved" again
    assert refusals == ["live evidence differs from the approved receipt: "
                        "receipt['units']['grid-scheduler.service']['main_pid']: approved 2004, now 2003"]


def test_a_receipt_not_in_canonical_form_refuses(host, tmp_path):
    path, _ = _receipt(host, tmp_path)
    pretty = json.dumps(json.loads(path.read_bytes()), indent=2).encode()
    path.write_bytes(pretty)
    _, refusals = _verify(host, path, hashlib.sha256(pretty).hexdigest())
    assert refusals == [f"the approved receipt {path} is not a canonical receipt (write it with --receipt-out)"]


def test_a_stale_receipt_refuses(host, tmp_path):
    path, sha = _receipt(host, tmp_path)
    host.now += timedelta(seconds=elig.RECEIPT_MAX_AGE_S + 1)
    _, refusals = _verify(host, path, sha)
    assert refusals == [f"the approved receipt is {elig.RECEIPT_MAX_AGE_S + 1} s old; an apply takes one at most "
                        f"{elig.RECEIPT_MAX_AGE_S} s old (run a new dry run)"]


def test_a_receipt_from_the_future_refuses(host, tmp_path):
    path, sha = _receipt(host, tmp_path)
    host.now -= timedelta(minutes=5)
    _, refusals = _verify(host, path, sha)
    assert refusals == ["the approved receipt is stamped 300 s ahead of this host's clock"]


def test_a_restart_between_dry_run_and_apply_refuses(host, tmp_path):
    path, sha = _receipt(host, tmp_path)
    host.run("grid-scheduler.service", 7777, host.pinned, host.pinned)  # same tree, new process
    _, refusals = _verify(host, path, sha)
    unit = "live evidence differs from the approved receipt: receipt['units']['grid-scheduler.service']"
    assert refusals == [
        f"{unit}['main_pid']: approved 2003, now 7777",
        f"{unit}['process_start']: approved '200300', now '777700'",
        f"{unit}['process_started_at']: approved '2026-11-01T00:33:23+00:00', now '2026-11-01T02:09:37+00:00'",
        f"{unit}['restart_receipt']['process_started_at']: approved '2026-11-01T00:33:23+00:00', "
        "now '2026-11-01T02:09:37+00:00'",
    ]


def test_a_repin_and_restart_between_dry_run_and_apply_refuses(host, tmp_path):
    """The 08:03Z case after the fact: config and process agree again, but not with what the owner approved."""
    path, sha = _receipt(host, tmp_path)
    host.run("grid-scheduler.service", 7777, host.current, host.current)
    receipt, refusals = _verify(host, path, sha)
    assert _gate(host)["ok"]                                  # the live evidence is self-consistent
    named = {r.split(": receipt", 1)[1].split(":")[0] for r in refusals}
    unit = "['units']['grid-scheduler.service']"
    tree = "['trees']"
    assert {f"{unit}['main_pid']", f"{unit}['process_start']", f"{unit}['working_directory']",
            f"{unit}['working_directory_resolved']", f"{unit}['process_cwd']", f"{unit}['tree']",
            f"{unit}['process_started_at']", f"{unit}['restart_receipt']['process_started_at']",
            f"{unit}['entrypoint']['entry_file']", f"{unit}['process_entrypoint']['entry_file']",
            f"{tree}[{str(host.current)!r}]['used_by']", f"{tree}[{str(host.pinned)!r}]['used_by']"} == named


def test_the_receipt_must_be_for_this_host_boot_and_database(host, tmp_path):
    path, sha = _receipt(host, tmp_path)
    host.host, host.boot = "laptop", "boot-2"
    _, refusals = _verify(host, path, sha, database=DATABASE.replace("griddb", "griddb_copy"))
    assert sorted(r.split(": receipt", 1)[1].split(":")[0] for r in refusals) == [
        "['boot_id']", "['database']", "['host']"]


def test_a_receipt_with_refusals_or_without_planned_rows_cannot_approve(host, tmp_path):
    host.git_overrides[str(host.pinned)] = {"clean": False}
    path, sha = _receipt(host, tmp_path, planned=None)
    host.git_overrides.clear()
    _, refusals = _verify(host, path, sha)
    assert refusals[0] == "the approved receipt records 1 refusal(s); it cannot approve a write"
    assert refusals[1] == "the approved receipt has no planned changes (its dry run did not read the database)"
    path.unlink()
    path, sha = _receipt(host, tmp_path, backend=None, change_set="not-a-uuid")
    _, refusals = _verify(host, path, sha)
    assert refusals == [
        "the approved receipt has no database backend identity (its dry run did not read the database)",
        "the approved receipt's change_set 'not-a-uuid' is not a uuid"]


# ── the rollback receipt (#1383) ─────────────────────────────────────────────────────────────────────────

ROLLED = [{"feature_id": 11, "name": "vix_spot", "old_eligible": True, "new_eligible": False}]
SET_1 = uuid.UUID("0b0e4f7e-1111-4c1a-9d2e-5f6a7b8c9d00")


def _rollback_receipt(host: FakeHost, tmp_path: Path, status="dry_run", changes=ROLLED, refusals=(),
                      name="rollback.json"):
    report = {"status": status, "changes": changes, "backend": BACKEND, "refusals": list(refusals)}
    body = elig.canonical(elig.build_rollback_receipt(SET_1, report, DATABASE, host.host, host.boot, host.now,
                                                      CHANGE_SET))
    path = tmp_path / name
    path.write_bytes(body)
    return path, hashlib.sha256(body).hexdigest()


def _verify_rollback(host, path, sha, target=SET_1, database=DATABASE):
    return elig.verify_rollback_receipt(path, sha, target, host.host, host.boot, database, host.now)


def test_a_rollback_receipt_binds_its_rows_backend_set_host_and_time(host, tmp_path):
    path, sha = _rollback_receipt(host, tmp_path)
    receipt, refusals = _verify_rollback(host, path, sha)
    assert refusals == []
    assert (receipt["rollback_of"], receipt["change_set"], receipt["planned_changes"], receipt["backend"]) == (
        str(SET_1), CHANGE_SET, ROLLED, BACKEND)
    assert "units" not in receipt and "trees" not in receipt and "deploy_locks" not in receipt
    assert elig.Approval.from_receipt(receipt) == elig.Approval(tuple(ROLLED), BACKEND, uuid.UUID(CHANGE_SET))


def test_a_rollback_receipt_does_not_depend_on_the_deployed_code(host, tmp_path):
    """An emergency rollback: every forward check fails, the rollback receipt still verifies."""
    path, sha = _rollback_receipt(host, tmp_path)
    host.ancestors[str(host.current)] = False
    host.units["grid-api.service"] = {"LoadState": "not-found"}
    assert not _gate(host)["ok"]
    assert _verify_rollback(host, path, sha)[1] == []


def test_a_rollback_receipt_refuses_for_another_set_host_boot_or_database(host, tmp_path):
    path, sha = _rollback_receipt(host, tmp_path)
    other = uuid.UUID(int=1)
    assert f"the approved receipt rolls back {SET_1}, not {other}" in _verify_rollback(host, path, sha, other)[1]
    host.host, host.boot = "laptop", "boot-2"
    refusals = _verify_rollback(host, path, sha, database=DATABASE.replace("griddb", "griddb_copy"))[1]
    assert sorted(r.split(": receipt", 1)[1].split(":")[0] for r in refusals) == [
        "['boot_id']", "['database']", "['host']"]


def test_a_rollback_receipt_with_refusals_or_no_rows_cannot_approve(host, tmp_path):
    path, sha = _rollback_receipt(host, tmp_path, status="refused", refusals=["1 of 4 row(s) diverged"])
    refusals = _verify_rollback(host, path, sha)[1]
    assert refusals[:2] == ["the approved receipt records 1 refusal(s); it cannot approve a write",
                            "the approved receipt has no planned changes (its dry run did not read the database)"]


def test_a_stale_rollback_receipt_refuses(host, tmp_path):
    path, sha = _rollback_receipt(host, tmp_path)
    host.now += timedelta(seconds=elig.RECEIPT_MAX_AGE_S + 1)
    assert "s old; an apply takes one at most" in _verify_rollback(host, path, sha)[1][0]


def test_an_apply_receipt_cannot_approve_a_rollback_or_the_reverse(host, tmp_path):
    forward, forward_sha = _receipt(host, tmp_path)
    assert _verify_rollback(host, forward, forward_sha)[1] == [
        f"{forward} is not a version {elig.ROLLBACK_RECEIPT_VERSION} {elig.ROLLBACK_RECEIPT_KIND}"]
    back, back_sha = _rollback_receipt(host, tmp_path, name="back.json")
    assert _verify(host, back, back_sha)[1] == [f"{back} is not a version {elig.RECEIPT_VERSION} {elig.RECEIPT_KIND}"]


def test_a_rollback_needs_both_receipt_flags(host, tmp_path):
    path, sha = _rollback_receipt(host, tmp_path)
    for args in ((None, sha), (path, None)):
        refusals = _verify_rollback(host, *args)[1]
        assert refusals[0].startswith("--rollback with --apply needs --approved-receipt and --approved-receipt-sha256")


def test_divergences_name_every_changed_field():
    approved = {"a": 1, "b": {"c": [1], "d": "x"}, "e": 0}
    live = {"a": 1, "b": {"c": [2], "f": True}, "g": 0}
    assert elig.divergences(approved, live) == [
        "receipt['b']['c']: approved [1], now [2]",
        "receipt['b']['d']: approved 'x', absent now",
        "receipt['b']['f']: absent from the approved receipt, now True",
        "receipt['e']: approved 0, absent now",
        "receipt['g']: absent from the approved receipt, now 0",
    ]


# ── run() and main() without a database ─────────────────────────────────────────────────────────────────────


def test_a_refused_gate_never_opens_a_connection():
    engine = MagicMock()
    report = elig.run(engine, write=True, prerequisites={"ok": False, "refusals": ["no polarity"], "checks": []})
    assert report["status"] == "refused" and report["refusals"] == ["no polarity"]
    assert not report["writes_to_database"]
    engine.connect.assert_not_called()


def test_a_write_without_its_approval_or_boundary_is_a_programming_error():
    gate = {"ok": True, "refusals": []}
    boundary = elig.WriteBoundary(hold=MagicMock(), recheck=lambda: [])
    with pytest.raises(ValueError, match="write boundary"):
        elig.run(MagicMock(), write=True, prerequisites=gate)
    with pytest.raises(ValueError, match="approved receipt's approval"):
        elig.run(MagicMock(), write=True, prerequisites=gate, boundary=boundary)
    with pytest.raises(ValueError, match="a rollback needs its write boundary"):
        elig.run(MagicMock(), write=True, rollback_of=uuid.uuid4())
    with pytest.raises(ValueError, match="approved receipt's approval"):
        elig.run(MagicMock(), write=True, rollback_of=uuid.uuid4(), boundary=boundary)


@pytest.mark.parametrize("argv", [
    ["--rollback", "not-a-uuid"],
    ["--apply", "--receipt-out", "r.json"],
    ["--approved-receipt", "r.json", "--approved-receipt-sha256", "0" * 64],
    ["--rollback", "00000000-0000-0000-0000-000000000000", "--apply", "--receipt-out", "r.json"],
    ["--rollback", "00000000-0000-0000-0000-000000000000", "--code-root", "/tmp"],
    ["--rollback", "00000000-0000-0000-0000-000000000000", "--approved-receipt", "r.json"],
])
def test_main_refuses_flags_that_do_not_belong_together(argv):
    with pytest.raises(SystemExit) as exc:
        elig.main(argv)
    assert exc.value.code == 2


# ── the plan and the SQL ────────────────────────────────────────────────────────────────────────────────────


def test_the_plan_lists_are_disjoint():
    plan = elig.PLAN
    assert len(plan.enable) == 13 and len(plan.disable) == 22
    assert len(set(plan.enable)) == 13 and len(set(plan.disable)) == 22
    assert not set(plan.enable) & set(plan.disable)
    assert not any(name.startswith(plan.disable_prefix) for name in plan.enable)
    assert {"vix_spot", "yld_curve_2s10s"} <= set(plan.enable)


def _text_calls(tree: ast.AST):
    return [node for node in ast.walk(tree)
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and node.func.id == "text"]


def test_every_statement_is_a_whole_literal():
    """.claude/rules/security.md: no SQL is built in Python, not even from constants."""
    for path in (ROOT / "scripts" / "apply_eligibility_change.py", MIGRATION):
        tree = ast.parse(path.read_text())
        for call in _text_calls(tree):
            assert len(call.args) == 1 and isinstance(call.args[0], ast.Constant), (path.name, call.lineno)
    migration = ast.parse(MIGRATION.read_text())
    assert not [n for n in ast.walk(migration) if isinstance(n, ast.JoinedStr)]
    assert not [n for n in ast.walk(migration)
                if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute) and n.func.attr == "format"]


def test_the_host_reads_are_fixed_argv_and_never_write():
    source = (ROOT / "scripts" / "apply_eligibility_change.py").read_text()
    # systemctl show; git rev-parse, diff, merge-base --is-ancestor, ls-files, reflog
    assert source.count("subprocess.run(") == 6
    assert "shell=True" not in source
    assert source.count('"--no-optional-locks"') == 5          # git takes no index lock and refreshes nothing
    assert "O_RDONLY | os.O_NOFOLLOW" in source               # the deploy lock is opened read-only


def test_the_migration_changes_no_flag():
    """The migration's SQL never names feature_registry: a deploy flips nothing."""
    m = importlib.import_module("migrations.versions.eligibility_core_20261122")
    tree = ast.parse(MIGRATION.read_text())
    docstring = tree.body[0].value
    sql = [n.value for n in ast.walk(tree)
           if isinstance(n, ast.Constant) and isinstance(n.value, str) and n is not docstring]
    assert sql and not [s for s in sql if "feature_registry" in s or "model_eligible" in s]
    assert m.down_revision == "regime_history_revs_20261122"


@pytest.mark.parametrize("flag, value, ok", [
    ("--lock-timeout", "0", False), ("--lock-timeout", "61", False), ("--lock-timeout", "5", True),
    ("--statement-timeout", "9", False), ("--statement-timeout", "601", False), ("--statement-timeout", "60", True),
])
def test_timeouts_are_bounded_at_the_command_line(flag, value, ok):
    parse = {"--lock-timeout": elig._bounded_seconds(1, 60), "--statement-timeout": elig._bounded_seconds(10, 600)}[flag]
    if ok:
        assert parse(value) == int(value)
    else:
        with pytest.raises(Exception, match="between"):
            parse(value)


def test_the_blocker_query_is_one_literal_with_bound_values():
    sql = str(elig.BLOCKERS_SQL)
    assert ":key" in sql and "AccessShareLock" in sql and "pg_backend_pid()" in sql
    assert "{" not in sql and "%" not in sql
