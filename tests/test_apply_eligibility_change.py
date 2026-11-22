"""scripts/apply_eligibility_change.py without a database (#1301): the literal gate, the deployed-code evidence,
the owner receipt, the plan and the SQL shape.

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
from datetime import timedelta
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scripts import apply_eligibility_change as elig
from tests.eligibility_fakes import (
    GOOD_REGIME, HYSTERESIS, NEXT_SHA, PINNED_SHA, POLARITY, FakeHost, write_tree,
)

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "versions" / "eligibility_core_20261122.py"
DATABASE = "postgresql://grid_eligibility_owner:***@127.0.0.1:5432/griddb"
PLANNED = [{"feature_id": 11, "name": "vix_spot", "old_eligible": False, "new_eligible": True}]


# ── the literals ────────────────────────────────────────────────────────────────────────────────────────────


def test_the_gate_refuses_on_this_tree_today():
    """Neither prerequisite has landed, so --apply must refuse.

    When the Oracle polarity fix or #1307 adds its constant, this test is the one to update.
    """
    gate = elig.check_prerequisites([("repo", ROOT)])
    assert not gate["ok"]
    assert {(c["file"], c["constant"], c["found"], c["ok"]) for c in gate["checks"]} == {
        ("oracle/engine.py", "SIGNAL_POLARITY_RULE", None, False),
        ("scripts/auto_regime.py", "REGIME_HYSTERESIS_RULE", None, False),
    }
    assert any("SIGNAL_POLARITY_RULE is not defined" in r for r in gate["refusals"])
    assert any("REGIME_HYSTERESIS_RULE is not defined" in r for r in gate["refusals"])


def test_the_gate_passes_with_both_exact_literals(tmp_path):
    gate = elig.check_prerequisites([("t", write_tree(tmp_path))])
    assert gate["ok"] and gate["refusals"] == []
    assert [c["found"] for c in gate["checks"]] == [POLARITY.expected, HYSTERESIS.expected]


@pytest.mark.parametrize("oracle, problem", [
    (None, "cannot read"),
    ("import os\n", "is not defined at module level"),
    (f'{POLARITY.constant} = "yes"\n', "is 'yes', not"),
    (f'{POLARITY.constant} = "{POLARITY.expected}".lower()\n', "is not a string literal"),
    (f'{POLARITY.constant} = f"{POLARITY.expected}"\n', "is not a string literal"),
    (f'if True:\n    {POLARITY.constant} = "{POLARITY.expected}"\n', "is not defined at module level"),
    (f'{POLARITY.constant} = "{POLARITY.expected}"\n{POLARITY.constant} = ""\n', "is '', not"),
    (f'{POLARITY.constant} = "{POLARITY.expected}"\ndef (:\n', "does not parse"),
])
def test_the_gate_refuses_anything_but_the_exact_literal(tmp_path, oracle, problem):
    gate = elig.check_prerequisites([("t", write_tree(tmp_path, oracle, GOOD_REGIME))])
    assert not gate["ok"]
    assert len(gate["refusals"]) == 1 and problem in gate["refusals"][0]
    assert "Oracle polarity fix" in gate["refusals"][0]


def test_the_gate_reads_without_importing(tmp_path):
    oracle = f'raise SystemExit("imported")\n{POLARITY.constant} = "{POLARITY.expected}"\n'
    assert elig.check_prerequisites([("t", write_tree(tmp_path, oracle, GOOD_REGIME))])["ok"]


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
    assert trees[str(host.current)]["prerequisites"] == {POLARITY.constant: POLARITY.expected,
                                                         HYSTERESIS.constant: HYSTERESIS.expected}
    assert evidence["host"] == "grid-svr" and evidence["boot_id"] == "boot-1"


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
    (repo / POLARITY.path).touch()                            # stale stat info, same content: still clean
    index = repo / ".git" / "index"
    stamp = (index.stat().st_mtime_ns, index.read_bytes())
    identity = elig.git_identity(repo)
    assert identity == {"toplevel": str(repo.resolve()), "head": head,
                        "head_tree": _git("rev-parse", "HEAD^{tree}", cwd=repo).stdout.strip(), "clean": True}
    assert (index.stat().st_mtime_ns, index.read_bytes()) == stamp and not (repo / ".git" / "index.lock").exists()
    (repo / POLARITY.path).write_text("changed\n")
    assert elig.git_identity(repo)["clean"] is False
    with pytest.raises(RuntimeError, match="git rev-parse"):
        elig.git_identity(tmp_path / "not-a-repo")


@pytest.mark.skipif(not Path("/proc/self/stat").exists(), reason="needs /proc")
def test_the_real_proc_probes_read_this_process():
    import os

    assert Path(elig.proc_cwd(os.getpid())) == Path.cwd().resolve()
    assert elig.proc_start(os.getpid()).isdigit() and elig.proc_start(os.getpid()) == elig.proc_start(os.getpid())
    assert len(elig.boot_id()) == 36


def test_the_inventory_includes_the_pinned_units():
    assert {"grid-scheduler.service", "grid-realtime.service"} <= set(elig.SERVICE_UNITS)
    assert "MainPID" in elig.UNIT_PROPERTIES and "NeedDaemonReload" in elig.UNIT_PROPERTIES


# ── the owner receipt ───────────────────────────────────────────────────────────────────────────────────────


def _receipt(host: FakeHost, tmp_path: Path, planned=PLANNED, database=DATABASE):
    """A dry run's receipt, saved as --receipt-out would; returns (path, sha256)."""
    body = elig.canonical(elig.build_receipt(_gate(host), planned, database))
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
    assert refusals == [
        "live evidence differs from the approved receipt: receipt['units']['grid-scheduler.service']['main_pid']: "
        "approved 2003, now 7777",
        "live evidence differs from the approved receipt: receipt['units']['grid-scheduler.service']"
        "['process_start']: approved '200300', now '777700'",
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
    assert refusals[0] == "the approved receipt records 1 refusal(s); it cannot approve an apply"
    assert refusals[1] == "the approved receipt has no planned changes (its dry run did not read the database)"


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


def test_an_apply_without_approved_rows_is_a_programming_error():
    with pytest.raises(ValueError, match="approved receipt's planned changes"):
        elig.run(MagicMock(), write=True, prerequisites={"ok": True, "refusals": []})


@pytest.mark.parametrize("argv", [
    ["--rollback", "not-a-uuid"],
    ["--apply", "--receipt-out", "r.json"],
    ["--approved-receipt", "r.json", "--approved-receipt-sha256", "0" * 64],
    ["--rollback", "00000000-0000-0000-0000-000000000000", "--apply", "--approved-receipt", "r.json"],
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
    assert source.count("subprocess.run(") == 3                # systemctl show, git rev-parse, git diff
    assert "shell=True" not in source
    assert source.count('"--no-optional-locks"') == 2          # git takes no index lock and refreshes nothing


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
