"""scripts/apply_eligibility_change.py without a database (#1301): the prerequisite gate, the plan, the SQL shape.

The database behaviour (authority, apply, rollback, divergence) is proved on real PostgreSQL in
tests/test_eligibility_core_migration_pg.py.
"""

from __future__ import annotations

import ast
import importlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scripts import apply_eligibility_change as elig

ROOT = Path(__file__).resolve().parents[1]
MIGRATION = ROOT / "migrations" / "versions" / "eligibility_core_20261122.py"

POLARITY, HYSTERESIS = elig.PREREQUISITES


def _tree(tmp_path: Path, oracle: str | None = None, regime: str | None = None) -> Path:
    """A code tree whose two prerequisite files hold the given module source (None: the file is absent)."""
    for rel, source in ((POLARITY.path, oracle), (HYSTERESIS.path, regime)):
        if source is not None:
            (tmp_path / rel).parent.mkdir(parents=True, exist_ok=True)
            (tmp_path / rel).write_text(source)
    return tmp_path


GOOD_ORACLE = f'"""Oracle."""\nimport os\n{POLARITY.constant} = "{POLARITY.expected}"\n'
GOOD_REGIME = f'{HYSTERESIS.constant}: str = "{HYSTERESIS.expected}"\n'


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
    gate = elig.check_prerequisites([("t", _tree(tmp_path, GOOD_ORACLE, GOOD_REGIME))])
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
    gate = elig.check_prerequisites([("t", _tree(tmp_path, oracle, GOOD_REGIME))])
    assert not gate["ok"]
    assert len(gate["refusals"]) == 1 and problem in gate["refusals"][0]
    assert "Oracle polarity fix" in gate["refusals"][0]


def test_the_gate_reads_without_importing(tmp_path):
    oracle = f'raise SystemExit("imported")\n{POLARITY.constant} = "{POLARITY.expected}"\n'
    assert elig.check_prerequisites([("t", _tree(tmp_path, oracle, GOOD_REGIME))])["ok"]


def _inventory(tree, **overrides) -> dict[str, dict[str, str] | Exception]:
    """Every inventory unit loaded on ``tree``, with per-unit overrides."""
    table = {u: {"LoadState": "loaded", "ActiveState": "active", "WorkingDirectory": str(tree)}
             for u in elig.SERVICE_UNITS}
    table.update(overrides)
    return table


def _units(table: dict[str, dict[str, str] | Exception]):
    def show(unit: str):
        value = table.get(unit, {"LoadState": "not-found"})
        if isinstance(value, Exception):
            raise value
        return value
    return show


def test_every_deployed_tree_must_pass(tmp_path, monkeypatch):
    """A pinned older release (the scheduler's, say) without the constants refuses even if this tree has them."""
    current = _tree(tmp_path / "current", GOOD_ORACLE, GOOD_REGIME)
    pinned = _tree(tmp_path / "pinned", GOOD_ORACLE, "import os\n")
    monkeypatch.setattr(elig, "ROOT", current)
    show = _units(_inventory(current, **{
        "grid-scheduler.service": {"LoadState": "loaded", "ActiveState": "active", "WorkingDirectory": str(pinned)}}))
    roots, problems, notes = elig.code_roots(show=show)
    assert problems == [] and notes == []
    assert [path for _, path in roots] == [current, pinned]
    assert roots[1][0] == "grid-scheduler.service (active)"
    gate = elig.deployed_prerequisites(show=show)
    assert not gate["ok"]
    assert gate["refusals"] == [
        f"REGIME_HYSTERESIS_RULE is not defined at module level in {pinned / HYSTERESIS.path} "
        f"(needs {HYSTERESIS.provided_by})"]


def test_extra_code_roots_are_checked_too(tmp_path, monkeypatch):
    monkeypatch.setattr(elig, "ROOT", _tree(tmp_path / "current", GOOD_ORACLE, GOOD_REGIME))
    gate = elig.deployed_prerequisites([tmp_path / "missing"], show=_units(_inventory(elig.ROOT)))
    assert not gate["ok"] and len(gate["refusals"]) == 2
    assert all("cannot read" in r for r in gate["refusals"])


@pytest.mark.parametrize("props, problem", [
    ({"LoadState": "loaded", "WorkingDirectory": ""}, "has no absolute WorkingDirectory"),
    ({"LoadState": "loaded", "WorkingDirectory": "~"}, "has no absolute WorkingDirectory"),
    (RuntimeError("systemctl show exited 1"), "cannot ask systemd about grid-worker.service"),
])
def test_a_unit_whose_tree_cannot_be_named_refuses(tmp_path, monkeypatch, props, problem):
    monkeypatch.setattr(elig, "ROOT", _tree(tmp_path, GOOD_ORACLE, GOOD_REGIME))
    gate = elig.deployed_prerequisites(show=_units(_inventory(elig.ROOT, **{"grid-worker.service": props})))
    assert not gate["ok"]
    assert len(gate["refusals"]) == 1 and problem in gate["refusals"][0]


def test_a_host_without_systemd_refuses_even_with_valid_local_literals(tmp_path, monkeypatch):
    """Codex (6011670010): no systemctl was a note, so a laptop with compliant literals could certify the database."""
    monkeypatch.setattr(elig, "ROOT", _tree(tmp_path, GOOD_ORACLE, GOOD_REGIME))
    gate = elig.deployed_prerequisites(show=lambda unit: None)
    assert not gate["ok"]
    assert gate["refusals"] == [
        "systemctl is not available on this host, so the deployed service inventory cannot be checked; "
        "run --apply on the host that runs the GRID units"]
    assert all(c["ok"] for c in gate["checks"])               # the local literals were fine


@pytest.mark.parametrize("unit", elig.SERVICE_UNITS)
def test_a_missing_inventory_unit_refuses_even_with_valid_local_literals(tmp_path, monkeypatch, unit):
    tree = _tree(tmp_path, GOOD_ORACLE, GOOD_REGIME)
    monkeypatch.setattr(elig, "ROOT", tree)
    gate = elig.deployed_prerequisites(show=_units(_inventory(tree, **{unit: {"LoadState": "not-found"}})))
    assert not gate["ok"]
    assert gate["refusals"] == [f"{unit} is in the deployed service inventory but is not-found here, "
                                "so its code tree cannot be checked"]
    assert all(c["ok"] for c in gate["checks"])


def test_the_full_inventory_on_compliant_trees_passes(tmp_path, monkeypatch):
    tree = _tree(tmp_path, GOOD_ORACLE, GOOD_REGIME)
    monkeypatch.setattr(elig, "ROOT", tree)
    gate = elig.deployed_prerequisites(show=_units(_inventory(tree)))
    assert gate["ok"] and gate["refusals"] == []


def test_the_inventory_includes_the_pinned_units():
    assert {"grid-scheduler.service", "grid-realtime.service"} <= set(elig.SERVICE_UNITS)


def test_systemd_working_directory_prefixes_are_stripped(tmp_path, monkeypatch):
    monkeypatch.setattr(elig, "ROOT", _tree(tmp_path / "a", GOOD_ORACLE, GOOD_REGIME))
    other = _tree(tmp_path / "b", GOOD_ORACLE, GOOD_REGIME)
    roots, problems, _ = elig.code_roots(show=_units(_inventory(elig.ROOT, **{
        "grid-hermes.service": {"LoadState": "loaded", "ActiveState": "inactive", "WorkingDirectory": f"-{other}"}})))
    assert problems == [] and roots[-1] == ("grid-hermes.service (inactive)", other)


def test_a_refused_gate_never_opens_a_connection():
    engine = MagicMock()
    report = elig.run(engine, write=True, prerequisites={"ok": False, "refusals": ["no polarity"], "checks": []})
    assert report["status"] == "refused" and report["refusals"] == ["no polarity"]
    assert not report["writes_to_database"]
    engine.connect.assert_not_called()


def test_rollback_needs_a_uuid():
    with pytest.raises(SystemExit) as exc:
        elig.main(["--rollback", "not-a-uuid"])
    assert exc.value.code == 2


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
