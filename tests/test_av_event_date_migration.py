"""Offline migration control-flow and source-bootstrap parity contracts."""
import ast
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

ROOT = Path(__file__).resolve().parents[1]


def load_migration(exists=True):
    path = ROOT / "migrations/versions/av_event_dates_20261108.py"
    tree = ast.parse(path.read_text())
    tree.body = [n for n in tree.body if not isinstance(n, ast.ImportFrom)]
    op = MagicMock()
    op.get_bind.return_value.exec_driver_sql.return_value.scalar.return_value = (
        "earnings_events" if exists else None)
    scope = {"op": op}
    exec(compile(tree, str(path), "exec"), scope)
    return SimpleNamespace(**scope), op


def test_missing_runtime_table_skips_bridge_creation():
    migration, op = load_migration(False)
    migration.upgrade()
    assert op.execute.call_count == 2  # transaction-local timeouts only
    op.get_bind.return_value.exec_driver_sql.assert_called_once_with(
        "SELECT to_regclass('earnings_events')")


def test_forward_contract_and_bootstrap_stay_aligned():
    migration, op = load_migration()
    migration.upgrade()
    statements = [c.args[0] for c in op.execute.call_args_list]
    assert migration.VIEW_SQL in statements and migration.SYNC_SQL in statements
    assert "NULL::date AS earnings_date" in migration.VIEW_SQL
    assert "INSERT INTO" not in migration.SYNC_SQL
    assert "GRANT SELECT ON earnings_calendar_compat TO grid" in statements
    assert "GRANT EXECUTE ON FUNCTION sync_earnings_events_to_calendar() TO grid" in statements
    bootstrap = (ROOT / "scripts/migrations/20260517_earnings_events_compat.sql").read_text()
    assert migration.VIEW_SQL.strip() in bootstrap
    assert migration.SYNC_SQL.strip() in bootstrap


def test_downgrade_never_restores_false_calendar_dates():
    migration, op = load_migration()
    with pytest.raises(RuntimeError, match="fabricated earnings dates"):
        migration.downgrade()
    op.execute.assert_not_called()
