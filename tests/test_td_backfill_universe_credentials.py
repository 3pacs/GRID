"""scripts/td_backfill_universe.py ran on grid-svr for months as an
untracked, uncommitted file with a live plaintext Postgres password
hardcoded in its DSN. Committed here for the first time (2026-09-17)
alongside the fix. This guards the fix, not just the wiring.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "td_backfill_universe.py"


def test_no_hardcoded_password_in_source() -> None:
    src = SCRIPT.read_text(encoding="utf-8")
    assert "gridmaster" not in src
    assert 'password=' not in src.split("def _dsn_from_env")[0]


def test_dsn_is_built_from_environment(monkeypatch) -> None:
    monkeypatch.setenv("DB_HOST", "db.example")
    monkeypatch.setenv("DB_PORT", "6543")
    monkeypatch.setenv("DB_NAME", "testdb")
    monkeypatch.setenv("DB_USER", "testuser")
    monkeypatch.setenv("DB_PASSWORD", "not-a-real-secret")
    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")

    spec = importlib.util.spec_from_file_location("td_backfill_universe", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]

    assert mod.DSN == (
        "host=db.example port=6543 dbname=testdb user=testuser "
        "password=not-a-real-secret"
    )


def test_exits_clearly_when_db_password_missing(monkeypatch) -> None:
    monkeypatch.delenv("DB_PASSWORD", raising=False)
    monkeypatch.setenv("TWELVEDATA_API_KEY", "test-key")

    spec = importlib.util.spec_from_file_location("td_backfill_universe", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    try:
        spec.loader.exec_module(mod)  # type: ignore[union-attr]
        raised = False
    except SystemExit as exc:
        raised = True
        assert "DB_PASSWORD" in str(exc)
    assert raised
