"""scripts/td_backfill_universe.py ran on grid-svr for months as an
untracked, uncommitted file with a live plaintext Postgres password
hardcoded in its DSN, plus real bugs found by follow-up review:
DRY_RUN still reached the unconditional source_catalog write/commit,
and a totally failed run (every ticker failed) still stamped four
source_catalog rows as freshly pulled — three of which (DIVIDENDS,
SPLITS, STATS) this script never fetches at all. These tests guard the
fixes, not just the credential wiring.
"""
from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts" / "td_backfill_universe.py"


def _load_module(monkeypatch, env: dict[str, str]):
    for key in ("DB_HOST", "DB_PORT", "DB_NAME", "DB_USER", "DB_PASSWORD", "TWELVEDATA_API_KEY"):
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    spec = importlib.util.spec_from_file_location("td_backfill_universe", SCRIPT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def _default_env(**overrides: str) -> dict[str, str]:
    env = {
        "DB_HOST": "db.example",
        "DB_PORT": "6543",
        "DB_NAME": "testdb",
        "DB_USER": "testuser",
        "DB_PASSWORD": "not-a-real-secret",
        "TWELVEDATA_API_KEY": "test-key",
    }
    env.update(overrides)
    return env


class _FakeCursor:
    def __init__(self, work_rows, insert_rowcount: int = 1):
        self._work_rows = work_rows
        self._insert_rowcount = insert_rowcount
        self.executed: list[str] = []

    def execute(self, sql, params=None):
        self.executed.append(" ".join(sql.split()))

    def fetchall(self):
        return self._work_rows

    @property
    def rowcount(self):
        return self._insert_rowcount


class _FakeConn:
    def __init__(self, work_rows, insert_rowcount: int = 1):
        self.cursor_obj = _FakeCursor(work_rows, insert_rowcount)
        self.commits = 0
        self.closed = False

    def cursor(self, cursor_factory=None):
        return self.cursor_obj

    def commit(self):
        self.commits += 1

    def close(self):
        self.closed = True


def _install_fake_db(monkeypatch, work_rows, insert_rowcount: int = 1):
    fake_conn = _FakeConn(work_rows, insert_rowcount)
    monkeypatch.setattr("psycopg2.connect", lambda **kwargs: fake_conn)
    return fake_conn


def test_no_hardcoded_password_in_source() -> None:
    src = SCRIPT.read_text(encoding="utf-8")
    assert "gridmaster" not in src
    assert 'password=' not in src.split("def _connect_params_from_env")[0]


def test_connect_params_are_built_from_environment(monkeypatch) -> None:
    mod = _load_module(monkeypatch, _default_env())

    assert mod.CONNECT_PARAMS == {
        "host": "db.example",
        "port": 6543,
        "dbname": "testdb",
        "user": "testuser",
        "password": "not-a-real-secret",
    }


def test_exits_clearly_when_db_password_missing(monkeypatch) -> None:
    with pytest.raises(SystemExit, match="DB_PASSWORD"):
        _load_module(monkeypatch, _default_env(DB_PASSWORD=""))


def test_dry_run_performs_no_writes_or_commits(monkeypatch) -> None:
    mod = _load_module(monkeypatch, _default_env(DRY_RUN="1"))
    fake_conn = _install_fake_db(monkeypatch, work_rows=[
        {"ticker": "AAA", "bucket": "DEAD"},
        {"ticker": "BBB", "bucket": "STALE_30+"},
    ])

    rc = mod.main()

    assert rc == 0
    assert fake_conn.commits == 0
    assert not any("UPDATE" in sql for sql in fake_conn.cursor_obj.executed)
    assert not any("INSERT" in sql for sql in fake_conn.cursor_obj.executed)
    assert fake_conn.closed is True


def test_total_failure_does_not_stamp_source_catalog_and_exits_nonzero(monkeypatch) -> None:
    mod = _load_module(monkeypatch, _default_env())
    fake_conn = _install_fake_db(monkeypatch, work_rows=[{"ticker": "AAA", "bucket": "DEAD"}])
    monkeypatch.setattr(mod, "fetch_td", lambda ticker, start, end: [])
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    rc = mod.main()

    assert rc == 1
    assert not any("source_catalog" in sql for sql in fake_conn.cursor_obj.executed)


def test_success_stamps_only_twelvedata_not_dividends_splits_stats(monkeypatch) -> None:
    mod = _load_module(monkeypatch, _default_env())
    fake_conn = _install_fake_db(monkeypatch, work_rows=[{"ticker": "AAA", "bucket": "DEAD"}])
    monkeypatch.setattr(mod, "fetch_td", lambda ticker, start, end: [{"date": "2026-01-01", "close": 1.0}])
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    rc = mod.main()

    assert rc == 0
    stamp_sql = [sql for sql in fake_conn.cursor_obj.executed if "source_catalog" in sql]
    assert len(stamp_sql) == 1
    assert "TWELVEDATA" in stamp_sql[0]
    assert "DIVIDENDS" not in stamp_sql[0]
    assert "SPLITS" not in stamp_sql[0]
    assert "STATS" not in stamp_sql[0]


def test_all_rows_xbrl_protected_is_not_total_failure(monkeypatch) -> None:
    """The bug this fixes: inserted_total == 0 was read as "every ticker
    failed." A successful fetch whose every (ticker, obs_date) row is
    already SEC-XBRL-owned also yields inserted_total == 0 via the ON
    CONFLICT ... WHERE guard's cur.rowcount == 0 — that's the intended,
    protective outcome, not a failure, and the source was still reached.
    """
    mod = _load_module(monkeypatch, _default_env())
    fake_conn = _install_fake_db(
        monkeypatch,
        work_rows=[{"ticker": "AAA", "bucket": "DEAD"}],
        insert_rowcount=0,  # every INSERT ... ON CONFLICT ... WHERE is a no-op
    )
    monkeypatch.setattr(mod, "fetch_td", lambda ticker, start, end: [{"date": "2026-01-01", "close": 1.0}])
    monkeypatch.setattr(mod.time, "sleep", lambda _s: None)

    rc = mod.main()

    assert rc == 0
    stamp_sql = [sql for sql in fake_conn.cursor_obj.executed if "source_catalog" in sql]
    assert len(stamp_sql) == 1  # fetched_ok > 0 still stamps TWELVEDATA as pulled


def test_redact_api_key_strips_the_key_from_arbitrary_text(monkeypatch) -> None:
    mod = _load_module(monkeypatch, _default_env(TWELVEDATA_API_KEY="super-secret-key"))

    redacted = mod._redact_api_key(
        "GET https://api.twelvedata.com/time_series?apikey=super-secret-key timed out"
    )

    assert "super-secret-key" not in redacted
    assert "***REDACTED***" in redacted


def test_fetch_td_network_error_does_not_leak_api_key(monkeypatch, capsys) -> None:
    mod = _load_module(monkeypatch, _default_env(TWELVEDATA_API_KEY="super-secret-key"))

    def _raise(*_args, **_kwargs):
        raise requests.exceptions.ConnectionError(
            "GET https://api.twelvedata.com/time_series?apikey=super-secret-key timed out"
        )

    monkeypatch.setattr(mod.requests, "get", _raise)

    bars = mod.fetch_td("AAA", mod.date(2026, 1, 1), mod.date(2026, 1, 2))

    assert bars == []
    captured = capsys.readouterr()
    assert "super-secret-key" not in captured.err
