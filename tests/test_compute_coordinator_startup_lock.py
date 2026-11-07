"""Coordinator startup takes no ACCESS EXCLUSIVE lock when the schema is current.

``ALTER TABLE compute_workers ADD COLUMN IF NOT EXISTS ollama_models`` takes
ACCESS EXCLUSIVE even when the column exists. On 2026-11-07 the nightly
pg_dump's ACCESS SHARE made every coordinator start time out (crash loop
03:32-04:34Z, deploys failing at "Restart grid-coordinator"). The ALTER now
runs only when the column is missing.
"""

from __future__ import annotations

from scripts import compute_coordinator as coordinator


class _Cur:
    def __init__(self, has_column: bool) -> None:
        self.has_column = has_column
        self.statements: list[str] = []
        self._last = ""

    def execute(self, sql, *args, **kwargs):
        self._last = str(sql)
        self.statements.append(self._last)

    def fetchone(self):
        if "information_schema.columns" in self._last:
            return (1,) if self.has_column else None
        return None


class _Conn:
    def __init__(self, cur: _Cur) -> None:
        self._cur = cur

    def cursor(self, *args, **kwargs):
        return self._cur


def _startup(has_column: bool) -> list[str]:
    cur = _Cur(has_column)
    coordinator._create_tables(_Conn(cur))
    return cur.statements


def test_startup_skips_alter_when_the_column_exists():
    statements = _startup(has_column=True)
    assert not any("ALTER TABLE" in s for s in statements)
    assert not any("lock_timeout" in s for s in statements)


def test_startup_adds_the_missing_column_under_a_lock_timeout():
    statements = _startup(has_column=False)
    alter = statements.index(coordinator.COMPUTE_WORKERS_ADD_OLLAMA_MODELS_SQL)
    assert statements[alter - 1] == "SET lock_timeout = '5s'"
    assert statements[alter + 1] == "RESET lock_timeout"
