"""The startup resolver, scripts/bulk_resolve.py, applies the obs_date guard.

scripts/server_startup.sh runs bulk_resolve.py after every reboot. It
bypasses normalization/resolver.py and writes resolved_series with one
INSERT ... SELECT per feature, so the resolver's Python guard never sees its
rows. Its two raw_series reads (the insert and the --dry-run count) carry the
same verdict in SQL, with the series tables matched in Python and passed as
bound id arrays.

These tests fail if the startup script stops running bulk_resolve.py
unnoticed, if either raw_series read loses the guard, or if the SQL's logic
drifts from ``normalization.resolver.obs_date_verdict``. The SQL itself runs
against PostgreSQL in tests/test_bulk_resolve_future_guard_pg.py.
"""

from __future__ import annotations

import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

import scripts.bulk_resolve as bulk_resolve
from normalization.resolver import (
    FUTURE_OBS_TOLERANCE_DAYS,
    LOCAL_DATE_TOLERANCE_DAYS,
    SKIP_COUNT_KEYS,
    obs_date_verdict,
)

ROOT = Path(__file__).resolve().parents[1]
STARTUP = ROOT / "scripts" / "server_startup.sh"
BULK = ROOT / "scripts" / "bulk_resolve.py"

#: The predicate both raw_series reads must carry, whitespace-normalised.
GUARD_SQL = " ".join("""
    AND (
      rs.obs_date <= (rs.pull_timestamp AT TIME ZONE 'UTC')::date + :observed_days
      OR (rs.series_id = ANY(CAST(:local_date_sids AS text[]))
          AND rs.obs_date <= (rs.pull_timestamp AT TIME ZONE 'UTC')::date + :local_date_days)
      OR rs.series_id = ANY(CAST(:event_dated_sids AS text[]))
    )
""".split())

SERIES = ["VIXCLS", "pla_activity:aircraft_sorties", "sge:usd_per_oz",
          "EARNINGS:AAPL:estimate", "ephemeris.lunar_phase"]


def _normalise(sql: str) -> str:
    return " ".join(sql.split())


def _sql_literals() -> list[str]:
    tree = ast.parse(BULK.read_text())
    return [node.value for node in ast.walk(tree)
            if isinstance(node, ast.Constant) and isinstance(node.value, str)
            and ("raw_series" in node.value or "resolved_series" in node.value)]


@pytest.mark.unit
def test_startup_script_runs_bulk_resolve():
    live = [line for line in STARTUP.read_text().splitlines()
            if "scripts/bulk_resolve.py" in line and not line.lstrip().startswith("#")]
    assert live, "server_startup.sh no longer runs bulk_resolve.py: revisit this guard"


@pytest.mark.unit
def test_every_raw_series_read_carries_the_guard():
    reads = [sql for sql in _sql_literals() if "FROM raw_series" in sql]
    assert len(reads) == 2, "expected the insert and the --dry-run count"
    for sql in reads:
        assert GUARD_SQL in _normalise(sql), sql


@pytest.mark.unit
def test_the_only_resolved_series_insert_is_guarded():
    inserts = [sql for sql in _sql_literals() if "INSERT INTO resolved_series" in sql]
    assert len(inserts) == 1
    assert "FROM raw_series" in inserts[0]
    assert GUARD_SQL in _normalise(inserts[0])


@pytest.mark.unit
def test_guard_params_come_from_the_resolver_tables():
    params = bulk_resolve.obs_date_guard_params(SERIES)
    assert params == {
        "observed_days": FUTURE_OBS_TOLERANCE_DAYS,
        "local_date_days": LOCAL_DATE_TOLERANCE_DAYS,
        "local_date_sids": ["pla_activity:aircraft_sorties", "sge:usd_per_oz"],
        "event_dated_sids": ["EARNINGS:AAPL:estimate"],
    }


def _sql_admits(params: dict, series_id: str, ahead: int) -> bool:
    """The guard predicate, evaluated the way PostgreSQL evaluates it."""
    return (ahead <= params["observed_days"]
            or (series_id in params["local_date_sids"] and ahead <= params["local_date_days"])
            or series_id in params["event_dated_sids"])


@pytest.mark.unit
@pytest.mark.parametrize("series_id", SERIES)
@pytest.mark.parametrize("ahead", [-30, -1, 0, 1, 2, 5, 300])
def test_sql_logic_matches_the_resolver_verdict(series_id, ahead):
    pull = datetime(2026, 11, 21, 23, 30, tzinfo=timezone.utc)
    obs = pull.date() + timedelta(days=ahead)
    resolver_admits = obs_date_verdict(series_id, obs, pull) not in SKIP_COUNT_KEYS
    params = bulk_resolve.obs_date_guard_params([series_id])
    assert _sql_admits(params, series_id, ahead) is resolver_admits


class _Recorder:
    """Engine double: records each statement and its bound parameters."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def _ctx(self):
        recorder = self

        class _Result:
            rowcount = 0

            def scalar(self):
                return 0

        class _Conn:
            def execute(self, statement, params=None):
                recorder.calls.append((_normalise(str(statement)), dict(params or {})))
                return _Result()

        class _Ctx:
            def __enter__(self):
                return _Conn()

            def __exit__(self, *_exc):
                return False

        return _Ctx()

    def begin(self):
        return self._ctx()

    def connect(self):
        return self._ctx()


@pytest.mark.unit
@pytest.mark.parametrize("dry_run", [False, True], ids=["insert", "dry-run"])
def test_resolve_feature_batch_sends_the_guard_and_its_params(dry_run):
    engine = _Recorder()

    bulk_resolve.resolve_feature_batch(engine, 7, list(SERIES), dry_run=dry_run)

    assert len(engine.calls) == 1
    sql, params = engine.calls[0]
    assert GUARD_SQL in sql
    assert ("INSERT INTO resolved_series" in sql) is (not dry_run)
    assert params["sids"] == SERIES
    for key, value in bulk_resolve.obs_date_guard_params(SERIES).items():
        assert params[key] == value


@pytest.mark.unit
def test_guard_uses_the_pull_utc_date_not_the_session_date():
    """``pull_timestamp::date`` would follow the session TimeZone."""
    for sql in _sql_literals():
        if "FROM raw_series" in sql:
            assert "AT TIME ZONE 'UTC')::date" in sql
            assert "rs.obs_date <= rs.pull_timestamp::date" not in _normalise(sql)


@pytest.mark.unit
def test_module_docstring_states_the_guard_and_that_it_does_not_count():
    doc = ast.get_docstring(ast.parse(BULK.read_text())) or ""
    assert "server_startup.sh" in doc and "obs_date guard" in doc
    assert "not counted here" in doc
