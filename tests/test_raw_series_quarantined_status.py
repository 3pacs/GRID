"""``pull_status = 'QUARANTINED'`` is neither an observation nor a failure.

migrations/versions/raw_series_quarantined_20260926.py lets raw_series carry a
fourth status, QUARANTINED: a row that was once accepted as SUCCESS and has
since been found untrustworthy. These tests pin the two halves of its meaning
without a database server:

* invisible to the sanctioned readers (``store/observations.py``), even when
  the quarantined row is the newest one by obs_date and by pull_timestamp, and
  even when it comes from a second source (it must not trip the mixed-source
  fail-closed check);
* never counted as a failed pull: every failure counter matches ``= 'FAILED'``
  (or ``= 'PARTIAL'``) exactly, never "anything that is not SUCCESS".

The real-PostgreSQL half (constraint swap, lock/timeouts, downgrade guard,
``check_db_health`` counts) is tests/test_raw_series_quarantined_migration_pg.py.
"""

from __future__ import annotations

import importlib
import pathlib
import re
import sqlite3
from datetime import date, datetime, timedelta

import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

from store import observations as obs

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

REPO = pathlib.Path(__file__).resolve().parent.parent
T0 = datetime(2026, 9, 20, 6, 0, 0)
YF_SRC = 1
FILL_SRC = 2
MIGRATION = "migrations.versions.raw_series_quarantined_20260926"


@pytest.fixture()
def conn():
    engine = create_engine("sqlite://")
    md = MetaData()
    source_catalog = Table(
        "source_catalog", md,
        Column("id", Integer, primary_key=True),
        Column("name", String, nullable=False),
    )
    raw = Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, ForeignKey("source_catalog.id"), nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(engine)

    def row(d, v, status="SUCCESS", ts_offset_h=0, source_id=YF_SRC):
        return {
            "series_id": "YF:SPY:close", "source_id": source_id, "obs_date": d,
            "pull_timestamp": T0 + timedelta(hours=ts_offset_h),
            "value": v, "raw_payload": "{}", "pull_status": status,
        }

    rows = [
        row(date(2026, 9, 21), 600.0, ts_offset_h=0),
        row(date(2026, 9, 22), 602.0, ts_offset_h=24),
        # A later vintage for 09-22 that was quarantined: newer pull_timestamp
        # than the accepted one, so a status-blind "latest vintage wins" read
        # would serve it.
        row(date(2026, 9, 22), 5_000.0, status="QUARANTINED", ts_offset_h=30),
        # Quarantined rows newer by obs_date than anything accepted, one from
        # the same source and one from a second source writing the same id.
        row(date(2026, 9, 23), 9_999.0, status="QUARANTINED", ts_offset_h=48),
        row(date(2026, 9, 24), 8_888.0, status="QUARANTINED", ts_offset_h=72,
            source_id=FILL_SRC),
    ]
    with engine.begin() as c:
        c.execute(source_catalog.insert(), [
            {"id": YF_SRC, "name": "yfinance"},
            {"id": FILL_SRC, "name": "fill_missing_features"},
        ])
        c.execute(raw.insert(), rows)
    with engine.connect() as c:
        yield c


def test_read_latest_never_serves_a_quarantined_row(conn):
    o = obs.read_latest(conn, "YF:SPY:close")
    assert o is not None
    assert (o.obs_date, o.value, o.source) == (date(2026, 9, 22), 602.0, "yfinance")
    assert o.pull_timestamp == T0 + timedelta(hours=24)


def test_read_window_and_latest_n_drop_quarantined_rows(conn):
    window = obs.read_window(conn, "YF:SPY:close")
    assert [(o.obs_date, o.value) for o in window] == [
        (date(2026, 9, 21), 600.0),
        (date(2026, 9, 22), 602.0),
    ]
    latest_n = obs.read_latest_n(conn, "YF:SPY:close", 5)
    assert [o.value for o in latest_n] == [602.0, 600.0]


def test_quarantined_rows_from_another_source_do_not_make_a_read_mixed(conn):
    """Only SUCCESS rows count toward the mixed-source check: the second
    source here has quarantined rows only, so the read stays single-source
    instead of raising MixedSourceError."""
    assert obs.read_latest(conn, "YF:SPY:close").source == "yfinance"
    assert obs.read_latest(conn, "YF:SPY:close", source="fill_missing_features") is None


def test_observations_filters_on_success_only():
    """The reader is SUCCESS-only by construction (one bound status, ``:ok``),
    not a deny-list that a new status could slip through."""
    source = (REPO / "store" / "observations.py").read_text(encoding="utf-8")
    assert obs.SUCCESS == "SUCCESS"
    assert source.count("r.pull_status = :ok") == 3
    assert not re.search(r"pull_status\s*(!=|<>|NOT\s+IN)", source, re.I)


# ── not a failure ─────────────────────────────────────────────────────────

_COUNTER_FILES = (
    "scripts/hermes_health.py",
    "scripts/hermes_fixers.py",
    "intelligence/source_quality_ablation.py",
)
_SCANNED_ROOTS = ("alerts", "analysis", "api", "evaluation", "intelligence",
                  "normalization", "oracle", "scripts", "store", "trading")
_NOT_SUCCESS = re.compile(
    r"pull_status\s*(!=|<>)\s*'SUCCESS'|pull_status\s+NOT\s+IN\s*\(\s*'SUCCESS'",
    re.I,
)


@pytest.mark.parametrize("rel", _COUNTER_FILES)
def test_failure_counters_match_failed_exactly(rel):
    source = (REPO / rel).read_text(encoding="utf-8")
    assert "pull_status = 'FAILED'" in source
    assert not _NOT_SUCCESS.search(source)


def test_no_reader_treats_every_non_success_status_as_a_failure():
    """``pull_status != 'SUCCESS'`` would now count QUARANTINED rows as failed
    pulls (and page on a quarantine). Count FAILED/PARTIAL explicitly."""
    offenders = []
    for root in _SCANNED_ROOTS:
        for path in (REPO / root).rglob("*.py"):
            text = path.read_text(encoding="utf-8", errors="ignore")
            if _NOT_SUCCESS.search(text):
                offenders.append(str(path.relative_to(REPO)))
    assert offenders == []


def test_hermes_health_reports_quarantined_rows_separately_and_bounded():
    import inspect

    from scripts import hermes_health

    source = inspect.getsource(hermes_health.check_db_health)
    assert "pull_status = 'QUARANTINED'" in source
    assert "LIMIT :cap" in source
    assert '"quarantined_rows"' in source
    assert hermes_health.QUARANTINED_COUNT_CAP > 0


def test_operator_cycle_report_shows_quarantined_rows_only_when_present():
    from scripts.hermes_operator import _build_obsidian_cycle_body

    base = {"cycle": 7, "health": {"db": {"healthy": True, "failed_pulls_1h": 0,
                                           "failed_pulls_24h": 2}}}
    assert "quarantined" not in _build_obsidian_cycle_body(base, [])

    base["health"]["db"].update(quarantined_rows=100_000, quarantined_rows_capped=True)
    body = _build_obsidian_cycle_body(base, [])
    assert "quarantined raw_series rows (not failures): 100,000+" in body
    assert "failed pulls (1h / 24h): 0 / 2" in body


# ── migration shape (offline) ─────────────────────────────────────────────

class _RecordingOp:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql) -> None:
        self.statements.append(" ".join(str(sql).split()))


def _run(fn_name: str) -> list[str]:
    migration = importlib.import_module(MIGRATION)
    recorder = _RecordingOp()
    real_op = migration.op
    migration.op = recorder
    try:
        getattr(migration, fn_name)()
    finally:
        migration.op = real_op
    return recorder.statements


def test_migration_is_parented_on_robinhood_guards():
    migration = importlib.import_module(MIGRATION)
    assert migration.revision == "raw_series_quarantined_20260926"
    assert migration.down_revision == "robinhood_guards_20260924"
    assert len(migration.revision) <= 32


def test_upgrade_adds_not_valid_constraint_and_never_validates():
    stmts = _run("upgrade")
    assert stmts[:2] == ["SET LOCAL lock_timeout = '5s'",
                         "SET LOCAL statement_timeout = '15s'"]
    add, drop, rename = stmts[2:]
    assert add.startswith("ALTER TABLE raw_series ADD CONSTRAINT raw_series_pull_status_check_next")
    assert "('SUCCESS', 'PARTIAL', 'FAILED', 'QUARANTINED')" in add
    assert add.endswith("NOT VALID")
    assert drop == "ALTER TABLE raw_series DROP CONSTRAINT IF EXISTS raw_series_pull_status_check"
    assert rename.endswith("TO raw_series_pull_status_check")
    assert not any("VALIDATE" in s.upper().replace("NOT VALID", "") for s in stmts)


def test_downgrade_refuses_while_quarantined_rows_exist():
    stmts = _run("downgrade")
    assert stmts[2] == "LOCK TABLE raw_series IN ACCESS EXCLUSIVE MODE"
    assert "pull_status = 'QUARANTINED'" in stmts[3] and "RAISE EXCEPTION" in stmts[3]
    assert "('SUCCESS', 'PARTIAL', 'FAILED')) NOT VALID" in stmts[4]
    assert "QUARANTINED" not in stmts[4]
