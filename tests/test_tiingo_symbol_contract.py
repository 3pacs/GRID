"""TIINGO rows that are another instrument's price never reach a reader.

The Tiingo puller used to strip ``=F``/``=X``: TIINGO's ``YF:CL=F:close`` is
Colgate-Palmolive and ``YF:HG=F:close`` Hamilton Insurance (grid-svr,
2026-11-09), and ``cl_close`` resolved to Colgate on 2,722 dates. Tiingo also
wrote rows for acquired symbols after their last session (K: 2026-06-08,
Kellanova left 2025-12). ``tiingo_symbol_contract`` names those rows; the
resolver and every ``store.observations`` raw reader skip them.

SQLite fixture only.
"""

from __future__ import annotations

import inspect
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
)

import tiingo_symbol_contract as tsc
from store import observations as obs

YF, TI = 1, 2
T0 = datetime(2026, 6, 20, 6, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _cutoffs(monkeypatch):
    monkeypatch.setattr(tsc, "_last_sessions", lambda: {"K": date(2025, 12, 11)})


@pytest.fixture()
def conn():
    engine = create_engine("sqlite://")
    md = MetaData()
    Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String))
    raw = Table("raw_series", md,
                Column("series_id", String), Column("source_id", Integer), Column("obs_date", Date),
                Column("pull_timestamp", DateTime), Column("value", Float), Column("raw_payload", Text),
                Column("pull_status", String))
    md.create_all(engine)

    def r(sid, src, d, v, h=0):
        return {"series_id": sid, "source_id": src, "obs_date": d, "value": v, "raw_payload": "{}",
                "pull_timestamp": (T0 + timedelta(hours=h)).replace(tzinfo=None), "pull_status": "SUCCESS"}

    with engine.begin() as c:
        c.execute(md.tables["source_catalog"].insert(), [{"id": YF, "name": "yfinance"},
                                                          {"id": TI, "name": "TIINGO"}])
        c.execute(raw.insert(), [
            r("YF:CL=F:close", YF, date(2026, 5, 21), 85.0),
            r("YF:CL=F:close", TI, date(2026, 5, 21), 96.6, h=5),   # Colgate, newer vintage
            r("YF:CL=F:close", TI, date(2026, 5, 22), 96.0),        # Colgate, no yfinance row
            r("YF:K:close", TI, date(2025, 12, 11), 83.5),
            r("YF:K:close", TI, date(2026, 6, 8), 39.75),          # after Kellanova's last session
            r("YF:EURUSD=X:close", TI, date(2026, 5, 21), 1.08),
            r("YF:AAPL:close", TI, date(2026, 5, 21), 229.0),
        ])
    with engine.connect() as c:
        yield c


@pytest.mark.parametrize(("sid", "src", "d", "foreign"), [
    ("YF:CL=F:close", "TIINGO", date(2026, 5, 21), True),
    ("YF:CL=F:close", "tiingo", date(1990, 1, 2), True),
    ("YF:CL=F:close", "yfinance", date(2026, 5, 21), False),
    ("YF:HG=F:close", "TIINGO", date(2026, 8, 11), True),
    ("YF:BRL=X:close", "TIINGO", date(2007, 1, 2), True),
    ("YF:EURUSD=X:close", "TIINGO", date(2026, 5, 21), False),
    ("YF:K:close", "TIINGO", date(2025, 12, 11), False),
    ("YF:K:close", "TIINGO", date(2026, 6, 8), True),
    ("YF:AAPL:close", "TIINGO", date(2026, 5, 21), False),
    ("FRED:WALCL", "TIINGO", date(2026, 5, 21), False),
])
def test_is_foreign_tiingo_row(sid, src, d, foreign):
    assert tsc.is_foreign_tiingo_row(sid, src, d) is foreign


def test_cutoffs():
    assert tsc.tiingo_cutoff("YF:CL=F:close") == date.min
    assert tsc.tiingo_cutoff("YF:K:close") == date(2025, 12, 11)
    assert tsc.tiingo_cutoff("YF:AAPL:close") is None


def test_window_and_latest_n_drop_foreign_rows(conn):
    assert obs.read_window(conn, "YF:CL=F:close", source="TIINGO") == []
    assert [(o.obs_date, o.value) for o in obs.read_window(conn, "YF:CL=F:close", source="yfinance")] == [
        (date(2026, 5, 21), 85.0)]
    assert [o.obs_date for o in obs.read_window(conn, "YF:K:close", source="TIINGO")] == [date(2025, 12, 11)]
    assert [o.value for o in obs.read_latest_n(conn, "YF:K:close", 5, source="TIINGO")] == [83.5]
    assert [o.value for o in obs.read_window(conn, "YF:EURUSD=X:close", source="TIINGO")] == [1.08]


def test_latest_reads_back_to_the_last_trusted_date(conn):
    k = obs.read_latest(conn, "YF:K:close", source="TIINGO")
    assert (k.obs_date, k.value) == (date(2025, 12, 11), 83.5)
    assert obs.read_latest(conn, "YF:CL=F:close", source="TIINGO") is None
    assert obs.read_latest(conn, "YF:AAPL:close", source="TIINGO").value == 229.0


def test_vintages_keep_only_the_real_instrument(conn):
    got = obs.read_vintages(conn, "YF:CL=F:close", start=date(2026, 5, 1), end=date(2026, 5, 31))
    assert [(o.obs_date, o.value, o.source) for o in got] == [(date(2026, 5, 21), 85.0, "yfinance")]


def test_completed_closes_skip_post_delisting_rows(conn, monkeypatch):
    # SQLite drops the tz the reader insists on; restore it for this fixture.
    real = obs._coerce_ts
    monkeypatch.setattr(obs, "_coerce_ts", lambda v: (t := real(v)) and t.replace(tzinfo=t.tzinfo or timezone.utc))
    got = obs.read_completed_closes(conn, "YF:K:close", start=date(2025, 12, 1), end=date(2026, 6, 30),
                                    boundary=lambda d: datetime(2000, 1, 1, tzinfo=timezone.utc),
                                    source=("TIINGO",))
    assert sorted(got) == [date(2025, 12, 11)]


def test_resolver_and_puller_share_the_rules():
    from ingestion import tiingo_pull
    from normalization import resolver
    assert tiingo_pull.is_tiingo_equity is tsc.is_tiingo_equity
    assert "is_foreign_tiingo_row(row[0], row[6], row[1])" in inspect.getsource(resolver)
