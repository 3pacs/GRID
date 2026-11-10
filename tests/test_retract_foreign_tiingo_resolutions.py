"""scripts/retract_foreign_tiingo_resolutions.py: dry run by default, retracts
only TIINGO cells of features fed by a symbol Tiingo cannot serve (or dated
after a delisted ticker's last session), and never updates or deletes.

SQLite stands in for Postgres; the SQL is plain enough for both.
"""

from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from sqlalchemy import create_engine, text

import tiingo_symbol_contract as tsc
from scripts import retract_foreign_tiingo_resolutions as rt

MAP = {"YF:CL=F:close": "cl_close", "YF:AAPL:close": "aapl_full", "YF:K:close": "k_full",
       "FRED:WALCL": "fed_bs"}


@pytest.fixture(autouse=True)
def _cutoffs(monkeypatch):
    monkeypatch.setattr(tsc, "_last_sessions", lambda: {"K": date(2025, 12, 11)})


@pytest.fixture()
def engine():
    e = create_engine("sqlite://")
    with e.begin() as c:
        c.execute(text("CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT)"))
        c.execute(text("CREATE TABLE feature_registry (id INTEGER PRIMARY KEY, name TEXT)"))
        c.execute(text("""CREATE TABLE resolved_series (id INTEGER PRIMARY KEY, feature_id INT, obs_date DATE,
                          vintage_date DATE, value REAL, source_priority_used INT)"""))
        c.execute(text("""CREATE TABLE resolved_series_retractions (id INTEGER PRIMARY KEY, feature_id INT,
                          obs_date DATE, vintage_date DATE, reason TEXT, run_tag TEXT,
                          UNIQUE (feature_id, obs_date, vintage_date))"""))
        c.execute(text("INSERT INTO source_catalog VALUES (2, 'yfinance'), (524, 'TIINGO')"))
        c.execute(text("INSERT INTO feature_registry VALUES (1, 'cl_close'), (2, 'aapl_full'), (3, 'k_full')"))
        c.execute(text("""INSERT INTO resolved_series (feature_id, obs_date, vintage_date, value, source_priority_used) VALUES
            (1, '1990-01-02', '2026-09-01', 31.2, 524),   -- Colgate in crude
            (1, '2026-05-22', '2026-05-23', 96.6, 524),   -- Colgate in crude
            (1, '2026-05-21', '2026-05-22', 85.0, 2),     -- real crude (yfinance)
            (2, '2026-05-21', '2026-05-22', 229.0, 524),  -- AAPL on Tiingo: fine
            (3, '2025-12-11', '2025-12-12', 83.5, 524),   -- Kellanova's last session: fine
            (3, '2026-06-08', '2026-06-09', 39.75, 524)   -- after it: another listing"""))
    return e


def test_scope_is_features_fed_by_a_symbol_with_a_cutoff():
    assert rt.scope(MAP) == {"cl_close": ("YF:CL=F:close", date.min), "k_full": ("YF:K:close", date(2025, 12, 11))}


def test_dry_run_plans_and_writes_nothing(engine):
    report = rt.run(engine, apply=False, run_tag=None, mappings=MAP)
    by = {f["feature"]: f for f in report["features"]}
    assert (by["cl_close"]["status"], by["cl_close"]["cells"]) == ("PLANNED", 2)
    assert (by["k_full"]["status"], by["k_full"]["cells"]) == ("PLANNED", 1)
    with engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM resolved_series_retractions")).scalar() == 0


def test_apply_retracts_only_foreign_cells_and_is_idempotent(engine):
    report = rt.run(engine, apply=True, run_tag="t1", mappings=MAP)
    assert report["retracted"] == 3
    with engine.connect() as c:
        got = c.execute(text("SELECT feature_id, obs_date, run_tag FROM resolved_series_retractions ORDER BY 1, 2")).fetchall()
        assert [(f, str(d)) for f, d, _ in got] == [(1, "1990-01-02"), (1, "2026-05-22"), (3, "2026-06-08")]
        assert c.execute(text("SELECT count(*) FROM resolved_series")).scalar() == 6  # nothing deleted
    again = rt.run(engine, apply=True, run_tag="t2", mappings=MAP)
    assert again["cells"] == 0 and again["retracted"] == 0


def test_main_refuses_apply_without_a_tag_or_inside_the_db_window(engine):
    assert rt.main(["--apply"], engine=engine) == 2
    inside = datetime(2026, 11, 10, 4, 0, tzinfo=timezone.utc)
    assert rt.main(["--apply", "--run-tag", "t"], engine=engine, now=inside) == 1
    with engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM resolved_series_retractions")).scalar() == 0
