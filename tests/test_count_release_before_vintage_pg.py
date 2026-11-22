"""scripts/count_release_before_vintage.py: exact counts on PostgreSQL, read-only, no writes, DB window.

The PostgreSQL tests use only ``GRID_TEST_DB_URL`` (a disposable ``*_test``
database; production names are refused before connecting) and a throwaway
schema; unset or unreachable, they skip.
"""

from __future__ import annotations

import os
from datetime import date, datetime, timezone
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import make_url

from scripts import count_release_before_vintage as count

_PROD_NAMES = {"grid", "griddb", "grid_obsidian", "grid_v4", "gridprod", "postgres"}

_DDL = (
    "CREATE TABLE source_catalog (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    "CREATE TABLE feature_registry (id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    """CREATE TABLE resolved_series (
           id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL REFERENCES feature_registry(id),
           obs_date DATE NOT NULL, release_date DATE NOT NULL, vintage_date DATE NOT NULL,
           value DOUBLE PRECISION NOT NULL,
           source_priority_used INTEGER NOT NULL REFERENCES source_catalog(id))""",
    "CREATE UNIQUE INDEX uq_resolved_series_composite ON resolved_series (feature_id, obs_date, vintage_date)",
    """CREATE TABLE resolved_series_retractions (
           id BIGSERIAL PRIMARY KEY, feature_id INTEGER NOT NULL, obs_date DATE NOT NULL,
           vintage_date DATE NOT NULL, retracted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
           reason TEXT NOT NULL, run_tag TEXT NOT NULL,
           UNIQUE (feature_id, obs_date, vintage_date))""",
)

TD, YF = 60, 2
AAPL, MSFT, CLEAN = 1, 2, 3
FETCH = date(2026, 5, 17)
A, B, C, D = date(2026, 4, 1), date(2026, 3, 30), date(2026, 3, 27), date(2026, 3, 25)
_OK_CLOCK = lambda: datetime(2026, 11, 21, 12, 0, tzinfo=timezone.utc)  # noqa: E731


@pytest.fixture()
def pg_engine():
    url = os.environ.get("GRID_TEST_DB_URL", "").strip()
    if not url:
        pytest.skip("GRID_TEST_DB_URL unset")
    name = (make_url(url).database or "").lower()
    if not name.endswith("_test") and (name in _PROD_NAMES or name.startswith("grid") or not name):
        pytest.fail(f"refusing scratch schemas in {name!r}: not a *_test database")
    try:
        admin = create_engine(url, pool_pre_ping=True)
        with admin.connect() as conn:
            conn.execute(text("SELECT 1"))
    except Exception:  # pragma: no cover - environment dependent
        pytest.skip("PostgreSQL not available")
    schema = f"rbv_count_{uuid4().hex[:12]}"
    with admin.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(admin.url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
        yield engine
    finally:
        engine.dispose()
        with admin.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))
        admin.dispose()


def _seed(engine) -> None:
    rows = [
        # aapl: A early only; B honest + early; C two honest; D early and already retracted.
        (AAPL, A, A, FETCH, TD),
        (AAPL, B, date(2026, 3, 31), date(2026, 3, 31), YF),
        (AAPL, B, B, FETCH, TD),
        (AAPL, C, date(2026, 3, 28), date(2026, 3, 28), YF),
        (AAPL, C, date(2026, 4, 2), date(2026, 4, 2), YF),
        (AAPL, D, D, FETCH, TD),
        # msft: one early yfinance row (3-day gap), no honest sibling.
        (MSFT, A, A, date(2026, 4, 4), YF),
        # clean: honest only.
        (CLEAN, A, date(2026, 4, 2), date(2026, 4, 2), YF),
    ]
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO source_catalog VALUES (60, 'TWELVEDATA'), (2, 'yfinance')"))
        conn.execute(text("INSERT INTO feature_registry VALUES (1, 'aapl_full'), (2, 'msft_full'), (3, 'clean_full')"))
        conn.execute(
            text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value, "
                 "source_priority_used) VALUES (:f, :od, :rd, :vd, 1.0, :s)"),
            [{"f": f, "od": od, "rd": rd, "vd": vd, "s": s} for f, od, rd, vd, s in rows],
        )
        conn.execute(text("INSERT INTO resolved_series_retractions (feature_id, obs_date, vintage_date, reason, "
                          "run_tag) VALUES (1, :od, :vd, 'test', 'test')"), {"od": D, "vd": FETCH})


def _table_counts(engine) -> tuple[int, int]:
    with engine.connect() as conn:
        return (conn.execute(text("SELECT count(*) FROM resolved_series")).scalar_one(),
                conn.execute(text("SELECT count(*) FROM resolved_series_retractions")).scalar_one())


def test_counts_are_exact_and_nothing_is_written(pg_engine):
    _seed(pg_engine)
    before = _table_counts(pg_engine)
    report = count.run(pg_engine, ["aapl_full", "msft_full", "clean_full", "missing_feature"], clock=_OK_CLOCK)
    assert _table_counts(pg_engine) == before

    assert report["read_only"] is True
    assert report["features_scanned"] == 4 and report["features_with_rows"] == 2
    assert report["rows_release_before_vintage"] == 4
    by_name = {f["feature"]: f for f in report["features"]}
    assert set(by_name) == {"aapl_full", "msft_full", "missing_feature"}  # clean_full has none
    aapl = by_name["aapl_full"]
    assert (aapl["rows"], aapl["already_retracted"], aapl["cells_left_with_no_value"]) == (3, 1, 1)
    assert aapl["gap_days_max"] == (FETCH - D).days
    assert (aapl["obs_min"], aapl["obs_max"]) == (D.isoformat(), A.isoformat())
    assert [s["source_name"] for s in aapl["by_source"]] == ["TWELVEDATA"]
    assert aapl["by_source"][0]["gap_days_mean"] == round(((FETCH - A).days + (FETCH - B).days + (FETCH - D).days) / 3, 2)
    msft = by_name["msft_full"]
    assert (msft["rows"], msft["gap_days_max"], msft["cells_left_with_no_value"]) == (1, 3, 1)
    assert by_name["missing_feature"]["error"] == "feature not in feature_registry"
    assert report["by_source"] == {"TWELVEDATA": {"rows": 3, "features": 1, "gap_days_max": 53},
                                   "yfinance": {"rows": 1, "features": 1, "gap_days_max": 3}}
    preview = report["retraction_preview"]
    assert preview["written"] is False
    assert (preview["rows"], preview["features"], preview["cells_left_with_no_value"]) == (3, 2, 2)
    assert report["complete"] is True


def test_source_filter_counts_only_that_source(pg_engine):
    _seed(pg_engine)
    report = count.run(pg_engine, ["aapl_full", "msft_full"], source="TWELVEDATA", clock=_OK_CLOCK)
    assert report["rows_release_before_vintage"] == 3
    assert [f["feature"] for f in report["features"]] == ["aapl_full"]


def test_missing_retractions_table_reads_as_zero(pg_engine):
    _seed(pg_engine)
    with pg_engine.begin() as conn:
        conn.execute(text("DROP TABLE resolved_series_retractions"))
    aapl = count.run(pg_engine, ["aapl_full"], clock=_OK_CLOCK)["features"][0]
    # D is no longer retracted, and it has no honest sibling either.
    assert (aapl["error"], aapl["rows"], aapl["already_retracted"], aapl["cells_left_with_no_value"]) == (None, 3, 0, 2)


def test_all_features_listing_is_read_only_and_uncapped_by_default(pg_engine):
    _seed(pg_engine)
    assert count.list_features(pg_engine, None) == ["aapl_full", "clean_full", "msft_full"]
    assert count.list_features(pg_engine, 1) == ["aapl_full"]
    with pg_engine.connect() as conn, conn.begin():
        count._readonly(conn, 30_000)
        assert conn.execute(text("SHOW transaction_read_only")).scalar() == "on"
        with pytest.raises(Exception, match="read-only"):
            conn.execute(text("INSERT INTO source_catalog VALUES (1, 'x')"))


def test_a_statement_timeout_is_reported_per_feature(pg_engine, monkeypatch):
    _seed(pg_engine)
    monkeypatch.setattr(count, "_COUNTS_WITH_RETRACTIONS_SQL", text("SELECT pg_sleep(2), :fid, :source"))
    report = count.run(pg_engine, ["aapl_full", "missing_feature"], timeout_ms=200, clock=_OK_CLOCK)
    slow, nxt = report["features"]
    assert slow["error"] is not None and "statement timeout" in slow["error"].lower(), slow["error"]
    assert nxt["error"] == "feature not in feature_registry"


# ── no database needed ──────────────────────────────────────────────────


def test_refuses_inside_the_db_window():
    class _NoEngine:
        def connect(self):
            raise AssertionError("connected inside the DB window")

    with pytest.raises(count.DbWindowRefused):
        count.run(_NoEngine(), ["aapl_full"], clock=lambda: datetime(2026, 11, 21, 5, 0, tzinfo=timezone.utc))


def test_cli_refuses_inside_the_window(monkeypatch, capsys):
    class _Frozen(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 21, 6, 0, tzinfo=timezone.utc)

    monkeypatch.setattr(count, "datetime", _Frozen)
    assert count.main(["--all-features", "--db-url", "postgresql://x@nowhere/none_test"]) == 2
    assert "refusing" in capsys.readouterr().err


def test_script_holds_no_write_statement():
    source = (count.__file__ and open(count.__file__, encoding="utf-8").read()).upper()
    for verb in ("INSERT ", "UPDATE ", "DELETE ", "TRUNCATE", "ALTER ", "DROP ", "CREATE "):
        assert verb not in source, verb
