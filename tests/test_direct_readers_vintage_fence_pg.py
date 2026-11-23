"""Direct ``resolved_series`` readers fence ``vintage_date <= as_of``, not only ``release_date``.

Knowable-at for a resolved row is the later of ``release_date`` and
``vintage_date``. The TwelveData bridge
(``scripts/backfill_resolved_series_from_ticker_metrics.sql``, E1-V7c) wrote
rows with ``release_date = obs_date`` and ``vintage_date`` = the fetch date,
up to ~60 days later. A reader that fences release only serves such a row to
every ``as_of`` in ``[obs_date, fetch date)``, a value GRID did not yet hold.
``store/pit.py`` fences both (GRID#1365); these are the readers that query
``resolved_series`` themselves (``tests/test_resolved_series_vintage_fence_guard.py``
keeps new ones from appearing).

Per reader: a row with ``release_date <= as_of < vintage_date`` is never
returned, and an honest earlier vintage of the same cell is. Runs in a
scratch schema on ``pg_engine`` (``GRID_TEST_DB_URL``); skips without
PostgreSQL.
"""

from __future__ import annotations

from datetime import date, timedelta
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

_DDL = (
    "CREATE TABLE feature_registry (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    """
    CREATE TABLE resolved_series (
        id                    BIGSERIAL PRIMARY KEY,
        feature_id            INTEGER NOT NULL REFERENCES feature_registry(id),
        obs_date              DATE NOT NULL,
        release_date          DATE NOT NULL,
        vintage_date          DATE NOT NULL,
        value                 DOUBLE PRECISION NOT NULL,
        source_priority_used  INTEGER NOT NULL DEFAULT 1
    )
    """,
    "CREATE UNIQUE INDEX uq_resolved_series_composite ON resolved_series (feature_id, obs_date, vintage_date)",
    """
    CREATE TABLE resolved_series_retractions (
        id            BIGSERIAL PRIMARY KEY,
        feature_id    INTEGER NOT NULL,
        obs_date      DATE NOT NULL,
        vintage_date  DATE NOT NULL,
        retracted_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        reason        TEXT NOT NULL,
        run_tag       TEXT NOT NULL,
        UNIQUE (feature_id, obs_date, vintage_date)
    )
    """,
)

# Cell A (Wed 2026-04-01): only a backdated-release row, fetched 2026-05-17
#   (the bridge shape: release = obs, vintage = fetch date).
# Cell B (Mon 2026-03-30): an honest vintage on 03-31, then a backdated
#   re-fetch on 05-17 with another value.
# Cell C (Fri 2026-03-27): two honest vintages (release = vintage).
A, B, C = date(2026, 4, 1), date(2026, 3, 30), date(2026, 3, 27)
FETCH = date(2026, 5, 17)
CELLS = (
    (A, A, FETCH, 200.0),
    (B, date(2026, 3, 31), date(2026, 3, 31), 100.0),
    (B, B, FETCH, 201.0),
    (C, date(2026, 3, 28), date(2026, 3, 28), 50.0),
    (C, date(2026, 4, 2), date(2026, 4, 2), 51.0),
)
AS_OF = date(2026, 4, 15)  # after every release_date, before the 05-17 fetch
LEAKED = {200.0, 201.0}


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"direct_vfence_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema} -ctimezone=UTC"})
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _seed(engine: Engine, name: str, rows=CELLS) -> int:
    with engine.begin() as conn:
        fid = conn.execute(
            text("INSERT INTO feature_registry (name) VALUES (:n) RETURNING id"), {"n": name}
        ).scalar_one()
        conn.execute(
            text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                 "VALUES (:f, :od, :rd, :vd, :v)"),
            [{"f": fid, "od": od, "rd": rd, "vd": vd, "v": v} for od, rd, vd, v in rows],
        )
    return fid


def _by_day(series) -> dict[date, float]:
    return {ts.date(): float(v) for ts, v in series.items()}


def test_conviction_scorer_latest_and_price(scratch):
    from alpha_research import conviction_scorer as cs

    _seed(scratch, "aapl_full")
    with scratch.connect() as conn:
        assert cs._load_latest(conn, "aapl_full", as_of_date=AS_OF) == 100.0
        price = cs._load_price(conn, "AAPL", as_of_date=AS_OF)
        # From the fetch date on, the bridged rows are what GRID held.
        assert cs._load_latest(conn, "aapl_full", as_of_date=FETCH) == 200.0
    assert list(price.index.date) == [C, B]
    assert not LEAKED & set(price.tolist())


def test_conviction_scorer_universe_excludes_rows_not_yet_held(scratch, monkeypatch):
    from alpha_research import conviction_scorer as cs

    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(60)]
    _seed(scratch, "bbb_full", [(d, d, FETCH, 10.0) for d in days])   # bridged only
    _seed(scratch, "ccc_full", [(d, d, d, 10.0) for d in days])       # honest
    seen: list[str] = []

    def fake_score(_engine, ticker, *, as_of_date=None, ftd_series_ids=None):
        seen.append(ticker)
        return cs.ConvictionReport(ticker, 90, 90.0, (), "CONVICTION", "ts")

    monkeypatch.setattr(cs, "score_ticker", fake_score)
    # The scratch schema has no raw_series; the FTD lookup is covered in test_conviction_scorer.
    monkeypatch.setattr(cs, "_ftd_series_ids", lambda _conn, tickers, _as_of: {})
    reports = cs.scan_all(scratch, min_score=0, as_of_date=AS_OF)
    assert [r.ticker for r in reports] == ["CCC"]
    assert seen == ["CCC"]


def test_prediction_context_latest_feature_value(scratch):
    from oracle import prediction_context

    _seed(scratch, "aapl_full")
    assert prediction_context._latest_feature_value(scratch, ["aapl_full"], AS_OF, lookback_days=30) == 100.0
    assert prediction_context._latest_feature_value(scratch, ["aapl_full"], FETCH, lookback_days=60) == 200.0


def test_credit_cycle_feature_series(scratch):
    from alpha_research.signals import credit_cycle

    fid = _seed(scratch, "hy_oas_spread")
    series = credit_cycle._get_feature_series(scratch, fid, C, AS_OF)
    # One value per obs_date, the latest vintage held on AS_OF.
    assert _by_day(series) == {C: 51.0, B: 100.0}


def test_volume_panel(scratch):
    from alpha_research.data import panel_builder

    _seed(scratch, "aapl_avg_volume")
    _seed(scratch, "msft_avg_volume", [(C, C, C, 7.0)])
    panel = panel_builder.build_volume_panel(scratch, ["AAPL"], start_date=C, end_date=AS_OF, as_of_date=AS_OF)
    assert list(panel.columns) == ["AAPL"]
    assert _by_day(panel["AAPL"].dropna()) == {C: 51.0, B: 100.0}
    every = panel_builder.build_volume_panel(scratch, None, start_date=C, end_date=AS_OF, as_of_date=AS_OF)
    assert sorted(every.columns) == ["AAPL", "MSFT"]


def test_astrogrid_price_reads_with_a_release_cutoff(scratch):
    from store.astrogrid import _PRICE_FEATURE_BY_SYMBOL, AstroGridStore

    feature = _PRICE_FEATURE_BY_SYMBOL["AAPL"]
    _seed(scratch, feature)
    store = AstroGridStore(scratch)

    path = store._load_price_path("AAPL", C, A, release_cutoff=AS_OF)
    assert path == [(C, 51.0), (B, 100.0)]

    # Cell A was not held on AS_OF: the newest held close is Monday's, too old
    # for a Wednesday target, so the read says so instead of scoring 200.
    at_a = store._lookup_symbol_price("AAPL", A, release_cutoff=AS_OF)
    assert at_a["price"] is None and at_a["status"] == "stale_canonical_price"
    assert at_a["latest_obs_date"] == B.isoformat()
    at_b = store._lookup_symbol_price("AAPL", B, release_cutoff=AS_OF)
    assert (at_b["status"], at_b["price"]) == ("ok", 100.0)

    # No cutoff is a latest read: the bridged row is what GRID holds now.
    latest = store._lookup_symbol_price("AAPL", A)
    assert (latest["status"], latest["price"]) == ("ok", 200.0)
