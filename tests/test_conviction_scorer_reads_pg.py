"""conviction_scorer's faster reads return what the old SQL returned, on PostgreSQL.

2026-11-23 the scorer's resolved_series reads were reshaped for speed
(scan_all took ~600 s for 139 tickers on griddb):

* ``_load_latest`` / ``_load_price`` bind the feature id instead of matching
  the name through a feature_registry join;
* ``_load_price`` returns one row per obs_date with DISTINCT ON instead of
  every vintage for Python to drop;
* the scan universe stops at each feature's 51st known row (EXISTS ... OFFSET
  50) instead of counting every row.

Each test runs the legacy statement verbatim next to the new reader over
cells built to separate the orderings: vintages whose release order and
vintage order disagree, retracted latest vintages, rows not yet known at the
as-of date, and features with exactly 50 and 51 known rows. Runs in a
scratch schema on ``pg_engine`` (``GRID_TEST_DB_URL``); skips without
PostgreSQL.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from alpha_research import conviction_scorer as cs
from alpha_research.data.split_adjuster import adjust_splits
from store.pit import retraction_cutoff

_DDL = (
    "CREATE TABLE feature_registry (id SERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE)",
    """
    CREATE TABLE resolved_series (
        id            BIGSERIAL PRIMARY KEY,
        feature_id    INTEGER NOT NULL REFERENCES feature_registry(id),
        obs_date      DATE NOT NULL,
        release_date  DATE NOT NULL,
        vintage_date  DATE NOT NULL,
        value         DOUBLE PRECISION NOT NULL
    )
    """,
    "CREATE UNIQUE INDEX uq_rs ON resolved_series (feature_id, obs_date, vintage_date)",
    """
    CREATE TABLE resolved_series_retractions (
        id            BIGSERIAL PRIMARY KEY,
        feature_id    INTEGER NOT NULL,
        obs_date      DATE NOT NULL,
        vintage_date  DATE NOT NULL,
        retracted_at  TIMESTAMPTZ NOT NULL,
        UNIQUE (feature_id, obs_date, vintage_date)
    )
    """,
)

_RETRACT = """
      AND NOT EXISTS (
          SELECT 1 FROM resolved_series_retractions rr
          WHERE rr.feature_id = rs.feature_id
            AND rr.obs_date = rs.obs_date
            AND rr.vintage_date = rs.vintage_date
            AND rr.retracted_at <= :retraction_cutoff
      )"""
# The statements as they were before 2026-11-23, verbatim.
LEGACY_LATEST = f"""
    SELECT rs.value FROM resolved_series rs
    JOIN feature_registry fr ON rs.feature_id = fr.id
    WHERE fr.name = :n
      AND rs.obs_date <= :as_of
      AND rs.release_date <= :as_of
      AND rs.vintage_date <= :as_of {_RETRACT}
    ORDER BY rs.obs_date DESC, rs.release_date DESC, rs.vintage_date DESC
    LIMIT 1
"""
LEGACY_PRICE = f"""
    SELECT rs.obs_date, rs.value FROM resolved_series rs
    JOIN feature_registry fr ON rs.feature_id = fr.id
    WHERE fr.name = :n
      AND rs.obs_date <= :as_of
      AND rs.release_date <= :as_of
      AND rs.vintage_date <= :as_of {_RETRACT}
    ORDER BY rs.obs_date, rs.release_date, rs.vintage_date
"""
LEGACY_UNIVERSE = """
    SELECT UPPER(REPLACE(fr.name, '_full', ''))
    FROM feature_registry fr JOIN resolved_series rs ON fr.id = rs.feature_id
    WHERE fr.name LIKE '%%_full'
      AND rs.obs_date <= :as_of
      AND rs.release_date <= :as_of
      AND rs.vintage_date <= :as_of
    GROUP BY fr.name HAVING COUNT(*) > 50
"""

D0 = date(2026, 3, 2)
AS_OF = date(2026, 4, 15)
LATER = date(2026, 4, 20)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"conviction_reads_{uuid4().hex[:12]}"
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


def _seed(engine: Engine, name: str, rows) -> int:
    with engine.begin() as conn:
        fid = conn.execute(
            text("INSERT INTO feature_registry (name) VALUES (:n) RETURNING id"), {"n": name}
        ).scalar_one()
        if rows:
            conn.execute(
                text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                     "VALUES (:f, :od, :rd, :vd, :v)"),
                [{"f": fid, "od": od, "rd": rd, "vd": vd, "v": v} for od, rd, vd, v in rows],
            )
    return fid


def _retract(engine: Engine, fid: int, obs: date, vintage: date, at: date) -> None:
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO resolved_series_retractions (feature_id, obs_date, vintage_date, retracted_at) "
                 "VALUES (:f, :o, :v, :t)"),
            {"f": fid, "o": obs, "v": vintage, "t": datetime.combine(at, datetime.min.time(), tzinfo=timezone.utc)},
        )


def _cells() -> list[tuple[date, date, date, float]]:
    """40 days, each with up to three vintages whose two orderings disagree."""
    rows = []
    for i in range(40):
        d = D0 + timedelta(days=i)
        base = 100.0 + i
        rows.append((d, d, d, base))                                   # first print
        if i % 3 == 0:  # later vintage, earlier release: release order wins
            rows.append((d, d + timedelta(days=5), d + timedelta(days=1), base + 0.5))
            rows.append((d, d + timedelta(days=2), d + timedelta(days=4), base + 0.25))
        if i % 4 == 1:  # a revision not yet known at AS_OF
            rows.append((d, d + timedelta(days=1), LATER, base + 9.0))
        if i % 5 == 2:  # release known, vintage after AS_OF (the bridged shape)
            rows.append((d, d, LATER - timedelta(days=1), base + 7.0))
    return rows


def _legacy_price(conn, name: str, as_of: date) -> pd.Series:
    rows = conn.execute(text(LEGACY_PRICE), {
        "n": name, "as_of": as_of, "retraction_cutoff": retraction_cutoff(as_of),
    }).fetchall()
    if not rows:
        return pd.Series(dtype=float)
    s = pd.Series([r[1] for r in rows], index=pd.to_datetime([r[0] for r in rows]))
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return adjust_splits(s)


def test_load_price_and_latest_match_the_legacy_statements(scratch):
    fid = _seed(scratch, "abc_full", _cells())
    # Retract the winning vintage of two dates, one before and one after AS_OF.
    _retract(scratch, fid, D0, D0 + timedelta(days=1), at=date(2026, 4, 1))
    _retract(scratch, fid, D0 + timedelta(days=39), D0 + timedelta(days=40), at=date(2026, 4, 18))
    _seed(scratch, "other_full", [(D0, D0, D0, 1.0)])  # another feature must not mix in

    with scratch.connect() as conn:
        for as_of in (D0 + timedelta(days=10), AS_OF, date(2026, 4, 18), LATER):
            new = cs._load_price(conn, "ABC", as_of_date=as_of)
            old = _legacy_price(conn, "abc_full", as_of)
            pd.testing.assert_series_equal(new, old)
            assert not new.empty and new.index.is_unique
            legacy_latest = conn.execute(text(LEGACY_LATEST), {
                "n": "abc_full", "as_of": as_of, "retraction_cutoff": retraction_cutoff(as_of),
            }).scalar()
            assert cs._load_latest(conn, "abc_full", as_of_date=as_of) == legacy_latest

        # The release-order winner: D0+3 has vintages (rd +5, vd +1) and (rd +2, vd +4).
        price = cs._load_price(conn, "ABC", as_of_date=AS_OF)
        assert price[pd.Timestamp(D0 + timedelta(days=3))] == 103.5
        # D0's release-order winner is retracted from 04-01 on: falls back to the next one.
        assert price[pd.Timestamp(D0)] == 100.25
        assert cs._load_price(conn, "ZZZ", as_of_date=AS_OF).empty
        assert cs._load_latest(conn, "zzz_full", as_of_date=AS_OF) is None


def test_scan_universe_matches_the_legacy_count_and_name_order(scratch, monkeypatch):
    days = [date(2026, 1, 1) + timedelta(days=i) for i in range(60)]  # all before AS_OF
    _seed(scratch, "fifty_full", [(d, d, d, 1.0) for d in days[:50]])       # 50 known rows: out
    _seed(scratch, "fiftyone_full", [(d, d, d, 1.0) for d in days[:51]])    # 51: in
    _seed(scratch, "bbb_full", [(d, d, d, 1.0) for d in days[:30]]
          + [(d, d, d + timedelta(days=1), 2.0) for d in days[:30]])         # 60 rows over 30 dates: in
    _seed(scratch, "late_full", [(d, d, LATER, 1.0) for d in days])          # none known at AS_OF: out
    _seed(scratch, "aaa_full", [(d, d, d, 1.0) for d in days])               # in
    _seed(scratch, "vix_spot", [(d, d, d, 30.0) for d in days])              # not a price panel
    seen: list[tuple[str, cs.MarketSetup | None]] = []

    def fake_score(_engine, ticker, *, as_of_date=None, **kwargs):
        seen.append((ticker, kwargs.get("market")))
        return cs.ConvictionReport(ticker, 50, 50.0, (), "PILOT", "ts")

    monkeypatch.setattr(cs, "score_ticker", fake_score)
    # GRID#1395 adds a raw_series FTD lookup to scan_all; this schema has no
    # raw_series (raising=False: a no-op where the lookup does not exist).
    monkeypatch.setattr(cs, "_ftd_series_ids", lambda _conn, _tickers, _as_of: {}, raising=False)
    reports = cs.scan_all(scratch, min_score=0, as_of_date=AS_OF)

    with scratch.connect() as conn:
        legacy = [r[0] for r in conn.execute(text(LEGACY_UNIVERSE), {"as_of": AS_OF})]
    assert sorted(legacy) == ["AAA", "BBB", "FIFTYONE"]
    assert [t for t, _ in seen] == ["AAA", "BBB", "FIFTYONE"]
    assert [r.ticker for r in reports] == ["AAA", "BBB", "FIFTYONE"]  # ties keep name order
    markets = {id(m) for _, m in seen}
    assert len(markets) == 1
    assert seen[0][1] == cs.MarketSetup(as_of=AS_OF, vix=30.0, hy=None, yc=None)
