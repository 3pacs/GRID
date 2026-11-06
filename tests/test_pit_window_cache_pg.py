"""PITWindowCache and snapshot-backed rotation backtests vs PostgreSQL (W2-7).

The in-memory window and its snapshot file must give exactly what
``PITStore.get_pit``'s SQL gives on the same database, and a rotation
backtest must produce identical metrics on the three data paths:

* ``db``          -- ``GRID_ROTATION_PIT_CACHE=0``: every rebalance reads SQL
* ``pit_window``  -- the default: one bounded window read, answered in memory
* ``snapshot``    -- the window written to a hash-receipted file and read back

Runs in a scratch schema on the shared ``pg_engine`` (``GRID_TEST_DB_URL``);
skips when no PostgreSQL is reachable. The retractions table here is created
without the production append-only trigger so a fixture can hold
retractions dated in the past.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from uuid import uuid4

import numpy as np
import pandas as pd
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

from store.pit import PITStore, PITWindowCache

_DDL = (
    """
    CREATE TABLE feature_registry (
        id    SERIAL PRIMARY KEY,
        name  TEXT NOT NULL UNIQUE
    )
    """,
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
        UNIQUE (feature_id, obs_date, vintage_date),
        FOREIGN KEY (feature_id, obs_date, vintage_date)
            REFERENCES resolved_series (feature_id, obs_date, vintage_date)
    )
    """,
)


@pytest.fixture()
def scratch(pg_engine: Engine):
    schema = f"pit_window_{uuid4().hex[:12]}"
    with pg_engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_engine(pg_engine.url, connect_args={"options": f"-csearch_path={schema}"})
    try:
        with engine.begin() as conn:
            for ddl in _DDL:
                conn.execute(text(ddl))
        yield engine
    finally:
        engine.dispose()
        with pg_engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE'))


def _features(engine: Engine, names: list[str]) -> dict[str, int]:
    with engine.begin() as conn:
        return {
            n: conn.execute(
                text("INSERT INTO feature_registry (name) VALUES (:n) RETURNING id"), {"n": n}
            ).scalar_one()
            for n in names
        }


def _insert(engine: Engine, rows: list[dict]) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                "VALUES (:f, :o, :r, :v, :val)"
            ),
            rows,
        )


def _retract(engine: Engine, fid: int, obs: date, vintage: date, at: datetime) -> None:
    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO resolved_series_retractions "
                "(feature_id, obs_date, vintage_date, retracted_at, reason, run_tag) "
                "VALUES (:f, :o, :v, :at, 'test', 'test')"
            ),
            {"f": fid, "o": obs, "v": vintage, "at": at},
        )


def _records(df: pd.DataFrame) -> list[dict]:
    return df.reset_index(drop=True).to_dict("records")


# ── get_pit: window and snapshot equal SQL ───────────────────────────────

D0 = date(2025, 1, 1)


def test_window_and_snapshot_reads_equal_sql(scratch, tmp_path):
    ids = _features(scratch, ["a_full", "b_full"])
    rows = []
    for fid in ids.values():
        for k in range(30):
            obs = D0 + timedelta(days=k)
            rows.append({"f": fid, "o": obs, "r": obs + timedelta(days=1), "v": obs + timedelta(days=1), "val": k * 1.0})
            rows.append({"f": fid, "o": obs, "r": obs + timedelta(days=4), "v": obs + timedelta(days=4), "val": k + 0.25})
            if k % 5 == 0:
                rows.append({"f": fid, "o": obs, "r": obs + timedelta(days=15), "v": obs + timedelta(days=15), "val": k + 0.5})
    _insert(scratch, rows)
    a = ids["a_full"]
    _retract(scratch, a, D0 + timedelta(days=5), D0 + timedelta(days=9),
             datetime(2025, 1, 12, 15, 0, tzinfo=timezone.utc))
    _retract(scratch, a, D0 + timedelta(days=10), D0 + timedelta(days=11),
             datetime(2025, 3, 1, tzinfo=timezone.utc))  # after max_as_of: dropped at load

    max_as_of = D0 + timedelta(days=50)
    cache = PITWindowCache.load(
        scratch, list(ids.values()), D0, D0 + timedelta(days=50), max_as_of,
        feature_names=dict(ids), allow_db_fallback=False,
    )
    snap = PITWindowCache.read_snapshot(cache.write_snapshot(tmp_path))
    db = PITStore(scratch)

    for offset in range(51):
        as_of = D0 + timedelta(days=offset)
        for policy in ("LATEST_AS_OF", "FIRST_RELEASE"):
            for bounds in ({"start_date": D0}, {"start_date": D0 + timedelta(days=4), "end_date": D0 + timedelta(days=12)}):
                want = _records(db.get_pit(list(ids.values()), as_of, policy, **bounds))
                assert _records(cache.get_pit(list(ids.values()), as_of, policy, **bounds)) == want
                assert _records(snap.get_pit(list(ids.values()), as_of, policy, **bounds)) == want
    assert cache.db_fallbacks == 0


def test_bounded_sql_read_is_the_unbounded_read_restricted(scratch):
    ids = _features(scratch, ["a_full"])
    fid = ids["a_full"]
    _insert(scratch, [
        {"f": fid, "o": D0 + timedelta(days=k), "r": D0 + timedelta(days=k + d),
         "v": D0 + timedelta(days=k + d), "val": k + d / 10}
        for k in range(20) for d in (1, 3)
    ])
    db = PITStore(scratch)
    as_of = D0 + timedelta(days=12)
    full = db.get_pit([fid], as_of)
    lo, hi = D0 + timedelta(days=3), D0 + timedelta(days=8)
    bounded = db.get_pit([fid], as_of, start_date=lo, end_date=hi)
    want = full[(full.obs_date >= lo) & (full.obs_date <= hi)]
    assert _records(bounded) == _records(want)


# ── rotation backtest: db == pit_window == snapshot ──────────────────────


def _seed_rotation_universe(engine: Engine, today: date) -> None:
    from alpha_research.data.panel_builder import VIX_FEATURE_NAME
    from alpha_research.strategies import rotation_variant_backtest as rvb

    names = rvb.rotation_feature_names()
    ids = _features(engine, names)
    rng = np.random.default_rng(7)
    days = pd.bdate_range("2024-01-02", "2025-07-31").date
    rows = []
    for name in names:
        fid = ids[name]
        if name == VIX_FEATURE_NAME:
            level = 18 + np.cumsum(rng.normal(0, 0.6, len(days)))
            for d, v in zip(days, np.clip(level, 10, 60)):
                # VIX is known the same day
                rows.append({"f": fid, "o": d, "r": d, "v": d, "val": float(v)})
            continue
        drift = rng.normal(0.0004, 0.0003)
        prices = 100 * np.exp(np.cumsum(rng.normal(drift, 0.01, len(days))))
        for i, (d, p) in enumerate(zip(days, prices)):
            rows.append({"f": fid, "o": d, "r": d + timedelta(days=1), "v": d + timedelta(days=1), "val": float(p)})
            if i % 9 == 0:  # a later revision for some cells
                rows.append({"f": fid, "o": d, "r": d + timedelta(days=6), "v": d + timedelta(days=6),
                             "val": float(p) * 1.001})
    _insert(engine, rows)
    spy = ids["spy_full"]
    # a revision retracted today hides it from the price read (as_of today)
    d = days[90]
    _retract(engine, spy, d, d + timedelta(days=6),
             datetime.combine(today, datetime.min.time(), timezone.utc) + timedelta(seconds=1))


def test_rotation_backtest_identical_on_db_window_and_snapshot(scratch, tmp_path, monkeypatch):
    from alpha_research.strategies import rotation_variant_backtest as rvb

    today = date.today()  # noqa: DTZ011 - build_price_panel's default clock
    _seed_rotation_universe(scratch, today)
    ws, we = date(2025, 2, 3), date(2025, 6, 30)
    cfg = rvb.RotationConfig(TREND_WEEKS=10, RANKING_WEEKS=4, VIX_ZSCORE_THRESHOLD=2.5)

    monkeypatch.setenv(rvb.PIT_CACHE_ENV, "0")
    db_run = rvb.backtest_rotation_variant(scratch, cfg, ws, we, persist=False)
    monkeypatch.setenv(rvb.PIT_CACHE_ENV, "1")
    window_run = rvb.backtest_rotation_variant(scratch, cfg, ws, we, persist=False)

    path = rvb.build_rotation_snapshot(scratch, tmp_path, ws, we, today)
    snap = rvb.find_rotation_snapshot(tmp_path, ws, we)
    assert snap is not None and snap.receipt["path"] == str(path)
    snap_run = rvb.backtest_rotation_variant(scratch, cfg, ws, we, persist=False, pit=snap)

    assert db_run["data_source"] == {"mode": "db"}
    assert window_run["data_source"]["mode"] == "pit_window"
    assert window_run["data_source"]["db_fallbacks"] == 0
    assert snap_run["data_source"]["mode"] == "snapshot"
    assert snap_run["data_source"]["pit_snapshot_sha256"] in path.name
    assert snap_run["data_source"]["db_fallbacks"] == 0

    # the fixture must actually trade, or equality proves nothing
    assert db_run["n_trades"] > 0 and db_run["rebalance_count"] > 10
    def strip(m: dict) -> dict:
        return {k: v for k, v in m.items() if k != "data_source"}

    assert strip(window_run) == strip(db_run)
    assert strip(snap_run) == strip(db_run)


def test_snapshot_lookup_skips_windows_it_does_not_cover(scratch, tmp_path):
    from alpha_research.strategies import rotation_variant_backtest as rvb

    _seed_rotation_universe(scratch, date.today())  # noqa: DTZ011
    rvb.build_rotation_snapshot(scratch, tmp_path, date(2025, 3, 3), date(2025, 5, 30), date(2025, 7, 31))
    assert rvb.find_rotation_snapshot(tmp_path, date(2025, 3, 3), date(2025, 5, 30)) is not None
    assert rvb.find_rotation_snapshot(tmp_path, date(2025, 2, 3), date(2025, 5, 30)) is None  # starts earlier
    assert rvb.find_rotation_snapshot(tmp_path, date(2025, 3, 3), date(2025, 6, 30)) is None  # ends later
    assert rvb.find_rotation_snapshot(tmp_path, date(2025, 3, 3), date(2025, 5, 30), sha256="0" * 64) is None
