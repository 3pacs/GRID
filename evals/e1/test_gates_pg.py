"""E1 gates on real PostgreSQL (look-ahead canary + provenance fixture).

The parts SQLite cannot prove: ``store.pit.PITStore`` (``DISTINCT ON``,
retraction anti-join), ``read_window_known_at`` over ``TIMESTAMPTZ`` pull
times, the state vector's PostgreSQL-only SQL (``cross_reference_checks``),
and the shared ``raw_series`` writer against the real DDL (NOT NULL
``source_id`` / ``pull_status``, ``DEFAULT NOW()`` ``pull_timestamp``).

Throwaway schema per test (``conftest.pg_scratch``). The CI step sets
``E1_REQUIRE_PG=1``: no PostgreSQL is a failure, not a skip.
"""

from __future__ import annotations

import json
from datetime import date, datetime, time, timedelta, timezone

import numpy as np
from sqlalchemy import text

from evals.e1 import world as W
from evals.e1.test_lookahead_canary import (
    AS_OF_LIVE,
    AS_OF_MODELED,
    LEAK_THRESHOLD,
    _corr,
    _dump,
    _lags,
    future_macro_rows,
)
from store import observations as obs
from store.pit import PITStore

PIT_AS_OF = date(2024, 9, 30)


def _feature(engine, name: str) -> int:
    with engine.begin() as c:
        return c.execute(text("INSERT INTO feature_registry (name) VALUES (:n) RETURNING id"), {"n": name}).scalar_one()


def _resolved(engine, rows) -> None:
    with engine.begin() as c:
        c.execute(
            text("INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                 "VALUES (:f, :d, :r, :v, :x)"),
            rows,
        )


def _bday(d: date, n: int) -> date:
    return np.busday_offset(np.datetime64(d, "D"), n, roll="forward").astype(object)


def _pit_snapshot(store: PITStore, fids: list[int], as_of: date) -> str:
    out = {}
    for policy in ("LATEST_AS_OF", "FIRST_RELEASE"):
        df = store.get_pit(fids, as_of, policy)
        out[policy] = df.sort_values(["feature_id", "obs_date"]).astype(str).to_dict("records")
    m = store.get_feature_matrix(fids, date(2024, 1, 1), as_of, as_of, "FIRST_RELEASE")
    out["matrix"] = m.round(12).astype(str).to_dict("split")
    return json.dumps(out, sort_keys=True, default=str)


def test_pit_store_ignores_vintages_released_after_as_of(pg_scratch):
    engine = pg_scratch
    a, b = _feature(engine, "e1_macro_a"), _feature(engine, "e1_macro_b")
    rows = []
    for fid in (a, b):
        for i, d in enumerate(W.bdays(date(2024, 1, 2), date(2024, 12, 31))):
            rows.append({"f": fid, "d": d, "r": _bday(d, 1), "v": _bday(d, 1), "x": fid + i * 0.01})
            if i % 5 == 0:  # an ordinary revision 30 days later
                rows.append({"f": fid, "d": d, "r": d + timedelta(days=30), "v": d + timedelta(days=30),
                             "x": fid + i * 0.01 + 0.5})
    _resolved(engine, [r for r in rows if r["r"] <= PIT_AS_OF])
    store = PITStore(engine)
    before = _pit_snapshot(store, [a, b], PIT_AS_OF)

    # Everything released after as_of: late revisions of old dates, newer dates,
    # and a retraction made after as_of (it may only hide the row from later reads).
    _resolved(engine, [r for r in rows if r["r"] > PIT_AS_OF])
    _resolved(engine, [{"f": a, "d": d, "r": PIT_AS_OF + timedelta(days=3), "v": PIT_AS_OF + timedelta(days=3),
                        "x": -1e6} for d in W.bdays(date(2024, 8, 1), PIT_AS_OF)])
    with engine.begin() as c:
        c.execute(text("INSERT INTO resolved_series_retractions VALUES (:f, :d, :v, :t)"),
                  {"f": b, "d": date(2024, 9, 3), "v": _bday(date(2024, 9, 3), 1),
                   "t": datetime.combine(PIT_AS_OF + timedelta(days=1), time(9), tzinfo=timezone.utc)})
    assert _pit_snapshot(store, [a, b], PIT_AS_OF) == before


def _pit_leak(engine, reader) -> float:
    path = W.spy_path(date(2023, 1, 2), date(2024, 12, 31), seed=41)
    days = sorted(path)
    h = 10
    outcome = {days[i]: float(np.log(path[days[i + h]] / path[days[i]])) for i in range(len(days) - h)}
    fid = _feature(engine, "e1_leak")
    _resolved(engine, [{"f": fid, "d": d, "r": days[days.index(d) + h + 1], "v": days[days.index(d) + h + 1],
                        "x": z} for d, z in outcome.items() if days.index(d) + h + 1 < len(days)])
    decisions = days[260:len(days) - h - 2:7]
    seen = [reader(fid, t) for t in decisions]
    return _corr(seen, [outcome[t] for t in decisions])


def test_pit_store_does_not_see_a_planted_future_leak(pg_scratch):
    store = PITStore(pg_scratch)

    def latest(fid, t):
        df = store.get_pit([fid], t, "LATEST_AS_OF")
        return float(df.sort_values("obs_date")["value"].iloc[-1]) if not df.empty else np.nan

    r = _pit_leak(pg_scratch, latest)
    assert abs(r) < LEAK_THRESHOLD, f"corr with the next-period outcome {r:.2f}"


def test_pit_leak_self_test_trips_on_an_observation_date_reader(pg_scratch):
    def leaky(fid, t):
        with pg_scratch.connect() as c:
            v = c.execute(text("SELECT value FROM resolved_series WHERE feature_id = :f AND obs_date <= :t "
                               "ORDER BY obs_date DESC LIMIT 1"), {"f": fid, "t": t}).scalar()
        return np.nan if v is None else float(v)

    assert _pit_leak(pg_scratch, leaky) > 0.95


def _tz_rows(rows):
    return [{**r, "ts": W.as_utc(r["ts"])} for r in rows]


def test_read_window_known_at_on_timestamptz_ignores_future_rows(pg_scratch):
    engine = pg_scratch
    base = W.macro_rows() + W.live_rows()
    W.insert(engine, _tz_rows(base))
    # A pull at 23:30 New York on as_of is 03:30 UTC the next day: not known at as_of.
    evening = datetime(AS_OF_LIVE.year, AS_OF_LIVE.month, AS_OF_LIVE.day, 3, 30, tzinfo=timezone.utc) + timedelta(days=1)
    lags = _lags()
    series = W.DAILY + W.MONTHLY + ("ICSA",)

    def snapshot(as_of):
        with engine.connect() as conn:
            return {sid: _dump(obs.read_window_known_at(conn, sid, as_of=as_of, lag=lags.get(sid)))
                    for sid in series}

    before = {d: snapshot(d) for d in (AS_OF_MODELED, AS_OF_LIVE)}
    W.insert(engine, _tz_rows(future_macro_rows(AS_OF_LIVE, base)))
    W.insert(engine, [W.row("VIXCLS", AS_OF_LIVE, 55.0, evening)])
    assert snapshot(AS_OF_LIVE) == before[AS_OF_LIVE]
    W.insert(engine, _tz_rows(future_macro_rows(AS_OF_MODELED, base)))
    assert snapshot(AS_OF_MODELED) == before[AS_OF_MODELED]


def test_state_vector_on_postgres_ignores_future_macro_rows_and_checks(pg_scratch):
    from intelligence.regime.state_vector import DIM_NAMES, compute_state_vector

    engine = pg_scratch
    base = W.macro_rows() + W.live_rows()
    W.insert(engine, _tz_rows(base))
    with engine.begin() as c:
        c.execute(text("INSERT INTO cross_reference_checks (checked_at, divergence_zscore) VALUES (:t, :z)"),
                  [{"t": datetime.combine(AS_OF_LIVE - timedelta(days=k), time(12), tzinfo=timezone.utc),
                    "z": 0.5 + k / 10} for k in range(0, 6)])

    def vector():
        sv = compute_state_vector(engine, AS_OF_LIVE)
        return json.dumps([list(sv.values), list(sv.stale_dimensions), sv.completeness, sv.price_basis])

    before = vector()
    assert json.loads(before)[0][DIM_NAMES.index("crossref_divergence")] is not None
    W.insert(engine, _tz_rows(future_macro_rows(AS_OF_LIVE, base)))
    with engine.begin() as c:
        c.execute(text("INSERT INTO cross_reference_checks (checked_at, divergence_zscore) VALUES (:t, :z)"),
                  [{"t": datetime.combine(AS_OF_LIVE + timedelta(days=k), time(12), tzinfo=timezone.utc),
                    "z": 50.0} for k in range(1, 4)])
    assert vector() == before


# ── provenance fixture: the shared raw_series writer on the real DDL ──


def test_base_puller_insert_records_source_status_and_pull_time(pg_scratch):
    from ingestion.base import BasePuller

    class _Fixture(BasePuller):
        SOURCE_NAME = "fred"

    puller = _Fixture(pg_scratch)
    with pg_scratch.connect() as c:
        start = c.execute(text("SELECT NOW()")).scalar()
    with pg_scratch.begin() as c:
        puller._insert_raw(c, "E1:PROVENANCE", date(2026, 9, 29), 1.5, {"k": "v"})
        puller._insert_raw(c, "E1:PROVENANCE", date(2026, 9, 30), 0.0, None, pull_status="FAILED")
    with pg_scratch.connect() as c:
        rows = c.execute(text("SELECT sc.name, r.pull_status, r.pull_timestamp FROM raw_series r "
                              "JOIN source_catalog sc ON sc.id = r.source_id "
                              "WHERE r.series_id = 'E1:PROVENANCE' ORDER BY r.obs_date")).fetchall()
        end = c.execute(text("SELECT NOW()")).scalar()
    assert [(r[0], r[1]) for r in rows] == [("fred", "SUCCESS"), ("fred", "FAILED")]
    assert all(start <= r[2] <= end for r in rows)
