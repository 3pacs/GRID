"""PostgreSQL contract: CROSS_ASSET momentum is the underlying's own completed close, fenced at run_date.

``trading/options_recommender.py::_sanity_cross_asset`` used to read
``{ticker}_close`` from ``resolved_series``. Measured on griddb 2026-11-23
(read-only), of the 194 tickers with an options_daily_signals row in 30 days
only CL had that feature, and ``cl_close`` is WTI crude (YF:CL=F), not
Colgate, the CL option's underlying. The other 193 read a fake +0.00%
momentum and passed. The read had no point-in-time fence.

The read is now ``underlying_momentum``: ``YF:{TICKER}:close`` completed
closes (yfinance + TIINGO, pulled after 17:00 New York on the session day,
``store.observations.read_completed_closes``), labels up to ``run_date`` and
pulls up to the end of ``run_date`` in New York, the close 5 sessions before
the newest one, recorded splits re-based. No measurable momentum is SKIP.
The journal state is fenced by the same cutoff.

Calendar used by the seed: run date Monday 2026-11-23, so the fresh window
starts Friday 11-20 and the base session is Friday 11-13 (EST: 17:00 New York
is 22:00 UTC). A replay dated Wednesday 11-18 reads 11-11 to 11-18.

Skips without ``GRID_TEST_DB_URL`` (a local throwaway database); fails
instead when ``REQUIRE_BOUND_ID_READS_PG=1`` (CI, the bound-id reads step).
The two source guards at the bottom need no database.
"""

from __future__ import annotations

import ast
import json
import os
import pathlib
from datetime import date, datetime, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine.url import make_url

pytestmark = pytest.mark.xdist_group("postgres")

_REQUIRE_ENV = "REQUIRE_BOUND_ID_READS_PG"
REPO = pathlib.Path(__file__).resolve().parent.parent

RUN = date(2026, 11, 23)
REPLAY = date(2026, 11, 18)
SESSIONS = [date(2026, 11, d) for d in (11, 12, 13, 16, 17, 18, 19, 20)]


def _utc(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=timezone.utc)


def _next(d: date) -> date:
    return date.fromordinal(d.toordinal() + 1)


# (series_id, source, obs_date, pull_timestamp, value, pull_status, raw_payload)
RAW: list[tuple] = []


def _tiingo_closes(symbol: str, values: dict[date, float]) -> None:
    for d, v in values.items():
        RAW.append((f"YF:{symbol}:close", "TIINGO", d, _utc(d, 23, 30), v, "SUCCESS", None))


# AAA: the plain case and the fences.
_tiingo_closes("AAA", dict(zip(SESSIONS, (100.0, 101.0, 100.0, 99.0, 98.0, 97.0, 96.0, 95.0), strict=True)))
RAW += [
    # A 15:00 New York capture of 11-20: before the close was complete.
    ("YF:AAA:close", "yfinance", SESSIONS[-1], _utc(SESSIONS[-1], 20), 90.0, "SUCCESS", None),
    # A failed pull marker dated 11-20.
    ("YF:AAA:close", "yfinance", SESSIONS[-1], _utc(SESSIONS[-1], 23, 45), 0.0, "FAILED", None),
    # A re-pull of 11-18 at 06:00 UTC on 11-19: after a run dated 11-18 could
    # see it. Seen, it would make 11-18 ambiguous (97.0 vs 97.5).
    ("YF:AAA:close", "yfinance", date(2026, 11, 18), _utc(date(2026, 11, 19), 6), 97.5, "SUCCESS", None),
]
# SPL: 2-for-1 effective 11-18; TIINGO closes are as traded.
_tiingo_closes("SPL", {SESSIONS[2]: 200.0, SESSIONS[3]: 202.0, SESSIONS[4]: 204.0,
                       SESSIONS[5]: 103.0, SESSIONS[6]: 104.0, SESSIONS[7]: 105.0})
RAW.append(("YF:SPL:split_factor", "TIINGO", date(2026, 11, 18), _utc(date(2026, 11, 17), 23, 30),
            2.0, "SUCCESS", None))
# AMB: a later completed yfinance vintage of 11-20 disagrees with TIINGO.
_tiingo_closes("AMB", {d: 50.0 for d in SESSIONS})
RAW.append(("YF:AMB:close", "yfinance", SESSIONS[-1], _utc(date(2026, 11, 21), 5, 15), 51.0, "SUCCESS", None))
# STL: nothing after 11-18.
_tiingo_closes("STL", {d: 20.0 for d in SESSIONS[:6]})
# GAP: no close on the base session 11-13.
_tiingo_closes("GAP", {d: 30.0 for d in SESSIONS if d != date(2026, 11, 13)})
# UNK: only yfinance rows the writer marked capture_state UNKNOWN (griddb:
# AMBR, AVLN, COYA, KLRA, UPB on 2026-11-23).
for d in SESSIONS:
    RAW.append(("YF:UNK:close", "yfinance", d, _utc(_next(d), 5, 52), 3.0, "SUCCESS",
                {"capture_state": "UNKNOWN", "price_basis": "YF:UNK:close"}))
# CL: Colgate's TIINGO closes as on griddb (11-13 99.67, 11-20 98.71).
_tiingo_closes("CL", dict(zip(SESSIONS, (98.09, 97.90, 99.67, 100.99, 99.61, 98.85, 96.88, 98.71), strict=True)))
# cl_close = WTI crude in resolved_series, which the old read used (11-13
# 88.92 -> 11-20 83.62: -5.96%).
CRUDE = {date(2026, 11, 13): 88.92, date(2026, 11, 16): 85.15, date(2026, 11, 17): 84.98,
         date(2026, 11, 18): 82.45, date(2026, 11, 19): 82.92, date(2026, 11, 20): 83.62}

JOURNAL = [
    (_utc(date(2026, 11, 17), 15), "GROWTH"),
    (_utc(date(2026, 11, 20), 15), "FRAGILE"),
    # After the end of 2026-11-23 in New York (05:00 UTC on 11-24): unseen.
    (_utc(date(2026, 11, 24), 15), "GROWTH"),
]


@pytest.fixture(scope="module")
def pg():
    url = os.environ.get("GRID_TEST_DB_URL")
    if not url:
        if os.environ.get(_REQUIRE_ENV) == "1":
            pytest.fail(f"GRID_TEST_DB_URL is required ({_REQUIRE_ENV}=1)")
        pytest.skip("GRID_TEST_DB_URL is required for the disposable PostgreSQL proof")
    parsed = make_url(url)
    if parsed.host not in {"localhost", "127.0.0.1"} or "test" not in (parsed.database or ""):
        pytest.fail("options momentum contract requires a local disposable test database")

    schema = "opt_momentum_" + uuid4().hex[:12]
    admin = create_engine(url)
    with admin.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA {schema}"))
    engine = create_engine(url, connect_args={"options": f"-csearch_path={schema}"})
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE source_catalog (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL)"))
        conn.execute(text(
            "CREATE TABLE raw_series (id BIGSERIAL PRIMARY KEY, series_id TEXT NOT NULL, "
            "source_id INTEGER NOT NULL REFERENCES source_catalog(id), obs_date DATE NOT NULL, "
            "pull_timestamp TIMESTAMPTZ NOT NULL, value DOUBLE PRECISION NOT NULL, raw_payload JSONB, "
            "pull_status TEXT NOT NULL CHECK (pull_status IN ('SUCCESS', 'PARTIAL', 'FAILED', 'QUARANTINED')))"
        ))
        conn.execute(text("CREATE INDEX ON raw_series (series_id, obs_date)"))
        conn.execute(text("CREATE TABLE feature_registry (id SERIAL PRIMARY KEY, name TEXT UNIQUE NOT NULL)"))
        conn.execute(text(
            "CREATE TABLE resolved_series (id SERIAL PRIMARY KEY, feature_id INTEGER NOT NULL, "
            "obs_date DATE NOT NULL, release_date DATE, vintage_date DATE NOT NULL, "
            "value DOUBLE PRECISION, UNIQUE (feature_id, obs_date, vintage_date))"
        ))
        conn.execute(text(
            "CREATE TABLE decision_journal (id SERIAL PRIMARY KEY, decision_timestamp TIMESTAMPTZ, "
            "inferred_state TEXT, grid_recommendation TEXT)"
        ))
        src = {name: conn.execute(text("INSERT INTO source_catalog (name) VALUES (:n) RETURNING id"),
                                  {"n": name}).scalar_one() for name in ("TIINGO", "yfinance")}
        for sid, source, obs, pulled, value, status, payload in RAW:
            conn.execute(text(
                "INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, "
                "pull_status, raw_payload) VALUES (:s, :src, :o, :p, :v, :st, CAST(:pl AS JSONB))"
            ), {"s": sid, "src": src[source], "o": obs, "p": pulled, "v": value, "st": status,
                "pl": None if payload is None else json.dumps(payload)})
        fid = conn.execute(text("INSERT INTO feature_registry (name) VALUES ('cl_close') RETURNING id")).scalar_one()
        for obs, value in CRUDE.items():
            conn.execute(text(
                "INSERT INTO resolved_series (feature_id, obs_date, release_date, vintage_date, value) "
                "VALUES (:f, :o, :o, :o, :v)"), {"f": fid, "o": obs, "v": value})
        for ts, state in JOURNAL:
            conn.execute(text(
                "INSERT INTO decision_journal (decision_timestamp, inferred_state, grid_recommendation) "
                "VALUES (:ts, :s, 'HOLD')"), {"ts": ts, "s": state})
    yield engine
    engine.dispose()
    with admin.begin() as conn:
        conn.execute(text(f"DROP SCHEMA {schema} CASCADE"))
    admin.dispose()


def _cross_asset(pg, ticker: str, direction: str, run_date: date = RUN) -> dict[str, str]:
    from trading.options_recommender import OptionsRecommender

    rec = SimpleNamespace(ticker=ticker, direction=direction)
    return OptionsRecommender._sanity_cross_asset(None, rec, pg, run_date=run_date)


def _momentum(pg, ticker: str, run_date: date = RUN):
    from trading.options_recommender import underlying_momentum

    with pg.connect() as conn:
        return underlying_momentum(conn, ticker, run_date)


def test_momentum_is_completed_close_five_sessions_back(pg):
    m = _momentum(pg, "AAA")
    # 95 / 100: the 15:00 capture (90) and the FAILED zero are not closes.
    assert m.momentum == pytest.approx(-0.05)
    assert (m.start, m.end) == (date(2026, 11, 13), date(2026, 11, 20))


def test_cross_asset_verdicts_on_the_run_date(pg):
    # FRAGILE (11-20) is the newest journal state by the cutoff; the GROWTH
    # entry of 11-24 is after it. -5% is a sharp decline for a CALL.
    assert _cross_asset(pg, "AAA", "CALL") == {
        "status": "FAIL",
        "reason": "Momentum (-5.00%, AAA close 2026-11-13 to 2026-11-20) and regime both contradict CALL",
    }
    assert _cross_asset(pg, "aaa", "PUT") == {
        "status": "PASS",
        "reason": "Momentum (-5.00%, AAA close 2026-11-13 to 2026-11-20) and regime aligned with PUT",
    }


def test_replay_is_fenced_at_run_date(pg):
    # Dated 11-18: closes labelled after 11-18 and the 11-18 re-pull of
    # 06:00 UTC 11-19 are unseen (seen, 11-18 would be ambiguous), and the
    # journal state is GROWTH (11-17), not the later FRAGILE.
    m = _momentum(pg, "AAA", REPLAY)
    assert m.momentum == pytest.approx(97.0 / 100.0 - 1.0)
    assert (m.start, m.end) == (date(2026, 11, 11), date(2026, 11, 18))
    assert _cross_asset(pg, "AAA", "CALL", REPLAY) == {
        "status": "PASS",
        "reason": "Partial alignment — momentum=-3.00% (AAA close 2026-11-11 to 2026-11-18), news_aligned=True",
    }
    assert _cross_asset(pg, "AAA", "PUT", REPLAY) == {
        "status": "PASS",
        "reason": "Partial alignment — momentum=-3.00% (AAA close 2026-11-11 to 2026-11-18), news_aligned=False",
    }


def test_split_is_rebased_not_read_as_a_crash(pg):
    # 105 against 200 / 2: +5%, not -47.5%.
    m = _momentum(pg, "SPL")
    assert m.momentum == pytest.approx(0.05)
    assert _cross_asset(pg, "SPL", "PUT") == {
        "status": "PASS",
        "reason": "Partial alignment — momentum=+5.00% (SPL close 2026-11-13 to 2026-11-20), news_aligned=True",
    }


def test_cl_reads_colgate_not_crude(pg):
    # Colgate 98.71 / 99.67 = -0.96% (griddb's verdict on 2026-11-23); the
    # crude feature cl_close would read -5.96% and FAIL the CALL.
    assert _cross_asset(pg, "CL", "CALL") == {
        "status": "PASS",
        "reason": "Partial alignment — momentum=-0.96% (CL close 2026-11-13 to 2026-11-20), news_aligned=False",
    }


@pytest.mark.parametrize(("ticker", "reason"), [
    ("AMB", "No momentum: completed AMB close for 2026-11-20 is ambiguous (vintages disagree)"),
    ("STL", "No momentum: newest completed STL close is 2026-11-18, before the fresh window from 2026-11-20"),
    ("GAP", "No momentum: no completed GAP close for 2026-11-13, 5 sessions before 2026-11-20"),
    ("UNK", "No momentum: no completed UNK close from 2026-11-02 to 2026-11-23"),
    ("NONE", "No momentum: no completed NONE close from 2026-11-02 to 2026-11-23"),
])
@pytest.mark.parametrize("direction", ["CALL", "PUT"])
def test_no_measurable_momentum_is_skip_not_zero(pg, ticker, reason, direction):
    assert _cross_asset(pg, ticker, direction) == {"status": "SKIP", "reason": reason}


# ── Source guards (no database) ──────────────────────────────────────────────

def _function(name: str) -> ast.FunctionDef:
    tree = ast.parse((REPO / "trading/options_recommender.py").read_text(encoding="utf-8"))
    found = [n for n in ast.walk(tree) if isinstance(n, ast.FunctionDef) and n.name == name]
    assert len(found) == 1, name
    return found[0]


def test_cross_asset_reads_no_feature_and_has_no_zero_fallback():
    fn = _function("_sanity_cross_asset")
    body = fn.body[1:] if ast.get_docstring(fn) else fn.body  # the docstring tells the history
    src = "\n".join(ast.unparse(n) for n in body)
    assert "resolved_series" not in src
    assert "_close" not in src
    assert "momentum = 0" not in src


def test_pipeline_passes_run_date_to_cross_asset():
    calls = [n for n in ast.walk(_function("_run_sanity_pipeline"))
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr == "_sanity_cross_asset"]
    assert len(calls) == 1
    kw = {k.arg: k.value for k in calls[0].keywords}
    assert isinstance(kw.get("run_date"), ast.Name) and kw["run_date"].id == "run_date"
