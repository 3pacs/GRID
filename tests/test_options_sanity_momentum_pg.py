"""PostgreSQL contract: CROSS_ASSET momentum is the underlying's own completed close, fenced at the decision instant.

``trading/options_recommender.py::_sanity_cross_asset`` used to read
``{ticker}_close`` from ``resolved_series``. Measured on griddb 2026-11-23
(read-only), of the 194 tickers with an options_daily_signals row in 30 days
only CL had that feature, and ``cl_close`` is WTI crude (YF:CL=F), not
Colgate, the CL option's underlying. The other 193 read a fake +0.00%
momentum and passed. The read had no point-in-time fence.

The read is now ``underlying_momentum``: ``YF:{TICKER}:close`` completed
closes (yfinance + TIINGO, pulled after 17:00 New York on the session day,
``store.observations.read_completed_closes``), labels up to ``run_date`` and
pulls up to the decision instant. All 6 consecutive sessions ending at the
newest (fresh) close need a valid, unambiguous close; momentum is newest over
first, recorded splits re-based. No measurable momentum is SKIP. The split
and journal reads are fenced at the same instant.

Codex review 6030559940 (#1400 at d63cd12e) asked for the decision instant
rather than the end of the run date, and an intraday same-day fixture. A
recommendation at 11:00 New York must not see that day's close pulled at
17:05 or a regime written that afternoon; one after them must. A replay
with only a date is a labelled reconstruction and keeps its real verdict.

CROSS_ASSET is a shadow check (Anik, 2026-11-23: "shadow until validated"):
its verdict is recorded with ``"shadow": True`` and never rejects. The gate
itself is tested in tests/test_options_recommender_pricing.py.

Codex review 6030034930 (#1400 at 344f5d11) asked for tests of: the ticker
collision (CL the equity vs crude), missing and sparse rows (two rows must
not be enough), reversals and retractions, and a later regime not lifting a
historical veto. Each has a named test below.

Calendar used by the seed: run date Monday 2026-11-23, so the fresh window
starts Friday 11-20 and the base session is Friday 11-13 (EST: 17:00 New York
is 22:00 UTC). A replay dated Wednesday 11-18 reads 11-11 to 11-18; one
dated Friday 11-20 reads 11-13 to 11-20.

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
FRIDAY = date(2026, 11, 20)
SESSIONS = [date(2026, 11, d) for d in (11, 12, 13, 16, 17, 18, 19, 20)]


def _utc(d: date, hh: int, mm: int = 0) -> datetime:
    return datetime(d.year, d.month, d.day, hh, mm, tzinfo=timezone.utc)


# Decision instants (EST: New York = UTC-5).
AT_RUN = _utc(RUN, 21)                  # Mon 11-23 16:00 New York
AT_REPLAY = _utc(REPLAY, 23, 45)        # Wed 11-18 18:45 New York, after that day's 18:30 close
FRI_1100 = _utc(FRIDAY, 16)             # Fri 11-20 11:00 New York
FRI_1704 = _utc(FRIDAY, 22, 4)          # 17:04, one minute before INT's close is pulled
FRI_1706 = _utc(FRIDAY, 22, 6)          # 17:06, one minute after
FRI_1730 = _utc(FRIDAY, 22, 30)         # 17:30, after the 14:00 regime entry


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
]
# REV: a later vintage revises 11-18 (97.0 -> 97.5, pulled 06:00 UTC 11-19,
# after a run dated 11-18 could see it). Seen, the label is ambiguous.
_tiingo_closes("REV", dict(zip(SESSIONS, (100.0, 101.0, 100.0, 99.0, 98.0, 97.0, 96.0, 95.0), strict=True)))
RAW.append(("YF:REV:close", "yfinance", date(2026, 11, 18), _utc(date(2026, 11, 19), 6), 97.5, "SUCCESS", None))
# INT: closes pulled at 17:05 New York (22:05 UTC) each session.
for d, v in zip(SESSIONS, (100.0, 100.0, 101.0, 102.0, 103.0, 104.0, 105.0, 110.0), strict=True):
    RAW.append(("YF:INT:close", "TIINGO", d, _utc(d, 22, 5), v, "SUCCESS", None))
# PATH: down 15% and back inside the window; momentum is newest over first.
_tiingo_closes("PATH", dict(zip(SESSIONS, (100.0, 100.0, 100.0, 90.0, 85.0, 90.0, 95.0, 101.0), strict=True)))
# SPL: 2-for-1 effective 11-18; TIINGO closes are as traded.
_tiingo_closes("SPL", {SESSIONS[2]: 200.0, SESSIONS[3]: 202.0, SESSIONS[4]: 204.0,
                       SESSIONS[5]: 103.0, SESSIONS[6]: 104.0, SESSIONS[7]: 105.0})
RAW.append(("YF:SPL:split_factor", "TIINGO", date(2026, 11, 18), _utc(date(2026, 11, 17), 23, 30),
            2.0, "SUCCESS", None))
# SPL2: the same 2-for-1, but the split row was recorded at 03:00 UTC 11-24
# (22:00 New York on 11-23): after a 16:00 decision that day, before its end.
_tiingo_closes("SPL2", {SESSIONS[2]: 200.0, SESSIONS[3]: 202.0, SESSIONS[4]: 204.0,
                        SESSIONS[5]: 103.0, SESSIONS[6]: 104.0, SESSIONS[7]: 105.0})
RAW.append(("YF:SPL2:split_factor", "TIINGO", date(2026, 11, 18), _utc(date(2026, 11, 24), 3),
            2.0, "SUCCESS", None))
# AMB: a later completed yfinance vintage of 11-20 disagrees with TIINGO.
_tiingo_closes("AMB", {d: 50.0 for d in SESSIONS})
RAW.append(("YF:AMB:close", "yfinance", SESSIONS[-1], _utc(date(2026, 11, 21), 5, 15), 51.0, "SUCCESS", None))
# STL: nothing after 11-18.
_tiingo_closes("STL", {d: 20.0 for d in SESSIONS[:6]})
# GAP: no close on the base session 11-13.
_tiingo_closes("GAP", {d: 30.0 for d in SESSIONS if d != date(2026, 11, 13)})
# SPARSE: no close on 11-17, inside the window.
_tiingo_closes("SPARSE", {d: 30.0 for d in SESSIONS if d != date(2026, 11, 17)})
# TWO: only the two end points; the old read needed no more than this.
_tiingo_closes("TWO", {date(2026, 11, 13): 30.0, date(2026, 11, 20): 33.0})
# QRT: the only 11-18 row was quarantined (a withdrawn observation).
_tiingo_closes("QRT", {d: 40.0 for d in SESSIONS if d != date(2026, 11, 18)})
RAW.append(("YF:QRT:close", "TIINGO", date(2026, 11, 18), _utc(date(2026, 11, 18), 23, 30), 44.0,
            "QUARANTINED", None))
# QOK: a quarantined row beside the SUCCESS row of 11-17 disagrees; it is
# not a vintage, so the label is not ambiguous.
_tiingo_closes("QOK", dict(zip(SESSIONS, (40.0, 40.0, 40.0, 40.0, 40.0, 41.0, 41.0, 42.0), strict=True)))
RAW.append(("YF:QOK:close", "TIINGO", date(2026, 11, 17), _utc(date(2026, 11, 18), 1), 400.0, "QUARANTINED", None))
# K: Kellanova's last session is 2025-12-11 (analysis.sector_map status), so
# TIINGO rows after it are another listing's prices (tiingo_symbol_contract,
# the rule behind cl_close's 35,223 retractions on griddb).
_tiingo_closes("K", {d: 60.0 for d in SESSIONS})
# UNK: only yfinance rows the writer marked capture_state UNKNOWN (griddb:
# AMBR, AVLN, COYA, KLRA, UPB on 2026-11-23).
for d in SESSIONS:
    RAW.append(("YF:UNK:close", "yfinance", d, _utc(_next(d), 5, 52), 3.0, "SUCCESS",
                {"capture_state": "UNKNOWN", "price_basis": "YF:UNK:close"}))
# CL: Colgate's TIINGO closes as on griddb (11-13 99.67, 11-20 98.71).
_tiingo_closes("CL", dict(zip(SESSIONS, (98.09, 97.90, 99.67, 100.99, 99.61, 98.85, 96.88, 98.71), strict=True)))
# Crude under its own symbol (yfinance YF:CL=F); the CL option never reads it.
RAW += [("YF:CL=F:close", "yfinance", d, _utc(_next(d), 0, 30), v, "SUCCESS", None)
        for d, v in zip(SESSIONS, (85.0, 86.0, 88.92, 85.15, 84.98, 82.45, 82.92, 83.62), strict=True)]
# cl_close = WTI crude in resolved_series, which the old read used (11-13
# 88.92 -> 11-20 83.62: -5.96%).
CRUDE = {date(2026, 11, 13): 88.92, date(2026, 11, 16): 85.15, date(2026, 11, 17): 84.98,
         date(2026, 11, 18): 82.45, date(2026, 11, 19): 82.92, date(2026, 11, 20): 83.62}

JOURNAL = [
    (_utc(date(2026, 11, 17), 15), "GROWTH"),
    (_utc(FRIDAY, 15), "FRAGILE"),          # Fri 10:00 New York
    (_utc(FRIDAY, 19), "GROWTH"),           # Fri 14:00 New York: later the same day
    (_utc(date(2026, 11, 22), 12), "FRAGILE"),
    # After the run (and after the end of 2026-11-23 in New York): unseen.
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


def _check(pg, ticker: str, direction: str, run_date: date = RUN,
           decision_ts: datetime | None = AT_RUN) -> dict[str, str]:
    from trading.options_recommender import OptionsRecommender

    rec = SimpleNamespace(ticker=ticker, direction=direction)
    return OptionsRecommender._sanity_cross_asset(None, rec, pg, run_date=run_date, decision_ts=decision_ts)


def _cross_asset(pg, ticker: str, direction: str, run_date: date = RUN,
                 decision_ts: datetime | None = AT_RUN) -> dict[str, str]:
    """Status and reason only; ``as_of`` / ``basis`` are pinned in their own tests."""
    out = _check(pg, ticker, direction, run_date, decision_ts)
    return {"status": out["status"], "reason": out["reason"]}


def _momentum(pg, ticker: str, run_date: date = RUN, as_of_ts: datetime | None = AT_RUN):
    from trading.options_recommender import underlying_momentum

    with pg.connect() as conn:
        return underlying_momentum(conn, ticker, run_date, as_of_ts=as_of_ts)


def test_momentum_is_completed_close_five_sessions_back(pg):
    m = _momentum(pg, "AAA")
    # 95 / 100: the 15:00 capture (90) and the FAILED zero are not closes.
    assert m.momentum == pytest.approx(-0.05)
    assert (m.start, m.end) == (date(2026, 11, 13), date(2026, 11, 20))


def test_cross_asset_verdicts_on_the_run_date(pg):
    # FRAGILE (11-22) is the newest journal state at the decision instant;
    # the GROWTH entry of 11-24 is after it. -5% is a sharp decline for a CALL.
    assert _cross_asset(pg, "AAA", "CALL") == {
        "status": "FAIL",
        "reason": "Momentum (-5.00%, AAA close 2026-11-13 to 2026-11-20) and regime both contradict CALL",
    }
    assert _cross_asset(pg, "aaa", "PUT") == {
        "status": "PASS",
        "reason": "Momentum (-5.00%, AAA close 2026-11-13 to 2026-11-20) and regime aligned with PUT",
    }


def test_result_carries_its_decision_instant(pg):
    out = _check(pg, "AAA", "PUT")
    assert (out["as_of"], out["basis"]) == ("2026-11-23T21:00:00+00:00", "decision_time")
    skip = _check(pg, "NONE", "PUT")
    assert (skip["status"], skip["as_of"], skip["basis"]) == ("SKIP", "2026-11-23T21:00:00+00:00", "decision_time")


def test_replay_is_fenced_at_its_decision_instant(pg):
    # Wed 11-18 18:45 New York: closes labelled after 11-18 are unseen, and
    # the journal state is GROWTH (11-17), not the later FRAGILE.
    m = _momentum(pg, "AAA", REPLAY, AT_REPLAY)
    assert m.momentum == pytest.approx(97.0 / 100.0 - 1.0)
    assert (m.start, m.end) == (date(2026, 11, 11), date(2026, 11, 18))
    assert _cross_asset(pg, "AAA", "CALL", REPLAY, AT_REPLAY) == {
        "status": "PASS",
        "reason": "Partial alignment — momentum=-3.00% (AAA close 2026-11-11 to 2026-11-18), news_aligned=True",
    }
    assert _cross_asset(pg, "AAA", "PUT", REPLAY, AT_REPLAY) == {
        "status": "PASS",
        "reason": "Partial alignment — momentum=-3.00% (AAA close 2026-11-11 to 2026-11-18), news_aligned=False",
    }


def test_intraday_decision_does_not_see_that_days_later_close(pg):
    # INT's 11-20 close was pulled at 17:05 New York. At 11:00 and at 17:04
    # the newest close is 11-19 (window 11-12..11-19, 105/100); from 17:06
    # on it is 11-20 (window 11-13..11-20, 110/101).
    for at in (FRI_1100, FRI_1704):
        m = _momentum(pg, "INT", FRIDAY, at)
        assert (m.start, m.end) == (date(2026, 11, 12), date(2026, 11, 19))
        assert m.momentum == pytest.approx(105.0 / 100.0 - 1.0)
    m = _momentum(pg, "INT", FRIDAY, FRI_1706)
    assert (m.start, m.end) == (date(2026, 11, 13), date(2026, 11, 20))
    assert m.momentum == pytest.approx(110.0 / 101.0 - 1.0)


def test_intraday_decision_does_not_see_a_regime_written_later_that_day(pg):
    # FRAGILE at 10:00 New York, GROWTH at 14:00. AAA's 11-20 close is pulled
    # at 18:30, so both decisions read 11-12..11-19 (96/101 = -4.95%).
    assert _cross_asset(pg, "AAA", "CALL", FRIDAY, FRI_1100) == {
        "status": "FAIL",
        "reason": "Momentum (-4.95%, AAA close 2026-11-12 to 2026-11-19) and regime both contradict CALL",
    }
    assert _cross_asset(pg, "AAA", "CALL", FRIDAY, FRI_1730) == {
        "status": "PASS",
        "reason": "Partial alignment — momentum=-4.95% (AAA close 2026-11-12 to 2026-11-19), news_aligned=True",
    }


def test_later_regime_does_not_lift_a_historical_veto(pg):
    # The CALL vetoes decided at Fri 11:00 (FRAGILE) and Mon 16:00 (FRAGILE of
    # 11-22) stand, although the journal now ends in GROWTH (11-24) and had a
    # GROWTH entry at Fri 14:00.
    with pg.connect() as conn:
        newest = conn.execute(text(
            "SELECT inferred_state FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1")).scalar()
    assert newest == "GROWTH"
    assert _cross_asset(pg, "AAA", "CALL", FRIDAY, FRI_1100)["status"] == "FAIL"
    assert _cross_asset(pg, "AAA", "CALL", RUN, AT_RUN)["status"] == "FAIL"


def test_date_only_replay_is_a_labelled_reconstruction(pg):
    # No decision instant: read to the end of the run date in New York and
    # label it. The verdict is kept as computed.
    call = _check(pg, "AAA", "CALL", RUN, None)
    assert call == {
        "status": "FAIL",
        "reason": "Momentum (-5.00%, AAA close 2026-11-13 to 2026-11-20) and regime both contradict CALL",
        "as_of": "2026-11-24T04:59:59.999999+00:00",
        "basis": "date_only_reconstruction",
    }
    put = _check(pg, "AAA", "PUT", RUN, None)
    assert (put["status"], put["basis"]) == ("PASS", "date_only_reconstruction")


def test_tripwire_reconstructions_never_veto_because_cross_asset_is_shadow():
    """Codex 6030559940: a date-only reconstruction is never a replayed veto.

    That holds today because CROSS_ASSET is in SHADOW_CHECKS and never
    rejects. Promoting it out of shadow (after an approved validation look)
    must keep DATE_ONLY_BASIS results non-vetoing; update this test then.
    """
    from trading.options_recommender import OPTIONAL_CHECKS, REQUIRED_CHECKS, SHADOW_CHECKS

    assert "CROSS_ASSET" in SHADOW_CHECKS
    assert "CROSS_ASSET" not in REQUIRED_CHECKS | OPTIONAL_CHECKS


def test_live_call_without_date_or_instant_decides_now():
    from trading.options_recommender import DECISION_TIME_BASIS, decision_time

    before = datetime.now(timezone.utc)
    decided = decision_time(None, None)
    assert decided is not None and before <= decided <= datetime.now(timezone.utc)
    assert decision_time(RUN, None) is None
    assert decision_time(RUN, datetime(2026, 11, 23, 16, 0)) == _utc(RUN, 16)  # naive is UTC
    assert DECISION_TIME_BASIS == "decision_time"


def test_revision_pulled_after_the_cutoff_is_unseen_then_ambiguous(pg):
    # Reversal of a stored close: 11-18 is revised 97.0 -> 97.5 at 06:00 UTC
    # 11-19. A decision on 11-18 never saw the revision; a later one sees two
    # completed values for one session and does not pick one.
    m = _momentum(pg, "REV", REPLAY, AT_REPLAY)
    assert m.momentum == pytest.approx(97.0 / 100.0 - 1.0)
    assert _cross_asset(pg, "REV", "PUT") == {
        "status": "SKIP",
        "reason": "No momentum: completed REV close for 2026-11-18 is ambiguous (vintages disagree)",
    }


def test_path_reversal_inside_the_window_is_newest_over_first(pg):
    # 100 -> 85 -> 101: the 5-session return is +1%, not the -15% trough.
    assert _momentum(pg, "PATH").momentum == pytest.approx(0.01)


def test_quarantined_rows_are_withdrawn(pg):
    # A QUARANTINED row (the raw_series form of a retraction) is no close:
    # the only 11-18 row of QRT leaves a hole; QOK's beside a SUCCESS row
    # does not make that label ambiguous.
    assert _cross_asset(pg, "QRT", "CALL") == {
        "status": "SKIP",
        "reason": "No momentum: no completed QRT close for 2026-11-18 (needs all 6 sessions 2026-11-13 to 2026-11-20)",
    }
    assert _momentum(pg, "QOK").momentum == pytest.approx(42.0 / 40.0 - 1.0)


def test_another_listings_tiingo_rows_are_not_the_tickers(pg):
    # K stopped trading 2025-12-11; TIINGO rows after that are another
    # listing's prices (tiingo_symbol_contract), so K has no close at all.
    assert _cross_asset(pg, "K", "CALL") == {
        "status": "SKIP",
        "reason": "No momentum: no completed K close from 2026-11-02 to 2026-11-23",
    }


def test_symbol_collision_cl_equity_vs_crude(pg):
    # The option CL is Colgate: 98.71 / 99.67 = -0.96% (griddb's verdict on
    # 2026-11-23). Crude reads only under its own symbol, CL=F (-5.96%);
    # cl_close (crude, in resolved_series) was the old read and would FAIL
    # the CALL.
    assert _momentum(pg, "CL").momentum == pytest.approx(98.71 / 99.67 - 1.0)
    assert _momentum(pg, "CL=F").momentum == pytest.approx(83.62 / 88.92 - 1.0)
    assert _cross_asset(pg, "CL", "CALL") == {
        "status": "PASS",
        "reason": "Partial alignment — momentum=-0.96% (CL close 2026-11-13 to 2026-11-20), news_aligned=False",
    }


def test_split_is_rebased_not_read_as_a_crash(pg):
    # 105 against 200 / 2: +5%, not -47.5%.
    m = _momentum(pg, "SPL")
    assert m.momentum == pytest.approx(0.05)
    assert _cross_asset(pg, "SPL", "PUT") == {
        "status": "PASS",
        "reason": "Partial alignment — momentum=+5.00% (SPL close 2026-11-13 to 2026-11-20), news_aligned=True",
    }


def test_split_read_is_fenced_at_the_decision_instant(pg):
    # A decision at 16:00 New York did not know the split recorded at 22:00:
    # it read the as-traded drop (105 / 200). The end-of-day reconstruction
    # knows it (+5%).
    assert _momentum(pg, "SPL2", RUN, AT_RUN).momentum == pytest.approx(105.0 / 200.0 - 1.0)
    assert _momentum(pg, "SPL2", RUN, None).momentum == pytest.approx(0.05)


_WINDOW = "(needs all 6 sessions 2026-11-13 to 2026-11-20)"


@pytest.mark.parametrize(("ticker", "reason"), [
    ("AMB", "No momentum: completed AMB close for 2026-11-20 is ambiguous (vintages disagree)"),
    ("STL", "No momentum: newest completed STL close is 2026-11-18, before the fresh window from 2026-11-20"),
    ("GAP", f"No momentum: no completed GAP close for 2026-11-13 {_WINDOW}"),
    ("SPARSE", f"No momentum: no completed SPARSE close for 2026-11-17 {_WINDOW}"),
    ("TWO", "No momentum: no completed TWO close for 2026-11-16, 2026-11-17, 2026-11-18, 2026-11-19 "
            f"{_WINDOW}"),
    ("UNK", "No momentum: no completed UNK close from 2026-11-02 to 2026-11-23"),
    ("NONE", "No momentum: no completed NONE close from 2026-11-02 to 2026-11-23"),
])
@pytest.mark.parametrize("direction", ["CALL", "PUT"])
def test_missing_or_sparse_momentum_is_skip_not_zero(pg, ticker, reason, direction):
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


def test_pipeline_passes_run_date_and_decision_instant_to_cross_asset():
    calls = [n for n in ast.walk(_function("_run_sanity_pipeline"))
             if isinstance(n, ast.Call) and isinstance(n.func, ast.Attribute)
             and n.func.attr == "_sanity_cross_asset"]
    assert len(calls) == 1
    kw = {k.arg: k.value for k in calls[0].keywords}
    assert isinstance(kw.get("run_date"), ast.Name) and kw["run_date"].id == "run_date"
    assert isinstance(kw.get("decision_ts"), ast.Name) and kw["decision_ts"].id == "decision_ts"
