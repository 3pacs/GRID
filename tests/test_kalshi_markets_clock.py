"""kalshi-acquired/1: the Kalshi writer's acquisition clock, and how the market-state reader cuts rows by it.

The writer runs against in-memory SQLite with its HTTP calls replaced by API-shaped markets (field names as the
public v2 API returns them) and its clock replaced by a scripted one, so a pull can straddle a reader's cutoff.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone

import pytest
from sqlalchemy import Column, Date, DateTime, Float, Integer, MetaData, String, Table, Text, create_engine, text

from ingestion.altdata import kalshi_markets as K
from intelligence.regime import market_state as M
from intelligence.regime import policy_path as PP

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

UTC = timezone.utc
RUN = datetime(2026, 11, 23, 20, 59, 30, tzinfo=UTC)          # the pull starts 30 s before the cutoff
RESPONSE = datetime(2026, 11, 23, 21, 0, 30, tzinfo=UTC)      # and its markets response lands 30 s after it
CUTOFF = datetime(2026, 11, 23, 21, 0, 0, tzinfo=UTC)
EVENT = "KXFED-26DEC"


def _market(k: float, bid, ask, **extra) -> dict:
    return {"ticker": f"{EVENT}-T{k:.2f}", "event_ticker": EVENT, "strike_type": "greater", "floor_strike": k,
            "cap_strike": None,
            "title": f"Will the upper bound of the federal funds rate be above {k:.2f}% following the Fed's Dec 9, "
                     f"2026 meeting?",
            "yes_bid_dollars": bid, "yes_ask_dollars": ask, "yes_bid_size_fp": "100.00", "yes_ask_size_fp": "100.00",
            "last_price_dollars": "0.5000", "volume_fp": "10.00", "volume_24h_fp": "1.00", "open_interest_fp": "5.00",
            **extra}


MARKETS = [_market(3.50, "0.9900", "1.0000"), _market(3.75, "0.9900", "1.0000"), _market(4.00, "0.9300", "0.9400"),
           _market(4.25, "0.0300", "0.0400"), _market(4.50, None, "0.0100"), _market(4.75, "abc", "0.0100")]


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    md = MetaData()
    Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String, nullable=False))
    Table("raw_series", md,
          Column("series_id", String, nullable=False), Column("source_id", Integer),
          Column("obs_date", Date, nullable=False), Column("pull_timestamp", DateTime),
          Column("value", Float, nullable=False), Column("raw_payload", Text), Column("pull_status", String, nullable=False))
    md.create_all(eng)
    with eng.begin() as c:
        c.execute(text("INSERT INTO source_catalog (id, name) VALUES (3, 'Kalshi')"))
    return eng


def _run(engine, monkeypatch, markets=MARKETS):
    clock = {"now": RUN}
    monkeypatch.setattr(K, "_now", lambda: clock["now"])
    puller = K.KalshiMarketsPuller(engine)

    def events(series):
        return [{"event_ticker": EVENT, "title": "Fed Dec 9", "series_ticker": "KXFED",
                 "strike_date": "2026-12-09T19:00:00Z"}] if series == "KXFED" else []

    def fetch_markets(event):
        clock["now"] = RESPONSE                   # the response arrives after the cutoff
        return [dict(m) for m in markets]

    monkeypatch.setattr(puller, "_fetch_events", events)
    monkeypatch.setattr(puller, "_fetch_markets", fetch_markets)
    return puller.pull_all()[0]


def _kxfed(engine, known_by):
    return M._Reads(engine, known_by.date(), M.LOOKBACK_DAYS, known_by=known_by).kxfed()


def test_a_pull_that_straddles_the_cutoff_is_not_known_at_it(engine, monkeypatch):
    out = _run(engine, monkeypatch)
    assert out["status"] == "SUCCESS" and out["rows_inserted"] == 1
    with engine.connect() as c:
        ts, payload = c.execute(text("SELECT pull_timestamp, raw_payload FROM raw_series")).fetchone()
    payload = json.loads(payload)
    assert payload["clock"]["rule"] == PP.ACQUIRED_CLOCK_RULE
    assert datetime.fromisoformat(payload["clock"]["acquired_at"]) == RESPONSE
    assert M._instant(ts) == RESPONSE                         # the stored pull clock is the response, not the start
    assert _kxfed(engine, CUTOFF) == []
    snaps = _kxfed(engine, RESPONSE + PP.COMMIT_GRACE)
    assert len(snaps) == 1 and snaps[0].clock_basis == "acquired" and snaps[0].ladder is not None
    assert snaps[0].acquired_from == RUN and snaps[0].acquired_by == RESPONSE    # the request started before it
    assert datetime.fromisoformat(payload["clock"]["requested_at"]) == RUN
    assert _kxfed(engine, RESPONSE + PP.COMMIT_GRACE - timedelta(seconds=1)) == []


def test_an_old_transaction_start_row_waits_out_the_legacy_span(engine):
    # The writer before kalshi-acquired/1: pull_timestamp = the transaction's start, quotes read later, no clock.
    old = {"event_ticker": EVENT, "strike_date": "2026-12-09T19:00:00Z",
           "all_strikes": [{"ticker": m["ticker"], "title": m["title"], "yes_bid": float(m["yes_bid_dollars"] or 0)
                            if m["yes_bid_dollars"] not in (None, "abc") else 0.0,
                            "yes_ask": float(m["yes_ask_dollars"])} for m in MARKETS]}
    with engine.begin() as c:
        c.execute(text("INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, "
                       "pull_status) VALUES (:s, 3, :d, :ts, 0.93, :p, 'SUCCESS')"),
                  {"s": f"KALSHI:{EVENT}", "d": RUN.date(), "ts": RUN, "p": json.dumps(old)})
    assert _kxfed(engine, CUTOFF) == []                       # started before the cutoff; its quotes may be after
    assert _kxfed(engine, RUN + PP.LEGACY_SPAN - timedelta(seconds=1)) == []
    snaps = _kxfed(engine, RUN + PP.LEGACY_SPAN)
    assert len(snaps) == 1 and snaps[0].clock_basis == "transaction_start"
    # its stored zeros for the missing bids read as absent sides, so the ladder still prices from one-sided bands
    assert snaps[0].ladder is not None and snaps[0].ladder.one_sided == 4


def test_the_writer_keeps_missing_quotes_missing(engine, monkeypatch):
    _run(engine, monkeypatch)
    with engine.connect() as c:
        payload = json.loads(c.execute(text("SELECT raw_payload FROM raw_series")).scalar())
    by_k = {s["floor_strike"]: s for s in payload["all_strikes"]}
    assert by_k[4.50]["yes_bid"] is None and by_k[4.75]["yes_bid"] is None     # missing and malformed: null, not 0.0
    assert by_k[4.00]["yes_bid"] == 0.93 and by_k[4.00]["yes_bid_size"] == 100.0
    assert by_k[4.00]["event_ticker"] == EVENT and by_k[4.00]["strike_type"] == "greater"
    lad, why = PP.price_ladder(payload["all_strikes"], event=f"KALSHI:{EVENT}",
                               meeting_at=datetime(2026, 12, 9, 19, tzinfo=UTC))
    assert why is None and lad.one_sided == 4


def test_a_market_from_another_event_in_the_response_makes_the_ladder_unusable(engine, monkeypatch):
    stray = _market(4.25, "0.0300", "0.0400", ticker="KXFED-27JAN-T4.25", event_ticker="KXFED-27JAN")
    _run(engine, monkeypatch, markets=MARKETS[:3] + [stray] + MARKETS[4:])
    snaps = _kxfed(engine, RESPONSE + PP.COMMIT_GRACE)
    assert snaps[0].ladder is None and "not a contract of KXFED-26DEC" in snaps[0].reason


def test_a_second_run_the_same_day_writes_nothing(engine, monkeypatch):
    _run(engine, monkeypatch)
    assert _run(engine, monkeypatch)["rows_inserted"] == 0
    with engine.connect() as c:
        assert c.execute(text("SELECT count(*) FROM raw_series")).scalar() == 1


def test_a_response_after_utc_midnight_is_dated_by_its_own_day(engine, monkeypatch):
    clock = {"now": datetime(2026, 11, 23, 23, 59, 50, tzinfo=UTC)}
    monkeypatch.setattr(K, "_now", lambda: clock["now"])
    puller = K.KalshiMarketsPuller(engine)
    monkeypatch.setattr(puller, "_fetch_events", lambda series: [
        {"event_ticker": EVENT, "series_ticker": "KXFED", "strike_date": "2026-12-09T19:00:00Z"}]
        if series == "KXFED" else [])

    def fetch_markets(event):
        clock["now"] = datetime(2026, 11, 24, 0, 0, 5, tzinfo=UTC)
        return [dict(m) for m in MARKETS]

    monkeypatch.setattr(puller, "_fetch_markets", fetch_markets)
    puller.pull_all()
    with engine.connect() as c:
        assert str(c.execute(text("SELECT obs_date FROM raw_series")).scalar())[:10] == "2026-11-24"
