"""The session still trading is not stored as an ETF flow row; recent rows are revised.

Production, read-only, 2026-11-27: yfinance returns the current session's bar
while it is still trading, with the volume so far. ``_pull_etf_flows`` stored
it and then skipped that date for good. SPY, stored volume against the final
one: 11-17 2,144,207 (written 14:33Z) vs 71,408,000; 11-18 7,432,349 vs
51,595,000; 11-19 10,713,031 vs 76,476,400; 11-20 20,344,147 vs 151,049,800.
About 160 of the trading days since 2026-03-30 were written on their own day,
for each of the 11 ETFs and all three series.

The writer now leaves the current New York date's bar for a later run. That is
a conservative minimum, not provider finality, so a stored date inside the
revision window is written again, as a new vintage, when its value changed.
Nothing is updated in place, and a reader asked about a past date sees only
what had been received by then (Codex review 6084304156). The real writer runs
here against a fake yfinance and a clock the test sets (network and clock
replaced only).
"""

from __future__ import annotations

import json
import sys
import types
from datetime import date, datetime, timedelta

import pandas as pd
import pytest
from sqlalchemy import text

from analysis.money_flow_engine import layer_institutional
from ingestion import flow_materializer
from ingestion.altdata import institutional_flows as inst
from ingestion.smart_scheduler import _classify_outcome
from intelligence import dollar_flows
from tests import thirteenf_holdings_support as s

TODAY = date(2026, 11, 20)            # a Friday; the session is trading
SERIES_5D = "ETF_FLOW:SPY:5d"
FINAL_VOLUME = 1_000_000              # every finished session, for easy sums
CLOSE = 100.0
FIVE_DAY = 5 * FINAL_VOLUME * CLOSE   # the 5d flow of any date with five finished sessions


def _bars(last: date, partial_volume: int | None = None) -> pd.DataFrame:
    """60 business days of bars ending at ``last``. With ``partial_volume`` the
    last bar is the session still trading and carries only that volume."""
    days = pd.bdate_range(end=pd.Timestamp(last), periods=60)
    volumes = [FINAL_VOLUME] * len(days)
    if partial_volume is not None:
        volumes[-1] = partial_volume
    return pd.DataFrame({"Close": [CLOSE] * len(days), "Volume": volumes}, index=days)


@pytest.fixture()
def engine():
    return s.make_sqlite_engine()


@pytest.fixture()
def run(engine, monkeypatch):
    """run(today, frame, **kwargs) -> results of the real ETF writer for SPY."""
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY"])
    monkeypatch.setattr(inst.time, "sleep", lambda *_a: None)

    def _run(today: date, frame: pd.DataFrame, start: date | None = None, **kwargs) -> list[dict]:
        monkeypatch.setattr(inst, "_new_york_today", lambda: today)
        monkeypatch.setitem(sys.modules, "yfinance",
                            types.SimpleNamespace(download=lambda *_a, **_k: frame.copy()))
        puller = inst.InstitutionalFlowsPuller(engine)
        return puller._pull_etf_flows(start or today - timedelta(days=7), today, **kwargs)

    return _run


def _rows(engine, series_id: str = SERIES_5D) -> list[tuple[date, float, dict, str]]:
    with engine.connect() as conn:
        found = conn.execute(text(
            "SELECT obs_date, value, raw_payload, pull_status FROM raw_series "
            "WHERE series_id = :sid ORDER BY obs_date, id"), {"sid": series_id}).fetchall()
    out = []
    for obs_date, value, payload, status in found:
        if isinstance(obs_date, str):
            obs_date = date.fromisoformat(obs_date[:10])
        out.append((obs_date, float(value), json.loads(payload) if isinstance(payload, str) else payload, status))
    return out


def _store(engine, obs_date: date, value: float, volume: int, status: str = "SUCCESS",
           pulled: datetime | None = None, series_id: str = SERIES_5D) -> None:
    """A row as an earlier run left it, received at ``pulled`` (default: now)."""
    with engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO raw_series (series_id, source_id, obs_date, value, raw_payload, pull_status, "
            "pull_timestamp) VALUES (:sid, 7, :d, :v, :p, :st, COALESCE(:ts, CURRENT_TIMESTAMP))"),
            {"sid": series_id, "d": obs_date, "v": value, "ts": pulled,
             "p": json.dumps({"ticker": series_id.split(":")[1], "close": CLOSE, "volume": volume}),
             "st": status})


def _item(results: list[dict], series_id: str = SERIES_5D) -> dict:
    (item,) = [r for r in results if r["feature"] == series_id]
    return item


# ── The session still trading is not a bar ─────────────────────────────────


def test_the_session_still_trading_is_not_stored(engine, run):
    results = run(TODAY, _bars(TODAY, partial_volume=30_000))       # 3% of the day so far

    dates = [d for d, _v, _p, _s in _rows(engine)]
    assert TODAY not in dates and max(dates) == date(2026, 11, 19)
    assert all(value == FIVE_DAY for _d, value, _p, _s in _rows(engine))    # no partial volume in any sum
    assert _item(results)["session_bars_left"] == 1
    assert _classify_outcome(results)[0] == "SUCCESS"                     # the settled dates were new


def test_a_run_before_new_york_midnight_still_leaves_the_day(engine, run):
    """The close has passed but the date has not changed in New York: the
    volume is still provisional."""
    run(TODAY, _bars(TODAY, partial_volume=950_000))
    assert TODAY not in [d for d, _v, _p, _s in _rows(engine)]


def test_the_day_is_stored_with_its_final_volume_once_new_york_has_moved_past_it(engine, run):
    run(TODAY, _bars(TODAY, partial_volume=30_000))
    before = len(_rows(engine))

    results = run(TODAY + timedelta(days=1), _bars(TODAY))            # Saturday; Friday's bar is final

    rows = _rows(engine)
    (friday,) = [r for r in rows if r[0] == TODAY]
    assert friday[1] == FIVE_DAY and friday[2]["volume"] == FINAL_VOLUME and "revises_value" not in friday[2]
    assert len(rows) == before + 1
    assert _item(results)["rows_inserted"] == 1 and _item(results)["rows_revised"] == 0
    assert _item(results)["session_bars_left"] == 0


def test_a_holiday_or_weekend_run_stores_every_finished_session(engine, run):
    run(date(2026, 11, 22), _bars(TODAY))                             # Sunday, no session in progress
    assert max(d for d, _v, _p, _s in _rows(engine)) == TODAY


def test_a_run_with_nothing_new_to_store_is_clean_and_writes_nothing(engine, run):
    run(TODAY + timedelta(days=1), _bars(TODAY))
    stored = len(_rows(engine))

    results = run(TODAY + timedelta(days=1), _bars(TODAY))

    assert len(_rows(engine)) == stored
    assert [r["rows_inserted"] for r in results] == [0, 0, 0]
    assert _classify_outcome(results)[0] == "NO_NEW_DATA"


# ── A recent date stored from a partial bar is revised ─────────────────────


def test_a_recent_row_stored_from_a_partial_bar_gets_a_new_vintage_and_keeps_the_old_one(engine, run):
    partial_value = (4 * FINAL_VOLUME + 30_000) * CLOSE              # what the old writer stored at 14:33Z
    _store(engine, TODAY, partial_value, volume=30_000)

    results = run(TODAY + timedelta(days=1), _bars(TODAY))

    vintages = [r for r in _rows(engine) if r[0] == TODAY]
    assert [value for _d, value, _p, _s in vintages] == [partial_value, FIVE_DAY]     # old first, kept
    assert vintages[1][2]["revises_value"] == partial_value and vintages[1][2]["volume"] == FINAL_VOLUME
    assert _item(results)["rows_revised"] == 1


def test_a_rerun_that_computes_the_same_values_writes_nothing(engine, run):
    _store(engine, TODAY, (4 * FINAL_VOLUME + 30_000) * CLOSE, volume=30_000)
    run(TODAY + timedelta(days=1), _bars(TODAY))
    stored = len(_rows(engine))

    results = run(TODAY + timedelta(days=1), _bars(TODAY))

    assert len(_rows(engine)) == stored
    assert _item(results)["rows_inserted"] == 0 and _item(results)["rows_revised"] == 0


def test_a_later_correction_by_the_provider_is_a_third_vintage(engine, run):
    run(TODAY + timedelta(days=1), _bars(TODAY))
    corrected = _bars(TODAY)
    corrected.iloc[-1, corrected.columns.get_loc("Volume")] = FINAL_VOLUME + 5_000

    results = run(TODAY + timedelta(days=3), corrected)

    values = [value for d, value, _p, _s in _rows(engine) if d == TODAY]
    assert values == [FIVE_DAY, FIVE_DAY + 5_000 * CLOSE]
    assert _item(results)["rows_revised"] == 1


def test_a_date_before_the_revision_floor_is_left_alone(engine, run):
    """Bounded on purpose: adjusted closes move for all of history on every
    dividend, so an unbounded comparison would rewrite the whole series."""
    old = TODAY - timedelta(days=inst._ETF_REVISION_DAYS + 7)        # a Friday, 21 days back
    _store(engine, old, 123.0, volume=1)

    # The run covers that date (its neighbours are written), and leaves it.
    results = run(TODAY + timedelta(days=1), _bars(TODAY), start=old - timedelta(days=3))

    dates = [d for d, _v, _p, _s in _rows(engine)]
    assert old - timedelta(days=1) in dates and old + timedelta(days=3) in dates
    assert [value for d, value, _p, _s in _rows(engine) if d == old] == [123.0]
    assert _item(results)["rows_revised"] == 0


def test_an_older_date_is_revised_only_when_the_caller_names_how_far_back(engine, monkeypatch):
    old = TODAY - timedelta(days=inst._ETF_REVISION_DAYS + 7)
    _store(engine, old, 123.0, volume=1)
    monkeypatch.setattr(inst, "ETF_FLOW_TICKERS", ["SPY"])
    monkeypatch.setattr(inst.time, "sleep", lambda *_a: None)
    monkeypatch.setattr(inst, "_new_york_today", lambda: TODAY + timedelta(days=1))
    monkeypatch.setitem(sys.modules, "yfinance",
                        types.SimpleNamespace(download=lambda *_a, **_k: _bars(TODAY)))
    puller = inst.InstitutionalFlowsPuller(engine)

    scheduled = puller.pull_etf_only(start_date=old - timedelta(days=3))
    assert [value for d, value, _p, _s in _rows(engine) if d == old] == [123.0]
    assert _item(scheduled)["rows_revised"] == 0

    repair = puller.pull_etf_only(start_date=old - timedelta(days=3), revise_from=old.isoformat())
    assert [value for d, value, _p, _s in _rows(engine) if d == old] == [123.0, FIVE_DAY]
    assert _item(repair)["rows_revised"] == 1


def test_a_quarantined_row_is_not_a_stored_value(engine, run):
    _store(engine, TODAY, 999.0, volume=1, status="QUARANTINED")

    results = run(TODAY + timedelta(days=1), _bars(TODAY))

    rows = [r for r in _rows(engine) if r[0] == TODAY]
    assert [(value, status) for _d, value, _p, status in rows] == [(999.0, "QUARANTINED"), (FIVE_DAY, "SUCCESS")]
    assert "revises_value" not in rows[1][2] and _item(results)["rows_revised"] == 0


def test_the_20d_and_accel_series_follow_the_same_rule(engine, run):
    run(TODAY, _bars(TODAY, partial_volume=30_000))
    for series_id in ("ETF_FLOW:SPY:20d", "ETF_FLOW:SPY:accel"):
        assert TODAY not in [d for d, _v, _p, _s in _rows(engine, series_id)]
    assert all(value == 20 * FINAL_VOLUME * CLOSE for _d, value, _p, _s in _rows(engine, "ETF_FLOW:SPY:20d"))
    assert all(value == 0.0 for _d, value, _p, _s in _rows(engine, "ETF_FLOW:SPY:accel"))


def test_new_york_today_is_the_exchange_date_not_the_utc_date():
    assert inst._NEW_YORK.key == "America/New_York"
    assert isinstance(inst._new_york_today(), date)


# ── Readers: what was received by then, not merely the newest row ──────────
#
# Codex review 6084304156 replayed a 20 November value of 403, first seen that
# day and revised to 490 on the 24th, and got 490 back for as_of = 20 November.
# An as-of read must be tied to when each vintage was received.

NOV19, NOV20, NOV23, NOV24 = date(2026, 11, 19), date(2026, 11, 20), date(2026, 11, 23), date(2026, 11, 24)


def _first_seen_403_then_revised_to_490(engine) -> None:
    # The 19th as the fixed writer stores it: first received the day after.
    _store(engine, NOV19, 380.0, volume=1, pulled=datetime(2026, 11, 20, 5, 10))
    # The 20th as the old writer stored it, mid-session, then its revision.
    _store(engine, NOV20, 403.0, volume=1, pulled=datetime(2026, 11, 20, 15, 0))
    _store(engine, NOV20, 490.0, volume=1, pulled=datetime(2026, 11, 24, 5, 10))


@pytest.mark.parametrize("as_of,expected", [
    (NOV20, 403.0),         # the revision did not exist yet
    (NOV23, 403.0),
    (NOV24, 490.0),         # received that day
    (date(2026, 11, 27), 490.0),
], ids=["old_as_of", "day_before_the_revision", "day_of_the_revision", "new_as_of"])
def test_the_money_flow_proxy_returns_what_was_held_by_the_end_of_as_of(engine, as_of, expected):
    _first_seen_403_then_revised_to_490(engine)

    node = layer_institutional._build_etf_flow_proxy(engine, as_of)

    assert node.value == expected and node.metadata["etf_count"] == 1


def test_a_date_first_received_after_as_of_does_not_reach_the_proxy(engine):
    """The 19th was first stored on the 20th. Asked about the 19th, the proxy
    has nothing: an observation date at or before as_of is not enough."""
    _first_seen_403_then_revised_to_490(engine)

    node = layer_institutional._build_etf_flow_proxy(engine, NOV19)

    assert node.value is None and node.metadata["etf_count"] == 0


def test_the_proxy_takes_each_etfs_own_latest_held_date(engine):
    _first_seen_403_then_revised_to_490(engine)
    _store(engine, NOV19, 50.0, volume=1, pulled=datetime(2026, 11, 20, 5, 12), series_id="ETF_FLOW:QQQ:5d")

    node = layer_institutional._build_etf_flow_proxy(engine, NOV20)

    assert node.value == 403.0 + 50.0 and node.metadata["etf_count"] == 2      # SPY's 20th, QQQ's 19th


@pytest.mark.parametrize("as_of,expected", [(NOV20, 403.0), (NOV23, 403.0), (NOV24, 490.0)],
                         ids=["old_as_of", "day_before_the_revision", "new_as_of"])
def test_the_dollar_flow_reader_already_honours_receipt_time_for_a_revised_etf_row(engine, as_of, expected):
    """Unchanged by this PR: pinned so the existing convention covers the
    revisions the writer now makes."""
    _first_seen_403_then_revised_to_490(engine)

    flows = dollar_flows._normalize_etf_flows(engine, days=30, as_of=as_of)

    (nov20,) = [f for f in flows if f["flow_date"] == NOV20]
    assert nov20["evidence"]["raw_value"] == expected


def test_the_dollar_flow_reader_does_not_see_a_date_first_received_after_as_of(engine):
    _first_seen_403_then_revised_to_490(engine)

    assert dollar_flows._normalize_etf_flows(engine, days=30, as_of=NOV19) == []


# ── The materialized table is a current view ───────────────────────────────


def _etf_flows_table(engine, monkeypatch) -> None:
    with engine.begin() as conn:
        conn.execute(text(
            "CREATE TABLE etf_flows (ticker TEXT, flow_date DATE, flow_value REAL, source TEXT, "
            "confidence TEXT, UNIQUE (ticker, flow_date, source))"))
    monkeypatch.setattr(flow_materializer, "_ensure_tables", lambda _engine: None)


def _materialized(engine) -> dict[str, float]:
    with engine.connect() as conn:
        return {str(d)[:10]: float(v) for d, v in conn.execute(text(
            "SELECT flow_date, flow_value FROM etf_flows WHERE ticker = 'SPY'")).fetchall()}


def test_the_materialized_table_holds_the_newest_vintage_of_each_date(engine, monkeypatch):
    _first_seen_403_then_revised_to_490(engine)
    _etf_flows_table(engine, monkeypatch)

    upserted = flow_materializer.sync_etf_flows(engine)

    assert _materialized(engine) == {"2026-11-19": 380.0, "2026-11-20": 490.0}
    assert upserted == 2                                              # one row offered per date, not per vintage


def test_the_sync_limit_counts_dates_not_vintages(engine, monkeypatch):
    """Limiting raw rows before choosing the newest vintage spent the limit on
    the older vintage of the 20th and never reached the 19th."""
    _first_seen_403_then_revised_to_490(engine)
    _etf_flows_table(engine, monkeypatch)
    monkeypatch.setattr(flow_materializer, "_ETF_FLOW_SYNC_LIMIT", 2)

    flow_materializer.sync_etf_flows(engine)

    assert _materialized(engine) == {"2026-11-19": 380.0, "2026-11-20": 490.0}


def test_a_later_sync_replaces_the_value_so_the_table_is_not_a_history(engine, monkeypatch):
    _store(engine, NOV20, 403.0, volume=1, pulled=datetime(2026, 11, 20, 15, 0))
    _etf_flows_table(engine, monkeypatch)
    flow_materializer.sync_etf_flows(engine)
    assert _materialized(engine) == {"2026-11-20": 403.0}

    _store(engine, NOV20, 490.0, volume=1, pulled=datetime(2026, 11, 24, 5, 10))
    flow_materializer.sync_etf_flows(engine)

    assert _materialized(engine) == {"2026-11-20": 490.0}             # 403 is gone from this table
    assert "not a history" in flow_materializer.sync_etf_flows.__doc__
