"""Point-in-time SPY and insider dimensions of the regime state vector (E1-V1, E1-V2).

The E1 look-ahead canary (``evals/e1/test_lookahead_canary.py``) found two
reads the #740 PIT fix left out:

* **E1-V1** -- the raw ``YF:SPY:close`` fallback of ``_fetch_spy_prices`` was
  bounded by observation date only, so a close re-pulled or restated after
  ``as_of`` rewrote ``spy_momentum``/``spy_rsi`` of every past vector. It now
  goes through ``read_window_known_at`` with ``SPY_CLOSE_LAG`` (a close is
  public at the end of its own session).
* **E1-V2** -- ``_get_insider_sentiment`` counted Form 4s by *transaction*
  date, so filings made (and pulled) after ``as_of`` entered past vectors. A
  row now counts only if it was pulled, or filed (VS1 convention: filing
  date 22:00 America/New_York), by the end of ``as_of`` UTC.

Fixture: a real ``raw_series`` + ``source_catalog`` on in-memory SQLite, as
in ``tests/test_regime_state_vector.py``; the PostgreSQL path (JSONB
``raw_payload->>'filing_date'``, TIMESTAMPTZ pulls) is covered by
``evals/e1/test_gates_pg.py``.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import date, datetime, timedelta, timezone

import pandas as pd
import pytest
from sqlalchemy import (
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    create_engine,
    text,
)

sqlite3.register_adapter(date, lambda d: d.isoformat())
sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" "))

SEC_SRC, OTHER_SRC, YF_SRC = 1, 2, 3
AS_OF = date(2025, 6, 10)  # a Tuesday
LATE = datetime(2026, 4, 9, 10, 0)  # griddb: every pre-2026 INSIDER / SPY row was pulled in 2026


@pytest.fixture()
def engine():
    eng = create_engine("sqlite://")
    md = MetaData()
    Table("source_catalog", md, Column("id", Integer, primary_key=True), Column("name", String, nullable=False))
    Table(
        "raw_series", md,
        Column("series_id", String, nullable=False),
        Column("source_id", Integer, nullable=False),
        Column("obs_date", Date, nullable=False),
        Column("pull_timestamp", DateTime, nullable=False),
        Column("value", Float, nullable=False),
        Column("raw_payload", Text),
        Column("pull_status", String, nullable=False),
    )
    md.create_all(eng)
    with eng.begin() as c:
        c.execute(text("INSERT INTO source_catalog (id, name) VALUES (:id, :name)"),
                  [{"id": SEC_SRC, "name": "SEC_INSIDER"}, {"id": OTHER_SRC, "name": "other"},
                   {"id": YF_SRC, "name": "yfinance"}])
    return eng


def _insert(engine, sid, d, v, ts, *, filed=None, src=SEC_SRC, status="SUCCESS"):
    payload = {} if filed is None else {"filing_date": filed if isinstance(filed, str) else filed.isoformat()}
    with engine.begin() as c:
        c.execute(
            text("INSERT INTO raw_series (series_id, source_id, obs_date, pull_timestamp, value, raw_payload, "
                 "pull_status) VALUES (:sid, :src, :d, :ts, :v, :p, :st)"),
            {"sid": sid, "src": src, "d": d, "ts": ts, "v": v, "p": json.dumps(payload), "st": status},
        )


def _sentiment(engine, as_of=AS_OF):
    from intelligence.regime.state_vector import _get_insider_sentiment

    return _get_insider_sentiment(engine, as_of)


# ── Form 4 known-at convention ─────────────────────────────────────────


@pytest.mark.parametrize("filed", [
    date(2025, 1, 15),   # EST
    date(2025, 3, 7),    # Friday before the spring DST switch
    date(2025, 3, 10),   # Monday after it
    date(2025, 7, 1),    # EDT
    date(2025, 10, 31),  # Friday before the autumn switch
    date(2025, 11, 3),   # Monday after it
])
def test_form4_known_at_matches_vs1_filing_known_at(filed):
    from analysis.panel_insider_density import filing_known_at
    from intelligence.regime.state_vector import _form4_known_at

    vs1 = filing_known_at(pd.Series([pd.Timestamp(filed)])).iloc[0].to_pydatetime()
    assert _form4_known_at(filed) == vs1
    assert _form4_known_at(filed.isoformat()) == vs1
    assert _form4_known_at(filed.strftime("%d-%b-%Y").upper()) == vs1  # SEC data-set format


@pytest.mark.parametrize("raw", [None, "", "  ", "n/a", "2025-13-40", "15/01/2025"])
def test_unusable_filing_date_has_no_known_at(raw):
    from intelligence.regime.state_vector import _form4_known_at

    assert _form4_known_at(raw) is None


def test_a_filing_is_public_the_utc_day_after_it_is_filed():
    from intelligence.regime.state_vector import _form4_known_at

    # 22:00 New York is 02:00 (EDT) / 03:00 (EST) UTC the next day.
    assert _form4_known_at(date(2025, 6, 10)) == datetime(2025, 6, 11, 2, 0, tzinfo=timezone.utc)
    assert _form4_known_at(date(2025, 1, 10)) == datetime(2025, 1, 11, 3, 0, tzinfo=timezone.utc)


# ── Insider sentiment availability ─────────────────────────────────────


def test_counts_a_form4_pulled_by_as_of_whatever_its_payload(engine):
    _insert(engine, "INSIDER:AAA:x:BUY", AS_OF - timedelta(days=5), 100.0, datetime(2025, 6, 9, 6, 0))
    assert _sentiment(engine) == pytest.approx(1.0)


def test_a_form4_filed_and_pulled_after_as_of_is_not_counted(engine):
    _insert(engine, "INSIDER:AAA:x:BUY", AS_OF - timedelta(days=5), 100.0, datetime(2025, 6, 9, 6, 0))
    _insert(engine, "INSIDER:BBB:y:SELL", AS_OF - timedelta(days=2), 900.0, LATE, filed=AS_OF + timedelta(days=1))
    assert _sentiment(engine) == pytest.approx(1.0)


def test_a_form4_filed_on_as_of_is_public_only_the_next_utc_day(engine):
    _insert(engine, "INSIDER:BBB:y:SELL", AS_OF - timedelta(days=2), 900.0, LATE, filed=AS_OF)
    assert _sentiment(engine) is None
    assert _sentiment(engine, AS_OF + timedelta(days=1)) == pytest.approx(-1.0)


def test_a_form4_filed_before_as_of_but_pulled_later_is_counted(engine):
    # Backfilled history: public at as_of through its filing date, not its pull.
    _insert(engine, "INSIDER:BBB:y:SELL", AS_OF - timedelta(days=2), 900.0, LATE, filed=AS_OF - timedelta(days=1))
    assert _sentiment(engine) == pytest.approx(-1.0)


def test_a_late_pull_without_a_filing_date_is_not_counted(engine):
    _insert(engine, "INSIDER:BBB:y:SELL", AS_OF - timedelta(days=2), 900.0, LATE)
    _insert(engine, "INSIDER:CCC:z:SELL", AS_OF - timedelta(days=2), 900.0, LATE, filed="not a date")
    assert _sentiment(engine) is None


def test_a_years_late_filing_does_not_reach_back_to_its_trade_date(engine):
    # griddb: INSIDER rows for 2018-2025 trades were all filed in 2026.
    _insert(engine, "INSIDER:OLD:q:BUY", AS_OF - timedelta(days=10), 500.0, LATE, filed=date(2026, 3, 9))
    assert _sentiment(engine) is None


def test_revision_pulled_after_as_of_does_not_replace_the_known_vintage(engine):
    d = AS_OF - timedelta(days=4)
    _insert(engine, "INSIDER:AAA:x:BUY", d, 100.0, datetime(2025, 6, 8, 6, 0), filed=d)
    _insert(engine, "INSIDER:AAA:x:BUY", d, 9000.0, LATE, filed=d)
    _insert(engine, "INSIDER:BBB:y:SELL", d, 100.0, datetime(2025, 6, 8, 6, 0), filed=d)
    assert _sentiment(engine) == pytest.approx(0.0)


def test_latest_vintage_pulled_by_as_of_wins(engine):
    d = AS_OF - timedelta(days=4)
    _insert(engine, "INSIDER:AAA:x:BUY", d, 100.0, datetime(2025, 6, 7, 6, 0))
    _insert(engine, "INSIDER:AAA:x:BUY", d, 300.0, datetime(2025, 6, 9, 6, 0))
    _insert(engine, "INSIDER:BBB:y:SELL", d, 100.0, datetime(2025, 6, 7, 6, 0))
    assert _sentiment(engine) == pytest.approx((300 - 100) / 400)


def test_filed_but_unpulled_vintages_use_the_earliest_pull(engine):
    # Two late pulls of one filing known at as_of: the first pull is what a backfill held first.
    d = AS_OF - timedelta(days=4)
    filed = AS_OF - timedelta(days=2)
    _insert(engine, "INSIDER:AAA:x:BUY", d, 300.0, LATE, filed=filed)
    _insert(engine, "INSIDER:AAA:x:BUY", d, 5000.0, LATE + timedelta(days=30), filed=filed)
    _insert(engine, "INSIDER:BBB:y:SELL", d, 100.0, LATE, filed=filed)
    assert _sentiment(engine) == pytest.approx((300 - 100) / 400)


def test_mixed_source_is_judged_on_what_was_known_at_as_of(engine):
    d = AS_OF - timedelta(days=4)
    _insert(engine, "INSIDER:MIX:x:BUY", d, 100.0, datetime(2025, 6, 7, 6, 0))
    _insert(engine, "INSIDER:CLEAN:y:SELL", d, 100.0, datetime(2025, 6, 7, 6, 0))
    # A second source for the same series, filed and pulled after as_of: invisible, so no mix.
    _insert(engine, "INSIDER:MIX:x:BUY", d - timedelta(days=1), 100.0, LATE, filed=AS_OF + timedelta(days=3),
            src=OTHER_SRC)
    assert _sentiment(engine) == pytest.approx(0.0)
    # Once that second source is known (a later as_of), the series fails closed.
    assert _sentiment(engine, AS_OF + timedelta(days=5)) == pytest.approx(-1.0)


def test_a_visible_second_source_excludes_the_series(engine):
    d = AS_OF - timedelta(days=4)
    _insert(engine, "INSIDER:MIX:x:BUY", d, 100.0, datetime(2025, 6, 7, 6, 0))
    _insert(engine, "INSIDER:MIX:x:BUY", d - timedelta(days=1), 100.0, datetime(2025, 6, 7, 6, 0), src=OTHER_SRC)
    _insert(engine, "INSIDER:CLEAN:y:SELL", d, 100.0, datetime(2025, 6, 7, 6, 0))
    assert _sentiment(engine) == pytest.approx(-1.0)


def test_failed_rows_and_rows_outside_the_window_never_count(engine):
    _insert(engine, "INSIDER:AAA:x:BUY", AS_OF - timedelta(days=3), 100.0, datetime(2025, 6, 8), status="FAILED")
    _insert(engine, "INSIDER:AAA:x:BUY", AS_OF - timedelta(days=45), 100.0, datetime(2025, 6, 8))
    _insert(engine, "INSIDER:AAA:x:BUY", AS_OF + timedelta(days=1), 100.0, datetime(2025, 6, 8))
    assert _sentiment(engine) is None


# ── SPY raw fallback (E1-V1) ───────────────────────────────────────────


def _spy(engine, d, v, ts):
    _insert(engine, "YF:SPY:close", d, v, ts, src=YF_SRC)


def test_spy_close_restated_after_as_of_does_not_replace_the_backfilled_close(engine):
    from intelligence.regime.state_vector import _fetch_spy_prices

    d = AS_OF - timedelta(days=1)
    _spy(engine, d, 500.0, LATE)  # backfill: public by its own session's end
    _spy(engine, d, 625.0, LATE + timedelta(days=90))  # a later restatement / basis change
    series, basis = _fetch_spy_prices(engine, AS_OF)
    assert basis == "YF:SPY:close"
    assert list(series.items()) == [(d, 500.0)]


def test_spy_close_pulled_by_as_of_is_the_latest_such_vintage(engine):
    from intelligence.regime.state_vector import _fetch_spy_prices

    d = AS_OF - timedelta(days=1)
    _spy(engine, d, 499.0, datetime(2025, 6, 9, 15, 0))  # intraday pull
    _spy(engine, d, 500.0, datetime(2025, 6, 9, 21, 30))  # after the close
    _spy(engine, d, 625.0, LATE)
    series, _ = _fetch_spy_prices(engine, AS_OF)
    assert list(series.items()) == [(d, 500.0)]


def test_spy_close_of_as_of_itself_is_visible_and_later_dates_are_not(engine):
    from intelligence.regime.state_vector import _fetch_spy_prices

    _spy(engine, AS_OF, 510.0, LATE)
    _spy(engine, AS_OF + timedelta(days=1), 520.0, LATE)
    series, _ = _fetch_spy_prices(engine, AS_OF)
    assert list(series.items()) == [(AS_OF, 510.0)]


def test_spy_lag_is_the_session_close():
    from intelligence.regime.state_vector import SPY_CLOSE_LAG

    days = [date(2025, 6, 6), date(2025, 6, 9), date(2025, 6, 10)]
    assert SPY_CLOSE_LAG.known_dates(days) == days
