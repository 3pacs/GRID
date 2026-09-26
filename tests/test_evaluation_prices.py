"""Exercise the real raw-close accessor over a fake read-only SQL connection."""
from datetime import date, datetime, timezone

import pytest

from evaluation.prices import AmbiguousPriceError, PITPriceAccessor, UnsupportedInstrumentError

VERIFIED_SINCE = datetime(2026, 8, 1, tzinfo=timezone.utc)  # injected test evidence only


def _row(obs_date, value, series_id, pulled_at, distinct_value_count=1):
    """Build a fake 5-column result row: (obs_date, value, series_id, pull_timestamp, distinct_value_count)."""
    return (obs_date, value, series_id, pulled_at, distinct_value_count)


class Result:
    def __init__(self, row):
        self.row = row

    def fetchone(self):
        return self.row


class Engine:
    def __init__(self, row):
        self.row = row
        self.calls = []

    def connect(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, query, params):
        self.calls.append((str(query), params))
        return Result(self.row)


def test_accessor_uses_exact_raw_close_and_pit_pull_cutoff():
    pulled = datetime(2026, 9, 1, 22, tzinfo=timezone.utc)
    engine = Engine(_row(date(2026, 9, 1), 123.5, "YF:AAA:close", pulled))
    point = PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))
    sql, params = engine.calls[0]
    assert "raw_series" in sql and "source_catalog" in sql
    assert "pull_timestamp <= :cutoff" in sql and "pull_timestamp >= :verified_since" in sql
    assert "pull_status = 'SUCCESS'" in sql
    assert "resolved_series" not in sql and "adj_close" not in sql
    assert params["series_id"] == "YF:AAA:close"
    assert point.basis == "raw_close" and point.bar_date == date(2026, 9, 1)


def test_missing_history_is_none():
    assert PITPriceAccessor(Engine(None), verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1)) is None


def test_unverified_historical_basis_refused_without_query():
    engine = Engine(None)
    with pytest.raises(UnsupportedInstrumentError, match="cutover"):
        PITPriceAccessor(engine)("AAA", date(2026, 9, 1))
    assert engine.calls == []


def test_wrong_series_identity_refused():
    engine = Engine(_row(date(2026, 9, 1), 100, "YF:AAA:adj_close", datetime(2026, 9, 1, 22, tzinfo=timezone.utc)))
    with pytest.raises(UnsupportedInstrumentError, match="identity"):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_nonpositive_or_nonfinite_raw_price_refused(value):
    engine = Engine(_row(date(2026, 9, 1), value, "YF:AAA:close", datetime(2026, 9, 1, 22, tzinfo=timezone.utc)))
    with pytest.raises(UnsupportedInstrumentError, match="positive finite"):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))


def test_naive_pull_timestamp_refused():
    engine = Engine(_row(date(2026, 9, 1), 100, "YF:AAA:close", datetime(2026, 9, 1, 22)))
    with pytest.raises(UnsupportedInstrumentError, match="timestamp"):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))


def test_pre_cutover_row_refused_even_if_fake_db_returns_it():
    engine = Engine(_row(date(2026, 9, 1), 100, "YF:AAA:close", datetime(2026, 7, 31, 22, tzinfo=timezone.utc)))
    with pytest.raises(UnsupportedInstrumentError, match="timestamp"):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))


def test_invalid_ticker_refused_without_query():
    engine = Engine(None)
    with pytest.raises(UnsupportedInstrumentError):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA'; DROP TABLE raw_series", date(2026, 9, 1))
    assert engine.calls == []


def test_multi_valued_date_refused_even_though_a_single_value_would_pass():
    """Confirmed live contamination: fill_missing_features.py wrote adjusted
    closes under the same YF:{ticker}:close series/source identity as the
    raw-close puller (see PRICE_SERIES_CONTRACT.md). The accessor must fail
    closed on any date with >1 distinct value for that date's window, not
    silently pick "the latest pull wins"."""
    engine = Engine(_row(date(2026, 9, 1), 123.5, "YF:AAA:close",
                          datetime(2026, 9, 1, 22, tzinfo=timezone.utc), distinct_value_count=2))
    with pytest.raises(AmbiguousPriceError, match="multiple distinct"):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))


def test_null_distinct_count_refused_rather_than_treated_as_unambiguous():
    engine = Engine(_row(date(2026, 9, 1), 123.5, "YF:AAA:close",
                          datetime(2026, 9, 1, 22, tzinfo=timezone.utc), distinct_value_count=None))
    with pytest.raises(AmbiguousPriceError):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))


@pytest.mark.parametrize("ticker", [
    "BTC-USD", "ETH-USD", "SOL-USD", "TAO-USD",
    "btc-usd", "Btc-Usd", "eth-USD",  # case-insensitive: same instrument, same refusal
])
def test_crypto_instrument_refused_without_query(ticker):
    """fill_missing_features.py pulls exactly these tickers as YF:{ticker}:close
    rows via the same yfinance source_id this accessor reads (§3 of
    GRID-642-PRICE-KNOWNAT-SESSION-CONTRACT-DECISION-20260924.md). A 24/7
    instrument must be refused outright, not silently run through NYSE-session
    exact-date bar matching."""
    engine = Engine(None)
    with pytest.raises(UnsupportedInstrumentError, match="crypto"):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)(ticker, date(2026, 9, 1))
    assert engine.calls == []


@pytest.mark.parametrize("ticker", ["AAPL", "SPY", "BRK.B", "^GSPC"])
def test_non_crypto_instrument_not_gated(ticker):
    pulled = datetime(2026, 9, 1, 22, tzinfo=timezone.utc)
    engine = Engine(_row(date(2026, 9, 1), 100.0, f"YF:{ticker}:close", pulled))
    point = PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)(ticker, date(2026, 9, 1))
    assert point is not None
