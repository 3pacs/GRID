"""Exercise the real raw-close accessor over a fake read-only SQL connection."""
from datetime import date, datetime, timezone

import pytest

from evaluation.prices import PITPriceAccessor, UnsupportedInstrumentError

VERIFIED_SINCE = datetime(2026, 8, 1, tzinfo=timezone.utc)  # injected test evidence only


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
    engine = Engine((date(2026, 9, 1), 123.5, "YF:AAA:close", pulled))
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
    engine = Engine((date(2026, 9, 1), 100, "YF:AAA:adj_close", datetime(2026, 9, 1, 22, tzinfo=timezone.utc)))
    with pytest.raises(UnsupportedInstrumentError, match="identity"):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))


@pytest.mark.parametrize("value", [0, -1, float("nan"), float("inf")])
def test_nonpositive_or_nonfinite_raw_price_refused(value):
    engine = Engine((date(2026, 9, 1), value, "YF:AAA:close", datetime(2026, 9, 1, 22, tzinfo=timezone.utc)))
    with pytest.raises(UnsupportedInstrumentError, match="positive finite"):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))


def test_naive_pull_timestamp_refused():
    engine = Engine((date(2026, 9, 1), 100, "YF:AAA:close", datetime(2026, 9, 1, 22)))
    with pytest.raises(UnsupportedInstrumentError, match="timestamp"):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))


def test_pre_cutover_row_refused_even_if_fake_db_returns_it():
    engine = Engine((date(2026, 9, 1), 100, "YF:AAA:close", datetime(2026, 7, 31, 22, tzinfo=timezone.utc)))
    with pytest.raises(UnsupportedInstrumentError, match="timestamp"):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA", date(2026, 9, 1))


def test_invalid_ticker_refused_without_query():
    engine = Engine(None)
    with pytest.raises(UnsupportedInstrumentError):
        PITPriceAccessor(engine, verified_raw_close_since=VERIFIED_SINCE)("AAA'; DROP TABLE raw_series", date(2026, 9, 1))
    assert engine.calls == []
