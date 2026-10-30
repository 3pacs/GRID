"""Tests for ``ingestion/schwab_quote_path.py`` — private GRID quote path.

Deterministic and offline: the reader is replaced by an in-memory fake that
returns ``SchwabQuote`` rows; no HTTP, no token file, no credentials, no
clock reads (``now`` is always injected).
"""

from __future__ import annotations

import dataclasses
import inspect
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
from loguru import logger

from ingestion import schwab_quote_path as qp
from ingestion.schwab_market import SchwabAuthError, SchwabQuote, SchwabTransientError
from ingestion.schwab_quote_path import (
    AudienceError,
    PrivateQuote,
    PrivateQuoteSnapshot,
    classify_quote,
    read_private_quotes,
)
from store.availability import cacheable, is_unavailable

NOW = datetime(2026, 10, 30, 14, 30, 0, tzinfo=timezone.utc)
NOW_MS = int(NOW.timestamp() * 1000)

# Sentinel values chosen so they cannot appear by accident in any projection.
LAST, BID, ASK = 731.37, 731.35, 731.39
BID_SIZE, ASK_SIZE, VOLUME = 1_234, 5_678, 98_765_432
SENTINELS = ("731.37", "731.35", "731.39", "1234", "5678", "98765432")
SECRET_MARKER = "SECRET-TEXT-must-not-leak-9f3c"


def make_quote(symbol: str = "SPY", *, age_s: float = 3.0, **over: Any) -> SchwabQuote:
    fields: dict[str, Any] = {
        "symbol": symbol,
        "asset_main_type": "EQUITY",
        "realtime": True,
        "last_price": LAST,
        "bid_price": BID,
        "ask_price": ASK,
        "bid_size": BID_SIZE,
        "ask_size": ASK_SIZE,
        "total_volume": VOLUME,
        "quote_time_ms": NOW_MS - int(age_s * 1000),
        "trade_time_ms": NOW_MS - int(age_s * 1000) - 50,
        "raw": {},
    }
    fields.update(over)
    return SchwabQuote(**fields)


class FakeReader:
    """Stands in for SchwabMarketClient; records calls, never touches I/O."""

    def __init__(self, rows: dict[str, SchwabQuote] | None = None, error: Exception | None = None):
        self.rows = rows or {}
        self.error = error
        self.calls: list[tuple[str, ...]] = []

    def quotes(self, symbols, fields="quote"):
        self.calls.append(tuple(symbols))
        if self.error:
            raise self.error
        return dict(self.rows)


@pytest.fixture
def log_lines() -> list[str]:
    lines: list[str] = []
    sink_id = logger.add(lambda m: lines.append(str(m)), level="DEBUG")
    yield lines
    logger.remove(sink_id)


# ---------------------------------------------------------------------------
# classification: happy path and every status


def test_fresh_quote_is_live_with_provenance():
    row = classify_quote("SPY", make_quote(age_s=3.0), now=NOW)
    assert row.status == "fresh" and row.live is True and row.reason is None
    assert row.age_seconds == pytest.approx(3.0)
    payload = row.to_private_dict()
    assert payload["available"] is True and payload["status"] == "ok"
    assert payload["quote_status"] == "fresh" and payload["live"] is True
    assert payload["provenance"]["source"] == qp.SOURCE
    assert payload["provenance"]["as_of"] == "2026-10-30"
    assert payload["provenance"]["vintage"] == NOW.isoformat()
    assert payload["provenance"]["estimated"] is False and payload["provenance"]["basis"] is None
    assert payload["as_of"] == "2026-10-30"
    assert payload["quote_time"] == (NOW - timedelta(seconds=3)).isoformat()
    assert payload["freshness"]["stale"] is False
    assert payload["freshness"]["received_at"] == NOW.isoformat()
    assert (payload["last"], payload["bid"], payload["ask"]) == (LAST, BID, ASK)


def test_as_of_is_quote_time_not_receipt_time():
    quote_day = NOW - timedelta(days=1, seconds=10)
    q = make_quote(quote_time_ms=int(quote_day.timestamp() * 1000))
    payload = classify_quote("SPY", q, now=NOW).to_private_dict()
    assert payload["as_of"] == "2026-10-29"  # the provider's own quote date
    assert payload["freshness"]["received_at"] == NOW.isoformat()
    assert payload["quote_status"] == "stale"


@pytest.mark.parametrize(
    "age_s,expected",
    [(0.0, "fresh"), (19.999, "fresh"), (20.0, "fresh"), (20.001, "stale"), (600.0, "stale")],
)
def test_stale_boundary_is_max_quote_age(age_s: float, expected: str):
    row = classify_quote("SPY", make_quote(age_s=age_s), now=NOW)
    assert row.status == expected
    assert row.live is (expected == "fresh")


def test_stale_quote_keeps_values_but_is_not_live_and_is_flagged():
    row = classify_quote("SPY", make_quote(age_s=45.0), now=NOW)
    payload = row.to_private_dict()
    assert payload["available"] is True and payload["status"] == "stale"
    assert payload["quote_status"] == "stale" and payload["live"] is False
    assert payload["reason"] == "quote older than 20s"
    assert payload["freshness"]["stale"] is True
    assert payload["last"] == LAST  # the observation is still an observation


def test_custom_max_age_is_honoured():
    row = classify_quote("SPY", make_quote(age_s=45.0), now=NOW, max_quote_age_s=60.0)
    assert row.status == "fresh" and row.to_private_dict()["freshness"]["stale_after_seconds"] == 60.0


def test_delayed_entitlement_is_measured_but_never_live():
    row = classify_quote("SPY", make_quote(realtime=False), now=NOW)
    assert row.status == "delayed" and row.live is False
    payload = row.to_private_dict()
    assert payload["available"] is True and payload["status"] == "delayed"
    assert payload["realtime"] is False and payload["last"] == LAST


def test_unknown_realtime_flag_does_not_block_fresh():
    row = classify_quote("SPY", make_quote(realtime=None), now=NOW)
    assert row.status == "fresh" and row.to_private_dict()["realtime"] is None


# ---------------------------------------------------------------------------
# negative controls: missing / invalid / unavailable


@pytest.mark.parametrize(
    "field,reason",
    [
        ("quote_time_ms", "quoteTime missing"),
        ("last_price", "lastPrice missing"),
        ("bid_price", "bidPrice missing"),
        ("ask_price", "askPrice missing"),
    ],
)
def test_missing_required_field_is_unavailable_with_nulls(field: str, reason: str):
    row = classify_quote("SPY", make_quote(**{field: None}), now=NOW)
    assert row.status == "missing" and row.live is False and row.reason == reason
    payload = row.to_private_dict()
    assert is_unavailable(payload) and payload["available"] is False
    assert payload["reason"] == reason and payload["as_of"] is None
    for k in ("last", "bid", "ask", "bid_size", "ask_size", "total_volume", "quote_time", "trade_time"):
        assert payload[k] is None, k
    # Never a zero or a midpoint in place of the missing observation.
    assert 0 not in (payload["last"], payload["bid"], payload["ask"])


def test_symbol_absent_from_response_is_missing():
    row = classify_quote("QQQ", None, now=NOW)
    assert row.status == "missing" and row.reason == "symbol absent from reader response"
    assert is_unavailable(row.to_private_dict())


@pytest.mark.parametrize(
    "over,reason",
    [
        ({"quote_time_ms": NOW_MS + 6_000}, "quoteTime is in the future beyond tolerance"),
        ({"bid_price": 0.0}, "non-positive price"),
        ({"ask_price": -1.0}, "non-positive price"),
        ({"last_price": 0.0}, "non-positive price"),
        ({"bid_price": ASK, "ask_price": BID}, "bid above ask"),
        ({"bid_price": 700.0, "ask_price": 760.0}, "spread wider than max_spread_pct"),
    ],
)
def test_invalid_provider_values_are_unavailable(over: dict[str, Any], reason: str):
    row = classify_quote("SPY", make_quote(**over), now=NOW)
    assert row.status == "invalid" and row.live is False and row.reason == reason
    payload = row.to_private_dict()
    assert is_unavailable(payload) and payload["last"] is None and payload["bid"] is None


def test_future_quote_within_tolerance_is_fresh():
    row = classify_quote("SPY", make_quote(quote_time_ms=NOW_MS + 2_000), now=NOW)
    assert row.status == "fresh" and row.age_seconds == pytest.approx(-2.0)


def test_spread_check_can_be_disabled():
    row = classify_quote(
        "SPY", make_quote(bid_price=700.0, ask_price=760.0), now=NOW, max_spread_pct=None
    )
    assert row.status == "fresh"


def test_unavailable_payload_is_never_cacheable_and_measured_is_cacheable_but_not_persistable():
    bad = classify_quote("SPY", make_quote(bid_price=None), now=NOW).to_private_dict()
    good = classify_quote("SPY", make_quote(), now=NOW).to_private_dict()
    assert cacheable(bad).cache is False
    assert cacheable(good).cache is True  # labelled observation; TTL cache ok
    assert good["persist_allowed"] is False and bad["persist_allowed"] is False
    assert qp.PERSIST_ALLOWED is False


# ---------------------------------------------------------------------------
# read_private_quotes: reader interaction, failures, audience boundary


def test_read_private_quotes_classifies_every_requested_symbol():
    reader = FakeReader({"SPY": make_quote(), "QQQ": make_quote("QQQ", age_s=30.0)})
    snap = read_private_quotes(reader, "spy, qqq, iwm, spy", now=NOW)
    assert reader.calls == [("SPY", "QQQ", "IWM")]  # upper-cased, de-duplicated, ordered
    assert snap.requested == ("SPY", "QQQ", "IWM")
    by = snap.by_symbol()
    assert by["SPY"].status == "fresh" and by["QQQ"].status == "stale" and by["IWM"].status == "missing"
    assert snap.counts() == {
        "fresh": 1, "stale": 1, "delayed": 0, "missing": 1, "invalid": 0, "unavailable": 0
    }
    assert snap.source_error is None
    d = snap.to_private_dict()
    assert d["audience"] == "private" and d["redistribution"] == "prohibited"
    assert set(d["quotes"]) == {"SPY", "QQQ", "IWM"}


def test_empty_symbol_list_never_calls_reader():
    reader = FakeReader()
    snap = read_private_quotes(reader, " , ,", now=NOW)
    assert snap.quotes == () and reader.calls == []


@pytest.mark.parametrize("exc_type", [SchwabAuthError, SchwabTransientError])
def test_reader_failure_yields_unavailable_rows_with_class_name_only(exc_type, log_lines):
    reader = FakeReader(error=exc_type(SECRET_MARKER))
    snap = read_private_quotes(reader, ["SPY", "QQQ"], now=NOW)
    assert snap.source_error == exc_type.__name__
    assert all(q.status == "unavailable" for q in snap.quotes)
    assert all(q.reason == f"reader raised {exc_type.__name__}" for q in snap.quotes)
    blob = json.dumps(snap.to_private_dict()) + json.dumps(snap.status_only()) + "".join(log_lines)
    assert SECRET_MARKER not in blob
    assert any(exc_type.__name__ in line for line in log_lines)


def test_non_schwab_exceptions_propagate():
    reader = FakeReader(error=RuntimeError("programming error"))
    with pytest.raises(RuntimeError):
        read_private_quotes(reader, "SPY", now=NOW)


@pytest.mark.parametrize("audience", ["public", "", "PRIVATE", "gex-public-page"])
def test_non_private_audience_is_refused_before_reader_is_touched(audience: str):
    reader = FakeReader({"SPY": make_quote()})
    with pytest.raises(AudienceError):
        read_private_quotes(reader, "SPY", now=NOW, audience=audience)
    assert reader.calls == []


@pytest.mark.parametrize(
    "bad_now",
    [datetime(2026, 10, 30, 14, 30), "2026-10-30T14:30:00Z", None],  # noqa: DTZ001 - naive on purpose
)
def test_naive_or_non_datetime_now_is_rejected(bad_now):
    with pytest.raises(ValueError):
        classify_quote("SPY", make_quote(), now=bad_now)  # type: ignore[arg-type]
    with pytest.raises(ValueError):
        read_private_quotes(FakeReader(), "SPY", now=bad_now)  # type: ignore[arg-type]


def test_non_utc_aware_now_is_normalised_to_utc():
    est = timezone(timedelta(hours=-4))
    row = classify_quote("SPY", make_quote(), now=NOW.astimezone(est))
    assert row.received_at == NOW and row.received_at.tzinfo == timezone.utc


@pytest.mark.parametrize("kwargs", [{"max_quote_age_s": 0}, {"max_quote_age_s": -1}, {"future_tolerance_s": -1}])
def test_bad_limits_are_rejected(kwargs):
    with pytest.raises(ValueError):
        classify_quote("SPY", make_quote(), now=NOW, **kwargs)


# ---------------------------------------------------------------------------
# private boundary: projections and logs carry no measured values


def _all_projections(snap: PrivateQuoteSnapshot) -> str:
    return json.dumps(snap.status_only(), sort_keys=True) + "".join(
        json.dumps(q.status_only(), sort_keys=True) for q in snap.quotes
    )


def test_status_only_projection_contains_no_measured_values():
    reader = FakeReader({"SPY": make_quote(), "QQQ": make_quote("QQQ", age_s=30.0, realtime=False)})
    snap = read_private_quotes(reader, ["SPY", "QQQ"], now=NOW)
    blob = _all_projections(snap)
    for s in SENTINELS:
        assert s not in blob, s
    assert "quote_time" not in blob and "trade_time" not in blob
    assert "2026-10-30T14:29" not in blob  # no provider timestamp either
    so = snap.status_only()
    assert so["quotes"]["SPY"] == {
        "symbol": "SPY", "source": qp.SOURCE, "quote_status": "fresh", "live": True,
        "age_bucket": "lt_5s", "redistribution": "prohibited",
    }
    assert so["quotes"]["QQQ"]["quote_status"] == "delayed" and so["quotes"]["QQQ"]["age_bucket"] == "lt_60s"
    assert set(so["quotes"]["SPY"]) == {"symbol", "source", "quote_status", "live", "age_bucket", "redistribution"}


@pytest.mark.parametrize(
    "age,bucket",
    [(None, "unknown"), (-1.0, "future"), (0.0, "lt_5s"), (4.99, "lt_5s"), (5.0, "lt_20s"),
     (19.99, "lt_20s"), (20.0, "lt_60s"), (59.99, "lt_60s"), (60.0, "lt_5m"), (299.99, "lt_5m"),
     (300.0, "ge_5m"), (1e9, "ge_5m")],
)
def test_age_bucket_edges(age, bucket):
    assert qp._age_bucket(age) == bucket


def test_logs_never_contain_measured_values_or_symbols_prices(log_lines):
    reader = FakeReader({"SPY": make_quote(), "QQQ": make_quote("QQQ", bid_price=None)})
    read_private_quotes(reader, ["SPY", "QQQ"], now=NOW)
    joined = "".join(log_lines)
    assert "Schwab private quotes" in joined
    for s in SENTINELS:
        assert s not in joined, s


def test_every_payload_carries_private_boundary_markers():
    rows = [
        classify_quote("SPY", make_quote(), now=NOW),
        classify_quote("SPY", make_quote(age_s=99), now=NOW),
        classify_quote("SPY", make_quote(realtime=False), now=NOW),
        classify_quote("SPY", make_quote(bid_price=None), now=NOW),
        classify_quote("SPY", make_quote(bid_price=ASK, ask_price=BID), now=NOW),
        classify_quote("SPY", None, now=NOW),
    ]
    assert {r.status for r in rows} == {"fresh", "stale", "delayed", "missing", "invalid"}
    for r in rows:
        p = r.to_private_dict()
        assert p["audience"] == "private" and p["redistribution"] == "prohibited"
        assert p["persist_allowed"] is False and p["quote_status"] == r.status
        assert p["live"] is (r.status == "fresh")
        assert r.status_only()["redistribution"] == "prohibited"


def test_payloads_are_deterministic_and_rows_frozen():
    q = make_quote()
    a = classify_quote("SPY", q, now=NOW).to_private_dict()
    b = classify_quote("SPY", q, now=NOW).to_private_dict()
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
    row = classify_quote("SPY", q, now=NOW)
    with pytest.raises(dataclasses.FrozenInstanceError):
        row.last = 1.0  # type: ignore[misc]
    assert isinstance(row, PrivateQuote)


# ---------------------------------------------------------------------------
# source guards: no I/O, no credentials, no network, no persistence paths


def _code_without_module_docstring() -> str:
    src = inspect.getsource(qp)
    _, _, body = src.partition('"""')
    _, _, body = body.partition('"""')
    return body


def test_module_has_no_io_network_or_credential_paths():
    src = _code_without_module_docstring()
    forbidden = (
        "import requests", "urllib", "socket", "load_credentials", "SchwabMarketClient(",
        "TOKEN_URL", "MARKET_BASE", "open(", "Path(", "raw_series", "sqlalchemy", "get_engine",
        "datetime.now(", "time.time(", "os.environ", "settings.",
    )
    for needle in forbidden:
        assert needle not in src, needle


def test_module_imports_only_the_reviewed_reader_types():
    src = inspect.getsource(qp)
    imports = re.findall(r"^from ingestion\.schwab_market import (.+)$", src, re.MULTILINE)
    assert imports == ["SchwabError, SchwabQuote"]


def test_public_surface_is_exactly_the_documented_one():
    assert set(qp.__all__) == {
        "AUDIENCE_PRIVATE", "DEFAULT_FUTURE_TOLERANCE_S", "DEFAULT_MAX_QUOTE_AGE_S",
        "DEFAULT_MAX_SPREAD_PCT", "MEASURED_STATUSES", "PERSIST_ALLOWED", "PROVIDER",
        "REDISTRIBUTION", "SOURCE", "STATUSES", "AudienceError", "PrivateQuote",
        "PrivateQuoteSnapshot", "QuoteSource", "classify_quote", "read_private_quotes",
    }
    assert qp.DEFAULT_MAX_QUOTE_AGE_S == 20.0  # matches gex-watch's RTD callback-age gate
    assert qp.REDISTRIBUTION == "prohibited" and qp.AUDIENCE_PRIVATE == "private"
