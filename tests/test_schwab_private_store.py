"""Deterministic tests for ingestion/schwab_private_store.py (no network, no DB, no clock).

Negative controls cover loss of provenance (audience, redistribution, missing provider
quote time, live-but-not-fresh, naive timestamps, non-integer sizes, non-finite prices),
display-time ageing (fresh -> stale, never promoted) and integer precision above 2**53.
"""

from __future__ import annotations

import json
import math
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from ingestion.schwab_private_store import (
    DEFAULT_MAX_SYMBOLS,
    MAX_SYMBOLS_LIMIT,
    SCHEMA,
    MaterializedQuote,
    PrivateQuoteStore,
    ProvenanceError,
    materialize,
)
from ingestion.schwab_quote_path import (
    AUDIENCE_PRIVATE,
    MAX_COUNT,
    REDISTRIBUTION,
    SOURCE,
    STATUSES,
    PrivateQuote,
    PrivateQuoteSnapshot,
)
from store.availability import is_unavailable

NOW = datetime(2026, 10, 31, 14, 30, 0, tzinfo=timezone.utc)
LAST, BID, ASK = 731.37, 731.35, 731.39


def row(
    symbol: str = "SPY",
    status: str = "fresh",
    *,
    age: float = 1.0,
    live: bool | None = None,
    last: float | None = LAST,
    bid: float | None = BID,
    ask: float | None = ASK,
    bid_size: int | None = 100,
    ask_size: int | None = 200,
    total_volume: int | None = 12_345,
    realtime: bool | None = True,
    reason: str | None = None,
    provider_symbol: str | None = None,
    received_at: datetime = NOW,
    quote_time: datetime | None | str = "auto",
    max_quote_age_s: float = 20.0,
) -> PrivateQuote:
    measured = status in ("fresh", "stale", "delayed")
    if quote_time == "auto":
        quote_time = received_at - timedelta(seconds=age) if measured else None
    return PrivateQuote(
        symbol=symbol,
        status=status,  # type: ignore[arg-type]
        reason=reason if reason is not None else (None if status == "fresh" else status),
        live=(status == "fresh") if live is None else live,
        last=last if measured else None,
        bid=bid if measured else None,
        ask=ask if measured else None,
        bid_size=bid_size if measured else None,
        ask_size=ask_size if measured else None,
        total_volume=total_volume if measured else None,
        quote_time=quote_time,  # type: ignore[arg-type]
        trade_time=quote_time if measured else None,  # type: ignore[arg-type]
        received_at=received_at,
        age_seconds=age if measured else None,
        realtime=realtime if measured else None,
        asset_main_type="EQUITY" if measured else None,
        max_quote_age_s=max_quote_age_s,
        provider_symbol=provider_symbol if provider_symbol is not None else symbol,
    )


def snap(
    *rows: PrivateQuote,
    received_at: datetime = NOW,
    audience: str = AUDIENCE_PRIVATE,
    redistribution: str = REDISTRIBUTION,
) -> PrivateQuoteSnapshot:
    return PrivateQuoteSnapshot(
        tuple(rows), received_at, tuple(r.symbol for r in rows), None, audience, redistribution
    )


# ── materialize ──────────────────────────────────────────────────────────────

class TestMaterialize:
    def test_fresh_row_carries_identity_as_of_and_vintage(self):
        (m,) = materialize(snap(row(provider_symbol="SPY")))
        assert isinstance(m, MaterializedQuote)
        assert (m.symbol, m.provider_symbol, m.status, m.live) == ("SPY", "SPY", "fresh", True)
        assert m.as_of == NOW - timedelta(seconds=1)
        assert m.vintage == NOW
        assert m.source == SOURCE
        assert (m.last, m.bid, m.ask) == (LAST, BID, ASK)
        assert (m.bid_size, m.ask_size, m.total_volume) == (100, 200, 12_345)

    @pytest.mark.parametrize("status", ["stale", "delayed"])
    def test_measured_non_fresh_rows_are_never_live(self, status):
        (m,) = materialize(snap(row(status=status, age=90.0)))
        assert m.status == status
        assert m.live is False
        assert m.last == LAST

    @pytest.mark.parametrize("status", ["missing", "invalid", "unavailable"])
    def test_unmeasured_rows_have_no_values_and_no_as_of(self, status):
        (m,) = materialize(snap(row(status=status)))
        assert m.status == status
        assert m.live is False
        assert m.as_of is None
        assert (m.last, m.bid, m.ask, m.bid_size, m.ask_size, m.total_volume) == (None,) * 6
        assert m.vintage == NOW

    def test_every_status_is_accepted(self):
        rows = materialize(snap(*(row(symbol=f"S{i}", status=s) for i, s in enumerate(STATUSES))))
        assert [m.status for m in rows] == list(STATUSES)

    def test_empty_snapshot(self):
        assert materialize(snap()) == ()


class TestLossOfProvenance:
    def test_non_snapshot_is_refused(self):
        with pytest.raises(ProvenanceError, match="not a PrivateQuoteSnapshot"):
            materialize({"quotes": []})  # type: ignore[arg-type]

    def test_non_private_audience_is_refused(self):
        with pytest.raises(ProvenanceError, match="audience"):
            materialize(snap(row(), audience="public"))

    def test_redistribution_label_is_refused(self):
        with pytest.raises(ProvenanceError, match="redistribution"):
            materialize(snap(row(), redistribution="allowed"))

    def test_measured_row_without_provider_quote_time_is_refused(self):
        with pytest.raises(ProvenanceError, match="no provider quote time"):
            materialize(snap(row(quote_time=None)))

    def test_live_but_not_fresh_is_refused(self):
        with pytest.raises(ProvenanceError, match="labelled live but not fresh"):
            materialize(snap(row(status="stale", age=60.0, live=True)))

    def test_naive_received_at_is_refused(self):
        with pytest.raises(ProvenanceError, match="timezone-aware"):
            materialize(snap(row(received_at=NOW.replace(tzinfo=None)), received_at=NOW))

    def test_naive_quote_time_is_refused(self):
        with pytest.raises(ProvenanceError, match="timezone-aware"):
            materialize(snap(row(quote_time=NOW.replace(tzinfo=None))))

    @pytest.mark.parametrize("value", [100.0, True, "100"])
    def test_non_integer_sizes_are_refused(self, value):
        with pytest.raises(ProvenanceError, match="not an exact integer"):
            materialize(snap(row(bid_size=value)))  # type: ignore[arg-type]

    @pytest.mark.parametrize("value", [math.nan, math.inf, -math.inf, True, "731.37"])
    def test_non_finite_prices_are_refused(self, value):
        with pytest.raises(ProvenanceError, match="not a finite number"):
            materialize(snap(row(last=value)))  # type: ignore[arg-type]

    def test_unknown_status_is_refused(self):
        with pytest.raises(ProvenanceError, match="unknown quote status"):
            materialize(snap(row(status="live")))

    def test_non_quote_row_is_refused(self):
        bad = PrivateQuoteSnapshot(({"symbol": "SPY"},), NOW, ("SPY",), None)  # type: ignore[arg-type]
        with pytest.raises(ProvenanceError, match="not a PrivateQuote"):
            materialize(bad)


# ── display-time ageing ──────────────────────────────────────────────────────

class TestAgeing:
    def test_fresh_row_becomes_stale_after_gate_and_is_not_live(self):
        (m,) = materialize(snap(row(age=1.0)))
        later = NOW + timedelta(seconds=25)
        aged = m.at(later)
        assert aged.status == "stale"
        assert aged.live is False
        assert aged.reason and "aged out" in aged.reason
        assert aged.last == LAST  # values are kept; only the label and liveness change
        assert aged.as_of == m.as_of and aged.vintage == m.vintage

    def test_fresh_row_within_gate_is_unchanged(self):
        (m,) = materialize(snap(row(age=1.0)))
        assert m.at(NOW + timedelta(seconds=18)) == m

    @pytest.mark.parametrize("status", ["stale", "delayed", "missing", "invalid", "unavailable"])
    def test_rows_are_never_promoted(self, status):
        (m,) = materialize(snap(row(status=status, age=60.0)))
        # evaluated "earlier" than the quote time: still not fresh, still not live
        earlier = m.at(NOW - timedelta(hours=1))
        assert earlier.status == status
        assert earlier.live is False

    def test_at_requires_aware_now(self):
        (m,) = materialize(snap(row()))
        with pytest.raises(ProvenanceError):
            m.at(NOW.replace(tzinfo=None))


# ── payloads ─────────────────────────────────────────────────────────────────

class TestPayload:
    def test_fresh_payload_has_provenance_identity_and_live(self):
        (m,) = materialize(snap(row(provider_symbol="SPY")))
        p = m.to_private_dict(NOW)
        assert p["schema"] == SCHEMA
        assert p["status"] == "ok"  # store.availability STATUS_OK
        assert p["quote_status"] == "fresh" and p["live"] is True
        assert p["provenance"]["source"] == SOURCE
        assert p["provenance"]["as_of"] == (NOW - timedelta(seconds=1)).date().isoformat()
        assert p["provenance"]["vintage"] == NOW.isoformat()
        assert p["freshness"]["as_of"] == (NOW - timedelta(seconds=1)).isoformat()
        assert p["freshness"]["vintage"] == NOW.isoformat()
        assert p["freshness"]["evaluated_at"] == NOW.isoformat()
        assert p["freshness"]["age_seconds"] == 1.0 and p["freshness"]["stale"] is False
        assert (p["symbol"], p["provider_symbol"]) == ("SPY", "SPY")
        assert (p["audience"], p["redistribution"], p["persist_allowed"]) == ("private", "prohibited", False)
        assert (p["last"], p["bid"], p["ask"]) == (LAST, BID, ASK)

    def test_stale_at_display_is_labelled_and_not_live(self):
        (m,) = materialize(snap(row(age=1.0)))
        p = m.to_private_dict(NOW + timedelta(seconds=30))
        assert p["quote_status"] == "stale" and p["status"] == "stale"
        assert p["live"] is False and p["freshness"]["stale"] is True
        assert p["last"] == LAST  # measured, but clearly labelled

    @pytest.mark.parametrize("status", ["missing", "invalid", "unavailable"])
    def test_unmeasured_payload_is_unavailable_with_none_values(self, status):
        (m,) = materialize(snap(row(status=status, reason="why")))
        p = m.to_private_dict(NOW)
        assert is_unavailable(p)
        assert p["reason"] == "why"
        assert p["quote_status"] == status and p["live"] is False
        for key in ("last", "bid", "ask", "bid_size", "ask_size", "total_volume"):
            assert p[key] is None
        assert p["freshness"]["as_of"] is None and p["freshness"]["age_seconds"] is None

    @pytest.mark.parametrize("value", [2**53 + 1, MAX_COUNT, 0])
    def test_integer_precision_survives_json(self, value):
        (m,) = materialize(snap(row(bid_size=value, total_volume=value)))
        text = json.dumps(m.to_private_dict(NOW), allow_nan=False)
        back = json.loads(text)
        assert back["bid_size"] == value and isinstance(back["bid_size"], int)
        assert back["total_volume"] == value and isinstance(back["total_volume"], int)
        assert re.search(rf'"bid_size": {value}\b', text)  # serialised as an exact integer literal


# ── store ────────────────────────────────────────────────────────────────────

class TestStore:
    def test_put_and_view(self):
        store = PrivateQuoteStore()
        assert store.put(snap(row("SPY"), row("QQQ", status="delayed", age=3.0))) == 2
        view = store.view(now=NOW)
        assert view["schema"] == SCHEMA and view["storage"] == "process_local_bounded"
        assert view["counts"]["fresh"] == 1 and view["counts"]["delayed"] == 1
        assert view["rows"]["SPY"]["live"] is True and view["rows"]["QQQ"]["live"] is False
        assert view["reads_materialized"] == 1
        json.dumps(view, allow_nan=False)

    def test_view_ages_rows_at_read_time(self):
        store = PrivateQuoteStore()
        store.put(snap(row("SPY", age=1.0)))
        later = store.view(now=NOW + timedelta(seconds=40))
        assert later["rows"]["SPY"]["quote_status"] == "stale"
        assert later["rows"]["SPY"]["live"] is False
        assert later["counts"] == {"fresh": 0, "stale": 1, "delayed": 0, "missing": 0, "invalid": 0, "unavailable": 0}

    def test_requested_symbol_absent_from_store_is_missing_unavailable(self):
        store = PrivateQuoteStore()
        view = store.view(now=NOW, symbols=["spy "])
        p = view["rows"]["SPY"]
        assert is_unavailable(p) and p["quote_status"] == "missing" and p["live"] is False
        assert view["counts"]["missing"] == 1

    def test_newer_vintage_replaces_older_and_older_is_ignored(self):
        store = PrivateQuoteStore()
        store.put(snap(row("SPY", last=1.0), received_at=NOW))
        newer = NOW + timedelta(seconds=5)
        assert store.put(snap(row("SPY", last=2.0, received_at=newer), received_at=newer)) == 1
        assert store.get("SPY", now=newer).last == 2.0
        older = NOW - timedelta(seconds=5)
        assert store.put(snap(row("SPY", last=0.5, received_at=older), received_at=older)) == 0
        assert store.get("SPY", now=newer).last == 2.0

    def test_bounded_symbols(self):
        store = PrivateQuoteStore(max_symbols=2)
        assert store.put(snap(row("A"), row("B"), row("C"))) == 2
        assert store.symbols() == ("A", "B")
        # an existing symbol can still be refreshed when the store is full
        newer = NOW + timedelta(seconds=1)
        assert store.put(snap(row("A", received_at=newer), received_at=newer)) == 1

    def test_get_normalises_symbol_and_clear(self):
        store = PrivateQuoteStore()
        store.put(snap(row("SPY")))
        assert store.get(" spy", now=NOW) is not None
        assert store.get("QQQ", now=NOW) is None
        store.clear()
        assert store.symbols() == ()

    def test_put_refuses_loss_of_provenance_and_stores_nothing(self):
        store = PrivateQuoteStore()
        with pytest.raises(ProvenanceError):
            store.put(snap(row("SPY"), row("QQQ", quote_time=None)))
        assert store.symbols() == ()

    @pytest.mark.parametrize("value", [True, 1.5, "8"])
    def test_ctor_rejects_non_int(self, value):
        with pytest.raises(TypeError):
            PrivateQuoteStore(max_symbols=value)  # type: ignore[arg-type]

    @pytest.mark.parametrize("value", [0, -1, MAX_SYMBOLS_LIMIT + 1])
    def test_ctor_rejects_out_of_range(self, value):
        with pytest.raises(ValueError):
            PrivateQuoteStore(max_symbols=value)

    def test_defaults(self):
        assert 1 <= DEFAULT_MAX_SYMBOLS <= MAX_SYMBOLS_LIMIT

    def test_view_requires_aware_now(self):
        with pytest.raises(ProvenanceError):
            PrivateQuoteStore().view(now=NOW.replace(tzinfo=None))


# ── source guard ─────────────────────────────────────────────────────────────

def test_module_has_no_io_or_credential_paths():
    src = Path("ingestion/schwab_private_store.py").read_text()
    body = src.split('"""', 2)[2]  # drop the module docstring
    for token in ("requests", "urllib", "socket", "open(", "Path(", "sqlalchemy", "psycopg",
                  "load_credentials", "SchwabMarketClient", "tokens.json", "credentials.env"):
        assert token not in body, token
