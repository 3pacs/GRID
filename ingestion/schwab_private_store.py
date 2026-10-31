"""Private in-process materialization of Schwab quote snapshots.

Newly wired behaviour (2026-10-31), layered on the reviewed modules:

* ``ingestion/schwab_market.py`` (PR #931, 29f636a2 + r4 boundary) — the reader.
* ``ingestion/schwab_quote_path.py`` (r4 b7af6579) — classification, provenance and
  the availability-contract payloads.

This module is the *materialization* step between the quote path and any private
display: it keeps, per symbol, the latest classified row together with its identity
(requested symbol and the provider's own symbol), its as-of time (the provider's
``quoteTime``), its vintage (GRID receipt time) and its source, and it re-evaluates
freshness at **read** time so a row that was ``fresh`` when received is reported
``stale`` once it has aged past the gate. A stored row is never promoted: a
``delayed``/``stale``/``missing``/``invalid``/``unavailable`` row stays what it was.

Storage is process-local and bounded (no database, no file, no network). Schwab
data may not be redistributed and the quote path labels every row
``persist_allowed=False``; durable persistence (``raw_series`` or a dedicated table)
is a separate gate that needs Anik's sign-off and is deliberately not provided
here. Each API worker therefore holds its own view; the view is a cache of the
last private read, not a record.

Fail-closed: :func:`materialize` refuses anything that is not a
:class:`PrivateQuoteSnapshot` from the quote path, a snapshot whose declared
audience is not ``"private"`` or whose redistribution label is not
``"prohibited"``, and a measured row that lacks a provider quote time (loss of
provenance). Integer fields (sizes, volume) are carried as Python ``int`` and are
never passed through ``float``, so values above 2**53 keep their precision in JSON;
the view also carries ``*_exact`` decimal strings for JavaScript consumers (r2).
"""

from __future__ import annotations

import json
import math
import threading
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Any

from config import log
from ingestion.schwab_quote_path import (
    AUDIENCE_PRIVATE,
    MEASURED_STATUSES,
    PERSIST_ALLOWED,
    PROVIDER,
    REDISTRIBUTION,
    SOURCE,
    STATUSES,
    PrivateQuote,
    PrivateQuoteSnapshot,
)
from store.availability import Provenance, available, unavailable

SCHEMA = "schwab-private-quote-view-v1"
DEFAULT_MAX_SYMBOLS = 64
MAX_SYMBOLS_LIMIT = 4096


class ProvenanceError(ValueError):
    """Raised when a snapshot or row cannot be materialized without losing provenance."""


@dataclass(frozen=True)
class MaterializedQuote:
    """One stored row: identity, as-of/vintage, status and the measured values."""

    symbol: str
    provider_symbol: str | None
    status: str
    reason: str | None
    live: bool
    last: float | None
    bid: float | None
    ask: float | None
    bid_size: int | None
    ask_size: int | None
    total_volume: int | None
    as_of: datetime | None  # provider quoteTime (exchange time), never receipt time
    trade_time: datetime | None
    vintage: datetime  # GRID receipt time of the read that produced the row
    realtime: bool | None
    asset_main_type: str | None
    max_quote_age_s: float
    source: str = SOURCE
    provider: str = PROVIDER

    @property
    def measured(self) -> bool:
        return self.status in MEASURED_STATUSES

    def age_seconds(self, now: datetime) -> float | None:
        if self.as_of is None:
            return None
        return (now - self.as_of).total_seconds()

    def at(self, now: datetime) -> MaterializedQuote:
        """The row as it should be displayed at ``now``.

        Only one transition exists: ``fresh`` -> ``stale`` once the provider
        quote time is older than ``max_quote_age_s``. Nothing is ever promoted.
        """
        now = _require_aware(now, "now")
        if self.status != "fresh":
            return self
        age = self.age_seconds(now)
        if age is not None and age > self.max_quote_age_s:
            return replace(
                self,
                status="stale",
                live=False,
                reason=f"aged out in store: {age:.1f}s > {self.max_quote_age_s:.0f}s",
            )
        return self

    def to_private_dict(self, now: datetime) -> dict[str, Any]:
        """Availability-contract payload for the private view at ``now``."""
        row = self.at(now)
        age = row.age_seconds(now)
        common = {
            "schema": SCHEMA,
            "symbol": row.symbol,
            "provider_symbol": row.provider_symbol,
            "audience": AUDIENCE_PRIVATE,
            "redistribution": REDISTRIBUTION,
            "persist_allowed": PERSIST_ALLOWED,
            "quote_status": row.status,
            "live": row.live,
            "freshness": {
                "as_of": row.as_of.isoformat() if row.as_of else None,
                "vintage": row.vintage.isoformat(),
                "evaluated_at": now.isoformat(),
                "age_seconds": age,
                "stale": None if age is None else age > row.max_quote_age_s,
                "stale_after_seconds": row.max_quote_age_s,
            },
        }
        if not row.measured:
            payload = unavailable(
                row.reason or row.status,
                source=row.source,
                last=None,
                bid=None,
                ask=None,
                bid_size=None,
                ask_size=None,
                total_volume=None,
                trade_time=None,
                realtime=None,
                asset_main_type=None,
            )
            payload.update(common)
            payload.update(_exact_strings(None, None, None))
            return payload
        provenance = Provenance(
            source=row.source,
            as_of=row.as_of.date() if row.as_of else None,
            vintage=row.vintage,
            basis=None,
            estimated=False,
        )
        payload = available(
            provenance=provenance,
            last=row.last,
            bid=row.bid,
            ask=row.ask,
            bid_size=row.bid_size,
            ask_size=row.ask_size,
            total_volume=row.total_volume,
            trade_time=row.trade_time.isoformat() if row.trade_time else None,
            realtime=row.realtime,
            asset_main_type=row.asset_main_type,
            provider=row.provider,
        )
        if row.status != "fresh":
            payload["status"] = row.status
            payload["reason"] = row.reason
        payload.update(common)
        payload.update(_exact_strings(row.bid_size, row.ask_size, row.total_volume))
        return payload


def _exact_strings(bid_size: int | None, ask_size: int | None, total_volume: int | None) -> dict[str, str | None]:
    """Decimal-string twins of the integer fields for JSON clients whose numbers lose
    precision above 2**53 (JavaScript). ``None`` stays ``None``; never ``"0"`` for missing."""
    return {
        "bid_size_exact": None if bid_size is None else str(bid_size),
        "ask_size_exact": None if ask_size is None else str(ask_size),
        "total_volume_exact": None if total_volume is None else str(total_volume),
    }


def materialize(snapshot: PrivateQuoteSnapshot) -> tuple[MaterializedQuote, ...]:
    """Turn a quote-path snapshot into stored rows, or raise :class:`ProvenanceError`."""
    if not isinstance(snapshot, PrivateQuoteSnapshot):
        raise ProvenanceError(f"not a PrivateQuoteSnapshot: {type(snapshot).__name__}")
    if snapshot.audience != AUDIENCE_PRIVATE:
        raise ProvenanceError("snapshot audience is not private")
    if snapshot.redistribution != REDISTRIBUTION:
        raise ProvenanceError("snapshot redistribution label is not 'prohibited'")
    _require_aware(snapshot.received_at, "snapshot.received_at")
    rows = []
    for quote in snapshot.quotes:
        rows.append(_materialize_row(quote))
    return tuple(rows)


def _materialize_row(quote: PrivateQuote) -> MaterializedQuote:
    if not isinstance(quote, PrivateQuote):
        raise ProvenanceError(f"not a PrivateQuote: {type(quote).__name__}")
    if quote.status not in STATUSES:
        raise ProvenanceError(f"unknown quote status {quote.status!r}")
    vintage = _require_aware(quote.received_at, "received_at")
    measured = quote.status in MEASURED_STATUSES
    if measured and quote.quote_time is None:
        raise ProvenanceError(f"measured row for {quote.symbol} has no provider quote time")
    if measured and quote.live and quote.status != "fresh":
        raise ProvenanceError(f"row for {quote.symbol} is labelled live but not fresh")
    as_of = _require_aware(quote.quote_time, "quote_time") if quote.quote_time else None
    trade_time = _require_aware(quote.trade_time, "trade_time") if quote.trade_time else None
    for name in ("bid_size", "ask_size", "total_volume"):
        value = getattr(quote, name)
        if value is not None and (isinstance(value, bool) or not isinstance(value, int)):
            raise ProvenanceError(f"{name} for {quote.symbol} is not an exact integer")
    for name in ("last", "bid", "ask"):
        value = getattr(quote, name)
        if value is not None and (
            isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value)
        ):
            raise ProvenanceError(f"{name} for {quote.symbol} is not a finite number")
    return MaterializedQuote(
        symbol=quote.symbol,
        provider_symbol=quote.provider_symbol,
        status=quote.status,
        reason=quote.reason,
        live=bool(quote.live) and quote.status == "fresh",
        last=quote.last if measured else None,
        bid=quote.bid if measured else None,
        ask=quote.ask if measured else None,
        bid_size=quote.bid_size if measured else None,
        ask_size=quote.ask_size if measured else None,
        total_volume=quote.total_volume if measured else None,
        as_of=as_of if measured else None,
        trade_time=trade_time if measured else None,
        vintage=vintage,
        realtime=quote.realtime if measured else None,
        asset_main_type=quote.asset_main_type if measured else None,
        max_quote_age_s=float(quote.max_quote_age_s),
    )


class PrivateQuoteStore:
    """Bounded, thread-safe, process-local store of the latest row per symbol."""

    def __init__(self, max_symbols: int = DEFAULT_MAX_SYMBOLS) -> None:
        if not isinstance(max_symbols, int) or isinstance(max_symbols, bool):
            raise TypeError("max_symbols must be an int")
        if not 1 <= max_symbols <= MAX_SYMBOLS_LIMIT:
            raise ValueError(f"max_symbols must be in 1..{MAX_SYMBOLS_LIMIT}")
        self._max = max_symbols
        self._rows: dict[str, MaterializedQuote] = {}
        self._lock = threading.Lock()
        self._puts = 0

    def put(self, snapshot: PrivateQuoteSnapshot) -> int:
        """Materialize ``snapshot`` and keep each row that is at least as new as the
        stored one (by vintage). Returns the number of rows stored."""
        rows = materialize(snapshot)
        stored = 0
        with self._lock:
            for row in rows:
                current = self._rows.get(row.symbol)
                if current is not None and current.vintage > row.vintage:
                    continue
                if current is None and len(self._rows) >= self._max:
                    log.warning(
                        "Schwab private store full ({n} symbols); dropping {s}",
                        n=self._max,
                        s=row.symbol,
                    )
                    continue
                self._rows[row.symbol] = row
                stored += 1
            self._puts += 1
        return stored

    def get(self, symbol: str, *, now: datetime) -> MaterializedQuote | None:
        now = _require_aware(now, "now")
        with self._lock:
            row = self._rows.get(symbol.strip().upper())
        return row.at(now) if row is not None else None

    def symbols(self) -> tuple[str, ...]:
        with self._lock:
            return tuple(sorted(self._rows))

    def clear(self) -> None:
        with self._lock:
            self._rows.clear()

    def view(self, *, now: datetime, symbols: Iterable[str] | None = None) -> dict[str, Any]:
        """Private availability-contract view evaluated at ``now`` (strict JSON)."""
        now = _require_aware(now, "now")
        with self._lock:
            rows = dict(self._rows)
            puts = self._puts
        wanted = (
            sorted(rows) if symbols is None else [s.strip().upper() for s in symbols if s.strip()]
        )
        out_rows: dict[str, Any] = {}
        counts = dict.fromkeys(STATUSES, 0)
        for sym in wanted:
            row = rows.get(sym)
            if row is None:
                payload = unavailable("not in private store", source=SOURCE)
                payload.update(
                    {
                        "schema": SCHEMA,
                        "symbol": sym,
                        "provider_symbol": None,
                        "audience": AUDIENCE_PRIVATE,
                        "redistribution": REDISTRIBUTION,
                        "persist_allowed": PERSIST_ALLOWED,
                        "quote_status": "missing",
                        "live": False,
                    }
                )
                counts["missing"] += 1
            else:
                payload = row.to_private_dict(now)
                counts[payload["quote_status"]] += 1
            out_rows[sym] = payload
        view = {
            "schema": SCHEMA,
            "source": SOURCE,
            "provider": PROVIDER,
            "audience": AUDIENCE_PRIVATE,
            "redistribution": REDISTRIBUTION,
            "persist_allowed": PERSIST_ALLOWED,
            "storage": "process_local_bounded",
            "evaluated_at": now.isoformat(),
            "reads_materialized": puts,
            "counts": counts,
            "rows": out_rows,
        }
        json.dumps(view, allow_nan=False)  # strict: raises on NaN/inf before anything is served
        return view


def _require_aware(value: datetime, name: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise ProvenanceError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


__all__ = [
    "DEFAULT_MAX_SYMBOLS",
    "MAX_SYMBOLS_LIMIT",
    "SCHEMA",
    "MaterializedQuote",
    "PrivateQuoteStore",
    "ProvenanceError",
    "materialize",
]
