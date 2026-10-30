"""Private GRID quote path over the reviewed Schwab reader.

This module turns the raw ``SchwabQuote`` rows produced by
``ingestion/schwab_market.py`` (reviewed at 29f636a2) into availability-contract
payloads (``docs/reference/AVAILABILITY_CONTRACT.md``) that a *private* GRID
consumer can rely on. It adds what the reader deliberately leaves out:

* **Provenance** — ``source`` / ``as_of`` / ``vintage`` on every row, where
  ``as_of`` is the provider's own ``quoteTime`` and ``vintage`` is GRID's
  receipt time. Response time is never written as data time.
* **Freshness** — age of the quote against ``max_quote_age_s`` (default 20 s,
  the same callback-age rule gex-watch's broker applies to the RTD leg).
* **Explicit stale / missing / invalid handling** — a row is exactly one of
  ``fresh``, ``stale``, ``delayed``, ``missing``, ``invalid`` or
  ``unavailable``; only ``fresh`` is ``live``. Missing numbers are ``None``,
  never ``0`` or a midpoint.
* **Fail-closed inputs** — a row is ``live`` only when the provider's
  ``realtime`` flag is exactly ``True``; a missing flag is ``missing`` and a
  non-boolean flag is ``invalid``. Non-finite or non-numeric prices, and
  quote timestamps that are malformed or out of range, are ``invalid``; a
  malformed optional field (sizes, volume, trade time) is dropped to ``None``.
  No single bad row aborts the snapshot. Every payload serialises under
  strict JSON (``allow_nan=False``).
* **Declared audience label, not access control** — ``audience`` is a
  caller-supplied declaration (default ``"private"``) that the read is for
  private GRID use; ``read_private_quotes`` refuses any other value as a
  declared-intent check. It is not authentication and does not gate any
  endpoint. Every payload carries ``redistribution="prohibited"`` and
  ``persist_allowed=False`` as labels. A real authorized private endpoint is
  a separate integration gate that this module does not provide; no endpoint
  is wired. ``status_only()`` is the projection intended for non-private
  consumers (statuses and age buckets, no measured values); whether it may
  reach any surface is that surface owner's gate. Schwab's terms forbid
  redistributing its data; nothing here may feed a public page.

Boundaries this module keeps on purpose:

* No network, no credentials, no token file: the Schwab client (or any object
  with a ``quotes(symbols)`` method) is injected by the caller.
* No persistence: ``persist_allowed`` is always ``False``. GRID stores no
  Schwab quote in ``raw_series`` or any snapshot table from this path.
* No scheduling, no Hermes/GEX wiring, no endpoint, no public projection.
* Exception text from the reader is never echoed; only the class name is.
"""

from __future__ import annotations

import json
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, Protocol

from config import log
from ingestion.schwab_market import SchwabError, SchwabQuote
from store.availability import Provenance, available, unavailable

SOURCE = "schwab-marketdata-v1"
PROVIDER = "Schwab Trader API, Market Data Production, scope readonly"
AUDIENCE_PRIVATE = "private"
REDISTRIBUTION = "prohibited"
PERSIST_ALLOWED = False

DEFAULT_MAX_QUOTE_AGE_S = 20.0
DEFAULT_FUTURE_TOLERANCE_S = 5.0
DEFAULT_MAX_SPREAD_PCT = 1.0
MAX_EPOCH_MS = 4_102_444_800_000  # 2100-01-01T00:00:00Z; later timestamps are malformed

QuoteStatus = Literal["fresh", "stale", "delayed", "missing", "invalid", "unavailable"]
STATUSES: tuple[str, ...] = ("fresh", "stale", "delayed", "missing", "invalid", "unavailable")
MEASURED_STATUSES: frozenset[str] = frozenset({"fresh", "stale", "delayed"})
AGE_BUCKETS: tuple[tuple[float, str], ...] = (
    (5.0, "lt_5s"),
    (20.0, "lt_20s"),
    (60.0, "lt_60s"),
    (300.0, "lt_5m"),
)


class AudienceError(PermissionError):
    """Raised when the caller declares a non-private audience.

    A declared-intent check only: ``audience`` is caller-controlled and is not
    authentication or access control.
    """


class QuoteSource(Protocol):
    """The slice of ``SchwabMarketClient`` this path uses (injectable)."""

    def quotes(
        self, symbols: str | Iterable[str], fields: str | None = "quote"
    ) -> Mapping[str, SchwabQuote]: ...


@dataclass(frozen=True)
class PrivateQuote:
    """One symbol's quote with provenance, freshness and an explicit status."""

    symbol: str
    status: QuoteStatus
    reason: str | None
    live: bool
    last: float | None
    bid: float | None
    ask: float | None
    bid_size: int | None
    ask_size: int | None
    total_volume: int | None
    quote_time: datetime | None
    trade_time: datetime | None
    received_at: datetime
    age_seconds: float | None
    realtime: bool | None
    asset_main_type: str | None
    max_quote_age_s: float

    @property
    def measured(self) -> bool:
        """True when the row carries measured numbers (fresh, stale or delayed)."""
        return self.status in MEASURED_STATUSES

    def to_private_dict(self) -> dict[str, Any]:
        """Availability-contract payload for private GRID consumers."""
        common = {
            "symbol": self.symbol,
            "audience": AUDIENCE_PRIVATE,
            "redistribution": REDISTRIBUTION,
            "persist_allowed": PERSIST_ALLOWED,
            "quote_status": self.status,
            "freshness": self._freshness(),
        }
        if not self.measured:
            payload = unavailable(
                self.reason or self.status,
                source=SOURCE,
                last=None,
                bid=None,
                ask=None,
                bid_size=None,
                ask_size=None,
                total_volume=None,
                quote_time=None,
                trade_time=None,
                realtime=None,
            )
            payload["live"] = False
            payload.update(common)
            return payload
        provenance = Provenance(
            source=SOURCE,
            as_of=self.quote_time.date() if self.quote_time else None,
            vintage=self.received_at,
            basis=None,
            estimated=False,
        )
        payload = available(
            provenance=provenance,
            last=self.last,
            bid=self.bid,
            ask=self.ask,
            bid_size=self.bid_size,
            ask_size=self.ask_size,
            total_volume=self.total_volume,
            quote_time=self.quote_time.isoformat() if self.quote_time else None,
            trade_time=self.trade_time.isoformat() if self.trade_time else None,
            realtime=self.realtime,
            asset_main_type=self.asset_main_type,
            provider=PROVIDER,
        )
        payload["live"] = self.live
        if self.status != "fresh":
            payload["status"] = self.status
            payload["reason"] = self.reason
        payload.update(common)
        return payload

    def status_only(self) -> dict[str, Any]:
        """Projection intended for non-private consumers.

        Carries status, liveness and an age bucket. No price, size, volume or
        timestamp from the provider is included. Whether it may reach any
        surface is that surface owner's gate, not this module's.
        """
        return {
            "symbol": self.symbol,
            "source": SOURCE,
            "quote_status": self.status,
            "live": self.live,
            "age_bucket": _age_bucket(self.age_seconds),
            "redistribution": REDISTRIBUTION,
        }

    def _freshness(self) -> dict[str, Any]:
        return {
            "as_of": self.quote_time.isoformat() if self.quote_time else None,
            "received_at": self.received_at.isoformat(),
            "age_seconds": self.age_seconds,
            "stale": None if self.age_seconds is None else self.age_seconds > self.max_quote_age_s,
            "stale_after_seconds": self.max_quote_age_s,
        }


@dataclass(frozen=True)
class PrivateQuoteSnapshot:
    """All requested symbols, each with a row, plus the read's own provenance."""

    quotes: tuple[PrivateQuote, ...]
    received_at: datetime
    requested: tuple[str, ...]
    source_error: str | None  # exception class name when the read itself failed
    audience: str = AUDIENCE_PRIVATE
    redistribution: str = REDISTRIBUTION

    def by_symbol(self) -> dict[str, PrivateQuote]:
        return {q.symbol: q for q in self.quotes}

    def counts(self) -> dict[str, int]:
        out = dict.fromkeys(STATUSES, 0)
        for q in self.quotes:
            out[q.status] += 1
        return out

    def to_private_dict(self) -> dict[str, Any]:
        return {
            "source": SOURCE,
            "provider": PROVIDER,
            "audience": self.audience,
            "redistribution": self.redistribution,
            "persist_allowed": PERSIST_ALLOWED,
            "received_at": self.received_at.isoformat(),
            "requested": list(self.requested),
            "source_error": self.source_error,
            "counts": self.counts(),
            "quotes": {q.symbol: q.to_private_dict() for q in self.quotes},
        }

    def status_only(self) -> dict[str, Any]:
        return {
            "source": SOURCE,
            "received_at": self.received_at.isoformat(),
            "source_error": self.source_error,
            "counts": self.counts(),
            "quotes": {q.symbol: q.status_only() for q in self.quotes},
        }


def read_private_quotes(
    client: QuoteSource,
    symbols: str | Iterable[str],
    *,
    now: datetime,
    audience: str = AUDIENCE_PRIVATE,
    max_quote_age_s: float = DEFAULT_MAX_QUOTE_AGE_S,
    future_tolerance_s: float = DEFAULT_FUTURE_TOLERANCE_S,
    max_spread_pct: float | None = DEFAULT_MAX_SPREAD_PCT,
) -> PrivateQuoteSnapshot:
    """Read quotes through an injected reader and classify every requested symbol.

    ``now`` must be timezone-aware; it is the receipt clock used for ages and
    the ``vintage`` of every row. ``audience`` is a caller declaration, not
    access control: any value other than ``"private"`` raises
    :class:`AudienceError` before the reader is touched. A row whose
    classification fails on malformed provider data becomes ``invalid``
    (class name only); the snapshot itself never aborts on one row.
    """
    _require_private(audience)
    now = _require_aware(now, "now")
    _check_limits(max_quote_age_s, future_tolerance_s, max_spread_pct)
    requested = _normalise_symbols(symbols)
    if not requested:
        return PrivateQuoteSnapshot((), now, (), None)
    try:
        rows = client.quotes(requested)
    except SchwabError as exc:
        name = type(exc).__name__
        log.warning(
            "Schwab private quote read failed: {n} for {k} symbols", n=name, k=len(requested)
        )
        quotes = tuple(
            _unavailable_row(sym, now, f"reader raised {name}", max_quote_age_s)
            for sym in requested
        )
        return PrivateQuoteSnapshot(quotes, now, requested, name)
    classified: list[PrivateQuote] = []
    for sym in requested:
        try:
            classified.append(
                classify_quote(
                    sym,
                    rows.get(sym) if isinstance(rows, Mapping) else None,
                    now=now,
                    max_quote_age_s=max_quote_age_s,
                    future_tolerance_s=future_tolerance_s,
                    max_spread_pct=max_spread_pct,
                )
            )
        except (ValueError, TypeError, OverflowError, OSError, ArithmeticError) as exc:
            name = type(exc).__name__
            log.warning("Schwab quote classification failed for one symbol: {n}", n=name)
            classified.append(
                _unavailable_row(
                    sym, now, f"classification failed: {name}", max_quote_age_s, status="invalid"
                )
            )
    quotes = tuple(classified)
    snap = PrivateQuoteSnapshot(quotes, now, requested, None)
    log.info("Schwab private quotes: {c}", c=json.dumps(snap.counts(), sort_keys=True))
    return snap


def classify_quote(
    symbol: str,
    quote: SchwabQuote | None,
    *,
    now: datetime,
    max_quote_age_s: float = DEFAULT_MAX_QUOTE_AGE_S,
    future_tolerance_s: float = DEFAULT_FUTURE_TOLERANCE_S,
    max_spread_pct: float | None = DEFAULT_MAX_SPREAD_PCT,
) -> PrivateQuote:
    """Pure classification of one reader row. Deterministic for fixed ``now``."""
    now = _require_aware(now, "now")
    _check_limits(max_quote_age_s, future_tolerance_s, max_spread_pct)
    if quote is None:
        return _unavailable_row(
            symbol, now, "symbol absent from reader response", max_quote_age_s, status="missing"
        )

    quote_time = _ms_to_dt(quote.quote_time_ms)
    trade_time = _ms_to_dt(quote.trade_time_ms)  # optional: malformed -> None
    age: float | None = None
    if quote_time is not None:
        try:
            age = (now - quote_time).total_seconds()
        except (OverflowError, ArithmeticError):
            quote_time, age = None, None
    if age is not None and not math.isfinite(age):
        quote_time, age = None, None
    bid_size = _finite_int(quote.bid_size)
    ask_size = _finite_int(quote.ask_size)
    total_volume = _finite_int(quote.total_volume)

    def row(status: QuoteStatus, reason: str | None, live: bool) -> PrivateQuote:
        measured = status in MEASURED_STATUSES
        return PrivateQuote(
            symbol=symbol,
            status=status,
            reason=reason,
            live=live,
            last=quote.last_price if measured else None,
            bid=quote.bid_price if measured else None,
            ask=quote.ask_price if measured else None,
            bid_size=bid_size if measured else None,
            ask_size=ask_size if measured else None,
            total_volume=total_volume if measured else None,
            quote_time=quote_time if measured else None,
            trade_time=trade_time if measured else None,
            received_at=now,
            age_seconds=age,
            realtime=quote.realtime if isinstance(quote.realtime, bool) else None,
            asset_main_type=quote.asset_main_type,
            max_quote_age_s=max_quote_age_s,
        )

    # Missing: any required field absent. Never substitute 0 or a midpoint.
    required = (
        ("quoteTime", quote.quote_time_ms),
        ("lastPrice", quote.last_price),
        ("bidPrice", quote.bid_price),
        ("askPrice", quote.ask_price),
        ("realtime", quote.realtime),
    )
    for name, value in required:
        if value is None:
            return row("missing", f"{name} missing", False)

    # Invalid: provider values that cannot be a quote.
    if quote_time is None or age is None:
        return row("invalid", "quoteTime malformed or out of range", False)
    for name, value in (("lastPrice", quote.last_price), ("bidPrice", quote.bid_price),
                        ("askPrice", quote.ask_price)):
        if not _is_finite_number(value):
            return row("invalid", f"{name} non-finite or non-numeric", False)
    if not isinstance(quote.realtime, bool):
        return row("invalid", "realtime flag not boolean", False)
    if age < -future_tolerance_s:
        return row("invalid", "quoteTime is in the future beyond tolerance", False)
    bid, ask, last = float(quote.bid_price), float(quote.ask_price), float(quote.last_price)
    if bid <= 0 or ask <= 0 or last <= 0:
        return row("invalid", "non-positive price", False)
    if bid > ask:
        return row("invalid", "bid above ask", False)
    if max_spread_pct is not None:
        mid = (bid + ask) / 2.0
        if (ask - bid) / mid * 100.0 > max_spread_pct:
            return row("invalid", "spread wider than max_spread_pct", False)

    # Delayed: provider says the entitlement is not real-time. Measured but never live.
    if quote.realtime is False:
        return row("delayed", "provider marked quote as not realtime", False)

    # Stale vs fresh by the provider's own quote time. Only realtime=True reaches here.
    if age > max_quote_age_s:
        return row("stale", f"quote older than {max_quote_age_s:g}s", False)
    return row("fresh", None, True)


# ---------------------------------------------------------------------------
# helpers


def _require_private(audience: str) -> None:
    if audience != AUDIENCE_PRIVATE:
        raise AudienceError(
            "Schwab-sourced quotes are available to the private GRID audience only"
        )


def _require_aware(value: datetime, name: str) -> datetime:
    if (
        not isinstance(value, datetime)
        or value.tzinfo is None
        or value.utcoffset() is None
    ):
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def _check_limits(
    max_quote_age_s: float, future_tolerance_s: float, max_spread_pct: float | None = None
) -> None:
    if not _is_finite_number(max_quote_age_s) or max_quote_age_s <= 0:
        raise ValueError("max_quote_age_s must be a finite number > 0")
    if not _is_finite_number(future_tolerance_s) or future_tolerance_s < 0:
        raise ValueError("future_tolerance_s must be a finite number >= 0")
    if max_spread_pct is not None and (not _is_finite_number(max_spread_pct) or max_spread_pct < 0):
        raise ValueError("max_spread_pct must be None or a finite number >= 0")


def _is_finite_number(value: Any) -> bool:
    """True for int/float (not bool) that is finite."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return False
    try:
        return math.isfinite(value)
    except (TypeError, OverflowError):
        return False


def _finite_int(value: Any) -> int | None:
    """Optional count fields: int when finite and numeric, else None (never a guess)."""
    if not _is_finite_number(value):
        return None
    return int(value)


def _normalise_symbols(symbols: str | Iterable[str]) -> tuple[str, ...]:
    if isinstance(symbols, str):
        parts = [s.strip().upper() for s in symbols.split(",")]
    else:
        parts = [str(s).strip().upper() for s in symbols]
    seen: dict[str, None] = {}
    for p in parts:
        if p:
            seen.setdefault(p, None)
    return tuple(seen)


def _ms_to_dt(ms: Any) -> datetime | None:
    """Epoch milliseconds -> aware UTC datetime; ``None`` for anything malformed.

    Rejects bools, non-numbers, non-finite values, negatives and anything past
    ``MAX_EPOCH_MS``. Never raises.
    """
    if not _is_finite_number(ms) or ms < 0 or ms > MAX_EPOCH_MS:
        return None
    try:
        return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None


def _age_bucket(age: float | None) -> str:
    if age is None:
        return "unknown"
    if age < 0:
        return "future"
    for limit, label in AGE_BUCKETS:
        if age < limit:
            return label
    return "ge_5m"


def _unavailable_row(
    symbol: str,
    now: datetime,
    reason: str,
    max_quote_age_s: float,
    status: QuoteStatus = "unavailable",
) -> PrivateQuote:
    return PrivateQuote(
        symbol=symbol,
        status=status,
        reason=reason,
        live=False,
        last=None,
        bid=None,
        ask=None,
        bid_size=None,
        ask_size=None,
        total_volume=None,
        quote_time=None,
        trade_time=None,
        received_at=now,
        age_seconds=None,
        realtime=None,
        asset_main_type=None,
        max_quote_age_s=max_quote_age_s,
    )


__all__ = [
    "AUDIENCE_PRIVATE",
    "DEFAULT_FUTURE_TOLERANCE_S",
    "DEFAULT_MAX_QUOTE_AGE_S",
    "DEFAULT_MAX_SPREAD_PCT",
    "MAX_EPOCH_MS",
    "MEASURED_STATUSES",
    "PERSIST_ALLOWED",
    "PROVIDER",
    "REDISTRIBUTION",
    "SOURCE",
    "STATUSES",
    "AudienceError",
    "PrivateQuote",
    "PrivateQuoteSnapshot",
    "QuoteSource",
    "classify_quote",
    "read_private_quotes",
]
