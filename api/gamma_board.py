"""Read a private instrument panel from the approved reader or an immutable GRID capture."""

from __future__ import annotations

from collections import deque
from datetime import datetime, time, timedelta, timezone
import math
import logging
from threading import Lock
from time import monotonic
from zoneinfo import ZoneInfo

from sqlalchemy import text

from api.gamma_board_symbols import provider_symbol
from api.dependencies import get_db_engine
from ingestion.market_calendar import is_market_open, market_close_time
from physics.greeks.black_scholes import gamma

log = logging.getLogger(__name__)
NY = ZoneInfo("America/New_York")
SYMBOLS = ("SPX", "SPY", "QQQ")
_LOCKS = {s: Lock() for s in SYMBOLS}
# Dedicated locks/cache slots for the three pinned panels. Extra symbols share a
# fixed number of locks and a bounded cache, even with concurrent admin clients.
_EXTRA_LOCKS = [Lock() for _ in range(4)]
_CACHE_GUARD = Lock()
_EXTRA_STARTS = deque()
_EXTRA_LIMIT = 12
_EXTRA_CAPACITY = 24
_CACHE = {}
_REFRESH_SECONDS = 60
_MAX_AGE = timedelta(days=7)
_LIMIT = 10000


def session_open(now):
    et = now.astimezone(NY)
    return is_market_open(et.date()) and time(9, 30) <= et.time() < market_close_time(
        et.date()
    )


def unavailable(symbol, reason):
    return dict(
        symbol=symbol,
        status="unavailable",
        reason=reason,
        cells=[],
        expiries=[],
        spot=None,
        net_gex=None,
        call_wall=None,
        put_wall=None,
        as_of=None,
        spot_as_of=None,
    )


def _database_panel(symbol, now):
    if symbol == "SPX":
        return (
            None  # Historical GRID contracts don't identify AM vs PM index settlement.
        )
    with get_db_engine().begin() as con:
        con.execute(text("SET TRANSACTION READ ONLY"))
        con.execute(text("SET LOCAL statement_timeout = '3s'"))
        batch = (
            con.execute(
                text("""
            SELECT capture_batch_id, capture_completed_at, spot_price, spot_provider,
                   provider_regular_market_at, snap_date
            FROM options_capture_batches
            WHERE ticker=:symbol AND backfilled=false AND capture_completed_at BETWEEN :cutoff AND :now
            ORDER BY snap_date DESC, capture_ordinal DESC LIMIT 1
        """),
                {"symbol": symbol, "cutoff": now - _MAX_AGE, "now": now},
            )
            .mappings()
            .first()
        )
        if not batch:
            return None
        spot = batch["spot_price"]
        if not spot or not math.isfinite(spot) or spot <= 0:
            return None
        clock = batch["capture_completed_at"]
        local = clock.astimezone(NY)
        day = local.date()
        if day != batch["snap_date"]:
            return None
        start = day if session_open(clock) else day + timedelta(days=1)
        rows = (
            con.execute(
                text("""
            SELECT expiry, strike, opt_type, implied_vol, open_interest
            FROM options_snapshots_all WHERE capture_batch_id=:id AND ticker=:symbol
              AND expiry BETWEEN :start AND :end AND strike BETWEEN :lo AND :hi
            ORDER BY expiry, strike, opt_type LIMIT :limit
        """),
                {
                    "id": batch["capture_batch_id"],
                    "symbol": symbol,
                    "start": start,
                    "end": day + timedelta(days=30),
                    "lo": 0.95 * spot,
                    "hi": 1.05 * spot,
                    "limit": _LIMIT + 1,
                },
            )
            .mappings()
            .all()
        )
    if not rows or len(rows) > _LIMIT:
        return None
    # A missing leg can reverse the sign of a cell. Withhold that cell in full,
    # including its valid legs, from both the heat map and the wall calculation.
    incomplete = {
        (r["expiry"], r["strike"])
        for r in rows
        if r["open_interest"] is None
        or r["open_interest"] < 0
        or (
            r["open_interest"] > 0
            and (
                not r["implied_vol"]
                or not math.isfinite(r["implied_vol"])
                or r["implied_vol"] <= 0
            )
        )
    }
    grouped = {}
    levels = {}
    used = 0
    for r in rows:
        oi, iv, side = r["open_interest"], r["implied_vol"], r["opt_type"]
        if (r["expiry"], r["strike"]) in incomplete:
            continue
        if side not in {"call", "put"}:
            continue
        k, exp = r["strike"], r["expiry"]
        end = datetime.combine(exp, market_close_time(exp), NY)
        years = (end - local).total_seconds() / (365 * 86400)
        if years <= 0:
            continue
        g = gamma(spot, k, years, 0.05, iv) if oi else 0.0
        signed = g * oi * 100.0 * spot**2 * 0.01 * (1 if side == "call" else -1)
        if not math.isfinite(signed):
            continue
        used += 1
        cell = grouped.setdefault(
            (exp, k),
            dict(
                strike=k,
                expiry=exp.isoformat(),
                call_gex=0.0,
                put_gex=0.0,
                call_oi=0,
                put_oi=0,
            ),
        )
        cell[side + "_oi"] += oi
        cell[side + "_gex"] += signed
        levels.setdefault(k, {"call": 0.0, "put": 0.0})[side] += signed
    if not grouped:
        return None
    cells = list(grouped.values())
    for cell in cells:
        cell["call_gex"] = round(cell["call_gex"], 2)
        cell["put_gex"] = round(cell["put_gex"], 2)
        cell["net_gex"] = round(cell["call_gex"] + cell["put_gex"], 2)
    strikes = sorted(levels)
    cw = max(strikes, key=lambda k: levels[k]["call"])
    pw = min(strikes, key=lambda k: levels[k]["put"])
    provider_clock = batch["provider_regular_market_at"]
    return dict(
        symbol=symbol,
        status="available",
        source_label="GRID stored options capture",
        source="GRID",
        method="Black–Scholes gamma from captured IV, 5% annual rate, 100 multiplier. Calls positive, puts negative.",
        units="USD per 1% underlying move",
        scope="Captured contracts within ±5% of spot through 30 calendar days. Same-day contracts included during the cash session.",
        coverage=f"{used} of {len(rows)} in-scope captured contracts in cells with complete inputs.",
        contracts=used,
        eligible_contracts=len(rows),
        excluded_contracts=len(rows) - used,
        as_of=clock.isoformat(),
        spot_as_of=provider_clock.isoformat() if provider_clock else None,
        spot_provider=batch["spot_provider"],
        spot=spot,
        last_trade_at=None,
        call_wall=cw if levels[cw]["call"] > 0 else None,
        put_wall=pw if levels[pw]["put"] < 0 else None,
        net_gex=round(sum(c["net_gex"] for c in cells), 2),
        expiries=sorted({c["expiry"] for c in cells}),
        cells=cells,
    )


def _stamp(panel, now):
    clocks = [panel.get("as_of"), panel.get("spot_as_of")]
    live_provider = panel.get("source") == "Schwab"
    maximum = timedelta(minutes=2 if live_provider else 20)
    try:
        ages = [now - datetime.fromisoformat(s) for s in clocks]
        outdated = any(age < timedelta(seconds=-5) or age > maximum for age in ages)
    except (TypeError, ValueError):
        outdated = True
    is_open = session_open(now)
    stale = bool(panel.get("stale") or outdated or (live_provider and not is_open))
    return {
        **panel,
        "session_open": is_open,
        "stale": stale,
        "fresh": bool(panel.get("fresh") and not stale),
    }


def read_panel(symbol, enabled):
    """Serialize each symbol's refresh. Disabled sources cannot return their cached quotes."""
    provider_symbol(symbol)
    key = (symbol, enabled)
    pinned = symbol in _LOCKS
    lock = _LOCKS[symbol] if pinned else _EXTRA_LOCKS[hash(symbol) % len(_EXTRA_LOCKS)]
    with lock:
        now = datetime.now(timezone.utc)
        before = monotonic()
        with _CACHE_GUARD:
            old = _CACHE.get(key)
            if old and before - old[0] < _REFRESH_SECONDS:
                return _stamp(old[1], now)
            if not pinned:
                while _EXTRA_STARTS and before - _EXTRA_STARTS[0] >= _REFRESH_SECONDS:
                    _EXTRA_STARTS.popleft()
                if len(_EXTRA_STARTS) >= _EXTRA_LIMIT:
                    return _stamp(
                        unavailable(
                            symbol,
                            "Additional ticker refresh limit reached. Retry shortly.",
                        ),
                        now,
                    )
                _EXTRA_STARTS.append(before)
        reason = "Private chain provider disabled."
        panel = None
        if enabled:
            try:
                from api.gamma_board_schwab import read_board

                candidate = read_board(symbol, now=now)
                if candidate.get("status") == "available":
                    panel = candidate
            except Exception as exc:
                log.warning(
                    "Gamma board provider unavailable for %s (%s)",
                    symbol,
                    type(exc).__name__,
                )
            reason = (
                "SPX chain or matching contract open interest unavailable."
                if symbol == "SPX"
                else "Private chain unavailable."
            )
        if panel is None and pinned:
            try:
                panel = _database_panel(symbol, now)
            except Exception as exc:
                log.warning(
                    "Gamma board capture unavailable for %s (%s)",
                    symbol,
                    type(exc).__name__,
                )
                panel = None
            if panel:
                panel["source_note"] = reason
        if panel is None and old and old[1]["status"] == "available":
            previous = old[1]
            issued = datetime.fromisoformat(previous["as_of"])
            if timedelta(0) <= now - issued <= _MAX_AGE:
                panel = {
                    **previous,
                    "stale": True,
                    "source_note": "Source unavailable. Displaying the last successful snapshot.",
                }
        if panel is None:
            panel = unavailable(
                symbol,
                reason
                + (
                    " No qualifying GRID capture."
                    if pinned
                    else " No verified standard options chain."
                ),
            )
        with _CACHE_GUARD:
            if not pinned and key not in _CACHE:
                extras = [k for k in _CACHE if k[0] not in _LOCKS]
                if len(extras) >= _EXTRA_CAPACITY:
                    del _CACHE[min(extras, key=lambda k: _CACHE[k][0])]
            _CACHE[key] = (monotonic(), panel)
        return _stamp(panel, datetime.now(timezone.utc))
