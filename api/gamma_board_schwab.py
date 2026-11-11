"""Personal GRID board: aggregate Schwab chains in memory behind the board's admin gate."""

import re
from collections import defaultdict
from datetime import date, datetime, time, timedelta, timezone
from math import isfinite
from zoneinfo import ZoneInfo

import requests

from api.gamma_board_symbols import provider_symbol
from ingestion.market_calendar import is_market_open, market_close_time
from ingestion.schwab_market import SchwabMarketClient

NY = ZoneInfo("America/New_York")
OCC = re.compile(r"([A-Z]+) *(\d{6})([CP])(\d{8})\Z")
METHOD = (
    "Provider gamma × daily open interest × 100 × spot² × 0.01. "
    "Calls positive, puts negative; estimated exposure, not measured dealer positions."
)
SCOPE = "Up to 160 provider-selected strikes per expiry, filtered to standard 100-unit contracts within ±5% of spot and 30 calendar days. Same-day PM series only during the cash session."


def _number(x):
    return isinstance(x, (float, int)) and not isinstance(x, bool) and isfinite(x)


def _clock(ms, now):
    if not _number(ms) or not 0 < ms <= (now.timestamp() + 30) * 1000:
        return None
    return datetime.fromtimestamp(ms / 1000, timezone.utc)


def _rows(chain, side):
    for strikes in (chain.get(side + "ExpDateMap") or {}).values():
        for rows in strikes.values():
            yield from rows


class MissingOpenInterest(ValueError):
    """Across the entire eligible chain the provider has no positive open interest."""


def aggregate(symbol, chain, *, now, daily_oi=None):
    """Use the OCC identity, declared deliverable and provider timestamps; never substitute receipt."""
    requested = provider_symbol(symbol)
    roots_allowed = {"SPX", "SPXW", "SPXO"} if symbol == "SPX" else {symbol}
    u = chain.get("underlying") or {}
    if (
        chain.get("status") != "SUCCESS"
        or chain.get("symbol") != requested
        or u.get("symbol") != requested
    ):
        raise ValueError("Chain or underlying identity unavailable")
    # Pair the displayed underlying price with the timestamp of its own last trade.
    spot = u.get("last")
    if not _number(spot) or spot <= 0:
        raise ValueError("Underlying price unavailable")
    trade = _clock(u.get("tradeTime"), now)
    quote = _clock(u.get("quoteTime"), now)
    local = now.astimezone(NY)
    today = local.date()
    in_session = is_market_open(today) and time(
        9, 30
    ) <= local.time() < market_close_time(today)
    cells, roots, totals = {}, defaultdict(set), defaultdict(lambda: [0.0, 0.0])
    latest = None
    seen, incomplete = set(), set()
    covered = defaultdict(list)
    dropped = count = positive_oi = eligible = 0
    for i, side in enumerate(("call", "put")):
        for row in _rows(chain, side):
            occ = OCC.fullmatch(str(row.get("symbol", "")))
            if not occ or occ[1] not in roots_allowed:
                continue
            root, day, cp, strike_code = occ.groups()
            strike = int(strike_code) / 1000
            exp = date.fromisoformat(f"20{day[:2]}-{day[2:4]}-{day[4:]}")
            days = (exp - today).days
            if not 0 <= days <= 30 or not 0.95 * spot <= strike <= 1.05 * spot:
                continue
            if days == 0 and (
                not in_session
                or root in {"SPX", "SPXO"}
                or row.get("settlementType") != "P"
            ):
                continue
            if (
                row.get("nonStandard") is not False
                or row.get("mini") is not False
                or row.get("multiplier") != 100
            ):
                continue
            ident = (root, day, cp, strike)
            if ident in seen:
                raise ValueError("Duplicate contract identity")
            seen.add(ident)
            if (
                row.get("strikePrice") != strike
                or row.get("optionRoot") != root
                or row.get("putCall") != side.upper()
                or cp != side[0].upper()
                or str(row.get("expirationDate", ""))[:10] != exp.isoformat()
            ):
                raise ValueError("Inconsistent contract identity")
            key = (exp.isoformat(), strike)
            eligible += 1
            oi = (
                row.get("openInterest")
                if daily_oi is None
                else daily_oi.get(occ[0].replace(" ", ""))
            )
            gamma = row.get("gamma")
            if _number(oi) and oi > 0:
                positive_oi += 1
            if (
                not _number(oi)
                or oi < 0
                or int(oi) != oi
                or (oi > 0 and (not _number(gamma) or gamma < 0))
            ):
                incomplete.add(key)
                dropped += 1
                continue
            exposure = gamma * oi * spot**2 * (1 if i == 0 else -1) if oi else 0.0
            cell = cells.setdefault(
                key,
                {
                    "expiry": key[0],
                    "strike": strike,
                    "call_oi": 0,
                    "put_oi": 0,
                    "call_gex": 0.0,
                    "put_gex": 0.0,
                },
            )
            cell[side + "_oi"] += int(oi)
            cell[side + "_gex"] += exposure
            covered[key].append((root, _clock(row.get("quoteTimeInLong"), now)))
    for key in incomplete:
        cells.pop(key, None)
    for key, cell in cells.items():
        totals[key[1]][0] += cell["call_gex"]
        totals[key[1]][1] += cell["put_gex"]
        for root, stamp in covered[key]:
            roots[key[0]].add(root)
            if stamp and (latest is None or stamp > latest):
                latest = stamp
            count += 1
    if eligible and not positive_oi:
        raise MissingOpenInterest(
            "Provider has no usable open interest across the chain"
        )
    if daily_oi is not None and count < 0.95 * eligible:
        raise ValueError("Insufficient matched contract open interest")
    if not count or not latest:
        raise ValueError("No dated option quotes in scope")
    for cell in cells.values():
        for k in ("call_gex", "put_gex"):
            cell[k] = round(cell[k], 2)
        cell["net_gex"] = round(cell["call_gex"] + cell["put_gex"], 2)
    ordered = sorted(totals)
    cw = max(ordered, key=lambda s: totals[s][0])
    pw = min(ordered, key=lambda s: totals[s][1])
    if totals[cw][0] <= 0 and totals[pw][1] >= 0:
        raise ValueError("No usable gamma exposure across the chain")
    realtime = chain.get("isDelayed") is False and u.get("delayed") is False
    fresh = bool(
        in_session
        and realtime
        and trade
        and all(-30 <= (now - t).total_seconds() <= 120 for t in (trade, latest))
    )
    return {
        "symbol": symbol,
        "status": "available",
        "spot": spot,
        "source_label": "Schwab options chain",
        "method": METHOD,
        "scope": SCOPE,
        "units": "USD per 1% underlying move",
        "source": "Schwab",
        "as_of": latest.isoformat(),
        "spot_as_of": trade.isoformat() if trade else None,
        "last_trade_at": trade.isoformat() if trade else None,
        "underlying_quote_at": quote.isoformat() if quote else None,
        "received_at": now.isoformat(),
        "coverage": f"{count:,} contracts · {dropped:,} missing inputs ({len(incomplete):,} cells withheld) · chain time is newest included quote",
        "fresh": fresh,
        "stale": not fresh,
        "session_open": in_session,
        "feed_latency": "non-delayed" if realtime else "delayed or unconfirmed",
        "call_wall": cw if totals[cw][0] > 0 else None,
        "put_wall": pw if totals[pw][1] < 0 else None,
        "net_gex": round(sum(c["net_gex"] for c in cells.values()), 2),
        "contracts": count,
        "contracts_missing_inputs": dropped,
        "expiries": sorted(roots),
        "cells": [cells[k] for k in sorted(cells)],
        "session_date": today.isoformat(),
        "included_0dte": today.isoformat() in roots,
        "roots": sorted(set.union(*roots.values())),
        "expiry_roots": {k: sorted(v) for k, v in sorted(roots.items())},
    }


def read_board(symbol: str, *, now: datetime) -> dict:
    day = now.astimezone(NY).date()
    with requests.Session() as http:
        reader = SchwabMarketClient(session=http, timeout=12, max_retries=0)
        chain = reader.option_chain(
            provider_symbol(symbol),
            contractType="ALL",
            includeUnderlyingQuote="TRUE",
            strategy="SINGLE",
            strikeCount=160,
            fromDate=day.isoformat(),
            toDate=(day + timedelta(days=30)).isoformat(),
        )
    received = datetime.now(timezone.utc)
    try:
        return aggregate(symbol, chain, now=received)
    except MissingOpenInterest:
        if symbol != "SPX":
            raise
    # Index chains can report zero OI throughout overnight trading. Match a single
    # complete provider snapshot by OCC root, expiry, side and strike. Never combine
    # different OI feeds across cells or assume that an unmatched contract has zero OI.
    from api.gamma_board_oi import read_spx_oi

    spot = chain["underlying"]["last"]
    positions, observed = read_spx_oi(
        day=day, low=spot * 0.95, high=spot * 1.05, now=received
    )
    panel = aggregate(symbol, chain, now=received, daily_oi=positions)
    panel.update(
        source_label="Schwab options chain + Massive daily open interest",
        oi_source="Massive options snapshot",
        oi_retrieved_at=observed.isoformat(),
        source_note=(
            "Daily open interest is matched by exact contract. Massive reports the latest "
            "end-of-day count without a separate OI date. Snapshot retrieved "
            + observed.isoformat(timespec="minutes")
            + "."
        ),
    )
    return panel
