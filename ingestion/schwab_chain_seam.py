"""Schwab ``/chains`` body -> GEX ``records[]`` shape, as pure synthetic normalization.

Implements the seam described in ``docs/reference/SCHWAB_GEX_CHAIN_SEAM.md`` (section 3).
Newly wired behaviour (2026-10-31); nothing in ``collectors/gamma_watch/`` or the deployed
gex-watch is changed, imported or called. The GEX read contract (``broker.py``) is
*preserved*, not entered: the envelope produced here carries its own identity
(``schema: "spy-chain-schwab-v1"``, ``host`` = the GRID host) so the owner's identity
gate (``schema == "spy-rtd-v1" and host == "ANIK"``) rejects it until the GEX owner
decides otherwise. Record shape, field names and value conventions match what
``broker.build_feed`` consumes so that acceptance would be a one-line owner decision
per gate, not a parser change.

Rules (all fail closed, per contract):

* body must be a mapping with ``status`` absent or ``"SUCCESS"`` and at least one of
  ``callExpDateMap`` / ``putExpDateMap`` as a mapping; otherwise :class:`InvalidChainError`;
* identity (r2): the body's ``symbol``, every universe contract's root, and the underlying's
  identity (the raw ``underlying.symbol`` or an injected :class:`PrivateQuote`'s requested and
  provider symbols) must equal the **requested** ``symbol`` or an explicitly passed alias
  (``aliases``: requested symbol -> accepted provider symbols; none by default); missing or
  mismatched identity raises :class:`InvalidChainError` (fail closed) and underlying records
  are keyed by the requested symbol, never a literal;
* the contract universe is the GEX owner's ``contracts.json`` (``symbol, expiry, opt_type,
  strike``); it is read, validated and never rewritten;
* duplicates (r2): an expiry/type/strike key that appears more than once in the body is
  ambiguous; every one of its entries is dropped, the contract is emitted ``callback_seen:
  false`` and the reason is recorded, whether the duplicates are identical or conflicting;
* a contract is *seen* only when the body entry's identity matches the universe entry on
  all of OCC symbol (converted to the thinkorswim form), ``putCall``, expiry and strike,
  and carries a parseable ``quoteTimeInLong``; anything else is recorded in ``rejected``
  and emitted as ``callback_seen: false`` so the consumer counts it missing;
* ``volatility`` is a percent -> fraction (/100); non-finite, non-positive or sentinel
  values are absent; sizes/open interest/volume go through the reader's exact-integer
  parser; prices through its finite-float parser; nothing is coerced to 0;
* ``received_at`` of every contract record is the provider's ``quoteTimeInLong`` (an
  exchange timestamp, unlike RTD receipt time) — see owner decision 2 in the seam note;
* the underlying SPY ``LAST/BID/ASK`` records come from an injected
  :class:`PrivateQuote` (quote path) when it is measured, else from the body's
  ``underlying`` object, else they are absent (the consumer then rejects the snapshot,
  which is the honest outcome).

No I/O, no clock (``received_at`` is injected), no credentials, no GEX import.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import date, datetime, timezone
from typing import Any

from ingestion.schwab_market import _as_float, _as_int, _as_str
from ingestion.schwab_quote_path import (
    AUDIENCE_PRIVATE,
    NO_ALIASES,
    PERSIST_ALLOWED,
    PROVIDER,
    REDISTRIBUTION,
    SOURCE,
    Aliases,
    PrivateQuote,
    _ms_to_dt,
)

SCHEMA = "spy-chain-schwab-v1"
ENTITLEMENT = "Schwab Trader API read-only app; exchange timestamps per provider; redistribution prohibited"
DEFAULT_HOST = "GRID"
CONTRACT_FIELDS: tuple[str, ...] = ("BID", "ASK", "IMPL_VOL", "OPEN_INT", "GAMMA", "DELTA", "VOLUME")
UNDERLYING_FIELDS: tuple[str, ...] = ("LAST", "BID", "ASK")
MAX_CONTRACTS = 20_000
IV_SENTINELS: frozenset[float] = frozenset({-999.0, -999.99})

_OCC = re.compile(r"^(?P<root>[A-Z]{1,6})\s*(?P<ymd>\d{6})(?P<cp>[CP])(?P<strike>\d{8})$")
_TOS = re.compile(r"^\.(?P<root>[A-Z]{1,6})(?P<ymd>\d{6})(?P<cp>[CP])(?P<strike>\d+(?:\.\d+)?)$")
_EXPIRY_KEY = re.compile(r"^(?P<date>\d{4}-\d{2}-\d{2})(?::-?\d+)?$")


class InvalidChainError(ValueError):
    """The chain body is structurally unusable (not a per-contract problem)."""


@dataclass(frozen=True)
class Contract:
    symbol: str  # thinkorswim form, e.g. ".SPY260928C735"
    expiry: str  # YYYY-MM-DD
    opt_type: str  # "call" | "put"
    strike: float


def tos_symbol(root: str, yymmdd: str, cp: str, strike: float) -> str:
    """Build the thinkorswim symbol: strike without trailing zeros (735, 735.5)."""
    text = f"{strike:.3f}".rstrip("0").rstrip(".")
    return f".{root}{yymmdd}{cp}{text}"


def occ_to_tos(occ: Any) -> str | None:
    """``"SPY   260928C00735000"`` -> ``".SPY260928C735"``; ``None`` when malformed."""
    if not isinstance(occ, str):
        return None
    m = _OCC.match(occ.strip().upper())
    if not m:
        return None
    strike = int(m.group("strike")) / 1000.0
    return tos_symbol(m.group("root"), m.group("ymd"), m.group("cp"), strike)


def _normalise_symbol(value: Any) -> str | None:
    text = _as_str(value)
    if text is None:
        return None
    text = text.strip().upper()
    return text or None


def _require_identity(requested: str, provider: Any, aliases: Aliases | None, what: str) -> None:
    """``provider`` must equal ``requested`` or one of its explicitly approved aliases."""
    got = _normalise_symbol(provider)
    if got is None:
        raise InvalidChainError(f"{what} symbol missing")
    if got == requested:
        return
    for key, accepted in (aliases or NO_ALIASES).items():
        if _normalise_symbol(key) != requested:
            continue
        if any(_normalise_symbol(a) == got for a in accepted):
            return
    raise InvalidChainError(f"{what} symbol does not match the requested symbol")


def parse_contracts(universe: Any, *, symbol: str | None = None) -> tuple[Contract, ...]:
    """Validate a ``contracts.json``-shaped universe; raises :class:`InvalidChainError`.

    With ``symbol``, every contract's root must equal it (universe identity)."""
    if isinstance(universe, (str, bytes, Mapping)) or not isinstance(universe, Iterable):
        raise InvalidChainError("contract universe must be a list of contract mappings")
    symbol_root = _normalise_symbol(symbol) if symbol is not None else None
    out: list[Contract] = []
    seen: set[str] = set()
    for i, raw in enumerate(universe):
        if not isinstance(raw, Mapping):
            raise InvalidChainError(f"contract {i} is not a mapping")
        symbol = _as_str(raw.get("symbol"))
        expiry = _as_str(raw.get("expiry"))
        opt_type = _as_str(raw.get("opt_type"))
        strike = _as_float(raw.get("strike"))
        if symbol is None or not _TOS.match(symbol):
            raise InvalidChainError(f"contract {i} has no thinkorswim symbol")
        if expiry is None or _parse_date(expiry) is None:
            raise InvalidChainError(f"contract {symbol} has no YYYY-MM-DD expiry")
        if opt_type not in ("call", "put"):
            raise InvalidChainError(f"contract {symbol} opt_type must be call|put")
        if strike is None or strike <= 0:
            raise InvalidChainError(f"contract {symbol} strike must be a positive number")
        m = _TOS.match(symbol)
        assert m is not None
        if symbol_root is not None and m.group("root") != symbol_root:
            raise InvalidChainError(f"contract {symbol} is not a {symbol_root} contract")
        expected = tos_symbol(
            m.group("root"), _parse_date(expiry).strftime("%y%m%d"), "C" if opt_type == "call" else "P", strike
        )
        if expected != symbol:
            raise InvalidChainError(f"contract {symbol} disagrees with its expiry/type/strike")
        if symbol in seen:
            raise InvalidChainError(f"contract {symbol} listed twice")
        seen.add(symbol)
        out.append(Contract(symbol, expiry, opt_type, strike))
        if len(out) > MAX_CONTRACTS:
            raise InvalidChainError(f"more than {MAX_CONTRACTS} contracts")
    return tuple(out)


def schwab_chain_to_rtd_records(
    chain_body: Any,
    contracts: Any,
    *,
    received_at: datetime,
    host: str = DEFAULT_HOST,
    underlying: PrivateQuote | None = None,
    symbol: str = "SPY",
    aliases: Aliases | None = None,
) -> dict[str, Any]:
    """Normalize a ``/chains`` body against the GEX universe into a ``spy-rtd-v1``-shaped
    record list under the seam's own identity. Pure; strict JSON; see module docstring."""
    received_at = _require_aware(received_at)
    requested = _normalise_symbol(symbol)
    if requested is None:
        raise InvalidChainError("requested symbol must be a non-empty string")
    universe = parse_contracts(contracts, symbol=requested)
    if not isinstance(chain_body, Mapping):
        raise InvalidChainError("chain body is not a mapping")
    _require_identity(requested, chain_body.get("symbol"), aliases, "chain")
    status = chain_body.get("status")
    if status is not None and status != "SUCCESS":
        raise InvalidChainError("chain status is not SUCCESS")
    index, index_problems, ambiguous = _index_chain(chain_body)
    if not isinstance(host, str) or not host.strip():
        raise InvalidChainError("host must be a non-empty string")

    records: list[dict[str, Any]] = []
    rejected: list[dict[str, str]] = list(index_problems)
    seen = 0
    for c in universe:
        key = (c.expiry, c.opt_type, c.strike)
        if key in ambiguous:
            rejected.append({"symbol": c.symbol, "reason": "ambiguous duplicate entries"})
            for field in CONTRACT_FIELDS:
                records.append(_unseen(c.symbol, field))
            continue
        entry = index.get(key)
        problem = None if entry is not None else "absent from chain body"
        if entry is not None:
            problem = _identity_problem(c, entry)
        quote_dt = _ms_to_dt(entry.get("quoteTimeInLong")) if entry is not None and problem is None else None
        if entry is not None and problem is None and quote_dt is None:
            problem = "no parseable quoteTimeInLong"
        if problem is not None:
            if entry is not None:
                rejected.append({"symbol": c.symbol, "reason": problem})
            for field in CONTRACT_FIELDS:
                records.append(_unseen(c.symbol, field))
            continue
        seen += 1
        assert entry is not None and quote_dt is not None
        values = _contract_values(entry)
        for field in CONTRACT_FIELDS:
            value = values.get(field)
            if value is None:
                records.append(_unseen(c.symbol, field))
            else:
                records.append(
                    {
                        "symbol": c.symbol,
                        "field": field,
                        "value": value,
                        "received_at": quote_dt.isoformat(),
                        "callback_seen": True,
                    }
                )

    records.extend(_underlying_records(chain_body, underlying, requested, aliases))
    envelope = {
        "schema": SCHEMA,
        "host": host.strip(),
        "symbol": requested,
        "source": SOURCE,
        "provider": PROVIDER,
        "audience": AUDIENCE_PRIVATE,
        "redistribution": REDISTRIBUTION,
        "persist_allowed": PERSIST_ALLOWED,
        "entitlement": ENTITLEMENT,
        "exchange_timestamp_available": True,
        "collected_at": received_at.isoformat(),
        "heartbeat": 1 if seen else 0,
        "update_count": seen,
        "topic_count": len(records),
        "universe_size": len(universe),
        "seen_contracts": seen,
        "rejected": rejected,
        "records": records,
    }
    json.dumps(envelope, allow_nan=False)
    return envelope


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _index_chain(
    body: Mapping[str, Any],
) -> tuple[dict[tuple[str, str, float], Mapping[str, Any]], list[dict[str, str]], set[tuple[str, str, float]]]:
    index: dict[tuple[str, str, float], Mapping[str, Any]] = {}
    problems: list[dict[str, str]] = []
    ambiguous: set[tuple[str, str, float]] = set()
    maps = (("callExpDateMap", "call"), ("putExpDateMap", "put"))
    present = 0
    for key, opt_type in maps:
        exp_map = body.get(key)
        if exp_map is None:
            continue
        if not isinstance(exp_map, Mapping):
            raise InvalidChainError(f"{key} is not a mapping")
        present += 1
        for exp_key, strikes in exp_map.items():
            m = _EXPIRY_KEY.match(str(exp_key))
            expiry = m.group("date") if m else None
            if expiry is None or _parse_date(expiry) is None or not isinstance(strikes, Mapping):
                problems.append({"symbol": f"{key}:{exp_key}", "reason": "malformed expiry bucket"})
                continue
            for strike_key, entries in strikes.items():
                strike = _as_float(strike_key)
                if strike is None or strike <= 0 or isinstance(entries, (str, bytes, Mapping)) or not isinstance(entries, Iterable):
                    problems.append({"symbol": f"{key}:{exp_key}:{strike_key}", "reason": "malformed strike bucket"})
                    continue
                for entry in entries:
                    if not isinstance(entry, Mapping):
                        problems.append({"symbol": f"{key}:{exp_key}:{strike_key}", "reason": "contract entry is not a mapping"})
                        continue
                    k = (expiry, opt_type, strike)
                    if k in index or k in ambiguous:
                        ambiguous.add(k)
                        problems.append({"symbol": f"{key}:{exp_key}:{strike_key}", "reason": "duplicate contract entry"})
                        continue
                    index[k] = entry
                    if len(index) > MAX_CONTRACTS:
                        raise InvalidChainError(f"chain body holds more than {MAX_CONTRACTS} contracts")
    if present == 0:
        raise InvalidChainError("chain body has neither callExpDateMap nor putExpDateMap")
    for k in ambiguous:
        index.pop(k, None)  # every entry of an ambiguous key is dropped, first included
    return index, problems, ambiguous


def _identity_problem(c: Contract, entry: Mapping[str, Any]) -> str | None:
    tos = occ_to_tos(entry.get("symbol"))
    if tos is None:
        return "provider symbol missing or not OCC form"
    if tos != c.symbol:
        return "provider symbol does not match universe contract"
    put_call = _as_str(entry.get("putCall"))
    if put_call is None or put_call.strip().upper() != ("CALL" if c.opt_type == "call" else "PUT"):
        return "putCall does not match universe contract"
    strike = _as_float(entry.get("strikePrice"))
    if strike is None or not math.isclose(strike, c.strike, rel_tol=0.0, abs_tol=1e-6):
        return "strikePrice does not match universe contract"
    return None


def _contract_values(entry: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    bid = _as_float(entry.get("bid"))
    ask = _as_float(entry.get("ask"))
    if bid is not None and bid >= 0:
        out["BID"] = bid
    if ask is not None and ask >= 0:
        out["ASK"] = ask
    iv = _as_float(entry.get("volatility"))
    if iv is not None and iv > 0 and iv not in IV_SENTINELS:
        out["IMPL_VOL"] = iv / 100.0
    oi = _as_int(entry.get("openInterest"))
    if oi is not None:
        out["OPEN_INT"] = oi
    vol = _as_int(entry.get("totalVolume"))
    if vol is not None:
        out["VOLUME"] = vol
    gamma = _as_float(entry.get("gamma"))
    if gamma is not None:
        out["GAMMA"] = gamma
    delta = _as_float(entry.get("delta"))
    if delta is not None:
        out["DELTA"] = delta
    return out


def _underlying_records(
    body: Mapping[str, Any], quote: PrivateQuote | None, requested: str, aliases: Aliases | None
) -> list[dict[str, Any]]:
    """Underlying LAST/BID/ASK keyed by the requested symbol; identity is verified first.

    An injected classified quote must have been requested for ``requested`` and its
    provider symbol must pass the alias check; a raw ``underlying`` object must carry a
    matching ``symbol``. Missing or mismatched identity fails closed. An absent
    underlying (no object, no injected quote) is simply unseen."""
    if quote is not None:
        if not isinstance(quote, PrivateQuote):
            raise InvalidChainError("underlying must be a quote-path PrivateQuote")
        if _normalise_symbol(quote.symbol) != requested:
            raise InvalidChainError("underlying quote was requested for a different symbol")
        _require_identity(requested, quote.provider_symbol, aliases, "underlying provider")
        if not quote.measured or quote.quote_time is None:
            return [_unseen(requested, f) for f in UNDERLYING_FIELDS]
        ts = quote.quote_time.isoformat()
        vals = {"LAST": quote.last, "BID": quote.bid, "ASK": quote.ask}
        return [_seen_or_unseen(requested, f, vals[f], ts) for f in UNDERLYING_FIELDS]
    raw = body.get("underlying")
    if raw is None:
        return [_unseen(requested, f) for f in UNDERLYING_FIELDS]
    if not isinstance(raw, Mapping):
        raise InvalidChainError("underlying is not a mapping")
    _require_identity(requested, raw.get("symbol"), aliases, "underlying")
    ts_dt = _ms_to_dt(raw.get("quoteTime"))
    if ts_dt is None:
        return [_unseen(requested, f) for f in UNDERLYING_FIELDS]
    ts = ts_dt.isoformat()
    vals = {"LAST": _as_float(raw.get("last")), "BID": _as_float(raw.get("bid")), "ASK": _as_float(raw.get("ask"))}
    return [_seen_or_unseen(requested, f, vals[f], ts) for f in UNDERLYING_FIELDS]


def _seen_or_unseen(symbol: str, field: str, value: float | None, ts: str) -> dict[str, Any]:
    if value is None or value <= 0:
        return _unseen(symbol, field)
    return {"symbol": symbol, "field": field, "value": value, "received_at": ts, "callback_seen": True}


def _unseen(symbol: str, field: str) -> dict[str, Any]:
    return {"symbol": symbol, "field": field, "value": None, "received_at": None, "callback_seen": False}


def _parse_date(text: str) -> date | None:
    try:
        return date.fromisoformat(text)
    except (TypeError, ValueError):
        return None


def _require_aware(value: datetime) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise InvalidChainError("received_at must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


__all__ = [
    "CONTRACT_FIELDS",
    "DEFAULT_HOST",
    "ENTITLEMENT",
    "MAX_CONTRACTS",
    "SCHEMA",
    "UNDERLYING_FIELDS",
    "Contract",
    "InvalidChainError",
    "occ_to_tos",
    "parse_contracts",
    "schwab_chain_to_rtd_records",
    "tos_symbol",
]
