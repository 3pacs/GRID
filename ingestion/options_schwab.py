"""Schwab Trader API option chains as an ``OptionsPuller`` provider (2026-11-21).

Why: on 2026-11-20 Cboe's delayed-quotes file answered HTTP 403 for SPY all
session (first refusal 14:43:57Z; the same refusal was seen from a second host
that day, 3pacs/gex ``schwab_chain.py``). ``OptionsPuller`` stops a run on a
refusal by design (#1226: no Yahoo fallback, no retry of another symbol), so
GRID stored no option chain for that session. This module does not change that
rule and does not contact Cboe. It is a different source, selected only by
``GRID_OPTIONS_PRIMARY_PROVIDER=schwab`` (owner decision 2026-11-21: Schwab for
the hand-listed core, stored in GRID's private database). The default provider
is unchanged.

What it serves: ``ingestion.options.EQUITY_TICKERS`` only (the hand-listed
core). Any other ticker is never requested; ``OptionsPuller.pull_all`` reports
it SKIPPED ``provider_core_list_only``, so a default-universe run under this
provider is PARTIAL by construction and never publishes source freshness for
the catalyst names it did not read.

Shared allowance: the Schwab app's per-minute allowance is shared with the
live quote recorder (gex-watch), whose lost minutes cost live evidence
(``ingestion/schwab_usage.py``). So this client

* spaces its requests ``MIN_REQUEST_INTERVAL_SECONDS`` apart;
* never retries: a 429, 5xx, transport or auth failure stops the whole run
  (``OptionsSourceStopped``), exactly like an access denial, and later tickers
  are not requested;
* does not start a run (``schwab_capture_hold``) when the shared call ledger
  shows any client refused with 429 in the last ``RATE_LIMIT_HOLD_SECONDS``,
  or shows this client's own calls in the last ``MIN_RUN_GAP_SECONDS`` (a
  repair loop cannot re-pull the universe every few minutes).

Request shape (measured 2026-11-21, market closed, 20 calls): one ``/chains``
call for today..+41 days, and a second for +42..+400 days only when the first
holds fewer than ``MAX_EXPIRATIONS`` expiries. SPY: 13 expiries, 6.7 MB, one
call. AAPL / NVDA / KO / BRK-B: 10 / 10 / 6 / 6 expiries in the first window,
so two calls, under 2 MB each. A default run is about 80 calls.

Fail closed: a body that is not ``status: SUCCESS`` for the requested symbol,
is marked truncated, or repeats a contract is a failed fetch (no rows). Only
standard contracts whose OCC symbol names the same root, expiry, side and
strike as the bucket they sit in are kept. IV is the provider's percent as a
fraction; a non-positive or sentinel value is missing, never zero.

Clocks: the underlying's ``last`` and its ``tradeTime`` date the capture (the
same reading the Cboe client takes); every contract keeps the provider's own
quote and last-trade times. Schwab data may not be redistributed: rows are
labelled ``audience: private`` and the options API routes require auth.
"""

from __future__ import annotations

import math
import re
import time
from collections.abc import Callable, Mapping
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from config import log, settings
from ingestion import options as _options
from ingestion import schwab_market, schwab_usage

PROVIDER = "schwab"
CLIENT_NAME = "grid-options"
MIN_REQUEST_INTERVAL_SECONDS = 2.0
RATE_LIMIT_HOLD_SECONDS = 600
MIN_RUN_GAP_SECONDS = 45 * 60
# (first day, last day) offsets from today's New York date, inclusive.
WINDOWS_DAYS: tuple[tuple[int, int], ...] = ((0, 41), (42, 400))
IV_SENTINELS: frozenset[float] = frozenset({-999.0, -999.99})

_EXPIRY_KEY = re.compile(r"^(\d{4}-\d{2}-\d{2})(?::-?\d+)?$")


def schwab_symbol(ticker: str) -> str:
    """GRID ticker -> Schwab symbol (``BRK-B`` -> ``BRK/B``, verified 2026-11-21)."""
    return ticker.strip().upper().replace("-", "/")


def _occ_root(ticker: str) -> str:
    return ticker.strip().upper().replace("-", "").replace(".", "").replace("/", "")


def _epoch_ms(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        return None
    return int(value)


def _contract(entry: Mapping[str, Any], strike: float, is_call: bool, spot: float | None) -> dict[str, Any]:
    iv = _options._finite(entry.get("volatility"))
    in_the_money = entry.get("inTheMoney")
    if not isinstance(in_the_money, bool):
        in_the_money = bool(spot is not None and (strike < spot if is_call else strike > spot))
    return {
        "contractSymbol": str(entry.get("symbol")).replace(" ", ""),
        "clockMetadata": {
            "contract_quote_time_ms": _epoch_ms(entry.get("quoteTimeInLong")),
            "contract_last_trade_ms": _epoch_ms(entry.get("tradeTimeInLong")),
        },
        "strike": strike,
        "lastPrice": _options._finite(entry.get("last")),
        "bid": _options._finite(entry.get("bid")),
        "ask": _options._finite(entry.get("ask")),
        "volume": _options._count(entry.get("totalVolume")),
        "openInterest": _options._count(entry.get("openInterest")),
        "impliedVolatility": iv / 100.0 if iv is not None and iv > 0 and iv not in IV_SENTINELS else None,
        "inTheMoney": in_the_money,
    }


def schwab_chain_pages(bodies: list[Any], ticker: str, today: date) -> dict[str, Any] | None:
    """Turn Schwab ``/chains`` bodies (window order) into Yahoo-shaped expiry pages.

    Returns ``{"expirations", "pages", "quote", "skipped"}`` or None when any
    body is not a complete chain for ``ticker`` (see the module docstring).
    Pure: no I/O, no clock. ``skipped`` counts entries left out as
    non-standard or not naming the contract of the bucket they sit in.
    """
    if not bodies:
        return None
    symbol, root = schwab_symbol(ticker), _occ_root(ticker)
    pages: dict[int, dict[str, list[dict[str, Any]]]] = {}
    seen: set[tuple[date, str, float]] = set()
    skipped = 0
    spot: float | None = None
    quote: dict[str, Any] | None = None
    for body in bodies:
        if not isinstance(body, Mapping) or str(body.get("status") or "").upper() != "SUCCESS":
            return None
        if str(body.get("symbol") or "").strip().upper() != symbol or body.get("isChainTruncated") is True:
            return None
        if quote is None:
            underlying = body.get("underlying")
            if not isinstance(underlying, Mapping) or str(underlying.get("symbol") or "").strip().upper() != symbol:
                return None
            spot = _options._finite(underlying.get("last"))
            trade_ms = _epoch_ms(underlying.get("tradeTime"))
            quote = {
                "regularMarketPrice": spot,
                "regularMarketTime": trade_ms / 1000.0 if trade_ms is not None else None,
                # Shared provider semantics, once per capture (schema 2, as the
                # Cboe client writes them). Contract rows carry their own times.
                "clockMetadata": {
                    "schema_version": 2,
                    "provider": PROVIDER,
                    "document_timestamp_raw": None,
                    "document_timestamp_timezone": "unknown",
                    "underlying_last_trade_raw": str(trade_ms) if trade_ms is not None else None,
                    "underlying_last_trade_timezone": "UTC epoch milliseconds",
                    "underlying_quote_time_ms": _epoch_ms(underlying.get("quoteTime")),
                    "contract_last_trade_timezone": "UTC epoch milliseconds",
                    "bid_ask_updated_at": None,
                    "bid_ask_freshness": "per_contract_quote_time",
                    "synchronization": "unknown",
                    "provider_delayed": body.get("isDelayed") is True or underlying.get("delayed") is True,
                    "audience": "private",
                    "redistribution": "prohibited",
                },
            }
        present = 0
        for key, is_call in (("callExpDateMap", True), ("putExpDateMap", False)):
            exp_map = body.get(key)
            if exp_map is None:
                continue
            if not isinstance(exp_map, Mapping):
                return None
            present += 1
            for exp_key, strikes in exp_map.items():
                match = _EXPIRY_KEY.match(str(exp_key))
                if match is None or not isinstance(strikes, Mapping):
                    return None
                try:
                    expiry = date.fromisoformat(match[1])
                except ValueError:
                    return None
                if expiry < today:
                    continue
                for entries in strikes.values():
                    if not isinstance(entries, list):
                        return None
                    for entry in entries:
                        if not isinstance(entry, Mapping):
                            return None
                        parsed = _options.parse_occ_symbol(str(entry.get("symbol") or "").replace(" ", "").upper())
                        if (parsed is None or parsed[0] != root or parsed[1] != expiry
                                or (parsed[2] == "C") != is_call
                                or entry.get("nonStandard") is True or entry.get("mini") is True):
                            skipped += 1
                            continue
                        strike = parsed[3]
                        ident = (expiry, parsed[2], strike)
                        if ident in seen:
                            return None  # the same contract twice: ambiguous, no rows
                        seen.add(ident)
                        page = pages.setdefault(_options._expiry_ts(expiry), {"calls": [], "puts": []})
                        page["calls" if is_call else "puts"].append(_contract(entry, strike, is_call, spot))
        if present == 0:
            return None
    assert quote is not None
    return {"expirations": sorted(pages), "pages": pages, "quote": quote, "skipped": skipped}


def schwab_capture_hold(
    now_epoch: float | None = None, *, usage_dir: str | Path | None = None,
    configured: Callable[[], bool] = schwab_market.is_configured,
) -> str | None:
    """Why a Schwab options capture must not start now, or None to proceed.

    Reads the shared call ledger only; makes no request. An unreadable ledger
    reads as empty (the in-run stop rule still applies).
    """
    if not configured():
        return "schwab_not_configured"
    now = time.time() if now_epoch is None else now_epoch
    ledger = Path(usage_dir) if usage_dir is not None else schwab_usage.usage_dir(
        Path(settings.SCHWAB_TOKEN_FILE).expanduser())
    rows = schwab_usage.read(ledger, now - max(RATE_LIMIT_HOLD_SECONDS, MIN_RUN_GAP_SECONDS), now + 1)
    if any(status == "429" and t >= now - RATE_LIMIT_HOLD_SECONDS for t, _c, _p, _w, status in rows):
        return "schwab_recent_rate_limit"
    if any(client == CLIENT_NAME and t >= now - MIN_RUN_GAP_SECONDS for t, client, _p, _w, _s in rows):
        return "schwab_recent_capture"
    return None


class SchwabOptionsClient:
    """Schwab chains behind the ``YahooOptionsClient.get_options`` contract.

    ``get_options(ticker)`` downloads the ticker's nearest expiries (one or
    two paced requests) and returns the nearest expiry page;
    ``get_options(ticker, expiry_ts)`` is served from that download.
    """

    provider = PROVIDER
    is_available = True  # configuration is checked by schwab_capture_hold before a run
    remote_expiry_pages = False

    def __init__(
        self, market: Any | None = None, *,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
        max_expirations: int = _options.MAX_EXPIRATIONS,
    ) -> None:
        self._market = market
        self._sleep = sleep
        self._monotonic = monotonic
        self._max_expirations = max_expirations
        self._last_request: float | None = None
        self._stopped: _options.OptionsSourceStopped | None = None
        self._ticker: str | None = None
        self._chain: dict[str, Any] | None = None
        self.last_no_chain = False
        self.requests_made = 0

    def _client(self) -> Any:
        if self._market is None:
            # max_retries=0: a refused call is never repeated inside the shared allowance.
            self._market = schwab_market.SchwabMarketClient(client=CLIENT_NAME, max_retries=0)
        return self._market

    def _stop(self, why: str, status: int | None) -> _options.OptionsSourceStopped:
        self._stopped = _options.OptionsSourceStopped(PROVIDER, status or 0, why)
        return self._stopped

    def _request(self, symbol: str, first_day: date, last_day: date) -> Any:
        if self._last_request is not None:
            wait = MIN_REQUEST_INTERVAL_SECONDS - (self._monotonic() - self._last_request)
            if wait > 0:
                self._sleep(wait)
        self._last_request = self._monotonic()
        self.requests_made += 1
        try:
            return self._client().option_chain(
                symbol, contractType="ALL", includeUnderlyingQuote="true", strategy="SINGLE",
                fromDate=first_day.isoformat(), toDate=last_day.isoformat(),
            )
        except (schwab_market.SchwabAuthError, schwab_market.SchwabConfigError):
            raise self._stop("not authorised (operator must re-run the login helper)", 401) from None
        except schwab_market.SchwabTransientError as exc:
            status = getattr(exc, "status", None)
            raise self._stop("rate limited" if status == 429 else "unavailable", status) from None

    def _page(self, chain: dict[str, Any], expiry_ts: int) -> dict[str, Any] | None:
        page = chain["pages"].get(expiry_ts)
        if page is None:
            return None
        return {"expirations": list(chain["expirations"]),
                "strikes": sorted({o["strike"] for o in page["calls"] + page["puts"]}),
                "calls": list(page["calls"]), "puts": list(page["puts"]),
                "quote": dict(chain["quote"])}

    def get_options(self, ticker: str, expiry_ts: int | None = None) -> Any:
        self.last_no_chain = False
        if self._stopped is not None:
            raise self._stopped
        if expiry_ts is not None:
            if self._ticker != ticker or self._chain is None:
                return None
            return self._page(self._chain, expiry_ts)

        self._ticker, self._chain = ticker, None
        if ticker not in _options.EQUITY_TICKERS:
            # pull_all never asks; a direct caller gets a failed fetch, not a request.
            log.warning("Schwab options {t}: not in the hand-listed core, not requested", t=ticker)
            return None
        today = _options._utc_now().astimezone(_options._EQUITY_TZ).date()
        symbol = schwab_symbol(ticker)
        bodies: list[Any] = []
        chain: dict[str, Any] | None = None
        for first_offset, last_offset in WINDOWS_DAYS:
            try:
                bodies.append(self._request(symbol, today + timedelta(days=first_offset),
                                            today + timedelta(days=last_offset)))
            except schwab_market.SchwabError as exc:
                # Another 4xx for this symbol only (bad symbol, bad request): a failed fetch.
                log.warning("Schwab options {t}: {kind} (HTTP {s})", t=ticker,
                            kind=type(exc).__name__, s=getattr(exc, "status", None))
                return None
            chain = schwab_chain_pages(bodies, ticker, today)
            if chain is None:
                log.warning("Schwab options {t}: chain body refused (not a complete chain for the symbol)", t=ticker)
                return None
            if len(chain["expirations"]) >= self._max_expirations:
                break
        if chain is None or not chain["expirations"]:
            return _options.NO_CHAIN
        if chain["skipped"]:
            log.info("Schwab options {t}: {n} non-standard or mismatched entries left out",
                     t=ticker, n=chain["skipped"])
        self._chain = chain
        return self._page(chain, chain["expirations"][0])
