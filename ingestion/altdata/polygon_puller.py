"""
Polygon.io puller — stocks, options with Greeks, crypto, forex, dividends.

This is the Swiss Army knife data source. Polygon provides:
- Options snapshots with full Greeks (delta, gamma, theta, vega, IV)
- Stock snapshots (price, volume, VWAP, pre/post market)
- Ticker details (sector, SIC code, market cap, description)
- Dividends and stock splits
- Crypto tickers and forex pairs

API: https://polygon.io/docs (Polygon.io is now Massive; api.polygon.io still serves)
Free tier: 5 API calls/minute. Paid: unlimited.

Entitlement (probed from grid-svr 2026-11-04): the configured key gets
``403 NOT_AUTHORIZED`` on ``/v3/snapshot/options/{ticker}`` -- the options
snapshot (OI, volume, Greeks) needs an options plan. The free reference
endpoint lists contracts without OI or Greeks, so a "summary" built from it
is all zeros. ``pull()`` therefore stops on the first entitlement refusal and
reports SKIPPED with the operator action instead of storing zero-valued
Greeks for 30 tickers.

Re-probed 2026-11-23: the key now gets the snapshot with OI and Greeks for
all 30 tickers. The HTTP 400s logged since 2026-11-11 ("30 of 30 tickers
failed") were this module's own request: ``sort=volume`` is not a sort key
of this endpoint (Polygon answers ``Invalid value for sort parameter``; it
sorts only by ticker, expiration_date or strike_price), so "the 250 most
active contracts" was never available. The chain is now read whole inside a
fixed expiry window (today .. today + ``OPTIONS_WINDOW_DAYS``) by following
``next_url``; SPY is about 20 pages of 250. A 400 stops the run on the first
ticker with Polygon's own error text, instead of repeating it 30 times.

Review 6030035125 (GRID#1401) rules, 2026-11-23:

* **Default off.** ``pull()`` writes nothing unless
  ``POLYGON_OPTIONS_PULL_ENABLED`` is true. Unset (or false) means disabled,
  so deploying this module does not let Hermes's stale-source repair (key
  ``polygon``) start writing source 722; turning it on is a separate rollout
  decision.
* **A chain is stored whole or not at all.** Every page must be a JSON object
  with provider status OK/DELAYED and a ``results`` list. Every contract must
  name its option symbol, a call/put type, an expiry inside the window and
  this underlying. No option symbol may repeat. The cursor must stay on this
  endpoint and never repeat. Any violation fails that ticker and commits
  nothing for it.
* **No known-invalid values.** Absent open interest or IV is not zero, so a
  family whose inputs are incomplete is withheld. A zero call-OI denominator
  is not a ratio. Net gamma/delta are not emitted until a dealer-side sign
  convention is defined. The old net delta also negated put deltas that
  Polygon already signs. Every stored row carries the chain's coverage and
  provenance, plus the reasons for anything withheld.
* **Pacing and budget.** Requests are paced at most one per
  ``REQUEST_INTERVAL_S`` across every page, not per ticker. A run also stops
  cooperatively at ``RUN_BUDGET_SECONDS`` or when Hermes's ``should_continue``
  goes false. Unattempted tickers are returned for resumption
  (``ticker_list``).
* **Rate limits.** A 429 ends the run with a DEFERRED result
  (``rate_limited``), not FAILED. It records a hold honouring Retry-After
  (at least ``RATE_LIMIT_HOLD_SECONDS``, at most
  ``MAX_RATE_LIMIT_HOLD_SECONDS``) in ``GRID_POLYGON_HOLD_FILE``, so later
  runs make no request until the hold passes.
* **Shared plan budget.** The same key serves gex and research
  minute-history pulls. No live probe was made to measure the plan. The pace
  relies on two existing records: the documented tiers (free 5 requests/min,
  paid unlimited; config.py, docs/DATA_GAPS.md), and the 2026-11-23 read of
  all 30 chains (199 calls in 75 s, no 429), which shows the key is not on the
  5/min tier. At ``REQUEST_INTERVAL_S`` = 0.5 s this source asks for at most
  120 requests a minute, only while a run is active.

The key is sent as an ``Authorization: Bearer`` header, never as the
``apiKey`` query parameter: request URLs end up in exception text and logs.
"""

from __future__ import annotations

import email.utils
import json
import math
import os
import tempfile
import threading
import time
from collections.abc import Callable, Iterable
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller

POLYGON_BASE = "https://api.polygon.io"

# Polygon's documented status for a plan that does not include the endpoint.
_NOT_ENTITLED_STATUS = "NOT_AUTHORIZED"
_NOT_ENTITLED_REASON = (
    "Polygon/Massive plan is not entitled to /v3/snapshot/options "
    "(HTTP 403 NOT_AUTHORIZED); options Greeks need an options plan "
    "upgrade, or retire this source"
)
_entitlement_warned = False

_DISABLED_REASON = (
    "POLYGON_OPTIONS_PULL_ENABLED is off: automatic Polygon options writes "
    "wait for a separate rollout decision (GRID#1401)"
)

# Top tickers for options Greeks tracking
OPTIONS_TICKERS = [
    "SPY", "QQQ", "IWM", "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA",
    "AMD", "JPM", "GS", "BAC", "XLF", "XLE", "XLK", "XLV", "GLD", "SLV",
    "TLT", "HYG", "VIX", "COIN", "MARA", "PLTR", "CRWD", "SMCI", "ARM", "NFLX",
]


# Options chain window and paging. The summary covers every contract that
# expires today through today + OPTIONS_WINDOW_DAYS, read whole: on
# 2026-11-23 that was 20 pages for SPY, 21 for QQQ and 199 calls for all 30
# tickers. MAX_CHAIN_PAGES bounds a runaway cursor; a chain that needs more
# pages is not summarized at all (a truncated chain is not the window).
OPTIONS_WINDOW_DAYS = 35
CHAIN_PAGE_LIMIT = 250
MAX_CHAIN_PAGES = 60
SUMMARY_METHOD = "polygon-snapshot-window/3"
_CHAIN_PATH = "/v3/snapshot/options/"
# Polygon answers OK, or DELAYED on plans with delayed data; anything else
# (ERROR, a missing status) is not a page of this chain.
_PAGE_STATUSES = frozenset({"OK", "DELAYED"})

# Pacing and run budget. Hermes joins a repair for at most
# PER_SOURCE_REPAIR_SECONDS (60 s; scripts/hermes_fixers.py), and a worker
# still running then is abandoned. A run therefore stops starting requests
# at RUN_BUDGET_SECONDS, so one in-flight request (REQUEST_TIMEOUT_S) and one
# ticker's insert still finish inside that cap. A stopped run returns its
# unattempted tickers, and Hermes resumes from them on the next pass.
REQUEST_INTERVAL_S = 0.5
REQUEST_TIMEOUT_S = 10.0
RUN_BUDGET_SECONDS = 40.0

# Rate-limit hold across runs (same shape as the Yahoo hold in
# ingestion/options_schwab.py). A 429 holds every later run for
# max(Retry-After, RATE_LIMIT_HOLD_SECONDS), capped at
# MAX_RATE_LIMIT_HOLD_SECONDS.
RATE_LIMIT_HOLD_SECONDS = 900.0
MAX_RATE_LIMIT_HOLD_SECONDS = 6 * 3600.0
HOLD_FILE_DEFAULT = "~/.local/state/grid/polygon_options_hold.json"
_HOLD_LOCK = threading.Lock()
_held_until_epoch = 0.0  # in-process latch, used even if the file cannot be written

# What each stored family means. Rows are written only for these.
FAMILY_DEFINITIONS = {
    "put_call_ratio": "put open interest / call open interest over the expiry window",
    "avg_iv": ("day-volume-weighted mean implied volatility over window contracts "
               "that carry both an implied volatility and day volume"),
    "call_oi": "sum of open interest over call contracts in the expiry window",
    "put_oi": "sum of open interest over put contracts in the expiry window",
}
_NOT_EMITTED = {
    "net_gamma": "not emitted: no dealer-side sign convention is defined for this source",
    "net_delta": "not emitted: no dealer-side sign convention is defined for this source",
}


def options_writes_enabled() -> bool:
    """True only when POLYGON_OPTIONS_PULL_ENABLED is set true; unset is off."""
    from config import settings

    return getattr(settings, "POLYGON_OPTIONS_PULL_ENABLED", False) is True


class PolygonNotEntitled(RuntimeError):
    """The API key's plan does not include the requested endpoint (permanent)."""


class _PolygonHTTPError(RuntimeError):
    """Non-2xx Polygon response; the message never contains the request URL."""

    def __init__(self, status_code: int, endpoint: str, detail: str = "") -> None:
        self.status_code = status_code
        suffix = f": {detail}" if detail else ""
        super().__init__(f"Polygon HTTP {status_code} on {endpoint}{suffix}")


class PolygonBadRequest(_PolygonHTTPError):
    """HTTP 400: the request itself is wrong, so every ticker would fail alike."""


class PolygonRateLimited(_PolygonHTTPError):
    """HTTP 429: stop the run, hold later runs, do not keep calling."""

    def __init__(self, status_code: int, endpoint: str, detail: str = "",
                 retry_after_s: float | None = None) -> None:
        super().__init__(status_code, endpoint, detail)
        self.retry_after_s = retry_after_s


class PolygonChainTruncated(RuntimeError):
    """The chain needed more than MAX_CHAIN_PAGES pages (not summarized)."""


class PolygonChainInvalid(RuntimeError):
    """A page, contract or cursor broke the chain's integrity (not summarized)."""


class _RunStopped(Exception):
    """The run's budget ran out or the caller asked it to stop."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _error_detail(resp: Any, secret: str) -> str:
    """Polygon's own error text from a JSON body, short and without the key."""
    try:
        body = resp.json() or {}
    except ValueError:
        return ""
    if not isinstance(body, dict):
        return ""
    text = str(body.get("error") or body.get("message") or "")[:200]
    return text.replace(secret, "<key>") if secret else text


def _retry_after_seconds(resp: Any, now_epoch: float | None = None) -> float | None:
    """Retry-After as seconds (delta-seconds or HTTP-date), or None when absent or unreadable."""
    headers = getattr(resp, "headers", None) or {}
    raw = headers.get("Retry-After") or headers.get("retry-after")
    if raw is None:
        return None
    try:
        seconds = float(str(raw).strip())
    except ValueError:
        try:
            when = email.utils.parsedate_to_datetime(str(raw))
        except (TypeError, ValueError):
            return None
        if when is None:
            return None
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        seconds = when.timestamp() - (time.time() if now_epoch is None else now_epoch)
    if not math.isfinite(seconds):
        return None
    return max(0.0, seconds)


def hold_file() -> Path:
    """Marker of the last Polygon 429 (GRID_POLYGON_HOLD_FILE)."""
    return Path(os.getenv("GRID_POLYGON_HOLD_FILE", "").strip() or HOLD_FILE_DEFAULT).expanduser()


def record_rate_limit(now_epoch: float, retry_after_s: float | None,
                      path: str | Path | None = None) -> float:
    """Hold later runs after a 429; returns the held-until epoch. Never raises.

    The hold is max(Retry-After, RATE_LIMIT_HOLD_SECONDS), capped at
    MAX_RATE_LIMIT_HOLD_SECONDS. The file holds only a UTC time, the status
    and the Retry-After seconds. It is owner-only (mkstemp) and replaced
    atomically. The in-process latch is set even when the file cannot be
    written.
    """
    global _held_until_epoch
    hold = max(RATE_LIMIT_HOLD_SECONDS, retry_after_s or 0.0)
    until = now_epoch + min(hold, MAX_RATE_LIMIT_HOLD_SECONDS)
    with _HOLD_LOCK:
        # A later 429 never shortens a hold this process already holds.
        _held_until_epoch = max(_held_until_epoch, until)
        until = _held_until_epoch
    target = Path(path) if path is not None else hold_file()
    payload = {
        "held_until": datetime.fromtimestamp(until, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "status": 429,
        "retry_after_s": None if retry_after_s is None else round(retry_after_s, 1),
    }
    try:
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(dir=target.parent, prefix=".polygon-options-hold-")
        try:
            with os.fdopen(fd, "w") as handle:
                json.dump(payload, handle)
            os.replace(temp, target)
        except OSError:
            Path(temp).unlink(missing_ok=True)
            raise
    except OSError as exc:
        log.warning("Polygon rate-limit hold not written ({kind}); in-process hold only",
                    kind=type(exc).__name__)
    return until


def rate_limit_hold(now_epoch: float | None = None, path: str | Path | None = None) -> float | None:
    """The held-until epoch while a 429 hold is active, else None.

    Like the Yahoo marker this fails open. No file means no hold. A file that
    cannot be read or parsed is logged and treated as no hold, and so is a
    time further ahead than MAX_RATE_LIMIT_HOLD_SECONDS (not one this code
    wrote). The in-process latch applies either way.
    """
    now = time.time() if now_epoch is None else now_epoch
    with _HOLD_LOCK:
        latched = _held_until_epoch
    held: float | None = latched if latched > now else None
    target = Path(path) if path is not None else hold_file()
    try:
        raw = target.read_bytes()
    except FileNotFoundError:
        return held
    except OSError as exc:
        log.warning("Polygon rate-limit hold unreadable ({kind}); treated as not held",
                    kind=type(exc).__name__)
        return held
    try:
        until = datetime.strptime(json.loads(raw.decode("utf-8"))["held_until"],
                                  "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc).timestamp()
    except Exception as exc:  # noqa: BLE001 -- any content must fail open
        log.warning("Polygon rate-limit hold is not in the expected form ({kind}); treated as not held",
                    kind=type(exc).__name__)
        return held
    if 0 < until - now <= MAX_RATE_LIMIT_HOLD_SECONDS:
        return max(until, held or 0.0)
    return held


def _inserted(result: Any) -> int:
    """Committed-row contribution of one INSERT; unknown counts fail closed."""
    affected = getattr(result, "rowcount", None)
    if type(affected) is not int or affected < 0:
        raise ValueError("Unknown affected row count")
    return affected


def _measure(value: Any) -> float | None:
    """A finite, non-negative number, or None (absent is not zero)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    number = float(value)
    return number if math.isfinite(number) and number >= 0 else None


def _validate_contract(contract: Any, ticker: str, gte: date, lte: date, where: str) -> tuple[str, str]:
    """(option symbol, call/put) of one snapshot contract; raises PolygonChainInvalid."""
    if not isinstance(contract, dict):
        raise PolygonChainInvalid(f"{where}: a contract is not an object")
    details = contract.get("details")
    if not isinstance(details, dict):
        raise PolygonChainInvalid(f"{where}: a contract has no details")
    symbol = details.get("ticker")
    if not isinstance(symbol, str) or not symbol:
        raise PolygonChainInvalid(f"{where}: a contract has no option symbol")
    kind = details.get("contract_type")
    if kind not in ("call", "put"):
        raise PolygonChainInvalid(f"{where}: {symbol} has contract type {kind!r}")
    try:
        expiry = date.fromisoformat(str(details.get("expiration_date")))
    except ValueError:
        raise PolygonChainInvalid(f"{where}: {symbol} has no readable expiry") from None
    if not gte <= expiry <= lte:
        raise PolygonChainInvalid(f"{where}: {symbol} expires {expiry}, outside {gte}..{lte}")
    underlying = contract.get("underlying_asset")
    underlying_ticker = underlying.get("ticker") if isinstance(underlying, dict) else None
    # Index options (VIX) name their underlying with Polygon's "I:" prefix.
    if underlying_ticker not in (ticker, f"I:{ticker}"):
        raise PolygonChainInvalid(f"{where}: {symbol} has underlying {underlying_ticker!r}, not {ticker}")
    return symbol, kind


def summarize_chain(ticker: str, chain: list[dict[str, Any]], paging: dict[str, Any]) -> dict[str, Any]:
    """Families with a defined meaning and complete inputs, what was withheld and why, and coverage.

    ``chain`` must already have passed ``_validate_contract`` (every contract
    is a call or a put inside the window, with no repeats).
    """
    calls = [c for c in chain if c["details"]["contract_type"] == "call"]
    puts = [c for c in chain if c["details"]["contract_type"] == "put"]
    families: dict[str, float] = {}
    withheld: dict[str, str] = dict(_NOT_EMITTED)

    def _oi_family(name: str, side: list[dict[str, Any]], label: str) -> None:
        values = [_measure(c.get("open_interest")) for c in side]
        missing = sum(v is None for v in values)
        if not side:
            withheld[name] = f"no {label} contracts in the window"
        elif missing:
            withheld[name] = f"{missing} of {len(side)} {label} contracts have no open interest"
        else:
            families[name] = float(sum(v for v in values if v is not None))

    _oi_family("call_oi", calls, "call")
    _oi_family("put_oi", puts, "put")
    if "call_oi" in families and "put_oi" in families:
        if families["call_oi"] > 0:
            families["put_call_ratio"] = round(families["put_oi"] / families["call_oi"], 4)
        else:
            withheld["put_call_ratio"] = "call open interest is 0: the ratio is undefined"
    else:
        withheld["put_call_ratio"] = "open interest is incomplete"

    iv_sum = 0.0
    iv_weight = 0.0
    with_iv = with_volume = with_greeks = with_oi = 0
    for contract in chain:
        iv = _measure(contract.get("implied_volatility"))
        day = contract.get("day")
        volume = _measure(day.get("volume")) if isinstance(day, dict) else None
        greeks = contract.get("greeks")
        if isinstance(greeks, dict) and all(
                isinstance(greeks.get(k), (int, float)) and not isinstance(greeks.get(k), bool)
                and math.isfinite(greeks[k]) for k in ("delta", "gamma")):
            with_greeks += 1
        with_oi += _measure(contract.get("open_interest")) is not None
        with_iv += iv is not None and iv > 0
        with_volume += volume is not None
        if iv and volume:
            iv_sum += iv * volume
            iv_weight += volume
    if iv_weight > 0:
        families["avg_iv"] = round(iv_sum / iv_weight, 4)
    else:
        withheld["avg_iv"] = "no contract in the window has both an implied volatility and day volume"

    coverage = {
        "method": SUMMARY_METHOD,
        "expiration_gte": paging["expiration_gte"],
        "expiration_lte": paging["expiration_lte"],
        "pages": paging["pages"],
        "provider_statuses": paging["provider_statuses"],
        "contracts_count": len(chain),
        "call_contracts": len(calls),
        "put_contracts": len(puts),
        "contracts_with_oi": with_oi,
        "contracts_with_iv": with_iv,
        "contracts_with_volume": with_volume,
        "contracts_with_greeks": with_greeks,
        "iv_weight_volume": iv_weight,
    }
    for name in families:
        withheld.pop(name, None)
    return {"ticker": ticker, "families": families, "withheld": withheld, "coverage": coverage}


class PolygonPuller(BasePuller):
    """Pull stocks, options Greeks, crypto, and reference data from Polygon.io."""

    SOURCE_NAME = "polygon"
    SOURCE_CONFIG = {
        "base_url": POLYGON_BASE,
        "cost_tier": "PAID",
        "latency_class": "REALTIME",
        "pit_available": True,
        "revision_behavior": "NEVER",
        "trust_score": "HIGH",
        "priority_rank": 10,
    }

    def __init__(self, db_engine: Engine, api_key: str | None = None) -> None:
        super().__init__(db_engine)
        if api_key:
            self.api_key = api_key
        else:
            from config import settings
            self.api_key = getattr(settings, "POLYGON_API_KEY", "")
        if not self.api_key:
            log.warning("POLYGON_API_KEY not set — Polygon puller disabled")

    def _api_get(self, endpoint: str, params: dict[str, Any] | None = None) -> dict[str, Any]:
        """Make a Polygon API call (key in the Authorization header).

        Raises:
            PolygonNotEntitled: 403 whose body says the plan lacks the endpoint.
            PolygonBadRequest: 400, with Polygon's error text.
            PolygonRateLimited: 429, with Retry-After when given.
            _PolygonHTTPError: any other non-2xx status, or a body that is not JSON.
        """
        return self._get(f"{POLYGON_BASE}{endpoint}", params, endpoint)

    def _pace(self) -> None:
        """At most one request per REQUEST_INTERVAL_S, pages included."""
        last = getattr(self, "_last_request_at", None)
        if last is not None:
            wait = last + REQUEST_INTERVAL_S - time.monotonic()
            if wait > 0:
                time.sleep(wait)
        self._last_request_at = time.monotonic()

    def _check_stop(self) -> None:
        """Raise _RunStopped when the caller's stop or this run's budget says so."""
        should_continue = getattr(self, "_should_continue", None)
        if should_continue is not None and not should_continue():
            raise _RunStopped("stop_requested")
        deadline = getattr(self, "_deadline", None)
        if deadline is not None and time.monotonic() >= deadline:
            raise _RunStopped("run_budget")

    def _get(self, url: str, params: dict[str, Any] | None, endpoint: str) -> dict[str, Any]:
        """One paced GET against ``POLYGON_BASE``; ``endpoint`` is the label used in errors.

        No in-call retry: a run must not outlive its budget, and a ticker
        that fails here is reported failed for this run.
        """
        if not url.startswith(f"{POLYGON_BASE}/"):
            # A next_url pointing anywhere else never gets the key.
            raise _PolygonHTTPError(0, endpoint, "refused a URL outside api.polygon.io")
        self._pace()
        self._api_calls = getattr(self, "_api_calls", 0) + 1
        resp = requests.get(
            url,
            params=params or None,
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=REQUEST_TIMEOUT_S,
        )
        if resp.status_code == 403:
            try:
                status = (resp.json() or {}).get("status")
            except ValueError:
                status = None
            if status == _NOT_ENTITLED_STATUS:
                raise PolygonNotEntitled(f"{endpoint}: {_NOT_ENTITLED_STATUS}")
        if resp.status_code == 400:
            raise PolygonBadRequest(400, endpoint, _error_detail(resp, self.api_key))
        if resp.status_code == 429:
            raise PolygonRateLimited(429, endpoint, _error_detail(resp, self.api_key),
                                     retry_after_s=_retry_after_seconds(resp))
        if resp.status_code >= 400:
            raise _PolygonHTTPError(resp.status_code, endpoint)
        try:
            return resp.json()
        except ValueError:
            raise _PolygonHTTPError(resp.status_code, endpoint, "response body is not JSON") from None

    # ------------------------------------------------------------------
    # Options with Greeks
    # ------------------------------------------------------------------

    def pull_options_chain(self, ticker: str, as_of: date | None = None) -> list[dict[str, Any]]:
        """Pull the options chain snapshot with Greeks for a ticker.

        Returns options contracts with: strike, expiry, type (call/put),
        bid, ask, last, volume, OI, IV, delta, gamma, theta, vega.

        Uses ``/v3/snapshot/options`` (options plan). There is deliberately
        no fallback to ``/v3/reference/options/contracts``: that list has no
        open interest, volume or Greeks, so every aggregate computed from it
        is zero -- not a measurement.

        Raises:
            PolygonNotEntitled: the key's plan lacks the snapshot endpoint.
            PolygonChainTruncated: the window needed more than MAX_CHAIN_PAGES.
            PolygonChainInvalid: a page, contract or cursor failed validation.
        """
        return self._options_chain_window(ticker, as_of or date.today())[0]

    def _options_chain_window(self, ticker: str, as_of: date) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        """Every contract expiring as_of .. as_of + OPTIONS_WINDOW_DAYS, validated, and paging facts.

        Each page is checked before its contracts count. The checks are:
        provider status and a ``results`` list; each contract's symbol, type,
        expiry and underlying; no repeated symbol; a cursor that stays on this
        endpoint and never repeats. A failure raises, so nothing from this
        ticker is summarized or stored.
        """
        endpoint = f"{_CHAIN_PATH}{ticker}"
        first_url = f"{POLYGON_BASE}{endpoint}"
        gte, lte = as_of, as_of + timedelta(days=OPTIONS_WINDOW_DAYS)
        params: dict[str, Any] | None = {
            "limit": CHAIN_PAGE_LIMIT,
            "expiration_date.gte": gte.isoformat(),
            "expiration_date.lte": lte.isoformat(),
        }
        url: str | None = first_url
        seen_urls = {first_url}
        symbols: set[str] = set()
        statuses: set[str] = set()
        contracts: list[dict[str, Any]] = []
        pages = 0
        while url:
            if pages >= MAX_CHAIN_PAGES:
                raise PolygonChainTruncated(
                    f"{endpoint}: more than {MAX_CHAIN_PAGES} pages of {CHAIN_PAGE_LIMIT}")
            self._check_stop()
            # next_url carries the cursor and the original filters; the key
            # still travels only in the header.
            data = self._get(url, params, endpoint)
            pages += 1
            params = None
            where = f"{endpoint} page {pages}"
            if not isinstance(data, dict):
                raise PolygonChainInvalid(f"{where}: body is not an object")
            status = data.get("status")
            if status not in _PAGE_STATUSES:
                raise PolygonChainInvalid(f"{where}: provider status {status!r}")
            statuses.add(status)
            results = data.get("results")
            if not isinstance(results, list):
                raise PolygonChainInvalid(f"{where}: no results list")
            next_url = data.get("next_url")
            if next_url in (None, ""):
                next_url = None
            elif not isinstance(next_url, str):
                raise PolygonChainInvalid(f"{where}: next_url is not a string")
            if not results and (pages > 1 or next_url):
                raise PolygonChainInvalid(f"{where}: empty page inside a paged chain")
            for contract in results:
                symbol, _kind = _validate_contract(contract, ticker, gte, lte, where)
                if symbol in symbols:
                    raise PolygonChainInvalid(f"{where}: {symbol} repeats")
                symbols.add(symbol)
                contracts.append(contract)
            if next_url is not None:
                if not next_url.startswith(f"{first_url}?"):
                    raise PolygonChainInvalid(f"{where}: cursor leaves {endpoint}")
                if next_url in seen_urls:
                    raise PolygonChainInvalid(f"{where}: cursor repeats")
                seen_urls.add(next_url)
            url = next_url
        paging = {
            "pages": pages,
            "provider_statuses": sorted(statuses),
            "expiration_gte": gte.isoformat(),
            "expiration_lte": lte.isoformat(),
        }
        return contracts, paging

    def pull_options_greeks_summary(self, ticker: str, as_of: date | None = None) -> dict[str, Any]:
        """Validated chain summary for a ticker: families, withheld reasons, coverage.

        Returns {} when the window holds no contracts. See summarize_chain
        for which families are emitted and when one is withheld.
        """
        as_of = as_of or date.today()
        chain, paging = self._options_chain_window(ticker, as_of)
        if not chain:
            return {}
        return summarize_chain(ticker, chain, paging)

    # ------------------------------------------------------------------
    # Stock snapshots
    # ------------------------------------------------------------------

    def pull_stock_snapshot(self, ticker: str) -> dict[str, Any] | None:
        """Pull current stock snapshot (price, volume, VWAP)."""
        data = self._api_get(f"/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}")
        return data.get("ticker", None)

    def pull_market_snapshot(self) -> list[dict[str, Any]]:
        """Pull snapshot of all US stock tickers."""
        data = self._api_get("/v2/snapshot/locale/us/markets/stocks/tickers")
        return data.get("tickers", [])

    # ------------------------------------------------------------------
    # Dividends and splits
    # ------------------------------------------------------------------

    def pull_dividends(self, ticker: str) -> list[dict[str, Any]]:
        """Pull dividend history."""
        data = self._api_get("/v3/reference/dividends", {
            "ticker": ticker, "limit": 50, "order": "desc",
        })
        return data.get("results", [])

    # ------------------------------------------------------------------
    # Ticker details
    # ------------------------------------------------------------------

    def pull_ticker_details(self, ticker: str) -> dict[str, Any] | None:
        """Pull ticker metadata (sector, industry, market cap, description)."""
        data = self._api_get(f"/v3/reference/tickers/{ticker}")
        return data.get("results", None)

    # ------------------------------------------------------------------
    # Main pull
    # ------------------------------------------------------------------

    def pull(
        self,
        should_continue: Callable[[], bool] | None = None,
        ticker_list: Iterable[str] | None = None,
        max_seconds: float | None = RUN_BUDGET_SECONDS,
    ) -> dict[str, Any]:
        """Pull options summaries for tracked tickers (off unless POLYGON_OPTIONS_PULL_ENABLED).

        Args:
            should_continue: Hermes's cooperative stop, checked before every
                request; when it goes false the run stops and reports
                ``stopped_by_budget``.
            ticker_list: resume list (Hermes's repair backlog). Names outside
                OPTIONS_TICKERS are ignored; an empty result means all.
            max_seconds: this run's own budget (None = no budget).

        Returns:
            Write-contract summary: ``status`` plus ``rows_inserted`` (rows
            committed to raw_series) and per-ticker coverage counts. A
            stopped or rate-limited run also carries ``stopped_by_budget``
            and ``tickers_not_attempted``, so Hermes keeps the backlog.
        """
        global _entitlement_warned
        if not options_writes_enabled():
            return {"status": "SKIPPED", "rows_inserted": 0, "skipped_reason": _DISABLED_REASON}
        if not self.api_key:
            return {"status": "SKIPPED", "rows_inserted": 0,
                    "skipped_reason": "POLYGON_API_KEY not configured"}

        requested = [t for t in (ticker_list or []) if t in OPTIONS_TICKERS]
        tickers = requested or list(OPTIONS_TICKERS)
        held_until = rate_limit_hold()
        if held_until is not None:
            return self._rate_limited_result(tickers, held_until, rows_inserted=0, api_calls=0,
                                             note="held before any request")

        today = date.today()
        rows_inserted = 0
        succeeded = 0
        no_data = 0
        failed: list[str] = []
        not_attempted: list[str] = []
        stop_reason: str | None = None
        withheld_counts: dict[str, int] = {}
        self._api_calls = 0
        self._last_request_at = None
        self._should_continue = should_continue
        self._deadline = None if max_seconds is None else time.monotonic() + max_seconds
        anomalies: list[dict[str, Any]] = []

        try:
            for idx, ticker in enumerate(tickers):
                try:
                    summary = self.pull_options_greeks_summary(ticker, as_of=today)
                except _RunStopped as stop:
                    stop_reason = stop.reason
                    not_attempted = tickers[idx:]
                    break
                except PolygonNotEntitled:
                    if succeeded or rows_inserted:
                        # Entitlement changed mid-run: keep what was committed.
                        failed.extend(tickers[idx:])
                        break
                    if not _entitlement_warned:
                        log.warning("Polygon: {r}", r=_NOT_ENTITLED_REASON)
                        _entitlement_warned = True
                    return {"status": "SKIPPED", "rows_inserted": 0,
                            "skipped_reason": _NOT_ENTITLED_REASON, "api_calls": self._api_calls}
                except PolygonBadRequest as exc:
                    if not (succeeded or rows_inserted or no_data):
                        # The request is wrong for every ticker alike: one call, not 30.
                        log.warning("Polygon options request rejected on {t}: {e}", t=ticker, e=str(exc))
                        return {"status": "FAILED", "rows_inserted": 0, "api_calls": self._api_calls,
                                "total": len(tickers), "tickers_failed": len(tickers),
                                "error": f"request rejected (HTTP 400): {exc}"}
                    failed.append(ticker)
                    log.warning("Polygon options failed for {t}: {e}", t=ticker, e=str(exc))
                    continue
                except PolygonRateLimited as exc:
                    until = record_rate_limit(time.time(), exc.retry_after_s)
                    log.warning("Polygon rate-limited on {t}; holding later runs: {e}", t=ticker, e=str(exc))
                    return self._rate_limited_result(
                        tickers[idx:], until, rows_inserted=rows_inserted, api_calls=self._api_calls,
                        note=f"HTTP 429 on {ticker}", pulled=succeeded, no_data=no_data, failed=failed)
                except Exception as exc:
                    failed.append(ticker)
                    log.warning("Polygon options failed for {t}: {e}", t=ticker, e=str(exc))
                    continue

                if not summary or not summary["families"]:
                    no_data += 1
                    continue
                for name in summary["withheld"]:
                    withheld_counts[name] = withheld_counts.get(name, 0) + 1

                try:
                    ticker_rows = 0
                    with self.engine.begin() as conn:
                        for suffix in ("put_call_ratio", "avg_iv", "call_oi", "put_oi"):
                            if suffix not in summary["families"]:
                                continue
                            payload = {
                                "family": suffix,
                                "definition": FAMILY_DEFINITIONS[suffix],
                                **summary["coverage"],
                                "withheld": summary["withheld"],
                            }
                            ticker_rows += _inserted(self._insert_raw(
                                conn,
                                series_id=f"polygon:{ticker}:{suffix}",
                                obs_date=today,
                                value=summary["families"][suffix],
                                raw_payload=payload,
                                return_result=True,
                            ))
                    # Counted only after the transaction committed.
                    rows_inserted += ticker_rows
                    succeeded += 1
                except Exception as exc:
                    failed.append(ticker)
                    log.warning("Polygon store failed for {t}: {e}", t=ticker, e=str(exc))
                    continue

                pcr = summary["families"].get("put_call_ratio")
                if pcr is not None and pcr > 1.5:
                    anomalies.append({
                        "ticker": ticker, "signal": "extreme_put_call",
                        "put_call_ratio": pcr, "direction": "bearish",
                    })
                elif pcr is not None and 0 < pcr < 0.5:
                    anomalies.append({
                        "ticker": ticker, "signal": "extreme_put_call",
                        "put_call_ratio": pcr, "direction": "bullish",
                    })
        finally:
            self._should_continue = None
            self._deadline = None

        log.info("Polygon: {ok}/{n} tickers, {r} rows, {a} anomalies, {api} API calls{s}",
                 ok=succeeded, n=len(tickers), r=rows_inserted, a=len(anomalies),
                 api=self._api_calls, s=f", stopped ({stop_reason})" if stop_reason else "")

        total = len(tickers)
        if stop_reason or failed:
            status = "PARTIAL" if (succeeded or rows_inserted or stop_reason) else "FAILED"
        else:
            status = "SUCCESS"
        result: dict[str, Any] = {
            "status": status,
            "rows_inserted": rows_inserted,
            "succeeded": succeeded + no_data,
            "total": total,
            "tickers_pulled": succeeded,
            "tickers_no_data": no_data,
            "tickers_failed": len(failed),
            "failed_symbols": failed,
            "withheld_families": withheld_counts,
            "anomalies": anomalies,
            "api_calls": self._api_calls,
        }
        if stop_reason:
            result.update(stopped_by_budget=True, stop_reason=stop_reason,
                          tickers_not_attempted=not_attempted)
        if failed:
            result["error"] = f"{len(failed)} of {total} tickers failed"
        return result

    def _rate_limited_result(self, remaining: list[str], held_until: float, *, rows_inserted: int,
                             api_calls: int, note: str, pulled: int = 0, no_data: int = 0,
                             failed: list[str] | None = None) -> dict[str, Any]:
        """DEFERRED, not FAILED. The remaining tickers stay in Hermes's backlog."""
        until = datetime.fromtimestamp(held_until, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        return {
            "status": "DEFERRED",
            "rate_limited": True,
            "held_until": until,
            "skipped_reason": f"Polygon rate limit (HTTP 429), {note}; no request until {until}",
            "rows_inserted": rows_inserted,
            "api_calls": api_calls,
            "tickers_pulled": pulled,
            "tickers_no_data": no_data,
            "tickers_failed": len(failed or []),
            "failed_symbols": list(failed or []),
            "stopped_by_budget": True,
            "stop_reason": "rate_limited",
            "tickers_not_attempted": list(remaining),
        }
