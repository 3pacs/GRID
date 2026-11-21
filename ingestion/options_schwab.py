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

Yahoo names under this provider (re-review of #1299, 2026-11-21). On the
default path Yahoo serves only fallbacks; here it serves every non-core name
(about 160 on a full run), and Yahoo answered 429 to this server from
2026-09-29 into November. So, in this mode only:

* requests to Yahoo are at least ``YAHOO_MIN_REQUEST_INTERVAL_SECONDS`` apart;
* a non-core name is read to ``YAHOO_MAX_EXPIRATIONS`` expiries, not twelve;
* a run sends at most about ``YAHOO_RUN_REQUEST_BUDGET`` requests (1,000, enough
  for about 160 names at six expiries); names past it are SKIPPED
  ``yahoo_request_budget``. The caller's time budget can end a run sooner;
* a 429 latches for the run: the remaining expiries of that ticker are not
  requested (its capture is refused as incomplete) and the next non-core name
  stops the run (``OptionsSourceStopped("yahoo", 429)``);
* a 429, on a chain request or on Yahoo's crumb handshake, writes a "held
  until" time to a marker file in GRID's state directory and no run asks
  Yahoo again before it, ``YAHOO_RATE_LIMIT_HOLD_SECONDS`` later (SKIPPED
  ``yahoo_recent_rate_limit``). The file is created 0600 and holds a time and
  a status code only. Its directory is shared with other GRID state: it is
  created 0700 when missing and an existing one is left as it is. A marker
  that cannot be read or parsed, whatever is in it, is logged and treated as
  not held;
* after the cash close nothing is requested from either source.

What Schwab serves: ``ingestion.options.EQUITY_TICKERS`` only (the hand-listed
core). Every other ticker (the catalyst names, GEM) is never sent to Schwab:
``SchwabFirstOptionsClient`` reads it from Yahoo, one ticker at a time, as the
default path's fallback already does, and records the provider per ticker.
A batch never mixes providers. A core ticker is never retried on Yahoo. With
every name read or accounted for, a default-universe run can be complete, so
source freshness moves again (review of #1299, item 6; owner's word relayed
2026-11-21).

Shared allowance: the Schwab app's per-minute allowance is shared with the
live quote recorder (gex-watch), whose lost minutes cost live evidence
(``ingestion/schwab_usage.py``). So this client

* spaces its requests ``MIN_REQUEST_INTERVAL_SECONDS`` apart;
* never retries: a 429, 5xx, transport or auth failure stops the whole run
  (``OptionsSourceStopped``), exactly like an access denial, and later tickers
  are not requested. That includes a 401 that survives the client's one token
  refresh and any 403: every ticker sent to Schwab is a known-optionable core
  name, so a refusal cannot mean "no chain" (the reasoning of #1226's
  ``_is_access_denial``);
* does not read the core (``schwab_capture_hold``) when the shared call ledger
  shows any client refused with 429 in the last ``RATE_LIMIT_HOLD_SECONDS``,
  or shows the same writer's own calls in the last ``MIN_RUN_GAP_SECONDS`` (a
  repair loop cannot re-pull the universe every few minutes). The gap is per
  writer: each ``capture_source`` logs as ``grid-options/<capture_source>``,
  so the scheduler's pull does not hold back another writer's.

Session only: the chain's underlying carries ``last`` and ``tradeTime``, and
after the close those are an extended-hours trade. The regular-session fields
of ``/quotes`` do not help: measured 2026-11-21, ``regularMarketTradeTime`` was
stamped 20:00 New York for SPY and 19:00 for BRK/B, the end of the extended
session, so it cannot date a price to the regular session. So a spot price
counts only when its trade falls inside the regular session (09:30 to the cash
close) of the session day; anything else leaves the capture undated and it is
refused. ``OptionsPuller.pull_all`` does not ask Schwab at all after the cash
close (``schwab_core_block``: ``post-session``).

Request shape (measured 2026-11-21, market closed, 20 calls): one ``/chains``
call for today..+41 days, and a second for +42..+400 days only when the first
holds fewer than ``MAX_EXPIRATIONS`` expiries. SPY: 13 expiries, 6.7 MB, one
call. AAPL / NVDA / KO / BRK-B: 10 / 10 / 6 / 6 expiries in the first window,
so two calls, under 2 MB each. A default run is about 80 calls.

Fail closed: a body that is not ``status: SUCCESS`` for the requested symbol,
is marked truncated, or repeats a contract is a failed fetch (no rows). Only
standard contracts whose OCC symbol names the same root, expiry, side and
strike as the bucket they sit in (expiry key, call/put map, strike key) are
kept. IV is the provider's percent as a
fraction; a non-positive or sentinel value is missing, never zero.

Clocks: the underlying's ``last`` and its ``tradeTime`` date the capture (the
same reading the Cboe client takes); every contract keeps the provider's own
quote and last-trade times. Schwab data may not be redistributed: rows are
labelled ``audience: private`` in the batch clock metadata. That label is not
access control; ``store/options_provenance.py`` is: readers that serve
contributors, the e-mail digests, chat context and oracle publication leave
Schwab-provider rows out (admin requests see them), and a Schwab capture is
never written to ``resolved_series`` (review of #1299, item 3).
"""

from __future__ import annotations

import json
import math
import os
import re
import tempfile
import time
from collections.abc import Callable, Mapping
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from config import log, settings
from ingestion import options as _options
from ingestion import schwab_market, schwab_usage
from ingestion.market_calendar import market_close_time

PROVIDER = "schwab"
CLIENT_NAME = "grid-options"
REFUSAL_STATUSES: frozenset[int] = frozenset({401, 403})
YAHOO_MIN_REQUEST_INTERVAL_SECONDS = 1.0
# About 160 non-core names at YAHOO_MAX_EXPIRATIONS is up to 960 requests, so 1,000 lets a full run
# finish on requests. At one request a second that tail alone is about 16 minutes, which is longer
# than the 900 s the smart scheduler gives the options job (ingestion/smart_scheduler.py); under
# that job the time budget ends the run first and the remaining names are DEFERRED.
YAHOO_RUN_REQUEST_BUDGET = 1000
YAHOO_MAX_EXPIRATIONS = 6
YAHOO_RATE_LIMIT_HOLD_SECONDS = 30 * 60
# GRID's per-user state directory (the scheduler and the AV transcript ledger keep theirs there).
YAHOO_HOLD_FILE_DEFAULT = "~/.local/state/grid/options_yahoo_hold.json"
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


def writer_client(writer: str | None = None) -> str:
    """Call-ledger name of one options writer: ``grid-options/<capture_source>``."""
    return schwab_usage.client_name(f"{CLIENT_NAME}/{writer or _options.DEFAULT_CAPTURE_SOURCE}")


def regular_session(day: date) -> tuple[datetime, datetime]:
    """(open, cash close) of ``day``'s regular session, UTC."""
    opened = datetime(day.year, day.month, day.day, *_options.REGULAR_SESSION_OPEN_ET, tzinfo=_options._EQUITY_TZ)
    close = market_close_time(day)
    closed = datetime(day.year, day.month, day.day, close.hour, close.minute, tzinfo=_options._EQUITY_TZ)
    return opened.astimezone(timezone.utc), closed.astimezone(timezone.utc)


def _occ_root(ticker: str) -> str:
    return ticker.strip().upper().replace("-", "").replace(".", "").replace("/", "")


def _epoch_ms(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value <= 0:
        return None
    return int(value)


def _as_float(value: Any) -> Any:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
    session_open, session_close = regular_session(today)
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
            # An extended-hours or other-day trade cannot date a regular-session price.
            in_session = trade_ms is not None and (
                session_open.timestamp() <= trade_ms / 1000.0 <= session_close.timestamp())
            quote = {
                "regularMarketPrice": spot,
                "regularMarketTime": trade_ms / 1000.0 if in_session else None,
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
                    "underlying_trade_in_regular_session": in_session,
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
                for strike_key, entries in strikes.items():
                    bucket_strike = _options._finite(_as_float(strike_key))
                    if not isinstance(entries, list) or bucket_strike is None or bucket_strike <= 0:
                        return None
                    for entry in entries:
                        if not isinstance(entry, Mapping):
                            return None
                        parsed = _options.parse_occ_symbol(str(entry.get("symbol") or "").replace(" ", "").upper())
                        if (parsed is None or parsed[0] != root or parsed[1] != expiry
                                or (parsed[2] == "C") != is_call or abs(parsed[3] - bucket_strike) > 1e-6
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
    now_epoch: float | None = None, *, writer: str | None = None, usage_dir: str | Path | None = None,
    configured: Callable[[], bool] = schwab_market.is_configured,
) -> str | None:
    """Why ``writer`` must not read the core from Schwab now, or None to proceed.

    Reads the shared call ledger only; makes no request. An unreadable ledger
    reads as empty (the in-run stop rule still applies). Anyone's 429 holds
    every writer; a writer's own recent calls hold only that writer.
    """
    if not configured():
        return "schwab_not_configured"
    now = time.time() if now_epoch is None else now_epoch
    ledger = Path(usage_dir) if usage_dir is not None else schwab_usage.usage_dir(
        Path(settings.SCHWAB_TOKEN_FILE).expanduser())
    rows = schwab_usage.read(ledger, now - max(RATE_LIMIT_HOLD_SECONDS, MIN_RUN_GAP_SECONDS), now + 1)
    if any(status == "429" and t >= now - RATE_LIMIT_HOLD_SECONDS for t, _c, _p, _w, status in rows):
        return "schwab_recent_rate_limit"
    mine = writer_client(writer)
    if any(client == mine and t >= now - MIN_RUN_GAP_SECONDS for t, client, _p, _w, _s in rows):
        return "schwab_recent_capture"
    return None


def schwab_post_session(now: datetime | None = None) -> bool:
    """True once the cash close of ``now``'s New York day has passed."""
    moment = now if now is not None else _options._utc_now()
    return moment > regular_session(moment.astimezone(_options._EQUITY_TZ).date())[1]


def yahoo_hold_file() -> Path:
    """Marker of the last Yahoo 429 seen in schwab mode (GRID_OPTIONS_YAHOO_HOLD_FILE)."""
    return Path(os.getenv("GRID_OPTIONS_YAHOO_HOLD_FILE", "").strip() or YAHOO_HOLD_FILE_DEFAULT).expanduser()


def record_yahoo_rate_limit(now_epoch: float, path: str | Path | None = None) -> bool:
    """Write the marker: a UTC "held until" time and the status code, nothing else.

    The file is owner-only (mkstemp creates it 0600) and replaced atomically. The directory is
    created 0700 only when it does not exist; an existing shared state directory keeps its mode.
    True when written; never raises.
    """
    target = Path(path) if path is not None else yahoo_hold_file()
    until = datetime.fromtimestamp(now_epoch + YAHOO_RATE_LIMIT_HOLD_SECONDS, timezone.utc)
    try:
        target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        fd, temp = tempfile.mkstemp(dir=target.parent, prefix=".options-yahoo-hold-")
        try:
            with os.fdopen(fd, "w") as handle:
                json.dump({"held_until": until.strftime("%Y-%m-%dT%H:%M:%SZ"), "status": 429}, handle)
            os.replace(temp, target)
        except OSError:
            Path(temp).unlink(missing_ok=True)
            raise
        return True
    except OSError as exc:
        log.warning("options: Yahoo rate-limit marker not written ({kind})", kind=type(exc).__name__)
        return False


def yahoo_tail_hold(now_epoch: float | None = None, path: str | Path | None = None) -> str | None:
    """``yahoo_recent_rate_limit`` until the marker's "held until" time, else None.

    Fails open: no marker is no hold, and a marker that cannot be read or parsed (unreadable,
    not UTF-8, not JSON, nested past the parser's depth, wrong shape) is logged and treated as
    not held, so a bad file can never block options or abort a run (the in-run latch still
    applies). A "held until" further ahead than one hold from now is not one this code wrote
    and is ignored.
    """
    now = time.time() if now_epoch is None else now_epoch
    target = Path(path) if path is not None else yahoo_hold_file()
    try:
        raw = target.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as exc:
        log.warning("options: Yahoo rate-limit marker unreadable ({kind}); treated as not held", kind=type(exc).__name__)
        return None
    try:
        held_until = datetime.strptime(json.loads(raw.decode("utf-8"))["held_until"], "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc).timestamp()
    except Exception as exc:  # noqa: BLE001 -- any content at all must fail open (UnicodeDecodeError, RecursionError, ...)
        log.warning("options: Yahoo rate-limit marker is not in the expected form ({kind}); treated as not held",
                    kind=type(exc).__name__)
        return None
    return "yahoo_recent_rate_limit" if 0 < held_until - now <= YAHOO_RATE_LIMIT_HOLD_SECONDS else None


def schwab_core_block(now: datetime | None = None, *, writer: str | None = None) -> str | None:
    """Why this run must not send the core to Schwab, or None: after the cash close, or a ledger hold."""
    moment = now if now is not None else _options._utc_now()
    if schwab_post_session(moment):
        return "post-session"
    return schwab_capture_hold(moment.timestamp(), writer=writer)


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
        writer: str | None = None,
    ) -> None:
        self._market = market
        self.client_name = writer_client(writer)
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
            self._market = schwab_market.SchwabMarketClient(client=self.client_name, max_retries=0)
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
        except schwab_market.SchwabError as exc:
            # A 401 that survived the client's one refresh, or a 403: Schwab is refusing us. Every
            # ticker sent here is a known-optionable core name, so this is never "no chain".
            if getattr(exc, "status", None) in REFUSAL_STATUSES:
                raise self._stop("refused access", exc.status) from None
            raise

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


class SchwabFirstOptionsClient:
    """Schwab for the hand-listed core, Yahoo for every other ticker; one provider per ticker.

    A core ticker is served by Schwab only and is never retried on Yahoo. Any
    other ticker is never sent to Schwab: Yahoo serves it, as it does on the
    default path when it stands in for Cboe. Yahoo is initialised lazily (its
    crumb handshake costs two requests) and only when a non-core ticker comes
    up. A Schwab stop latches in the Schwab client and ends the run for every
    remaining ticker (``OptionsPuller.pull_all``).

    Yahoo is paced, budgeted and latched here (see the module docstring); the
    Yahoo client itself and the default path are unchanged.
    """

    is_available = True

    def __init__(
        self, schwab: SchwabOptionsClient | None = None, *, writer: str | None = None,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
        now: Callable[[], float] | None = None,
    ) -> None:
        self.schwab = schwab if schwab is not None else SchwabOptionsClient(writer=writer)
        self._yahoo: Any = None
        self._active: Any = self.schwab
        self._sleep = sleep if sleep is not None else time.sleep
        self._monotonic = monotonic if monotonic is not None else time.monotonic
        # The puller's own clock, so the marker's time and the next run's read agree.
        self._now = now if now is not None else (lambda: _options._utc_now().timestamp())
        self._yahoo_last: float | None = None
        self._yahoo_limited = False
        self.yahoo_requests = 0
        self.provider_by_ticker: dict[str, str] = {}

    @property
    def last_no_chain(self) -> bool:
        return getattr(self._active, "last_no_chain", False) is True

    @property
    def remote_expiry_pages(self) -> bool:
        return getattr(self._active, "remote_expiry_pages", True)

    @property
    def yahoo_rate_limited(self) -> bool:
        """True once Yahoo answered 429 in this run (a chain request or the crumb handshake)."""
        return self._yahoo_limited

    def _yahoo_limit_seen(self, where: str) -> None:
        self._yahoo_limited = True
        record_yahoo_rate_limit(self._now())
        log.warning("Yahoo options: HTTP 429 ({where}); no further Yahoo request this run", where=where)

    @property
    def yahoo_budget_spent(self) -> bool:
        """True once this run has sent its budget of Yahoo requests; later names are not started."""
        return self.yahoo_requests >= YAHOO_RUN_REQUEST_BUDGET

    def _yahoo_request(self, ticker: str, expiry_ts: int | None) -> Any:
        if self._yahoo_limited:
            return None   # the rest of the limited ticker: no request, its capture stays incomplete
        if self._yahoo_last is not None:
            wait = YAHOO_MIN_REQUEST_INTERVAL_SECONDS - (self._monotonic() - self._yahoo_last)
            if wait > 0:
                self._sleep(wait)
        self._yahoo_last = self._monotonic()
        self.yahoo_requests += 1
        page = self._yahoo.get_options(ticker, expiry_ts)
        if getattr(self._yahoo, "last_status", None) == 429:
            self._yahoo_limit_seen(f"chain request for {ticker}")
            return None
        return page

    def get_options(self, ticker: str, expiry_ts: int | None = None) -> Any:
        if expiry_ts is not None:
            if self._active is self._yahoo and self._yahoo is not None:
                return self._yahoo_request(ticker, expiry_ts)
            return self._active.get_options(ticker, expiry_ts)
        if ticker in _options.EQUITY_TICKERS:
            self._active = self.schwab
            self.provider_by_ticker[ticker] = PROVIDER
            return self.schwab.get_options(ticker)
        self.provider_by_ticker[ticker] = "yahoo"
        if self._yahoo_limited:
            # The next name after a 429: the run ends here, nothing is sent.
            raise _options.OptionsSourceStopped("yahoo", 429, "rate limited")
        if self._yahoo is None:
            self._yahoo = _options.YahooOptionsClient()
            if not self._yahoo.is_available and getattr(self._yahoo, "init_status", None) == 429:
                # Refused at the crumb handshake, the likeliest place to meet Yahoo's block.
                self._yahoo_limit_seen("crumb handshake")
        if not self._yahoo.is_available:
            self._active = self.schwab   # nothing to page through; the ticker is a failed fetch
            return None
        self._active = self._yahoo
        return self._yahoo_request(ticker, None)
