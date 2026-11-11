"""GRID — Options chain ingestion (Cboe delayed quotes, Yahoo fallback).

Pulls options chains for equity tickers, computes daily signals
(put/call ratio, max pain, IV skew, total OI, vol surface metrics),
and pushes to resolved_series for PIT-correct access.

Primary provider since 2026-11-06: Cboe's delayed-quotes JSON (one request
per ticker, ~15 min delayed, full OPRA chain). Yahoo Finance's crumb+cookie
API is the per-ticker fallback when Cboe fails in transport. The
source_catalog row is still named YFINANCE_OPTIONS (id wired through the
smart scheduler and Hermes); the per-ticker provider is logged.
"""

from __future__ import annotations

import math
import os
import re
import time
from datetime import date, datetime, timezone
from typing import Any, Callable
from uuid import uuid4
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
from loguru import logger as log
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool

from ingestion.base import BasePuller
from ingestion.market_calendar import is_market_open

# Tickers with listed equity options
EQUITY_TICKERS: list[str] = [
    "SPY", "QQQ", "IWM",           # Index ETFs
    "AAPL", "MSFT", "NVDA", "TSLA", "AMZN", "META", "GOOGL", "AVGO",  # Mega-cap tech
    "JPM", "BAC", "GS", "BLK", "UNH", "BRK-B",  # Financials/Insurance
    "JNJ", "PFE", "MRK", "ABBV", "LLY", "TMO",   # Healthcare
    "EOG", "DVN", "XOM",           # Energy
    "CMCSA", "CI", "PYPL", "CRM", "V", "MA",      # Software/Payments
    "RTX", "GD", "HD", "COST", "PG", "KO", "PEP", # Industrials/Staples
    "INTC", "AMD",                  # Semis
]

# Maximum expirations to pull per ticker
MAX_EXPIRATIONS = 12
MAX_CAPTURE_SECONDS = 120  # each in-flight Yahoo request also has a 15s timeout
# Final catalog publication has no provider work. Short connection/statement
# limits and a local transaction deadline fit inside the scheduler's 60s margin.
CATALOG_PUBLICATION_SECONDS = 15
_EQUITY_TZ = ZoneInfo("America/New_York")
# Regular equity session open, America/New_York wall clock. Before this instant
# on a session day Yahoo's quote still carries the PREVIOUS session's
# ``regularMarketTime`` (there has been no regular-session print yet), so
# ``_regular_market_time`` cannot date a chain to today and every ticker
# would otherwise end as FAILED "incomplete options chain response" -- the
# grid-hermes 2026-10-20 04:00:49Z smart:options run (00:00:49 ET, 0/211
# tickers, pull_log "no ticker succeeded"). That is not a provider failure:
# it is "no session quote yet", the same nothing-to-capture truth as a
# closed day, so it is reported as SKIPPED without contacting the provider.
# A stale quote served AFTER the open stays FAILED (provider defect).
REGULAR_SESSION_OPEN_ET = (9, 30)
DEFAULT_CAPTURE_SOURCE = "options_puller"

# Tickers Yahoo serves no listed options for. The catalyst universe carries
# ~30 sub-$2B names (NUOX, VXRT, TKPHF, ...) that have no listed options; on
# grid-hermes every one of them was re-fetched and re-warned on every run
# (336 lines in the 24 h to 2026-11-04) and, reported SKIPPED/FAILED, kept the
# whole options source PARTIAL. A confirmed "no listed options" answer is
# remembered for NO_OPTIONS_TTL_SECONDS in process memory, logged once per
# ticker per TTL, and reported per ticker as NO_NEW_DATA with
# ``no_options=True`` -- a checked ticker with nothing to capture. After the
# TTL the ticker is probed again, so a new listing is picked up within a day.
# Hand-listed EQUITY_TICKERS are never cached: they are known optionable, so
# an empty answer for them stays a visible incomplete capture.
NO_OPTIONS_TTL_SECONDS = 24 * 3600
_NO_OPTIONS_UNTIL: dict[str, float] = {}


class _NoChain:
    """``get_options`` result for a well-formed response carrying no chain.

    Falsy, so any caller that only checks ``if not chain`` keeps treating it
    as a failed fetch; ``_pull_ticker`` checks identity to tell "this symbol
    has no listed options" from a transport failure.
    """

    def __bool__(self) -> bool:
        return False

    def __repr__(self) -> str:
        return "NO_CHAIN"


NO_CHAIN = _NoChain()


def _no_options_cached(ticker: str) -> bool:
    until = _NO_OPTIONS_UNTIL.get(ticker)
    if until is None:
        return False
    if time.monotonic() >= until:
        del _NO_OPTIONS_UNTIL[ticker]
        return False
    return True


def _no_options_result(ticker: str, why: str) -> dict[str, Any]:
    """Remember ``ticker`` as optionless (unless hand-listed) and report it."""
    if ticker in EQUITY_TICKERS:
        log.warning("{t}: {w} (hand-listed optionable ticker; not cached)", t=ticker, w=why)
        return {"ticker": ticker, "status": "SKIPPED", "rows_inserted": 0, "reason": why}
    _NO_OPTIONS_UNTIL[ticker] = time.monotonic() + NO_OPTIONS_TTL_SECONDS
    log.info(
        "{t}: no listed options ({w}) -- not re-checked for {h}h",
        t=ticker, w=why, h=NO_OPTIONS_TTL_SECONDS // 3600,
    )
    return {"ticker": ticker, "status": "NO_NEW_DATA", "rows_inserted": 0,
            "reason": f"no listed options: {why}", "no_options": True}


class _OptionsBudgetExpired(Exception):
    """Stop a capture without publishing an incomplete chain."""


def _check_budget(should_continue: Callable[[], bool] | None) -> None:
    if should_continue is not None and not should_continue():
        raise _OptionsBudgetExpired


def _affected_rows(result: Any) -> int | None:
    count = getattr(result, "rowcount", None)
    return count if isinstance(count, int) and not isinstance(count, bool) and count >= 0 else None


class OptionsPullResults(list[dict[str, Any]]):
    """Per-ticker list, with the source-wide outcome available to wrappers.

    GEM can still iterate/serialize this as a list. Wrappers must classify
    ``summary`` rather than infer source freshness from one successful item.
    Explicit ticker lists, reduced expiry caps and omitted catalyst tickers
    are subsets even if every requested ticker succeeded.
    """

    # Per-ticker SKIPPED reasons meaning "checked: Yahoo lists no options
    # chain for this symbol today". 2026-11-04: every full run reported
    # PARTIAL "source coverage incomplete" (yfinance_options stale since
    # 2026-09-29) because ~31 catalyst-universe names -- OTC/foreign listings
    # (TKPHF, SNYNF), micro caps without listed options (VXRT, ONCY) and
    # delisted symbols (NUOX) -- can never produce a chain. They are
    # accounted-for coverage, not failures. The hand-listed EQUITY_TICKERS
    # always have listed options, so for them the same answer is still a
    # capture failure.
    _CLEAN_NO_CHAIN_REASONS: frozenset[str] = frozenset({
        "no expirations", "no chain listed", "no dated regular-market price",
    })
    _CORE_TICKERS: frozenset[str] = frozenset(EQUITY_TICKERS)

    def __init__(self, items: list[dict[str, Any]], *, full_universe: bool) -> None:
        super().__init__(items)
        self.full_universe = full_universe
        self.publication_error: str | None = None

    def _is_clean_no_chain(self, result: dict[str, Any]) -> bool:
        return (
            result.get("status") == "SKIPPED"
            and result.get("reason") in self._CLEAN_NO_CHAIN_REASONS
            and result.get("ticker") not in self._CORE_TICKERS
        )

    @property
    def summary(self) -> dict[str, Any]:
        statuses = [r.get("status") for r in self]
        counts = [r.get("rows_inserted") for r in self]
        known_rows = all(isinstance(n, int) and not isinstance(n, bool) and n >= 0 for n in counts)
        rows = sum(counts) if known_rows else None
        ok = statuses.count("SUCCESS")
        deferred = statuses.count("DEFERRED")
        # Checked tickers with nothing to capture complete coverage rather
        # than break it: cached/confirmed "no listed options" (NO_NEW_DATA,
        # no_options=True) and the remaining clean SKIPPED no-chain reasons
        # for non-core tickers. A row is one or the other, never both.
        no_options = sum(1 for r in self if r.get("no_options") and r.get("status") == "NO_NEW_DATA")
        no_data = [r.get("ticker") for r in self if self._is_clean_no_chain(r)]
        if not self or all(s == "SKIPPED" for s in statuses):
            reasons = sorted({str(r["reason"]) for r in self if r.get("reason")})
            status, note = "SKIPPED", "no ticker capture completed"
            if reasons:
                note = f"{note}: {', '.join(reasons)}"
        elif not ok and not deferred and not no_options:
            status, note = "FAILED", "no ticker succeeded"
        elif (self.publication_error or not self.full_universe
                or ok + no_options + len(no_data) != len(self) or not known_rows):
            status, note = "PARTIAL", self.publication_error or "source coverage incomplete or row count unknown"
            if not self.publication_error and self.full_universe and known_rows:
                missing = [r.get("ticker") for r in self
                           if r.get("status") != "SUCCESS" and not self._is_clean_no_chain(r)
                           and not (r.get("no_options") and r.get("status") == "NO_NEW_DATA")]
                note = f"{note}: {len(missing)} tickers not captured ({', '.join(map(str, missing[:10]))})"
        elif not rows:
            status, note = "NO_NEW_DATA", "run completed, 0 rows written"
        else:
            status, note = "SUCCESS", "full default universe captured"
        return {
            "status": status, "rows_inserted": rows,
            "scope": "full_universe" if self.full_universe else "subset",
            "tickers_total": len(self), "tickers_ok": ok,
            "tickers_deferred": deferred,
            "tickers_no_chain": len(no_data),
            "no_chain_tickers": no_data[:50],
            "tickers_no_options": no_options,
            "snapshots": sum(r.get("snapshots", 0) for r in self),
            "error": note if status in ("PARTIAL", "FAILED") else None,
            "reason": note,
        }



def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _before_regular_open(now: datetime) -> bool:
    """True before the regular equity session opens on ``now``'s ET day."""
    local = now.astimezone(_EQUITY_TZ)
    return (local.hour, local.minute) < REGULAR_SESSION_OPEN_ET


def automatic_options_capture_guard(engine: Engine) -> dict[str, Any] | None:
    """Owner-gated daily-writer policy for automatic secondary callers only.

    Batch existence is diagnostic, never proof of complete source coverage or
    scheduler provenance. Secondary writers also defer with no batches: letting
    one start while the daily writer is fetching would recreate the race. The
    daily scheduler and explicit GEM capture do not call this guard.
    """
    policy = os.getenv("GRID_OPTIONS_AUTOMATIC_WRITER_POLICY", "legacy").strip().lower()
    if policy in {"", "legacy"}:
        return None
    result: dict[str, Any] = {
        "status": "SKIPPED", "outcome": "SKIPPED", "rows_inserted": 0,
        "reason": "daily_scheduler_owns_options",
        "observed_session": _utc_now().date().isoformat(),
        "registered_non_gem_batches": None, "captured_tickers": None,
    }
    if policy != "daily_scheduler":
        result["reason"] = "invalid_options_writer_policy"
        return result
    try:
        with engine.connect() as conn:
            conn.execute(text("SET TRANSACTION READ ONLY"))
            conn.execute(text("SET LOCAL statement_timeout = '5s'"))
            conn.execute(text("SET LOCAL lock_timeout = '1s'"))
            row = conn.execute(text(
                "SELECT COUNT(*), COUNT(DISTINCT ticker) FROM options_capture_batches "
                "WHERE snap_date = :day AND capture_source <> 'gem'"
            ), {"day": result["observed_session"]}).fetchone()
            result.update(registered_non_gem_batches=int(row[0]), captured_tickers=int(row[1]))
    except Exception as exc:
        # A failed observation cannot grant another writer permission. Avoid
        # publishing connection details from driver exceptions.
        log.warning("Options writer-policy observation unavailable ({kind})", kind=type(exc).__name__)
        result["observation_error"] = type(exc).__name__
    return result


def _regular_market_time(quote: Any, session_day: date) -> datetime | None:
    """Read Yahoo's *reported quote time*, not an options-chain revision time."""
    if not isinstance(quote, dict):
        return None
    raw = quote.get("regularMarketTime")
    if (not isinstance(raw, (int, float)) or isinstance(raw, bool)
            or not math.isfinite(raw)):
        return None
    try:
        reported = datetime.fromtimestamp(raw, timezone.utc)
    except (OverflowError, OSError, ValueError):
        return None
    if (reported.date() != session_day
            or reported.astimezone(_EQUITY_TZ).date() != session_day):
        return None
    return reported

# Catalyst-universe coverage (2026-09-10).
#
# EQUITY_TICKERS above is 41 hand-listed mega caps. Measured on 2026-09-10,
# options_daily_signals held zero rows for every sub-$2B catalyst name the Long
# Plays board tracks — OLMA, TECX, OCGN, MLTX, RNAC, ANNX, DNTH, BLTE, APGE,
# MESO among them. That single gap disabled two things at once: the options
# expression of a catalyst bet, and the IV surface that
# ``intelligence.catalyst_ev.upside_multiple_from_iv`` inverts for its upside
# anchor. The names with the most convex payoff were the only ones with no
# chain data.
#
# Yahoo lists chains for most of these, so the fix is coverage, not a vendor:
# union the hand-list with the tickers that actually have a dated catalyst.
CATALYST_UNIVERSE_HORIZON_DAYS: int = 560
CATALYST_UNIVERSE_MAX_TICKERS: int = 200

_CATALYST_UNIVERSE_SQL = text(
    """
    SELECT DISTINCT cc.ticker
    FROM catalyst_calendar cc
    WHERE cc.is_active
      AND cc.expected_date >= CURRENT_DATE
      AND cc.expected_date <= CURRENT_DATE + :horizon_days
      AND cc.ticker ~ '^[A-Z.-]{1,6}$'
    ORDER BY cc.ticker
    LIMIT :max_tickers
    """
)


def catalyst_options_universe(
    engine: Engine,
    *,
    horizon_days: int = CATALYST_UNIVERSE_HORIZON_DAYS,
    max_tickers: int = CATALYST_UNIVERSE_MAX_TICKERS,
    require_complete: bool = False,
) -> list[str]:
    """Tickers with a dated catalyst inside ``horizon_days``.

    Bounded on both sides and capped, because this list drives an outbound
    fetch loop. Routine callers retain the empty-list fallback. A source-wide
    capture sets require_complete so lookup failure cannot masquerade as a
    complete run of the smaller hand-listed universe.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                _CATALYST_UNIVERSE_SQL,
                {"horizon_days": int(horizon_days), "max_tickers": int(max_tickers)},
            ).fetchall()
    except Exception:  # noqa: BLE001
        log.warning("options: catalyst universe unavailable")
        if require_complete:
            raise RuntimeError("options catalyst universe unavailable") from None
        return []
    return [str(r[0]).strip().upper() for r in rows if r[0] and str(r[0]).strip()]


# ── Yahoo Finance direct API client ────────────────────────────

class YahooOptionsClient:
    """Fetch options chains directly from Yahoo Finance API with crumb auth."""

    provider = "yahoo"
    remote_expiry_pages = True

    _USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"  # = _BROWSER_UA

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": self._USER_AGENT})
        self.crumb: str | None = None
        # True after get_options() saw a clean empty chain (no such symbol).
        self.last_no_chain = False
        self._init_session()

    def _init_session(self) -> None:
        """Get cookies and crumb from Yahoo Finance.

        Uses fc.yahoo.com to obtain the A3 cookie (returns 404 but sets cookie),
        then fetches crumb from query2.finance.yahoo.com.
        """
        try:
            # Step 1: Get A3 cookie via fc.yahoo.com (404 is expected)
            self.session.get("https://fc.yahoo.com", timeout=10, allow_redirects=True)

            # Step 2: Get crumb using query2 endpoint
            resp = self.session.get(
                "https://query2.finance.yahoo.com/v1/test/getcrumb", timeout=10,
            )
            if resp.status_code == 200 and resp.text:
                self.crumb = resp.text
                log.info("Yahoo options client initialised — crumb obtained")
            else:
                log.warning("Yahoo crumb fetch failed: {s}", s=resp.status_code)
        except Exception as exc:
            log.warning("Yahoo session init failed: {e}", e=str(exc))

    @property
    def is_available(self) -> bool:
        return self.crumb is not None

    def get_options(
        self, ticker: str, expiry_ts: int | None = None,
    ) -> dict[str, Any] | None:
        """Fetch options chain for a ticker and optional expiry timestamp.

        Returns dict with keys: expirations, strikes, calls, puts, quote.
        """
        self.last_no_chain = False
        if not self.crumb:
            return None

        url = f"https://query2.finance.yahoo.com/v7/finance/options/{ticker}"
        params: dict[str, Any] = {"crumb": self.crumb}
        if expiry_ts is not None:
            params["date"] = expiry_ts

        try:
            resp = self.session.get(url, params=params, timeout=15)
            if resp.status_code != 200:
                log.warning("Yahoo options {t}: HTTP {s}", t=ticker, s=resp.status_code)
                return None
            data = resp.json()
            chain = data.get("optionChain") if isinstance(data, dict) else None
            if isinstance(chain, dict) and chain.get("error") is not None:
                # A present, non-null provider error is a failed fetch even
                # when residual result data rides along: an explicit empty
                # chain beside an error is not measured zero coverage. The
                # error body is not logged. An absent key or ``"error": null``
                # is the normal successful shape and stays accepted.
                log.warning("Yahoo options {t}: provider error in chain response", t=ticker)
                return None
            results = chain.get("result") if isinstance(chain, dict) else None
            if results == []:
                # Well-formed response, no error, empty result list: Yahoo has
                # no chain for this symbol (NUOX, CRNX on 2026-11-04, every
                # run). Not a transport failure; the caller decides.
                log.debug("Yahoo options {t}: no chain result", t=ticker)
                return NO_CHAIN
            if not isinstance(results, list):
                log.warning("Yahoo options {t}: no chain result", t=ticker)
                # HTTP 200, no provider error, empty result: Yahoo has no
                # chain for this symbol (e.g. delisted), not a transport failure.
                self.last_no_chain = isinstance(results, list)
                return None
            result = results[0]
            if not isinstance(result, dict):
                log.warning("Yahoo options {t}: malformed chain result", t=ticker)
                return None
            options_list = result.get("options")
            if options_list == []:
                # Explicit empty page: Yahoo lists the expiry but serves no
                # contracts for it (thin names). The chain as served, not a
                # transport failure (it used to raise IndexError here and
                # return None like a failed fetch).
                calls: list[Any] = []
                puts: list[Any] = []
            elif (isinstance(options_list, list) and isinstance(options_list[0], dict)
                    and isinstance(options_list[0].get("calls"), list)
                    and isinstance(options_list[0].get("puts"), list)):
                calls = options_list[0]["calls"]
                puts = options_list[0]["puts"]
            else:
                # Missing, null or non-list ``options``, or an element without
                # both sides as lists: not a chain. Fail closed like a failed
                # fetch; an explicit ``[]`` is the only accepted empty shape.
                log.warning("Yahoo options {t}: malformed chain page", t=ticker)
                return None
            return {
                "expirations": result.get("expirationDates", []),
                "strikes": result.get("strikes", []),
                "calls": calls,
                "puts": puts,
                "quote": result.get("quote", {}),
            }
        except Exception as exc:
            log.warning("Yahoo options {t} failed: {e}", t=ticker, e=str(exc))
            return None


# ── Cboe delayed-quotes client (primary since 2026-11-06) ──────
#
# Yahoo's /v7/finance/options has answered HTTP 429 to grid-svr since
# 2026-09-29, so source_catalog YFINANCE_OPTIONS went 30+ days without a
# complete capture. Cboe publishes the full OPRA chain per underlying as one
# delayed (~15 min) JSON document with bid/ask/iv/OI/volume and greeks; the
# gamma-watch sector collector (collectors/gamma_watch/sectors.py) and the
# GEX P2-B harness already read it. One request per ticker per run replaces
# Yahoo's one request per expiry.
#
# The client below presents the Yahoo page shape (``expirations``, ``calls``,
# ``puts``, ``quote.regularMarketPrice``/``regularMarketTime``) so every
# guard in ``OptionsPuller._pull_ticker`` -- session dating, empty pages,
# capture budget, append-only publication -- applies unchanged.

_BROWSER_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
CBOE_OPTIONS_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{symbol}.json"
CBOE_TIMEOUT_SECONDS = 30  # SPY is ~6 MB; the CDN 307-redirects to cdn-api.cboe.com
# Cash-settled indices are addressed with a leading underscore (``_SPX``).
CBOE_INDEX_SYMBOLS: frozenset[str] = frozenset({
    "SPX", "XSP", "NDX", "RUT", "VIX", "DJX", "OEX", "XEO", "MRUT",
})
# OCC/OSI option symbol: root, YYMMDD, C/P, strike * 1000 (8 digits).
_OCC_SYMBOL = re.compile(r"^([A-Z]{1,6})(\d{6})([CP])(\d{8})$")


def cboe_symbol(ticker: str) -> str:
    """GRID ticker -> Cboe document symbol (``BRK-B`` -> ``BRK.B``, ``SPX`` -> ``_SPX``)."""
    sym = ticker.strip().upper().replace("-", ".")
    return f"_{sym}" if sym in CBOE_INDEX_SYMBOLS else sym


def parse_occ_symbol(option: Any) -> tuple[str, date, str, float] | None:
    """Parse ``KO261224C00099000`` into (root, expiry, 'C'|'P', strike)."""
    if not isinstance(option, str):
        return None
    match = _OCC_SYMBOL.fullmatch(option)
    if match is None:
        return None
    try:
        expiry = datetime.strptime(match[2], "%y%m%d").date()
    except ValueError:
        return None
    return match[1], expiry, match[3], int(match[4]) / 1000.0


def _finite(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def _count(value: Any) -> int | None:
    number = _finite(value)
    return int(number) if number is not None and number >= 0 else None


def _expiry_ts(day: date) -> int:
    """Expiry key in Yahoo's convention: midnight UTC of the expiry date."""
    return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())


def cboe_chain_pages(doc: Any, ticker: str, today: date) -> dict[str, Any] | None:
    """Turn one Cboe delayed-quotes document into Yahoo-shaped expiry pages.

    Returns ``{"expirations", "pages", "quote"}`` or None when the document
    is not a chain for ``ticker``. Only standard contracts are kept: the OCC
    root must equal the underlying (``BRKB`` for ``BRK.B``). Adjusted roots
    (``KO1``) carry non-standard deliverables and would corrupt OI/max-pain;
    index weeklies (``SPXW``) are a separate root and are not merged.
    Expiries before ``today`` (ET session date) are dropped. A non-positive
    or missing IV is stored as missing, never as zero volatility.
    """
    if not isinstance(doc, dict) or not isinstance(doc.get("data"), dict):
        return None
    data = doc["data"]
    sym = cboe_symbol(ticker)
    reported = doc.get("symbol") or data.get("symbol")
    if not isinstance(reported, str) or reported.lstrip("_^") != sym.lstrip("_"):
        return None
    options_list = data.get("options")
    if not isinstance(options_list, list):
        return None
    root = sym.lstrip("_").replace(".", "")

    spot = _finite(data.get("current_price"))
    quote: dict[str, Any] = {"regularMarketPrice": spot, "regularMarketTime": None,
                             "iv30": _finite(data.get("iv30"))}
    trade_time = data.get("last_trade_time")
    if isinstance(trade_time, str):
        try:
            local = datetime.fromisoformat(trade_time)
        except ValueError:
            local = None
        # Cboe's naive underlying trade time is America/New_York wall clock
        # (same reading as scripts/gex_p2b/harness.py). An offset-bearing value
        # is unexpected and left undated rather than reinterpreted.
        if local is not None and local.tzinfo is None:
            quote["regularMarketTime"] = int(local.replace(tzinfo=_EQUITY_TZ).timestamp())

    pages: dict[int, dict[str, list[dict[str, Any]]]] = {}
    for item in options_list:
        if not isinstance(item, dict):
            continue
        parsed = parse_occ_symbol(item.get("option"))
        if parsed is None or parsed[0] != root or parsed[1] < today:
            continue
        _, expiry, side, strike = parsed
        iv = _finite(item.get("iv"))
        page = pages.setdefault(_expiry_ts(expiry), {"calls": [], "puts": []})
        is_call = side == "C"
        page["calls" if is_call else "puts"].append({
            "contractSymbol": item.get("option"),
            "strike": strike,
            "lastPrice": _finite(item.get("last_trade_price")),
            "bid": _finite(item.get("bid")),
            "ask": _finite(item.get("ask")),
            "volume": _count(item.get("volume")),
            "openInterest": _count(item.get("open_interest")),
            "impliedVolatility": iv if iv is not None and iv > 0 else None,
            "inTheMoney": bool(spot is not None and (strike < spot if is_call else strike > spot)),
        })
    return {"expirations": sorted(pages), "pages": pages, "quote": quote}


class CboeOptionsClient:
    """Cboe delayed chains behind the ``YahooOptionsClient.get_options`` contract.

    ``get_options(ticker)`` downloads the ticker's whole chain once and
    returns the nearest expiry page; ``get_options(ticker, expiry_ts)`` is
    served from that download without another request.
    """

    provider = "cboe"
    is_available = True  # no auth handshake
    remote_expiry_pages = False

    def __init__(self, session: requests.Session | None = None) -> None:
        self.session = session or requests.Session()
        self.session.headers.update({"User-Agent": _BROWSER_UA,
                                     "Accept": "application/json"})
        self.last_no_chain = False
        self._ticker: str | None = None
        self._chain: dict[str, Any] | None = None

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
        if expiry_ts is not None:
            if self._ticker != ticker or self._chain is None:
                return None
            return self._page(self._chain, expiry_ts)

        self._ticker, self._chain = ticker, None
        url = CBOE_OPTIONS_URL.format(symbol=cboe_symbol(ticker))
        try:
            resp = self.session.get(url, timeout=CBOE_TIMEOUT_SECONDS, allow_redirects=True)
        except Exception as exc:  # noqa: BLE001 -- transport failure, caller may fall back
            log.warning("Cboe options {t}: {kind}", t=ticker, kind=type(exc).__name__)
            return None
        if resp.status_code in (403, 404):
            # The CDN answers S3 AccessDenied for symbols with no listed
            # options (verified 2026-11-06 for ZZZZQ, BRK-B, BRKB).
            log.debug("Cboe options {t}: HTTP {s} (no chain)", t=ticker, s=resp.status_code)
            return NO_CHAIN
        if resp.status_code != 200:
            log.warning("Cboe options {t}: HTTP {s}", t=ticker, s=resp.status_code)
            return None
        try:
            doc = resp.json()
        except ValueError:
            log.warning("Cboe options {t}: response is not JSON", t=ticker)
            return None
        today = _utc_now().astimezone(_EQUITY_TZ).date()
        chain = cboe_chain_pages(doc, ticker, today)
        if chain is None:
            log.warning("Cboe options {t}: malformed chain document", t=ticker)
            return None
        if not chain["expirations"]:
            return NO_CHAIN
        self._chain = chain
        return self._page(chain, chain["expirations"][0])


def options_primary_provider() -> str:
    """``cboe`` (default) or ``yahoo``, from GRID_OPTIONS_PRIMARY_PROVIDER."""
    value = os.getenv("GRID_OPTIONS_PRIMARY_PROVIDER", "cboe").strip().lower()
    return "yahoo" if value == "yahoo" else "cboe"


class CboeFirstOptionsClient:
    """Cboe primary, Yahoo fallback, one provider per ticker capture.

    The provider that served a ticker's first page serves all its expiry
    pages, so a batch never mixes vendors. Yahoo is initialised lazily (its
    crumb handshake costs two requests) and only when Cboe fails in transport
    for a ticker; a Cboe "no listed options" answer is authoritative (Cboe
    carries the consolidated OPRA chain) and is not retried on Yahoo.
    """

    is_available = True

    def __init__(self) -> None:
        self.cboe = CboeOptionsClient()
        self._yahoo: Any = None
        self._active: Any = self.cboe
        self.provider_by_ticker: dict[str, str] = {}

    @property
    def last_no_chain(self) -> bool:
        return getattr(self._active, "last_no_chain", False) is True

    @property
    def remote_expiry_pages(self) -> bool:
        return getattr(self._active, "remote_expiry_pages", True)

    def _fallback(self) -> Any:
        if self._yahoo is None:
            self._yahoo = YahooOptionsClient()
        return self._yahoo if self._yahoo.is_available else None

    def get_options(self, ticker: str, expiry_ts: int | None = None) -> Any:
        if expiry_ts is not None:
            return self._active.get_options(ticker, expiry_ts)
        self._active = self.cboe
        first = self.cboe.get_options(ticker)
        if first is None:
            yahoo = self._fallback()
            if yahoo is not None:
                log.info("{t}: Cboe chain unavailable, falling back to Yahoo", t=ticker)
                self._active = yahoo
                first = yahoo.get_options(ticker)
        self.provider_by_ticker[ticker] = getattr(self._active, "provider", "yahoo")
        return first


class OptionsPuller(BasePuller):
    """Pull options chains and compute daily signals for watchlist tickers.

    Attributes:
        SOURCE_NAME: Fixed to 'YFINANCE_OPTIONS' in source_catalog.
    """

    SOURCE_NAME = "YFINANCE_OPTIONS"

    def __init__(self, db_engine: Engine) -> None:
        super().__init__(db_engine)
        self._ensure_tables()

    def _ensure_tables(self) -> None:
        """Create options tables if they don't exist."""
        with self.engine.begin() as conn:
            # The chain store is migration-owned since
            # options_append_only_20260930: ``options_snapshots_all`` keeps
            # every capture batch, ``options_capture_batches`` registers the
            # complete ones, and ``options_snapshots`` is a *view* of the
            # latest complete batch per ticker/day. DDL against that name
            # (CREATE INDEX ... ON options_snapshots) would fail on the view,
            # so this method no longer touches it; a database without the
            # migration makes every capture fail closed at publication.
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS options_daily_signals (
                    id              BIGSERIAL PRIMARY KEY,
                    ticker          TEXT NOT NULL,
                    signal_date     DATE NOT NULL,
                    put_call_ratio  DOUBLE PRECISION,
                    max_pain        DOUBLE PRECISION,
                    iv_skew         DOUBLE PRECISION,
                    total_oi        BIGINT,
                    total_volume    BIGINT,
                    near_expiry     DATE,
                    spot_price      DOUBLE PRECISION,
                    iv_atm          DOUBLE PRECISION,
                    iv_25d_put      DOUBLE PRECISION,
                    iv_25d_call     DOUBLE PRECISION,
                    term_structure_slope DOUBLE PRECISION,
                    oi_concentration     DOUBLE PRECISION,
                    created_at      TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE (ticker, signal_date)
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_opts_sig_ticker
                ON options_daily_signals (ticker, signal_date)
            """))

    def pull_all(
        self,
        tickers: list[str] | None = None,
        *,
        include_catalyst_universe: bool = True,
        max_expirations: int = MAX_EXPIRATIONS,
        should_continue: Callable[[], bool] | None = None,
        capture_source: str = DEFAULT_CAPTURE_SOURCE,
    ) -> OptionsPullResults:
        """Pull options chains for all tickers and compute signals.

        Parameters:
            tickers: Override ticker list. When given it is used verbatim and
                ``include_catalyst_universe`` is ignored.
            include_catalyst_universe: Union ``EQUITY_TICKERS`` with the
                tickers carrying a dated catalyst (see
                ``catalyst_options_universe``). On by default: without it the
                sub-$2B names the Long Plays board gates on have no chain and
                no IV surface, which is the state measured on 2026-09-10.
            max_expirations: Limit the nearest complete expiries per ticker.
                The default preserves the scheduler's existing 12-expiry cap;
                the legacy GEM timer used six.
            should_continue: Deadline check between tickers, expirations and
                publication statements. Cancellation rolls back the current
                ticker and reports it and the remaining tickers as DEFERRED.
            capture_source: Writer label recorded on each registered batch
                in ``options_capture_batches`` (e.g. "gem_daily").

        Returns:
            OptionsPullResults: List-compatible results with a source-wide
                summary. Only a complete default-universe SUCCESS bumps the
                catalog; explicit scopes such as GEM never do.
        """
        if (not isinstance(max_expirations, int) or isinstance(max_expirations, bool)
                or not 1 <= max_expirations <= MAX_EXPIRATIONS):
            raise ValueError(f"max_expirations must be between 1 and {MAX_EXPIRATIONS}")

        full_universe = tickers is None and include_catalyst_universe and max_expirations == MAX_EXPIRATIONS
        if tickers is None:
            tickers = list(EQUITY_TICKERS)
            if include_catalyst_universe:
                extra = [t for t in catalyst_options_universe(self.engine, require_complete=True) if t not in set(tickers)]
                if extra:
                    log.info(
                        "options: +{n} catalyst-universe tickers beyond the {b} hand-listed",
                        n=len(extra), b=len(tickers),
                    )
                    tickers = tickers + extra
        now = _utc_now()
        today = now.date()
        today_str = today.isoformat()
        if not is_market_open(today) or now.astimezone(_EQUITY_TZ).date() != today:
            log.info("Options pull skipped: {day} is not a scheduled equity session", day=today_str)
            return OptionsPullResults([
                {"ticker": ticker, "status": "SKIPPED", "rows_inserted": 0,
                 "reason": "non-equity-session"} for ticker in tickers
            ], full_universe=full_universe)
        if _before_regular_open(now):
            log.info(
                "Options pull skipped: {day} session has not opened yet ({et} ET)",
                day=today_str, et=now.astimezone(_EQUITY_TZ).strftime("%H:%M"),
            )
            return OptionsPullResults([
                {"ticker": ticker, "status": "SKIPPED", "rows_inserted": 0,
                 "reason": "pre-session"} for ticker in tickers
            ], full_universe=full_universe)

        # Cboe delayed chains first, Yahoo only as a per-ticker fallback.
        # GRID_OPTIONS_PRIMARY_PROVIDER=yahoo restores the Yahoo-only path
        # (operator rollback knob; the test suite pins it so no test reaches
        # Cboe unless it opts in). ``_yahoo`` keeps its historical attribute
        # name: callers and tests inject any ``get_options`` client there.
        if options_primary_provider() == "yahoo":
            self._yahoo = YahooOptionsClient()
        else:
            self._yahoo = CboeFirstOptionsClient()
        if not self._yahoo.is_available:
            log.error("Yahoo options client unavailable — cannot pull options")
            return OptionsPullResults([
                {"ticker": "N/A", "status": "FAILED", "rows_inserted": 0, "error": "Yahoo auth failed"}
            ], full_universe=full_universe)
        results: list[dict[str, Any]] = []

        for idx, ticker in enumerate(tickers):
            if should_continue is not None and not should_continue():
                results.extend(
                    {"ticker": t, "status": "DEFERRED", "rows_inserted": 0, "reason": "time budget"}
                    for t in tickers[idx:]
                )
                break
            ticker_kwargs: dict[str, Any] = {
                "max_expirations": max_expirations, "should_continue": should_continue,
            }
            if capture_source != DEFAULT_CAPTURE_SOURCE:
                # Only non-default writers pass a label; the default call
                # shape stays what existing wrappers and doubles expect.
                ticker_kwargs["capture_source"] = capture_source
            result = self._pull_ticker(ticker, today_str, **ticker_kwargs)
            results.append(result)
            if result["status"] == "DEFERRED":
                results.extend(
                    {"ticker": t, "status": "DEFERRED", "rows_inserted": 0, "reason": "time budget"}
                    for t in tickers[idx + 1:]
                )
                break
            time.sleep(0.3)  # rate limit

        total_snaps = sum(r.get("snapshots", 0) for r in results)
        succeeded = sum(1 for r in results if r["status"] == "SUCCESS")
        no_options = sum(1 for r in results if r.get("no_options"))
        log.info(
            "Options pull complete — {ok}/{total} tickers, {snaps} snapshots, "
            "{no} without listed options",
            ok=succeeded, total=len(tickers), snaps=total_snaps, no=no_options,
        )
        outcome = OptionsPullResults(results, full_universe=full_universe)
        if outcome.summary["status"] == "SUCCESS":
            if should_continue is not None and not should_continue():
                outcome.publication_error = "time budget expired before catalog publication"
            elif not self._mark_catalog_pulled(should_continue=should_continue):
                outcome.publication_error = "catalog publication deferred or failed"
        return outcome

    def _mark_catalog_pulled(self, *, should_continue: Callable[[], bool] | None = None) -> bool:
        """Publish complete-source freshness, or roll back on cancellation.

        Use a short-lived connection to avoid the shared pool's 30s checkout
        wait, with a 5s database connection timeout configured explicitly.
        PostgreSQL 14 has no transaction_timeout; check the local deadline at
        each boundary, configure 5s statement limits, and check immediately
        before commit. An in-progress durable COMMIT/WAL wait is not bounded
        by that cooperative check or an absolute 5s wall-clock guarantee.
        """
        publication_deadline = time.monotonic() + CATALOG_PUBLICATION_SECONDS
        catalog_engine = None

        def check_publication_budget() -> None:
            _check_budget(should_continue)
            if time.monotonic() >= publication_deadline:
                raise _OptionsBudgetExpired

        try:
            check_publication_budget()
            catalog_engine = create_engine(
                self.engine.url,
                poolclass=NullPool,
                connect_args={
                    "connect_timeout": 5,
                    "options": "-c statement_timeout=5000 -c lock_timeout=3000 "
                               "-c idle_in_transaction_session_timeout=5000",
                },
            )
            check_publication_budget()
            with catalog_engine.begin() as conn:
                check_publication_budget()
                conn.execute(text("SET LOCAL lock_timeout = '3s'"))
                check_publication_budget()
                conn.execute(text("SET LOCAL statement_timeout = '5s'"))
                check_publication_budget()
                conn.execute(text("SET LOCAL idle_in_transaction_session_timeout = '5s'"))
                check_publication_budget()
                conn.execute(
                    text("UPDATE source_catalog SET last_pull_at = NOW() WHERE id = :sid"),
                    {"sid": self.source_id},
                )
                # A callback that expires during UPDATE must roll back that
                # update rather than allow context-manager exit to commit it.
                check_publication_budget()
            return True
        except _OptionsBudgetExpired:
            log.info("options: source_catalog freshness publication deferred by budget")
            return False
        except Exception:  # best effort; never emit connection/credential text
            log.warning("options: source_catalog freshness update failed")
            return False
        finally:
            if catalog_engine is not None:
                catalog_engine.dispose()

    def _pull_ticker(
        self, ticker: str, today_str: str, *, max_expirations: int = MAX_EXPIRATIONS,
        should_continue: Callable[[], bool] | None = None,
        capture_source: str = DEFAULT_CAPTURE_SOURCE,
    ) -> dict[str, Any]:
        """Pull options chain for a single ticker and compute signals."""
        try:
            _check_budget(should_continue)
            session_day = date.fromisoformat(today_str)
            now = _utc_now()
            if (session_day != now.date() or not is_market_open(session_day)
                    or now.astimezone(_EQUITY_TZ).date() != session_day):
                return {"ticker": ticker, "status": "SKIPPED", "rows_inserted": 0,
                        "reason": "non-equity-session or UTC date mismatch"}
            if _before_regular_open(now):
                return {"ticker": ticker, "status": "SKIPPED", "rows_inserted": 0,
                        "reason": "pre-session"}
            if _no_options_cached(ticker):
                return {"ticker": ticker, "status": "NO_NEW_DATA", "rows_inserted": 0,
                        "reason": "no listed options (cached)", "no_options": True}
            capture_clock = time.monotonic()
            # Force a 64-bit PostgreSQL transaction ID for each capture. It is
            # allocated in database order, survives rollback, and needs no
            # separate sequence grant for a runtime role. This transaction
            # ends before any provider request.
            with self.engine.begin() as conn:
                capture_ordinal, capture_started_at = conn.execute(
                    text("SELECT txid_current(), clock_timestamp()"),
                ).fetchone()
            first = self._yahoo.get_options(ticker)
            if first is NO_CHAIN:
                return _no_options_result(ticker, "provider returned no chain")
            if not first:
                if getattr(self._yahoo, "last_no_chain", False) is True:
                    return {"ticker": ticker, "status": "SKIPPED", "rows_inserted": 0,
                            "reason": "no chain listed"}
                return {"ticker": ticker, "status": "FAILED", "rows_inserted": 0, "error": "no data from options provider"}

            quote = first.get("quote", {})
            spot_price = quote.get("regularMarketPrice")
            # Bind the header spot to this exact first response, including
            # an explicitly empty first expiry. Later page clocks cannot
            # substantiate this price's observation time.
            spot_market_at = _regular_market_time(quote, session_day)
            spot_provider = getattr(self._yahoo, "provider_by_ticker", {}).get(
                ticker, getattr(self._yahoo, "provider", None))
            if (not isinstance(spot_price, (int, float)) or isinstance(spot_price, bool)
                    or not math.isfinite(spot_price) or spot_price <= 0):
                return {"ticker": ticker, "status": "SKIPPED", "rows_inserted": 0,
                        "reason": "no dated regular-market price"}

            expirations = first.get("expirations", [])
            if not expirations:
                return _no_options_result(ticker, "no expirations")
            selected_expirations = expirations[:max_expirations]

            total_call_oi = 0
            total_put_oi = 0
            total_call_vol = 0
            total_put_vol = 0
            snap_count = 0
            batch_id = str(uuid4())
            complete = True
            failed_fetches = 0
            undated_quotes = 0
            empty_expiries = 0
            # Quote time of every accepted page, empty or not, so the publish
            # check below bounds pages that contribute no rows as well.
            page_times: list[datetime] = []
            snapshot_rows: list[dict[str, Any]] = []

            # Per-expiry IV data for term structure
            expiry_ivs: list[tuple[str, float]] = []

            # Collect calls/puts DataFrames for signal computation
            all_calls_dfs: list[pd.DataFrame] = []
            all_puts_dfs: list[pd.DataFrame] = []
            near_calls_df = pd.DataFrame()
            near_puts_df = pd.DataFrame()

            for i, exp_ts in enumerate(selected_expirations):
                _check_budget(should_continue)
                if time.monotonic() - capture_clock >= MAX_CAPTURE_SECONDS:
                    complete = False
                    break
                # Use data from first request for first expiry, fetch rest
                if i == 0:
                    chain_data = first
                else:
                    chain_data = self._yahoo.get_options(ticker, exp_ts)
                    if getattr(self._yahoo, "remote_expiry_pages", True):
                        time.sleep(0.2)  # per-expiry request pacing (Yahoo)
                    if chain_data is NO_CHAIN:
                        # Listed expiry, no chain page: a failed fetch, as before.
                        log.warning("Yahoo options {t}: no chain result", t=ticker)
                if not chain_data:
                    complete = False
                    failed_fetches += 1
                    continue
                provider_regular_market_at = _regular_market_time(chain_data.get("quote"), session_day)
                if provider_regular_market_at is None:
                    complete = False
                    undated_quotes += 1
                    break
                page_times.append(provider_regular_market_at)
                if not chain_data["calls"] and not chain_data["puts"]:
                    # Dated page whose sides are both explicitly empty lists
                    # (get_options admits no other empty shape): Yahoo lists
                    # the expiry but serves nothing for it (thin catalyst names,
                    # grid-hermes 2026-10-22: 416 tickers/day FAILED here). That
                    # is the chain as served, not a partial response, so it adds
                    # no rows and does not fail the ticker. An explicitly empty
                    # side beside a served one is likewise stored as served; the
                    # empty side is zero, which is what a dated GEX batch should
                    # sum. Failed or malformed fetches, missing sides, undated or
                    # future quotes and the capture budget still fail closed.
                    empty_expiries += 1

                exp_date = datetime.utcfromtimestamp(exp_ts).strftime("%Y-%m-%d")

                for opt_type, raw_list in [("call", chain_data.get("calls", [])),
                                           ("put", chain_data.get("puts", []))]:
                    rows_for_df = []
                    for opt in raw_list or []:
                        strike = opt.get("strike")
                        if strike is None:
                            continue
                        vol = opt.get("volume", 0) or 0
                        oi = opt.get("openInterest", 0) or 0
                        iv = opt.get("impliedVolatility")
                        last_price = opt.get("lastPrice")
                        bid = opt.get("bid")
                        ask = opt.get("ask")
                        itm = opt.get("inTheMoney", False)

                        snapshot_rows.append({
                            "ticker": ticker, "snap_date": today_str,
                            "expiry": exp_date, "opt_type": opt_type,
                            "strike": strike, "last_price": last_price,
                            "bid": bid, "ask": ask, "volume": vol,
                            "oi": oi, "iv": iv, "itm": itm,
                            "batch_id": batch_id,
                            "provider_regular_market_at": provider_regular_market_at,
                        })
                        snap_count += 1
                        rows_for_df.append({
                            "strike": strike, "volume": vol,
                            "openInterest": oi, "impliedVolatility": iv,
                            "lastPrice": last_price, "bid": bid, "ask": ask,
                            "inTheMoney": itm,
                        })

                    df = pd.DataFrame(rows_for_df) if rows_for_df else pd.DataFrame()
                    if opt_type == "call":
                        total_call_oi += df["openInterest"].fillna(0).sum() if not df.empty else 0
                        total_call_vol += df["volume"].fillna(0).sum() if not df.empty else 0
                        all_calls_dfs.append(df)
                        if i == 0:
                            near_calls_df = df
                    else:
                        total_put_oi += df["openInterest"].fillna(0).sum() if not df.empty else 0
                        total_put_vol += df["volume"].fillna(0).sum() if not df.empty else 0
                        all_puts_dfs.append(df)
                        if i == 0:
                            near_puts_df = df

                # ATM IV for this expiry
                all_opts = chain_data.get("calls", []) + chain_data.get("puts", [])
                atm_ivs = [
                    o["impliedVolatility"] for o in all_opts
                    if o.get("impliedVolatility") and o.get("strike")
                    and spot_price * 0.97 <= o["strike"] <= spot_price * 1.03
                ]
                if atm_ivs:
                    expiry_ivs.append((exp_date, float(np.mean(atm_ivs))))

            _check_budget(should_continue)
            timed_out = time.monotonic() - capture_clock >= MAX_CAPTURE_SECONDS
            if not complete or not snap_count or timed_out:
                raise ValueError(
                    "incomplete options chain response: "
                    f"{failed_fetches} failed expiry fetch(es), {undated_quotes} undated quote(s), "
                    f"{empty_expiries} empty of {len(selected_expirations)} expiries, "
                    f"{snap_count} contracts, timed_out={timed_out}"
                )

            # Provider availability begins after the final response. Sample
            # the database clock before waiting for the short publish lock.
            publish_clock = time.monotonic()
            with self.engine.begin() as conn:
                conn.execute(text("SET LOCAL lock_timeout = '5s'"))
                conn.execute(text("SET LOCAL statement_timeout = '30s'"))
                completed_at = conn.execute(text("SELECT clock_timestamp()")).fetchone()[0]
                # Every accepted page is bounded by the publish clock, not only
                # the pages that produced rows: an empty page used to carry a
                # future quote time past this check unnoticed.
                if (completed_at.astimezone(timezone.utc).date() != session_day
                        or completed_at.astimezone(_EQUITY_TZ).date() != session_day
                        or capture_started_at.astimezone(timezone.utc).date() != session_day
                        or capture_started_at.astimezone(_EQUITY_TZ).date() != session_day
                        or any(page_time > completed_at for page_time in page_times)):
                    raise ValueError("options capture crossed session date or quote time is future")
                conn.execute(
                    text("SELECT pg_advisory_xact_lock(hashtext(:ticker), hashtext(:snap_date))"),
                    {"ticker": ticker, "snap_date": today_str},
                )
                latest = conn.execute(
                    text("""SELECT MAX(capture_ordinal) FROM options_capture_batches
                             WHERE ticker = :ticker AND snap_date = :snap_date"""),
                    {"ticker": ticker, "snap_date": today_str},
                ).fetchone()
                # Append-only (options_append_only_20260930): nothing deletes
                # or replaces an earlier batch. Every complete capture is
                # registered and kept; readers of the ``options_snapshots``
                # view see the highest-ordinal batch, and any earlier batch
                # stays replayable by capture_batch_id. An older-started
                # capture that commits after a newer one is still kept, but
                # it does not overwrite the newer batch's daily signals.
                is_latest = not (latest and latest[0] is not None and latest[0] > capture_ordinal)

                # One row per contract per batch. A provider page that repeats
                # a contract keeps its first quote; the unique batch key then
                # makes any further conflict a hard failure, never a silent
                # partial batch.
                unique_rows: dict[tuple, dict[str, Any]] = {}
                for row in snapshot_rows:
                    unique_rows.setdefault((row["expiry"], row["opt_type"], float(row["strike"])), row)
                batch_rows = list(unique_rows.values())

                _check_budget(should_continue)
                conn.execute(
                    text(
                        "INSERT INTO options_capture_batches "
                        "(capture_batch_id, ticker, snap_date, capture_ordinal, "
                        "capture_started_at, capture_completed_at, row_count, "
                        "spot_price, capture_source, provider_regular_market_at, spot_provider) "
                        "VALUES (:batch_id, :ticker, :snap_date, :ordinal, "
                        ":started_at, :completed_at, :row_count, :spot, :source, :spot_market_at, :spot_provider)"
                    ),
                    {"batch_id": batch_id, "ticker": ticker, "snap_date": today_str,
                     "ordinal": capture_ordinal, "started_at": capture_started_at,
                     "completed_at": completed_at, "row_count": len(batch_rows),
                     "spot": float(spot_price), "source": capture_source,
                     "spot_market_at": spot_market_at, "spot_provider": spot_provider},
                )
                snapshots_inserted = 0
                rows_known = True
                for row in batch_rows:
                    _check_budget(should_continue)
                    written = conn.execute(
                        text(
                            "INSERT INTO options_snapshots_all "
                            "(ticker, snap_date, expiry, opt_type, strike, "
                            "last_price, bid, ask, volume, open_interest, "
                            "implied_vol, in_the_money, capture_batch_id, "
                            "capture_ordinal, capture_started_at, capture_completed_at, "
                            "provider_regular_market_at) "
                            "VALUES (:ticker, :snap_date, :expiry, :opt_type, :strike, "
                            ":last_price, :bid, :ask, :volume, :oi, :iv, :itm, "
                            ":batch_id, :ordinal, :started_at, :completed_at, "
                            ":provider_regular_market_at)"
                        ),
                        {**row, "ordinal": capture_ordinal,
                         "started_at": capture_started_at, "completed_at": completed_at},
                    )
                    count = _affected_rows(written)
                    rows_known = rows_known and count is not None
                    snapshots_inserted += count or 0
                if not rows_known or snapshots_inserted != len(batch_rows):
                    # Roll back the header and every row: a batch is complete
                    # or absent, never partial.
                    raise ValueError("options batch insert count mismatch")

                if not is_latest:
                    log.info("{t}: older overlapping options capture kept as batch {b}; "
                             "newer batch keeps the daily signals", t=ticker, b=batch_id)
                    return {"ticker": ticker, "status": "SUCCESS",
                            "snapshots": snap_count, "snapshots_inserted": snapshots_inserted,
                            "rows_inserted": snapshots_inserted,
                            "capture_batch_id": batch_id, "capture_ordinal": capture_ordinal,
                            "latest_batch": False}

                # Compute signals from nearest LIQUID expiration
                # Skip expiries within 2 days (near-worthless, garbage data)
                today_ts = _utc_now().timestamp()
                min_dte_seconds = 2 * 86400  # 2 days
                liquid_expirations = [e for e in selected_expirations
                                      if e - today_ts >= min_dte_seconds]
                if not liquid_expirations:
                    liquid_expirations = selected_expirations  # Fallback within captured chain
                near_expiry = datetime.utcfromtimestamp(liquid_expirations[0]).strftime("%Y-%m-%d")

                # If the nearest expiry was skipped, rebuild near_calls/puts from the correct expiry
                if liquid_expirations[0] != selected_expirations[0]:
                    # Find which index in our pulled chains matches the liquid expiry
                    liquid_idx = None
                    for ci, e in enumerate(selected_expirations):
                        if e == liquid_expirations[0]:
                            liquid_idx = ci
                            break
                    if liquid_idx is not None and liquid_idx < len(all_calls_dfs) and liquid_idx < len(all_puts_dfs):
                        near_calls_df = all_calls_dfs[liquid_idx]
                        near_puts_df = all_puts_dfs[liquid_idx]
                        log.info("{t}: skipped near-expiry, using DTE {d}d chain",
                                 t=ticker, d=int((liquid_expirations[0] - today_ts) / 86400))

                put_call_ratio = float(total_put_oi / total_call_oi) if total_call_oi > 0 else None
                max_pain = compute_max_pain(near_calls_df, near_puts_df, spot_price)
                iv_skew = compute_iv_skew(near_puts_df, spot_price)
                total_oi = int(total_call_oi + total_put_oi)
                total_volume = int(total_call_vol + total_put_vol)

                # ATM IV
                iv_atm = _compute_atm_iv(near_calls_df, near_puts_df, spot_price)

                # 25-delta wings
                iv_25d_put = _compute_wing_iv(near_puts_df, spot_price, delta_strike_pct=0.90)
                iv_25d_call = _compute_wing_iv(near_calls_df, spot_price, delta_strike_pct=1.10)

                # Term structure slope (IV of far expiry - near expiry)
                term_slope = None
                if len(expiry_ivs) >= 2:
                    term_slope = expiry_ivs[-1][1] - expiry_ivs[0][1]

                # OI concentration (max single strike OI / total OI)
                oi_conc = _compute_oi_concentration(near_calls_df, near_puts_df, total_oi)

                # Insert daily signals
                _check_budget(should_continue)
                signal_write = conn.execute(
                    text(
                        "INSERT INTO options_daily_signals "
                        "(ticker, signal_date, put_call_ratio, max_pain, iv_skew, "
                        "total_oi, total_volume, near_expiry, spot_price, iv_atm, "
                        "iv_25d_put, iv_25d_call, term_structure_slope, oi_concentration) "
                        "VALUES (:ticker, :sd, :pcr, :mp, :ivs, :oi, :vol, :ne, :spot, "
                        ":iv_atm, :iv_25d_put, :iv_25d_call, :ts_slope, :oi_conc) "
                        "ON CONFLICT (ticker, signal_date) DO UPDATE SET "
                        "put_call_ratio = EXCLUDED.put_call_ratio, "
                        "max_pain = EXCLUDED.max_pain, "
                        "iv_skew = EXCLUDED.iv_skew, "
                        "total_oi = EXCLUDED.total_oi, "
                        "total_volume = EXCLUDED.total_volume, "
                        "near_expiry = EXCLUDED.near_expiry, "
                        "spot_price = EXCLUDED.spot_price, "
                        "iv_atm = EXCLUDED.iv_atm, "
                        "iv_25d_put = EXCLUDED.iv_25d_put, "
                        "iv_25d_call = EXCLUDED.iv_25d_call, "
                        "term_structure_slope = EXCLUDED.term_structure_slope, "
                        "oi_concentration = EXCLUDED.oi_concentration"
                    ),
                    {
                        "ticker": ticker, "sd": today_str,
                        "pcr": put_call_ratio, "mp": max_pain, "ivs": iv_skew,
                        "oi": total_oi, "vol": total_volume,
                        "ne": near_expiry, "spot": spot_price,
                        "iv_atm": iv_atm, "iv_25d_put": iv_25d_put,
                        "iv_25d_call": iv_25d_call,
                        "ts_slope": term_slope, "oi_conc": oi_conc,
                    },
                )

                # Push to resolved_series for PIT access
                resolved_inserted = self._push_to_resolved(conn, ticker, today_str, {
                    "pcr": ("sentiment", f"{ticker} Put/Call Ratio", put_call_ratio),
                    "max_pain": ("sentiment", f"{ticker} Max Pain Strike", max_pain),
                    "iv_skew": ("vol", f"{ticker} IV Skew (OTM/ATM)", iv_skew),
                    "total_oi": ("sentiment", f"{ticker} Total Open Interest", total_oi),
                    "opt_vol": ("sentiment", f"{ticker} Total Options Volume", total_volume),
                    "iv_atm": ("vol", f"{ticker} ATM Implied Volatility", iv_atm),
                    "iv_25d_put": ("vol", f"{ticker} 25-Delta Put IV", iv_25d_put),
                    "iv_25d_call": ("vol", f"{ticker} 25-Delta Call IV", iv_25d_call),
                    "term_slope": ("vol", f"{ticker} IV Term Structure Slope", term_slope),
                    "oi_conc": ("sentiment", f"{ticker} OI Concentration", oi_conc),
                }, should_continue=should_continue)
                _check_budget(should_continue)
                signals_written = _affected_rows(signal_write)
                rows_inserted = (
                    snapshots_inserted + signals_written + resolved_inserted
                    if rows_known and signals_written is not None and resolved_inserted is not None else None
                )

            log.info(
                "{t}: {n} snaps via {provider}, capture={capture:.1f}s, publish={publish:.1f}s, "
                "PCR={pcr}, MaxPain={mp}, OI={oi}, IV_ATM={iv}",
                t=ticker, n=snap_count,
                provider=getattr(self._yahoo, "provider_by_ticker", {}).get(
                    ticker, getattr(self._yahoo, "provider", "provider")),
                capture=publish_clock - capture_clock,
                publish=time.monotonic() - publish_clock,
                pcr=f"{put_call_ratio:.2f}" if put_call_ratio else "N/A",
                mp=f"${max_pain:,.0f}" if max_pain else "N/A",
                oi=f"{total_oi:,}",
                iv=f"{iv_atm:.1%}" if iv_atm else "N/A",
            )
            return {
                "ticker": ticker, "status": "SUCCESS",
                "snapshots": snap_count, "snapshots_inserted": snapshots_inserted if rows_known else None,
                "rows_inserted": rows_inserted,
                "capture_batch_id": batch_id, "capture_ordinal": capture_ordinal,
                "latest_batch": True,
                "signals": {
                    "put_call_ratio": put_call_ratio,
                    "max_pain": max_pain,
                    "iv_skew": iv_skew,
                    "iv_atm": iv_atm,
                    "total_oi": total_oi,
                },
            }

        except _OptionsBudgetExpired:
            return {"ticker": ticker, "status": "DEFERRED", "rows_inserted": 0, "reason": "time budget"}
        except Exception as e:
            log.error("{t}: {e}", t=ticker, e=e)
            return {"ticker": ticker, "status": "FAILED", "error": str(e), "rows_inserted": 0}

    def _push_to_resolved(
        self,
        conn: Any,
        ticker: str,
        today_str: str,
        signals: dict[str, tuple[str, str, Any]],
        *,
        should_continue: Callable[[], bool] | None = None,
    ) -> int | None:
        """Push computed signals to feature_registry + resolved_series."""
        prefix = ticker.lower().replace("-", "_")
        inserted = 0
        known = True
        for suffix, (family, desc, val) in signals.items():
            _check_budget(should_continue)
            if val is None:
                continue
            feat_name = f"{prefix}_{suffix}"

            # Ensure family is valid for schema constraint
            if family not in ("rates", "credit", "equity", "vol", "fx",
                              "commodity", "sentiment", "macro", "crypto",
                              "alternative", "flows", "systemic", "trade",
                              "breadth", "earnings"):
                family = "sentiment"

            row = conn.execute(
                text(
                    "INSERT INTO feature_registry "
                    "(name, family, description, transformation, "
                    "transformation_version, lag_days, normalization, "
                    "missing_data_policy, eligible_from_date, model_eligible) "
                    "VALUES (:name, :fam, :desc, 'RAW', 1, 0, 'ZSCORE', "
                    "'FORWARD_FILL', '2024-04-01', TRUE) "
                    "ON CONFLICT (name) DO NOTHING RETURNING id"
                ),
                {"name": feat_name, "fam": family, "desc": desc},
            ).fetchone()

            if row:
                fid = row[0]
            else:
                fid_row = conn.execute(
                    text("SELECT id FROM feature_registry WHERE name = :name"),
                    {"name": feat_name},
                ).fetchone()
                if fid_row is None:
                    continue
                fid = fid_row[0]

            _check_budget(should_continue)
            written = conn.execute(
                text(
                    "INSERT INTO resolved_series "
                    "(feature_id, obs_date, release_date, vintage_date, value, "
                    "source_priority_used) "
                    "VALUES (:fid, :od, :rd, :vd, :val, :src) "
                    "ON CONFLICT DO NOTHING"
                ),
                {
                    "fid": fid, "od": today_str, "rd": today_str,
                    "vd": today_str, "val": float(val), "src": self.source_id,
                },
            )
            count = _affected_rows(written)
            known = known and count is not None
            inserted += count or 0
        return inserted if known else None


# ---------------------------------------------------------------------------
# Signal computation helpers
# ---------------------------------------------------------------------------

def compute_max_pain(
    calls_df: pd.DataFrame, puts_df: pd.DataFrame, spot_price: float
) -> float | None:
    """Compute max pain strike — where total option losses are minimized for writers."""
    if calls_df.empty or puts_df.empty:
        return None

    all_strikes = sorted(set(
        calls_df["strike"].tolist() + puts_df["strike"].tolist()
    ))
    if not all_strikes:
        return None

    min_pain = float("inf")
    max_pain_strike = spot_price

    for strike in all_strikes:
        call_pain = sum(
            max(0, strike - s) * oi
            for s, oi in zip(calls_df["strike"], calls_df["openInterest"].fillna(0))
        )
        put_pain = sum(
            max(0, s - strike) * oi
            for s, oi in zip(puts_df["strike"], puts_df["openInterest"].fillna(0))
        )
        total_pain = call_pain + put_pain
        if total_pain < min_pain:
            min_pain = total_pain
            max_pain_strike = strike

    return float(max_pain_strike)


def compute_iv_skew(puts_df: pd.DataFrame, spot_price: float) -> float | None:
    """IV skew: OTM put IV vs ATM put IV."""
    if puts_df.empty or "impliedVolatility" not in puts_df.columns:
        return None

    atm_puts = puts_df[
        (puts_df["strike"] >= spot_price * 0.97) &
        (puts_df["strike"] <= spot_price * 1.03)
    ]
    otm_puts = puts_df[
        (puts_df["strike"] >= spot_price * 0.85) &
        (puts_df["strike"] <= spot_price * 0.92)
    ]

    if atm_puts.empty or otm_puts.empty:
        return None

    atm_iv = atm_puts["impliedVolatility"].mean()
    otm_iv = otm_puts["impliedVolatility"].mean()

    if atm_iv and atm_iv > 0:
        return float(otm_iv / atm_iv)
    return None


def _compute_atm_iv(
    calls_df: pd.DataFrame, puts_df: pd.DataFrame, spot_price: float
) -> float | None:
    """ATM implied volatility — average of calls and puts near the money."""
    all_opts = pd.concat([calls_df, puts_df])
    if all_opts.empty or "impliedVolatility" not in all_opts.columns:
        return None

    atm = all_opts[
        (all_opts["strike"] >= spot_price * 0.98) &
        (all_opts["strike"] <= spot_price * 1.02)
    ]
    if atm.empty:
        return None

    iv = atm["impliedVolatility"].mean()
    return float(iv) if pd.notna(iv) else None


def _compute_wing_iv(
    df: pd.DataFrame, spot_price: float, delta_strike_pct: float
) -> float | None:
    """Compute average IV around a specific strike level (e.g. 90% or 110% of spot)."""
    if df.empty or "impliedVolatility" not in df.columns:
        return None

    target = spot_price * delta_strike_pct
    band = spot_price * 0.03
    wing = df[
        (df["strike"] >= target - band) &
        (df["strike"] <= target + band)
    ]
    if wing.empty:
        return None

    iv = wing["impliedVolatility"].mean()
    return float(iv) if pd.notna(iv) else None


def _compute_oi_concentration(
    calls_df: pd.DataFrame, puts_df: pd.DataFrame, total_oi: int
) -> float | None:
    """Max single-strike OI as fraction of total OI."""
    if total_oi == 0:
        return None

    all_opts = pd.concat([calls_df, puts_df])
    if all_opts.empty:
        return None

    max_oi = all_opts["openInterest"].fillna(0).max()
    return float(max_oi / total_oi)


if __name__ == "__main__":
    from db import get_engine

    engine = get_engine()
    puller = OptionsPuller(db_engine=engine)
    results = puller.pull_all()
    for r in results:
        print(f"  {r['ticker']:6s}  {r['status']}")
