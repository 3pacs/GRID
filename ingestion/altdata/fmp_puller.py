"""
Financial Modeling Prep puller — earnings, quotes, sector performance.

FMP retired its ``/api/v3`` endpoints for accounts created after
2025-08-31 (probed from grid-svr 2026-11-04: every v3 call answers
``403 "Legacy Endpoint"``; the old puller treated that 403 as "paid tier
unavailable" and silently stored nothing since 2026-09-30). This module now
uses the ``/stable`` API, which the configured free key does serve:

- ``earnings-calendar`` (one call): EPS/revenue actual + estimate for every
  reporter in a date window -- replaces 120 per-ticker history calls.
  Upcoming reports are stored as ``earnings:calendar:{T}`` observed today
  with value = days until the report (the old code stored a future
  obs_date, which is not point-in-time and tripped the future-date check).
- ``sector-performance-snapshot`` (one call per exchange): sector moves.
- ``quote`` (one call per ticker; ``batch-quote`` is a paid endpoint).

The API key travels in the ``apikey`` header, never in the URL, so request
URLs in exception text and logs carry no secret.

API: https://site.financialmodelingprep.com/developer/docs
Free tier: 250 requests/day. One ``pull()`` uses about 125.
"""

from __future__ import annotations

import time
from datetime import date, datetime, timedelta
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy.engine import Engine

from ingestion.base import BasePuller, retry_on_failure

FMP_BASE = "https://financialmodelingprep.com/stable"

# Earnings window read from one earnings-calendar call (FMP caps it at 90 days).
_CALENDAR_LOOKBACK_DAYS = 7
_CALENDAR_LOOKAHEAD_DAYS = 30
_SECTOR_EXCHANGES: tuple[str, ...] = ("NASDAQ", "NYSE")
_SECTOR_MAX_LOOKBACK_DAYS = 5  # weekends/holidays return an empty snapshot
_QUOTE_FIELDS: tuple[tuple[str, str], ...] = (
    ("price", "price"),
    ("changePercentage", "change_pct"),
    ("volume", "volume"),
    ("marketCap", "market_cap"),
)

# Top 120 tickers to track earnings for
EARNINGS_UNIVERSE: list[str] = [
    # Mega-cap tech
    "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "AVGO", "ORCL", "CRM",
    "AMD", "INTC", "QCOM", "TXN", "AMAT", "LRCX", "KLAC", "NOW", "INTU", "ADBE",
    # Financials
    "JPM", "V", "MA", "BRK-B", "GS", "MS", "BLK", "SCHW", "AXP", "C",
    "BAC", "WFC", "PNC", "USB", "TFC", "CME", "ICE", "CB", "MMC", "AIG",
    # Healthcare
    "UNH", "JNJ", "ABBV", "MRK", "PFE", "LLY", "TMO", "ABT", "DHR", "BMY",
    "GILD", "VRTX", "REGN", "CI", "HUM", "ISRG", "SYK", "ZTS", "MDT", "ELV",
    # Consumer
    "PG", "KO", "PEP", "COST", "WMT", "HD", "LOW", "MCD", "NKE", "SBUX",
    "TGT", "CL", "EL", "MDLZ", "PM", "MO", "DG", "DLTR", "ROST", "TJX",
    # Industrial / Energy / Other
    "CAT", "BA", "HON", "UNP", "RTX", "GD", "DE", "MMM", "GE", "LMT",
    "XOM", "CVX", "COP", "SLB", "EOG", "NEE", "DUK", "SO", "AEP", "D",
    # Crypto / Fintech / Growth
    "COIN", "SQ", "SHOP", "PLTR", "CRWD", "SNOW", "DDOG", "NET", "ZS", "SMCI",
    "ARM", "MARA", "RIOT", "PYPL", "FIS", "NFLX", "ABNB", "UBER", "DASH", "RBLX",
]



class FMPError(RuntimeError):
    """Non-2xx FMP response; the message never contains the request URL."""

    def __init__(self, status_code: int, endpoint: str, kind: str) -> None:
        self.status_code = status_code
        self.kind = kind
        super().__init__(f"FMP {kind} (HTTP {status_code}) on {endpoint}")


class FMPRestricted(FMPError):
    """The free plan does not serve this endpoint or symbol (permanent)."""


class FMPAuthError(FMPError):
    """The API key was rejected; nothing in this run can succeed."""


class FMPLimitReached(FMPError):
    """Daily/minute request limit reached; stop this run."""


def _inserted(result: Any) -> int:
    """Committed-row contribution of one INSERT; unknown counts fail closed."""
    affected = getattr(result, "rowcount", None)
    if type(affected) is not int or affected < 0:
        raise ValueError("Unknown affected row count")
    return affected


def _parse_day(value: Any) -> date | None:
    try:
        return datetime.strptime(str(value), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return None


def _finite(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if out == out and out not in (float("inf"), float("-inf")) else None


class FMPPuller(BasePuller):
    """Pull earnings, quotes and sector performance from Financial Modeling Prep."""

    SOURCE_NAME = "fmp"
    SOURCE_CONFIG = {
        "base_url": FMP_BASE,
        "cost_tier": "FREE",
        "latency_class": "EOD",
        "pit_available": True,
        "revision_behavior": "RARE",
        "trust_score": "HIGH",
        "priority_rank": 20,
    }

    def __init__(self, db_engine: Engine, api_key: str | None = None) -> None:
        super().__init__(db_engine)
        if api_key:
            self.api_key = api_key
        else:
            from config import settings
            self.api_key = getattr(settings, "FMP_API_KEY", "")
        if not self.api_key:
            log.warning("FMP_API_KEY not set — FMP puller disabled")

    @retry_on_failure(max_attempts=2, retryable_exceptions=(ConnectionError, TimeoutError, OSError))
    def _api_get(self, endpoint: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]] | dict[str, Any]:
        """Make an FMP ``/stable`` call with the key in the ``apikey`` header.

        Raises:
            FMPAuthError: 401 (bad key).
            FMPRestricted: 402/403 (plan does not include endpoint/symbol).
            FMPLimitReached: 429 (request limit).
            FMPError: any other non-2xx status.
        """
        resp = requests.get(
            f"{FMP_BASE}/{endpoint}",
            params=params or None,
            headers={"apikey": self.api_key},
            timeout=30,
        )
        code = resp.status_code
        if code == 401:
            raise FMPAuthError(code, endpoint, "API key rejected")
        if code in (402, 403):
            raise FMPRestricted(code, endpoint, "not available on this plan")
        if code == 429:
            raise FMPLimitReached(code, endpoint, "request limit reached")
        if code >= 400:
            raise FMPError(code, endpoint, "request failed")
        return resp.json()

    def _get_list(self, endpoint: str, params: dict[str, Any] | None = None) -> list[dict[str, Any]]:
        data = self._api_get(endpoint, params)
        return [row for row in data if isinstance(row, dict)] if isinstance(data, list) else []

    # ------------------------------------------------------------------
    # Endpoint helpers (``/stable`` names)
    # ------------------------------------------------------------------

    def pull_earnings_history(self, ticker: str) -> list[dict[str, Any]]:
        """Historical + upcoming earnings for one ticker (date, epsActual, ...)."""
        return self._get_list("earnings", {"symbol": ticker, "limit": 40})

    def pull_earnings_calendar(self, from_date: date | None = None, to_date: date | None = None) -> list[dict[str, Any]]:
        """Earnings for every reporter in [from_date, to_date] (default today..+30d)."""
        if from_date is None:
            from_date = date.today()
        if to_date is None:
            to_date = from_date + timedelta(days=30)
        return self._get_list("earnings-calendar", {
            "from": from_date.isoformat(), "to": to_date.isoformat(),
        })

    def pull_analyst_estimates(self, ticker: str) -> list[dict[str, Any]]:
        """Analyst EPS/revenue estimates (may require a paid plan)."""
        return self._get_list("analyst-estimates", {"symbol": ticker, "period": "annual", "limit": 12})

    def pull_income_statement(self, ticker: str, period: str = "quarter") -> list[dict[str, Any]]:
        """Income statements ("quarter" or "annual"; may require a paid plan)."""
        return self._get_list("income-statement", {"symbol": ticker, "period": period, "limit": 20})

    def pull_balance_sheet(self, ticker: str, period: str = "quarter") -> list[dict[str, Any]]:
        """Balance sheets (may require a paid plan)."""
        return self._get_list("balance-sheet-statement", {"symbol": ticker, "period": period, "limit": 20})

    def pull_cash_flow(self, ticker: str, period: str = "quarter") -> list[dict[str, Any]]:
        """Cash-flow statements (may require a paid plan)."""
        return self._get_list("cash-flow-statement", {"symbol": ticker, "period": period, "limit": 20})

    def pull_transcript(self, ticker: str, year: int, quarter: int) -> dict[str, Any] | None:
        """Earnings call transcript, or None (paid plan)."""
        data = self._get_list("earning-call-transcript", {"symbol": ticker, "year": year, "quarter": quarter})
        return data[0] if data else None

    def pull_quote(self, ticker: str) -> dict[str, Any] | None:
        """Quote (price, change %, volume, market cap). Free tier."""
        data = self._get_list("quote", {"symbol": ticker})
        return data[0] if data else None

    def pull_profile(self, ticker: str) -> dict[str, Any] | None:
        """Company profile (sector, industry, description, CEO)."""
        data = self._get_list("profile", {"symbol": ticker})
        return data[0] if data else None

    def pull_sector_performance(self, snapshot_date: date | None = None, exchange: str | None = None) -> list[dict[str, Any]]:
        """Sector average change for one trading day and exchange."""
        params: dict[str, Any] = {"date": (snapshot_date or date.today()).isoformat()}
        if exchange:
            params["exchange"] = exchange
        return self._get_list("sector-performance-snapshot", params)

    # ------------------------------------------------------------------
    # Storage
    # ------------------------------------------------------------------

    def _store_new(self, conn: Any, series_id: str, obs_date: date, value: float,
                   existing: set[date], raw_payload: dict[str, Any] | None = None) -> int:
        if obs_date in existing:
            return 0
        n = _inserted(self._insert_raw(
            conn, series_id=series_id, obs_date=obs_date, value=value,
            raw_payload=raw_payload, return_result=True,
        ))
        existing.add(obs_date)
        return n

    def _store_calendar(self, events: list[dict[str, Any]], universe: set[str], today: date,
                        window: tuple[date, date]) -> tuple[int, int]:
        """Store actuals (reported) and upcoming dates for universe tickers.

        Returns (rows_inserted, events_used). Counted after commit.
        """
        rows = 0
        used = 0
        by_ticker: dict[str, list[dict[str, Any]]] = {}
        for ev in events:
            sym = str(ev.get("symbol") or "")
            if sym in universe and _parse_day(ev.get("date")) is not None:
                by_ticker.setdefault(sym, []).append(ev)
        with self.engine.begin() as conn:
            for ticker, evs in by_ticker.items():
                existing: dict[str, set[date]] = {}

                def seen(suffix: str) -> set[date]:
                    sid = f"earnings:{ticker}:{suffix}"
                    if sid not in existing:
                        existing[sid] = self._get_existing_dates(sid, conn, start_date=window[0], end_date=window[1])
                    return existing[sid]

                for ev in evs:
                    obs = _parse_day(ev.get("date"))
                    eps_a, eps_e = _finite(ev.get("epsActual")), _finite(ev.get("epsEstimated"))
                    rev_a, rev_e = _finite(ev.get("revenueActual")), _finite(ev.get("revenueEstimated"))
                    used += 1
                    if eps_a is None:
                        if obs < today:
                            continue  # reported date passed, actual not published yet
                        # Upcoming report: observed TODAY (when it was known),
                        # value = days until the report; never a future obs_date.
                        sid = f"earnings:calendar:{ticker}"
                        if sid not in existing:
                            existing[sid] = self._get_existing_dates(sid, conn, start_date=today, end_date=today)
                        rows += self._store_new(conn, sid, today, float((obs - today).days),
                                                existing[sid], raw_payload={**ev, "report_date": obs.isoformat()})
                        continue
                    if obs > today:
                        continue  # an "actual" dated in the future is not credible
                    payload = {"ticker": ticker, "eps_actual": eps_a, "eps_estimate": eps_e,
                               "revenue_actual": rev_a, "revenue_estimate": rev_e,
                               "last_updated": ev.get("lastUpdated", "")}
                    rows += self._store_new(conn, f"earnings:{ticker}:eps_actual", obs, eps_a, seen("eps_actual"), payload)
                    if eps_e is not None:
                        rows += self._store_new(conn, f"earnings:{ticker}:eps_estimate", obs, eps_e, seen("eps_estimate"))
                        if eps_e != 0:
                            surprise = round((eps_a - eps_e) / abs(eps_e) * 100, 2)
                            rows += self._store_new(conn, f"earnings:{ticker}:surprise_pct", obs, surprise, seen("surprise_pct"))
                    if rev_a is not None:
                        rows += self._store_new(conn, f"earnings:{ticker}:revenue_actual", obs, rev_a, seen("revenue_actual"), {"ticker": ticker})
                    if rev_e is not None:
                        rows += self._store_new(conn, f"earnings:{ticker}:revenue_estimate", obs, rev_e, seen("revenue_estimate"))
        return rows, used

    # ------------------------------------------------------------------
    # Main pull
    # ------------------------------------------------------------------

    def pull(self, tickers: list[str] | None = None) -> dict[str, Any]:
        """Pull earnings (calendar window), sector snapshots and quotes.

        Args:
            tickers: Override ticker list. Defaults to EARNINGS_UNIVERSE.

        Returns:
            Write-contract summary: ``status``, ``rows_inserted`` (rows
            committed to raw_series), and ``succeeded``/``total`` coverage
            over the run's work items (calendar, each sector exchange,
            each ticker quote).
        """
        if not self.api_key:
            return {"status": "SKIPPED", "rows_inserted": 0,
                    "error": "FMP_API_KEY not configured",
                    "skipped_reason": "FMP_API_KEY not configured"}

        if tickers is None:
            tickers = EARNINGS_UNIVERSE
        universe = set(tickers)
        today = date.today()
        rows = 0
        api_calls = 0
        done = 0          # work items checked (written or cleanly empty)
        failures: list[str] = []
        restricted: list[str] = []
        beats: list[dict[str, Any]] = []
        misses: list[dict[str, Any]] = []
        total = 1 + len(_SECTOR_EXCHANGES) + len(tickers)
        stopped: str | None = None

        def fatal(exc: FMPError) -> bool:
            nonlocal stopped
            if isinstance(exc, (FMPAuthError, FMPLimitReached)):
                stopped = str(exc)
                return True
            return False

        # 1. Earnings calendar window: actuals for recent reporters + upcoming dates.
        window = (today - timedelta(days=_CALENDAR_LOOKBACK_DAYS), today + timedelta(days=_CALENDAR_LOOKAHEAD_DAYS))
        try:
            api_calls += 1
            events = self.pull_earnings_calendar(*window)
            n, _used = self._store_calendar(events, universe, today, window)
            rows += n
            done += 1
            for ev in events:
                eps_a, eps_e = _finite(ev.get("epsActual")), _finite(ev.get("epsEstimated"))
                if ev.get("symbol") in universe and eps_a is not None and eps_e:
                    pct = round((eps_a - eps_e) / abs(eps_e) * 100, 2)
                    rec = {"ticker": ev["symbol"], "date": ev.get("date"), "surprise_pct": pct,
                           "eps_actual": eps_a, "eps_estimate": eps_e}
                    if pct > 10:
                        beats.append(rec)
                    elif pct < -10:
                        misses.append(rec)
        except FMPError as exc:
            failures.append(f"earnings-calendar: {exc}")
            fatal(exc)
        except Exception as exc:
            failures.append(f"earnings-calendar: {type(exc).__name__}: {exc}")

        # 2. Sector performance snapshot for the latest trading day per exchange.
        for exchange in _SECTOR_EXCHANGES:
            if stopped:
                break
            try:
                snapshot: list[dict[str, Any]] = []
                snap_day = today
                for back in range(_SECTOR_MAX_LOOKBACK_DAYS):
                    snap_day = today - timedelta(days=back)
                    if snap_day.weekday() >= 5:
                        continue
                    api_calls += 1
                    snapshot = self.pull_sector_performance(snap_day, exchange)
                    if snapshot:
                        break
                with self.engine.begin() as conn:
                    n = 0
                    for row in snapshot:
                        sector = str(row.get("sector") or "").strip()
                        change = _finite(row.get("averageChange"))
                        obs = _parse_day(row.get("date")) or snap_day
                        if not sector or change is None:
                            continue
                        sid = f"fmp:sector:{sector.lower().replace(' ', '_')}:{exchange.lower()}"
                        existing = self._get_existing_dates(sid, conn, start_date=obs, end_date=obs)
                        n += self._store_new(conn, sid, obs, change, existing, raw_payload=row)
                rows += n
                done += 1
            except FMPError as exc:
                failures.append(f"sector {exchange}: {exc}")
                if fatal(exc):
                    break
            except Exception as exc:
                failures.append(f"sector {exchange}: {type(exc).__name__}: {exc}")

        # 3. Quotes, one call per ticker (batch-quote is a paid endpoint).
        for ticker in tickers:
            if stopped:
                break
            try:
                api_calls += 1
                quote = self.pull_quote(ticker)
                time.sleep(0.2)
                if not quote:
                    done += 1
                    continue
                with self.engine.begin() as conn:
                    n = 0
                    for field, suffix in _QUOTE_FIELDS:
                        val = _finite(quote.get(field))
                        if val is None:
                            continue
                        sid = f"fmp:{ticker}:{suffix}"
                        existing = self._get_existing_dates(sid, conn, start_date=today, end_date=today)
                        n += self._store_new(conn, sid, today, val, existing)
                rows += n
                done += 1
            except FMPRestricted:
                # e.g. BRK-B: the symbol is a premium symbol on this plan.
                restricted.append(ticker)
                done += 1
            except FMPError as exc:
                failures.append(f"quote {ticker}: {exc}")
                if fatal(exc):
                    break
            except Exception as exc:
                failures.append(f"quote {ticker}: {type(exc).__name__}: {exc}")

        log.info(
            "FMP pull: {r} rows, {d}/{t} items checked, {f} failed, {rs} restricted, "
            "{b} beats, {m} misses, {api} API calls",
            r=rows, d=done, t=total, f=len(failures), rs=len(restricted),
            b=len(beats), m=len(misses), api=api_calls,
        )
        if failures:
            log.warning("FMP pull incomplete: {n} failures, first: {e}", n=len(failures), e=failures[0])

        if done == total and not failures:
            status = "SUCCESS"
        elif done or rows:
            status = "PARTIAL"
        else:
            status = "FAILED"
        result: dict[str, Any] = {
            "status": status,
            "rows_inserted": rows,
            "succeeded": done,
            "total": total,
            "beats": beats,
            "misses": misses,
            "api_calls": api_calls,
            "restricted_symbols": restricted,
        }
        if status != "SUCCESS":
            result["error"] = stopped or (failures[0] if failures else f"{done} of {total} items checked")
            result["failures"] = failures[:10]
        return result
