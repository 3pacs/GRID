"""Small-cap fundamentals enrichment for ``company_profiles`` (extends
``company_profiles_puller``; does not replace it).

Why
---
The Trial Gem Hunter's ``mcap < $2B`` gate, its cash-runway score and the
Long Plays board all read ``company_profiles.profile`` — but the only writers
were the LLM company analyzer (89 rows, April 2026) and the FMP market-cap
puller. Nothing supplied cash / burn / runway / shares for the sub-$2B biotech
universe. This puller does, from free-first sources, and degrades to whatever
subset is available.

Universe
--------
tickers in ``trial_signals`` (180 d) ∪ resolved ``catalyst_calendar`` tickers
(active, ticker-shaped) ∪ tickers in ``options_mispricing_scans`` (30 d),
keeping those whose known market cap is < $2B or unknown.

Sources, in order (first non-None per field wins)
-------------------------------------------------
1. ``FMPPuller`` profile / quote / balance-sheet / cash-flow / income statement
   when ``FMP_API_KEY`` is set (budgeted to ``FMP_CALL_BUDGET`` calls per run).
2. Tiingo fundamentals ``raw_series`` ``TIINGO_FUND:{T}:market_cap`` (large-cap
   fallback for market cap only — the series covers ~26 tickers).
3. SEC XBRL companyfacts (``https://data.sec.gov/api/xbrl/companyfacts/CIK##########.json``,
   User-Agent ``GRID Intelligence ops@stepdad.finance``; CIK via the SEC
   ``company_tickers.json`` map in ``grid.signals.sponsor_resolver``) for
   cash / total debt / revenue TTM / net income TTM / shares / quarterly burn.
   Free, no key. 0.3 s between calls (well under the SEC 10 req/s limit).

Written into ``company_profiles.profile`` (JSONB merge — analyzer keys survive)
------------------------------------------------------------------------------
``market_cap, shares_outstanding, cash, total_debt, revenue_ttm, net_income_ttm,
quarterly_burn, cash_runway_months, float_pct, sector, industry, description
(≤ 400 chars), enriched_at, enrichment_source`` (+ ``name`` / ``sector`` columns).

``quarterly_burn`` is the positive operating cash outflow of the latest
quarter (0 when cash-flow positive); ``cash_runway_months = cash / max(quarterly_burn / 3, 1)``
i.e. cash over monthly burn, ``None`` when there is no burn or no cash figure.

Hermes: registered as ``small_cap_enrichment`` (24 h) after ``trial_signal``;
``pull_all(engine) -> dict`` is the entry point.
"""

from __future__ import annotations

import json
import re
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any, Callable

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from ingestion.altdata.company_profiles_puller import _UPSERT_SQL, _to_float
from intelligence.company_analyzer import ensure_table

# ── Config ────────────────────────────────────────────────────────────────────

SEC_COMPANYFACTS_URL = "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
SEC_UA = "GRID Intelligence ops@stepdad.finance"
SEC_TIMEOUT_S = 30
CALL_PAUSE_S = 0.3                 # between every external call (SEC limit is 10 req/s)

SMALL_CAP_MAX_USD = 2e9
TRIAL_LOOKBACK_DAYS = 180
OPTIONS_LOOKBACK_DAYS = 30
REFRESH_AFTER_HOURS = 20           # skip tickers enriched more recently (daily cadence)
FMP_CALL_BUDGET = 200              # FMP free tier: 250 req/day
DESCRIPTION_MAX = 400
MAX_TICKERS_PER_RUN = 400

TICKER_SHAPE_SQL_RE = r"^[A-Z.\-]{1,6}$"
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,6}$")
_QUARTER_FRAME_RE = re.compile(r"^CY(\d{4})Q([1-4])$")

ENRICHED_FIELDS: tuple[str, ...] = (
    "market_cap", "shares_outstanding", "cash", "total_debt", "revenue_ttm",
    "net_income_ttm", "quarterly_burn", "cash_runway_months", "float_pct",
    "sector", "industry", "description",
)

# XBRL concepts (first present wins). Instant concepts -> latest end date;
# duration concepts -> TTM from four consecutive quarters, else latest annual.
_SEC_INSTANT: dict[str, list[tuple[str, str]]] = {
    "cash": [
        ("us-gaap", "CashAndCashEquivalentsAtCarryingValue"),
        ("us-gaap", "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"),
        ("us-gaap", "Cash"),
    ],
    "total_debt": [
        ("us-gaap", "LongTermDebt"),
        ("us-gaap", "LongTermDebtAndCapitalLeaseObligations"),
        ("us-gaap", "LongTermDebtNoncurrent"),
        ("us-gaap", "DebtInstrumentCarryingAmount"),
    ],
    "shares_outstanding": [
        ("dei", "EntityCommonStockSharesOutstanding"),
        ("us-gaap", "CommonStockSharesOutstanding"),
    ],
}
_SEC_DURATION: dict[str, list[tuple[str, str]]] = {
    "revenue_ttm": [
        ("us-gaap", "Revenues"),
        ("us-gaap", "RevenueFromContractWithCustomerExcludingAssessedTax"),
        ("us-gaap", "SalesRevenueNet"),
    ],
    "net_income_ttm": [
        ("us-gaap", "NetIncomeLoss"),
        ("us-gaap", "ProfitLoss"),
    ],
    "operating_cf": [
        ("us-gaap", "NetCashProvidedByUsedInOperatingActivities"),
    ],
}


# ── Pure helpers (no I/O) ─────────────────────────────────────────────────────


def compute_runway_months(cash: float | None, quarterly_burn: float | None) -> float | None:
    """Months of cash at the current burn: ``cash / max(quarterly_burn / 3, 1)``.

    ``None`` when cash is unknown, or when there is no burn (cash-flow
    positive / unknown) — a runway only makes sense for a company that is
    spending down its cash.
    """
    c = _to_float(cash)
    b = _to_float(quarterly_burn)
    if c is None or c < 0 or b is None or b <= 0:
        return None
    return round(c / max(b / 3.0, 1.0), 1)


def _entries(facts: dict[str, Any], taxonomy: str, concept: str) -> list[dict[str, Any]]:
    units = (((facts or {}).get("facts") or {}).get(taxonomy) or {}).get(concept, {}).get("units") or {}
    if not units:
        return []
    # Prefer USD, then shares, then whatever single unit is present.
    for key in ("USD", "shares"):
        if key in units and isinstance(units[key], list):
            return units[key]
    first = next(iter(units.values()), [])
    return first if isinstance(first, list) else []


def _iso(value: Any) -> date | None:
    try:
        return date.fromisoformat(str(value)) if value else None
    except ValueError:
        return None


def _latest_instant(entries: list[dict[str, Any]]) -> tuple[float | None, str | None]:
    """Value with the latest ``end`` (ties broken by ``filed``)."""
    best: tuple[date, str, float, str] | None = None
    for e in entries:
        val = _to_float(e.get("val"))
        end = _iso(e.get("end"))
        if val is None or end is None:
            continue
        key = (end, str(e.get("filed") or ""), val, end.isoformat())
        if best is None or key[:2] > best[:2]:
            best = key
    return (best[2], best[3]) if best else (None, None)


def _ttm(entries: list[dict[str, Any]]) -> tuple[float | None, str | None]:
    """Trailing-twelve-month sum of the four latest consecutive quarterly
    values; else the latest annual value. Returns ``(value, period_end)``."""
    quarters: dict[tuple[int, int], float] = {}
    annual: list[tuple[date, float]] = []
    for e in entries:
        val = _to_float(e.get("val"))
        start, end = _iso(e.get("start")), _iso(e.get("end"))
        if val is None or start is None or end is None:
            continue
        span = (end - start).days
        if 80 <= span <= 100:
            m = _QUARTER_FRAME_RE.match(str(e.get("frame") or ""))
            key = (int(m.group(1)), int(m.group(2))) if m else (end.year, (end.month - 1) // 3 + 1)
            if m or key not in quarters:
                quarters[key] = val
        elif 350 <= span <= 380:
            annual.append((end, val))
    keys = sorted(quarters)
    if len(keys) >= 4:
        last4 = keys[-4:]
        idx = [y * 4 + q for y, q in last4]
        if idx == list(range(idx[0], idx[0] + 4)):
            y, q = last4[-1]
            return round(sum(quarters[k] for k in last4), 2), f"{y}Q{q}"
    if annual:
        annual.sort()
        end, val = annual[-1]
        return val, end.isoformat()
    return None, None


def _latest_quarter(entries: list[dict[str, Any]]) -> tuple[float | None, str | None]:
    """Latest single-quarter duration value (for burn); falls back to annual / 4."""
    best_q: tuple[date, float] | None = None
    best_a: tuple[date, float] | None = None
    for e in entries:
        val = _to_float(e.get("val"))
        start, end = _iso(e.get("start")), _iso(e.get("end"))
        if val is None or start is None or end is None:
            continue
        span = (end - start).days
        if 80 <= span <= 100 and (best_q is None or end > best_q[0]):
            best_q = (end, val)
        elif 350 <= span <= 380 and (best_a is None or end > best_a[0]):
            best_a = (end, val)
    if best_q is not None and (best_a is None or best_q[0] >= best_a[0] - timedelta(days=100)):
        return best_q[1], best_q[0].isoformat()
    if best_a is not None:
        return best_a[1] / 4.0, best_a[0].isoformat()
    return None, None


def parse_sec_companyfacts(facts: dict[str, Any]) -> dict[str, Any]:
    """Extract the enrichment fields from a decoded SEC companyfacts document.

    Pure function. Missing concepts yield ``None``; ``quarterly_burn`` is the
    positive operating cash outflow of the latest quarter (0.0 when positive).
    """
    out: dict[str, Any] = {k: None for k in ("cash", "total_debt", "shares_outstanding",
                                             "revenue_ttm", "net_income_ttm", "quarterly_burn")}
    out["name"] = (facts or {}).get("entityName") or None
    out["fiscal_period_end"] = None
    for field, concepts in _SEC_INSTANT.items():
        for taxonomy, concept in concepts:
            val, end = _latest_instant(_entries(facts, taxonomy, concept))
            if val is not None:
                out[field] = val
                if field == "cash":
                    out["fiscal_period_end"] = end
                break
    for field, concepts in _SEC_DURATION.items():
        for taxonomy, concept in concepts:
            entries = _entries(facts, taxonomy, concept)
            if not entries:
                continue
            if field == "operating_cf":
                ocf, _end = _latest_quarter(entries)
                if ocf is not None:
                    out["quarterly_burn"] = round(-ocf, 2) if ocf < 0 else 0.0
                    break
            else:
                val, _end = _ttm(entries)
                if val is not None:
                    out[field] = val
                    break
    return out


def merge_sources(
    fmp: dict[str, Any] | None,
    tiingo_market_cap: float | None,
    sec: dict[str, Any] | None,
) -> dict[str, Any]:
    """First non-None per field in source order FMP → Tiingo (mcap) → SEC.

    Adds ``enrichment_source`` (comma-joined contributors) and derives
    ``cash_runway_months`` from the merged cash / burn.
    """
    fmp = fmp or {}
    sec = sec or {}
    merged: dict[str, Any] = {}
    contributors: list[str] = []
    for field in (*ENRICHED_FIELDS, "name"):
        if field == "cash_runway_months":
            continue
        val = fmp.get(field)
        src = "fmp"
        if val is None and field == "market_cap" and tiingo_market_cap is not None:
            val, src = tiingo_market_cap, "tiingo"
        if val is None and sec.get(field) is not None:
            val, src = sec.get(field), "sec_xbrl"
        merged[field] = val
        if val is not None and src not in contributors:
            contributors.append(src)
    merged["cash_runway_months"] = compute_runway_months(merged.get("cash"), merged.get("quarterly_burn"))
    if sec.get("fiscal_period_end"):
        merged["fiscal_period_end"] = sec["fiscal_period_end"]
    merged["enrichment_source"] = ",".join(contributors) if contributors else None
    return merged


def shape_enrichment_row(ticker: str, fields: dict[str, Any], now: datetime | None = None) -> dict[str, Any] | None:
    """Bind-ready ``company_profiles`` upsert row, or ``None`` when nothing was learned."""
    ticker = (ticker or "").strip().upper()
    if not ticker or not fields:
        return None
    if all(fields.get(f) is None for f in ENRICHED_FIELDS):
        return None
    now = now or datetime.now(timezone.utc)
    profile: dict[str, Any] = {f: fields.get(f) for f in ENRICHED_FIELDS}
    desc = profile.get("description")
    if isinstance(desc, str):
        profile["description"] = desc.strip()[:DESCRIPTION_MAX] or None
    profile["ticker"] = ticker
    if fields.get("name"):
        profile["name"] = fields["name"]
    if fields.get("fiscal_period_end"):
        profile["fiscal_period_end"] = fields["fiscal_period_end"]
    profile["enriched_at"] = now.isoformat()
    profile["enrichment_source"] = fields.get("enrichment_source")
    return {
        "ticker": ticker,
        "name": fields.get("name") or None,
        "sector": fields.get("sector") or None,
        "profile": profile,
        "last_analyzed": now,
    }


# ── SQL (all parameterised) ───────────────────────────────────────────────────

_TRIAL_TICKERS_SQL = text(
    "SELECT DISTINCT ticker FROM trial_signals WHERE ticker IS NOT NULL AND created_at >= :start"
)
_CALENDAR_TICKERS_SQL = text(
    "SELECT DISTINCT ticker FROM catalyst_calendar WHERE is_active = TRUE AND ticker ~ :shape"
)
_OPTIONS_TICKERS_SQL = text(
    "SELECT DISTINCT ticker FROM options_mispricing_scans WHERE ticker IS NOT NULL AND scan_date >= :start"
)
_PROFILE_CAPS_SQL = text(
    "SELECT ticker, profile->>'market_cap', profile->>'enriched_at' "
    "FROM company_profiles WHERE ticker = ANY(:tickers)"
)
_METRICS_CAPS_SQL = text(
    """
    SELECT DISTINCT ON (ticker) ticker, market_cap_usd
    FROM ticker_metrics_daily
    WHERE ticker = ANY(:tickers) AND market_cap_usd IS NOT NULL AND obs_date <= :as_of
    ORDER BY ticker, obs_date DESC
    """
)
_TIINGO_MCAP_SQL = text(
    """
    SELECT value FROM raw_series
    WHERE series_id = :sid AND pull_status = 'SUCCESS' AND obs_date <= :as_of
    ORDER BY obs_date DESC, pull_timestamp DESC
    LIMIT 1
    """
)


# ── Puller ────────────────────────────────────────────────────────────────────


class SmallCapEnrichmentPuller:
    """Enriches ``company_profiles`` for the small-cap trial / options universe.

    Attributes:
        engine: SQLAlchemy engine connected to the GRID database.
    """

    def __init__(
        self,
        db_engine: Engine,
        api_key: str | None = None,
        *,
        http_get: Callable[..., Any] | None = None,
        sleep: Callable[[float], None] | None = None,
    ) -> None:
        self.engine = db_engine
        self._api_key = api_key
        self._fmp: Any = None
        self._fmp_calls = 0
        self._http_get = http_get or requests.get
        self._sleep = sleep or time.sleep

    # ── FMP (lazy; only when a key exists) ────────────────────────────────

    @property
    def fmp_api_key(self) -> str:
        if self._api_key is not None:
            return self._api_key
        try:
            from config import settings

            self._api_key = str(getattr(settings, "FMP_API_KEY", "") or "")
        except Exception:  # noqa: BLE001
            self._api_key = ""
        return self._api_key

    @property
    def fmp(self) -> Any:
        """``FMPPuller`` when a key is configured, else ``None``."""
        if self._fmp is None and self.fmp_api_key:
            try:
                from ingestion.altdata.fmp_puller import FMPPuller

                self._fmp = FMPPuller(self.engine, api_key=self.fmp_api_key)
            except Exception as exc:  # noqa: BLE001
                log.warning("small_cap_enrichment: FMP puller unavailable: {e}", e=str(exc))
                self._fmp = None
        return self._fmp

    def _fmp_call(self, fn: Callable[..., Any], *args: Any) -> Any:
        if self._fmp_calls >= FMP_CALL_BUDGET:
            return None
        self._fmp_calls += 1
        try:
            out = fn(*args)
        except Exception as exc:  # noqa: BLE001
            log.warning("small_cap_enrichment: FMP {f} failed: {e}", f=getattr(fn, "__name__", "call"), e=str(exc))
            out = None
        self._sleep(CALL_PAUSE_S)
        return out

    def fmp_fields(self, ticker: str) -> dict[str, Any]:
        """Profile / quote / statements from FMP mapped onto the enrichment fields."""
        fmp = self.fmp
        if fmp is None:
            return {}
        profile = self._fmp_call(fmp.pull_profile, ticker) or {}
        quote = self._fmp_call(fmp.pull_quote, ticker) or {}
        balance = self._fmp_call(fmp.pull_balance_sheet, ticker, "quarter") or []
        cashflow = self._fmp_call(fmp.pull_cash_flow, ticker, "quarter") or []
        income = self._fmp_call(fmp.pull_income_statement, ticker, "quarter") or []
        bs0 = balance[0] if balance else {}
        cf0 = cashflow[0] if cashflow else {}
        ocf = _to_float(cf0.get("operatingCashFlow"))
        rev4 = [_to_float(s.get("revenue")) for s in income[:4]]
        ni4 = [_to_float(s.get("netIncome")) for s in income[:4]]
        return {
            "market_cap": _to_float(quote.get("marketCap")) or _to_float(profile.get("mktCap")),
            "shares_outstanding": _to_float(quote.get("sharesOutstanding")),
            "cash": _to_float(bs0.get("cashAndShortTermInvestments")) or _to_float(bs0.get("cashAndCashEquivalents")),
            "total_debt": _to_float(bs0.get("totalDebt")),
            "revenue_ttm": sum(v for v in rev4 if v is not None) if len(rev4) == 4 and all(v is not None for v in rev4) else None,
            "net_income_ttm": sum(v for v in ni4 if v is not None) if len(ni4) == 4 and all(v is not None for v in ni4) else None,
            "quarterly_burn": (round(-ocf, 2) if ocf < 0 else 0.0) if ocf is not None else None,
            "float_pct": None,
            "sector": profile.get("sector") or None,
            "industry": profile.get("industry") or None,
            "description": profile.get("description") or None,
            "name": profile.get("companyName") or quote.get("name") or None,
        }

    # ── Tiingo (raw_series) ───────────────────────────────────────────────

    def tiingo_market_cap(self, ticker: str, as_of: date) -> float | None:
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    _TIINGO_MCAP_SQL, {"sid": f"TIINGO_FUND:{ticker}:market_cap", "as_of": as_of}
                ).first()
        except Exception as exc:  # noqa: BLE001
            log.debug("small_cap_enrichment: tiingo lookup failed for {t}: {e}", t=ticker, e=str(exc))
            return None
        val = _to_float(row[0]) if row else None
        return val if val and val > 0 else None

    # ── SEC companyfacts ──────────────────────────────────────────────────

    def sec_fields(self, ticker: str) -> dict[str, Any]:
        """Parsed companyfacts for ``ticker`` ({} when CIK unknown / network failure)."""
        try:
            from grid.signals.sponsor_resolver import sec_cik_for_ticker

            cik = sec_cik_for_ticker(ticker)
        except Exception as exc:  # noqa: BLE001
            log.warning("small_cap_enrichment: CIK map unavailable: {e}", e=str(exc))
            return {}
        if not cik:
            return {}
        try:
            resp = self._http_get(
                SEC_COMPANYFACTS_URL.format(cik=cik),
                headers={"User-Agent": SEC_UA, "Accept": "application/json"},
                timeout=SEC_TIMEOUT_S,
            )
            self._sleep(CALL_PAUSE_S)
            if getattr(resp, "status_code", 200) != 200:
                log.warning("small_cap_enrichment: SEC companyfacts {t} HTTP {s}", t=ticker, s=resp.status_code)
                return {}
            facts = resp.json()
        except Exception as exc:  # noqa: BLE001
            log.warning("small_cap_enrichment: SEC companyfacts {t} failed: {e}", t=ticker, e=str(exc))
            return {}
        return parse_sec_companyfacts(facts if isinstance(facts, dict) else {})

    # ── Universe ──────────────────────────────────────────────────────────

    def _distinct(self, sql: Any, params: dict[str, Any]) -> set[str]:
        out: set[str] = set()
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(sql, params).fetchall()
        except Exception as exc:  # noqa: BLE001
            log.debug("small_cap_enrichment: universe query skipped: {e}", e=str(exc))
            return out
        for row in rows:
            tk = str(row[0] or "").strip().upper()
            if tk and _TICKER_RE.match(tk):
                out.add(tk)
        return out

    def known_caps(self, tickers: list[str], as_of: date) -> tuple[dict[str, float], dict[str, datetime]]:
        """``(ticker -> known market cap USD, ticker -> enriched_at)`` from GRID tables."""
        caps: dict[str, float] = {}
        enriched: dict[str, datetime] = {}
        if not tickers:
            return caps, enriched
        try:
            with self.engine.connect() as conn:
                for row in conn.execute(_PROFILE_CAPS_SQL, {"tickers": tickers}).fetchall():
                    tk = str(row[0] or "").upper()
                    cap = _to_float(row[1])
                    if cap and cap > 0:
                        caps[tk] = cap
                    if row[2]:
                        try:
                            ts = datetime.fromisoformat(str(row[2]))
                            enriched[tk] = ts if ts.tzinfo else ts.replace(tzinfo=timezone.utc)
                        except ValueError:
                            pass
                for row in conn.execute(_METRICS_CAPS_SQL, {"tickers": tickers, "as_of": as_of}).fetchall():
                    tk = str(row[0] or "").upper()
                    cap = _to_float(row[1])
                    if cap and cap > 0:
                        caps.setdefault(tk, cap)
        except Exception as exc:  # noqa: BLE001
            log.debug("small_cap_enrichment: known-cap lookup failed: {e}", e=str(exc))
        return caps, enriched

    def universe(self, as_of: date | None = None, *, force: bool = False) -> list[str]:
        """Small-cap-or-unknown tickers from trial_signals ∪ catalyst_calendar ∪ options scans."""
        as_of = as_of or date.today()
        as_of_ts = datetime.combine(as_of, datetime.max.time()).replace(tzinfo=timezone.utc)
        pool: set[str] = set()
        pool |= self._distinct(_TRIAL_TICKERS_SQL, {"start": as_of_ts - timedelta(days=TRIAL_LOOKBACK_DAYS)})
        pool |= self._distinct(_CALENDAR_TICKERS_SQL, {"shape": TICKER_SHAPE_SQL_RE})
        pool |= self._distinct(_OPTIONS_TICKERS_SQL, {"start": as_of - timedelta(days=OPTIONS_LOOKBACK_DAYS)})
        tickers = sorted(pool)
        caps, enriched = self.known_caps(tickers, as_of)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=REFRESH_AFTER_HOURS)
        out: list[str] = []
        for tk in tickers:
            cap = caps.get(tk)
            if cap is not None and cap >= SMALL_CAP_MAX_USD:
                continue
            if not force and enriched.get(tk) is not None and enriched[tk] >= cutoff:
                continue
            out.append(tk)
        return out[:MAX_TICKERS_PER_RUN]

    # ── Enrichment ────────────────────────────────────────────────────────

    def enrich_ticker(self, ticker: str, as_of: date | None = None) -> dict[str, Any] | None:
        """Fetch + merge + shape one ticker (no DB write). ``None`` when nothing was learned."""
        ticker = (ticker or "").strip().upper()
        if not ticker:
            return None
        as_of = as_of or date.today()
        fmp = self.fmp_fields(ticker)
        tiingo = self.tiingo_market_cap(ticker, as_of) if fmp.get("market_cap") is None else None
        need_sec = any(fmp.get(f) is None for f in ("cash", "revenue_ttm", "net_income_ttm", "shares_outstanding", "quarterly_burn"))
        sec = self.sec_fields(ticker) if need_sec else {}
        merged = merge_sources(fmp, tiingo, sec)
        return shape_enrichment_row(ticker, merged)

    def pull_all(
        self,
        tickers: list[str] | None = None,
        *,
        force: bool = False,
        as_of: date | None = None,
    ) -> dict[str, Any]:
        """Enrich the universe (or ``tickers``) and upsert into ``company_profiles``.

        Returns ``{status, tickers_attempted, rows_upserted, fmp_calls, errors, ...}``.
        Never raises.
        """
        started = time.monotonic()
        as_of = as_of or date.today()
        try:
            ensure_table(self.engine)
        except Exception as exc:  # noqa: BLE001
            log.warning("small_cap_enrichment: ensure_table failed: {e}", e=str(exc))
        if tickers is None:
            tickers = self.universe(as_of, force=force)
        tickers = [t for t in (str(x).strip().upper() for x in tickers) if t]
        if not tickers:
            log.info("small_cap_enrichment: no small-cap tickers to enrich")
            return {"status": "SUCCESS", "tickers_attempted": 0, "rows_upserted": 0, "fmp_calls": 0,
                    "fmp_enabled": bool(self.fmp_api_key), "errors": []}

        upserted = 0
        skipped = 0
        errors: list[str] = []
        sources: dict[str, int] = {}
        for ticker in tickers:
            try:
                row = self.enrich_ticker(ticker, as_of)
                if row is None:
                    skipped += 1
                    continue
                for src in (row["profile"].get("enrichment_source") or "").split(","):
                    if src:
                        sources[src] = sources.get(src, 0) + 1
                bound = dict(row)
                bound["profile"] = json.dumps(row["profile"], default=str)
                with self.engine.begin() as conn:
                    conn.execute(text(_UPSERT_SQL), bound)
                upserted += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("small_cap_enrichment: {t} failed: {e}", t=ticker, e=str(exc))
                errors.append(f"{ticker}: {str(exc)[:120]}")
        log.info(
            "small_cap_enrichment: {n}/{t} rows upserted (skipped={s}, fmp_calls={c}, sources={src})",
            n=upserted, t=len(tickers), s=skipped, c=self._fmp_calls, src=sources,
        )
        return {
            "status": "SUCCESS" if not errors or upserted else "PARTIAL",
            "tickers_attempted": len(tickers),
            "rows_upserted": upserted,
            "skipped_no_data": skipped,
            "fmp_calls": self._fmp_calls,
            "fmp_enabled": bool(self.fmp_api_key),
            "sources": sources,
            "errors": errors,
            "elapsed_s": round(time.monotonic() - started, 1),
        }


def pull_all(engine: Engine, tickers: list[str] | None = None, **kwargs: Any) -> dict[str, Any]:
    """Hermes entry point (registry ``small_cap_enrichment``)."""
    try:
        return SmallCapEnrichmentPuller(engine).pull_all(tickers, **kwargs)
    except Exception as exc:  # noqa: BLE001
        log.warning("small_cap_enrichment.pull_all failed: {e}", e=str(exc))
        return {"status": "FAILED", "error": str(exc)[:200], "rows_upserted": 0}
