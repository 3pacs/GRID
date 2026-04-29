"""SEC XBRL Company Facts ingestor for normalized capital_flows.

Pulls the SEC EDGAR XBRL Company Facts API for every US-listed ticker
in ``analysis.sector_map.SECTOR_MAP`` and writes normalized
``capital_flows`` rows (one per flow_type per fiscal_period).

Data source:
    - Base URL: ``https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json``
    - Ticker → CIK map: ``https://www.sec.gov/files/company_tickers.json``
    - Rate limit: 10 req/sec (SEC Fair Access). We sleep ~120ms between.
    - Auth: none. Requires a descriptive ``User-Agent`` header.

Taxonomies:
    - Domestic issuers report in the ``us-gaap`` taxonomy via 10-K / 10-Q.
    - Foreign private issuers (TSM, ASML, BHP, RIO, NTR, AZN, BP, NVO,
      SAN, TD, RY, SHOP, SE, BABA, JD, UL, BTI, NSRGY, FMX, KOF, DEO,
      BUD, HEINY, and others) report in ``ifrs-full`` via 20-F / 6-K,
      frequently in local currency (EUR, GBP, JPY, CHF, CAD, ...).
    - This puller tries us-gaap first, then falls back to ifrs-full,
      and auto-converts local currency to USD using the latest
      ``DEX*`` FX rates available in ``raw_series``. When no FX rate
      is available the local amount is still stored (with
      ``confidence='estimated'``) and the ``currency`` column records
      the source unit so downstream consumers can convert later.

Schema:
    Writes into ``capital_flows`` (migration 0021 + migration 0024 for
    the ``currency`` column). Unique constraint:
    ``(actor_id, fiscal_period, period_type, flow_type, counterparty_id,
    source_filing)``. All inserts use ``ON CONFLICT DO UPDATE`` so that
    higher-confidence SEC rows refresh (not duplicate) existing rows —
    the hand-curated seed data in ``data/seed/capital_flow_seed.json``
    remains as a floor.

Idempotency:
    - ``actor_id`` is the lowercase ticker (matches seed convention).
    - ``counterparty_id`` is written as ``NULL`` (not ``''``) so the
      ``capital_flow`` router's ``COALESCE(NULLIF(counterparty_id, ''))``
      dedup key folds cleanly across seed and SEC rows. Historically
      this puller wrote ``''`` for "no counterparty", which double-
      counted against the seed loader's NULL convention. PostgreSQL
      treats NULLs as distinct in UNIQUE constraints — so dedup relies
      on the row already existing under the same NULL key via the
      existing ON CONFLICT path (which handles NULL==NULL correctly
      on re-runs because we UPSERT per-tuple and never insert the same
      tuple twice in a single transaction).
    - Re-running this puller will UPDATE existing rows with the latest
      amount + as_of timestamp; it will never create duplicates.

Checkpointing:
    Progress is written to ``/tmp/grid_xbrl_checkpoint.json`` after every
    ticker. Resume logic: on startup we read the checkpoint and skip any
    tickers before (and including) the last successful one.

Runtime safety:
    - Per-request timeout: 30s.
    - Total runtime cap: 1 hour per invocation (resumable).
    - 404 / 403 → log warning and continue (some tickers have no XBRL).
"""

from __future__ import annotations

import json
import os
import time
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import requests
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ───────────────────────────────────────────────────────────

_SEC_USER_AGENT: str = "GRID Intelligence ops@stepdad.finance"
_SEC_HEADERS: dict[str, str] = {
    "User-Agent": _SEC_USER_AGENT,
    "Accept": "application/json",
    "Host": "data.sec.gov",
}
_SEC_TICKER_HEADERS: dict[str, str] = {
    "User-Agent": _SEC_USER_AGENT,
    "Accept": "application/json",
}

_COMPANY_FACTS_URL: str = (
    "https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
)
_COMPANY_TICKERS_URL: str = "https://www.sec.gov/files/company_tickers.json"

_REQUEST_TIMEOUT_SECS: int = 30
_RATE_LIMIT_DELAY_SECS: float = 0.12  # ~8.3 req/sec, safely under 10/sec cap
_MAX_RUNTIME_SECS: int = 3600  # 1 hour

_CHECKPOINT_PATH: Path = Path("/tmp/grid_xbrl_checkpoint.json")

# XBRL us-gaap tag → (flow_type, direction, candidate tags to try in order).
# For debt_issuance we compute a net: ProceedsFromIssuanceOfLongTermDebt
# minus RepaymentsOfLongTermDebt, only writing if the net is > 0.
_FLOW_MAPPINGS: list[dict[str, Any]] = [
    {
        # Tags listed lowest-priority first, highest-priority last.
        # Newer ASC 606 taxonomy (RevenueFromContract...) wins over
        # legacy ``Revenues`` / ``SalesRevenueNet``.
        "flow_type": "revenue",
        "direction": "in",
        "tags": [
            "SalesRevenueNet",
            "Revenues",
            "RevenueFromContractWithCustomerExcludingAssessedTax",
        ],
    },
    {
        # Order is lowest-priority first, highest-priority last.
        # CostsAndExpenses is the broadest aggregate (used by integrated
        # oil majors like XOM that don't break out a discrete COGS line);
        # we accept it as a last-resort fallback so gross_margin is at
        # least populated. Pure-COGS tags win when they exist.
        "flow_type": "cogs",
        "direction": "out",
        "tags": [
            "CostsAndExpenses",
            "CostOfGoodsAndServicesSold",
            "CostOfRevenue",
            "CostOfGoodsSold",
        ],
    },
    {
        # OperatingExpenses is the broader total; SG&A is a subset used
        # when no total is reported. Put the broader one last so it wins.
        "flow_type": "opex",
        "direction": "out",
        "tags": [
            "SellingGeneralAndAdministrativeExpense",
            "OperatingExpenses",
        ],
    },
    {
        "flow_type": "r_and_d",
        "direction": "out",
        "tags": ["ResearchAndDevelopmentExpense"],
    },
    {
        "flow_type": "capex",
        "direction": "out",
        "tags": ["PaymentsToAcquirePropertyPlantAndEquipment"],
    },
    {
        "flow_type": "interest_paid",
        "direction": "out",
        "tags": ["InterestExpense", "InterestPaid"],
    },
    {
        "flow_type": "tax",
        "direction": "out",
        "tags": ["IncomeTaxExpenseBenefit"],
    },
    {
        "flow_type": "dividends",
        "direction": "out",
        "tags": ["PaymentsOfDividendsCommonStock", "PaymentsOfDividends"],
    },
    {
        "flow_type": "buybacks",
        "direction": "out",
        "tags": ["PaymentsForRepurchaseOfCommonStock"],
    },
    {
        "flow_type": "acquisitions",
        "direction": "out",
        "tags": ["PaymentsToAcquireBusinessesNetOfCashAcquired"],
    },
    {
        "flow_type": "equity_issuance",
        "direction": "in",
        "tags": ["ProceedsFromIssuanceOfCommonStock"],
    },
]

# IFRS-full tag → flow_type mapping. Tried after us-gaap comes up empty
# for a given flow. Ordering within each list is lowest → highest
# priority, matching the us-gaap convention.
_IFRS_TAG_MAP: dict[str, list[str]] = {
    "revenue": [
        "Revenue",
        "RevenueFromContractsWithCustomers",
    ],
    "cogs": [
        "CostOfGoodsSold",
        "CostOfSales",
    ],
    "opex": [
        "GeneralAndAdministrativeExpense",
        "SellingGeneralAndAdministrativeExpense",
        "OperatingExpense",
    ],
    "r_and_d": [
        "ResearchAndDevelopmentExpense",
    ],
    "capex": [
        "PurchaseOfPropertyPlantAndEquipment",
    ],
    "interest_paid": [
        "InterestExpense",
        "FinanceCosts",
    ],
    "tax": [
        "IncomeTaxExpenseContinuingOperations",
    ],
    "dividends": [
        "DividendsPaid",
    ],
    "buybacks": [
        "PurchaseOfTreasuryShares",
    ],
    "acquisitions": [
        "CashFlowsFromUsedInObtainingControlOfSubsidiariesOrOtherBusinesses",
    ],
    "equity_issuance": [
        "ProceedsFromIssuingShares",
    ],
}

# Special: debt_issuance = net of proceeds minus repayments.
_DEBT_PROCEEDS_TAG: str = "ProceedsFromIssuanceOfLongTermDebt"
_DEBT_REPAY_TAG: str = "RepaymentsOfLongTermDebt"

# IFRS equivalents for the debt-issuance net calc.
_IFRS_DEBT_PROCEEDS_TAG: str = (
    "ProceedsFromBorrowingsClassifiedAsFinancingActivities"
)
_IFRS_DEBT_REPAY_TAG: str = (
    "RepaymentsOfBorrowingsClassifiedAsFinancingActivities"
)

_ANNUAL_FORMS: frozenset[str] = frozenset(
    {"10-K", "10-K/A", "20-F", "20-F/A", "40-F", "40-F/A"}
)
_QUARTER_FORMS: frozenset[str] = frozenset(
    {"10-Q", "10-Q/A", "6-K", "6-K/A"}
)

# Preferred currency order when a fact has multiple ``units`` entries.
# USD wins; then common reporting currencies.
_PREFERRED_CURRENCIES: tuple[str, ...] = (
    "USD", "EUR", "GBP", "JPY", "CHF", "CAD", "AUD", "CNY", "HKD",
    "SGD", "KRW", "TWD", "DKK", "SEK", "NOK", "BRL", "MXN", "INR",
    "ZAR", "ILS",
)

# Hard-coded FX fallback rates — USD per 1 unit of foreign currency —
# used ONLY when ``raw_series`` has no DEX* row for the pair. These are
# approximate early-2026 spot rates; any row using the fallback is
# marked ``confidence='estimated'``. Better than storing a raw local
# number with no USD equivalent at all.
_FX_FALLBACK_USD_PER_CCY: dict[str, float] = {
    "USD": 1.0,
    "EUR": 1.08,
    "GBP": 1.27,
    "JPY": 0.0066,   # ~151 JPY/USD
    "CHF": 1.11,
    "CAD": 0.74,
    "AUD": 0.66,
    "CNY": 0.14,
    "HKD": 0.128,
    "SGD": 0.74,
    "KRW": 0.00074,
    "TWD": 0.031,
    "DKK": 0.145,
    "SEK": 0.096,
    "NOK": 0.094,
    "BRL": 0.20,
    "MXN": 0.058,
    "INR": 0.012,
    "ZAR": 0.053,
    "ILS": 0.27,
}

# FRED DEX* series → (currency, "usd_per_ccy" | "ccy_per_usd").
# DEXUSEU = USD per EUR, etc.
_FRED_FX_SERIES: dict[str, tuple[str, str]] = {
    "DEXUSEU": ("EUR", "usd_per_ccy"),
    "DEXUSUK": ("GBP", "usd_per_ccy"),
    "DEXUSAL": ("AUD", "usd_per_ccy"),
    "DEXUSNZ": ("NZD", "usd_per_ccy"),
    "DEXJPUS": ("JPY", "ccy_per_usd"),
    "DEXCAUS": ("CAD", "ccy_per_usd"),
    "DEXSZUS": ("CHF", "ccy_per_usd"),
    "DEXCHUS": ("CNY", "ccy_per_usd"),
    "DEXHKUS": ("HKD", "ccy_per_usd"),
    "DEXSIUS": ("SGD", "ccy_per_usd"),
    "DEXKOUS": ("KRW", "ccy_per_usd"),
    "DEXTAUS": ("TWD", "ccy_per_usd"),
    "DEXDNUS": ("DKK", "ccy_per_usd"),
    "DEXSDUS": ("SEK", "ccy_per_usd"),
    "DEXNOUS": ("NOK", "ccy_per_usd"),
    "DEXBZUS": ("BRL", "ccy_per_usd"),
    "DEXMXUS": ("MXN", "ccy_per_usd"),
    "DEXINUS": ("INR", "ccy_per_usd"),
    "DEXSFUS": ("ZAR", "ccy_per_usd"),
    "DEXISUS": ("ILS", "ccy_per_usd"),
}


# ── Ticker universe ─────────────────────────────────────────────────────


def _sector_map_tickers() -> list[str]:
    """Return US-listed tickers from SECTOR_MAP in priority order.

    Priority = sector declaration order in SECTOR_MAP, then subsector
    declaration order, then actor order. Non-ticker actors and foreign
    unlisted actors (ticker == None) are skipped.
    """
    try:
        from analysis.sector_map import SECTOR_MAP
    except Exception as exc:
        log.error("Could not import SECTOR_MAP: {e}", e=str(exc))
        return []

    seen: set[str] = set()
    ordered: list[str] = []
    for _sector_name, sector in SECTOR_MAP.items():
        if not isinstance(sector, dict):
            continue
        for _sub_name, sub in (sector.get("subsectors") or {}).items():
            if not isinstance(sub, dict):
                continue
            for actor in sub.get("actors") or []:
                ticker = (actor.get("ticker") or "").strip().upper()
                if not ticker:
                    continue
                if ticker in seen:
                    continue
                seen.add(ticker)
                ordered.append(ticker)
    return ordered


# ── CIK lookup ──────────────────────────────────────────────────────────


def _fetch_ticker_to_cik_map() -> dict[str, str]:
    """Fetch and parse the SEC ticker→CIK mapping file.

    Returns:
        Dict mapping upper-case ticker symbol to 10-digit zero-padded CIK.
        Empty dict on failure.
    """
    try:
        resp = requests.get(
            _COMPANY_TICKERS_URL,
            headers=_SEC_TICKER_HEADERS,
            timeout=_REQUEST_TIMEOUT_SECS,
        )
        resp.raise_for_status()
        raw = resp.json()
    except requests.RequestException as exc:
        log.error("Failed to fetch SEC ticker map: {e}", e=str(exc))
        return {}
    except ValueError as exc:
        log.error("SEC ticker map JSON parse failed: {e}", e=str(exc))
        return {}

    out: dict[str, str] = {}
    # The SEC file is a dict of numeric-string keys → {cik_str, ticker, title}
    for _, entry in raw.items():
        if not isinstance(entry, dict):
            continue
        ticker = str(entry.get("ticker") or "").strip().upper()
        cik_raw = entry.get("cik_str")
        if not ticker or cik_raw is None:
            continue
        try:
            cik_padded = str(int(cik_raw)).zfill(10)
        except (TypeError, ValueError):
            continue
        # First occurrence wins (ticker map has some duplicates for
        # different share classes; the first is usually primary).
        out.setdefault(ticker, cik_padded)
    log.info("SEC ticker map loaded: {n} tickers", n=len(out))
    return out


# ── Company Facts fetch ─────────────────────────────────────────────────


def _fetch_company_facts(cik_padded: str) -> dict[str, Any] | None:
    """Fetch Company Facts JSON for a zero-padded CIK.

    Returns:
        Parsed JSON dict, or None on 404/403/parse failure (caller should
        log and continue).
    """
    url = _COMPANY_FACTS_URL.format(cik=cik_padded)
    try:
        resp = requests.get(
            url, headers=_SEC_HEADERS, timeout=_REQUEST_TIMEOUT_SECS
        )
    except requests.RequestException as exc:
        log.warning("SEC companyfacts CIK={c} network error: {e}",
                    c=cik_padded, e=str(exc))
        return None

    if resp.status_code in (403, 404):
        log.warning("SEC companyfacts CIK={c} HTTP {s}",
                    c=cik_padded, s=resp.status_code)
        return None
    if resp.status_code != 200:
        log.warning("SEC companyfacts CIK={c} HTTP {s}",
                    c=cik_padded, s=resp.status_code)
        return None

    try:
        return resp.json()
    except ValueError as exc:
        log.warning("SEC companyfacts CIK={c} JSON parse: {e}",
                    c=cik_padded, e=str(exc))
        return None


# ── FX conversion ───────────────────────────────────────────────────────


def _load_fx_rates(engine: Engine) -> dict[str, float]:
    """Return {currency: USD-per-1-ccy} from FRED DEX* series in raw_series.

    We take the latest non-zero value for each mapped series. Falls back
    silently to an empty dict if the DB is unreachable — callers then
    use ``_FX_FALLBACK_USD_PER_CCY``.
    """
    out: dict[str, float] = {"USD": 1.0}
    if engine is None:
        return out
    try:
        with engine.connect() as conn:
            for series_id, (ccy, orient) in _FRED_FX_SERIES.items():
                row = conn.execute(
                    text(
                        "SELECT value FROM raw_series "
                        "WHERE series_id = :sid AND value > 0 "
                        "ORDER BY obs_date DESC LIMIT 1"
                    ).bindparams(sid=series_id),
                ).fetchone()
                if not row:
                    continue
                try:
                    v = float(row[0])
                except (TypeError, ValueError):
                    continue
                if v <= 0:
                    continue
                if orient == "usd_per_ccy":
                    out[ccy] = v
                else:  # ccy_per_usd
                    out[ccy] = 1.0 / v
    except Exception as exc:
        log.warning("SEC_XBRL: FX rate load failed: {e}", e=str(exc))
    return out


def _convert_to_usd(
    amount: float,
    currency: str,
    fx_rates: dict[str, float],
) -> tuple[float, bool]:
    """Return (usd_amount, used_fallback).

    ``used_fallback`` is True when we fell back to the hardcoded table
    because ``fx_rates`` had no entry for this currency.
    """
    ccy_u = currency.upper()
    if ccy_u == "USD":
        return amount, False
    rate = fx_rates.get(ccy_u)
    if rate is not None and rate > 0:
        return amount * rate, False
    fallback = _FX_FALLBACK_USD_PER_CCY.get(ccy_u)
    if fallback is not None and fallback > 0:
        return amount * fallback, True
    # Unknown currency — keep the local value unconverted so downstream
    # consumers see *something* rather than nothing; flag as fallback
    # so confidence drops.
    return amount, True


# ── Fact extraction ─────────────────────────────────────────────────────


def _unit_entries_by_currency(
    facts: dict[str, Any], taxonomy: str, tag: str,
) -> dict[str, list[dict[str, Any]]]:
    """Return the ``units`` map for a (taxonomy, tag) pair.

    The XBRL Company Facts schema is
    ``facts[taxonomy][tag].units[{currency}]`` — e.g. ``units.USD``,
    ``units.EUR``, ``units.GBP``. We return the full dict so callers can
    pick the best currency key.
    """
    try:
        units = (
            facts.get("facts", {})
            .get(taxonomy, {})
            .get(tag, {})
            .get("units", {})
        )
    except AttributeError:
        return {}
    if not isinstance(units, dict):
        return {}
    # Only currency-like keys — skip "shares", "pure", "USD/shares" etc.
    out: dict[str, list[dict[str, Any]]] = {}
    for ukey, entries in units.items():
        if not isinstance(entries, list):
            continue
        u = str(ukey).strip().upper()
        # Accept 3-letter ISO codes only.
        if len(u) != 3 or not u.isalpha():
            continue
        out[u] = entries
    return out


def _pick_currency(units_by_ccy: dict[str, list[dict[str, Any]]]) -> str | None:
    """Pick the best currency key from a units dict.

    Priority: USD > preferred list > whatever's there alphabetically.
    """
    if not units_by_ccy:
        return None
    if "USD" in units_by_ccy:
        return "USD"
    for ccy in _PREFERRED_CURRENCIES:
        if ccy in units_by_ccy:
            return ccy
    # Deterministic fallback.
    return sorted(units_by_ccy.keys())[0]


def _extract_entries(
    facts: dict[str, Any], tag: str,
) -> tuple[list[dict[str, Any]], str, str] | tuple[None, None, None]:
    """Find entries for a tag across us-gaap then ifrs-full taxonomies.

    Returns ``(entries, taxonomy, currency)`` or ``(None, None, None)``
    if no currency-denominated entries exist under either taxonomy.
    Prefers us-gaap when both taxonomies have the same tag.
    """
    for taxonomy in ("us-gaap", "ifrs-full"):
        units_by_ccy = _unit_entries_by_currency(facts, taxonomy, tag)
        ccy = _pick_currency(units_by_ccy)
        if ccy is None:
            continue
        entries = units_by_ccy.get(ccy) or []
        if entries:
            return entries, taxonomy, ccy
    return None, None, None


def _pick_period_type(form: str) -> str | None:
    """Map SEC form string to period_type. Returns None for unusable forms."""
    if form in _ANNUAL_FORMS:
        return "annual"
    if form in _QUARTER_FORMS:
        return "quarter"
    return None


def _normalize_entry(
    entry: dict[str, Any],
) -> tuple[date, str, str] | None:
    """Return (fiscal_period, period_type, source_filing) for a USD fact.

    ``source_filing`` is ``"{form} {fp}"`` (e.g. "10-K FY", "10-Q Q3").
    Returns None if the entry is missing required fields or the form
    is not 10-K/10-Q.
    """
    form = str(entry.get("form") or "")
    period_type = _pick_period_type(form)
    if period_type is None:
        return None
    end_str = str(entry.get("end") or "")
    try:
        fp = date.fromisoformat(end_str)
    except ValueError:
        return None
    fp_label = str(entry.get("fp") or "")
    source_filing = f"{form} {fp_label}".strip()
    return fp, period_type, source_filing


def _iter_tagged_entries(
    facts: dict[str, Any],
    candidate_tags: list[str],
    taxonomy: str,
) -> list[tuple[int, str, str, dict[str, Any]]]:
    """Yield ``(tag_priority, tag, currency, entry)`` tuples for a tag list.

    For each candidate tag we call ``_extract_entries`` which picks the
    best currency key for THAT tag's ``units`` dict independently.
    Different tags on the same company may resolve to different
    currencies (e.g. IFRS tags → EUR for a eurozone issuer).
    """
    out: list[tuple[int, str, str, dict[str, Any]]] = []
    for tag_priority, tag in enumerate(candidate_tags):
        entries, resolved_taxonomy, currency = _extract_entries(facts, tag)
        if not entries or resolved_taxonomy != taxonomy:
            continue
        for entry in entries:
            out.append((tag_priority, tag, currency or "USD", entry))
    return out


def _collect_simple_flow(
    facts: dict[str, Any],
    flow_type: str,
    direction: str,
    usgaap_tags: list[str],
    ifrs_tags: list[str],
    fx_rates: dict[str, float],
) -> list[dict[str, Any]]:
    """Return capital_flows row dicts for a single-tag flow_type.

    Tries us-gaap first. Only falls back to ifrs-full if us-gaap
    produced zero entries (foreign private issuers file 20-F under
    ifrs-full and have no us-gaap facts at all). When a tag's units
    dict has multiple currencies, the preferred-currency picker in
    ``_extract_entries`` selects one; we then FX-convert the amount
    to USD via ``fx_rates``.

    Deduplicates by (fiscal_period, period_type, source_filing). When
    multiple tags cover the same period, priority goes to:
      1. Higher tag priority (later tags = newer taxonomy).
      2. Later ``filed`` date for the same tag.
    """
    # Try us-gaap first.
    tagged = _iter_tagged_entries(facts, usgaap_tags, "us-gaap")
    if not tagged:
        tagged = _iter_tagged_entries(facts, ifrs_tags, "ifrs-full")
    if not tagged:
        return []

    rows: dict[tuple[date, str, str], dict[str, Any]] = {}
    for tag_priority, tag, currency, entry in tagged:
        normalized = _normalize_entry(entry)
        if normalized is None:
            continue
        fp, period_type, source_filing = normalized
        val = entry.get("val")
        if val is None:
            continue
        try:
            amount_local = abs(float(val))
        except (TypeError, ValueError):
            continue
        amount_usd, used_fallback = _convert_to_usd(
            amount_local, currency, fx_rates
        )
        confidence = "estimated" if used_fallback else "confirmed"
        filed = str(entry.get("filed") or "")
        key = (fp, period_type, source_filing)
        existing = rows.get(key)
        if existing is None:
            rows[key] = {
                "fiscal_period": fp,
                "period_type": period_type,
                "flow_type": flow_type,
                "direction": direction,
                "amount_usd": amount_usd,
                "amount_local": amount_local,
                "currency": currency,
                "confidence": confidence,
                "source_filing": source_filing,
                "xbrl_tag": tag,
                "_filed": filed,
                "_tag_priority": tag_priority,
            }
            continue
        existing_priority = int(existing.get("_tag_priority", -1))
        if tag_priority > existing_priority or (
            tag_priority == existing_priority
            and filed > existing.get("_filed", "")
        ):
            rows[key] = {
                "fiscal_period": fp,
                "period_type": period_type,
                "flow_type": flow_type,
                "direction": direction,
                "amount_usd": amount_usd,
                "amount_local": amount_local,
                "currency": currency,
                "confidence": confidence,
                "source_filing": source_filing,
                "xbrl_tag": tag,
                "_filed": filed,
                "_tag_priority": tag_priority,
            }
    for r in rows.values():
        r.pop("_filed", None)
        r.pop("_tag_priority", None)
    return list(rows.values())


def _collect_net_debt_issuance_one(
    facts: dict[str, Any],
    proceeds_tag: str,
    repay_tag: str,
    fx_rates: dict[str, float],
) -> list[dict[str, Any]]:
    """Net debt issuance for ONE taxonomy's tag pair (us-gaap or IFRS)."""
    p_entries, _p_tax, p_ccy = _extract_entries(facts, proceeds_tag)
    r_entries, _r_tax, r_ccy = _extract_entries(facts, repay_tag)
    if not p_entries:
        return []
    p_currency = p_ccy or "USD"
    r_currency = r_ccy or p_currency

    repay_idx: dict[tuple[date, str, str], float] = {}
    if r_entries:
        for entry in r_entries:
            normalized = _normalize_entry(entry)
            if normalized is None:
                continue
            val = entry.get("val")
            if val is None:
                continue
            try:
                repay_idx[normalized] = abs(float(val))
            except (TypeError, ValueError):
                continue

    rows: dict[tuple[date, str, str], dict[str, Any]] = {}
    for entry in p_entries:
        normalized = _normalize_entry(entry)
        if normalized is None:
            continue
        val = entry.get("val")
        if val is None:
            continue
        try:
            p_amount = abs(float(val))
        except (TypeError, ValueError):
            continue
        r_amount = repay_idx.get(normalized, 0.0)
        # Both sides assumed to be in the proceeds currency — SEC
        # companyfacts almost always reports proceeds/repayments in the
        # same unit. If currencies do mismatch, keep the proceeds-side
        # unit and accept the small error (flagged below).
        net_local = p_amount - r_amount
        if net_local <= 0:
            continue
        net_usd, used_fallback = _convert_to_usd(
            net_local, p_currency, fx_rates
        )
        currency_mismatch = (r_currency != p_currency)
        confidence = (
            "estimated" if (used_fallback or currency_mismatch) else "confirmed"
        )
        fp, period_type, source_filing = normalized
        filed = str(entry.get("filed") or "")
        key = (fp, period_type, source_filing)
        existing = rows.get(key)
        if existing is None or filed > existing.get("_filed", ""):
            rows[key] = {
                "fiscal_period": fp,
                "period_type": period_type,
                "flow_type": "debt_issuance",
                "direction": "in",
                "amount_usd": net_usd,
                "amount_local": net_local,
                "currency": p_currency,
                "confidence": confidence,
                "source_filing": source_filing,
                "xbrl_tag": f"{proceeds_tag}-{repay_tag}",
                "_filed": filed,
            }
    for r in rows.values():
        r.pop("_filed", None)
    return list(rows.values())


def _collect_net_debt_issuance(
    facts: dict[str, Any],
    fx_rates: dict[str, float],
) -> list[dict[str, Any]]:
    """Net debt issuance, trying us-gaap then ifrs-full."""
    out = _collect_net_debt_issuance_one(
        facts, _DEBT_PROCEEDS_TAG, _DEBT_REPAY_TAG, fx_rates,
    )
    if out:
        return out
    return _collect_net_debt_issuance_one(
        facts, _IFRS_DEBT_PROCEEDS_TAG, _IFRS_DEBT_REPAY_TAG, fx_rates,
    )


def _collect_all_flows(
    facts: dict[str, Any],
    fx_rates: dict[str, float],
) -> list[dict[str, Any]]:
    """Return all flow-row dicts for a single company's facts payload."""
    out: list[dict[str, Any]] = []
    for spec in _FLOW_MAPPINGS:
        ifrs_tags = _IFRS_TAG_MAP.get(spec["flow_type"], [])
        out.extend(
            _collect_simple_flow(
                facts,
                flow_type=spec["flow_type"],
                direction=spec["direction"],
                usgaap_tags=spec["tags"],
                ifrs_tags=ifrs_tags,
                fx_rates=fx_rates,
            )
        )
    out.extend(_collect_net_debt_issuance(facts, fx_rates))
    return out


# ── DB writer ───────────────────────────────────────────────────────────


# Deletes the exact (actor, fp, period_type, flow_type, source_filing)
# row with NULL counterparty *before* insert, so re-runs never create
# duplicates even though PG treats NULL as distinct in UNIQUE keys.
# Migration 0024 ALSO rebuilds the UNIQUE key with NULLS NOT DISTINCT,
# which makes this delete strictly a belt-and-braces measure — but it's
# cheap and keeps the writer correct on older DBs that haven't applied
# 0024 yet.
_DELETE_BEFORE_INSERT_SQL = text(
    """
    DELETE FROM capital_flows
    WHERE actor_id = :actor_id
      AND fiscal_period = :fiscal_period
      AND period_type = :period_type
      AND flow_type = :flow_type
      AND counterparty_id IS NULL
      AND source_filing = :source_filing
    """
)

# Plain INSERT with no ON CONFLICT — the writer runs a DELETE first
# which guarantees there's no existing row to collide with. We
# deliberately skip ON CONFLICT because the original migration 0021
# unique constraint has ``counterparty_id`` in its key list and PG<15
# treats NULLs as DISTINCT, which means ON CONFLICT can never match a
# NULL counterparty row and silently inserts duplicates. Migration
# 0024 adds a functional unique index on
# ``COALESCE(NULLIF(counterparty_id, ''), '__none__')`` that does
# enforce uniqueness — but pointing ``ON CONFLICT`` at an expression-
# based index requires repeating the expression in the target clause
# (PG14 doesn't let you name the index), which is brittle. DELETE then
# INSERT is simpler and equally correct.
_INSERT_SQL = text(
    """
    INSERT INTO capital_flows (
        actor_id, fiscal_period, period_type, flow_type, direction,
        amount_usd, counterparty_id, source_filing, confidence,
        currency, as_of
    ) VALUES (
        :actor_id, :fiscal_period, :period_type, :flow_type, :direction,
        :amount_usd, NULL, :source_filing, :confidence,
        :currency, NOW()
    )
    """
)

# Pre-0024 DBs don't have the ``currency`` column. Keep a legacy
# insert statement available so the puller still works against a
# schema that hasn't had migration 0024 applied yet.
_LEGACY_INSERT_SQL = text(
    """
    INSERT INTO capital_flows (
        actor_id, fiscal_period, period_type, flow_type, direction,
        amount_usd, counterparty_id, source_filing, confidence, as_of
    ) VALUES (
        :actor_id, :fiscal_period, :period_type, :flow_type, :direction,
        :amount_usd, NULL, :source_filing, :confidence, NOW()
    )
    """
)


def _write_rows(
    engine: Engine,
    actor_id: str,
    rows: list[dict[str, Any]],
) -> int:
    """Upsert rows into capital_flows. Returns number written.

    Strategy: DELETE any existing (actor_id, fp, period_type, flow_type,
    source_filing) row with NULL counterparty, then INSERT. This is
    necessary because the original migration 0021 unique index treats
    NULLs as DISTINCT, so ON CONFLICT can't match NULL counterparties.
    Migration 0024 rebuilds the index with NULLS NOT DISTINCT; once
    that's applied we still run the DELETE (cheap) but ON CONFLICT also
    kicks in as a safety net.
    """
    if not rows:
        return 0
    written = 0
    with engine.begin() as conn:
        for r in rows:
            params = {
                "actor_id": actor_id,
                "fiscal_period": r["fiscal_period"],
                "period_type": r["period_type"],
                "flow_type": r["flow_type"],
                "direction": r["direction"],
                "amount_usd": r["amount_usd"],
                "source_filing": r["source_filing"],
                "confidence": r.get("confidence") or "confirmed",
                "currency": r.get("currency") or "USD",
            }
            try:
                conn.execute(_DELETE_BEFORE_INSERT_SQL, {
                    "actor_id": params["actor_id"],
                    "fiscal_period": params["fiscal_period"],
                    "period_type": params["period_type"],
                    "flow_type": params["flow_type"],
                    "source_filing": params["source_filing"],
                })
                try:
                    conn.execute(_INSERT_SQL, params)
                except Exception:
                    # Column ``currency`` missing → pre-0024 schema.
                    # Drop it and retry with the legacy insert.
                    legacy_params = {
                        k: v for k, v in params.items() if k != "currency"
                    }
                    conn.execute(_LEGACY_INSERT_SQL, legacy_params)
                written += 1
            except Exception as exc:
                log.warning(
                    "capital_flows upsert failed for {a} {f} {ft}: {e}",
                    a=actor_id,
                    f=r.get("fiscal_period"),
                    ft=r.get("flow_type"),
                    e=str(exc),
                )
    return written


# ── Checkpoint ──────────────────────────────────────────────────────────


def _load_checkpoint() -> dict[str, Any]:
    """Load /tmp checkpoint. Returns empty dict if missing/corrupt."""
    if not _CHECKPOINT_PATH.exists():
        return {}
    try:
        return json.loads(_CHECKPOINT_PATH.read_text())
    except (OSError, ValueError) as exc:
        log.warning("checkpoint load failed: {e}", e=str(exc))
        return {}


def _save_checkpoint(
    completed: list[str], last_ticker: str, total_rows: int,
) -> None:
    """Atomically write checkpoint to /tmp."""
    payload = {
        "last_ticker": last_ticker,
        "completed": completed,
        "total_rows": total_rows,
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        tmp_path = _CHECKPOINT_PATH.with_suffix(".tmp")
        tmp_path.write_text(json.dumps(payload))
        os.replace(tmp_path, _CHECKPOINT_PATH)
    except OSError as exc:
        log.warning("checkpoint save failed: {e}", e=str(exc))


# ── Main runner ─────────────────────────────────────────────────────────


class SECXBRLFinancialsPuller:
    """Pulls SEC XBRL Company Facts and writes capital_flows rows.

    Attributes:
        engine: SQLAlchemy engine connected to griddb.
    """

    SOURCE_NAME: str = "SEC_XBRL_FINANCIALS"

    def __init__(self, db_engine: Engine) -> None:
        """Initialise the puller.

        Parameters:
            db_engine: SQLAlchemy engine connected to the GRID database.
        """
        self.engine = db_engine

    def pull_all(
        self,
        limit: int | None = None,
        resume: bool = True,
        tickers: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Pull XBRL Company Facts for SECTOR_MAP tickers.

        Parameters:
            limit: Maximum tickers to process this invocation (None = all).
            resume: If True, skip tickers already recorded in the
                checkpoint. Set False to force full re-run.
            tickers: If provided, restrict the universe to these tickers
                only (case-insensitive). Useful for targeted re-runs of
                foreign-issuer IFRS filers.

        Returns:
            List of per-ticker result dicts.
        """
        start_ts = time.monotonic()

        universe = _sector_map_tickers()
        if not universe:
            log.error("SEC_XBRL: empty universe — aborting")
            return [{"status": "FAILED", "error": "empty universe"}]

        if tickers:
            allowed = {t.strip().upper() for t in tickers if t.strip()}
            universe_set = set(universe)
            # Make sure any explicit tickers not in SECTOR_MAP still get
            # pulled (e.g. NSRGY, HEINY — ADRs that may not be in the
            # canonical sector map).
            base = [t for t in universe if t in allowed]
            extras = sorted(allowed - universe_set)
            universe = base + extras

        checkpoint = _load_checkpoint() if resume else {}
        already_done: set[str] = set(checkpoint.get("completed") or [])
        total_rows_before: int = int(checkpoint.get("total_rows") or 0)

        # Pre-load FX rates once so every ticker's facts can convert
        # non-USD amounts to USD without a DB hit per row.
        fx_rates = _load_fx_rates(self.engine)
        log.info(
            "SEC_XBRL: FX rates loaded for {n} currencies ({c})",
            n=len(fx_rates),
            c=",".join(sorted(fx_rates.keys())),
        )

        cik_map = _fetch_ticker_to_cik_map()
        if not cik_map:
            log.error("SEC_XBRL: CIK map empty — aborting")
            return [{"status": "FAILED", "error": "cik map empty"}]

        # Small courtesy delay after the first SEC call.
        time.sleep(_RATE_LIMIT_DELAY_SECS)

        results: list[dict[str, Any]] = []
        processed = 0
        rows_this_run = 0
        completed_list: list[str] = list(already_done)

        for ticker in universe:
            if limit is not None and processed >= limit:
                log.info("SEC_XBRL: limit {n} reached — stopping", n=limit)
                break
            if time.monotonic() - start_ts > _MAX_RUNTIME_SECS:
                log.warning("SEC_XBRL: runtime cap reached — stopping")
                break

            if ticker in already_done:
                continue

            actor_id = ticker.lower()
            cik_padded = cik_map.get(ticker)
            if not cik_padded:
                log.info("SEC_XBRL: {t} has no CIK — skip", t=ticker)
                results.append({
                    "ticker": ticker, "status": "NO_CIK", "rows": 0,
                })
                completed_list.append(ticker)
                processed += 1
                _save_checkpoint(
                    completed_list, ticker, total_rows_before + rows_this_run,
                )
                continue

            facts = _fetch_company_facts(cik_padded)
            time.sleep(_RATE_LIMIT_DELAY_SECS)
            if facts is None:
                results.append({
                    "ticker": ticker, "status": "NO_FACTS", "rows": 0,
                })
                completed_list.append(ticker)
                processed += 1
                _save_checkpoint(
                    completed_list, ticker, total_rows_before + rows_this_run,
                )
                continue

            flow_rows = _collect_all_flows(facts, fx_rates)
            written = _write_rows(self.engine, actor_id, flow_rows)
            rows_this_run += written

            log.info(
                "SEC_XBRL: {t} CIK={c} → {w} rows ({f} flow records)",
                t=ticker, c=cik_padded, w=written, f=len(flow_rows),
            )
            results.append({
                "ticker": ticker,
                "cik": cik_padded,
                "status": "SUCCESS" if written > 0 else "NO_FLOWS",
                "rows": written,
                "candidates": len(flow_rows),
            })
            completed_list.append(ticker)
            processed += 1
            _save_checkpoint(
                completed_list, ticker, total_rows_before + rows_this_run,
            )

        elapsed = time.monotonic() - start_ts
        log.info(
            "SEC_XBRL complete — {p} tickers, {r} rows, {e:.1f}s",
            p=processed, r=rows_this_run, e=elapsed,
        )
        results.append({
            "status": "SUMMARY",
            "tickers_processed": processed,
            "rows_written": rows_this_run,
            "elapsed_seconds": round(elapsed, 1),
        })
        return results


if __name__ == "__main__":
    from db import get_engine

    puller = SECXBRLFinancialsPuller(db_engine=get_engine())
    out = puller.pull_all(limit=5)
    for r in out:
        print(r)
