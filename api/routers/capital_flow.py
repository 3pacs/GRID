"""Capital-flow endpoint for actor profile pages.

Serves per-actor, multi-period inflows / outflows from ``capital_flows``
(XBRL / 10-K ingestion). If the table is missing or empty, returns a
graceful fallback with ``provenance.source = "fallback"`` so the
frontend can still render the shell.

## Dedup policy

The unique key in ``capital_flows`` includes ``source_filing``, which
means that SEC XBRL loads (``source_filing LIKE '10-%'``) coexist with
seed loads (``source_filing = 'seed'`` etc.) for the same
(actor, fiscal_period, period_type, flow_type, counterparty_id).
Summing across them double-counts (seed used total revenue, SEC uses
net sales; WMT and JPM ended up with negative CAGR because of this).

``_load_rows`` now picks ONE row per natural key using a priority
order:
  1. SEC 10-* filings first
  2. Then confidence rank (confirmed > derived > estimated > …)
  3. Then most recent ``as_of``
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from fastapi import APIRouter, Depends, Query
from loguru import logger as log
from sqlalchemy import text

from api.auth import require_auth
from api.dependencies import get_db_engine
from utils.fx import convert_to_usd
from utils.ttl_cache import TTLCache

router = APIRouter(prefix="/api/v1/actors", tags=["capital_flow"])

_CACHE: TTLCache = TTLCache(ttl=600.0, max_size=256)

# Canonical display ordering. Unknown flow types are appended after.
_INFLOW_TYPES: tuple[str, ...] = ("revenue", "debt_issuance", "equity_issuance")
_OUTFLOW_TYPES: tuple[str, ...] = (
    "cogs", "opex", "r_and_d", "capex", "interest_paid", "tax",
    "dividends", "buybacks", "acquisitions", "working_capital_delta",
    "fcf_to_equity",
)
_VALID_PERIOD_TYPES: frozenset[str] = frozenset({"annual", "quarter", "ttm"})


# ── Helpers ──────────────────────────────────────────────────────────


def _table_exists(conn: Any, name: str) -> bool:
    try:
        row = conn.execute(text("SELECT to_regclass(:n)").bindparams(n=name)).fetchone()
        return bool(row and row[0])
    except Exception:
        return False


def _safe_div(num: float | None, den: float | None) -> float | None:
    if num is None or den is None:
        return None
    try:
        d = float(den)
        return None if d == 0.0 else float(num) / d
    except (TypeError, ValueError):
        return None


def _round5(v: float | None) -> float | None:
    if v is None:
        return None
    try:
        return round(float(v), 5)
    except (TypeError, ValueError):
        return None


def _period_label(fp: Any, period_type: str) -> str:
    if fp is None:
        return "unknown"
    try:
        year, month = fp.year, fp.month
    except AttributeError:
        return str(fp)
    if period_type == "annual":
        return f"FY{year}"
    q = max(1, min(4, (month - 1) // 3 + 1))
    if period_type == "quarter":
        return f"Q{q}-{year}"
    if period_type == "ttm":
        return f"TTM-{year}Q{q}"
    return str(fp)


_CANVAS_NODE_PREFIXES = ("a:corp_", "a:ticker_", "a:person_", "a:govt_", "a:org_", "a:fund_", "a:")


def _strip_canvas_prefix(actor_id: str) -> str:
    """Strip canvas graph node-id prefixes so 'a:corp_KO' → 'KO'."""
    s = (actor_id or "").strip()
    for pfx in _CANVAS_NODE_PREFIXES:
        if s.startswith(pfx):
            return s[len(pfx):]
    return s


def _looks_like_ticker(actor_id: str) -> bool:
    """True if ``actor_id`` looks like an equity ticker (short, all alpha).

    Used to decide whether an unknown id should be upper-cased for
    display. Names like ``"larry-fink"`` or ``"berkshire_hathaway"``
    fail this check and are left as-is.
    """
    s = (actor_id or "").strip()
    if not s or len(s) > 6:
        return False
    return s.isalpha()


def _display_label(actor_id: str) -> str:
    """Return a sensible display label for an actor_id with no metadata.

    Tickers like ``"hsy"`` get upper-cased to ``"HSY"``. Free-form
    actor ids are returned unchanged so we don't mangle ``"larry-fink"``
    into ``"LARRY-FINK"``.
    """
    aid = (actor_id or "").strip()
    if _looks_like_ticker(aid):
        return aid.upper()
    return aid


def _lookup_actor(actor_id: str) -> dict[str, Any]:
    """Resolve actor_id → display metadata. Non-tickers get None sectors.

    Lookup is case-insensitive. When sector_map has no entry, the
    fallback id is preserved (lower-case for DB joins) but the display
    label is upper-cased if the id looks like an equity ticker so the
    UI doesn't render "hsy posted ..." for HSY etc.
    """
    fallback_label = _display_label(actor_id)
    out: dict[str, Any] = {
        "id": actor_id,
        "label": fallback_label,
        "type": "unknown",
        "sector": None,
        "subsector": None,
    }
    try:
        from analysis.sector_map import SECTOR_MAP
    except Exception:
        return out

    aid_u = actor_id.strip().upper()
    for sector_name, sector in SECTOR_MAP.items():
        if not isinstance(sector, dict):
            continue
        for sub_name, sub in (sector.get("subsectors") or {}).items():
            if not isinstance(sub, dict):
                continue
            for actor in sub.get("actors") or []:
                tk = (actor.get("ticker") or "").upper()
                if tk and tk == aid_u:
                    return {
                        "id": tk,
                        "label": actor.get("name") or tk,
                        "type": actor.get("type") or "ticker",
                        "sector": sector_name,
                        "subsector": sub_name,
                    }
    return out


# ── Market cap lookup ────────────────────────────────────────────────


def _fetch_market_cap(
    engine: Any,
    actor_id: str,
    fiscal_period: date | None,
) -> float | None:
    """Return the closest-to-period market_cap_usd for ``actor_id``.

    Falls back to the single most recent row if no row exists on or
    before ``fiscal_period``. Returns None if the table is missing or
    the ticker has no data. ``actor_id`` is case-insensitive; we probe
    UPPER(actor_id) since ticker_metrics_daily stores tickers upper-cased.
    """
    ticker = (actor_id or "").strip().upper()
    if not ticker:
        return None
    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "public.ticker_metrics_daily"):
                return None
            if fiscal_period is not None:
                row = conn.execute(
                    text(
                        "SELECT market_cap_usd FROM ticker_metrics_daily "
                        "WHERE ticker = :t AND obs_date <= :d "
                        "AND market_cap_usd IS NOT NULL "
                        "ORDER BY obs_date DESC LIMIT 1"
                    ).bindparams(t=ticker, d=fiscal_period),
                ).fetchone()
                if row and row[0] is not None:
                    return float(row[0])
            row = conn.execute(
                text(
                    "SELECT market_cap_usd FROM ticker_metrics_daily "
                    "WHERE ticker = :t AND market_cap_usd IS NOT NULL "
                    "ORDER BY obs_date DESC LIMIT 1"
                ).bindparams(t=ticker),
            ).fetchone()
            if row and row[0] is not None:
                return float(row[0])
    except Exception as exc:
        log.debug(
            "capital_flow: market_cap lookup failed for {a}: {e}",
            a=actor_id, e=str(exc),
        )
    return None


# ── Data loading ─────────────────────────────────────────────────────


# Dedup rank SQL — pulled out so it can be unit-tested / inspected.
# ORDER BY:
#   1. SEC filings first (source_filing LIKE '10-%')
#   2. Confidence rank
#   3. Most recent as_of tie-break
# Dedup is keyed on NULLIF(counterparty_id,'') because the DB has a mix of
# NULL and '' for the same logical "no counterparty" case (seed loader vs
# SEC loader disagree). Without NULLIF both rows survive and get SUMmed,
# which is exactly the WMT / JPM negative-CAGR bug.
_DEDUP_SQL = """
WITH ranked AS (
    SELECT
        fiscal_period,
        flow_type,
        direction,
        amount_usd,
        currency,
        counterparty_id,
        source_filing,
        confidence,
        ROW_NUMBER() OVER (
            PARTITION BY actor_id, fiscal_period, period_type,
                         flow_type, direction,
                         COALESCE(NULLIF(counterparty_id, ''), '__none__')
            ORDER BY
                -- Prefer SEC filings, then other regulatory, then seed/other.
                CASE
                    WHEN source_filing LIKE '10-%' THEN 1
                    WHEN source_filing LIKE '20-%' THEN 2
                    WHEN source_filing LIKE '8-%'  THEN 3
                    WHEN source_filing LIKE 'seed%' THEN 5
                    ELSE 4
                END,
                CASE confidence
                    WHEN 'confirmed' THEN 1
                    WHEN 'derived'   THEN 2
                    WHEN 'estimated' THEN 3
                    WHEN 'rumored'   THEN 4
                    WHEN 'inferred'  THEN 5
                    ELSE 6
                END,
                as_of DESC NULLS LAST
        ) AS rk
    FROM capital_flows
    WHERE actor_id = :aid
      AND period_type = :pt
      AND fiscal_period = ANY(:fps)
)
SELECT fiscal_period, flow_type, direction, amount_usd, currency,
       counterparty_id, source_filing, confidence
FROM ranked
WHERE rk = 1
"""


_DEAL_ANNOUNCEMENT_SQL = """
SELECT
    fiscal_period,
    flow_type,
    direction,
    amount_usd,
    currency,
    counterparty_id,
    source_filing,
    confidence,
    as_of
FROM capital_flows
WHERE actor_id = :aid
  AND period_type = 'announcement'
  AND flow_type = :ft
  AND direction = 'out'
  AND amount_usd IS NOT NULL
  AND fiscal_period IS NOT NULL
  AND EXTRACT(YEAR FROM fiscal_period)::int = ANY(:years)
ORDER BY fiscal_period DESC, amount_usd DESC NULLS LAST
"""


def _load_deal_announcements(
    engine: Any,
    actor_id: str,
    flow_type: str,
    years: list[int],
) -> dict[int, list[dict[str, Any]]]:
    """Return announcement-level rows for ``flow_type`` grouped by fiscal year.

    Used to decompose aggregated annual outflows (primarily
    ``acquisitions``) back into the specific 8-K event rows that
    comprised them. Returns an empty dict when no matching rows exist
    or the table is missing.
    """
    if not years:
        return {}
    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "public.capital_flows"):
                return {}
            rows = conn.execute(
                text(_DEAL_ANNOUNCEMENT_SQL).bindparams(
                    aid=actor_id, ft=flow_type, years=years,
                ),
            ).fetchall()
    except Exception as exc:
        log.debug(
            "capital_flow: load_deal_announcements failed for {a}/{ft}: {e}",
            a=actor_id, ft=flow_type, e=str(exc),
        )
        return {}

    out: dict[int, list[dict[str, Any]]] = {}
    for r in rows:
        fp = r[0]
        if fp is None:
            continue
        try:
            year = int(fp.year)
        except AttributeError:
            continue
        amt = r[3]
        try:
            amt_f = float(amt) if amt is not None else None
        except (TypeError, ValueError):
            amt_f = None
        cp_raw = r[5]
        cp = cp_raw if cp_raw not in (None, "") else None
        deal = {
            "target": cp,
            "target_label": _display_label(cp) if cp else None,
            "amount_usd": amt_f,
            "currency": (r[4] or "USD").upper() if r[4] else "USD",
            "announcement_date": fp.isoformat() if hasattr(fp, "isoformat") else str(fp),
            "source_filing": r[6],
            "confidence": r[7],
        }
        out.setdefault(year, []).append(deal)

    # Sort each year's deals by amount desc so the largest deals
    # render first in the frontend popover.
    for year, deals in out.items():
        deals.sort(
            key=lambda d: (d.get("amount_usd") or 0.0),
            reverse=True,
        )
    return out


def _load_rows(engine: Any, actor_id: str, period_type: str, n: int) -> list[dict[str, Any]]:
    """Fetch capital_flows rows for the N most recent fiscal periods.

    Applies the dedup policy so each (fiscal_period, flow_type, direction,
    counterparty_id) appears at most once — preferring SEC over seed.
    """
    try:
        with engine.connect() as conn:
            if not _table_exists(conn, "public.capital_flows"):
                return []
            recent = conn.execute(
                text(
                    "SELECT DISTINCT fiscal_period FROM capital_flows "
                    "WHERE actor_id = :aid AND period_type = :pt "
                    "AND fiscal_period IS NOT NULL "
                    "ORDER BY fiscal_period DESC LIMIT :lim"
                ).bindparams(aid=actor_id, pt=period_type, lim=n),
            ).fetchall()
            if not recent:
                return []
            fps = [r[0] for r in recent]
            rows = conn.execute(
                text(_DEDUP_SQL).bindparams(aid=actor_id, pt=period_type, fps=fps),
            ).fetchall()
            return [
                {
                    "fiscal_period": r[0],
                    "flow_type": r[1],
                    "direction": r[2],
                    "amount_usd": float(r[3]) if r[3] is not None else None,
                    "currency": (r[4] or "USD").upper() if r[4] else "USD",
                    "counterparty_id": r[5],
                    "source_filing": r[6],
                    "confidence": r[7],
                }
                for r in rows
            ]
    except Exception as exc:
        log.debug("capital_flow: load_rows failed for {a}: {e}", a=actor_id, e=str(exc))
        return []


# ── Period aggregation ───────────────────────────────────────────────


def _sort_flows(flows: list[dict[str, Any]], order: tuple[str, ...]) -> list[dict[str, Any]]:
    rank = {ft: i for i, ft in enumerate(order)}
    return sorted(flows, key=lambda f: (rank.get(f.get("flow_type") or "", len(rank)), f.get("flow_type") or ""))


def _build_period(
    fp: Any,
    rows: list[dict[str, Any]],
    period_type: str,
    market_cap: float | None = None,
    engine: Any = None,
    fx_counter: list[int] | None = None,
    deals_for_period: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """Aggregate rows for one fiscal_period into the response shape.

    Rows entering here have already been deduped by ``_load_rows`` so each
    (direction, flow_type, counterparty_id) appears once. We still sum over
    counterparties within a flow_type to get per-flow totals.

    FX conversion: when a row carries a non-USD ``currency``, the amount
    is converted to USD at the fiscal_period spot rate via ``utils.fx``.
    The pre-conversion value is preserved as ``amount_local`` on the
    output flow entry so the frontend can show provenance. ``fx_counter``
    is a mutable one-element list used to tally successful conversions
    across all periods in a single response.
    """
    agg: dict[tuple[str, str, Any], dict[str, Any]] = {}
    for r in rows:
        raw_amt = r.get("amount_usd")
        if raw_amt is None:
            continue
        direction = (r.get("direction") or "").lower()
        if direction not in ("in", "out"):
            continue
        ft = r.get("flow_type") or "unknown"
        # Normalize empty-string counterparty → None so '' and NULL fold together.
        cp_raw = r.get("counterparty_id")
        cp = cp_raw if cp_raw not in (None, "") else None

        # FX normalization: XBRL loaders may store non-USD filings in
        # the local reporting currency (IFRS issuers). Convert to true
        # USD here so cross-actor comparisons are meaningful.
        currency = (r.get("currency") or "USD").upper()
        amount_local: float | None = None
        try:
            amt = float(raw_amt)
        except (TypeError, ValueError):
            continue
        if currency != "USD" and engine is not None and fp is not None:
            converted = convert_to_usd(engine, amt, currency, fp)
            if converted is not None:
                amount_local = amt
                amt = converted
                if fx_counter is not None:
                    fx_counter[0] += 1
            else:
                # Conversion failed — keep original numeric value so we
                # don't zero-out the row, but flag it so the response
                # carries explicit provenance.
                amount_local = amt

        key = (direction, ft, cp)
        if key not in agg:
            agg[key] = {
                "direction": direction, "flow_type": ft, "amount_usd": 0.0,
                "amount_local": amount_local,
                "currency": currency if currency != "USD" else None,
                "counterparty_id": cp,
                "source_filing": r.get("source_filing"),
                "confidence": r.get("confidence"),
            }
        agg[key]["amount_usd"] += amt
        # Keep amount_local only if every contributing row was non-USD;
        # mixing USD + non-USD rows into one flow_type makes the local
        # total ambiguous, so we null it in that case.
        if amount_local is None:
            agg[key]["amount_local"] = None
            agg[key]["currency"] = None

    inflows_raw = [e for e in agg.values() if e["direction"] == "in"]
    outflows_raw = [e for e in agg.values() if e["direction"] == "out"]
    inflow_total = sum(e["amount_usd"] for e in inflows_raw)
    outflow_total = sum(e["amount_usd"] for e in outflows_raw)

    def _finalize(
        entries: list[dict[str, Any]],
        total: float,
        *,
        attach_deals: bool = False,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        for e in entries:
            entry = {
                "flow_type": e["flow_type"],
                "amount_usd": e["amount_usd"],
                "amount_local": e.get("amount_local"),
                "currency": e.get("currency"),
                "counterparty_id": e.get("counterparty_id"),
                "share": _round5(_safe_div(e["amount_usd"], total)),
                "confidence": e.get("confidence"),
                "source_filing": e.get("source_filing"),
            }
            if attach_deals and deals_for_period is not None:
                ft = e.get("flow_type") or ""
                deals = deals_for_period.get(ft)
                if deals:
                    entry["deals"] = deals
            out.append(entry)
        return out

    # Decomposition into specific 8-K deal announcements only makes sense for
    # the ANNUAL rollup view. Quarter/TTM responses don't fold announcements
    # so there's nothing to decompose.
    attach = period_type == "annual"
    inflows = _sort_flows(_finalize(inflows_raw, inflow_total), _INFLOW_TYPES)
    outflows = _sort_flows(
        _finalize(outflows_raw, outflow_total, attach_deals=attach),
        _OUTFLOW_TYPES,
    )

    # Per-flow-type totals for ratio math (direction-agnostic; each flow
    # type is unambiguously one direction in the contract).
    amounts: dict[str, float] = {}
    for e in inflows_raw + outflows_raw:
        amounts[e["flow_type"]] = amounts.get(e["flow_type"], 0.0) + e["amount_usd"]

    ratios = _compute_ratios(amounts, market_cap=market_cap)

    return {
        "fiscal_period": str(fp) if fp is not None else None,
        "market_cap_usd": market_cap,
        "label": _period_label(fp, period_type),
        "inflows": inflows,
        "outflows": outflows,
        "totals": {
            "inflow_usd": round(inflow_total, 2),
            "outflow_usd": round(outflow_total, 2),
            "net_usd": round(inflow_total - outflow_total, 2),
        },
        "ratios": ratios,
        "_amounts": amounts,  # internal; stripped before return
    }


def _compute_ratios(
    amounts: dict[str, float],
    market_cap: float | None = None,
) -> dict[str, Any]:
    """Derived ratios for a single period. All nullable, all safe-divided."""
    rev = amounts.get("revenue")
    cogs = amounts.get("cogs")
    opex = amounts.get("opex")
    rnd = amounts.get("r_and_d")
    capex = amounts.get("capex")
    tax = amounts.get("tax")
    div = amounts.get("dividends")
    buy = amounts.get("buybacks")
    debt_iss = amounts.get("debt_issuance")
    acq = amounts.get("acquisitions")

    gross_margin = (
        _safe_div(rev - cogs, rev) if rev is not None and cogs is not None else None
    )

    dividend_payout: float | None = None
    if div is not None and rev is not None and cogs is not None:
        ni_proxy = rev - cogs - (opex or 0.0) - (tax or 0.0)
        dividend_payout = _safe_div(div, ni_proxy)

    fcf_conv: float | None = None
    if rev is not None and cogs is not None:
        fcf_conv = _safe_div(rev - cogs - (opex or 0.0) - (capex or 0.0), rev)

    # New efficiency ratios
    rev_per_capex: float | None = None
    if rev is not None and capex is not None:
        rev_per_capex = _safe_div(rev, capex)

    shareholder_yield: float | None = None
    if rev is not None and (div is not None or buy is not None):
        shareholder_yield = _safe_div((div or 0.0) + (buy or 0.0), rev)

    reinvestment_ratio: float | None = None
    if rev is not None and (capex is not None or rnd is not None):
        reinvestment_ratio = _safe_div((capex or 0.0) + (rnd or 0.0), rev)

    # Canonical buyback_yield is buybacks / market_cap. When market_cap
    # is missing (older period, no XBRL shares, non-ticker) fall back to
    # buybacks / revenue so the field is never silently null just because
    # the shares puller hasn't landed yet.
    buyback_yield: float | None = None
    if buy is not None and market_cap is not None and market_cap > 0:
        buyback_yield = _safe_div(buy, market_cap)
    elif buy is not None and rev is not None:
        buyback_yield = _safe_div(buy, rev)

    return {
        "gross_margin": _round5(gross_margin),
        "opex_intensity": _round5(_safe_div(opex, rev)),
        "capex_intensity": _round5(_safe_div(capex, rev)),
        "r_and_d_intensity": _round5(_safe_div(rnd, rev)) if rnd is not None else None,
        "dividend_payout": _round5(dividend_payout),
        "buyback_yield": _round5(buyback_yield),
        "fcf_conversion": _round5(fcf_conv),
        # new ratios
        "revenue_per_dollar_capex": _round5(rev_per_capex),
        "shareholder_yield": _round5(shareholder_yield),
        "reinvestment_ratio": _round5(reinvestment_ratio),
        "net_debt_issuance_intensity": _round5(_safe_div(debt_iss, rev)) if debt_iss is not None else None,
        "acquisition_intensity": _round5(_safe_div(acq, rev)) if acq is not None else None,
        # delta fields filled in post-hoc by _fill_deltas once the series exists
        "delta_gross_margin_bp": None,
        "delta_opex_intensity_bp": None,
    }


def _fill_deltas(periods: list[dict[str, Any]]) -> None:
    """Walk newest→oldest and populate delta_*_bp for each period using the
    prior period's ratio. In-place."""
    for i in range(len(periods) - 1):
        cur = periods[i]["ratios"]
        prv = periods[i + 1]["ratios"]
        for key in ("gross_margin", "opex_intensity"):
            c = cur.get(key)
            p = prv.get(key)
            if c is not None and p is not None:
                cur[f"delta_{key}_bp"] = _round5((c - p) * 10000.0)


def _fill_percentiles(
    engine: Any,
    ticker_upper: str,
    sector_name: str | None,
    periods: list[dict[str, Any]],
    period_type: str,
) -> None:
    """Attach per-sector percentile rankings to each period's ratios.

    Writes ``ratios._percentiles = {ratio_name: percentile_or_None}``
    using the cached sector-wide rankings from ``features.lab``.
    (Moved from ``intelligence.ratio_percentiles`` on 2026-04-11 — SYNTH-12.)
    No-ops if the ticker has no sector mapping or the computation fails.
    """
    try:
        from features.lab import (
            RATIO_NAMES,
            compute_all_percentiles,
        )
    except Exception as exc:
        log.debug("capital_flow: features.lab percentile import failed: {e}", e=exc)
        return

    if not sector_name:
        # No sector → no peer set. Still emit an empty _percentiles block
        # so the frontend can distinguish "not wired" from "no peers".
        for p in periods:
            p.setdefault("ratios", {})["_percentiles"] = {
                r: None for r in RATIO_NAMES
            }
        return

    try:
        all_pct = compute_all_percentiles(engine, period_type)
    except Exception as exc:
        log.debug("capital_flow: compute_all_percentiles failed: {e}", e=exc)
        return

    # Only the latest period has peer context — we only scan the latest
    # fiscal_period per ticker on the sector side. For older periods we
    # emit the same snapshot so the frontend badge stays informative.
    snapshot: dict[str, float | None] = {}
    for ratio_name in RATIO_NAMES:
        ratio_bucket = all_pct.get(ratio_name) or {}
        sector_bucket = ratio_bucket.get(sector_name) or {}
        snapshot[ratio_name] = sector_bucket.get(ticker_upper)

    for p in periods:
        p.setdefault("ratios", {})["_percentiles"] = dict(snapshot)


# ── Summary + narrative ──────────────────────────────────────────────


def _trichotomy(values: list[float], tol: float = 0.005) -> str | None:
    """Label a short series as expanding / contracting / flat.

    ``values`` is ordered newest-first (index 0 = latest). Flat tolerance
    is 50 bp by default. Needs at least 2 points to return anything.
    """
    v = [float(x) for x in values if x is not None]
    if len(v) < 2:
        return None
    delta = v[0] - v[-1]  # latest minus oldest in window
    if abs(delta) < tol:
        return "flat"
    return "expanding" if delta > 0 else "contracting"


def _compute_summary(periods: list[dict[str, Any]]) -> dict[str, Any]:
    """Build the summary block. ``periods`` is newest-first."""
    if not periods:
        return dict(_EMPTY_SUMMARY)

    latest = periods[0]
    latest_amounts = latest["_amounts"]
    latest_revenue = latest_amounts.get("revenue")

    # 3y CAGR requires 4 points (year 0 vs year -3).
    cagr: float | None = None
    if len(periods) >= 4:
        rev_latest = periods[0]["_amounts"].get("revenue")
        rev_3y = periods[3]["_amounts"].get("revenue")
        if rev_latest is not None and rev_3y is not None and rev_3y > 0:
            try:
                cagr = (rev_latest / rev_3y) ** (1.0 / 3.0) - 1.0
            except (ValueError, ZeroDivisionError):
                cagr = None

    # 3-period window for trailing averages / trends.
    window = periods[: min(3, len(periods))]

    cxs = [
        p["ratios"].get("capex_intensity") for p in window
        if p["ratios"].get("capex_intensity") is not None
    ]
    capex_3y_avg = sum(cxs) / len(cxs) if cxs else None

    gms = [
        p["ratios"].get("gross_margin") for p in window
        if p["ratios"].get("gross_margin") is not None
    ]
    gm_trend = _trichotomy(gms)
    capex_trend = _trichotomy(cxs)

    # Shareholder return: USD total (back-compat) + 3y avg yield (new).
    shareholder_total = 0.0
    shareholder_seen = False
    sy_ratios: list[float] = []
    for p in window:
        for ft in ("dividends", "buybacks"):
            v = p["_amounts"].get(ft)
            if v is not None:
                shareholder_total += float(v)
                shareholder_seen = True
        sy = p["ratios"].get("shareholder_yield")
        if sy is not None:
            sy_ratios.append(float(sy))
    shareholder_yield_3y_avg = (
        sum(sy_ratios) / len(sy_ratios) if sy_ratios else None
    )

    # Capital allocation mix: % of total outflows over the window.
    use_totals: dict[str, float] = {}
    for p in window:
        for row in p.get("outflows") or []:
            ft = row.get("flow_type") or "unknown"
            use_totals[ft] = use_totals.get(ft, 0.0) + float(row.get("amount_usd") or 0.0)
    total_outflows = sum(use_totals.values())
    mix: dict[str, float] = {}
    if total_outflows > 0:
        for ft, amt in use_totals.items():
            mix[ft] = _round5(amt / total_outflows)
    top_use = (
        max(use_totals.items(), key=lambda kv: kv[1])[0] if use_totals else None
    )

    # Top inflow counterparties across window — skip if all null.
    cp_totals: dict[str, float] = {}
    for p in window:
        for row in p.get("inflows") or []:
            cp = row.get("counterparty_id")
            if not cp:
                continue
            cp_totals[cp] = cp_totals.get(cp, 0.0) + float(row.get("amount_usd") or 0.0)
    top_3_inflow_counterparties: list[dict[str, Any]] | None = None
    if cp_totals:
        ordered = sorted(cp_totals.items(), key=lambda kv: kv[1], reverse=True)[:3]
        top_3_inflow_counterparties = [
            {"counterparty_id": cp, "amount_usd": round(amt, 2)}
            for cp, amt in ordered
        ]

    return {
        "latest_revenue_usd": latest_revenue,
        "revenue_3y_cagr": _round5(cagr),
        "capex_3y_avg_intensity": _round5(capex_3y_avg),
        "shareholder_return_3y_total_usd": round(shareholder_total, 2) if shareholder_seen else None,
        "top_use_of_capital_3y": top_use,
        # new fields
        "gross_margin_latest": latest["ratios"].get("gross_margin"),
        "gross_margin_trend_3y": gm_trend,
        "capex_intensity_trend_3y": capex_trend,
        "shareholder_yield_3y_avg": _round5(shareholder_yield_3y_avg),
        "capital_allocation_mix_3y": mix or None,
        "top_3_inflow_counterparties": top_3_inflow_counterparties,
    }


def _source_coverage(rows: list[dict[str, Any]]) -> dict[str, int]:
    """Bucket source_filing strings into coarse categories."""
    buckets = {"sec_filings": 0, "seed": 0, "announcement": 0, "other": 0}
    for r in rows:
        sf = (r.get("source_filing") or "").lower()
        if not sf:
            buckets["other"] += 1
        elif sf.startswith("10-") or sf.startswith("20-"):
            buckets["sec_filings"] += 1
        elif sf.startswith("seed"):
            buckets["seed"] += 1
        elif sf.startswith("8-") or "announce" in sf or "press" in sf:
            buckets["announcement"] += 1
        else:
            buckets["other"] += 1
    return buckets


# ── Narrative ────────────────────────────────────────────────────────


def _fmt_pct(v: float | None, places: int = 1) -> str:
    if v is None:
        return "n/a"
    return f"{v * 100:.{places}f}%"


def _fmt_usd(v: float | None) -> str:
    if v is None:
        return "n/a"
    a = abs(v)
    if a >= 1e12:
        return f"${v / 1e12:.2f}T"
    if a >= 1e9:
        return f"${v / 1e9:.2f}B"
    if a >= 1e6:
        return f"${v / 1e6:.2f}M"
    return f"${v:.0f}"


def _narrative(label: str, periods: list[dict[str, Any]], summary: dict[str, Any]) -> str:
    """Deterministic 2-3 sentence narrative. No LLM."""
    if not periods:
        return f"Capital flow data not yet available for {label} — pending XBRL ingestion."

    latest = periods[0]
    latest_label = latest["label"]
    sentences: list[str] = []

    # Sentence 1 — revenue + margin trajectory
    rev = latest["_amounts"].get("revenue") if "_amounts" in latest else None
    if rev is None:
        # periods may have been stripped of _amounts already
        rev = summary.get("latest_revenue_usd")
    cagr = summary.get("revenue_3y_cagr")
    gm = summary.get("gross_margin_latest")
    gm_trend = summary.get("gross_margin_trend_3y")

    bits_s1 = [f"{label} posted {_fmt_usd(rev)} in revenue for {latest_label}"]
    if cagr is not None:
        direction = "growing" if cagr > 0 else ("shrinking" if cagr < 0 else "flat")
        bits_s1.append(f"{direction} at a {_fmt_pct(abs(cagr))} 3y CAGR")
    if gm is not None:
        trend_str = f" and {gm_trend}" if gm_trend and gm_trend != "flat" else ""
        bits_s1.append(f"with gross margin at {_fmt_pct(gm)}{trend_str}")
    sentences.append(", ".join(bits_s1) + ".")

    # Sentence 2 — capital allocation
    top_use = summary.get("top_use_of_capital_3y")
    mix = summary.get("capital_allocation_mix_3y") or {}
    sy = summary.get("shareholder_yield_3y_avg")
    if top_use or sy is not None:
        top_share = mix.get(top_use) if top_use else None
        parts_s2: list[str] = []
        if top_use:
            share_str = f" ({_fmt_pct(top_share)} of 3y outflows)" if top_share is not None else ""
            parts_s2.append(f"Capital is concentrated in {top_use}{share_str}")
        if sy is not None:
            parts_s2.append(f"shareholder yield averaged {_fmt_pct(sy)} of revenue over 3y")
        if parts_s2:
            sentences.append("; ".join(parts_s2) + ".")

    # Sentence 3 — anomaly: worst margin period, or notable acquisition.
    anomaly = _pick_anomaly(periods)
    if anomaly:
        sentences.append(anomaly)

    return " ".join(sentences)


def _pick_anomaly(periods: list[dict[str, Any]]) -> str | None:
    """Return a short sentence describing the most notable data anomaly."""
    # 1. biggest acquisition in window.
    best_acq: tuple[float, str] | None = None
    for p in periods:
        amounts = p.get("_amounts") or {}
        acq = amounts.get("acquisitions")
        if acq is not None and acq > 0:
            if best_acq is None or acq > best_acq[0]:
                best_acq = (float(acq), p["label"])
    if best_acq and best_acq[0] >= 1e9:
        return f"Notable: {_fmt_usd(best_acq[0])} of acquisitions in {best_acq[1]}."

    # 2. worst gross margin period in the window.
    worst_gm: tuple[float, str] | None = None
    best_gm: tuple[float, str] | None = None
    for p in periods:
        gm = (p.get("ratios") or {}).get("gross_margin")
        if gm is None:
            continue
        if worst_gm is None or gm < worst_gm[0]:
            worst_gm = (float(gm), p["label"])
        if best_gm is None or gm > best_gm[0]:
            best_gm = (float(gm), p["label"])

    if worst_gm and best_gm and worst_gm[1] != best_gm[1]:
        spread = best_gm[0] - worst_gm[0]
        if spread >= 0.02:
            return (
                f"Margin range across window: "
                f"low of {_fmt_pct(worst_gm[0])} in {worst_gm[1]}, "
                f"high of {_fmt_pct(best_gm[0])} in {best_gm[1]}."
            )
    return None


# ── Fallback + endpoint ──────────────────────────────────────────────


_EMPTY_RATIOS: dict[str, Any] = {
    "gross_margin": None, "opex_intensity": None, "capex_intensity": None,
    "r_and_d_intensity": None, "dividend_payout": None, "buyback_yield": None,
    "fcf_conversion": None,
    "revenue_per_dollar_capex": None, "shareholder_yield": None,
    "reinvestment_ratio": None, "net_debt_issuance_intensity": None,
    "acquisition_intensity": None,
    "delta_gross_margin_bp": None, "delta_opex_intensity_bp": None,
    "_percentiles": {
        "gross_margin": None, "opex_intensity": None, "capex_intensity": None,
        "r_and_d_intensity": None, "dividend_payout": None, "buyback_yield": None,
        "fcf_conversion": None,
        "revenue_per_dollar_capex": None, "shareholder_yield": None,
        "reinvestment_ratio": None, "net_debt_issuance_intensity": None,
        "acquisition_intensity": None,
        "delta_gross_margin_bp": None, "delta_opex_intensity_bp": None,
    },
}
_EMPTY_SUMMARY: dict[str, Any] = {
    "latest_revenue_usd": None, "revenue_3y_cagr": None,
    "capex_3y_avg_intensity": None, "shareholder_return_3y_total_usd": None,
    "top_use_of_capital_3y": None,
    "gross_margin_latest": None, "gross_margin_trend_3y": None,
    "capex_intensity_trend_3y": None, "shareholder_yield_3y_avg": None,
    "capital_allocation_mix_3y": None, "top_3_inflow_counterparties": None,
    "market_cap_latest_usd": None,
}


def _fallback_payload(actor_meta: dict[str, Any], period_type: str) -> dict[str, Any]:
    label = actor_meta.get("label") or actor_meta.get("id") or "actor"
    return {
        "actor": actor_meta,
        "period_type": period_type,
        "period_labels": [],
        "periods": [{
            "fiscal_period": None, "label": "pending",
            "inflows": [], "outflows": [],
            "totals": {"inflow_usd": None, "outflow_usd": None, "net_usd": None},
            "ratios": dict(_EMPTY_RATIOS),
        }],
        "summary": dict(_EMPTY_SUMMARY),
        "narrative": f"Capital flow data not yet available for {label} — pending XBRL ingestion.",
        "provenance": {
            "rows": 0, "source": "fallback",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "confidence_mix": {},
            "source_coverage": {"sec_filings": 0, "seed": 0, "announcement": 0, "other": 0},
            "fx_conversions_applied": 0,
        },
    }


@router.get("/{actor_id}/capital_flow")
async def get_capital_flow(
    actor_id: str,
    periods: int = Query(4, ge=1, le=12),
    period_type: str = Query("annual", regex="^(annual|quarter|ttm)$"),
    _token: str = Depends(require_auth),
) -> dict:
    """Return per-period capital inflows, outflows, ratios, and summary."""
    if period_type not in _VALID_PERIOD_TYPES:
        period_type = "annual"

    # Strip canvas graph node-id prefixes (e.g. "a:corp_KO" → "KO").
    actor_id = _strip_canvas_prefix(actor_id)

    # Normalize id: seed data stores lowercase tickers; sector_map uses uppercase.
    id_variants = [actor_id, actor_id.lower(), actor_id.upper()]
    id_variants = list(dict.fromkeys(v for v in id_variants if v))

    cache_key = f"{actor_id}|{periods}|{period_type}"
    cached = _CACHE.get(cache_key)
    if cached is not None:
        return cached

    actor_meta = _lookup_actor(actor_id)
    engine = get_db_engine()
    rows: list[dict] = []
    for variant in id_variants:
        rows = _load_rows(engine, variant, period_type, periods)
        if rows:
            break
    if not rows:
        payload = _fallback_payload(actor_meta, period_type)
        _CACHE.set(cache_key, payload)
        return payload

    grouped: dict[Any, list[dict[str, Any]]] = {}
    for r in rows:
        grouped.setdefault(r["fiscal_period"], []).append(r)
    sorted_fps = sorted((fp for fp in grouped if fp is not None), reverse=True)

    # Per-period market cap from ticker_metrics_daily. Non-ticker actors
    # will get None here and the buyback_yield fallback handles them.
    mcap_ticker = actor_id  # _fetch_market_cap upper-cases internally.
    fx_counter: list[int] = [0]

    # Fetch deal-level announcement rows for decomposition. Only applicable
    # to annual rollups; the aggregator attaches them per outflow entry.
    # Today we decompose acquisitions only — other flow_types don't have a
    # useful deal-level view yet (no target counterparties stored).
    deals_by_year_by_flow: dict[str, dict[int, list[dict[str, Any]]]] = {}
    if period_type == "annual" and sorted_fps:
        years = sorted({int(fp.year) for fp in sorted_fps if hasattr(fp, "year")})
        # Use the first id variant that produced rows so the announcement
        # rows match the same actor_id spelling.
        deal_actor = variant if rows else actor_id
        for deal_ft in ("acquisitions",):
            deals_by_year_by_flow[deal_ft] = _load_deal_announcements(
                engine, deal_actor, deal_ft, years,
            )

    def _deals_for_fp(fp: Any) -> dict[str, list[dict[str, Any]]]:
        if not hasattr(fp, "year"):
            return {}
        y = int(fp.year)
        result: dict[str, list[dict[str, Any]]] = {}
        for ft, year_map in deals_by_year_by_flow.items():
            deals = year_map.get(y) or []
            if deals:
                result[ft] = deals
        return result

    period_records = [
        _build_period(
            fp, grouped[fp], period_type,
            market_cap=_fetch_market_cap(engine, mcap_ticker, fp),
            engine=engine,
            fx_counter=fx_counter,
            deals_for_period=_deals_for_fp(fp),
        )
        for fp in sorted_fps
    ]
    _fill_deltas(period_records)
    _fill_percentiles(
        engine,
        (actor_id or "").strip().upper(),
        actor_meta.get("sector"),
        period_records,
        period_type,
    )
    summary = _compute_summary(period_records)
    summary["market_cap_latest_usd"] = _fetch_market_cap(
        engine, mcap_ticker, None,
    )

    period_labels_desc = [p["label"] for p in period_records]

    # Build narrative BEFORE stripping _amounts so sentence 1 can see revenue.
    narrative = _narrative(
        actor_meta.get("label") or actor_id,
        period_records,
        summary,
    )

    for p in period_records:
        p.pop("_amounts", None)

    confidence_mix: dict[str, int] = {}
    for r in rows:
        c = r.get("confidence") or "unspecified"
        confidence_mix[c] = confidence_mix.get(c, 0) + 1

    source_coverage = _source_coverage(rows)

    payload: dict[str, Any] = {
        "actor": actor_meta,
        "period_type": period_type,
        # Ascending chronological order for the frontend axis.
        "period_labels": list(reversed(period_labels_desc)),
        "periods": period_records,
        "summary": summary,
        "narrative": narrative,
        "provenance": {
            "rows": len(rows),
            "source": "db",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "confidence_mix": confidence_mix,
            "source_coverage": source_coverage,
            "fx_conversions_applied": fx_counter[0],
        },
    }

    _CACHE.set(cache_key, payload)
    return payload
