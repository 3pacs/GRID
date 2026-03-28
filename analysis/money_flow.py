"""
GRID — Global Money Flow Map.

Aggregates all available flow data into a single hierarchical structure
showing how money moves through the global financial system:

    Central Banks (9)  ->  Banking System  ->  Institutional Layer
        ->  Markets  ->  Sectors  ->  Individual Positions

The Central Banks layer covers 9 major global central banks:
  Fed, ECB, BOJ, PBOC, BOE, RBA, BOC, SNB, RBI

For each bank we attempt to pull live data from resolved_series / raw_series
(we have pullers for Fed/FRED, ECB, RBI, and J-Quants/Japan).  Where live
data is unavailable, we use best-estimate fallbacks clearly marked with
confidence: "estimated".

Key aggregate metrics computed:
  - global_liquidity_total:     sum of all CB balance sheets in USD
  - global_liquidity_change_1m: aggregate monthly change (THE most important number)
  - global_policy_score:        GDP-weighted stance (-1 tightening to +1 easing)
  - currency_impacts:           rate-differential based FX strength signals

Special notes:
  - SNB holds $170B+ in US equities (AAPL, MSFT, etc.) which shows up in equity flows
  - Every estimated value carries a confidence label for frontend rendering
    (confirmed = solid, estimated = dashed/italic)

Data sources:
  - FRED: Fed balance sheet (WALCL), reverse repo (RRPONTSYD), TGA (WTREGEN),
          bank credit (TOTBKCR), delinquency (DRTSCIS), M2 (M2SL), FEDFUNDS
  - ECB SDW: ecb_total_assets, ecb_main_refi_rate, ecb_m3_yoy
  - RBI: india_fx_reserves, india_repo_rate
  - J-Quants: japan_topix, japan_nikkei225 (BOJ series when available)
  - yfinance / resolved_series: prices for SPY, TLT, GLD, BTC, DXY, sector ETFs
  - options_daily_signals: GEX regime per ticker
  - dark pool (FINRA ATS): accumulation/distribution signals
  - signal_sources: insider, congressional, dark pool signals
  - trust_scorer: convergence detection
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Global Central Banks Registry ─────────────────────────────────────
# Each entry carries the series IDs we try to resolve from raw_series /
# resolved_series and a fallback estimated balance sheet in USD.
# `confidence` is "confirmed" when we have live data, "estimated" when we
# use the hard-coded fallback.

CENTRAL_BANKS: dict[str, dict[str, Any]] = {
    "fed": {
        "name": "Federal Reserve",
        "country": "US",
        "balance_sheet_series": "WALCL",
        "rate_series": "FEDFUNDS",
        "currency": "USD",
        "estimated_balance_sheet_usd": None,  # from live data
        "confidence": "confirmed",
        "gdp_weight": 0.26,  # share of world GDP for policy score weighting
    },
    "ecb": {
        "name": "European Central Bank",
        "country": "EU",
        "balance_sheet_series": "ecb_total_assets",
        "rate_series": "ecb_main_refi_rate",
        "currency": "EUR",
        "estimated_balance_sheet_usd": 7_000_000_000_000,  # ~€6.5T
        "confidence": "estimated",
        "gdp_weight": 0.17,
    },
    "boj": {
        "name": "Bank of Japan",
        "country": "JP",
        "balance_sheet_series": "boj_total_assets",
        "rate_series": "boj_policy_rate",
        "currency": "JPY",
        "estimated_balance_sheet_usd": 4_500_000_000_000,  # ~¥700T
        "confidence": "estimated",
        "gdp_weight": 0.05,
    },
    "pboc": {
        "name": "People's Bank of China",
        "country": "CN",
        "balance_sheet_series": None,
        "rate_series": None,
        "currency": "CNY",
        "estimated_balance_sheet_usd": 6_000_000_000_000,
        "confidence": "estimated",
        "gdp_weight": 0.18,
    },
    "boe": {
        "name": "Bank of England",
        "country": "GB",
        "balance_sheet_series": "boe_total_assets",
        "rate_series": "boe_bank_rate",
        "currency": "GBP",
        "estimated_balance_sheet_usd": 1_000_000_000_000,
        "confidence": "estimated",
        "gdp_weight": 0.03,
    },
    "rba": {
        "name": "Reserve Bank of Australia",
        "country": "AU",
        "balance_sheet_series": None,
        "rate_series": None,
        "currency": "AUD",
        "estimated_balance_sheet_usd": 400_000_000_000,
        "confidence": "estimated",
        "gdp_weight": 0.02,
    },
    "boc": {
        "name": "Bank of Canada",
        "country": "CA",
        "balance_sheet_series": None,
        "rate_series": None,
        "currency": "CAD",
        "estimated_balance_sheet_usd": 300_000_000_000,
        "confidence": "estimated",
        "gdp_weight": 0.02,
    },
    "snb": {
        "name": "Swiss National Bank",
        "country": "CH",
        "balance_sheet_series": None,
        "rate_series": None,
        "currency": "CHF",
        "estimated_balance_sheet_usd": 800_000_000_000,  # holds $170B+ US equities
        "confidence": "estimated",
        "gdp_weight": 0.01,
        "us_equity_holdings_usd": 170_000_000_000,  # AAPL, MSFT, etc.
    },
    "rbi": {
        "name": "Reserve Bank of India",
        "country": "IN",
        "balance_sheet_series": "india_fx_reserves",
        "rate_series": "india_repo_rate",
        "currency": "INR",
        "estimated_balance_sheet_usd": 650_000_000_000,
        "confidence": "estimated",
        "gdp_weight": 0.04,
    },
}


# ── Sector ETF mapping (mirrors capital_flows.py) ──────────────────────
SECTOR_ETFS: dict[str, str] = {
    "Technology": "XLK",
    "Financials": "XLF",
    "Energy": "XLE",
    "Healthcare": "XLV",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Industrials": "XLI",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Utilities": "XLU",
    "Communication Services": "XLC",
}

# Market-level tickers
MARKET_TICKERS: dict[str, str] = {
    "equities": "SPY",
    "bonds": "TLT",
    "commodities": "GLD",
    "crypto": "BTC-USD",
    "fx": "DX-Y.NYB",
}

# FRED series for each layer
FRED_CENTRAL_BANK = {
    "balance_sheet": "WALCL",
    "reverse_repo": "RRPONTSYD",
    "tga_balance": "WTREGEN",
}

FRED_BANKING = {
    "bank_credit": "TOTBKCR",
    "m2": "M2SL",
}


class _NumpyEncoder(json.JSONEncoder):
    """JSON encoder that handles numpy types."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            val = float(obj)
            if np.isnan(val) or np.isinf(val):
                return None
            return val
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        return super().default(obj)


# ── Helpers ────────────────────────────────────────────────────────────

def _safe_pct_change(current: float | None, previous: float | None) -> float | None:
    """Percentage change, returning None if either value is missing/zero."""
    if current is None or previous is None or previous == 0:
        return None
    return round((current - previous) / abs(previous), 6)


def _get_fred_latest(
    engine: Engine, series_id: str, as_of: date | None = None,
) -> float | None:
    """Fetch the most recent value for a FRED series from resolved_series or raw_series."""
    if as_of is None:
        as_of = date.today()
    with engine.connect() as conn:
        # Try resolved_series first (higher quality)
        row = conn.execute(text("""
            SELECT rs.value
            FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :name AND rs.obs_date <= :d
            ORDER BY rs.obs_date DESC LIMIT 1
        """), {"name": series_id.lower(), "d": as_of}).fetchone()
        if row:
            return float(row[0])

        # Fallback to raw_series
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 1
        """), {"sid": series_id, "d": as_of}).fetchone()
        if row:
            return float(row[0])
    return None


def _get_fred_value_at(
    engine: Engine, series_id: str, target_date: date,
) -> float | None:
    """Fetch the FRED series value at or near a target date."""
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 1
        """), {"sid": series_id, "d": target_date}).fetchone()
        if row:
            return float(row[0])
    return None


def _get_price(
    engine: Engine, ticker: str, as_of: date | None = None,
) -> float | None:
    """Get the latest close price for a ticker."""
    if as_of is None:
        as_of = date.today()
    with engine.connect() as conn:
        # Try yfinance raw_series
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 1
        """), {"sid": f"YF:{ticker}:close", "d": as_of}).fetchone()
        if row:
            return float(row[0])

        # Try resolved_series with lowercase ticker
        row = conn.execute(text("""
            SELECT rs.value
            FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE (fr.name = :n1 OR fr.name = :n2)
            AND rs.obs_date <= :d
            ORDER BY rs.obs_date DESC LIMIT 1
        """), {
            "n1": f"{ticker.lower()}_close",
            "n2": ticker.lower(),
            "d": as_of,
        }).fetchone()
        if row:
            return float(row[0])
    return None


def _get_price_change(
    engine: Engine, ticker: str, days: int, as_of: date | None = None,
) -> float | None:
    """Compute price change percentage over a period."""
    if as_of is None:
        as_of = date.today()
    current = _get_price(engine, ticker, as_of)
    past = _get_price(engine, ticker, as_of - timedelta(days=days))
    return _safe_pct_change(current, past)


# ── Central Bank Data Helpers ──────────────────────────────────────────

def _get_series_latest(
    engine: Engine, series_id: str | None, as_of: date | None = None,
) -> float | None:
    """Try resolved_series first (lowercase feature name), then raw_series.

    Works for any series_id: FRED, ECB, BOJ, BOE, RBI, etc.
    """
    if not series_id:
        return None
    if as_of is None:
        as_of = date.today()

    with engine.connect() as conn:
        # resolved_series (canonical feature name, lowercase)
        row = conn.execute(text("""
            SELECT rs.value
            FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :name AND rs.obs_date <= :d
            ORDER BY rs.obs_date DESC LIMIT 1
        """), {"name": series_id.lower(), "d": as_of}).fetchone()
        if row:
            return float(row[0])

        # raw_series (exact series_id or lowercase variant)
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE (series_id = :sid OR series_id = :sid_lower)
            AND obs_date <= :d AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 1
        """), {"sid": series_id, "sid_lower": series_id.lower(), "d": as_of}).fetchone()
        if row:
            return float(row[0])
    return None


def _get_series_value_at(
    engine: Engine, series_id: str | None, target_date: date,
) -> float | None:
    """Fetch any series value at or near a target date."""
    if not series_id:
        return None
    with engine.connect() as conn:
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE (series_id = :sid OR series_id = :sid_lower)
            AND obs_date <= :d AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 1
        """), {"sid": series_id, "sid_lower": series_id.lower(), "d": target_date}).fetchone()
        if row:
            return float(row[0])
        # Try resolved_series
        row = conn.execute(text("""
            SELECT rs.value
            FROM resolved_series rs
            JOIN feature_registry fr ON rs.feature_id = fr.id
            WHERE fr.name = :name AND rs.obs_date <= :d
            ORDER BY rs.obs_date DESC LIMIT 1
        """), {"name": series_id.lower(), "d": target_date}).fetchone()
        if row:
            return float(row[0])
    return None


def _resolve_cb_balance_sheet(
    engine: Engine,
    cb_id: str,
    cb_config: dict[str, Any],
    as_of: date,
) -> tuple[float | None, str]:
    """Resolve a central bank's balance sheet value and confidence label.

    Returns:
        (value_in_usd_or_native, confidence) — "confirmed" if from live data,
        "estimated" if using the hard-coded fallback.
    """
    series = cb_config.get("balance_sheet_series")
    live_val = _get_series_latest(engine, series, as_of) if series else None
    if live_val is not None:
        return live_val, "confirmed"

    fallback = cb_config.get("estimated_balance_sheet_usd")
    return fallback, "estimated"


def _resolve_cb_rate(
    engine: Engine,
    cb_id: str,
    cb_config: dict[str, Any],
    as_of: date,
) -> tuple[float | None, str]:
    """Resolve a central bank's policy rate and confidence label."""
    series = cb_config.get("rate_series")
    live_val = _get_series_latest(engine, series, as_of) if series else None
    if live_val is not None:
        return live_val, "confirmed"
    return None, "estimated"


def _infer_policy_stance(
    engine: Engine,
    cb_id: str,
    cb_config: dict[str, Any],
    as_of: date,
) -> tuple[str, float]:
    """Determine policy stance: tightening / easing / holding.

    Returns:
        (stance_label, score) where score is -1 (tightening) to +1 (easing).
        0 = holding / unknown.
    """
    series = cb_config.get("rate_series")
    if not series:
        return "unknown", 0.0

    current = _get_series_latest(engine, series, as_of)
    past = _get_series_value_at(engine, series, as_of - timedelta(days=90))

    if current is None or past is None:
        return "unknown", 0.0

    diff = current - past
    if diff > 0.10:
        return "tightening", -1.0
    elif diff < -0.10:
        return "easing", 1.0
    else:
        return "holding", 0.0


def _compute_bs_change(
    engine: Engine,
    cb_config: dict[str, Any],
    as_of: date,
    days: int = 30,
) -> tuple[float | None, str]:
    """Compute 1-month balance sheet change.

    Returns:
        (change_value, confidence).
    """
    series = cb_config.get("balance_sheet_series")
    if not series:
        return None, "estimated"

    current = _get_series_latest(engine, series, as_of)
    past = _get_series_value_at(engine, series, as_of - timedelta(days=days))
    if current is not None and past is not None:
        return round(current - past, 2), "confirmed"
    return None, "estimated"


def _get_gex_regime(engine: Engine, ticker: str) -> str | None:
    """Get the current GEX regime for a ticker from options_daily_signals."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT gex_regime FROM options_daily_signals
                WHERE ticker = :t
                ORDER BY signal_date DESC LIMIT 1
            """), {"t": ticker}).fetchone()
            if row and row[0]:
                return str(row[0])
    except Exception:
        pass
    return None


def _get_dark_pool_signal(engine: Engine, ticker: str) -> str | None:
    """Determine dark pool signal from recent volume spikes."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT signal_value FROM signal_sources
                WHERE source_type = 'darkpool' AND ticker = :t
                AND signal_date >= :d
                ORDER BY signal_date DESC LIMIT 1
            """), {"t": ticker, "d": date.today() - timedelta(days=14)}).fetchone()
            if row and row[0]:
                payload = json.loads(row[0]) if isinstance(row[0], str) else row[0]
                ratio = payload.get("spike_ratio", 1.0)
                if ratio >= 2.0:
                    return "accumulation"
                elif ratio >= 1.5:
                    return "above_average"
                return "normal"
    except Exception:
        pass
    return None


def _get_insider_signal(engine: Engine, ticker: str) -> str | None:
    """Check for recent insider buying/selling clusters."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT direction, COUNT(*) as cnt
                FROM signal_sources
                WHERE source_type = 'insider' AND ticker = :t
                AND signal_date >= :d
                GROUP BY direction
            """), {"t": ticker, "d": date.today() - timedelta(days=30)}).fetchall()
            if rows:
                buys = sum(r[1] for r in rows if r[0] == "BUY")
                sells = sum(r[1] for r in rows if r[0] == "SELL")
                if buys >= 3:
                    return "cluster_buy"
                if sells >= 3:
                    return "cluster_sell"
                if buys > sells:
                    return "net_buying"
                if sells > buys:
                    return "net_selling"
    except Exception:
        pass
    return None


def _get_congressional_signal(engine: Engine, ticker: str) -> str | None:
    """Check for recent congressional trading signals."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT direction, COUNT(*) as cnt
                FROM signal_sources
                WHERE source_type = 'congressional' AND ticker = :t
                AND signal_date >= :d
                GROUP BY direction
            """), {"t": ticker, "d": date.today() - timedelta(days=45)}).fetchall()
            if rows:
                buys = sum(r[1] for r in rows if r[0] == "BUY")
                sells = sum(r[1] for r in rows if r[0] == "SELL")
                if buys > 0 and sells == 0:
                    return "buying"
                if sells > 0 and buys == 0:
                    return "selling"
                if buys > sells:
                    return "net_buying"
                if sells > buys:
                    return "net_selling"
    except Exception:
        pass
    return None


def _get_whale_flow(engine: Engine, ticker: str) -> str | None:
    """Infer whale flow from options daily signals (put/call ratio)."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT pcr, total_volume FROM options_daily_signals
                WHERE ticker = :t
                ORDER BY signal_date DESC LIMIT 1
            """), {"t": ticker}).fetchone()
            if row:
                pcr = float(row[0]) if row[0] else None
                if pcr is not None:
                    if pcr < 0.5:
                        return "call_heavy"
                    elif pcr < 0.8:
                        return "slightly_bullish"
                    elif pcr > 1.5:
                        return "put_heavy"
                    elif pcr > 1.2:
                        return "slightly_bearish"
                    return "neutral"
    except Exception:
        pass
    return None


# ── Layer Builders ─────────────────────────────────────────────────────

def _build_central_banks_layer(engine: Engine, as_of: date) -> dict:
    """Build the Central Banks layer with all 9 major central banks.

    For each bank we try to resolve live data from resolved_series / raw_series,
    falling back to hard-coded estimates.  Every value carries a confidence label
    ("confirmed" or "estimated") so the frontend can render confirmed data as
    solid and estimates as dashed / italic.

    Also computes aggregate metrics:
      - global_liquidity_total:  sum of all CB balance sheets (USD)
      - global_liquidity_change_1m:  aggregate 1-month change (THE key number)
      - global_policy_score:  GDP-weighted average stance (-1 to +1)
      - global_policy_label:  human-readable stance summary
      - currency_impacts:  rate-differential driven FX signals
    """
    one_month_ago = as_of - timedelta(days=30)
    one_week_ago = as_of - timedelta(days=7)

    nodes: list[dict[str, Any]] = []

    # Accumulators for global aggregates
    total_balance_sheet_usd: float = 0.0
    total_bs_change_1m_usd: float = 0.0
    weighted_policy_score: float = 0.0
    total_policy_weight: float = 0.0
    rate_map: dict[str, float] = {}  # cb_id -> rate for currency impact calc

    for cb_id, cb_cfg in CENTRAL_BANKS.items():
        # ── Balance sheet ────────────────────────────────────────────
        bs_val, bs_conf = _resolve_cb_balance_sheet(engine, cb_id, cb_cfg, as_of)

        # For the Fed, WALCL is in *millions* — convert to USD for apples-to-
        # apples comparison with other banks stored in absolute USD.
        if cb_id == "fed" and bs_val is not None and bs_conf == "confirmed":
            bs_val_usd = bs_val * 1_000_000  # WALCL millions -> USD
        elif bs_val is not None:
            bs_val_usd = bs_val
        else:
            bs_val_usd = 0.0

        # ── Balance sheet change (1 month) ───────────────────────────
        bs_change, bs_change_conf = _compute_bs_change(engine, cb_cfg, as_of, days=30)

        # Convert Fed change from millions to USD
        bs_change_usd: float | None = None
        if bs_change is not None:
            bs_change_usd = bs_change * 1_000_000 if cb_id == "fed" else bs_change

        # ── Policy rate ──────────────────────────────────────────────
        rate_val, rate_conf = _resolve_cb_rate(engine, cb_id, cb_cfg, as_of)

        # ── Policy stance ────────────────────────────────────────────
        stance_label, stance_score = _infer_policy_stance(engine, cb_id, cb_cfg, as_of)

        # ── Fed-specific extras (reverse repo, TGA, net liquidity) ───
        fed_extras: dict[str, Any] = {}
        if cb_id == "fed":
            reverse_repo = _get_fred_latest(engine, "RRPONTSYD", as_of)
            reverse_repo_1w = _get_fred_value_at(engine, "RRPONTSYD", one_week_ago)
            tga = _get_fred_latest(engine, "WTREGEN", as_of)

            net_liq = None
            if bs_val is not None:  # bs_val is in millions for Fed
                net_liq = bs_val
                if reverse_repo is not None:
                    net_liq -= reverse_repo
                if tga is not None:
                    net_liq -= tga

            net_liq_1m = None
            bs_val_1m = _get_fred_value_at(engine, "WALCL", one_month_ago)
            if bs_val_1m is not None:
                net_liq_1m = bs_val_1m
                rr_1m = _get_fred_value_at(engine, "RRPONTSYD", one_month_ago)
                tga_1m = _get_fred_value_at(engine, "WTREGEN", one_month_ago)
                if rr_1m is not None:
                    net_liq_1m -= rr_1m
                if tga_1m is not None:
                    net_liq_1m -= tga_1m

            nl_change_val = None
            if net_liq is not None and net_liq_1m is not None:
                nl_change_val = round(net_liq - net_liq_1m, 0)

            rr_change_val = None
            if reverse_repo is not None and reverse_repo_1w is not None:
                rr_change_val = round(reverse_repo - reverse_repo_1w, 0)

            fed_extras = {
                "reverse_repo": reverse_repo,
                "reverse_repo_change_1w": rr_change_val,
                "tga_balance": tga,
                "net_liquidity": net_liq,
                "net_liquidity_change_1m": nl_change_val,
            }

            # The Fed signal uses net-liquidity change
            fed_signal = "stable"
            if bs_change is not None:
                if bs_change < -20:
                    fed_signal = "draining"
                elif bs_change > 20:
                    fed_signal = "injecting"
            fed_extras["signal"] = fed_signal

        # ── SNB special: US equity holdings ──────────────────────────
        snb_extras: dict[str, Any] = {}
        if cb_id == "snb":
            snb_extras = {
                "us_equity_holdings_usd": cb_cfg.get("us_equity_holdings_usd"),
                "us_equity_holdings_note": (
                    "SNB holds $170B+ in US equities (AAPL, MSFT, etc.) "
                    "— shows up directly in equity flows"
                ),
                "us_equity_confidence": "estimated",
            }

        # ── Accumulate global aggregates ─────────────────────────────
        total_balance_sheet_usd += bs_val_usd

        if bs_change_usd is not None:
            total_bs_change_1m_usd += bs_change_usd

        gdp_weight = cb_cfg.get("gdp_weight", 0.0)
        if stance_label != "unknown":
            weighted_policy_score += stance_score * gdp_weight
            total_policy_weight += gdp_weight

        if rate_val is not None:
            rate_map[cb_id] = rate_val

        # ── Build node ───────────────────────────────────────────────
        metrics: dict[str, Any] = {
            "balance_sheet_usd": bs_val_usd if bs_val_usd else None,
            "balance_sheet_confidence": bs_conf,
            "balance_sheet_change_1m_usd": bs_change_usd,
            "balance_sheet_change_confidence": bs_change_conf,
            "policy_rate": rate_val,
            "policy_rate_confidence": rate_conf,
            "policy_stance": stance_label,
            "policy_stance_score": stance_score,
            "currency": cb_cfg.get("currency"),
            "country": cb_cfg.get("country"),
        }
        # Merge Fed-specific or SNB-specific extras
        metrics.update(fed_extras)
        metrics.update(snb_extras)

        nodes.append({
            "id": cb_id,
            "label": cb_cfg["name"],
            "metrics": metrics,
        })

    # ── Global aggregate metrics ─────────────────────────────────────
    global_policy_score = (
        round(weighted_policy_score / total_policy_weight, 4)
        if total_policy_weight > 0 else 0.0
    )

    if global_policy_score > 0.2:
        global_policy_label = "net_easing"
    elif global_policy_score < -0.2:
        global_policy_label = "net_tightening"
    else:
        global_policy_label = "mixed"

    # Rate-differential based currency strength signals
    currency_impacts: list[dict[str, Any]] = []
    fed_rate = rate_map.get("fed")
    if fed_rate is not None:
        for cb_id, rate in rate_map.items():
            if cb_id == "fed":
                continue
            diff = fed_rate - rate
            ccy = CENTRAL_BANKS[cb_id].get("currency", "?")
            if diff > 1.0:
                currency_impacts.append({
                    "currency": ccy,
                    "vs_usd": "weakening",
                    "rate_differential": round(diff, 2),
                    "confidence": "estimated",
                    "note": f"USD yields {diff:.1f}pp above {ccy}",
                })
            elif diff < -1.0:
                currency_impacts.append({
                    "currency": ccy,
                    "vs_usd": "strengthening",
                    "rate_differential": round(diff, 2),
                    "confidence": "estimated",
                    "note": f"{ccy} yields {abs(diff):.1f}pp above USD",
                })

    # Monthly liquidity flow direction narrative
    liq_direction = "expanding" if total_bs_change_1m_usd > 0 else "contracting"
    liq_narrative = (
        f"Global liquidity is {liq_direction} at "
        f"${abs(total_bs_change_1m_usd) / 1e9:,.1f}B/month"
    )

    return {
        "id": "central_banks",
        "label": "Central Banks",
        "nodes": nodes,
        "global_liquidity": {
            "total_usd": round(total_balance_sheet_usd, 0),
            "change_1m_usd": round(total_bs_change_1m_usd, 0),
            "direction": liq_direction,
            "narrative": liq_narrative,
            "confidence": "estimated",  # mix of confirmed + estimated
            "bank_count": len(nodes),
        },
        "global_policy": {
            "score": global_policy_score,
            "label": global_policy_label,
            "description": (
                f"GDP-weighted policy score: {global_policy_score:+.2f} "
                f"(-1 = tightening, +1 = easing)"
            ),
            "confidence": "estimated",
        },
        "currency_impacts": currency_impacts,
    }


def _build_banking_layer(engine: Engine, as_of: date) -> dict:
    """Build the Banking System layer."""
    one_month_ago = as_of - timedelta(days=30)

    bank_credit = _get_fred_latest(engine, "TOTBKCR", as_of)
    bank_credit_1m = _get_fred_value_at(engine, "TOTBKCR", one_month_ago)

    m2 = _get_fred_latest(engine, "M2SL", as_of)
    m2_1m = _get_fred_value_at(engine, "M2SL", one_month_ago)

    signal = "stable"
    if bank_credit is not None and bank_credit_1m is not None:
        change = _safe_pct_change(bank_credit, bank_credit_1m)
        if change is not None:
            if change > 0.005:
                signal = "expanding"
            elif change < -0.005:
                signal = "contracting"

    return {
        "id": "banking",
        "label": "Banking System",
        "nodes": [
            {
                "id": "bank_credit",
                "label": "Bank Credit",
                "metrics": {
                    "total_credit": bank_credit,
                    "credit_change_1m": _safe_pct_change(bank_credit, bank_credit_1m),
                    "m2": m2,
                    "m2_change_1m": _safe_pct_change(m2, m2_1m),
                    "signal": signal,
                },
            }
        ],
    }


def _build_markets_layer(engine: Engine, as_of: date) -> dict:
    """Build the Markets layer (equities, bonds, commodities, crypto, FX)."""
    nodes = []
    for market_id, ticker in MARKET_TICKERS.items():
        price = _get_price(engine, ticker, as_of)
        change_1m = _get_price_change(engine, ticker, 30, as_of)
        gex = _get_gex_regime(engine, ticker) if market_id == "equities" else None
        dp = _get_dark_pool_signal(engine, ticker) if market_id == "equities" else None

        label_map = {
            "equities": "Equities",
            "bonds": "Bonds",
            "commodities": "Commodities",
            "crypto": "Crypto",
            "fx": "FX (Dollar)",
        }

        metrics: dict[str, Any] = {
            "price": price,
            "price_change_1m": change_1m,
        }
        if gex:
            metrics["gex_regime"] = gex
        if dp:
            metrics["dark_pool_signal"] = dp

        nodes.append({
            "id": market_id,
            "label": label_map.get(market_id, market_id),
            "metrics": metrics,
        })

    return {
        "id": "markets",
        "label": "Markets",
        "nodes": nodes,
    }


def _build_sectors_layer(engine: Engine, as_of: date) -> dict:
    """Build the Sectors layer with ETF data, intelligence signals, and real dollar flows."""
    # Pull real aggregated dollar flows from flow_aggregator
    sector_flows: dict[str, dict] = {}
    try:
        from analysis.flow_aggregator import aggregate_by_sector
        sector_flows = aggregate_by_sector(engine, days=30)
    except Exception as exc:
        log.debug("flow_aggregator unavailable, falling back to estimates: {e}", e=str(exc))

    nodes = []
    for sector_name, etf in SECTOR_ETFS.items():
        price_change = _get_price_change(engine, etf, 30, as_of)
        insider = _get_insider_signal(engine, etf)
        congress = _get_congressional_signal(engine, etf)
        dp = _get_dark_pool_signal(engine, etf)
        whale = _get_whale_flow(engine, etf)

        # Short sector id
        sector_id = sector_name.lower().replace(" ", "_")

        metrics: dict[str, Any] = {
            "etf_ticker": etf,
            "price_change_1m": price_change,
        }
        if insider:
            metrics["insider_signal"] = insider
        if congress:
            metrics["congressional_signal"] = congress
        if dp:
            metrics["dark_pool"] = dp
        if whale:
            metrics["whale_flow"] = whale

        # Overlay real dollar flow data from flow_aggregator
        sf = sector_flows.get(sector_name)
        if sf:
            metrics["dollar_flow_net"] = sf.get("net_flow")
            metrics["dollar_flow_direction"] = sf.get("direction")
            metrics["dollar_flow_acceleration"] = sf.get("acceleration")
            metrics["dollar_flow_inflow"] = sf.get("inflow")
            metrics["dollar_flow_outflow"] = sf.get("outflow")

        nodes.append({
            "id": sector_id,
            "label": sector_name,
            "metrics": metrics,
        })

    return {
        "id": "sectors",
        "label": "Sectors",
        "nodes": nodes,
    }


# ── Flow Inference ─────────────────────────────────────────────────────

def _infer_flows(layers: list[dict], engine: Engine, as_of: date) -> list[dict]:
    """Infer money flows between layers based on available data.

    Flow logic:
      - Central banks -> Markets: net liquidity change drives equity/bond flows
      - Banking -> Markets: credit expansion/contraction flows into equities
      - Markets -> Sectors: relative performance drives sector rotation
    """
    flows: list[dict] = []

    # Find layer nodes by id for quick lookup
    node_map: dict[str, dict] = {}
    for layer in layers:
        for node in layer.get("nodes", []):
            node_map[node["id"]] = node

    # Central bank -> Markets flows (all 9 banks)
    cb_layer = next((l for l in layers if l["id"] == "central_banks"), None)
    if cb_layer:
        for cb_node in cb_layer.get("nodes", []):
            cb_id = cb_node["id"]
            m = cb_node.get("metrics", {})
            bs_change = m.get("balance_sheet_change_1m_usd")
            bs_conf = m.get("balance_sheet_change_confidence", "estimated")

            # For the Fed, use net_liquidity_change_1m (more precise —
            # accounts for reverse repo and TGA).
            if cb_id == "fed":
                nl_change = m.get("net_liquidity_change_1m")
                if nl_change is not None:
                    abs_vol = abs(nl_change)
                    direction = "inflow" if nl_change > 0 else "outflow"
                    flows.append({
                        "from": "fed",
                        "to": "equities",
                        "volume": abs_vol * 0.5,
                        "direction": direction,
                        "change": _safe_pct_change(abs_vol, abs_vol * 0.9),
                        "label": f"Fed liquidity {'injection' if direction == 'inflow' else 'drain'}",
                        "confidence": "confirmed",
                    })
                    flows.append({
                        "from": "fed",
                        "to": "bonds",
                        "volume": abs_vol * 0.3,
                        "direction": "inflow" if direction == "outflow" else "outflow",
                        "change": None,
                        "label": "Flight to/from safety",
                        "confidence": "confirmed",
                    })
                continue

            # SNB is special — its US equity holdings feed directly into equities
            if cb_id == "snb":
                us_eq = m.get("us_equity_holdings_usd")
                if us_eq:
                    flows.append({
                        "from": "snb",
                        "to": "equities",
                        "volume": us_eq,
                        "direction": "inflow",
                        "change": None,
                        "label": f"SNB US equity holdings: ${us_eq / 1e9:.0f}B (AAPL, MSFT, etc.)",
                        "confidence": "estimated",
                    })

            # Other central banks: estimate flow from balance sheet change
            if bs_change is not None and bs_change != 0:
                abs_vol = abs(bs_change)
                direction = "inflow" if bs_change > 0 else "outflow"
                label = cb_node["label"]
                stance = m.get("policy_stance", "unknown")

                # Majority of CB expansion flows into equities + bonds
                flows.append({
                    "from": cb_id,
                    "to": "equities",
                    "volume": abs_vol * 0.4,
                    "direction": direction,
                    "change": None,
                    "label": f"{label} balance sheet {direction} ({stance})",
                    "confidence": bs_conf,
                })
                flows.append({
                    "from": cb_id,
                    "to": "bonds",
                    "volume": abs_vol * 0.4,
                    "direction": direction,
                    "change": None,
                    "label": f"{label} sovereign bond channel ({stance})",
                    "confidence": bs_conf,
                })
            elif m.get("balance_sheet_usd"):
                # No change data — still show the static link with estimated
                # volume proportional to balance sheet size (1% monthly churn)
                bs_usd = m["balance_sheet_usd"]
                est_monthly_flow = bs_usd * 0.01
                stance = m.get("policy_stance", "unknown")
                flows.append({
                    "from": cb_id,
                    "to": "equities",
                    "volume": est_monthly_flow * 0.4,
                    "direction": "inflow" if stance == "easing" else "outflow",
                    "change": None,
                    "label": f"{cb_node['label']} est. market channel ({stance})",
                    "confidence": "estimated",
                })

    # Banking -> Markets flows
    bank = node_map.get("bank_credit", {})
    bank_metrics = bank.get("metrics", {})
    credit_change = bank_metrics.get("credit_change_1m")
    if credit_change is not None:
        direction = "inflow" if credit_change > 0 else "outflow"
        flows.append({
            "from": "bank_credit",
            "to": "equities",
            "volume": abs(credit_change) * 1e9,
            "direction": direction,
            "change": credit_change,
            "label": f"Credit {'expansion' if direction == 'inflow' else 'contraction'}",
        })

    # Markets -> Sectors flows
    # Prefer real dollar flow data from flow_aggregator; fall back to
    # relative-performance estimates when dollar flows are unavailable.
    equities = node_map.get("equities", {})
    spy_change = equities.get("metrics", {}).get("price_change_1m")

    sectors_layer = next((l for l in layers if l["id"] == "sectors"), None)
    if sectors_layer:
        for sector_node in sectors_layer.get("nodes", []):
            sm = sector_node.get("metrics", {})

            # Use real dollar flows when available
            dollar_net = sm.get("dollar_flow_net")
            if dollar_net is not None and dollar_net != 0:
                direction = sm.get("dollar_flow_direction", "inflow")
                accel = sm.get("dollar_flow_acceleration", "stable")
                volume = abs(dollar_net)
                label = (
                    f"{sector_node['label']}: "
                    f"${volume:,.0f} net {direction} ({accel})"
                )
                flows.append({
                    "from": "equities",
                    "to": sector_node["id"],
                    "volume": round(volume, 0),
                    "direction": direction,
                    "change": None,
                    "label": label,
                    "source": "dollar_flows",
                })
            elif spy_change is not None:
                # Fallback: estimate from relative performance
                s_change = sm.get("price_change_1m")
                if s_change is not None:
                    relative = s_change - spy_change
                    direction = "inflow" if relative > 0 else "outflow"
                    volume = abs(relative) * 1e10  # Scale for visual weight
                    flows.append({
                        "from": "equities",
                        "to": sector_node["id"],
                        "volume": round(volume, 0),
                        "direction": direction,
                        "change": round(relative, 4),
                        "label": f"{sector_node['label']} vs SPY: {relative*100:+.1f}%",
                        "source": "estimated",
                    })

    return flows


# ── Intelligence Aggregation ───────────────────────────────────────────

def _build_intelligence(engine: Engine) -> dict:
    """Aggregate convergence alerts and trusted signals."""
    result: dict[str, Any] = {
        "convergence_alerts": [],
        "trusted_signals": [],
        "narrative": None,
    }

    try:
        from intelligence.trust_scorer import detect_convergence
        convergence = detect_convergence(engine)
        result["convergence_alerts"] = convergence[:10] if convergence else []
    except Exception as exc:
        log.debug("Could not fetch convergence: {e}", e=str(exc))

    # Top trusted signals from recent period
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_type, source_id, ticker, direction,
                       signal_date, trust_score
                FROM signal_sources
                WHERE signal_date >= :d AND trust_score >= 0.6
                ORDER BY trust_score DESC, signal_date DESC
                LIMIT 10
            """), {"d": date.today() - timedelta(days=14)}).fetchall()

            result["trusted_signals"] = [
                {
                    "source_type": r[0],
                    "source_id": r[1],
                    "ticker": r[2],
                    "direction": r[3],
                    "signal_date": str(r[4]),
                    "trust_score": round(float(r[5]), 3) if r[5] else 0.5,
                }
                for r in rows
            ]
    except Exception as exc:
        log.debug("Could not fetch trusted signals: {e}", e=str(exc))

    return result


# ── LLM Narrative ──────────────────────────────────────────────────────

def _generate_narrative(
    layers: list[dict],
    flows: list[dict],
    intelligence: dict,
) -> str | None:
    """Use LLM to generate a one-paragraph narrative of the flow picture."""
    try:
        from ollama.client import get_client
        client = get_client()
    except Exception:
        return _generate_fallback_narrative(layers, flows, intelligence)

    # Build a condensed summary for the LLM
    summary_parts = []
    for layer in layers:
        for node in layer.get("nodes", []):
            m = node.get("metrics", {})
            sig = m.get("signal", "")
            parts = [f"{node['label']}:"]
            for k, v in m.items():
                if v is not None and k != "signal":
                    parts.append(f"  {k}={v}")
            if sig:
                parts.append(f"  signal={sig}")
            summary_parts.append(" ".join(parts))

    flow_summary = []
    for f in flows[:15]:
        flow_summary.append(
            f"  {f['from']} -> {f['to']}: {f.get('label', '')} "
            f"({f['direction']}, vol={f.get('volume')})"
        )

    convergence_summary = ""
    if intelligence.get("convergence_alerts"):
        alerts = intelligence["convergence_alerts"]
        convergence_summary = "Convergence alerts: " + ", ".join(
            f"{a.get('ticker', '?')} {a.get('direction', '?')} ({a.get('source_count', 0)} sources)"
            for a in alerts[:5]
        )

    prompt = f"""You are a macro strategist. Given the following global money flow data,
write ONE paragraph (3-5 sentences) explaining:
1. Where money is flowing and why
2. What is leaving and where
3. The key thing to watch

Data:
{chr(10).join(summary_parts)}

Flows:
{chr(10).join(flow_summary)}

{convergence_summary}

Be specific, cite numbers. No hedging language. Direct and actionable."""

    try:
        response = client.chat(
            messages=[{"role": "user", "content": prompt}],
            temperature=0.3,
        )
        if response and hasattr(response, "message"):
            return response.message.content
        if isinstance(response, dict):
            return response.get("message", {}).get("content")
    except Exception as exc:
        log.debug("LLM narrative generation failed: {e}", e=str(exc))

    return _generate_fallback_narrative(layers, flows, intelligence)


def _generate_fallback_narrative(
    layers: list[dict],
    flows: list[dict],
    intelligence: dict,
) -> str:
    """Generate a rule-based narrative when LLM is unavailable."""
    parts = []

    # Global liquidity signal
    for layer in layers:
        if layer["id"] == "central_banks":
            gl = layer.get("global_liquidity", {})
            gl_narrative = gl.get("narrative")
            if gl_narrative:
                parts.append(gl_narrative + ".")

            # Fed-specific detail
            for node in layer["nodes"]:
                if node["id"] != "fed":
                    continue
                sig = node.get("metrics", {}).get("signal")
                nl = node.get("metrics", {}).get("net_liquidity_change_1m")
                if sig == "draining" and nl is not None:
                    parts.append(
                        f"Fed is draining liquidity (net liquidity change: {nl:+,.0f}M)."
                    )
                elif sig == "injecting" and nl is not None:
                    parts.append(
                        f"Fed is injecting liquidity (net liquidity change: {nl:+,.0f}M)."
                    )

            # Global policy stance
            gp = layer.get("global_policy", {})
            gp_label = gp.get("label")
            if gp_label and gp_label != "mixed":
                parts.append(
                    f"Global monetary policy is {gp_label.replace('_', ' ')} "
                    f"(score: {gp.get('score', 0):+.2f})."
                )

    # Top sector flows
    inflows = [f for f in flows if f.get("direction") == "inflow" and "equities" == f.get("from")]
    outflows = [f for f in flows if f.get("direction") == "outflow" and "equities" == f.get("from")]
    inflows.sort(key=lambda x: x.get("volume", 0), reverse=True)
    outflows.sort(key=lambda x: x.get("volume", 0), reverse=True)

    if inflows:
        top_in = inflows[0]
        parts.append(f"Money rotating into {top_in['to'].replace('_', ' ').title()}.")
    if outflows:
        top_out = outflows[0]
        parts.append(f"Outflows from {top_out['to'].replace('_', ' ').title()}.")

    # Convergence
    alerts = intelligence.get("convergence_alerts", [])
    if alerts:
        a = alerts[0]
        parts.append(
            f"Convergence alert: {a.get('ticker', '?')} "
            f"{a.get('direction', '?')} ({a.get('source_count', 0)} sources agree)."
        )

    return " ".join(parts) if parts else "Insufficient data to generate narrative."


# ── Levers ─────────────────────────────────────────────────────────────

def _build_levers(
    layers: list[dict],
    flows: list[dict],
    intelligence: dict,
) -> list[dict]:
    """Identify the top 5 market-moving levers right now, ranked by impact."""
    levers: list[dict] = []

    # 1. Global liquidity (sum of all central banks)
    for layer in layers:
        if layer["id"] == "central_banks":
            gl = layer.get("global_liquidity", {})
            gl_change = gl.get("change_1m_usd", 0)
            gl_direction = gl.get("direction", "stable")
            gl_narrative = gl.get("narrative", "")
            if gl_change != 0:
                levers.append({
                    "name": "Global Central Bank Liquidity",
                    "direction": gl_direction,
                    "magnitude": abs(gl_change),
                    "magnitude_label": gl_narrative,
                    "source": f"{gl.get('bank_count', 0)} central banks",
                    "impact_score": min(abs(gl_change) / 1e11, 10),
                    "confidence": gl.get("confidence", "estimated"),
                })

            # Also add Fed net liquidity as a separate lever (it's the
            # highest-fidelity single-bank signal we have)
            for node in layer["nodes"]:
                if node["id"] != "fed":
                    continue
                m = node.get("metrics", {})
                nl = m.get("net_liquidity_change_1m")
                sig = m.get("signal", "stable")
                if nl is not None:
                    levers.append({
                        "name": "Fed Net Liquidity",
                        "direction": "drain" if nl < 0 else "inject",
                        "magnitude": abs(nl),
                        "magnitude_label": f"{nl:+,.0f}M / month",
                        "source": "FRED (WALCL, RRPONTSYD, WTREGEN)",
                        "impact_score": min(abs(nl) / 100, 10),
                        "confidence": "confirmed",
                    })

            # Global policy stance lever
            gp = layer.get("global_policy", {})
            gp_score = gp.get("score", 0)
            if gp_score != 0:
                levers.append({
                    "name": "Global Monetary Policy Stance",
                    "direction": gp.get("label", "mixed"),
                    "magnitude": abs(gp_score),
                    "magnitude_label": gp.get("description", ""),
                    "source": "GDP-weighted central bank rates",
                    "impact_score": min(abs(gp_score) * 5, 8),
                    "confidence": "estimated",
                })

    # 2. Credit conditions
    for layer in layers:
        if layer["id"] == "banking":
            for node in layer["nodes"]:
                m = node.get("metrics", {})
                cc = m.get("credit_change_1m")
                if cc is not None:
                    levers.append({
                        "name": "Bank Credit",
                        "direction": "expanding" if cc > 0 else "contracting",
                        "magnitude": abs(cc),
                        "magnitude_label": f"{cc*100:+.2f}% / month",
                        "source": "FRED (TOTBKCR)",
                        "impact_score": min(abs(cc) * 200, 8),
                    })

    # 3. Sector rotation signals
    sector_flows = [f for f in flows if f.get("from") == "equities"]
    sector_flows.sort(key=lambda x: x.get("volume", 0), reverse=True)
    for sf in sector_flows[:2]:
        change_val = sf.get("change", 0) or 0
        levers.append({
            "name": f"Sector Rotation: {sf['to'].replace('_', ' ').title()}",
            "direction": sf["direction"],
            "magnitude": sf.get("volume", 0),
            "magnitude_label": sf.get("label", ""),
            "source": "YFinance sector ETFs",
            "impact_score": min(abs(change_val) * 50, 7),
        })

    # 4. Convergence alerts
    for alert in intelligence.get("convergence_alerts", [])[:2]:
        levers.append({
            "name": f"Convergence: {alert.get('ticker', '?')} {alert.get('direction', '?')}",
            "direction": alert.get("direction", "").lower(),
            "magnitude": alert.get("source_count", 0),
            "magnitude_label": f"{alert.get('source_count', 0)} sources agree",
            "source": "Trust Scorer",
            "impact_score": min(alert.get("source_count", 0) * 2, 9),
        })

    # Sort by impact score, return top 5
    levers.sort(key=lambda x: x.get("impact_score", 0), reverse=True)
    return levers[:5]


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════

def build_flow_map(engine: Engine, as_of: date | None = None) -> dict:
    """Build the complete money flow hierarchy.

    Aggregates data from FRED, yfinance, options, dark pools, insider/
    congressional signals, and trust scorer convergence into a single
    hierarchical structure for the global money flow visualization.

    Parameters:
        engine: SQLAlchemy database engine.
        as_of: Reference date (default: today).

    Returns:
        dict with keys: timestamp, layers, flows, intelligence, levers.
    """
    if as_of is None:
        as_of = date.today()

    log.info("Building global money flow map as_of={d}", d=as_of)

    # Build each layer
    layers: list[dict] = []

    try:
        layers.append(_build_central_banks_layer(engine, as_of))
    except Exception as exc:
        log.warning("Central banks layer failed: {e}", e=str(exc))
        layers.append({"id": "central_banks", "label": "Central Banks", "nodes": []})

    try:
        layers.append(_build_banking_layer(engine, as_of))
    except Exception as exc:
        log.warning("Banking layer failed: {e}", e=str(exc))
        layers.append({"id": "banking", "label": "Banking System", "nodes": []})

    try:
        layers.append(_build_markets_layer(engine, as_of))
    except Exception as exc:
        log.warning("Markets layer failed: {e}", e=str(exc))
        layers.append({"id": "markets", "label": "Markets", "nodes": []})

    try:
        layers.append(_build_sectors_layer(engine, as_of))
    except Exception as exc:
        log.warning("Sectors layer failed: {e}", e=str(exc))
        layers.append({"id": "sectors", "label": "Sectors", "nodes": []})

    # Infer flows between layers
    try:
        flows = _infer_flows(layers, engine, as_of)
    except Exception as exc:
        log.warning("Flow inference failed: {e}", e=str(exc))
        flows = []

    # Intelligence aggregation
    try:
        intelligence = _build_intelligence(engine)
    except Exception as exc:
        log.warning("Intelligence build failed: {e}", e=str(exc))
        intelligence = {"convergence_alerts": [], "trusted_signals": [], "narrative": None}

    # LLM narrative
    try:
        narrative = _generate_narrative(layers, flows, intelligence)
        intelligence["narrative"] = narrative
    except Exception as exc:
        log.warning("Narrative generation failed: {e}", e=str(exc))

    # Build levers
    try:
        levers = _build_levers(layers, flows, intelligence)
    except Exception as exc:
        log.warning("Levers build failed: {e}", e=str(exc))
        levers = []

    # Extract global_liquidity and global_policy from the CB layer for
    # top-level access (the most important numbers in the system).
    cb_layer = next((l for l in layers if l["id"] == "central_banks"), {})
    global_liquidity = cb_layer.get("global_liquidity", {})
    global_policy = cb_layer.get("global_policy", {})
    currency_impacts = cb_layer.get("currency_impacts", [])

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "as_of": as_of.isoformat(),
        "layers": layers,
        "flows": flows,
        "intelligence": intelligence,
        "levers": levers,
        # Top-level aggregates — the numbers that matter most
        "global_liquidity": global_liquidity,
        "global_policy": global_policy,
        "currency_impacts": currency_impacts,
    }

    cb_count = len(cb_layer.get("nodes", []))
    log.info(
        "Money flow map built: {nl} layers, {nf} flows, {nlev} levers, "
        "{ncb} central banks, global liq change=${glc}",
        nl=len(layers),
        nf=len(flows),
        nlev=len(levers),
        ncb=cb_count,
        glc=f"{global_liquidity.get('change_1m_usd', 0):,.0f}",
    )

    return result


# ══════════════════════════════════════════════════════════════════════════
# HIERARCHICAL DRILL-DOWN — Sector & Company level
# ══════════════════════════════════════════════════════════════════════════


def get_sector_drill(engine: Engine, sector: str) -> dict:
    """Drill into a sector: subsectors, top companies, actors, and aggregate flows.

    Parameters:
        engine: SQLAlchemy database engine.
        sector: Sector name (e.g. "Technology").

    Returns:
        dict with keys: sector, subsectors (each with companies), actors,
        total_flow, confidence.
    """
    from analysis.sector_map import SECTOR_MAP, get_actor_influence

    log.info("Sector drill-down: {s}", s=sector)

    sector_data = SECTOR_MAP.get(sector)
    if not sector_data:
        # Try case-insensitive match
        for name, data in SECTOR_MAP.items():
            if name.lower() == sector.lower():
                sector = name
                sector_data = data
                break

    if not sector_data:
        return {
            "sector": sector,
            "subsectors": [],
            "actors": [],
            "total_flow": "$0",
            "confidence": "estimated",
        }

    subsectors_out: list[dict] = []
    all_actors: list[dict] = []
    total_flow_est = 0.0
    confidence_sources: list[str] = []

    for sub_name, sub_data in sector_data.get("subsectors", {}).items():
        companies: list[dict] = []
        for actor in sub_data.get("actors", []):
            ticker = actor.get("ticker")
            if not ticker or actor.get("type") != "company":
                continue

            # Fetch live data for this company
            price = _get_price(engine, ticker)
            insider = _get_insider_signal(engine, ticker)
            congress = _get_congressional_signal(engine, ticker)

            # Estimate flow from price movement
            price_change = _get_price_change(engine, ticker, 30)
            flow_est = 0.0
            if price_change is not None:
                flow_est = abs(price_change) * 1e9 * actor.get("weight", 0.1)
                total_flow_est += flow_est

            flow_direction = "inflow"
            if price_change is not None and price_change < 0:
                flow_direction = "outflow"

            confidence_sources.append("confirmed" if insider or congress else "estimated")

            companies.append({
                "ticker": ticker,
                "name": actor["name"],
                "price": price,
                "flow": round(flow_est, 0),
                "flow_direction": flow_direction,
                "insider_signal": insider,
                "congressional_signal": congress,
            })

        # Cap at top 5 companies per subsector, sorted by flow
        companies.sort(key=lambda c: abs(c.get("flow", 0)), reverse=True)
        subsectors_out.append({
            "name": sub_name,
            "weight": sub_data.get("weight", 0),
            "companies": companies[:5],
        })

    # Get actors with influence scores
    actor_list = get_actor_influence(sector)
    for actor_info in actor_list[:15]:
        recent_action = None
        amount = 0.0
        trust = 0.5

        # Try to pull recent signal for actors with tickers
        ticker = actor_info.get("ticker")
        if ticker:
            insider = _get_insider_signal(engine, ticker)
            congress = _get_congressional_signal(engine, ticker)
            if insider:
                recent_action = f"insider:{insider}"
            elif congress:
                recent_action = f"congressional:{congress}"

        all_actors.append({
            "name": actor_info["name"],
            "role": actor_info.get("type", "unknown"),
            "recent_action": recent_action,
            "amount": amount,
            "trust_score": trust,
            "influence": actor_info.get("influence", 0),
        })

    # Format total flow
    abs_flow = abs(total_flow_est)
    if abs_flow >= 1e12:
        total_flow_str = f"${abs_flow / 1e12:.1f}T"
    elif abs_flow >= 1e9:
        total_flow_str = f"${abs_flow / 1e9:.1f}B"
    elif abs_flow >= 1e6:
        total_flow_str = f"${abs_flow / 1e6:.1f}M"
    else:
        total_flow_str = f"${abs_flow:,.0f}"

    # Determine overall confidence
    confirmed = sum(1 for c in confidence_sources if c == "confirmed")
    if confirmed > len(confidence_sources) * 0.5:
        confidence = "confirmed"
    elif confirmed > 0:
        confidence = "mixed"
    else:
        confidence = "estimated"

    return {
        "sector": sector,
        "subsectors": subsectors_out,
        "actors": all_actors,
        "total_flow": total_flow_str,
        "total_flow_raw": round(total_flow_est, 0),
        "confidence": confidence,
    }


def get_company_drill(engine: Engine, ticker: str) -> dict:
    """Drill into a company: power players, their actions, and connections.

    Combines actor_network context with company_analyzer profile data
    to produce a full picture of who is involved with this company.

    Parameters:
        engine: SQLAlchemy database engine.
        ticker: Stock ticker symbol (e.g. "NVDA").

    Returns:
        dict with keys: ticker, name, price, actors (list of power players),
        insider_summary, congressional_summary, confidence.
    """
    ticker = ticker.strip().upper()
    log.info("Company drill-down: {t}", t=ticker)

    price = _get_price(engine, ticker)
    actors_out: list[dict] = []
    insider_summary = None
    congressional_summary = None

    # Pull actor context from actor_network
    try:
        from intelligence.actor_network import get_actor_context_for_ticker
        ctx = get_actor_context_for_ticker(engine, ticker)

        for action in ctx.get("recent_actions", []):
            actors_out.append({
                "name": action.get("source_id", action.get("actor", "Unknown")),
                "role": action.get("source_type", "unknown"),
                "recent_action": action.get("direction", "unknown"),
                "amount": float(action.get("amount", 0) or 0),
                "trust_score": round(float(action.get("trust_score", 0.5) or 0.5), 2),
                "date": action.get("date", ""),
                "confidence": "confirmed" if float(action.get("trust_score", 0) or 0) >= 0.7 else "estimated",
            })

        insider_summary = ctx.get("power_summary", "")
    except Exception as exc:
        log.debug("Actor context for {t} failed: {e}", t=ticker, e=str(exc))

    # Enrich with company_analyzer profile data
    try:
        from intelligence.company_analyzer import get_all_profiles
        profiles = get_all_profiles(engine)
        for p in profiles:
            if p.ticker == ticker:
                # Add congressional holders as actors
                for holder in p.congress_holders:
                    member_name = holder.get("member", "Unknown")
                    # Skip if already in actors list
                    if any(a["name"] == member_name for a in actors_out):
                        continue
                    actors_out.append({
                        "name": member_name,
                        "role": "congressional",
                        "recent_action": "holds",
                        "amount": float(holder.get("shares_est", 0) or 0),
                        "trust_score": 0.6,
                        "date": "",
                        "confidence": "confirmed",
                        "committee": holder.get("committee", ""),
                    })

                congressional_summary = (
                    f"{len(p.congress_holders)} members, "
                    f"{p.committee_overlap_count} committee overlaps"
                )

                # Add insider direction as meta
                insider_summary = (
                    f"{p.insider_net_direction}, "
                    f"${p.insider_total_value_90d:,.0f} (90d), "
                    f"{p.cluster_signals} cluster signals"
                )
                break
    except Exception as exc:
        log.debug("Company profile for {t} failed: {e}", t=ticker, e=str(exc))

    # Deduplicate and sort actors by trust score descending
    seen_names: set[str] = set()
    unique_actors: list[dict] = []
    for a in actors_out:
        key = a["name"].lower()
        if key not in seen_names:
            seen_names.add(key)
            unique_actors.append(a)
    unique_actors.sort(key=lambda a: a.get("trust_score", 0), reverse=True)

    # Company name lookup
    try:
        from intelligence.company_analyzer import _TICKER_NAMES
        name = _TICKER_NAMES.get(ticker, ticker)
    except Exception:
        name = ticker

    return {
        "ticker": ticker,
        "name": name,
        "price": price,
        "actors": unique_actors[:20],
        "actor_count": len(unique_actors),
        "insider_summary": insider_summary,
        "congressional_summary": congressional_summary,
        "confidence": "confirmed" if any(
            a.get("confidence") == "confirmed" for a in unique_actors
        ) else "estimated",
    }
