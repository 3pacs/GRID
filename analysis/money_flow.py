"""
GRID — Global Money Flow Map.

Aggregates all available flow data into a single hierarchical structure
showing how money moves through the global financial system:

    Central Banks  ->  Banking System  ->  Institutional Layer
        ->  Markets  ->  Sectors  ->  Individual Positions

Each layer contains nodes with live metrics and signals. Flows between
layers carry volume, direction, and change data. An LLM-generated
narrative explains the current picture.

Data sources:
  - FRED: Fed balance sheet (WALCL), reverse repo (RRPONTSYD), TGA (WTREGEN),
          bank credit (TOTBKCR), delinquency (DRTSCIS), M2 (M2SL)
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
    """Build the Central Banks layer with Fed data."""
    one_month_ago = as_of - timedelta(days=30)
    one_week_ago = as_of - timedelta(days=7)

    balance_sheet = _get_fred_latest(engine, "WALCL", as_of)
    balance_sheet_1m = _get_fred_value_at(engine, "WALCL", one_month_ago)

    reverse_repo = _get_fred_latest(engine, "RRPONTSYD", as_of)
    reverse_repo_1w = _get_fred_value_at(engine, "RRPONTSYD", one_week_ago)

    tga = _get_fred_latest(engine, "WTREGEN", as_of)

    # Net liquidity = balance sheet - reverse repo - TGA
    net_liq = None
    if balance_sheet is not None:
        net_liq = balance_sheet
        if reverse_repo is not None:
            net_liq -= reverse_repo
        if tga is not None:
            net_liq -= tga

    net_liq_1m = None
    if balance_sheet_1m is not None:
        net_liq_1m = balance_sheet_1m
        rr_1m = _get_fred_value_at(engine, "RRPONTSYD", one_month_ago)
        tga_1m = _get_fred_value_at(engine, "WTREGEN", one_month_ago)
        if rr_1m is not None:
            net_liq_1m -= rr_1m
        if tga_1m is not None:
            net_liq_1m -= tga_1m

    # Determine signal
    signal = "stable"
    if balance_sheet is not None and balance_sheet_1m is not None:
        bs_change = balance_sheet - balance_sheet_1m
        if bs_change < -20:  # WALCL is in millions
            signal = "draining"
        elif bs_change > 20:
            signal = "injecting"

    bs_change_val = None
    if balance_sheet is not None and balance_sheet_1m is not None:
        bs_change_val = round(balance_sheet - balance_sheet_1m, 0)

    rr_change_val = None
    if reverse_repo is not None and reverse_repo_1w is not None:
        rr_change_val = round(reverse_repo - reverse_repo_1w, 0)

    nl_change_val = None
    if net_liq is not None and net_liq_1m is not None:
        nl_change_val = round(net_liq - net_liq_1m, 0)

    return {
        "id": "central_banks",
        "label": "Central Banks",
        "nodes": [
            {
                "id": "fed",
                "label": "Federal Reserve",
                "metrics": {
                    "balance_sheet": balance_sheet,
                    "balance_sheet_change_1m": bs_change_val,
                    "reverse_repo": reverse_repo,
                    "reverse_repo_change_1w": rr_change_val,
                    "tga_balance": tga,
                    "net_liquidity": net_liq,
                    "net_liquidity_change_1m": nl_change_val,
                    "signal": signal,
                },
            }
        ],
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

    # Central bank -> Markets flows
    fed = node_map.get("fed", {})
    fed_metrics = fed.get("metrics", {})
    fed_signal = fed_metrics.get("signal", "stable")
    nl_change = fed_metrics.get("net_liquidity_change_1m")

    if nl_change is not None:
        abs_vol = abs(nl_change)
        direction = "inflow" if nl_change > 0 else "outflow"

        # Fed liquidity primarily flows to equities and bonds
        flows.append({
            "from": "fed",
            "to": "equities",
            "volume": abs_vol * 0.5,
            "direction": direction,
            "change": _safe_pct_change(abs_vol, abs_vol * 0.9),
            "label": f"Fed liquidity {'injection' if direction == 'inflow' else 'drain'}",
        })
        flows.append({
            "from": "fed",
            "to": "bonds",
            "volume": abs_vol * 0.3,
            "direction": "inflow" if direction == "outflow" else "outflow",
            "change": None,
            "label": "Flight to/from safety",
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

    # Fed signal
    for layer in layers:
        if layer["id"] == "central_banks":
            for node in layer["nodes"]:
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

    # 1. Fed liquidity
    for layer in layers:
        if layer["id"] == "central_banks":
            for node in layer["nodes"]:
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

    result = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "as_of": as_of.isoformat(),
        "layers": layers,
        "flows": flows,
        "intelligence": intelligence,
        "levers": levers,
    }

    log.info(
        "Money flow map built: {nl} layers, {nf} flows, {nlev} levers",
        nl=len(layers),
        nf=len(flows),
        nlev=len(levers),
    )

    return result
