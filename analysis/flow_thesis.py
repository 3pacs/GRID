"""
GRID — Flow Thesis Knowledge Base.

Maintains the system's unified understanding of how capital flows drive
markets. Each thesis is a named mental model backed by data: Fed liquidity,
dealer gamma, vanna/charm, institutional rotation, congressional signals,
insider clusters, cross-reference divergences, supply chain leading,
prediction markets, and trust convergence.

Key entry points:
  update_current_states(engine) — fill live data into each thesis
  generate_unified_thesis(engine) — combine all theses into one market view
"""

from __future__ import annotations

import json
import math
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ══════════════════════════════════════════════════════════════════════════
# FLOW KNOWLEDGE BASE
# ══════════════════════════════════════════════════════════════════════════

FLOW_KNOWLEDGE: dict[str, dict[str, Any]] = {
    "fed_liquidity": {
        "thesis": (
            "Net liquidity (balance sheet - TGA - RRP) is the single most "
            "important driver of risk asset prices. When the Fed drains "
            "liquidity, equities fall. When they inject, equities rise. "
            "This relationship has held since 2008."
        ),
        "mechanism": (
            "Fed buys bonds -> bank reserves increase -> banks lend/invest "
            "-> asset prices rise. Reverse: QT -> reserves drain -> less "
            "capital in system -> prices fall."
        ),
        "key_metric": "fed_net_liquidity",
        "correlation_to_spy": 0.87,
        "lead_time_days": 5,
        "current_state": None,
        "confidence": "high",
        "source": "confirmed",
    },
    "dealer_gamma": {
        "thesis": (
            "When dealers are short gamma (negative GEX), they must "
            "delta-hedge in the same direction as the move -- buying "
            "rallies, selling drops -- amplifying volatility. When long "
            "gamma, they do the opposite, dampening moves. GEX determines "
            "the market's 'personality'."
        ),
        "mechanism": (
            "Retail/institutions buy options -> dealers sell -> dealers "
            "delta-hedge -> GEX determines hedge direction -> price impact"
        ),
        "key_metric": "net_gex",
        "current_state": None,
        "confidence": "high",
        "source": "confirmed",
    },
    "vanna_charm": {
        "thesis": (
            "Vanna (delta sensitivity to IV changes) and charm (delta "
            "sensitivity to time) create predictable dealer flows around "
            "OpEx. As time passes, charm forces dealers to adjust delta "
            "-- creating a gravitational pull toward max pain."
        ),
        "mechanism": (
            "Time passes -> options decay -> dealer delta changes -> "
            "forced hedging -> price gravitates to max pain"
        ),
        "lead_time_days": 3,
        "current_state": None,
        "confidence": "high",
        "source": "confirmed",
    },
    "institutional_rotation": {
        "thesis": (
            "Large institutions (BlackRock, Vanguard, pensions) rebalance "
            "quarterly. They sell winners and buy losers to maintain target "
            "allocations. This creates predictable mean-reversion around "
            "quarter-ends."
        ),
        "mechanism": (
            "Quarter end -> rebalancing -> sell outperformers -> buy "
            "underperformers -> mean reversion"
        ),
        "timing": "Last 2 weeks of March, June, September, December",
        "current_state": None,
        "confidence": "moderate",
        "source": "confirmed",
    },
    "congressional_signal": {
        "thesis": (
            "Congressional committee members who trade in sectors they "
            "oversee are likely trading on non-public information. Their "
            "trades have historically outperformed the market by 6% "
            "annually."
        ),
        "mechanism": (
            "Oversight power -> early knowledge of regulation/contracts "
            "-> informed trades -> market moves after public disclosure"
        ),
        "current_state": None,
        "confidence": "moderate",
        "source": "confirmed",
    },
    "insider_cluster": {
        "thesis": (
            "When 3+ corporate insiders buy their own company's stock "
            "within a 2-week window, it signals institutional confidence. "
            "Cluster buys have historically preceded 15%+ moves within "
            "6 months."
        ),
        "mechanism": (
            "Multiple insiders buying simultaneously -> they all see "
            "something -> price hasn't adjusted yet"
        ),
        "current_state": None,
        "confidence": "moderate",
        "source": "confirmed",
    },
    "cross_reference_divergence": {
        "thesis": (
            "When official economic statistics diverge from physical "
            "reality indicators (electricity, shipping, satellite "
            "imagery), someone is misrepresenting. The divergence "
            "eventually corrects -- either the statistics get revised "
            "or the market adjusts."
        ),
        "mechanism": (
            "Misrepresentation -> false market pricing -> physical "
            "reality eventually wins -> correction"
        ),
        "current_state": None,
        "confidence": "high",
        "source": "confirmed",
    },
    "supply_chain_leading": {
        "thesis": (
            "Shipping rates and container volumes lead economic activity "
            "by 3-6 months. A spike in Baltic Dry Index precedes "
            "industrial production increases. A collapse precedes "
            "recession."
        ),
        "mechanism": (
            "Goods ordered -> shipped -> received -> consumed -> "
            "measured in GDP"
        ),
        "lead_time_days": 120,
        "current_state": None,
        "confidence": "moderate",
        "source": "confirmed",
    },
    "prediction_market_signal": {
        "thesis": (
            "Rapid probability shifts (>10% in 24h) on Polymarket/Kalshi "
            "for economic events signal informed positioning before "
            "mainstream awareness."
        ),
        "mechanism": (
            "Informed participants bet -> odds shift -> market reprices "
            "-> we detect early"
        ),
        "current_state": None,
        "confidence": "low",
        "source": "estimated",
    },
    "trust_convergence": {
        "thesis": (
            "When 3+ independent, high-trust signal sources (congressional, "
            "insider, dark pool, social) all point the same direction on "
            "the same ticker, the probability of a significant move "
            "increases dramatically."
        ),
        "mechanism": (
            "Multiple independent informed actors -> all seeing the same "
            "thing -> convergence = high conviction"
        ),
        "current_state": None,
        "confidence": "moderate",
        "source": "derived",
    },
}


# ── Direction constants ──────────────────────────────────────────────────

BULLISH = "bullish"
BEARISH = "bearish"
NEUTRAL = "neutral"


# ══════════════════════════════════════════════════════════════════════════
# CURRENT STATE UPDATERS
# ══════════════════════════════════════════════════════════════════════════

def _get_fed_liquidity_state(engine: Engine) -> dict[str, Any]:
    """Fetch Fed net liquidity and determine direction."""
    try:
        with engine.connect() as conn:
            # Balance sheet
            bs_row = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'WALCL' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            rr_row = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'RRPONTSYD' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            tga_row = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'WTREGEN' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            bs = float(bs_row[0]) if bs_row else None
            rr = float(rr_row[0]) if rr_row else None
            tga = float(tga_row[0]) if tga_row else None

            if bs is None:
                return {"direction": NEUTRAL, "value": None, "detail": "No data"}

            net_liq = bs - (rr or 0) - (tga or 0)

            # Compare to 30 days ago
            bs_30 = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'WALCL' AND pull_status = 'SUCCESS'
                AND obs_date <= CURRENT_DATE - 30
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            if bs_30:
                rr_30 = conn.execute(text("""
                    SELECT value FROM raw_series
                    WHERE series_id = 'RRPONTSYD' AND pull_status = 'SUCCESS'
                    AND obs_date <= CURRENT_DATE - 30
                    ORDER BY obs_date DESC LIMIT 1
                """)).fetchone()
                tga_30 = conn.execute(text("""
                    SELECT value FROM raw_series
                    WHERE series_id = 'WTREGEN' AND pull_status = 'SUCCESS'
                    AND obs_date <= CURRENT_DATE - 30
                    ORDER BY obs_date DESC LIMIT 1
                """)).fetchone()
                net_liq_30 = float(bs_30[0]) - (float(rr_30[0]) if rr_30 else 0) - (float(tga_30[0]) if tga_30 else 0)
                change = net_liq - net_liq_30

                if change > 50:
                    direction = BULLISH
                elif change < -50:
                    direction = BEARISH
                else:
                    direction = NEUTRAL
            else:
                change = None
                direction = NEUTRAL

            return {
                "direction": direction,
                "value": round(net_liq, 0),
                "change_30d": round(change, 0) if change is not None else None,
                "detail": f"Net liq ${net_liq:,.0f}M, chg {change:+,.0f}M" if change else f"Net liq ${net_liq:,.0f}M",
            }
    except Exception as exc:
        log.debug("Fed liquidity state failed: {e}", e=str(exc))
        return {"direction": NEUTRAL, "value": None, "detail": f"Error: {exc}"}


def _get_dealer_gamma_state(engine: Engine) -> dict[str, Any]:
    """Fetch SPY GEX regime from options_daily_signals."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT put_call_ratio, spot_price FROM options_daily_signals
                WHERE ticker = 'SPY'
                ORDER BY signal_date DESC LIMIT 1
            """)).fetchone()
            if row and row[0]:
                pcr = float(row[0])
                if pcr < 0.7:
                    direction = BULLISH
                elif pcr > 1.3:
                    direction = BEARISH
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": regime,
                    "detail": f"SPY GEX regime: {regime}",
                }
    except Exception as exc:
        log.debug("Dealer gamma state failed: {e}", e=str(exc))
    return {"direction": NEUTRAL, "value": None, "detail": "No GEX data"}


def _get_vanna_charm_state(engine: Engine) -> dict[str, Any]:
    """Assess vanna/charm pressure from max pain vs spot."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT max_pain, spot_price, signal_date FROM options_daily_signals
                WHERE ticker = 'SPY' AND max_pain IS NOT NULL AND spot_price > 0
                ORDER BY signal_date DESC LIMIT 1
            """)).fetchone()
            if row:
                mp, spot, sig_date = float(row[0]), float(row[1]), row[2]
                gap_pct = (spot - mp) / spot * 100
                if gap_pct > 1.5:
                    direction = BEARISH  # Spot above max pain, charm pulls down
                elif gap_pct < -1.5:
                    direction = BULLISH  # Spot below max pain, charm pulls up
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": round(gap_pct, 2),
                    "max_pain": mp,
                    "spot": spot,
                    "detail": f"Spot {gap_pct:+.1f}% from max pain ${mp:.0f}",
                }
    except Exception as exc:
        log.debug("Vanna/charm state failed: {e}", e=str(exc))
    return {"direction": NEUTRAL, "value": None, "detail": "No max pain data"}


def _get_institutional_rotation_state(engine: Engine) -> dict[str, Any]:
    """Determine if we're in a quarterly rebalancing window."""
    today = date.today()
    month = today.month
    day = today.day

    # Rebalancing windows: last 2 weeks of quarter-end months
    in_window = month in (3, 6, 9, 12) and day >= 15

    if in_window:
        return {
            "direction": NEUTRAL,  # Mean-reversion = mixed
            "value": True,
            "detail": f"Active rebalancing window ({today.strftime('%b %d')})",
        }
    return {
        "direction": NEUTRAL,
        "value": False,
        "detail": "Outside rebalancing window",
    }


def _get_congressional_signal_state(engine: Engine) -> dict[str, Any]:
    """Aggregate recent congressional trading direction."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT signal_type, COUNT(*) as cnt
                FROM signal_sources
                WHERE source_type = 'congressional'
                AND signal_date >= CURRENT_DATE - 45
                GROUP BY direction
            """)).fetchall()
            if rows:
                buys = sum(r[1] for r in rows if r[0] == "BUY")
                sells = sum(r[1] for r in rows if r[0] == "SELL")
                total = buys + sells
                if buys > sells * 1.5:
                    direction = BULLISH
                elif sells > buys * 1.5:
                    direction = BEARISH
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": {"buys": buys, "sells": sells},
                    "detail": f"{buys} buys vs {sells} sells (45d)",
                }
    except Exception as exc:
        log.debug("Congressional signal state failed: {e}", e=str(exc))
    return {"direction": NEUTRAL, "value": None, "detail": "No congressional data"}


def _get_insider_cluster_state(engine: Engine) -> dict[str, Any]:
    """Check for active insider cluster buy/sell events."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT ticker, signal_type, COUNT(*) as cnt
                FROM signal_sources
                WHERE source_type = 'insider'
                AND signal_date >= CURRENT_DATE - 14
                GROUP BY ticker, signal_type
                HAVING COUNT(*) >= 3
                ORDER BY COUNT(*) DESC
            """)).fetchall()
            if rows:
                clusters = [{"ticker": r[0], "direction": r[1], "count": r[2]} for r in rows]
                buy_clusters = [c for c in clusters if c["direction"] == "BUY"]
                sell_clusters = [c for c in clusters if c["direction"] == "SELL"]
                if len(buy_clusters) > len(sell_clusters):
                    direction = BULLISH
                elif len(sell_clusters) > len(buy_clusters):
                    direction = BEARISH
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": clusters[:5],
                    "detail": f"{len(buy_clusters)} buy clusters, {len(sell_clusters)} sell clusters",
                }
    except Exception as exc:
        log.debug("Insider cluster state failed: {e}", e=str(exc))
    return {"direction": NEUTRAL, "value": None, "detail": "No active clusters"}


def _get_cross_reference_state(engine: Engine) -> dict[str, Any]:
    """Check for recent cross-reference red flags."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT report_data FROM cross_reference_reports
                ORDER BY created_at DESC LIMIT 1
            """)).fetchone()
            if row and row[0]:
                data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                red_flags = data.get("red_flags", [])
                if len(red_flags) >= 3:
                    direction = BEARISH  # Many divergences = risk
                elif len(red_flags) >= 1:
                    direction = NEUTRAL
                else:
                    direction = BULLISH
                return {
                    "direction": direction,
                    "value": len(red_flags),
                    "detail": f"{len(red_flags)} divergences flagged",
                }
    except Exception as exc:
        log.debug("Cross-reference state failed: {e}", e=str(exc))
    return {"direction": NEUTRAL, "value": None, "detail": "No cross-ref data"}


def _get_supply_chain_state(engine: Engine) -> dict[str, Any]:
    """Check supply chain / shipping signals."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT rs.value, rs.obs_date
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name LIKE '%bdi%' OR fr.name LIKE '%baltic%'
                OR fr.name LIKE '%shipping%' OR fr.name LIKE '%container%'
                ORDER BY rs.obs_date DESC LIMIT 1
            """)).fetchone()
            if row:
                val = float(row[0])
                # Get 3-month prior
                prior = conn.execute(text("""
                    SELECT rs.value
                    FROM resolved_series rs
                    JOIN feature_registry fr ON rs.feature_id = fr.id
                    WHERE (fr.name LIKE '%bdi%' OR fr.name LIKE '%baltic%'
                    OR fr.name LIKE '%shipping%' OR fr.name LIKE '%container%')
                    AND rs.obs_date <= CURRENT_DATE - 90
                    ORDER BY rs.obs_date DESC LIMIT 1
                """)).fetchone()
                if prior:
                    change = (val - float(prior[0])) / float(prior[0])
                    if change > 0.1:
                        direction = BULLISH
                    elif change < -0.1:
                        direction = BEARISH
                    else:
                        direction = NEUTRAL
                    return {
                        "direction": direction,
                        "value": round(val, 1),
                        "change_3m": round(change * 100, 1),
                        "detail": f"Supply chain index {val:.0f}, 3m chg {change*100:+.1f}%",
                    }
    except Exception as exc:
        log.debug("Supply chain state failed: {e}", e=str(exc))
    return {"direction": NEUTRAL, "value": None, "detail": "No supply chain data"}


def _get_prediction_market_state(engine: Engine) -> dict[str, Any]:
    """Check for rapid probability shifts on prediction markets."""
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT signal_value, signal_date FROM signal_sources
                WHERE source_type = 'prediction_market'
                AND signal_date >= CURRENT_DATE - 7
                ORDER BY signal_date DESC
                LIMIT 5
            """)).fetchall()
            if rows:
                rapid_shifts = []
                for r in rows:
                    val = r[0] if isinstance(r[0], dict) else json.loads(r[0]) if r[0] else {}
                    shift = val.get("shift_pct", 0)
                    if abs(shift) >= 10:
                        rapid_shifts.append(val)
                if rapid_shifts:
                    # Net direction of rapid shifts
                    avg_shift = sum(s.get("shift_pct", 0) for s in rapid_shifts) / len(rapid_shifts)
                    direction = BULLISH if avg_shift > 0 else BEARISH
                    return {
                        "direction": direction,
                        "value": rapid_shifts,
                        "detail": f"{len(rapid_shifts)} rapid shifts detected",
                    }
    except Exception as exc:
        log.debug("Prediction market state failed: {e}", e=str(exc))
    return {"direction": NEUTRAL, "value": None, "detail": "No prediction market signals"}


def _get_trust_convergence_state(engine: Engine) -> dict[str, Any]:
    """Check for multi-source convergence events."""
    try:
        from intelligence.trust_scorer import detect_convergence
        convergence = detect_convergence(engine)
        if convergence:
            buy_conv = [c for c in convergence if c.get("direction", "").upper() == "BUY"]
            sell_conv = [c for c in convergence if c.get("direction", "").upper() == "SELL"]
            if len(buy_conv) > len(sell_conv):
                direction = BULLISH
            elif len(sell_conv) > len(buy_conv):
                direction = BEARISH
            else:
                direction = NEUTRAL
            top = convergence[:5]
            return {
                "direction": direction,
                "value": top,
                "detail": f"{len(convergence)} convergence events ({len(buy_conv)} buy, {len(sell_conv)} sell)",
            }
    except Exception as exc:
        log.debug("Trust convergence state failed: {e}", e=str(exc))
    return {"direction": NEUTRAL, "value": None, "detail": "No convergence events"}


# ── Updater map ──────────────────────────────────────────────────────────

_STATE_UPDATERS: dict[str, Any] = {
    "fed_liquidity": _get_fed_liquidity_state,
    "dealer_gamma": _get_dealer_gamma_state,
    "vanna_charm": _get_vanna_charm_state,
    "institutional_rotation": _get_institutional_rotation_state,
    "congressional_signal": _get_congressional_signal_state,
    "insider_cluster": _get_insider_cluster_state,
    "cross_reference_divergence": _get_cross_reference_state,
    "supply_chain_leading": _get_supply_chain_state,
    "prediction_market_signal": _get_prediction_market_state,
    "trust_convergence": _get_trust_convergence_state,
}


# ══════════════════════════════════════════════════════════════════════════
# PUBLIC API
# ══════════════════════════════════════════════════════════════════════════

def update_current_states(engine: Engine) -> dict[str, dict]:
    """Fill current_state for every thesis from live data.

    Returns the updated FLOW_KNOWLEDGE dict (with current_state populated).
    """
    for thesis_key, updater in _STATE_UPDATERS.items():
        try:
            state = updater(engine)
            FLOW_KNOWLEDGE[thesis_key]["current_state"] = state
        except Exception as exc:
            log.warning("Thesis state update failed for {k}: {e}", k=thesis_key, e=str(exc))
            FLOW_KNOWLEDGE[thesis_key]["current_state"] = {
                "direction": NEUTRAL,
                "value": None,
                "detail": f"Update error: {exc}",
            }
    return FLOW_KNOWLEDGE


def generate_unified_thesis(engine: Engine) -> dict[str, Any]:
    """Combine all theses into a unified market view.

    Steps:
      1. Update all current states
      2. Score bullish/bearish/neutral across all theses
      3. Weight by confidence level
      4. Identify agreements and contradictions
      5. Produce unified direction, conviction, drivers, risks, narrative

    Returns:
        dict with: overall_direction, conviction, key_drivers, risk_factors,
                   agreements, contradictions, theses, narrative,
                   generated_at
    """
    knowledge = update_current_states(engine)

    # Weight map for confidence levels
    confidence_weights = {"high": 3.0, "moderate": 2.0, "low": 1.0}

    bullish_score = 0.0
    bearish_score = 0.0
    neutral_count = 0
    total_weight = 0.0

    bullish_drivers: list[dict] = []
    bearish_drivers: list[dict] = []
    neutral_drivers: list[dict] = []
    overall_direction = NEUTRAL
    conviction = 0.0

    for key, thesis in knowledge.items():
        state = thesis.get("current_state")
        if not state or state.get("value") is None:
            continue

        weight = confidence_weights.get(thesis.get("confidence", "low"), 1.0)
        total_weight += weight
        direction = state.get("direction", NEUTRAL)

        entry = {
            "key": key,
            "name": key.replace("_", " ").title(),
            "direction": direction,
            "detail": state.get("detail", ""),
            "confidence": thesis.get("confidence", "low"),
            "weight": weight,
        }

        if direction == BULLISH:
            bullish_score += weight
            bullish_drivers.append(entry)
        elif direction == BEARISH:
            bearish_score += weight
            bearish_drivers.append(entry)
        else:
            neutral_count += 1
            neutral_drivers.append(entry)

    # Overall direction
    if total_weight == 0:
        overall_direction = NEUTRAL
        conviction = 0.0
    else:
        net_score = bullish_score - bearish_score
        max_possible = total_weight
        conviction = abs(net_score) / max_possible if max_possible > 0 else 0.0

        if net_score > 0.5:
            overall_direction = BULLISH
        elif net_score < -0.5:
            overall_direction = BEARISH
        else:
            overall_direction = NEUTRAL

    # Conviction as percentage (0-100)
    conviction_pct = min(100, round(conviction * 100))

    # Key drivers: top 3 theses supporting the direction
    if overall_direction == BULLISH:
        key_drivers = sorted(bullish_drivers, key=lambda d: -d["weight"])[:3]
        risk_factors = sorted(bearish_drivers, key=lambda d: -d["weight"])[:3]
    elif overall_direction == BEARISH:
        key_drivers = sorted(bearish_drivers, key=lambda d: -d["weight"])[:3]
        risk_factors = sorted(bullish_drivers, key=lambda d: -d["weight"])[:3]
    else:
        all_active = bullish_drivers + bearish_drivers
        key_drivers = sorted(all_active, key=lambda d: -d["weight"])[:3]
        risk_factors = []

    # Agreement matrix: which theses agree
    agreements: list[dict] = []
    contradictions: list[dict] = []

    active_theses = bullish_drivers + bearish_drivers + neutral_drivers
    direction_groups: dict[str, list] = {BULLISH: [], BEARISH: [], NEUTRAL: []}
    for t in active_theses:
        direction_groups[t["direction"]].append(t["key"])

    for dir_label, members in direction_groups.items():
        if len(members) >= 2:
            agreements.append({
                "direction": dir_label,
                "members": members,
                "count": len(members),
                "conviction": "high" if len(members) >= 3 else "moderate",
            })

    # Contradictions: high-confidence theses pointing opposite directions
    for bull in bullish_drivers:
        for bear in bearish_drivers:
            if bull["confidence"] in ("high", "moderate") and bear["confidence"] in ("high", "moderate"):
                contradictions.append({
                    "bullish": bull["key"],
                    "bearish": bear["key"],
                    "bull_detail": bull["detail"],
                    "bear_detail": bear["detail"],
                })

    # Narrative
    narrative = _build_narrative(
        overall_direction, conviction_pct, key_drivers,
        risk_factors, agreements, contradictions,
        bullish_score, bearish_score, len(active_theses),
    )

    # Build per-thesis output
    theses_output = []
    for key, thesis in knowledge.items():
        state = thesis.get("current_state") or {}
        theses_output.append({
            "key": key,
            "name": key.replace("_", " ").title(),
            "thesis": thesis.get("thesis", ""),
            "mechanism": thesis.get("mechanism", ""),
            "confidence": thesis.get("confidence", "low"),
            "source": thesis.get("source", "estimated"),
            "direction": state.get("direction", NEUTRAL),
            "current_value": state.get("value"),
            "detail": state.get("detail", "No data"),
            "key_metric": thesis.get("key_metric"),
            "lead_time_days": thesis.get("lead_time_days"),
            "correlation_to_spy": thesis.get("correlation_to_spy"),
            "timing": thesis.get("timing"),
        })

    return {
        "overall_direction": overall_direction.upper(),
        "conviction": conviction_pct,
        "bullish_score": round(bullish_score, 1),
        "bearish_score": round(bearish_score, 1),
        "active_theses": len(active_theses),
        "key_drivers": key_drivers,
        "risk_factors": risk_factors,
        "agreements": agreements,
        "contradictions": contradictions,
        "theses": theses_output,
        "narrative": narrative,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


def _build_narrative(
    direction: str,
    conviction: int,
    key_drivers: list[dict],
    risk_factors: list[dict],
    agreements: list[dict],
    contradictions: list[dict],
    bull_score: float,
    bear_score: float,
    total: int,
) -> str:
    """Build a human-readable narrative synthesizing the unified thesis."""
    parts: list[str] = []

    # Direction statement
    dir_label = {"bullish": "BULLISH", "bearish": "BEARISH", "neutral": "NEUTRAL"}.get(direction, "NEUTRAL")
    parts.append(
        f"GRID's unified thesis is {dir_label} with {conviction}% conviction "
        f"({bull_score:.0f} bull vs {bear_score:.0f} bear across {total} active signals)."
    )

    # Key drivers
    if key_drivers:
        driver_strs = [f"{d['name']} ({d['detail']})" for d in key_drivers]
        parts.append(f"Primary drivers: {'; '.join(driver_strs)}.")

    # Convergence
    high_conv = [a for a in agreements if a["conviction"] == "high"]
    if high_conv:
        for a in high_conv:
            member_names = [m.replace("_", " ").title() for m in a["members"]]
            parts.append(
                f"High-conviction convergence: {len(a['members'])} models agree "
                f"{a['direction']} ({', '.join(member_names)})."
            )

    # Contradictions
    if contradictions:
        parts.append(
            f"Warning: {len(contradictions)} model contradiction(s) detected. "
        )
        for c in contradictions[:2]:
            parts.append(
                f"{c['bullish'].replace('_', ' ').title()} says bull "
                f"while {c['bearish'].replace('_', ' ').title()} says bear."
            )

    # Risk factors
    if risk_factors:
        risk_strs = [f"{r['name']} ({r['detail']})" for r in risk_factors]
        parts.append(f"Key risks that could invalidate: {'; '.join(risk_strs)}.")

    return " ".join(parts)
