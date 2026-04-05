"""
GRID — Flow Thesis Knowledge Base (data module).

Contains the FLOW_KNOWLEDGE dict, direction constants, and all state
updater functions that fetch live data for each thesis model.
"""

from __future__ import annotations

import json
import math
import types
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ══════════════════════════════════════════════════════════════════════════
# FLOW KNOWLEDGE BASE
# ══════════════════════════════════════════════════════════════════════════

_FLOW_KNOWLEDGE_MUTABLE: dict[str, dict[str, Any]] = {
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
    # ── Extended flow theses (14 additional) ────────────────────────────
    "treasury_issuance": {
        "thesis": (
            "Heavy Treasury issuance absorbs capital from risk assets. "
            "When the Treasury refills the TGA (Treasury General Account), "
            "liquidity drains from markets. Debt ceiling resolutions "
            "trigger supply floods."
        ),
        "mechanism": (
            "Treasury issues debt -> investors buy bonds -> cash moves "
            "from bank reserves to TGA -> less liquidity in system"
        ),
        "key_metric": "tga_change_1m",
        "current_state": None,
        "confidence": "high",
        "source": "confirmed",
    },
    "bank_lending_cycle": {
        "thesis": (
            "Bank credit expansion drives asset prices. When banks tighten "
            "lending standards (Sr. Loan Officer Survey), credit contracts "
            "and risk assets fall 3-6 months later."
        ),
        "mechanism": (
            "Banks loosen standards -> credit expands -> more capital in "
            "system -> asset prices rise. Reverse: tighten -> contract -> fall"
        ),
        "key_metric": "bank_credit_yoy",
        "lead_time_days": 120,
        "current_state": None,
        "confidence": "moderate",
        "source": "confirmed",
    },
    "money_market_stress": {
        "thesis": (
            "Record money market fund inflows signal extreme risk aversion. "
            "Historically, peak MMF assets precede equity bottoms by 2-4 "
            "months as cash eventually redeploys."
        ),
        "mechanism": (
            "Fear -> investors park cash in MMFs -> MMF assets peak -> "
            "fear subsides -> cash redeploys into equities -> bottom forms"
        ),
        "key_metric": "mmf_assets",
        "lead_time_days": 75,
        "current_state": None,
        "confidence": "moderate",
        "source": "estimated",
    },
    "pension_rebalancing": {
        "thesis": (
            "Quarter-end pension rebalancing creates predictable flows. "
            "Pensions sell equities that outperformed and buy bonds/ "
            "underperformers. Creates 1-3% mean reversion in the last "
            "2 weeks of quarter."
        ),
        "mechanism": (
            "Quarter end approaches -> pensions mark-to-market -> sell "
            "winners, buy losers -> 1-3% mean reversion in final 2 weeks"
        ),
        "key_metric": "quarter_end_proximity",
        "timing": "Last 2 weeks of March, June, September, December",
        "current_state": None,
        "confidence": "moderate",
        "source": "confirmed",
    },
    "sovereign_accumulation": {
        "thesis": (
            "Central bank gold buying is at multi-decade highs. China, "
            "India, Turkey, and Poland are de-dollarizing reserves. This "
            "creates a structural bid under gold regardless of rates."
        ),
        "mechanism": (
            "Central banks diversify away from USD -> buy gold -> "
            "structural demand floor -> gold rises independent of rates"
        ),
        "key_metric": "gold_change_1m",
        "current_state": None,
        "confidence": "moderate",
        "source": "derived",
    },
    "corporate_buyback_cycle": {
        "thesis": (
            "S&P 500 buybacks run ~$200B/quarter but stop during blackout "
            "windows (2 weeks before earnings). ~40% of S&P is in blackout "
            "simultaneously. Absence of the largest buyer = bearish."
        ),
        "mechanism": (
            "Earnings approach -> companies enter blackout -> buybacks "
            "pause -> largest bid disappears -> selling pressure increases"
        ),
        "key_metric": "blackout_pct",
        "current_state": None,
        "confidence": "moderate",
        "source": "derived",
    },
    "margin_debt_leverage": {
        "thesis": (
            "Rising margin debt is complacent leverage. Historically, "
            "margin debt peaks precede market corrections by 1-3 months. "
            "It's a contrarian indicator at extremes."
        ),
        "mechanism": (
            "Confidence rises -> investors lever up via margin -> margin "
            "debt peaks -> minor dip triggers margin calls -> forced "
            "selling -> correction"
        ),
        "key_metric": "margin_debt_level",
        "lead_time_days": 60,
        "current_state": None,
        "confidence": "low",
        "source": "estimated",
    },
    "stablecoin_flows": {
        "thesis": (
            "Rising stablecoin supply (USDT + USDC) is crypto's QE. "
            "New stablecoin issuance = new capital entering crypto "
            "ecosystem. Declining supply = capital exiting."
        ),
        "mechanism": (
            "Fiat deposited -> stablecoins minted -> deployed into "
            "crypto markets -> asset prices rise. Reverse: redemptions "
            "-> supply shrinks -> prices fall"
        ),
        "key_metric": "stablecoin_total",
        "current_state": None,
        "confidence": "low",
        "source": "estimated",
    },
    "fx_carry_trade": {
        "thesis": (
            "Rate differentials drive FX flows. US rates above EU/Japan "
            "rates attract capital into USD, strengthening dollar and "
            "pressuring EM/commodities. Carry unwind is violent when "
            "differentials narrow."
        ),
        "mechanism": (
            "US rates higher -> borrow cheap currency, buy USD assets "
            "-> USD strengthens -> EM/commodities pressured. Differential "
            "narrows -> unwind -> USD weakens violently"
        ),
        "key_metric": "rate_differential_us_eu",
        "current_state": None,
        "confidence": "moderate",
        "source": "confirmed",
    },
    "insurance_float": {
        "thesis": (
            "Insurance companies invest ~$7T in float. Rising long-term "
            "yields increase float returns, making insurers more "
            "profitable and willing to deploy capital into riskier assets."
        ),
        "mechanism": (
            "Long yields rise -> insurance float earns more -> improved "
            "profitability -> insurers allocate more to risk assets"
        ),
        "key_metric": "treasury_30y_yield",
        "current_state": None,
        "confidence": "low",
        "source": "derived",
    },
    "private_credit_cycle": {
        "thesis": (
            "Private credit ($1.5T+ AUM) is the shadow banking system. "
            "When public markets are volatile, PE/VC dry powder deploys "
            "into private credit instead. This is invisible to public "
            "markets but absorbs capital."
        ),
        "mechanism": (
            "Public volatility rises -> allocators shift to private "
            "credit -> capital leaves public equities -> invisible drag "
            "on public market liquidity"
        ),
        "key_metric": "vix_level",
        "current_state": None,
        "confidence": "low",
        "source": "estimated",
    },
    "trade_balance_flows": {
        "thesis": (
            "US trade deficit = capital surplus. Every dollar of trade "
            "deficit must be recycled back as foreign investment. "
            "Widening deficit means more foreign capital flowing into "
            "US assets."
        ),
        "mechanism": (
            "US imports > exports -> dollars flow abroad -> foreigners "
            "recycle dollars into US Treasuries/equities -> capital "
            "inflow supports asset prices"
        ),
        "key_metric": "trade_balance",
        "current_state": None,
        "confidence": "moderate",
        "source": "confirmed",
    },
    "commodity_supercycle": {
        "thesis": (
            "When copper, oil, and shipping (BDI) all rise simultaneously, "
            "it signals genuine global demand acceleration — not just "
            "financialization."
        ),
        "mechanism": (
            "Global demand accelerates -> copper (construction/electronics) "
            "+ oil (transport/energy) + shipping (trade volume) all rise "
            "-> confirms real economic expansion"
        ),
        "key_metric": "commodity_breadth",
        "current_state": None,
        "confidence": "moderate",
        "source": "derived",
    },
    "fiscal_multiplier": {
        "thesis": (
            "Government deficit spending injects demand. When fiscal "
            "deficit is large AND GDP is growing, the multiplier is "
            "working — government spending creates more than $1 of "
            "economic activity per $1 spent."
        ),
        "mechanism": (
            "Government spends -> businesses receive revenue -> hire/ "
            "invest -> multiplier effect -> GDP grows faster than debt"
        ),
        "key_metric": "fiscal_impulse",
        "current_state": None,
        "confidence": "low",
        "source": "estimated",
    },
}

# Freeze the knowledge base to prevent accidental runtime mutation.
# Consumers must use {**FLOW_KNOWLEDGE, ...} or update_current_states() for copies.
FLOW_KNOWLEDGE: types.MappingProxyType[str, dict[str, Any]] = types.MappingProxyType(
    _FLOW_KNOWLEDGE_MUTABLE
)


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
                pcr_label = "bullish" if pcr < 0.7 else "bearish" if pcr > 1.3 else "neutral"
                return {
                    "direction": direction,
                    "value": pcr,
                    "detail": f"SPY PCR: {pcr:.2f} ({pcr_label})",
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


# ── Extended state updaters (14 additional) ─────────────────────────────


def _get_treasury_issuance_state(engine: Engine) -> dict[str, Any]:
    """TGA change over 30 days — rising TGA drains liquidity."""
    try:
        with engine.connect() as conn:
            tga_now = conn.execute(text("""
                SELECT value, obs_date FROM raw_series
                WHERE series_id = 'WTREGEN' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            if not tga_now:
                return {"direction": NEUTRAL, "value": None, "detail": "No TGA data"}

            tga_30 = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'WTREGEN' AND pull_status = 'SUCCESS'
                AND obs_date <= CURRENT_DATE - 30
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            current = float(tga_now[0])
            if tga_30:
                prior = float(tga_30[0])
                change = current - prior
                # Rising TGA = draining liquidity = bearish
                if change > 50:
                    direction = BEARISH
                elif change < -50:
                    direction = BULLISH
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": round(current, 0),
                    "change_30d": round(change, 0),
                    "detail": f"TGA ${current:,.0f}M, 30d chg {change:+,.0f}M",
                }
            return {
                "direction": NEUTRAL,
                "value": round(current, 0),
                "detail": f"TGA ${current:,.0f}M (no 30d comparison)",
            }
    except Exception as exc:
        log.debug("Treasury issuance state failed: {e}", e=str(exc))
        return {"direction": NEUTRAL, "value": None, "detail": f"Error: {exc}"}


def _get_bank_lending_cycle_state(engine: Engine) -> dict[str, Any]:
    """Bank credit YoY change — positive growth = bullish."""
    try:
        with engine.connect() as conn:
            now_row = conn.execute(text("""
                SELECT value, obs_date FROM raw_series
                WHERE series_id IN ('TOTBKCR', 'H8B1023NCBCMG')
                AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            if not now_row:
                return {"direction": NEUTRAL, "value": None, "detail": "No bank credit data"}

            yoy_row = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id IN ('TOTBKCR', 'H8B1023NCBCMG')
                AND pull_status = 'SUCCESS'
                AND obs_date <= CURRENT_DATE - 365
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            current = float(now_row[0])
            if yoy_row:
                prior = float(yoy_row[0])
                yoy_pct = ((current - prior) / prior) * 100
                if yoy_pct > 2:
                    direction = BULLISH
                elif yoy_pct < -1:
                    direction = BEARISH
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": round(yoy_pct, 2),
                    "detail": f"Bank credit YoY {yoy_pct:+.1f}%",
                }
            return {
                "direction": NEUTRAL,
                "value": current,
                "detail": "Bank credit data (no YoY comparison)",
            }
    except Exception as exc:
        log.debug("Bank lending cycle state failed: {e}", e=str(exc))
        return {"direction": NEUTRAL, "value": None, "detail": f"Error: {exc}"}


def _get_money_market_stress_state(engine: Engine) -> dict[str, Any]:
    """MMF assets — estimated, no live data yet."""
    return {
        "direction": NEUTRAL,
        "value": None,
        "detail": "Estimated — no live MMF data feed configured",
    }


def _get_pension_rebalancing_state(engine: Engine) -> dict[str, Any]:
    """Quarter-end proximity and equity YTD performance."""
    today = date.today()
    month = today.month
    # Quarter-end months and their last days
    qe_targets = {3: 31, 6: 30, 9: 30, 12: 31}
    # Find nearest quarter end
    qe_month = None
    for m in sorted(qe_targets.keys()):
        if month <= m:
            qe_month = m
            break
    if qe_month is None:
        qe_month = 3  # Next year's Q1
    qe_day = qe_targets[qe_month]
    qe_year = today.year if qe_month >= month else today.year + 1
    qe_date = date(qe_year, qe_month, qe_day)
    days_to_qe = (qe_date - today).days

    if days_to_qe > 14:
        return {
            "direction": NEUTRAL,
            "value": days_to_qe,
            "detail": f"{days_to_qe}d to quarter end — outside rebalancing window",
        }

    # Within 14 days — check equity YTD to estimate direction
    try:
        with engine.connect() as conn:
            spy_now = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'YF:SPY:close' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            spy_jan = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'YF:SPY:close' AND pull_status = 'SUCCESS'
                AND obs_date >= :jan1
                ORDER BY obs_date ASC LIMIT 1
            """), {"jan1": date(today.year, 1, 1)}).fetchone()

            if spy_now and spy_jan:
                ytd_pct = (float(spy_now[0]) - float(spy_jan[0])) / float(spy_jan[0]) * 100
                # Equities up YTD -> pensions sell equities -> bearish for equities
                if ytd_pct > 5:
                    direction = BEARISH
                elif ytd_pct < -5:
                    direction = BULLISH
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": days_to_qe,
                    "detail": f"Active rebalancing ({days_to_qe}d to QE), SPY YTD {ytd_pct:+.1f}%",
                    "components": {"days_to_qe": days_to_qe, "spy_ytd_pct": round(ytd_pct, 2)},
                }
    except Exception as exc:
        log.debug("Pension rebalancing SPY lookup failed: {e}", e=str(exc))

    return {
        "direction": NEUTRAL,
        "value": days_to_qe,
        "detail": f"Active rebalancing ({days_to_qe}d to QE), no YTD data",
    }


def _get_sovereign_accumulation_state(engine: Engine) -> dict[str, Any]:
    """Gold price 30d change as proxy for central bank accumulation."""
    try:
        with engine.connect() as conn:
            gold_now = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'YF:GLD:close' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            gold_30 = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'YF:GLD:close' AND pull_status = 'SUCCESS'
                AND obs_date <= CURRENT_DATE - 30
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            if gold_now and gold_30:
                current = float(gold_now[0])
                prior = float(gold_30[0])
                change_pct = ((current - prior) / prior) * 100
                if change_pct > 2:
                    direction = BULLISH
                elif change_pct < -2:
                    direction = BEARISH
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": round(current, 2),
                    "change_30d_pct": round(change_pct, 2),
                    "detail": f"Gold ${current:.0f}, 30d chg {change_pct:+.1f}%",
                }
            if gold_now:
                return {
                    "direction": NEUTRAL,
                    "value": float(gold_now[0]),
                    "detail": f"Gold ${float(gold_now[0]):.0f} (no 30d comparison)",
                }
    except Exception as exc:
        log.debug("Sovereign accumulation state failed: {e}", e=str(exc))
        return {"direction": NEUTRAL, "value": None, "detail": f"Error: {exc}"}
    return {"direction": NEUTRAL, "value": None, "detail": "No gold data"}


def _get_corporate_buyback_cycle_state(engine: Engine) -> dict[str, Any]:
    """Estimate buyback blackout percentage based on calendar."""
    today = date.today()
    month = today.month
    day = today.day

    # Peak blackout: early Jan, Apr, Jul, Oct (2 weeks before earnings season)
    # Earnings seasons start ~mid-Jan, mid-Apr, mid-Jul, mid-Oct
    blackout_pct = 10  # default: open window
    if month in (1, 4, 7, 10):
        if day <= 20:
            blackout_pct = 40  # peak blackout
        else:
            blackout_pct = 20  # tapering
    elif month in (3, 6, 9, 12) and day >= 20:
        blackout_pct = 25  # early blackout starts

    if blackout_pct >= 30:
        direction = BEARISH
    elif blackout_pct <= 15:
        direction = BULLISH
    else:
        direction = NEUTRAL

    return {
        "direction": direction,
        "value": blackout_pct,
        "detail": f"~{blackout_pct}% of S&P in buyback blackout",
    }


def _get_margin_debt_leverage_state(engine: Engine) -> dict[str, Any]:
    """Margin debt — estimated, requires separate FINRA data puller."""
    return {
        "direction": NEUTRAL,
        "value": None,
        "detail": "Estimated — FINRA margin data not yet configured",
    }


def _get_stablecoin_flows_state(engine: Engine) -> dict[str, Any]:
    """Stablecoin supply — estimated, no live feed yet."""
    return {
        "direction": NEUTRAL,
        "value": None,
        "detail": "Estimated — no live stablecoin supply feed configured",
    }


def _get_fx_carry_trade_state(engine: Engine) -> dict[str, Any]:
    """Rate differential proxy via fed funds and EUR/USD movement."""
    try:
        with engine.connect() as conn:
            ff_row = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'DFF' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            eurusd_now = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'DEXUSEU' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            eurusd_30 = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'DEXUSEU' AND pull_status = 'SUCCESS'
                AND obs_date <= CURRENT_DATE - 30
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            if ff_row and eurusd_now and eurusd_30:
                ff_rate = float(ff_row[0])
                eur_now = float(eurusd_now[0])
                eur_prior = float(eurusd_30[0])
                eur_chg = ((eur_now - eur_prior) / eur_prior) * 100
                # Falling EUR/USD = rising dollar = carry flowing in
                if eur_chg < -1:
                    direction = BULLISH  # USD strengthening, carry inflows
                elif eur_chg > 1:
                    direction = BEARISH  # USD weakening, carry unwinding
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": round(ff_rate, 2),
                    "detail": f"Fed funds {ff_rate:.2f}%, EUR/USD 30d chg {eur_chg:+.1f}%",
                    "components": {
                        "fed_funds": ff_rate,
                        "eurusd": round(eur_now, 4),
                        "eurusd_30d_chg_pct": round(eur_chg, 2),
                    },
                }
            if ff_row:
                return {
                    "direction": NEUTRAL,
                    "value": float(ff_row[0]),
                    "detail": f"Fed funds {float(ff_row[0]):.2f}% (no FX data)",
                }
    except Exception as exc:
        log.debug("FX carry trade state failed: {e}", e=str(exc))
        return {"direction": NEUTRAL, "value": None, "detail": f"Error: {exc}"}
    return {"direction": NEUTRAL, "value": None, "detail": "No rate data"}


def _get_insurance_float_state(engine: Engine) -> dict[str, Any]:
    """30-year Treasury yield — rising yields bullish for insurance deployment."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT value, obs_date FROM raw_series
                WHERE series_id = 'DGS30' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            prior = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'DGS30' AND pull_status = 'SUCCESS'
                AND obs_date <= CURRENT_DATE - 30
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            if row:
                current = float(row[0])
                if prior:
                    prev = float(prior[0])
                    change_bp = (current - prev) * 100
                    if change_bp > 10:
                        direction = BULLISH
                    elif change_bp < -10:
                        direction = BEARISH
                    else:
                        direction = NEUTRAL
                    return {
                        "direction": direction,
                        "value": round(current, 3),
                        "detail": f"30Y yield {current:.2f}%, 30d chg {change_bp:+.0f}bp",
                    }
                return {
                    "direction": NEUTRAL,
                    "value": round(current, 3),
                    "detail": f"30Y yield {current:.2f}% (no 30d comparison)",
                }
    except Exception as exc:
        log.debug("Insurance float state failed: {e}", e=str(exc))
        return {"direction": NEUTRAL, "value": None, "detail": f"Error: {exc}"}
    return {"direction": NEUTRAL, "value": None, "detail": "No DGS30 data"}


def _get_private_credit_cycle_state(engine: Engine) -> dict[str, Any]:
    """VIX level as proxy for capital shifting to private credit."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'VIXCLS' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            if row:
                vix = float(row[0])
                if vix > 25:
                    direction = BEARISH  # Capital shifting to private credit
                elif vix < 15:
                    direction = BULLISH  # Low vol, capital stays in public markets
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": round(vix, 2),
                    "detail": f"VIX {vix:.1f} — {'high vol, capital shifting to private credit' if vix > 25 else 'low vol, public markets favored' if vix < 15 else 'moderate vol'}",
                }
    except Exception as exc:
        log.debug("Private credit cycle state failed: {e}", e=str(exc))
        return {"direction": NEUTRAL, "value": None, "detail": f"Error: {exc}"}
    return {"direction": NEUTRAL, "value": None, "detail": "No VIX data"}


def _get_trade_balance_flows_state(engine: Engine) -> dict[str, Any]:
    """US trade balance — more negative = larger deficit = more capital inflows."""
    try:
        with engine.connect() as conn:
            row = conn.execute(text("""
                SELECT value, obs_date FROM raw_series
                WHERE series_id = 'BOPGTB' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            prior = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'BOPGTB' AND pull_status = 'SUCCESS'
                AND obs_date <= CURRENT_DATE - 90
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            if row:
                balance = float(row[0])
                if prior:
                    prev_balance = float(prior[0])
                    # More negative = wider deficit = more inflows
                    if balance < prev_balance - 5:
                        direction = BULLISH  # Wider deficit = more foreign capital
                    elif balance > prev_balance + 5:
                        direction = BEARISH  # Narrowing deficit = less inflow
                    else:
                        direction = NEUTRAL
                    return {
                        "direction": direction,
                        "value": round(balance, 1),
                        "detail": f"Trade balance ${balance:,.0f}B, 3m prior ${prev_balance:,.0f}B",
                    }
                return {
                    "direction": NEUTRAL,
                    "value": round(balance, 1),
                    "detail": f"Trade balance ${balance:,.0f}B (no 3m comparison)",
                }
    except Exception as exc:
        log.debug("Trade balance flows state failed: {e}", e=str(exc))
        return {"direction": NEUTRAL, "value": None, "detail": f"Error: {exc}"}
    return {"direction": NEUTRAL, "value": None, "detail": "No trade balance data"}


def _get_commodity_supercycle_state(engine: Engine) -> dict[str, Any]:
    """Commodity breadth: copper, crude, and optionally BDI all rising."""
    try:
        with engine.connect() as conn:
            positives = 0
            total_checked = 0
            components: dict[str, Any] = {}

            for label, series_ids in [
                ("copper", ["YF:HG=F:close"]),
                ("crude", ["YF:CL=F:close"]),
            ]:
                now_row = None
                prior_row = None
                for sid in series_ids:
                    now_row = conn.execute(text("""
                        SELECT value FROM raw_series
                        WHERE series_id = :sid AND pull_status = 'SUCCESS'
                        ORDER BY obs_date DESC LIMIT 1
                    """), {"sid": sid}).fetchone()
                    if now_row:
                        prior_row = conn.execute(text("""
                            SELECT value FROM raw_series
                            WHERE series_id = :sid AND pull_status = 'SUCCESS'
                            AND obs_date <= CURRENT_DATE - 30
                            ORDER BY obs_date DESC LIMIT 1
                        """), {"sid": sid}).fetchone()
                        break
                if now_row and prior_row:
                    total_checked += 1
                    chg = (float(now_row[0]) - float(prior_row[0])) / float(prior_row[0]) * 100
                    components[label] = round(chg, 1)
                    if chg > 0:
                        positives += 1

            # Try BDI from feature registry / resolved_series
            bdi_row = conn.execute(text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.name LIKE '%bdi%' OR fr.name LIKE '%baltic%'
                ORDER BY rs.obs_date DESC LIMIT 1
            """)).fetchone()
            bdi_prior = conn.execute(text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE (fr.name LIKE '%bdi%' OR fr.name LIKE '%baltic%')
                AND rs.obs_date <= CURRENT_DATE - 30
                ORDER BY rs.obs_date DESC LIMIT 1
            """)).fetchone()
            if bdi_row and bdi_prior:
                total_checked += 1
                bdi_chg = (float(bdi_row[0]) - float(bdi_prior[0])) / float(bdi_prior[0]) * 100
                components["bdi"] = round(bdi_chg, 1)
                if bdi_chg > 0:
                    positives += 1

            if total_checked == 0:
                return {"direction": NEUTRAL, "value": None, "detail": "No commodity data"}

            if positives == total_checked and total_checked >= 2:
                direction = BULLISH
            elif positives == 0:
                direction = BEARISH
            else:
                direction = NEUTRAL

            return {
                "direction": direction,
                "value": positives,
                "detail": f"{positives}/{total_checked} commodities rising (30d)",
                "components": components,
            }
    except Exception as exc:
        log.debug("Commodity supercycle state failed: {e}", e=str(exc))
        return {"direction": NEUTRAL, "value": None, "detail": f"Error: {exc}"}


def _get_fiscal_multiplier_state(engine: Engine) -> dict[str, Any]:
    """Fiscal impulse: debt growth rate + employment growth."""
    try:
        with engine.connect() as conn:
            debt_now = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'WDTOTAL' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            debt_prior = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'WDTOTAL' AND pull_status = 'SUCCESS'
                AND obs_date <= CURRENT_DATE - 90
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            payems_now = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'PAYEMS' AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()
            payems_prior = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = 'PAYEMS' AND pull_status = 'SUCCESS'
                AND obs_date <= CURRENT_DATE - 365
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            if debt_now and debt_prior and payems_now and payems_prior:
                debt_chg = ((float(debt_now[0]) - float(debt_prior[0])) / float(debt_prior[0])) * 100
                payroll_chg = ((float(payems_now[0]) - float(payems_prior[0])) / float(payems_prior[0])) * 100
                # Rapidly rising debt + positive payroll = multiplier working
                if debt_chg > 2 and payroll_chg > 1:
                    direction = BULLISH
                elif payroll_chg < 0:
                    direction = BEARISH
                else:
                    direction = NEUTRAL
                return {
                    "direction": direction,
                    "value": round(debt_chg, 2),
                    "detail": f"Debt 3m chg {debt_chg:+.1f}%, payrolls YoY {payroll_chg:+.1f}%",
                    "components": {
                        "debt_3m_chg_pct": round(debt_chg, 2),
                        "payroll_yoy_chg_pct": round(payroll_chg, 2),
                    },
                }
            return {
                "direction": NEUTRAL,
                "value": None,
                "detail": "Insufficient data for fiscal multiplier",
            }
    except Exception as exc:
        log.debug("Fiscal multiplier state failed: {e}", e=str(exc))
        return {"direction": NEUTRAL, "value": None, "detail": f"Error: {exc}"}


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
    # Extended theses
    "treasury_issuance": _get_treasury_issuance_state,
    "bank_lending_cycle": _get_bank_lending_cycle_state,
    "money_market_stress": _get_money_market_stress_state,
    "pension_rebalancing": _get_pension_rebalancing_state,
    "sovereign_accumulation": _get_sovereign_accumulation_state,
    "corporate_buyback_cycle": _get_corporate_buyback_cycle_state,
    "margin_debt_leverage": _get_margin_debt_leverage_state,
    "stablecoin_flows": _get_stablecoin_flows_state,
    "fx_carry_trade": _get_fx_carry_trade_state,
    "insurance_float": _get_insurance_float_state,
    "private_credit_cycle": _get_private_credit_cycle_state,
    "trade_balance_flows": _get_trade_balance_flows_state,
    "commodity_supercycle": _get_commodity_supercycle_state,
    "fiscal_multiplier": _get_fiscal_multiplier_state,
}
