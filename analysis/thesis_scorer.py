"""
GRID — Granular Thesis Scoring Engine.

Replaces the old weight-accumulation system in flow_thesis.py with
auditable, decomposable scoring.  Every claim traces to a data point,
a threshold, and a reason.

What it scores: SPY direction over the next 5 trading days.
How it scores:  Each model produces a score (-100 to +100) and a
                confidence (0-100%).  The final score is the
                confidence-weighted average.  Every number adds up.

Output contract:
  {
    "score": -23.4,           # weighted average, -100..+100
    "direction": "BEARISH",   # derived from score sign
    "conviction": 23,         # abs(score), 0..100
    "bull_pct": 38.3,         # % of weighted votes bullish
    "bear_pct": 61.7,         # % of weighted votes bearish
    "models": [ ... ],        # per-model breakdown (see ModelVerdict)
    "evaluation_window": "5d",
    "generated_at": "...",
  }

Each model in the breakdown:
  {
    "key": "fed_liquidity",
    "name": "Fed Net Liquidity",
    "score": -40,             # this model's call: -100..+100
    "confidence": 72,         # 0..100, weights this model's vote
    "direction": "bearish",
    "data_point": "-$187B 30d change",
    "threshold": "bearish < -$50B, bullish > +$50B",
    "reasoning": "Net liquidity fell $187B in 30 days ...",
    "weight_in_final": 0.18,  # how much this model contributed
    "historical_accuracy": 0.62,  # win rate from past snapshots
    "data_age_hours": 4.2,    # how fresh the underlying data is
    "status": "active",       # active | stale | broken | no_data
  }
"""

from __future__ import annotations

import json
import math
import time as _time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ───────────────────────────────────────────────────────────

EVALUATION_WINDOW_DAYS = 5
STALE_HOURS = 72  # data older than this gets downweighted
DEFAULT_ACCURACY = 0.50  # start at coin-flip until we have history


# ══════════════════════════════════════════════════════════════════════════
# MODEL VERDICT (immutable output per model)
# ══════════════════════════════════════════════════════════════════════════

def _verdict(
    key: str,
    name: str,
    score: float,
    confidence: float,
    data_point: str,
    threshold: str,
    reasoning: str,
    data_age_hours: float | None = None,
    historical_accuracy: float = DEFAULT_ACCURACY,
    status: str = "active",
) -> dict[str, Any]:
    """Build an immutable model verdict dict.

    score: -100 to +100.  confidence: 0 to 100.
    """
    clamped_score = max(-100, min(100, score))
    clamped_conf = max(0, min(100, confidence))

    if clamped_score > 5:
        direction = "bullish"
    elif clamped_score < -5:
        direction = "bearish"
    else:
        direction = "neutral"

    return {
        "key": key,
        "name": name,
        "score": round(clamped_score, 1),
        "confidence": round(clamped_conf, 1),
        "direction": direction,
        "data_point": data_point,
        "threshold": threshold,
        "reasoning": reasoning,
        "data_age_hours": round(data_age_hours, 1) if data_age_hours is not None else None,
        "historical_accuracy": round(historical_accuracy, 3),
        "status": status,
    }


# ══════════════════════════════════════════════════════════════════════════
# INDIVIDUAL MODEL SCORERS
# ══════════════════════════════════════════════════════════════════════════

def _score_fed_liquidity(engine: Engine, accuracy: float) -> dict:
    """Fed net liquidity: balance sheet minus TGA minus reverse repo.

    Logic: 30-day change in net liquidity.
    - Change > +$100B → strong bullish (+60 to +80)
    - Change > +$50B  → moderate bullish (+30 to +50)
    - Change -$50B to +$50B → neutral (-20 to +20)
    - Change < -$50B  → moderate bearish (-30 to -50)
    - Change < -$100B → strong bearish (-60 to -80)

    Score scales linearly within bands.  Confidence from data freshness
    and historical accuracy.
    """
    try:
        with engine.connect() as conn:
            bs = conn.execute(text(
                "SELECT value, pull_timestamp FROM raw_series "
                "WHERE series_id = 'WALCL' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()
            rr = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'RRPONTSYD' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()
            tga = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'WTREGEN' AND pull_status = 'SUCCESS' "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            if not bs:
                return _verdict("fed_liquidity", "Fed Net Liquidity",
                    0, 0, "no data", "", "No Fed balance sheet data available.",
                    status="no_data", historical_accuracy=accuracy)

            net_liq = float(bs[0]) - (float(rr[0]) if rr else 0) - (float(tga[0]) if tga else 0)
            pull_ts = bs[1] if bs[1] else datetime.now(timezone.utc)
            age_hours = (datetime.now(timezone.utc) - pull_ts.replace(tzinfo=timezone.utc)).total_seconds() / 3600 if hasattr(pull_ts, 'replace') else None

            # 30-day prior
            bs_30 = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'WALCL' AND pull_status = 'SUCCESS' "
                "AND obs_date <= CURRENT_DATE - 30 "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()
            rr_30 = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'RRPONTSYD' AND pull_status = 'SUCCESS' "
                "AND obs_date <= CURRENT_DATE - 30 "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()
            tga_30 = conn.execute(text(
                "SELECT value FROM raw_series "
                "WHERE series_id = 'WTREGEN' AND pull_status = 'SUCCESS' "
                "AND obs_date <= CURRENT_DATE - 30 "
                "ORDER BY obs_date DESC LIMIT 1"
            )).fetchone()

            if not bs_30:
                return _verdict("fed_liquidity", "Fed Net Liquidity",
                    0, 20, f"${net_liq:,.0f}B current",
                    "need 30d history", "Current net liquidity known but no 30-day baseline.",
                    data_age_hours=age_hours, historical_accuracy=accuracy, status="stale")

            net_liq_30 = float(bs_30[0]) - (float(rr_30[0]) if rr_30 else 0) - (float(tga_30[0]) if tga_30 else 0)
            change = net_liq - net_liq_30

            # Score: linear scale within bands
            if change >= 100:
                score = 60 + min(20, (change - 100) / 50 * 20)
            elif change >= 50:
                score = 30 + (change - 50) / 50 * 30
            elif change >= -50:
                score = change / 50 * 25
            elif change >= -100:
                score = -30 + (change + 50) / 50 * -30
            else:
                score = -60 - min(20, (abs(change) - 100) / 50 * 20)

            # Confidence: base 60%, +20% if data fresh, scale by accuracy
            conf_base = 60
            conf_fresh = 20 if (age_hours and age_hours < 24) else 0
            conf_accuracy = accuracy * 20  # 0-20% from track record
            confidence = min(95, conf_base + conf_fresh + conf_accuracy)

            return _verdict(
                "fed_liquidity", "Fed Net Liquidity",
                score, confidence,
                data_point=f"${change:+,.0f}B 30d change (current ${net_liq:,.0f}B)",
                threshold="bearish < -$50B, bullish > +$50B, strong at ±$100B",
                reasoning=(
                    f"Net liquidity {'rose' if change > 0 else 'fell'} "
                    f"${abs(change):,.0f}B over 30 days. "
                    f"{'Expanding liquidity supports risk assets.' if change > 50 else 'Contracting liquidity pressures risk assets.' if change < -50 else 'Liquidity roughly flat — no strong directional signal.'}"
                ),
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Fed liquidity scorer error: {e}", e=str(exc))
        return _verdict("fed_liquidity", "Fed Net Liquidity",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_dealer_gamma(engine: Engine, accuracy: float) -> dict:
    """Dealer gamma via SPY put/call ratio.

    Logic: PCR is the ratio of put volume to call volume.
    - PCR < 0.6  → extreme complacency, contrarian bearish (-30)
    - PCR 0.6-0.8 → healthy bullish sentiment (+20 to +40)
    - PCR 0.8-1.0 → neutral
    - PCR 1.0-1.3 → fear building, contrarian bullish (+20 to +40)
    - PCR > 1.3  → extreme fear, strong contrarian bullish (+50 to +70)

    Note: PCR is CONTRARIAN.  High put buying = bullish setup.
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT put_call_ratio, spot_price, signal_date "
                "FROM options_daily_signals "
                "WHERE ticker = 'SPY' AND put_call_ratio IS NOT NULL "
                "ORDER BY signal_date DESC LIMIT 1"
            )).fetchone()

            if not row or not row[0]:
                return _verdict("dealer_gamma", "Options Sentiment (PCR)",
                    0, 0, "no data", "", "No SPY options data available.",
                    status="no_data", historical_accuracy=accuracy)

            pcr = float(row[0])
            spot = float(row[1]) if row[1] else None
            sig_date = row[2]
            age_hours = (datetime.now(timezone.utc).date() - sig_date).days * 24 if sig_date else None

            # Contrarian scoring
            if pcr < 0.6:
                score = -30  # extreme complacency → bearish
                label = "extreme complacency — contrarian bearish"
            elif pcr < 0.8:
                score = 20 + (0.8 - pcr) / 0.2 * 20
                label = "healthy bullish sentiment"
            elif pcr < 1.0:
                score = (pcr - 0.8) / 0.2 * 10 - 5  # slight range around 0
                label = "neutral sentiment"
            elif pcr < 1.3:
                score = 20 + (pcr - 1.0) / 0.3 * 20
                label = "elevated fear — contrarian bullish"
            else:
                score = 50 + min(20, (pcr - 1.3) / 0.3 * 20)
                label = "extreme fear — strong contrarian bullish"

            confidence = 55 + (accuracy * 20)
            if age_hours and age_hours > 48:
                confidence *= 0.7

            return _verdict(
                "dealer_gamma", "Options Sentiment (PCR)",
                score, confidence,
                data_point=f"SPY PCR = {pcr:.2f}" + (f", spot ${spot:.0f}" if spot else ""),
                threshold="<0.6 contrarian bear, 0.6-0.8 bull, 1.0-1.3 contrarian bull, >1.3 strong contrarian bull",
                reasoning=f"SPY put/call ratio is {pcr:.2f} ({label}). "
                    f"{'High put buying historically precedes reversals upward.' if pcr > 1.0 else 'Low put buying signals complacency risk.' if pcr < 0.7 else 'Sentiment in neutral range.'}",
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Dealer gamma scorer error: {e}", e=str(exc))
        return _verdict("dealer_gamma", "Options Sentiment (PCR)",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_vanna_charm(engine: Engine, accuracy: float) -> dict:
    """Vanna/charm: spot distance from max pain.

    Logic: Options market makers hedge toward max pain as expiry approaches.
    - Spot > max pain by 2%+ → charm pulls DOWN (bearish -30 to -50)
    - Spot > max pain by 1-2% → mild bearish (-10 to -30)
    - Spot within 1% of max pain → pinned/neutral
    - Spot < max pain by 1-2% → mild bullish (+10 to +30)
    - Spot < max pain by 2%+ → charm pulls UP (bullish +30 to +50)

    Confidence increases closer to OpEx (Friday).
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT max_pain, spot_price, signal_date "
                "FROM options_daily_signals "
                "WHERE ticker = 'SPY' AND max_pain IS NOT NULL AND spot_price > 0 "
                "ORDER BY signal_date DESC LIMIT 1"
            )).fetchone()

            if not row:
                return _verdict("vanna_charm", "Vanna/Charm (Max Pain)",
                    0, 0, "no data", "", "No max pain data available.",
                    status="no_data", historical_accuracy=accuracy)

            mp, spot, sig_date = float(row[0]), float(row[1]), row[2]
            gap_pct = (spot - mp) / spot * 100
            age_hours = (datetime.now(timezone.utc).date() - sig_date).days * 24 if sig_date else None

            # Score: linear in gap
            if gap_pct > 2:
                score = -30 - min(20, (gap_pct - 2) * 10)
            elif gap_pct > 1:
                score = -10 - (gap_pct - 1) * 20
            elif gap_pct > -1:
                score = gap_pct * 8  # gentle pull
            elif gap_pct > -2:
                score = 10 + (abs(gap_pct) - 1) * 20
            else:
                score = 30 + min(20, (abs(gap_pct) - 2) * 10)

            # Confidence: higher closer to Friday (OpEx)
            today = date.today()
            days_to_friday = (4 - today.weekday()) % 7
            opex_boost = max(0, 15 - days_to_friday * 3)
            confidence = 40 + opex_boost + (accuracy * 15)
            if age_hours and age_hours > 48:
                confidence *= 0.6

            return _verdict(
                "vanna_charm", "Vanna/Charm (Max Pain)",
                score, confidence,
                data_point=f"Spot ${spot:.0f} vs max pain ${mp:.0f} (gap {gap_pct:+.1f}%)",
                threshold="bearish if spot >1% above max pain, bullish if >1% below",
                reasoning=(
                    f"SPY spot is {abs(gap_pct):.1f}% {'above' if gap_pct > 0 else 'below'} "
                    f"max pain (${mp:.0f}). "
                    f"{'Charm and dealer hedging should pull price down toward max pain.' if gap_pct > 1 else 'Charm should pull price up toward max pain.' if gap_pct < -1 else 'Spot near max pain — dealers are balanced, expect low volatility.'}"
                    + (f" OpEx in {days_to_friday} day{'s' if days_to_friday != 1 else ''} — gravitational pull strengthening." if days_to_friday <= 3 else "")
                ),
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Vanna/charm scorer error: {e}", e=str(exc))
        return _verdict("vanna_charm", "Vanna/Charm (Max Pain)",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_congressional(engine: Engine, accuracy: float) -> dict:
    """Congressional trading signal.

    Logic: Net buy/sell ratio of congressional trades in last 45 days.
    Score scales with the imbalance.  Confidence is lower because of
    disclosure lag (trades reported 30-45 days after execution).
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT signal_type, COUNT(*) as cnt "
                "FROM signal_sources "
                "WHERE source_type = 'congressional' "
                "AND signal_date >= CURRENT_DATE - 45 "
                "GROUP BY signal_type"
            )).fetchall()

            if not rows:
                return _verdict("congressional", "Congressional Trading",
                    0, 0, "no data", "", "No congressional trades in last 45 days.",
                    status="no_data", historical_accuracy=accuracy)

            buys = sum(r[1] for r in rows if r[0] and r[0].upper() in ("BUY", "PURCHASE"))
            sells = sum(r[1] for r in rows if r[0] and r[0].upper() in ("SELL", "SALE", "SALE_FULL", "SALE_PARTIAL"))
            total = buys + sells

            if total == 0:
                return _verdict("congressional", "Congressional Trading",
                    0, 10, "0 trades", "", "No buy/sell trades found.",
                    status="no_data", historical_accuracy=accuracy)

            buy_ratio = buys / total
            # Score: centered at 50/50, scales with imbalance
            # 70% buys → +40, 80% buys → +60, 90%+ → +80
            # 30% buys → -40, 20% buys → -60, 10%- → -80
            imbalance = (buy_ratio - 0.5) * 2  # -1 to +1
            score = imbalance * 80

            # Confidence: low base (lagging data), boost with volume
            conf_base = 30  # inherently lagging
            conf_volume = min(20, total / 5)  # more trades = more signal
            conf_accuracy = accuracy * 15
            confidence = conf_base + conf_volume + conf_accuracy

            return _verdict(
                "congressional", "Congressional Trading",
                score, confidence,
                data_point=f"{buys} buys vs {sells} sells (45d, {total} total)",
                threshold="bearish if sells >60%, bullish if buys >60%",
                reasoning=(
                    f"Congress members made {buys} buys and {sells} sells in 45 days "
                    f"({buy_ratio*100:.0f}% buy rate). "
                    f"{'Net buying suggests insiders see upside.' if buy_ratio > 0.6 else 'Net selling suggests insiders see risk.' if buy_ratio < 0.4 else 'Roughly balanced — no strong insider signal.'} "
                    f"Note: 30-45 day disclosure lag means these trades already happened."
                ),
                data_age_hours=45 * 24,  # inherent lag
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Congressional scorer error: {e}", e=str(exc))
        return _verdict("congressional", "Congressional Trading",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_insider_cluster(engine: Engine, accuracy: float) -> dict:
    """Insider cluster buy/sell detection.

    Logic: 3+ insiders buying the same stock in 14 days = cluster.
    Net cluster direction across all active clusters determines signal.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT ticker, signal_type, COUNT(*) as cnt "
                "FROM signal_sources "
                "WHERE source_type = 'insider' "
                "AND signal_date >= CURRENT_DATE - 14 "
                "GROUP BY ticker, signal_type "
                "HAVING COUNT(*) >= 3 "
                "ORDER BY COUNT(*) DESC"
            )).fetchall()

            if not rows:
                return _verdict("insider_cluster", "Insider Clusters",
                    0, 15, "no active clusters", "",
                    "No insider cluster events (3+ insiders, same stock, 14 days) detected.",
                    status="active", historical_accuracy=accuracy)

            buy_clusters = [r for r in rows if r[1] and r[1].upper() in ("BUY", "PURCHASE")]
            sell_clusters = [r for r in rows if r[1] and r[1].upper() in ("SELL", "SALE", "SALE_FULL")]
            buy_count = len(buy_clusters)
            sell_count = len(sell_clusters)
            total = buy_count + sell_count

            if total == 0:
                return _verdict("insider_cluster", "Insider Clusters",
                    0, 10, "no buy/sell clusters", "",
                    "Clusters detected but no clear buy/sell direction.",
                    status="active", historical_accuracy=accuracy)

            # Score: each net cluster ~ 15 points
            net = buy_count - sell_count
            score = net * 15
            score = max(-80, min(80, score))

            # Build detail string
            buy_tickers = [f"{r[0]}({r[2]})" for r in buy_clusters[:3]]
            sell_tickers = [f"{r[0]}({r[2]})" for r in sell_clusters[:3]]

            confidence = 35 + min(25, total * 5) + (accuracy * 15)

            return _verdict(
                "insider_cluster", "Insider Clusters",
                score, confidence,
                data_point=f"{buy_count} buy clusters, {sell_count} sell clusters",
                threshold="each cluster ≈ ±15 points; 3+ insiders in 14 days = cluster",
                reasoning=(
                    f"{buy_count} buy cluster{'s' if buy_count != 1 else ''}"
                    + (f" ({', '.join(buy_tickers)})" if buy_tickers else "")
                    + f" vs {sell_count} sell cluster{'s' if sell_count != 1 else ''}"
                    + (f" ({', '.join(sell_tickers)})" if sell_tickers else "")
                    + ". "
                    + ("Net insider buying — corporate insiders see value." if net > 0
                       else "Net insider selling — insiders reducing exposure." if net < 0
                       else "Balanced insider activity — no clear signal.")
                ),
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Insider cluster scorer error: {e}", e=str(exc))
        return _verdict("insider_cluster", "Insider Clusters",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_supply_chain(engine: Engine, accuracy: float) -> dict:
    """Supply chain leading indicators (FRED manufacturing/trade data).

    Logic: composite of durable goods orders, industrial production,
    trade balance changes over 3 months. These are SLOW signals
    (90-120 day lead) so confidence is moderate and score is capped.
    """
    # Key FRED series for supply chain health
    _SC_SERIES = [
        "supply_chain.durable_goods_orders",
        "supply_chain.mfg_new_orders",
        "supply_chain.industrial_production",
        "supply_chain.mfg_shipments",
        "supply_chain.capex_orders",
        "supply_chain.trade_balance",
    ]
    try:
        with engine.connect() as conn:
            # Get latest values for each supply chain series from raw_series
            changes: list[float] = []
            latest_date = None
            details: list[str] = []

            for sid in _SC_SERIES:
                recent = conn.execute(text(
                    "SELECT value, obs_date FROM raw_series "
                    "WHERE series_id = :sid "
                    "ORDER BY obs_date DESC LIMIT 1"
                ), {"sid": sid}).fetchone()

                if not recent:
                    continue

                prior = conn.execute(text(
                    "SELECT value FROM raw_series "
                    "WHERE series_id = :sid "
                    "AND obs_date <= CURRENT_DATE - 90 "
                    "ORDER BY obs_date DESC LIMIT 1"
                ), {"sid": sid}).fetchone()

                if not prior or float(prior[0]) == 0:
                    continue

                pct = (float(recent[0]) - float(prior[0])) / abs(float(prior[0])) * 100
                changes.append(pct)
                short_name = sid.split(".")[-1]
                details.append(f"{short_name} {pct:+.1f}%")

                if latest_date is None or recent[1] > latest_date:
                    latest_date = recent[1]

            if not changes:
                return _verdict("supply_chain", "Supply Chain",
                    0, 0, "no data", "", "No supply chain data available.",
                    status="no_data", historical_accuracy=accuracy)

            avg_change = sum(changes) / len(changes)
            age_days = (date.today() - latest_date).days if latest_date else None

            # Score: capped at ±50 (slow signal)
            score = max(-50, min(50, avg_change * 3))

            confidence = 35 + (accuracy * 15) + min(20, len(changes) * 3)
            if age_days and age_days > 30:
                confidence *= 0.7

            direction = "expanding" if avg_change > 2 else "contracting" if avg_change < -2 else "flat"

            return _verdict(
                "supply_chain", "Supply Chain",
                score, confidence,
                data_point=f"{len(changes)} indicators, avg 3m change {avg_change:+.1f}%",
                threshold="bearish if avg <-2%, bullish if >+2%",
                reasoning=(
                    f"Supply chain composite ({len(changes)} FRED series) is {direction} "
                    f"with avg 3-month change of {avg_change:+.1f}%. "
                    f"Components: {', '.join(details)}. "
                    f"Manufacturing/trade data leads GDP by 3-6 months."
                ),
                data_age_hours=(age_days or 0) * 24,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Supply chain scorer error: {e}", e=str(exc))
        return _verdict("supply_chain", "Supply Chain",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_capital_flows(engine: Engine, accuracy: float) -> dict:
    """Net capital flows across all sectors (from dollar_flows).

    Logic: Are institutions putting money IN or TAKING money OUT?
    Uses the flow_aggregator to get net flows by actor tier.
    """
    try:
        from analysis.flow_aggregator import get_full_aggregation
        agg = get_full_aggregation(engine, days=14)
        tiers = agg.get("by_actor_tier", {})

        inst = tiers.get("institutional", {})
        inst_net = inst.get("net_flow", 0)
        inst_dir = inst.get("direction", "neutral")

        indiv = tiers.get("individual", {})
        indiv_net = indiv.get("net_flow", 0)

        total_net = inst_net + indiv_net

        if total_net == 0:
            return _verdict("capital_flows", "Capital Flows (14d)",
                0, 10, "no flow data", "", "No dollar flow data in last 14 days.",
                status="no_data", historical_accuracy=accuracy)

        # Score: institutional flows matter more (3x weight)
        # Normalize by dividing by $1B increments
        inst_signal = inst_net / 1e9 * 3  # $1B inst inflow ≈ +3 points
        indiv_signal = indiv_net / 1e9 * 0.5  # $1B individual ≈ +0.5 points
        score = max(-70, min(70, inst_signal + indiv_signal))

        # Sector breakdown for detail
        sectors = agg.get("by_sector", {})
        top_inflows = sorted(
            [(k, v.get("net_flow", 0)) for k, v in sectors.items() if v.get("net_flow", 0) > 0],
            key=lambda x: -x[1]
        )[:3]
        top_outflows = sorted(
            [(k, v.get("net_flow", 0)) for k, v in sectors.items() if v.get("net_flow", 0) < 0],
            key=lambda x: x[1]
        )[:3]

        inflow_str = ", ".join(f"{s} +${f/1e6:,.0f}M" for s, f in top_inflows) if top_inflows else "none"
        outflow_str = ", ".join(f"{s} -${abs(f)/1e6:,.0f}M" for s, f in top_outflows) if top_outflows else "none"

        confidence = 45 + (accuracy * 15)

        return _verdict(
            "capital_flows", "Capital Flows (14d)",
            score, confidence,
            data_point=f"Institutional: ${inst_net/1e9:+,.1f}B, Individual: ${indiv_net/1e6:+,.0f}M",
            threshold="$1B institutional inflow ≈ +3 points; outflow ≈ -3 points",
            reasoning=(
                f"14-day institutional net flow: ${inst_net/1e9:+,.1f}B ({inst_dir}). "
                f"Individual net: ${indiv_net/1e6:+,.0f}M. "
                f"Top inflows: {inflow_str}. Top outflows: {outflow_str}."
            ),
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("Capital flows scorer error: {e}", e=str(exc))
        return _verdict("capital_flows", "Capital Flows (14d)",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_timesfm_consensus(engine: Engine, accuracy: float) -> dict:
    """TimesFM forward-looking signal consensus.

    Logic: TimesFM forecasts all resolved signals 30 steps ahead.
    If >60% of signals forecast UP, that's bullish.
    If >60% forecast DOWN, that's bearish.
    The expected move magnitude and confidence band width determine conviction.

    This is the ONLY forward-looking model in the scorer — all others are
    backward-looking evidence.  TimesFM converts historical patterns into
    probabilistic directional calls.
    """
    try:
        from inference.timesfm_service import get_forecast_summary
        summary = get_forecast_summary(engine)

        total = summary.get("total_forecasted", 0)
        if total == 0:
            return _verdict("timesfm_consensus", "TimesFM Signal Consensus",
                0, 0, "no forecasts", "", "No TimesFM forecasts available. Run forecast cycle first.",
                status="no_data", historical_accuracy=accuracy)

        consensus = summary.get("consensus", "MIXED")
        up_pct = summary.get("up_pct", 50)
        down_pct = summary.get("down_pct", 50)
        families = summary.get("families", {})

        # Score from direction imbalance
        # 60% UP → +20, 70% → +40, 80% → +60, 90%+ → +80
        imbalance = (up_pct - down_pct) / 100  # -1 to +1
        score = imbalance * 80

        # Boost/penalize based on key families
        equity_fam = families.get("equity", {})
        macro_fam = families.get("macro", {})
        vol_fam = families.get("vol", {})

        # If equity signals strongly directional, boost
        equity_net = equity_fam.get("net_move", 0)
        if abs(equity_net) > 2:
            score += equity_net * 3  # each 1% equity move → 3 points

        # If vol signals UP (rising VIX), dampen bullish / boost bearish
        vol_up = vol_fam.get("UP", 0)
        vol_down = vol_fam.get("DOWN", 0)
        if vol_up > vol_down:
            score -= 10  # rising vol → headwind

        score = max(-90, min(90, score))

        # Confidence: higher with more signals and narrower bands
        dir_counts = summary.get("direction_counts", {})
        avg_band = max(
            dir_counts.get("UP", {}).get("avg_band", 50),
            dir_counts.get("DOWN", {}).get("avg_band", 50),
        )
        # Narrow bands (<10%) → high confidence, wide bands (>50%) → low
        band_conf = max(10, 70 - avg_band)
        volume_conf = min(20, total / 5)
        confidence = band_conf + volume_conf + (accuracy * 15)

        return _verdict(
            "timesfm_consensus", "TimesFM Signal Consensus",
            score, confidence,
            data_point=f"{total} signals: {up_pct:.0f}% UP, {down_pct:.0f}% DOWN",
            threshold=">60% UP = bullish, >60% DOWN = bearish, else mixed",
            reasoning=(
                f"TimesFM forecasts {total} signals 30 days ahead. "
                f"{up_pct:.0f}% forecast UP, {down_pct:.0f}% DOWN. "
                f"Consensus: {consensus}. "
                f"Equity signals avg {equity_net:+.1f}% expected move. "
                + ("Rising volatility signals detected — adds uncertainty. " if vol_up > vol_down else "")
                + "This is the only FORWARD-LOOKING model in the scorer."
            ),
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("TimesFM consensus scorer error: {e}", e=str(exc))
        return _verdict("timesfm_consensus", "TimesFM Signal Consensus",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_trust_convergence(engine: Engine, accuracy: float) -> dict:
    """Multi-source convergence: when 3+ independent sources agree on a ticker.

    This is the highest-conviction signal — multiple actors seeing the same thing.
    """
    try:
        from intelligence.trust_scorer import detect_convergence
        events = detect_convergence(engine)

        if not events:
            return _verdict("trust_convergence", "Signal Convergence",
                0, 15, "no convergence events", "",
                "No multi-source convergence events detected (need 3+ independent sources on same ticker).",
                status="active", historical_accuracy=accuracy)

        buy_events = [e for e in events if e.get("direction", "").upper() == "BUY"]
        sell_events = [e for e in events if e.get("direction", "").upper() == "SELL"]

        net = len(buy_events) - len(sell_events)
        score = net * 20  # each convergence event ≈ 20 points
        score = max(-80, min(80, score))

        # Confidence: convergence events are high-quality by definition
        confidence = 60 + min(20, len(events) * 5) + (accuracy * 10)

        buy_detail = [f"{e.get('ticker', '?')}({e.get('source_count', '?')}src)" for e in buy_events[:3]]
        sell_detail = [f"{e.get('ticker', '?')}({e.get('source_count', '?')}src)" for e in sell_events[:3]]

        return _verdict(
            "trust_convergence", "Signal Convergence",
            score, confidence,
            data_point=f"{len(buy_events)} buy convergences, {len(sell_events)} sell convergences",
            threshold="each convergence ≈ ±20 points; 3+ independent sources required",
            reasoning=(
                f"{len(events)} convergence event{'s' if len(events) != 1 else ''}: "
                + (f"BUY signals on {', '.join(buy_detail)}" if buy_detail else "no buy convergences")
                + "; "
                + (f"SELL signals on {', '.join(sell_detail)}" if sell_detail else "no sell convergences")
                + ". Multi-source agreement is the strongest conviction signal."
            ),
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("Trust convergence scorer error: {e}", e=str(exc))
        return _verdict("trust_convergence", "Signal Convergence",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy)


def _score_regime_changepoints(engine: Engine, accuracy: float) -> dict:
    """Regime changepoint signals from the discovery pipeline.

    Logic: The changepoint detector publishes regime change signals
    (rising/falling/stable) to signal_registry every 12h.  Count
    bullish vs bearish regime transitions in the last 48 hours.

    Score scales with the net direction and average confidence of
    the detected changepoints.
    """
    try:
        with engine.connect() as conn:
            # Guard: table might not exist yet
            try:
                conn.execute(text("SELECT 1 FROM signal_registry LIMIT 0"))
            except Exception:
                return _verdict(
                    "regime_changepoints", "Regime Changepoints",
                    0, 0, "no table", "",
                    "signal_registry table does not exist.",
                    status="no_data", historical_accuracy=accuracy,
                )

            rows = conn.execute(text(
                "SELECT direction, confidence, value, valid_from "
                "FROM signal_registry "
                "WHERE source_module = :src "
                "AND signal_type = :stype "
                "AND valid_from >= NOW() - INTERVAL '48 hours' "
                "ORDER BY valid_from DESC"
            ).bindparams(
                src="discovery.changepoint_detector",
                stype="regime_change",
            )).fetchall()

        if not rows:
            return _verdict(
                "regime_changepoints", "Regime Changepoints",
                0, 0, "no recent signals", "",
                "No regime change signals in last 48 hours.",
                status="no_data", historical_accuracy=accuracy,
            )

        bullish = [r for r in rows if r[0] == "bullish"]
        bearish = [r for r in rows if r[0] == "bearish"]
        total = len(rows)
        bull_count = len(bullish)
        bear_count = len(bearish)

        # Average confidence across all signals (0-1 scale)
        avg_conf = sum(float(r[1]) for r in rows) / total
        # Average magnitude
        avg_mag = sum(abs(float(r[2])) for r in rows) / total

        # Net direction: each signal ~ 12 points, scaled by avg confidence
        net = bull_count - bear_count
        score = net * 12 * avg_conf
        score = max(-80, min(80, score))

        # Confidence: base from signal count and their confidence values
        conf_base = 25
        conf_volume = min(25, total * 5)
        conf_quality = avg_conf * 25  # 0-25 from signal confidence
        conf_accuracy = accuracy * 15
        confidence = min(90, conf_base + conf_volume + conf_quality + conf_accuracy)

        # Data age from most recent signal
        most_recent = rows[0][3]  # valid_from of most recent
        age_hours = None
        if most_recent:
            try:
                ts = most_recent if most_recent.tzinfo else most_recent.replace(tzinfo=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            except Exception as exc:
                log.warning("Failed to compute regime changepoints age: {e}", e=exc)

        return _verdict(
            "regime_changepoints", "Regime Changepoints",
            score, confidence,
            data_point=f"{bull_count} bullish, {bear_count} bearish, {total - bull_count - bear_count} neutral (48h, avg conf {avg_conf:.0%})",
            threshold="each changepoint ~ +/-12 pts scaled by confidence; net direction determines signal",
            reasoning=(
                f"{total} regime changepoints in 48h: "
                f"{bull_count} bullish, {bear_count} bearish. "
                f"Average confidence {avg_conf:.0%}, magnitude {avg_mag:.2f}. "
                + ("Net bullish regime shifts suggest improving conditions. "
                   if net > 0
                   else "Net bearish regime shifts suggest deteriorating conditions. "
                   if net < 0
                   else "Balanced regime shifts — no clear directional signal. ")
                + "Changepoints detect structural breaks in feature time series."
            ),
            data_age_hours=age_hours,
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("Regime changepoints scorer error: {e}", e=str(exc))
        return _verdict(
            "regime_changepoints", "Regime Changepoints",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


# ══════════════════════════════════════════════════════════════════════════
# HISTORICAL ACCURACY LOADER
# ══════════════════════════════════════════════════════════════════════════

_accuracy_cache: dict[str, Any] = {"data": None, "ts": 0.0}
_ACCURACY_CACHE_TTL = 300.0  # seconds (5 min)


def _load_model_accuracies(engine: Engine) -> dict[str, float]:
    """Load per-model win rates from thesis_snapshots.

    Returns {model_key: accuracy} where accuracy is between 0 and 1.
    Falls back to DEFAULT_ACCURACY (0.50) for models with no history.
    """
    now = _time.time()
    if (
        _accuracy_cache["data"] is not None
        and (now - _accuracy_cache["ts"]) < _ACCURACY_CACHE_TTL
    ):
        return _accuracy_cache["data"]

    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT model_states, outcome FROM thesis_snapshots "
                "WHERE outcome IS NOT NULL AND model_states IS NOT NULL "
                "ORDER BY timestamp DESC LIMIT 200"
            )).fetchall()

        if not rows:
            return {}

        # Count hits and misses per model
        hits: dict[str, int] = {}
        total: dict[str, int] = {}

        for row in rows:
            states = row[0] if isinstance(row[0], dict) else json.loads(row[0]) if row[0] else {}
            outcome = row[1]  # correct / wrong / partial

            for model_key, state in states.items():
                model_dir = state.get("direction", "neutral")
                if model_dir == "neutral":
                    continue

                total[model_key] = total.get(model_key, 0) + 1

                if outcome == "correct":
                    # This model agreed with the majority and the majority was right
                    hits[model_key] = hits.get(model_key, 0) + 1
                elif outcome == "partial":
                    hits[model_key] = hits.get(model_key, 0) + 0.5

        accuracies = {}
        for key in total:
            if total[key] >= 5:  # need at least 5 scored snapshots
                accuracies[key] = hits.get(key, 0) / total[key]

        _accuracy_cache["data"] = accuracies
        _accuracy_cache["ts"] = _time.time()
        return accuracies

    except Exception as exc:
        log.debug("Failed to load model accuracies: {e}", e=str(exc))
        return {}


# ══════════════════════════════════════════════════════════════════════════
# MAIN SCORING FUNCTION
# ══════════════════════════════════════════════════════════════════════════

# All model scorers in execution order
def _score_news_sentiment(engine: Engine, accuracy: float) -> dict:
    """News sentiment: aggregate mood from recent headlines.

    Queries news_articles for last 6 hours.  Counts bullish/bearish/neutral
    sentiment labels and surfaces the top headlines so the thesis reflects
    breaking events (wars, crashes, policy shifts).

    Score: +100 = all bullish, -100 = all bearish.  Weighted by recency.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT title, source, sentiment, published_at,
                       confidence, llm_summary
                FROM news_articles
                WHERE published_at >= NOW() - INTERVAL '6 hours'
                ORDER BY published_at DESC
            """)).fetchall()

        if not rows:
            return _verdict(
                "news_sentiment", "News Sentiment",
                0, 10, "no recent articles", "",
                "No news articles in the last 6 hours.",
                status="stale", historical_accuracy=accuracy,
            )

        total = len(rows)
        bull = sum(1 for r in rows if r[2] and r[2].upper() == "BULLISH")
        bear = sum(1 for r in rows if r[2] and r[2].upper() == "BEARISH")
        neutral = total - bull - bear

        # Weighted score: high-confidence articles count more
        weighted_bull = 0.0
        weighted_bear = 0.0
        for r in rows:
            sent = (r[2] or "").upper()
            conf = float(r[4]) if r[4] is not None else 0.5
            if sent == "BULLISH":
                weighted_bull += conf
            elif sent == "BEARISH":
                weighted_bear += conf

        # Score: weighted net sentiment scaled to [-100, +100]
        weight_total = weighted_bull + weighted_bear
        if weight_total > 0:
            net_pct = (weighted_bull - weighted_bear) / weight_total
            score = net_pct * 100
        else:
            score = 0

        # Confidence: volume + skew + quality
        conf_volume = min(40, total * 0.5)
        conf_skew = min(30, abs(bull - bear) * 2)
        conf_accuracy = accuracy * 20
        confidence = min(90, 20 + conf_volume + conf_skew + conf_accuracy)

        # Top stories with LLM impact reasoning (prefer non-neutral, high-conf)
        scored = [
            (r, float(r[4]) if r[4] is not None else 0.5)
            for r in rows if r[2] and r[2].upper() != "NEUTRAL"
        ]
        scored.sort(key=lambda x: -x[1])
        top = [s[0] for s in scored[:5]]
        if not top:
            top = rows[:5]

        headline_parts = []
        for r in top:
            impact = r[5] or r[0][:80]  # llm_summary or title fallback
            headline_parts.append(
                f"[{(r[2] or 'NEUTRAL').upper()}] {r[1]}: {impact[:120]}"
            )
        headlines_text = "\n  ".join(headline_parts)

        # Data age
        most_recent = rows[0][3]
        age_hours = None
        if most_recent:
            try:
                ts = most_recent if most_recent.tzinfo else most_recent.replace(
                    tzinfo=timezone.utc)
                age_hours = (datetime.now(timezone.utc) - ts).total_seconds() / 3600
            except Exception:
                pass

        if bear > bull * 2:
            mood = "strongly negative — risk-off environment"
        elif bear > bull:
            mood = "leaning negative"
        elif bull > bear * 2:
            mood = "strongly positive — risk-on environment"
        elif bull > bear:
            mood = "leaning positive"
        else:
            mood = "mixed — no dominant narrative"

        reasoning = (
            f"{total} articles in last 6h: {bull} bullish, {bear} bearish, "
            f"{neutral} neutral. Overall mood: {mood}.\n"
            f"  Top stories:\n  {headlines_text}"
        )

        return _verdict(
            "news_sentiment", "News Sentiment",
            score, confidence,
            data_point=f"{total} articles: {bull} bullish, {bear} bearish, {neutral} neutral",
            threshold="net sentiment % scaled to [-100, +100]",
            reasoning=reasoning,
            data_age_hours=age_hours,
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("News sentiment scorer error: {e}", e=str(exc))
        return _verdict(
            "news_sentiment", "News Sentiment",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_geopolitical_risk(engine: Engine, accuracy: float) -> dict:
    """Geopolitical risk from news + GDELT + Crucix OSINT signals.

    Scans news_articles for war, sanctions, military, tariff keywords
    in the last 12 hours. High concentration of geopolitical headlines
    signals risk-off environment. Also checks Crucix bridge data and
    GDELT event counts if available.
    """
    try:
        with engine.connect() as conn:
            # News-based geopolitical signal (most reliable)
            geo_keywords = [
                '%war%', '%bomb%', '%strike%', '%attack%', '%military%',
                '%missile%', '%invasion%', '%escalation%', '%sanctions%',
                '%tariff%', '%embargo%', '%nuclear%', '%threat%',
                '%ceasefire%', '%peace deal%', '%treaty%',
            ]
            conditions = " OR ".join(
                f"title ILIKE :k{i}" for i in range(len(geo_keywords))
            )
            params = {f"k{i}": kw for i, kw in enumerate(geo_keywords)}

            geo_count = conn.execute(text(
                f"SELECT COUNT(*) FROM news_articles "
                f"WHERE published_at >= NOW() - INTERVAL '12 hours' "
                f"AND ({conditions})"
            ), params).fetchone()[0]

            total_news = conn.execute(text(
                "SELECT COUNT(*) FROM news_articles "
                "WHERE published_at >= NOW() - INTERVAL '12 hours'"
            )).fetchone()[0]

            # Peace vs conflict ratio
            peace_kw = ['%ceasefire%', '%peace deal%', '%treaty%',
                        '%de-escalation%', '%truce%']
            peace_cond = " OR ".join(
                f"title ILIKE :p{i}" for i in range(len(peace_kw))
            )
            peace_params = {f"p{i}": kw for i, kw in enumerate(peace_kw)}
            peace_count = conn.execute(text(
                f"SELECT COUNT(*) FROM news_articles "
                f"WHERE published_at >= NOW() - INTERVAL '12 hours' "
                f"AND ({peace_cond})"
            ), peace_params).fetchone()[0]

            conflict_count = geo_count - peace_count

            # Crucix OSINT signals (if available)
            crucix_alerts = 0
            try:
                r = conn.execute(text(
                    "SELECT COUNT(*) FROM raw_series "
                    "WHERE series_id LIKE 'crucix:alert%' "
                    "AND obs_date >= CURRENT_DATE - 1"
                )).fetchone()
                crucix_alerts = r[0] if r else 0
            except Exception:
                pass

            # GDELT event count (if available)
            gdelt_events = 0
            try:
                r = conn.execute(text(
                    "SELECT COUNT(*) FROM raw_series "
                    "WHERE series_id LIKE 'GDELT%' "
                    "AND obs_date >= CURRENT_DATE - 1"
                )).fetchone()
                gdelt_events = r[0] if r else 0
            except Exception:
                pass

        if total_news == 0:
            return _verdict(
                "geopolitical_risk", "Geopolitical Risk",
                0, 10, "no recent news", "",
                "No news in the last 12 hours to assess geopolitical risk.",
                status="stale", historical_accuracy=accuracy,
            )

        geo_pct = geo_count / total_news * 100 if total_news > 0 else 0

        # Score: more geopolitical news = more bearish
        # >30% of news is geopolitical = high risk
        # Peace headlines offset conflict
        net_conflict = conflict_count - peace_count * 2
        if geo_pct > 30 and net_conflict > 5:
            score = -80
        elif geo_pct > 20 and net_conflict > 3:
            score = -60
        elif geo_pct > 10 and net_conflict > 0:
            score = -40
        elif peace_count > conflict_count:
            score = 20  # de-escalation is bullish
        elif geo_pct > 5:
            score = -20
        else:
            score = 0

        confidence = min(85, 30 + geo_pct * 1.5 + accuracy * 15)

        reasoning = (
            f"{geo_count}/{total_news} articles ({geo_pct:.0f}%) are geopolitical in last 12h. "
            f"{conflict_count} conflict, {peace_count} peace/de-escalation. "
        )
        if crucix_alerts > 0:
            reasoning += f"Crucix flagged {crucix_alerts} OSINT alerts. "
        if gdelt_events > 0:
            reasoning += f"GDELT logged {gdelt_events} events. "

        if score < -40:
            reasoning += "Elevated geopolitical risk — markets in risk-off mode."
        elif score > 0:
            reasoning += "De-escalation signals — risk appetite recovering."
        else:
            reasoning += "Background geopolitical noise, not yet market-moving."

        return _verdict(
            "geopolitical_risk", "Geopolitical Risk",
            score, confidence,
            data_point=f"{geo_count} geo articles / {total_news} total ({geo_pct:.0f}%)",
            threshold=">30% = high risk (-80), >20% = elevated (-60), >10% = moderate (-40)",
            reasoning=reasoning,
            historical_accuracy=accuracy,
        )
    except Exception as exc:
        log.debug("Geopolitical risk scorer error: {e}", e=str(exc))
        return _verdict(
            "geopolitical_risk", "Geopolitical Risk",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_social_sentiment(engine: Engine, accuracy: float) -> dict:
    """Social media sentiment from signal_feed (Reddit, StockTwits, Google Trends).

    Reads the signal_feed table for social-origin anomalies and
    raw_series for Google Trends / StockTwits data.
    """
    try:
        with engine.connect() as conn:
            # Signal feed social signals
            social = conn.execute(text("""
                SELECT COUNT(*),
                       SUM(CASE WHEN z_score > 1.5 THEN 1 ELSE 0 END),
                       SUM(CASE WHEN z_score < -1.5 THEN 1 ELSE 0 END)
                FROM signal_feed
                WHERE created_at >= NOW() - INTERVAL '24 hours'
                AND (family ILIKE '%social%' OR family ILIKE '%sentiment%'
                     OR signal_type ILIKE '%reddit%' OR signal_type ILIKE '%social%'
                     OR signal_type ILIKE '%trend%' OR signal_type ILIKE '%stocktwits%')
            """)).fetchone()

            total = social[0] or 0
            bull = social[1] or 0  # positive z-score = bullish anomaly
            bear = social[2] or 0  # negative z-score = bearish anomaly

            if total == 0:
                # Fallback: check signal_data for social/sentiment signals
                alt = conn.execute(text("""
                    SELECT COUNT(*) FROM signal_data
                    WHERE created_at >= NOW() - INTERVAL '24 hours'
                    AND (signal_type ILIKE '%social%' OR signal_type ILIKE '%reddit%'
                         OR signal_type ILIKE '%sentiment%' OR signal_type ILIKE '%google%')
                """)).fetchone()
                if alt and alt[0] > 0:
                    total = alt[0]
                    # Can't distinguish bull/bear from signal_data easily
                    return _verdict(
                        "social_sentiment", "Social Sentiment",
                        0, 25,
                        data_point=f"{total} social signals (direction unknown)",
                        threshold="net social sentiment scaled to [-100, +100]",
                        reasoning=f"{total} social media signals detected but direction not resolved. "
                                  "Treat as neutral until signal_feed captures direction.",
                        historical_accuracy=accuracy,
                    )
                return _verdict(
                    "social_sentiment", "Social Sentiment",
                    0, 10, "no social signals", "",
                    "No social media signals in the last 24 hours.",
                    status="stale", historical_accuracy=accuracy,
                )

            net_pct = (bull - bear) / total if total > 0 else 0
            score = net_pct * 80  # cap at ±80 (social is noisy)

            confidence = min(70, 15 + total * 0.5 + accuracy * 15)

            if bull > bear * 2:
                mood = "retail euphoria"
            elif bull > bear:
                mood = "leaning positive"
            elif bear > bull * 2:
                mood = "retail panic"
            elif bear > bull:
                mood = "leaning negative"
            else:
                mood = "mixed chatter"

            reasoning = (
                f"{total} social signals (24h): {bull} bullish, {bear} bearish. "
                f"Retail mood: {mood}. "
                "Social sentiment is a lagging/contrarian indicator — "
                "extreme retail euphoria often precedes corrections."
            )

            return _verdict(
                "social_sentiment", "Social Sentiment",
                score, confidence,
                data_point=f"{total} signals: {bull} bullish, {bear} bearish",
                threshold="net social direction scaled to [-80, +80]",
                reasoning=reasoning,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Social sentiment scorer error: {e}", e=str(exc))
        return _verdict(
            "social_sentiment", "Social Sentiment",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


def _score_crypto_risk(engine: Engine, accuracy: float) -> dict:
    """Crypto market risk signal from CoinGecko/DeFi Llama/Binance data.

    Crypto often leads equity risk-off moves. Bitcoin below 200-day MA
    or stablecoin depegs signal broad risk aversion.
    """
    try:
        with engine.connect() as conn:
            # Bitcoin price from raw_series
            btc = conn.execute(text("""
                SELECT value, obs_date FROM raw_series
                WHERE series_id IN ('coingecko:bitcoin:usd', 'YF:BTC-USD:close',
                                    'binance:BTCUSDT:close', 'CG:bitcoin:usd')
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            btc_30d = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id IN ('coingecko:bitcoin:usd', 'YF:BTC-USD:close',
                                    'binance:BTCUSDT:close', 'CG:bitcoin:usd')
                AND obs_date <= CURRENT_DATE - 30
                ORDER BY obs_date DESC LIMIT 1
            """)).fetchone()

            if not btc:
                return _verdict(
                    "crypto_risk", "Crypto Risk Barometer",
                    0, 10, "no BTC data", "",
                    "No Bitcoin price data available.",
                    status="stale", historical_accuracy=accuracy,
                )

            btc_price = float(btc[0])
            btc_date = btc[1]

            # 30-day change
            change_30d = 0.0
            if btc_30d:
                prior = float(btc_30d[0])
                if prior > 0:
                    change_30d = (btc_price - prior) / prior * 100

            # DeFi TVL from raw_series (if available)
            defi_tvl = None
            try:
                r = conn.execute(text("""
                    SELECT value FROM raw_series
                    WHERE series_id LIKE 'defillama%tvl%'
                    ORDER BY obs_date DESC LIMIT 1
                """)).fetchone()
                if r:
                    defi_tvl = float(r[0])
            except Exception:
                pass

            # Score: BTC down >15% in 30d = bearish risk signal
            if change_30d < -20:
                score = -70
            elif change_30d < -10:
                score = -45
            elif change_30d < -5:
                score = -20
            elif change_30d > 20:
                score = 50
            elif change_30d > 10:
                score = 30
            elif change_30d > 5:
                score = 15
            else:
                score = 0

            confidence = min(75, 30 + abs(change_30d) * 1.5 + accuracy * 15)

            reasoning = (
                f"Bitcoin at ${btc_price:,.0f} ({change_30d:+.1f}% over 30 days). "
            )
            if change_30d < -10:
                reasoning += "Significant crypto drawdown — risk appetite shrinking across all speculative assets. "
            elif change_30d > 15:
                reasoning += "Strong crypto rally — risk-on environment, speculative appetite high. "
            else:
                reasoning += "Crypto range-bound — no strong risk signal. "

            if defi_tvl:
                reasoning += f"DeFi TVL: ${defi_tvl/1e9:.1f}B. "

            # Age
            age_hours = None
            if btc_date:
                from datetime import date as dt_date
                if isinstance(btc_date, dt_date):
                    days = (date.today() - btc_date).days
                    age_hours = days * 24.0

            return _verdict(
                "crypto_risk", "Crypto Risk Barometer",
                score, confidence,
                data_point=f"BTC ${btc_price:,.0f} ({change_30d:+.1f}% 30d)",
                threshold=">-20% = bearish (-70), >+20% = bullish (+50)",
                reasoning=reasoning,
                data_age_hours=age_hours,
                historical_accuracy=accuracy,
            )
    except Exception as exc:
        log.debug("Crypto risk scorer error: {e}", e=str(exc))
        return _verdict(
            "crypto_risk", "Crypto Risk Barometer",
            0, 0, "error", "", f"Scorer error: {exc}",
            status="broken", historical_accuracy=accuracy,
        )


_MODEL_SCORERS = [
    _score_fed_liquidity,
    _score_dealer_gamma,
    _score_vanna_charm,
    _score_congressional,
    _score_insider_cluster,
    _score_supply_chain,
    _score_capital_flows,
    _score_timesfm_consensus,
    _score_trust_convergence,
    _score_regime_changepoints,
    _score_news_sentiment,
    _score_geopolitical_risk,
    _score_social_sentiment,
    _score_crypto_risk,
]


def _get_regime_context(engine: Engine) -> dict[str, Any]:
    """Load current regime state from decision_journal.

    Returns regime name, confidence, and model weight adjustments
    based on which signals matter more in each regime.
    """
    try:
        with engine.connect() as conn:
            row = conn.execute(text(
                "SELECT inferred_state, state_confidence, counterfactual "
                "FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1"
            )).fetchone()

        if not row or not row[0]:
            return {"regime": "UNKNOWN", "confidence": 0, "adjustments": {}}

        regime = str(row[0]).upper()
        conf = float(row[1]) if row[1] else 0

        # Parse stress index
        stress = None
        cf = row[2] or ""
        if "S=" in cf:
            try:
                stress = float(cf.split("S=")[1].split(",")[0])
            except (ValueError, IndexError):
                pass

        # Regime-specific weight adjustments (multiply confidence)
        # In volatile regimes: options/gamma signals matter more
        # In trending regimes: flows/momentum matter more
        # In crisis: everything defensive matters more
        adjustments: dict[str, float] = {}
        if regime in ("CRISIS", "STRESS", "RISK_OFF"):
            adjustments = {
                "dealer_gamma": 1.4, "vanna_charm": 1.4,
                "capital_flows": 1.2, "trust_convergence": 1.3,
                "regime_changepoints": 1.3,
                "supply_chain": 0.7,
            }
        elif regime in ("EXPANSION", "RISK_ON", "GROWTH"):
            adjustments = {
                "capital_flows": 1.3, "insider_cluster": 1.2,
                "fed_liquidity": 1.2, "supply_chain": 1.1,
                "regime_changepoints": 0.8,
                "vanna_charm": 0.8,
            }
        elif regime in ("MEAN_REVERSION", "CONSOLIDATION", "NEUTRAL"):
            adjustments = {
                "vanna_charm": 1.3, "insider_cluster": 1.2,
                "dealer_gamma": 1.1,
            }

        return {
            "regime": regime,
            "confidence": conf,
            "stress_index": stress,
            "adjustments": adjustments,
        }

    except Exception as exc:
        log.debug("Regime context load failed: {e}", e=str(exc))
        return {"regime": "UNKNOWN", "confidence": 0, "adjustments": {}}


def score_thesis(engine: Engine) -> dict[str, Any]:
    """Run all models and produce a unified, decomposable thesis score.

    Returns a dict that any human can read and verify:
    - Every model shows its score, confidence, data point, and reasoning
    - The final score is a confidence-weighted average (the math adds up)
    - Bull/bear percentages show exactly how much goes each way
    - Current regime context influences model weights
    """
    accuracies = _load_model_accuracies(engine)
    regime_ctx = _get_regime_context(engine)

    verdicts: list[dict] = []
    for scorer_fn in _MODEL_SCORERS:
        key = scorer_fn.__name__.replace("_score_", "")
        acc = accuracies.get(key, DEFAULT_ACCURACY)
        verdict = scorer_fn(engine, acc)
        verdicts.append(verdict)

    # ── Apply regime adjustments ───────────────────────────────────────
    adjustments = regime_ctx.get("adjustments", {})
    for v in verdicts:
        adj = adjustments.get(v["key"], 1.0)
        if adj != 1.0 and v["confidence"] > 0:
            v["confidence"] = round(min(95, v["confidence"] * adj), 1)
            v["regime_adjusted"] = True
        else:
            v["regime_adjusted"] = False

    # ── Weighted average ────────────────────────────────────────────────
    # Each model votes with: score × confidence.
    # Final score = sum(score_i × conf_i) / sum(conf_i)
    weighted_sum = 0.0
    conf_sum = 0.0
    bull_weight = 0.0
    bear_weight = 0.0

    for v in verdicts:
        conf = v["confidence"]
        if conf <= 0 or v["status"] in ("broken", "no_data"):
            continue
        weighted_sum += v["score"] * conf
        conf_sum += conf
        if v["score"] > 5:
            bull_weight += conf
        elif v["score"] < -5:
            bear_weight += conf

    if conf_sum > 0:
        final_score = weighted_sum / conf_sum
        bull_pct = round(bull_weight / conf_sum * 100, 1)
        bear_pct = round(bear_weight / conf_sum * 100, 1)
    else:
        final_score = 0
        bull_pct = 0
        bear_pct = 0

    # Assign weight_in_final to each verdict
    for v in verdicts:
        if conf_sum > 0 and v["confidence"] > 0 and v["status"] not in ("broken", "no_data"):
            v["weight_in_final"] = round(v["confidence"] / conf_sum, 3)
        else:
            v["weight_in_final"] = 0.0

    # Direction from score
    if final_score > 10:
        direction = "BULLISH"
    elif final_score < -10:
        direction = "BEARISH"
    else:
        direction = "NEUTRAL"

    conviction = round(min(100, abs(final_score)))
    active_models = sum(1 for v in verdicts if v["status"] == "active")
    broken_models = sum(1 for v in verdicts if v["status"] in ("broken", "no_data"))

    return {
        "score": round(final_score, 1),
        "direction": direction,
        "conviction": conviction,
        "bull_pct": bull_pct,
        "bear_pct": bear_pct,
        "neutral_pct": round(100 - bull_pct - bear_pct, 1),
        "active_models": active_models,
        "broken_models": broken_models,
        "total_models": len(verdicts),
        "regime": regime_ctx.get("regime", "UNKNOWN"),
        "regime_confidence": regime_ctx.get("confidence", 0),
        "stress_index": regime_ctx.get("stress_index"),
        "evaluation_window": f"{EVALUATION_WINDOW_DAYS}d",
        "scoring_method": "confidence-weighted average of model scores (-100 to +100)",
        "models": verdicts,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }


# ══════════════════════════════════════════════════════════════════════════
# SNAPSHOT (persist for accuracy tracking)
# ══════════════════════════════════════════════════════════════════════════

def snapshot_thesis(engine: Engine, thesis: dict[str, Any]) -> int | None:
    """Persist a thesis score to thesis_snapshots for future accuracy evaluation.

    Returns the snapshot ID, or None on failure.
    """
    try:
        model_states = {}
        for m in thesis.get("models", []):
            model_states[m["key"]] = {
                "direction": m["direction"],
                "score": m["score"],
                "confidence": m["confidence"],
                "data_point": m["data_point"],
                "reasoning": m["reasoning"],
                "status": m["status"],
            }

        key_drivers = [
            {"key": m["key"], "name": m["name"], "detail": m["reasoning"]}
            for m in sorted(thesis.get("models", []), key=lambda x: -abs(x["score"]))[:3]
            if m["status"] == "active"
        ]
        risk_factors = [
            {"key": m["key"], "name": m["name"], "detail": m["reasoning"]}
            for m in thesis.get("models", [])
            if m["status"] == "active" and (
                (thesis["direction"] == "BULLISH" and m["direction"] == "bearish")
                or (thesis["direction"] == "BEARISH" and m["direction"] == "bullish")
            )
        ]

        with engine.begin() as conn:
            row = conn.execute(text(
                "INSERT INTO thesis_snapshots "
                "(overall_direction, conviction, key_drivers, risk_factors, "
                "model_states, narrative) "
                "VALUES (:dir, :conv, :kd, :rf, :ms, :narr) "
                "RETURNING id"
            ), {
                "dir": thesis["direction"].lower(),
                "conv": thesis["conviction"] / 100,
                "kd": json.dumps(key_drivers),
                "rf": json.dumps(risk_factors),
                "ms": json.dumps(model_states),
                "narr": _build_narrative(thesis),
            }).fetchone()

        snap_id = row[0] if row else None
        log.info("Thesis snapshot saved: id={id}, dir={d}, conv={c}",
                 id=snap_id, d=thesis["direction"], c=thesis["conviction"])
        return snap_id

    except Exception as exc:
        log.warning("Failed to snapshot thesis: {e}", e=str(exc))
        return None


def _build_narrative(thesis: dict) -> str:
    """Build a plain-English narrative from the scored thesis.

    Output must be understandable by a non-expert.  Lead with a simple
    verdict, explain *why* in conversational language, then append raw
    numbers in a compact detail block.
    """
    direction = thesis["direction"]
    conviction = thesis["conviction"]
    score = thesis["score"]
    bull = thesis["bull_pct"]
    bear = thesis["bear_pct"]

    # ── Plain-English verdict ──────────────────────────────────────────
    if direction == "BULLISH" and conviction >= 40:
        verdict = "Markets look positive — most signals point up."
    elif direction == "BULLISH":
        verdict = "Slight lean toward up, but not much agreement between models."
    elif direction == "BEARISH" and conviction >= 40:
        verdict = "Warning signs are flashing — most signals lean negative."
    elif direction == "BEARISH":
        verdict = "Slight lean toward down, but the picture is mixed."
    else:
        if conviction <= 10:
            verdict = "Markets look flat — nobody's confident either way."
        else:
            verdict = "Mixed signals — no clear direction right now."

    parts = [verdict]

    # ── Why (top 3 drivers in plain language) ──────────────────────────
    active = [m for m in thesis.get("models", []) if m["status"] == "active"]
    top = sorted(active, key=lambda x: -abs(x["score"]))[:3]
    if top:
        parts.append("Here's why:")
        for m in top:
            plain = _simplify_driver(m["name"], m["reasoning"], m["score"])
            parts.append(f"  • {plain}")

    # ── Compact raw data (for power users / LLM context) ───────────────
    parts.append(
        f"\n[Details: {direction}, {conviction}% conviction, score {score:+.1f}. "
        f"Bull {bull}% / Bear {bear}%. "
        f"Window: {thesis['evaluation_window']}. "
        f"{thesis['active_models']}/{thesis['total_models']} models active.]"
    )

    return "\n".join(parts)


# Maps jargon-heavy model names to plain descriptions
_MODEL_PLAIN_NAMES: dict[str, str] = {
    "timesfm": "AI Price Forecasts",
    "timesfm_consensus": "AI Price Forecasts",
    "fed_net_liquidity": "Fed Money Supply",
    "fed_liquidity": "Fed Money Supply",
    "insider_clusters": "Corporate Insider Trading",
    "insider_cluster": "Corporate Insider Trading",
    "options_flow": "Options Market Bets",
    "dealer_gamma": "Dealer Positioning",
    "vanna_charm": "Options Decay & Flows",
    "gex": "Dealer Positioning",
    "news_sentiment": "News Mood",
    "social_sentiment": "Social Media Mood",
    "macro_regime": "Economic Conditions",
    "cross_reference": "Data Cross-Check",
    "trust_scorer": "Source Reliability",
    "trust_convergence": "Source Agreement",
    "regime_changepoints": "Regime Shifts",
    "supply_chain": "Supply Chain Health",
    "capital_flows": "Money Flows",
    "congressional": "Congressional Trading",
    "geopolitical_risk": "Geopolitical Risk",
    "crypto_risk": "Crypto Risk Barometer",
}


def _simplify_driver(name: str, reasoning: str, score: float) -> str:
    """Turn a model driver into plain English."""
    plain_name = _MODEL_PLAIN_NAMES.get(
        name.lower().replace(" ", "_").replace("-", "_"),
        name.replace("_", " ").title(),
    )
    direction = "positive" if score > 0 else "negative"

    # Simplify common jargon patterns in reasoning text
    simple = reasoning
    for old, new in [
        ("Net liquidity rose", "The Fed pumped more money into the system —"),
        ("Net liquidity fell", "The Fed pulled money out of the system —"),
        ("Expanding liquidity supports risk assets", "that usually helps stocks go up"),
        ("Contracting liquidity pressures risk assets", "that usually pushes stocks down"),
        ("buy clusters", "groups of insider buying"),
        ("sell clusters", "groups of insider selling"),
        ("Net insider selling — insiders reducing exposure",
         "insiders are selling more than buying — they're cautious"),
        ("Net insider buying", "insiders are buying — they see opportunity"),
        ("Consensus: MIXED", "no clear agreement"),
        ("Consensus: BULLISH", "leaning positive"),
        ("Consensus: BEARISH", "leaning negative"),
        ("expected move", "predicted change"),
        ("forecast UP", "predict prices will rise"),
        ("forecast DOWN", "predict prices will fall"),
        ("This is the only FORWARD-LOOKING model in the scorer.", ""),
        ("Equity signals avg", "Average stock signal:"),
    ]:
        simple = simple.replace(old, new)

    return f"{plain_name} ({direction}): {simple}"
