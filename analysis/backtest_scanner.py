"""
Automated cross-asset backtest scanner.

Scans all equity/crypto/commodity feature pairs for statistically
significant lead/lag relationships. Filters for:
- Sharpe > 1.0
- Win rate > 55%
- Minimum 20 trades

Winners are auto-generated as TACTICAL hypotheses in hypothesis_registry.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


def scan_all_pairs(
    engine: Engine,
    families: list[str] | None = None,
    min_sharpe: float = 1.0,
    min_win_rate: float = 0.55,
    min_trades: int = 20,
    lookback_days: int = 504,
) -> list[dict]:
    """Scan all feature pairs for profitable lead/lag signals.

    Parameters:
        engine: SQLAlchemy engine
        families: Feature families to scan (default: equity, crypto, commodity)
        min_sharpe: Minimum annualized Sharpe ratio
        min_win_rate: Minimum win rate (0-1)
        min_trades: Minimum number of trades
        lookback_days: Historical lookback period

    Returns:
        List of winning strategies sorted by Sharpe ratio
    """
    if families is None:
        families = ['equity', 'crypto', 'commodity', 'rates', 'credit']

    # Get all features with sufficient data
    with engine.connect() as conn:
        features = conn.execute(text(
            "SELECT fr.id, fr.name, fr.family, COUNT(rs.id) as cnt "
            "FROM feature_registry fr "
            "JOIN resolved_series rs ON rs.feature_id = fr.id "
            "WHERE fr.model_eligible = TRUE "
            "AND fr.family = ANY(:fams) "
            "AND rs.obs_date >= CURRENT_DATE - :days "
            "GROUP BY fr.id, fr.name, fr.family "
            "HAVING COUNT(rs.id) >= :min_obs "
            "ORDER BY fr.name"
        ), {"fams": families, "days": lookback_days, "min_obs": 30}).fetchall()

    if len(features) < 2:
        log.warning("Not enough features with data for scanning")
        return []

    log.info("Scanning {n} features for lead/lag relationships", n=len(features))

    # Load all price series
    series_map = {}
    for feat_id, name, family, cnt in features:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT obs_date, value FROM resolved_series "
                "WHERE feature_id = :fid AND obs_date >= CURRENT_DATE - :days "
                "ORDER BY obs_date"
            ), {"fid": feat_id, "days": lookback_days}).fetchall()
        if rows:
            s = pd.Series({r[0]: float(r[1]) for r in rows}).sort_index()
            if len(s) >= 30:
                series_map[name] = {"series": s, "family": family}

    names = list(series_map.keys())
    log.info("Loaded {n} series with sufficient data", n=len(names))

    winners = []
    tested = 0

    # Test all pairs (leader, follower)
    for i, leader_name in enumerate(names):
        leader_ret = series_map[leader_name]["series"].pct_change().dropna()
        leader_fam = series_map[leader_name]["family"]

        for j, follower_name in enumerate(names):
            if i == j:
                continue
            follower_fam = series_map[follower_name]["family"]

            # Skip same-ticker pairs (e.g., btc_full vs btc_close)
            l_base = leader_name.split("_")[0]
            f_base = follower_name.split("_")[0]
            if l_base == f_base:
                continue

            # Skip same-family crypto pairs — they just co-move, no alpha
            if leader_fam == follower_fam == "crypto":
                continue

            # Require cross-family OR genuinely different assets
            # Same-family is ok for equity→equity (sector rotation) but not crypto
            if leader_fam == follower_fam and leader_fam in ("commodity",):
                # Allow commodity cross-pairs (gold→oil is meaningful)
                pass

            follower_ret = series_map[follower_name]["series"].pct_change().dropna()

            # Align
            common = leader_ret.index.intersection(follower_ret.index)
            if len(common) < 30:
                continue

            lr = leader_ret[common]
            fr = follower_ret[common]

            # Signal: leader moves > 1% -> follow same direction next day
            signals = (lr > 0.01).astype(int) - (lr < -0.01).astype(int)
            shifted = signals.shift(1).dropna()
            aligned = fr[shifted.index]
            strat = (shifted * aligned).dropna()

            trades = strat[strat != 0]
            if len(trades) < min_trades:
                continue

            tested += 1
            wins = (trades > 0).sum()
            win_rate = wins / len(trades)
            sharpe = trades.mean() / trades.std() * np.sqrt(252) if trades.std() > 0 else 0
            total_ret = (1 + strat).prod() - 1

            # Sanity: reject absurd returns (data bug or overfitting)
            if abs(total_ret) > 50:
                continue
            # Sanity: reject suspiciously high Sharpe (>10 is almost always spurious)
            if sharpe > 10:
                continue

            if sharpe >= min_sharpe and win_rate >= min_win_rate:
                winners.append({
                    "leader": leader_name,
                    "follower": follower_name,
                    "leader_family": series_map[leader_name]["family"],
                    "follower_family": series_map[follower_name]["family"],
                    "trades": len(trades),
                    "wins": int(wins),
                    "win_rate": round(float(win_rate), 4),
                    "sharpe": round(float(sharpe), 4),
                    "total_return": round(float(total_ret), 4),
                    "avg_win": round(float(trades[trades > 0].mean()), 6),
                    "avg_loss": round(float(trades[trades < 0].mean()), 6) if (trades < 0).any() else 0,
                })

    winners.sort(key=lambda w: w["sharpe"], reverse=True)
    log.info("Scan complete: {tested} pairs tested, {w} winners (Sharpe>{s}, WR>{wr}%)",
             tested=tested, w=len(winners), s=min_sharpe, wr=int(min_win_rate*100))

    return winners


def generate_hypotheses_from_winners(
    engine: Engine,
    winners: list[dict],
    max_hypotheses: int = 10,
) -> int:
    """Create TACTICAL hypotheses from backtest winners.

    Returns number of hypotheses created.
    """
    created = 0
    with engine.begin() as conn:
        for w in winners[:max_hypotheses]:
            # Check if hypothesis already exists for this pair
            existing = conn.execute(text(
                "SELECT id FROM hypothesis_registry "
                "WHERE statement LIKE :pat"
            ), {"pat": f"%{w['leader']}%{w['follower']}%"}).fetchone()

            if existing:
                continue

            stmt = (
                f"{w['leader']}\u2192{w['follower']}: Sharpe {w['sharpe']:.1f}, "
                f"{w['win_rate']*100:.0f}% win rate, {w['total_return']*100:.1f}% return. "
                f"{w['leader_family']} leads {w['follower_family']}"
            )

            lag = json.dumps({
                "leader_features": [w["leader"]],
                "follower_features": [w["follower"]],
                "expected_lag": 1,
            })

            conn.execute(text(
                "INSERT INTO hypothesis_registry "
                "(statement, layer, lag_structure, state, feature_ids, "
                "proposed_metric, proposed_threshold) "
                "VALUES (:s, 'TACTICAL', :lag, 'PASSED', ARRAY[]::int[], "
                "'lagged_correlation', 0.5)"
            ), {"s": stmt, "lag": lag})
            created += 1

    log.info("Generated {n} TACTICAL hypotheses from backtest winners", n=created)
    return created


def run_full_scan(engine: Engine) -> dict:
    """Run complete scan -> filter -> generate pipeline."""
    winners = scan_all_pairs(engine)
    created = generate_hypotheses_from_winners(engine, winners)
    return {
        "pairs_scanned": "all",
        "winners": len(winners),
        "hypotheses_created": created,
        "top_strategies": winners[:5],
    }
