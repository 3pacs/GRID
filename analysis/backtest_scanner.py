"""
Automated cross-asset backtest scanner.

Scans all equity/crypto/commodity feature pairs for statistically
significant lead/lag relationships. Filters for:
- Sharpe > 1.0
- Win rate > 55%
- Minimum 20 trades

Winners are auto-generated as TACTICAL hypotheses in hypothesis_registry.
An optional LLM sanity check filters out spurious or trivially correlated
pairs before hypothesis creation.
"""

from __future__ import annotations

import json
import re
from datetime import date, timedelta
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Human-readable display names for feature keys ────────────────

_DISPLAY_NAMES: dict[str, str] = {
    "sp500_close": "S&P 500",
    "btc_full": "Bitcoin", "btc_close": "Bitcoin",
    "eth": "Ethereum", "eth_full": "Ethereum", "eth_usd_full": "Ethereum",
    "sol_usd_full": "Solana", "sol": "Solana",
    "doge_usd_full": "Dogecoin",
    "tao_usd_full": "TAO (Bittensor)",
    "gold_close": "Gold", "gld_close": "Gold ETF",
    "dxy_index": "US Dollar Index",
    "vix_close": "VIX", "vix_full": "VIX",
    "tlt_close": "20yr Treasury Bond",
    "xlk_close": "Tech Sector", "xlf_close": "Financials",
    "xle_close": "Energy Sector", "xlv_close": "Healthcare",
    "smh_close": "Semiconductors",
    "defi_total_tvl": "DeFi Total Value Locked",
    "dex_sol_liquidity": "Solana DEX Liquidity",
}


def _display_name(feature_name: str) -> str:
    """Return a human-readable display name for a feature key.

    Looks up *feature_name* in ``_DISPLAY_NAMES``.  Falls back to
    replacing underscores with spaces and title-casing the result.
    """
    if feature_name in _DISPLAY_NAMES:
        return _DISPLAY_NAMES[feature_name]
    return feature_name.replace("_", " ").title()


# ── LLM client helper ────────────────────────────────────────────

def _get_llm_client() -> Any | None:
    """Return the best available LLM client, or None.

    Tries llamacpp first, then ollama.  Returns None if neither is
    reachable so callers can gracefully skip LLM-dependent logic.
    """
    try:
        from llm.router import get_llm, Tier
        client = get_llm(Tier.ORACLE)
        if client.is_available:
            return client
    except Exception:
        pass

    try:
        from ollama.client import get_client as get_ollama
        client = get_ollama()
        if client.is_available:
            return client
    except Exception:
        pass

    return None


# ── LLM sanity check on candidate hypotheses ─────────────────────

def _llm_sanity_check(winners: list[dict]) -> list[dict]:
    """Ask the LLM to review candidate hypotheses and reject spurious ones.

    If no LLM is available the full list is returned unchanged.
    """
    if not winners:
        return winners

    client = _get_llm_client()
    if client is None:
        log.info("No LLM available for sanity check — passing all {n} winners through", n=len(winners))
        return winners

    # Build a numbered list of candidates for the prompt
    lines: list[str] = []
    for i, w in enumerate(winners, 1):
        leader_disp = _display_name(w["leader"])
        follower_disp = _display_name(w["follower"])
        lines.append(
            f"{i}. {leader_disp} ({w['leader']}, {w['leader_family']}) -> "
            f"{follower_disp} ({w['follower']}, {w['follower_family']}): "
            f"Sharpe {w['sharpe']:.1f}, {w['win_rate']*100:.0f}% win rate, "
            f"{w['total_return']*100:.1f}% return, {w['trades']} trades"
        )

    prompt = (
        "You are a quantitative finance reviewer. Review these trading hypotheses "
        "generated from a cross-asset lead/lag backtest scanner.\n\n"
        "For each hypothesis, respond with exactly one line in the format:\n"
        "  <number>. KEEP <one-line reason>\n"
        "  or\n"
        "  <number>. REJECT <one-line reason>\n\n"
        "Reject any that are:\n"
        "(a) trivially correlated assets just co-moving (e.g. two crypto tokens)\n"
        "(b) same asset class pairs with no plausible causal mechanism\n"
        "(c) suspiciously high returns suggesting a data artifact\n"
        "(d) relationships that wouldn't survive transaction costs\n\n"
        "Hypotheses:\n" + "\n".join(lines)
    )

    log.info("Sending {n} candidates to LLM for sanity check", n=len(winners))

    try:
        response = client.generate(
            prompt=prompt,
            system="You are a concise quantitative analyst. Respond only with numbered KEEP/REJECT lines.",
            temperature=0.2,
            num_predict=2000,
        )
    except Exception as exc:
        log.warning("LLM sanity check failed: {e} — passing all winners through", e=str(exc))
        return winners

    if not response:
        log.warning("LLM returned empty response — passing all winners through")
        return winners

    # Parse the response: look for lines like "1. KEEP ..." or "3. REJECT ..."
    keep_indices: set[int] = set()
    reject_log: list[str] = []

    for line in response.strip().splitlines():
        line = line.strip()
        m = re.match(r"(\d+)\.\s*(KEEP|REJECT)\s*(.*)", line, re.IGNORECASE)
        if m:
            idx = int(m.group(1))
            verdict = m.group(2).upper()
            reason = m.group(3).strip()
            if verdict == "KEEP":
                keep_indices.add(idx)
            else:
                reject_log.append(f"  #{idx} REJECTED: {reason}")

    # If parsing failed completely (no verdicts extracted), keep everything
    if not keep_indices and not reject_log:
        log.warning("Could not parse LLM sanity check response — passing all winners through")
        return winners

    # If a candidate wasn't mentioned at all, default to KEEP
    for i in range(1, len(winners) + 1):
        mentioned = any(
            re.match(rf"{i}\.", line.strip())
            for line in response.strip().splitlines()
        )
        if not mentioned:
            keep_indices.add(i)

    filtered = [w for i, w in enumerate(winners, 1) if i in keep_indices]

    if reject_log:
        log.info("LLM sanity check rejected {n} candidates:\n{details}",
                 n=len(winners) - len(filtered),
                 details="\n".join(reject_log))
    log.info("LLM sanity check: {kept}/{total} candidates kept",
             kept=len(filtered), total=len(winners))

    return filtered


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

            leader_disp = _display_name(w["leader"])
            follower_disp = _display_name(w["follower"])
            stmt = (
                f"When {leader_disp} moves >1%, {follower_disp} follows "
                f"within 1 day (Sharpe {w['sharpe']:.1f}, "
                f"{w['win_rate']*100:.0f}% win rate, "
                f"{w['total_return']*100:.1f}% return)"
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
    """Run complete scan -> LLM sanity check -> generate pipeline."""
    winners = scan_all_pairs(engine)
    raw_count = len(winners)
    winners = _llm_sanity_check(winners)
    created = generate_hypotheses_from_winners(engine, winners)
    return {
        "pairs_scanned": "all",
        "winners_raw": raw_count,
        "winners_after_llm_filter": len(winners),
        "hypotheses_created": created,
        "top_strategies": winners[:5],
    }


# ── Scheduled review of existing hypotheses ──────────────────────

def review_existing_hypotheses(engine: Engine) -> dict:
    """Use LLM to review all PASSED/TESTING hypotheses for quality issues.

    Loads hypotheses from the DB, sends them to the LLM in batches,
    and flags any that should be re-evaluated.  Flagged hypotheses are
    moved to TESTING state with a note explaining the concern.

    Returns:
        Summary dict with counts and details of flagged hypotheses.
    """
    client = _get_llm_client()
    if client is None:
        log.warning("No LLM available for hypothesis review")
        return {"error": "no_llm_available", "flagged": 0, "reviewed": 0}

    # Load PASSED and TESTING hypotheses
    with engine.connect() as conn:
        rows = conn.execute(text(
            "SELECT id, statement, state, lag_structure, created_at "
            "FROM hypothesis_registry "
            "WHERE state IN ('PASSED', 'TESTING') "
            "ORDER BY id"
        )).fetchall()

    if not rows:
        log.info("No PASSED/TESTING hypotheses to review")
        return {"flagged": 0, "reviewed": 0, "details": []}

    log.info("Reviewing {n} existing hypotheses with LLM", n=len(rows))

    batch_size = 15
    all_flagged: list[dict] = []
    total_reviewed = 0

    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start:batch_start + batch_size]
        total_reviewed += len(batch)

        lines: list[str] = []
        batch_ids: list[int] = []
        for row in batch:
            hyp_id, statement, state, lag_structure, created_at = row
            batch_ids.append(hyp_id)
            created_str = str(created_at)[:10] if created_at else "unknown"
            lines.append(
                f"{hyp_id}. [{state}] (created {created_str}) {statement}"
            )

        prompt = (
            "You are a quantitative finance auditor reviewing trading hypotheses "
            "that passed backtesting. For each hypothesis, respond with exactly "
            "one line in the format:\n"
            "  <id>. OK\n"
            "  or\n"
            "  <id>. FLAG <one-line reason>\n\n"
            "Flag any hypothesis that has:\n"
            "- Stale data (created long ago, may no longer hold)\n"
            "- Circular logic (testing a tautology)\n"
            "- Survivorship bias (only works because both assets survived)\n"
            "- Unrealistic assumptions (ignores slippage, liquidity, fees)\n"
            "- Self-correlation disguised as a signal\n\n"
            "Hypotheses:\n" + "\n".join(lines)
        )

        try:
            response = client.generate(
                prompt=prompt,
                system="You are a concise quantitative auditor. Respond only with numbered OK/FLAG lines.",
                temperature=0.2,
                num_predict=2000,
            )
        except Exception as exc:
            log.warning("LLM review batch failed: {e}", e=str(exc))
            continue

        if not response:
            continue

        # Parse response for FLAG lines
        for line in response.strip().splitlines():
            line = line.strip()
            m = re.match(r"(\d+)\.\s*FLAG\s+(.*)", line, re.IGNORECASE)
            if m:
                flagged_id = int(m.group(1))
                reason = m.group(2).strip()
                if flagged_id in batch_ids:
                    all_flagged.append({"id": flagged_id, "reason": reason})

    # Update flagged hypotheses in DB
    if all_flagged:
        with engine.begin() as conn:
            for item in all_flagged:
                conn.execute(text(
                    "UPDATE hypothesis_registry "
                    "SET state = 'TESTING', "
                    "    kill_reason = :reason, "
                    "    updated_at = NOW() "
                    "WHERE id = :id AND state = 'PASSED'"
                ), {
                    "reason": f"LLM review flag: {item['reason']}",
                    "id": item["id"],
                })

        log.info("Flagged {n} hypotheses for re-evaluation", n=len(all_flagged))

    return {
        "reviewed": total_reviewed,
        "flagged": len(all_flagged),
        "details": all_flagged,
    }
