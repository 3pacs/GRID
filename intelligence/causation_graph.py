"""
GRID Intelligence — Causal Connection Engine (graph module).

Multi-hop causal chain tracing, detection of chains in progress,
chain narrative generation, and chain storage/retrieval.
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.causation_core import (
    CausalLink,
    CausalChain,
    ensure_table,
    _MACRO_SERIES_PATTERNS,
    _parse_json,
    _safe_float,
    _macro_series_to_name,
)
from intelligence.causation_scoring import find_causes


# ══════════════════════════════════════════════════════════════════════════
# MULTI-HOP CAUSAL CHAINS
# ══════════════════════════════════════════════════════════════════════════

# Hop type priority for backward chaining.  Given an event of a certain
# type, we know which upstream event types to look for next.
_BACKWARD_CHAIN_MAP: dict[str, list[str]] = {
    "price_move":        ["insider", "congressional", "earnings", "contract", "legislation", "macro"],
    "insider":           ["contract", "legislation", "earnings", "insider_knowledge"],
    "congressional":     ["legislation", "contract", "earnings"],
    "contract":          ["legislation", "lobbying"],
    "legislation":       ["lobbying", "hearing"],
    "earnings":          ["macro"],
    "lobbying":          ["hearing"],
    "hearing":           [],
    "macro":             [],
}

# Readable labels for hop types
_HOP_TYPE_LABELS: dict[str, str] = {
    "price_move":    "price move",
    "insider":       "insider trade(s)",
    "congressional": "congressional trade(s)",
    "contract":      "government contract award",
    "legislation":   "legislative action",
    "lobbying":      "lobbying spend",
    "hearing":       "congressional hearing",
    "earnings":      "earnings event",
    "macro":         "macro event",
    "insider_knowledge": "cluster insider activity",
}


def trace_causal_chain(
    engine: Engine,
    ticker: str,
    end_date: str | None = None,
    max_hops: int = 5,
) -> list[CausalChain]:
    """Trace multi-hop causal chains backward from price moves or trades.

    Starting from a significant price move or trade, walks backwards asking:
    "what caused this? what caused THAT?" — up to max_hops or until no more
    upstream causes are found.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol.
        end_date: ISO date string to anchor the chain (default: today).
        max_hops: Maximum number of backward hops.

    Returns:
        List of CausalChain instances, longest first, with confidence scoring.
    """
    ensure_table(engine)
    ticker = ticker.strip().upper()

    if end_date:
        try:
            anchor = date.fromisoformat(end_date[:10])
        except (ValueError, TypeError):
            anchor = date.today()
    else:
        anchor = date.today()

    log.info(
        "Tracing causal chains for {t} from {d} (max_hops={h})",
        t=ticker, d=anchor, h=max_hops,
    )

    # Step 1: Find seed events — significant price moves and trades in the window
    seeds = _find_chain_seeds(engine, ticker, anchor, lookback_days=30)
    if not seeds:
        log.info("No seed events found for {t} near {d}", t=ticker, d=anchor)
        return []

    chains: list[CausalChain] = []

    for seed in seeds:
        # Build a chain by walking backwards from this seed
        chain_links: list[CausalLink] = [seed]
        current_link = seed
        visited_dates: set[str] = {current_link.action_date}

        for _hop in range(max_hops - 1):
            upstream = _find_upstream_cause(
                engine, ticker, current_link, visited_dates,
            )
            if upstream is None:
                break
            chain_links.append(upstream)
            visited_dates.add(upstream.action_date)
            current_link = upstream

        if len(chain_links) < 2:
            continue  # Single-hop chains are already in causal_links

        # Compute chain metrics
        first_date = _parse_chain_date(chain_links[-1].action_date)
        last_date = _parse_chain_date(chain_links[0].action_date)
        timespan = (last_date - first_date).days if first_date and last_date else 0

        all_actors: list[str] = []
        total_flow = 0.0
        for link in chain_links:
            if link.actor and link.actor not in all_actors:
                all_actors.append(link.actor)
            for ev in link.evidence:
                amt = _safe_float(ev.get("amount")) or _safe_float(ev.get("amount_usd"))
                if amt:
                    total_flow += amt

        # Chain confidence is the geometric mean of link probabilities,
        # boosted by chain length (longer = more interesting)
        probs = [link.probability for link in chain_links if link.probability > 0]
        if probs:
            geo_mean = 1.0
            for p in probs:
                geo_mean *= p
            geo_mean = geo_mean ** (1.0 / len(probs))
            length_bonus = min(0.15, 0.03 * len(chain_links))
            confidence = min(1.0, geo_mean + length_bonus)
        else:
            confidence = 0.1

        chain = CausalChain(
            ticker=ticker,
            chain=chain_links,
            total_hops=len(chain_links),
            timespan_days=max(0, timespan),
            total_dollar_flow=round(total_flow, 2),
            key_actors=all_actors[:10],
            narrative="",  # filled by generate_chain_narrative
            confidence=round(confidence, 3),
        )

        # Generate narrative for this chain
        chain.narrative = generate_chain_narrative(engine, chain)

        chains.append(chain)

    # Sort by hops descending, then confidence descending
    chains.sort(key=lambda c: (-c.total_hops, -c.confidence))

    # Persist chains
    if chains:
        _store_chains(engine, chains)

    log.info(
        "Traced {n} causal chains for {t}, longest={h} hops",
        n=len(chains), t=ticker,
        h=chains[0].total_hops if chains else 0,
    )
    return chains


def find_longest_chains(engine: Engine, days: int = 180) -> list[CausalChain]:
    """Find the longest traceable causal chains across all tickers.

    Parameters:
        engine: SQLAlchemy engine.
        days: How far back to search for tickers with activity.

    Returns:
        List of CausalChain instances, sorted longest and highest confidence first.
    """
    ensure_table(engine)
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ticker, COUNT(DISTINCT source_type) AS src_types, "
                "       COUNT(*) AS total_signals "
                "FROM signal_sources "
                "WHERE signal_date >= :cutoff "
                "AND ticker IS NOT NULL "
                "GROUP BY ticker "
                "HAVING COUNT(DISTINCT source_type) >= 2 "
                "ORDER BY src_types DESC, total_signals DESC "
                "LIMIT 30"
            ),
            {"cutoff": cutoff},
        ).fetchall()

    if not rows:
        log.info("find_longest_chains: no multi-source tickers found")
        return []

    all_chains: list[CausalChain] = []
    for row in rows:
        ticker = row[0]
        try:
            chains = trace_causal_chain(engine, ticker, max_hops=5)
            all_chains.extend(chains)
        except Exception as exc:
            log.debug("Chain trace for {t} failed: {e}", t=ticker, e=str(exc))

    all_chains.sort(key=lambda c: (-c.total_hops, -c.confidence))

    log.info(
        "find_longest_chains: {n} chains across {t} tickers, "
        "longest={h} hops",
        n=len(all_chains),
        t=len({c.ticker for c in all_chains}),
        h=all_chains[0].total_hops if all_chains else 0,
    )
    return all_chains


def generate_chain_narrative(engine: Engine, chain: CausalChain) -> str:
    """Generate a human-readable story from a causal chain.

    Tries LLM first, falls back to a rule-based template.

    Parameters:
        engine: SQLAlchemy engine.
        chain: The CausalChain to narrate.

    Returns:
        Narrative string.
    """
    # Try LLM first
    llm_narrative = _try_chain_llm_narrative(chain)
    if llm_narrative:
        return llm_narrative

    # Rule-based fallback
    if not chain.chain:
        return f"No causal chain data for {chain.ticker}."

    links_chrono = list(reversed(chain.chain))

    lines: list[str] = []
    lines.append(
        f"The {chain.ticker} story begins "
        f"{chain.timespan_days} days ago and spans {chain.total_hops} hops:"
    )
    lines.append("")

    for i, link in enumerate(links_chrono, 1):
        hop_label = _HOP_TYPE_LABELS.get(link.cause_type, link.cause_type)
        arrow = "  " if i == 1 else "  <- "
        lines.append(
            f"  Hop {i}: {arrow}{link.probable_cause} "
            f"[{hop_label}, {link.action_date}] "
            f"(probability: {link.probability:.0%})"
        )

    lines.append("")

    if chain.key_actors:
        lines.append(f"Key actors: {', '.join(chain.key_actors[:5])}")

    if chain.total_dollar_flow > 0:
        lines.append(f"Total estimated dollar flow: ${chain.total_dollar_flow:,.0f}")

    lines.append(f"Overall chain confidence: {chain.confidence:.0%}")

    return "\n".join(lines)


def detect_chain_in_progress(engine: Engine) -> list[dict]:
    """Detect causal chains that appear to be currently in progress.

    Looks at recent events and matches them against known chain patterns.

    Returns:
        List of dicts describing active chain patterns, with predictions
        about what might happen next.
    """
    ensure_table(engine)
    today = date.today()
    cutoff_recent = today - timedelta(days=30)
    cutoff_historical = today - timedelta(days=365)

    active_patterns: list[dict] = []

    # Step 1: Find tickers with recent multi-type activity
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT ticker, "
                "       array_agg(DISTINCT source_type) AS source_types, "
                "       COUNT(*) AS signal_count "
                "FROM signal_sources "
                "WHERE signal_date >= :cutoff "
                "AND ticker IS NOT NULL "
                "GROUP BY ticker "
                "HAVING COUNT(DISTINCT source_type) >= 2 "
                "ORDER BY signal_count DESC "
                "LIMIT 20"
            ),
            {"cutoff": cutoff_recent},
        ).fetchall()

    if not rows:
        log.info("detect_chain_in_progress: no active multi-source tickers")
        return []

    # Step 2: For each active ticker, check for known chain-start patterns
    for row in rows:
        ticker = row[0]
        source_types = row[1] if isinstance(row[1], list) else []
        signal_count = row[2]

        signals: dict[str, list[dict]] = {}
        try:
            signals = _fetch_recent_signals_by_type(engine, ticker, cutoff_recent)
        except Exception as exc:
            log.debug("Signal fetch for {t} failed: {e}", t=ticker, e=str(exc))
            continue

        # Pattern A: Lobbying + legislation/hearing activity
        has_lobbying = bool(signals.get("lobbying"))
        has_legislation = bool(signals.get("legislation"))
        has_hearing = bool(signals.get("hearing"))

        # Pattern B: Insider/congressional buying + upcoming contract/earnings
        has_insider_buy = any(
            s.get("action") == "BUY"
            for s in signals.get("insider", []) + signals.get("congressional", [])
        )
        has_contract = bool(signals.get("gov_contract"))

        # Pattern C: Multiple insiders same direction (cluster)
        insider_sells = [
            s for s in signals.get("insider", []) + signals.get("congressional", [])
            if s.get("action") == "SELL"
        ]
        has_sell_cluster = len(insider_sells) >= 2

        # Score and describe active patterns
        pattern_score = 0.0
        pattern_flags: list[str] = []
        predicted_next: list[str] = []

        if has_lobbying and (has_legislation or has_hearing):
            pattern_score += 0.4
            pattern_flags.append("lobbying_plus_legislation")
            predicted_next.append("Watch for government contract award or regulatory action")

        if has_insider_buy and has_contract:
            pattern_score += 0.5
            pattern_flags.append("insider_buy_plus_contract")
            predicted_next.append("Potential price move up — contract may be underpriced")

        if has_sell_cluster:
            pattern_score += 0.35
            pattern_flags.append("insider_sell_cluster")
            n_sellers = len(insider_sells)
            total_selling = sum(
                _safe_float(s.get("amount")) or 0 for s in insider_sells
            )
            predicted_next.append(
                f"{n_sellers} insiders selling — watch for negative earnings or regulatory action"
            )
            if total_selling:
                predicted_next[-1] += f" (est ${total_selling:,.0f} total)"

        if has_lobbying and has_sell_cluster:
            pattern_score += 0.2
            pattern_flags.append("lobbying_plus_insider_sell")
            predicted_next.append(
                "Lobbying spend increase with insider selling — possible failed lobbying effort"
            )

        # Step 3: Compare against historical completed chains
        historical_match = _match_historical_chain(
            engine, ticker, pattern_flags, cutoff_historical,
        )
        if historical_match:
            pattern_score += 0.15
            pattern_flags.append("historical_chain_match")
            predicted_next.append(
                f"Similar to a past chain: {historical_match.get('summary', '')}"
            )

        if pattern_flags:
            pattern_score = min(1.0, pattern_score)
            active_patterns.append({
                "ticker": ticker,
                "source_types_active": source_types,
                "signal_count_30d": signal_count,
                "pattern_score": round(pattern_score, 3),
                "pattern_flags": pattern_flags,
                "predicted_next_events": predicted_next,
                "signals_summary": {
                    stype: len(slist) for stype, slist in signals.items() if slist
                },
                "historical_match": historical_match,
                "detected_at": datetime.now(timezone.utc).isoformat(),
            })

    active_patterns.sort(key=lambda p: p["pattern_score"], reverse=True)

    log.info(
        "detect_chain_in_progress: {n} active patterns across {t} tickers",
        n=len(active_patterns),
        t=len({p["ticker"] for p in active_patterns}),
    )
    return active_patterns


# ── Multi-Hop Helpers ──────────────────────────────────────────────────


def _find_chain_seeds(
    engine: Engine, ticker: str, anchor: date, lookback_days: int = 30,
) -> list[CausalLink]:
    """Find seed events (price moves, trades) to start backward chaining from."""
    seeds: list[CausalLink] = []
    window_start = anchor - timedelta(days=lookback_days)

    # Seed type 1: Significant price moves
    try:
        from intelligence.forensics import find_significant_moves
        moves = find_significant_moves(engine, ticker, days=lookback_days, threshold=0.03)
        for move in moves[:5]:
            seeds.append(CausalLink(
                action_id=f"price_move:{ticker}:{move['date']}",
                actor="MARKET",
                action=move["direction"].upper(),
                ticker=ticker,
                action_date=move["date"],
                probable_cause=(
                    f"{ticker} moved {move['direction']} {abs(move['pct_change']):.1f}%"
                ),
                cause_type="price_move",
                evidence=[{
                    "type": "price_move",
                    "pct_change": move["pct_change"],
                    "direction": move["direction"],
                    "date": move["date"],
                }],
                probability=0.95,
                lead_time_days=0,
            ))
    except Exception as exc:
        log.debug("Price move seed failed for {t}: {e}", t=ticker, e=str(exc))

    # Seed type 2: Recent insider/congressional trades
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, source_type, source_id, signal_type, signal_date, "
                    "       signal_value "
                    "FROM signal_sources "
                    "WHERE ticker = :ticker "
                    "AND source_type IN ('congressional', 'insider') "
                    "AND signal_date BETWEEN :wstart AND :anchor "
                    "ORDER BY signal_date DESC "
                    "LIMIT 10"
                ),
                {"ticker": ticker, "wstart": window_start, "anchor": anchor},
            ).fetchall()

        for row in rows:
            sig_value = _parse_json(row[5])
            amount_str = sig_value.get("amount_range", sig_value.get("amount", ""))
            seeds.append(CausalLink(
                action_id=str(row[0]),
                actor=row[2] or "",
                action=row[3] or "TRADE",
                ticker=ticker,
                action_date=str(row[4]),
                probable_cause=(
                    f"{row[2] or 'Unknown'} {row[3] or 'traded'} {ticker}"
                    + (f" ({amount_str})" if amount_str else "")
                ),
                cause_type=row[1] or "insider",
                evidence=[{
                    "type": row[1],
                    "actor": row[2],
                    "action": row[3],
                    "date": str(row[4]),
                    "amount": amount_str,
                }],
                probability=0.9,
                lead_time_days=0,
            ))
    except Exception as exc:
        log.debug("Trade seed failed for {t}: {e}", t=ticker, e=str(exc))

    return seeds


def _find_upstream_cause(
    engine: Engine,
    ticker: str,
    current_link: CausalLink,
    visited_dates: set[str],
) -> CausalLink | None:
    """Find the most likely upstream cause for a given link."""
    current_type = current_link.cause_type
    upstream_types = _BACKWARD_CHAIN_MAP.get(current_type, [])

    if not upstream_types:
        return None

    try:
        current_date = date.fromisoformat(current_link.action_date[:10])
    except (ValueError, TypeError):
        return None

    best_cause: CausalLink | None = None
    best_score = 0.0

    for utype in upstream_types:
        candidates = _search_upstream_by_type(
            engine, ticker, utype, current_date, window_days=60,
        )
        for candidate in candidates:
            if candidate.action_date in visited_dates:
                continue
            try:
                cand_date = date.fromisoformat(candidate.action_date[:10])
            except (ValueError, TypeError):
                continue
            days_gap = (current_date - cand_date).days
            if days_gap < 0:
                continue
            recency = max(0.1, 1.0 - (days_gap / 60.0))
            score = candidate.probability * recency
            if score > best_score:
                best_score = score
                best_cause = candidate

    return best_cause


def _search_upstream_by_type(
    engine: Engine,
    ticker: str,
    cause_type: str,
    before_date: date,
    window_days: int = 60,
) -> list[CausalLink]:
    """Search for upstream events of a specific type before a given date."""
    window_start = before_date - timedelta(days=window_days)
    results: list[CausalLink] = []

    try:
        if cause_type in ("insider", "congressional"):
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT id, source_type, source_id, signal_type, "
                        "       signal_date, signal_value "
                        "FROM signal_sources "
                        "WHERE ticker = :ticker "
                        "AND source_type = :stype "
                        "AND signal_date BETWEEN :wstart AND :before "
                        "ORDER BY signal_date DESC "
                        "LIMIT 5"
                    ),
                    {
                        "ticker": ticker,
                        "stype": cause_type,
                        "wstart": window_start,
                        "before": before_date,
                    },
                ).fetchall()

            for row in rows:
                sig_value = _parse_json(row[5])
                amount = sig_value.get("amount_range", sig_value.get("amount", ""))
                days_before = (before_date - row[4]).days if isinstance(row[4], date) else 0
                prob = max(0.2, 0.7 - 0.01 * days_before)
                results.append(CausalLink(
                    action_id=str(row[0]),
                    actor=row[2] or "",
                    action=row[3] or "TRADE",
                    ticker=ticker,
                    action_date=str(row[4]),
                    probable_cause=(
                        f"{row[2] or 'Unknown'} {row[3] or 'traded'} {ticker}"
                        + (f" ({amount})" if amount else "")
                    ),
                    cause_type=cause_type,
                    evidence=[{
                        "type": cause_type,
                        "actor": row[2],
                        "action": row[3],
                        "date": str(row[4]),
                        "amount": amount,
                    }],
                    probability=round(prob, 3),
                    lead_time_days=days_before,
                ))

        elif cause_type == "contract":
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT signal_date, signal_value "
                        "FROM signal_sources "
                        "WHERE source_type = 'gov_contract' "
                        "AND ticker = :ticker "
                        "AND signal_date BETWEEN :wstart AND :before "
                        "ORDER BY signal_date DESC "
                        "LIMIT 5"
                    ),
                    {"ticker": ticker, "wstart": window_start, "before": before_date},
                ).fetchall()

            for row in rows:
                c_value = _parse_json(row[1])
                amount = c_value.get("amount", 0)
                agency = c_value.get("awarding_agency", c_value.get("recipient_name", ""))
                c_date = row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0])[:10])
                days_before = (before_date - c_date).days
                prob = max(0.2, 0.65 - 0.01 * days_before)
                results.append(CausalLink(
                    action_id=f"contract:{ticker}:{c_date}",
                    actor=agency or "Government",
                    action="AWARD",
                    ticker=ticker,
                    action_date=str(c_date),
                    probable_cause=(
                        f"${amount:,.0f} contract from {agency}" if amount
                        else f"Contract from {agency}"
                    ),
                    cause_type="contract",
                    evidence=[{
                        "type": "gov_contract",
                        "date": str(c_date),
                        "amount": amount,
                        "agency": agency,
                    }],
                    probability=round(prob, 3),
                    lead_time_days=days_before,
                ))

        elif cause_type in ("legislation", "hearing"):
            pattern = "LEGISLATION:%:hearing" if cause_type == "hearing" else "LEGISLATION:%"
            exclude = "" if cause_type == "hearing" else "%:hearing"
            with engine.connect() as conn:
                query = (
                    "SELECT series_id, obs_date, raw_payload "
                    "FROM raw_series "
                    "WHERE series_id LIKE :pattern "
                    "AND obs_date BETWEEN :wstart AND :before "
                    "AND pull_status = 'SUCCESS' "
                )
                if exclude:
                    query += "AND series_id NOT LIKE :exclude "
                query += "ORDER BY obs_date DESC LIMIT 5"
                params: dict[str, Any] = {
                    "pattern": pattern,
                    "wstart": window_start,
                    "before": before_date,
                }
                if exclude:
                    params["exclude"] = exclude
                rows = conn.execute(text(query), params).fetchall()

            for row in rows:
                payload = _parse_json(row[2])
                affected = payload.get("affected_tickers", [])
                if ticker not in affected:
                    continue
                obs_date = row[1] if isinstance(row[1], date) else date.fromisoformat(str(row[1])[:10])
                days_before = (before_date - obs_date).days
                title = payload.get("title", "")
                bill_id = payload.get("bill_id", "")
                prob = max(0.2, 0.6 - 0.008 * days_before)
                results.append(CausalLink(
                    action_id=f"{cause_type}:{bill_id or row[0]}",
                    actor="Congress",
                    action="LEGISLATE" if cause_type == "legislation" else "HEARING",
                    ticker=ticker,
                    action_date=str(obs_date),
                    probable_cause=(
                        f"Hearing: {title[:80]}" if cause_type == "hearing"
                        else f"Bill {bill_id}: {title[:80]}" if title
                        else f"Legislation {bill_id}"
                    ),
                    cause_type=cause_type,
                    evidence=[{
                        "type": cause_type,
                        "bill_id": bill_id,
                        "title": title[:200],
                        "date": str(obs_date),
                        "committees": payload.get("committees", []),
                    }],
                    probability=round(prob, 3),
                    lead_time_days=days_before,
                ))

        elif cause_type == "lobbying":
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT signal_date, signal_value "
                        "FROM signal_sources "
                        "WHERE source_type = 'lobbying' "
                        "AND ticker = :ticker "
                        "AND signal_date BETWEEN :wstart AND :before "
                        "ORDER BY signal_date DESC "
                        "LIMIT 5"
                    ),
                    {"ticker": ticker, "wstart": window_start, "before": before_date},
                ).fetchall()

            for row in rows:
                val = _parse_json(row[1])
                amount = float(val.get("amount", 0) or 0)
                client = val.get("client_name", "")
                l_date = row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0])[:10])
                days_before = (before_date - l_date).days
                prob = max(0.2, 0.55 - 0.008 * days_before)
                results.append(CausalLink(
                    action_id=f"lobbying:{ticker}:{l_date}",
                    actor=client or ticker,
                    action="LOBBY",
                    ticker=ticker,
                    action_date=str(l_date),
                    probable_cause=(
                        f"${amount:,.0f} lobbying spend by {client or ticker}"
                        if amount else f"Lobbying activity by {client or ticker}"
                    ),
                    cause_type="lobbying",
                    evidence=[{
                        "type": "lobbying",
                        "date": str(l_date),
                        "amount": amount,
                        "client": client,
                        "issue_codes": val.get("issue_codes", []),
                    }],
                    probability=round(prob, 3),
                    lead_time_days=days_before,
                ))

        elif cause_type == "earnings":
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT earnings_date, eps_estimate, eps_actual, "
                        "       eps_surprise_pct, classification "
                        "FROM earnings_calendar "
                        "WHERE ticker = :ticker "
                        "AND earnings_date BETWEEN :wstart AND :before "
                        "ORDER BY earnings_date DESC "
                        "LIMIT 3"
                    ),
                    {"ticker": ticker, "wstart": window_start, "before": before_date},
                ).fetchall()

            for row in rows:
                e_date = row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0])[:10])
                eps_act = _safe_float(row[2])
                eps_est = _safe_float(row[1])
                surprise = _safe_float(row[3])
                days_before = (before_date - e_date).days
                prob = max(0.2, 0.6 - 0.01 * days_before)

                if eps_act is not None and eps_est is not None:
                    if eps_act > eps_est:
                        desc = f"Earnings beat ({eps_act:.2f} vs {eps_est:.2f} est)"
                    elif eps_act < eps_est:
                        desc = f"Earnings miss ({eps_act:.2f} vs {eps_est:.2f} est)"
                    else:
                        desc = f"Earnings inline ({eps_act:.2f})"
                else:
                    desc = f"Earnings event on {e_date}"

                results.append(CausalLink(
                    action_id=f"earnings:{ticker}:{e_date}",
                    actor=ticker,
                    action="REPORT",
                    ticker=ticker,
                    action_date=str(e_date),
                    probable_cause=desc,
                    cause_type="earnings",
                    evidence=[{
                        "type": "earnings",
                        "date": str(e_date),
                        "eps_estimate": eps_est,
                        "eps_actual": eps_act,
                        "surprise_pct": surprise,
                    }],
                    probability=round(prob, 3),
                    lead_time_days=days_before,
                ))

        elif cause_type == "insider_knowledge":
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT source_id, signal_type, signal_date "
                        "FROM signal_sources "
                        "WHERE ticker = :ticker "
                        "AND source_type IN ('congressional', 'insider') "
                        "AND signal_date BETWEEN :wstart AND :before "
                        "ORDER BY signal_date DESC "
                        "LIMIT 20"
                    ),
                    {"ticker": ticker, "wstart": window_start, "before": before_date},
                ).fetchall()

            if len(rows) >= 2:
                actors_seen = list({r[0] for r in rows if r[0]})
                cluster_date = rows[0][2] if isinstance(rows[0][2], date) else date.fromisoformat(str(rows[0][2])[:10])
                days_before = (before_date - cluster_date).days
                prob = min(0.75, 0.3 + 0.1 * len(rows))
                results.append(CausalLink(
                    action_id=f"cluster:{ticker}:{cluster_date}",
                    actor=", ".join(actors_seen[:3]),
                    action="CLUSTER",
                    ticker=ticker,
                    action_date=str(cluster_date),
                    probable_cause=(
                        f"Cluster: {len(rows)} insiders traded {ticker} "
                        f"within {window_days} days"
                    ),
                    cause_type="insider_knowledge",
                    evidence=[{
                        "type": "cluster_signal",
                        "cluster_size": len(rows),
                        "actors": actors_seen[:10],
                        "window_days": window_days,
                    }],
                    probability=round(prob, 3),
                    lead_time_days=days_before,
                ))

        elif cause_type == "macro":
            for pattern in _MACRO_SERIES_PATTERNS:
                with engine.connect() as conn:
                    rows = conn.execute(
                        text(
                            "SELECT series_id, obs_date, value "
                            "FROM raw_series "
                            "WHERE series_id LIKE :pattern "
                            "AND obs_date BETWEEN :wstart AND :before "
                            "AND pull_status = 'SUCCESS' "
                            "ORDER BY obs_date DESC "
                            "LIMIT 1"
                        ),
                        {"pattern": pattern + "%", "wstart": window_start, "before": before_date},
                    ).fetchall()

                for row in rows:
                    obs_date = row[1] if isinstance(row[1], date) else date.fromisoformat(str(row[1])[:10])
                    days_before = (before_date - obs_date).days
                    macro_name = _macro_series_to_name(row[0])
                    results.append(CausalLink(
                        action_id=f"macro:{row[0]}:{obs_date}",
                        actor="Federal Reserve" if "FOMC" in macro_name or "Fed" in macro_name else "BLS/BEA",
                        action="RELEASE",
                        ticker=ticker,
                        action_date=str(obs_date),
                        probable_cause=f"Macro: {macro_name} release on {obs_date}",
                        cause_type="macro",
                        evidence=[{
                            "type": "macro_event",
                            "series": row[0],
                            "name": macro_name,
                            "date": str(obs_date),
                            "value": float(row[2]) if row[2] else None,
                        }],
                        probability=round(max(0.15, 0.35 - 0.01 * days_before), 3),
                        lead_time_days=days_before,
                    ))

    except Exception as exc:
        log.debug(
            "Upstream search for {t}/{ct} failed: {e}",
            t=ticker, ct=cause_type, e=str(exc),
        )

    return results


def _parse_chain_date(date_str: str) -> date | None:
    """Parse a date string for chain timespan calculation."""
    try:
        return date.fromisoformat(date_str[:10])
    except (ValueError, TypeError):
        return None


def _fetch_recent_signals_by_type(
    engine: Engine, ticker: str, cutoff: date,
) -> dict[str, list[dict]]:
    """Fetch recent signals grouped by source_type for chain-in-progress detection."""
    signals: dict[str, list[dict]] = {}

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT source_type, source_id, signal_type, signal_date, "
                "       signal_value "
                "FROM signal_sources "
                "WHERE ticker = :ticker "
                "AND signal_date >= :cutoff "
                "ORDER BY signal_date DESC "
                "LIMIT 100"
            ),
            {"ticker": ticker, "cutoff": cutoff},
        ).fetchall()

    for row in rows:
        stype = row[0] or "unknown"
        sig_value = _parse_json(row[4])
        signals.setdefault(stype, []).append({
            "actor": row[1],
            "action": row[2],
            "date": str(row[3]),
            "amount": sig_value.get("amount", sig_value.get("amount_range")),
        })

    return signals


def _match_historical_chain(
    engine: Engine,
    ticker: str,
    current_flags: list[str],
    cutoff: date,
) -> dict | None:
    """Check if current pattern flags match a completed historical chain."""
    if not current_flags:
        return None

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT chain, total_hops, narrative, confidence "
                    "FROM causal_chains "
                    "WHERE ticker = :ticker "
                    "AND created_at >= :cutoff "
                    "AND total_hops >= 3 "
                    "ORDER BY confidence DESC "
                    "LIMIT 5"
                ),
                {"ticker": ticker, "cutoff": cutoff},
            ).fetchall()

        for row in rows:
            chain_data = row[0] if isinstance(row[0], list) else json.loads(row[0] or "[]")
            chain_types = {
                link.get("cause_type", "") for link in chain_data if isinstance(link, dict)
            }

            flag_types = set()
            for flag in current_flags:
                parts = flag.split("_plus_")
                flag_types.update(parts)
                for part in parts:
                    if part in _BACKWARD_CHAIN_MAP:
                        flag_types.add(part)

            overlap = flag_types & chain_types
            if len(overlap) >= 2:
                return {
                    "total_hops": row[1],
                    "summary": (row[2] or "")[:200],
                    "confidence": float(row[3]) if row[3] else 0,
                    "matching_types": list(overlap),
                }
    except Exception as exc:
        log.debug("Historical chain match failed: {e}", e=str(exc))

    return None


def _try_chain_llm_narrative(chain: CausalChain) -> str | None:
    """Attempt LLM-based narrative generation for a causal chain."""
    try:
        from llm.router import get_llm, Tier
        llm = get_llm(Tier.REASON)
        if not llm.is_available:
            raise RuntimeError("LLM unavailable")
    except Exception:
        try:
            from ollama.client import get_client as get_ollama
            llm = get_ollama()
            if llm is None:
                return None
        except Exception:
            return None

    links_chrono = list(reversed(chain.chain))
    hop_lines: list[str] = []
    for i, link in enumerate(links_chrono, 1):
        hop_lines.append(
            f"  Hop {i}: [{link.cause_type}] {link.probable_cause} "
            f"on {link.action_date} (probability: {link.probability:.0%})"
        )

    rag_context = ""
    try:
        from intelligence.rag import get_rag_context
        from db import get_engine as _get_engine
        rag_query = (
            f"{chain.ticker} causal chain "
            f"{' '.join(chain.key_actors[:3])}"
        )
        rag_context = get_rag_context(_get_engine(), rag_query, top_k=5, max_chars=2000)
    except Exception as exc:
        log.debug("Causation: chain RAG context retrieval failed: {e}", e=str(exc))

    prompt = (
        f"You are a forensic financial analyst. Write a 3-5 sentence investigative "
        f"narrative connecting the following causal chain for {chain.ticker}.\n\n"
        f"CHAIN ({chain.total_hops} hops, {chain.timespan_days} days):\n"
        + "\n".join(hop_lines)
        + f"\n\nKey actors: {', '.join(chain.key_actors[:5]) or 'none identified'}"
        + f"\nTotal dollar flow: ${chain.total_dollar_flow:,.0f}"
    )
    if rag_context:
        prompt += f"\n\n{rag_context}"
    prompt += (
        f"\n\nWrite as if telling a story. Start with 'The {chain.ticker} story "
        f"begins when...' and trace the causal path to the final effect. "
        f"Reference historical precedents if relevant. "
        f"Be specific and data-driven. No disclaimers."
    )

    try:
        if hasattr(llm, 'generate') and callable(llm.generate):
            response = llm.generate(
                prompt=prompt,
                system="You are the GRID forensic intelligence system. Be concise and specific.",
                temperature=0.3,
                num_predict=500,
            )
            if isinstance(response, str) and len(response.strip()) > 30:
                return response.strip()
            if isinstance(response, dict):
                text_resp = response.get("response", "").strip()
                if len(text_resp) > 30:
                    return text_resp
    except Exception as exc:
        log.debug("LLM chain narrative failed: {e}", e=str(exc))

    return None


# ── Chain Storage ──────────────────────────────────────────────────────


def _store_chains(engine: Engine, chains: list[CausalChain]) -> int:
    """Persist CausalChain objects to the causal_chains table.

    Returns:
        Number of rows stored.
    """
    if not chains:
        return 0

    stored = 0
    with engine.begin() as conn:
        for chain in chains:
            chain_json = [link.to_dict() for link in chain.chain]
            conn.execute(
                text(
                    "INSERT INTO causal_chains "
                    "(ticker, chain, total_hops, timespan_days, "
                    " total_dollar_flow, key_actors, narrative, confidence) "
                    "VALUES (:ticker, :chain, :hops, :timespan, "
                    "        :flow, :actors, :narrative, :confidence)"
                ),
                {
                    "ticker": chain.ticker,
                    "chain": json.dumps(chain_json, default=str),
                    "hops": chain.total_hops,
                    "timespan": chain.timespan_days,
                    "flow": chain.total_dollar_flow,
                    "actors": json.dumps(chain.key_actors),
                    "narrative": chain.narrative,
                    "confidence": chain.confidence,
                },
            )
            stored += 1

    log.info("Stored {n} causal chains", n=stored)
    return stored


def load_causal_chains(
    engine: Engine,
    ticker: str | None = None,
    min_hops: int = 2,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Load stored causal chains for the API.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Optional ticker filter.
        min_hops: Minimum chain length.
        limit: Max results.

    Returns:
        List of chain dicts.
    """
    ensure_table(engine)

    if ticker:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT ticker, chain, total_hops, timespan_days, "
                    "       total_dollar_flow, key_actors, narrative, "
                    "       confidence, created_at "
                    "FROM causal_chains "
                    "WHERE ticker = :ticker AND total_hops >= :min_hops "
                    "ORDER BY total_hops DESC, confidence DESC "
                    "LIMIT :lim"
                ),
                {"ticker": ticker.upper(), "min_hops": min_hops, "lim": limit},
            ).fetchall()
    else:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT ticker, chain, total_hops, timespan_days, "
                    "       total_dollar_flow, key_actors, narrative, "
                    "       confidence, created_at "
                    "FROM causal_chains "
                    "WHERE total_hops >= :min_hops "
                    "ORDER BY total_hops DESC, confidence DESC "
                    "LIMIT :lim"
                ),
                {"min_hops": min_hops, "lim": limit},
            ).fetchall()

    results: list[dict[str, Any]] = []
    for r in rows:
        chain_data = r[1] if isinstance(r[1], list) else json.loads(r[1] or "[]")
        actors_data = r[5] if isinstance(r[5], list) else json.loads(r[5] or "[]")
        results.append({
            "ticker": r[0],
            "chain": chain_data,
            "total_hops": r[2],
            "timespan_days": r[3],
            "total_dollar_flow": float(r[4]) if r[4] else 0,
            "key_actors": actors_data,
            "narrative": r[6],
            "confidence": float(r[7]) if r[7] else 0,
            "created_at": str(r[8]) if r[8] else None,
        })

    return results
