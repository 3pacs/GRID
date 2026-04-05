"""
GRID Intelligence — Causal Connection Engine (scoring module).

Cause-checking helpers, suspicious trade detection, storage,
and narrative generation for single-hop causal analysis.
"""

from __future__ import annotations

import json
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence.causation_core import (
    CausalLink,
    ensure_table,
    _PRE_WINDOW_DAYS,
    _POST_WINDOW_DAYS,
    _MACRO_SERIES_PATTERNS,
    _parse_json,
    _safe_float,
    _macro_series_to_name,
)


# ── 1. find_causes ───────────────────────────────────────────────────────


def find_causes(
    engine: Engine,
    actor: str,
    action: str,
    ticker: str,
    action_date: str,
    signal_id: int | str | None = None,
) -> list[CausalLink]:
    """Search for probable causes of a specific actor action.

    Checks in order:
      1. Government contracts awarded to this company
      2. Legislation / bills affecting this ticker/sector
      3. Committee hearings near the date
      4. Upcoming or recent earnings
      5. Other insider activity on the same ticker (cluster signal)
      6. Macro events (FOMC, CPI, payrolls) near the date

    Parameters:
        engine: SQLAlchemy engine.
        actor: Actor name / source_id.
        action: 'BUY' or 'SELL'.
        ticker: Ticker symbol.
        action_date: ISO date string of the action.
        signal_id: Optional signal_sources row id.

    Returns:
        List of CausalLink sorted by probability descending.
    """
    try:
        act_date = date.fromisoformat(action_date[:10])
    except (ValueError, TypeError):
        log.warning("Invalid action_date: {d}", d=action_date)
        return []

    aid = str(signal_id) if signal_id else f"{actor}:{ticker}:{action_date}"
    causes: list[CausalLink] = []

    # 1. Government contracts
    causes.extend(_check_contracts(engine, aid, actor, action, ticker, act_date))

    # 2. Legislation / bills
    causes.extend(_check_legislation(engine, aid, actor, action, ticker, act_date))

    # 3. Committee hearings
    causes.extend(_check_hearings(engine, aid, actor, action, ticker, act_date))

    # 4. Earnings
    causes.extend(_check_earnings(engine, aid, actor, action, ticker, act_date))

    # 5. Cluster / other insider activity
    causes.extend(_check_cluster_signals(engine, aid, actor, action, ticker, act_date))

    # 6. Macro events
    causes.extend(_check_macro_events(engine, aid, actor, action, ticker, act_date))

    # Sort by probability descending
    causes.sort(key=lambda c: c.probability, reverse=True)

    log.info(
        "Causation: {n} causes found for {a} {act} {t} on {d}",
        n=len(causes), a=actor, act=action, t=ticker, d=action_date,
    )
    return causes


# ── 2. batch_find_causes ─────────────────────────────────────────────────


def batch_find_causes(engine: Engine, days: int = 30) -> list[CausalLink]:
    """Run find_causes for all recent signal_sources entries, store results.

    Parameters:
        engine: SQLAlchemy engine.
        days: How far back to search for signals.

    Returns:
        All CausalLink objects found.
    """
    ensure_table(engine)
    cutoff = date.today() - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, source_type, source_id, ticker, signal_type, signal_date "
                "FROM signal_sources "
                "WHERE signal_date >= :cutoff "
                "AND source_type IN ('congressional', 'insider') "
                "ORDER BY signal_date DESC"
            ),
            {"cutoff": cutoff},
        ).fetchall()

    if not rows:
        log.info("batch_find_causes: no signals in last {d} days", d=days)
        return []

    all_causes: list[CausalLink] = []
    for row in rows:
        sig_id = row[0]
        actor = row[2]
        ticker = row[3]
        action = row[4]
        action_date = str(row[5])

        causes = find_causes(engine, actor, action, ticker, action_date, signal_id=sig_id)
        all_causes.extend(causes)

    # Store results
    if all_causes:
        _store_causal_links(engine, all_causes)

    log.info(
        "batch_find_causes: {n} causes from {r} signals",
        n=len(all_causes), r=len(rows),
    )
    return all_causes


# ── 3. get_suspicious_trades ─────────────────────────────────────────────


def get_suspicious_trades(engine: Engine, days: int = 90) -> list[dict]:
    """Identify trades where the cause is likely non-public information.

    Suspicious patterns:
      - Congressional trade + committee jurisdiction overlap + upcoming legislation
      - Insider buy + contract award within 30 days
      - Insider sell + earnings miss within 14 days

    Parameters:
        engine: SQLAlchemy engine.
        days: How far back to search.

    Returns:
        List of dicts with trade info, cause, and suspicion_score, sorted
        by suspicion_score descending.
    """
    # Freshness guard — warn if core signals are stale, but never block
    try:
        from intelligence.freshness_guard import check_freshness, log_stale_features
        statuses = check_freshness(engine, ["sp500_close", "vix_spot"])
        log_stale_features(statuses, caller="causation.get_suspicious_trades")
    except Exception as exc:
        log.warning("Freshness check failed in causation: {e}", e=exc)

    cutoff = date.today() - timedelta(days=days)
    suspicious: list[dict] = []

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, source_type, source_id, ticker, signal_type, "
                "       signal_date, signal_value, signal_value "
                "FROM signal_sources "
                "WHERE signal_date >= :cutoff "
                "AND source_type IN ('congressional', 'insider') "
                "ORDER BY signal_date DESC"
            ),
            {"cutoff": cutoff},
        ).fetchall()

    if not rows:
        return []

    for row in rows:
        sig_id = row[0]
        source_type = row[1]
        actor = row[2]
        ticker = row[3]
        action = row[4]
        sig_date = row[5]
        sig_value = _parse_json(row[6])
        metadata = _parse_json(row[7])

        try:
            act_date = sig_date if isinstance(sig_date, date) else date.fromisoformat(str(sig_date)[:10])
        except (ValueError, TypeError):
            continue

        suspicion_score = 0.0
        flags: list[str] = []
        evidence: list[dict] = []

        # Pattern 1: Congressional + committee overlap + legislation
        if source_type == "congressional":
            committee = metadata.get("committee", "") or sig_value.get("committee", "")
            leg_overlap = _has_legislation_overlap(engine, ticker, act_date, committee)
            if leg_overlap:
                suspicion_score += 0.5
                flags.append("committee_legislation_overlap")
                evidence.append(leg_overlap)

            # Check if member sits on relevant committee
            if committee and _committee_has_jurisdiction(committee, ticker):
                suspicion_score += 0.25
                flags.append("committee_jurisdiction")

        # Pattern 2: Insider buy + contract award within 30 days
        if source_type == "insider" and action == "BUY":
            contract_hit = _has_contract_award(engine, ticker, act_date, window_days=30)
            if contract_hit:
                suspicion_score += 0.6
                flags.append("pre_contract_buy")
                evidence.append(contract_hit)

        # Pattern 3: Insider sell + earnings miss within 14 days
        if source_type == "insider" and action == "SELL":
            earnings_hit = _has_earnings_miss(engine, ticker, act_date, window_days=14)
            if earnings_hit:
                suspicion_score += 0.5
                flags.append("pre_earnings_miss_sell")
                evidence.append(earnings_hit)

        # Pattern 4: Large disclosure lag (congressional)
        if source_type == "congressional":
            disc_date_str = sig_value.get("disclosure_date", "")
            txn_date_str = sig_value.get("transaction_date", "")
            if disc_date_str and txn_date_str:
                try:
                    disc = date.fromisoformat(disc_date_str[:10])
                    txn = date.fromisoformat(txn_date_str[:10])
                    lag = (disc - txn).days
                    if lag > 30:
                        suspicion_score += 0.15
                        flags.append(f"disclosure_lag_{lag}d")
                except (ValueError, TypeError):
                    pass

        # Only include if there's at least one flag
        if flags:
            suspicion_score = min(1.0, suspicion_score)
            suspicious.append({
                "signal_id": sig_id,
                "source_type": source_type,
                "actor": actor,
                "ticker": ticker,
                "action": action,
                "action_date": str(act_date),
                "suspicion_score": round(suspicion_score, 3),
                "flags": flags,
                "evidence": evidence,
                "metadata": {
                    k: v for k, v in {**sig_value, **metadata}.items()
                    if k in (
                        "committee", "amount_range", "transaction_type",
                        "member_name", "insider_name", "title",
                    )
                },
            })

    suspicious.sort(key=lambda x: x["suspicion_score"], reverse=True)

    log.info(
        "Suspicious trades: {n} flagged from {r} signals in last {d} days",
        n=len(suspicious), r=len(rows), d=days,
    )
    return suspicious


# ── 4. generate_causal_narrative ─────────────────────────────────────────


def generate_causal_narrative(engine: Engine, ticker: str) -> str:
    """Generate an LLM or rule-based narrative explaining trading activity.

    "Here's why people are trading NVDA right now..."

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol.

    Returns:
        Narrative string.
    """
    ticker = ticker.strip().upper()
    cutoff = date.today() - timedelta(days=30)

    # Gather recent signals for this ticker
    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT source_type, source_id, signal_type, signal_date, "
                "       signal_value, signal_value "
                "FROM signal_sources "
                "WHERE ticker = :t AND signal_date >= :c "
                "ORDER BY signal_date DESC "
                "LIMIT 50"
            ),
            {"t": ticker, "c": cutoff},
        ).fetchall()

    if not rows:
        return f"No recent trading activity found for {ticker}."

    # Gather causes for recent signals
    causes: list[CausalLink] = []
    for row in rows[:20]:  # limit to avoid long runtime
        actor = row[1]
        action = row[2]
        sig_date = str(row[3])
        c = find_causes(engine, actor, action, ticker, sig_date)
        causes.extend(c)

    # Build context
    buys = [r for r in rows if r[2] == "BUY"]
    sells = [r for r in rows if r[2] == "SELL"]
    actors = list({r[1] for r in rows})
    source_types = list({r[0] for r in rows})

    cause_summary = _summarize_causes(causes)

    # Try LLM
    llm_narrative = _try_llm_narrative(ticker, rows, causes)
    if llm_narrative:
        return llm_narrative

    # Rule-based fallback
    lines: list[str] = []
    lines.append(f"## Why People Are Trading {ticker}")
    lines.append("")
    lines.append(
        f"In the last 30 days: {len(buys)} buy signal(s), {len(sells)} sell signal(s) "
        f"from {len(actors)} actor(s) across {', '.join(source_types)} sources."
    )

    if cause_summary.get("contract"):
        lines.append(
            f"\n**Government Contracts:** {cause_summary['contract']['count']} contract-related cause(s) "
            f"detected. {cause_summary['contract']['top']}"
        )
    if cause_summary.get("legislation"):
        lines.append(
            f"\n**Legislation:** {cause_summary['legislation']['count']} legislation-related cause(s). "
            f"{cause_summary['legislation']['top']}"
        )
    if cause_summary.get("earnings"):
        lines.append(
            f"\n**Earnings:** {cause_summary['earnings']['count']} earnings-related cause(s). "
            f"{cause_summary['earnings']['top']}"
        )
    if cause_summary.get("insider_knowledge"):
        lines.append(
            f"\n**Insider Knowledge:** {cause_summary['insider_knowledge']['count']} signal(s) "
            f"suggest possible non-public information. {cause_summary['insider_knowledge']['top']}"
        )
    if cause_summary.get("macro"):
        lines.append(
            f"\n**Macro Events:** {cause_summary['macro']['count']} macro-related cause(s). "
            f"{cause_summary['macro']['top']}"
        )

    if not causes:
        lines.append(
            "\nNo clear causal link found — activity may be routine rebalancing "
            "or driven by signals outside our current data sources."
        )

    # Top actors
    if actors[:5]:
        lines.append(f"\n**Key actors:** {', '.join(actors[:5])}")

    return "\n".join(lines)


# ── Cause-Checking Helpers ───────────────────────────────────────────────


def _check_contracts(
    engine: Engine, aid: str, actor: str, action: str, ticker: str, act_date: date,
) -> list[CausalLink]:
    """Check for government contracts near the action date."""
    causes: list[CausalLink] = []
    window_start = act_date - timedelta(days=_PRE_WINDOW_DAYS)
    window_end = act_date + timedelta(days=_POST_WINDOW_DAYS)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT signal_date, signal_value "
                    "FROM signal_sources "
                    "WHERE source_type = 'gov_contract' "
                    "AND ticker = :ticker "
                    "AND signal_date BETWEEN :wstart AND :wend "
                    "ORDER BY signal_date"
                ),
                {"ticker": ticker, "wstart": window_start, "wend": window_end},
            ).fetchall()

        for row in rows:
            c_date = row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0])[:10])
            c_value = _parse_json(row[1])
            lead_days = (c_date - act_date).days  # negative = cause before action

            # Trade BEFORE contract award is more suspicious
            if lead_days > 0:
                prob = min(0.85, 0.5 + 0.35 * (1.0 - abs(lead_days) / _POST_WINDOW_DAYS))
            else:
                prob = max(0.1, 0.4 - 0.3 * (abs(lead_days) / _PRE_WINDOW_DAYS))

            amount = c_value.get("amount", 0)
            agency = c_value.get("awarding_agency", c_value.get("recipient_name", "unknown"))
            desc = c_value.get("description", "government contract")

            cause_desc = (
                f"${amount:,.0f} contract from {agency}" if amount
                else f"Contract from {agency}: {desc[:80]}"
            )

            causes.append(CausalLink(
                action_id=aid,
                actor=actor,
                action=action,
                ticker=ticker,
                action_date=str(act_date),
                probable_cause=cause_desc,
                cause_type="contract",
                evidence=[{
                    "type": "gov_contract",
                    "date": str(c_date),
                    "amount": amount,
                    "agency": agency,
                    "description": desc[:200],
                }],
                probability=round(prob, 3),
                lead_time_days=abs(lead_days),
            ))
    except Exception as exc:
        log.debug("Contract cause check failed for {t}: {e}", t=ticker, e=str(exc))

    return causes


def _check_legislation(
    engine: Engine, aid: str, actor: str, action: str, ticker: str, act_date: date,
) -> list[CausalLink]:
    """Check for legislation / bills affecting this ticker near the action date."""
    causes: list[CausalLink] = []
    window_start = act_date - timedelta(days=_PRE_WINDOW_DAYS)
    window_end = act_date + timedelta(days=_POST_WINDOW_DAYS)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT series_id, obs_date, raw_payload "
                    "FROM raw_series "
                    "WHERE series_id LIKE :pattern "
                    "AND obs_date BETWEEN :wstart AND :wend "
                    "AND pull_status = 'SUCCESS' "
                    "AND series_id NOT LIKE :hearing_exclude "
                    "ORDER BY obs_date"
                ),
                {
                    "pattern": "LEGISLATION:%",
                    "wstart": window_start,
                    "wend": window_end,
                    "hearing_exclude": "%:hearing",
                },
            ).fetchall()

        for row in rows:
            payload = _parse_json(row[2])
            affected = payload.get("affected_tickers", [])
            if ticker not in affected:
                continue

            obs_date = row[1] if isinstance(row[1], date) else date.fromisoformat(str(row[1])[:10])
            lead_days = (obs_date - act_date).days

            bill_id = payload.get("bill_id", "")
            title = payload.get("title", "")
            status = payload.get("status", "")

            if lead_days <= 0:
                prob = min(0.7, 0.3 + 0.4 * (1.0 - abs(lead_days) / _PRE_WINDOW_DAYS))
            else:
                prob = min(0.6, 0.2 + 0.4 * (1.0 - lead_days / _POST_WINDOW_DAYS))

            causes.append(CausalLink(
                action_id=aid,
                actor=actor,
                action=action,
                ticker=ticker,
                action_date=str(act_date),
                probable_cause=f"Bill {bill_id}: {title[:80]}" if title else f"Legislation {bill_id} ({status})",
                cause_type="legislation",
                evidence=[{
                    "type": "legislation",
                    "bill_id": bill_id,
                    "title": title[:200],
                    "status": status,
                    "date": str(obs_date),
                    "committees": payload.get("committees", []),
                }],
                probability=round(prob, 3),
                lead_time_days=abs(lead_days),
            ))
    except Exception as exc:
        log.debug("Legislation cause check failed for {t}: {e}", t=ticker, e=str(exc))

    return causes


def _check_hearings(
    engine: Engine, aid: str, actor: str, action: str, ticker: str, act_date: date,
) -> list[CausalLink]:
    """Check for committee hearings near the action date."""
    causes: list[CausalLink] = []
    window_start = act_date - timedelta(days=14)
    window_end = act_date + timedelta(days=14)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT series_id, obs_date, raw_payload "
                    "FROM raw_series "
                    "WHERE series_id LIKE :pattern "
                    "AND obs_date BETWEEN :wstart AND :wend "
                    "AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date"
                ),
                {"pattern": "LEGISLATION:%:hearing", "wstart": window_start, "wend": window_end},
            ).fetchall()

        for row in rows:
            payload = _parse_json(row[2])
            affected = payload.get("affected_tickers", [])
            if ticker not in affected:
                continue

            obs_date = row[1] if isinstance(row[1], date) else date.fromisoformat(str(row[1])[:10])
            lead_days = (obs_date - act_date).days

            title = payload.get("title", "Committee hearing")
            committees = payload.get("committees", [])

            prob = 0.5 if lead_days > 0 else 0.35
            if abs(lead_days) <= 3:
                prob += 0.2

            causes.append(CausalLink(
                action_id=aid,
                actor=actor,
                action=action,
                ticker=ticker,
                action_date=str(act_date),
                probable_cause=f"Hearing: {title[:80]} ({', '.join(committees[:2])})",
                cause_type="legislation",
                evidence=[{
                    "type": "hearing",
                    "title": title[:200],
                    "committees": committees,
                    "date": str(obs_date),
                }],
                probability=round(min(1.0, prob), 3),
                lead_time_days=abs(lead_days),
            ))
    except Exception as exc:
        log.debug("Hearing cause check failed for {t}: {e}", t=ticker, e=str(exc))

    return causes


def _check_earnings(
    engine: Engine, aid: str, actor: str, action: str, ticker: str, act_date: date,
) -> list[CausalLink]:
    """Check for earnings events near the action date."""
    causes: list[CausalLink] = []
    window_start = act_date - timedelta(days=30)
    window_end = act_date + timedelta(days=14)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT earnings_date, eps_estimate, eps_actual, "
                    "       eps_surprise_pct, classification "
                    "FROM earnings_calendar "
                    "WHERE ticker = :ticker "
                    "AND earnings_date BETWEEN :wstart AND :wend "
                    "ORDER BY earnings_date"
                ),
                {"ticker": ticker, "wstart": window_start, "wend": window_end},
            ).fetchall()

        for row in rows:
            e_date = row[0] if isinstance(row[0], date) else date.fromisoformat(str(row[0])[:10])
            eps_est = _safe_float(row[1])
            eps_act = _safe_float(row[2])
            surprise_pct = _safe_float(row[3])
            classification = row[4]

            lead_days = (e_date - act_date).days

            if lead_days > 0:
                prob = min(0.75, 0.35 + 0.4 * (1.0 - lead_days / 14))
                if action == "BUY" and surprise_pct and surprise_pct > 0:
                    prob = min(0.9, prob + 0.15)
                elif action == "SELL" and surprise_pct and surprise_pct < 0:
                    prob = min(0.9, prob + 0.15)
            else:
                prob = max(0.15, 0.3 - 0.2 * (abs(lead_days) / 30))

            if eps_act is not None and eps_est is not None:
                if eps_act > eps_est:
                    desc = f"Earnings beat ({eps_act:.2f} vs {eps_est:.2f} est)"
                elif eps_act < eps_est:
                    desc = f"Earnings miss ({eps_act:.2f} vs {eps_est:.2f} est)"
                else:
                    desc = f"Earnings inline ({eps_act:.2f})"
            else:
                desc = f"Earnings event on {e_date}" + (
                    f" ({classification})" if classification else ""
                )

            causes.append(CausalLink(
                action_id=aid,
                actor=actor,
                action=action,
                ticker=ticker,
                action_date=str(act_date),
                probable_cause=desc,
                cause_type="earnings",
                evidence=[{
                    "type": "earnings",
                    "date": str(e_date),
                    "eps_estimate": eps_est,
                    "eps_actual": eps_act,
                    "surprise_pct": surprise_pct,
                    "classification": classification,
                }],
                probability=round(prob, 3),
                lead_time_days=abs(lead_days),
            ))
    except Exception as exc:
        log.debug("Earnings cause check failed for {t}: {e}", t=ticker, e=str(exc))

    return causes


def _check_cluster_signals(
    engine: Engine, aid: str, actor: str, action: str, ticker: str, act_date: date,
) -> list[CausalLink]:
    """Check for other insider/congressional activity on the same ticker."""
    causes: list[CausalLink] = []
    window_start = act_date - timedelta(days=14)
    window_end = act_date + timedelta(days=3)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT source_type, source_id, signal_type, signal_date "
                    "FROM signal_sources "
                    "WHERE ticker = :ticker "
                    "AND source_type IN ('congressional', 'insider') "
                    "AND source_id != :actor "
                    "AND signal_date BETWEEN :wstart AND :wend "
                    "ORDER BY signal_date"
                ),
                {"ticker": ticker, "actor": actor, "wstart": window_start, "wend": window_end},
            ).fetchall()

        if len(rows) < 2:
            return causes

        same_direction = [r for r in rows if r[2] == action]
        cluster_size = len(same_direction)

        if cluster_size >= 2:
            actors_in_cluster = list({r[1] for r in same_direction})
            prob = min(0.8, 0.3 + 0.1 * cluster_size)

            causes.append(CausalLink(
                action_id=aid,
                actor=actor,
                action=action,
                ticker=ticker,
                action_date=str(act_date),
                probable_cause=(
                    f"Cluster {action}: {cluster_size} actors traded {ticker} "
                    f"in same direction within 14 days"
                ),
                cause_type="insider_knowledge",
                evidence=[{
                    "type": "cluster_signal",
                    "cluster_size": cluster_size,
                    "actors": actors_in_cluster[:10],
                    "direction": action,
                    "window_days": 14,
                }],
                probability=round(prob, 3),
                lead_time_days=0,
            ))
    except Exception as exc:
        log.debug("Cluster cause check failed for {t}: {e}", t=ticker, e=str(exc))

    return causes


def _check_macro_events(
    engine: Engine, aid: str, actor: str, action: str, ticker: str, act_date: date,
) -> list[CausalLink]:
    """Check for macro events (FOMC, CPI, payrolls) near the action date."""
    causes: list[CausalLink] = []
    window_start = act_date - timedelta(days=7)
    window_end = act_date + timedelta(days=7)

    try:
        for pattern in _MACRO_SERIES_PATTERNS:
            with engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT series_id, obs_date, value "
                        "FROM raw_series "
                        "WHERE series_id LIKE :pattern "
                        "AND obs_date BETWEEN :wstart AND :wend "
                        "AND pull_status = 'SUCCESS' "
                        "ORDER BY obs_date DESC "
                        "LIMIT 3"
                    ),
                    {"pattern": pattern + "%", "wstart": window_start, "wend": window_end},
                ).fetchall()

            for row in rows:
                obs_date = row[1] if isinstance(row[1], date) else date.fromisoformat(str(row[1])[:10])
                lead_days = (obs_date - act_date).days
                series = row[0]

                prob = 0.25
                if abs(lead_days) <= 2:
                    prob = 0.35

                macro_name = _macro_series_to_name(series)

                causes.append(CausalLink(
                    action_id=aid,
                    actor=actor,
                    action=action,
                    ticker=ticker,
                    action_date=str(act_date),
                    probable_cause=f"Macro release: {macro_name} on {obs_date}",
                    cause_type="rebalancing",
                    evidence=[{
                        "type": "macro_event",
                        "series": series,
                        "name": macro_name,
                        "date": str(obs_date),
                        "value": float(row[2]) if row[2] else None,
                    }],
                    probability=round(prob, 3),
                    lead_time_days=abs(lead_days),
                ))
    except Exception as exc:
        log.debug("Macro cause check failed for {t}: {e}", t=ticker, e=str(exc))

    return causes


# ── Suspicious Trade Helpers ─────────────────────────────────────────────


def _has_legislation_overlap(
    engine: Engine, ticker: str, act_date: date, committee: str,
) -> dict | None:
    """Check if there's active legislation for this ticker near the date."""
    window_start = act_date - timedelta(days=30)
    window_end = act_date + timedelta(days=14)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT series_id, obs_date, raw_payload "
                    "FROM raw_series "
                    "WHERE series_id LIKE :pattern "
                    "AND obs_date BETWEEN :wstart AND :wend "
                    "AND pull_status = 'SUCCESS' "
                    "ORDER BY obs_date DESC "
                    "LIMIT 5"
                ),
                {"pattern": "LEGISLATION:%", "wstart": window_start, "wend": window_end},
            ).fetchall()

        for row in rows:
            payload = _parse_json(row[2])
            affected = payload.get("affected_tickers", [])
            leg_committees = [c.lower() for c in payload.get("committees", [])]

            ticker_match = ticker in affected
            committee_match = committee and any(
                committee.lower() in c for c in leg_committees
            )

            if ticker_match or committee_match:
                return {
                    "type": "legislation_overlap",
                    "bill_id": payload.get("bill_id", ""),
                    "title": payload.get("title", "")[:200],
                    "date": str(row[1]),
                    "ticker_match": ticker_match,
                    "committee_match": committee_match,
                }
    except Exception as exc:
        log.debug("Causation: legislation overlap query failed: {e}", e=str(exc))

    return None


def _has_contract_award(
    engine: Engine, ticker: str, act_date: date, window_days: int = 30,
) -> dict | None:
    """Check if a contract was awarded to this company within window_days after the trade."""
    window_end = act_date + timedelta(days=window_days)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT signal_date, signal_value "
                    "FROM signal_sources "
                    "WHERE source_type = 'gov_contract' "
                    "AND ticker = :ticker "
                    "AND signal_date BETWEEN :act_date AND :wend "
                    "ORDER BY signal_date "
                    "LIMIT 3"
                ),
                {"ticker": ticker, "act_date": act_date, "wend": window_end},
            ).fetchall()

        if rows:
            c_value = _parse_json(rows[0][1])
            return {
                "type": "contract_award",
                "date": str(rows[0][0]),
                "amount": c_value.get("amount", 0),
                "agency": c_value.get("awarding_agency", ""),
                "days_after_trade": (rows[0][0] - act_date).days if isinstance(rows[0][0], date) else None,
            }
    except Exception as exc:
        log.debug("Causation: gov contracts query failed: {e}", e=str(exc))

    return None


def _has_earnings_miss(
    engine: Engine, ticker: str, act_date: date, window_days: int = 14,
) -> dict | None:
    """Check if there was an earnings miss within window_days after a SELL."""
    window_end = act_date + timedelta(days=window_days)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT earnings_date, eps_estimate, eps_actual, eps_surprise_pct "
                    "FROM earnings_calendar "
                    "WHERE ticker = :ticker "
                    "AND earnings_date BETWEEN :act_date AND :wend "
                    "AND eps_actual IS NOT NULL "
                    "AND eps_surprise_pct < 0 "
                    "ORDER BY earnings_date "
                    "LIMIT 1"
                ),
                {"ticker": ticker, "act_date": act_date, "wend": window_end},
            ).fetchall()

        if rows:
            return {
                "type": "earnings_miss",
                "date": str(rows[0][0]),
                "eps_estimate": _safe_float(rows[0][1]),
                "eps_actual": _safe_float(rows[0][2]),
                "surprise_pct": _safe_float(rows[0][3]),
            }
    except Exception as exc:
        log.debug("Causation: earnings catalyst query failed: {e}", e=str(exc))

    return None


def _committee_has_jurisdiction(committee: str, ticker: str) -> bool:
    """Check if a congressional committee has jurisdiction over a ticker's sector."""
    try:
        from intelligence.lever_pullers import SECTOR_COMMITTEE_MAP
        committee_lower = committee.lower()
        for _sector_etf, committees in SECTOR_COMMITTEE_MAP.items():
            if any(c in committee_lower for c in committees):
                return True
    except ImportError:
        pass
    return False


# ── Storage ──────────────────────────────────────────────────────────────


def _store_causal_links(engine: Engine, causes: list[CausalLink]) -> None:
    """Persist CausalLink objects to the causal_links table."""
    ensure_table(engine)

    with engine.begin() as conn:
        for c in causes:
            try:
                sig_id = int(c.action_id)
            except (ValueError, TypeError):
                sig_id = None

            conn.execute(
                text(
                    "INSERT INTO causal_links "
                    "(signal_id, actor, ticker, action_date, cause_type, "
                    " probable_cause, evidence, probability) "
                    "VALUES (:sig_id, :actor, :ticker, :action_date, :cause_type, "
                    "        :probable_cause, :evidence, :probability)"
                ),
                {
                    "sig_id": sig_id,
                    "actor": c.actor,
                    "ticker": c.ticker,
                    "action_date": c.action_date,
                    "cause_type": c.cause_type,
                    "probable_cause": c.probable_cause,
                    "evidence": json.dumps(c.evidence),
                    "probability": c.probability,
                },
            )

    log.info("Stored {n} causal links", n=len(causes))


# ── Narrative Helpers ────────────────────────────────────────────────────


def _summarize_causes(causes: list[CausalLink]) -> dict[str, dict]:
    """Group causes by type and pick the top one for each."""
    summary: dict[str, dict] = {}
    by_type: dict[str, list[CausalLink]] = {}

    for c in causes:
        by_type.setdefault(c.cause_type, []).append(c)

    for ctype, items in by_type.items():
        items.sort(key=lambda x: x.probability, reverse=True)
        summary[ctype] = {
            "count": len(items),
            "top": items[0].probable_cause if items else "",
            "max_probability": items[0].probability if items else 0,
        }

    return summary


def _try_llm_narrative(
    ticker: str,
    signals: list,
    causes: list[CausalLink],
) -> str | None:
    """Attempt LLM-based narrative generation. Returns None if unavailable."""
    try:
        from ollama.client import get_client
        client = get_client()
    except Exception:
        client = None

    if client is None:
        return None

    # Build prompt
    signal_lines = []
    for s in signals[:15]:
        signal_lines.append(
            f"  - {s[0]} {s[1]}: {s[2]} on {s[3]}"
        )

    cause_lines = []
    for c in causes[:15]:
        cause_lines.append(
            f"  - [{c.cause_type}] {c.probable_cause} (prob={c.probability:.2f}, "
            f"lead={c.lead_time_days:.0f}d)"
        )

    # RAG: retrieve historical context for causal analysis
    rag_context = ""
    try:
        from intelligence.rag import get_rag_context
        from db import get_engine as _get_engine
        rag_query = f"{ticker} causal analysis trading activity signals"
        rag_context = get_rag_context(_get_engine(), rag_query, top_k=5, max_chars=2000)
    except Exception as exc:
        log.debug("Causation: RAG context retrieval failed: {e}", e=str(exc))

    prompt = (
        f"You are a financial intelligence analyst. Explain concisely why people "
        f"are trading {ticker} right now. Be specific, cite evidence, and flag "
        f"anything suspicious.\n\n"
    )
    if rag_context:
        prompt += f"{rag_context}\n"
    prompt += (
        f"Recent trading signals:\n"
        + "\n".join(signal_lines)
        + "\n\nProbable causes:\n"
        + "\n".join(cause_lines)
        + "\n\nWrite 3-5 sentences. Reference historical patterns if relevant. "
        f"Be direct. No disclaimers."
    )

    try:
        response = client.generate(
            model="hermes",
            prompt=prompt,
            options={"temperature": 0.4, "num_predict": 400},
        )
        result = response.get("response", "").strip()
        return result if result else None
    except Exception as exc:
        log.debug("LLM causal narrative failed: {e}", e=str(exc))
        return None
