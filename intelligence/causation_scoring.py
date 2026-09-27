"""
GRID Intelligence — Causal Connection Engine (scoring module).

Single-hop "what public event preceded this trade" lookups, suspicious
trade detection and narrative generation.

Since slice N2 (2026-09-27) ``find_causes`` / ``batch_find_causes`` are thin
adapters over :mod:`intelligence.causal_links`, which only links events that
were public BEFORE the trade day and stamps every edge with known_at. The
previous checks (post-trade events scored as causes, macro series that
matched nothing, same-direction co-trading labelled ``insider_knowledge``,
committee-basket legislation matches) were removed; see that module.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from intelligence import causal_links as _cl
from intelligence.causation_core import (
    CausalLink,
    _parse_json,
    _safe_float,
)


# ── 1. find_causes ───────────────────────────────────────────────────────


def _edge_to_causal_link(
    aid: str, actor: str, action: str, ev: "_cl.AntecedentEvent", lead: float,
    act_date: date, action_known_at: str | None = None, known_at: str | None = None,
    edge_key: str | None = None,
) -> CausalLink:
    return CausalLink(
        action_id=aid,
        actor=actor,
        action=action,
        ticker=ev.ticker,
        action_date=str(act_date),
        probable_cause=ev.description,
        cause_type=ev.kind,
        evidence=[{
            "type": ev.kind,
            "event_key": ev.key,
            "date": ev.event_date.isoformat(),
            "event_known_at": ev.known_at.isoformat(),
            "event_known_at_basis": ev.known_at_basis,
            "claim": "public event preceded the trade; not proof of cause",
            **ev.evidence,
        }],
        probability=_cl.recency_score(lead, ev.window_days),
        lead_time_days=round(lead, 3),
        event_date=ev.event_date.isoformat(),
        event_known_at=ev.known_at.isoformat(),
        event_known_at_basis=ev.known_at_basis,
        action_known_at=action_known_at,
        known_at=known_at,
        score_method=_cl.SCORE_METHOD,
        edge_key=edge_key,
    )


def find_causes(
    engine: Engine,
    actor: str,
    action: str,
    ticker: str,
    action_date: str,
    signal_id: int | str | None = None,
) -> list[CausalLink]:
    """Public events on ``ticker`` that were knowable before ``action_date``.

    Only events whose known_at is at or before the start of the trade day
    are returned (earnings releases; contract awards GRID had already seen).
    ``probability`` is the heuristic recency score (``score_method``), not a
    calibrated probability. Read-only.

    Returns:
        List of CausalLink sorted by score descending.
    """
    try:
        act_date = date.fromisoformat(str(action_date)[:10])
    except (ValueError, TypeError):
        log.warning("Invalid action_date: {d}", d=action_date)
        return []

    ticker = (ticker or "").strip().upper()
    if not ticker:
        return []
    aid = str(signal_id) if signal_id else f"{actor}:{ticker}:{action_date}"
    as_of = datetime.now(timezone.utc)
    trade_start = _cl.day_start(act_date)

    try:
        with engine.connect() as conn:
            earn_rows, contract_rows = _cl.load_event_rows(conn, [ticker], act_date, as_of)
    except Exception as exc:
        log.debug("find_causes: event load failed for {t}: {e}", t=ticker, e=str(exc))
        return []

    causes: list[CausalLink] = []
    for ev in _cl.earnings_events(earn_rows) + _cl.contract_events(contract_rows):
        if ev.known_at > trade_start or ev.known_at > as_of:
            continue
        lead = (trade_start - ev.known_at).total_seconds() / 86400.0
        if lead > ev.window_days:
            continue
        causes.append(_edge_to_causal_link(aid, actor, action, ev, lead, act_date))

    causes.sort(key=lambda c: c.probability, reverse=True)
    log.debug(
        "Causation: {n} preceding events for {a} {act} {t} on {d}",
        n=len(causes), a=actor, act=action, t=ticker, d=action_date,
    )
    return causes


# ── 2. batch_find_causes ─────────────────────────────────────────────────


def batch_find_causes(
    engine: Engine, days: int = 30, persist: bool = True,
) -> list[CausalLink]:
    """Antecedent edges for every recent trade (bounded batches).

    Parameters:
        engine: SQLAlchemy engine.
        days: How far back to look for trades.
        persist: When True (default), upsert into ``causal_links`` with a
            ``causal_link_runs`` record (requires the
            ``causal_links_provenance_20260927`` migration). False computes
            without writing.

    Returns:
        All edges as CausalLink objects.
    """
    summary = _cl.run_causal_links(
        engine,
        days=days,
        code_sha=_cl.resolve_code_sha(),
        dry_run=not persist,
        keep_edges=True,
    )
    links = [
        _edge_to_causal_link(
            str(e.action.source_refs[0]) if e.action.source_refs else e.action.actor_key,
            e.action.actor, e.action.direction, e.event, e.lead_time_days,
            e.action.action_date,
            action_known_at=e.action.known_at.isoformat(),
            known_at=e.known_at.isoformat(),
            edge_key=e.edge_key,
        )
        for e in summary.edges
    ]
    log.info(
        "batch_find_causes: {n} edges from {a} trades ({s})",
        n=len(links), a=summary.actions_processed, s=summary.status,
    )
    return links


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
    """Narrative text only; see :func:`generate_causal_narrative_with_source`."""
    return generate_causal_narrative_with_source(engine, ticker)[0]


def generate_causal_narrative_with_source(engine: Engine, ticker: str) -> tuple[str, str]:
    """Generate an LLM or rule-based narrative about recent trading activity.

    Returns ``(text, source)`` with source ``'llm'``, ``'rule_based'`` or
    ``'none'`` so callers can label LLM prose as interpretation, not fact.
    The events it cites are only those public before each trade.
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
        return f"No recent trading activity found for {ticker}.", "none"

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

    # ── Actor graph enrichment ──────────────────────────────────
    # Pull actor intelligence: who these people are, their track
    # records, and what else they're connected to.
    actor_context: list[dict] = []
    try:
        from intelligence.actor_signal_bridge import get_actor_context_for_causation
        actor_context = get_actor_context_for_causation(engine, ticker, days=30)
    except Exception as exc:
        log.debug("Actor context enrichment skipped for {t}: {e}", t=ticker, e=str(exc))

    cause_summary = _summarize_causes(causes)

    # Try LLM
    llm_narrative = _try_llm_narrative(ticker, rows, causes)
    if llm_narrative:
        return llm_narrative, "llm"

    # Rule-based fallback
    lines: list[str] = []
    lines.append(f"## Recent Trading in {ticker}")
    lines.append("")
    lines.append(
        f"In the last 30 days: {len(buys)} buy signal(s), {len(sells)} sell signal(s) "
        f"from {len(actors)} actor(s) across {', '.join(source_types)} sources."
    )

    if cause_summary.get("contract"):
        lines.append(
            f"\n**Government Contracts:** {cause_summary['contract']['count']} contract award(s) "
            f"GRID had seen before a trade. {cause_summary['contract']['top']}"
        )
    if cause_summary.get("earnings"):
        lines.append(
            f"\n**Earnings:** {cause_summary['earnings']['count']} earnings release(s) "
            f"public before a trade. {cause_summary['earnings']['top']}"
        )
    # ── Actor intelligence section ──
    if actor_context:
        lines.append(f"\n**Key Actors ({len(actor_context)}):**")
        for ac in actor_context[:5]:
            trust_pct = int(ac.get("trust_score", 0.5) * 100)
            inf_pct = int(ac.get("influence_score", 0.3) * 100)
            conns = ac.get("other_connections", [])
            conn_str = ""
            if conns:
                conn_str = " | Also connected to: " + ", ".join(
                    f"{c['target']} ({c['relationship']})" for c in conns[:3]
                )
            lines.append(
                f"- **{ac['actor']}** ({ac.get('category', '?')}, {ac.get('tier', '?')}) "
                f"→ {ac['direction']} via {ac['signal_type']} on {ac['signal_date']} "
                f"[trust={trust_pct}%, influence={inf_pct}%]{conn_str}"
            )

    if causes:
        lines.append(
            "\nThese events were public before the trades; timing alone does not "
            "establish that they caused them."
        )
    else:
        lines.append(
            "\nNo public earnings release or known contract award preceded these "
            "trades within the lookback windows."
        )

    # Top actors
    if actors[:5]:
        lines.append(f"\n**Key actors:** {', '.join(actors[:5])}")

    return "\n".join(lines), "rule_based"


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
        from llm.router import get_llm, Tier
        client = get_llm(Tier.REASON)
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
            f"  - [{c.cause_type}] {c.probable_cause} (score={c.probability:.2f}, "
            f"lead={c.lead_time_days:.0f}d)"
        )

    # RAG: retrieve historical context for causal analysis
    rag_context = ""
    try:
        from intelligence.rag import get_rag_context
        from db import get_engine as _get_engine
        rag_query = f"{ticker} causal analysis trading activity signals"
        rag_context = get_rag_context(_get_engine(), rag_query, top_k=5, max_chars=1500)
    except Exception as exc:
        log.debug("Causation: RAG context retrieval failed: {e}", e=str(exc))

    prompt = (
        f"You are a financial intelligence analyst. Explain concisely why people "
        f"are trading {ticker} right now. Name the specific actor and the liquidity "
        f"valve they are operating — do not merely list conditions.\n\n"
        f"Separate LEVERS (actor + action + valve) from CONDITIONS (volume, "
        f"volatility, sentiment that amplify but do not cause). If you cannot "
        f"name a lever, say so explicitly.\n\n"
    )
    if rag_context:
        prompt += f"{rag_context}\n"
    prompt += (
        "Recent trading signals:\n"
        + "\n".join(signal_lines)
        + "\n\nPublic events that preceded the trades (timing only, not established causes):\n"
        + "\n".join(cause_lines)
        + "\n\nFor each cause, state:\n"
        "LEVER: [Who] did [what] affecting [which valve]\n"
        "CONDITION: [Environmental factor] that amplifies/dampens the lever\n\n"
        "Write 3-5 sentences. Reference historical patterns if relevant. "
        "Be direct, but do not state that any event caused a trade: these are "
        "time-ordered co-occurrences, not established causes."
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
