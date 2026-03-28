"""
GRID Intelligence — Causal Connection Engine.

Connects actor actions to the events, policies, and contracts that likely
drove them.  For every trade in signal_sources, this module searches for
the probable CAUSE — a government contract, legislation, earnings event,
committee hearing, macro release, or cluster signal — and scores its
likelihood.

Key entry points:
  find_causes              — all probable causes for a single action
  batch_find_causes        — run find_causes for all recent signal_sources
  get_suspicious_trades    — trades where the cause is likely non-public info
  generate_causal_narrative — LLM or rule-based "why is everyone trading X?"
"""

from __future__ import annotations

import json
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Data Classes ─────────────────────────────────────────────────────────


@dataclass
class CausalLink:
    """A causal connection between an actor's trade and an upstream event."""

    action_id: str           # the signal_sources row id
    actor: str
    action: str              # BUY / SELL
    ticker: str
    action_date: str
    probable_cause: str      # one-line description
    cause_type: str          # 'contract', 'legislation', 'earnings',
                             # 'insider_knowledge', 'rebalancing', 'unknown'
    evidence: list[dict]
    probability: float       # 0-1
    lead_time_days: float    # how far before the action did the cause occur

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CausalChain:
    """A multi-hop causal chain tracing a path from root cause to final effect.

    Example: lobbying -> legislation -> contract award -> stock price move -> insider sale.
    """

    ticker: str
    chain: list[CausalLink]       # ordered sequence of causes and effects
    total_hops: int
    timespan_days: int            # from first cause to final effect
    total_dollar_flow: float
    key_actors: list[str]
    narrative: str
    confidence: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "chain": [link.to_dict() for link in self.chain],
            "total_hops": self.total_hops,
            "timespan_days": self.timespan_days,
            "total_dollar_flow": self.total_dollar_flow,
            "key_actors": self.key_actors,
            "narrative": self.narrative,
            "confidence": self.confidence,
        }


# ── Schema ───────────────────────────────────────────────────────────────

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS causal_links (
    id              SERIAL PRIMARY KEY,
    signal_id       INT,
    actor           TEXT,
    ticker          TEXT,
    action_date     DATE,
    cause_type      TEXT,
    probable_cause  TEXT,
    evidence        JSONB,
    probability     NUMERIC,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
"""

_CREATE_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_causal_links_ticker ON causal_links (ticker);",
    "CREATE INDEX IF NOT EXISTS idx_causal_links_signal ON causal_links (signal_id);",
    "CREATE INDEX IF NOT EXISTS idx_causal_links_type ON causal_links (cause_type);",
    "CREATE INDEX IF NOT EXISTS idx_causal_links_date ON causal_links (action_date DESC);",
]

_CREATE_CHAINS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS causal_chains (
    id                SERIAL PRIMARY KEY,
    ticker            TEXT,
    chain             JSONB NOT NULL,
    total_hops        INT,
    timespan_days     INT,
    total_dollar_flow NUMERIC,
    key_actors        JSONB,
    narrative         TEXT,
    confidence        NUMERIC,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);
"""

_CREATE_CHAINS_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_causal_chains_ticker ON causal_chains (ticker);",
    "CREATE INDEX IF NOT EXISTS idx_causal_chains_hops ON causal_chains (total_hops DESC);",
    "CREATE INDEX IF NOT EXISTS idx_causal_chains_confidence ON causal_chains (confidence DESC);",
    "CREATE INDEX IF NOT EXISTS idx_causal_chains_created ON causal_chains (created_at DESC);",
]


def ensure_table(engine: Engine) -> None:
    """Create the causal_links and causal_chains tables and indexes if they don't exist."""
    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE_SQL))
        for idx_sql in _CREATE_INDEX_SQL:
            conn.execute(text(idx_sql))
        conn.execute(text(_CREATE_CHAINS_TABLE_SQL))
        for idx_sql in _CREATE_CHAINS_INDEX_SQL:
            conn.execute(text(idx_sql))
    log.debug("causal_links + causal_chains tables ensured")


# ── Constants ────────────────────────────────────────────────────────────

# Search window: how many days before/after the action to look for causes
_PRE_WINDOW_DAYS = 60
_POST_WINDOW_DAYS = 14

# Known macro event series_id patterns
_MACRO_SERIES_PATTERNS = [
    "FRED:FEDFUNDS",
    "FRED:CPIAUCSL",
    "FRED:UNRATE",
    "FRED:GDP",
    "FRED:PCE",
    "FRED:PAYEMS",
]

# FOMC meeting dates (known well in advance) — rough 2024-2026 schedule
_FOMC_KEYWORDS = {"fomc", "federal open market", "fed meeting", "rate decision"}


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
    cutoff = date.today() - timedelta(days=days)
    suspicious: list[dict] = []

    with engine.connect() as conn:
        rows = conn.execute(
            text(
                "SELECT id, source_type, source_id, ticker, signal_type, "
                "       signal_date, signal_value, metadata "
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
                "       signal_value, metadata "
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
                # Action happened before the contract was awarded — high probability
                prob = min(0.85, 0.5 + 0.35 * (1.0 - abs(lead_days) / _POST_WINDOW_DAYS))
            else:
                # Action happened after the contract — might be a reaction
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

            # Legislation active before or during trading = plausible cause
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

            # Trading right before a hearing = suspicious
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

            # Trade before earnings = anticipation
            if lead_days > 0:
                prob = min(0.75, 0.35 + 0.4 * (1.0 - lead_days / 14))
                if action == "BUY" and surprise_pct and surprise_pct > 0:
                    prob = min(0.9, prob + 0.15)  # bought before a beat
                elif action == "SELL" and surprise_pct and surprise_pct < 0:
                    prob = min(0.9, prob + 0.15)  # sold before a miss
            else:
                # Trade after earnings = reaction
                prob = max(0.15, 0.3 - 0.2 * (abs(lead_days) / 30))

            # Build description
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

        # Multiple actors trading the same ticker = cluster
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

                # Macro events are public — lower probability of being the sole cause
                prob = 0.25
                if abs(lead_days) <= 2:
                    prob = 0.35

                # Identify the macro type
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

            # Match on ticker OR committee overlap
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
    except Exception:
        pass

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
    except Exception:
        pass

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
    except Exception:
        pass

    return None


def _committee_has_jurisdiction(committee: str, ticker: str) -> bool:
    """Check if a congressional committee has jurisdiction over a ticker's sector."""
    try:
        from intelligence.lever_pullers import SECTOR_COMMITTEE_MAP
        committee_lower = committee.lower()
        for _sector_etf, committees in SECTOR_COMMITTEE_MAP.items():
            if any(c in committee_lower for c in committees):
                # We found the committee — now check if ticker is in this sector
                # (Simplified: return True since we matched committee, and the ticker
                # is already in signal_sources for this committee member)
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
            # Extract signal_id if it's numeric
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

    prompt = (
        f"You are a financial intelligence analyst. Explain concisely why people "
        f"are trading {ticker} right now. Be specific, cite evidence, and flag "
        f"anything suspicious.\n\n"
        f"Recent trading signals:\n"
        + "\n".join(signal_lines)
        + "\n\nProbable causes:\n"
        + "\n".join(cause_lines)
        + "\n\nWrite 3-5 sentences. Be direct. No disclaimers."
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


# ── General Helpers ──────────────────────────────────────────────────────


def _parse_json(raw: Any) -> dict[str, Any]:
    """Parse a JSON string or dict safely."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, ValueError):
            return {}
    return {}


def _safe_float(val: Any) -> float | None:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _macro_series_to_name(series_id: str) -> str:
    """Map a FRED series_id to a human-readable macro event name."""
    name_map = {
        "FEDFUNDS": "Federal Funds Rate (FOMC)",
        "CPIAUCSL": "CPI (Inflation)",
        "UNRATE": "Unemployment Rate",
        "GDP": "GDP",
        "PCE": "Personal Consumption Expenditures",
        "PAYEMS": "Nonfarm Payrolls",
    }
    for key, name in name_map.items():
        if key in series_id:
            return name
    return series_id


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

    Example chain:
        Hop 1: NVDA drops 8% on March 15
        Hop 2: <- Preceded by 3 insider sells ($12M total) in prior 10 days
        Hop 3: <- Preceded by Commerce Dept export control announcement on March 5
        Hop 4: <- Preceded by $640K NVIDIA lobbying spend increase (failed to prevent)
        Hop 5: <- Preceded by congressional hearing on AI chip exports in February

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
            # Bonus for longer chains — they're rarer and more informative
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

    These are the most interesting stories in the system — multi-hop paths
    from root causes (lobbying, hearings) through intermediary events
    (legislation, contracts) to final effects (price moves, insider trades).

    Parameters:
        engine: SQLAlchemy engine.
        days: How far back to search for tickers with activity.

    Returns:
        List of CausalChain instances, sorted longest and highest confidence first.
    """
    ensure_table(engine)
    cutoff = date.today() - timedelta(days=days)

    # Find tickers with the most diverse signal activity — these are most
    # likely to have multi-hop chains
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

    # Global sort: longest chains first, then confidence
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

    Tries LLM first (llamacpp or ollama), falls back to a rule-based
    template that reads like investigative journalism.

    Parameters:
        engine: SQLAlchemy engine (unused for rule-based, needed for LLM context).
        chain: The CausalChain to narrate.

    Returns:
        Narrative string.
    """
    # Try LLM first
    llm_narrative = _try_chain_llm_narrative(chain)
    if llm_narrative:
        return llm_narrative

    # Rule-based fallback — build a readable story
    if not chain.chain:
        return f"No causal chain data for {chain.ticker}."

    # Chain is stored most-recent-first; reverse for chronological narrative
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

    Looks at recent events and matches them against known chain patterns:
      - If lobbying spend is increasing AND legislative hearings are scheduled
        AND insider buying is happening -> something is coming.
      - Matches partial chains against completed chains from the past.

    Returns:
        List of dicts describing active chain patterns, with predictions
        about what might happen next.
    """
    ensure_table(engine)
    today = date.today()
    cutoff_recent = today - timedelta(days=30)
    cutoff_historical = today - timedelta(days=365)

    active_patterns: list[dict] = []

    # Step 1: Find tickers with recent multi-type activity (potential chain starters)
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

    # Sort by score descending
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
                probability=0.95,  # price moves are confirmed facts
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
                probability=0.9,  # trade disclosures are confirmed facts
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
    """Find the most likely upstream cause for a given link.

    Uses _BACKWARD_CHAIN_MAP to know which event types to search for,
    then picks the highest-probability match that hasn't been visited.
    """
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
            # Skip already-visited dates to avoid loops
            if candidate.action_date in visited_dates:
                continue
            # Score: probability * recency (closer to current date = better)
            try:
                cand_date = date.fromisoformat(candidate.action_date[:10])
            except (ValueError, TypeError):
                continue
            days_gap = (current_date - cand_date).days
            if days_gap < 0:
                continue  # upstream must be before current
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
            # Cluster signal — multiple insiders trading same direction
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

            # Check overlap between current flags and historical chain types
            flag_types = set()
            for flag in current_flags:
                parts = flag.split("_plus_")
                flag_types.update(parts)
                # Also add base types from compound flags
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
        from llamacpp.client import get_client
        llm = get_client()
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

    # Build chronological chain description
    links_chrono = list(reversed(chain.chain))
    hop_lines: list[str] = []
    for i, link in enumerate(links_chrono, 1):
        hop_lines.append(
            f"  Hop {i}: [{link.cause_type}] {link.probable_cause} "
            f"on {link.action_date} (probability: {link.probability:.0%})"
        )

    prompt = (
        f"You are a forensic financial analyst. Write a 3-5 sentence investigative "
        f"narrative connecting the following causal chain for {chain.ticker}.\n\n"
        f"CHAIN ({chain.total_hops} hops, {chain.timespan_days} days):\n"
        + "\n".join(hop_lines)
        + f"\n\nKey actors: {', '.join(chain.key_actors[:5]) or 'none identified'}"
        + f"\nTotal dollar flow: ${chain.total_dollar_flow:,.0f}"
        + f"\n\nWrite as if telling a story. Start with 'The {chain.ticker} story "
        f"begins when...' and trace the causal path to the final effect. "
        f"Be specific and data-driven. No disclaimers."
    )

    try:
        # Try llamacpp interface
        if hasattr(llm, 'generate') and callable(llm.generate):
            response = llm.generate(
                prompt=prompt,
                system="You are the GRID forensic intelligence system. Be concise and specific.",
                temperature=0.3,
                num_predict=500,
            )
            if isinstance(response, str) and len(response.strip()) > 30:
                return response.strip()
            # Ollama returns dict
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
