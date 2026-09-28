"""
GRID — Automated Daily Market Diary.

Each trading day, the system writes a diary entry — a structured research
note covering what happened, who was active, what the data shows, pre-open
thesis accuracy, and what to watch tomorrow. The LLM narrates while the
data sections are rule-based, and the narrative is instructed to say so
when a driver or verdict isn't actually in the supplied data rather than
invent one (Wave 3 §4.1 fix).

Triggered today only via ``POST intelligence/diary/generate``
(``api/routers/intelligence_thesis.py``) or ``scripts/run_market_diary.py``
(supports ``--dry-run``). ``schedule_daily_diary`` below (a 22:00 UTC
thread) is defined but not called from anywhere — see
``deploy/systemd/grid-market-diary.timer.template`` for the reviewed,
not-yet-installed weekday 22:30Z alternative.

Price reads are gated by ``GRID_MARKET_DIARY_PRICES_ENABLED`` (default
off) pending operator confirmation of the YF quarantine; see the comment
above ``PRICES_ENABLED`` below.

Usage::

    from intelligence.market_diary import write_diary_entry, get_diary_entry

    # Generate today's diary
    result = write_diary_entry(engine)

    # Dry run — compute and render, do not persist
    result = write_diary_entry(engine, dry_run=True)

    # Retrieve a past entry
    entry = get_diary_entry(engine, date(2026, 3, 27))
"""

from __future__ import annotations

import json
import os
from datetime import date, datetime, time, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from store.observations import read_latest_n

# ──────────────────────────────────────────────────────────────────
# Price-freshness gate (Wave 3 §4.1 fix #2)
# ──────────────────────────────────────────────────────────────────
#
# ``read_latest_n`` is already SUCCESS-only, vintage-collapsed and carries
# an ``as_of`` bound (store/observations.py) — but it must only feed this
# diary once the "YF quarantine" (migration raw_series_quarantined_20260926,
# which lets a once-accepted row be marked QUARANTINED and excluded) has
# actually been run against the contaminated raw_series batches. Whether
# that quarantine run has happened is an operator/host fact this worktree
# cannot see. Mirrors the module-local env-bool "wait for infra" gate used
# by intelligence/edge_signals.py (``GRID_EDGE_SIGNALS_ENABLED``): defaults
# OFF so un-holding this job does not silently start reporting prices
# before the operator has confirmed the quarantine landed. Flip on with
# ``GRID_MARKET_DIARY_PRICES_ENABLED=true`` once confirmed.
#
# This flag is the operator's explicit "go" — it is NOT, by itself, what
# makes a flip-on safe. The obs_date freshness check in
# ``_gather_market_moves`` (every price read requires
# ``rows[0].obs_date == target_date`` or the entry is refused, never
# reported from a stale/wrong vintage) is the actual safety mechanism, and
# it applies whether or not the quarantine is complete.


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "")
    if not raw.strip():
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on", "enabled")


PRICES_ENABLED: bool = _env_bool("GRID_MARKET_DIARY_PRICES_ENABLED", False)
PRICE_BASIS = "raw_close"  # YF:*:close is pulled with auto_adjust=False (ingestion/yfinance_pull.py)
_PRICE_SOURCE = "yfinance"

# ──────────────────────────────────────────────────────────────────
# Schema
# ──────────────────────────────────────────────────────────────────

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS market_diary (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL UNIQUE,
    content TEXT NOT NULL,
    market_moves JSONB,
    active_actors JSONB,
    thesis_accuracy JSONB,
    narrative_model TEXT,
    narrative_fallback BOOLEAN NOT NULL DEFAULT FALSE,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# Added after the table's first release (2026-04) — a pre-existing table
# in production predates these columns, so add them on top rather than
# assuming CREATE TABLE IF NOT EXISTS filled them in.
_ENSURE_COLUMNS_SQL: tuple[str, ...] = (
    "ALTER TABLE market_diary ADD COLUMN IF NOT EXISTS narrative_model TEXT",
    "ALTER TABLE market_diary ADD COLUMN IF NOT EXISTS narrative_fallback BOOLEAN NOT NULL DEFAULT FALSE",
)


def ensure_table(engine: Engine) -> None:
    """Create the market_diary table if it does not exist."""
    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE))
    _ensure_diary_columns(engine)


def _ensure_diary_columns(engine: Engine) -> None:
    """Add narrative_model/narrative_fallback if this table predates them.

    Idempotent; never raises (mirrors intelligence/trial_outcomes.py's
    ``ensure_outcome_columns``) — each ALTER runs in its own transaction so
    a permission error on one does not poison ``ensure_table``'s CREATE
    TABLE or block reads/writes of the columns that already exist.
    """
    for ddl in _ENSURE_COLUMNS_SQL:
        try:
            with engine.begin() as conn:
                conn.execute(text(ddl))
        except Exception as exc:  # noqa: BLE001
            log.warning("market_diary: ensure_table column add failed: {e}", e=str(exc))


# ──────────────────────────────────────────────────────────────────
# Data gatherers (rule-based sections)
# ──────────────────────────────────────────────────────────────────

def _fresh_pair(conn: Any, series_id: str, target_date: date) -> tuple[Any, Any] | None:
    """The (today, prior) Observation pair for ``series_id``, or None.

    Requires >= 2 accepted observation dates AND the newest one dated
    exactly ``target_date`` — a stale or wrong-vintage read (the newest
    accepted row dated before ``target_date``, e.g. because today hasn't
    pulled yet, or a re-pull landed on a different day) is refused rather
    than silently reported as "today's" move. See §4.1 fix #2(b).
    """
    rows = read_latest_n(conn, series_id, 2, source=_PRICE_SOURCE, as_of=target_date)
    if len(rows) < 2 or rows[0].obs_date != target_date:
        return None
    return rows[0], rows[1]


def _gather_market_moves(engine: Engine, target_date: date) -> dict[str, Any]:
    """Section 1: What happened — major index moves, sector leaders/laggards."""
    moves: dict[str, Any] = {
        "indices": {},
        "sector_leaders": [],
        "sector_laggards": [],
        "notable": [],
        "prices_enabled": PRICES_ENABLED,
        "price_basis": PRICE_BASIS,
        "price_source": _PRICE_SOURCE,
    }

    if not PRICES_ENABLED:
        moves["disabled_reason"] = (
            "price reads held pending operator confirmation that the YF "
            "quarantine (migration raw_series_quarantined_20260926) has "
            "run; set GRID_MARKET_DIARY_PRICES_ENABLED=true once confirmed"
        )
        return moves

    try:
        with engine.connect() as conn:
            # Major indices — today vs prior close
            index_tickers = {
                "^GSPC": "S&P 500",
                "^DJI": "Dow Jones",
                "^IXIC": "Nasdaq",
                "^RUT": "Russell 2000",
                "^VIX": "VIX",
            }
            for yf_ticker, label in index_tickers.items():
                pair = _fresh_pair(conn, f"YF:{yf_ticker}:close", target_date)
                if pair is None:
                    moves["indices"][label] = {"status": "no close for date"}
                    continue
                today, prior = pair
                chg = today.value - prior.value
                chg_pct = (chg / prior.value * 100) if prior.value else None
                moves["indices"][label] = {
                    "close": round(today.value, 2),
                    "change": round(chg, 2),
                    "change_pct": round(chg_pct, 2) if chg_pct is not None else None,
                    "obs_date": today.obs_date.isoformat(),
                    "price_basis": PRICE_BASIS,
                    "source": today.source or _PRICE_SOURCE,
                }

            # Sector ETFs for leaders/laggards
            sector_etfs = {
                "XLK": "Technology", "XLF": "Financials", "XLE": "Energy",
                "XLV": "Health Care", "XLI": "Industrials", "XLY": "Consumer Disc",
                "XLP": "Consumer Staples", "XLU": "Utilities", "XLRE": "Real Estate",
                "XLC": "Communications", "XLB": "Materials",
            }
            sector_perf: list[dict] = []
            sector_no_close: list[str] = []
            for etf, name in sector_etfs.items():
                pair = _fresh_pair(conn, f"YF:{etf}:close", target_date)
                if pair is None:
                    sector_no_close.append(etf)
                    continue
                today, prior = pair
                pct = (today.value - prior.value) / prior.value * 100 if prior.value else None
                if pct is None:
                    sector_no_close.append(etf)
                    continue
                sector_perf.append({
                    "sector": name, "etf": etf, "change_pct": round(pct, 2),
                    "obs_date": today.obs_date.isoformat(),
                    "price_basis": PRICE_BASIS,
                    "source": today.source or _PRICE_SOURCE,
                })

            sector_perf.sort(key=lambda x: x["change_pct"], reverse=True)
            moves["sector_leaders"] = sector_perf[:3]
            moves["sector_laggards"] = sector_perf[-3:]
            if sector_no_close:
                moves["sector_no_close"] = sector_no_close

            # Notable single-day moves (VIX spike, gold, oil, DXY)
            notable_series = {
                "YF:GC=F:close": "Gold",
                "YF:CL=F:close": "Crude Oil",
                "YF:UUP:close": "Dollar (UUP)",
                "YF:TLT:close": "Long Bonds (TLT)",
            }
            notable_no_close: list[str] = []
            for sid, label in notable_series.items():
                pair = _fresh_pair(conn, sid, target_date)
                if pair is None:
                    notable_no_close.append(label)
                    continue
                today, prior = pair
                pct = (today.value - prior.value) / prior.value * 100 if prior.value else None
                if pct is None:
                    notable_no_close.append(label)
                    continue
                if abs(pct) >= 0.5:
                    moves["notable"].append({
                        "asset": label,
                        "close": round(today.value, 2),
                        "change_pct": round(pct, 2),
                        "obs_date": today.obs_date.isoformat(),
                        "price_basis": PRICE_BASIS,
                        "source": today.source or _PRICE_SOURCE,
                    })
            if notable_no_close:
                moves["notable_no_close"] = notable_no_close

    except Exception as exc:
        log.warning("market_diary: failed to gather market moves: {e}", e=str(exc))

    return moves


def _gather_active_actors(engine: Engine, target_date: date) -> dict[str, Any]:
    """Section 3: Who was active — lever-pullers, congressional trades, insider filings."""
    actors: dict[str, Any] = {
        "congressional_trades": [],
        "insider_filings": [],
        "lever_puller_actions": [],
    }

    try:
        with engine.connect() as conn:
            # Congressional trades around this date
            rows = conn.execute(
                text(
                    "SELECT ticker, signal_type, signal_value, signal_date "
                    "FROM signal_sources "
                    "WHERE source_type = 'congressional' "
                    "AND signal_date BETWEEN :start AND :end "
                    "ORDER BY signal_date DESC LIMIT 10"
                ),
                {"start": target_date - timedelta(days=2), "end": target_date},
            ).fetchall()
            for r in rows:
                actors["congressional_trades"].append({
                    "ticker": r[0],
                    "direction": r[1],
                    "date": str(r[3]),
                })

            # Insider filings
            rows = conn.execute(
                text(
                    "SELECT ticker, signal_type, signal_value, signal_date "
                    "FROM signal_sources "
                    "WHERE source_type = 'insider' "
                    "AND signal_date BETWEEN :start AND :end "
                    "ORDER BY signal_date DESC LIMIT 10"
                ),
                {"start": target_date - timedelta(days=2), "end": target_date},
            ).fetchall()
            for r in rows:
                actors["insider_filings"].append({
                    "ticker": r[0],
                    "direction": r[1],
                    "date": str(r[3]),
                })

            # Lever-puller actions from decision_journal
            rows = conn.execute(
                text(
                    "SELECT inferred_state, grid_recommendation, "
                    "state_confidence, decision_timestamp "
                    "FROM decision_journal "
                    "WHERE DATE(decision_timestamp) = :dt "
                    "ORDER BY decision_timestamp DESC LIMIT 5"
                ),
                {"dt": target_date},
            ).fetchall()
            for r in rows:
                actors["lever_puller_actions"].append({
                    "state": r[0],
                    "recommendation": r[1],
                    "confidence": round(float(r[2]), 3) if r[2] else None,
                    "timestamp": str(r[3]),
                })
    except Exception as exc:
        log.warning("market_diary: failed to gather actors: {e}", e=str(exc))

    return actors


_PRE_OPEN_CUTOFF_UTC = time(13, 30)  # roughly US market open


def _gather_thesis_accuracy(engine: Engine, target_date: date) -> dict[str, Any]:
    """Section 5/6: Compare the pre-open thesis to the actual outcome.

    Deliberately does not compute a fresh unified thesis at write time --
    doing so at 22:00Z graded the diary against a thesis that had already
    seen the whole trading day (look-ahead / self-grading). Instead this
    reads the actual pre-open snapshot that was archived that morning. See
    §4.1 fix #1.
    """
    accuracy: dict[str, Any] = {
        "morning_thesis": None,
        "morning_conviction": None,
        "actual_outcome": None,
        "verdict": None,  # correct / wrong / partial / None (no basis to grade)
        "reason": None,
        "details": [],
    }

    try:
        cutoff = datetime.combine(target_date, _PRE_OPEN_CUTOFF_UTC, tzinfo=timezone.utc)
        day_start = datetime.combine(target_date, time.min, tzinfo=timezone.utc)

        with engine.connect() as conn:
            snap = conn.execute(
                text(
                    "SELECT overall_direction, conviction, timestamp "
                    "FROM thesis_snapshots "
                    "WHERE timestamp >= :day_start AND timestamp < :cutoff "
                    "ORDER BY timestamp DESC LIMIT 1"
                ),
                {"day_start": day_start, "cutoff": cutoff},
            ).fetchone()

        if snap is None:
            accuracy["reason"] = "no pre-open thesis snapshot"
        else:
            direction = (snap[0] or "").strip().upper() or None
            accuracy["morning_thesis"] = direction
            accuracy["morning_conviction"] = float(snap[1]) if snap[1] is not None else None
            ts = snap[2]
            accuracy["morning_thesis_timestamp"] = ts.isoformat() if hasattr(ts, "isoformat") else str(ts)

        # Determine actual market direction from S&P close — gated by the
        # same price-freshness rule as _gather_market_moves (§4.1 fix #2).
        if PRICES_ENABLED:
            with engine.connect() as conn:
                pair = _fresh_pair(conn, "YF:^GSPC:close", target_date)
            if pair is not None:
                today, prior = pair
                if prior.value:
                    daily_return = (today.value - prior.value) / prior.value * 100
                    accuracy["actual_outcome"] = (
                        "BULLISH" if daily_return > 0.1 else ("BEARISH" if daily_return < -0.1 else "NEUTRAL")
                    )
                    accuracy["sp500_return_pct"] = round(daily_return, 2)
                    accuracy["sp500_obs_date"] = today.obs_date.isoformat()
            if accuracy["actual_outcome"] is None and accuracy["reason"] is None:
                accuracy["reason"] = "no close for date"
        elif accuracy["reason"] is None:
            accuracy["reason"] = "prices disabled pending YF quarantine confirmation"

        # Compare — only when both sides of the comparison are honest.
        if accuracy["morning_thesis"] and accuracy["actual_outcome"]:
            if accuracy["morning_thesis"] == accuracy["actual_outcome"]:
                accuracy["verdict"] = "correct"
            elif accuracy["morning_thesis"] == "NEUTRAL" or accuracy["actual_outcome"] == "NEUTRAL":
                accuracy["verdict"] = "partial"
            else:
                accuracy["verdict"] = "wrong"
        elif accuracy["reason"] is None:
            accuracy["reason"] = "morning thesis or actual outcome unavailable"

        # Cross-reference anomalies for the day. cross_reference_reports is
        # empty in production (see the Wave 3 triage report) and is never
        # read here; cross_reference_checks is the table every other
        # consumer (api/routers/chat.py, api/routers/intel.py,
        # intelligence/regime/state_vector.py, etc.) reads for the same
        # "anomalies detected today" purpose. See §4.1 fix #4.
        try:
            with engine.connect() as conn:
                cr_rows = conn.execute(
                    text(
                        "SELECT name, category, assessment, implication "
                        "FROM cross_reference_checks "
                        "WHERE DATE(checked_at) = :dt "
                        "AND assessment IS NOT NULL AND assessment != 'consistent' "
                        "ORDER BY checked_at DESC LIMIT 5"
                    ),
                    {"dt": target_date},
                ).fetchall()
            if cr_rows:
                accuracy["anomalies_detected"] = len(cr_rows)
                accuracy["details"] = [
                    {
                        "type": "cross_reference",
                        "flag": f"{r[0]} ({r[1]}): {r[2]} — {(r[3] or '')[:180]}",
                    }
                    for r in cr_rows
                ]
        except Exception as cr_exc:
            log.debug("cross_reference_checks query failed (table may not exist): {e}", e=str(cr_exc))

    except Exception as exc:
        log.warning("market_diary: failed to assess thesis accuracy: {e}", e=str(exc))

    return accuracy


# ──────────────────────────────────────────────────────────────────
# LLM narrative generation
# ──────────────────────────────────────────────────────────────────

_DIARY_SYSTEM_PROMPT = """\
Daily market diary. Sections: WHAT HAPPENED (lead with #1 move, specific \
numbers, only from the data supplied below), WHY (state an actor + action \
that drove the day's move ONLY if the supplied actors/signals data below \
actually names one; if no such actor or signal is present, say "cause not \
identified" — do not invent a driver. Conditions like volatility, \
sentiment or positioning may be described but never presented as a \
cause), RIGHT (which pre-open signal/model called it, if a pre-open \
thesis snapshot is present below), WRONG (what the pre-open thesis missed \
and why, if applicable — state "no pre-open thesis to grade" if none is \
present), WATCH TOMORROW (catalysts with ticker + time + expected impact, \
only ones present in the supplied context). Under 500 words. Present \
tense. State only what the supplied data shows; where the data is absent \
or marked unavailable, say so instead of filling the gap.\
"""


def _build_diary_prompt(
    target_date: date,
    moves: dict,
    actors: dict,
    thesis_accuracy: dict,
) -> str:
    """Construct the user prompt with embedded data for the LLM."""
    lines: list[str] = []
    lines.append(f"Write the GRID market diary entry for {target_date.strftime('%A, %B %d, %Y')}.")
    lines.append("")

    # Index performance
    lines.append("### INDEX PERFORMANCE")
    if not moves.get("prices_enabled", True):
        lines.append(f"- prices unavailable: {moves.get('disabled_reason', 'disabled')}")
    for name, data in moves.get("indices", {}).items():
        if data.get("change_pct") is None or data.get("close") is None:
            lines.append(f"- {name}: {data.get('status', 'no close for date')}")
        else:
            lines.append(f"- {name}: {data['close']} ({data['change_pct']:+.2f}%)")

    # Sector leaders / laggards
    if moves.get("sector_leaders"):
        lines.append("\n### SECTOR LEADERS")
        for s in moves["sector_leaders"]:
            lines.append(f"- {s['sector']} ({s['etf']}): {s['change_pct']:+.2f}%")
    if moves.get("sector_laggards"):
        lines.append("\n### SECTOR LAGGARDS")
        for s in moves["sector_laggards"]:
            lines.append(f"- {s['sector']} ({s['etf']}): {s['change_pct']:+.2f}%")

    # Notable moves
    if moves.get("notable"):
        lines.append("\n### NOTABLE MOVES")
        for n in moves["notable"]:
            lines.append(f"- {n['asset']}: {n['close']} ({n['change_pct']:+.2f}%)")

    # Active actors
    if actors.get("congressional_trades"):
        lines.append("\n### CONGRESSIONAL TRADES")
        for t in actors["congressional_trades"][:5]:
            lines.append(f"- {t['ticker']} {t['direction']} ({t['date']})")
    if actors.get("insider_filings"):
        lines.append("\n### INSIDER FILINGS")
        for t in actors["insider_filings"][:5]:
            lines.append(f"- {t['ticker']} {t['direction']} ({t['date']})")

    # Thesis accuracy
    lines.append("\n### THESIS PERFORMANCE")
    lines.append(f"- Pre-open thesis: {thesis_accuracy.get('morning_thesis') or 'none (no pre-open snapshot)'}")
    lines.append(f"- Actual outcome: {thesis_accuracy.get('actual_outcome') or 'unavailable'}")
    if thesis_accuracy.get("sp500_return_pct") is not None:
        lines.append(f"- S&P 500 return: {thesis_accuracy['sp500_return_pct']}%")
    lines.append(f"- Verdict: {thesis_accuracy.get('verdict') or 'no verdict'}")
    if thesis_accuracy.get("reason"):
        lines.append(f"- Reason: {thesis_accuracy['reason']}")
    if thesis_accuracy.get("anomalies_detected"):
        lines.append(f"- Cross-reference anomalies: {thesis_accuracy['anomalies_detected']}")

    # Intelligence context: hypotheses and postmortems
    try:
        from db import get_engine as _get_engine
        from intelligence.context_provider import (
            get_active_hypotheses,
            get_recent_postmortems,
        )
        _eng = _get_engine()
        hyp_context = get_active_hypotheses(_eng, limit=5)
        pm_context = get_recent_postmortems(_eng, limit=3)
        if hyp_context:
            lines.append("")
            lines.append(hyp_context)
        if pm_context:
            lines.append("")
            lines.append(pm_context)
    except Exception as exc:
        from loguru import logger as log
        log.debug("Market diary: intelligence context injection failed: {e}", e=str(exc))

    lines.append("")
    lines.append("Use all the above data to write the diary entry. Interpret, don't just list.")

    return "\n".join(lines)


def _generate_narrative(
    target_date: date,
    moves: dict,
    actors: dict,
    thesis_accuracy: dict,
    ollama_client: Any = None,
) -> tuple[str, str | None, bool]:
    """Use the LLM to write the narrative sections of the diary entry.

    Returns ``(content, narrative_model, narrative_fallback)``.
    ``narrative_model`` is the LLM's model name when an LLM answered, or
    ``None`` when the rule-based fallback was used (``narrative_fallback``
    True). Mirrors how ``intelligence/deep_dive.py`` records
    ``model_used``/``provider_used`` alongside its LLM narrative.
    """
    user_prompt = _build_diary_prompt(target_date, moves, actors, thesis_accuracy)

    # Try to get an LLM client (LOCAL tier — high-volume narrative)
    if ollama_client is None:
        try:
            from llm.router import Tier, get_llm
            ollama_client = get_llm(Tier.LOCAL)
        except Exception as exc:
            log.warning("LLM client unavailable for market diary: {e}", e=exc)

    if ollama_client is not None:
        try:
            content = ollama_client.chat(
                messages=[
                    {"role": "system", "content": _DIARY_SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.4,
                num_predict=1200,
            )
            if content:
                model_name = getattr(ollama_client, "model", None) or "local"
                return content, model_name, False
        except Exception as exc:
            log.warning("market_diary: LLM generation failed: {e}", e=str(exc))

    # Fallback: rule-based summary
    return _build_fallback_narrative(target_date, moves, actors, thesis_accuracy), None, True


def _build_fallback_narrative(
    target_date: date,
    moves: dict,
    actors: dict,
    thesis_accuracy: dict,
) -> str:
    """Generate a data-driven diary entry when the LLM is unavailable."""
    lines: list[str] = []
    lines.append(f"# GRID Market Diary — {target_date.strftime('%A, %B %d, %Y')}")
    lines.append("")
    lines.append("*AI narrative unavailable — data summary below.*")
    lines.append("")

    lines.append("## What Happened")
    if not moves.get("prices_enabled", True):
        lines.append(f"*{moves.get('disabled_reason', 'Prices unavailable.')}*")
    for name, data in moves.get("indices", {}).items():
        if data.get("change_pct") is None or data.get("close") is None:
            lines.append(f"- **{name}**: {data.get('status', 'no close for date')}")
            continue
        direction = "up" if data["change_pct"] > 0 else "down"
        lines.append(f"- **{name}** closed at {data['close']}, {direction} {abs(data['change_pct']):.2f}%")

    if moves.get("sector_leaders"):
        lines.append("\n**Sector Leaders:**")
        for s in moves["sector_leaders"]:
            lines.append(f"- {s['sector']}: {s['change_pct']:+.2f}%")
    if moves.get("sector_laggards"):
        lines.append("\n**Sector Laggards:**")
        for s in moves["sector_laggards"]:
            lines.append(f"- {s['sector']}: {s['change_pct']:+.2f}%")

    if moves.get("notable"):
        lines.append("\n## Notable Moves")
        for n in moves["notable"]:
            lines.append(f"- {n['asset']}: {n['close']} ({n['change_pct']:+.2f}%)")

    lines.append("\n## Who Was Active")
    if actors.get("congressional_trades"):
        lines.append(f"- {len(actors['congressional_trades'])} congressional trades detected")
    if actors.get("insider_filings"):
        lines.append(f"- {len(actors['insider_filings'])} insider filings detected")
    if not actors.get("congressional_trades") and not actors.get("insider_filings"):
        lines.append("- No notable actor activity today")

    lines.append("\n## Thesis Accuracy")
    lines.append(f"- Pre-open call: **{thesis_accuracy.get('morning_thesis') or 'none (no pre-open snapshot)'}**")
    lines.append(f"- Actual: **{thesis_accuracy.get('actual_outcome') or 'unavailable'}**")
    lines.append(f"- Verdict: **{thesis_accuracy.get('verdict') or 'no verdict'}**")
    if thesis_accuracy.get("reason"):
        lines.append(f"- Reason: {thesis_accuracy['reason']}")

    lines.append("\n---")
    lines.append(f"*Generated: {datetime.now(timezone.utc).isoformat()}*")

    return "\n".join(lines)


# ──────────────────────────────────────────────────────────────────
# Public API
# ──────────────────────────────────────────────────────────────────

def write_diary_entry(
    engine: Engine,
    target_date: date | None = None,
    ollama_client: Any = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Generate and store today's market diary entry.

    Parameters:
        engine: SQLAlchemy engine.
        target_date: Date to write the diary for (defaults to today).
        ollama_client: Optional Ollama client for LLM narrative.
        dry_run: When True, gather and render the entry but do not upsert
            it into ``market_diary`` or log it to the LLM insight archive
            (used by ``scripts/run_market_diary.py --dry-run``).

    Returns:
        dict with date, content, market_moves, active_actors,
        thesis_accuracy, narrative_model, narrative_fallback,
        generated_at, dry_run.
    """
    if target_date is None:
        target_date = date.today()

    log.info("Writing market diary for {d}{dr}", d=target_date, dr=" [dry-run]" if dry_run else "")
    ensure_table(engine)

    # Gather structured data
    moves = _gather_market_moves(engine, target_date)
    actors = _gather_active_actors(engine, target_date)
    thesis_acc = _gather_thesis_accuracy(engine, target_date)

    # Generate narrative
    narrative, narrative_model, narrative_fallback = _generate_narrative(
        target_date, moves, actors, thesis_acc, ollama_client,
    )

    # Build the full entry content (narrative + data appendix)
    content_parts: list[str] = [narrative]
    content_parts.append("\n\n---\n")
    content_parts.append("## Data Appendix\n")

    # Index table
    if moves.get("indices"):
        content_parts.append("| Index | Close | Change |")
        content_parts.append("|-------|-------|--------|")
        for name, data in moves["indices"].items():
            if data.get("change_pct") is None or data.get("close") is None:
                content_parts.append(f"| {name} | {data.get('status', 'no close for date')} | — |")
            else:
                content_parts.append(
                    f"| {name} | {data['close']} | {data['change_pct']:+.2f}% |"
                )
        content_parts.append("")

    # Actor table
    all_trades = actors.get("congressional_trades", []) + actors.get("insider_filings", [])
    if all_trades:
        content_parts.append("| Source | Ticker | Direction | Date |")
        content_parts.append("|--------|--------|-----------|------|")
        for t in actors.get("congressional_trades", [])[:5]:
            content_parts.append(f"| Congress | {t['ticker']} | {t['direction']} | {t['date']} |")
        for t in actors.get("insider_filings", [])[:5]:
            content_parts.append(f"| Insider | {t['ticker']} | {t['direction']} | {t['date']} |")
        content_parts.append("")

    full_content = "\n".join(content_parts)
    generated_at = datetime.now(timezone.utc)

    if dry_run:
        log.info("Market diary dry-run for {d}: not persisted", d=target_date)
    else:
        # Upsert into DB
        try:
            with engine.begin() as conn:
                conn.execute(
                    text(
                        "INSERT INTO market_diary (date, content, market_moves, "
                        "active_actors, thesis_accuracy, narrative_model, "
                        "narrative_fallback, generated_at) "
                        "VALUES (:dt, :content, :moves, :actors, :accuracy, "
                        ":narrative_model, :narrative_fallback, :gen_at) "
                        "ON CONFLICT (date) DO UPDATE SET "
                        "content = EXCLUDED.content, "
                        "market_moves = EXCLUDED.market_moves, "
                        "active_actors = EXCLUDED.active_actors, "
                        "thesis_accuracy = EXCLUDED.thesis_accuracy, "
                        "narrative_model = EXCLUDED.narrative_model, "
                        "narrative_fallback = EXCLUDED.narrative_fallback, "
                        "generated_at = EXCLUDED.generated_at"
                    ),
                    {
                        "dt": target_date,
                        "content": full_content,
                        "moves": json.dumps(moves),
                        "actors": json.dumps(actors),
                        "accuracy": json.dumps(thesis_acc),
                        "narrative_model": narrative_model,
                        "narrative_fallback": narrative_fallback,
                        "gen_at": generated_at,
                    },
                )
            log.info("Market diary saved for {d}", d=target_date)
        except Exception as exc:
            log.error("Failed to save market diary: {e}", e=str(exc))

        # Also log to the LLM insight archive
        try:
            from outputs.llm_logger import log_insight
            log_insight(
                category="briefing",
                title=f"Market Diary — {target_date}",
                content=full_content,
                metadata={
                    "date": str(target_date),
                    "verdict": thesis_acc.get("verdict"),
                    "sp500_return": thesis_acc.get("sp500_return_pct"),
                    "narrative_model": narrative_model,
                    "narrative_fallback": narrative_fallback,
                },
                provider="market_diary",
            )
        except Exception as exc:
            log.warning("Failed to store diary entry: {e}", e=exc)

    result = {
        "date": str(target_date),
        "content": full_content,
        "market_moves": moves,
        "active_actors": actors,
        "thesis_accuracy": thesis_acc,
        "narrative_model": narrative_model,
        "narrative_fallback": narrative_fallback,
        "generated_at": generated_at.isoformat(),
        "dry_run": dry_run,
    }

    return result


def get_diary_entry(engine: Engine, target_date: date | None = None) -> dict[str, Any] | None:
    """Retrieve a diary entry for a specific date.

    If target_date is None, returns the most recent entry.
    Returns None if no entry exists.
    """
    ensure_table(engine)

    if target_date is None:
        target_date = date.today()

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT id, date, content, market_moves, active_actors, "
                    "thesis_accuracy, generated_at, narrative_model, narrative_fallback "
                    "FROM market_diary WHERE date = :dt"
                ),
                {"dt": target_date},
            ).fetchone()

            if not row:
                return None

            return {
                "id": row[0],
                "date": str(row[1]),
                "content": row[2],
                "market_moves": row[3] if isinstance(row[3], dict) else (json.loads(row[3]) if row[3] else {}),
                "active_actors": row[4] if isinstance(row[4], dict) else (json.loads(row[4]) if row[4] else {}),
                "thesis_accuracy": row[5] if isinstance(row[5], dict) else (json.loads(row[5]) if row[5] else {}),
                "generated_at": str(row[6]),
                "narrative_model": row[7] if len(row) > 7 else None,
                "narrative_fallback": bool(row[8]) if len(row) > 8 and row[8] is not None else False,
            }
    except Exception as exc:
        log.warning("Failed to retrieve diary entry: {e}", e=str(exc))
        return None


def list_diary_entries(
    engine: Engine,
    limit: int = 30,
    offset: int = 0,
) -> dict[str, Any]:
    """List diary entries ordered by date descending.

    Returns dict with 'entries' (list of summaries) and 'total' count.
    """
    ensure_table(engine)

    try:
        with engine.connect() as conn:
            total_row = conn.execute(
                text("SELECT COUNT(*) FROM market_diary")
            ).fetchone()
            total = total_row[0] if total_row else 0

            rows = conn.execute(
                text(
                    "SELECT id, date, thesis_accuracy, generated_at "
                    "FROM market_diary "
                    "ORDER BY date DESC "
                    "LIMIT :lim OFFSET :off"
                ),
                {"lim": limit, "off": offset},
            ).fetchall()

            entries = []
            for r in rows:
                acc = r[2] if isinstance(r[2], dict) else (json.loads(r[2]) if r[2] else {})
                entries.append({
                    "id": r[0],
                    "date": str(r[1]),
                    "verdict": acc.get("verdict", "unknown"),
                    "sp500_return_pct": acc.get("sp500_return_pct"),
                    "morning_thesis": acc.get("morning_thesis"),
                    "generated_at": str(r[3]),
                })

            return {"entries": entries, "total": total}
    except Exception as exc:
        log.warning("Failed to list diary entries: {e}", e=str(exc))
        return {"entries": [], "total": 0}


def search_diary(
    engine: Engine,
    query: str,
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Full-text search across diary content.

    Parameters:
        engine: SQLAlchemy engine.
        query: Search term (case-insensitive LIKE match).
        limit: Max results.

    Returns:
        List of matching entry summaries.
    """
    ensure_table(engine)

    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, date, thesis_accuracy, generated_at "
                    "FROM market_diary "
                    "WHERE content ILIKE :q "
                    "ORDER BY date DESC "
                    "LIMIT :lim"
                ),
                {"q": f"%{query}%", "lim": limit},
            ).fetchall()

            results = []
            for r in rows:
                acc = r[2] if isinstance(r[2], dict) else (json.loads(r[2]) if r[2] else {})
                results.append({
                    "id": r[0],
                    "date": str(r[1]),
                    "verdict": acc.get("verdict", "unknown"),
                    "sp500_return_pct": acc.get("sp500_return_pct"),
                    "generated_at": str(r[3]),
                })
            return results
    except Exception as exc:
        log.warning("Failed to search diary: {e}", e=str(exc))
        return []


# ──────────────────────────────────────────────────────────────────
# Scheduler
# ──────────────────────────────────────────────────────────────────

def schedule_daily_diary(engine: Engine) -> None:
    """Register the daily diary writer to run at 10 PM UTC.

    Call this from the API startup to enable automatic diary generation.
    """
    import threading

    def _diary_loop() -> None:
        import time as _time

        import schedule as _sched

        _sched.every().monday.at("22:00").do(write_diary_entry, engine=engine)
        _sched.every().tuesday.at("22:00").do(write_diary_entry, engine=engine)
        _sched.every().wednesday.at("22:00").do(write_diary_entry, engine=engine)
        _sched.every().thursday.at("22:00").do(write_diary_entry, engine=engine)
        _sched.every().friday.at("22:00").do(write_diary_entry, engine=engine)

        log.info("Market diary scheduled — daily at 22:00 UTC (Mon-Fri)")

        while True:
            _sched.run_pending()
            _time.sleep(30)

    t = threading.Thread(target=_diary_loop, daemon=True, name="market-diary")
    t.start()
    log.info("Market diary scheduler thread started")


# ──────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="GRID Market Diary")
    parser.add_argument("--date", type=str, default=None, help="Date (YYYY-MM-DD), defaults to today")
    parser.add_argument("--list", action="store_true", help="List recent entries")
    parser.add_argument("--search", type=str, default=None, help="Search diary entries")
    args = parser.parse_args()

    try:
        from db import get_engine
        eng = get_engine()
    except Exception as exc:
        log.error("Could not connect to database: {e}", e=exc)
        raise SystemExit(1)

    if args.list:
        result = list_diary_entries(eng)
        for e in result["entries"]:
            badge = {"correct": "+", "wrong": "X", "partial": "~"}.get(e["verdict"], "?")
            print(f"[{badge}] {e['date']}  SP500: {e.get('sp500_return_pct', '?')}%  thesis: {e.get('morning_thesis', '?')}")
    elif args.search:
        results = search_diary(eng, args.search)
        for r in results:
            print(f"{r['date']}: verdict={r['verdict']}")
    else:
        target = date.fromisoformat(args.date) if args.date else date.today()
        result = write_diary_entry(eng, target_date=target)
        print(result["content"])
