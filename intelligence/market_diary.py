"""
GRID — Automated Daily Market Diary.

Every trading day at 10 PM UTC, the system writes a diary entry —
a structured research note covering what happened, why, who was
active, what the data shows, thesis accuracy, and what to watch
tomorrow.  Think of it as a hedge fund's daily research journal
that the LLM narrates while the data sections are rule-based.

Usage::

    from intelligence.market_diary import write_diary_entry, get_diary_entry

    # Generate today's diary
    result = write_diary_entry(engine)

    # Retrieve a past entry
    entry = get_diary_entry(engine, date(2026, 3, 27))
"""

from __future__ import annotations

import json
import time
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


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
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""


def ensure_table(engine: Engine) -> None:
    """Create the market_diary table if it does not exist."""
    with engine.begin() as conn:
        conn.execute(text(_CREATE_TABLE))


# ──────────────────────────────────────────────────────────────────
# Data gatherers (rule-based sections)
# ──────────────────────────────────────────────────────────────────

def _gather_market_moves(engine: Engine, target_date: date) -> dict[str, Any]:
    """Section 1: What happened — major index moves, sector leaders/laggards."""
    moves: dict[str, Any] = {
        "indices": {},
        "sector_leaders": [],
        "sector_laggards": [],
        "notable": [],
    }

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
                rows = conn.execute(
                    text(
                        "SELECT value, obs_date FROM raw_series "
                        "WHERE series_id = :sid "
                        "AND obs_date <= :dt "
                        "ORDER BY obs_date DESC LIMIT 2"
                    ),
                    {"sid": f"YF:{yf_ticker}:close", "dt": target_date},
                ).fetchall()
                if len(rows) >= 2:
                    today_val, prior_val = float(rows[0][0]), float(rows[1][0])
                    chg = today_val - prior_val
                    chg_pct = (chg / prior_val * 100) if prior_val else 0
                    moves["indices"][label] = {
                        "close": round(today_val, 2),
                        "change": round(chg, 2),
                        "change_pct": round(chg_pct, 2),
                    }

            # Sector ETFs for leaders/laggards
            sector_etfs = {
                "XLK": "Technology", "XLF": "Financials", "XLE": "Energy",
                "XLV": "Health Care", "XLI": "Industrials", "XLY": "Consumer Disc",
                "XLP": "Consumer Staples", "XLU": "Utilities", "XLRE": "Real Estate",
                "XLC": "Communications", "XLB": "Materials",
            }
            sector_perf: list[dict] = []
            for etf, name in sector_etfs.items():
                rows = conn.execute(
                    text(
                        "SELECT value, obs_date FROM raw_series "
                        "WHERE series_id = :sid "
                        "AND obs_date <= :dt "
                        "ORDER BY obs_date DESC LIMIT 2"
                    ),
                    {"sid": f"YF:{etf}:close", "dt": target_date},
                ).fetchall()
                if len(rows) >= 2:
                    t, p = float(rows[0][0]), float(rows[1][0])
                    pct = (t - p) / p * 100 if p else 0
                    sector_perf.append({"sector": name, "etf": etf, "change_pct": round(pct, 2)})

            sector_perf.sort(key=lambda x: x["change_pct"], reverse=True)
            moves["sector_leaders"] = sector_perf[:3]
            moves["sector_laggards"] = sector_perf[-3:]

            # Notable single-day moves (VIX spike, gold, oil, DXY)
            notable_series = {
                "YF:GC=F:close": "Gold",
                "YF:CL=F:close": "Crude Oil",
                "YF:UUP:close": "Dollar (UUP)",
                "YF:TLT:close": "Long Bonds (TLT)",
            }
            for sid, label in notable_series.items():
                rows = conn.execute(
                    text(
                        "SELECT value, obs_date FROM raw_series "
                        "WHERE series_id = :sid "
                        "AND obs_date <= :dt "
                        "ORDER BY obs_date DESC LIMIT 2"
                    ),
                    {"sid": sid, "dt": target_date},
                ).fetchall()
                if len(rows) >= 2:
                    t, p = float(rows[0][0]), float(rows[1][0])
                    pct = (t - p) / p * 100 if p else 0
                    if abs(pct) >= 0.5:
                        moves["notable"].append({
                            "asset": label,
                            "close": round(t, 2),
                            "change_pct": round(pct, 2),
                        })

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
                    "SELECT ticker, direction, signal_value, signal_date "
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
                    "SELECT ticker, direction, signal_value, signal_date "
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


def _gather_thesis_accuracy(engine: Engine, target_date: date) -> dict[str, Any]:
    """Section 5/6: Compare morning thesis to actual outcome."""
    accuracy: dict[str, Any] = {
        "morning_thesis": None,
        "actual_outcome": None,
        "verdict": "unknown",  # correct / wrong / partial
        "details": [],
    }

    try:
        from analysis.flow_thesis import generate_unified_thesis, BULLISH, BEARISH

        # Get the thesis generated in the morning (or most recent before market open)
        with engine.connect() as conn:
            thesis_row = conn.execute(
                text(
                    "SELECT content, generated_at FROM market_diary "
                    "WHERE date = :dt - 1 "
                    "ORDER BY generated_at DESC LIMIT 1"
                ),
                {"dt": target_date},
            ).fetchone()

        # Get today's unified thesis for current state
        try:
            current_thesis = generate_unified_thesis(engine)
            accuracy["morning_thesis"] = current_thesis.get("overall_direction", "NEUTRAL")
            accuracy["morning_conviction"] = current_thesis.get("conviction", 0)
        except Exception:
            pass

        # Determine actual market direction from S&P close
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT value, obs_date FROM raw_series "
                    "WHERE series_id = 'YF:^GSPC:close' "
                    "AND obs_date <= :dt "
                    "ORDER BY obs_date DESC LIMIT 2"
                ),
                {"dt": target_date},
            ).fetchall()
            if len(rows) >= 2:
                today_close, prior_close = float(rows[0][0]), float(rows[1][0])
                daily_return = (today_close - prior_close) / prior_close * 100
                accuracy["actual_outcome"] = "BULLISH" if daily_return > 0.1 else ("BEARISH" if daily_return < -0.1 else "NEUTRAL")
                accuracy["sp500_return_pct"] = round(daily_return, 2)

                # Compare
                if accuracy["morning_thesis"] and accuracy["actual_outcome"]:
                    if accuracy["morning_thesis"] == accuracy["actual_outcome"]:
                        accuracy["verdict"] = "correct"
                    elif accuracy["morning_thesis"] == "NEUTRAL" or accuracy["actual_outcome"] == "NEUTRAL":
                        accuracy["verdict"] = "partial"
                    else:
                        accuracy["verdict"] = "wrong"

        # Get any cross-reference anomalies for the day
        with engine.connect() as conn:
            cr_row = conn.execute(
                text(
                    "SELECT report_data FROM cross_reference_reports "
                    "WHERE DATE(created_at) = :dt "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"dt": target_date},
            ).fetchone()
            if cr_row and cr_row[0]:
                data = cr_row[0] if isinstance(cr_row[0], dict) else json.loads(cr_row[0])
                red_flags = data.get("red_flags", [])
                accuracy["anomalies_detected"] = len(red_flags)
                accuracy["details"] = [
                    {"type": "cross_reference", "flag": str(f)[:200]}
                    for f in red_flags[:5]
                ]

    except Exception as exc:
        log.warning("market_diary: failed to assess thesis accuracy: {e}", e=str(exc))

    return accuracy


# ──────────────────────────────────────────────────────────────────
# LLM narrative generation
# ──────────────────────────────────────────────────────────────────

_DIARY_SYSTEM_PROMPT = """\
You are GRID's chief market strategist writing the daily market diary.
This is a permanent research record — like a hedge fund's daily research note.
Be precise, analytical, and opinionated. Interpret every data point.

Structure your entry with these sections:

## What Happened
Narrative summary of the day's market action. Lead with the single most
important development. Mention specific numbers but always interpret them.

## Why It Happened
Connect today's moves to macro forces, thesis models, catalysts.
Reference regime state and flow thesis when relevant.

## What We Got Right
Thesis predictions that played out. Be specific about which model
or signal nailed the call.

## What We Got Wrong
Failed predictions with honest explanation of why. This section builds
institutional memory for future improvement.

## What to Watch Tomorrow
Upcoming catalysts: earnings, FOMC, OpEx, data releases, key levels.
Be specific about time, ticker, and expected impact.

Keep it under 600 words total. Write in present-tense for immediacy.
No fluff, no hedging language, no "it remains to be seen." Take a stand.
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
    for name, data in moves.get("indices", {}).items():
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
    lines.append(f"- Morning thesis: {thesis_accuracy.get('morning_thesis', 'N/A')}")
    lines.append(f"- Actual outcome: {thesis_accuracy.get('actual_outcome', 'N/A')}")
    lines.append(f"- S&P 500 return: {thesis_accuracy.get('sp500_return_pct', 'N/A')}%")
    lines.append(f"- Verdict: {thesis_accuracy.get('verdict', 'unknown')}")
    if thesis_accuracy.get("anomalies_detected"):
        lines.append(f"- Cross-reference anomalies: {thesis_accuracy['anomalies_detected']}")

    lines.append("")
    lines.append("Use all the above data to write the diary entry. Interpret, don't just list.")

    return "\n".join(lines)


def _generate_narrative(
    target_date: date,
    moves: dict,
    actors: dict,
    thesis_accuracy: dict,
    ollama_client: Any = None,
) -> str:
    """Use the LLM to write the narrative sections of the diary entry."""
    user_prompt = _build_diary_prompt(target_date, moves, actors, thesis_accuracy)

    # Try to get an Ollama client
    if ollama_client is None:
        try:
            from ollama.client import get_client
            ollama_client = get_client()
        except Exception:
            pass

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
                return content
        except Exception as exc:
            log.warning("market_diary: LLM generation failed: {e}", e=str(exc))

    # Fallback: rule-based summary
    return _build_fallback_narrative(target_date, moves, actors, thesis_accuracy)


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
    for name, data in moves.get("indices", {}).items():
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
    lines.append(f"- Morning call: **{thesis_accuracy.get('morning_thesis', 'N/A')}**")
    lines.append(f"- Actual: **{thesis_accuracy.get('actual_outcome', 'N/A')}**")
    lines.append(f"- Verdict: **{thesis_accuracy.get('verdict', 'unknown')}**")

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
) -> dict[str, Any]:
    """Generate and store today's market diary entry.

    Parameters:
        engine: SQLAlchemy engine.
        target_date: Date to write the diary for (defaults to today).
        ollama_client: Optional Ollama client for LLM narrative.

    Returns:
        dict with date, content, market_moves, active_actors,
        thesis_accuracy, generated_at.
    """
    if target_date is None:
        target_date = date.today()

    log.info("Writing market diary for {d}", d=target_date)
    ensure_table(engine)

    # Gather structured data
    moves = _gather_market_moves(engine, target_date)
    actors = _gather_active_actors(engine, target_date)
    thesis_acc = _gather_thesis_accuracy(engine, target_date)

    # Generate narrative
    narrative = _generate_narrative(
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

    # Upsert into DB
    try:
        with engine.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO market_diary (date, content, market_moves, "
                    "active_actors, thesis_accuracy, generated_at) "
                    "VALUES (:dt, :content, :moves, :actors, :accuracy, :gen_at) "
                    "ON CONFLICT (date) DO UPDATE SET "
                    "content = EXCLUDED.content, "
                    "market_moves = EXCLUDED.market_moves, "
                    "active_actors = EXCLUDED.active_actors, "
                    "thesis_accuracy = EXCLUDED.thesis_accuracy, "
                    "generated_at = EXCLUDED.generated_at"
                ),
                {
                    "dt": target_date,
                    "content": full_content,
                    "moves": json.dumps(moves),
                    "actors": json.dumps(actors),
                    "accuracy": json.dumps(thesis_acc),
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
            },
            provider="market_diary",
        )
    except Exception:
        pass

    result = {
        "date": str(target_date),
        "content": full_content,
        "market_moves": moves,
        "active_actors": actors,
        "thesis_accuracy": thesis_acc,
        "generated_at": generated_at.isoformat(),
    }

    return result


def get_diary_entry(engine: Engine, target_date: date) -> dict[str, Any] | None:
    """Retrieve a diary entry for a specific date.

    Returns None if no entry exists for that date.
    """
    ensure_table(engine)

    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT id, date, content, market_moves, active_actors, "
                    "thesis_accuracy, generated_at "
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
    except Exception:
        print("Error: Could not connect to database")
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
