"""
GRID — Post-Query Data Gap Scanner.

Runs asynchronously after every user query to:
  1. Identify which features were relevant to the query
  2. Check freshness and completeness of each
  3. Log gaps to the query_data_gaps table
  4. Dispatch collection for stale/missing data

Architecture:
  user query → chat.ask_grid() → _build_context_block()
                                → spawn post_query_scan()  ← this module
                                     ├─ check freshness
                                     ├─ log gaps
                                     └─ dispatch ingestion
"""

from __future__ import annotations

import re
import threading
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Freshness SLAs per family (days) ────────────────────────────────────

FRESHNESS_SLA: dict[str, int] = {
    "equity": 1,
    "rates": 1,
    "credit": 1,
    "vol": 1,
    "fx": 1,
    "commodity": 2,
    "crypto": 1,
    "sentiment": 2,
    "macro": 7,
    "alternative": 3,
    "flows": 2,
    "systemic": 3,
    "trade": 7,
    "breadth": 2,
    "earnings": 7,
}

DEFAULT_SLA_DAYS = 3


# ── Scanner ─────────────────────────────────────────────────────────────

def scan_data_gaps(
    engine: Engine,
    question: str,
    ticker: str | None,
    sources_used: list[str],
) -> dict[str, Any]:
    """Scan for data gaps relevant to the current query.

    Returns a summary dict with gap counts and dispatched actions.
    """
    gaps: list[dict] = []
    dispatched: list[str] = []

    try:
        with engine.connect() as conn:
            # 1. Find features relevant to this query
            relevant_features = _find_relevant_features(conn, question, ticker)

            if not relevant_features:
                return {"gaps": 0, "dispatched": 0}

            # 2. Check freshness of each
            today = date.today()
            for feat in relevant_features:
                fid = feat["id"]
                name = feat["name"]
                family = feat["family"]
                sla_days = FRESHNESS_SLA.get(family, DEFAULT_SLA_DAYS)

                # Get latest data point
                row = conn.execute(text("""
                    SELECT MAX(obs_date) FROM resolved_series
                    WHERE feature_id = :fid AND value IS NOT NULL
                """), {"fid": fid}).fetchone()

                last_date = row[0] if row and row[0] else None

                if last_date is None:
                    gap_type = "missing"
                    days_stale = 9999
                elif (today - last_date).days > sla_days:
                    gap_type = "stale"
                    days_stale = (today - last_date).days
                else:
                    continue  # fresh enough

                gaps.append({
                    "feature_id": fid,
                    "feature_name": name,
                    "family": family,
                    "gap_type": gap_type,
                    "last_data_date": str(last_date) if last_date else None,
                    "days_stale": days_stale,
                    "sla_days": sla_days,
                })

            # 3. Log gaps
            if gaps:
                _log_gaps(conn, gaps, question, ticker)

            # 4. Dispatch collection for gaps
            if gaps:
                dispatched = _dispatch_collection(engine, gaps)

    except Exception as exc:
        log.error("Post-query scan failed: {e}", e=str(exc))
        return {"gaps": 0, "dispatched": 0, "error": str(exc)}

    if gaps:
        log.info(
            "Post-query scan: {g} gaps found, {d} dispatched | query={q}",
            g=len(gaps), d=len(dispatched), q=question[:80],
        )

    return {
        "gaps": len(gaps),
        "dispatched": len(dispatched),
        "gap_details": gaps[:20],  # cap for logging
        "dispatched_sources": dispatched,
    }


def _find_relevant_features(
    conn: Any,
    question: str,
    ticker: str | None,
) -> list[dict]:
    """Find features relevant to the query via ticker match and keyword scan."""
    features = []

    # Direct ticker match
    if ticker:
        rows = conn.execute(text("""
            SELECT id, name, family FROM feature_registry
            WHERE name ILIKE :pattern AND model_eligible = true
            ORDER BY name
        """), {"pattern": f"%{ticker.lower()}%"}).fetchall()
        for r in rows:
            features.append({"id": r[0], "name": r[1], "family": r[2]})

    # Always check core signals that every query benefits from
    core_signals = [
        "vix_spot", "move_index", "spy_full", "treasury_10y", "treasury_2y",
        "hyg_full", "dxy", "gold_spot", "wti_crude", "btc",
    ]
    placeholders = ",".join([f"'{s}'" for s in core_signals])
    rows = conn.execute(text(f"""
        SELECT id, name, family FROM feature_registry
        WHERE name IN ({placeholders})
    """)).fetchall()
    existing_ids = {f["id"] for f in features}
    for r in rows:
        if r[0] not in existing_ids:
            features.append({"id": r[0], "name": r[1], "family": r[2]})

    # Keyword extraction from question
    keywords = _extract_keywords(question)
    if keywords:
        for kw in keywords[:5]:  # cap at 5 keyword searches
            rows = conn.execute(text("""
                SELECT id, name, family FROM feature_registry
                WHERE name ILIKE :pattern AND model_eligible = true
                LIMIT 10
            """), {"pattern": f"%{kw}%"}).fetchall()
            for r in rows:
                if r[0] not in existing_ids:
                    features.append({"id": r[0], "name": r[1], "family": r[2]})
                    existing_ids.add(r[0])

    return features


def _extract_keywords(question: str) -> list[str]:
    """Extract likely feature-relevant keywords from a question."""
    # Common financial terms that map to features
    keyword_map = {
        "oil": "wti",
        "crude": "wti",
        "gold": "gold",
        "dollar": "dxy",
        "volatility": "vix",
        "vix": "vix",
        "bonds": "treasury",
        "yields": "treasury",
        "credit": "hyg",
        "bitcoin": "btc",
        "ethereum": "eth",
        "shipping": "freight",
        "freight": "freight",
        "inflation": "cpi",
        "pmi": "pmi",
        "employment": "payroll",
        "jobs": "payroll",
        "fed": "fed_funds",
        "rates": "treasury",
        "spread": "spread",
        "options": "opt",
        "gamma": "gex",
        "sentiment": "sentiment",
    }

    question_lower = question.lower()
    found = []
    for trigger, feature_kw in keyword_map.items():
        if trigger in question_lower and feature_kw not in found:
            found.append(feature_kw)

    # Also extract tickers (1-5 uppercase letters)
    tickers = re.findall(r'\b[A-Z]{1,5}\b', question)
    for t in tickers:
        if t not in {"I", "A", "THE", "AND", "OR", "NOT", "IS", "IT", "AT", "TO", "IN", "ON", "FOR", "DO", "IF"}:
            found.append(t.lower())

    return found


def _log_gaps(
    conn: Any,
    gaps: list[dict],
    question: str,
    ticker: str | None,
) -> None:
    """Log gaps to query_data_gaps table (best effort)."""
    try:
        for gap in gaps:
            conn.execute(text("""
                INSERT INTO query_data_gaps
                    (feature_id, feature_name, family, gap_type,
                     last_data_date, days_stale, sla_days,
                     query_text, query_ticker, scanned_at)
                VALUES
                    (:fid, :fname, :family, :gap_type,
                     :last_date, :days_stale, :sla_days,
                     :query, :ticker, NOW())
            """), {
                "fid": gap["feature_id"],
                "fname": gap["feature_name"],
                "family": gap["family"],
                "gap_type": gap["gap_type"],
                "last_date": gap["last_data_date"],
                "days_stale": gap["days_stale"],
                "sla_days": gap["sla_days"],
                "query": question[:500],
                "ticker": ticker,
            })
        conn.commit()
    except Exception as exc:
        # Table might not exist yet — log and continue
        log.debug("Could not log gaps (table may not exist): {e}", e=str(exc))


def _dispatch_collection(
    engine: Engine,
    gaps: list[dict],
) -> list[str]:
    """Dispatch data collection for stale/missing features.

    Groups gaps by family and triggers the appropriate puller.
    Returns list of dispatched source names.
    """
    dispatched = []

    # Group by family
    families: dict[str, list[dict]] = {}
    for gap in gaps:
        families.setdefault(gap["family"], []).append(gap)

    # Family → puller mapping
    puller_map: dict[str, str] = {
        "equity": "price_fallback",
        "rates": "fred",
        "credit": "fred",
        "vol": "price_fallback",
        "fx": "fred",
        "commodity": "fred",
        "crypto": "coingecko",
        "sentiment": "social_sentiment",
        "macro": "fred",
        "alternative": "openbb_pipeline",
        "flows": "fred",
        "trade": "comtrade",
    }

    for family, family_gaps in families.items():
        puller_name = puller_map.get(family)
        if not puller_name:
            continue

        try:
            _run_puller(engine, puller_name, family_gaps)
            dispatched.append(f"{puller_name}:{family}")
            log.info(
                "Dispatched {p} for {n} stale {f} features",
                p=puller_name, n=len(family_gaps), f=family,
            )
        except Exception as exc:
            log.warning(
                "Failed to dispatch {p} for {f}: {e}",
                p=puller_name, f=family, e=str(exc),
            )

    return dispatched


def _run_puller(engine: Engine, puller_name: str, gaps: list[dict]) -> None:
    """Run a specific puller for the given gaps.

    Uses the ingestion module's existing pullers where available,
    falls back to generic price refresh.
    """
    if puller_name == "price_fallback":
        try:
            from ingestion.price_fallback import refresh_stale_prices
            refresh_stale_prices(engine)
        except ImportError:
            log.debug("price_fallback puller not available")

    elif puller_name == "fred":
        try:
            from ingestion.fred import pull_fred_batch
            # Extract feature names that might be FRED series
            series_names = [g["feature_name"] for g in gaps]
            pull_fred_batch(engine, series_names)
        except (ImportError, Exception) as exc:
            log.debug("FRED puller dispatch failed: {e}", e=str(exc))

    elif puller_name == "coingecko":
        try:
            from ingestion.coingecko import pull_coingecko
            pull_coingecko(engine)
        except (ImportError, Exception) as exc:
            log.debug("CoinGecko puller dispatch failed: {e}", e=str(exc))

    elif puller_name == "social_sentiment":
        try:
            from ingestion.social_sentiment import pull_social_sentiment
            pull_social_sentiment(engine)
        except (ImportError, Exception) as exc:
            log.debug("Social sentiment puller dispatch failed: {e}", e=str(exc))

    elif puller_name == "openbb_pipeline":
        try:
            from ingestion.openbb_pipeline import pull_openbb
            pull_openbb(engine)
        except (ImportError, Exception) as exc:
            log.debug("OpenBB puller dispatch failed: {e}", e=str(exc))


# ── Async entry point (called from chat router) ────────────────────────

def spawn_post_query_scan(
    engine: Engine,
    question: str,
    ticker: str | None,
    sources_used: list[str],
) -> None:
    """Fire-and-forget post-query scan in a background thread."""
    thread = threading.Thread(
        target=scan_data_gaps,
        args=(engine, question, ticker, sources_used),
        daemon=True,
        name="post-query-scan",
    )
    thread.start()
