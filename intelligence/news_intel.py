"""
GRID Intelligence — News Intelligence & Narrative Analysis.

Queries the signal_data table (news signal types) and business_events table
to provide news feed, statistics, narrative shift detection, forensic
pre-move analysis, and structured briefings.

Signal types considered news:
  news_event, tiingo_news, marketwatch_news, polygon_news, breaking_news

Functions:
  1. get_news_feed        — combined signal_data + business_events feed
  2. get_news_stats       — aggregate counts by direction, type, ticker
  3. detect_narrative_shift — sentiment shift detection (recent vs prior)
  4. find_news_before_move — forensic: what news preceded a price move?
  5. generate_news_briefing — markdown briefing from top signals
"""

from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

NEWS_SIGNAL_TYPES = (
    "news_event",
    "tiingo_news",
    "marketwatch_news",
    "polygon_news",
    "breaking_news",
)


# ── helpers ────────────────────────────────────────────────────────────────

def _parse_data_field(raw: Any) -> dict:
    """Safely parse the signal_data.data JSONB column."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            return {}
    return {}


def _business_events_exist(conn) -> bool:
    """Check whether the business_events table exists."""
    try:
        conn.execute(text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'business_events' LIMIT 1"
        ))
        row = conn.execute(text(
            "SELECT 1 FROM information_schema.tables "
            "WHERE table_name = 'business_events' LIMIT 1"
        )).fetchone()
        return row is not None
    except Exception:
        return False


def _direction_from_signal(direction: str | None, data: dict) -> str:
    """Normalise direction to bullish / bearish / neutral."""
    d = (direction or "").strip().lower()
    if d in ("bullish", "up", "positive", "long"):
        return "bullish"
    if d in ("bearish", "down", "negative", "short"):
        return "bearish"
    # Try the nested data blob
    nested = str(data.get("direction", data.get("sentiment", ""))).lower()
    if nested in ("bullish", "up", "positive"):
        return "bullish"
    if nested in ("bearish", "down", "negative"):
        return "bearish"
    return "neutral"


# ── 1. News Feed ───────────────────────────────────────────────────────────

def get_news_feed(
    engine: Engine,
    ticker: str | None = None,
    hours: int = 24,
) -> list[dict]:
    """Get recent news from signal_data + business_events, sorted by date DESC.

    Args:
        engine: SQLAlchemy engine.
        ticker: Optional ticker filter (case-insensitive).
        hours:  Hours to look back (default 24).

    Returns:
        Combined list of news items, limit 100, sorted newest-first.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    items: list[dict] = []

    # ── signal_data ──
    try:
        if ticker:
            q = text("""
                SELECT id, signal_type, signal_date, ticker, actor,
                       direction, magnitude, description, data,
                       confidence, source_id, created_at
                FROM signal_data
                WHERE signal_type = ANY(:types)
                  AND UPPER(ticker) = :ticker
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 100
            """)
            params: dict[str, Any] = {
                "types": list(NEWS_SIGNAL_TYPES),
                "ticker": ticker.upper(),
                "cutoff": cutoff,
            }
        else:
            q = text("""
                SELECT id, signal_type, signal_date, ticker, actor,
                       direction, magnitude, description, data,
                       confidence, source_id, created_at
                FROM signal_data
                WHERE signal_type = ANY(:types)
                  AND signal_date >= :cutoff
                ORDER BY signal_date DESC
                LIMIT 100
            """)
            params = {
                "types": list(NEWS_SIGNAL_TYPES),
                "cutoff": cutoff,
            }

        with engine.connect() as conn:
            rows = conn.execute(q, params).fetchall()

        for r in rows:
            data = _parse_data_field(r[8])
            items.append({
                "title": data.get("title") or data.get("headline") or (r[7] or "")[:120],
                "summary": r[7] or "",
                "ticker": r[3] or "",
                "source": data.get("source") or r[1] or "",
                "direction": _direction_from_signal(r[5], data),
                "confidence": float(r[9]) if r[9] is not None else 0.5,
                "magnitude": float(r[6]) if r[6] is not None else 0.0,
                "published_at": (r[2].isoformat() if r[2] else
                                 r[11].isoformat() if r[11] else None),
                "signal_type": r[1],
                "category": data.get("category", "news"),
            })
    except Exception as exc:
        log.warning("get_news_feed signal_data query failed: {e}", e=str(exc))

    # ── business_events ──
    try:
        with engine.connect() as conn:
            if not _business_events_exist(conn):
                log.debug("business_events table does not exist, skipping")
            else:
                if ticker:
                    bq = text("""
                        SELECT event_id, category, tickers, headline,
                               description, source, direction,
                               estimated_bps, horizon, dollar_value,
                               confidence, published_at, article_url,
                               metadata, created_at
                        FROM business_events
                        WHERE :ticker = ANY(tickers)
                          AND COALESCE(published_at, created_at) >= :cutoff
                        ORDER BY COALESCE(published_at, created_at) DESC
                        LIMIT 100
                    """)
                    bp: dict[str, Any] = {"ticker": ticker.upper(), "cutoff": cutoff}
                else:
                    bq = text("""
                        SELECT event_id, category, tickers, headline,
                               description, source, direction,
                               estimated_bps, horizon, dollar_value,
                               confidence, published_at, article_url,
                               metadata, created_at
                        FROM business_events
                        WHERE COALESCE(published_at, created_at) >= :cutoff
                        ORDER BY COALESCE(published_at, created_at) DESC
                        LIMIT 100
                    """)
                    bp = {"cutoff": cutoff}

                be_rows = conn.execute(bq, bp).fetchall()
                for r in be_rows:
                    tickers_list = r[2] or []
                    pub = r[11] or r[14]
                    items.append({
                        "title": r[3] or "",
                        "summary": r[4] or "",
                        "ticker": tickers_list[0] if tickers_list else "",
                        "source": r[5] or "business_event",
                        "direction": _direction_from_signal(r[6], {}),
                        "confidence": float(r[10]) if r[10] is not None else 0.5,
                        "magnitude": float(r[7]) if r[7] is not None else 0.0,
                        "published_at": pub.isoformat() if pub else None,
                        "signal_type": "business_event",
                        "category": r[1] or "business",
                    })
    except Exception as exc:
        log.warning("get_news_feed business_events query failed: {e}", e=str(exc))

    # Sort combined list newest-first, cap at 100
    items.sort(
        key=lambda x: x.get("published_at") or "",
        reverse=True,
    )
    return items[:100]


# ── 2. News Stats ──────────────────────────────────────────────────────────

def get_news_stats(engine: Engine, hours: int = 24) -> dict[str, Any]:
    """Aggregate news statistics from signal_data.

    Returns:
        Dict with total, by_direction, by_signal_type, top_tickers,
        avg_confidence, hours.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)

    result: dict[str, Any] = {
        "total": 0,
        "by_direction": {"bullish": 0, "bearish": 0, "neutral": 0},
        "by_signal_type": {},
        "top_tickers": [],
        "avg_confidence": 0.0,
        "hours": hours,
    }

    try:
        with engine.connect() as conn:
            # All news signals in window
            rows = conn.execute(text("""
                SELECT signal_type, ticker, direction, confidence, data
                FROM signal_data
                WHERE signal_type = ANY(:types)
                  AND signal_date >= :cutoff
            """), {"types": list(NEWS_SIGNAL_TYPES), "cutoff": cutoff}).fetchall()
    except Exception as exc:
        log.warning("get_news_stats query failed: {e}", e=str(exc))
        result["error"] = str(exc)
        return result

    if not rows:
        return result

    direction_counts: dict[str, int] = {"bullish": 0, "bearish": 0, "neutral": 0}
    type_counts: dict[str, int] = {}
    ticker_counts: dict[str, int] = {}
    confidence_sum = 0.0
    confidence_n = 0

    for r in rows:
        sig_type, tkr, direction, confidence, data_raw = r
        data = _parse_data_field(data_raw)

        # Direction
        d = _direction_from_signal(direction, data)
        direction_counts[d] = direction_counts.get(d, 0) + 1

        # Signal type
        type_counts[sig_type] = type_counts.get(sig_type, 0) + 1

        # Ticker
        if tkr:
            t = tkr.upper()
            ticker_counts[t] = ticker_counts.get(t, 0) + 1

        # Confidence
        if confidence is not None:
            confidence_sum += float(confidence)
            confidence_n += 1

    total = len(rows)
    top_tickers = sorted(ticker_counts.items(), key=lambda x: x[1], reverse=True)[:10]

    result["total"] = total
    result["by_direction"] = direction_counts
    result["by_signal_type"] = type_counts
    result["top_tickers"] = [{"ticker": t, "count": c} for t, c in top_tickers]
    result["avg_confidence"] = round(confidence_sum / confidence_n, 3) if confidence_n else 0.0

    return result


# ── 3. Narrative Shift Detection ───────────────────────────────────────────

def detect_narrative_shift(
    engine: Engine,
    ticker: str,
    days: int = 7,
) -> dict[str, Any]:
    """Detect when media narrative changes direction on a ticker.

    Compares the last 2 days of sentiment vs the prior (days - 2) days.
    Shift = recent bullish ratio - prior bullish ratio.

    Args:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol.
        days:   Total lookback (default 7). Must be >= 3 to have a prior window.

    Returns:
        Dict with shift_detected, shift_magnitude, direction, sentiment
        breakdowns, and article counts.
    """
    now = datetime.now(timezone.utc)
    recent_cutoff = now - timedelta(days=2)
    prior_cutoff = now - timedelta(days=max(days, 3))

    out: dict[str, Any] = {
        "ticker": ticker.upper(),
        "shift_detected": False,
        "shift_magnitude": 0.0,
        "recent_sentiment": {},
        "prior_sentiment": {},
        "articles_recent": 0,
        "articles_prior": 0,
        "direction": None,
    }

    try:
        with engine.connect() as conn:
            recent_rows = conn.execute(text("""
                SELECT direction, data, confidence
                FROM signal_data
                WHERE signal_type = ANY(:types)
                  AND UPPER(ticker) = :ticker
                  AND signal_date >= :recent_cutoff
            """), {
                "types": list(NEWS_SIGNAL_TYPES),
                "ticker": ticker.upper(),
                "recent_cutoff": recent_cutoff,
            }).fetchall()

            prior_rows = conn.execute(text("""
                SELECT direction, data, confidence
                FROM signal_data
                WHERE signal_type = ANY(:types)
                  AND UPPER(ticker) = :ticker
                  AND signal_date >= :prior_cutoff
                  AND signal_date < :recent_cutoff
            """), {
                "types": list(NEWS_SIGNAL_TYPES),
                "ticker": ticker.upper(),
                "prior_cutoff": prior_cutoff,
                "recent_cutoff": recent_cutoff,
            }).fetchall()
    except Exception as exc:
        log.warning("detect_narrative_shift query failed for {t}: {e}", t=ticker, e=str(exc))
        return out

    def _sentiment_breakdown(rows) -> dict[str, Any]:
        bullish = 0
        bearish = 0
        neutral = 0
        for r in rows:
            d = _direction_from_signal(r[0], _parse_data_field(r[1]))
            if d == "bullish":
                bullish += 1
            elif d == "bearish":
                bearish += 1
            else:
                neutral += 1
        total = bullish + bearish + neutral
        return {
            "bullish": bullish,
            "bearish": bearish,
            "neutral": neutral,
            "total": total,
            "bullish_ratio": round(bullish / total, 3) if total else 0.0,
        }

    recent = _sentiment_breakdown(recent_rows)
    prior = _sentiment_breakdown(prior_rows)

    out["recent_sentiment"] = recent
    out["prior_sentiment"] = prior
    out["articles_recent"] = recent["total"]
    out["articles_prior"] = prior["total"]

    # Need at least 2 articles in each window to be meaningful
    if recent["total"] >= 2 and prior["total"] >= 2:
        shift = recent["bullish_ratio"] - prior["bullish_ratio"]
        magnitude = abs(shift)
        out["shift_magnitude"] = round(magnitude, 3)

        if magnitude >= 0.25:
            out["shift_detected"] = True
            out["direction"] = "bearish_to_bullish" if shift > 0 else "bullish_to_bearish"
            log.info(
                "Narrative shift for {t}: {d} magnitude={m:.3f}",
                t=ticker, d=out["direction"], m=magnitude,
            )

    return out


# ── 4. News Before Move (Forensic) ────────────────────────────────────────

def find_news_before_move(
    engine: Engine,
    ticker: str,
    move_date: date | str,
) -> list[dict]:
    """Forensic: what news appeared in the 3 days before a price move?

    Args:
        engine:    SQLAlchemy engine.
        ticker:    Ticker symbol.
        move_date: The date of the price move (YYYY-MM-DD str or date).

    Returns:
        List of signal_data articles sorted by date, most recent first.
    """
    if isinstance(move_date, str):
        move_date = date.fromisoformat(move_date)

    window_start = move_date - timedelta(days=3)

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT id, signal_type, signal_date, ticker, actor,
                       direction, magnitude, description, data,
                       confidence, source_id, created_at
                FROM signal_data
                WHERE signal_type = ANY(:types)
                  AND UPPER(ticker) = :ticker
                  AND signal_date >= :window_start
                  AND signal_date <= :move_date
                ORDER BY signal_date DESC
            """), {
                "types": list(NEWS_SIGNAL_TYPES),
                "ticker": ticker.upper(),
                "window_start": datetime.combine(
                    window_start, datetime.min.time(), tzinfo=timezone.utc,
                ),
                "move_date": datetime.combine(
                    move_date, datetime.max.time().replace(microsecond=0),
                    tzinfo=timezone.utc,
                ),
            }).fetchall()
    except Exception as exc:
        log.warning(
            "find_news_before_move failed for {t} on {d}: {e}",
            t=ticker, d=move_date, e=str(exc),
        )
        return []

    articles: list[dict] = []
    move_dt = datetime.combine(move_date, datetime.min.time(), tzinfo=timezone.utc)

    for r in rows:
        data = _parse_data_field(r[8])
        sig_date = r[2]
        hours_before = None
        if sig_date:
            if hasattr(sig_date, "hour"):
                delta = move_dt - sig_date
            else:
                delta = move_dt - datetime.combine(
                    sig_date, datetime.min.time(), tzinfo=timezone.utc,
                )
            hours_before = round(delta.total_seconds() / 3600, 1)

        articles.append({
            "title": data.get("title") or data.get("headline") or (r[7] or "")[:120],
            "summary": r[7] or "",
            "ticker": r[3] or "",
            "source": data.get("source") or r[1] or "",
            "direction": _direction_from_signal(r[5], data),
            "confidence": float(r[9]) if r[9] is not None else 0.5,
            "magnitude": float(r[6]) if r[6] is not None else 0.0,
            "published_at": sig_date.isoformat() if sig_date else None,
            "signal_type": r[1],
            "category": data.get("category", "news"),
            "hours_before_move": hours_before,
        })

    if articles:
        log.info(
            "Found {n} news items before {t} move on {d}",
            n=len(articles), t=ticker, d=move_date,
        )

    return articles


# ── 5. News Briefing (structured, no LLM) ─────────────────────────────────

def generate_news_briefing(engine: Engine) -> str:
    """Generate a markdown market briefing from the top 20 highest-confidence
    news signals in the last 12 hours. No LLM call — pure formatting.

    Args:
        engine: SQLAlchemy engine.

    Returns:
        Markdown-formatted briefing string.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=12)

    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT signal_type, signal_date, ticker, direction,
                       magnitude, description, data, confidence
                FROM signal_data
                WHERE signal_type = ANY(:types)
                  AND signal_date >= :cutoff
                  AND confidence IS NOT NULL
                ORDER BY confidence DESC
                LIMIT 20
            """), {"types": list(NEWS_SIGNAL_TYPES), "cutoff": cutoff}).fetchall()
    except Exception as exc:
        log.warning("generate_news_briefing query failed: {e}", e=str(exc))
        return "## Market News Briefing\n\nBriefing unavailable — query failed."

    if not rows:
        return "## Market News Briefing\n\nNo high-confidence news in the last 12 hours."

    lines: list[str] = [
        "## Market News Briefing",
        "",
        f"*{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')} — "
        f"Top {len(rows)} signals by confidence*",
        "",
    ]

    for r in rows:
        sig_type, sig_date, tkr, direction, magnitude, description, data_raw, confidence = r
        data = _parse_data_field(data_raw)
        d = _direction_from_signal(direction, data)

        arrow = {"bullish": "^", "bearish": "v", "neutral": "-"}.get(d, "-")
        tkr_str = f"**{tkr}**" if tkr else "MARKET"
        conf_str = f"{float(confidence):.0%}" if confidence is not None else "?"
        title = data.get("title") or data.get("headline") or (description or "")[:120]

        lines.append(
            f"- [{arrow}] {tkr_str} ({conf_str}) — {title}"
        )

    return "\n".join(lines)
