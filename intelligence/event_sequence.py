"""
GRID Intelligence — Event Sequence Builder.

Creates a chronological timeline of ALL events for any ticker, pulling from
every intelligence data source in the system. Enables pattern recognition,
lead-time computation, and recurring sequence detection.

Data sources:
  - signal_sources       (congressional, insider, dark_pool, whale, prediction signals)
  - news_articles        (news events with sentiment)
  - options_daily_signals (options flow / regime changes)
  - decision_journal     (model decisions and recommendations)
  - cross_reference_checks (macro divergence events)
  - earnings_calendar    (earnings dates and surprises)

Key entry points:
  build_sequence          — full chronological event list for a ticker
  build_sector_sequence   — all events across a sector's tickers
  compute_lead_times      — annotate events with hours-until-next-price-move
  find_recurring_patterns — detect repeating event sequences across history
"""

from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ──────────────────────────────────────────────────────────────

# Minimum price move (%) to count as a "significant move" for lead-time calc
SIGNIFICANT_MOVE_PCT: float = 1.0

# Maximum events per query to prevent memory issues
MAX_EVENTS_PER_QUERY: int = 5000

# Minimum pattern occurrences for recurring pattern detection
DEFAULT_MIN_OCCURRENCES: int = 3

# Pattern window: max hours between events in a sequence to consider them linked
PATTERN_WINDOW_HOURS: float = 168.0  # 7 days

# Source type mapping from signal_sources.source_type to event_type
_SOURCE_TYPE_MAP: dict[str, str] = {
    "congressional": "congressional",
    "insider": "insider",
    "darkpool": "dark_pool",
    "dark_pool": "dark_pool",
    "social": "news",
    "scanner": "whale",
    "whale": "whale",
    "prediction": "prediction",
}


# ── Data Classes ───────────────────────────────────────────────────────────


@dataclass
class Event:
    """A single event in the chronological timeline."""

    timestamp: str
    event_type: str  # 'congressional', 'insider', 'dark_pool', 'whale',
    #                   'news', 'prediction', 'price_move', 'regime',
    #                   'crossref', 'earnings', 'macro'
    actor: str | None
    ticker: str
    direction: str  # 'bullish', 'bearish', 'neutral'
    amount_usd: float | None
    description: str
    source: str
    confidence: str
    lead_time_to_next_move: float | None  # computed after the fact


# ── Helpers ────────────────────────────────────────────────────────────────


def _parse_ts(value: Any) -> str:
    """Normalise a date/datetime/string to ISO-8601 string."""
    if value is None:
        return ""
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.isoformat()
    if isinstance(value, date):
        return datetime(value.year, value.month, value.day, tzinfo=timezone.utc).isoformat()
    return str(value)


def _direction_from_signal(signal_type: str | None) -> str:
    """Map a signal_type (BUY/SELL) to a direction label."""
    if not signal_type:
        return "neutral"
    s = str(signal_type).upper()
    if s in ("BUY", "BULLISH", "LONG"):
        return "bullish"
    if s in ("SELL", "BEARISH", "SHORT"):
        return "bearish"
    return "neutral"


def _direction_from_sentiment(sentiment: str | None) -> str:
    """Map a news sentiment string to a direction label."""
    if not sentiment:
        return "neutral"
    s = str(sentiment).lower()
    if "bullish" in s or "positive" in s:
        return "bullish"
    if "bearish" in s or "negative" in s:
        return "bearish"
    return "neutral"


def _confidence_label(score: float | None) -> str:
    """Map a 0-1 score to a confidence label."""
    if score is None:
        return "estimated"
    if score >= 0.8:
        return "confirmed"
    if score >= 0.5:
        return "derived"
    if score >= 0.2:
        return "estimated"
    return "rumored"


def _safe_float(val: Any) -> float | None:
    """Convert to float or return None."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _parse_json_metadata(raw: Any) -> dict[str, Any]:
    """Parse a JSONB / JSON string field safely."""
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return {}


def _get_sector_tickers(engine: Engine, sector: str) -> list[str]:
    """Resolve a sector name or ETF to its constituent tickers.

    Tries the analysis.sector_map first, then falls back to the watchlist table.
    """
    try:
        from analysis.sector_map import SECTOR_MAP

        # Try sector name match (e.g., "Technology")
        sec = SECTOR_MAP.get(sector, {})
        if sec:
            tickers: list[str] = []
            for sub in sec.get("subsectors", {}).values():
                tickers.extend(sub.get("tickers", []))
            etf = sec.get("etf")
            if etf:
                tickers.append(etf)
            if tickers:
                return list(set(tickers))

        # Try ETF match (e.g., "XLK")
        for _name, data in SECTOR_MAP.items():
            if data.get("etf", "").upper() == sector.upper():
                tickers = []
                for sub in data.get("subsectors", {}).values():
                    tickers.extend(sub.get("tickers", []))
                tickers.append(sector.upper())
                return list(set(tickers))
    except ImportError:
        pass

    # Fallback: query watchlist for tickers with matching notes/sector
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT ticker FROM watchlist WHERE active = TRUE ORDER BY ticker"
            )).fetchall()
            return [r[0] for r in rows] if rows else [sector.upper()]
    except Exception:
        return [sector.upper()]


def _get_price_series(
    engine: Engine, ticker: str, days: int,
) -> list[tuple[datetime, float]]:
    """Fetch daily close prices for lead-time computation.

    Returns list of (datetime, price) sorted chronologically.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(days=days + 10)
    prices: list[tuple[datetime, float]] = []

    with engine.connect() as conn:
        # Try options_daily_signals first (has spot_price)
        rows = conn.execute(text("""
            SELECT signal_date, spot_price
            FROM options_daily_signals
            WHERE ticker = :t AND signal_date >= :c AND spot_price > 0
            ORDER BY signal_date
        """), {"t": ticker, "c": cutoff.date()}).fetchall()

        if rows:
            for r in rows:
                dt = datetime(r[0].year, r[0].month, r[0].day, tzinfo=timezone.utc) if isinstance(r[0], date) else r[0]
                prices.append((dt, float(r[1])))
            return prices

        # Fallback: raw_series YF close
        rows = conn.execute(text("""
            SELECT obs_date, value
            FROM raw_series
            WHERE series_id = :sid AND obs_date >= :c AND pull_status = 'SUCCESS'
            ORDER BY obs_date
        """), {"sid": f"YF:{ticker}:close", "c": cutoff.date()}).fetchall()

        for r in rows:
            dt = datetime(r[0].year, r[0].month, r[0].day, tzinfo=timezone.utc) if isinstance(r[0], date) else r[0]
            prices.append((dt, float(r[1])))

    return prices


# ── Event Extraction from Each Source ──────────────────────────────────────


def _pull_signal_source_events(
    engine: Engine, ticker: str, cutoff: datetime,
) -> list[Event]:
    """Pull events from signal_sources table."""
    events: list[Event] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT source_type, source_id, ticker, signal_type,
                       signal_date, signal_value, trust_score,
                       outcome
                FROM signal_sources
                WHERE ticker = :t AND signal_date >= :c
                ORDER BY signal_date DESC
                LIMIT :lim
            """), {"t": ticker, "c": cutoff, "lim": MAX_EVENTS_PER_QUERY}).fetchall()

            for r in rows:
                src_type = r[0] or ""
                src_id = r[1] or ""
                meta = _parse_json_metadata(r[6])
                amount = _safe_float(meta.get("amount_usd") or meta.get("estimated_value"))
                if amount is None:
                    amount = _safe_float(r[5])  # signal_value as fallback

                event_type = _SOURCE_TYPE_MAP.get(src_type.lower(), src_type.lower())

                desc_parts = [f"{src_type} signal from {src_id}"]
                if meta.get("description"):
                    desc_parts.append(str(meta["description"]))
                elif meta.get("transaction_type"):
                    desc_parts.append(str(meta["transaction_type"]))

                outcome_str = str(r[8] or "PENDING")
                if outcome_str != "PENDING":
                    desc_parts.append(f"[outcome: {outcome_str}]")

                events.append(Event(
                    timestamp=_parse_ts(r[4]),
                    event_type=event_type,
                    actor=src_id if src_id else None,
                    ticker=ticker,
                    direction=_direction_from_signal(r[3]),
                    amount_usd=amount,
                    description=" — ".join(desc_parts),
                    source=f"signal_sources:{src_type}",
                    confidence=_confidence_label(_safe_float(r[7])),
                    lead_time_to_next_move=None,
                ))
    except Exception as exc:
        log.debug("signal_sources pull for {t} failed: {e}", t=ticker, e=str(exc))
    return events


def _pull_news_events(
    engine: Engine, ticker: str, cutoff: datetime,
) -> list[Event]:
    """Pull events from news_articles table."""
    events: list[Event] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT title, source, published_at, sentiment, confidence,
                       tickers, summary, llm_summary
                FROM news_articles
                WHERE :ticker = ANY(tickers)
                  AND created_at >= :c
                ORDER BY published_at DESC NULLS LAST
                LIMIT :lim
            """), {"ticker": ticker.upper(), "c": cutoff, "lim": MAX_EVENTS_PER_QUERY}).fetchall()

            for r in rows:
                title = r[0] or "Untitled"
                source = r[1] or "unknown"
                summary = r[6] or r[7] or ""
                desc = f"{title}"
                if summary:
                    desc += f" — {summary[:200]}"

                events.append(Event(
                    timestamp=_parse_ts(r[2]),
                    event_type="news",
                    actor=None,
                    ticker=ticker,
                    direction=_direction_from_sentiment(r[3]),
                    amount_usd=None,
                    description=desc,
                    source=f"news:{source}",
                    confidence=_confidence_label(_safe_float(r[4])),
                    lead_time_to_next_move=None,
                ))
    except Exception as exc:
        log.debug("news_articles pull for {t} failed: {e}", t=ticker, e=str(exc))
    return events


def _pull_options_events(
    engine: Engine, ticker: str, cutoff: datetime,
) -> list[Event]:
    """Pull events from options_daily_signals (regime shifts, unusual volume)."""
    events: list[Event] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT signal_date, put_call_ratio, max_pain, iv_skew,
                       total_oi, total_volume, spot_price, iv_atm
                FROM options_daily_signals
                WHERE ticker = :t AND signal_date >= :c
                ORDER BY signal_date DESC
                LIMIT :lim
            """), {"t": ticker, "c": cutoff.date(), "lim": MAX_EVENTS_PER_QUERY}).fetchall()

            for r in rows:
                sig_date = r[0]
                pcr = _safe_float(r[1])
                max_pain = _safe_float(r[2])
                iv_skew = _safe_float(r[3])
                total_volume = int(r[5]) if r[5] else 0
                spot = _safe_float(r[6])
                iv_atm = _safe_float(r[7])

                # High put/call ratio (bearish signal)
                if pcr is not None and pcr > 1.5:
                    events.append(Event(
                        timestamp=_parse_ts(sig_date),
                        event_type="whale",
                        actor=None,
                        ticker=ticker,
                        direction="bearish",
                        amount_usd=None,
                        description=f"Elevated put/call ratio: {pcr:.2f}",
                        source="options_daily_signals:pcr",
                        confidence="derived",
                        lead_time_to_next_move=None,
                    ))
                elif pcr is not None and pcr < 0.5:
                    events.append(Event(
                        timestamp=_parse_ts(sig_date),
                        event_type="whale",
                        actor=None,
                        ticker=ticker,
                        direction="bullish",
                        amount_usd=None,
                        description=f"Low put/call ratio: {pcr:.2f}",
                        source="options_daily_signals:pcr",
                        confidence="derived",
                        lead_time_to_next_move=None,
                    ))

                # IV skew anomaly
                if iv_skew is not None and abs(iv_skew) > 0.15:
                    direction = "bearish" if iv_skew > 0 else "bullish"
                    events.append(Event(
                        timestamp=_parse_ts(sig_date),
                        event_type="whale",
                        actor=None,
                        ticker=ticker,
                        direction=direction,
                        amount_usd=None,
                        description=f"IV skew anomaly: {iv_skew:.3f}",
                        source="options_daily_signals:iv_skew",
                        confidence="derived",
                        lead_time_to_next_move=None,
                    ))
    except Exception as exc:
        log.debug("options_daily_signals pull for {t} failed: {e}", t=ticker, e=str(exc))
    return events


def _pull_decision_journal_events(
    engine: Engine, ticker: str, cutoff: datetime,
) -> list[Event]:
    """Pull events from decision_journal (model decisions mentioning ticker)."""
    events: list[Event] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT decision_timestamp, inferred_state, state_confidence,
                       grid_recommendation, action_taken,
                       outcome_value, outcome_recorded_at
                FROM decision_journal
                WHERE decision_timestamp >= :c
                  AND (
                      grid_recommendation ILIKE :pat
                      OR action_taken ILIKE :pat
                      OR inferred_state ILIKE :pat
                  )
                ORDER BY decision_timestamp DESC
                LIMIT :lim
            """), {
                "c": cutoff,
                "pat": f"%{ticker}%",
                "lim": MAX_EVENTS_PER_QUERY,
            }).fetchall()

            for r in rows:
                rec = r[3] or ""
                action = r[4] or ""
                state = r[1] or ""
                conf = _safe_float(r[2])
                outcome = _safe_float(r[5])

                desc = f"Model decision: state={state}"
                if rec:
                    desc += f", recommendation={rec[:100]}"
                if action:
                    desc += f", action={action[:100]}"
                if outcome is not None:
                    desc += f", outcome={outcome:.4f}"

                # Direction from recommendation text
                direction = "neutral"
                combined = f"{rec} {action}".lower()
                if any(w in combined for w in ("buy", "long", "bullish", "overweight")):
                    direction = "bullish"
                elif any(w in combined for w in ("sell", "short", "bearish", "underweight")):
                    direction = "bearish"

                events.append(Event(
                    timestamp=_parse_ts(r[0]),
                    event_type="prediction",
                    actor=None,
                    ticker=ticker,
                    direction=direction,
                    amount_usd=None,
                    description=desc,
                    source="decision_journal",
                    confidence=_confidence_label(conf),
                    lead_time_to_next_move=None,
                ))
    except Exception as exc:
        log.debug("decision_journal pull for {t} failed: {e}", t=ticker, e=str(exc))
    return events


def _pull_crossref_events(
    engine: Engine, ticker: str, cutoff: datetime,
) -> list[Event]:
    """Pull macro cross-reference checks that may affect the ticker."""
    events: list[Event] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT name, category, assessment, implication,
                       divergence_zscore, confidence, checked_at,
                       official_source, physical_source
                FROM cross_reference_checks
                WHERE checked_at >= :c
                  AND (assessment IN ('major_divergence', 'contradiction')
                       OR ABS(divergence_zscore) >= 2.0)
                ORDER BY checked_at DESC
                LIMIT :lim
            """), {"c": cutoff, "lim": MAX_EVENTS_PER_QUERY}).fetchall()

            for r in rows:
                name = r[0] or "unknown check"
                category = r[1] or ""
                assessment = r[2] or ""
                implication = r[3] or ""
                zscore = _safe_float(r[4])
                conf = _safe_float(r[5])

                desc = f"Cross-ref: {name} ({category}) — {assessment}"
                if implication:
                    desc += f" — {implication[:150]}"
                if zscore is not None:
                    desc += f" [z={zscore:.1f}]"

                # Direction from implication text
                direction = "neutral"
                impl_lower = implication.lower()
                if any(w in impl_lower for w in ("bullish", "positive", "growth", "expansion")):
                    direction = "bullish"
                elif any(w in impl_lower for w in ("bearish", "negative", "contraction", "recession", "risk")):
                    direction = "bearish"

                events.append(Event(
                    timestamp=_parse_ts(r[6]),
                    event_type="crossref" if category != "macro" else "macro",
                    actor=None,
                    ticker=ticker,
                    direction=direction,
                    amount_usd=None,
                    description=desc,
                    source=f"cross_reference:{category}",
                    confidence=_confidence_label(conf),
                    lead_time_to_next_move=None,
                ))
    except Exception as exc:
        log.debug("cross_reference_checks pull for {t} failed: {e}", t=ticker, e=str(exc))
    return events


def _pull_earnings_events(
    engine: Engine, ticker: str, cutoff: datetime,
) -> list[Event]:
    """Pull events from earnings_calendar."""
    events: list[Event] = []
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT earnings_date, fiscal_quarter, eps_estimate, eps_actual,
                       eps_surprise_pct, revenue_estimate, revenue_actual,
                       revenue_surprise_pct, classification, reported
                FROM earnings_calendar
                WHERE ticker = :t AND earnings_date >= :c
                ORDER BY earnings_date DESC
                LIMIT :lim
            """), {"t": ticker, "c": cutoff.date(), "lim": MAX_EVENTS_PER_QUERY}).fetchall()

            for r in rows:
                quarter = r[1] or ""
                eps_est = _safe_float(r[2])
                eps_act = _safe_float(r[3])
                surprise = _safe_float(r[4])
                rev_est = _safe_float(r[5])
                rev_act = _safe_float(r[6])
                rev_surprise = _safe_float(r[7])
                classification = r[8] or "pending"
                reported = r[9]

                if reported and eps_act is not None:
                    # Reported earnings — include surprise info
                    direction = "neutral"
                    if surprise is not None:
                        if surprise > 5.0:
                            direction = "bullish"
                        elif surprise < -5.0:
                            direction = "bearish"

                    desc = f"Earnings {quarter}: EPS actual={eps_act:.2f}"
                    if eps_est is not None:
                        desc += f" (est={eps_est:.2f})"
                    if surprise is not None:
                        desc += f", surprise={surprise:+.1f}%"
                    if rev_act is not None:
                        desc += f", revenue=${rev_act/1e9:.2f}B" if rev_act > 1e6 else f", revenue=${rev_act:.0f}"
                    if classification:
                        desc += f" [{classification}]"

                    amount = rev_act if rev_act else None
                else:
                    # Upcoming / unported earnings
                    direction = "neutral"
                    desc = f"Earnings scheduled {quarter}"
                    if eps_est is not None:
                        desc += f": EPS est={eps_est:.2f}"
                    if rev_est is not None:
                        desc += f", revenue est=${rev_est/1e9:.2f}B" if rev_est > 1e6 else ""
                    amount = None

                events.append(Event(
                    timestamp=_parse_ts(r[0]),
                    event_type="earnings",
                    actor=None,
                    ticker=ticker,
                    direction=direction,
                    amount_usd=amount,
                    description=desc,
                    source="earnings_calendar",
                    confidence="confirmed" if reported else "estimated",
                    lead_time_to_next_move=None,
                ))
    except Exception as exc:
        log.debug("earnings_calendar pull for {t} failed: {e}", t=ticker, e=str(exc))
    return events


def _pull_price_move_events(
    engine: Engine, ticker: str, cutoff: datetime,
) -> list[Event]:
    """Detect significant daily price moves (>1%) and emit price_move events."""
    events: list[Event] = []
    prices = _get_price_series(engine, ticker, days=int((datetime.now(timezone.utc) - cutoff).days))
    if len(prices) < 2:
        return events

    for i in range(1, len(prices)):
        prev_dt, prev_price = prices[i - 1]
        cur_dt, cur_price = prices[i]
        if prev_price <= 0:
            continue

        pct_change = ((cur_price - prev_price) / prev_price) * 100.0
        if abs(pct_change) >= SIGNIFICANT_MOVE_PCT:
            direction = "bullish" if pct_change > 0 else "bearish"
            events.append(Event(
                timestamp=_parse_ts(cur_dt),
                event_type="price_move",
                actor=None,
                ticker=ticker,
                direction=direction,
                amount_usd=None,
                description=f"Price move: {pct_change:+.2f}% (${prev_price:.2f} -> ${cur_price:.2f})",
                source="price_data",
                confidence="confirmed",
                lead_time_to_next_move=None,
            ))

    return events


# ── Core Functions ─────────────────────────────────────────────────────────


def build_sequence(
    engine: Engine,
    ticker: str,
    days: int = 90,
) -> list[Event]:
    """Build a chronological timeline of ALL events for a ticker.

    Pulls from signal_sources, news_articles, options_daily_signals,
    decision_journal, cross_reference_checks, and earnings_calendar.
    Sorts all events chronologically.

    Parameters:
        engine: SQLAlchemy engine.
        ticker: Stock ticker symbol.
        days: Lookback window in days.

    Returns:
        List of Event instances sorted by timestamp (oldest first).
    """
    ticker = ticker.upper()
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    log.info("Building event sequence for {t} ({d} days)", t=ticker, d=days)

    # Pull from all sources — each gracefully degrades on failure
    all_events: list[Event] = []
    all_events.extend(_pull_signal_source_events(engine, ticker, cutoff))
    all_events.extend(_pull_news_events(engine, ticker, cutoff))
    all_events.extend(_pull_options_events(engine, ticker, cutoff))
    all_events.extend(_pull_decision_journal_events(engine, ticker, cutoff))
    all_events.extend(_pull_crossref_events(engine, ticker, cutoff))
    all_events.extend(_pull_earnings_events(engine, ticker, cutoff))
    all_events.extend(_pull_price_move_events(engine, ticker, cutoff))

    # Sort chronologically (oldest first)
    all_events.sort(key=lambda e: e.timestamp or "")

    log.info(
        "Event sequence for {t}: {n} events from {d} days",
        t=ticker, n=len(all_events), d=days,
    )
    return all_events


def build_sector_sequence(
    engine: Engine,
    sector: str,
    days: int = 90,
) -> list[Event]:
    """Build a chronological timeline for all tickers in a sector.

    Resolves the sector to its constituent tickers via analysis.sector_map
    or the watchlist table, then aggregates events across all tickers.

    Parameters:
        engine: SQLAlchemy engine.
        sector: Sector name (e.g., "Technology") or ETF symbol (e.g., "XLK").
        days: Lookback window in days.

    Returns:
        List of Event instances sorted by timestamp (oldest first).
    """
    tickers = _get_sector_tickers(engine, sector)
    log.info(
        "Building sector sequence for {s}: {n} tickers ({d} days)",
        s=sector, n=len(tickers), d=days,
    )

    all_events: list[Event] = []
    for ticker in tickers:
        all_events.extend(build_sequence(engine, ticker, days=days))

    # Re-sort the combined list chronologically
    all_events.sort(key=lambda e: e.timestamp or "")

    log.info(
        "Sector sequence for {s}: {n} total events",
        s=sector, n=len(all_events),
    )
    return all_events


def compute_lead_times(
    events: list[Event],
    price_series: list[tuple[datetime, float]],
) -> list[Event]:
    """For each event, compute hours until the next >1% price move.

    Parameters:
        events: List of Event instances (chronologically sorted).
        price_series: List of (datetime, price) tuples, sorted chronologically.

    Returns:
        The same list of Event instances with lead_time_to_next_move populated.
    """
    if not price_series or len(price_series) < 2:
        return events

    # Pre-compute significant move timestamps
    move_times: list[tuple[datetime, float]] = []
    for i in range(1, len(price_series)):
        prev_dt, prev_price = price_series[i - 1]
        cur_dt, cur_price = price_series[i]
        if prev_price <= 0:
            continue
        pct = abs((cur_price - prev_price) / prev_price) * 100.0
        if pct >= SIGNIFICANT_MOVE_PCT:
            move_times.append((cur_dt, pct))

    if not move_times:
        return events

    # For each event, find the next significant move after it
    for event in events:
        if not event.timestamp:
            continue
        try:
            if "T" in event.timestamp:
                event_dt = datetime.fromisoformat(event.timestamp.replace("Z", "+00:00"))
            else:
                event_dt = datetime.fromisoformat(event.timestamp)
            if event_dt.tzinfo is None:
                event_dt = event_dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue

        # Binary-style search for the next move after this event
        for move_dt, _move_pct in move_times:
            if move_dt.tzinfo is None:
                move_dt = move_dt.replace(tzinfo=timezone.utc)
            if move_dt > event_dt:
                delta_hours = (move_dt - event_dt).total_seconds() / 3600.0
                event.lead_time_to_next_move = round(delta_hours, 2)
                break

    return events


def find_recurring_patterns(
    engine: Engine,
    min_occurrences: int = DEFAULT_MIN_OCCURRENCES,
) -> list[dict[str, Any]]:
    """Detect recurring event sequences across all tickers.

    Looks for 2- and 3-event sequences (by event_type + direction) that
    repeat at least min_occurrences times within PATTERN_WINDOW_HOURS.

    Example patterns detected:
      - "insider sell -> dark_pool spike -> price drop" (5 occurrences)
      - "congressional buy -> price move bullish" (8 occurrences)

    Parameters:
        engine: SQLAlchemy engine.
        min_occurrences: Minimum times a pattern must occur to be reported.

    Returns:
        List of pattern dicts with keys: pattern, occurrences, tickers, avg_lead_hours, examples.
    """
    log.info("Scanning for recurring event patterns (min_occurrences={m})", m=min_occurrences)

    # Pull all scored signals and their outcomes to find patterns
    cutoff = datetime.now(timezone.utc) - timedelta(days=365)
    all_events: list[Event] = []

    try:
        with engine.connect() as conn:
            # Get active tickers from watchlist
            rows = conn.execute(text(
                "SELECT DISTINCT ticker FROM watchlist WHERE active = TRUE LIMIT 100"
            )).fetchall()
            tickers = [r[0] for r in rows] if rows else []
    except Exception:
        tickers = []

    if not tickers:
        # Fallback: get tickers from signal_sources
        try:
            with engine.connect() as conn:
                rows = conn.execute(text(
                    "SELECT DISTINCT ticker FROM signal_sources "
                    "WHERE signal_date >= :c LIMIT 100"
                ), {"c": cutoff}).fetchall()
                tickers = [r[0] for r in rows] if rows else []
        except Exception:
            tickers = []

    if not tickers:
        log.info("No tickers found for pattern detection")
        return []

    # Build events per ticker
    events_by_ticker: dict[str, list[Event]] = {}
    for ticker in tickers:
        seq = build_sequence(engine, ticker, days=365)
        if seq:
            events_by_ticker[ticker] = seq

    # Extract 2-event and 3-event sequences within the window
    pair_counts: dict[str, list[dict]] = defaultdict(list)
    triple_counts: dict[str, list[dict]] = defaultdict(list)

    for ticker, events in events_by_ticker.items():
        for i in range(len(events)):
            ev_a = events[i]
            if not ev_a.timestamp:
                continue
            try:
                dt_a = datetime.fromisoformat(ev_a.timestamp.replace("Z", "+00:00"))
            except (ValueError, TypeError):
                continue

            key_a = f"{ev_a.event_type}:{ev_a.direction}"

            # Look for pairs
            for j in range(i + 1, min(i + 20, len(events))):
                ev_b = events[j]
                if not ev_b.timestamp:
                    continue
                try:
                    dt_b = datetime.fromisoformat(ev_b.timestamp.replace("Z", "+00:00"))
                except (ValueError, TypeError):
                    continue

                gap_hours = (dt_b - dt_a).total_seconds() / 3600.0
                if gap_hours > PATTERN_WINDOW_HOURS:
                    break
                if gap_hours < 0:
                    continue

                key_b = f"{ev_b.event_type}:{ev_b.direction}"
                pair_key = f"{key_a} -> {key_b}"
                pair_counts[pair_key].append({
                    "ticker": ticker,
                    "ts_a": ev_a.timestamp,
                    "ts_b": ev_b.timestamp,
                    "gap_hours": round(gap_hours, 1),
                    "desc_a": ev_a.description[:80],
                    "desc_b": ev_b.description[:80],
                })

                # Look for triples
                for k in range(j + 1, min(j + 10, len(events))):
                    ev_c = events[k]
                    if not ev_c.timestamp:
                        continue
                    try:
                        dt_c = datetime.fromisoformat(ev_c.timestamp.replace("Z", "+00:00"))
                    except (ValueError, TypeError):
                        continue

                    gap_bc = (dt_c - dt_b).total_seconds() / 3600.0
                    if gap_bc > PATTERN_WINDOW_HOURS:
                        break

                    key_c = f"{ev_c.event_type}:{ev_c.direction}"
                    triple_key = f"{key_a} -> {key_b} -> {key_c}"
                    triple_counts[triple_key].append({
                        "ticker": ticker,
                        "ts_a": ev_a.timestamp,
                        "ts_b": ev_b.timestamp,
                        "ts_c": ev_c.timestamp,
                        "gap_hours_ab": round(gap_hours, 1),
                        "gap_hours_bc": round(gap_bc, 1),
                    })

    # Filter to patterns meeting the min_occurrences threshold
    patterns: list[dict[str, Any]] = []

    for pattern_key, occurrences in sorted(
        triple_counts.items(), key=lambda x: -len(x[1]),
    ):
        if len(occurrences) < min_occurrences:
            continue
        unique_tickers = list(set(o["ticker"] for o in occurrences))
        avg_gap = sum(
            o.get("gap_hours_ab", 0) + o.get("gap_hours_bc", 0)
            for o in occurrences
        ) / max(len(occurrences), 1)
        patterns.append({
            "pattern": pattern_key,
            "sequence_length": 3,
            "occurrences": len(occurrences),
            "tickers": unique_tickers[:20],
            "avg_total_gap_hours": round(avg_gap, 1),
            "examples": occurrences[:5],
        })

    for pattern_key, occurrences in sorted(
        pair_counts.items(), key=lambda x: -len(x[1]),
    ):
        if len(occurrences) < min_occurrences:
            continue
        unique_tickers = list(set(o["ticker"] for o in occurrences))
        avg_gap = sum(o.get("gap_hours", 0) for o in occurrences) / max(len(occurrences), 1)
        patterns.append({
            "pattern": pattern_key,
            "sequence_length": 2,
            "occurrences": len(occurrences),
            "tickers": unique_tickers[:20],
            "avg_total_gap_hours": round(avg_gap, 1),
            "examples": occurrences[:5],
        })

    # Sort by occurrences descending, longer sequences first
    patterns.sort(key=lambda p: (-p["sequence_length"], -p["occurrences"]))

    # Cross-reference with the pattern engine (if available) to enrich
    # results with hit-rate and confidence data.
    try:
        from intelligence.pattern_engine import _load_patterns as _load_engine_patterns
        stored = _load_engine_patterns(engine)
        stored_map = {"->".join(p.sequence): p for p in stored}
        for pat in patterns:
            key = pat["pattern"]
            if key in stored_map:
                sp = stored_map[key]
                pat["hit_rate"] = sp.hit_rate
                pat["confidence"] = sp.confidence
                pat["actionable"] = sp.actionable
                pat["avg_return_after"] = sp.avg_return_after
    except Exception:
        pass  # pattern engine not initialised yet — fine

    log.info("Found {n} recurring patterns", n=len(patterns))
    return patterns


# ── Convenience ────────────────────────────────────────────────────────────


def build_sequence_with_lead_times(
    engine: Engine,
    ticker: str,
    days: int = 90,
) -> list[Event]:
    """Build event sequence and compute lead times in one call.

    Combines build_sequence() and compute_lead_times() for the common case.
    """
    events = build_sequence(engine, ticker, days=days)
    if not events:
        return events

    price_series = _get_price_series(engine, ticker, days=days)
    return compute_lead_times(events, price_series)


def events_to_dicts(events: list[Event]) -> list[dict[str, Any]]:
    """Convert a list of Event instances to serialisable dicts."""
    return [asdict(e) for e in events]
