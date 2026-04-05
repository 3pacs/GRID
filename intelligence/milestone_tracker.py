"""
Milestone Tracker — plot company milestones on a timeline, score execution.

Uses earnings data, income statements, and news to build a milestone
timeline for each company. Tracks whether they're meeting, exceeding,
or kicking the can on guidance and growth targets.

Key insight: companies that consistently beat → price premium.
Companies that miss or defer → the market eventually punishes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


@dataclass(frozen=True)
class Milestone:
    """A company milestone on the timeline."""
    ticker: str
    date: date
    category: str           # earnings, revenue, guidance, product, regulatory, leadership
    description: str
    expected_value: float | None
    actual_value: float | None
    beat_miss: str          # BEAT, MISS, MET, DEFERRED, UNKNOWN
    magnitude: float        # % above/below expectation
    price_impact_5d: float | None  # 5-day price change after milestone


def build_earnings_timeline(engine: Engine, ticker: str) -> list[Milestone]:
    """Build a milestone timeline from earnings data.

    Pulls EPS, revenue, and financial statement data to construct
    a quarter-by-quarter scorecard.

    Args:
        engine: SQLAlchemy engine.
        ticker: Stock ticker.

    Returns:
        List of Milestone objects, sorted chronologically.
    """
    milestones: list[Milestone] = []

    with engine.connect() as conn:
        # Get earnings data (EPS surprises)
        eps_rows = conn.execute(text("""
            SELECT obs_date, value, raw_payload
            FROM raw_series
            WHERE series_id = :sid AND pull_status = 'SUCCESS'
            ORDER BY obs_date
        """), {"sid": f"av:earnings:{ticker}:eps"}).fetchall()

        for row in eps_rows:
            obs = row[0]
            eps_actual = float(row[1])
            payload = row[2] if isinstance(row[2], dict) else {}

            eps_estimate = float(payload.get("estimate", payload.get("estimatedEPS", 0)) or 0)
            surprise = float(payload.get("surprise_pct", payload.get("surprisePercentage", 0)) or 0)

            if eps_estimate != 0:
                beat_miss = "BEAT" if surprise > 2 else "MISS" if surprise < -2 else "MET"
            else:
                beat_miss = "UNKNOWN"

            milestones.append(Milestone(
                ticker=ticker,
                date=obs,
                category="earnings",
                description=f"Q EPS: ${eps_actual:.2f} vs est ${eps_estimate:.2f} ({surprise:+.1f}%)",
                expected_value=eps_estimate,
                actual_value=eps_actual,
                beat_miss=beat_miss,
                magnitude=surprise,
                price_impact_5d=None,  # Filled later
            ))

        # Get revenue data
        rev_rows = conn.execute(text("""
            SELECT obs_date, value
            FROM raw_series
            WHERE series_id = :sid AND pull_status = 'SUCCESS'
            ORDER BY obs_date
        """), {"sid": f"av:income:{ticker}:revenue"}).fetchall()

        prev_rev = None
        for row in rev_rows:
            obs = row[0]
            revenue = float(row[1])
            rev_b = revenue / 1e9

            if prev_rev and prev_rev > 0:
                growth = (revenue - prev_rev) / prev_rev * 100
                beat_miss = "BEAT" if growth > 5 else "MISS" if growth < -5 else "MET"
                milestones.append(Milestone(
                    ticker=ticker,
                    date=obs,
                    category="revenue",
                    description=f"Q Revenue: ${rev_b:.1f}B ({growth:+.1f}% QoQ)",
                    expected_value=None,
                    actual_value=revenue,
                    beat_miss=beat_miss,
                    magnitude=growth,
                    price_impact_5d=None,
                ))
            prev_rev = revenue

        # Get margin data (operating income / revenue)
        income_rows = conn.execute(text("""
            SELECT r1.obs_date, r1.value AS operating, r2.value AS revenue
            FROM raw_series r1
            JOIN raw_series r2 ON r1.obs_date = r2.obs_date
            WHERE r1.series_id = :oi_sid AND r2.series_id = :rev_sid
            AND r1.pull_status = 'SUCCESS' AND r2.pull_status = 'SUCCESS'
            ORDER BY r1.obs_date
        """), {
            "oi_sid": f"av:income:{ticker}:operating_income",
            "rev_sid": f"av:income:{ticker}:revenue",
        }).fetchall()

        prev_margin = None
        for row in income_rows:
            obs = row[0]
            operating = float(row[1])
            revenue = float(row[2])
            if revenue > 0:
                margin = operating / revenue * 100
                if prev_margin is not None:
                    delta = margin - prev_margin
                    beat_miss = "BEAT" if delta > 1 else "MISS" if delta < -1 else "MET"
                    milestones.append(Milestone(
                        ticker=ticker,
                        date=obs,
                        category="margin",
                        description=f"Op Margin: {margin:.1f}% ({delta:+.1f}pp QoQ)",
                        expected_value=prev_margin,
                        actual_value=margin,
                        beat_miss=beat_miss,
                        magnitude=delta,
                        price_impact_5d=None,
                    ))
                prev_margin = margin

    # Sort chronologically
    milestones.sort(key=lambda m: m.date)
    return milestones


def score_execution(milestones: list[Milestone]) -> dict[str, Any]:
    """Score a company's execution based on milestone history.

    Returns a scorecard with beat/miss rates, consistency,
    trend direction, and overall grade.
    """
    if not milestones:
        return {"grade": "N/A", "beats": 0, "misses": 0, "total": 0}

    beats = sum(1 for m in milestones if m.beat_miss == "BEAT")
    misses = sum(1 for m in milestones if m.beat_miss == "MISS")
    mets = sum(1 for m in milestones if m.beat_miss == "MET")
    total = beats + misses + mets

    beat_rate = beats / total if total > 0 else 0

    # Trend: are recent milestones better or worse than older ones?
    if len(milestones) >= 4:
        recent = milestones[-4:]
        older = milestones[:-4]
        recent_beat_rate = sum(1 for m in recent if m.beat_miss == "BEAT") / len(recent)
        older_beat_rate = sum(1 for m in older if m.beat_miss == "BEAT") / max(len(older), 1)
        trend = "IMPROVING" if recent_beat_rate > older_beat_rate + 0.1 else \
                "DECLINING" if recent_beat_rate < older_beat_rate - 0.1 else "STABLE"
    else:
        trend = "INSUFFICIENT_DATA"

    # Consecutive beats/misses (streak)
    streak = 0
    streak_type = ""
    for m in reversed(milestones):
        if m.beat_miss in ("BEAT", "MISS"):
            if not streak_type:
                streak_type = m.beat_miss
            if m.beat_miss == streak_type:
                streak += 1
            else:
                break

    # Grade
    if beat_rate >= 0.8:
        grade = "A"
    elif beat_rate >= 0.6:
        grade = "B"
    elif beat_rate >= 0.4:
        grade = "C"
    elif beat_rate >= 0.2:
        grade = "D"
    else:
        grade = "F"

    # Adjust for trend
    if trend == "IMPROVING" and grade != "A":
        grade += "+"
    elif trend == "DECLINING" and grade != "F":
        grade += "-"

    return {
        "grade": grade,
        "beat_rate": round(beat_rate, 3),
        "beats": beats,
        "misses": misses,
        "mets": mets,
        "total": total,
        "trend": trend,
        "streak": streak,
        "streak_type": streak_type,
        "avg_magnitude": round(sum(m.magnitude for m in milestones) / len(milestones), 2),
    }


def scan_all_tickers(engine: Engine, tickers: list[str] | None = None) -> list[dict[str, Any]]:
    """Scan all tickers and rank by execution quality.

    Args:
        engine: SQLAlchemy engine.
        tickers: Override ticker list.

    Returns:
        List of scorecard dicts, sorted by grade.
    """
    if tickers is None:
        # Get tickers that have earnings data
        with engine.connect() as conn:
            rows = conn.execute(text(
                "SELECT DISTINCT split_part(series_id, ':', 3) AS ticker "
                "FROM raw_series "
                "WHERE series_id LIKE :pattern "
                "AND pull_status = 'SUCCESS'"
            ), {"pattern": "av:earnings:%:eps"}).fetchall()
            tickers = [r[0] for r in rows]

    results: list[dict[str, Any]] = []
    for ticker in tickers:
        milestones = build_earnings_timeline(engine, ticker)
        if milestones:
            score = score_execution(milestones)
            score["ticker"] = ticker
            score["milestones"] = len(milestones)
            results.append(score)

    # Sort by grade then beat_rate
    grade_order = {"A+": 0, "A": 1, "A-": 2, "B+": 3, "B": 4, "B-": 5,
                   "C+": 6, "C": 7, "C-": 8, "D+": 9, "D": 10, "D-": 11, "F": 12}
    results.sort(key=lambda r: (grade_order.get(r["grade"], 99), -r["beat_rate"]))

    return results
