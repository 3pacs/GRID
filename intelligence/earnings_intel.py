"""
GRID Intelligence — Earnings Analysis & Prediction System.

Pre-earnings: uses options IV, historical surprise patterns, sector context,
and congressional/insider activity to predict post-earnings reaction.

Post-earnings: analyses surprise %, price reaction vs historical, and
tracks prediction accuracy over time.

Pipeline:
  1. get_earnings_calendar    — upcoming earnings for watchlist tickers
  2. analyze_earnings_surprise — post-earnings: surprise %, price reaction
  3. predict_earnings_reaction — pre-earnings: IV, patterns, sector context
  4. get_prediction_scorecard  — how good are our earnings predictions?
  5. run_earnings_cycle        — full cycle for hermes scheduling
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass, asdict
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Data Classes ─────────────────────────────────────────────────────────

@dataclass
class EarningsPrediction:
    """A prediction for post-earnings price reaction."""
    id: str
    ticker: str
    earnings_date: str
    predicted_direction: str      # up / down / flat
    predicted_move_pct: float     # expected % move
    confidence: float             # 0-1
    iv_rank: float | None         # current IV percentile
    historical_surprise_avg: float | None
    historical_beat_rate: float | None
    sector_momentum: float | None
    insider_signal: str | None    # bullish / bearish / neutral / none
    congressional_signal: str | None
    reasoning: str
    # Scoring (filled post-earnings)
    actual_direction: str | None = None
    actual_move_pct: float | None = None
    verdict: str = "pending"      # hit / miss / partial / pending
    scored_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Table Setup ──────────────────────────────────────────────────────────

def _ensure_tables(engine: Engine) -> None:
    """Create earnings prediction tables if they don't exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS earnings_predictions (
                id TEXT PRIMARY KEY,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                ticker TEXT NOT NULL,
                earnings_date DATE NOT NULL,
                predicted_direction TEXT NOT NULL,
                predicted_move_pct DOUBLE PRECISION,
                confidence DOUBLE PRECISION,
                iv_rank DOUBLE PRECISION,
                historical_surprise_avg DOUBLE PRECISION,
                historical_beat_rate DOUBLE PRECISION,
                sector_momentum DOUBLE PRECISION,
                insider_signal TEXT,
                congressional_signal TEXT,
                reasoning TEXT,
                actual_direction TEXT,
                actual_move_pct DOUBLE PRECISION,
                verdict TEXT DEFAULT 'pending',
                scored_at TIMESTAMPTZ,
                UNIQUE (ticker, earnings_date)
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_earnings_pred_date
            ON earnings_predictions (earnings_date)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_earnings_pred_verdict
            ON earnings_predictions (verdict)
        """))


# ── 1. Earnings Calendar ────────────────────────────────────────────────

def get_earnings_calendar(engine: Engine, days_ahead: int = 30) -> list[dict]:
    """Upcoming earnings for watchlist tickers.

    Returns enriched calendar entries with IV data, historical surprise
    patterns, and any pre-existing predictions.

    Args:
        engine: SQLAlchemy engine.
        days_ahead: Days ahead to look.

    Returns:
        List of upcoming earnings event dicts with enrichment.
    """
    _ensure_tables(engine)
    cutoff = date.today() + timedelta(days=days_ahead)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT ec.ticker, ec.earnings_date, ec.fiscal_quarter,
                   ec.eps_estimate, ec.revenue_estimate, ec.reported,
                   ep.predicted_direction, ep.predicted_move_pct,
                   ep.confidence, ep.verdict
            FROM earnings_calendar ec
            LEFT JOIN earnings_predictions ep
                ON ec.ticker = ep.ticker AND ec.earnings_date = ep.earnings_date
            WHERE ec.earnings_date >= CURRENT_DATE
              AND ec.earnings_date <= :cutoff
            ORDER BY ec.earnings_date ASC, ec.ticker ASC
        """), {"cutoff": cutoff}).fetchall()

    results = []
    for r in rows:
        entry = {
            "ticker": r[0],
            "earnings_date": r[1].isoformat() if r[1] else None,
            "fiscal_quarter": r[2],
            "eps_estimate": r[3],
            "revenue_estimate": r[4],
            "reported": r[5],
            "days_until": (r[1] - date.today()).days if r[1] else None,
            "prediction": None,
        }
        if r[6]:
            entry["prediction"] = {
                "direction": r[6],
                "move_pct": r[7],
                "confidence": r[8],
                "verdict": r[9],
            }

        # Enrich with IV data
        iv_data = _get_iv_data(conn, r[0])
        if iv_data:
            entry["iv_rank"] = iv_data.get("iv_rank")
            entry["iv_atm"] = iv_data.get("iv_atm")
            entry["expected_move_options"] = iv_data.get("expected_move")

        results.append(entry)

    return results


# ── 2. Post-Earnings Surprise Analysis ──────────────────────────────────

def analyze_earnings_surprise(engine: Engine, ticker: str) -> dict:
    """Post-earnings analysis: surprise %, price reaction, vs historical.

    Combines the latest earnings result with price data to measure the
    actual market reaction and compare against the ticker's history.

    Args:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol.

    Returns:
        Dict with surprise analysis, price reaction, and historical comparison.
    """
    _ensure_tables(engine)

    with engine.connect() as conn:
        # Latest reported earnings
        latest = conn.execute(text("""
            SELECT earnings_date, eps_estimate, eps_actual, eps_surprise_pct,
                   revenue_estimate, revenue_actual, revenue_surprise_pct,
                   classification
            FROM earnings_calendar
            WHERE ticker = :ticker AND reported = TRUE
            ORDER BY earnings_date DESC LIMIT 1
        """), {"ticker": ticker}).fetchone()

        if not latest:
            return {"ticker": ticker, "status": "no_reported_earnings"}

        earn_date = latest[0]
        eps_est, eps_act, eps_surprise = latest[1], latest[2], latest[3]
        rev_est, rev_act, rev_surprise = latest[4], latest[5], latest[6]
        classification = latest[7]

        # Price reaction: close day before vs close day after
        price_before = _get_price_near(conn, ticker, earn_date - timedelta(days=1))
        price_after = _get_price_near(conn, ticker, earn_date + timedelta(days=1))

        price_reaction_pct = None
        if price_before and price_after:
            price_reaction_pct = round((price_after - price_before) / price_before * 100, 2)

        # Historical surprise pattern for this ticker
        history = conn.execute(text("""
            SELECT eps_surprise_pct, classification
            FROM earnings_calendar
            WHERE ticker = :ticker AND reported = TRUE
            ORDER BY earnings_date DESC LIMIT 12
        """), {"ticker": ticker}).fetchall()

        hist_surprises = [float(h[0]) for h in history if h[0] is not None]
        hist_beats = sum(1 for h in history if h[1] == "beat")
        hist_misses = sum(1 for h in history if h[1] == "miss")
        hist_total = len(history)

        result = {
            "ticker": ticker,
            "earnings_date": earn_date.isoformat(),
            "eps": {
                "estimate": eps_est,
                "actual": eps_act,
                "surprise_pct": round(eps_surprise, 2) if eps_surprise is not None else None,
            },
            "revenue": {
                "estimate": rev_est,
                "actual": rev_act,
                "surprise_pct": round(rev_surprise, 2) if rev_surprise is not None else None,
            },
            "classification": classification,
            "price_reaction_pct": price_reaction_pct,
            "historical": {
                "avg_surprise_pct": round(np.mean(hist_surprises), 2) if hist_surprises else None,
                "median_surprise_pct": round(float(np.median(hist_surprises)), 2) if hist_surprises else None,
                "beat_rate": round(hist_beats / hist_total, 2) if hist_total > 0 else None,
                "miss_rate": round(hist_misses / hist_total, 2) if hist_total > 0 else None,
                "total_quarters": hist_total,
            },
        }

        # Score our prediction if we had one
        pred = conn.execute(text("""
            SELECT id, predicted_direction, predicted_move_pct, verdict
            FROM earnings_predictions
            WHERE ticker = :ticker AND earnings_date = :edate
        """), {"ticker": ticker, "edate": earn_date}).fetchone()

        if pred and pred[3] == "pending" and price_reaction_pct is not None:
            result["our_prediction"] = _score_prediction(
                engine, pred[0], pred[1], pred[2], price_reaction_pct,
            )

    return result


# ── 3. Pre-Earnings Reaction Prediction ─────────────────────────────────

def predict_earnings_reaction(engine: Engine, ticker: str) -> dict:
    """Pre-earnings prediction using IV, historical patterns, sector context.

    IV crush post-earnings is predictable. Historical surprise direction
    shows persistent patterns (some companies consistently beat by X%).
    Sector momentum going into earnings provides context. Congressional
    and insider activity pre-earnings may signal informed trading.

    Args:
        engine: SQLAlchemy engine.
        ticker: Ticker symbol.

    Returns:
        Dict with prediction details and reasoning.
    """
    _ensure_tables(engine)
    signals: dict[str, Any] = {}
    reasoning_parts: list[str] = []

    with engine.connect() as conn:
        # Check upcoming earnings date
        upcoming = conn.execute(text("""
            SELECT earnings_date, eps_estimate, revenue_estimate
            FROM earnings_calendar
            WHERE ticker = :ticker AND earnings_date >= CURRENT_DATE
            ORDER BY earnings_date ASC LIMIT 1
        """), {"ticker": ticker}).fetchone()

        if not upcoming:
            return {"ticker": ticker, "status": "no_upcoming_earnings"}

        earn_date = upcoming[0]
        eps_estimate = upcoming[1]

        # ── Signal 1: Historical surprise pattern ──
        hist = conn.execute(text("""
            SELECT eps_surprise_pct, classification
            FROM earnings_calendar
            WHERE ticker = :ticker AND reported = TRUE
            ORDER BY earnings_date DESC LIMIT 8
        """), {"ticker": ticker}).fetchall()

        hist_surprises = [float(h[0]) for h in hist if h[0] is not None]
        hist_beats = sum(1 for h in hist if h[1] == "beat")
        hist_total = len(hist)

        if hist_surprises:
            avg_surprise = float(np.mean(hist_surprises))
            beat_rate = hist_beats / hist_total if hist_total > 0 else 0.5
            signals["historical_surprise_avg"] = round(avg_surprise, 2)
            signals["historical_beat_rate"] = round(beat_rate, 2)

            if beat_rate >= 0.75:
                reasoning_parts.append(
                    f"Strong beat history: {beat_rate:.0%} beat rate over {hist_total} quarters, "
                    f"avg surprise {avg_surprise:+.1f}%"
                )
            elif beat_rate <= 0.25:
                reasoning_parts.append(
                    f"Weak beat history: {beat_rate:.0%} beat rate over {hist_total} quarters"
                )

        # ── Signal 2: Options IV (implied volatility) ──
        iv_data = _get_iv_data(conn, ticker)
        if iv_data:
            signals["iv_rank"] = iv_data.get("iv_rank")
            iv_atm = iv_data.get("iv_atm")
            if iv_atm:
                # Expected move from options = IV * sqrt(1/252)
                expected_move = iv_atm * 100 * math.sqrt(1 / 252)
                signals["expected_move_options"] = round(expected_move, 2)
                reasoning_parts.append(
                    f"Options imply {expected_move:.1f}% move. "
                    f"IV rank: {iv_data.get('iv_rank', 'N/A')}"
                )

        # ── Signal 3: Sector momentum ──
        sector_mom = _get_sector_momentum(conn, ticker)
        if sector_mom is not None:
            signals["sector_momentum"] = round(sector_mom, 3)
            if sector_mom > 0.02:
                reasoning_parts.append(f"Sector tailwind: +{sector_mom*100:.1f}% momentum")
            elif sector_mom < -0.02:
                reasoning_parts.append(f"Sector headwind: {sector_mom*100:.1f}% momentum")

        # ── Signal 4: Congressional / insider activity ──
        insider_sig = _get_insider_signal(conn, ticker)
        signals["insider_signal"] = insider_sig
        if insider_sig in ("bullish", "bearish"):
            reasoning_parts.append(f"Insider activity pre-earnings: {insider_sig}")

        congress_sig = _get_congressional_signal(conn, ticker)
        signals["congressional_signal"] = congress_sig
        if congress_sig in ("bullish", "bearish"):
            reasoning_parts.append(f"Congressional trading pre-earnings: {congress_sig} (informed?)")

        # ── Combine signals into prediction ──
        bull_score = 0.0
        bear_score = 0.0

        # Historical pattern weight
        beat_rate = signals.get("historical_beat_rate", 0.5)
        if beat_rate > 0.5:
            bull_score += (beat_rate - 0.5) * 4.0
        else:
            bear_score += (0.5 - beat_rate) * 4.0

        # Sector momentum
        sec_mom = signals.get("sector_momentum", 0)
        if sec_mom > 0:
            bull_score += min(sec_mom * 10, 1.0)
        else:
            bear_score += min(abs(sec_mom) * 10, 1.0)

        # Insider/congressional
        for sig in [insider_sig, congress_sig]:
            if sig == "bullish":
                bull_score += 0.5
            elif sig == "bearish":
                bear_score += 0.5

        # Direction
        if bull_score > bear_score + 0.3:
            direction = "up"
            net = bull_score - bear_score
        elif bear_score > bull_score + 0.3:
            direction = "down"
            net = bear_score - bull_score
        else:
            direction = "flat"
            net = 0.0

        # Confidence
        confidence = min(0.9, max(0.1, net / 3.0))

        # Expected move magnitude
        hist_avg = abs(signals.get("historical_surprise_avg", 0))
        opt_move = signals.get("expected_move_options", 2.0)
        predicted_move = (hist_avg * 0.3 + opt_move * 0.7) if opt_move else hist_avg
        if direction == "down":
            predicted_move = -predicted_move

        # Build reasoning
        if not reasoning_parts:
            reasoning_parts.append("Limited signal data — low confidence prediction")

        reasoning = "; ".join(reasoning_parts)

        # Create prediction
        pred_id = hashlib.md5(
            f"earnings:{ticker}:{earn_date.isoformat()}".encode()
        ).hexdigest()[:16]

        pred = EarningsPrediction(
            id=pred_id,
            ticker=ticker,
            earnings_date=earn_date.isoformat(),
            predicted_direction=direction,
            predicted_move_pct=round(predicted_move, 2),
            confidence=round(confidence, 3),
            iv_rank=signals.get("iv_rank"),
            historical_surprise_avg=signals.get("historical_surprise_avg"),
            historical_beat_rate=signals.get("historical_beat_rate"),
            sector_momentum=signals.get("sector_momentum"),
            insider_signal=insider_sig,
            congressional_signal=congress_sig,
            reasoning=reasoning,
        )

        # Store prediction
        _store_prediction(engine, pred)

    return pred.to_dict()


# ── 4. Prediction Scorecard ─────────────────────────────────────────────

def get_prediction_scorecard(engine: Engine) -> dict[str, Any]:
    """How good are our earnings predictions? Full track record.

    Returns:
        Dict with overall stats, per-direction accuracy, trend, and
        recent predictions with outcomes.
    """
    _ensure_tables(engine)

    with engine.connect() as conn:
        # Overall stats
        scored = conn.execute(text("""
            SELECT verdict, COUNT(*) as cnt
            FROM earnings_predictions
            WHERE verdict != 'pending'
            GROUP BY verdict
        """)).fetchall()

        totals = {r[0]: r[1] for r in scored}
        total_scored = sum(totals.values())
        hits = totals.get("hit", 0)
        overall_pct = round(hits / total_scored * 100, 1) if total_scored > 0 else 0.0

        # Per-direction accuracy
        dir_stats = conn.execute(text("""
            SELECT predicted_direction,
                   COUNT(*) as total,
                   COUNT(*) FILTER (WHERE verdict = 'hit') as hits
            FROM earnings_predictions
            WHERE verdict != 'pending'
            GROUP BY predicted_direction
        """)).fetchall()

        per_direction = [
            {
                "direction": r[0],
                "total": r[1],
                "hits": r[2],
                "accuracy_pct": round(r[2] / r[1] * 100, 1) if r[1] > 0 else 0,
            }
            for r in dir_stats
        ]

        # Recent predictions with outcomes
        recent = conn.execute(text("""
            SELECT ticker, earnings_date, predicted_direction, predicted_move_pct,
                   confidence, actual_direction, actual_move_pct, verdict, scored_at
            FROM earnings_predictions
            ORDER BY earnings_date DESC
            LIMIT 30
        """)).fetchall()

        recent_list = [
            {
                "ticker": r[0],
                "earnings_date": r[1].isoformat() if r[1] else None,
                "predicted_direction": r[2],
                "predicted_move_pct": r[3],
                "confidence": r[4],
                "actual_direction": r[5],
                "actual_move_pct": r[6],
                "verdict": r[7],
                "scored_at": r[8].isoformat() if r[8] else None,
            }
            for r in recent
        ]

        # Confidence calibration: are high-confidence predictions more accurate?
        calibration = conn.execute(text("""
            SELECT
                CASE
                    WHEN confidence >= 0.7 THEN 'high'
                    WHEN confidence >= 0.4 THEN 'medium'
                    ELSE 'low'
                END as bucket,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE verdict = 'hit') as hits
            FROM earnings_predictions
            WHERE verdict != 'pending'
            GROUP BY 1
        """)).fetchall()

        calibration_data = [
            {
                "bucket": r[0],
                "total": r[1],
                "hits": r[2],
                "accuracy_pct": round(r[2] / r[1] * 100, 1) if r[1] > 0 else 0,
            }
            for r in calibration
        ]

    return {
        "overall": {
            "accuracy_pct": overall_pct,
            "total_scored": total_scored,
            "hits": hits,
            "misses": totals.get("miss", 0),
            "partials": totals.get("partial", 0),
            "pending": _count_pending(engine),
        },
        "per_direction": per_direction,
        "calibration": calibration_data,
        "recent": recent_list,
    }


# ── 5. Full Earnings Cycle ──────────────────────────────────────────────

def run_earnings_cycle(engine: Engine) -> dict[str, Any]:
    """Run a full earnings intelligence cycle.

    1. Score past predictions that now have results
    2. Generate predictions for upcoming earnings
    3. Return summary

    Args:
        engine: SQLAlchemy engine.

    Returns:
        Summary dict.
    """
    _ensure_tables(engine)
    report: dict[str, Any] = {}

    # 1. Score pending predictions with reported results
    scored = _score_pending_predictions(engine)
    report["scored"] = scored

    # 2. Generate predictions for upcoming earnings without one
    predictions_generated = 0
    with engine.connect() as conn:
        upcoming = conn.execute(text("""
            SELECT ec.ticker
            FROM earnings_calendar ec
            LEFT JOIN earnings_predictions ep
                ON ec.ticker = ep.ticker AND ec.earnings_date = ep.earnings_date
            WHERE ec.earnings_date >= CURRENT_DATE
              AND ec.earnings_date <= CURRENT_DATE + 14
              AND ec.reported = FALSE
              AND ep.id IS NULL
            ORDER BY ec.earnings_date ASC
            LIMIT 50
        """)).fetchall()

    for row in upcoming:
        try:
            predict_earnings_reaction(engine, row[0])
            predictions_generated += 1
        except Exception as e:
            log.warning("Prediction failed for {t}: {e}", t=row[0], e=str(e))

    report["predictions_generated"] = predictions_generated

    # 3. Scorecard summary
    try:
        scorecard = get_prediction_scorecard(engine)
        report["scorecard"] = scorecard["overall"]
    except Exception as e:
        log.warning("Scorecard failed: {e}", e=str(e))

    log.info(
        "Earnings cycle complete: scored={s}, new_predictions={p}",
        s=scored, p=predictions_generated,
    )
    return report


# ── Helper Functions ─────────────────────────────────────────────────────

def _get_iv_data(conn, ticker: str) -> dict | None:
    """Get latest IV data from options_daily_signals."""
    try:
        row = conn.execute(text("""
            SELECT iv_atm, iv_skew, term_structure_slope, spot_price
            FROM options_daily_signals
            WHERE ticker = :t AND iv_atm IS NOT NULL AND iv_atm >= 0.03
            ORDER BY signal_date DESC LIMIT 1
        """), {"t": ticker}).fetchone()
        if not row:
            return None

        iv_atm = float(row[0]) if row[0] else None

        # IV rank: compare current IV to 52-week range
        iv_rank = None
        if iv_atm:
            hist = conn.execute(text("""
                SELECT iv_atm FROM options_daily_signals
                WHERE ticker = :t AND iv_atm IS NOT NULL
                AND signal_date >= CURRENT_DATE - 252
                ORDER BY signal_date DESC
            """), {"t": ticker}).fetchall()
            if len(hist) > 10:
                ivs = sorted(float(h[0]) for h in hist if h[0])
                rank_pos = sum(1 for iv in ivs if iv <= iv_atm)
                iv_rank = round(rank_pos / len(ivs) * 100, 1)

        return {
            "iv_atm": iv_atm,
            "iv_rank": iv_rank,
            "expected_move": round(iv_atm * 100 * math.sqrt(1/252), 2) if iv_atm else None,
        }
    except Exception:
        return None


def _get_sector_momentum(conn, ticker: str) -> float | None:
    """Get sector momentum for the ticker's sector."""
    try:
        # Try to get sector from capital_flow_snapshots relative_strength
        row = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id LIKE :pattern AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 1
        """), {"pattern": f"YF:{ticker}:close"}).fetchone()

        if not row:
            return None

        # Get 20-day return
        rows = conn.execute(text("""
            SELECT value FROM raw_series
            WHERE series_id = :sid AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 20
        """), {"sid": f"YF:{ticker}:close"}).fetchall()

        if len(rows) >= 2:
            latest = float(rows[0][0])
            oldest = float(rows[-1][0])
            if oldest > 0:
                return (latest - oldest) / oldest
    except Exception:
        pass
    return None


def _get_insider_signal(conn, ticker: str) -> str:
    """Check for recent insider buying/selling activity."""
    try:
        rows = conn.execute(text("""
            SELECT raw_payload FROM raw_series
            WHERE series_id LIKE :pattern
            AND obs_date >= CURRENT_DATE - 30
            AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 10
        """), {"pattern": f"INSIDER:%:{ticker}:%"}).fetchall()

        if not rows:
            return "none"

        buys = sum(1 for r in rows if r[0] and "purchase" in str(r[0]).lower())
        sells = sum(1 for r in rows if r[0] and "sale" in str(r[0]).lower())

        if buys > sells + 1:
            return "bullish"
        elif sells > buys + 1:
            return "bearish"
        return "neutral"
    except Exception:
        return "none"


def _get_congressional_signal(conn, ticker: str) -> str:
    """Check for recent congressional trading in this ticker."""
    try:
        rows = conn.execute(text("""
            SELECT raw_payload FROM raw_series
            WHERE series_id LIKE :pattern
            AND obs_date >= CURRENT_DATE - 60
            AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC LIMIT 10
        """), {"pattern": f"CONGRESS:%:{ticker}:%"}).fetchall()

        if not rows:
            return "none"

        buys = sum(1 for r in rows if r[0] and "purchase" in str(r[0]).lower())
        sells = sum(1 for r in rows if r[0] and "sale" in str(r[0]).lower())

        if buys > sells:
            return "bullish"
        elif sells > buys:
            return "bearish"
        return "neutral"
    except Exception:
        return "none"


def _get_price_near(conn, ticker: str, target_date: date) -> float | None:
    """Get price at or near a specific date."""
    row = conn.execute(text("""
        SELECT spot_price FROM options_daily_signals
        WHERE ticker = :t AND signal_date <= :d AND spot_price > 0
        ORDER BY signal_date DESC LIMIT 1
    """), {"t": ticker, "d": target_date}).fetchone()
    if row:
        return float(row[0])

    row = conn.execute(text("""
        SELECT value FROM raw_series
        WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
        ORDER BY obs_date DESC LIMIT 1
    """), {"sid": f"YF:{ticker}:close", "d": target_date}).fetchone()
    return float(row[0]) if row else None


def _store_prediction(engine: Engine, pred: EarningsPrediction) -> None:
    """Store an earnings prediction."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO earnings_predictions
                (id, ticker, earnings_date, predicted_direction, predicted_move_pct,
                 confidence, iv_rank, historical_surprise_avg, historical_beat_rate,
                 sector_momentum, insider_signal, congressional_signal, reasoning)
            VALUES
                (:id, :ticker, :edate, :dir, :move, :conf, :iv, :hist_avg,
                 :beat_rate, :sec_mom, :insider, :congress, :reasoning)
            ON CONFLICT (ticker, earnings_date)
            DO UPDATE SET
                predicted_direction = EXCLUDED.predicted_direction,
                predicted_move_pct = EXCLUDED.predicted_move_pct,
                confidence = EXCLUDED.confidence,
                reasoning = EXCLUDED.reasoning
        """), {
            "id": pred.id,
            "ticker": pred.ticker,
            "edate": pred.earnings_date,
            "dir": pred.predicted_direction,
            "move": pred.predicted_move_pct,
            "conf": pred.confidence,
            "iv": pred.iv_rank,
            "hist_avg": pred.historical_surprise_avg,
            "beat_rate": pred.historical_beat_rate,
            "sec_mom": pred.sector_momentum,
            "insider": pred.insider_signal,
            "congress": pred.congressional_signal,
            "reasoning": pred.reasoning,
        })


def _score_prediction(
    engine: Engine,
    pred_id: str,
    predicted_dir: str,
    predicted_move: float | None,
    actual_move_pct: float,
) -> dict:
    """Score a single prediction against reality."""
    actual_dir = "up" if actual_move_pct > 0.5 else "down" if actual_move_pct < -0.5 else "flat"

    # Verdict logic
    if predicted_dir == actual_dir:
        verdict = "hit"
    elif predicted_dir == "flat" and abs(actual_move_pct) < 2.0:
        verdict = "partial"
    elif actual_dir == "flat":
        verdict = "partial"
    else:
        verdict = "miss"

    with engine.begin() as conn:
        conn.execute(text("""
            UPDATE earnings_predictions
            SET actual_direction = :adir, actual_move_pct = :amove,
                verdict = :verdict, scored_at = NOW()
            WHERE id = :id
        """), {
            "adir": actual_dir,
            "amove": round(actual_move_pct, 2),
            "verdict": verdict,
            "id": pred_id,
        })

    return {
        "prediction_id": pred_id,
        "predicted_direction": predicted_dir,
        "actual_direction": actual_dir,
        "actual_move_pct": round(actual_move_pct, 2),
        "verdict": verdict,
    }


def _score_pending_predictions(engine: Engine) -> int:
    """Score all pending predictions that now have reported earnings."""
    scored = 0
    with engine.connect() as conn:
        pending = conn.execute(text("""
            SELECT ep.id, ep.ticker, ep.earnings_date,
                   ep.predicted_direction, ep.predicted_move_pct
            FROM earnings_predictions ep
            JOIN earnings_calendar ec
                ON ep.ticker = ec.ticker AND ep.earnings_date = ec.earnings_date
            WHERE ep.verdict = 'pending'
              AND ec.reported = TRUE
              AND ep.earnings_date <= CURRENT_DATE
        """)).fetchall()

    for p in pending:
        pred_id, ticker, earn_date, pred_dir, pred_move = p
        with engine.connect() as conn:
            price_before = _get_price_near(conn, ticker, earn_date - timedelta(days=1))
            price_after = _get_price_near(conn, ticker, earn_date + timedelta(days=1))

        if price_before and price_after:
            actual_move = (price_after - price_before) / price_before * 100
            _score_prediction(engine, pred_id, pred_dir, pred_move, actual_move)
            scored += 1

    return scored


def _count_pending(engine: Engine) -> int:
    """Count pending predictions."""
    with engine.connect() as conn:
        row = conn.execute(text(
            "SELECT COUNT(*) FROM earnings_predictions WHERE verdict = 'pending'"
        )).fetchone()
    return row[0] if row else 0
