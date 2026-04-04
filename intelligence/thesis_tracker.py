"""
GRID Intelligence — Thesis Version Tracker & Post-Mortem System.

Every unified thesis (overall direction + model states) is versioned and
archived.  After the scoring window elapses, the thesis is compared against
actual market movement.  Wrong or partially-correct theses trigger automated
post-mortems that explain what went wrong, which models were right or wrong,
and what lessons should inform future thesis generation.

Pipeline:
  1. snapshot_thesis         — archive the current thesis with full model states
  2. score_old_theses        — compare unscored theses to actual SPY movement
  3. generate_thesis_postmortem — LLM-assisted failure analysis for wrong theses
  4. get_thesis_history      — evolution of thinking over time
  5. get_thesis_accuracy     — accuracy stats with per-model breakdowns
  6. run_thesis_cycle        — full cycle for hermes scheduling

Scoring rules:
  - Thesis said bullish  and SPY up   >0.5%  → correct
  - Thesis said bearish  and SPY down >0.5%  → correct
  - Thesis said bullish  and SPY down >0.5%  → wrong
  - Thesis said bearish  and SPY up   >0.5%  → wrong
  - Everything else                           → partial

Designed to be called by hermes_operator or via the API.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Data Classes ──────────────────────────────────────────────────────────

ROOT_CAUSES = [
    "model_disagreement_ignored",
    "external_shock",
    "bad_data",
    "thesis_outdated",
    "correct_but_early",
]


@dataclass
class ThesisSnapshot:
    """A versioned snapshot of the unified thesis at a point in time."""

    id: int
    timestamp: str
    overall_direction: str          # bullish / bearish / neutral
    conviction: float               # 0-1
    key_drivers: list[str]
    risk_factors: list[str]
    model_states: dict              # {model_name: {direction, confidence, current_state}}
    narrative: str

    # Filled later by scoring
    outcome: str | None = None      # correct / wrong / partial
    actual_market_move: float | None = None  # SPY % change over scoring period
    scored_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class ThesisPostMortem:
    """Post-mortem analysis for a thesis that got it wrong."""

    snapshot_id: int
    thesis_direction: str
    actual_direction: str
    models_that_were_right: list[str]
    models_that_were_wrong: list[str]
    what_we_missed: str
    root_cause: str                 # one of ROOT_CAUSES
    lesson: str
    generated_at: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ── Table Setup ───────────────────────────────────────────────────────────

def _ensure_tables(engine: Engine) -> None:
    """Create thesis tracking tables if they do not exist."""
    with engine.begin() as conn:
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS thesis_snapshots (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                overall_direction TEXT NOT NULL,
                conviction NUMERIC,
                key_drivers JSONB,
                risk_factors JSONB,
                model_states JSONB,
                narrative TEXT,
                outcome TEXT,
                actual_market_move NUMERIC,
                scored_at TIMESTAMPTZ
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_thesis_snapshots_ts
                ON thesis_snapshots (timestamp DESC)
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_thesis_snapshots_unscored
                ON thesis_snapshots (timestamp)
                WHERE outcome IS NULL
        """))
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS thesis_postmortems (
                id SERIAL PRIMARY KEY,
                snapshot_id INT REFERENCES thesis_snapshots(id),
                thesis_direction TEXT,
                actual_direction TEXT,
                models_right JSONB,
                models_wrong JSONB,
                what_we_missed TEXT,
                root_cause TEXT,
                lesson TEXT,
                generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """))
        conn.execute(text("""
            CREATE INDEX IF NOT EXISTS idx_thesis_pm_snapshot
                ON thesis_postmortems (snapshot_id)
        """))


# ── 1. Snapshot Current Thesis ────────────────────────────────────────────

def snapshot_thesis(engine: Engine, thesis_data: dict) -> int:
    """Archive the current unified thesis with full model states.

    Args:
        engine: SQLAlchemy engine.
        thesis_data: Dict with keys: overall_direction, conviction,
            key_drivers, risk_factors, model_states, narrative.

    Returns:
        Snapshot ID.
    """
    _ensure_tables(engine)

    direction = thesis_data.get("overall_direction", "neutral")
    conviction = float(thesis_data.get("conviction", 0.5))
    key_drivers = thesis_data.get("key_drivers", [])
    risk_factors = thesis_data.get("risk_factors", [])
    model_states = thesis_data.get("model_states", {})
    narrative = thesis_data.get("narrative", "")

    with engine.begin() as conn:
        row = conn.execute(text("""
            INSERT INTO thesis_snapshots
                (overall_direction, conviction, key_drivers, risk_factors,
                 model_states, narrative)
            VALUES
                (:direction, :conviction, :drivers, :risks, :states, :narrative)
            RETURNING id
        """), {
            "direction": direction,
            "conviction": conviction,
            "drivers": json.dumps(key_drivers),
            "risks": json.dumps(risk_factors),
            "states": json.dumps(model_states, default=str),
            "narrative": narrative,
        }).fetchone()

    snapshot_id = row[0]
    log.info(
        "Thesis snapshot #{id} archived: {d} (conviction {c:.0f}%)",
        id=snapshot_id, d=direction, c=conviction,
    )
    return snapshot_id


# ── 2. Score Old Theses ───────────────────────────────────────────────────

def score_old_theses(engine: Engine, lookback_days: int = 90) -> dict[str, Any]:
    """Score unscored thesis snapshots older than 3 days.

    For each unscored snapshot, compare its direction to actual SPY
    movement over the 3 days following the snapshot.

    Args:
        engine: SQLAlchemy engine.
        lookback_days: How far back to look for unscored snapshots (default 90).

    Returns:
        Summary dict with counts of correct/wrong/partial.
    """
    _ensure_tables(engine)

    cutoff = datetime.now(timezone.utc) - timedelta(days=lookback_days)
    scoring_age = datetime.now(timezone.utc) - timedelta(days=3)

    results = {"correct": 0, "wrong": 0, "partial": 0, "skipped": 0}

    with engine.begin() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, overall_direction
            FROM thesis_snapshots
            WHERE outcome IS NULL
              AND timestamp >= :cutoff
              AND timestamp <= :scoring_age
            ORDER BY timestamp
        """), {"cutoff": cutoff, "scoring_age": scoring_age}).fetchall()

        for row in rows:
            snap_id, snap_ts, direction = row[0], row[1], row[2]

            # Get SPY price at snapshot time and 3 days later
            spy_at_snap = _get_spy_price_near(conn, snap_ts)
            spy_after = _get_spy_price_near(
                conn, snap_ts + timedelta(days=3),
            )

            if spy_at_snap is None or spy_after is None:
                results["skipped"] += 1
                continue

            pct_move = (spy_after - spy_at_snap) / spy_at_snap * 100

            # Score
            if direction == "bullish" and pct_move > 0.5:
                outcome = "correct"
            elif direction == "bearish" and pct_move < -0.5:
                outcome = "correct"
            elif direction == "bullish" and pct_move < -0.5:
                outcome = "wrong"
            elif direction == "bearish" and pct_move > 0.5:
                outcome = "wrong"
            elif direction == "neutral" and abs(pct_move) <= 0.5:
                outcome = "correct"
            else:
                outcome = "partial"

            conn.execute(text("""
                UPDATE thesis_snapshots
                SET outcome = :outcome,
                    actual_market_move = :move,
                    scored_at = NOW()
                WHERE id = :id
            """), {"outcome": outcome, "move": round(pct_move, 4), "id": snap_id})

            results[outcome] += 1

    total = results["correct"] + results["wrong"] + results["partial"]
    log.info(
        "Scored {n} theses: {c}C/{w}W/{p}P (skipped {s})",
        n=total, c=results["correct"], w=results["wrong"],
        p=results["partial"], s=results["skipped"],
    )
    return results


def _get_spy_price_near(conn, target_ts) -> float | None:
    """Get SPY price nearest to a target timestamp.

    Tries options_daily_signals first, then raw_series as fallback.
    """
    target_date = target_ts.date() if hasattr(target_ts, "date") else target_ts

    # Try options_daily_signals (most common SPY source)
    row = conn.execute(text("""
        SELECT spot_price FROM options_daily_signals
        WHERE ticker = 'SPY'
          AND signal_date <= :d
          AND spot_price > 0
        ORDER BY signal_date DESC
        LIMIT 1
    """), {"d": target_date}).fetchone()
    if row:
        return float(row[0])

    # Fallback to raw_series
    row = conn.execute(text("""
        SELECT value FROM raw_series
        WHERE series_id = 'YF:SPY:close'
          AND obs_date <= :d
          AND pull_status = 'SUCCESS'
        ORDER BY obs_date DESC
        LIMIT 1
    """), {"d": target_date}).fetchone()
    if row:
        return float(row[0])

    return None


# ── 3. Generate Thesis Post-Mortem ────────────────────────────────────────

def generate_thesis_postmortem(
    engine: Engine, snapshot_id: int,
) -> ThesisPostMortem | None:
    """Generate a post-mortem for a wrong or partial thesis.

    Analyses which models were right vs wrong, uses LLM to determine
    root cause and produce an actionable lesson.

    Args:
        engine: SQLAlchemy engine.
        snapshot_id: thesis_snapshots.id

    Returns:
        ThesisPostMortem or None if snapshot not found or not wrong/partial.
    """
    _ensure_tables(engine)

    with engine.connect() as conn:
        snap = conn.execute(text("""
            SELECT id, timestamp, overall_direction, conviction,
                   key_drivers, risk_factors, model_states, narrative,
                   outcome, actual_market_move, scored_at
            FROM thesis_snapshots
            WHERE id = :id
        """), {"id": snapshot_id}).fetchone()

    if not snap:
        log.warning("Thesis post-mortem: snapshot {id} not found", id=snapshot_id)
        return None

    (sid, ts, direction, conviction, drivers_json, risks_json,
     states_json, narrative, outcome, actual_move, scored_at) = snap

    if outcome not in ("wrong", "partial"):
        log.debug(
            "Thesis post-mortem: snapshot {id} outcome={o} — not a failure",
            id=snapshot_id, o=outcome,
        )
        return None

    # Check if post-mortem already exists
    with engine.connect() as conn:
        existing = conn.execute(text("""
            SELECT id FROM thesis_postmortems WHERE snapshot_id = :sid
        """), {"sid": snapshot_id}).fetchone()
    if existing:
        log.debug("Thesis post-mortem already exists for snapshot {id}", id=snapshot_id)
        return None

    # Parse stored JSON
    key_drivers = _parse_json(drivers_json, [])
    risk_factors = _parse_json(risks_json, [])
    model_states = _parse_json(states_json, {})
    actual_move_f = float(actual_move) if actual_move is not None else 0.0

    # Determine actual direction
    if actual_move_f > 0.5:
        actual_direction = "bullish"
    elif actual_move_f < -0.5:
        actual_direction = "bearish"
    else:
        actual_direction = "neutral"

    # Which models were right vs wrong
    models_right = []
    models_wrong = []
    for model_name, state in model_states.items():
        model_dir = state.get("direction", "neutral") if isinstance(state, dict) else "neutral"
        # Normalise direction labels
        model_dir_norm = _normalise_direction(model_dir)
        if model_dir_norm == actual_direction:
            models_right.append(model_name)
        elif model_dir_norm != "neutral":
            models_wrong.append(model_name)

    # Classify root cause (rule-based first, LLM refines)
    root_cause = _classify_root_cause(
        direction=direction,
        actual_direction=actual_direction,
        actual_move=actual_move_f,
        models_right=models_right,
        models_wrong=models_wrong,
        model_states=model_states,
    )

    # LLM-assisted analysis
    llm_result = _get_llm_thesis_postmortem(
        direction=direction,
        actual_direction=actual_direction,
        actual_move=actual_move_f,
        key_drivers=key_drivers,
        risk_factors=risk_factors,
        model_states=model_states,
        narrative=narrative or "",
        models_right=models_right,
        models_wrong=models_wrong,
        root_cause=root_cause,
    )

    what_missed = llm_result.get("what_we_missed", "Analysis unavailable.")
    lesson = llm_result.get("lesson", f"Re-evaluate weight of {', '.join(models_right) or 'dissenting models'}.")

    now = datetime.now(timezone.utc)

    pm = ThesisPostMortem(
        snapshot_id=snapshot_id,
        thesis_direction=direction,
        actual_direction=actual_direction,
        models_that_were_right=models_right,
        models_that_were_wrong=models_wrong,
        what_we_missed=what_missed,
        root_cause=root_cause,
        lesson=lesson,
        generated_at=now.isoformat(),
    )

    # Persist
    _store_thesis_postmortem(engine, pm)

    log.info(
        "Thesis post-mortem for snapshot #{id}: {d}→{a} (root: {r})",
        id=snapshot_id, d=direction, a=actual_direction, r=root_cause,
    )
    return pm


# ── 4. Thesis History ─────────────────────────────────────────────────────

def get_thesis_history(engine: Engine, days: int = 90) -> list[ThesisSnapshot]:
    """Return archived theses with outcomes over the lookback window.

    Args:
        engine: SQLAlchemy engine.
        days: Lookback window in days.

    Returns:
        List of ThesisSnapshot ordered by timestamp descending.
    """
    _ensure_tables(engine)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT id, timestamp, overall_direction, conviction,
                   key_drivers, risk_factors, model_states, narrative,
                   outcome, actual_market_move, scored_at
            FROM thesis_snapshots
            WHERE timestamp >= :cutoff
            ORDER BY timestamp DESC
        """), {"cutoff": cutoff}).fetchall()

    snapshots = []
    for r in rows:
        snapshots.append(ThesisSnapshot(
            id=r[0],
            timestamp=r[1].isoformat() if r[1] else "",
            overall_direction=r[2],
            conviction=float(r[3]) if r[3] is not None else 0.0,
            key_drivers=_parse_json(r[4], []),
            risk_factors=_parse_json(r[5], []),
            model_states=_parse_json(r[6], {}),
            narrative=r[7] or "",
            outcome=r[8],
            actual_market_move=float(r[9]) if r[9] is not None else None,
            scored_at=r[10].isoformat() if r[10] else None,
        ))

    return snapshots


# ── 5. Thesis Accuracy Stats ─────────────────────────────────────────────

def get_thesis_accuracy(engine: Engine) -> dict[str, Any]:
    """Compute thesis accuracy statistics.

    Returns:
        Dict with overall accuracy, per-model accuracy, trend over time,
        and best-condition analysis.
    """
    _ensure_tables(engine)

    with engine.connect() as conn:
        # Overall accuracy last 90 days
        scored = conn.execute(text("""
            SELECT outcome, COUNT(*) as cnt,
                   AVG(actual_market_move) as avg_move
            FROM thesis_snapshots
            WHERE outcome IS NOT NULL
              AND timestamp >= NOW() - INTERVAL '90 days'
            GROUP BY outcome
        """)).fetchall()

        totals = {r[0]: {"count": r[1], "avg_move": float(r[2]) if r[2] else 0.0} for r in scored}
        total_scored = sum(v["count"] for v in totals.values())
        correct_count = totals.get("correct", {}).get("count", 0)
        overall_pct = correct_count / total_scored * 100 if total_scored > 0 else 0.0

        # Per-model accuracy: which models aligned with the correct outcome most often
        model_accuracy = _compute_model_accuracy(conn)

        # Trend: accuracy in 30-day windows
        trend = _compute_accuracy_trend(conn)

        # Best conditions: conviction level vs outcome
        best_conditions = _compute_best_conditions(conn)

    return {
        "overall": {
            "accuracy_pct": round(overall_pct, 1),
            "total_scored": total_scored,
            "correct": totals.get("correct", {}).get("count", 0),
            "wrong": totals.get("wrong", {}).get("count", 0),
            "partial": totals.get("partial", {}).get("count", 0),
        },
        "per_model": model_accuracy,
        "trend": trend,
        "best_conditions": best_conditions,
    }


def _compute_model_accuracy(conn) -> list[dict]:
    """For each model stored in model_states, compute how often it matched
    the actual market direction on scored theses."""
    rows = conn.execute(text("""
        SELECT model_states, outcome, actual_market_move
        FROM thesis_snapshots
        WHERE outcome IS NOT NULL
          AND model_states IS NOT NULL
          AND timestamp >= NOW() - INTERVAL '90 days'
    """)).fetchall()

    # Accumulate per model
    model_stats: dict[str, dict] = {}
    for r in rows:
        states = _parse_json(r[0], {})
        outcome = r[1]
        actual_move = float(r[2]) if r[2] is not None else 0.0

        if actual_move > 0.5:
            actual_dir = "bullish"
        elif actual_move < -0.5:
            actual_dir = "bearish"
        else:
            actual_dir = "neutral"

        for model_name, state in states.items():
            if model_name not in model_stats:
                model_stats[model_name] = {"correct": 0, "wrong": 0, "total": 0}

            model_dir = _normalise_direction(
                state.get("direction", "neutral") if isinstance(state, dict) else "neutral"
            )
            model_stats[model_name]["total"] += 1
            if model_dir == actual_dir:
                model_stats[model_name]["correct"] += 1
            elif model_dir != "neutral":
                model_stats[model_name]["wrong"] += 1

    result = []
    for name, stats in sorted(model_stats.items(), key=lambda x: -x[1].get("correct", 0)):
        total = stats["total"]
        pct = stats["correct"] / total * 100 if total > 0 else 0.0
        result.append({
            "model": name,
            "accuracy_pct": round(pct, 1),
            "correct": stats["correct"],
            "wrong": stats["wrong"],
            "total": total,
        })

    return result


def _compute_accuracy_trend(conn) -> list[dict]:
    """Compute accuracy in 30-day rolling windows for the last 90 days."""
    rows = conn.execute(text("""
        SELECT
            DATE_TRUNC('month', timestamp) as month,
            COUNT(*) FILTER (WHERE outcome = 'correct') as correct,
            COUNT(*) FILTER (WHERE outcome = 'wrong') as wrong,
            COUNT(*) FILTER (WHERE outcome = 'partial') as partial,
            COUNT(*) as total
        FROM thesis_snapshots
        WHERE outcome IS NOT NULL
          AND timestamp >= NOW() - INTERVAL '90 days'
        GROUP BY DATE_TRUNC('month', timestamp)
        ORDER BY month
    """)).fetchall()

    trend = []
    for r in rows:
        total = r[4]
        correct = r[1]
        pct = correct / total * 100 if total > 0 else 0.0
        trend.append({
            "month": r[0].isoformat() if r[0] else "",
            "accuracy_pct": round(pct, 1),
            "correct": correct,
            "wrong": r[2],
            "partial": r[3],
            "total": total,
        })

    return trend


def _compute_best_conditions(conn) -> dict:
    """Analyse when the thesis works best — by conviction level."""
    rows = conn.execute(text("""
        SELECT conviction, outcome
        FROM thesis_snapshots
        WHERE outcome IS NOT NULL
          AND conviction IS NOT NULL
          AND timestamp >= NOW() - INTERVAL '90 days'
    """)).fetchall()

    # Bucket by conviction: low (0-0.4), medium (0.4-0.7), high (0.7-1.0)
    buckets: dict[str, dict] = {
        "low_conviction": {"correct": 0, "total": 0},
        "medium_conviction": {"correct": 0, "total": 0},
        "high_conviction": {"correct": 0, "total": 0},
    }

    for r in rows:
        conv = float(r[0]) if r[0] is not None else 0.5
        outcome = r[1]

        if conv < 0.4:
            bucket = "low_conviction"
        elif conv < 0.7:
            bucket = "medium_conviction"
        else:
            bucket = "high_conviction"

        buckets[bucket]["total"] += 1
        if outcome == "correct":
            buckets[bucket]["correct"] += 1

    for k, v in buckets.items():
        v["accuracy_pct"] = round(
            v["correct"] / v["total"] * 100 if v["total"] > 0 else 0.0, 1,
        )

    return buckets


# ── 6. Full Thesis Cycle ─────────────────────────────────────────────────

def run_thesis_cycle(engine: Engine) -> dict[str, Any]:
    """Run a full thesis tracking cycle for hermes scheduling.

    1. Snapshot current thesis (if available from flow_thesis)
    2. Score old unscored theses
    3. Generate post-mortems for wrong/partial theses
    4. Return summary report

    Args:
        engine: SQLAlchemy engine.

    Returns:
        Summary dict with snapshot_id, scoring results, postmortem count.
    """
    _ensure_tables(engine)
    report: dict[str, Any] = {}

    # 1. Snapshot current thesis (try to get from flow_thesis if available)
    snapshot_id = None
    thesis_data = None
    try:
        from analysis.flow_thesis import generate_unified_thesis
        thesis_data = generate_unified_thesis(engine)
        if thesis_data:
            snapshot_id = snapshot_thesis(engine, thesis_data)
            report["snapshot_id"] = snapshot_id
    except ImportError:
        log.debug("flow_thesis not available yet — skipping snapshot")
        report["snapshot_id"] = None
        report["snapshot_note"] = "flow_thesis module not available"
    except Exception as exc:
        log.warning("Thesis snapshot failed: {e}", e=str(exc))
        report["snapshot_id"] = None
        report["snapshot_error"] = str(exc)

    # 1b. Launch deep dive in background (never blocks the cycle)
    if snapshot_id and thesis_data:
        try:
            from intelligence.deep_dive import deep_dive_async
            deep_dive_async(engine, thesis_data, snapshot_id)
            report["deep_dive"] = "launched"
        except Exception as exc:
            log.warning("Deep dive launch failed: {e}", e=str(exc))
            report["deep_dive"] = f"failed: {exc}"

    # 2. Score old theses
    score_results = score_old_theses(engine)
    report["scoring"] = score_results

    # 3. Generate post-mortems for wrong/partial theses that lack one
    postmortems_generated = 0
    with engine.connect() as conn:
        wrong_snaps = conn.execute(text("""
            SELECT ts.id
            FROM thesis_snapshots ts
            LEFT JOIN thesis_postmortems tp ON ts.id = tp.snapshot_id
            WHERE ts.outcome IN ('wrong', 'partial')
              AND tp.id IS NULL
              AND ts.timestamp >= NOW() - INTERVAL '30 days'
            ORDER BY ts.timestamp DESC
            LIMIT 20
        """)).fetchall()

    for row in wrong_snaps:
        try:
            pm = generate_thesis_postmortem(engine, row[0])
            if pm:
                postmortems_generated += 1
        except Exception as exc:
            log.warning(
                "Post-mortem for snapshot {id} failed: {e}",
                id=row[0], e=str(exc),
            )

    report["postmortems_generated"] = postmortems_generated

    # 4. Quick accuracy summary
    try:
        accuracy = get_thesis_accuracy(engine)
        report["accuracy"] = accuracy["overall"]
    except Exception as exc:
        log.warning("Accuracy computation failed: {e}", e=str(exc))
        report["accuracy"] = {"error": str(exc)}

    log.info(
        "Thesis cycle complete: snap={s}, scored={sc}, postmortems={pm}",
        s=snapshot_id, sc=score_results.get("correct", 0) + score_results.get("wrong", 0),
        pm=postmortems_generated,
    )
    return report


# ── Load Post-Mortems ─────────────────────────────────────────────────────

def load_thesis_postmortems(
    engine: Engine, days: int = 90,
) -> list[dict[str, Any]]:
    """Load thesis post-mortems for the API.

    Args:
        engine: SQLAlchemy engine.
        days: Lookback window.

    Returns:
        List of post-mortem dicts with snapshot context.
    """
    _ensure_tables(engine)
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    with engine.connect() as conn:
        rows = conn.execute(text("""
            SELECT tp.id, tp.snapshot_id, tp.thesis_direction,
                   tp.actual_direction, tp.models_right, tp.models_wrong,
                   tp.what_we_missed, tp.root_cause, tp.lesson,
                   tp.generated_at,
                   ts.timestamp as thesis_ts, ts.conviction,
                   ts.narrative, ts.actual_market_move
            FROM thesis_postmortems tp
            JOIN thesis_snapshots ts ON tp.snapshot_id = ts.id
            WHERE tp.generated_at >= :cutoff
            ORDER BY tp.generated_at DESC
        """), {"cutoff": cutoff}).fetchall()

    results = []
    for r in rows:
        results.append({
            "id": r[0],
            "snapshot_id": r[1],
            "thesis_direction": r[2],
            "actual_direction": r[3],
            "models_right": _parse_json(r[4], []),
            "models_wrong": _parse_json(r[5], []),
            "what_we_missed": r[6],
            "root_cause": r[7],
            "lesson": r[8],
            "generated_at": r[9].isoformat() if r[9] else "",
            "thesis_timestamp": r[10].isoformat() if r[10] else "",
            "conviction": float(r[11]) if r[11] is not None else 0.0,
            "narrative": r[12] or "",
            "actual_market_move": float(r[13]) if r[13] is not None else None,
        })

    return results


# ── Helpers ───────────────────────────────────────────────────────────────

def _parse_json(val, default=None):
    """Safely parse a JSON value that may already be a Python object."""
    if val is None:
        return default if default is not None else {}
    if isinstance(val, (dict, list)):
        return val
    try:
        return json.loads(val)
    except (json.JSONDecodeError, TypeError):
        return default if default is not None else {}


def _normalise_direction(direction: str) -> str:
    """Normalise various direction labels to bullish/bearish/neutral."""
    d = direction.lower().strip()
    if d in ("bullish", "call", "long", "up", "buy"):
        return "bullish"
    if d in ("bearish", "put", "short", "down", "sell"):
        return "bearish"
    return "neutral"


def _classify_root_cause(
    *,
    direction: str,
    actual_direction: str,
    actual_move: float,
    models_right: list[str],
    models_wrong: list[str],
    model_states: dict,
) -> str:
    """Rule-based root cause classification for a wrong thesis."""
    # If many models disagreed with the thesis direction → model_disagreement_ignored
    if len(models_right) >= 2 and len(models_right) > len(models_wrong):
        return "model_disagreement_ignored"

    # If the actual move was very large (>3%), likely external shock
    if abs(actual_move) > 3.0:
        return "external_shock"

    # If direction was right but magnitude was small → correct_but_early
    if direction == actual_direction:
        return "correct_but_early"

    # If no models predicted the actual direction → possibly bad data
    if not models_right:
        return "bad_data"

    # Default: thesis was stale
    return "thesis_outdated"


def _store_thesis_postmortem(engine: Engine, pm: ThesisPostMortem) -> None:
    """Persist a thesis post-mortem to the database."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO thesis_postmortems
                (snapshot_id, thesis_direction, actual_direction,
                 models_right, models_wrong, what_we_missed,
                 root_cause, lesson, generated_at)
            VALUES
                (:snapshot_id, :thesis_dir, :actual_dir,
                 :models_right, :models_wrong, :what_missed,
                 :root_cause, :lesson, :generated_at)
        """), {
            "snapshot_id": pm.snapshot_id,
            "thesis_dir": pm.thesis_direction,
            "actual_dir": pm.actual_direction,
            "models_right": json.dumps(pm.models_that_were_right),
            "models_wrong": json.dumps(pm.models_that_were_wrong),
            "what_missed": pm.what_we_missed,
            "root_cause": pm.root_cause,
            "lesson": pm.lesson,
            "generated_at": pm.generated_at,
        })


# ── LLM Integration ──────────────────────────────────────────────────────

def _get_llm_thesis_postmortem(
    *,
    direction: str,
    actual_direction: str,
    actual_move: float,
    key_drivers: list[str],
    risk_factors: list[str],
    model_states: dict,
    narrative: str,
    models_right: list[str],
    models_wrong: list[str],
    root_cause: str,
) -> dict[str, Any]:
    """Ask the LLM for a thesis post-mortem narrative.

    Returns a dict with keys: what_we_missed, lesson.
    Falls back to rule-based defaults if LLM is unavailable.
    """
    defaults = {
        "what_we_missed": (
            f"Thesis was {direction} but market moved {actual_direction} "
            f"({actual_move:+.2f}%). "
            f"Models that called it correctly: {', '.join(models_right) or 'none'}."
        ),
        "lesson": (
            f"Root cause: {root_cause}. "
            f"Consider re-weighting models: {', '.join(models_right)} "
            f"showed better signal."
            if models_right
            else f"Root cause: {root_cause}. Review data inputs and thesis freshness."
        ),
    }

    try:
        from llm.router import get_llm, Tier
        llm = get_llm(Tier.REASON)
        if not llm.is_available:
            return defaults
    except Exception:
        return defaults

    # Build model state summary
    model_summary = []
    for name, state in model_states.items():
        if isinstance(state, dict):
            model_summary.append(
                f"  {name}: direction={state.get('direction', '?')}, "
                f"confidence={state.get('confidence', '?')}"
            )
    model_text = "\n".join(model_summary) if model_summary else "  (no model details)"

    drivers_text = ", ".join(key_drivers) if key_drivers else "none specified"
    risks_text = ", ".join(risk_factors) if risk_factors else "none specified"

    # RAG: retrieve historical thesis outcomes and similar market conditions
    rag_context = ""
    try:
        from intelligence.rag import get_rag_context
        from db import get_engine as _get_engine
        rag_query = (
            f"thesis {direction} postmortem {root_cause} "
            f"{' '.join(models_right[:2])} {' '.join(key_drivers[:2])}"
        )
        rag_context = get_rag_context(_get_engine(), rag_query, top_k=5, max_chars=2000)
    except Exception:
        pass

    prompt = (
        f"You are a quantitative trading analyst reviewing a thesis post-mortem.\n\n"
        f"THESIS DIRECTION: {direction}\n"
        f"ACTUAL DIRECTION: {actual_direction} ({actual_move:+.2f}% SPY)\n"
        f"ROOT CAUSE: {root_cause}\n\n"
    )
    if rag_context:
        prompt += f"{rag_context}\n"
    prompt += (
        f"NARRATIVE AT TIME:\n{narrative[:800]}\n\n"
        f"KEY DRIVERS: {drivers_text}\n"
        f"RISK FACTORS: {risks_text}\n\n"
        f"MODEL STATES:\n{model_text}\n\n"
        f"MODELS THAT WERE RIGHT: {', '.join(models_right) or 'none'}\n"
        f"MODELS THAT WERE WRONG: {', '.join(models_wrong) or 'none'}\n\n"
        f"Provide:\n"
        f"1. What we missed — reference similar past thesis failures if available (2-3 sentences)\n"
        f"2. Actionable lesson (1-2 sentences)\n\n"
        f"Format as:\n"
        f"MISSED: ...\n"
        f"LESSON: ..."
    )

    try:
        response = llm.generate(
            prompt=prompt,
            system="You are a quantitative trading analyst. Be specific, data-driven, concise.",
            temperature=0.3,
        )

        if not response:
            return defaults

        result = dict(defaults)
        for line in response.strip().split("\n"):
            line = line.strip()
            if line.upper().startswith("MISSED:"):
                result["what_we_missed"] = line[7:].strip()
            elif line.upper().startswith("LESSON:"):
                result["lesson"] = line[7:].strip()

        return result

    except Exception as exc:
        log.debug("LLM thesis post-mortem failed: {e}", e=str(exc))
        return defaults
