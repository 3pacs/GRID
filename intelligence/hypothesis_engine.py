"""
GRID Intelligence — Hypothesis Discovery Engine.

PhD-level intelligence: the system generates hypotheses nobody has thought of,
tests them against data, and scores the results.  This is not pattern matching
on expected relationships — it scans ALL signal pairs in the database, surfaces
statistically significant temporal patterns, detects anomalies across every
dimension, and auto-generates testable hypotheses with clear invalidation
criteria.

Pipeline:
  1. TemporalPatternDetector  — scan every signal-pair for lead-lag relationships
  2. AnomalyHunter            — volume spikes, actor behaviour shifts, convergence
  3. HypothesisGenerator      — create testable hypotheses from patterns + anomalies
  4. score_hypothesis          — test hypotheses against incoming data
  5. auto_discover             — full pipeline: detect → hunt → generate → store

Storage: discovered_hypotheses table (see ensure_tables).

CLI:
  python intelligence/hypothesis_engine.py [discover | scan-patterns | scan-anomalies | score-all | stats]
"""

from __future__ import annotations

import hashlib
import json
import sys
import uuid
from dataclasses import dataclass, asdict, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

import numpy as np
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine


# ── Constants ────────────────────────────────────────────────────────────────

MIN_OBSERVATIONS = 5          # Minimum co-occurrences to consider a pattern real
SIGNIFICANCE_THRESHOLD = 0.05 # p-value cutoff
ANOMALY_SIGMA = 3.0           # Standard deviations for anomaly detection
CONVERGENCE_MIN_SOURCES = 3   # Minimum independent sources for convergence
CONFIDENCE_PRIOR = 0.5        # Bayesian prior for new hypotheses
SCORING_WINDOW_DAYS = 90      # How long to wait before scoring a hypothesis


# ── Data Classes ─────────────────────────────────────────────────────────────

@dataclass
class DiscoveredPattern:
    signal_a: str
    signal_b: str
    lag_days: int
    correlation: float
    p_value: float
    n_observations: int
    direction: str  # "positive" or "negative"

    @property
    def is_significant(self) -> bool:
        return (
            self.p_value < SIGNIFICANCE_THRESHOLD
            and self.n_observations >= MIN_OBSERVATIONS
        )


@dataclass
class Anomaly:
    anomaly_type: str       # "volume_spike", "actor_shift", "convergence"
    entity: str             # ticker, actor name, or category
    description: str
    magnitude: float        # how many sigma above normal, or convergence count
    signal_date: date
    details: dict = field(default_factory=dict)


@dataclass
class Hypothesis:
    id: str
    thesis: str
    pattern_type: str       # "lead_lag", "anomaly_convergence", "actor_shift"
    evidence: list[dict]
    test_criteria: dict     # what to watch for to validate
    invalidation: str       # what would disprove it
    confidence: float
    status: str = "active"  # active, confirmed, invalidated, expired


# ── Schema ───────────────────────────────────────────────────────────────────

_SCHEMA_SQL = text("""
    CREATE TABLE IF NOT EXISTS discovered_hypotheses (
        id               TEXT PRIMARY KEY,
        thesis           TEXT NOT NULL,
        pattern_type     TEXT,
        evidence         JSONB,
        test_criteria    JSONB,
        invalidation     TEXT,
        confidence       DOUBLE PRECISION DEFAULT 0.5,
        status           TEXT DEFAULT 'active',
        times_tested     INTEGER DEFAULT 0,
        times_correct    INTEGER DEFAULT 0,
        created_at       TIMESTAMPTZ DEFAULT NOW(),
        last_tested      TIMESTAMPTZ
    )
""")

_IDX_STATUS = text("""
    CREATE INDEX IF NOT EXISTS idx_discovered_hypotheses_status
        ON discovered_hypotheses (status)
""")

_IDX_CONFIDENCE = text("""
    CREATE INDEX IF NOT EXISTS idx_discovered_hypotheses_confidence
        ON discovered_hypotheses (confidence DESC)
""")

_IDX_CREATED = text("""
    CREATE INDEX IF NOT EXISTS idx_discovered_hypotheses_created
        ON discovered_hypotheses (created_at DESC)
""")


def ensure_tables(engine: Engine) -> None:
    """Create the discovered_hypotheses table if it does not exist."""
    with engine.begin() as conn:
        conn.execute(_SCHEMA_SQL)
        conn.execute(_IDX_STATUS)
        conn.execute(_IDX_CONFIDENCE)
        conn.execute(_IDX_CREATED)
    log.info("hypothesis_engine: tables ensured")


# ── Temporal Pattern Detector ────────────────────────────────────────────────

class TemporalPatternDetector:
    """Discovers temporal patterns across data sources.

    Core question: 'Every time X happens, Y follows within N days.'
    Uses cross-correlation at varying lags to find lead-lag relationships
    between signal categories in analytical_snapshots and signal_data.
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    # ── public ───────────────────────────────────────────────────────────

    def scan_lead_lag(
        self,
        signal_a: str,
        signal_b: str,
        max_lag_days: int = 30,
    ) -> dict:
        """Test if *signal_a* leads *signal_b*.

        Queries both signal types from analytical_snapshots and signal_data,
        aligns them on date, and computes cross-correlation at each lag
        from 1 to *max_lag_days*.

        Returns dict with: best_lag, correlation, p_value, n_observations,
        direction, all_lags (list of {lag, corr, p}).
        """
        ts_a = self._load_daily_counts(signal_a)
        ts_b = self._load_daily_counts(signal_b)

        if len(ts_a) < MIN_OBSERVATIONS or len(ts_b) < MIN_OBSERVATIONS:
            return {
                "signal_a": signal_a,
                "signal_b": signal_b,
                "best_lag": None,
                "correlation": 0.0,
                "p_value": 1.0,
                "n_observations": 0,
                "direction": "none",
                "all_lags": [],
            }

        # Align to common date range
        all_dates = sorted(set(ts_a.keys()) | set(ts_b.keys()))
        arr_a = np.array([ts_a.get(d, 0.0) for d in all_dates], dtype=float)
        arr_b = np.array([ts_b.get(d, 0.0) for d in all_dates], dtype=float)

        # Normalise
        arr_a = self._zscore(arr_a)
        arr_b = self._zscore(arr_b)

        best_lag = 0
        best_corr = 0.0
        best_p = 1.0
        lag_results = []

        for lag in range(1, max_lag_days + 1):
            if lag >= len(arr_a):
                break
            a_shifted = arr_a[: len(arr_a) - lag]
            b_lagged = arr_b[lag:]
            n = len(a_shifted)
            if n < MIN_OBSERVATIONS:
                continue

            corr = float(np.corrcoef(a_shifted, b_lagged)[0, 1])
            if np.isnan(corr):
                corr = 0.0

            # Two-tailed t-test for correlation significance
            p_value = self._correlation_p_value(corr, n)

            lag_results.append({"lag": lag, "corr": round(corr, 4), "p": round(p_value, 6)})

            if abs(corr) > abs(best_corr):
                best_corr = corr
                best_lag = lag
                best_p = p_value

        return {
            "signal_a": signal_a,
            "signal_b": signal_b,
            "best_lag": best_lag,
            "correlation": round(best_corr, 4),
            "p_value": round(best_p, 6),
            "n_observations": len(all_dates),
            "direction": "positive" if best_corr > 0 else "negative",
            "all_lags": lag_results,
        }

    def discover_patterns(self, min_occurrences: int = MIN_OBSERVATIONS) -> list[dict]:
        """Scan ALL signal pairs for temporal lead-lag patterns.

        Fetches every distinct category from analytical_snapshots and
        signal_type from signal_data, then tests every ordered pair.
        Filters by statistical significance and occurrence count.

        Returns discovered patterns sorted by absolute correlation strength.
        """
        categories = self._get_all_signal_types()
        log.info(
            "hypothesis_engine: scanning {} signal types for lead-lag patterns",
            len(categories),
        )

        patterns: list[dict] = []
        tested = 0

        for i, cat_a in enumerate(categories):
            for cat_b in categories[i + 1 :]:
                # Test A → B
                result_ab = self.scan_lead_lag(cat_a, cat_b)
                tested += 1
                if (
                    result_ab["p_value"] < SIGNIFICANCE_THRESHOLD
                    and result_ab["n_observations"] >= min_occurrences
                    and result_ab["best_lag"] is not None
                ):
                    result_ab["pattern"] = f"{cat_a} → {cat_b}"
                    patterns.append(result_ab)

                # Test B → A
                result_ba = self.scan_lead_lag(cat_b, cat_a)
                tested += 1
                if (
                    result_ba["p_value"] < SIGNIFICANCE_THRESHOLD
                    and result_ba["n_observations"] >= min_occurrences
                    and result_ba["best_lag"] is not None
                ):
                    result_ba["pattern"] = f"{cat_b} → {cat_a}"
                    patterns.append(result_ba)

        # Sort by absolute correlation, strongest first
        patterns.sort(key=lambda p: abs(p["correlation"]), reverse=True)
        log.info(
            "hypothesis_engine: tested {} pairs, found {} significant patterns",
            tested,
            len(patterns),
        )
        return patterns

    # ── private ──────────────────────────────────────────────────────────

    def _get_all_signal_types(self) -> list[str]:
        """Collect distinct signal types from both snapshots and signal_data."""
        q_snap = text("""
            SELECT DISTINCT category FROM analytical_snapshots
            ORDER BY category
        """)
        q_sig = text("""
            SELECT DISTINCT signal_type FROM signal_data
            ORDER BY signal_type
        """)
        types: set[str] = set()
        with self.engine.connect() as conn:
            for row in conn.execute(q_snap):
                types.add(f"snap:{row[0]}")
            for row in conn.execute(q_sig):
                types.add(f"sig:{row[0]}")
        return sorted(types)

    def _load_daily_counts(self, signal_key: str) -> dict[date, float]:
        """Load daily event counts or metric values for a signal type.

        signal_key format: 'snap:<category>' or 'sig:<signal_type>'
        Returns {date: count_or_magnitude}.
        """
        prefix, name = signal_key.split(":", 1)

        if prefix == "snap":
            q = text("""
                SELECT snapshot_date, COUNT(*) AS cnt
                FROM analytical_snapshots
                WHERE category = :cat
                GROUP BY snapshot_date
                ORDER BY snapshot_date
            """)
            with self.engine.connect() as conn:
                rows = conn.execute(q, {"cat": name}).fetchall()
            return {row[0]: float(row[1]) for row in rows}

        elif prefix == "sig":
            # Use magnitude if available, otherwise count
            q = text("""
                SELECT signal_date,
                       COALESCE(SUM(magnitude), COUNT(*)) AS val
                FROM signal_data
                WHERE signal_type = :stype
                GROUP BY signal_date
                ORDER BY signal_date
            """)
            with self.engine.connect() as conn:
                rows = conn.execute(q, {"stype": name}).fetchall()
            return {row[0]: float(row[1]) for row in rows}

        return {}

    @staticmethod
    def _zscore(arr: np.ndarray) -> np.ndarray:
        """Z-score normalise, handling zero-variance gracefully."""
        std = arr.std()
        if std == 0 or np.isnan(std):
            return np.zeros_like(arr)
        return (arr - arr.mean()) / std

    @staticmethod
    def _correlation_p_value(r: float, n: int) -> float:
        """Two-tailed p-value for Pearson correlation using t-distribution.

        Falls back to a normal approximation for large n.
        """
        if n < 3 or abs(r) >= 1.0:
            return 0.0 if abs(r) >= 1.0 else 1.0

        t_stat = r * np.sqrt((n - 2) / (1 - r * r))

        # Use scipy if available, otherwise a conservative normal approx
        try:
            from scipy.stats import t as t_dist
            p = float(2 * t_dist.sf(abs(t_stat), df=n - 2))
        except ImportError:
            # Normal approximation (conservative for small n)
            from math import erfc, sqrt
            p = erfc(abs(t_stat) / sqrt(2))
        return p


# ── Anomaly Hunter ───────────────────────────────────────────────────────────

class AnomalyHunter:
    """Finds statistical anomalies across all data.

    Three scan modes:
      1. volume_anomalies  — categories with unusual activity spikes
      2. actor_anomalies   — actors behaving differently than historical pattern
      3. convergence       — multiple independent signals pointing same direction
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    def scan_volume_anomalies(self, lookback_days: int = 180) -> list[Anomaly]:
        """Find categories with unusual activity spikes.

        For each snapshot category and signal type: compute rolling mean + stddev
        over *lookback_days*.  Flag any day with volume > ANOMALY_SIGMA above mean.
        """
        anomalies: list[Anomaly] = []

        # --- analytical_snapshots ---
        q = text("""
            SELECT category, snapshot_date, COUNT(*) AS cnt
            FROM analytical_snapshots
            WHERE snapshot_date >= (CURRENT_DATE - :lb * INTERVAL '1 day')
            GROUP BY category, snapshot_date
            ORDER BY category, snapshot_date
        """)
        with self.engine.connect() as conn:
            rows = conn.execute(q, {"lb": lookback_days}).fetchall()

        anomalies.extend(self._detect_spikes(rows, source="analytical_snapshots"))

        # --- signal_data ---
        q2 = text("""
            SELECT signal_type, signal_date, COUNT(*) AS cnt
            FROM signal_data
            WHERE signal_date >= (CURRENT_DATE - :lb * INTERVAL '1 day')
            GROUP BY signal_type, signal_date
            ORDER BY signal_type, signal_date
        """)
        with self.engine.connect() as conn:
            rows2 = conn.execute(q2, {"lb": lookback_days}).fetchall()

        anomalies.extend(self._detect_spikes(rows2, source="signal_data"))

        anomalies.sort(key=lambda a: a.magnitude, reverse=True)
        log.info("hypothesis_engine: found {} volume anomalies", len(anomalies))
        return anomalies

    def scan_actor_anomalies(self, lookback_days: int = 180) -> list[Anomaly]:
        """Find actors behaving differently than their historical pattern.

        Detects:
          - Actors who suddenly appear in new signal categories
          - Actors whose activity volume changed dramatically
        """
        anomalies: list[Anomaly] = []

        # --- new-category appearances ---
        q_new_cats = text("""
            WITH actor_history AS (
                SELECT actor, signal_type,
                       MIN(signal_date) AS first_seen,
                       MAX(signal_date) AS last_seen,
                       COUNT(*) AS total
                FROM signal_data
                WHERE actor IS NOT NULL
                GROUP BY actor, signal_type
            )
            SELECT actor, signal_type, first_seen, total
            FROM actor_history
            WHERE first_seen >= (CURRENT_DATE - :recent * INTERVAL '1 day')
              AND actor IN (
                  SELECT actor FROM signal_data
                  WHERE actor IS NOT NULL
                  GROUP BY actor
                  HAVING MIN(signal_date) < (CURRENT_DATE - :recent * INTERVAL '1 day')
              )
            ORDER BY first_seen DESC
        """)
        with self.engine.connect() as conn:
            for row in conn.execute(q_new_cats, {"recent": 30}):
                anomalies.append(Anomaly(
                    anomaly_type="actor_new_category",
                    entity=row[0],
                    description=(
                        f"Actor '{row[0]}' appeared in signal type '{row[1]}' "
                        f"for the first time on {row[2]} (previously unseen in this category)"
                    ),
                    magnitude=2.0,  # inherently interesting
                    signal_date=row[2],
                    details={
                        "actor": row[0],
                        "new_signal_type": row[1],
                        "first_seen": str(row[2]),
                        "total_in_category": row[3],
                    },
                ))

        # --- volume shift per actor ---
        q_vol = text("""
            WITH monthly AS (
                SELECT actor,
                       DATE_TRUNC('month', signal_date) AS month,
                       COUNT(*) AS cnt
                FROM signal_data
                WHERE actor IS NOT NULL
                  AND signal_date >= (CURRENT_DATE - :lb * INTERVAL '1 day')
                GROUP BY actor, DATE_TRUNC('month', signal_date)
            ),
            stats AS (
                SELECT actor,
                       AVG(cnt) AS avg_cnt,
                       STDDEV_POP(cnt) AS std_cnt
                FROM monthly
                GROUP BY actor
                HAVING COUNT(*) >= 3
            )
            SELECT m.actor, m.month, m.cnt, s.avg_cnt, s.std_cnt
            FROM monthly m
            JOIN stats s ON m.actor = s.actor
            WHERE s.std_cnt > 0
              AND (m.cnt - s.avg_cnt) / s.std_cnt > :sigma
            ORDER BY (m.cnt - s.avg_cnt) / s.std_cnt DESC
        """)
        with self.engine.connect() as conn:
            for row in conn.execute(q_vol, {"lb": lookback_days, "sigma": ANOMALY_SIGMA}):
                sigma_val = (row[2] - row[3]) / row[4] if row[4] > 0 else 0
                anomalies.append(Anomaly(
                    anomaly_type="actor_volume_spike",
                    entity=row[0],
                    description=(
                        f"Actor '{row[0]}' had {row[2]} signals in {row[1].strftime('%Y-%m')}, "
                        f"vs average {row[3]:.1f} ({sigma_val:.1f} sigma)"
                    ),
                    magnitude=round(sigma_val, 2),
                    signal_date=row[1].date() if hasattr(row[1], "date") else row[1],
                    details={
                        "actor": row[0],
                        "month": str(row[1]),
                        "count": row[2],
                        "avg": round(float(row[3]), 2),
                        "std": round(float(row[4]), 2),
                    },
                ))

        anomalies.sort(key=lambda a: a.magnitude, reverse=True)
        log.info("hypothesis_engine: found {} actor anomalies", len(anomalies))
        return anomalies

    def scan_convergence(self, window_days: int = 7) -> list[Anomaly]:
        """Find multiple independent signals pointing in the same direction.

        Groups signals by ticker/entity within a rolling window.
        When CONVERGENCE_MIN_SOURCES or more independent signal types agree
        on direction, that is a convergence event.
        """
        anomalies: list[Anomaly] = []

        q = text("""
            SELECT ticker,
                   direction,
                   COUNT(DISTINCT signal_type) AS n_sources,
                   ARRAY_AGG(DISTINCT signal_type) AS sources,
                   MAX(signal_date) AS latest_date,
                   AVG(magnitude) AS avg_magnitude
            FROM signal_data
            WHERE ticker IS NOT NULL
              AND direction IS NOT NULL
              AND signal_date >= (CURRENT_DATE - :window * INTERVAL '1 day')
            GROUP BY ticker, direction
            HAVING COUNT(DISTINCT signal_type) >= :min_src
            ORDER BY COUNT(DISTINCT signal_type) DESC
        """)

        with self.engine.connect() as conn:
            for row in conn.execute(
                q, {"window": window_days, "min_src": CONVERGENCE_MIN_SOURCES}
            ):
                ticker, direction, n_src, sources, latest, avg_mag = row
                anomalies.append(Anomaly(
                    anomaly_type="convergence",
                    entity=ticker,
                    description=(
                        f"{n_src} independent sources agree: {ticker} → {direction} "
                        f"(sources: {', '.join(sources)})"
                    ),
                    magnitude=float(n_src),
                    signal_date=latest,
                    details={
                        "ticker": ticker,
                        "direction": direction,
                        "n_sources": int(n_src),
                        "sources": list(sources),
                        "avg_magnitude": round(float(avg_mag), 4) if avg_mag else None,
                    },
                ))

        # Also check oracle prediction convergence
        q_oracle = text("""
            SELECT ticker, direction,
                   COUNT(DISTINCT model_name) AS n_models,
                   ARRAY_AGG(DISTINCT model_name) AS models,
                   AVG(confidence) AS avg_conf,
                   MAX(created_at) AS latest
            FROM oracle_predictions
            WHERE created_at >= (NOW() - :window * INTERVAL '1 day')
              AND verdict = 'pending'
            GROUP BY ticker, direction
            HAVING COUNT(DISTINCT model_name) >= :min_src
            ORDER BY COUNT(DISTINCT model_name) DESC
        """)
        with self.engine.connect() as conn:
            for row in conn.execute(
                q_oracle, {"window": window_days, "min_src": CONVERGENCE_MIN_SOURCES}
            ):
                ticker, direction, n_models, models, avg_conf, latest = row
                anomalies.append(Anomaly(
                    anomaly_type="oracle_convergence",
                    entity=ticker,
                    description=(
                        f"{n_models} prediction models agree: {ticker} → {direction} "
                        f"(avg confidence: {avg_conf:.2f}, models: {', '.join(models)})"
                    ),
                    magnitude=float(n_models),
                    signal_date=latest.date() if hasattr(latest, "date") else latest,
                    details={
                        "ticker": ticker,
                        "direction": direction,
                        "n_models": int(n_models),
                        "models": list(models),
                        "avg_confidence": round(float(avg_conf), 4),
                    },
                ))

        anomalies.sort(key=lambda a: a.magnitude, reverse=True)
        log.info("hypothesis_engine: found {} convergence events", len(anomalies))
        return anomalies

    # ── private ──────────────────────────────────────────────────────────

    @staticmethod
    def _detect_spikes(
        rows: list, source: str
    ) -> list[Anomaly]:
        """Given (category, date, count) rows, detect sigma spikes per category."""
        from collections import defaultdict

        by_cat: dict[str, list[tuple[date, float]]] = defaultdict(list)
        for row in rows:
            by_cat[row[0]].append((row[1], float(row[2])))

        anomalies: list[Anomaly] = []
        for cat, series in by_cat.items():
            if len(series) < 5:
                continue
            vals = np.array([v for _, v in series])
            mean = vals.mean()
            std = vals.std()
            if std == 0:
                continue

            for dt, val in series:
                sigma = (val - mean) / std
                if sigma > ANOMALY_SIGMA:
                    anomalies.append(Anomaly(
                        anomaly_type="volume_spike",
                        entity=cat,
                        description=(
                            f"{source} category '{cat}' had {val:.0f} events on {dt}, "
                            f"{sigma:.1f} sigma above mean ({mean:.1f})"
                        ),
                        magnitude=round(sigma, 2),
                        signal_date=dt,
                        details={
                            "category": cat,
                            "source": source,
                            "value": val,
                            "mean": round(float(mean), 2),
                            "std": round(float(std), 2),
                            "sigma": round(sigma, 2),
                        },
                    ))
        return anomalies


# ── Hypothesis Generator ─────────────────────────────────────────────────────

class HypothesisGenerator:
    """Generates testable hypotheses from discovered patterns and anomalies.

    Each hypothesis follows the GRID causation standard:
      - LEVER:        who did what affecting which valve
      - CONDITION:    environmental factor amplifying the lever
      - THESIS:       expected direction, magnitude, timeframe
      - INVALIDATION: what would disprove it
    """

    def __init__(self, engine: Engine):
        self.engine = engine
        self.detector = TemporalPatternDetector(engine)
        self.hunter = AnomalyHunter(engine)

    def generate(
        self,
        patterns: list[dict] | None = None,
        anomalies: list[Anomaly] | None = None,
    ) -> list[Hypothesis]:
        """Create hypotheses from patterns and anomalies.

        If *patterns* or *anomalies* are None, runs the respective scanners.
        """
        if patterns is None:
            patterns = self.detector.discover_patterns()
        if anomalies is None:
            vol_anom = self.hunter.scan_volume_anomalies()
            actor_anom = self.hunter.scan_actor_anomalies()
            convergence = self.hunter.scan_convergence()
            anomalies = vol_anom + actor_anom + convergence

        hypotheses: list[Hypothesis] = []

        # --- From lead-lag patterns ---
        for pat in patterns:
            hyp = self._hypothesis_from_pattern(pat)
            if hyp:
                hypotheses.append(hyp)

        # --- From convergence anomalies ---
        for anom in anomalies:
            if anom.anomaly_type in ("convergence", "oracle_convergence"):
                hyp = self._hypothesis_from_convergence(anom)
                if hyp:
                    hypotheses.append(hyp)

        # --- From volume spikes ---
        for anom in anomalies:
            if anom.anomaly_type == "volume_spike":
                hyp = self._hypothesis_from_volume_spike(anom)
                if hyp:
                    hypotheses.append(hyp)

        # --- From actor shifts ---
        for anom in anomalies:
            if anom.anomaly_type in ("actor_new_category", "actor_volume_spike"):
                hyp = self._hypothesis_from_actor_shift(anom)
                if hyp:
                    hypotheses.append(hyp)

        log.info("hypothesis_engine: generated {} hypotheses", len(hypotheses))
        return hypotheses

    def score_hypothesis(self, hypothesis_id: str) -> dict:
        """Score a hypothesis against new data since it was created.

        Loads the hypothesis from DB, checks whether the predicted outcome
        occurred, and updates confidence using Bayesian updating.
        Returns the updated hypothesis record.
        """
        q = text("""
            SELECT id, thesis, pattern_type, evidence, test_criteria,
                   invalidation, confidence, status, times_tested,
                   times_correct, created_at
            FROM discovered_hypotheses
            WHERE id = :hid
        """)
        with self.engine.connect() as conn:
            row = conn.execute(q, {"hid": hypothesis_id}).fetchone()

        if row is None:
            return {"error": f"hypothesis {hypothesis_id} not found"}

        h_id, thesis, ptype, evidence, criteria, inv, conf, status, tested, correct, created = row
        if status != "active":
            return {"id": h_id, "status": status, "message": "not active, skipping"}

        criteria = criteria if isinstance(criteria, dict) else json.loads(criteria or "{}")
        outcome = self._evaluate_criteria(criteria, created)

        # Bayesian update
        tested += 1
        if outcome == "confirmed":
            correct += 1
        new_conf = (correct + 1) / (tested + 2)  # Beta-distribution posterior

        new_status = status
        if outcome == "confirmed" and new_conf > 0.75:
            new_status = "confirmed"
        elif outcome == "invalidated":
            new_conf = max(new_conf * 0.5, 0.01)  # Harsh penalty
            if new_conf < 0.1:
                new_status = "invalidated"

        update = text("""
            UPDATE discovered_hypotheses
            SET confidence = :conf,
                status = :status,
                times_tested = :tested,
                times_correct = :correct,
                last_tested = NOW()
            WHERE id = :hid
        """)
        with self.engine.begin() as conn:
            conn.execute(update, {
                "conf": round(new_conf, 4),
                "status": new_status,
                "tested": tested,
                "correct": correct,
                "hid": h_id,
            })

        return {
            "id": h_id,
            "thesis": thesis,
            "outcome": outcome,
            "confidence": round(new_conf, 4),
            "status": new_status,
            "times_tested": tested,
            "times_correct": correct,
        }

    def score_all(self) -> list[dict]:
        """Score all active hypotheses older than a reasonable test window."""
        q = text("""
            SELECT id FROM discovered_hypotheses
            WHERE status = 'active'
              AND created_at < NOW() - INTERVAL '7 days'
            ORDER BY created_at
        """)
        with self.engine.connect() as conn:
            ids = [row[0] for row in conn.execute(q)]

        results = []
        for hid in ids:
            result = self.score_hypothesis(hid)
            results.append(result)

        log.info(
            "hypothesis_engine: scored {} hypotheses",
            len(results),
        )
        return results

    def auto_discover(self) -> list[dict]:
        """Full pipeline: detect patterns, find anomalies, generate hypotheses, store.

        Returns list of generated hypothesis dicts.
        """
        ensure_tables(self.engine)

        log.info("hypothesis_engine: starting auto-discovery pipeline")

        # Phase 1: temporal patterns
        patterns = self.detector.discover_patterns()

        # Phase 2: anomalies
        vol_anomalies = self.hunter.scan_volume_anomalies()
        actor_anomalies = self.hunter.scan_actor_anomalies()
        convergence = self.hunter.scan_convergence()
        all_anomalies = vol_anomalies + actor_anomalies + convergence

        # Phase 3: generate
        hypotheses = self.generate(patterns=patterns, anomalies=all_anomalies)

        # Phase 4: store (deduplicate by thesis hash)
        stored = 0
        for hyp in hypotheses:
            if self._store_hypothesis(hyp):
                stored += 1

        # Phase 5: score existing
        scored = self.score_all()

        log.info(
            "hypothesis_engine: discovery complete — "
            "{} patterns, {} anomalies, {} new hypotheses stored, {} scored",
            len(patterns),
            len(all_anomalies),
            stored,
            len(scored),
        )

        return [asdict(h) for h in hypotheses]

    # ── private: hypothesis builders ─────────────────────────────────────

    def _hypothesis_from_pattern(self, pat: dict) -> Hypothesis | None:
        """Build a hypothesis from a lead-lag pattern."""
        sig_a = pat.get("signal_a", "")
        sig_b = pat.get("signal_b", "")
        lag = pat.get("best_lag", 0)
        corr = pat.get("correlation", 0)
        direction = "increases" if corr > 0 else "decreases"

        thesis = (
            f"When {sig_a} activity spikes, {sig_b} activity {direction} "
            f"within {lag} days (r={corr:.3f}, p={pat.get('p_value', 1):.4f})"
        )

        return Hypothesis(
            id=self._make_id(thesis),
            thesis=thesis,
            pattern_type="lead_lag",
            evidence=[{
                "signal_a": sig_a,
                "signal_b": sig_b,
                "lag_days": lag,
                "correlation": corr,
                "p_value": pat.get("p_value"),
                "n_observations": pat.get("n_observations"),
            }],
            test_criteria={
                "watch_signal": sig_a,
                "expect_signal": sig_b,
                "lag_days": lag,
                "expected_direction": direction,
            },
            invalidation=(
                f"If {sig_a} spikes 3+ times and {sig_b} does NOT {direction} "
                f"within {lag} days on any occasion, the pattern is broken"
            ),
            confidence=min(0.3 + abs(corr) * 0.4, 0.8),  # Corr-scaled prior
        )

    def _hypothesis_from_convergence(self, anom: Anomaly) -> Hypothesis | None:
        """Build a hypothesis from a convergence anomaly."""
        d = anom.details
        ticker = d.get("ticker", anom.entity)
        direction = d.get("direction", "unknown")
        n_src = d.get("n_sources") or d.get("n_models", 0)
        sources = d.get("sources") or d.get("models", [])

        thesis = (
            f"CONVERGENCE: {n_src} independent sources agree {ticker} is heading "
            f"{direction}. Sources: {', '.join(sources[:5])}. "
            f"Multi-source agreement historically precedes significant moves."
        )

        return Hypothesis(
            id=self._make_id(thesis),
            thesis=thesis,
            pattern_type="convergence",
            evidence=[{
                "type": anom.anomaly_type,
                "ticker": ticker,
                "direction": direction,
                "n_sources": n_src,
                "sources": sources,
                "date": str(anom.signal_date),
            }],
            test_criteria={
                "ticker": ticker,
                "expected_direction": direction,
                "window_days": 14,
                "min_move_pct": 2.0,
            },
            invalidation=(
                f"If {ticker} moves opposite to {direction} by >2% within 14 days, "
                f"the convergence signal failed"
            ),
            confidence=min(0.4 + n_src * 0.08, 0.85),
        )

    def _hypothesis_from_volume_spike(self, anom: Anomaly) -> Hypothesis | None:
        """Build a hypothesis from a volume spike anomaly."""
        d = anom.details
        cat = d.get("category", anom.entity)
        sigma = d.get("sigma", anom.magnitude)

        if sigma < ANOMALY_SIGMA:
            return None

        thesis = (
            f"VOLUME ANOMALY: {cat} activity spiked to {sigma:.1f} sigma above "
            f"historical mean on {anom.signal_date}. Unusual {cat} activity "
            f"may precede a significant move in related instruments."
        )

        return Hypothesis(
            id=self._make_id(thesis),
            thesis=thesis,
            pattern_type="volume_anomaly",
            evidence=[{
                "category": cat,
                "date": str(anom.signal_date),
                "sigma": sigma,
                "value": d.get("value"),
                "mean": d.get("mean"),
            }],
            test_criteria={
                "watch_category": cat,
                "window_days": 30,
                "look_for": "price_move_or_follow_on_activity",
            },
            invalidation=(
                f"If the {cat} spike was noise (no follow-on activity or price "
                f"impact within 30 days), downgrade"
            ),
            confidence=min(0.2 + sigma * 0.05, 0.6),
        )

    def _hypothesis_from_actor_shift(self, anom: Anomaly) -> Hypothesis | None:
        """Build a hypothesis from an actor behaviour change."""
        d = anom.details
        actor = d.get("actor", anom.entity)

        if anom.anomaly_type == "actor_new_category":
            new_type = d.get("new_signal_type", "unknown")
            thesis = (
                f"ACTOR SHIFT: '{actor}' appeared in signal category '{new_type}' "
                f"for the first time. When established actors enter new domains, "
                f"it often signals strategic repositioning ahead of a catalyst."
            )
            invalidation = (
                f"If '{actor}' does not persist in '{new_type}' (no further "
                f"signals within 60 days), this was noise"
            )
        else:
            thesis = (
                f"ACTOR VOLUME SHIFT: '{actor}' dramatically increased activity "
                f"({anom.magnitude:.1f} sigma above normal). Sudden changes in "
                f"actor behaviour may indicate awareness of upcoming events."
            )
            invalidation = (
                f"If '{actor}' activity normalises without any related market "
                f"event within 45 days, this was noise"
            )

        return Hypothesis(
            id=self._make_id(thesis),
            thesis=thesis,
            pattern_type="actor_shift",
            evidence=[{
                "actor": actor,
                "anomaly_type": anom.anomaly_type,
                "date": str(anom.signal_date),
                "magnitude": anom.magnitude,
                "details": d,
            }],
            test_criteria={
                "watch_actor": actor,
                "window_days": 45,
                "look_for": "follow_on_activity_or_market_move",
            },
            invalidation=invalidation,
            confidence=min(0.2 + anom.magnitude * 0.05, 0.55),
        )

    # ── private: scoring evaluation ──────────────────────────────────────

    def _evaluate_criteria(self, criteria: dict, created_at: datetime) -> str:
        """Evaluate test criteria against actual data.

        Returns 'confirmed', 'invalidated', or 'inconclusive'.
        """
        # --- convergence / ticker-based criteria ---
        ticker = criteria.get("ticker")
        if ticker and criteria.get("expected_direction"):
            return self._check_ticker_move(
                ticker,
                criteria["expected_direction"],
                created_at,
                criteria.get("window_days", 14),
                criteria.get("min_move_pct", 2.0),
            )

        # --- lead-lag pattern criteria ---
        watch = criteria.get("watch_signal")
        expect = criteria.get("expect_signal")
        if watch and expect:
            return self._check_lead_lag_recurrence(
                watch, expect,
                criteria.get("lag_days", 7),
                criteria.get("expected_direction", "increases"),
                created_at,
            )

        # --- actor / category watch ---
        if criteria.get("watch_actor") or criteria.get("watch_category"):
            return self._check_follow_on_activity(criteria, created_at)

        return "inconclusive"

    def _check_ticker_move(
        self,
        ticker: str,
        direction: str,
        since: datetime,
        window_days: int,
        min_move_pct: float,
    ) -> str:
        """Check if a ticker moved in the expected direction since hypothesis creation."""
        q = text("""
            SELECT actual_move_pct, direction AS pred_dir
            FROM oracle_predictions
            WHERE ticker = :ticker
              AND scored_at IS NOT NULL
              AND created_at >= :since
              AND created_at <= :until
            ORDER BY scored_at DESC
            LIMIT 5
        """)
        until = since + timedelta(days=window_days)
        with self.engine.connect() as conn:
            rows = conn.execute(q, {
                "ticker": ticker,
                "since": since,
                "until": until,
            }).fetchall()

        if not rows:
            return "inconclusive"

        for row in rows:
            move = row[0]
            if move is None:
                continue
            if direction in ("bullish", "up") and move > min_move_pct:
                return "confirmed"
            if direction in ("bearish", "down") and move < -min_move_pct:
                return "confirmed"
            if direction in ("bullish", "up") and move < -min_move_pct:
                return "invalidated"
            if direction in ("bearish", "down") and move > min_move_pct:
                return "invalidated"

        return "inconclusive"

    def _check_lead_lag_recurrence(
        self,
        watch_signal: str,
        expect_signal: str,
        lag_days: int,
        direction: str,
        since: datetime,
    ) -> str:
        """Re-check a lead-lag pattern since hypothesis creation."""
        result = self.detector.scan_lead_lag(watch_signal, expect_signal, max_lag_days=lag_days + 5)
        if result["best_lag"] is None:
            return "inconclusive"

        if result["p_value"] < SIGNIFICANCE_THRESHOLD:
            actual_dir = "increases" if result["correlation"] > 0 else "decreases"
            if actual_dir == direction:
                return "confirmed"
            return "invalidated"
        return "inconclusive"

    def _check_follow_on_activity(self, criteria: dict, since: datetime) -> str:
        """Check if follow-on activity occurred for actor/category watches."""
        actor = criteria.get("watch_actor")
        category = criteria.get("watch_category")
        window = criteria.get("window_days", 45)

        if actor:
            q = text("""
                SELECT COUNT(*) FROM signal_data
                WHERE actor = :actor
                  AND signal_date >= :since::date
                  AND signal_date <= (:since::date + :window * INTERVAL '1 day')
            """)
            with self.engine.connect() as conn:
                cnt = conn.execute(q, {
                    "actor": actor, "since": since, "window": window
                }).scalar()
            return "confirmed" if cnt and cnt > 3 else "inconclusive"

        if category:
            q = text("""
                SELECT COUNT(*) FROM analytical_snapshots
                WHERE category = :cat
                  AND snapshot_date >= :since::date
                  AND snapshot_date <= (:since::date + :window * INTERVAL '1 day')
            """)
            with self.engine.connect() as conn:
                cnt = conn.execute(q, {
                    "cat": category, "since": since, "window": window
                }).scalar()
            return "confirmed" if cnt and cnt > 5 else "inconclusive"

        return "inconclusive"

    # ── private: storage ─────────────────────────────────────────────────

    def _store_hypothesis(self, hyp: Hypothesis) -> bool:
        """Upsert a hypothesis into discovered_hypotheses. Returns True if inserted."""
        upsert = text("""
            INSERT INTO discovered_hypotheses
                (id, thesis, pattern_type, evidence, test_criteria,
                 invalidation, confidence, status)
            VALUES
                (:id, :thesis, :ptype, :evidence, :criteria,
                 :inv, :conf, :status)
            ON CONFLICT (id) DO NOTHING
        """)
        with self.engine.begin() as conn:
            result = conn.execute(upsert, {
                "id": hyp.id,
                "thesis": hyp.thesis,
                "ptype": hyp.pattern_type,
                "evidence": json.dumps(hyp.evidence),
                "criteria": json.dumps(hyp.test_criteria),
                "inv": hyp.invalidation,
                "conf": hyp.confidence,
                "status": hyp.status,
            })
        return result.rowcount > 0

    @staticmethod
    def _make_id(thesis: str) -> str:
        """Deterministic ID from thesis text (deduplicates identical hypotheses)."""
        return "hyp_" + hashlib.sha256(thesis.encode()).hexdigest()[:16]


# ── Stats ────────────────────────────────────────────────────────────────────

def get_stats(engine: Engine) -> dict:
    """Return summary statistics for the hypothesis engine."""
    q = text("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE status = 'active') AS active,
            COUNT(*) FILTER (WHERE status = 'confirmed') AS confirmed,
            COUNT(*) FILTER (WHERE status = 'invalidated') AS invalidated,
            COUNT(*) FILTER (WHERE status = 'expired') AS expired,
            AVG(confidence) AS avg_confidence,
            AVG(confidence) FILTER (WHERE status = 'active') AS avg_active_confidence,
            SUM(times_tested) AS total_tests,
            SUM(times_correct) AS total_correct,
            MAX(created_at) AS latest_created,
            MAX(last_tested) AS latest_tested
        FROM discovered_hypotheses
    """)
    with engine.connect() as conn:
        row = conn.execute(q).fetchone()

    if row is None or row[0] == 0:
        return {"total": 0, "message": "no hypotheses yet"}

    total_tests = row[7] or 0
    total_correct = row[8] or 0
    accuracy = total_correct / total_tests if total_tests > 0 else 0

    # Top hypotheses by confidence
    q_top = text("""
        SELECT id, thesis, confidence, times_tested, times_correct, status
        FROM discovered_hypotheses
        WHERE status = 'active'
        ORDER BY confidence DESC
        LIMIT 10
    """)
    with engine.connect() as conn:
        top = [
            {
                "id": r[0],
                "thesis": r[1][:120],
                "confidence": round(float(r[2]), 3),
                "tested": r[3],
                "correct": r[4],
                "status": r[5],
            }
            for r in conn.execute(q_top)
        ]

    # Pattern type breakdown
    q_types = text("""
        SELECT pattern_type, COUNT(*), AVG(confidence)
        FROM discovered_hypotheses
        GROUP BY pattern_type
        ORDER BY COUNT(*) DESC
    """)
    with engine.connect() as conn:
        by_type = {
            r[0]: {"count": r[1], "avg_confidence": round(float(r[2]), 3)}
            for r in conn.execute(q_types)
        }

    return {
        "total": row[0],
        "active": row[1],
        "confirmed": row[2],
        "invalidated": row[3],
        "expired": row[4],
        "avg_confidence": round(float(row[5] or 0), 3),
        "avg_active_confidence": round(float(row[6] or 0), 3),
        "total_tests": total_tests,
        "total_correct": total_correct,
        "accuracy": round(accuracy, 3),
        "latest_created": str(row[9]) if row[9] else None,
        "latest_tested": str(row[10]) if row[10] else None,
        "top_hypotheses": top,
        "by_pattern_type": by_type,
    }


# ── CLI ──────────────────────────────────────────────────────────────────────

def _print_json(obj: Any) -> None:
    """Pretty-print a JSON-serialisable object."""
    def _default(o: Any) -> Any:
        if isinstance(o, (date, datetime)):
            return o.isoformat()
        if isinstance(o, np.floating):
            return float(o)
        if isinstance(o, np.integer):
            return int(o)
        return str(o)

    print(json.dumps(obj, indent=2, default=_default))


def main() -> None:
    """CLI entrypoint."""
    from db import get_engine

    engine = get_engine()
    ensure_tables(engine)

    cmd = sys.argv[1] if len(sys.argv) > 1 else "stats"

    if cmd == "discover":
        gen = HypothesisGenerator(engine)
        results = gen.auto_discover()
        print(f"\n=== Auto-Discovery Complete ===")
        print(f"Generated {len(results)} hypotheses\n")
        for h in results[:10]:
            print(f"  [{h['pattern_type']}] {h['thesis'][:100]}...")
            print(f"    confidence: {h['confidence']:.3f}")
            print()

    elif cmd == "scan-patterns":
        detector = TemporalPatternDetector(engine)
        patterns = detector.discover_patterns()
        print(f"\n=== Temporal Patterns Found: {len(patterns)} ===\n")
        for p in patterns[:20]:
            print(
                f"  {p['pattern']}: lag={p['best_lag']}d, "
                f"r={p['correlation']:.3f}, p={p['p_value']:.4f}, "
                f"n={p['n_observations']}"
            )
        print()

    elif cmd == "scan-anomalies":
        hunter = AnomalyHunter(engine)
        vol = hunter.scan_volume_anomalies()
        act = hunter.scan_actor_anomalies()
        conv = hunter.scan_convergence()
        print(f"\n=== Anomalies Found ===")
        print(f"  Volume spikes:  {len(vol)}")
        print(f"  Actor shifts:   {len(act)}")
        print(f"  Convergence:    {len(conv)}")
        print()
        for a in (vol + act + conv)[:15]:
            print(f"  [{a.anomaly_type}] {a.description[:100]}")
        print()

    elif cmd == "score-all":
        gen = HypothesisGenerator(engine)
        results = gen.score_all()
        print(f"\n=== Scored {len(results)} Hypotheses ===\n")
        for r in results:
            print(
                f"  {r.get('id', '?')[:20]}: {r.get('outcome', '?')} "
                f"(conf={r.get('confidence', 0):.3f})"
            )
        print()

    elif cmd == "stats":
        stats = get_stats(engine)
        print("\n=== Hypothesis Engine Stats ===\n")
        _print_json(stats)
        print()

    else:
        print("Usage: python intelligence/hypothesis_engine.py "
              "[discover | scan-patterns | scan-anomalies | score-all | stats]")
        sys.exit(1)


if __name__ == "__main__":
    main()
