"""
GRID Oracle Engine — Self-improving prediction loop.

The Oracle makes direct, scoreable predictions and iterates on what works.
Every prediction is logged with full provenance. After expiry, predictions
are scored against reality. Model weights evolve based on track record.

Architecture:
  1. Signal Assembly  — gather all available signals for a ticker
  2. Anti-Signal Check — explicitly find contradicting evidence
  3. Model Ensemble   — run all active models, weight by track record
  4. Prediction Generation — specific strike, expiry, direction, confidence
  5. Immutable Logging — every prediction journaled with full context
  6. Post-Expiry Scoring — automated P/L and hit-rate tracking
  7. Weight Evolution — winning models get more weight, losers decay
  8. Oracle Report — formatted digest with predictions + anti-signals

The Oracle tests constantly. It doesn't wait for perfect signals.
It makes predictions at every confidence level and scores them ALL.
Low-confidence predictions that hit teach us something.
High-confidence predictions that miss teach us more.
"""

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, timedelta, timezone
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

import os
_USE_SIGNAL_REGISTRY = os.getenv("GRID_SIGNAL_REGISTRY", "0") == "1"


# ── Prediction Types ────────────────────────────────────────────────────────

class PredictionType(str, Enum):
    DIRECTION = "direction"        # Up/down by expiry
    STRIKE_HIT = "strike_hit"      # Will reach strike price
    RANGE = "range"                # Will stay within range
    FLOW_SHIFT = "flow_shift"      # Capital flow direction change


class Verdict(str, Enum):
    PENDING = "pending"
    HIT = "hit"
    MISS = "miss"
    PARTIAL = "partial"            # Right direction, wrong magnitude
    NO_DATA = "no_data"             # Price data unavailable at generation time


@dataclass
class Signal:
    """A single signal contributing to a prediction."""
    name: str
    family: str                    # rates, credit, vol, equity, etc.
    value: float
    z_score: float
    direction: str                 # "bullish", "bearish", "neutral"
    weight: float                  # Model weight for this signal
    freshness_hours: float         # How old is this data


@dataclass
class AntiSignal:
    """Evidence contradicting the prediction."""
    name: str
    family: str
    value: float
    z_score: float
    contradiction: str             # What it contradicts and why
    severity: float                # 0-1, how much it undermines the prediction


@dataclass
class OraclePrediction:
    """A complete, scoreable prediction."""
    id: str                        # Unique hash
    timestamp: datetime
    ticker: str
    prediction_type: PredictionType
    direction: str                 # "CALL" / "PUT" / "LONG" / "SHORT"
    target_price: float | None     # Strike or target
    current_price: float
    expiry: date                   # When to score this
    confidence: float              # 0-1
    expected_move_pct: float       # Expected % move

    # Signal/anti-signal breakdown
    signals: list[Signal] = field(default_factory=list)
    anti_signals: list[AntiSignal] = field(default_factory=list)
    signal_strength: float = 0.0   # Net signal after anti-signal deduction
    coherence: float = 0.0         # How aligned are the signals (0-1)

    # Model attribution
    model_name: str = ""
    model_version: str = ""
    model_weights: dict = field(default_factory=dict)

    # Capital flow context
    flow_context: dict = field(default_factory=dict)

    # Scoring (filled after expiry)
    verdict: Verdict = Verdict.PENDING
    actual_price: float | None = None
    actual_move_pct: float | None = None
    pnl_pct: float | None = None
    scored_at: datetime | None = None
    score_notes: str = ""

    def to_dict(self) -> dict:
        d = asdict(self)
        d["prediction_type"] = self.prediction_type.value
        d["verdict"] = self.verdict.value
        d["timestamp"] = self.timestamp.isoformat()
        d["expiry"] = self.expiry.isoformat()
        d["signals"] = [asdict(s) for s in self.signals]
        d["anti_signals"] = [asdict(a) for a in self.anti_signals]
        if self.scored_at:
            d["scored_at"] = self.scored_at.isoformat()
        return d


# ── Model Registry ──────────────────────────────────────────────────────────

@dataclass
class OracleModel:
    """A prediction model with evolving weights."""
    name: str
    version: str
    description: str
    signal_families: list[str]     # Which signal families it uses
    weight: float = 1.0            # Current weight (evolves)
    predictions_made: int = 0
    hits: int = 0
    misses: int = 0
    partials: int = 0
    cumulative_pnl: float = 0.0
    sharpe: float = 0.0
    last_updated: datetime | None = None

    @property
    def hit_rate(self) -> float:
        total = self.hits + self.misses + self.partials
        return self.hits / total if total > 0 else 0.0

    @property
    def total_scored(self) -> int:
        return self.hits + self.misses + self.partials


# Default models — each combines different signal families
DEFAULT_MODELS = [
    OracleModel(
        name="flow_momentum",
        version="1.0",
        description="Capital flow direction + price momentum. "
                    "Predicts continuation when flows and momentum align.",
        signal_families=["equity", "flows", "breadth", "vol"],
    ),
    OracleModel(
        name="regime_contrarian",
        version="1.0",
        description="Regime state + mean reversion signals. "
                    "Contrarian when regime is extreme + OU parameters suggest reversion.",
        signal_families=["rates", "credit", "vol", "macro"],
    ),
    OracleModel(
        name="options_flow",
        version="1.0",
        description="Options positioning + dark pool signals. "
                    "PCR extremes + IV skew + max pain convergence.",
        signal_families=["sentiment", "vol", "equity"],
    ),
    OracleModel(
        name="cross_asset",
        version="1.0",
        description="Cross-asset confirmation. Rates, FX, commodities, credit "
                    "must all confirm equity direction.",
        signal_families=["rates", "fx", "commodity", "credit", "equity"],
    ),
    OracleModel(
        name="news_energy",
        version="1.0",
        description="News sentiment momentum + coherence. "
                    "When news energy aligns across sources, follow the force vector.",
        signal_families=["sentiment", "alternative", "equity"],
    ),
    OracleModel(
        name="timeseries_enhanced",
        version="1.0",
        description="TimesFM foundation model forecasts. "
                    "Uses probabilistic time-series predictions for direction, "
                    "confidence, and momentum signals.",
        signal_families=["timeseries_forecast"],
    ),
]


# ── Oracle Engine ───────────────────────────────────────────────────────────

class OracleEngine:
    """The self-improving prediction engine."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self._ensure_tables()
        self.models = self._load_models()
        log.info("Oracle initialised — {n} models loaded", n=len(self.models))

    def _ensure_tables(self) -> None:
        """Create oracle tables if they don't exist."""
        with self.engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oracle_predictions (
                    id TEXT PRIMARY KEY,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    ticker TEXT NOT NULL,
                    prediction_type TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    target_price DOUBLE PRECISION,
                    entry_price DOUBLE PRECISION NOT NULL,
                    expiry DATE NOT NULL,
                    confidence DOUBLE PRECISION NOT NULL,
                    expected_move_pct DOUBLE PRECISION,
                    signal_strength DOUBLE PRECISION,
                    coherence DOUBLE PRECISION,
                    model_name TEXT NOT NULL,
                    model_version TEXT,
                    signals JSONB,
                    anti_signals JSONB,
                    flow_context JSONB,
                    model_weights JSONB,
                    verdict TEXT DEFAULT 'pending',
                    actual_price DOUBLE PRECISION,
                    actual_move_pct DOUBLE PRECISION,
                    pnl_pct DOUBLE PRECISION,
                    scored_at TIMESTAMPTZ,
                    score_notes TEXT
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oracle_models (
                    name TEXT PRIMARY KEY,
                    version TEXT,
                    description TEXT,
                    signal_families JSONB,
                    weight DOUBLE PRECISION DEFAULT 1.0,
                    predictions_made INTEGER DEFAULT 0,
                    hits INTEGER DEFAULT 0,
                    misses INTEGER DEFAULT 0,
                    partials INTEGER DEFAULT 0,
                    cumulative_pnl DOUBLE PRECISION DEFAULT 0.0,
                    sharpe DOUBLE PRECISION DEFAULT 0.0,
                    last_updated TIMESTAMPTZ
                )
            """))
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS oracle_iterations (
                    id SERIAL PRIMARY KEY,
                    iteration_at TIMESTAMPTZ DEFAULT NOW(),
                    models_updated INTEGER,
                    predictions_scored INTEGER,
                    best_model TEXT,
                    best_hit_rate DOUBLE PRECISION,
                    worst_model TEXT,
                    worst_hit_rate DOUBLE PRECISION,
                    weight_changes JSONB,
                    notes TEXT
                )
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_oracle_pred_expiry
                ON oracle_predictions (expiry) WHERE verdict = 'pending'
            """))
            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_oracle_pred_ticker
                ON oracle_predictions (ticker, created_at DESC)
            """))
            # TimesFM forecast storage (used by forecaster_adapter)
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS timeseries_forecasts (
                    ticker TEXT NOT NULL,
                    forecast_date DATE NOT NULL,
                    horizon INTEGER NOT NULL,
                    predictions TEXT NOT NULL,
                    lower_bound TEXT NOT NULL,
                    upper_bound TEXT NOT NULL,
                    forecast_std TEXT NOT NULL,
                    model_version TEXT,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    PRIMARY KEY (ticker, forecast_date, horizon)
                )
            """))

    def _load_models(self) -> list[OracleModel]:
        """Load models from DB or seed defaults."""
        models = []
        with self.engine.connect() as conn:
            rows = conn.execute(text("SELECT * FROM oracle_models")).fetchall()
            if rows:
                for r in rows:
                    models.append(OracleModel(
                        name=r[0], version=r[1] or "1.0", description=r[2] or "",
                        signal_families=r[3] or [], weight=r[4] or 1.0,
                        predictions_made=r[5] or 0, hits=r[6] or 0,
                        misses=r[7] or 0, partials=r[8] or 0,
                        cumulative_pnl=r[9] or 0.0, sharpe=r[10] or 0.0,
                        last_updated=r[11],
                    ))
            else:
                # Seed defaults
                models = list(DEFAULT_MODELS)
                with self.engine.begin() as wconn:
                    for m in models:
                        wconn.execute(text(
                            "INSERT INTO oracle_models (name, version, description, signal_families, weight) "
                            "VALUES (:n, :v, :d, :sf, :w) ON CONFLICT DO NOTHING"
                        ), {"n": m.name, "v": m.version, "d": m.description,
                            "sf": json.dumps(m.signal_families), "w": m.weight})
        return models

    # ── Signal Assembly ─────────────────────────────────────────────────

    def _gather_signals(self, ticker: str, families: list[str]) -> list[Signal]:
        """Gather all available signals for a ticker across specified families."""
        signals = []
        with self.engine.connect() as conn:
            # Get latest z-scores for relevant features
            rows = conn.execute(text("""
                SELECT fr.name, fr.family, rs.value, rs.obs_date
                FROM resolved_series rs
                JOIN feature_registry fr ON rs.feature_id = fr.id
                WHERE fr.family = ANY(:fams)
                AND fr.model_eligible = TRUE
                AND rs.obs_date >= CURRENT_DATE - 30
                ORDER BY fr.name, rs.obs_date DESC
            """), {"fams": families}).fetchall()

            # Group by feature, compute z-score from recent history
            feature_data: dict[str, list] = {}
            for r in rows:
                feature_data.setdefault(r[0], []).append({"value": r[2], "date": r[3], "family": r[1]})

            for fname, data_points in feature_data.items():
                if len(data_points) < 5:
                    continue
                values = [d["value"] for d in data_points if d["value"] is not None]
                if not values:
                    continue
                latest = values[0]
                mean = np.mean(values)
                std = np.std(values) if len(values) > 1 else 1.0
                z = (latest - mean) / std if std > 0 else 0.0

                # Determine direction
                if z > 0.5:
                    direction = "bullish"
                elif z < -0.5:
                    direction = "bearish"
                else:
                    direction = "neutral"

                # Freshness
                latest_date = data_points[0]["date"]
                hours_old = (date.today() - latest_date).days * 24

                signals.append(Signal(
                    name=fname, family=data_points[0]["family"],
                    value=latest, z_score=round(z, 3),
                    direction=direction, weight=1.0,
                    freshness_hours=hours_old,
                ))

            # Add options signals if available
            opt_row = conn.execute(text("""
                SELECT put_call_ratio, iv_atm, iv_skew, max_pain, spot_price,
                       total_oi, term_structure_slope, oi_concentration
                FROM options_daily_signals
                WHERE ticker = :t AND total_oi >= 1000 AND (iv_atm IS NULL OR iv_atm >= 0.03)
                ORDER BY signal_date DESC LIMIT 1
            """), {"t": ticker}).fetchone()

            if opt_row:
                pcr, iv, skew, mp, spot, oi, term, conc = opt_row
                if pcr is not None:
                    pcr_dir = "bearish" if pcr > 1.2 else "bullish" if pcr < 0.7 else "neutral"
                    signals.append(Signal("pcr", "sentiment", pcr, 0, pcr_dir, 1.0, 0))
                if iv is not None:
                    signals.append(Signal("iv_atm", "vol", iv, 0, "neutral", 1.0, 0))
                if mp is not None and spot:
                    mp_pct = (spot - mp) / spot * 100
                    mp_dir = "bearish" if mp_pct > 3 else "bullish" if mp_pct < -3 else "neutral"
                    signals.append(Signal("max_pain_gap", "sentiment", mp_pct, 0, mp_dir, 1.0, 0))

        return signals

    def _gather_signals_from_registry(self, ticker: str, model: Any) -> list[Signal]:
        """Gather signals from the signal_registry for a model's subscriptions.

        Returns signals in the same Signal format as _gather_signals() so
        downstream code (direction scoring, anti-signal, etc.) works unchanged.
        Returns [] if registry is empty or unavailable — caller falls back to legacy.
        """
        try:
            from oracle.model_factory import ModelFactory
            factory = ModelFactory(self.engine)
            raw = factory.get_signals_for_model(model.name, datetime.now(timezone.utc))
            if not raw:
                return []

            signals = []
            for s in raw:
                direction = s.get("direction", "neutral")
                z = float(s.get("z_score") or s.get("value") or 0)
                conf = float(s.get("confidence", 0.5))
                name = s.get("source_module", "unknown")
                family = name.split(":")[1] if ":" in name else name
                sig_dir = "bullish" if direction == "bullish" else ("bearish" if direction == "bearish" else "neutral")
                signals.append(Signal(name, family, z, 0, sig_dir, conf, 0))
            return signals
        except Exception as exc:
            log.debug("_gather_signals_from_registry failed for {m}: {e}", m=model.name, e=str(exc))
            return []

    def _find_anti_signals(
        self, signals: list[Signal], direction: str
    ) -> list[AntiSignal]:
        """Find signals that contradict the predicted direction."""
        anti = []
        target_dir = "bullish" if direction in ("CALL", "LONG") else "bearish"
        contra_dir = "bearish" if target_dir == "bullish" else "bullish"

        for sig in signals:
            if sig.direction == contra_dir and abs(sig.z_score) > 1.0:
                severity = min(1.0, abs(sig.z_score) / 3.0)
                anti.append(AntiSignal(
                    name=sig.name,
                    family=sig.family,
                    value=sig.value,
                    z_score=sig.z_score,
                    contradiction=f"{sig.name} ({sig.family}) at z={sig.z_score:.2f} "
                                  f"points {contra_dir} against predicted {target_dir}",
                    severity=severity,
                ))

        return sorted(anti, key=lambda a: -a.severity)

    def _compute_coherence(self, signals: list[Signal], direction: str) -> float:
        """Measure how aligned signals are with the prediction direction."""
        if not signals:
            return 0.0
        target = "bullish" if direction in ("CALL", "LONG") else "bearish"
        aligned = sum(1 for s in signals if s.direction == target)
        opposed = sum(1 for s in signals if s.direction != target and s.direction != "neutral")
        total = aligned + opposed
        return aligned / total if total > 0 else 0.5

    # ── Convergence Integration ────────────────────────────────────────

    def _get_convergence_for_ticker(self, ticker: str) -> list[dict]:
        """Query trust_scorer convergence events for this ticker.

        Returns list of convergence events, each with:
        - signal_type: BUY/SELL
        - source_count: number of independent source types
        - combined_confidence: weighted avg trust score
        - sources: list of {source_type, source_id, trust_score}
        """
        try:
            from intelligence.trust_scorer import detect_convergence
            events = detect_convergence(self.engine, ticker=ticker)
            return events or []
        except Exception as e:
            log.warning("Convergence detection failed for {t}: {e}", t=ticker, e=str(e))
            return []

    # ── Credit Cycle → Factor Family Routing ──────────────────────────

    def _get_credit_cycle_routing(self) -> dict[str, float]:
        """
        Query the latest credit cycle regime signal and return family weight boosts.

        Contraction → favor vol/alternative signals, penalize equity/flows.
        Expansion → favor equity/flows, penalize defensive signals.
        """
        try:
            if not _USE_SIGNAL_REGISTRY:
                return {}
            with self.engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT metadata->>'state' as state,
                           confidence
                    FROM signal_registry
                    WHERE source_module = 'alpha_research:credit_cycle'
                    ORDER BY valid_from DESC LIMIT 1
                """)).fetchone()
            if not row:
                return {}
            state = row[0]
            confidence = float(row[1]) if row[1] else 0.5
            scale = 0.3 * confidence  # max ±30% boost at full confidence
            if state == "contraction":
                return {
                    "vol": 1.0 + scale,
                    "alternative": 1.0 + scale,
                    "credit": 1.0 + scale,
                    "equity": 1.0 - scale,
                    "flows": 1.0 - scale,
                }
            elif state == "expansion":
                return {
                    "equity": 1.0 + scale,
                    "flows": 1.0 + scale,
                    "vol": 1.0 - scale * 0.5,
                    "alternative": 1.0 - scale * 0.5,
                }
            return {}
        except Exception as e:
            log.warning("Credit cycle routing failed: {e}", e=str(e))
            return {}

    # ── Decision Journal Feedback ──────────────────────────────────────

    def _get_journal_feedback(self, ticker: str) -> dict[str, float]:
        """
        Read recent decision journal outcomes to adjust confidence.

        If recent predictions on this ticker/direction have been mostly wrong,
        reduce confidence. If mostly right, boost slightly.
        """
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(text("""
                    SELECT grid_recommendation, verdict
                    FROM decision_journal
                    WHERE decision_timestamp >= NOW() - INTERVAL '30 days'
                      AND verdict IS NOT NULL
                    ORDER BY decision_timestamp DESC
                    LIMIT 50
                """)).fetchall()
            if not rows or len(rows) < 5:
                return {}
            hits = sum(1 for r in rows if r[1] == "HELPED")
            misses = sum(1 for r in rows if r[1] == "HARMED")
            total = hits + misses
            if total < 5:
                return {}
            hit_rate = hits / total
            # Bias: >60% hit rate → slight boost, <40% → penalize
            if hit_rate > 0.6:
                return {"confidence_multiplier": 1.0 + (hit_rate - 0.6) * 0.5}
            elif hit_rate < 0.4:
                return {"confidence_multiplier": 1.0 - (0.4 - hit_rate) * 0.5}
            return {}
        except Exception as e:
            log.warning("Journal feedback failed for {t}: {e}", t=ticker, e=str(e))
            return {}

    # ── Capital Flow Context ────────────────────────────────────────────

    def _get_flow_context(self, ticker: str) -> dict:
        """Get capital flow context for a ticker."""
        context = {}
        try:
            with self.engine.connect() as conn:
                # Get latest capital flow snapshot
                row = conn.execute(text("""
                    SELECT relative_strength, narrative FROM capital_flow_snapshots
                    ORDER BY snapshot_date DESC LIMIT 1
                """)).fetchone()
                if row and row[0]:
                    rs_data = row[0] if isinstance(row[0], dict) else json.loads(row[0])
                    # Find sector for this ticker from relative_strength
                    for sector, info in rs_data.items():
                        signal = info.get("signal", "NEUTRAL") if isinstance(info, dict) else "NEUTRAL"
                        context.setdefault("sectors_scanned", []).append(sector)
                        # Match ticker to sector via options_daily_signals
                    if row[1]:
                        context["flow_narrative"] = str(row[1])[:200]

                # Get regime
                row = conn.execute(text("""
                    SELECT inferred_state, state_confidence, transition_probability
                    FROM decision_journal ORDER BY decision_timestamp DESC LIMIT 1
                """)).fetchone()
                if row:
                    context["regime"] = row[0]
                    context["regime_confidence"] = row[1]
                    context["transition_prob"] = row[2]
        except Exception as e:
            log.warning("Flow context failed: {e}", e=str(e))

        return context

    # ── TimesFM Forecast Integration ──────────────────────────────────────

    def _get_timesfm_forecast(self, ticker: str) -> dict | None:
        """Fetch the latest TimesFM forecast for a ticker from the database.

        Returns a dict with predictions, lower_bound, upper_bound,
        forecast_std, horizon, model_version — or None if unavailable.
        """
        try:
            with self.engine.connect() as conn:
                row = conn.execute(text("""
                    SELECT predictions, lower_bound, upper_bound,
                           forecast_std, horizon, model_version, forecast_date
                    FROM timeseries_forecasts
                    WHERE ticker = :t
                      AND forecast_date >= CURRENT_DATE - 3
                    ORDER BY forecast_date DESC
                    LIMIT 1
                """), {"t": ticker}).fetchone()

            if not row:
                return None

            import ast

            def _parse_list(val: str) -> list[float]:
                parsed = ast.literal_eval(val)
                return [float(x) for x in parsed]

            return {
                "predictions": _parse_list(row[0]),
                "lower_bound": _parse_list(row[1]),
                "upper_bound": _parse_list(row[2]),
                "forecast_std": _parse_list(row[3]),
                "horizon": int(row[4]),
                "model_version": row[5] or "unknown",
                "forecast_date": row[6],
            }
        except Exception as exc:
            log.debug(
                "TimesFM forecast lookup failed for {t}: {e}",
                t=ticker, e=str(exc),
            )
            return None

    # ── Prediction Generation ───────────────────────────────────────────

    def generate_predictions(
        self, tickers: list[str] | None = None
    ) -> list[OraclePrediction]:
        """Generate predictions for all tickers using all models.

        Each model produces a prediction for each ticker. Predictions
        with low confidence are still logged — they're how we learn.
        """
        if tickers is None:
            tickers = self._get_active_tickers()

        all_predictions = []
        now = datetime.now(timezone.utc)

        for ticker in tickers:
            flow_ctx = self._get_flow_context(ticker)

            # Get current price
            spot = self._get_spot_price(ticker)
            if not spot:
                log.warning(
                    "Oracle: no spot price for {t} - storing no_data placeholder per model",
                    t=ticker,
                )
                for model in self.models:
                    pred_id = hashlib.md5(
                        f"{ticker}:{model.name}:no_data:{now.isoformat()}".encode()
                    ).hexdigest()[:16]
                    placeholder = OraclePrediction(
                        id=pred_id,
                        timestamp=now,
                        ticker=ticker,
                        prediction_type=PredictionType.DIRECTION,
                        direction="NONE",
                        target_price=None,
                        current_price=0.0,
                        expiry=self._next_monthly_expiry(),
                        confidence=0.0,
                        expected_move_pct=0.0,
                        model_name=model.name,
                        model_version=model.version,
                        verdict=Verdict.NO_DATA,
                        score_notes="No spot price available at prediction time",
                    )
                    all_predictions.append(placeholder)
                continue

            # Credit cycle → family weight routing
            credit_family_boost = self._get_credit_cycle_routing()

            # Decision journal feedback: learn from recent hits/misses
            journal_bias = self._get_journal_feedback(ticker)

            for model in self.models:
                try:
                    # ── TimesFM model: use forecaster_adapter ──────────
                    if model.name == "timeseries_enhanced":
                        try:
                            from oracle.forecaster_adapter import (
                                forecast_to_anti_signals,
                                forecast_to_prediction,
                                forecast_to_signals,
                            )

                            fc = self._get_timesfm_forecast(ticker)
                            if not fc:
                                continue  # No forecast available for this ticker

                            # Build a lightweight forecast result object
                            class _ForecastResult:
                                pass

                            fr = _ForecastResult()
                            fr.predictions = fc["predictions"]
                            fr.lower_bound = fc["lower_bound"]
                            fr.upper_bound = fc["upper_bound"]
                            fr.forecast_std = fc["forecast_std"]
                            fr.horizon = fc["horizon"]
                            fr.model_version = fc["model_version"]
                            fr.forecast_date = fc["forecast_date"]

                            tsf_signals = forecast_to_signals(fr, current_price=spot)
                            tsf_anti = forecast_to_anti_signals(fr, [])

                            pred = forecast_to_prediction(
                                fr, ticker, spot,
                                signals=tsf_signals,
                                anti_signals=tsf_anti,
                            )
                            if pred is not None:
                                # Apply journal feedback to confidence
                                journal_mult = journal_bias.get(
                                    "confidence_multiplier", 1.0,
                                )
                                pred = replace(
                                    pred,
                                    confidence=round(
                                        min(0.95, pred.confidence * journal_mult), 4,
                                    ),
                                    model_weights={
                                        m.name: m.weight for m in self.models
                                    },
                                )
                                all_predictions.append(pred)
                        except Exception as exc:
                            log.debug(
                                "TimesFM model skipped for {t}: {e}",
                                t=ticker, e=str(exc),
                            )
                        continue  # Skip standard signal gathering for this model

                    # Try signal registry first (when enabled), fall back to legacy
                    signals = self._gather_signals_from_registry(ticker, model) if _USE_SIGNAL_REGISTRY else []
                    if not signals:
                        signals = self._gather_signals(ticker, model.signal_families)

                    # Apply credit-cycle-based family weighting
                    if credit_family_boost:
                        for sig in signals:
                            src = getattr(sig, "source_module", "") or ""
                            for family_key, boost in credit_family_boost.items():
                                if family_key in src:
                                    sig = sig._replace(weight=sig.weight * boost) if hasattr(sig, '_replace') else sig
                    if len(signals) < 3:
                        continue  # Not enough data for this model

                    # Compute net direction
                    bull_score = sum(s.z_score * s.weight for s in signals if s.direction == "bullish")
                    bear_score = sum(abs(s.z_score) * s.weight for s in signals if s.direction == "bearish")

                    if bull_score > bear_score:
                        direction = "CALL"
                        net_score = bull_score - bear_score
                    elif bear_score > bull_score:
                        direction = "PUT"
                        net_score = bear_score - bull_score
                    else:
                        continue  # No signal

                    # Anti-signals
                    anti_signals = self._find_anti_signals(signals, direction)
                    anti_deduction = sum(a.severity for a in anti_signals) * 0.3

                    # Signal strength = net score - anti-signal deduction
                    signal_strength = max(0, net_score - anti_deduction)
                    coherence = self._compute_coherence(signals, direction)

                    # ── Convergence amplification ──────────────────────
                    # If trust_scorer detects 3+ independent sources
                    # agreeing on this ticker+direction, boost confidence
                    convergence_boost = 1.0
                    try:
                        convergence_events = self._get_convergence_for_ticker(ticker)
                        pred_dir = "BUY" if direction in ("CALL", "LONG") else "SELL"
                        for evt in convergence_events:
                            if evt.get("signal_type") == pred_dir:
                                # Boost: 10% per source above minimum 3
                                src_count = evt.get("source_count", 0)
                                combined_conf = evt.get("combined_confidence", 0.5)
                                convergence_boost = 1.0 + 0.1 * (src_count - 2) * combined_conf
                                # Inject convergence sources as additional signals
                                for src in evt.get("sources", []):
                                    signals.append(Signal(
                                        name=f"convergence:{src['source_type']}",
                                        family="convergence",
                                        value=src.get("trust_score", 0.5),
                                        z_score=1.5 if pred_dir == "BUY" else -1.5,
                                        direction="bullish" if pred_dir == "BUY" else "bearish",
                                        weight=src.get("trust_score", 0.5),
                                        freshness_hours=0,
                                    ))
                                break  # Use first matching convergence event
                    except Exception as e:
                        log.debug("Convergence signal skipped for {t}: {e}", t=ticker, e=str(e))

                    # Confidence = signal strength × coherence × model weight × convergence
                    raw_confidence = signal_strength * coherence * model.weight * convergence_boost

                    # Apply decision journal feedback (learn from recent hit/miss rate)
                    journal_mult = journal_bias.get("confidence_multiplier", 1.0)
                    raw_confidence *= journal_mult

                    confidence = min(0.95, max(0.05, raw_confidence / 5.0))  # Normalize to 0-1

                    # Expected move (conservative estimate)
                    expected_move = signal_strength * 0.5  # 0.5% per unit of signal strength

                    # Target price
                    if direction == "CALL":
                        target = spot * (1 + expected_move / 100)
                    else:
                        target = spot * (1 - expected_move / 100)

                    # Expiry: next monthly options expiry (3rd Friday)
                    expiry = self._next_monthly_expiry()

                    # Create prediction
                    pred_id = hashlib.md5(
                        f"{ticker}:{model.name}:{direction}:{now.isoformat()}".encode()
                    ).hexdigest()[:16]

                    pred = OraclePrediction(
                        id=pred_id,
                        timestamp=now,
                        ticker=ticker,
                        prediction_type=PredictionType.DIRECTION,
                        direction=direction,
                        target_price=round(target, 2),
                        current_price=spot,
                        expiry=expiry,
                        confidence=round(confidence, 4),
                        expected_move_pct=round(expected_move, 2),
                        signals=signals[:10],  # Top 10 signals
                        anti_signals=anti_signals[:5],  # Top 5 anti-signals
                        signal_strength=round(signal_strength, 3),
                        coherence=round(coherence, 3),
                        model_name=model.name,
                        model_version=model.version,
                        model_weights={m.name: m.weight for m in self.models},
                        flow_context=flow_ctx,
                    )

                    all_predictions.append(pred)

                except Exception as e:
                    log.warning("Model {m} failed for {t}: {e}", m=model.name, t=ticker, e=str(e))

        # Sort by confidence
        all_predictions.sort(key=lambda p: -p.confidence)

        # Log all predictions
        self._store_predictions(all_predictions)

        log.info(
            "Oracle generated {n} predictions across {t} tickers × {m} models",
            n=len(all_predictions), t=len(tickers), m=len(self.models),
        )

        return all_predictions

    # ── Scoring Loop ────────────────────────────────────────────────────

    def score_expired_predictions(self) -> dict[str, Any]:
        """Score all predictions that have reached their expiry date.

        This is the feedback loop. Every scored prediction adjusts model weights.
        """
        today = date.today()
        scored = 0
        results = {"hits": 0, "misses": 0, "partials": 0, "total": 0}

        with self.engine.begin() as conn:
            # Get pending predictions past expiry
            rows = conn.execute(text("""
                SELECT id, ticker, direction, target_price, entry_price, expiry,
                       confidence, expected_move_pct, model_name
                FROM oracle_predictions
                WHERE verdict = 'pending' AND expiry <= :today
                ORDER BY expiry
            """), {"today": today}).fetchall()

            # no_data rows are already final - exclude from scoring loop
            rows = [r for r in rows if r[2] != "NONE"]
            for r in rows:
                pred_id, ticker, direction, target, entry, expiry, conf, expected, model = r

                # Get actual price at expiry
                actual = self._get_price_at_date(ticker, expiry)
                if actual is None:
                    continue

                actual_move = (actual - entry) / entry * 100

                # Score
                if direction == "CALL":
                    hit = actual > entry
                    pnl = actual_move
                elif direction == "PUT":
                    hit = actual < entry
                    pnl = -actual_move
                else:
                    continue

                # Verdict
                if hit and abs(actual_move) >= abs(expected) * 0.5:
                    verdict = "hit"
                    results["hits"] += 1
                elif hit:
                    verdict = "partial"
                    results["partials"] += 1
                else:
                    verdict = "miss"
                    results["misses"] += 1

                # Update prediction
                conn.execute(text("""
                    UPDATE oracle_predictions
                    SET verdict = :v, actual_price = :ap, actual_move_pct = :am,
                        pnl_pct = :pnl, scored_at = NOW(),
                        score_notes = :notes
                    WHERE id = :id
                """), {
                    "v": verdict, "ap": actual, "am": round(actual_move, 2),
                    "pnl": round(pnl, 2), "id": pred_id,
                    "notes": f"Entry ${entry:.2f} → Actual ${actual:.2f} ({actual_move:+.1f}%)",
                })

                # Update model stats
                # verdict is from internal logic (hit/partial/miss) — map to safe column names
                _verdict_col_map = {
                    "hit": "hits",
                    "partial": "partials",
                    "miss": "misses",
                }
                verdict_col = _verdict_col_map.get(verdict)
                if verdict_col is None:
                    log.warning("Unknown verdict {v}, skipping model stats update", v=verdict)
                else:
                    conn.execute(text(
                        f"UPDATE oracle_models "
                        f"SET {verdict_col} = {verdict_col} + 1, "
                        "    predictions_made = predictions_made + 1, "
                        "    cumulative_pnl = cumulative_pnl + :pnl, "
                        "    last_updated = NOW() "
                        "WHERE name = :model"
                    ), {"pnl": pnl, "model": model})

                scored += 1

        results["total"] = scored
        log.info("Scored {n} predictions: {h}H/{p}P/{m}M",
                 n=scored, h=results["hits"], p=results["partials"], m=results["misses"])

        return results

    # ── Weight Evolution ────────────────────────────────────────────────

    def evolve_weights(self) -> dict[str, Any]:
        """Adjust model weights based on track record.

        Models that hit more get higher weight. Models that miss decay.
        Minimum weight floor prevents complete abandonment (they might
        work in different regimes).
        """
        MIN_WEIGHT = 0.1
        MAX_WEIGHT = 3.0
        LEARNING_RATE = 0.1
        MIN_PREDICTIONS = 10  # Need at least 10 scored predictions to adjust

        changes = {}

        with self.engine.begin() as conn:
            rows = conn.execute(text(
                "SELECT name, weight, hits, misses, partials, predictions_made, cumulative_pnl "
                "FROM oracle_models"
            )).fetchall()

            best_model = None
            best_rate = 0
            worst_model = None
            worst_rate = 1.0

            for r in rows:
                name, weight, hits, misses, partials, total, pnl = r
                if total < MIN_PREDICTIONS:
                    continue

                hit_rate = hits / total if total > 0 else 0
                # Partial hits count as 0.5
                adj_rate = (hits + partials * 0.5) / total if total > 0 else 0

                # Bayesian update: move weight toward performance
                target_weight = 0.5 + adj_rate * 2.0  # 50% hit rate → weight 1.5
                new_weight = weight + LEARNING_RATE * (target_weight - weight)
                new_weight = max(MIN_WEIGHT, min(MAX_WEIGHT, new_weight))

                if new_weight != weight:
                    conn.execute(text(
                        "UPDATE oracle_models SET weight = :w, last_updated = NOW() WHERE name = :n"
                    ), {"w": round(new_weight, 4), "n": name})
                    changes[name] = {"old": round(weight, 4), "new": round(new_weight, 4),
                                     "hit_rate": round(hit_rate, 3), "adj_rate": round(adj_rate, 3)}

                if adj_rate > best_rate:
                    best_rate = adj_rate
                    best_model = name
                if adj_rate < worst_rate:
                    worst_rate = adj_rate
                    worst_model = name

            # Log iteration
            conn.execute(text("""
                INSERT INTO oracle_iterations
                (models_updated, predictions_scored, best_model, best_hit_rate,
                 worst_model, worst_hit_rate, weight_changes, notes)
                VALUES (:mu, :ps, :bm, :bhr, :wm, :whr, :wc, :notes)
            """), {
                "mu": len(changes), "ps": sum(r[5] for r in rows),
                "bm": best_model, "bhr": best_rate,
                "wm": worst_model, "whr": worst_rate,
                "wc": json.dumps(changes),
                "notes": f"Weight evolution: {len(changes)} models adjusted",
            })

        # Reload models
        self.models = self._load_models()

        log.info("Weight evolution: {n} models adjusted. Best: {b} ({br:.1%}), Worst: {w} ({wr:.1%})",
                 n=len(changes), b=best_model, br=best_rate, w=worst_model, wr=worst_rate)

        return {
            "changes": changes,
            "best_model": best_model, "best_rate": best_rate,
            "worst_model": worst_model, "worst_rate": worst_rate,
        }

    # ── Full Cycle ──────────────────────────────────────────────────────

    def run_cycle(self, tickers: list[str] | None = None) -> dict[str, Any]:
        """Run one full oracle cycle: score → evolve → predict → report."""
        log.info("═══ Oracle Cycle Starting ═══")

        # 1. Score expired predictions
        score_result = self.score_expired_predictions()

        # 2. Evolve weights based on scores
        evolve_result = self.evolve_weights()

        # 2.5 Run model evolver (autonomous mutation/crossover/kill)
        model_evolve_result = {}
        try:
            from oracle.model_evolver import ModelEvolver
            evolver = ModelEvolver(self.engine)
            model_evolve_result = evolver.evolve_cycle()
            log.info("Model evolver: killed={k} spawned={s}",
                     k=len(model_evolve_result.get("killed", [])),
                     s=len(model_evolve_result.get("spawned", [])))
        except Exception as exc:
            log.debug("Model evolver failed: {e}", e=str(exc))

        # 3. Generate new predictions
        predictions = self.generate_predictions(tickers)

        # 4. Get model leaderboard
        leaderboard = self._get_leaderboard()

        result = {
            "scoring": score_result,
            "evolution": evolve_result,
            "new_predictions": len(predictions),
            "top_predictions": [p.to_dict() for p in predictions[:10]],
            "leaderboard": leaderboard,
        }

        log.info("═══ Oracle Cycle Complete: {n} new predictions ═══", n=len(predictions))
        return result

    # ── Helpers ──────────────────────────────────────────────────────────

    def _get_active_tickers(self) -> list[str]:
        """Get tickers with recent options + equity data."""
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT DISTINCT ticker FROM options_daily_signals
                WHERE signal_date >= CURRENT_DATE - 7
                AND total_oi >= 1000
                ORDER BY ticker
            """)).fetchall()
        return [r[0] for r in rows]

    def _get_spot_price(self, ticker: str) -> float | None:
        """Get latest spot price for a ticker."""
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT spot_price FROM options_daily_signals
                WHERE ticker = :t AND spot_price > 0
                ORDER BY signal_date DESC LIMIT 1
            """), {"t": ticker}).fetchone()
            if row:
                return float(row[0])

            # Fallback to yfinance raw data
            row = conn.execute(text("""
                SELECT value FROM raw_series
                WHERE series_id = :sid AND pull_status = 'SUCCESS'
                ORDER BY obs_date DESC LIMIT 1
            """), {"sid": f"YF:{ticker}:close"}).fetchone()
            return float(row[0]) if row else None

    def _get_price_at_date(self, ticker: str, target_date: date) -> float | None:
        """Get price at or near a specific date for scoring."""
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT spot_price FROM options_daily_signals
                WHERE ticker = :t AND signal_date <= :d AND spot_price > 0
                ORDER BY signal_date DESC LIMIT 1
            """), {"t": ticker, "d": target_date}).fetchone()
            if row:
                return float(row[0])
            # Fallback: try direct ticker, then USD suffix (for crypto)
            for sid in [f"YF:{ticker}:close", f"YF:{ticker}-USD:close"]:
                row = conn.execute(text("""
                    SELECT value FROM raw_series
                    WHERE series_id = :sid AND obs_date <= :d AND pull_status = 'SUCCESS'
                    ORDER BY obs_date DESC LIMIT 1
                """), {"sid": sid, "d": target_date}).fetchone()
                if row:
                    return float(row[0])
            # Last resort: resolved_series
            row = conn.execute(text("""
                SELECT rs.value FROM resolved_series rs
                JOIN feature_registry fr ON fr.id = rs.feature_id
                WHERE (fr.name = :n1 OR fr.name = :n2)
                AND rs.obs_date <= :d AND rs.value IS NOT NULL
                ORDER BY rs.obs_date DESC LIMIT 1
            """), {
                "n1": f"{ticker.lower()}_full",
                "n2": f"{ticker.lower()}_usd_full",
                "d": target_date,
            }).fetchone()
            return float(row[0]) if row else None

    def _next_monthly_expiry(self) -> date:
        """Get the next monthly options expiry (3rd Friday)."""
        today = date.today()
        # Find 3rd Friday of this month
        first_day = today.replace(day=1)
        # Days until first Friday
        days_to_friday = (4 - first_day.weekday()) % 7
        first_friday = first_day + timedelta(days=days_to_friday)
        third_friday = first_friday + timedelta(weeks=2)

        if third_friday <= today:
            # Move to next month
            if today.month == 12:
                first_day = today.replace(year=today.year + 1, month=1, day=1)
            else:
                first_day = today.replace(month=today.month + 1, day=1)
            days_to_friday = (4 - first_day.weekday()) % 7
            first_friday = first_day + timedelta(days=days_to_friday)
            third_friday = first_friday + timedelta(weeks=2)

        return third_friday

    def _store_predictions(self, predictions: list[OraclePrediction]) -> None:
        """Store predictions to the journal."""
        with self.engine.begin() as conn:
            for p in predictions:
                conn.execute(text("""
                    INSERT INTO oracle_predictions
                    (id, ticker, prediction_type, direction, target_price, entry_price,
                     expiry, confidence, expected_move_pct, signal_strength, coherence,
                     model_name, model_version, signals, anti_signals, flow_context, model_weights)
                    VALUES (:id, :t, :pt, :d, :tp, :ep, :exp, :conf, :em, :ss, :coh,
                            :mn, :mv, :sig, :anti, :fc, :mw)
                    ON CONFLICT (id) DO NOTHING
                """), {
                    "id": p.id, "t": p.ticker, "pt": p.prediction_type.value,
                    "d": p.direction,
                    "tp": float(p.target_price) if p.target_price is not None else None,
                    "ep": float(p.current_price) if p.current_price is not None else None,
                    "exp": p.expiry,
                    "conf": float(p.confidence),
                    "em": float(p.expected_move_pct) if p.expected_move_pct is not None else None,
                    "ss": float(p.signal_strength) if p.signal_strength is not None else None,
                    "coh": float(p.coherence) if p.coherence is not None else None,
                    "mn": p.model_name, "mv": p.model_version,
                    "sig": json.dumps([asdict(s) for s in p.signals], default=str),
                    "anti": json.dumps([asdict(a) for a in p.anti_signals], default=str),
                    "fc": json.dumps(p.flow_context, default=str),
                    "mw": json.dumps(p.model_weights, default=str),
                })

    def _get_leaderboard(self) -> list[dict]:
        """Get model performance leaderboard."""
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT name, weight, predictions_made, hits, misses, partials,
                       cumulative_pnl, sharpe
                FROM oracle_models ORDER BY weight DESC
            """)).fetchall()
        return [
            {
                "name": r[0], "weight": r[1], "total": r[2],
                "hits": r[3], "misses": r[4], "partials": r[5],
                "hit_rate": round(r[3] / r[2], 3) if r[2] > 0 else 0,
                "pnl": round(r[6], 2), "sharpe": round(r[7], 2),
            }
            for r in rows
        ]
