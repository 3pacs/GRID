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

HORIZON_BUCKETS: tuple[str, ...] = ("1d", "7d", "30d", "90d")


def _default_horizon_buckets() -> dict[str, dict[str, float]]:
    return {
        bucket: {
            "weight": 1.0,
            "hits": 0,
            "misses": 0,
            "partials": 0,
            "scored": 0,
            "brier": 0.0,
            "ece": 0.0,
        }
        for bucket in HORIZON_BUCKETS
    }


def _parse_horizon_buckets(raw: Any) -> dict[str, dict[str, float]]:
    if raw is None:
        return _default_horizon_buckets()
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return _default_horizon_buckets()
    if not isinstance(raw, dict):
        return _default_horizon_buckets()

    defaults = _default_horizon_buckets()
    parsed: dict[str, dict[str, float]] = {}
    for bucket_key in HORIZON_BUCKETS:
        stored = raw.get(bucket_key)
        if not isinstance(stored, dict):
            parsed[bucket_key] = defaults[bucket_key]
            continue
        merged = dict(defaults[bucket_key])
        for field_name in merged:
            if field_name not in stored:
                continue
            try:
                if field_name in {"weight", "brier", "ece"}:
                    merged[field_name] = float(stored[field_name])
                else:
                    merged[field_name] = int(stored[field_name])
            except (TypeError, ValueError):
                pass
        parsed[bucket_key] = merged
    return parsed


def _horizon_key(horizon: int | str | None, *, default: str = "7d") -> str:
    if horizon is None:
        return default
    if isinstance(horizon, str):
        candidate = horizon.strip().lower()
        if candidate in HORIZON_BUCKETS:
            return candidate
        if candidate.endswith("d") and candidate[:-1].isdigit():
            horizon = int(candidate[:-1])
        else:
            return default
    try:
        days = int(horizon)
    except (TypeError, ValueError):
        return default
    canonical = {1: "1d", 7: "7d", 30: "30d", 90: "90d"}
    if days in canonical:
        return canonical[days]
    nearest = min(canonical.keys(), key=lambda d: (abs(d - days), d))
    return canonical[nearest]


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
    horizon_buckets: dict[str, dict[str, float]] = field(
        default_factory=_default_horizon_buckets
    )

    def bucket_weight(self, horizon: int | str | None) -> float:
        bucket = self.horizon_buckets.get(_horizon_key(horizon), {})
        weight = float(bucket.get("weight", 0.0) or 0.0)
        return weight if weight > 0.0 else float(self.weight)

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
    OracleModel(
        name="holder_overlap",
        version="1.0",
        description="Institutional holder deal overlap.",
        signal_families=["insider", "flows"],
    ),
    OracleModel(
        name="fundamental",
        version="1.0",
        description="Fundamental-vs-price divergence.",
        signal_families=["macro", "equity"],
    ),
    OracleModel(
        name="contagion",
        version="1.0",
        description="Supply-chain shock propagation.",
        signal_families=["supply", "macro", "equity"],
    ),
]


class ModelRegistry:
    """Contract-driven oracle model weight updater."""

    _VERDICT_ADJ = {"HIT": 1.0, "PARTIAL": 0.5, "MISS": 0.0}
    _VERDICT_COL = {"HIT": "hits", "MISS": "misses", "PARTIAL": "partials"}
    _LR = 0.05
    _MIN_WEIGHT = 0.1
    _MAX_WEIGHT = 5.0

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine

    def update_from_contract(self, evt: Any) -> int:
        verdict = getattr(evt, "verdict", None)
        if verdict not in self._VERDICT_ADJ:
            return 0
        try:
            weights_at_prediction = dict(
                getattr(evt, "model_weights_at_prediction", None) or {}
            )
        except TypeError:
            return 0
        if not weights_at_prediction:
            return 0

        updated = 0
        bucket_key = _horizon_key(getattr(evt, "horizon", None))
        raw_regime = getattr(evt, "regime", None)
        regime_key = None
        if raw_regime is not None:
            from oracle.regime_router import _canonical_regime

            regime_key = _canonical_regime(raw_regime)
        try:
            conn_ctx = self.engine.begin()
        except Exception as exc:
            log.error(
                "ModelRegistry.update_from_contract: engine.begin() failed: {e}",
                e=str(exc),
            )
            return 0

        try:
            with conn_ctx as conn:
                for model_id, prior_weight in weights_at_prediction.items():
                    try:
                        prior = float(prior_weight)
                    except (TypeError, ValueError):
                        prior = 1.0
                    _, touched = self._nudge_single_model(
                        conn=conn,
                        model_id=str(model_id),
                        prior_weight=prior,
                        verdict_adj=self._VERDICT_ADJ[verdict],
                        verdict_col=self._VERDICT_COL[verdict],
                        bucket_key=bucket_key,
                        regime_key=regime_key,
                    )
                    if touched:
                        updated += 1
        except Exception as exc:
            log.error(
                "ModelRegistry.update_from_contract: tx failed verdict={v}: {e}",
                v=verdict,
                e=str(exc),
            )
            return 0
        return updated

    def _nudge_single_model(
        self,
        *,
        conn: Any,
        model_id: str,
        prior_weight: float,
        verdict_adj: float,
        verdict_col: str,
        bucket_key: str,
        regime_key: str | None = None,
    ) -> tuple[float, bool]:
        if verdict_col not in {"hits", "misses", "partials"}:
            return prior_weight, False

        try:
            row = conn.execute(
                text(
                    "SELECT weight, hits, partials, misses, predictions_made, "
                    "horizon_buckets, regime_buckets "
                    "FROM oracle_models WHERE name = :name"
                ),
                {"name": model_id},
            ).fetchone()
        except Exception:
            row = None

        if row is None:
            target = 0.5 + verdict_adj * 2.0
            new_weight = prior_weight + self._LR * (target - prior_weight)
            new_weight = max(self._MIN_WEIGHT, min(self._MAX_WEIGHT, new_weight))
            conn.execute(
                text(
                    "UPDATE oracle_models "
                    "SET weight = :w, "
                    "    " + verdict_col + " = " + verdict_col + " + 1, "
                    "    predictions_made = predictions_made + 1, "
                    "    last_updated = NOW() "
                    "WHERE name = :name"
                ),
                {"w": round(new_weight, 6), "name": model_id},
            )
            return new_weight, True

        try:
            db_weight = float(row[0]) if row[0] is not None else prior_weight
            hits = int(row[1] or 0)
            partials = int(row[2] or 0)
            misses = int(row[3] or 0)
            buckets_raw = row[5] if len(row) > 5 else None
            regime_buckets_raw = row[6] if len(row) > 6 else None
        except (TypeError, ValueError, IndexError):
            return prior_weight, False

        if verdict_col == "hits":
            hits += 1
        elif verdict_col == "partials":
            partials += 1
        else:
            misses += 1

        total = hits + partials + misses
        adj_rate = (hits + partials * 0.5) / total if total > 0 else 0.0
        target = 0.5 + adj_rate * 2.0
        new_weight = db_weight + self._LR * (target - db_weight)
        new_weight = max(self._MIN_WEIGHT, min(self._MAX_WEIGHT, new_weight))

        parsed_buckets = _parse_horizon_buckets(buckets_raw)
        target_bucket = dict(parsed_buckets.get(bucket_key, {}))
        bucket_weight = float(target_bucket.get("weight", 1.0) or 1.0)
        bucket_hits = int(target_bucket.get("hits", 0) or 0)
        bucket_partials = int(target_bucket.get("partials", 0) or 0)
        bucket_misses = int(target_bucket.get("misses", 0) or 0)
        bucket_scored = int(target_bucket.get("scored", 0) or 0)

        if verdict_col == "hits":
            bucket_hits += 1
        elif verdict_col == "partials":
            bucket_partials += 1
        else:
            bucket_misses += 1
        bucket_scored += 1
        bucket_total = bucket_hits + bucket_partials + bucket_misses
        bucket_adj = (
            (bucket_hits + bucket_partials * 0.5) / bucket_total
            if bucket_total > 0
            else 0.0
        )
        bucket_target = 0.5 + bucket_adj * 2.0
        bucket_new = bucket_weight + self._LR * (bucket_target - bucket_weight)
        bucket_new = max(self._MIN_WEIGHT, min(self._MAX_WEIGHT, bucket_new))

        target_bucket.update(
            {
                "weight": round(bucket_new, 6),
                "hits": bucket_hits,
                "misses": bucket_misses,
                "partials": bucket_partials,
                "scored": bucket_scored,
            }
        )
        parsed_buckets[bucket_key] = target_bucket
        bucket_weights = [
            float(parsed_buckets[key].get("weight", 1.0) or 1.0)
            for key in HORIZON_BUCKETS
        ]

        regime_target_bucket = None
        if regime_key is not None:
            from oracle.regime_router import parse_regime_buckets

            parsed_regimes = parse_regime_buckets(regime_buckets_raw)
            regime_target_bucket = dict(parsed_regimes.get(regime_key, {}))
            regime_weight = float(regime_target_bucket.get("weight", 1.0) or 1.0)
            regime_hits = int(regime_target_bucket.get("hits", 0) or 0)
            regime_partials = int(regime_target_bucket.get("partials", 0) or 0)
            regime_misses = int(regime_target_bucket.get("misses", 0) or 0)
            regime_scored = int(regime_target_bucket.get("scored", 0) or 0)

            if verdict_col == "hits":
                regime_hits += 1
            elif verdict_col == "partials":
                regime_partials += 1
            else:
                regime_misses += 1
            regime_scored += 1
            regime_total = regime_hits + regime_partials + regime_misses
            regime_adj = (
                (regime_hits + regime_partials * 0.5) / regime_total
                if regime_total > 0
                else 0.0
            )
            regime_target = 0.5 + regime_adj * 2.0
            regime_new = regime_weight + self._LR * (regime_target - regime_weight)
            regime_new = max(self._MIN_WEIGHT, min(self._MAX_WEIGHT, regime_new))
            regime_target_bucket.update(
                {
                    "weight": round(regime_new, 6),
                    "hits": regime_hits,
                    "misses": regime_misses,
                    "partials": regime_partials,
                    "scored": regime_scored,
                }
            )
            bucket_weights.append(float(regime_target_bucket["weight"]))

        aggregate_weight = sum(bucket_weights) / len(bucket_weights)
        aggregate_weight = max(
            self._MIN_WEIGHT,
            min(self._MAX_WEIGHT, aggregate_weight),
        )

        conn.execute(
            text(
                "UPDATE oracle_models "
                "SET horizon_buckets = jsonb_set("
                "    COALESCE(horizon_buckets, '{}'::jsonb), "
                "    :path, CAST(:bucket AS JSONB), true) "
                "WHERE name = :name"
            ),
            {
                "path": "{" + bucket_key + "}",
                "bucket": json.dumps(target_bucket),
                "name": model_id,
            },
        )
        if regime_key is not None and regime_target_bucket is not None:
            conn.execute(
                text(
                    "UPDATE oracle_models "
                    "SET regime_buckets = jsonb_set("
                    "    COALESCE(regime_buckets, '{}'::jsonb), "
                    "    :path, CAST(:bucket AS JSONB), true) "
                    "WHERE name = :name"
                ),
                {
                    "path": "{" + regime_key + "}",
                    "bucket": json.dumps(regime_target_bucket),
                    "name": model_id,
                },
            )
        conn.execute(
            text(
                "UPDATE oracle_models "
                "SET weight = :w, "
                "    " + verdict_col + " = " + verdict_col + " + 1, "
                "    predictions_made = predictions_made + 1, "
                "    last_updated = NOW() "
                "WHERE name = :name"
            ),
            {"w": round(aggregate_weight, 6), "name": model_id},
        )
        return aggregate_weight, True

    def decay_model_by_source(self, source: str, factor: float) -> int:
        if not source or factor <= 0:
            return 0
        with self.engine.begin() as conn:
            result = conn.execute(
                text(
                    "UPDATE oracle_models "
                    "SET weight = GREATEST(:min_w, weight * :f), "
                    "    last_updated = NOW() "
                    "WHERE (signal_families)::text LIKE :needle"
                ),
                {
                    "f": float(factor),
                    "min_w": self._MIN_WEIGHT,
                    "needle": f"%{source}%",
                },
            )
            return result.rowcount or 0


# ── Oracle Engine ───────────────────────────────────────────────────────────

class OracleEngine:
    """The self-improving prediction engine."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self._ensure_tables()
        self.models = self._load_models()
        self._last_guard_verdicts: list = []
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
        self,
        signals: list[Signal],
        direction: str,
        ticker: str | None = None,
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

        if ticker:
            anti.extend(self._cross_lens_anti_signals(ticker, direction))
            anti.extend(self._regulatory_anti_signals(ticker))

        return sorted(anti, key=lambda a: -a.severity)

    def _cross_lens_anti_signals(
        self,
        ticker: str,
        direction: str,
    ) -> list[AntiSignal]:
        """Surface confirmed upstream supply shocks against bullish calls."""
        if direction not in ("CALL", "LONG"):
            return []

        sql = text(
            """
            SELECT upstream_id, shock_date, shock_magnitude,
                   downstream_move_pct, correlation, confidence, evidence
            FROM supply_shock_attributions
            WHERE downstream_id = ANY(:keys)
              AND confidence IN ('derived', 'confirmed')
              AND shock_date >= CURRENT_DATE - INTERVAL '45 days'
            ORDER BY shock_date DESC
            LIMIT 5
            """
        )
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    sql.bindparams(keys=[ticker.upper(), ticker.lower()])
                ).fetchall()
        except Exception as exc:
            log.debug(
                "oracle._cross_lens_anti_signals({t}): {e}",
                t=ticker,
                e=str(exc),
            )
            return []

        out: list[AntiSignal] = []
        for row in rows:
            upstream, shock_date, shock_mag, _dmove, corr, conf, _evidence = row
            corr_val = float(corr or 0.0)
            severity = min(1.0, abs(corr_val))
            if severity < 0.2:
                continue
            out.append(
                AntiSignal(
                    name="cross_lens_supply_shock",
                    family="supply",
                    value=float(shock_mag or 0.0),
                    z_score=corr_val,
                    contradiction=(
                        f"cross_lens confirmed upstream shock {upstream!r} "
                        f"on {shock_date} (corr={corr_val:+.2f}, "
                        f"confidence={conf!r}) drags {ticker} downstream "
                        f"against predicted {direction}."
                    ),
                    severity=severity,
                )
            )
        return out

    _REG_SEVERITY_MAP = {
        "low": 0.2,
        "medium": 0.4,
        "high": 0.7,
        "critical": 1.0,
    }

    def _regulatory_anti_signals(self, ticker: str) -> list[AntiSignal]:
        """Surface recent high/critical regulatory threats."""
        sql = text(
            """
            SELECT regulator, action_type, event_date, severity, title, url
            FROM regulatory_events
            WHERE :ticker = ANY(affected_tickers)
              AND severity IN ('high', 'critical')
              AND event_date >= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY event_date DESC
            LIMIT 10
            """
        )
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(sql.bindparams(ticker=ticker.upper())).fetchall()
        except Exception as exc:
            log.debug(
                "oracle._regulatory_anti_signals({t}): {e}",
                t=ticker,
                e=str(exc),
            )
            return []

        out: list[AntiSignal] = []
        for row in rows:
            regulator, action_type, event_date, severity, title, url = row
            mapped = self._REG_SEVERITY_MAP.get(str(severity or "").lower(), 0.5)
            out.append(
                AntiSignal(
                    name="regulatory_threat",
                    family="regulatory",
                    value=mapped,
                    z_score=0.0,
                    contradiction=(
                        f"{str(regulator).upper()} {action_type} "
                        f"({severity}) on {event_date}: {title or url or 'n/a'}"
                    ),
                    severity=mapped,
                )
            )
        return out

    def _get_sector_health_routing(self, sector: str) -> dict[str, float]:
        """Return family multipliers from latest sector health score."""
        if not sector:
            return {}
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT score, snapshot_date "
                        "FROM sector_health_snapshots "
                        "WHERE sector_name = :s "
                        "ORDER BY snapshot_date DESC LIMIT 1"
                    ).bindparams(s=sector),
                ).fetchone()
        except Exception as exc:
            log.debug(
                "sector_health routing failed for {s}: {e}",
                s=sector,
                e=str(exc),
            )
            return {}
        if not row or row[0] is None:
            return {}
        try:
            score = float(row[0])
        except (TypeError, ValueError):
            return {}

        norm = max(-1.0, min(1.0, (score - 50.0) / 50.0))
        if abs(norm) < 0.2:
            return {}
        scale = 0.25 * norm
        return {
            "equity": 1.0 + scale,
            "flows": 1.0 + scale,
            "vol": 1.0 - scale,
            "credit": 1.0 - scale,
        }

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
        signal_cache: dict[tuple[str, tuple[str, ...]], list[Signal]] = {}
        now = datetime.now(timezone.utc)

        total_tickers = len(tickers)
        for idx, ticker in enumerate(tickers, start=1):
            ticker_started = datetime.now(timezone.utc)
            log.info("Oracle generating {idx}/{total}: {ticker}", idx=idx, total=total_tickers, ticker=ticker)
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
                        family_key = tuple(sorted(str(f) for f in model.signal_families))
                        cache_key = (ticker, family_key)
                        if cache_key not in signal_cache:
                            signal_cache[cache_key] = self._gather_signals(ticker, model.signal_families)
                        signals = [replace(sig) for sig in signal_cache[cache_key]]

                    # ── Actor intelligence injection ──────────────────
                    # Enrich signals with actor trust/influence from the
                    # actor graph. Actors with proven track records get
                    # their signals amplified; unknown actors stay at 0.5.
                    try:
                        from intelligence.actor_signal_bridge import (
                            get_actor_signals_for_ticker,
                            get_actor_trust_weights,
                        )
                        actor_sigs = get_actor_signals_for_ticker(self.engine, ticker, days=30)
                        if actor_sigs:
                            # Build actor trust lookup
                            actor_trust_by_type: dict[str, float] = {}
                            for asig in actor_sigs:
                                st = asig["signal_type"]
                                trust = asig["actor_trust"] * asig["actor_influence"]
                                if st not in actor_trust_by_type or trust > actor_trust_by_type[st]:
                                    actor_trust_by_type[st] = trust

                            # Boost signal weights by actor credibility
                            for sig in signals:
                                src = getattr(sig, "source_module", "") or sig.family
                                actor_boost = actor_trust_by_type.get(src, None)
                                if actor_boost and actor_boost > 0.5:
                                    # Scale weight: 0.5 trust = 1x, 0.9 trust = 1.8x
                                    boost = 0.5 + actor_boost
                                    if hasattr(sig, '_replace'):
                                        sig = sig._replace(weight=sig.weight * boost)
                                    elif hasattr(sig, 'weight'):
                                        sig.weight = sig.weight * boost

                            # Inject high-influence actor signals directly
                            for asig in actor_sigs[:5]:  # Top 5 by influence
                                if asig["actor_influence"] > 0.7:
                                    signals.append(Signal(
                                        name=f"actor:{asig['actor']}",
                                        family="actor_intelligence",
                                        value=asig["actor_trust"],
                                        z_score=1.5 if asig["direction"] in ("buy", "bullish", "long") else -1.5,
                                        direction="bullish" if asig["direction"] in ("buy", "bullish", "long") else "bearish",
                                        weight=asig["actor_trust"] * asig["actor_influence"],
                                        freshness_hours=0,
                                    ))
                    except Exception as exc:
                        log.debug("Actor signal enrichment skipped for {t}: {e}", t=ticker, e=str(exc))

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

                    # Clamp expected move to ±30%
                    if expected_move < -30.0 or expected_move > 30.0:
                        log.debug("Clamping expected_move from {orig} to ±30% for {t}",
                                   orig=expected_move, t=ticker)
                        expected_move = max(-30.0, min(30.0, expected_move))

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
            elapsed = (datetime.now(timezone.utc) - ticker_started).total_seconds()
            log.info(
                "Oracle ticker complete {idx}/{total}: {ticker} in {seconds:.1f}s; cumulative predictions={n}",
                idx=idx,
                total=total_tickers,
                ticker=ticker,
                seconds=elapsed,
                n=len(all_predictions),
            )

        # Sort by confidence
        all_predictions.sort(key=lambda p: -p.confidence)

        # ── Hallucination Guard ─────────────────────────────────────
        # Run all predictions through the Feynman-inspired verification
        # layer BEFORE storage. Adjusts confidence downward for
        # hallucination signatures (stale signals, contradictions,
        # incoherent directions, uncalibrated models, mono-source, etc.)
        try:
            from oracle.hallucination_guard import verify_predictions

            model_stats = {
                m.name: {
                    "hits": m.hits,
                    "misses": m.misses,
                    "partials": m.partials,
                }
                for m in self.models
            }

            # Optionally pull calibration report
            cal_report = None
            try:
                from oracle.calibration import compute_calibration
                cal_report = compute_calibration(self)
            except Exception:
                pass  # Calibration data may not exist yet

            all_predictions, self._last_guard_verdicts = verify_predictions(
                all_predictions,
                calibration_report=cal_report,
                model_stats=model_stats,
            )

            # Re-sort after confidence adjustments
            all_predictions.sort(key=lambda p: -p.confidence)
        except Exception as exc:
            log.debug("Hallucination guard skipped: {e}", e=str(exc))
            self._last_guard_verdicts = []

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

    def evolve_weights(self, *, event_driven: bool = True) -> dict[str, Any]:
        """Adjust model weights based on track record.

        Models that hit more get higher weight. Models that miss decay.
        Minimum weight floor prevents complete abandonment (they might
        work in different regimes).
        """
        if event_driven:
            return self._reconcile_event_driven_weights()

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

    def _reconcile_event_driven_weights(self) -> dict[str, Any]:
        """Audit event-driven counters without mutating scalar weights."""
        drift: dict[str, Any] = {}
        bucket_drift: dict[str, Any] = {}
        rows = []

        with self.engine.begin() as conn:
            try:
                rows = conn.execute(
                    text(
                        "SELECT name, weight, hits, partials, misses, "
                        "predictions_made, predictions_made, horizon_buckets "
                        "FROM oracle_models"
                    )
                ).fetchall()
            except Exception:
                rows = []

        for row in rows:
            try:
                (
                    name,
                    weight,
                    hits,
                    partials,
                    misses,
                    predictions_made,
                    scored_count,
                    horizon_buckets,
                ) = row[:8]
            except Exception:
                continue

            batch_total = int(hits or 0) + int(partials or 0) + int(misses or 0)
            event_total = int(scored_count or 0)
            if batch_total != event_total:
                drift[str(name)] = {
                    "batch_total": batch_total,
                    "event_total": event_total,
                    "delta": event_total - batch_total,
                }

            parsed = _parse_horizon_buckets(horizon_buckets)
            legacy_weight = float(weight or 1.0)
            for bucket_key, bucket in parsed.items():
                try:
                    bucket_weight = float(bucket.get("weight", 1.0) or 1.0)
                except (TypeError, ValueError):
                    continue
                if legacy_weight == 0:
                    continue
                delta_pct = abs(bucket_weight - legacy_weight) / abs(legacy_weight) * 100
                if delta_pct > 2.0:
                    bucket_drift.setdefault(str(name), {})[bucket_key] = {
                        "legacy_weight": round(legacy_weight, 6),
                        "bucket_weight": round(bucket_weight, 6),
                        "delta_pct": round(delta_pct, 4),
                    }

        return {
            "mode": "event_driven",
            "changes": {},
            "drift": drift,
            "bucket_drift": bucket_drift,
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

        # 2.6 Run trace-based evolver (targeted mutations from postmortem traces)
        trace_evolve_result = {}
        try:
            from oracle.trace_evolver import TraceEvolver
            trace_evolver = TraceEvolver(self.engine)
            trace_evolve_result = trace_evolver.evolve_cycle()
            log.info("Trace evolver: {a} applied, {p} patterns",
                     a=len(trace_evolve_result.get("mutations_applied", [])),
                     p=len(trace_evolve_result.get("patterns_found", [])))
        except Exception as exc:
            log.debug("Trace evolver failed: {e}", e=str(exc))

        # 3. Generate new predictions
        predictions = self.generate_predictions(tickers)

        # 4. Get model leaderboard
        leaderboard = self._get_leaderboard()

        # Aggregate hallucination guard verdicts if available
        guard_result = {}
        try:
            from oracle.hallucination_guard import guard_summary
            verdicts = getattr(self, "_last_guard_verdicts", [])
            if verdicts:
                guard_result = guard_summary(verdicts)
        except Exception:
            pass

        result = {
            "scoring": score_result,
            "evolution": evolve_result,
            "trace_evolution": trace_evolve_result,
            "new_predictions": len(predictions),
            "top_predictions": [p.to_dict() for p in predictions[:10]],
            "leaderboard": leaderboard,
            "hallucination_guard": guard_result,
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
            # Prefer adj_close (accounts for splits/dividends), fall back to close
            for suffix in ("adj_close", "close"):
                row = conn.execute(text("""
                    SELECT value FROM raw_series
                    WHERE series_id = :sid AND pull_status = 'SUCCESS'
                    ORDER BY obs_date DESC LIMIT 1
                """), {"sid": f"YF:{ticker}:{suffix}"}).fetchone()
                if row:
                    return float(row[0])
            return None

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
            for sid in [f"YF:{ticker}:adj_close", f"YF:{ticker}:close",
                       f"YF:{ticker}-USD:adj_close", f"YF:{ticker}-USD:close"]:
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
        """Store predictions to the journal.

        Every row's ``signals`` JSONB payload is enriched with the 4 context
        keys required by the 11-layer conviction stack: ``regime``,
        ``fci_regime``, ``vix_level``, and ``signal_contributions``. Enrichment
        never raises — missing upstream features fall back to safe defaults
        (see ``oracle.prediction_context``).
        """
        from oracle.prediction_context import (
            build_prediction_context,
            enrich_signals_payload,
        )

        with self.engine.begin() as conn:
            for p in predictions:
                as_of_date = (
                    p.timestamp.date() if isinstance(p.timestamp, datetime) else date.today()
                )

                try:
                    model_votes = {
                        s.name: float(s.weight)
                        for s in p.signals
                        if getattr(s, "weight", None) is not None
                    }
                except Exception:
                    model_votes = {}

                try:
                    context = build_prediction_context(
                        self.engine,
                        as_of=as_of_date,
                        model_weights=p.model_weights,
                        model_votes=model_votes,
                    )
                except Exception as exc:
                    log.warning(
                        "prediction context enrichment failed for {tid}: {e}",
                        tid=p.id,
                        e=str(exc),
                    )
                    context = {
                        "regime": "NEUTRAL",
                        "fci_regime": "NEUTRAL",
                        "vix_level": None,
                        "signal_contributions": {},
                    }

                raw_signals = [asdict(s) for s in p.signals]
                enriched_signals = enrich_signals_payload(raw_signals, context)

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
                    "sig": json.dumps(enriched_signals, default=str),
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


# ── Lightweight Ensemble API ───────────────────────────────────────────────

HORIZON_BUCKETS: tuple[str, ...] = ("1d", "7d", "30d", "90d")


def _default_horizon_buckets() -> dict[str, dict[str, float]]:
    return {
        bucket: {
            "weight": 1.0,
            "hits": 0,
            "misses": 0,
            "partials": 0,
            "scored": 0,
            "brier": 0.0,
            "ece": 0.0,
        }
        for bucket in HORIZON_BUCKETS
    }


def _parse_horizon_buckets(raw: Any) -> dict[str, dict[str, float]]:
    if raw is None:
        return _default_horizon_buckets()
    if isinstance(raw, str):
        try:
            raw = json.loads(raw)
        except (TypeError, ValueError):
            return _default_horizon_buckets()
    if not isinstance(raw, dict):
        return _default_horizon_buckets()

    defaults = _default_horizon_buckets()
    parsed: dict[str, dict[str, float]] = {}
    for bucket_key in HORIZON_BUCKETS:
        stored = raw.get(bucket_key)
        if not isinstance(stored, dict):
            parsed[bucket_key] = defaults[bucket_key]
            continue
        merged = dict(defaults[bucket_key])
        for field_name in merged:
            if field_name not in stored:
                continue
            try:
                if field_name in {"weight", "brier", "ece"}:
                    merged[field_name] = float(stored[field_name])
                else:
                    merged[field_name] = int(stored[field_name])
            except (TypeError, ValueError):
                pass
        parsed[bucket_key] = merged
    return parsed


def _horizon_key(horizon: int | str | None, *, default: str = "7d") -> str:
    if horizon is None:
        return default
    if isinstance(horizon, str):
        candidate = horizon.strip().lower()
        if candidate in HORIZON_BUCKETS:
            return candidate
        if candidate.endswith("d") and candidate[:-1].isdigit():
            horizon = int(candidate[:-1])
        else:
            return default
    try:
        days = int(horizon)
    except (TypeError, ValueError):
        return default
    canonical = {1: "1d", 7: "7d", 30: "30d", 90: "90d"}
    if days in canonical:
        return canonical[days]
    nearest = min(canonical.keys(), key=lambda d: (abs(d - days), d))
    return canonical[nearest]


@dataclass(frozen=True)
class EnsemblePrediction:
    ticker: str
    direction: str
    score: int
    confidence: float
    strength: float
    coherence: float
    model_count: int
    level: str
    model_votes: list[dict[str, Any]]
    as_of: datetime | None
    horizon: int = 7
    catalyst_proximity: float = 0.0
    catalyst_type: str | None = None
    disagreement_score: float = 0.0
    directional_entropy: float = 0.0
    liquidity_state: str = ""
    liquidity_level_percentile: float = 50.0
    confidence_lower: float = 0.0
    confidence_upper: float = 0.0
    regime: str = ""
    regime_router_weights: dict[str, float] = field(default_factory=dict)
    fci_score: float = 0.0
    fci_regime: str = ""
    fragility_multiplier: float = 1.0
    shapley_top_contributor: str = ""
    shapley_top_share: float = 0.0
    crowdedness_score: float = 0.0
    crowd_direction: str = ""
    crowd_aligned: bool = False
    market_implied_prob: float = 0.0
    market_divergence_severity: str = ""


class EnsemblePredictor:
    """Meta-predictor that combines active oracle model heads."""

    def __init__(self, engine: Engine):
        from oracle.model_factory import ModelFactory
        from oracle.signal_aggregator import SignalAggregator

        self.engine = engine
        self.factory = ModelFactory(engine)
        self.aggregator = SignalAggregator()

    def predict(
        self,
        ticker: str,
        as_of: datetime | None = None,
        regime: str | None = None,
        *,
        horizon: int = 7,
    ) -> EnsemblePrediction:
        if as_of is None:
            as_of = datetime.now(timezone.utc)

        regime_obj: Any = None
        regime_state_for_routing = (regime or "NEUTRAL").upper()
        try:
            from intelligence.liquidity_regime import classify_current_regime

            regime_obj = classify_current_regime(self.engine)
            raw_state = getattr(regime_obj, "state", None)
            if raw_state:
                regime_state_for_routing = str(raw_state).upper()
        except Exception as exc:
            log.debug(
                "predict: liquidity_regime classify failed for {t}: {e}",
                t=ticker,
                e=str(exc),
            )

        try:
            from oracle.regime_router import RegimeRouter

            router = RegimeRouter(self.engine)
        except Exception:
            router = None

        votes: list[dict[str, Any]] = []
        regime_router_weights: dict[str, float] = {}

        for model in self.factory.list_active_models():
            try:
                signals = self.factory.get_signals_for_model(model.name, as_of)
                if len(signals) < getattr(model, "min_signals", 0):
                    continue
                agg = self.aggregator.aggregate(
                    signals,
                    getattr(model, "weight_config", None),
                    as_of,
                )
                hit_rate = self._get_hit_rate(model.name, horizon=horizon)
                bucket_weight = self._get_bucket_weight(model.name, horizon=horizon)
                regime_weight = 1.0
                if router is not None:
                    try:
                        regime_weight = float(
                            router.model_regime_weight(
                                model.name,
                                regime_state_for_routing,
                            )
                        )
                    except Exception:
                        regime_weight = 1.0
                regime_router_weights[model.name] = round(regime_weight, 4)
                votes.append(
                    {
                        "model_name": model.name,
                        "direction": agg.direction,
                        "strength": agg.strength,
                        "confidence": agg.confidence,
                        "coherence": agg.coherence,
                        "signal_count": agg.signal_count,
                        "hit_rate": hit_rate,
                        "horizon": horizon,
                        "bucket_weight": bucket_weight,
                        "regime": regime_state_for_routing,
                        "regime_weight": round(regime_weight, 4),
                        "vote_weight": round(
                            hit_rate
                            * float(agg.confidence)
                            * bucket_weight
                            * regime_weight,
                            4,
                        ),
                    }
                )
            except Exception as exc:
                log.debug("Ensemble: {m} failed: {e}", m=model.name, e=str(exc))

        if not votes:
            return EnsemblePrediction(
                ticker=ticker,
                direction="neutral",
                score=50,
                confidence=0.0,
                strength=0.0,
                coherence=0.0,
                model_count=0,
                level="meta",
                model_votes=[],
                as_of=as_of,
                horizon=horizon,
                regime=regime_state_for_routing if regime_obj is not None else "",
                regime_router_weights=regime_router_weights,
            )

        total_weight = sum(float(v["vote_weight"]) for v in votes) or 1.0
        bullish_weight = sum(
            float(v["vote_weight"]) for v in votes if v["direction"] == "bullish"
        )
        bearish_weight = sum(
            float(v["vote_weight"]) for v in votes if v["direction"] == "bearish"
        )

        if bullish_weight > bearish_weight:
            direction = "bullish"
        elif bearish_weight > bullish_weight:
            direction = "bearish"
        else:
            direction = "neutral"

        strength = round(abs(bullish_weight - bearish_weight) / total_weight, 4)
        confidence = round(
            sum(float(v["vote_weight"]) * float(v["confidence"]) for v in votes)
            / total_weight,
            4,
        )

        directional_votes = [v for v in votes if v["direction"] != "neutral"]
        if directional_votes:
            coherence = round(
                max(
                    sum(1 for v in directional_votes if v["direction"] == "bullish"),
                    sum(1 for v in directional_votes if v["direction"] == "bearish"),
                )
                / len(directional_votes),
                4,
            )
        else:
            coherence = 0.0

        catalyst_proximity = 0.0
        catalyst_type: str | None = None
        try:
            from intelligence.catalyst_aggregator import proximity_score

            catalyst = proximity_score(
                self.engine,
                ticker,
                as_of=as_of.date(),
                horizon_days=horizon,
            )
            catalyst_proximity = float(catalyst.get("score") or 0.0)
            catalyst_type = catalyst.get("catalyst_type")
        except Exception as exc:
            log.debug(
                "catalyst_aggregator unavailable for {t}: {e}",
                t=ticker,
                e=str(exc),
            )
        if catalyst_proximity > 0:
            confidence = round(confidence * (1.0 - 0.5 * catalyst_proximity), 4)

        disagreement_score_val = 0.0
        directional_entropy_val = 0.0
        try:
            from oracle.disagreement import compute_metrics

            disagreement = compute_metrics(votes)
            disagreement_score_val = float(disagreement.disagreement_score)
            directional_entropy_val = float(disagreement.directional_entropy)
        except Exception as exc:
            log.debug("disagreement metrics failed for {t}: {e}", t=ticker, e=str(exc))
        if disagreement_score_val > 0:
            confidence = round(confidence * (1.0 - 0.4 * disagreement_score_val), 4)

        liquidity_state_val = ""
        liquidity_level_pct = 50.0
        if regime_obj is not None:
            try:
                from intelligence.liquidity_regime import apply_to_confidence

                liquidity_state_val = str(getattr(regime_obj, "state", ""))
                liquidity_level_pct = float(
                    getattr(regime_obj, "level_percentile", 50.0)
                )
                confidence = round(
                    apply_to_confidence(confidence, liquidity_state_val),
                    4,
                )
            except Exception as exc:
                log.debug(
                    "liquidity_regime dampen failed for {t}: {e}",
                    t=ticker,
                    e=str(exc),
                )

        fci_score_val = 0.0
        fci_regime_val = ""
        try:
            from intelligence.financial_conditions_index import compute_fci

            fci_result = compute_fci(self.engine)
            fci_score_val = float(fci_result.score)
            fci_regime_val = str(fci_result.regime)
            confidence = round(
                confidence * (1.0 + 0.05 * max(-3.0, min(3.0, fci_score_val))),
                4,
            )
        except Exception as exc:
            log.debug("FCI unavailable for {t}: {e}", t=ticker, e=str(exc))

        fragility_mult = 1.0
        shap_top = ""
        shap_top_share = 0.0
        try:
            from intelligence.shapley_attribution import attribute_votes

            attr = attribute_votes(votes)
            fragility_mult = float(attr.fragility_multiplier)
            shap_top = str(attr.top_contributor)
            shap_top_share = float(attr.top_share)
            confidence = round(confidence * fragility_mult, 4)
        except Exception as exc:
            log.debug("shapley unavailable for {t}: {e}", t=ticker, e=str(exc))

        crowd_score_val = 0.0
        crowd_dir = ""
        crowd_aligned_flag = False
        try:
            from intelligence.consensus_crowdedness import (
                compute_crowdedness,
                compute_penalty,
            )

            crowd = compute_crowdedness(self.engine, ticker)
            crowd_score_val = float(crowd.score)
            crowd_dir = crowd.crowd_direction or ""
            penalty = compute_penalty(crowd, direction)
            crowd_aligned_flag = bool(penalty.aligned)
            confidence = round(confidence * float(penalty.multiplier), 4)
        except Exception as exc:
            log.debug("crowdedness unavailable for {t}: {e}", t=ticker, e=str(exc))

        market_prob_val = 0.0
        market_sev = ""
        try:
            from intelligence.market_implied_prob import (
                compare_to_oracle,
                options_implied_probability,
            )

            target_move = 0.03 if direction == "bullish" else -0.03
            market = options_implied_probability(
                self.engine,
                ticker,
                target_move_pct=target_move,
                horizon_days=horizon,
            )
            if market is not None:
                market_prob_val = float(market.prob)
                div_report = compare_to_oracle(confidence, market.prob)
                market_sev = str(div_report.severity)
                confidence = round(
                    confidence * float(div_report.confidence_multiplier),
                    4,
                )
        except Exception as exc:
            log.debug(
                "market_implied_prob unavailable for {t}: {e}",
                t=ticker,
                e=str(exc),
            )

        confidence = max(0.0, min(1.0, confidence))

        conf_lower = confidence
        conf_upper = confidence
        try:
            from oracle.uncertainty import compute_confidence_interval

            interval = compute_confidence_interval(votes, confidence, alpha=0.10)
            conf_lower = round(interval.lower, 4)
            conf_upper = round(interval.upper, 4)
        except Exception as exc:
            log.debug("uncertainty CI failed for {t}: {e}", t=ticker, e=str(exc))

        raw_score = 50 + (bullish_weight - bearish_weight) / total_weight * 50 * confidence
        score = max(0, min(100, round(raw_score)))

        return EnsemblePrediction(
            ticker=ticker,
            direction=direction,
            score=score,
            confidence=confidence,
            strength=strength,
            coherence=coherence,
            model_count=len(votes),
            level="meta",
            model_votes=sorted(votes, key=lambda x: -x["vote_weight"])[:10],
            as_of=as_of,
            horizon=horizon,
            catalyst_proximity=catalyst_proximity,
            catalyst_type=catalyst_type,
            disagreement_score=disagreement_score_val,
            directional_entropy=directional_entropy_val,
            liquidity_state=liquidity_state_val,
            liquidity_level_percentile=liquidity_level_pct,
            confidence_lower=conf_lower,
            confidence_upper=conf_upper,
            regime=regime_state_for_routing if regime_obj is not None else "",
            regime_router_weights=regime_router_weights,
            fci_score=fci_score_val,
            fci_regime=fci_regime_val,
            fragility_multiplier=fragility_mult,
            shapley_top_contributor=shap_top,
            shapley_top_share=shap_top_share,
            crowdedness_score=crowd_score_val,
            crowd_direction=crowd_dir,
            crowd_aligned=crowd_aligned_flag,
            market_implied_prob=market_prob_val,
            market_divergence_severity=market_sev,
        )

    def predict_batch(
        self,
        tickers: list[str],
        as_of: datetime | None = None,
        *,
        horizon: int = 7,
    ) -> dict[str, EnsemblePrediction]:
        return {ticker: self.predict(ticker, as_of, horizon=horizon) for ticker in tickers}

    def score_ensemble(
        self,
        prediction: EnsemblePrediction,
        actual_direction: str,
    ) -> dict[str, Any]:
        return {
            "correct": prediction.direction == actual_direction,
            "predicted": prediction.direction,
            "actual": actual_direction,
            "confidence": prediction.confidence,
            "model_count": prediction.model_count,
            "attribution": [
                {
                    "model": vote["model_name"],
                    "voted": vote["direction"],
                    "correct": vote["direction"] == actual_direction,
                    "weight": vote["vote_weight"],
                }
                for vote in prediction.model_votes
            ],
        }

    def _get_hit_rate(
        self,
        model_name: str,
        *,
        horizon: int | str | None = None,
    ) -> float:
        try:
            with self.engine.connect() as conn:
                if horizon is not None:
                    try:
                        bucket_row = conn.execute(
                            text(
                                "SELECT horizon_buckets FROM oracle_models "
                                "WHERE name = :n"
                            ),
                            {"n": model_name},
                        ).fetchone()
                        if bucket_row and bucket_row[0] is not None:
                            parsed = _parse_horizon_buckets(bucket_row[0])
                            bucket = parsed.get(_horizon_key(horizon), {})
                            hits = int(bucket.get("hits", 0) or 0)
                            misses = int(bucket.get("misses", 0) or 0)
                            partials = int(bucket.get("partials", 0) or 0)
                            total = hits + misses + partials
                            if total >= 5:
                                return (hits + partials * 0.5) / total
                    except Exception as exc:
                        log.debug(
                            "_get_hit_rate bucket lookup failed {m}: {e}",
                            m=model_name,
                            e=str(exc),
                        )
                row = conn.execute(
                    text(
                        "SELECT hits, misses, partials "
                        "FROM oracle_models WHERE name=:n"
                    ),
                    {"n": model_name},
                ).fetchone()
            if not row:
                return 0.5
            hits = int(row[0] or 0)
            misses = int(row[1] or 0)
            partials = int(row[2] or 0)
            total = hits + misses + partials
            return (hits + partials * 0.5) / total if total >= 5 else 0.5
        except Exception as exc:
            log.warning("Hit rate lookup failed for {m}: {e}", m=model_name, e=str(exc))
            return 0.5

    def _get_bucket_weight(
        self,
        model_name: str,
        *,
        horizon: int | str | None = None,
    ) -> float:
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    text(
                        "SELECT horizon_buckets, weight "
                        "FROM oracle_models WHERE name = :n"
                    ),
                    {"n": model_name},
                ).fetchone()
        except Exception as exc:
            log.debug(
                "_get_bucket_weight SELECT failed {m}: {e}",
                m=model_name,
                e=str(exc),
            )
            return 1.0
        if not row:
            return 1.0
        legacy_weight = float(row[1] or 1.0) if len(row) > 1 else 1.0
        if horizon is None:
            return legacy_weight
        try:
            parsed = _parse_horizon_buckets(row[0])
            bucket = parsed.get(_horizon_key(horizon), {})
            bucket_weight = float(bucket.get("weight", 0.0) or 0.0)
            return bucket_weight if bucket_weight > 0.0 else legacy_weight
        except Exception:
            return legacy_weight
