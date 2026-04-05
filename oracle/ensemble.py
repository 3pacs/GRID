"""
GRID Oracle — Ensemble Predictor.

Composes N individual models into multi-level ensemble predictions.
Each model queries its signal subscriptions, aggregates independently,
then votes are combined weighted by accuracy x confidence.
"""

from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from oracle.model_factory import ModelFactory
from oracle.signal_aggregator import SignalAggregator


@dataclass(frozen=True)
class EnsemblePrediction:
    ticker: str
    direction: str
    score: int  # 0-100 conviction score (50=neutral, 100=max bullish, 0=max bearish)
    confidence: float
    strength: float
    coherence: float
    model_count: int
    level: str
    model_votes: list[dict[str, Any]]
    as_of: datetime


class EnsemblePredictor:
    def __init__(self, engine: Engine):
        self.engine = engine
        self.factory = ModelFactory(engine)
        self.aggregator = SignalAggregator()

    def predict(self, ticker: str, as_of: datetime = None, regime: str = None) -> EnsemblePrediction:
        if as_of is None:
            as_of = datetime.now(timezone.utc)

        models = self.factory.list_active_models()
        votes = []

        for model in models:
            try:
                signals = self.factory.get_signals_for_model(model.name, as_of)
                if len(signals) < model.min_signals:
                    continue
                agg = self.aggregator.aggregate(signals, model.weight_config, as_of)
                hr = self._get_hit_rate(model.name)
                votes.append({
                    "model_name": model.name,
                    "direction": agg.direction,
                    "strength": agg.strength,
                    "confidence": agg.confidence,
                    "coherence": agg.coherence,
                    "signal_count": agg.signal_count,
                    "hit_rate": hr,
                    "vote_weight": round(hr * agg.confidence, 4),
                })
            except Exception as exc:
                log.debug("Ensemble: {m} failed: {e}", m=model.name, e=str(exc))

        if not votes:
            return EnsemblePrediction(
                ticker=ticker, direction="neutral", score=50, confidence=0.0, strength=0.0,
                coherence=0.0, model_count=0, level="meta", model_votes=[], as_of=as_of,
            )

        tw = sum(v["vote_weight"] for v in votes) or 1.0
        bw = sum(v["vote_weight"] for v in votes if v["direction"] == "bullish")
        brw = sum(v["vote_weight"] for v in votes if v["direction"] == "bearish")

        direction = "bullish" if bw > brw else ("bearish" if brw > bw else "neutral")
        strength = round(abs(bw - brw) / tw, 4)
        confidence = round(sum(v["vote_weight"] * v["confidence"] for v in votes) / tw, 4)

        directional = [v for v in votes if v["direction"] != "neutral"]
        if directional:
            coherence = round(max(
                sum(1 for v in directional if v["direction"] == "bullish"),
                sum(1 for v in directional if v["direction"] == "bearish"),
            ) / len(directional), 4)
        else:
            coherence = 0.0

        # Score: 0-100 where 50=neutral, 100=max bullish, 0=max bearish
        # Based on weighted net direction * confidence
        raw_score = 50 + (bw - brw) / tw * 50 * confidence
        score = max(0, min(100, round(raw_score)))

        return EnsemblePrediction(
            ticker=ticker, direction=direction, score=score, confidence=confidence,
            strength=strength, coherence=coherence, model_count=len(votes),
            level="meta", model_votes=sorted(votes, key=lambda x: -x["vote_weight"])[:10],
            as_of=as_of,
        )

    def predict_batch(self, tickers: list[str], as_of: datetime = None) -> dict[str, EnsemblePrediction]:
        return {t: self.predict(t, as_of) for t in tickers}

    def score_ensemble(self, prediction: EnsemblePrediction, actual_direction: str) -> dict:
        return {
            "correct": prediction.direction == actual_direction,
            "predicted": prediction.direction,
            "actual": actual_direction,
            "confidence": prediction.confidence,
            "model_count": prediction.model_count,
            "attribution": [
                {"model": v["model_name"], "voted": v["direction"],
                 "correct": v["direction"] == actual_direction, "weight": v["vote_weight"]}
                for v in prediction.model_votes
            ],
        }

    def _get_hit_rate(self, model_name: str) -> float:
        try:
            with self.engine.connect() as conn:
                row = conn.execute(text(
                    "SELECT hits, misses, partials FROM oracle_models WHERE name=:n"
                ), {"n": model_name}).fetchone()
            if not row: return 0.5
            h, m, p = int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
            t = h + m + p
            return (h + p * 0.5) / t if t >= 5 else 0.5
        except Exception as e:
            log.warning("Hit rate lookup failed for {m}: {e}", m=model_name, e=str(e))
            return 0.5
