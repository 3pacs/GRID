"""
AstroGrid persistence helpers.

AstroGrid writes its derived state into the dedicated ``astrogrid`` schema.
Shared GRID tables remain upstream-only inputs.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from uuid import uuid4

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import settings
from oracle.astrogrid_universe import scoreable_universe_by_symbol

_DEFAULT_GRID_WEIGHTS = {
    "regime": 0.9,
    "thesis": 0.8,
    "scorecard": 0.85,
    "flows": 0.75,
    "signals": 0.7,
}

_DEFAULT_MYSTICAL_WEIGHTS = {
    "seer": 0.2,
    "lunar": 0.18,
    "nakshatra": 0.14,
    "aspects": 0.12,
}

_SWING_HIT_THRESHOLD = 0.04
_MACRO_HIT_THRESHOLD = 0.08
_SWING_PARTIAL_THRESHOLD = 0.02
_MACRO_PARTIAL_THRESHOLD = 0.04
_NEUTRAL_MOVE_BAND = 0.01

_UNIVERSE_BY_SYMBOL = scoreable_universe_by_symbol()
_HYBRID_LOOKUP_BY_SYMBOL = {
    symbol: str(item["lookup_ticker"])
    for symbol, item in _UNIVERSE_BY_SYMBOL.items()
}
_HYBRID_LOOKUP_BY_SYMBOL["GOOG"] = _HYBRID_LOOKUP_BY_SYMBOL["GOOGL"]
_PRICE_FEATURE_BY_SYMBOL = {
    symbol: str(item["price_feature"])
    for symbol, item in _UNIVERSE_BY_SYMBOL.items()
}
_PRICE_FEATURE_BY_SYMBOL["GOOG"] = _PRICE_FEATURE_BY_SYMBOL["GOOGL"]

_VALID_SCORING_CLASSES = {
    "liquid_market",
    "illiquid_real_asset",
    "macro_narrative",
    "unscored_experimental",
}


def _safe_json(data: Any) -> str:
    return json.dumps(data, default=str)


def _json_loads(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            return default
    return default


def _safe_schema_name(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""):
        raise ValueError(f"Invalid schema name: {value!r}")
    return value


def _snapshot_source_mode(source: str) -> str:
    parts = {part.strip() for part in str(source or "").split("+") if part.strip()}
    has_local = "analysis.ephemeris" in parts
    has_shared = bool(parts.intersection({"resolved_series", "regime_history", "raw_series"}))
    if has_local and has_shared:
        return "hybrid"
    if has_shared:
        return "grid"
    if has_local:
        return "local"
    return "archive"


def _snapshot_precision_label(source_mode: str) -> str:
    if source_mode == "grid":
        return "authoritative"
    if source_mode == "hybrid":
        return "mixed"
    if source_mode == "archive":
        return "approximate"
    return "derived"


def _stable_snapshot_ts(snapshot_date: date) -> datetime:
    return datetime.combine(snapshot_date, time(hour=12, minute=0, tzinfo=timezone.utc))


def _coerce_confidence(value: Any) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result < 0 or result > 1:
        return None
    return result


def _compact_text(value: Any, fallback: str = "") -> str:
    text = " ".join(str(value or fallback).split())
    return text[:500]


def _normalize_scoring_class(value: Any) -> str:
    raw = str(value or "liquid_market").strip().lower()
    return raw if raw in _VALID_SCORING_CLASSES else "liquid_market"


def _prediction_direction(value: Any) -> str:
    raw = str(value or "").lower()
    if any(token in raw for token in ("sell", "short", "hedge", "fade", "risk off", "bear")):
        return "bearish"
    if any(token in raw for token in ("buy", "long", "press", "accumulate", "risk on", "bull")):
        return "bullish"
    return "neutral"


def _direction_sign(label: str) -> int:
    if label == "bullish":
        return 1
    if label == "bearish":
        return -1
    return 0


def _horizon_thresholds(horizon_label: str | None) -> tuple[float, float]:
    if str(horizon_label or "").lower() == "macro":
        return _MACRO_HIT_THRESHOLD, _MACRO_PARTIAL_THRESHOLD
    return _SWING_HIT_THRESHOLD, _SWING_PARTIAL_THRESHOLD


def _effective_verdict(
    status: str | None,
    realized_return: float | None,
    *,
    horizon_label: str | None,
) -> str:
    hit_threshold, partial_threshold = _horizon_thresholds(horizon_label)
    sign = _direction_sign(status or "neutral")
    if sign == 0:
        if realized_return is None:
            return "partial"
        return "hit" if abs(realized_return) <= _NEUTRAL_MOVE_BAND else "miss"
    if realized_return is None:
        return "expired"
    signed = realized_return * sign
    if signed >= hit_threshold:
        return "hit"
    if signed >= partial_threshold:
        return "partial"
    return "miss"


def _invalidation_status(verdict: str, signed_return: float | None) -> str:
    if verdict == "invalidated":
        return "violated"
    if verdict in {"hit", "partial"}:
        return "not_triggered"
    if signed_return is None:
        return "unknown"
    return "violated" if signed_return <= -0.02 else "respected"


def _review_weight(value: float, delta: float, *, floor: float = 0.0, ceiling: float = 1.5) -> float:
    return round(min(ceiling, max(floor, value + delta)), 4)


def _parse_json_blob(value: Any) -> dict[str, Any] | None:
    if isinstance(value, dict):
        return value
    if not isinstance(value, str):
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if "```" in cleaned:
        match = re.search(r"```(?:json)?\s*(\{.*\})\s*```", cleaned, re.DOTALL)
        if match:
            cleaned = match.group(1)
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start < 0 or end < 0 or end <= start:
        return None
    try:
        parsed = json.loads(cleaned[start : end + 1])
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


def _normalize_weight_map(source: Mapping[str, Any] | None, fallback: Mapping[str, float]) -> dict[str, float]:
    merged: dict[str, float] = {key: float(value) for key, value in fallback.items()}
    if not source:
        return merged
    for key, value in source.items():
        try:
            merged[str(key)] = float(value)
        except (TypeError, ValueError):
            continue
    return merged


def _dominant_driver_labels(payload: Mapping[str, Any] | None, keys: list[str]) -> list[str]:
    if not isinstance(payload, Mapping):
        return []
    drivers: list[str] = []
    for key in keys:
        value = payload.get(key)
        if value in (None, "", [], {}):
            continue
        if isinstance(value, list):
            drivers.extend(str(item) for item in value[:3] if item not in (None, ""))
        elif isinstance(value, Mapping):
            if "state" in value:
                drivers.append(f"{key}:{value.get('state')}")
            elif "title" in value:
                drivers.append(f"{key}:{value.get('title')}")
            else:
                drivers.append(key)
        else:
            drivers.append(f"{key}:{value}")
        if len(drivers) >= 5:
            break
    return drivers[:5]


class AstroGridStore:
    """Persist AstroGrid runs and snapshots into the dedicated schema."""

    def __init__(self, db_engine: Engine) -> None:
        self.engine = db_engine
        self.schema = _safe_schema_name(settings.ASTROGRID_DB_SCHEMA)

    def save_snapshot(self, snapshot: dict[str, Any]) -> int | None:
        """Persist or reuse a deterministic sky snapshot row."""
        if not snapshot:
            return None

        snapshot_date = date.fromisoformat(str(snapshot.get("date") or date.today().isoformat()))
        snapshot_ts = _stable_snapshot_ts(snapshot_date)
        location_key = str(snapshot.get("location_key") or "geocentric")
        source = str(snapshot.get("source") or "")
        source_mode = _snapshot_source_mode(source)
        precision = _snapshot_precision_label(source_mode)

        sql = text(
            f"""
            INSERT INTO {self.schema}.sky_snapshot (
                snapshot_date,
                snapshot_ts,
                location_key,
                source_mode,
                precision_label,
                source_trace,
                bodies_payload,
                aspects_payload,
                cycles_payload,
                events_payload,
                signals_payload,
                grid_overlay_payload
            )
            VALUES (
                :snapshot_date,
                :snapshot_ts,
                :location_key,
                :source_mode,
                :precision_label,
                CAST(:source_trace AS jsonb),
                CAST(:bodies_payload AS jsonb),
                CAST(:aspects_payload AS jsonb),
                CAST(:cycles_payload AS jsonb),
                CAST(:events_payload AS jsonb),
                CAST(:signals_payload AS jsonb),
                CAST(:grid_overlay_payload AS jsonb)
            )
            ON CONFLICT (snapshot_ts, location_key, source_mode) DO NOTHING
            RETURNING id
            """
        )
        lookup_sql = text(
            f"""
            SELECT id
            FROM {self.schema}.sky_snapshot
            WHERE snapshot_ts = :snapshot_ts
              AND location_key = :location_key
              AND source_mode = :source_mode
            LIMIT 1
            """
        )

        params = {
            "snapshot_date": snapshot_date,
            "snapshot_ts": snapshot_ts,
            "location_key": location_key,
            "source_mode": source_mode,
            "precision_label": precision,
            "source_trace": _safe_json({
                "source": source,
                "transport_timestamp": snapshot.get("timestamp"),
            }),
            "bodies_payload": _safe_json(snapshot.get("objects") or snapshot.get("bodies") or []),
            "aspects_payload": _safe_json(snapshot.get("aspects") or []),
            "cycles_payload": _safe_json({
                "lunar": snapshot.get("lunar") or {},
                "nakshatra": snapshot.get("nakshatra") or {},
                "void_of_course": snapshot.get("void_of_course") or {},
                "retrograde_planets": snapshot.get("retrograde_planets") or [],
            }),
            "events_payload": _safe_json(snapshot.get("events") or []),
            "signals_payload": _safe_json({
                "signals": snapshot.get("signals") or {},
                "signal_field": snapshot.get("signal_field") or [],
                "seer": snapshot.get("seer") or {},
                "summary": snapshot.get("summary") or {},
            }),
            "grid_overlay_payload": _safe_json(snapshot.get("grid") or {}),
        }

        try:
            with self.engine.begin() as conn:
                row = conn.execute(sql, params).fetchone()
                if row:
                    return int(row[0])
                row = conn.execute(lookup_sql, {
                    "snapshot_ts": snapshot_ts,
                    "location_key": location_key,
                    "source_mode": source_mode,
                }).fetchone()
                return int(row[0]) if row else None
        except Exception as exc:
            log.warning("AstroGrid snapshot persistence failed: {e}", e=str(exc))
            return None

    def ensure_lens_set(self, mode: str, lens_ids: list[str]) -> int | None:
        """Persist a stable lens-set identity for the current request shape."""
        clean_lenses = [str(lens).strip() for lens in lens_ids if str(lens).strip()]
        lens_set_key = f"{mode}:{'|'.join(sorted(clean_lenses)) or 'none'}"
        sql = text(
            f"""
            INSERT INTO {self.schema}.lens_set (
                lens_set_key,
                version,
                name,
                mode,
                allowed_lenses,
                forbidden_lenses,
                weighting,
                doctrine
            )
            VALUES (
                :lens_set_key,
                1,
                :name,
                :mode,
                :allowed_lenses,
                :forbidden_lenses,
                CAST(:weighting AS jsonb),
                :doctrine
            )
            ON CONFLICT (lens_set_key, version) DO NOTHING
            RETURNING id
            """
        )
        lookup_sql = text(
            f"SELECT id FROM {self.schema}.lens_set WHERE lens_set_key = :lens_set_key AND version = 1 LIMIT 1"
        )

        try:
            with self.engine.begin() as conn:
                row = conn.execute(sql, {
                    "lens_set_key": lens_set_key,
                    "name": lens_set_key,
                    "mode": mode,
                    "allowed_lenses": clean_lenses,
                    "forbidden_lenses": [],
                    "weighting": _safe_json({}),
                    "doctrine": "Runtime-selected lens set.",
                }).fetchone()
                if row:
                    return int(row[0])
                row = conn.execute(lookup_sql, {"lens_set_key": lens_set_key}).fetchone()
                return int(row[0]) if row else None
        except Exception as exc:
            log.warning("AstroGrid lens-set persistence failed: {e}", e=str(exc))
            return None

    def save_interpretation(self, request_payload: dict[str, Any], response_payload: dict[str, Any]) -> dict[str, int | None]:
        """Persist engine, Seer, and persona runs for an interpretation call."""
        snapshot = request_payload.get("snapshot") or {}
        snapshot_id = self.save_snapshot(snapshot) if snapshot else None
        lens_set_id = self.ensure_lens_set(
            str(request_payload.get("mode") or "chorus"),
            list(request_payload.get("lens_ids") or []),
        )
        if snapshot and snapshot_id is None:
            return {
                "snapshot_id": None,
                "lens_set_id": lens_set_id,
                "seer_run_id": None,
                "persona_run_id": None,
            }

        engine_run_ids: list[int] = []
        try:
            with self.engine.begin() as conn:
                seer_payload = response_payload.get("seer") or {}
                seer_run_id = None
                if snapshot_id is not None:
                    for engine_output in list(request_payload.get("engine_outputs") or [])[:12]:
                        row = conn.execute(
                            text(
                                f"""
                                INSERT INTO {self.schema}.engine_run (
                                    sky_snapshot_id,
                                    lens_set_id,
                                    engine_key,
                                    engine_family,
                                    provider_mode,
                                    model_name,
                                    direction_label,
                                    confidence,
                                    horizon_label,
                                    reading,
                                    omen,
                                    prediction,
                                    claim_payload,
                                    rationale_payload,
                                    contradiction_payload,
                                    feature_trace,
                                    citation_payload,
                                    raw_output
                                )
                                VALUES (
                                    :sky_snapshot_id,
                                    :lens_set_id,
                                    :engine_key,
                                    :engine_family,
                                    :provider_mode,
                                    :model_name,
                                    :direction_label,
                                    :confidence,
                                    :horizon_label,
                                    :reading,
                                    :omen,
                                    :prediction,
                                    CAST(:claim_payload AS jsonb),
                                    CAST(:rationale_payload AS jsonb),
                                    CAST(:contradiction_payload AS jsonb),
                                    CAST(:feature_trace AS jsonb),
                                    CAST(:citation_payload AS jsonb),
                                    CAST(:raw_output AS jsonb)
                                )
                                RETURNING id
                                """
                            ),
                            {
                                "sky_snapshot_id": snapshot_id,
                                "lens_set_id": lens_set_id,
                                "engine_key": engine_output.get("engine_id") or "unknown",
                                "engine_family": engine_output.get("family") or "unknown",
                                "provider_mode": "deterministic",
                                "model_name": None,
                                "direction_label": engine_output.get("direction_label"),
                                "confidence": _coerce_confidence(engine_output.get("confidence")),
                                "horizon_label": engine_output.get("horizon"),
                                "reading": engine_output.get("reading") or "",
                                "omen": engine_output.get("omen"),
                                "prediction": engine_output.get("prediction"),
                                "claim_payload": _safe_json(engine_output.get("claims") or []),
                                "rationale_payload": _safe_json(engine_output.get("rationale") or []),
                                "contradiction_payload": _safe_json(engine_output.get("contradictions") or []),
                                "feature_trace": _safe_json(engine_output.get("feature_trace") or {}),
                                "citation_payload": _safe_json(engine_output.get("citations") or []),
                                "raw_output": _safe_json(engine_output),
                            },
                        ).fetchone()
                        if row:
                            engine_run_ids.append(int(row[0]))

                    seer_row = conn.execute(
                        text(
                            f"""
                            INSERT INTO {self.schema}.seer_run (
                                sky_snapshot_id,
                                lens_set_id,
                                merge_mode,
                                supporting_lenses,
                                source_engine_runs,
                                convergence_map,
                                contradiction_map,
                                world_overlay_payload,
                                reading,
                                prediction,
                                confidence,
                                confidence_band,
                                key_factors,
                                conflict_payload,
                                action_bias,
                                window_label,
                                raw_output
                            )
                            VALUES (
                                :sky_snapshot_id,
                                :lens_set_id,
                                :merge_mode,
                                :supporting_lenses,
                                CAST(:source_engine_runs AS jsonb),
                                CAST(:convergence_map AS jsonb),
                                CAST(:contradiction_map AS jsonb),
                                CAST(:world_overlay_payload AS jsonb),
                                :reading,
                                :prediction,
                                :confidence,
                                :confidence_band,
                                CAST(:key_factors AS jsonb),
                                CAST(:conflict_payload AS jsonb),
                                :action_bias,
                                :window_label,
                                CAST(:raw_output AS jsonb)
                            )
                            RETURNING id
                            """
                        ),
                        {
                            "sky_snapshot_id": snapshot_id,
                            "lens_set_id": lens_set_id,
                            "merge_mode": request_payload.get("mode") or "chorus",
                            "supporting_lenses": list(request_payload.get("lens_ids") or []),
                            "source_engine_runs": _safe_json(engine_run_ids),
                            "convergence_map": _safe_json({}),
                            "contradiction_map": _safe_json({"warnings": seer_payload.get("warnings") or []}),
                            "world_overlay_payload": _safe_json((snapshot or {}).get("grid") or {}),
                            "reading": seer_payload.get("reading") or "",
                            "prediction": seer_payload.get("prediction") or response_payload.get("summary") or "",
                            "confidence": _coerce_confidence((request_payload.get("seer") or {}).get("confidence")),
                            "confidence_band": (request_payload.get("seer") or {}).get("confidence_band"),
                            "key_factors": _safe_json(seer_payload.get("why") or []),
                            "conflict_payload": _safe_json(seer_payload.get("warnings") or []),
                            "action_bias": None,
                            "window_label": None,
                            "raw_output": _safe_json(response_payload),
                        },
                    ).fetchone()
                    seer_run_id = int(seer_row[0]) if seer_row else None

                persona_row = conn.execute(
                    text(
                        f"""
                        INSERT INTO {self.schema}.persona_run (
                            seer_run_id,
                            lens_set_id,
                            persona_key,
                            provider_mode,
                            model_name,
                            question,
                            declared_lens,
                            allowed_lenses,
                            excluded_lenses,
                            answer_text,
                            answer_payload,
                            citation_payload,
                            raw_output
                        )
                        VALUES (
                            :seer_run_id,
                            :lens_set_id,
                            :persona_key,
                            :provider_mode,
                            :model_name,
                            :question,
                            :declared_lens,
                            :allowed_lenses,
                            :excluded_lenses,
                            :answer_text,
                            CAST(:answer_payload AS jsonb),
                            CAST(:citation_payload AS jsonb),
                            CAST(:raw_output AS jsonb)
                        )
                        RETURNING id
                        """
                    ),
                    {
                        "seer_run_id": seer_run_id,
                        "lens_set_id": lens_set_id,
                        "persona_key": request_payload.get("persona_id") or "seer",
                        "provider_mode": "llm" if response_payload.get("used_llm") else "deterministic",
                        "model_name": response_payload.get("model"),
                        "question": request_payload.get("question") or "What threads matter now?",
                        "declared_lens": f"{request_payload.get('persona_id') or 'seer'} / {request_payload.get('mode') or 'chorus'}",
                        "allowed_lenses": list(request_payload.get("lens_ids") or []),
                        "excluded_lenses": [],
                        "answer_text": response_payload.get("summary") or seer_payload.get("prediction") or "",
                        "answer_payload": _safe_json(response_payload),
                        "citation_payload": _safe_json(response_payload.get("threads") or []),
                        "raw_output": _safe_json(response_payload),
                    },
                ).fetchone()
                persona_run_id = int(persona_row[0]) if persona_row else None
        except Exception as exc:
            log.warning("AstroGrid interpretation persistence failed: {e}", e=str(exc))
            return {
                "snapshot_id": snapshot_id,
                "lens_set_id": lens_set_id,
                "seer_run_id": None,
                "persona_run_id": None,
            }

        return {
            "snapshot_id": snapshot_id,
            "lens_set_id": lens_set_id,
            "seer_run_id": seer_run_id,
            "persona_run_id": persona_run_id,
        }

    def save_prediction_stub_postmortem(
        self,
        prediction_run_id: int,
        *,
        summary: str,
        dominant_grid_drivers: list[str],
        dominant_mystical_drivers: list[str],
        invalidation_rule: str,
        feature_family_summary: dict[str, Any],
        raw_payload: dict[str, Any],
    ) -> int | None:
        sql = text(
            f"""
            INSERT INTO {self.schema}.prediction_postmortem (
                prediction_run_id,
                state,
                summary,
                dominant_grid_drivers,
                dominant_mystical_drivers,
                invalidation_rule,
                feature_family_summary,
                raw_payload
            )
            VALUES (
                :prediction_run_id,
                'pending',
                :summary,
                CAST(:dominant_grid_drivers AS jsonb),
                CAST(:dominant_mystical_drivers AS jsonb),
                :invalidation_rule,
                CAST(:feature_family_summary AS jsonb),
                CAST(:raw_payload AS jsonb)
            )
            ON CONFLICT (prediction_run_id) DO NOTHING
            RETURNING id
            """
        )
        lookup_sql = text(
            f"""
            SELECT id
            FROM {self.schema}.prediction_postmortem
            WHERE prediction_run_id = :prediction_run_id
            LIMIT 1
            """
        )
        params = {
            "prediction_run_id": prediction_run_id,
            "summary": _compact_text(summary, "Pending review."),
            "dominant_grid_drivers": _safe_json(dominant_grid_drivers),
            "dominant_mystical_drivers": _safe_json(dominant_mystical_drivers),
            "invalidation_rule": _compact_text(invalidation_rule),
            "feature_family_summary": _safe_json(feature_family_summary),
            "raw_payload": _safe_json(raw_payload),
        }
        with self.engine.begin() as conn:
            row = conn.execute(sql, params).fetchone()
            if row:
                return int(row[0])
            row = conn.execute(lookup_sql, {"prediction_run_id": prediction_run_id}).fetchone()
            return int(row[0]) if row else None

    def save_prediction(self, payload: dict[str, Any]) -> dict[str, Any] | None:
        snapshot = payload.get("snapshot") or {}
        snapshot_id = self.save_snapshot(snapshot) if snapshot else None
        lens_set_id = self.ensure_lens_set(
            str(payload.get("mode") or "chorus"),
            list(payload.get("lens_ids") or []),
        )
        prediction_id = str(payload.get("prediction_id") or uuid4())
        as_of_ts = payload.get("as_of_ts") or datetime.now(timezone.utc).isoformat()
        if isinstance(as_of_ts, str):
            as_of_ts_value = datetime.fromisoformat(as_of_ts.replace("Z", "+00:00"))
        else:
            as_of_ts_value = as_of_ts
        status = str(payload.get("status") or "pending")
        if status not in {"pending", "scored", "invalidated", "expired"}:
            status = "pending"
        live_or_local = str(payload.get("live_or_local") or "local")
        if live_or_local not in {"live", "local", "archive", "hybrid"}:
            live_or_local = "local"
        oracle_publish = payload.get("oracle_publish") if isinstance(payload.get("oracle_publish"), Mapping) else {}

        insert_sql = text(
            f"""
            INSERT INTO {self.schema}.prediction_run (
                prediction_id,
                as_of_ts,
                horizon_label,
                target_universe,
                scoring_class,
                target_symbols,
                question,
                call,
                timing,
                setup,
                invalidation,
                note,
                seer_summary,
                market_overlay_snapshot,
                mystical_feature_payload,
                grid_feature_payload,
                weight_version,
                model_version,
                live_or_local,
                status,
                comparable_publish_status,
                comparable_prediction_ref,
                comparable_publish_payload,
                lens_set_id,
                sky_snapshot_id,
                seer_run_id,
                persona_run_id
            )
            VALUES (
                :prediction_id,
                :as_of_ts,
                :horizon_label,
                :target_universe,
                :scoring_class,
                CAST(:target_symbols AS jsonb),
                :question,
                :call,
                :timing,
                :setup,
                :invalidation,
                :note,
                :seer_summary,
                CAST(:market_overlay_snapshot AS jsonb),
                CAST(:mystical_feature_payload AS jsonb),
                CAST(:grid_feature_payload AS jsonb),
                :weight_version,
                :model_version,
                :live_or_local,
                :status,
                :comparable_publish_status,
                :comparable_prediction_ref,
                CAST(:comparable_publish_payload AS jsonb),
                :lens_set_id,
                :sky_snapshot_id,
                :seer_run_id,
                :persona_run_id
            )
            RETURNING id
            """
        )

        params = {
            "prediction_id": prediction_id,
            "as_of_ts": as_of_ts_value,
            "horizon_label": str(payload.get("horizon_label") or "swing"),
            "target_universe": str(payload.get("target_universe") or "hybrid"),
            "scoring_class": _normalize_scoring_class(payload.get("scoring_class")),
            "target_symbols": _safe_json(list(payload.get("target_symbols") or [])),
            "question": _compact_text(payload.get("question"), "What should I watch now?"),
            "call": _compact_text(payload.get("call")),
            "timing": _compact_text(payload.get("timing")),
            "setup": _compact_text(payload.get("setup")),
            "invalidation": _compact_text(payload.get("invalidation")),
            "note": _compact_text(payload.get("note")),
            "seer_summary": _compact_text(payload.get("seer_summary")),
            "market_overlay_snapshot": _safe_json(payload.get("market_overlay_snapshot") or {}),
            "mystical_feature_payload": _safe_json(payload.get("mystical_feature_payload") or {}),
            "grid_feature_payload": _safe_json(payload.get("grid_feature_payload") or {}),
            "weight_version": _compact_text(payload.get("weight_version"), "astrogrid-v1"),
            "model_version": _compact_text(payload.get("model_version"), "astrogrid-oracle-v1"),
            "live_or_local": live_or_local,
            "status": status,
            "comparable_publish_status": str(oracle_publish.get("status") or "not_attempted"),
            "comparable_prediction_ref": oracle_publish.get("oracle_prediction_id"),
            "comparable_publish_payload": _safe_json(oracle_publish),
            "lens_set_id": lens_set_id,
            "sky_snapshot_id": snapshot_id,
            "seer_run_id": payload.get("seer_run_id"),
            "persona_run_id": payload.get("persona_run_id"),
        }

        try:
            with self.engine.begin() as conn:
                row = conn.execute(insert_sql, params).fetchone()
                if not row:
                    return None
                prediction_run_id = int(row[0])
        except Exception as exc:
            log.warning("AstroGrid prediction persistence failed: {e}", e=str(exc))
            return None

        try:
            self.save_prediction_stub_postmortem(
                prediction_run_id,
                summary=_compact_text(payload.get("postmortem_summary"), "Pending outcome review."),
                dominant_grid_drivers=list(payload.get("dominant_grid_drivers") or []),
                dominant_mystical_drivers=list(payload.get("dominant_mystical_drivers") or []),
                invalidation_rule=_compact_text(payload.get("invalidation")),
                feature_family_summary=dict(payload.get("feature_family_summary") or {}),
                raw_payload=dict(payload.get("postmortem_raw_payload") or {}),
            )
        except Exception as exc:
            log.warning("AstroGrid postmortem stub persistence failed: {e}", e=str(exc))

        return self.get_prediction(prediction_id)

    def ensure_active_weight_version(self) -> dict[str, Any]:
        lookup_sql = text(
            f"""
            SELECT version_key, status, grid_weights, mystical_weights, notes, approved_by, approved_at, created_at
            FROM {self.schema}.weight_version
            WHERE status = 'active'
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        insert_sql = text(
            f"""
            INSERT INTO {self.schema}.weight_version (
                version_key,
                status,
                grid_weights,
                mystical_weights,
                notes,
                approved_by,
                approved_at
            )
            VALUES (
                :version_key,
                'active',
                CAST(:grid_weights AS jsonb),
                CAST(:mystical_weights AS jsonb),
                :notes,
                :approved_by,
                NOW()
            )
            RETURNING version_key, status, grid_weights, mystical_weights, notes, approved_by, approved_at, created_at
            """
        )
        with self.engine.begin() as conn:
            row = conn.execute(lookup_sql).fetchone()
            if not row:
                row = conn.execute(
                    insert_sql,
                    {
                        "version_key": "astrogrid-v1",
                        "grid_weights": _safe_json(_DEFAULT_GRID_WEIGHTS),
                        "mystical_weights": _safe_json(_DEFAULT_MYSTICAL_WEIGHTS),
                        "notes": "Default AstroGrid baseline weights.",
                        "approved_by": "system",
                    },
                ).fetchone()
        return {
            "version_key": row[0],
            "status": row[1],
            "grid_weights": _json_loads(row[2], {}),
            "mystical_weights": _json_loads(row[3], {}),
            "notes": row[4],
            "approved_by": row[5],
            "approved_at": row[6].isoformat() if row[6] else None,
            "created_at": row[7].isoformat() if row[7] else None,
        }

    def _flatten_attribution_labels(self, value: Any) -> list[str]:
        items = _json_loads(value, [])
        labels: list[str] = []
        if isinstance(items, dict):
            items = [items]
        for item in items:
            if isinstance(item, str):
                label = _compact_text(item)
            elif isinstance(item, Mapping):
                label = _compact_text(
                    item.get("label")
                    or item.get("feature")
                    or item.get("driver")
                    or item.get("name")
                    or item.get("id")
                )
            else:
                label = ""
            if label:
                labels.append(label)
        return labels

    def _weight_proposal_effective_state(self, proposal_status: str, decision: str | None) -> str:
        return str(decision or proposal_status or "pending_review")

    def _build_deterministic_review(
        self,
        *,
        current_weights: dict[str, Any],
        prediction_rows: list[Any],
        backtest_summary: dict[str, Any],
    ) -> dict[str, Any]:
        grid_hits: Counter[str] = Counter()
        grid_misses: Counter[str] = Counter()
        myst_hits: Counter[str] = Counter()
        myst_misses: Counter[str] = Counter()
        noise_counter: Counter[str] = Counter()
        regime_counter: Counter[str] = Counter()
        hit_count = 0
        miss_count = 0

        for row in prediction_rows:
            verdict = str(row[0] or "")
            if verdict in {"hit", "partial"}:
                hit_count += 1
            elif verdict in {"miss", "invalidated", "expired"}:
                miss_count += 1
            regime = ""
            if isinstance(row[4], dict):
                regime = _compact_text(row[4].get("state"))
            else:
                regime = _compact_text((_json_loads(row[4], {}) or {}).get("state"))
            if regime:
                regime_counter[regime] += 1
            target_grid = grid_hits if verdict in {"hit", "partial"} else grid_misses
            target_myst = myst_hits if verdict in {"hit", "partial"} else myst_misses
            for label in self._flatten_attribution_labels(row[1]):
                target_grid[label] += 1
            for label in self._flatten_attribution_labels(row[2]):
                target_myst[label] += 1
            for label in self._flatten_attribution_labels(row[3]):
                noise_counter[label] += 1

        active_grid = _normalize_weight_map(current_weights.get("grid_weights"), _DEFAULT_GRID_WEIGHTS)
        active_mystical = _normalize_weight_map(current_weights.get("mystical_weights"), _DEFAULT_MYSTICAL_WEIGHTS)

        def _adjust_weights(
            base: dict[str, float],
            hits: Counter[str],
            misses: Counter[str],
            *,
            shrink_default: float = 0.0,
        ) -> dict[str, float]:
            adjusted = dict(base)
            for key, current in base.items():
                plus = sum(count for label, count in hits.items() if key in label.lower())
                minus = sum(count for label, count in misses.items() if key in label.lower())
                delta = 0.0
                if plus > minus:
                    delta += 0.05
                elif minus > plus:
                    delta -= 0.05
                if shrink_default and plus == 0 and minus > 0:
                    delta -= shrink_default
                adjusted[key] = _review_weight(current, delta)
            return adjusted

        proposed_grid = _adjust_weights(active_grid, grid_hits, grid_misses)
        proposed_mystical = _adjust_weights(active_mystical, myst_hits, myst_misses, shrink_default=0.03)

        top_grid = [label for label, _ in grid_hits.most_common(3)]
        weak_grid = [label for label, _ in grid_misses.most_common(3)]
        top_mystical = [label for label, _ in myst_hits.most_common(3)]
        weak_mystical = [label for label, _ in myst_misses.most_common(3)]
        noise = [label for label, _ in noise_counter.most_common(3)]
        regime_tags = [label for label, _ in regime_counter.most_common(3)]

        history = backtest_summary.get("history") or []
        latest_by_variant = backtest_summary.get("latest_by_variant") or {}
        best_variant = None
        best_alpha = None
        for variant, payload in latest_by_variant.items():
            alpha = ((payload or {}).get("summary") or {}).get("avg_alpha_vs_benchmark")
            if alpha is None:
                continue
            if best_alpha is None or float(alpha) > best_alpha:
                best_alpha = float(alpha)
                best_variant = variant

        confidence = 0.45
        if hit_count + miss_count >= 10:
            confidence += 0.15
        if best_alpha and best_alpha > 0.02:
            confidence += 0.15
        if top_grid:
            confidence += 0.05
        confidence = round(min(0.95, confidence), 3)

        what_worked = []
        if top_grid:
            what_worked.append(f"GRID drivers held: {', '.join(top_grid)}.")
        if top_mystical:
            what_worked.append(f"Mystical drivers that survived scoring: {', '.join(top_mystical)}.")
        if best_variant:
            what_worked.append(f"Best recent backtest variant: {best_variant}.")
        if not what_worked:
            what_worked.append("Not enough scored predictions to identify durable strengths yet.")

        what_failed = []
        if weak_grid:
            what_failed.append(f"GRID drivers that failed most often: {', '.join(weak_grid)}.")
        if weak_mystical:
            what_failed.append(f"Mystical drivers with weak follow-through: {', '.join(weak_mystical)}.")
        if miss_count > hit_count:
            what_failed.append("Recent misses exceed clean hits; keep leverage low until the loop stabilizes.")
        if not what_failed:
            what_failed.append("No dominant failure cluster yet.")

        noisy = noise or ["No consistent noise cluster yet."]
        regime_conditional = regime_tags or ["neutral"]
        reasoning_summary = " ".join(
            [
                what_worked[0],
                what_failed[0],
                f"Proposal leans {best_variant or 'grid_plus_mystical'} with regime tags {', '.join(regime_conditional)}.",
            ]
        )

        return {
            "what_worked": what_worked,
            "what_failed": what_failed,
            "appears_noisy": noisy,
            "regime_conditional": regime_conditional,
            "proposed_grid_weights": proposed_grid,
            "proposed_mystical_weights": proposed_mystical,
            "confidence": confidence,
            "reasoning_summary": reasoning_summary,
            "best_variant": best_variant,
            "latest_backtests": history[:3],
        }

    def _maybe_refine_review_with_llm(
        self,
        *,
        provider_mode: str,
        review_input: dict[str, Any],
        review_payload: dict[str, Any],
    ) -> tuple[str, str | None, dict[str, Any]]:
        if provider_mode not in {"llm", "hybrid"}:
            return "deterministic", None, review_payload
        try:
            from ollama.client import get_client

            client = get_client()
            if not getattr(client, "is_available", False):
                return "deterministic", getattr(client, "model", None), review_payload
            prompt = (
                "Return strict JSON with keys: what_worked, what_failed, appears_noisy, "
                "regime_conditional, proposed_grid_weights, proposed_mystical_weights, confidence, reasoning_summary. "
                "Use the provided deterministic review as the baseline. Do not invent unsupported claims.\n\n"
                f"INPUT:\n{json.dumps(review_input, default=str)}\n\n"
                f"BASELINE:\n{json.dumps(review_payload, default=str)}"
            )
            raw = client.chat(
                messages=[
                    {
                        "role": "system",
                        "content": "You are reviewing scored financial predictions. Stay terse, structured, and evidence-bound.",
                    },
                    {"role": "user", "content": prompt},
                ],
                temperature=0.1,
                num_predict=900,
            )
            parsed = _parse_json_blob(raw)
            if not parsed:
                return "deterministic", getattr(client, "model", None), review_payload
            merged = dict(review_payload)
            for key in (
                "what_worked",
                "what_failed",
                "appears_noisy",
                "regime_conditional",
                "proposed_grid_weights",
                "proposed_mystical_weights",
                "confidence",
                "reasoning_summary",
            ):
                if key in parsed:
                    merged[key] = parsed[key]
            merged["confidence"] = _coerce_confidence(merged.get("confidence")) or review_payload["confidence"]
            return "llm" if provider_mode == "llm" else "hybrid", getattr(client, "model", None), merged
        except Exception as exc:
            log.debug("AstroGrid review LLM unavailable: {e}", e=str(exc))
            return "deterministic", None, review_payload

    def generate_review_run(
        self,
        *,
        provider_mode: str = "deterministic",
        prediction_limit: int = 200,
        backtest_limit: int = 12,
    ) -> dict[str, Any]:
        provider_mode = provider_mode if provider_mode in {"deterministic", "llm", "hybrid"} else "deterministic"
        review_key = f"review-{uuid4()}"
        active_weights = self.ensure_active_weight_version()
        score_sql = text(
            f"""
            SELECT
                ps.verdict,
                ps.attribution_grid,
                ps.attribution_mystical,
                ps.attribution_noise,
                ps.regime_context
            FROM {self.schema}.prediction_score ps
            ORDER BY ps.scored_at DESC
            LIMIT :limit
            """
        )
        with self.engine.begin() as conn:
            prediction_rows = conn.execute(score_sql, {"limit": prediction_limit}).fetchall()
        backtest_summary = self.get_backtest_summary(limit=backtest_limit)
        review_input = {
            "prediction_count": len(prediction_rows),
            "backtest_summary": backtest_summary,
            "active_weights": active_weights,
        }
        deterministic = self._build_deterministic_review(
            current_weights=active_weights,
            prediction_rows=prediction_rows,
            backtest_summary=backtest_summary,
        )
        actual_mode, model_name, review_payload = self._maybe_refine_review_with_llm(
            provider_mode=provider_mode,
            review_input=review_input,
            review_payload=deterministic,
        )

        insert_review_sql = text(
            f"""
            INSERT INTO {self.schema}.review_run (
                review_key,
                provider_mode,
                model_name,
                based_on_prediction_count,
                based_on_backtest_window,
                input_payload,
                review_payload,
                status
            )
            VALUES (
                :review_key,
                :provider_mode,
                :model_name,
                :based_on_prediction_count,
                CAST(:based_on_backtest_window AS jsonb),
                CAST(:input_payload AS jsonb),
                CAST(:review_payload AS jsonb),
                'completed'
            )
            RETURNING id, created_at
            """
        )
        insert_proposal_sql = text(
            f"""
            INSERT INTO {self.schema}.weight_proposal (
                weight_proposal_id,
                review_run_id,
                based_on_prediction_count,
                based_on_backtest_window,
                proposed_grid_weights,
                proposed_mystical_weights,
                reasoning_summary,
                confidence,
                status
            )
            VALUES (
                :weight_proposal_id,
                :review_run_id,
                :based_on_prediction_count,
                CAST(:based_on_backtest_window AS jsonb),
                CAST(:proposed_grid_weights AS jsonb),
                CAST(:proposed_mystical_weights AS jsonb),
                :reasoning_summary,
                :confidence,
                'pending_review'
            )
            RETURNING id, created_at
            """
        )
        proposal_id = f"proposal-{uuid4()}"
        with self.engine.begin() as conn:
            review_row = conn.execute(
                insert_review_sql,
                {
                    "review_key": review_key,
                    "provider_mode": actual_mode,
                    "model_name": model_name,
                    "based_on_prediction_count": len(prediction_rows),
                    "based_on_backtest_window": _safe_json(
                        {
                            "limit": backtest_limit,
                            "latest_by_variant": list((backtest_summary.get("latest_by_variant") or {}).keys()),
                        }
                    ),
                    "input_payload": _safe_json(review_input),
                    "review_payload": _safe_json(review_payload),
                },
            ).fetchone()
            proposal_row = conn.execute(
                insert_proposal_sql,
                {
                    "weight_proposal_id": proposal_id,
                    "review_run_id": int(review_row[0]),
                    "based_on_prediction_count": len(prediction_rows),
                    "based_on_backtest_window": _safe_json(
                        {
                            "limit": backtest_limit,
                            "latest_by_variant": list((backtest_summary.get("latest_by_variant") or {}).keys()),
                        }
                    ),
                    "proposed_grid_weights": _safe_json(review_payload.get("proposed_grid_weights") or {}),
                    "proposed_mystical_weights": _safe_json(review_payload.get("proposed_mystical_weights") or {}),
                    "reasoning_summary": _compact_text(review_payload.get("reasoning_summary")),
                    "confidence": float(review_payload.get("confidence") or 0.5),
                },
            ).fetchone()

        return {
            "review_key": review_key,
            "provider_mode": actual_mode,
            "model_name": model_name,
            "created_at": review_row[1].isoformat() if review_row and review_row[1] else None,
            "based_on_prediction_count": len(prediction_rows),
            "review": review_payload,
            "proposal": {
                "weight_proposal_id": proposal_id,
                "created_at": proposal_row[1].isoformat() if proposal_row and proposal_row[1] else None,
                "status": "pending_review",
                "proposed_grid_weights": review_payload.get("proposed_grid_weights") or {},
                "proposed_mystical_weights": review_payload.get("proposed_mystical_weights") or {},
                "reasoning_summary": review_payload.get("reasoning_summary"),
                "confidence": review_payload.get("confidence"),
            },
        }

    def run_learning_loop(
        self,
        *,
        as_of_date: date | None = None,
        score_limit: int = 200,
        backtest_limit: int = 250,
        backtest_window_days: int = 180,
        provider_mode: str = "deterministic",
        horizon_label: str | None = None,
    ) -> dict[str, Any]:
        evaluation_date = as_of_date or date.today()
        score_summary = self.score_predictions(
            as_of_date=evaluation_date,
            limit=score_limit,
        )
        window_start = evaluation_date - timedelta(days=backtest_window_days)
        backtest_summary = self.run_backtests(
            strategy_variants=["grid_only", "grid_plus_mystical", "mystical_only"],
            horizon_label=horizon_label,
            window_start=window_start,
            window_end=evaluation_date,
            limit=backtest_limit,
        )
        if not any(((run.get("summary") or {}).get("total_predictions") or 0) for run in backtest_summary.get("runs", [])):
            fallback_window = self._scored_prediction_date_range(horizon_label=horizon_label)
            if fallback_window and fallback_window[0] and fallback_window[1]:
                backtest_summary = self.run_backtests(
                    strategy_variants=["grid_only", "grid_plus_mystical", "mystical_only"],
                    horizon_label=horizon_label,
                    window_start=fallback_window[0],
                    window_end=fallback_window[1],
                    limit=backtest_limit,
                )
        review_summary = self.generate_review_run(
            provider_mode=provider_mode,
            prediction_limit=score_limit,
            backtest_limit=12,
        )
        return {
            "evaluation_date": evaluation_date.isoformat(),
            "score": score_summary,
            "backtest": {
                "count": backtest_summary.get("count", 0),
                "runs": [
                    {
                        "run_key": run.get("run_key"),
                        "strategy_variant": run.get("strategy_variant"),
                        "summary": run.get("summary"),
                    }
                    for run in backtest_summary.get("runs", [])
                ],
            },
            "review": review_summary,
        }

    def score_predictions(
        self,
        *,
        as_of_date: date | None = None,
        limit: int = 100,
        prediction_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        evaluation_date = as_of_date or date.today()
        filters = []
        params: dict[str, Any] = {"limit": limit}
        if prediction_ids:
            filters.append("pr.prediction_id = ANY(:prediction_ids)")
            params["prediction_ids"] = prediction_ids
        params["evaluation_date"] = evaluation_date
        where_sql = f"AND {' AND '.join(filters)}" if filters else ""
        sql = text(
            f"""
            SELECT
                pr.id,
                pr.prediction_id,
                pr.as_of_ts,
                pr.horizon_label,
                pr.scoring_class,
                pr.target_symbols,
                pr.call,
                pr.setup,
                pr.invalidation,
                pr.market_overlay_snapshot,
                pr.mystical_feature_payload,
                pr.grid_feature_payload,
                pr.question,
                ps.id
            FROM {self.schema}.prediction_run pr
            LEFT JOIN {self.schema}.prediction_score ps
                ON ps.prediction_run_id = pr.id
            WHERE ps.id IS NULL
              AND pr.scoring_class = 'liquid_market'
              AND (
                  pr.as_of_ts::date
                  + CASE
                        WHEN pr.horizon_label = 'macro' THEN 30
                        ELSE 7
                    END
              ) <= :evaluation_date
            {where_sql}
            ORDER BY pr.as_of_ts ASC, pr.created_at ASC
            LIMIT :limit
            """
        )
        insert_sql = text(
            f"""
            INSERT INTO {self.schema}.prediction_score (
                prediction_run_id,
                scored_at,
                benchmark_symbol,
                realized_return,
                benchmark_return,
                alpha_vs_benchmark,
                verdict,
                invalidation_status,
                max_favorable_excursion,
                max_adverse_excursion,
                regime_context,
                attribution_grid,
                attribution_mystical,
                attribution_noise,
                raw_payload
            )
            VALUES (
                :prediction_run_id,
                NOW(),
                :benchmark_symbol,
                :realized_return,
                :benchmark_return,
                :alpha_vs_benchmark,
                :verdict,
                :invalidation_status,
                :max_favorable_excursion,
                :max_adverse_excursion,
                CAST(:regime_context AS jsonb),
                CAST(:attribution_grid AS jsonb),
                CAST(:attribution_mystical AS jsonb),
                CAST(:attribution_noise AS jsonb),
                CAST(:raw_payload AS jsonb)
            )
            ON CONFLICT (prediction_run_id) DO NOTHING
            RETURNING id
            """
        )

        summary = {
            "evaluation_date": evaluation_date.isoformat(),
            "candidates": 0,
            "scored": 0,
            "skipped_not_mature": 0,
            "skipped_unscoreable": 0,
            "skipped_no_price": 0,
            "verdicts": {"hit": 0, "miss": 0, "partial": 0, "invalidated": 0, "expired": 0},
            "prediction_ids": [],
        }
        with self.engine.begin() as conn:
            rows = conn.execute(sql, params).fetchall()
            summary["candidates"] = len(rows)
            for row in rows:
                maturity_days = 30 if row[3] == "macro" else 7
                start_date = row[2].date() if row[2] else evaluation_date
                maturity_date = start_date + timedelta(days=maturity_days)
                if evaluation_date < maturity_date:
                    summary["skipped_not_mature"] += 1
                    continue
                target_symbols = [str(symbol).upper() for symbol in _json_loads(row[5], [])]
                score = self._build_prediction_score(
                    prediction_id=row[1],
                    call=row[6],
                    setup=row[7],
                    invalidation=row[8],
                    market_overlay=_json_loads(row[9], {}),
                    mystical_payload=_json_loads(row[10], {}),
                    grid_payload=_json_loads(row[11], {}),
                    target_symbols=target_symbols,
                    start_date=start_date,
                    evaluation_date=evaluation_date,
                )
                if not score:
                    summary["skipped_no_price"] += 1
                    continue
                inserted = conn.execute(
                    insert_sql,
                    {
                        "prediction_run_id": row[0],
                        "benchmark_symbol": score["benchmark_symbol"],
                        "realized_return": score["realized_return"],
                        "benchmark_return": score["benchmark_return"],
                        "alpha_vs_benchmark": score["alpha_vs_benchmark"],
                        "verdict": score["verdict"],
                        "invalidation_status": score["invalidation_status"],
                        "max_favorable_excursion": score["max_favorable_excursion"],
                        "max_adverse_excursion": score["max_adverse_excursion"],
                        "regime_context": _safe_json(score["regime_context"]),
                        "attribution_grid": _safe_json(score["attribution_grid"]),
                        "attribution_mystical": _safe_json(score["attribution_mystical"]),
                        "attribution_noise": _safe_json(score["attribution_noise"]),
                        "raw_payload": _safe_json(score["raw_payload"]),
                    },
                ).fetchone()
                if inserted:
                    summary["scored"] += 1
                    summary["prediction_ids"].append(row[1])
                    summary["verdicts"][score["verdict"]] += 1
        return summary

    def build_prediction_scoreboard(self) -> dict[str, Any]:
        overall_sql = text(
            f"""
            SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN verdict = 'hit' THEN 1 ELSE 0 END) AS hits,
                SUM(CASE WHEN verdict = 'miss' THEN 1 ELSE 0 END) AS misses,
                SUM(CASE WHEN verdict = 'partial' THEN 1 ELSE 0 END) AS partials,
                SUM(CASE WHEN verdict = 'invalidated' THEN 1 ELSE 0 END) AS invalidated,
                SUM(CASE WHEN verdict = 'expired' THEN 1 ELSE 0 END) AS expired,
                AVG(realized_return) AS avg_realized,
                AVG(alpha_vs_benchmark) AS avg_alpha,
                AVG(max_favorable_excursion) AS avg_mfe,
                AVG(max_adverse_excursion) AS avg_mae
            FROM {self.schema}.prediction_score
            """
        )
        symbol_sql = text(
            f"""
            SELECT
                COALESCE(pr.target_symbols->>0, 'HYBRID') AS symbol,
                COUNT(*) AS total,
                SUM(CASE WHEN ps.verdict = 'hit' THEN 1 ELSE 0 END) AS hits,
                SUM(CASE WHEN ps.verdict = 'miss' THEN 1 ELSE 0 END) AS misses,
                SUM(CASE WHEN ps.verdict = 'partial' THEN 1 ELSE 0 END) AS partials,
                AVG(ps.realized_return) AS avg_realized,
                AVG(ps.alpha_vs_benchmark) AS avg_alpha
            FROM {self.schema}.prediction_score ps
            JOIN {self.schema}.prediction_run pr ON pr.id = ps.prediction_run_id
            GROUP BY COALESCE(pr.target_symbols->>0, 'HYBRID')
            ORDER BY COUNT(*) DESC, COALESCE(pr.target_symbols->>0, 'HYBRID')
            """
        )
        pending_sql = text(
            f"""
            SELECT COUNT(*)
            FROM {self.schema}.prediction_run pr
            LEFT JOIN {self.schema}.prediction_score ps ON ps.prediction_run_id = pr.id
            WHERE ps.id IS NULL
            """
        )
        with self.engine.connect() as conn:
            overall_row = conn.execute(overall_sql).fetchone()
            symbol_rows = conn.execute(symbol_sql).fetchall()
            pending = int(conn.execute(pending_sql).scalar() or 0)
        total = int(overall_row[0] or 0) if overall_row else 0
        hits = int(overall_row[1] or 0) if overall_row else 0
        misses = int(overall_row[2] or 0) if overall_row else 0
        partials = int(overall_row[3] or 0) if overall_row else 0
        invalidated = int(overall_row[4] or 0) if overall_row else 0
        expired = int(overall_row[5] or 0) if overall_row else 0
        scored = hits + misses + partials + invalidated + expired
        overall = {
            "total_predictions": total + pending,
            "scored": scored,
            "pending": pending,
            "hits": hits,
            "misses": misses,
            "partials": partials,
            "invalidated": invalidated,
            "expired": expired,
            "accuracy": round((hits + (partials * 0.5)) / scored, 4) if scored else 0.0,
            "avg_realized_return": round(float(overall_row[6] or 0.0), 4) if overall_row else 0.0,
            "avg_alpha_vs_benchmark": round(float(overall_row[7] or 0.0), 4) if overall_row else 0.0,
            "avg_mfe": round(float(overall_row[8] or 0.0), 4) if overall_row else 0.0,
            "avg_mae": round(float(overall_row[9] or 0.0), 4) if overall_row else 0.0,
        }
        by_symbol = []
        for row in symbol_rows:
            hits_i = int(row[2] or 0)
            misses_i = int(row[3] or 0)
            partials_i = int(row[4] or 0)
            scored_i = hits_i + misses_i + partials_i
            by_symbol.append(
                {
                    "symbol": row[0],
                    "total": int(row[1] or 0),
                    "hits": hits_i,
                    "misses": misses_i,
                    "partials": partials_i,
                    "accuracy": round((hits_i + (partials_i * 0.5)) / scored_i, 4) if scored_i else 0.0,
                    "avg_realized_return": round(float(row[5] or 0.0), 4),
                    "avg_alpha_vs_benchmark": round(float(row[6] or 0.0), 4),
                }
            )
        return {"overall": overall, "by_symbol": by_symbol}

    def run_backtests(
        self,
        *,
        strategy_variants: list[str],
        horizon_label: str | None = None,
        window_start: date | None = None,
        window_end: date | None = None,
        limit: int = 250,
    ) -> dict[str, Any]:
        valid_variants = [v for v in strategy_variants if v in {"grid_only", "grid_plus_mystical", "mystical_only"}]
        if not valid_variants:
            valid_variants = ["grid_only", "grid_plus_mystical", "mystical_only"]
        filters = ["1=1"]
        params: dict[str, Any] = {"limit": limit}
        if horizon_label in {"macro", "swing"}:
            filters.append("pr.horizon_label = :horizon_label")
            params["horizon_label"] = horizon_label
        if window_start:
            filters.append("pr.as_of_ts::date >= :window_start")
            params["window_start"] = window_start
        if window_end:
            filters.append("pr.as_of_ts::date <= :window_end")
            params["window_end"] = window_end
        rows_sql = text(
            f"""
            SELECT
                pr.id,
                pr.prediction_id,
                pr.as_of_ts::date,
                pr.horizon_label,
                pr.target_universe,
                pr.target_symbols,
                pr.call,
                pr.setup,
                pr.note,
                pr.market_overlay_snapshot,
                pr.mystical_feature_payload,
                pr.grid_feature_payload,
                ps.realized_return,
                ps.alpha_vs_benchmark,
                ps.attribution_grid,
                ps.attribution_mystical,
                ps.attribution_noise,
                ps.regime_context
            FROM {self.schema}.prediction_score ps
            JOIN {self.schema}.prediction_run pr ON pr.id = ps.prediction_run_id
            WHERE {" AND ".join(filters)}
            ORDER BY pr.as_of_ts DESC
            LIMIT :limit
            """
        )
        run_insert_sql = text(
            f"""
            INSERT INTO {self.schema}.backtest_run (
                run_key,
                strategy_variant,
                horizon_label,
                target_universe,
                started_at,
                completed_at,
                status,
                window_start,
                window_end,
                params_payload,
                summary_payload
            )
            VALUES (
                :run_key,
                :strategy_variant,
                :horizon_label,
                :target_universe,
                NOW(),
                NOW(),
                'completed',
                :window_start,
                :window_end,
                CAST(:params_payload AS jsonb),
                CAST(:summary_payload AS jsonb)
            )
            RETURNING id
            """
        )
        result_insert_sql = text(
            f"""
            INSERT INTO {self.schema}.backtest_result (
                backtest_run_id,
                result_key,
                strategy_variant,
                target_symbol,
                as_of_date,
                alpha_vs_benchmark,
                metrics_payload,
                attribution_grid,
                attribution_mystical,
                attribution_noise
            )
            VALUES (
                :backtest_run_id,
                :result_key,
                :strategy_variant,
                :target_symbol,
                :as_of_date,
                :alpha_vs_benchmark,
                CAST(:metrics_payload AS jsonb),
                CAST(:attribution_grid AS jsonb),
                CAST(:attribution_mystical AS jsonb),
                CAST(:attribution_noise AS jsonb)
            )
            ON CONFLICT (backtest_run_id, result_key) DO NOTHING
            """
        )
        with self.engine.begin() as conn:
            rows = conn.execute(rows_sql, params).fetchall()
            runs: list[dict[str, Any]] = []
            for variant in valid_variants:
                metrics = []
                for row in rows:
                    direction = self._variant_direction(
                        variant=variant,
                        call=row[6],
                        setup=row[7],
                        note=row[8],
                        market_overlay=_json_loads(row[9], {}),
                        mystical_payload=_json_loads(row[10], {}),
                        grid_payload=_json_loads(row[11], {}),
                    )
                    sign = _direction_sign(direction)
                    signed_return = float(row[12] or 0.0) * sign if sign else 0.0
                    signed_alpha = float(row[13] or 0.0) * sign if sign else 0.0
                    verdict = _effective_verdict(
                        direction,
                        float(row[12] or 0.0),
                        horizon_label=row[3],
                    )
                    target_symbol = (_json_loads(row[5], []) or ["HYBRID"])[0]
                    target_group = str(_UNIVERSE_BY_SYMBOL.get(target_symbol, {}).get("asset_class") or "unknown")
                    regime_context = _json_loads(row[17], {})
                    metrics.append(
                        {
                            "result_key": f"{variant}:{row[1]}",
                            "target_symbol": target_symbol,
                            "target_group": target_group,
                            "as_of_date": row[2],
                            "prediction_id": row[1],
                            "signed_return": round(signed_return, 4),
                            "signed_alpha": round(signed_alpha, 4),
                            "verdict": verdict,
                            "direction": direction,
                            "regime": str(regime_context.get("regime") or "unknown").lower(),
                            "attribution_grid": _json_loads(row[14], []),
                            "attribution_mystical": _json_loads(row[15], []),
                            "attribution_noise": _json_loads(row[16], []),
                        }
                    )
                summary = self._summarize_backtest_metrics(metrics)
                run_row = conn.execute(
                    run_insert_sql,
                    {
                        "run_key": f"{variant}:{uuid4()}",
                        "strategy_variant": variant,
                        "horizon_label": horizon_label or "swing",
                        "target_universe": "hybrid",
                        "window_start": window_start,
                        "window_end": window_end,
                        "params_payload": _safe_json({"limit": limit}),
                        "summary_payload": _safe_json(summary),
                    },
                ).fetchone()
                if run_row:
                    run_id = int(run_row[0])
                    for item in metrics:
                        conn.execute(
                            result_insert_sql,
                            {
                                "backtest_run_id": run_id,
                                "result_key": item["result_key"],
                                "strategy_variant": variant,
                                "target_symbol": item["target_symbol"],
                                "as_of_date": item["as_of_date"],
                                "alpha_vs_benchmark": item["signed_alpha"],
                                "metrics_payload": _safe_json(
                                    {
                                        "prediction_id": item["prediction_id"],
                                        "signed_return": item["signed_return"],
                                        "signed_alpha": item["signed_alpha"],
                                        "verdict": item["verdict"],
                                        "direction": item["direction"],
                                    }
                                ),
                                "attribution_grid": _safe_json(item["attribution_grid"]),
                                "attribution_mystical": _safe_json(item["attribution_mystical"]),
                                "attribution_noise": _safe_json(item["attribution_noise"]),
                            },
                        )
                    runs.append({"run_id": run_id, "strategy_variant": variant, "summary": summary})
        return {"runs": runs, "count": len(runs)}

    def get_backtest_summary(self, limit: int = 12) -> dict[str, Any]:
        sql = text(
            f"""
            SELECT strategy_variant, started_at, summary_payload
            FROM {self.schema}.backtest_run
            ORDER BY started_at DESC
            LIMIT :limit
            """
        )
        latest_by_variant: dict[str, Any] = {}
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"limit": limit}).fetchall()
        history = []
        for row in rows:
            payload = _json_loads(row[2], {})
            item = {
                "strategy_variant": row[0],
                "started_at": row[1].isoformat() if row[1] else None,
                "summary": payload,
            }
            history.append(item)
            latest_by_variant.setdefault(row[0], item)
        return {"latest_by_variant": latest_by_variant, "history": history}

    def list_backtest_results(self, *, strategy_variant: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        filters = []
        params: dict[str, Any] = {"limit": limit}
        if strategy_variant:
            filters.append("br.strategy_variant = :strategy_variant")
            params["strategy_variant"] = strategy_variant
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        sql = text(
            f"""
            SELECT
                br.strategy_variant,
                br.target_symbol,
                br.as_of_date,
                br.alpha_vs_benchmark,
                br.metrics_payload,
                br.attribution_grid,
                br.attribution_mystical,
                br.attribution_noise,
                br.created_at
            FROM {self.schema}.backtest_result br
            {where_sql}
            ORDER BY br.created_at DESC
            LIMIT :limit
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql, params).fetchall()
        return [
            {
                "strategy_variant": row[0],
                "target_symbol": row[1],
                "as_of_date": str(row[2]) if row[2] else None,
                "alpha_vs_benchmark": round(float(row[3] or 0.0), 4),
                "metrics": _json_loads(row[4], {}),
                "attribution": {
                    "grid": _json_loads(row[5], []),
                    "mystical": _json_loads(row[6], []),
                    "noise": _json_loads(row[7], []),
                },
                "created_at": row[8].isoformat() if row[8] else None,
            }
            for row in rows
        ]

    def _weight_proposal_row_to_dict(self, row: Any) -> dict[str, Any]:
        proposal_status = str(row[9] or "pending_review")
        decision = row[11]
        return {
            "weight_proposal_id": row[0],
            "review_run_id": row[1],
            "based_on_prediction_count": int(row[2] or 0),
            "based_on_backtest_window": _json_loads(row[3], {}),
            "proposed_grid_weights": _json_loads(row[4], {}),
            "proposed_mystical_weights": _json_loads(row[5], {}),
            "reasoning_summary": row[6] or "",
            "confidence": float(row[7] or 0.0),
            "created_at": row[8].isoformat() if row[8] else None,
            "status": self._weight_proposal_effective_state(proposal_status, decision),
            "proposal_status": proposal_status,
            "approved_weight_version_id": row[10],
            "decision": decision,
            "decision_notes": row[12] or "",
            "approved_weight_version_key": row[13],
            "decided_by": row[14],
            "decided_at": row[15].isoformat() if row[15] else None,
        }

    def list_weight_proposals(self, *, status: str | None = None, limit: int = 20) -> list[dict[str, Any]]:
        sql = text(
            f"""
            SELECT
                wp.weight_proposal_id,
                wp.review_run_id,
                wp.based_on_prediction_count,
                wp.based_on_backtest_window,
                wp.proposed_grid_weights,
                wp.proposed_mystical_weights,
                wp.reasoning_summary,
                wp.confidence,
                wp.created_at,
                wp.status,
                COALESCE(dpd.approved_weight_version_id, wp.approved_weight_version_id) AS approved_weight_version_id,
                dpd.decision,
                dpd.notes,
                wv.version_key,
                dpd.decided_by,
                dpd.created_at
            FROM {self.schema}.weight_proposal wp
            LEFT JOIN LATERAL (
                SELECT decision, notes, decided_by, approved_weight_version_id, created_at
                FROM {self.schema}.weight_proposal_decision
                WHERE weight_proposal_id = wp.weight_proposal_id
                ORDER BY created_at DESC
                LIMIT 1
            ) dpd ON TRUE
            LEFT JOIN {self.schema}.weight_version wv
                ON wv.id = COALESCE(dpd.approved_weight_version_id, wp.approved_weight_version_id)
            ORDER BY wp.created_at DESC
            LIMIT :limit
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"limit": limit}).fetchall()
        proposals = [self._weight_proposal_row_to_dict(row) for row in rows]
        if status:
            proposals = [proposal for proposal in proposals if proposal["status"] == status]
        return proposals

    def get_latest_review(self) -> dict[str, Any] | None:
        sql = text(
            f"""
            SELECT
                review_key,
                provider_mode,
                model_name,
                based_on_prediction_count,
                based_on_backtest_window,
                input_payload,
                review_payload,
                status,
                created_at,
                id
            FROM {self.schema}.review_run
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(sql).fetchone()
        if not row:
            return None
        proposal = next(
            (item for item in self.list_weight_proposals(limit=20) if item["review_run_id"] == int(row[9] or 0)),
            None,
        )
        return {
            "review_key": row[0],
            "provider_mode": row[1],
            "model_name": row[2],
            "based_on_prediction_count": int(row[3] or 0),
            "based_on_backtest_window": _json_loads(row[4], {}),
            "input_payload": _json_loads(row[5], {}),
            "review": _json_loads(row[6], {}),
            "status": row[7],
            "created_at": row[8].isoformat() if row[8] else None,
            "proposal": proposal,
        }

    def approve_weight_proposal(
        self,
        weight_proposal_id: str,
        *,
        decided_by: str = "system",
        notes: str = "",
    ) -> dict[str, Any] | None:
        proposal = next(
            (item for item in self.list_weight_proposals(limit=100) if item["weight_proposal_id"] == weight_proposal_id),
            None,
        )
        if not proposal:
            return None
        if proposal["status"] != "pending_review":
            return proposal
        active = self.ensure_active_weight_version()
        version_key = f"astrogrid-v{uuid4().hex[:12]}"
        insert_weight_sql = text(
            f"""
            INSERT INTO {self.schema}.weight_version (
                version_key,
                status,
                grid_weights,
                mystical_weights,
                notes,
                approved_by,
                approved_at
            )
            VALUES (
                :version_key,
                'active',
                CAST(:grid_weights AS jsonb),
                CAST(:mystical_weights AS jsonb),
                :notes,
                :approved_by,
                NOW()
            )
            RETURNING id
            """
        )
        insert_decision_sql = text(
            f"""
            INSERT INTO {self.schema}.weight_proposal_decision (
                decision_key,
                weight_proposal_id,
                decision,
                decided_by,
                notes,
                approved_weight_version_id
            )
            VALUES (
                :decision_key,
                :weight_proposal_id,
                'approved',
                :decided_by,
                :notes,
                :approved_weight_version_id
            )
            RETURNING id
            """
        )
        with self.engine.begin() as conn:
            version_row = conn.execute(
                insert_weight_sql,
                {
                    "version_key": version_key,
                    "grid_weights": _safe_json(proposal["proposed_grid_weights"] or active["grid_weights"]),
                    "mystical_weights": _safe_json(proposal["proposed_mystical_weights"] or active["mystical_weights"]),
                    "notes": _compact_text(notes or proposal["reasoning_summary"] or "Approved AstroGrid weight proposal."),
                    "approved_by": _compact_text(decided_by, "system"),
                },
            ).fetchone()
            conn.execute(
                insert_decision_sql,
                {
                    "decision_key": f"proposal-decision-{uuid4()}",
                    "weight_proposal_id": weight_proposal_id,
                    "decided_by": _compact_text(decided_by, "system"),
                    "notes": _compact_text(notes),
                    "approved_weight_version_id": int(version_row[0]),
                },
            )
        return next(
            (item for item in self.list_weight_proposals(limit=100) if item["weight_proposal_id"] == weight_proposal_id),
            None,
        )

    def reject_weight_proposal(
        self,
        weight_proposal_id: str,
        *,
        decided_by: str = "system",
        notes: str = "",
    ) -> dict[str, Any] | None:
        proposal = next(
            (item for item in self.list_weight_proposals(limit=100) if item["weight_proposal_id"] == weight_proposal_id),
            None,
        )
        if not proposal:
            return None
        if proposal["status"] != "pending_review":
            return proposal
        sql = text(
            f"""
            INSERT INTO {self.schema}.weight_proposal_decision (
                decision_key,
                weight_proposal_id,
                decision,
                decided_by,
                notes
            )
            VALUES (
                :decision_key,
                :weight_proposal_id,
                'rejected',
                :decided_by,
                :notes
            )
            RETURNING id
            """
        )
        with self.engine.begin() as conn:
            conn.execute(
                sql,
                {
                    "decision_key": f"proposal-decision-{uuid4()}",
                    "weight_proposal_id": weight_proposal_id,
                    "decided_by": _compact_text(decided_by, "system"),
                    "notes": _compact_text(notes),
                },
            )
        return next(
            (item for item in self.list_weight_proposals(limit=100) if item["weight_proposal_id"] == weight_proposal_id),
            None,
        )

    def list_predictions(self, *, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        sql = text(
            f"""
            SELECT
                pr.prediction_id,
                pr.created_at,
                pr.as_of_ts,
                pr.horizon_label,
                pr.target_universe,
                pr.scoring_class,
                pr.target_symbols,
                pr.question,
                pr.call,
                pr.timing,
                pr.setup,
                pr.invalidation,
                pr.note,
                pr.seer_summary,
                pr.weight_version,
                pr.model_version,
                pr.live_or_local,
                pr.status,
                ps.verdict,
                pr.comparable_publish_status,
                pr.comparable_prediction_ref,
                pp.state,
                pp.summary,
                pp.dominant_grid_drivers,
                pp.dominant_mystical_drivers,
                pp.invalidation_rule,
                pp.feature_family_summary
            FROM {self.schema}.prediction_run pr
            LEFT JOIN {self.schema}.prediction_postmortem pp
                ON pp.prediction_run_id = pr.id
            LEFT JOIN {self.schema}.prediction_score ps
                ON ps.prediction_run_id = pr.id
            ORDER BY pr.created_at DESC
            LIMIT :limit OFFSET :offset
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"limit": limit, "offset": offset}).fetchall()
        return [self._prediction_row_to_dict(row) for row in rows]

    def list_postmortems(self, *, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        sql = text(
            f"""
            SELECT
                pr.prediction_id,
                pr.created_at,
                pr.horizon_label,
                pr.scoring_class,
                pr.target_symbols,
                pr.call,
                pr.timing,
                pr.invalidation,
                pr.status,
                pp.state,
                pp.summary,
                pp.dominant_grid_drivers,
                pp.dominant_mystical_drivers,
                pp.invalidation_rule,
                pp.feature_family_summary
            FROM {self.schema}.prediction_postmortem pp
            JOIN {self.schema}.prediction_run pr
                ON pr.id = pp.prediction_run_id
            ORDER BY pp.created_at DESC
            LIMIT :limit OFFSET :offset
            """
        )
        with self.engine.connect() as conn:
            rows = conn.execute(sql, {"limit": limit, "offset": offset}).fetchall()
        return [self._postmortem_row_to_dict(row) for row in rows]

    def get_prediction(self, prediction_id: str) -> dict[str, Any] | None:
        sql = text(
            f"""
            SELECT
                pr.prediction_id,
                pr.created_at,
                pr.as_of_ts,
                pr.horizon_label,
                pr.target_universe,
                pr.scoring_class,
                pr.target_symbols,
                pr.question,
                pr.call,
                pr.timing,
                pr.setup,
                pr.invalidation,
                pr.note,
                pr.seer_summary,
                pr.market_overlay_snapshot,
                pr.mystical_feature_payload,
                pr.grid_feature_payload,
                pr.weight_version,
                pr.model_version,
                pr.live_or_local,
                pr.status,
                ps.verdict,
                pr.comparable_publish_status,
                pr.comparable_prediction_ref,
                pp.state,
                pp.summary,
                pp.dominant_grid_drivers,
                pp.dominant_mystical_drivers,
                pp.invalidation_rule,
                pp.feature_family_summary,
                pp.raw_payload
            FROM {self.schema}.prediction_run pr
            LEFT JOIN {self.schema}.prediction_postmortem pp
                ON pp.prediction_run_id = pr.id
            LEFT JOIN {self.schema}.prediction_score ps
                ON ps.prediction_run_id = pr.id
            WHERE pr.prediction_id = :prediction_id
            LIMIT 1
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(sql, {"prediction_id": prediction_id}).fetchone()
        return self._prediction_row_to_dict(row, detailed=True) if row else None

    def _prediction_row_to_dict(self, row: Any, detailed: bool = False) -> dict[str, Any]:
        data = {
            "prediction_id": row[0],
            "created_at": row[1].isoformat() if row[1] else None,
            "as_of_ts": row[2].isoformat() if row[2] else None,
            "horizon": row[3],
            "target_universe": row[4],
            "scoring_class": row[5],
            "target_symbols": _json_loads(row[6], []),
            "question": row[7],
            "call": row[8],
            "timing": row[9],
            "setup": row[10],
            "invalidation": row[11],
            "note": row[12],
            "seer_summary": row[13],
            "weight_version": row[17 if detailed else 14],
            "model_version": row[18 if detailed else 15],
            "live_or_local": row[19 if detailed else 16],
            "status": row[21 if detailed else 18] or row[20 if detailed else 17],
            "oracle_publish": {
                "status": row[22 if detailed else 19],
                "oracle_prediction_id": row[23 if detailed else 20],
            },
            "postmortem": {
                "state": "scored" if (row[21 if detailed else 18] or row[20 if detailed else 17]) in {"hit", "miss", "partial", "invalidated", "expired"} else row[24 if detailed else 21],
                "summary": row[25 if detailed else 22],
                "dominant_grid_drivers": _json_loads(row[26 if detailed else 23], []),
                "dominant_mystical_drivers": _json_loads(row[27 if detailed else 24], []),
                "invalidation_rule": row[28 if detailed else 25],
                "feature_family_summary": _json_loads(row[29 if detailed else 26], {}),
            },
        }
        if detailed:
            data["market_overlay_snapshot"] = _json_loads(row[14], {})
            data["mystical_feature_payload"] = _json_loads(row[15], {})
            data["grid_feature_payload"] = _json_loads(row[16], {})
            data["postmortem"]["raw_payload"] = _json_loads(row[30], {})
        return data

    def _postmortem_row_to_dict(self, row: Any) -> dict[str, Any]:
        return {
            "prediction_id": row[0],
            "created_at": row[1].isoformat() if row[1] else None,
            "horizon": row[2],
            "scoring_class": row[3],
            "target_symbols": _json_loads(row[4], []),
            "call": row[5],
            "timing": row[6],
            "invalidation": row[7],
            "status": row[8],
            "postmortem": {
                "state": row[9],
                "summary": row[10],
                "dominant_grid_drivers": _json_loads(row[11], []),
                "dominant_mystical_drivers": _json_loads(row[12], []),
                "invalidation_rule": row[13],
                "feature_family_summary": _json_loads(row[14], {}),
            },
        }

    def _build_prediction_score(
        self,
        *,
        prediction_id: str,
        call: str,
        setup: str,
        invalidation: str,
        market_overlay: dict[str, Any],
        mystical_payload: dict[str, Any],
        grid_payload: dict[str, Any],
        target_symbols: list[str],
        start_date: date,
        evaluation_date: date,
    ) -> dict[str, Any] | None:
        symbols = [symbol for symbol in target_symbols if symbol in _HYBRID_LOOKUP_BY_SYMBOL] or ["SPY"]
        realized_returns = []
        mfe_values = []
        mae_values = []
        for symbol in symbols:
            entry_price = self._get_symbol_price_at_date(symbol, start_date)
            exit_price = self._get_symbol_price_at_date(symbol, evaluation_date)
            if entry_price is None or exit_price is None or entry_price == 0:
                continue
            realized = (float(exit_price) - float(entry_price)) / float(entry_price)
            realized_returns.append(realized)
            path = self._load_price_path(symbol, start_date, evaluation_date)
            if path:
                rel_path = [((price - float(entry_price)) / float(entry_price)) for _, price in path]
                mfe_values.append(max(rel_path))
                mae_values.append(min(rel_path))
        if not realized_returns:
            return None
        realized_return = sum(realized_returns) / len(realized_returns)
        benchmark_symbol, benchmark_return = self._benchmark_return(symbols, start_date, evaluation_date)
        alpha = realized_return - benchmark_return if benchmark_return is not None else None
        direction = _prediction_direction(" ".join([call, setup]))
        sign = _direction_sign(direction)
        signed_return = realized_return * sign if sign else realized_return
        horizon_label = "macro" if (evaluation_date - start_date).days >= 30 else "swing"
        verdict = _effective_verdict(direction, realized_return, horizon_label=horizon_label)
        invalid_status = _invalidation_status(verdict, signed_return)
        grid_attr = self._attribution_grid(market_overlay, grid_payload)
        mystical_attr = self._attribution_mystical(mystical_payload)
        noise_attr = self._attribution_noise(grid_attr, mystical_attr, benchmark_symbol, benchmark_return)
        hit_threshold, partial_threshold = _horizon_thresholds(horizon_label)
        return {
            "benchmark_symbol": benchmark_symbol,
            "realized_return": round(realized_return, 6),
            "benchmark_return": round(benchmark_return, 6) if benchmark_return is not None else None,
            "alpha_vs_benchmark": round(alpha, 6) if alpha is not None else None,
            "verdict": verdict,
            "invalidation_status": invalid_status,
            "max_favorable_excursion": round(max(mfe_values), 6) if mfe_values else None,
            "max_adverse_excursion": round(min(mae_values), 6) if mae_values else None,
            "regime_context": {
                "regime": (market_overlay.get("regime") or {}).get("state") if isinstance(market_overlay.get("regime"), Mapping) else None,
                "thesis": (market_overlay.get("thesis") or {}).get("stance") if isinstance(market_overlay.get("thesis"), Mapping) else None,
            },
            "attribution_grid": grid_attr,
            "attribution_mystical": mystical_attr,
            "attribution_noise": noise_attr,
            "raw_payload": {
                "prediction_id": prediction_id,
                "symbols": symbols,
                "start_date": start_date.isoformat(),
                "evaluation_date": evaluation_date.isoformat(),
                "direction": direction,
                "signed_return": round(signed_return, 6),
                "horizon_label": horizon_label,
                "hit_threshold": hit_threshold,
                "partial_threshold": partial_threshold,
                "neutral_move_band": _NEUTRAL_MOVE_BAND,
            },
        }

    def _load_price_path(self, symbol: str, start_date: date, evaluation_date: date) -> list[tuple[date, float]]:
        feature_name = _PRICE_FEATURE_BY_SYMBOL.get(symbol.upper())
        if feature_name:
            sql = text(
                """
                SELECT rs.obs_date, rs.value
                FROM feature_registry fr
                JOIN resolved_series rs ON rs.feature_id = fr.id
                WHERE fr.name = :feature_name
                  AND rs.obs_date BETWEEN :start_date AND :evaluation_date
                ORDER BY rs.obs_date
                """
            )
            try:
                with self.engine.connect() as conn:
                    rows = conn.execute(
                        sql,
                        {
                            "feature_name": feature_name,
                            "start_date": start_date,
                            "evaluation_date": evaluation_date,
                        },
                    ).fetchall()
                return [(row[0], float(row[1])) for row in rows if row[0] is not None and row[1] is not None]
            except Exception:
                pass
        lookup_ticker = _HYBRID_LOOKUP_BY_SYMBOL.get(symbol.upper(), symbol)
        sql = text(
            """
            SELECT obs_date, value
            FROM raw_series
            WHERE series_id = :series_id
              AND obs_date BETWEEN :start_date AND :evaluation_date
              AND pull_status = 'SUCCESS'
            ORDER BY obs_date
            """
        )
        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    sql,
                    {
                        "series_id": f"YF:{lookup_ticker}:close",
                        "start_date": start_date,
                        "evaluation_date": evaluation_date,
                    },
                ).fetchall()
            return [(row[0], float(row[1])) for row in rows if row[0] is not None and row[1] is not None]
        except Exception:
            return []

    def _benchmark_return(self, symbols: list[str], start_date: date, evaluation_date: date) -> tuple[str, float | None]:
        resolved_symbols = [symbol for symbol in symbols if symbol in _UNIVERSE_BY_SYMBOL]
        benchmark_candidates = {
            str(_UNIVERSE_BY_SYMBOL[symbol].get("benchmark_symbol") or "")
            for symbol in resolved_symbols
        }
        benchmark_candidates.discard("")
        if len(benchmark_candidates) == 1:
            benchmark_symbol = next(iter(benchmark_candidates))
            benchmark_members = [benchmark_symbol]
        else:
            benchmark_symbol = "HYBRID"
            benchmark_members = ["BTC", "SPY"]
        returns = []
        for symbol in benchmark_members:
            entry = self._get_symbol_price_at_date(symbol, start_date)
            exit_price = self._get_symbol_price_at_date(symbol, evaluation_date)
            if entry is None or exit_price is None or entry == 0:
                continue
            returns.append((float(exit_price) - float(entry)) / float(entry))
        return benchmark_symbol, (sum(returns) / len(returns) if returns else None)

    def _get_symbol_price_at_date(self, symbol: str, target_date: date) -> float | None:
        symbol = str(symbol or "").upper()
        feature_name = _PRICE_FEATURE_BY_SYMBOL.get(symbol)
        if feature_name:
            sql = text(
                """
                SELECT rs.value
                FROM feature_registry fr
                JOIN resolved_series rs ON rs.feature_id = fr.id
                WHERE fr.name = :feature_name
                  AND rs.obs_date <= :target_date
                ORDER BY rs.obs_date DESC
                LIMIT 1
                """
            )
            try:
                with self.engine.connect() as conn:
                    row = conn.execute(sql, {"feature_name": feature_name, "target_date": target_date}).fetchone()
                if row and row[0] is not None:
                    return float(row[0])
            except Exception:
                pass
        lookup_ticker = _HYBRID_LOOKUP_BY_SYMBOL.get(symbol, symbol)
        sql = text(
            """
            SELECT value
            FROM raw_series
            WHERE series_id = :series_id
              AND obs_date <= :target_date
              AND pull_status = 'SUCCESS'
            ORDER BY obs_date DESC
            LIMIT 1
            """
        )
        try:
            with self.engine.connect() as conn:
                row = conn.execute(
                    sql,
                    {"series_id": f"YF:{lookup_ticker}:close", "target_date": target_date},
                ).fetchone()
            if row and row[0] is not None:
                return float(row[0])
        except Exception:
            return None
        return None

    def _scored_prediction_date_range(self, *, horizon_label: str | None = None) -> tuple[date | None, date | None] | None:
        filters = []
        params: dict[str, Any] = {}
        if horizon_label in {"macro", "swing"}:
            filters.append("pr.horizon_label = :horizon_label")
            params["horizon_label"] = horizon_label
        where_sql = f"WHERE {' AND '.join(filters)}" if filters else ""
        sql = text(
            f"""
            SELECT min(pr.as_of_ts::date), max(pr.as_of_ts::date)
            FROM {self.schema}.prediction_score ps
            JOIN {self.schema}.prediction_run pr ON pr.id = ps.prediction_run_id
            {where_sql}
            """
        )
        with self.engine.connect() as conn:
            row = conn.execute(sql, params).fetchone()
        if not row:
            return None
        return row[0], row[1]

    def _attribution_grid(self, market_overlay: dict[str, Any], grid_payload: dict[str, Any]) -> list[str]:
        labels = []
        regime = market_overlay.get("regime") if isinstance(market_overlay.get("regime"), Mapping) else {}
        thesis = market_overlay.get("thesis") if isinstance(market_overlay.get("thesis"), Mapping) else {}
        sector = market_overlay.get("sector_detail") if isinstance(market_overlay.get("sector_detail"), Mapping) else {}
        if regime.get("state"):
            labels.append(f"regime:{regime['state']}")
        if thesis.get("stance"):
            labels.append(f"thesis:{thesis['stance']}")
        if sector.get("sector"):
            labels.append(f"sector:{sector['sector']}")
        scorecard = grid_payload.get("scorecard") if isinstance(grid_payload.get("scorecard"), Mapping) else {}
        for leader in list(scorecard.get("leaders") or [])[:2]:
            if isinstance(leader, Mapping) and leader.get("symbol"):
                labels.append(f"leader:{leader['symbol']}")
        return labels[:6]

    def _attribution_mystical(self, mystical_payload: dict[str, Any]) -> list[str]:
        labels = []
        seer = mystical_payload.get("seer") if isinstance(mystical_payload.get("seer"), Mapping) else {}
        snapshot = mystical_payload.get("snapshot") if isinstance(mystical_payload.get("snapshot"), Mapping) else {}
        lunar = snapshot.get("lunar") if isinstance(snapshot.get("lunar"), Mapping) else {}
        nakshatra = snapshot.get("nakshatra") if isinstance(snapshot.get("nakshatra"), Mapping) else {}
        signals = snapshot.get("signals") if isinstance(snapshot.get("signals"), Mapping) else {}
        void_of_course = snapshot.get("void_of_course") if isinstance(snapshot.get("void_of_course"), Mapping) else {}
        retrograde_planets = snapshot.get("retrograde_planets") if isinstance(snapshot.get("retrograde_planets"), list) else []
        signal_field = snapshot.get("signal_field") if isinstance(snapshot.get("signal_field"), list) else []
        canonical_ephemeris = snapshot.get("canonical_ephemeris") if isinstance(snapshot.get("canonical_ephemeris"), Mapping) else {}
        if seer.get("prediction"):
            labels.append(f"seer:{_prediction_direction(seer['prediction'])}")
        if lunar.get("phase_name"):
            labels.append(f"moon:{lunar['phase_name']}")
        if nakshatra.get("nakshatra_name"):
            labels.append(f"nakshatra:{nakshatra['nakshatra_name']}")
        if nakshatra.get("pada") is not None:
            labels.append(f"pada:{nakshatra['pada']}")
        if void_of_course.get("is_void"):
            labels.append("void:active")
        if signals.get("planetaryStress") is not None:
            labels.append(f"stress:{int(signals['planetaryStress'])}")
        if signals.get("retrogradeCount") is not None:
            labels.append(f"retrograde:{int(signals['retrogradeCount'])}")
        if signals.get("solarGeomagneticStatus"):
            labels.append(f"geomagnetic:{str(signals['solarGeomagneticStatus']).lower()}")
        if signals.get("nakshatraQuality"):
            labels.append(f"nakshatra_quality:{signals['nakshatraQuality']}")
        if canonical_ephemeris.get("ephemeris_phase_bucket") is not None:
            labels.append(f"phase_bucket:{int(canonical_ephemeris['ephemeris_phase_bucket'])}")
        if canonical_ephemeris.get("ephemeris_tithi_index") is not None:
            labels.append(f"tithi:{int(canonical_ephemeris['ephemeris_tithi_index'])}")
        if canonical_ephemeris.get("ephemeris_hard_aspect_count") is not None:
            labels.append(f"hard_aspects:{int(canonical_ephemeris['ephemeris_hard_aspect_count'])}")
        if canonical_ephemeris.get("ephemeris_soft_aspect_count") is not None:
            labels.append(f"soft_aspects:{int(canonical_ephemeris['ephemeris_soft_aspect_count'])}")
        for signal in signal_field[:2]:
            if isinstance(signal, Mapping) and signal.get("key"):
                labels.append(f"signal:{signal['key']}")
        if retrograde_planets:
            for body in retrograde_planets[:2]:
                if isinstance(body, Mapping) and body.get("name"):
                    labels.append(f"rx:{body['name']}")
        return list(dict.fromkeys(labels))[:12]

    def _attribution_noise(
        self,
        grid_labels: list[str],
        mystical_labels: list[str],
        benchmark_symbol: str,
        benchmark_return: float | None,
    ) -> list[str]:
        noise = []
        if not grid_labels:
            noise.append("grid:thin")
        if not mystical_labels:
            noise.append("mystical:thin")
        if benchmark_return is None:
            noise.append(f"benchmark:{benchmark_symbol.lower()}:missing")
        return noise[:4]

    def _variant_direction(
        self,
        *,
        variant: str,
        call: str,
        setup: str,
        note: str,
        market_overlay: dict[str, Any],
        mystical_payload: dict[str, Any],
        grid_payload: dict[str, Any],
    ) -> str:
        if variant == "grid_plus_mystical":
            return _prediction_direction(" ".join([call, setup, note]))
        if variant == "grid_only":
            regime = market_overlay.get("regime") if isinstance(market_overlay.get("regime"), Mapping) else {}
            thesis = market_overlay.get("thesis") if isinstance(market_overlay.get("thesis"), Mapping) else {}
            scorecard = grid_payload.get("scorecard") if isinstance(grid_payload.get("scorecard"), Mapping) else {}
            joined = " ".join(
                [
                    str(regime.get("state") or ""),
                    str(thesis.get("stance") or ""),
                    " ".join(str(item.get("bias") or "") for item in list(scorecard.get("leaders") or [])[:2] if isinstance(item, Mapping)),
                ]
            )
            return _prediction_direction(joined)
        seer = mystical_payload.get("seer") if isinstance(mystical_payload.get("seer"), Mapping) else {}
        snapshot = mystical_payload.get("snapshot") if isinstance(mystical_payload.get("snapshot"), Mapping) else {}
        lunar = snapshot.get("lunar") if isinstance(snapshot.get("lunar"), Mapping) else {}
        joined = " ".join([str(seer.get("prediction") or ""), str(seer.get("reading") or ""), str(lunar.get("phase_name") or "")])
        return _prediction_direction(joined)

    def _summarize_backtest_metrics(self, metrics: list[dict[str, Any]]) -> dict[str, Any]:
        def _summary(items: list[dict[str, Any]]) -> dict[str, Any]:
            total_local = len(items)
            hits_local = sum(1 for item in items if item["verdict"] == "hit")
            misses_local = sum(1 for item in items if item["verdict"] == "miss")
            partials_local = sum(1 for item in items if item["verdict"] == "partial")
            avg_signed_return_local = (
                sum(float(item["signed_return"]) for item in items) / total_local if total_local else 0.0
            )
            avg_signed_alpha_local = (
                sum(float(item["signed_alpha"]) for item in items) / total_local if total_local else 0.0
            )
            return {
                "total_predictions": total_local,
                "hits": hits_local,
                "misses": misses_local,
                "partials": partials_local,
                "accuracy": round((hits_local + (partials_local * 0.5)) / total_local, 4) if total_local else 0.0,
                "avg_signed_return": round(avg_signed_return_local, 4),
                "avg_signed_alpha": round(avg_signed_alpha_local, 4),
            }

        total = len(metrics)
        by_regime_counter = Counter(
            str(item.get("regime") or "unknown").lower()
            for item in metrics
            if item.get("regime")
        )
        by_group_counter = Counter(
            str(item.get("target_group") or "unknown").lower()
            for item in metrics
            if item.get("target_group")
        )
        by_regime = {
            key: _summary([item for item in metrics if str(item.get("regime") or "unknown").lower() == key])
            for key, _ in by_regime_counter.most_common()
        }
        by_group = {
            key: _summary([item for item in metrics if str(item.get("target_group") or "unknown").lower() == key])
            for key, _ in by_group_counter.most_common()
        }
        summary = _summary(metrics)
        summary["by_regime"] = by_regime
        summary["by_group"] = by_group
        summary["dominant_regime"] = next(iter(by_regime), None)
        summary["dominant_group"] = next(iter(by_group), None)
        summary["total_predictions"] = total
        return summary
