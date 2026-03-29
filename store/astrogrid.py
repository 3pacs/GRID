"""
AstroGrid persistence helpers.

AstroGrid writes its derived state into the dedicated ``astrogrid`` schema.
Shared GRID tables remain upstream-only inputs.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta, timezone
from typing import Any
from uuid import uuid4

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import settings

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

_HYBRID_LOOKUP_BY_SYMBOL = {
    "BTC": "BTC",
    "ETH": "ETH",
    "SOL": "SOL",
    "AAPL": "AAPL",
    "MSFT": "MSFT",
    "GOOGL": "GOOGL",
    "GOOG": "GOOG",
    "NVDA": "NVDA",
    "META": "META",
    "SPY": "SPY",
    "QQQ": "QQQ",
    "TLT": "TLT",
    "DXY": "UUP",
    "GLD": "GLD",
    "CL": "CL=F",
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

    def score_predictions(
        self,
        *,
        as_of_date: date | None = None,
        limit: int = 100,
        prediction_ids: list[str] | None = None,
    ) -> dict[str, Any]:
        from trading.options_tracker import _get_price_at_date

        evaluation_date = as_of_date or date.today()
        filters = []
        params: dict[str, Any] = {"limit": limit}
        if prediction_ids:
            filters.append("pr.prediction_id = ANY(:prediction_ids)")
            params["prediction_ids"] = prediction_ids
        where_sql = f"AND {' AND '.join(filters)}" if filters else ""
        sql = text(
            f"""
            SELECT
                pr.id,
                pr.prediction_id,
                pr.as_of_ts,
                pr.horizon_label,
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
            {where_sql}
            ORDER BY pr.created_at ASC
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
                target_symbols = [str(symbol).upper() for symbol in _json_loads(row[4], [])]
                score = self._build_prediction_score(
                    _get_price_at_date=_get_price_at_date,
                    prediction_id=row[1],
                    call=row[5],
                    setup=row[6],
                    invalidation=row[7],
                    market_overlay=_json_loads(row[8], {}),
                    mystical_payload=_json_loads(row[9], {}),
                    grid_payload=_json_loads(row[10], {}),
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
                ps.attribution_noise
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
                    metrics.append(
                        {
                            "result_key": f"{variant}:{row[1]}",
                            "target_symbol": (_json_loads(row[5], []) or ["HYBRID"])[0],
                            "as_of_date": row[2],
                            "prediction_id": row[1],
                            "signed_return": round(signed_return, 4),
                            "signed_alpha": round(signed_alpha, 4),
                            "verdict": verdict,
                            "direction": direction,
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

    def list_predictions(self, *, limit: int = 20, offset: int = 0) -> list[dict[str, Any]]:
        sql = text(
            f"""
            SELECT
                pr.prediction_id,
                pr.created_at,
                pr.as_of_ts,
                pr.horizon_label,
                pr.target_universe,
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
            "target_symbols": _json_loads(row[5], []),
            "question": row[6],
            "call": row[7],
            "timing": row[8],
            "setup": row[9],
            "invalidation": row[10],
            "note": row[11],
            "seer_summary": row[12],
            "weight_version": row[16 if detailed else 13],
            "model_version": row[17 if detailed else 14],
            "live_or_local": row[18 if detailed else 15],
            "status": row[20 if detailed else 17] or row[19 if detailed else 16],
            "oracle_publish": {
                "status": row[21 if detailed else 18],
                "oracle_prediction_id": row[22 if detailed else 19],
            },
            "postmortem": {
                "state": "scored" if (row[20 if detailed else 17] or row[19 if detailed else 16]) in {"hit", "miss", "partial", "invalidated", "expired"} else row[23 if detailed else 20],
                "summary": row[24 if detailed else 21],
                "dominant_grid_drivers": _json_loads(row[25 if detailed else 22], []),
                "dominant_mystical_drivers": _json_loads(row[26 if detailed else 23], []),
                "invalidation_rule": row[27 if detailed else 24],
                "feature_family_summary": _json_loads(row[28 if detailed else 25], {}),
            },
        }
        if detailed:
            data["market_overlay_snapshot"] = _json_loads(row[13], {})
            data["mystical_feature_payload"] = _json_loads(row[14], {})
            data["grid_feature_payload"] = _json_loads(row[15], {})
            data["postmortem"]["raw_payload"] = _json_loads(row[29], {})
        return data

    def _postmortem_row_to_dict(self, row: Any) -> dict[str, Any]:
        return {
            "prediction_id": row[0],
            "created_at": row[1].isoformat() if row[1] else None,
            "horizon": row[2],
            "target_symbols": _json_loads(row[3], []),
            "call": row[4],
            "timing": row[5],
            "invalidation": row[6],
            "status": row[7],
            "postmortem": {
                "state": row[8],
                "summary": row[9],
                "dominant_grid_drivers": _json_loads(row[10], []),
                "dominant_mystical_drivers": _json_loads(row[11], []),
                "invalidation_rule": row[12],
                "feature_family_summary": _json_loads(row[13], {}),
            },
        }

    def _build_prediction_score(
        self,
        *,
        _get_price_at_date,
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
            lookup = _HYBRID_LOOKUP_BY_SYMBOL.get(symbol, symbol)
            entry_price = _get_price_at_date(self.engine, lookup, start_date)
            exit_price = _get_price_at_date(self.engine, lookup, evaluation_date)
            if entry_price is None or exit_price is None or entry_price == 0:
                continue
            realized = (float(exit_price) - float(entry_price)) / float(entry_price)
            realized_returns.append(realized)
            path = self._load_price_path(lookup, start_date, evaluation_date)
            if path:
                rel_path = [((price - float(entry_price)) / float(entry_price)) for _, price in path]
                mfe_values.append(max(rel_path))
                mae_values.append(min(rel_path))
        if not realized_returns:
            return None
        realized_return = sum(realized_returns) / len(realized_returns)
        benchmark_symbol, benchmark_return = self._benchmark_return(_get_price_at_date, symbols, start_date, evaluation_date)
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

    def _load_price_path(self, lookup_ticker: str, start_date: date, evaluation_date: date) -> list[tuple[date, float]]:
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

    def _benchmark_return(self, _get_price_at_date, symbols: list[str], start_date: date, evaluation_date: date) -> tuple[str, float | None]:
        if symbols and all(symbol in {"BTC", "ETH", "SOL"} for symbol in symbols):
            benchmark_symbol = "BTC"
            benchmark_members = ["BTC"]
        elif symbols and all(symbol in {"AAPL", "MSFT", "GOOGL", "GOOG", "NVDA", "META"} for symbol in symbols):
            benchmark_symbol = "QQQ"
            benchmark_members = ["QQQ"]
        elif symbols and all(symbol in {"SPY", "QQQ", "TLT", "DXY", "GLD", "CL"} for symbol in symbols):
            benchmark_symbol = "SPY"
            benchmark_members = ["SPY"]
        else:
            benchmark_symbol = "HYBRID"
            benchmark_members = ["BTC", "SPY"]
        returns = []
        for symbol in benchmark_members:
            lookup = _HYBRID_LOOKUP_BY_SYMBOL.get(symbol, symbol)
            entry = _get_price_at_date(self.engine, lookup, start_date)
            exit_price = _get_price_at_date(self.engine, lookup, evaluation_date)
            if entry is None or exit_price is None or entry == 0:
                continue
            returns.append((float(exit_price) - float(entry)) / float(entry))
        return benchmark_symbol, (sum(returns) / len(returns) if returns else None)

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
        if seer.get("prediction"):
            labels.append(f"seer:{_prediction_direction(seer['prediction'])}")
        if lunar.get("phase_name"):
            labels.append(f"moon:{lunar['phase_name']}")
        if nakshatra.get("nakshatra_name"):
            labels.append(f"nakshatra:{nakshatra['nakshatra_name']}")
        return labels[:6]

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
        total = len(metrics)
        hits = sum(1 for item in metrics if item["verdict"] == "hit")
        misses = sum(1 for item in metrics if item["verdict"] == "miss")
        partials = sum(1 for item in metrics if item["verdict"] == "partial")
        avg_signed_return = sum(float(item["signed_return"]) for item in metrics) / total if total else 0.0
        avg_signed_alpha = sum(float(item["signed_alpha"]) for item in metrics) / total if total else 0.0
        return {
            "total_predictions": total,
            "hits": hits,
            "misses": misses,
            "partials": partials,
            "accuracy": round((hits + (partials * 0.5)) / total, 4) if total else 0.0,
            "avg_signed_return": round(avg_signed_return, 4),
            "avg_signed_alpha": round(avg_signed_alpha, 4),
        }
