"""
AstroGrid persistence helpers.

AstroGrid writes its derived state into the dedicated ``astrogrid`` schema.
Shared GRID tables remain upstream-only inputs.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import date, datetime, time, timezone
from typing import Any
from uuid import uuid4

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import settings


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
            "status": row[19 if detailed else 16],
            "oracle_publish": {
                "status": row[20 if detailed else 17],
                "oracle_prediction_id": row[21 if detailed else 18],
            },
            "postmortem": {
                "state": row[22 if detailed else 19],
                "summary": row[23 if detailed else 20],
                "dominant_grid_drivers": _json_loads(row[24 if detailed else 21], []),
                "dominant_mystical_drivers": _json_loads(row[25 if detailed else 22], []),
                "invalidation_rule": row[26 if detailed else 23],
                "feature_family_summary": _json_loads(row[27 if detailed else 24], {}),
            },
        }
        if detailed:
            data["market_overlay_snapshot"] = _json_loads(row[13], {})
            data["mystical_feature_payload"] = _json_loads(row[14], {})
            data["grid_feature_payload"] = _json_loads(row[15], {})
            data["postmortem"]["raw_payload"] = _json_loads(row[28], {})
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
