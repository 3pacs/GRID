"""
AstroGrid persistence helpers.

AstroGrid writes its derived state into the dedicated ``astrogrid`` schema.
Shared GRID tables remain upstream-only inputs.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, time, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from config import settings


def _safe_json(data: Any) -> str:
    return json.dumps(data, default=str)


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

                seer_payload = response_payload.get("seer") or {}
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
