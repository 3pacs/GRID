"""
GRID Oracle — Model Factory.

Registers, mutates, queries, and retires Oracle models. Each model is
described by a ModelSpec with signal subscriptions serialised as JSONB.

Feature flag: GRID_SIGNAL_REGISTRY=1 enables signal_registry path.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from oracle.signal_aggregator import WeightConfig, WeightMode


def _signal_registry_enabled() -> bool:
    return os.getenv("GRID_SIGNAL_REGISTRY", "0") == "1"


_DEFAULT_SIGNAL_SOURCES: dict[str, list[str]] = {
    "flow_momentum":     ["feature:equity", "feature:flows", "feature:breadth", "feature:vol", "flow_thesis", "dollar_flows"],
    "regime_contrarian": ["feature:rates", "feature:credit", "feature:vol", "feature:macro", "cross_reference"],
    "options_flow":      ["feature:sentiment", "feature:vol", "feature:equity", "pattern_engine"],
    "cross_asset":       ["feature:rates", "feature:fx", "feature:commodity", "feature:credit", "feature:equity"],
    "news_energy":       ["feature:sentiment", "feature:alternative", "feature:equity", "news_intel"],
}


@dataclass
class ModelSpec:
    name: str
    version: str = "1.0"
    description: str = ""
    signal_sources: list[str] = field(default_factory=list)
    signal_filters: dict[str, Any] = field(default_factory=dict)
    weight_config: WeightConfig = field(default_factory=lambda: WeightConfig(mode=WeightMode.EQUAL))
    prediction_type: str = "directional"
    target_horizon_days: int = 7
    min_signals: int = 3
    active: bool = True
    created_by: str = "human"
    parent_model: str | None = None

    def to_jsonb_dict(self) -> dict[str, Any]:
        wc = self.weight_config
        return {
            "signal_sources": self.signal_sources,
            "signal_filters": self.signal_filters,
            "weight_config": {
                "mode": wc.mode,
                "trust_decay_half_life_days": wc.trust_decay_half_life_days,
                "min_weight": wc.min_weight,
                "max_weight": wc.max_weight,
                "family_weights": wc.family_weights,
            },
        }


class ModelFactory:

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._ensure_columns()

    # Whitelist of allowed column names for DDL — prevents injection via identifier
    _ALLOWED_COLUMNS = {
        "signal_sources", "signal_filters", "weight_config_json",
        "prediction_type_col", "target_horizon_days", "min_signals",
        "active", "created_by", "parent_model",
    }

    def _ensure_columns(self) -> None:
        import re
        _IDENT_RE = re.compile(r"^[a-z_][a-z0-9_]*$")
        cols = [
            ("signal_sources", "JSONB"),
            ("signal_filters", "JSONB"),
            ("weight_config_json", "JSONB"),
            ("prediction_type_col", "TEXT DEFAULT 'directional'"),
            ("target_horizon_days", "INTEGER DEFAULT 7"),
            ("min_signals", "INTEGER DEFAULT 3"),
            ("active", "BOOLEAN DEFAULT TRUE"),
            ("created_by", "TEXT DEFAULT 'human'"),
            ("parent_model", "TEXT"),
        ]
        with self.engine.begin() as conn:
            for col_name, col_def in cols:
                if col_name not in self._ALLOWED_COLUMNS or not _IDENT_RE.match(col_name):
                    raise ValueError(f"Blocked DDL for unwhitelisted column: {col_name}")
                conn.execute(text(f"ALTER TABLE oracle_models ADD COLUMN IF NOT EXISTS {col_name} {col_def}"))

    def create_model(self, spec: ModelSpec) -> str:
        if not spec.name or not spec.name.strip():
            raise ValueError("ModelSpec.name must be non-empty")
        jsonb = spec.to_jsonb_dict()
        with self.engine.begin() as conn:
            existing = conn.execute(text("SELECT name FROM oracle_models WHERE name = :n"), {"n": spec.name}).fetchone()
            if existing:
                raise ValueError(f"Model '{spec.name}' already exists")
            conn.execute(text("""
                INSERT INTO oracle_models
                (name, version, description, signal_families, weight,
                 signal_sources, signal_filters, weight_config_json,
                 prediction_type_col, target_horizon_days, min_signals,
                 active, created_by, parent_model, last_updated)
                VALUES
                (:name, :version, :description, :signal_families, 1.0,
                 :signal_sources, :signal_filters, :weight_config_json,
                 :prediction_type, :horizon, :min_sig,
                 :active, :created_by, :parent_model, NOW())
            """), {
                "name": spec.name, "version": spec.version, "description": spec.description,
                "signal_families": json.dumps(spec.signal_sources),
                "signal_sources": json.dumps(jsonb["signal_sources"]),
                "signal_filters": json.dumps(jsonb["signal_filters"]),
                "weight_config_json": json.dumps(jsonb["weight_config"]),
                "prediction_type": spec.prediction_type, "horizon": spec.target_horizon_days,
                "min_sig": spec.min_signals, "active": spec.active,
                "created_by": spec.created_by, "parent_model": spec.parent_model,
            })
        log.info("ModelFactory: created '{name}' ({n} sources)", name=spec.name, n=len(spec.signal_sources))
        return spec.name

    def spawn_variant(self, parent_name: str, mutations: dict[str, Any]) -> ModelSpec:
        parent = self.get_model_spec(parent_name)
        sources = list(parent.signal_sources)
        if "signal_sources" in mutations:
            sources = list(mutations["signal_sources"])
        if "add_sources" in mutations:
            for src in mutations["add_sources"]:
                if src not in sources:
                    sources.append(src)
        if "remove_sources" in mutations:
            sources = [s for s in sources if s not in mutations["remove_sources"]]

        parts = parent.version.split(".")
        try:
            parts[-1] = str(int(parts[-1]) + 1)
        except ValueError:
            parts.append("1")
        new_version = mutations.get("version", ".".join(parts))
        new_name = mutations.get("name", f"{parent_name}:v{new_version}")

        return ModelSpec(
            name=new_name, version=new_version,
            description=mutations.get("description", parent.description),
            signal_sources=sources,
            signal_filters=mutations.get("signal_filters", dict(parent.signal_filters)),
            weight_config=parent.weight_config,
            prediction_type=mutations.get("prediction_type", parent.prediction_type),
            target_horizon_days=mutations.get("target_horizon_days", parent.target_horizon_days),
            min_signals=mutations.get("min_signals", parent.min_signals),
            active=True, created_by="evolver", parent_model=parent_name,
        )

    def get_model_spec(self, model_name: str) -> ModelSpec:
        with self.engine.connect() as conn:
            row = conn.execute(text("""
                SELECT name, version, description, signal_sources, signal_filters,
                       weight_config_json, prediction_type_col, target_horizon_days,
                       min_signals, active, created_by, parent_model
                FROM oracle_models WHERE name = :n
            """), {"n": model_name}).mappings().fetchone()
        if row is None:
            raise KeyError(f"Model '{model_name}' not found")

        def _load(v, default):
            if v is None: return default
            if isinstance(v, (dict, list)): return v
            try: return json.loads(v)
            except: return default

        wc_raw = _load(row.get("weight_config_json"), {})
        wc = WeightConfig(
            mode=wc_raw.get("mode", WeightMode.EQUAL),
            trust_decay_half_life_days=float(wc_raw.get("trust_decay_half_life_days", 90.0)),
            min_weight=float(wc_raw.get("min_weight", 0.1)),
            max_weight=float(wc_raw.get("max_weight", 3.0)),
            family_weights=wc_raw.get("family_weights"),
        )

        return ModelSpec(
            name=row["name"], version=row.get("version") or "1.0",
            description=row.get("description") or "",
            signal_sources=_load(row.get("signal_sources"), []),
            signal_filters=_load(row.get("signal_filters"), {}),
            weight_config=wc,
            prediction_type=row.get("prediction_type_col") or "directional",
            target_horizon_days=int(row.get("target_horizon_days") or 7),
            min_signals=int(row.get("min_signals") or 3),
            active=bool(row.get("active") if row.get("active") is not None else True),
            created_by=row.get("created_by") or "human",
            parent_model=row.get("parent_model"),
        )

    def get_signals_for_model(self, model_name: str, as_of: datetime) -> list[dict[str, Any]]:
        if not _signal_registry_enabled():
            return []
        spec = self.get_model_spec(model_name)
        if not spec.signal_sources:
            return []

        as_of_utc = as_of if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
        params: dict[str, Any] = {"sources": spec.signal_sources, "as_of": as_of_utc}
        clauses = ["source_module = ANY(:sources)", "valid_from <= :as_of", "(valid_until IS NULL OR valid_until > :as_of)"]

        filters = spec.signal_filters or {}
        if filters.get("min_confidence"):
            params["min_conf"] = float(filters["min_confidence"])
            clauses.append("confidence >= :min_conf")

        where = " AND ".join(clauses)
        with self.engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT id, source_module, signal_type, ticker, direction, value, "
                f"z_score, confidence, valid_from, valid_until, metadata "
                f"FROM signal_registry WHERE {where} ORDER BY valid_from DESC"
            ), params).mappings().all()
        return [dict(r) for r in rows]

    def list_active_models(self) -> list[ModelSpec]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT name FROM oracle_models WHERE active IS TRUE OR active IS NULL
            """)).fetchall()
        return [self.get_model_spec(r[0]) for r in rows]

    def retire_model(self, model_name: str) -> None:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("UPDATE oracle_models SET active = FALSE, last_updated = NOW() WHERE name = :n"),
                {"n": model_name},
            )
            if result.rowcount == 0:
                raise KeyError(f"Model '{model_name}' not found")
        log.info("ModelFactory: retired '{name}'", name=model_name)


def migrate_default_models(engine: Engine) -> None:
    """Populate JSONB columns for the 5 legacy Oracle models. Idempotent."""
    factory = ModelFactory(engine)
    default_wc = {"mode": "equal", "trust_decay_half_life_days": 90.0, "min_weight": 0.1, "max_weight": 3.0, "family_weights": None}

    for model_name, sources in _DEFAULT_SIGNAL_SOURCES.items():
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE oracle_models
                SET signal_sources = :ss, signal_filters = :sf, weight_config_json = :wc,
                    active = TRUE, created_by = 'human', last_updated = NOW()
                WHERE name = :n AND signal_sources IS NULL
            """), {
                "n": model_name,
                "ss": json.dumps(sources),
                "sf": json.dumps({}),
                "wc": json.dumps(default_wc),
            })
    log.info("migrate_default_models: complete ({n} models)", n=len(_DEFAULT_SIGNAL_SOURCES))
