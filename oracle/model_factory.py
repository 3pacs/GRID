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
from sqlalchemy.engine import Connection, Engine

from oracle.signal_aggregator import WeightConfig, WeightMode


def signal_registry_enabled() -> bool:
    """Return True iff ``GRID_SIGNAL_REGISTRY`` is explicitly enabled (``"1"``).

    Shared helper used by both ``oracle.model_factory`` and ``oracle.engine``
    so both paths agree on the same default (``"0"``, disabled).
    """
    return os.getenv("GRID_SIGNAL_REGISTRY", "0") == "1"


_signal_registry_enabled = signal_registry_enabled


_DEFAULT_SIGNAL_SOURCES: dict[str, list[str]] = {
    "flow_momentum":     ["feature:equity", "feature:flows", "feature:breadth", "feature:vol", "flow_thesis", "dollar_flows", "sector_network", "trust_scorer", "thesis_tracker"],
    "regime_contrarian": ["feature:rates", "feature:credit", "feature:vol", "feature:macro", "cross_reference", "sector_network", "lever_pullers", "forensics"],
    "options_flow":      ["feature:sentiment", "feature:vol", "feature:equity", "pattern_engine"],
    "cross_asset":       ["feature:rates", "feature:fx", "feature:commodity", "feature:credit", "feature:equity"],
    "news_energy":       ["feature:sentiment", "feature:alternative", "feature:equity", "news_intel", "sleuth", "earnings_intel"],
    "ai_trader_crowd":   ["ai_trader", "feature:equity", "feature:sentiment"],
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

    @staticmethod
    def active_model_names(
        conn: Connection, model_names: list[str], *, lock: bool = False,
    ) -> set[str]:
        """Admit only explicitly active registry rows, on the caller's transaction.

        Writers hold FOR SHARE until commit so a concurrent retirement cannot
        cross the final admission decision. Read failures propagate: callers
        must never substitute a default or cached roster.
        """
        names = sorted(set(model_names))
        if not names:
            return set()
        query = (
            "SELECT name FROM oracle_models "
            "WHERE name = ANY(:names) AND active IS TRUE ORDER BY name FOR SHARE"
            if lock else
            "SELECT name FROM oracle_models "
            "WHERE name = ANY(:names) AND active IS TRUE ORDER BY name"
        )
        return {row[0] for row in conn.execute(text(query), {"names": names})}

    def __init__(self, engine: Engine) -> None:
        # Process-level guard: ALTER TABLE oracle_models would otherwise
        # fire on every construction. See schema_guard.py.
        from schema_guard import ensure_once
        self.engine = engine
        ensure_once("ModelFactory.columns", self._ensure_columns)

    # Security: frozen whitelists for DDL identifiers and type definitions.
    # col_name is checked against _ALLOWED_COLUMNS + regex; col_def is checked
    # against _ALLOWED_COL_DEFS so neither identifier nor type fragment can be
    # attacker-influenced.  Both must pass before the ALTER TABLE executes.
    _ALLOWED_COLUMNS = {
        "signal_sources", "signal_filters", "weight_config_json",
        "prediction_type_col", "target_horizon_days", "min_signals",
        "active", "created_by", "parent_model",
    }
    _ALLOWED_COL_DEFS = {
        "JSONB",
        "TEXT DEFAULT 'directional'",
        "INTEGER DEFAULT 7",
        "INTEGER DEFAULT 3",
        "BOOLEAN",
        "TEXT DEFAULT 'human'",
        "TEXT",
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
            # A legacy table without admission state has unknown membership.
            # Explicit model creation supplies True; migration must not infer it.
            ("active", "BOOLEAN"),
            ("created_by", "TEXT DEFAULT 'human'"),
            ("parent_model", "TEXT"),
        ]
        with self.engine.begin() as conn:
            # Inspect the same search-path relation that unqualified ALTER uses.
            # IF NOT EXISTS still takes ACCESS EXCLUSIVE even on complete tables.
            # Catalog errors propagate; never declare an unverified schema ready.
            existing = {
                row[0] for row in conn.execute(text("""
                    SELECT attname FROM pg_catalog.pg_attribute
                    WHERE attrelid = pg_catalog.to_regclass('oracle_models')
                      AND attnum > 0 AND NOT attisdropped
                """))
            }
            missing = [(name, definition) for name, definition in cols if name not in existing]
            if not missing:
                log.info("ModelFactory schema already complete; runtime DDL skipped")
                return
            log.info("ModelFactory schema compatibility path: {n} missing columns", n=len(missing))
            for col_name, col_def in missing:
                # Security: assert both identifier and type fragment are whitelisted
                # before interpolating into DDL.  Raises AssertionError (not caught
                # by generic Exception handlers) so a misconfigured cols list fails
                # loudly at startup rather than silently skipping.
                assert col_name in self._ALLOWED_COLUMNS and _IDENT_RE.match(col_name), \
                    f"Blocked DDL: column name '{col_name}' not in whitelist"
                assert col_def in self._ALLOWED_COL_DEFS, \
                    f"Blocked DDL: col_def '{col_def}' not in whitelist"
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
            if v is None:
                return default
            if isinstance(v, (dict, list)):
                return v
            try:
                return json.loads(v)
            except Exception as e:
                log.warning("ModelFactory: JSON parse failed: {e}", e=str(e))
                return default

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
            active=row.get("active") is True,
            created_by=row.get("created_by") or "human",
            parent_model=row.get("parent_model"),
        )

    def get_signals_for_model(
        self, model_name: str, as_of: datetime, ticker: str | None = None,
    ) -> list[dict[str, Any]]:
        """Active registry signals for a model's subscribed sources.

        ``ticker`` scopes the rows to that ticker plus market-wide rows (no
        ticker). The oracle passes it for every per-ticker prediction: without
        it a model saw every ticker's signals (2026-11-06: 297k changepoint rows
        across 47 tickers, 29k lever-puller rows across 215) as evidence about
        whichever ticker it was predicting. ``None`` keeps the unscoped listing
        the signal-registry API shows.
        """
        if not _signal_registry_enabled():
            return []
        spec = self.get_model_spec(model_name)
        if not spec.active or not spec.signal_sources:
            return []

        as_of_utc = as_of if as_of.tzinfo else as_of.replace(tzinfo=timezone.utc)
        params: dict[str, Any] = {
            "sources": spec.signal_sources, "as_of": as_of_utc, "model_name": model_name,
        }
        clauses = [
            "source_module = ANY(:sources)", "valid_from <= :as_of",
            "(valid_until IS NULL OR valid_until > :as_of)",
            "EXISTS (SELECT 1 FROM oracle_models WHERE name = :model_name AND active IS TRUE)",
        ]
        if ticker:
            params["ticker"] = ticker
            clauses.append("(ticker = :ticker OR ticker IS NULL OR ticker = '')")

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
        return latest_factor_obs_only([dict(r) for r in rows])

    def list_active_models(self) -> list[ModelSpec]:
        with self.engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT name FROM oracle_models WHERE active IS TRUE ORDER BY name
            """)).fetchall()
        specs = [self.get_model_spec(r[0]) for r in rows]
        return [spec for spec in specs if spec.active]

    def retire_model(self, model_name: str) -> None:
        with self.engine.begin() as conn:
            result = conn.execute(
                text("UPDATE oracle_models SET active = FALSE, last_updated = NOW() WHERE name = :n"),
                {"n": model_name},
            )
            if result.rowcount == 0:
                raise KeyError(f"Model '{model_name}' not found")
        log.info("ModelFactory: retired '{name}'", name=model_name)



def _factor_obs_date(row: dict[str, Any]) -> str | None:
    meta = row.get("metadata")
    if isinstance(meta, str):
        try:
            meta = json.loads(meta)
        except ValueError:
            return None
    if not isinstance(meta, dict):
        return None
    value = meta.get("factor_obs_date")
    return value if isinstance(value, str) else None


def latest_factor_obs_only(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """One row per (source_module, ticker) for a session-bounded factor.

    A factor published from a ticker's own latest ranked row records that
    row's date as ``metadata.factor_obs_date`` and is republished every cycle.
    Every copy stays valid, so a model would count Friday's and Monday's
    observations together, and n identical copies of one observation give
    sqrt(n) times its strength. The copy count differs by ticker (SPY's D-1
    rows accumulate from its overnight receipt; a same-day ticker's restart
    at each session's first pull), so it would weight tickers by publication
    count, not evidence. Keep, per (source_module, ticker), the single row
    with the newest ``factor_obs_date`` and, among those, the newest
    ``valid_from``. Once a (source_module, ticker) has a row with the field,
    its rows without it (published by the previous code and still valid for
    up to 24 h after the change) are dropped too, so the changeover counts one
    vote. Rows of a key with no such row (every other source) are unchanged.
    """
    best: dict[tuple[Any, Any], tuple[str, Any, int]] = {}
    for i, row in enumerate(rows):
        obs = _factor_obs_date(row)
        if obs is None:
            continue
        key = (row.get("source_module"), row.get("ticker"))
        rank = (obs, row.get("valid_from"), -i)
        held = best.get(key)
        if held is None or _newer(rank, held):
            best[key] = rank
    keep = {-rank[2] for rank in best.values()}
    return [
        row for i, row in enumerate(rows)
        if i in keep
        or (_factor_obs_date(row) is None
            and (row.get("source_module"), row.get("ticker")) not in best)
    ]


def _newer(a: tuple[str, Any, int], b: tuple[str, Any, int]) -> bool:
    """Newer observation date, then newer publish time, then the earlier row (ties)."""
    if a[0] != b[0]:
        return a[0] > b[0]
    if a[1] != b[1] and a[1] is not None and b[1] is not None:
        return a[1] > b[1]
    if a[1] is not None and b[1] is None:
        return True
    return False

def migrate_default_models(engine: Engine) -> None:
    """Populate JSONB columns for the legacy Oracle models. Idempotent.

    Two-pass strategy so this is safe to re-run after any edit to
    ``_DEFAULT_SIGNAL_SOURCES``:

    1. Cold rows (``signal_sources IS NULL``) are seeded with the full
       default list + default weight_config/filters.
    2. Warm rows get the union of their current sources + the defaults
       so that when a new source (like ``sector_network``) is added to
       the default list, existing prod rows actually pick it up.
    """
    ModelFactory(engine)
    default_wc = {"mode": "equal", "trust_decay_half_life_days": 90.0, "min_weight": 0.1, "max_weight": 3.0, "family_weights": None}

    for model_name, sources in _DEFAULT_SIGNAL_SOURCES.items():
        with engine.begin() as conn:
            conn.execute(text("""
                UPDATE oracle_models
                SET signal_sources = :ss, signal_filters = :sf, weight_config_json = :wc,
                    last_updated = NOW()
                WHERE name = :n AND signal_sources IS NULL
            """), {
                "n": model_name,
                "ss": json.dumps(sources),
                "sf": json.dumps({}),
                "wc": json.dumps(default_wc),
            })

            # Warm-row merge: union existing signal_sources with defaults.
            row = conn.execute(
                text("SELECT signal_sources FROM oracle_models WHERE name = :n"),
                {"n": model_name},
            ).fetchone()
            if row and row[0] is not None:
                try:
                    current = row[0] if isinstance(row[0], list) else json.loads(row[0])
                except (TypeError, ValueError):
                    current = []
                if not isinstance(current, list):
                    current = []
                merged = list(current)
                for s in sources:
                    if s not in merged:
                        merged.append(s)
                if merged != current:
                    conn.execute(
                        text("""
                            UPDATE oracle_models
                            SET signal_sources = :ss, last_updated = NOW()
                            WHERE name = :n
                        """),
                        {"n": model_name, "ss": json.dumps(merged)},
                    )
    log.info("migrate_default_models: complete ({n} models)", n=len(_DEFAULT_SIGNAL_SOURCES))
