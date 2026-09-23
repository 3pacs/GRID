"""
GRID analytical snapshot persistence.

Saves every analytical output (clustering, orthogonality, feature importance,
regime detection, options scans) to a database table with full provenance.
This allows comparing results across time — e.g. "how did the correlation
structure differ 6 months ago?" or "when did the regime model start
disagreeing with itself?"

Schema: ``analytical_snapshots`` table (see migration in scripts/migrate_snapshots.py).
"""

from __future__ import annotations

import json
from datetime import date, datetime
from typing import Any

import numpy as np
import pandas as pd
from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.ttl_cache import TTLCache


class _NumpyEncoder(json.JSONEncoder):
    """JSON encoder that handles numpy types and pandas objects."""

    def default(self, obj: Any) -> Any:
        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            val = float(obj)
            if np.isnan(val) or np.isinf(val):
                return None
            return val
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (pd.Timestamp, datetime)):
            return obj.isoformat()
        if isinstance(obj, date):
            return obj.isoformat()
        if isinstance(obj, pd.DataFrame):
            return obj.to_dict("records")
        if isinstance(obj, pd.Series):
            return obj.to_dict()
        return super().default(obj)


def _safe_json(data: Any) -> str:
    """Serialize data to JSON, handling numpy/pandas types."""
    return json.dumps(data, cls=_NumpyEncoder, default=str)


# ---------------------------------------------------------------------------
# Canonical schema
# ---------------------------------------------------------------------------
# This is the ONE definition of ``analytical_snapshots`` in the repository.
# Every writer and every migration must agree with it.
#
# A second, incompatible DDL for the same table name used to live in
# ``scripts/parse_datasets.py`` — it declared ``actor / ticker / title /
# summary / data`` columns that the real table has never had. The FTS
# migration ``phase4_fts_intelligence_search`` was written against that
# phantom shape and installed a BEFORE INSERT OR UPDATE trigger assigning
# ``NEW.title``, which PL/pgSQL rejects with
#
#     record "new" has no field "title"
#
# on *every* write. That took the table offline for writes on 2026-09-11
# (05:55–15:26 UTC) and silently dropped every analytical snapshot produced
# in that window. Keep the definition here and nowhere else; the guard in
# ``tests/test_migration_fts_columns.py`` reads this constant to check what
# the migrations are allowed to reference.
#
# Note ``search_vector tsvector`` is NOT declared here: it is added by the
# FTS migration, not by this table's own bootstrap, because that column
# needs a trigger to stay maintained and belongs to that migration's own
# lifecycle.
#
# ``actor_name TEXT`` IS declared here, unlike ``search_vector`` above: it
# needs no trigger, no backfill machinery beyond a one-time migration
# (``migrations/versions/snapshot_actor_col_20260914.py``), and every writer
# of this table can populate it directly at insert time -- today that's
# ``scripts/parse_datasets.py::_snapshot_row`` via its own bespoke insert, and
# ``AnalyticalSnapshotStore.save_snapshot()`` below via its optional
# ``actor_name`` parameter -- the same way every other column here works. A
# fresh database (dev, CI, a restored slice) gets it from first boot rather
# than waiting on a migration to run against a table that migration doesn't
# even know exists yet until `store/snapshots.py` creates it (see that
# migration's fresh-database guard).
ANALYTICAL_SNAPSHOTS_DDL = """
    CREATE TABLE IF NOT EXISTS analytical_snapshots (
        id            BIGSERIAL PRIMARY KEY,
        snapshot_date DATE NOT NULL,
        category      TEXT NOT NULL,
        subcategory   TEXT,
        as_of_date    DATE NOT NULL,
        payload       JSONB NOT NULL,
        actor_name    TEXT,
        metrics       JSONB,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
"""

ANALYTICAL_SNAPSHOTS_INDEX_DDL = (
    """
    CREATE INDEX IF NOT EXISTS idx_analytical_snapshots_date
        ON analytical_snapshots (snapshot_date DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_analytical_snapshots_category
        ON analytical_snapshots (category, snapshot_date DESC)
    """,
)


# ---------------------------------------------------------------------------
# Category discovery
# ---------------------------------------------------------------------------
# Which categories exist is a property of the DATA, not of any literal in this
# file, and no maintained tuple can ever enumerate it:
#
#   * two writers build the category at runtime — ``mcp_server.py``'s
#     f"mcp_research_{task_type}" and ``orchestration/llm_taskqueue.py``'s
#     f"llm_task_{task.task_type}" — so the value doesn't exist until the row
#     is written;
#   * five modules INSERT into ``analytical_snapshots`` directly and never
#     reach ``save_snapshot`` at all (``scripts/parse_datasets.py``,
#     ``ingestion/openbb_pipeline.py``, ``scripts/drain_backlog.py``,
#     ``scripts/drain_surfacer_backfill.py``, and this module).
#
# ``AnalyticalSnapshotStore.list_categories`` reads the table instead. A
# hardcoded list used to gate ``GET /api/v1/snapshots/latest/{category}``,
# which 400'd every category not in it — see the PIPELINE_CATEGORIES note.
#
# ``SELECT ... GROUP BY category`` is an index scan over
# ``idx_analytical_snapshots_category``; PostgreSQL 15 has no loose index
# scan, so it touches every row (~17K and growing as of 2026-09). Cheap, but
# not free per request, so the result is cached process-wide. There is one
# engine per process (``api/dependencies.get_db_engine``), so a single key is
# enough. ``save_snapshot`` invalidates on a genuinely new category, which
# makes a first-ever write visible immediately in-process; other processes
# pick it up within the TTL.
_CATEGORY_CACHE_TTL: float = 300.0
_CATEGORY_CACHE_KEY = "categories"
_category_cache: TTLCache = TTLCache(ttl=_CATEGORY_CACHE_TTL, max_size=1)


def clear_category_cache() -> None:
    """Drop the cached category listing so the next read hits the database."""
    _category_cache.clear()


def ensure_analytical_snapshots_table(db_engine: Engine) -> None:
    """Create ``analytical_snapshots`` and its indexes if they don't exist.

    Idempotent, and best-effort: a database that cannot be reached must not
    stop the caller from starting up.

    Parameters:
        db_engine: SQLAlchemy engine for database access.
    """
    try:
        with db_engine.begin() as conn:
            conn.execute(text(ANALYTICAL_SNAPSHOTS_DDL))
            for idx in ANALYTICAL_SNAPSHOTS_INDEX_DDL:
                conn.execute(text(idx))
    except Exception as exc:
        log.warning("Could not ensure analytical_snapshots table: {e}", e=str(exc))


class AnalyticalSnapshotStore:
    """Persist and query analytical outputs for historical comparison.

    Every run of clustering, orthogonality, regime detection, feature
    engineering, or options scanning gets a row in ``analytical_snapshots``
    with the full result payload as JSONB.

    Attributes:
        engine: SQLAlchemy engine for database access.
    """

    # The categories the core analytical pipeline produces — the ones
    # ``save_pipeline_snapshots`` writes and ``_extract_metrics`` knows how to
    # summarize.
    #
    # THIS IS NOT THE SET OF VALID CATEGORIES, and nothing may treat it as
    # one. It was named ``CATEGORIES`` until 2026-09, and that name read as
    # "every category there is": the snapshots API gated
    # ``GET /latest/{category}`` on membership and so returned HTTP 400 for
    # every category outside these eight — ``sleuth_investigation``,
    # ``alpha101``, ``strategy151``, ``research_sweep``, ``sector_flows``,
    # ``human_llm_insight``, ``congressional_trade``, ``opensanctions``,
    # ``crypto_price``, and both runtime-built families. Use
    # ``list_categories()`` for what actually exists; this tuple is
    # documentation of the canonical pipeline set only.
    PIPELINE_CATEGORIES = (
        "clustering",
        "orthogonality",
        "regime_detection",
        "feature_engineering",
        "feature_importance",
        "options_scan",
        "conflict_resolution",
        "pipeline_summary",
    )

    def __init__(
        self,
        db_engine: Engine,
        retention_per_category: int | None = None,
        ensure_table: bool = True,
    ) -> None:
        """
        Parameters:
            db_engine: SQLAlchemy engine for database access.
            retention_per_category: When set, `save_snapshot` prunes each
                category down to its `retention_per_category` most recent
                rows after every successful insert. `None` (default) keeps
                every row, matching pre-existing behavior for callers that
                don't opt in.
        """
        self.engine = db_engine
        self.retention_per_category = retention_per_category
        if ensure_table:
            self._ensure_table()

    def _ensure_table(self) -> None:
        """Create the analytical_snapshots table if it doesn't exist."""
        ensure_analytical_snapshots_table(self.engine)

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def save_snapshot(
        self,
        category: str,
        payload: dict[str, Any],
        as_of_date: date | None = None,
        subcategory: str | None = None,
        metrics: dict[str, Any] | None = None,
        actor_name: str | None = None,
    ) -> int | None:
        """Persist a single analytical snapshot.

        Parameters:
            category: Free-form category label (e.g. 'clustering',
                'sector_flows'). Not restricted to PIPELINE_CATEGORIES —
                callers invent categories, including at runtime.
            payload: Full result dict to store as JSONB.
            as_of_date: The decision date the analysis was run for.
            subcategory: Optional refinement (e.g. 'k=4', 'pre_2008').
            metrics: Optional summary metrics for fast querying.
            actor_name: Optional person/entity this snapshot is about,
                written to the indexed ``actor_name`` column (migration
                ``snapshot_actor_col_20260914``) so entity_resolver.py's
                snapshot search can find this row (see
                ``SNAPSHOT_SEARCH_SQL`` in intelligence/entity_resolver.py).
                Leave ``None`` for categories with no natural actor
                (clustering, regime_detection, ...) -- every current caller
                of this method does.

        Returns:
            int: Snapshot row ID, or None on failure.
        """
        if as_of_date is None:
            as_of_date = date.today()

        try:
            with self.engine.begin() as conn:
                row = conn.execute(
                    text(
                        "INSERT INTO analytical_snapshots "
                        "(snapshot_date, category, subcategory, as_of_date, payload, actor_name, metrics) "
                        "VALUES (:sd, :cat, :sub, :aod, CAST(:payload_json AS jsonb), :actor_name, CAST(:metrics_json AS jsonb)) "
                        "RETURNING id"
                    ),
                    {
                        "sd": date.today(),
                        "cat": category,
                        "sub": subcategory,
                        "aod": as_of_date,
                        "payload_json": _safe_json(payload),
                        "actor_name": actor_name,
                        "metrics_json": _safe_json(metrics) if metrics else None,
                    },
                ).fetchone()
            snap_id = row[0] if row else None
            log.info(
                "Snapshot saved — id={id}, category={cat}, as_of={d}",
                id=snap_id, cat=category, d=as_of_date,
            )
        except Exception as exc:
            log.error("Failed to save snapshot ({cat}): {e}", cat=category, e=str(exc))
            return None

        self._invalidate_category_cache_if_new(category)

        if self.retention_per_category is not None:
            self._prune_category(category, self.retention_per_category)
        return snap_id

    @staticmethod
    def _invalidate_category_cache_if_new(category: str) -> None:
        """Drop the cached category listing when `category` isn't in it.

        A first-ever write of a category would otherwise stay invisible to
        ``list_categories`` (and so to ``GET /api/v1/snapshots/categories``)
        for up to the cache TTL. Writes of an already-known category leave the
        cache alone, so the steady state still serves from cache.
        """
        cached = _category_cache.get(_CATEGORY_CACHE_KEY)
        if cached is None:
            return
        if not any(entry.get("category") == category for entry in cached):
            _category_cache.clear()

    def _prune_category(self, category: str, keep_n: int) -> None:
        """Delete all but the `keep_n` most recent snapshots for a category.

        Best-effort: a pruning failure must never fail the write that
        triggered it — the row is already committed by the time this runs.
        """
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text(
                        "DELETE FROM analytical_snapshots "
                        "WHERE category = :cat AND id NOT IN ("
                        "    SELECT id FROM analytical_snapshots "
                        "    WHERE category = :cat "
                        "    ORDER BY snapshot_date DESC, created_at DESC "
                        "    LIMIT :keep_n"
                        ")"
                    ),
                    {"cat": category, "keep_n": keep_n},
                )
        except Exception as exc:
            log.warning(
                "Could not prune old snapshots ({cat}): {e}", cat=category, e=str(exc),
            )

    def save_pipeline_snapshots(self, step_results: dict[str, Any]) -> int:
        """Save snapshots for all pipeline step results.

        Maps each pipeline step result to the appropriate category and
        persists it.  Handles None results (failed steps) gracefully.

        Parameters:
            step_results: Dict of step_name -> result from run_full_pipeline.

        Returns:
            int: Number of snapshots successfully saved.
        """
        today = date.today()
        saved = 0

        # Map pipeline step names to snapshot categories
        step_to_category = {
            "resolution": "conflict_resolution",
            "features": "feature_engineering",
            "orthogonality": "orthogonality",
            "regime": "regime_detection",
            "options_scan": "options_scan",
            "importance": "feature_importance",
        }

        for step_name, result in step_results.items():
            if result is None:
                continue
            category = step_to_category.get(step_name)
            if category is None:
                continue

            # Extract metrics for fast querying
            metrics = self._extract_metrics(category, result)

            snap_id = self.save_snapshot(
                category=category,
                payload=result if isinstance(result, dict) else {"result": result},
                as_of_date=today,
                metrics=metrics,
            )
            if snap_id is not None:
                saved += 1

        # Also save the full pipeline summary
        pipeline_metrics = {
            "steps_succeeded": sum(1 for v in step_results.values() if v is not None),
            "steps_failed": sum(1 for v in step_results.values() if v is None),
            "total_steps": len(step_results),
        }
        self.save_snapshot(
            category="pipeline_summary",
            payload=step_results,
            as_of_date=today,
            metrics=pipeline_metrics,
        )
        saved += 1

        return saved

    def _extract_metrics(self, category: str, result: Any) -> dict[str, Any] | None:
        """Extract summary metrics from a result for fast querying."""
        if not isinstance(result, dict):
            return None

        if category == "clustering":
            return {
                "best_k": result.get("best_k"),
                "n_observations": result.get("n_observations"),
                "variance_explained": result.get("variance_explained"),
            }
        elif category == "orthogonality":
            return {
                "n_features": result.get("n_features_analyzed"),
                "true_dimensionality": result.get("true_dimensionality"),
                "n_correlated_pairs": len(result.get("highly_correlated_pairs", [])),
                "n_unstable_pairs": len(result.get("unstable_pairs", [])),
            }
        elif category == "conflict_resolution":
            return {
                "resolved": result.get("resolved"),
                "conflicts_found": result.get("conflicts_found"),
                "errors": result.get("errors"),
            }
        elif category == "feature_engineering":
            if isinstance(result, dict):
                return {
                    "n_features": len(result),
                    "n_non_null": sum(1 for v in result.values() if v is not None),
                }
        elif category == "options_scan":
            return {
                "opportunities": result.get("opportunities"),
                "n_100x": result.get("100x"),
            }
        elif category == "feature_importance":
            summary = result.get("summary", [])
            return {
                "n_features": result.get("n_features"),
                "top_feature": summary[0]["feature_name"] if summary else None,
                "top_composite": summary[0]["composite_score"] if summary else None,
            }
        return None

    # ------------------------------------------------------------------
    # Read / compare
    # ------------------------------------------------------------------

    def list_categories(self, use_cache: bool = True) -> list[dict[str, Any]]:
        """Return every category actually present in ``analytical_snapshots``.

        Derived from the table, never from a maintained literal — see the
        "Category discovery" note above for why a literal cannot work. Result
        is cached process-wide for ``_CATEGORY_CACHE_TTL`` seconds.

        Parameters:
            use_cache: When False, always query the database (and refresh the
                cache with the result).

        Returns:
            list[dict]: One entry per category, ordered by category name, with
                ``category``, ``snapshot_count``, and ``latest_snapshot_date``
                (ISO date string, or None if the category has no dated rows).
                Empty list if the table is unreachable or absent.
        """
        if use_cache:
            cached = _category_cache.get(_CATEGORY_CACHE_KEY)
            if cached is not None:
                return cached

        try:
            with self.engine.connect() as conn:
                rows = conn.execute(
                    text(
                        "SELECT category, COUNT(*) AS snapshot_count, "
                        "       MAX(snapshot_date) AS latest_snapshot_date "
                        "FROM analytical_snapshots "
                        "GROUP BY category "
                        "ORDER BY category"
                    )
                ).fetchall()
        except Exception as exc:
            # Operational, not an application bug: the table is absent on a
            # fresh install and the database is briefly unreachable during a
            # restart. Warning, so errors.jsonl stays signal-rich (CLAUDE.md).
            log.warning("Could not list snapshot categories: {e}", e=str(exc))
            return []

        categories = [
            {
                "category": r[0],
                "snapshot_count": int(r[1]),
                "latest_snapshot_date": r[2].isoformat() if r[2] else None,
            }
            for r in rows
        ]
        _category_cache.set(_CATEGORY_CACHE_KEY, categories)
        return categories

    def get_latest(
        self,
        category: str,
        n: int = 1,
    ) -> list[dict[str, Any]]:
        """Retrieve the most recent snapshots for a category.

        Parameters:
            category: Snapshot category to query.
            n: Number of recent snapshots to return.

        Returns:
            list[dict]: Snapshot rows with id, snapshot_date, payload, metrics.
        """
        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT id, snapshot_date, as_of_date, subcategory, "
                    "       payload, metrics, created_at "
                    "FROM analytical_snapshots "
                    "WHERE category = :cat "
                    "ORDER BY snapshot_date DESC, created_at DESC "
                    "LIMIT :n"
                ),
                {"cat": category, "n": n},
            ).fetchall()

        return [
            {
                "id": r[0],
                "snapshot_date": r[1],
                "as_of_date": r[2],
                "subcategory": r[3],
                "payload": r[4],
                "metrics": r[5],
                "created_at": r[6],
            }
            for r in rows
        ]

    def get_history(
        self,
        category: str,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pd.DataFrame:
        """Retrieve snapshot metrics over a date range for trending.

        Returns a DataFrame of (snapshot_date, metrics) rows — useful for
        plotting how e.g. true_dimensionality or best_k changes over time.

        Parameters:
            category: Snapshot category.
            start_date: Earliest snapshot date (default: 90 days ago).
            end_date: Latest snapshot date (default: today).

        Returns:
            pd.DataFrame: Columns include snapshot_date + all metrics keys.
        """
        from datetime import timedelta

        if end_date is None:
            end_date = date.today()
        if start_date is None:
            start_date = end_date - timedelta(days=90)

        with self.engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT snapshot_date, metrics "
                    "FROM analytical_snapshots "
                    "WHERE category = :cat "
                    "  AND snapshot_date >= :sd "
                    "  AND snapshot_date <= :ed "
                    "ORDER BY snapshot_date"
                ),
                {"cat": category, "sd": start_date, "ed": end_date},
            ).fetchall()

        if not rows:
            return pd.DataFrame()

        records = []
        for r in rows:
            rec = {"snapshot_date": r[0]}
            if r[1]:
                rec.update(r[1])
            records.append(rec)

        return pd.DataFrame(records)

    def compare_snapshots(
        self,
        category: str,
        date_a: date,
        date_b: date,
    ) -> dict[str, Any]:
        """Compare two snapshots from different dates.

        Parameters:
            category: Snapshot category.
            date_a: First date.
            date_b: Second date.

        Returns:
            dict with keys: date_a, date_b, metrics_a, metrics_b, deltas.
        """
        with self.engine.connect() as conn:
            snap_a = conn.execute(
                text(
                    "SELECT metrics, payload FROM analytical_snapshots "
                    "WHERE category = :cat AND snapshot_date = :d "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"cat": category, "d": date_a},
            ).fetchone()

            snap_b = conn.execute(
                text(
                    "SELECT metrics, payload FROM analytical_snapshots "
                    "WHERE category = :cat AND snapshot_date = :d "
                    "ORDER BY created_at DESC LIMIT 1"
                ),
                {"cat": category, "d": date_b},
            ).fetchone()

        if snap_a is None or snap_b is None:
            missing = []
            if snap_a is None:
                missing.append(str(date_a))
            if snap_b is None:
                missing.append(str(date_b))
            return {"error": f"Missing snapshots for: {', '.join(missing)}"}

        metrics_a = snap_a[0] or {}
        metrics_b = snap_b[0] or {}

        # Compute deltas for numeric metrics
        deltas: dict[str, Any] = {}
        all_keys = set(metrics_a.keys()) | set(metrics_b.keys())
        for key in all_keys:
            val_a = metrics_a.get(key)
            val_b = metrics_b.get(key)
            if isinstance(val_a, (int, float)) and isinstance(val_b, (int, float)):
                deltas[key] = {"a": val_a, "b": val_b, "delta": val_b - val_a}
            else:
                deltas[key] = {"a": val_a, "b": val_b}

        return {
            "category": category,
            "date_a": date_a.isoformat(),
            "date_b": date_b.isoformat(),
            "metrics_a": metrics_a,
            "metrics_b": metrics_b,
            "deltas": deltas,
        }
