"""Idempotent guard for the oracle_predictions natural-key dedup index.

The three writers that INSERT into ``oracle_predictions``
(``oracle/engine.py``, ``oracle/publish.py``, ``intelligence/obsidian_agent.py``)
all use an ``ON CONFLICT (...) WHERE dedup_keep = TRUE DO UPDATE`` clause whose
arbiter is the partial unique index ``oracle_predictions_dedup_unique``.

PostgreSQL raises ``42P10`` (*"no unique or exclusion constraint matching the
ON CONFLICT specification"*) if that index does not yet exist — which would make
every insert from these paths fail outright on a DB where migration
``0055`` / ``migrations/versions/oracle_predictions_dedup.sql`` has not been
applied.

``OracleEngine._ensure_tables`` already creates this index ``IF NOT EXISTS`` on
construction, but the ``publish.py`` and ``obsidian_agent.py`` write paths do
*not* necessarily build an ``OracleEngine`` first (they are reached directly
from the astrogrid / oracle API routers and the vault sync loop). This helper
gives them the same self-healing guarantee so their ``ON CONFLICT`` is safe
pre-migration: the index is created ``IF NOT EXISTS`` at most once per process.

The DDL here is the *single source of truth* for the index definition and must
stay byte-for-byte identical to the bootstrap in ``oracle/engine.py`` and the
migrations, so the ``ON CONFLICT`` arbiter always resolves to the same index.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

# Canonical partial unique index backing the natural-key dedup. Keep this
# predicate identical to oracle/engine.py:_ensure_tables and the migrations.
# NOTE: created without CONCURRENTLY here because this runs inside the live
# request/cycle path against an existing (already-deduped) table; CONCURRENTLY
# cannot run inside the implicit transaction these callers use, and the
# concurrent build belongs in the migration (0055). IF NOT EXISTS makes this a
# cheap no-op once the migration (or a prior call) has created the index.
_DEDUP_INDEX_DDL = """
    CREATE UNIQUE INDEX IF NOT EXISTS oracle_predictions_dedup_unique
    ON oracle_predictions (
        ticker,
        direction,
        expiry,
        prediction_type,
        (COALESCE(model_version, '')),
        ((created_at AT TIME ZONE 'UTC')::date)
    )
    WHERE dedup_keep = TRUE
"""


def ensure_dedup_index(engine: Engine) -> None:
    """Ensure ``oracle_predictions_dedup_unique`` exists, at most once / process.

    Idempotent and safe to call on every write: the work runs exactly once per
    process via ``schema_guard.ensure_once`` and is a no-op (``IF NOT EXISTS``)
    thereafter. Never raises into the caller's hot path — a failure to create
    the index (e.g. insufficient privileges) is swallowed so the writer can
    still attempt its insert exactly as it would have without this guard.
    """
    from schema_guard import ensure_once

    def _create() -> None:
        try:
            with engine.begin() as conn:
                conn.execute(text(_DEDUP_INDEX_DDL))
        except Exception:  # pragma: no cover - defensive; never break the writer
            # Pre-migration safety is best-effort. If we cannot create the
            # index (locked, no DDL grant, etc.) we let the subsequent insert
            # surface the real error instead of masking it here.
            pass

    ensure_once("oracle_predictions.dedup_index", _create)
