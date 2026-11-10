"""Forward Oracle identity and immutable first-forecast storage contract.

The migration replaces the legacy arbiter atomically. Runtime code never
replaces an existing index: old schemas fail closed until migration. Legacy
writers cannot infer the new arbiter, so cannot overwrite forward forecasts.
"""
from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone

from sqlalchemy import text
from sqlalchemy.engine import Engine

FORWARD_RECORDING_POLICY = "oracle_forward_v2"
EXTERNAL_RECORDING_POLICY = "oracle_external_v2"

_DEDUP_INDEX_DDL = """
    CREATE UNIQUE INDEX IF NOT EXISTS oracle_predictions_dedup_unique
    ON oracle_predictions (
        ticker,
        direction,
        expiry,
        prediction_type,
        model_name,
        (COALESCE(model_version, '')),
        ((created_at AT TIME ZONE 'UTC')::date),
        (COALESCE(recording_policy, 'legacy'))
    )
    WHERE dedup_keep = TRUE
"""


def forecast_identity(*, ticker: str, direction: str, expiry: date,
                      prediction_type: str, model_name: str,
                      model_version: str | None, timestamp: datetime,
                      recording_policy: str = FORWARD_RECORDING_POLICY) -> str:
    """Stable full natural key, namespaced away from historical primary keys."""
    if timestamp.tzinfo is None or timestamp.utcoffset() is None:
        raise ValueError("prediction decision timestamp must be timezone-aware")
    key = [recording_policy, ticker, direction, expiry.isoformat(), prediction_type,
           model_name, model_version or "", timestamp.astimezone(timezone.utc).date().isoformat()]
    return "of2:" + hashlib.sha256(json.dumps(key, separators=(",", ":")).encode()).hexdigest()


def ensure_dedup_index(engine: Engine) -> None:
    """Ensure the v2 arbiter exists; never silently accept a legacy/invalid index.

    Checks are per engine/transaction, avoiding process-global schema caches
    that incorrectly skip verification on another database. A migrated schema
    performs only a catalogue read; index creation is for fresh bootstrap only.
    """
    with engine.begin() as conn:
        ensure_dedup_index_on_connection(conn)


def ensure_dedup_index_on_connection(conn) -> None:
    """Same verification inside bootstrap's existing transaction."""
    row = conn.execute(text("""
        SELECT i.indisvalid, pg_get_indexdef(i.indexrelid)
        FROM pg_index i
        WHERE i.indexrelid = to_regclass('oracle_predictions_dedup_unique')
    """)).fetchone()
    if row is None:
        conn.execute(text(_DEDUP_INDEX_DDL))
    elif not row[0] or "recording_policy" not in row[1] or "model_name" not in row[1]:
        raise RuntimeError("Oracle forward identity migration required; legacy/invalid dedup index")
