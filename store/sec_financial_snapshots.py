"""Atomic, versioned SEC financial evidence storage and owned-row projection.

Requires the sec_financial_snapshots migration. Never bootstraps schema or
reconciles unmarked legacy rows. No environment/default database discovery.
"""
from __future__ import annotations

import gzip
import json
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text

from ingestion.altdata.sec_financial_snapshots import FinancialSnapshot, validate_snapshot

_LOCK = text("SELECT pg_advisory_xact_lock(hashtextextended(:actor_id, 7363))")
_CURRENT = text('''SELECT h.snapshot_id, h.request_started_at, s.normalized_rows
    FROM sec_financial_snapshot_heads h
    JOIN sec_financial_snapshots s ON s.snapshot_id = h.snapshot_id
    WHERE h.actor_id = :actor_id FOR UPDATE OF h''')
_ARCHIVE = text('''INSERT INTO sec_financial_snapshots (
    snapshot_id, actor_id, cik, source_url, source_sha256, normalized_sha256,
    normalizer_version, source_gzip, normalized_rows, fx_inputs,
    first_request_started_at, first_received_at
) VALUES (:snapshot_id, :actor_id, :cik, :source_url, :source_sha256, :normalized_sha256,
    :normalizer_version, :source_gzip, CAST(:normalized_rows AS JSONB), CAST(:fx_inputs AS JSONB),
    :request_started_at, :received_at) ON CONFLICT (snapshot_id) DO NOTHING''')
_DELETE_OWNED = text('''DELETE FROM capital_flows WHERE actor_id = :actor_id
    AND counterparty_id IS NULL AND period_type IN ('annual', 'quarter')
    AND split_part(source_filing, ' ', 1) IN
      ('10-K','10-K/A','10-Q','10-Q/A','20-F','20-F/A','40-F','40-F/A','6-K','6-K/A')
    AND split_part(source_filing, ' ', 2) = '[SEC-XBRL-PERIOD-V1]' ''')
_HEAD = text('''INSERT INTO sec_financial_snapshot_heads
    (actor_id, snapshot_id, request_started_at, received_at)
    VALUES (:actor_id, :snapshot_id, :request_started_at, :received_at)
    ON CONFLICT (actor_id) DO UPDATE SET snapshot_id=EXCLUDED.snapshot_id,
        request_started_at=EXCLUDED.request_started_at, received_at=EXCLUDED.received_at,
        updated_at=NOW()''')


def _revision_regressed(previous: list[dict[str, Any]], incoming: list[dict[str, Any]]) -> bool:
    old = {(r['fiscal_period'], r['period_type'], r['flow_type']): r['filed_date'] for r in previous}
    return any(r['filed_date'] < old.get((r['fiscal_period'], r['period_type'], r['flow_type']), '')
               for r in incoming)


def write_snapshot(engine, snapshot: FinancialSnapshot) -> dict[str, Any]:
    """Archive source, then atomically replace only this producer's projection.

    Serializes per actor. Older captures cannot undo newer ones, identical
    content is idempotent, equal-time conflicting captures fail closed. A
    successfully admitted full response with zero normalized rows clears the
    prior owned projection; absent values are unavailable, not zero. Fetch or
    admission failures never reach this function. All SQL failures propagate.
    """
    from ingestion.altdata.sec_xbrl_financials import _INSERT_SQL

    validate_snapshot(snapshot)
    now = datetime.now(timezone.utc)
    if snapshot.received_at > now or snapshot.request_started_at > now:
        raise ValueError('Future source receipt cannot advance current projection')
    params = dict(snapshot_id=snapshot.snapshot_id, actor_id=snapshot.actor_id, cik=snapshot.cik,
                  source_url=snapshot.source_url, source_sha256=snapshot.source_sha256,
                  normalized_sha256=snapshot.normalized_sha256, normalizer_version=snapshot.normalizer_version,
                  source_gzip=gzip.compress(snapshot.source_body, mtime=0),
                  normalized_rows=snapshot.normalized_json, fx_inputs=snapshot.fx_json,
                  request_started_at=snapshot.request_started_at, received_at=snapshot.received_at)
    rows = snapshot.rows
    with engine.begin() as conn:
        conn.execute(_LOCK, params)
        current = conn.execute(_CURRENT, params).mappings().first()
        if current and snapshot.request_started_at == current['request_started_at'] and snapshot.snapshot_id != current['snapshot_id']:
            raise ValueError('Conflicting snapshot at the same request-start time')
        conn.execute(_ARCHIVE, params)
        if current and snapshot.request_started_at < current['request_started_at']:
            return dict(status='ignored_older_capture', rows_written=0, snapshot_id=snapshot.snapshot_id)
        if current and snapshot.snapshot_id != current['snapshot_id']:
            old = current['normalized_rows']
            old = json.loads(old) if isinstance(old, str) else old
            if _revision_regressed(old, rows):
                return dict(status='rejected_revision_regression', rows_written=0, snapshot_id=snapshot.snapshot_id)
        if current and snapshot.snapshot_id == current['snapshot_id']:
            conn.execute(_HEAD, params)
            return dict(status='unchanged', rows_written=0, snapshot_id=snapshot.snapshot_id)
        deleted = conn.execute(_DELETE_OWNED, params).rowcount
        for row in rows:
            conn.execute(_INSERT_SQL, dict(actor_id=snapshot.actor_id,
                fiscal_period=row['fiscal_period'], period_type=row['period_type'], flow_type=row['flow_type'],
                direction=row['direction'], amount_usd=row['amount_usd'], source_filing=row['source_filing'],
                confidence=row['confidence'], currency=row['currency']))
        conn.execute(_HEAD, params)
    return dict(status='applied', rows_written=len(rows), rows_removed=deleted, snapshot_id=snapshot.snapshot_id)
