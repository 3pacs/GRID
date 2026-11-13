"""Explicit bounded observations, not historical availability reconstruction.

Uses a new connection, never the writer transaction. One statement captures all
canonical versions/revisions for explicit act keys. A database clock sampled
AFTER that statement is conservative evidence those source bytes were committed
by that time. It is NOT the receipt's publication/consumer-arrival timestamp.

Retry the same request ID to recover a committed receipt after lost ACK. Use a
new ID for a new observation. No existing producer, schedule or reader calls this
module. Canonical history/admission gates remain unchanged.
"""
from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

CONTRACT = 'people-events-committed-observation-v1'
MAX_ACTS = 100
MAX_ROWS = 10000
MAX_BYTES = 4_000_000

_CAPTURE = text("""
WITH selected AS MATERIALIZED (
 SELECT e.* FROM people_events e
 JOIN jsonb_to_recordset(CAST(:scope AS jsonb)) AS k(channel text, dedup_key text)
 USING (channel, dedup_key) ORDER BY e.id LIMIT :cap
), revisions AS MATERIALIZED (
 SELECT r.* FROM people_event_revisions r JOIN selected s ON s.id=r.event_id
 ORDER BY r.id LIMIT :cap
)
SELECT jsonb_build_object(
 'contract', :contract, 'scope', CAST(:scope AS jsonb),
 'snapshot', txid_current_snapshot()::text,
 'source_statement_at', statement_timestamp(),
 'source_database', current_database(), 'source_schema', current_schema(),
 'events', COALESCE((SELECT jsonb_agg(to_jsonb(s) ORDER BY s.id) FROM selected s), '[]'::jsonb),
 'revisions', COALESCE((SELECT jsonb_agg(to_jsonb(r) ORDER BY r.id) FROM revisions r), '[]'::jsonb)
)::text
""")


@dataclass(frozen=True)
class Observation:
    request_id: str
    scope_sha256: str
    observation_contract: str
    observed_at: datetime
    payload_text: str
    payload_sha256: str
    receipt_sha256: str

    def verify(self) -> dict[str, Any]:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError('observation clock must be aware')
        if _receipt_digest(self.request_id, self.scope_sha256, self.observed_at,
                           self.payload_sha256) != self.receipt_sha256:
            raise ValueError('observation receipt hash mismatch')
        if self.observation_contract != CONTRACT:
            raise ValueError('unsupported observation contract')
        if hashlib.sha256(self.payload_text.encode()).hexdigest() != self.payload_sha256:
            raise ValueError('observation payload hash mismatch')
        payload = json.loads(self.payload_text)
        if payload.get('contract') != CONTRACT:
            raise ValueError('payload contract mismatch')
        source_clock = datetime.fromisoformat(payload['source_statement_at'])
        if source_clock.utcoffset() is None or source_clock > self.observed_at:
            raise ValueError('invalid source observation clock window')
        scope = _scope([(k['channel'], k['dedup_key']) for k in payload['scope']])
        if hashlib.sha256(scope.encode()).hexdigest() != self.scope_sha256:
            raise ValueError('observation scope hash mismatch')
        return payload


def _receipt_digest(request_id, scope_hash, observed_at, payload_hash):
    envelope = dict(request_id=request_id, scope_sha256=scope_hash, contract=CONTRACT,
                    observed_at=observed_at.astimezone(timezone.utc).isoformat(),
                    payload_sha256=payload_hash)
    return hashlib.sha256(json.dumps(envelope, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def _scope(acts: list[tuple[str, str]]) -> str:
    if not isinstance(acts, (list, tuple)) or not 1 <= len(acts) <= MAX_ACTS:
        raise ValueError('explicit bounded act keys required')
    normalized = []
    for act in acts:
        if not isinstance(act, (list, tuple)) or len(act) != 2:
            raise ValueError('act must contain channel and dedup_key')
        if any(not isinstance(x, str) or not x.strip() or len(x) > 512 for x in act):
            raise ValueError('invalid act key')
        normalized.append(tuple(act))
    return json.dumps([dict(channel=c, dedup_key=k) for c, k in sorted(set(normalized))],
                      separators=(',', ':'), sort_keys=True)


def _observation(row) -> Observation:
    result = Observation(**dict(row))
    result.verify()
    return result


def capture_observation(engine: Engine, *, request_id: str,
                        acts: list[tuple[str, str]]) -> Observation:
    """Persist one immutable source observation, or recover the exact prior one.

    Empty/missing source acts are recorded as empty evidence, not a signal. Bounds
    refuse rather than truncate. SQL/schema errors roll back, never auto-migrate.
    A request ID reused with another scope refuses. No external callbacks run
    between source capture and receipt commit.
    """
    if not isinstance(engine, Engine) or engine.dialect.name != 'postgresql':
        raise ValueError('a PostgreSQL Engine, not a writer connection, is required')
    if not isinstance(request_id, str) or not request_id.strip() or len(request_id) > 128:
        raise ValueError('invalid request_id')
    scope = _scope(acts)
    scope_hash = hashlib.sha256(scope.encode()).hexdigest()
    # Collision only serializes unrelated requests; primary key/scope check guards identity.
    lock = int.from_bytes(hashlib.sha256(('people-observation:' + request_id).encode()).digest()[:8],
                          'big', signed=True)
    with engine.connect().execution_options(isolation_level='READ COMMITTED') as conn:
        with conn.begin():
            conn.execute(text("SET LOCAL synchronous_commit = on"))
            conn.execute(text("SET LOCAL timezone = 'UTC'"))
            conn.execute(text("SET LOCAL lock_timeout = '5s'"))
            conn.execute(text("SET LOCAL statement_timeout = '15s'"))
            conn.execute(text('SELECT pg_advisory_xact_lock(:key)'), dict(key=lock))
            row = conn.execute(text('SELECT * FROM people_event_observations WHERE request_id=:id'),
                               dict(id=request_id)).mappings().first()
            if row is not None:
                prior = _observation(row)
                if prior.scope_sha256 != scope_hash:
                    raise ValueError('request_id already belongs to another scope')
                return prior
            before = conn.execute(text('SELECT clock_timestamp()')).scalar_one()
            payload_text = conn.execute(_CAPTURE, dict(scope=scope, cap=MAX_ROWS + 1,
                                                      contract=CONTRACT)).scalar_one()
            observed = conn.execute(text('SELECT clock_timestamp()')).scalar_one()
            if observed < before:
                raise ValueError('database clock moved backwards during observation')
            payload = json.loads(payload_text)
            if (len(payload['events']) > MAX_ROWS or len(payload['revisions']) > MAX_ROWS
                    or len(payload_text.encode()) > MAX_BYTES):
                raise ValueError('observation exceeds bounded capture; no receipt written')
            payload_hash = hashlib.sha256(payload_text.encode()).hexdigest()
            result = Observation(request_id, scope_hash, CONTRACT, observed, payload_text,
                                 payload_hash, _receipt_digest(request_id, scope_hash, observed, payload_hash))
            result.verify()
            conn.execute(text('''INSERT INTO people_event_observations
                (request_id, scope_sha256, observation_contract, observed_at, payload_text, payload_sha256, receipt_sha256)
                VALUES (:request_id, :scope_sha256, :observation_contract, :observed_at,
                         :payload_text, :payload_sha256, :receipt_sha256)'''), vars(result))
        return result


def read_observation(engine: Engine, *, request_id: str) -> Observation | None:
    """SELECT-only receipt lookup; returns no reconstructed/PIT/trading signal."""
    if not isinstance(engine, Engine) or engine.dialect.name != 'postgresql':
        raise ValueError('PostgreSQL Engine required')
    with engine.connect() as conn:
        row = conn.execute(text('SELECT * FROM people_event_observations WHERE request_id=:id'),
                           dict(id=request_id)).mappings().first()
        return None if row is None else _observation(row)
