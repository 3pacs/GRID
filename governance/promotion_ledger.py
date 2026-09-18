"""GRID promotion ledger — append-only record of what was promoted, by
whom, and against what evidence.

Motivation (GRID W7, 2026-09-18): research outputs — signal weight
overrides, model promotions, signal policy changes — must not mutate
live weights or trading state without a designated, auditable
promotion step. This module is that step. It is deliberately dumb: it
does not evaluate whether a promotion is a *good* idea (that's
validation/gates.py + the held-out promotion protocol in
docs/reference/LEARNING_PROMOTION_PROTOCOL.md); it only records, in an
immutable ledger, that a human (or an authorized process acting for
one) recommended something and — as a SEPARATE, later act — that
someone approved it.

Design invariants
------------------
* **Never UPDATE, never DELETE.** Every call in this module that
  changes state does so with a fresh INSERT. ``approve()`` does not
  mutate the recommendation row it approves; it inserts a NEW row
  whose ``recommendation_id`` points back at it. There is intentionally
  no function in this module's public API that can update or delete a
  row — if you find yourself wanting one, you're building the wrong
  thing; write a new record instead (a superseding recommendation, a
  rollback record, etc.).
* **Recommend and approve are separate.** ``recommend()`` always
  writes ``approved_by=None, approved_at=None``. Only ``approve()``
  (called with a *different* identity, in a later call) can produce a
  record that ``is_approved()`` will count. Nothing in this module
  lets one call produce an already-approved record — self-approval by
  construction requires two calls, and callers are responsible for
  making sure they're made by different, authorized identities.
* **Consumers check ``is_approved()``, not the recommendation.** A
  recommendation existing is not authorization to apply anything.

Schema: see migrations/versions/promotion_ledger_0918.py (revision
``promotion_ledger_0918``, down_revision
``god_view_market_tables_20260918``). ``PROMOTION_LEDGER_DDL`` here is
the single source of truth for the table shape — the migration
executes it verbatim, and tests call ``ensure_schema()`` against an
in-memory sqlite engine so there is exactly one place the columns are
defined.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

VALID_KINDS: tuple[str, ...] = ("weight_override", "model", "signal_policy")

TABLE_NAME = "promotion_ledger"

# Single source of truth for the table shape. Kept as individual
# statements (rather than one multi-statement string) so it can be
# executed identically via SQLAlchemy `text()` against both Postgres
# (the real deploy target, via the Alembic revision) and an in-memory
# sqlite engine (tests — no local Postgres per this workstream's
# constraints). Timestamps are always supplied by Python
# (datetime.now(timezone.utc)), never a SQL-side default, so no
# dialect-specific NOW()/CURRENT_TIMESTAMP divergence matters.
PROMOTION_LEDGER_DDL: tuple[str, ...] = (
    """
    CREATE TABLE IF NOT EXISTS promotion_ledger (
        id                  TEXT PRIMARY KEY,
        kind                TEXT NOT NULL CHECK (kind IN ('weight_override', 'model', 'signal_policy')),
        subject_hash        TEXT NOT NULL,
        evaluation_version  TEXT NOT NULL,
        evidence_ref        TEXT,
        recommended_by      TEXT NOT NULL,
        approved_by         TEXT,
        approved_at         TIMESTAMPTZ,
        canary_scope        TEXT,
        rollback_ref        TEXT,
        recommendation_id   TEXT REFERENCES promotion_ledger(id),
        created_at          TIMESTAMPTZ NOT NULL
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_promotion_ledger_subject
        ON promotion_ledger (kind, subject_hash, evaluation_version)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_promotion_ledger_recommendation
        ON promotion_ledger (recommendation_id)
    """,
)


def ensure_schema(engine: Engine) -> None:
    """Create the promotion_ledger table + indexes if they don't exist.

    Idempotent (every statement is CREATE ... IF NOT EXISTS). Used by
    tests against an in-memory sqlite engine; in real deployments the
    table is created by the Alembic revision instead — this exists so
    both paths run the exact same DDL.
    """
    with engine.begin() as conn:
        for stmt in PROMOTION_LEDGER_DDL:
            conn.execute(text(stmt))


def compute_subject_hash(payload: dict[str, Any]) -> str:
    """Deterministic sha256 hex digest of ``payload`` (canonical JSON,
    sorted keys). Callers own what goes into ``payload`` — e.g.
    ``intelligence.signal_weight_overrides.compute_subject_hash()``
    hashes ``{"overrides": {...}, "evaluation_version": "..."}``.
    """
    encoded = json.dumps(payload, sort_keys=True, default=str)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _row_to_dict(row: Any) -> dict[str, Any]:
    return dict(row._mapping)


def recommend(
    engine: Engine,
    *,
    kind: str,
    subject_hash: str,
    evaluation_version: str,
    evidence_ref: str,
    recommended_by: str,
    canary_scope: str | None = None,
    rollback_ref: str | None = None,
) -> dict[str, Any]:
    """Record a recommendation. Always writes approved_by=None,
    approved_at=None — a recommendation is never, by itself,
    authorization to apply anything.

    Returns the inserted record as a dict.
    """
    if kind not in VALID_KINDS:
        raise ValueError(f"kind must be one of {VALID_KINDS}, got {kind!r}")
    if not recommended_by or not recommended_by.strip():
        raise ValueError("recommended_by is required")

    record_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    record = {
        "id": record_id,
        "kind": kind,
        "subject_hash": subject_hash,
        "evaluation_version": evaluation_version,
        "evidence_ref": evidence_ref,
        "recommended_by": recommended_by,
        "approved_by": None,
        "approved_at": None,
        "canary_scope": canary_scope,
        "rollback_ref": rollback_ref,
        "recommendation_id": None,
        "created_at": now,
    }

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO promotion_ledger "
                "(id, kind, subject_hash, evaluation_version, evidence_ref, "
                " recommended_by, approved_by, approved_at, canary_scope, "
                " rollback_ref, recommendation_id, created_at) "
                "VALUES (:id, :kind, :subject_hash, :evaluation_version, "
                " :evidence_ref, :recommended_by, :approved_by, :approved_at, "
                " :canary_scope, :rollback_ref, :recommendation_id, :created_at)"
            ),
            record,
        )

    log.info(
        "promotion_ledger: recommendation {id} recorded (kind={k}, "
        "subject_hash={s}, evaluation_version={v}, by={b})",
        id=record_id,
        k=kind,
        s=subject_hash,
        v=evaluation_version,
        b=recommended_by,
    )
    return record


def approve(
    engine: Engine,
    *,
    recommendation_id: str,
    approved_by: str,
    canary_scope: str | None = None,
    rollback_ref: str | None = None,
) -> dict[str, Any]:
    """Approve an existing recommendation by writing a NEW record that
    references it — never mutates the recommendation row.

    Parameters:
        recommendation_id: id of a prior ``recommend()`` record.
        approved_by: identity of the approver (must be provided by the
            caller; this module does not enforce that it differs from
            ``recommended_by`` — that authorization check belongs to
            the calling workflow / operator process).
        canary_scope, rollback_ref: optional; if omitted, inherited
            from the recommendation.

    Returns the new approval record as a dict.

    Raises:
        ValueError: if the recommendation doesn't exist, or the given
            id already refers to an approval record (approved_by set)
            rather than a recommendation.
    """
    if not approved_by or not approved_by.strip():
        raise ValueError("approved_by is required")

    with engine.connect() as conn:
        row = conn.execute(
            text("SELECT * FROM promotion_ledger WHERE id = :id"),
            {"id": recommendation_id},
        ).fetchone()

    if row is None:
        raise ValueError(f"No promotion_ledger record {recommendation_id!r} found")

    rec = _row_to_dict(row)
    if rec.get("approved_by") is not None:
        raise ValueError(
            f"Record {recommendation_id!r} is already an approval "
            "(approved_by is set) — approve() takes a recommendation id, "
            "not another approval"
        )

    new_id = uuid.uuid4().hex
    now = datetime.now(timezone.utc)
    approval = {
        "id": new_id,
        "kind": rec["kind"],
        "subject_hash": rec["subject_hash"],
        "evaluation_version": rec["evaluation_version"],
        "evidence_ref": rec["evidence_ref"],
        "recommended_by": rec["recommended_by"],
        "approved_by": approved_by,
        "approved_at": now,
        "canary_scope": canary_scope if canary_scope is not None else rec.get("canary_scope"),
        "rollback_ref": rollback_ref if rollback_ref is not None else rec.get("rollback_ref"),
        "recommendation_id": recommendation_id,
        "created_at": now,
    }

    with engine.begin() as conn:
        conn.execute(
            text(
                "INSERT INTO promotion_ledger "
                "(id, kind, subject_hash, evaluation_version, evidence_ref, "
                " recommended_by, approved_by, approved_at, canary_scope, "
                " rollback_ref, recommendation_id, created_at) "
                "VALUES (:id, :kind, :subject_hash, :evaluation_version, "
                " :evidence_ref, :recommended_by, :approved_by, :approved_at, "
                " :canary_scope, :rollback_ref, :recommendation_id, :created_at)"
            ),
            approval,
        )

    log.info(
        "promotion_ledger: approval {id} recorded for recommendation {rid} "
        "by {a}",
        id=new_id,
        rid=recommendation_id,
        a=approved_by,
    )
    return approval


def is_approved(
    engine: Engine,
    *,
    kind: str,
    subject_hash: str,
    evaluation_version: str,
) -> bool:
    """True iff an approved record exists matching kind + subject_hash
    + evaluation_version exactly (approved_by AND approved_at both
    set). This is the check consumers (e.g.
    intelligence.signal_weight_overrides) must use before applying
    anything gated by this ledger."""
    with engine.connect() as conn:
        row = conn.execute(
            text(
                "SELECT id FROM promotion_ledger "
                "WHERE kind = :kind AND subject_hash = :subject_hash "
                "AND evaluation_version = :evaluation_version "
                "AND approved_by IS NOT NULL AND approved_at IS NOT NULL "
                "LIMIT 1"
            ),
            {
                "kind": kind,
                "subject_hash": subject_hash,
                "evaluation_version": evaluation_version,
            },
        ).fetchone()
    return row is not None


def list_records(
    engine: Engine,
    *,
    kind: str | None = None,
    subject_hash: str | None = None,
) -> list[dict[str, Any]]:
    """Read-only listing, newest first. For audits and tests — never
    used by consumers to decide whether to apply something (use
    ``is_approved()`` for that)."""
    clauses = []
    params: dict[str, Any] = {}
    if kind is not None:
        clauses.append("kind = :kind")
        params["kind"] = kind
    if subject_hash is not None:
        clauses.append("subject_hash = :subject_hash")
        params["subject_hash"] = subject_hash
    where = f"WHERE {' AND '.join(clauses)}" if clauses else ""

    with engine.connect() as conn:
        rows = conn.execute(
            text(f"SELECT * FROM promotion_ledger {where} ORDER BY created_at DESC"),
            params,
        ).fetchall()
    return [_row_to_dict(r) for r in rows]
