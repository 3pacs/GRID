"""
GRID governance — additive labels for legacy ``hypothesis_registry`` verdicts.

The registry's PASSED and TESTING states were never validated findings
(read-only audit, 2026-11-06, PR #1008):

* PASSED: 10,609 rows but 35 distinct statements, all TACTICAL, all from
  ``analysis/backtest_scanner.py``. In-sample 504-day Sharpe >= 1 and win rate
  >= 55%, no holdout, no multiple-testing correction, re-inserted up to ten per
  run without dedup.
* TESTING: 30,821 rows. 30,782 are PASSED rows the Hermes LLM review flagged
  ("LLM review flag: Unrealistic assumptions ..."); 39 are
  ``hypothesis_tester`` inconclusives ("Inconclusive: best_in_range=...") or
  AutoAgent rows that never got a verdict. Nothing ever reads TESTING again.

Rows are never rewritten. ``hypothesis_registry_labels`` (append-only,
migration ``hypothesis_registry_labels_20261107``) records one label per row
and label version; :func:`label_legacy_registry` writes them (dry run by
default). Every consumer that treats PASSED as validated reads through
:func:`passed_validation` or the same predicate inline:

    state = 'PASSED'
    AND created_at >= LEGACY_CUTOFF
    AND NOT EXISTS (SELECT 1 FROM hypothesis_registry_labels l WHERE l.registry_id = h.id)

The cutoff makes every pre-v2 PASSED row legacy even before the label rows
are written (fail closed); the labels are the durable, auditable record.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from loguru import logger as log
from sqlalchemy import text
from sqlalchemy.engine import Engine

LABEL_VERSION = "legacy-relabel/1"
# PASSED rows created before this instant came from producers with no holdout
# or multiple-testing control and are never treated as validated.
LEGACY_CUTOFF = datetime(2026, 11, 7, tzinfo=timezone.utc)

LEGACY_UNVALIDATED = "LEGACY_UNVALIDATED"
LEGACY_LLM_FLAGGED = "LEGACY_LLM_FLAGGED"
LEGACY_INCONCLUSIVE = "LEGACY_INCONCLUSIVE"
LABELS = (LEGACY_UNVALIDATED, LEGACY_LLM_FLAGGED, LEGACY_INCONCLUSIVE)

LLM_FLAG_PREFIX = "LLM review flag:%"

_REASONS = {
    LEGACY_UNVALIDATED: "PASSED by an in-sample scan: no holdout, no multiple-testing "
                        "correction, duplicated without dedup (audit 2026-11-06, #1008)",
    LEGACY_LLM_FLAGGED: "PASSED row the Hermes LLM review flagged back to TESTING; "
                        "never re-tested",
    LEGACY_INCONCLUSIVE: "hypothesis_tester inconclusive; TESTING is never revisited",
}

# One planned label per unlabelled legacy row. :label_version and :cutoff are
# bound; the CASE is the whole labelling rule.
_PLAN_SQL = text("""
    SELECT CASE
             WHEN h.state = 'PASSED' THEN 'LEGACY_UNVALIDATED'
             WHEN h.kill_reason LIKE :llm_flag THEN 'LEGACY_LLM_FLAGGED'
             ELSE 'LEGACY_INCONCLUSIVE'
           END AS label,
           COUNT(*) AS n
    FROM hypothesis_registry h
    WHERE h.state IN ('PASSED', 'TESTING')
      AND h.created_at < :cutoff
      AND NOT EXISTS (
          SELECT 1 FROM hypothesis_registry_labels l
          WHERE l.registry_id = h.id AND l.label_version = :label_version
      )
    GROUP BY 1
    ORDER BY 1
""")

_EXISTING_SQL = text("""
    SELECT label, COUNT(*) FROM hypothesis_registry_labels
    WHERE label_version = :label_version
    GROUP BY 1 ORDER BY 1
""")

_INSERT_SQL = text("""
    INSERT INTO hypothesis_registry_labels
        (registry_id, label, prior_state, reason, label_version, decided_by)
    SELECT h.id,
           CASE
             WHEN h.state = 'PASSED' THEN 'LEGACY_UNVALIDATED'
             WHEN h.kill_reason LIKE :llm_flag THEN 'LEGACY_LLM_FLAGGED'
             ELSE 'LEGACY_INCONCLUSIVE'
           END,
           h.state,
           CASE
             WHEN h.state = 'PASSED' THEN :reason_unvalidated
             WHEN h.kill_reason LIKE :llm_flag THEN :reason_flagged
             ELSE :reason_inconclusive
           END,
           :label_version,
           :decided_by
    FROM hypothesis_registry h
    WHERE h.state IN ('PASSED', 'TESTING')
      AND h.created_at < :cutoff
    ON CONFLICT (registry_id, label_version) DO NOTHING
""")

_VALIDATION_SQL = text("""
    SELECT h.state,
           h.created_at,
           (SELECT l.label FROM hypothesis_registry_labels l
            WHERE l.registry_id = h.id ORDER BY l.id DESC LIMIT 1) AS label
    FROM hypothesis_registry h
    WHERE h.id = :id
""")


def _params(**extra: Any) -> dict[str, Any]:
    return {"llm_flag": LLM_FLAG_PREFIX, "cutoff": LEGACY_CUTOFF,
            "label_version": LABEL_VERSION, **extra}


def label_legacy_registry(engine: Engine, *, dry_run: bool = True,
                          decided_by: str | None = None) -> dict[str, Any]:
    """Label every legacy PASSED/TESTING registry row once (idempotent).

    ``dry_run`` (default) runs in a READ ONLY transaction and returns the
    planned counts. ``dry_run=False`` needs ``decided_by`` (who authorised the
    relabel) and inserts the labels; rows already labelled at this
    ``LABEL_VERSION`` are skipped, so a re-run inserts nothing.
    """
    if not dry_run and not (decided_by or "").strip():
        raise ValueError("decided_by is required to write labels")
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            if dry_run:
                conn.execute(text("SET TRANSACTION READ ONLY"))
            planned = {r[0]: int(r[1]) for r in conn.execute(_PLAN_SQL, _params())}
            existing = {r[0]: int(r[1]) for r in conn.execute(
                _EXISTING_SQL, {"label_version": LABEL_VERSION})}
            inserted = 0
            if not dry_run:
                inserted = conn.execute(_INSERT_SQL, _params(
                    reason_unvalidated=_REASONS[LEGACY_UNVALIDATED],
                    reason_flagged=_REASONS[LEGACY_LLM_FLAGGED],
                    reason_inconclusive=_REASONS[LEGACY_INCONCLUSIVE],
                    decided_by=decided_by.strip(),
                )).rowcount
            if dry_run:
                trans.rollback()
            else:
                trans.commit()
        except Exception:
            trans.rollback()
            raise
    result = {
        "dry_run": dry_run,
        "label_version": LABEL_VERSION,
        "cutoff": LEGACY_CUTOFF.isoformat(),
        "planned": {label: planned.get(label, 0) for label in LABELS},
        "already_labelled": {label: existing.get(label, 0) for label in LABELS},
        "inserted": inserted,
    }
    log.info("hypothesis_registry legacy labels: {r}", r=result)
    return result


def passed_validation(conn: Any, registry_id: int | None) -> dict[str, Any]:
    """Whether a registry row counts as a validated PASSED hypothesis.

    Returns ``{"validated": bool, "state": ..., "label": ..., "reason": ...}``.
    A PASSED row is validated only when it was created at or after
    ``LEGACY_CUTOFF`` and carries no label.
    """
    if registry_id is None:
        return {"validated": False, "state": None, "label": None,
                "reason": "no hypothesis linked"}
    row = conn.execute(_VALIDATION_SQL, {"id": registry_id}).fetchone()
    if row is None:
        return {"validated": False, "state": None, "label": None,
                "reason": "hypothesis not found"}
    state, created_at, label = row[0], row[1], row[2]
    if state != "PASSED":
        return {"validated": False, "state": state, "label": label,
                "reason": f"hypothesis state is {state}, must be PASSED"}
    if label:
        return {"validated": False, "state": state, "label": label,
                "reason": f"hypothesis is labelled {label}"}
    if created_at is None or _aware(created_at) < LEGACY_CUTOFF:
        return {"validated": False, "state": state, "label": LEGACY_UNVALIDATED,
                "reason": f"PASSED before {LEGACY_CUTOFF.date()} is legacy (unvalidated)"}
    return {"validated": True, "state": state, "label": None, "reason": "validated PASSED"}


def _aware(ts: datetime) -> datetime:
    return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)
