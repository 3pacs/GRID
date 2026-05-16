"""Idle-fleet goal queue — Day 1 of IDLE-FLEET-AGENT-LOOP PoC.

Revision ID: idle_fleet_goal_queue_day1
Revises: tps_phase0_snapshots
Create Date: 2026-05-16 00:00:00.000000

Creates two tables in ``griddb`` that back the idle-fleet goal-queue PoC:

* ``goal_queue``  — one row per unit of work (e.g. "rescore hypothesis X").
  Workers running on Tailnet nodes (gridz4, ocr-node, z400, koala, redbox)
  poll, atomically claim a row (state transitions ``claimable`` ->
  ``claimed`` under ``SELECT ... FOR UPDATE SKIP LOCKED``), run the
  associated handler, and write the result.

* ``goal_results`` — one row per terminal goal outcome (success or
  failure). Kept separate from ``goal_queue`` so we can purge stale
  ``done`` queue rows without losing the audit trail.

Design notes:

* ``goal_type`` is a free-form string (``score_active_hypothesis``,
  ``write_postmortem``, ``scan_options``, ``expand_sector``, ...).
  Handlers are dispatched by name in ``scripts/goal_worker.py``.
* ``target_id`` is the domain row id the goal operates on (e.g. a
  ``discovered_hypotheses.id``). Combined with ``goal_type`` it forms
  the dedupe key — see the UNIQUE index below.
* ``hardware_tier`` is one of ``cpu``, ``medium_gpu``, ``large_gpu``,
  ``vision``. Workers refuse goals above their tier.
* ``allow_cloud`` defaults to ``FALSE`` — workers refuse cloud-LLM
  goals unless this is explicitly set. Locked decision #1 of the PoC.
* ``max_duty_cycle`` is an optional per-goal override of the
  worker-config default (locked decision #4: 50%).
* ``lease_expires_at`` is set on claim. A reaper (Day 2 work) sweeps
  expired leases back to ``claimable``.
* ``priority`` is integer, higher = served first. Default 100.
* ``payload`` carries handler-specific kwargs as JSONB.
* ``result_summary`` (on goal_results) stores a compact handler return
  dict; the full domain side-effect lives in the handler's own table.

Locked decisions referenced:
  docs/planning/IDLE-FLEET-AGENT-LOOP.md, section "Decisions locked
  2026-05-16".
"""

from typing import Sequence, Union

from alembic import op


# revision identifiers, used by Alembic.
revision: str = "idle_fleet_goal_queue_day1"
down_revision: Union[str, Sequence[str], None] = "tps_phase0_snapshots"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute(
        """
        CREATE TABLE IF NOT EXISTS goal_queue (
            id                  BIGSERIAL PRIMARY KEY,
            goal_type           TEXT NOT NULL,
            target_id           TEXT NOT NULL,
            payload             JSONB NOT NULL DEFAULT '{}'::jsonb,
            priority            INTEGER NOT NULL DEFAULT 100,
            hardware_tier       TEXT NOT NULL DEFAULT 'cpu',
            state               TEXT NOT NULL DEFAULT 'claimable',
            allow_cloud         BOOLEAN NOT NULL DEFAULT FALSE,
            max_duty_cycle      NUMERIC,
            claimed_by          TEXT,
            claimed_at          TIMESTAMPTZ,
            lease_expires_at    TIMESTAMPTZ,
            attempts            INTEGER NOT NULL DEFAULT 0,
            max_attempts        INTEGER NOT NULL DEFAULT 3,
            last_error          TEXT,
            dedupe_window       TEXT NOT NULL DEFAULT 'global',
            depth               INTEGER NOT NULL DEFAULT 0,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT goal_queue_state_chk CHECK (
                state IN ('claimable','claimed','done','failed','quarantined')
            ),
            CONSTRAINT goal_queue_tier_chk CHECK (
                hardware_tier IN ('cpu','medium_gpu','large_gpu','vision')
            ),
            CONSTRAINT goal_queue_depth_chk CHECK (depth >= 0 AND depth <= 3)
        );
        """
    )
    # Dedupe: only one open goal per (type, target, window).
    # Partial unique index so a new goal can be enqueued after the
    # previous one terminates (done / failed / quarantined).
    op.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS ux_goal_queue_open_dedupe
            ON goal_queue (goal_type, target_id, dedupe_window)
            WHERE state IN ('claimable','claimed');
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_goal_queue_claim_pick
            ON goal_queue (state, hardware_tier, priority DESC, created_at ASC)
            WHERE state = 'claimable';
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_goal_queue_lease
            ON goal_queue (lease_expires_at)
            WHERE state = 'claimed';
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_goal_queue_node
            ON goal_queue (claimed_by, state);
        """
    )

    op.execute(
        """
        CREATE TABLE IF NOT EXISTS goal_results (
            id                  BIGSERIAL PRIMARY KEY,
            goal_id             BIGINT NOT NULL,
            goal_type           TEXT NOT NULL,
            target_id           TEXT NOT NULL,
            node_id             TEXT NOT NULL,
            state               TEXT NOT NULL,
            result_summary      JSONB NOT NULL DEFAULT '{}'::jsonb,
            error_message       TEXT,
            duration_ms         INTEGER,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT goal_results_state_chk CHECK (
                state IN ('done','failed','quarantined')
            )
        );
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_goal_results_goal_id
            ON goal_results (goal_id);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_goal_results_node_recent
            ON goal_results (node_id, created_at DESC);
        """
    )
    op.execute(
        """
        CREATE INDEX IF NOT EXISTS ix_goal_results_type_recent
            ON goal_results (goal_type, created_at DESC);
        """
    )


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS ix_goal_results_type_recent;")
    op.execute("DROP INDEX IF EXISTS ix_goal_results_node_recent;")
    op.execute("DROP INDEX IF EXISTS ix_goal_results_goal_id;")
    op.execute("DROP TABLE IF EXISTS goal_results;")
    op.execute("DROP INDEX IF EXISTS ix_goal_queue_node;")
    op.execute("DROP INDEX IF EXISTS ix_goal_queue_lease;")
    op.execute("DROP INDEX IF EXISTS ix_goal_queue_claim_pick;")
    op.execute("DROP INDEX IF EXISTS ux_goal_queue_open_dedupe;")
    op.execute("DROP TABLE IF EXISTS goal_queue;")
