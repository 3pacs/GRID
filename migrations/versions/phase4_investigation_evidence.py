"""investigation evidence table

Revision ID: phase4_investigation_evidence
Revises: a1b2c3d4e5f6
Create Date: 2026-04-08 18:00:00.000000

Creates the investigation_evidence table for pinning charts, filing
excerpts, quotes, and signals as evidence on canvas investigation boards.

Each evidence row links to a board and optionally to a specific node,
carrying a confidence label (confirmed/derived/estimated/rumored/inferred)
consistent with GRID data-confidence standards.

Uses IF NOT EXISTS for idempotency.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'phase4_investigation_evidence'
down_revision: Union[str, Sequence[str], None] = 'a1b2c3d4e5f6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── investigation_evidence ───────────────────────────────────────
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS investigation_evidence (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            board_id        UUID REFERENCES canvas_boards(id) ON DELETE CASCADE,
            node_id         TEXT,
            evidence_type   TEXT NOT NULL,
            content         TEXT,
            source_url      TEXT,
            source_table    TEXT,
            source_id       TEXT,
            confidence      TEXT DEFAULT 'derived',
            captured_at     TIMESTAMPTZ DEFAULT NOW(),
            metadata        JSONB DEFAULT '{}'::jsonb
        )
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_investigation_evidence_board
        ON investigation_evidence (board_id)
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_investigation_evidence_node
        ON investigation_evidence (node_id)
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_investigation_evidence_type
        ON investigation_evidence (evidence_type)
    """))


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS investigation_evidence"))
