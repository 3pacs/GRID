"""canvas tables

Revision ID: a1b2c3d4e5f6
Revises: f1a2b3c4d5e6
Create Date: 2026-04-07 12:00:00.000000

Creates 3 tables for the canvas investigation board feature:
  - canvas_boards   — user-created investigation boards
  - canvas_nodes    — nodes on a board (actors, companies, hypotheses, signals, notes)
  - canvas_edges    — edges connecting nodes

All CREATE TABLE statements use IF NOT EXISTS for idempotency.
Tables already exist on the production server — this migration exists for
completeness and new-environment setup.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a1b2c3d4e5f6'
down_revision: Union[str, Sequence[str], None] = 'f1a2b3c4d5e6'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── canvas_boards ────────────────────────────────────────────────
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS canvas_boards (
            id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            name        TEXT NOT NULL,
            description TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """))

    # ── canvas_nodes ─────────────────────────────────────────────────
    # NOTE: the primary key column is `node_id`, matching the column
    # name used by every canvas router. Older copies of this migration
    # used `id`; the follow-up migration
    # scripts/migrations/20260411_rename_canvas_nodes_id.sql renames
    # `id` → `node_id` on DBs that were initialised against the old schema.
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS canvas_nodes (
            node_id     TEXT PRIMARY KEY,
            board_id    UUID NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
            node_type   TEXT NOT NULL DEFAULT 'note',
            label       TEXT,
            position_x  DOUBLE PRECISION NOT NULL DEFAULT 0,
            position_y  DOUBLE PRECISION NOT NULL DEFAULT 0,
            data        JSONB,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_canvas_nodes_board
        ON canvas_nodes (board_id)
    """))

    # ── canvas_edges ─────────────────────────────────────────────────
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS canvas_edges (
            id              TEXT PRIMARY KEY,
            board_id        UUID NOT NULL REFERENCES canvas_boards(id) ON DELETE CASCADE,
            source_node_id  TEXT NOT NULL REFERENCES canvas_nodes(node_id) ON DELETE CASCADE,
            target_node_id  TEXT NOT NULL REFERENCES canvas_nodes(node_id) ON DELETE CASCADE,
            edge_type       TEXT DEFAULT 'default',
            label           TEXT,
            data            JSONB,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
        )
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_canvas_edges_board
        ON canvas_edges (board_id)
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_canvas_edges_source
        ON canvas_edges (source_node_id)
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_canvas_edges_target
        ON canvas_edges (target_node_id)
    """))


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS canvas_edges"))
    op.execute(sa.text("DROP TABLE IF EXISTS canvas_nodes"))
    op.execute(sa.text("DROP TABLE IF EXISTS canvas_boards"))
