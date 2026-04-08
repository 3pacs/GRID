"""actor_analytics table for graph centrality metrics

Revision ID: phase4_actor_analytics
Revises: a1b2c3d4e5f6
Create Date: 2026-04-08 18:00:00.000000

Creates 1 table for precomputed graph analytics:
  - actor_analytics — PageRank, community, centrality scores per actor

All CREATE TABLE statements use IF NOT EXISTS for idempotency.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "phase4_actor_analytics"
down_revision: Union[str, Sequence[str], None] = "a1b2c3d4e5f6"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── actor_analytics ──────────────────────────────────────────────
    op.execute(sa.text("""
        CREATE TABLE IF NOT EXISTS actor_analytics (
            actor_id            TEXT PRIMARY KEY REFERENCES actors(id) ON DELETE CASCADE,
            pagerank            DOUBLE PRECISION DEFAULT 0,
            community_id        INTEGER,
            betweenness         DOUBLE PRECISION DEFAULT 0,
            eigenvector         DOUBLE PRECISION DEFAULT 0,
            degree_centrality   DOUBLE PRECISION DEFAULT 0,
            hub_score           DOUBLE PRECISION DEFAULT 0,
            authority_score     DOUBLE PRECISION DEFAULT 0,
            computed_at         TIMESTAMPTZ DEFAULT NOW()
        )
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_actor_analytics_pagerank
        ON actor_analytics (pagerank DESC)
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_actor_analytics_community
        ON actor_analytics (community_id)
    """))

    op.execute(sa.text("""
        CREATE INDEX IF NOT EXISTS idx_actor_analytics_betweenness
        ON actor_analytics (betweenness DESC)
    """))


def downgrade() -> None:
    op.execute(sa.text("DROP TABLE IF EXISTS actor_analytics"))
