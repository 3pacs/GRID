"""Phase 4.1.1: Add news_articles to intelligence_search

Revision ID: phase4_fts_002
Revises: phase4_fts_001
Create Date: 2026-05-08 00:00:00.000000

Adds a fifth UNION ALL branch ('news') to the intelligence_search materialized
view so that news_articles (highest-velocity feed) are full-text searchable
alongside actors, signals, hypotheses, and snapshots.

Bug: the original phase4_fts_001 view excluded news_articles entirely. The
search index showed 2M rows but zero news, even though the news puller is
landing ~50/day.
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'phase4_fts_002'
down_revision: Union[str, Sequence[str], None] = 'phase4_fts_001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


_REBUILD_SQL = """
DROP MATERIALIZED VIEW IF EXISTS intelligence_search CASCADE;

CREATE MATERIALIZED VIEW intelligence_search AS
SELECT 'actor'::text AS source_type,
       a.id AS source_id,
       a.name AS title,
       (COALESCE(a.category, ''::text) || ' '::text) || COALESCE(a.name, ''::text) AS body,
       to_tsvector('english'::regconfig, (COALESCE(a.name, ''::text) || ' '::text) || COALESCE(a.category, ''::text)) AS tsv
FROM actors a
WHERE a.name IS NOT NULL
UNION ALL
SELECT 'signal'::text AS source_type,
       s.id::text AS source_id,
       (COALESCE(s.signal_type, ''::text) || ': '::text) || COALESCE(s.ticker, ''::text) AS title,
       COALESCE(s.description, ''::text) AS body,
       to_tsvector('english'::regconfig, (((COALESCE(s.description, ''::text) || ' '::text) || COALESCE(s.ticker, ''::text)) || ' '::text) || COALESCE(s.actor, ''::text)) AS tsv
FROM signal_data s
WHERE s.description IS NOT NULL
UNION ALL
SELECT 'hypothesis'::text AS source_type,
       h.id AS source_id,
       COALESCE(h.thesis, ''::text) AS title,
       (COALESCE(h.thesis, ''::text) || ' '::text) || COALESCE(h.pattern_type, ''::text) AS body,
       to_tsvector('english'::regconfig, COALESCE(h.thesis, ''::text)) AS tsv
FROM discovered_hypotheses h
WHERE h.thesis IS NOT NULL
UNION ALL
SELECT 'snapshot'::text AS source_type,
       sn.id::text AS source_id,
       (COALESCE(sn.category, ''::text) || ' '::text) || COALESCE(sn.subcategory, ''::text) AS title,
       (COALESCE(sn.category, ''::text) || ' '::text) || COALESCE(sn.subcategory, ''::text) AS body,
       to_tsvector('english'::regconfig, (COALESCE(sn.category, ''::text) || ' '::text) || COALESCE(sn.subcategory, ''::text)) AS tsv
FROM analytical_snapshots sn
WHERE sn.category IS NOT NULL
UNION ALL
SELECT 'news'::text AS source_type,
       n.id::text AS source_id,
       COALESCE(n.title, ''::text) AS title,
       ((COALESCE(NULLIF(n.llm_summary, ''::text), n.summary, ''::text) || ' '::text) || COALESCE(array_to_string(n.tickers, ' '), ''::text)) AS body,
       to_tsvector('english'::regconfig,
           ((((COALESCE(n.title, ''::text) || ' '::text)
              || COALESCE(NULLIF(n.llm_summary, ''::text), n.summary, ''::text)) || ' '::text)
              || COALESCE(array_to_string(n.tickers, ' '), ''::text)) || ' '::text
              || COALESCE(n.source, ''::text)
       ) AS tsv
FROM news_articles n
WHERE n.title IS NOT NULL;

CREATE UNIQUE INDEX idx_intelligence_search_pk ON intelligence_search(source_type, source_id);
CREATE INDEX idx_intelligence_search_tsv ON intelligence_search USING GIN(tsv);
"""


_DOWNGRADE_SQL = """
DROP MATERIALIZED VIEW IF EXISTS intelligence_search CASCADE;

CREATE MATERIALIZED VIEW intelligence_search AS
SELECT 'actor' AS source_type, id::text AS source_id, name AS title,
       COALESCE(category, '') || ' ' || COALESCE(title, '') || ' ' || name AS body,
       to_tsvector('english', COALESCE(name, '') || ' ' || COALESCE(category, '') || ' ' || COALESCE(title, '')) AS tsv
FROM actors WHERE name IS NOT NULL
UNION ALL
SELECT 'signal' AS source_type, id::text,
       COALESCE(signal_type, '') || ': ' || COALESCE(ticker, '') AS title,
       COALESCE(description, '') AS body,
       to_tsvector('english', COALESCE(description, '') || ' ' || COALESCE(ticker, '') || ' ' || COALESCE(actor, '')) AS tsv
FROM signal_data WHERE description IS NOT NULL
UNION ALL
SELECT 'hypothesis' AS source_type, id::text,
       COALESCE(thesis, '') AS title,
       COALESCE(thesis, '') || ' ' || COALESCE(pattern_type, '') AS body,
       to_tsvector('english', COALESCE(thesis, '')) AS tsv
FROM discovered_hypotheses WHERE thesis IS NOT NULL
UNION ALL
SELECT 'snapshot' AS source_type, id::text,
       COALESCE(category, '') || ' ' || COALESCE(subcategory, '') AS title,
       COALESCE(category, '') || ' ' || COALESCE(subcategory, '') AS body,
       to_tsvector('english', COALESCE(category, '') || ' ' || COALESCE(subcategory, '')) AS tsv
FROM analytical_snapshots WHERE category IS NOT NULL;

CREATE UNIQUE INDEX idx_intelligence_search_pk ON intelligence_search(source_type, source_id);
CREATE INDEX idx_intelligence_search_tsv ON intelligence_search USING GIN(tsv);
"""


def upgrade() -> None:
    op.execute(sa.text(_REBUILD_SQL))


def downgrade() -> None:
    op.execute(sa.text(_DOWNGRADE_SQL))
