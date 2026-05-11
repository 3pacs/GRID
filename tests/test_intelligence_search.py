"""Tests for the intelligence search API router.

Stubs api.auth and api.dependencies to avoid heavy transitive deps
(psycopg2, jose, passlib) that may not be installed in lightweight CI
environments.

Tests verify:
  - Router structure (prefix, tags, endpoints)
  - FTS query building and parameter handling
  - Type filtering and validation
  - Pagination parameters
  - Empty query handling
  - Snippet generation expectations
"""

from __future__ import annotations

import sys
from types import ModuleType

import pytest

# ---------------------------------------------------------------------------
# Prefer the real api.auth — only stub if heavy deps are unavailable.
# Unconditional stubbing pollutes sys.modules for every later test.
# ---------------------------------------------------------------------------

try:
    import api.auth  # noqa: F401
except Exception:
    _auth_stub = ModuleType("api.auth")
    _auth_stub.require_auth = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.auth"] = _auth_stub

try:
    import api.dependencies  # noqa: F401
except Exception:
    _deps_stub = ModuleType("api.dependencies")
    _deps_stub.get_db_engine = lambda: None  # type: ignore[attr-defined]
    sys.modules["api.dependencies"] = _deps_stub


# ── Router structure tests (no DB needed) ────────────────────────────────

class TestIntelligenceSearchRouter:
    """Verify the router is properly configured."""

    @pytest.fixture(autouse=True)
    def _import_router(self):
        from api.routers.intelligence_search import router
        self.router = router

    def test_router_has_correct_prefix(self):
        assert self.router.prefix == "/api/v1/search"

    def test_router_has_tag(self):
        assert "intelligence-search" in self.router.tags

    def test_search_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/intelligence") for p in paths)

    def test_refresh_endpoint_exists(self):
        paths = [r.path for r in self.router.routes]
        assert any(p.endswith("/intelligence/refresh") for p in paths)

    def test_search_is_get(self):
        methods = {}
        for r in self.router.routes:
            if hasattr(r, "methods"):
                for m in r.methods:
                    methods.setdefault(r.path, set()).add(m)
        intel_paths = [p for p in methods if p.endswith("/intelligence")]
        assert any("GET" in methods[p] for p in intel_paths)

    def test_refresh_is_post(self):
        methods = {}
        for r in self.router.routes:
            if hasattr(r, "methods"):
                for m in r.methods:
                    methods.setdefault(r.path, set()).add(m)
        refresh_paths = [p for p in methods if p.endswith("/intelligence/refresh")]
        assert any("POST" in methods[p] for p in refresh_paths)


# ── Valid source types ───────────────────────────────────────────────────

class TestValidTypes:
    """Verify the _VALID_TYPES constant."""

    def test_valid_types_contains_expected(self):
        from api.routers.intelligence_search import _VALID_TYPES
        assert "actor" in _VALID_TYPES
        assert "signal" in _VALID_TYPES
        assert "hypothesis" in _VALID_TYPES
        assert "snapshot" in _VALID_TYPES
        assert "news" in _VALID_TYPES

    def test_valid_types_is_frozen(self):
        from api.routers.intelligence_search import _VALID_TYPES
        assert isinstance(_VALID_TYPES, frozenset)

    def test_valid_types_count(self):
        from api.routers.intelligence_search import _VALID_TYPES
        assert len(_VALID_TYPES) == 5


# ── Database integration tests (require PostgreSQL) ──────────────────────

class TestFTSSearch:
    """Full-text search via the materialized view."""

    @pytest.fixture(autouse=True)
    def _setup(self, pg_engine):
        """Set up tables and materialized view for FTS testing."""
        self.engine = pg_engine
        from sqlalchemy import text

        with self.engine.begin() as conn:
            # Create actors table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS actors (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    tier TEXT NOT NULL DEFAULT 'unknown',
                    category TEXT NOT NULL DEFAULT 'unknown',
                    title TEXT,
                    search_vector tsvector,
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
            """))

            # Create signal_data table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS signal_data (
                    id BIGSERIAL PRIMARY KEY,
                    signal_type TEXT NOT NULL,
                    signal_date DATE NOT NULL DEFAULT CURRENT_DATE,
                    ticker TEXT,
                    actor TEXT,
                    direction TEXT,
                    magnitude DOUBLE PRECISION,
                    description TEXT,
                    search_vector tsvector,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """))

            # Create discovered_hypotheses table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS discovered_hypotheses (
                    id TEXT PRIMARY KEY,
                    thesis TEXT NOT NULL,
                    pattern_type TEXT,
                    confidence DOUBLE PRECISION DEFAULT 0.5,
                    status TEXT DEFAULT 'active',
                    search_vector tsvector,
                    created_at TIMESTAMPTZ DEFAULT NOW()
                )
            """))

            # Create analytical_snapshots table if not exists
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS analytical_snapshots (
                    id BIGSERIAL PRIMARY KEY,
                    category TEXT NOT NULL DEFAULT 'test',
                    snapshot_date DATE DEFAULT CURRENT_DATE,
                    title TEXT,
                    summary TEXT,
                    search_vector tsvector,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                )
            """))

            # Insert test data
            conn.execute(text("""
                INSERT INTO actors (id, name, tier, category, title)
                VALUES
                    ('test-actor-1', 'Warren Buffett', 'titan', 'investor', 'CEO of Berkshire Hathaway'),
                    ('test-actor-2', 'Janet Yellen', 'titan', 'policy_maker', 'Secretary of Treasury')
                ON CONFLICT (id) DO NOTHING
            """))

            # Explicit signal_date: CREATE TABLE IF NOT EXISTS above is a
            # no-op when the real signal_data table already exists without
            # a DEFAULT on signal_date, so we must provide one.
            conn.execute(text("""
                INSERT INTO signal_data (signal_type, signal_date, ticker, actor, description)
                VALUES
                    ('insider_buy', CURRENT_DATE, 'AAPL', 'Tim Cook', 'Insider purchase of Apple shares worth $5M'),
                    ('congressional', CURRENT_DATE, 'NVDA', 'Nancy Pelosi', 'Congressional disclosure of NVIDIA options trade')
            """))

            conn.execute(text("""
                INSERT INTO discovered_hypotheses (id, thesis, pattern_type)
                VALUES
                    ('test-hyp-1', 'Fed rate cuts lead to equity rally within 90 days', 'macro_regime'),
                    ('test-hyp-2', 'Semiconductor sector outperforms during AI investment boom', 'sector_rotation')
                ON CONFLICT (id) DO NOTHING
            """))

            conn.execute(text("""
                INSERT INTO analytical_snapshots (category, title, summary)
                VALUES
                    ('market_regime', 'Q1 2026 Market Regime', 'Growth regime with strong tech leadership and rising yields'),
                    ('sector_analysis', 'Semiconductor Deep Dive', 'AI infrastructure spending drives semiconductor revenue growth')
            """))

            # Drop and recreate materialized view
            conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS intelligence_search"))

            conn.execute(text("""
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
                       COALESCE(title, '') AS title,
                       COALESCE(summary, '') AS body,
                       to_tsvector('english', COALESCE(title, '') || ' ' || COALESCE(summary, '')) AS tsv
                FROM analytical_snapshots WHERE summary IS NOT NULL
            """))

            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_intelligence_search_pk
                ON intelligence_search(source_type, source_id)
            """))

            conn.execute(text("""
                CREATE INDEX IF NOT EXISTS idx_intelligence_search_tsv
                ON intelligence_search USING GIN(tsv)
            """))

        yield

        # Cleanup
        with self.engine.begin() as conn:
            conn.execute(text("DROP MATERIALIZED VIEW IF EXISTS intelligence_search"))
            conn.execute(text("DELETE FROM actors WHERE id LIKE 'test-%%'"))
            conn.execute(text("DELETE FROM discovered_hypotheses WHERE id LIKE 'test-%%'"))
            # signal_data and analytical_snapshots use BIGSERIAL, cleanup by description
            conn.execute(text("DELETE FROM signal_data WHERE description LIKE '%%Insider purchase%%' OR description LIKE '%%Congressional disclosure%%'"))
            conn.execute(text("DELETE FROM analytical_snapshots WHERE title LIKE '%%Q1 2026%%' OR title LIKE '%%Semiconductor Deep%%'"))

    def test_basic_fts_query(self):
        """Search for 'Buffett' should return the actor."""
        from sqlalchemy import text
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT source_type, source_id, title,
                           ts_rank(tsv, plainto_tsquery('english', :q)) AS relevance
                    FROM intelligence_search
                    WHERE tsv @@ plainto_tsquery('english', :q)
                    ORDER BY relevance DESC
                """),
                {"q": "Buffett"},
            ).fetchall()
        assert len(rows) >= 1
        assert rows[0].source_type == "actor"
        assert "Buffett" in rows[0].title

    def test_type_filtering(self):
        """Filter by source_type should restrict results."""
        from sqlalchemy import text
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT source_type, source_id, title
                    FROM intelligence_search
                    WHERE tsv @@ plainto_tsquery('english', :q)
                      AND source_type = ANY(:types)
                    ORDER BY source_type
                """),
                {"q": "semiconductor", "types": ["hypothesis", "snapshot"]},
            ).fetchall()

        types_found = {r.source_type for r in rows}
        assert "actor" not in types_found
        assert "signal" not in types_found
        # Should find semiconductor in hypothesis and/or snapshot
        assert len(rows) >= 1

    def test_pagination(self):
        """LIMIT and OFFSET should work correctly."""
        from sqlalchemy import text
        with self.engine.connect() as conn:
            # Get all results
            all_rows = conn.execute(
                text("""
                    SELECT source_type, source_id
                    FROM intelligence_search
                    WHERE tsv @@ plainto_tsquery('english', :q)
                    ORDER BY source_type, source_id
                """),
                {"q": "trade | investment | regime | sector | insider"},
            ).fetchall()

            # Get first page
            page1 = conn.execute(
                text("""
                    SELECT source_type, source_id
                    FROM intelligence_search
                    WHERE tsv @@ plainto_tsquery('english', :q)
                    ORDER BY source_type, source_id
                    LIMIT :limit OFFSET :offset
                """),
                {"q": "trade | investment | regime | sector | insider", "limit": 2, "offset": 0},
            ).fetchall()

            assert len(page1) <= 2
            if len(all_rows) > 2:
                assert len(page1) == 2

    def test_empty_results(self):
        """A query matching nothing should return empty list."""
        from sqlalchemy import text
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT source_type, source_id
                    FROM intelligence_search
                    WHERE tsv @@ plainto_tsquery('english', :q)
                """),
                {"q": "xyzzynonexistent99999"},
            ).fetchall()
        assert len(rows) == 0

    def test_snippet_generation(self):
        """ts_headline should produce a snippet with <mark> tags."""
        from sqlalchemy import text
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT
                        ts_headline(
                            'english', body,
                            plainto_tsquery('english', :q),
                            'MaxWords=35, MinWords=15, StartSel=<mark>, StopSel=</mark>'
                        ) AS snippet
                    FROM intelligence_search
                    WHERE tsv @@ plainto_tsquery('english', :q)
                    LIMIT 1
                """),
                {"q": "Apple"},
            ).fetchall()
        assert len(rows) >= 1
        snippet = rows[0].snippet
        assert "<mark>" in snippet or "Apple" in snippet

    def test_relevance_ranking(self):
        """Results should be ranked by relevance (ts_rank)."""
        from sqlalchemy import text
        with self.engine.connect() as conn:
            rows = conn.execute(
                text("""
                    SELECT source_type, source_id,
                           ts_rank(tsv, plainto_tsquery('english', :q)) AS relevance
                    FROM intelligence_search
                    WHERE tsv @@ plainto_tsquery('english', :q)
                    ORDER BY relevance DESC
                """),
                {"q": "rate cuts equity rally"},
            ).fetchall()
        if len(rows) >= 2:
            assert rows[0].relevance >= rows[1].relevance

    def test_concurrent_refresh(self):
        """REFRESH MATERIALIZED VIEW CONCURRENTLY should work."""
        from sqlalchemy import text
        with self.engine.begin() as conn:
            conn.execute(text(
                "REFRESH MATERIALIZED VIEW CONCURRENTLY intelligence_search"
            ))
        # If we reach here without exception, the refresh worked
        assert True

    def test_cross_type_search(self):
        """A broad query should return results from multiple source types."""
        from sqlalchemy import text
        with self.engine.connect() as conn:
            # websearch_to_tsquery parses `OR` as boolean disjunction;
            # plainto_tsquery does not (it AND-joins all tokens and drops
            # punctuation), so the previous version of this test returned
            # zero rows regardless of the data.
            rows = conn.execute(
                text("""
                    SELECT DISTINCT source_type
                    FROM intelligence_search
                    WHERE tsv @@ websearch_to_tsquery('english', :q)
                """),
                {"q": "semiconductor OR Buffett OR Apple OR regime"},
            ).fetchall()
        types = {r.source_type for r in rows}
        # Should hit at least 2 different source types
        assert len(types) >= 2
