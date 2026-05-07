"""CAT-71 — WARN Act layoffs puller tests."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock


from ingestion.altdata.warn_layoffs import (
    CLOSURE_TYPES,
    SUPPORTED_STATES,
    WARNFiling,
    WARNLayoffsPuller,
)


def _build_puller():
    p = WARNLayoffsPuller.__new__(WARNLayoffsPuller)
    p.engine = MagicMock()
    p.source_id = 42
    return p


class TestWARNFiling:
    def test_valid_filing(self):
        f = WARNFiling(
            state="CA", company_name="Acme Corp",
            notice_date=date(2026, 4, 1), layoff_count=100,
        )
        assert f.is_valid() is True

    def test_missing_state_invalid(self):
        f = WARNFiling(
            state="", company_name="Acme",
            notice_date=date(2026, 4, 1), layoff_count=100,
        )
        assert f.is_valid() is False

    def test_missing_company_invalid(self):
        f = WARNFiling(
            state="CA", company_name="",
            notice_date=date(2026, 4, 1), layoff_count=100,
        )
        assert f.is_valid() is False


class TestEnsureSchema:
    def test_runs_create_statements(self):
        puller = _build_puller()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.begin.return_value = conn

        statements = []

        def capture(query, params=None):
            statements.append(str(query))
            return MagicMock()

        conn.execute = capture
        puller.ensure_schema()
        # Should run CREATE TABLE + 2 CREATE INDEX
        assert any("CREATE TABLE" in s for s in statements)
        assert any("idx_warn_filings_notice_date" in s for s in statements)
        assert any("idx_warn_filings_ticker" in s for s in statements)


class TestUpsertFilings:
    def _prep(self):
        puller = _build_puller()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.begin.return_value = conn
        return puller, conn

    def test_empty_list(self):
        puller, _ = self._prep()
        counts = puller.upsert_filings([])
        assert counts["inserted"] == 0
        assert counts["skipped_invalid"] == 0

    def test_skips_invalid_shape(self):
        puller, conn = self._prep()
        conn.execute = MagicMock(return_value=MagicMock(rowcount=1))
        filings = [
            WARNFiling(state="", company_name="", notice_date=date(2026, 4, 1), layoff_count=100),
            WARNFiling(state="CA", company_name="Acme", notice_date=date(2026, 4, 1), layoff_count=50),
        ]
        counts = puller.upsert_filings(filings)
        assert counts["skipped_invalid"] == 1
        assert counts["inserted"] == 1

    def test_dupe_via_on_conflict(self):
        puller, conn = self._prep()
        # rowcount=0 from the INSERT means the ON CONFLICT fired
        conn.execute = MagicMock(return_value=MagicMock(rowcount=0))
        filings = [
            WARNFiling(state="CA", company_name="Acme", notice_date=date(2026, 4, 1), layoff_count=50),
        ]
        counts = puller.upsert_filings(filings)
        assert counts["skipped_dupe"] == 1

    def test_closure_type_normalized(self):
        puller, conn = self._prep()
        captured = []

        def capture(query, params=None):
            if "INSERT INTO warn_filings" in str(query):
                captured.append(params)
            return MagicMock(rowcount=1)

        conn.execute = capture
        filings = [
            WARNFiling(
                state="CA", company_name="Acme",
                notice_date=date(2026, 4, 1), layoff_count=100,
                closure_type="something_weird",
            ),
        ]
        puller.upsert_filings(filings)
        assert captured[0]["closure"] == "other"

    def test_valid_closure_preserved(self):
        puller, conn = self._prep()
        captured = []

        def capture(query, params=None):
            if "INSERT INTO warn_filings" in str(query):
                captured.append(params)
            return MagicMock(rowcount=1)

        conn.execute = capture
        filings = [
            WARNFiling(
                state="CA", company_name="Acme",
                notice_date=date(2026, 4, 1), layoff_count=100,
                closure_type="mass_layoff",
            ),
        ]
        puller.upsert_filings(filings)
        assert captured[0]["closure"] == "mass_layoff"


class TestQueryHelpers:
    def test_recent_filings_by_ticker(self):
        puller = _build_puller()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.connect.return_value = conn
        result = MagicMock()
        result.fetchall.return_value = [
            ("CA", "Acme Corp", date(2026, 3, 1), date(2026, 5, 1), 150, "mass_layoff"),
        ]
        conn.execute.return_value = result
        rows = puller.recent_filings_by_ticker("ACME")
        assert len(rows) == 1
        assert rows[0]["company_name"] == "Acme Corp"
        assert rows[0]["layoff_count"] == 150

    def test_recent_filings_db_error(self):
        puller = _build_puller()
        puller.engine.connect.side_effect = RuntimeError("down")
        assert puller.recent_filings_by_ticker("ACME") == []

    def test_national_totals(self):
        puller = _build_puller()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.connect.return_value = conn
        result = MagicMock()
        result.fetchone.return_value = (42, 12500, 8)
        conn.execute.return_value = result
        totals = puller.national_totals()
        assert totals["filings"] == 42
        assert totals["total_layoffs"] == 12500
        assert totals["states_reporting"] == 8

    def test_national_totals_db_error(self):
        puller = _build_puller()
        puller.engine.connect.side_effect = RuntimeError("down")
        totals = puller.national_totals()
        assert totals["filings"] == 0


class TestConstants:
    def test_supported_states_has_ca(self):
        assert "CA" in SUPPORTED_STATES

    def test_closure_types_has_mass_layoff(self):
        assert "mass_layoff" in CLOSURE_TYPES
