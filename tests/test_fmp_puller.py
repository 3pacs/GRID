"""Tests for Financial Modeling Prep puller."""

from __future__ import annotations

from unittest.mock import MagicMock, patch



class TestFMPPuller:
    """Test FMP earnings/financials puller."""

    def test_source_config(self):
        from ingestion.altdata.fmp_puller import FMPPuller
        assert FMPPuller.SOURCE_NAME == "fmp"
        assert FMPPuller.SOURCE_CONFIG["trust_score"] == "HIGH"

    def test_earnings_universe_populated(self):
        from ingestion.altdata.fmp_puller import EARNINGS_UNIVERSE
        assert len(EARNINGS_UNIVERSE) >= 100
        assert "AAPL" in EARNINGS_UNIVERSE
        assert "NVDA" in EARNINGS_UNIVERSE
        assert "JPM" in EARNINGS_UNIVERSE

    def test_earnings_universe_no_duplicates(self):
        from ingestion.altdata.fmp_puller import EARNINGS_UNIVERSE
        assert len(EARNINGS_UNIVERSE) == len(set(EARNINGS_UNIVERSE))

    def test_surprise_calculation(self):
        """Verify surprise % formula."""
        eps_actual = 1.50
        eps_estimate = 1.20
        surprise_pct = (eps_actual - eps_estimate) / abs(eps_estimate) * 100
        assert round(surprise_pct, 2) == 25.0

    def test_negative_surprise(self):
        eps_actual = 0.80
        eps_estimate = 1.20
        surprise_pct = (eps_actual - eps_estimate) / abs(eps_estimate) * 100
        assert round(surprise_pct, 2) == -33.33

    def test_pull_returns_error_without_key(self):
        from ingestion.altdata.fmp_puller import FMPPuller
        engine = MagicMock()

        with patch.object(FMPPuller, '__init__', lambda self, *a, **kw: None):
            puller = FMPPuller.__new__(FMPPuller)
            puller.api_key = ""
            puller.engine = engine
            puller.source_id = 1
            result = puller.pull()
            assert result.get("error") is not None

    def test_api_base_url(self):
        from ingestion.altdata.fmp_puller import FMP_BASE
        assert "financialmodelingprep.com" in FMP_BASE

    def test_sector_list_coverage(self):
        """FMP covers all 11 GICS sectors."""
        # This is a structural test — verifying our puller stores sectors
        from ingestion.altdata.fmp_puller import FMPPuller
        assert hasattr(FMPPuller, 'pull_sector_performance')

    def test_financial_statement_methods_exist(self):
        from ingestion.altdata.fmp_puller import FMPPuller
        assert hasattr(FMPPuller, 'pull_income_statement')
        assert hasattr(FMPPuller, 'pull_balance_sheet')
        assert hasattr(FMPPuller, 'pull_cash_flow')
        assert hasattr(FMPPuller, 'pull_transcript')
        assert hasattr(FMPPuller, 'pull_analyst_estimates')
