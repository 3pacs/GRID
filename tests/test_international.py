"""
Tests for GRID international ingestion modules.

Validates series lists, country configurations, and puller initialization
without requiring live API access or database connectivity.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class TestECBPuller:
    """Tests for the ECB SDW puller."""

    def test_ecb_puller_init(self):
        """ECBPuller initializes without error when source exists."""
        import sys
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.fetchone.return_value = (1,)

        from ingestion.international.ecb import ECBPuller
        puller = ECBPuller(db_engine=mock_engine)
        assert puller.source_id == 1

    def test_ecb_series_list_not_empty(self):
        """ECB_SERIES_LIST has at least 4 entries."""
        from ingestion.international.ecb import ECB_SERIES_LIST
        assert len(ECB_SERIES_LIST) >= 4

    def test_ecb_series_list_has_bund(self):
        """ECB series includes German Bund yield."""
        from ingestion.international.ecb import ECB_SERIES_LIST
        bund_series = [v for v in ECB_SERIES_LIST.values() if "bund" in v.lower()]
        assert len(bund_series) >= 1


class TestOECDPuller:
    """Tests for the OECD SDMX puller."""

    def test_oecd_puller_cli_countries(self):
        """OECDPuller has USA and G-7 in CLI countries list."""
        from ingestion.international.oecd import OECD_CLI_COUNTRIES
        assert "USA" in OECD_CLI_COUNTRIES
        assert "G-7" in OECD_CLI_COUNTRIES

    def test_oecd_mei_series_not_empty(self):
        """OECD_MEI_SERIES has at least 4 entries."""
        from ingestion.international.oecd import OECD_MEI_SERIES
        assert len(OECD_MEI_SERIES) >= 4


class TestBISPuller:
    """Tests for the BIS Statistics puller."""

    def test_bis_series_list(self):
        """BIS_SERIES has credit gap series for US and CN."""
        from ingestion.international.bis import BIS_SERIES_LIST
        features = list(BIS_SERIES_LIST.values())
        assert "bis_credit_gdp_gap_us" in features
        assert "bis_credit_gdp_gap_cn" in features


class TestAKSharePuller:
    """Tests for the AKShare China macro puller."""

    def test_akshare_series_list(self):
        """AKSHARE_SERIES has at least 8 entries."""
        from ingestion.international.akshare_macro import AKSHARE_SERIES
        assert len(AKSHARE_SERIES) >= 8

    def test_akshare_has_m2(self):
        """AKShare series includes China M2."""
        from ingestion.international.akshare_macro import AKSHARE_SERIES
        assert "macro_china_money_supply" in AKSHARE_SERIES


class TestBCBPuller:
    """Tests for the BCB Brazil puller."""

    def test_bcb_series_list(self):
        """BCB_SERIES has SELIC (code 11) defined."""
        from ingestion.international.bcb import BCB_SERIES
        assert 11 in BCB_SERIES
        assert BCB_SERIES[11] == "brazil_selic_rate"


class TestKOSISPuller:
    """Tests for the KOSIS Korea puller."""

    def test_kosis_series_list(self):
        """KOSIS exports series defined."""
        from ingestion.international.kosis import KOSIS_SERIES
        features = list(KOSIS_SERIES.values())
        assert "korea_exports_total" in features
