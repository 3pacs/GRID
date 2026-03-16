"""
Tests for GRID physical economy ingestion modules.

Validates bounding boxes, CPC groups, and series configurations
without requiring live API access or database connectivity.
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


class TestVIIRSPuller:
    """Tests for the NASA VIIRS nighttime lights puller."""

    def test_viirs_bboxes(self):
        """VIIRS_BBOXES has us, china, india, eu regions."""
        from ingestion.physical.viirs import VIIRS_BBOXES
        assert "us" in VIIRS_BBOXES
        assert "china" in VIIRS_BBOXES
        assert "india" in VIIRS_BBOXES
        assert "eu" in VIIRS_BBOXES

    def test_viirs_bbox_valid(self):
        """All bbox lat/lon values are within valid ranges."""
        from ingestion.physical.viirs import VIIRS_BBOXES
        for region, bbox in VIIRS_BBOXES.items():
            assert -90 <= bbox["lat_min"] <= 90, f"{region} lat_min out of range"
            assert -90 <= bbox["lat_max"] <= 90, f"{region} lat_max out of range"
            assert -180 <= bbox["lon_min"] <= 180, f"{region} lon_min out of range"
            assert -180 <= bbox["lon_max"] <= 180, f"{region} lon_max out of range"
            assert bbox["lat_min"] < bbox["lat_max"], f"{region} lat_min >= lat_max"
            assert bbox["lon_min"] < bbox["lon_max"], f"{region} lon_min >= lon_max"


class TestPatentsPuller:
    """Tests for the USPTO PatentsView puller."""

    def test_patents_cpc_groups(self):
        """CPC_GROUPS has at least 6 entries."""
        from ingestion.physical.patents import CPC_GROUPS
        assert len(CPC_GROUPS) >= 6

    def test_patents_has_software(self):
        """CPC groups includes software (G06)."""
        from ingestion.physical.patents import CPC_GROUPS
        assert "G06" in CPC_GROUPS


class TestOFRPuller:
    """Tests for the OFR Financial Stability puller."""

    def test_ofr_puller_init(self):
        """OFRPuller initializes without error."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
        mock_engine.connect.return_value.__exit__ = MagicMock(return_value=False)
        mock_conn.execute.return_value.fetchone.return_value = (1,)

        from ingestion.physical.ofr import OFRPuller
        puller = OFRPuller(db_engine=mock_engine)
        assert puller.source_id == 1

    def test_ofr_datasets_defined(self):
        """OFR_DATASETS has FSI and STFM entries."""
        from ingestion.physical.ofr import OFR_DATASETS
        assert "financial-stress-index" in OFR_DATASETS
        assert "short-term-funding-monitor" in OFR_DATASETS


class TestOpportunityFiles:
    """Tests for Opportunity Insights file definitions."""

    def test_opportunity_files(self):
        """OI_FILES covers spend and employment."""
        from ingestion.altdata.opportunity import OI_FILES
        features = [f["feature"] for f in OI_FILES]
        assert "oi_consumer_spend" in features
        assert "oi_employment_overall" in features
        assert "oi_spend_low_income" in features
        assert "oi_spend_high_income" in features
