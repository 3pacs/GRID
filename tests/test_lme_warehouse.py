"""Unit tests for ingestion/altdata/lme_warehouse.py (CAT-51)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.lme_warehouse import (
    LME_METALS,
    LME_API_URL,
    LME_REPORT_URL,
    LMEStockSnapshot,
    LMEWarehousePuller,
    _parse_lme_html,
    _parse_lme_json,
    compute_cancelled_ratio,
    run_lme_warehouse_puller,
)


# ---------------------------------------------------------------------------
# Shared mock engine
# ---------------------------------------------------------------------------


def _make_mock_engine() -> MagicMock:
    """Build a mock Engine that resolves source_id=1 and never dedupes."""
    engine = MagicMock()

    # begin() context manager for save_to_db
    begin_conn = MagicMock()
    begin_result = MagicMock()
    begin_result.fetchone.return_value = None  # _row_exists -> False
    begin_result.fetchall.return_value = []
    begin_conn.execute.return_value = begin_result
    engine.begin.return_value.__enter__ = MagicMock(return_value=begin_conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    # connect() context manager for _resolve_source_id
    connect_conn = MagicMock()
    source_result = MagicMock()
    source_result.fetchone.return_value = (1,)
    connect_conn.execute.return_value = source_result
    engine.connect.return_value.__enter__ = MagicMock(return_value=connect_conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)

    engine._begin_conn = begin_conn
    return engine


def _make_dedup_engine() -> MagicMock:
    """Mock engine that reports every row as already-existing."""
    engine = _make_mock_engine()
    # Make _row_exists return True by returning a row from fetchone
    begin_conn = engine._begin_conn
    exists_result = MagicMock()
    exists_result.fetchone.return_value = (1,)
    exists_result.fetchall.return_value = []
    begin_conn.execute.return_value = exists_result
    return engine


# ---------------------------------------------------------------------------
# Canned data
# ---------------------------------------------------------------------------


_SIMPLE_HTML = """
<html><body>
<h2>LME Warehouse Stocks Report — 2026-04-13</h2>
<table>
  <thead><tr><th>Metal</th><th>Total Stocks (mt)</th><th>Cancelled Warrants (mt)</th></tr></thead>
  <tbody>
    <tr><td>Copper</td><td>120,000</td><td>30,000</td></tr>
    <tr><td>Aluminium</td><td>1,500,000</td><td>600,000</td></tr>
    <tr><td>Zinc</td><td>250,000</td><td>50,000</td></tr>
    <tr><td>Nickel</td><td>60,000</td><td>12,000</td></tr>
    <tr><td>Lead</td><td>90,000</td><td>18,000</td></tr>
    <tr><td>Tin</td><td>4,500</td><td>900</td></tr>
  </tbody>
</table>
</body></html>
"""

_SYNONYM_HTML = """
<html><body>
<p>Report date: 13 April 2026</p>
<table>
  <thead><tr><th>Metal</th><th>Total</th><th>Cancelled</th></tr></thead>
  <tbody>
    <tr><td>Copper (Cu)</td><td>120,000</td><td>30,000</td></tr>
  </tbody>
</table>
</body></html>
"""

_SAMPLE_JSON: dict = {
    "report_date": "2026-04-13",
    "metals": [
        {"metal": "Copper", "total_stocks": 120000, "cancelled_warrants": 30000},
        {"metal": "Aluminium", "total_stocks": 1500000, "cancelled_warrants": 600000},
        {"metal": "Zinc", "total_stocks": 250000, "cancelled_warrants": 50000},
        {"metal": "Nickel", "total_stocks": 60000, "cancelled_warrants": 12000},
        {"metal": "Lead", "total_stocks": 90000, "cancelled_warrants": 18000},
        {"metal": "Tin", "total_stocks": 4500, "cancelled_warrants": 900},
    ],
}


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


class TestConstants:
    def test_metals_tuple_exact(self):
        assert LME_METALS == (
            "copper",
            "aluminum",
            "zinc",
            "nickel",
            "lead",
            "tin",
        )
        assert isinstance(LME_METALS, tuple)
        assert len(LME_METALS) == 6

    def test_metals_is_frozen(self):
        # tuples are immutable — confirm the contract
        with pytest.raises((TypeError, AttributeError)):
            LME_METALS[0] = "gold"  # type: ignore[misc]

    def test_urls_present(self):
        assert LME_API_URL.startswith("https://")
        assert LME_REPORT_URL.startswith("https://")
        assert "lme.com" in LME_REPORT_URL


# ---------------------------------------------------------------------------
# compute_cancelled_ratio
# ---------------------------------------------------------------------------


class TestComputeCancelledRatio:
    def test_zero_total_returns_zero(self):
        assert compute_cancelled_ratio(0.0, 0.0) == 0.0
        assert compute_cancelled_ratio(0.0, 10.0) == 0.0

    def test_negative_total_returns_zero(self):
        assert compute_cancelled_ratio(-5.0, 1.0) == 0.0

    def test_basic_ratio(self):
        assert compute_cancelled_ratio(100.0, 30.0) == pytest.approx(0.30)

    def test_cancelled_exceeds_total_clamps_to_one(self):
        # Shouldn't happen in the wild, but defensive
        assert compute_cancelled_ratio(100.0, 200.0) == 1.0

    def test_full_ratio(self):
        assert compute_cancelled_ratio(100.0, 100.0) == 1.0

    def test_non_numeric_returns_zero(self):
        assert compute_cancelled_ratio("oops", "nope") == 0.0  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# LMEStockSnapshot
# ---------------------------------------------------------------------------


class TestLMEStockSnapshot:
    def test_frozen(self):
        snap = LMEStockSnapshot(
            date=date(2026, 4, 13),
            metal="copper",
            total_stocks_mt=100.0,
            cancelled_warrants_mt=25.0,
            live_stocks_mt=75.0,
            cancelled_ratio=0.25,
        )
        with pytest.raises(FrozenInstanceError):
            snap.metal = "gold"  # type: ignore[misc]

    def test_live_math(self):
        # total 100, cancelled 25, live should be 75
        snap = LMEStockSnapshot(
            date=date(2026, 4, 13),
            metal="copper",
            total_stocks_mt=100.0,
            cancelled_warrants_mt=25.0,
            live_stocks_mt=75.0,
            cancelled_ratio=0.25,
        )
        assert snap.live_stocks_mt == snap.total_stocks_mt - snap.cancelled_warrants_mt


# ---------------------------------------------------------------------------
# _parse_lme_html
# ---------------------------------------------------------------------------


class TestParseLmeHtml:
    def test_parses_six_metals(self):
        snaps = _parse_lme_html(_SIMPLE_HTML)
        assert len(snaps) == 6
        metals = {s.metal for s in snaps}
        assert metals == set(LME_METALS)

    def test_numeric_commas_stripped(self):
        snaps = _parse_lme_html(_SIMPLE_HTML)
        by_metal = {s.metal: s for s in snaps}
        assert by_metal["copper"].total_stocks_mt == 120000.0
        assert by_metal["copper"].cancelled_warrants_mt == 30000.0
        assert by_metal["copper"].live_stocks_mt == 90000.0
        assert by_metal["copper"].cancelled_ratio == pytest.approx(0.25)

    def test_aluminum_synonym(self):
        # "Aluminium" (UK spelling) must map to canonical "aluminum"
        snaps = _parse_lme_html(_SIMPLE_HTML)
        metals = {s.metal for s in snaps}
        assert "aluminum" in metals

    def test_empty_html(self):
        assert _parse_lme_html("") == []
        assert _parse_lme_html("<html></html>") == []

    def test_parenthesised_synonym(self):
        snaps = _parse_lme_html(_SYNONYM_HTML)
        assert len(snaps) == 1
        assert snaps[0].metal == "copper"
        assert snaps[0].date == date(2026, 4, 13)

    def test_handles_dash_and_na_cells(self):
        html = """
        <table>
          <thead><tr><th>Metal</th><th>Total</th><th>Cancelled</th></tr></thead>
          <tr><td>Copper</td><td>—</td><td>N/A</td></tr>
          <tr><td>Zinc</td><td>100</td><td>20</td></tr>
        </table>
        """
        snaps = _parse_lme_html(html)
        # Copper row has no numeric data → skipped; Zinc should parse
        metals = [s.metal for s in snaps]
        assert "zinc" in metals


# ---------------------------------------------------------------------------
# _parse_lme_json
# ---------------------------------------------------------------------------


class TestParseLmeJson:
    def test_canned_payload(self):
        snaps = _parse_lme_json(_SAMPLE_JSON)
        assert len(snaps) == 6
        assert {s.metal for s in snaps} == set(LME_METALS)

    def test_numbers_preserved(self):
        snaps = _parse_lme_json(_SAMPLE_JSON)
        by_metal = {s.metal: s for s in snaps}
        assert by_metal["copper"].total_stocks_mt == 120000.0
        assert by_metal["copper"].cancelled_ratio == pytest.approx(0.25)
        assert by_metal["aluminum"].total_stocks_mt == 1500000.0

    def test_empty_payload(self):
        assert _parse_lme_json({}) == []
        assert _parse_lme_json(None) == []  # type: ignore[arg-type]
        assert _parse_lme_json([]) == []

    def test_alternate_field_names(self):
        payload = [
            {"code": "Cu", "total": 200, "cancelled": 50},
            {"code": "Pb", "total": 80, "cancelled": 8},
        ]
        snaps = _parse_lme_json(payload)
        assert {s.metal for s in snaps} == {"copper", "lead"}


# ---------------------------------------------------------------------------
# run_lme_warehouse_puller — happy path JSON
# ---------------------------------------------------------------------------


class TestRunLmeWarehousePuller:
    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_happy_path_json(self, mock_get):
        resp = MagicMock()
        resp.json.return_value = _SAMPLE_JSON
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        engine = _make_mock_engine()
        result = run_lme_warehouse_puller(engine)

        assert result["source"] == "json"
        assert result["fetched"] == 6
        # 6 metals × 4 series per metal = 24 rows
        assert result["inserted"] == 24
        assert set(result["metals"].keys()) == set(LME_METALS)
        assert result["metals"]["copper"]["cancelled_ratio"] == pytest.approx(0.25)

    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_json_fails_html_fallback(self, mock_get):
        # First call: JSON probe fails with RequestException
        # Second call: HTML returns canned content
        import requests as req

        html_resp = MagicMock()
        html_resp.text = _SIMPLE_HTML
        html_resp.raise_for_status = MagicMock()

        mock_get.side_effect = [
            req.RequestException("503"),  # JSON probe
            html_resp,  # HTML fallback
        ]

        engine = _make_mock_engine()
        result = run_lme_warehouse_puller(engine)

        assert result["source"] == "html"
        assert result["fetched"] == 6
        assert result["inserted"] == 24

    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_both_paths_fail_returns_zero(self, mock_get):
        import requests as req

        mock_get.side_effect = req.RequestException("down")

        engine = _make_mock_engine()
        result = run_lme_warehouse_puller(engine)

        assert result["source"] == "none"
        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert result["metals"] == {}

    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_json_returns_non_json_body(self, mock_get):
        # JSON probe returns 200 but body is not JSON → fall back to HTML
        import requests as req

        json_resp = MagicMock()
        json_resp.raise_for_status = MagicMock()
        json_resp.json.side_effect = ValueError("not json")

        html_resp = MagicMock()
        html_resp.text = _SIMPLE_HTML
        html_resp.raise_for_status = MagicMock()

        mock_get.side_effect = [json_resp, html_resp]

        engine = _make_mock_engine()
        result = run_lme_warehouse_puller(engine)
        assert result["source"] == "html"
        assert result["fetched"] == 6

    @patch("ingestion.altdata.lme_warehouse.requests.get")
    def test_idempotent_rerun_no_duplicates(self, mock_get):
        """When _row_exists reports every row present, nothing should insert."""
        resp = MagicMock()
        resp.json.return_value = _SAMPLE_JSON
        resp.raise_for_status = MagicMock()
        mock_get.return_value = resp

        engine = _make_dedup_engine()
        result = run_lme_warehouse_puller(engine)

        assert result["fetched"] == 6  # still fetched
        assert result["inserted"] == 0  # but nothing new
