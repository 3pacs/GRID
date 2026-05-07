"""CAT-81 — Cass + ATA freight puller tests."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch


from ingestion.altdata.freight_cass_ata import (
    FREIGHT_SERIES,
    FreightPuller,
    FreightRow,
)


def _build_puller(api_key="fake"):
    p = FreightPuller.__new__(FreightPuller)
    p.engine = MagicMock()
    p.api_key = api_key
    p.source_id = 42
    return p


def _mock_resp(observations):
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.json.return_value = {"observations": observations}
    return r


class TestFetch:
    def test_no_key_empty(self):
        puller = _build_puller(api_key="")
        assert puller._fetch_series("FRGTCASSSHP") == []

    def test_happy_path(self):
        puller = _build_puller()
        obs = [
            {"date": "2026-01-01", "value": "125.3"},
            {"date": "2026-02-01", "value": "126.1"},
        ]
        with patch(
            "ingestion.altdata.freight_cass_ata.requests.get",
            return_value=_mock_resp(obs),
        ):
            rows = puller._fetch_series("FRGTCASSSHP")
        assert len(rows) == 2
        assert rows[0].series_id == "freight:cass_shipments"
        assert rows[0].value == 125.3

    def test_missing_values_skipped(self):
        puller = _build_puller()
        obs = [
            {"date": "2026-01-01", "value": "."},
            {"date": "2026-02-01", "value": "126.1"},
        ]
        with patch(
            "ingestion.altdata.freight_cass_ata.requests.get",
            return_value=_mock_resp(obs),
        ):
            rows = puller._fetch_series("FRGTCASSSHP")
        assert len(rows) == 1

    def test_http_failure(self):
        puller = _build_puller()
        with patch(
            "ingestion.altdata.freight_cass_ata.requests.get",
            side_effect=RuntimeError("down"),
        ):
            assert puller._fetch_series("FRGTCASSSHP") == []

    def test_series_id_uses_label(self):
        puller = _build_puller()
        obs = [{"date": "2026-01-01", "value": "100.0"}]
        with patch(
            "ingestion.altdata.freight_cass_ata.requests.get",
            return_value=_mock_resp(obs),
        ):
            rows = puller._fetch_series("TRUCKD11")
        assert rows[0].series_id == "freight:ata_tonnage_sa"


class TestUpsert:
    def test_empty(self):
        puller = _build_puller()
        assert puller._upsert_rows([]) == 0

    def test_skips_existing(self):
        puller = _build_puller()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.begin.return_value = conn
        puller._get_existing_dates = MagicMock(
            return_value={date(2026, 1, 1)}
        )
        inserts = []

        def capture(q, p=None):
            if "INSERT INTO raw_series" in str(q):
                inserts.append(p)
            return MagicMock()

        conn.execute = capture
        rows = [
            FreightRow("freight:cass_shipments", date(2026, 1, 1), 100.0),
            FreightRow("freight:cass_shipments", date(2026, 2, 1), 101.0),
        ]
        assert puller._upsert_rows(rows) == 1


class TestPullAll:
    def test_iterates_series(self):
        puller = _build_puller()
        with patch.object(puller, "_fetch_series", return_value=[
            FreightRow("freight:cass_shipments", date(2026, 1, 1), 100.0),
        ]), patch.object(puller, "_upsert_rows", return_value=1):
            result = puller.pull_all()
        assert result["fetched"] == len(FREIGHT_SERIES)
        assert result["inserted"] == len(FREIGHT_SERIES)


class TestConstants:
    def test_four_series(self):
        assert len(FREIGHT_SERIES) == 4

    def test_has_cass_and_ata(self):
        labels = set(FREIGHT_SERIES.values())
        assert "cass_shipments" in labels
        assert "cass_expenditures" in labels
        assert "ata_tonnage_sa" in labels
        assert "ata_tonnage_nsa" in labels
