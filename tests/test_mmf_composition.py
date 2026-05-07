"""CAT-30 — MMF composition puller tests."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch


from ingestion.altdata.mmf_composition import (
    MMF_SERIES,
    MMFCompositionPuller,
    MMFRow,
)


def _build_puller(api_key="fake"):
    puller = MMFCompositionPuller.__new__(MMFCompositionPuller)
    puller.engine = MagicMock()
    puller.api_key = api_key
    puller.source_id = 42
    return puller


def _mock_resp(observations):
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.json.return_value = {"observations": observations}
    return r


class TestFetchSeries:
    def test_no_key_empty(self):
        puller = _build_puller(api_key="")
        assert puller._fetch_series("MMMFFAQ027S") == []

    def test_happy_path(self):
        puller = _build_puller()
        obs = [
            {"date": "2026-01-01", "value": "5500.0"},
            {"date": "2026-02-01", "value": "5600.0"},
        ]
        with patch(
            "ingestion.altdata.mmf_composition.requests.get",
            return_value=_mock_resp(obs),
        ):
            rows = puller._fetch_series("MMMFFAQ027S")
        assert len(rows) == 2
        assert rows[0].series_id == "fed_mmf:MMMFFAQ027S"

    def test_missing_values_skipped(self):
        puller = _build_puller()
        obs = [
            {"date": "2026-01-01", "value": "."},
            {"date": "2026-02-01", "value": "5600.0"},
        ]
        with patch(
            "ingestion.altdata.mmf_composition.requests.get",
            return_value=_mock_resp(obs),
        ):
            rows = puller._fetch_series("MMMFFAQ027S")
        assert len(rows) == 1

    def test_http_failure(self):
        puller = _build_puller()
        with patch(
            "ingestion.altdata.mmf_composition.requests.get",
            side_effect=RuntimeError("down"),
        ):
            rows = puller._fetch_series("MMMFFAQ027S")
        assert rows == []


class TestUpsertRows:
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
            MMFRow("fed_mmf:X", date(2026, 1, 1), 100.0),
            MMFRow("fed_mmf:X", date(2026, 2, 1), 110.0),
        ]
        assert puller._upsert_rows(rows) == 1
        assert len(inserts) == 1


class TestPullAll:
    def test_iterates_series(self):
        puller = _build_puller()
        with patch.object(puller, "_fetch_series", return_value=[
            MMFRow("fed_mmf:X", date(2026, 1, 1), 100.0),
        ]), patch.object(puller, "_upsert_rows", return_value=1):
            result = puller.pull_all()
        assert result["fetched"] == len(MMF_SERIES)
        assert result["inserted"] == len(MMF_SERIES)


class TestConstants:
    def test_mmf_series_count(self):
        assert len(MMF_SERIES) == 4
        assert "MMMFFAQ027S" in MMF_SERIES
        assert "RRPONTSYD" in MMF_SERIES
