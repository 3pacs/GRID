"""CAT-49 — wage tracker puller tests."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch


from ingestion.altdata.wage_tracker import (
    WAGE_SERIES,
    WageRow,
    WageTrackerPuller,
)


def _build_puller(api_key="fake"):
    p = WageTrackerPuller.__new__(WageTrackerPuller)
    p.engine = MagicMock()
    p.api_key = api_key
    p.source_id = 42
    return p


def _mock_resp(obs):
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.json.return_value = {"observations": obs}
    return r


class TestFetch:
    def test_no_key_empty(self):
        assert _build_puller(api_key="")._fetch_series("FRBATLWGT3MMAUMHWGO") == []

    def test_happy_path(self):
        puller = _build_puller()
        obs = [{"date": "2026-01-01", "value": "4.5"}]
        with patch(
            "ingestion.altdata.wage_tracker.requests.get",
            return_value=_mock_resp(obs),
        ):
            rows = puller._fetch_series("FRBATLWGT3MMAUMHWGO")
        assert len(rows) == 1
        assert rows[0].series_id == "wage_tracker:median_3mma_overall"
        assert rows[0].value == 4.5

    def test_missing_skipped(self):
        puller = _build_puller()
        obs = [
            {"date": "2026-01-01", "value": "."},
            {"date": "2026-02-01", "value": "4.6"},
        ]
        with patch(
            "ingestion.altdata.wage_tracker.requests.get",
            return_value=_mock_resp(obs),
        ):
            rows = puller._fetch_series("FRBATLWGT3MMAUMHWGO")
        assert len(rows) == 1

    def test_http_failure(self):
        puller = _build_puller()
        with patch(
            "ingestion.altdata.wage_tracker.requests.get",
            side_effect=RuntimeError("down"),
        ):
            assert puller._fetch_series("FRBATLWGT3MMAUMHWGO") == []


class TestUpsert:
    def test_empty(self):
        assert _build_puller()._upsert_rows([]) == 0

    def test_skips_existing(self):
        puller = _build_puller()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.begin.return_value = conn
        puller._get_existing_dates = MagicMock(return_value={date(2026, 1, 1)})
        inserts = []

        def capture(q, p=None):
            if "INSERT INTO raw_series" in str(q):
                inserts.append(p)
            return MagicMock()

        conn.execute = capture
        rows = [
            WageRow("wage_tracker:median_3mma_overall", date(2026, 1, 1), 4.5),
            WageRow("wage_tracker:median_3mma_overall", date(2026, 2, 1), 4.6),
        ]
        assert puller._upsert_rows(rows) == 1


class TestPullAll:
    def test_iterates_series(self):
        puller = _build_puller()
        with patch.object(puller, "_fetch_series", return_value=[
            WageRow("wage_tracker:median_3mma_overall", date(2026, 1, 1), 4.5),
        ]), patch.object(puller, "_upsert_rows", return_value=1):
            result = puller.pull_all()
        assert result["fetched"] == len(WAGE_SERIES)
        assert result["inserted"] == len(WAGE_SERIES)


class TestConstants:
    def test_four_series(self):
        assert len(WAGE_SERIES) == 4
        labels = set(WAGE_SERIES.values())
        assert "median_3mma_overall" in labels
        assert "median_3mma_stayers" in labels
        assert "median_3mma_switchers" in labels
