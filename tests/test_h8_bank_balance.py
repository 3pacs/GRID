"""CAT-27 — H.8 bank balance sheet puller tests."""
from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch


from ingestion.altdata.h8_bank_balance import (
    H8_SERIES,
    H8BankBalancePuller,
    H8Row,
)


def _build_puller(api_key="fake"):
    """Build a puller with mocked DB engine + source_id resolution."""
    puller = H8BankBalancePuller.__new__(H8BankBalancePuller)
    puller.engine = MagicMock()
    puller.api_key = api_key
    puller.source_id = 42
    return puller


def _mock_fred_response(observations):
    """Return a mock response object with .json() + .raise_for_status()."""
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"observations": observations}
    return resp


class TestFetchSeries:
    def test_no_api_key_returns_empty(self):
        puller = _build_puller(api_key="")
        assert puller._fetch_series("H8B1001NCBCMG") == []

    def test_happy_path(self):
        puller = _build_puller()
        obs = [
            {"date": "2026-04-02", "value": "18500.5"},
            {"date": "2026-04-09", "value": "18520.1"},
        ]
        with patch(
            "ingestion.altdata.h8_bank_balance.requests.get",
            return_value=_mock_fred_response(obs),
        ):
            rows = puller._fetch_series("H8B1001NCBCMG")
        assert len(rows) == 2
        assert rows[0].series_id == "fed_h8:H8B1001NCBCMG"
        assert rows[0].obs_date == date(2026, 4, 2)
        assert rows[0].value == 18500.5

    def test_missing_value_skipped(self):
        puller = _build_puller()
        obs = [
            {"date": "2026-04-02", "value": "."},
            {"date": "2026-04-09", "value": "18520.1"},
        ]
        with patch(
            "ingestion.altdata.h8_bank_balance.requests.get",
            return_value=_mock_fred_response(obs),
        ):
            rows = puller._fetch_series("H8B1001NCBCMG")
        assert len(rows) == 1
        assert rows[0].obs_date == date(2026, 4, 9)

    def test_malformed_row_skipped(self):
        puller = _build_puller()
        obs = [
            {"date": "not-a-date", "value": "100"},
            {"date": "2026-04-09", "value": "abc"},
            {"date": "2026-04-16", "value": "18520.1"},
        ]
        with patch(
            "ingestion.altdata.h8_bank_balance.requests.get",
            return_value=_mock_fred_response(obs),
        ):
            rows = puller._fetch_series("H8B1001NCBCMG")
        assert len(rows) == 1
        assert rows[0].obs_date == date(2026, 4, 16)

    def test_http_failure_non_fatal(self):
        puller = _build_puller()
        with patch(
            "ingestion.altdata.h8_bank_balance.requests.get",
            side_effect=RuntimeError("connection reset"),
        ):
            rows = puller._fetch_series("H8B1001NCBCMG")
        assert rows == []

    def test_empty_observations(self):
        puller = _build_puller()
        with patch(
            "ingestion.altdata.h8_bank_balance.requests.get",
            return_value=_mock_fred_response([]),
        ):
            rows = puller._fetch_series("H8B1001NCBCMG")
        assert rows == []


class TestUpsertRows:
    def test_empty_no_op(self):
        puller = _build_puller()
        assert puller._upsert_rows([]) == 0

    def test_skips_existing_dates(self):
        puller = _build_puller()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.begin.return_value = conn

        # Pre-seed existing dates
        existing_result = MagicMock()
        existing_result.fetchall.return_value = [(date(2026, 4, 2),)]
        conn.execute.return_value = existing_result

        puller._get_existing_dates = MagicMock(
            return_value={date(2026, 4, 2)},
        )

        rows = [
            H8Row("fed_h8:H8B1001NCBCMG", date(2026, 4, 2), 100.0),
            H8Row("fed_h8:H8B1001NCBCMG", date(2026, 4, 9), 110.0),
        ]
        # Capture INSERT calls
        inserts = []

        def capture(query, params=None):
            sql = str(query)
            if "INSERT INTO raw_series" in sql:
                inserts.append(params)
            return MagicMock()

        conn.execute = capture
        inserted = puller._upsert_rows(rows)
        assert inserted == 1
        assert inserts[0]["od"] == date(2026, 4, 9)

    def test_all_new_rows(self):
        puller = _build_puller()
        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.begin.return_value = conn

        puller._get_existing_dates = MagicMock(return_value=set())

        inserts = []

        def capture(query, params=None):
            sql = str(query)
            if "INSERT INTO raw_series" in sql:
                inserts.append(params)
            return MagicMock()

        conn.execute = capture
        rows = [
            H8Row("fed_h8:H8B1001NCBCMG", date(2026, 4, 2), 100.0),
            H8Row("fed_h8:H8B1001NCBCMG", date(2026, 4, 9), 110.0),
        ]
        assert puller._upsert_rows(rows) == 2
        assert len(inserts) == 2


class TestPullAll:
    def test_pull_all_iterates_series(self):
        puller = _build_puller()
        with patch.object(puller, "_fetch_series", return_value=[
            H8Row("fed_h8:X", date(2026, 4, 2), 100.0),
        ]), patch.object(puller, "_upsert_rows", return_value=1):
            result = puller.pull_all()
        assert result["fetched"] == len(H8_SERIES)
        assert result["inserted"] == len(H8_SERIES)
        assert len(result["series"]) == len(H8_SERIES)

    def test_pull_all_handles_empty_fetches(self):
        puller = _build_puller()
        with patch.object(puller, "_fetch_series", return_value=[]), \
             patch.object(puller, "_upsert_rows", return_value=0):
            result = puller.pull_all()
        assert result["fetched"] == 0
        assert result["inserted"] == 0


class TestH8Constants:
    def test_eight_core_series(self):
        assert len(H8_SERIES) == 8

    def test_series_ids_are_fred_codes(self):
        for code in H8_SERIES:
            assert code.startswith("H8B")
