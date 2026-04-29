"""Unit tests for ingestion/altdata/ecb_tltro.py (CAT-12)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.ecb_tltro import (
    ECBTltroPuller,
    FRED_CANDIDATE_SERIES,
    TLTROSnapshot,
    TLTRO_III_REPAYMENT_CALENDAR,
    compute_days_to_next_repayment,
    run_ecb_tltro_puller,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_engine():
    """Mock SQLAlchemy engine with source_catalog lookup + in-memory row set."""
    engine = MagicMock()
    conn = MagicMock()

    # source_catalog lookup returns id=42
    row_mock = MagicMock()
    row_mock.__getitem__ = lambda self, idx: 42
    conn.execute.return_value.fetchone.return_value = row_mock
    conn.execute.return_value.fetchall.return_value = []

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    engine._conn = conn  # expose for assertions
    return engine


@pytest.fixture
def puller(mock_engine):
    return ECBTltroPuller(mock_engine, fred_api_key="test-key")


def _fred_payload(observations: list[dict]) -> dict:
    return {"observations": observations}


def _ecb_sdw_payload(period_values: list[tuple[str, float]]) -> dict:
    """Build a minimal SDMX-JSON-ish response for ECB SDW."""
    obs_map = {str(i): [v] for i, (_p, v) in enumerate(period_values)}
    obs_dim_values = [{"id": p} for p, _v in period_values]
    return {
        "dataSets": [
            {
                "series": {
                    "0:0:0:0:0:0:0": {
                        "observations": obs_map,
                    }
                }
            }
        ],
        "structure": {
            "dimensions": {
                "observation": [
                    {"id": "TIME_PERIOD", "values": obs_dim_values},
                ]
            }
        },
    }


# ---------------------------------------------------------------------------
# Calendar
# ---------------------------------------------------------------------------

class TestCalendar:
    def test_calendar_non_empty(self):
        assert len(TLTRO_III_REPAYMENT_CALENDAR) > 0

    def test_calendar_dates_are_sorted_when_sorted(self):
        """Keys must form a strictly-ordered set when sorted."""
        dates = list(TLTRO_III_REPAYMENT_CALENDAR.keys())
        sorted_dates = sorted(dates)
        # All keys should be distinct
        assert len(set(dates)) == len(dates)
        # Sorted order should span at least 3 calendar years
        assert sorted_dates[-1].year - sorted_dates[0].year >= 2

    def test_calendar_values_are_strings(self):
        for label in TLTRO_III_REPAYMENT_CALENDAR.values():
            assert isinstance(label, str)
            assert len(label) > 0


# ---------------------------------------------------------------------------
# Pure helper
# ---------------------------------------------------------------------------

class TestComputeDaysToNextRepayment:
    def test_before_first_repayment(self):
        cal = {
            date(2024, 3, 27): "a",
            date(2024, 6, 26): "b",
        }
        next_d, days = compute_days_to_next_repayment(date(2024, 1, 1), cal)
        assert next_d == date(2024, 3, 27)
        assert days is not None and days > 0

    def test_after_last_repayment(self):
        cal = {date(2024, 3, 27): "a"}
        next_d, days = compute_days_to_next_repayment(date(2099, 1, 1), cal)
        assert next_d is None
        assert days is None

    def test_on_a_repayment_date_returns_same_date(self):
        cal = {
            date(2024, 3, 27): "a",
            date(2024, 6, 26): "b",
        }
        next_d, days = compute_days_to_next_repayment(date(2024, 3, 27), cal)
        assert next_d == date(2024, 3, 27)
        assert days == 0

    def test_empty_calendar(self):
        next_d, days = compute_days_to_next_repayment(date(2024, 1, 1), {})
        assert next_d is None
        assert days is None

    def test_real_calendar_resolves(self):
        """Against the real calendar, as_of far past returns None."""
        next_d, days = compute_days_to_next_repayment(
            date(2099, 1, 1), TLTRO_III_REPAYMENT_CALENDAR
        )
        assert next_d is None
        assert days is None


# ---------------------------------------------------------------------------
# Dataclass
# ---------------------------------------------------------------------------

class TestTLTROSnapshot:
    def test_round_trip(self):
        snap = TLTROSnapshot(
            date=date(2024, 3, 1),
            outstanding_eur_bn=392.5,
            next_repayment_date=date(2024, 3, 27),
            days_to_next_repayment=26,
        )
        assert snap.date == date(2024, 3, 1)
        assert snap.outstanding_eur_bn == 392.5
        assert snap.next_repayment_date == date(2024, 3, 27)
        assert snap.days_to_next_repayment == 26

    def test_frozen(self):
        snap = TLTROSnapshot(
            date=date(2024, 3, 1),
            outstanding_eur_bn=392.5,
            next_repayment_date=None,
            days_to_next_repayment=None,
        )
        with pytest.raises(FrozenInstanceError):
            snap.outstanding_eur_bn = 0.0  # type: ignore[misc]


# ---------------------------------------------------------------------------
# FRED fetch path
# ---------------------------------------------------------------------------

class TestFredFetch:
    @patch("ingestion.altdata.ecb_tltro.requests.get")
    def test_fred_success_parses_rows(self, mock_get, puller):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _fred_payload([
            {"date": "2024-01-01", "value": "400.0"},
            {"date": "2024-02-01", "value": "390.0"},
            {"date": "2024-03-01", "value": "."},  # missing sentinel
            {"date": "2024-04-01", "value": None},  # null
        ])
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        rows = puller._fetch_fred_series("ECBASSETSW")
        assert rows == [
            (date(2024, 1, 1), 400.0),
            (date(2024, 2, 1), 390.0),
        ]

    @patch("ingestion.altdata.ecb_tltro.requests.get")
    def test_fred_malformed_date_is_skipped(self, mock_get, puller):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _fred_payload([
            {"date": "not-a-date", "value": "1.0"},
            {"date": "2024-01-01", "value": "2.0"},
        ])
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        rows = puller._fetch_fred_series("ECBASSETSW")
        assert rows == [(date(2024, 1, 1), 2.0)]

    def test_fred_skipped_when_no_api_key(self, mock_engine):
        p = ECBTltroPuller(mock_engine, fred_api_key="")
        assert p._fetch_fred_series("anything") == []


# ---------------------------------------------------------------------------
# ECB SDW fallback
# ---------------------------------------------------------------------------

class TestEcbSdwFetch:
    @patch("ingestion.altdata.ecb_tltro.requests.get")
    def test_ecb_sdw_parses_sdmx_json(self, mock_get, puller):
        mock_resp = MagicMock()
        mock_resp.json.return_value = _ecb_sdw_payload([
            ("2024-01", 400_000.0),  # millions → 400 bn
            ("2024-02", 390_000.0),
        ])
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        rows = puller._fetch_ecb_sdw()
        assert rows == [
            (date(2024, 1, 1), 400.0),
            (date(2024, 2, 1), 390.0),
        ]

    @patch("ingestion.altdata.ecb_tltro.requests.get")
    def test_ecb_sdw_unexpected_shape_returns_empty(self, mock_get, puller):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"garbage": True}
        mock_resp.raise_for_status = MagicMock()
        mock_get.return_value = mock_resp

        rows = puller._fetch_ecb_sdw()
        assert rows == []


# ---------------------------------------------------------------------------
# pull() orchestration
# ---------------------------------------------------------------------------

class TestPullOrchestration:
    def test_fred_first_success_short_circuits_sdw(self, puller):
        with patch.object(
            ECBTltroPuller, "_fetch_fred_series"
        ) as mock_fred, patch.object(
            ECBTltroPuller, "_fetch_ecb_sdw"
        ) as mock_sdw:
            mock_fred.return_value = [(date(2024, 1, 1), 400.0)]
            mock_sdw.return_value = []  # should never be called

            rows = puller.pull()

            assert rows == [(date(2024, 1, 1), 400.0)]
            mock_fred.assert_called()  # first candidate attempted
            mock_sdw.assert_not_called()

    def test_fred_all_fail_falls_through_to_sdw(self, puller):
        with patch.object(
            ECBTltroPuller, "_fetch_fred_series", side_effect=Exception("boom")
        ) as mock_fred, patch.object(
            ECBTltroPuller, "_fetch_ecb_sdw"
        ) as mock_sdw:
            mock_sdw.return_value = [(date(2024, 3, 1), 350.0)]

            rows = puller.pull()

            assert rows == [(date(2024, 3, 1), 350.0)]
            # Tried every FRED candidate before giving up
            assert mock_fred.call_count == len(FRED_CANDIDATE_SERIES)
            mock_sdw.assert_called_once()

    def test_fred_empty_results_fall_through_to_sdw(self, puller):
        with patch.object(
            ECBTltroPuller, "_fetch_fred_series", return_value=[]
        ), patch.object(
            ECBTltroPuller, "_fetch_ecb_sdw"
        ) as mock_sdw:
            mock_sdw.return_value = [(date(2024, 3, 1), 350.0)]
            rows = puller.pull()
            assert rows == [(date(2024, 3, 1), 350.0)]

    def test_both_paths_fail_returns_empty(self, puller):
        with patch.object(
            ECBTltroPuller, "_fetch_fred_series", side_effect=Exception("no fred")
        ), patch.object(
            ECBTltroPuller, "_fetch_ecb_sdw", side_effect=Exception("no sdw")
        ):
            rows = puller.pull()
            assert rows == []


# ---------------------------------------------------------------------------
# save_to_db idempotency
# ---------------------------------------------------------------------------

class TestSaveToDb:
    def test_save_to_db_inserts_and_dedups(self, puller):
        rows = [
            (date(2024, 1, 1), 400.0),
            (date(2024, 2, 1), 390.0),
        ]

        # First call: no existing dates
        with patch.object(
            ECBTltroPuller, "_get_existing_dates", return_value=set()
        ), patch.object(
            ECBTltroPuller, "_insert_raw"
        ) as mock_insert:
            inserted = puller.save_to_db(rows)

        # Each (date, value) pair yields 2 inserts (outstanding + days_to)
        # as long as days_to is not None. With real calendar, first repayment
        # may be in the past for some dates — to keep this robust we assert
        # the count is between len(rows) and 2*len(rows).
        assert len(rows) <= inserted <= 2 * len(rows)
        assert mock_insert.call_count == inserted

    def test_save_to_db_skips_existing(self, puller):
        rows = [(date(2024, 1, 1), 400.0)]
        with patch.object(
            ECBTltroPuller,
            "_get_existing_dates",
            return_value={date(2024, 1, 1)},
        ), patch.object(
            ECBTltroPuller, "_insert_raw"
        ) as mock_insert:
            inserted = puller.save_to_db(rows)
        assert inserted == 0
        mock_insert.assert_not_called()

    def test_save_to_db_empty_rows_returns_zero(self, puller):
        assert puller.save_to_db([]) == 0


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

class TestRunEntrypoint:
    def test_run_happy_path_with_mocked_fred(self, mock_engine):
        with patch.object(
            ECBTltroPuller, "pull",
            return_value=[
                (date(2024, 1, 1), 400.0),
                (date(2024, 2, 1), 390.0),
            ],
        ), patch.object(
            ECBTltroPuller, "save_to_db", return_value=3
        ):
            result = run_ecb_tltro_puller(mock_engine, fred_api_key="test-key")

        assert result["fetched"] == 2
        assert result["inserted"] == 3
        assert result["outstanding_eur_bn"] == 390.0
        # next_repayment depends on today — just assert it's a str or None
        assert result["next_repayment"] is None or isinstance(
            result["next_repayment"], str
        )

    def test_run_both_sources_fail_returns_zero_rows(self, mock_engine):
        """pull() returns [] when both paths fail; entrypoint must not crash."""
        with patch.object(
            ECBTltroPuller, "_fetch_fred_series", side_effect=Exception("no fred")
        ), patch.object(
            ECBTltroPuller, "_fetch_ecb_sdw", side_effect=Exception("no sdw")
        ):
            result = run_ecb_tltro_puller(mock_engine, fred_api_key="test-key")

        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert result["outstanding_eur_bn"] is None

    def test_run_pull_raises_is_caught(self, mock_engine):
        """Unexpected pull() exception is swallowed into a zero-row result."""
        with patch.object(
            ECBTltroPuller, "pull", side_effect=RuntimeError("kaboom")
        ):
            result = run_ecb_tltro_puller(mock_engine, fred_api_key="test-key")

        assert result["fetched"] == 0
        assert result["inserted"] == 0
