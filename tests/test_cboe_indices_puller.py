"""CBOE indices puller: VIX closes as the FRED VIXCLS backup, and no daily re-insert of history."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.altdata import cboe_indices
from ingestion.altdata.cboe_indices import CBOE_INDICES, CBOEIndicesPuller


def _engine(source_id: int = 5) -> MagicMock:
    engine = MagicMock()
    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = (source_id,)
    conn.execute.return_value.fetchall.return_value = []
    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine


def _vix_csv() -> pd.DataFrame:
    """Shape of https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv."""
    return pd.DataFrame({
        "DATE": ["09/23/2026", "09/24/2026", "09/25/2026"],
        "OPEN": [14.50, 15.40, 15.80],
        "HIGH": [15.30, 16.00, 15.90],
        "LOW": [14.20, 15.10, 14.70],
        "CLOSE": [15.18, 15.67, 14.87],
    })


@pytest.fixture
def puller(monkeypatch):
    monkeypatch.setattr(cboe_indices.time, "sleep", lambda *_a: None)
    return CBOEIndicesPuller(_engine())


@pytest.mark.unit
class TestVixBackupFeed:
    def test_vix_closes_are_registered_under_the_bulk_series_ids(self):
        # Same ids as scripts/bulk_historical_pull.py wrote (CBOE:VIX 1990→2026-03-25),
        # so normalization/entity_map.py already resolves them to vix_spot & co.
        for sid, expected_file in (
            ("CBOE:VIX", "VIX_History.csv"),
            ("CBOE:VIX3M", "VIX3M_History.csv"),
            ("CBOE:VIX9D", "VIX9D_History.csv"),
        ):
            assert sid in CBOE_INDICES
            assert CBOE_INDICES[sid]["value_col"] == "CLOSE"
            assert CBOE_INDICES[sid]["url"].endswith(expected_file)

    def test_pull_index_stores_the_close_column_not_the_last_numeric_one(self, puller):
        inserted: list[tuple[str, date, float]] = []
        with patch.object(puller, "_download_csv", return_value=_vix_csv()), \
             patch.object(puller, "_get_existing_dates", return_value=set()), \
             patch.object(puller, "_insert_raw",
                          side_effect=lambda conn, series_id, obs_date, value, raw_payload=None, **_:
                          inserted.append((series_id, obs_date, value))):
            out = puller.pull_index("CBOE:VIX", start_date="2026-09-01")

        assert out["status"] == "SUCCESS" and out["rows_inserted"] == 3
        assert inserted == [
            ("CBOE:VIX", date(2026, 9, 23), 15.18),
            ("CBOE:VIX", date(2026, 9, 24), 15.67),
            ("CBOE:VIX", date(2026, 9, 25), 14.87),
        ]


@pytest.mark.unit
class TestNoDailyHistoryReinsert:
    def test_dates_already_on_file_are_skipped(self, puller):
        inserted: list[date] = []
        with patch.object(puller, "_download_csv", return_value=_vix_csv()), \
             patch.object(puller, "_get_existing_dates",
                          return_value={date(2026, 9, 23), date(2026, 9, 24)}) as existing, \
             patch.object(puller, "_insert_raw",
                          side_effect=lambda conn, series_id, obs_date, value, **_:
                          inserted.append(obs_date)):
            out = puller.pull_index("CBOE:VIX", start_date="2026-09-01")

        assert inserted == [date(2026, 9, 25)]
        assert out["rows_inserted"] == 1
        # One window query for the whole CSV, bounded by start_date — not a
        # per-row one-hour lookback, which is what re-inserted all of history.
        existing.assert_called_once()
        assert existing.call_args.kwargs["start_date"] == date(2026, 9, 1)

    def test_rerun_with_full_history_on_file_inserts_nothing(self, puller):
        all_dates = {date(2026, 9, 23), date(2026, 9, 24), date(2026, 9, 25)}
        with patch.object(puller, "_download_csv", return_value=_vix_csv()), \
             patch.object(puller, "_get_existing_dates", return_value=all_dates), \
             patch.object(puller, "_insert_raw") as insert, \
             patch.object(puller, "_row_exists",
                          side_effect=AssertionError("per-row dedup must not be used")):
            out = puller.pull_index("CBOE:VIX", start_date="1990-01-01")

        insert.assert_not_called()
        assert out == {"status": "SUCCESS", "rows_inserted": 0, "feature_name": "CBOE:VIX"}

    def test_days_back_bounds_both_the_rows_and_the_existing_dates_query(self, puller, monkeypatch):
        class _Today(date):
            @classmethod
            def today(cls):
                return cls(2026, 9, 30)

        monkeypatch.setattr(cboe_indices, "date", _Today)
        with patch.object(puller, "_download_csv", return_value=_vix_csv()), \
             patch.object(puller, "_get_existing_dates", return_value=set()) as existing, \
             patch.object(puller, "_insert_raw") as insert:
            out = puller.pull_index("CBOE:VIX", start_date="1990-01-01", days_back=6)

        assert existing.call_args.kwargs["start_date"] == date(2026, 9, 24)
        assert [c.kwargs["obs_date"] for c in insert.call_args_list] == [
            date(2026, 9, 24), date(2026, 9, 25),
        ]
        assert out["rows_inserted"] == 2
