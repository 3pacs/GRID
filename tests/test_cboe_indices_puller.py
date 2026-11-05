"""CBOE indices puller: VIX closes as the FRED VIXCLS backup, and no daily re-insert of history."""

from __future__ import annotations

from datetime import date, timedelta
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


def _pcr_page(ratio: str = "0.87") -> dict:
    """Shape of cdn.cboe.com/data/us/options/market_statistics/daily/<day>_daily_options
    (probed from grid-svr 2026-11-04, trimmed)."""
    return {
        "ratios": [
            {"name": "TOTAL PUT/CALL RATIO", "value": ratio},
            {"name": "INDEX PUT/CALL RATIO", "value": "0.96"},
        ],
        "SUM OF ALL PRODUCTS": [
            {"name": "VOLUME", "call": 7971384, "put": 6917180, "total": 14888564},
        ],
    }


class _Today(date):
    @classmethod
    def today(cls):
        return cls(2026, 11, 4)  # a Wednesday


@pytest.mark.unit
class TestDeadFeedsReplaced:
    def test_dead_icj_and_totalpc_urls_are_gone(self):
        urls = " ".join(c["url"] + c.get("fallback_url", "") for c in CBOE_INDICES.values())
        assert "ICJ_History" not in urls and "totalpc.csv" not in urls
        assert "correlation_index" not in CBOE_INDICES and "put_call_ratio" not in CBOE_INDICES

    def test_cor3m_is_a_close_series_mapped_to_a_registered_name(self):
        from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS

        cfg = CBOE_INDICES["CBOE:COR3M"]
        assert cfg["url"].endswith("COR3M_History.csv") and cfg["value_col"] == "CLOSE"
        assert {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}["CBOE:COR3M"] == "implied_correlation_3m"

    def test_pull_all_includes_put_call(self, puller):
        ok = {"status": "SUCCESS", "rows_inserted": 0}
        with patch.object(puller, "pull_index", return_value=ok), \
             patch.object(puller, "pull_put_call_ratio", return_value=dict(ok)) as pcr:
            results = puller.pull_all(days_back=7)
        pcr.assert_called_once_with(days_back=7)
        assert len(results) == len(CBOE_INDICES) + 1


@pytest.mark.unit
class TestPutCallDailyPages:
    def _run(self, puller, monkeypatch, pages: dict, existing=frozenset(), days_back=None):
        monkeypatch.setattr(cboe_indices, "date", _Today)
        inserted: list[tuple[date, float]] = []
        requested: list[str] = []

        def fake_json(url):
            requested.append(url)
            return pages.get(url.rsplit("/", 1)[1].split("_")[0])

        with patch.object(puller, "_download_json", side_effect=fake_json), \
             patch.object(puller, "_get_existing_dates", return_value=set(existing)), \
             patch.object(puller, "_insert_raw",
                          side_effect=lambda conn, series_id, obs_date, value, **_:
                          inserted.append((obs_date, value))):
            out = puller.pull_put_call_ratio(days_back=days_back)
        return out, inserted, requested

    def test_stores_total_ratio_for_missing_weekdays_only(self, puller, monkeypatch):
        pages = {"2026-11-02": _pcr_page("0.87"), "2026-11-03": _pcr_page("0.91")}
        existing = {date(2026, 10, d) for d in range(25, 32)}
        out, inserted, requested = self._run(puller, monkeypatch, pages, existing)
        assert inserted == [(date(2026, 11, 2), 0.87), (date(2026, 11, 3), 0.91)]
        # 11-01 is Sunday: never requested. 11-04 (today) is requested, no page yet.
        assert [u.rsplit("/", 1)[1][:10] for u in requested] == ["2026-11-02", "2026-11-03", "2026-11-04"]
        assert out == {"status": "SUCCESS", "rows_inserted": 2, "feature_name": "put_call_ratio"}

    def test_no_page_for_today_is_not_a_failure(self, puller, monkeypatch):
        existing = {date(2026, 10, 25) + timedelta(days=i) for i in range(10)}
        out, inserted, requested = self._run(puller, monkeypatch, {}, existing)
        assert len(requested) == 1 and inserted == []
        assert out["status"] == "SUCCESS" and out["rows_inserted"] == 0

    def test_every_page_refused_is_failed(self, puller, monkeypatch):
        out, inserted, requested = self._run(puller, monkeypatch, {})
        assert len(requested) >= cboe_indices.PUT_CALL_ALL_REFUSED_MIN
        assert out["status"] == "FAILED" and "refused" in out["error"]

    def test_page_without_ratio_is_not_stored(self, puller, monkeypatch):
        pages = {"2026-11-03": {"ratios": [{"name": "INDEX PUT/CALL RATIO", "value": "1"}]}}
        existing = {date(2026, 10, 25) + timedelta(days=i) for i in range(9)}
        out, inserted, _ = self._run(puller, monkeypatch, pages, existing)
        assert inserted == [] and out["status"] == "FAILED"

    def test_download_json_maps_403_to_no_page(self, puller):
        resp = MagicMock(status_code=403)
        with patch.object(cboe_indices.requests, "get", return_value=resp):
            assert puller._download_json("https://example.invalid/x") is None

    def test_scheduler_sees_all_clean_items_as_no_new_data(self):
        from ingestion.smart_scheduler import OUTCOME_NO_NEW_DATA, _classify_outcome

        items = [{"status": "SUCCESS", "rows_inserted": 0, "feature_name": k} for k in CBOE_INDICES]
        items.append({"status": "SUCCESS", "rows_inserted": 0, "feature_name": "put_call_ratio"})
        assert _classify_outcome(items)[0] == OUTCOME_NO_NEW_DATA
