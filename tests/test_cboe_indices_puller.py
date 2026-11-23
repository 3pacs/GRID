"""CBOE indices puller: VIX closes as the FRED VIXCLS backup, and no daily re-insert of history."""

from __future__ import annotations

import importlib
import json
import re
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.altdata import cboe_indices
from ingestion.altdata.cboe_indices import (
    CBOE_INDICES,
    EQUITY_PUT_CALL_SERIES_ID,
    PUT_CALL_PAGE_SERIES,
    PUT_CALL_SERIES_ID,
    CBOEIndicesPuller,
)

TOTAL, EQUITY = PUT_CALL_SERIES_ID, EQUITY_PUT_CALL_SERIES_ID
# The 2026-11-20 page exactly as cdn.cboe.com served it to grid-svr on
# 2026-11-23 (sha256 4e8216da...c068).
REAL_PAGE = Path(__file__).parent / "fixtures" / "cboe" / "daily_options_2026-11-20.json"


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


def _pcr_page(ratio: str = "0.87", equity: str = "0.62") -> dict:
    """Shape of cdn.cboe.com/data/us/options/market_statistics/daily/<day>_daily_options
    (probed from grid-svr 2026-11-04, trimmed; the full page is REAL_PAGE)."""
    return {
        "ratios": [
            {"name": "TOTAL PUT/CALL RATIO", "value": ratio},
            {"name": "INDEX PUT/CALL RATIO", "value": "0.96"},
            {"name": "EQUITY PUT/CALL RATIO", "value": equity},
        ],
        "SUM OF ALL PRODUCTS": [
            {"name": "VOLUME", "call": 7971384, "put": 6917180, "total": 14888564},
        ],
        "EQUITY OPTIONS": [
            {"name": "VOLUME", "call": 3000000, "put": 1860000, "total": 4860000},
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

    def test_pull_all_includes_both_put_call_series(self, puller):
        ok = {"status": "SUCCESS", "rows_inserted": 0}
        per_series = [{**ok, "feature_name": sid} for sid in PUT_CALL_PAGE_SERIES]
        with patch.object(puller, "pull_index", return_value=ok), \
             patch.object(puller, "pull_put_call_ratio", return_value=per_series) as pcr:
            results = puller.pull_all(days_back=7)
        pcr.assert_called_once_with(days_back=7)
        assert len(results) == len(CBOE_INDICES) + 2
        assert [r["feature_name"] for r in results[-2:]] == [TOTAL, EQUITY]


@pytest.mark.unit
class TestRealDailyPage:
    """The parser against a real page, not a hand-built one."""

    def test_equity_and_total_ratios_are_the_published_values(self):
        page = json.loads(REAL_PAGE.read_text())
        assert CBOEIndicesPuller._page_ratio(page, "EQUITY PUT/CALL RATIO") == 0.77
        assert CBOEIndicesPuller._page_ratio(page, "TOTAL PUT/CALL RATIO") == 1.13
        assert CBOEIndicesPuller._total_put_call(page) == 1.13

    def test_each_ratio_name_matches_its_own_volume_block(self):
        # Pins name -> block: the published ratio is that block's put/call
        # volume, rounded to the page's two decimals. A wrong pairing (e.g. the
        # equity name with SUM OF ALL PRODUCTS) fails here.
        page = json.loads(REAL_PAGE.read_text())
        assert PUT_CALL_PAGE_SERIES == {
            TOTAL: ("TOTAL PUT/CALL RATIO", "SUM OF ALL PRODUCTS"),
            EQUITY: ("EQUITY PUT/CALL RATIO", "EQUITY OPTIONS"),
        }
        for name, block in PUT_CALL_PAGE_SERIES.values():
            vol = CBOEIndicesPuller._page_volume(page, block)
            assert round(vol["put_volume"] / vol["call_volume"], 2) == CBOEIndicesPuller._page_ratio(page, name)
        assert CBOEIndicesPuller._page_volume(page, "EQUITY OPTIONS") == {
            "put_volume": 2576245, "call_volume": 3360800,
        }

    def test_volume_is_read_by_row_name_not_position(self):
        page = {"EQUITY OPTIONS": [
            {"name": "OPEN INTEREST", "call": 1, "put": 2},
            {"name": "VOLUME", "call": 10, "put": 7},
        ]}
        assert CBOEIndicesPuller._page_volume(page, "EQUITY OPTIONS") == {"put_volume": 7, "call_volume": 10}
        assert CBOEIndicesPuller._page_volume({}, "EQUITY OPTIONS") == {"put_volume": None, "call_volume": None}

    @pytest.mark.parametrize("value", ["0.00", "n/a", None, "nan", "inf"])
    def test_unusable_ratio_is_none(self, value):
        page = {"ratios": [{"name": "EQUITY PUT/CALL RATIO", "value": value}]}
        assert CBOEIndicesPuller._page_ratio(page, "EQUITY PUT/CALL RATIO") is None


@pytest.mark.unit
class TestPutCallDailyPages:
    def _run(self, puller, monkeypatch, pages: dict, existing=None, days_back=None):
        """Run pull_put_call_ratio with ``pages`` by ISO date and ``existing`` dates per series."""
        monkeypatch.setattr(cboe_indices, "date", _Today)
        existing = existing or {}
        inserted: list[tuple[str, date, float, dict]] = []
        requested: list[str] = []

        def fake_json(url):
            requested.append(url)
            return pages.get(url.rsplit("/", 1)[1].split("_")[0])

        with patch.object(puller, "_download_json", side_effect=fake_json), \
             patch.object(puller, "_get_existing_dates",
                          side_effect=lambda sid, conn, **_: set(existing.get(sid, ()))), \
             patch.object(puller, "_insert_raw",
                          side_effect=lambda conn, series_id, obs_date, value, raw_payload=None, **_:
                          inserted.append((series_id, obs_date, value, raw_payload))):
            out = puller.pull_put_call_ratio(days_back=days_back)
        return {r["feature_name"]: r for r in out}, inserted, requested

    @staticmethod
    def _days(requested):
        return [u.rsplit("/", 1)[1][:10] for u in requested]

    def test_one_page_stores_both_ratios_for_missing_weekdays(self, puller, monkeypatch):
        pages = {"2026-11-02": _pcr_page("0.87", "0.61"), "2026-11-03": _pcr_page("0.91", "0.66")}
        on_file = {date(2026, 10, d) for d in range(25, 32)}
        out, inserted, requested = self._run(puller, monkeypatch, pages, {TOTAL: on_file, EQUITY: on_file})
        assert [(sid, d, v) for sid, d, v, _ in inserted] == [
            (TOTAL, date(2026, 11, 2), 0.87), (EQUITY, date(2026, 11, 2), 0.61),
            (TOTAL, date(2026, 11, 3), 0.91), (EQUITY, date(2026, 11, 3), 0.66),
        ]
        # One request per date for both series. 11-01 is Sunday: never
        # requested. 11-04 (today) is requested, no page yet.
        assert self._days(requested) == ["2026-11-02", "2026-11-03", "2026-11-04"]
        assert out == {
            TOTAL: {"status": "SUCCESS", "rows_inserted": 2, "feature_name": TOTAL},
            EQUITY: {"status": "SUCCESS", "rows_inserted": 2, "feature_name": EQUITY},
        }

    def test_payload_carries_each_series_own_volumes(self, puller, monkeypatch):
        page = json.loads(REAL_PAGE.read_text())
        on_file = {date(2026, 10, 25) + timedelta(days=i) for i in range(10)} - {date(2026, 11, 3)}
        _, inserted, _ = self._run(puller, monkeypatch, {"2026-11-03": page}, {TOTAL: on_file, EQUITY: on_file})
        payload = {sid: p for sid, _, _, p in inserted}
        url = "https://cdn.cboe.com/data/us/options/market_statistics/daily/2026-11-03_daily_options"
        assert payload[TOTAL] == {"source_url": url, "put_volume": 11753144, "call_volume": 10439201}
        assert payload[EQUITY] == {"source_url": url, "put_volume": 2576245, "call_volume": 3360800}

    def test_equity_fills_dates_the_total_already_has(self, puller, monkeypatch):
        # The first run after deploy: put_call_ratio holds the whole window,
        # equity_put_call_ratio nothing. Pages are still requested, and only
        # the equity value is written (no second total vintage).
        days = [date(2026, 10, 25) + timedelta(days=i) for i in range(11)]
        pages = {d.isoformat(): _pcr_page("0.9", "0.7") for d in days if d.weekday() < 5 and d < _Today.today()}
        out, inserted, requested = self._run(puller, monkeypatch, pages, {TOTAL: set(days)})
        assert {sid for sid, *_ in inserted} == {EQUITY}
        assert [d for _, d, _, _ in inserted] == [d for d in days if d.isoformat() in pages]
        assert self._days(requested) == [d.isoformat() for d in days if d.weekday() < 5]
        assert out[TOTAL] == {"status": "SUCCESS", "rows_inserted": 0, "feature_name": TOTAL}
        assert out[EQUITY]["status"] == "SUCCESS" and out[EQUITY]["rows_inserted"] == len(pages)

    def test_no_page_for_today_is_not_a_failure(self, puller, monkeypatch):
        on_file = {date(2026, 10, 25) + timedelta(days=i) for i in range(10)}
        out, inserted, requested = self._run(puller, monkeypatch, {}, {TOTAL: on_file, EQUITY: on_file})
        assert len(requested) == 1 and inserted == []
        assert all(r["status"] == "SUCCESS" and r["rows_inserted"] == 0 for r in out.values())

    def test_every_page_refused_fails_both_series(self, puller, monkeypatch):
        out, inserted, requested = self._run(puller, monkeypatch, {})
        assert len(requested) >= cboe_indices.PUT_CALL_ALL_REFUSED_MIN and inserted == []
        assert set(out) == {TOTAL, EQUITY}
        assert all(r["status"] == "FAILED" and "refused" in r["error"] for r in out.values())

    def test_page_without_the_equity_ratio_fails_equity_only(self, puller, monkeypatch):
        page = _pcr_page("0.87")
        page["ratios"] = [r for r in page["ratios"] if r["name"] != "EQUITY PUT/CALL RATIO"]
        on_file = {date(2026, 10, 25) + timedelta(days=i) for i in range(9)}
        out, inserted, _ = self._run(puller, monkeypatch, {"2026-11-03": page}, {TOTAL: on_file, EQUITY: on_file})
        assert [(sid, d) for sid, d, _, _ in inserted] == [(TOTAL, date(2026, 11, 3))]
        assert out[TOTAL]["status"] == "SUCCESS" and out[TOTAL]["rows_inserted"] == 1
        assert out[EQUITY] == {"status": "FAILED", "rows_inserted": 0, "feature_name": EQUITY,
                               "error": "1 put/call page(s) without EQUITY PUT/CALL RATIO"}

    def test_page_without_the_total_ratio_fails_total_only(self, puller, monkeypatch):
        pages = {"2026-11-03": {"ratios": [{"name": "EQUITY PUT/CALL RATIO", "value": "0.70"}]}}
        on_file = {date(2026, 10, 25) + timedelta(days=i) for i in range(9)}
        out, inserted, _ = self._run(puller, monkeypatch, pages, {TOTAL: on_file, EQUITY: on_file})
        assert [(sid, v) for sid, _, v, _ in inserted] == [(EQUITY, 0.70)]
        assert out[TOTAL]["status"] == "FAILED" and out[EQUITY]["status"] == "SUCCESS"

    def test_a_database_error_fails_both_series(self, puller, monkeypatch):
        monkeypatch.setattr(cboe_indices, "date", _Today)
        with patch.object(puller, "_get_existing_dates", side_effect=RuntimeError("db down")):
            out = puller.pull_put_call_ratio()
        assert [(r["feature_name"], r["status"], r["error"]) for r in out] == [
            (TOTAL, "FAILED", "db down"), (EQUITY, "FAILED", "db down"),
        ]

    def test_download_json_maps_403_to_no_page(self, puller):
        resp = MagicMock(status_code=403)
        with patch.object(cboe_indices.requests, "get", return_value=resp):
            assert puller._download_json("https://example.invalid/x") is None

    def test_scheduler_sees_all_clean_items_as_no_new_data(self):
        from ingestion.smart_scheduler import OUTCOME_NO_NEW_DATA, _classify_outcome

        items = [{"status": "SUCCESS", "rows_inserted": 0, "feature_name": k} for k in CBOE_INDICES]
        items += [{"status": "SUCCESS", "rows_inserted": 0, "feature_name": k} for k in PUT_CALL_PAGE_SERIES]
        assert _classify_outcome(items)[0] == OUTCOME_NO_NEW_DATA


# ── Every series_id the puller writes must resolve (D0 gap G11) ────────────
# The daily puller writes SKEW as series_id "skew_index"; only the old bulk
# pull wrote "CBOE:SKEW". With no mapping for "skew_index" the resolver
# dropped every daily row and the feature stopped at 2026-03-25.


def test_every_written_series_id_has_an_entity_mapping():
    from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS

    mappings = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
    written = [*CBOE_INDICES, *PUT_CALL_PAGE_SERIES]
    unmapped = [sid for sid in written if sid not in mappings]
    assert unmapped == [], f"CBOE series ids with no entity mapping: {unmapped}"


def test_skew_daily_and_bulk_ids_resolve_to_the_same_feature():
    from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS

    mappings = {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}
    assert mappings["skew_index"] == mappings["CBOE:SKEW"] == "skew_index"


# ── equity_put_call_ratio resolves: mapping, registry row, polarity ───────
# A mapping with no feature_registry row drops every row ("Mapping exists ...
# but feature not in registry", finra_ats_feature_20261020).

_EQUITY_REVISION = "migrations.versions.cboe_equity_pcr_20261123"


def test_equity_series_maps_to_the_feature_its_revision_registers():
    from normalization.entity_map import NEW_MAPPINGS_V2, SEED_MAPPINGS

    rev = importlib.import_module(_EQUITY_REVISION)
    registered = re.findall(r"\(\s*'([a-z0-9_]+)'\s*,\s*'vol'", rev.EQUITY_PCR_FEATURE)
    assert registered == ["equity_put_call_ratio"] == list(rev.REGISTERED_FEATURES)
    assert {**SEED_MAPPINGS, **NEW_MAPPINGS_V2}[EQUITY] == "equity_put_call_ratio"


def test_equity_revision_is_idempotent_dated_and_refuses_downgrade():
    rev = importlib.import_module(_EQUITY_REVISION)
    sql = " ".join(rev.EQUITY_PCR_FEATURE.split())
    assert sql.endswith("ON CONFLICT (name) DO NOTHING")
    # Dated from the total ratio on the same page; never model-eligible on day one.
    assert "'put_call_ratio')" in sql and "first_obs.obs_date, FALSE" in sql
    assert "%s" not in sql and "{}" not in sql
    assert len(rev.revision) <= 32 and rev.down_revision == "market_state_prov_20261122"
    with pytest.raises(RuntimeError, match="refused"):
        rev.downgrade()


def test_equity_feature_has_the_total_ratio_polarity():
    from oracle.feature_polarity import FEATURE_POLARITY

    assert FEATURE_POLARITY["equity_put_call_ratio"] == FEATURE_POLARITY["put_call_ratio"] == -1
