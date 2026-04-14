"""Unit tests for ingestion/altdata/credit_index_proxies.py.

Covers configuration, dataclass shape, the pure ``compute_ig_hy_basis``
helper, the FRED fetch with mocked ``requests.get``, and the end-to-end
``run_credit_index_proxies_puller`` entrypoint with happy path,
missing-key, partial-failure, sentinel handling, and idempotency cases.
"""

from __future__ import annotations

from dataclasses import FrozenInstanceError
from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.credit_index_proxies import (
    CreditIndexBasis,
    CreditIndexProxiesPuller,
    CreditProxySnapshot,
    PROXY_CORRELATION_NOTES,
    PROXY_SERIES,
    compute_ig_hy_basis,
    run_credit_index_proxies_puller,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def stateful_mock_engine():
    """Mock SQLAlchemy engine that records every _insert_raw call.

    Returns a tuple ``(engine, sink)`` where ``sink`` is a list of every
    INSERT executed, allowing tests to assert on series_id namespaces and
    counts. The connection's fetchone / fetchall return None / [] by
    default so existing-dates lookups behave as if the table were empty.
    """
    engine = MagicMock()
    sink: list[dict[str, Any]] = []
    state: dict[str, Any] = {"existing_by_sid": {}}

    conn = MagicMock()

    def execute(statement, params=None):
        result = MagicMock()
        sql_str = str(statement)
        # source_catalog lookup → return id 99
        if "source_catalog" in sql_str and "SELECT id" in sql_str:
            row = MagicMock()
            row.__getitem__ = lambda self, idx: 99
            result.fetchone.return_value = row
            return result
        # _get_existing_dates query
        if "SELECT DISTINCT obs_date" in sql_str and params is not None:
            sid = params.get("sid")
            existing = state["existing_by_sid"].get(sid, set())
            result.fetchall.return_value = [(d,) for d in sorted(existing)]
            return result
        # raw_series INSERT
        if "INSERT INTO raw_series" in sql_str:
            sink.append(dict(params or {}))
            sid = params.get("sid") if params else None
            od = params.get("od") if params else None
            if sid is not None and od is not None:
                state["existing_by_sid"].setdefault(sid, set()).add(od)
            return result
        result.fetchone.return_value = None
        result.fetchall.return_value = []
        return result

    conn.execute.side_effect = execute

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    return engine, sink, state


def _fred_payload(rows: list[tuple[str, str | float]]) -> dict[str, Any]:
    """Build a fake FRED REST observations payload."""
    return {
        "realtime_start": "2026-04-13",
        "realtime_end": "2026-04-13",
        "observations": [
            {
                "realtime_start": "2026-04-13",
                "realtime_end": "2026-04-13",
                "date": d,
                "value": v,
            }
            for d, v in rows
        ],
    }


def _make_response(payload: dict[str, Any], status: int = 200) -> MagicMock:
    """Build a mock requests Response."""
    resp = MagicMock()
    resp.json.return_value = payload
    resp.status_code = status
    if status >= 400:
        resp.raise_for_status.side_effect = Exception(f"HTTP {status}")
    else:
        resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------


class TestProxySeriesConfig:
    def test_three_groups(self):
        assert set(PROXY_SERIES.keys()) == {
            "cat_7_china_hy",
            "cat_13_euro_at1",
            "cat_42_cdx_itraxx",
        }

    def test_each_group_has_at_least_three_series(self):
        for group, series in PROXY_SERIES.items():
            assert len(series) >= 3, f"{group} has fewer than 3 series"

    def test_total_ten_fred_series(self):
        total = sum(len(s) for s in PROXY_SERIES.values())
        assert total == 10

    def test_all_fred_ids_are_non_empty_strings(self):
        for group, series in PROXY_SERIES.items():
            for label, fid in series.items():
                assert isinstance(fid, str), f"{group}/{label} not a string"
                assert fid.strip(), f"{group}/{label} is empty"


class TestProxyCorrelationNotes:
    def test_one_note_per_group(self):
        assert set(PROXY_CORRELATION_NOTES.keys()) == set(PROXY_SERIES.keys())

    def test_notes_are_non_empty(self):
        for group, note in PROXY_CORRELATION_NOTES.items():
            assert isinstance(note, str)
            assert len(note) > 20, f"{group} note too short"


# ---------------------------------------------------------------------------
# Dataclasses
# ---------------------------------------------------------------------------


class TestCreditProxySnapshot:
    def test_frozen_and_is_proxy_true(self):
        snap = CreditProxySnapshot(
            date=date(2025, 1, 1),
            group="cat_7_china_hy",
            series_label="em_hy_oas",
            value=512.0,
            is_proxy=True,
            proxy_target="iBoxx USD Asia HY",
        )
        assert snap.is_proxy is True
        with pytest.raises(FrozenInstanceError):
            snap.value = 999.0  # type: ignore[misc]

    def test_credit_index_basis_holds_optional_basis(self):
        b = CreditIndexBasis(
            date=date(2025, 1, 1),
            group="cat_42_cdx_itraxx",
            bbb_oas_bp=120.0,
            hy_bb_oas_bp=500.0,
            ig_hy_basis_bp=380.0,
        )
        assert b.ig_hy_basis_bp == 380.0


# ---------------------------------------------------------------------------
# Pure helper
# ---------------------------------------------------------------------------


class TestComputeIgHyBasis:
    def test_happy_path(self):
        assert compute_ig_hy_basis(120.0, 500.0) == 380.0

    def test_none_ig(self):
        assert compute_ig_hy_basis(None, 500.0) is None

    def test_none_hy(self):
        assert compute_ig_hy_basis(120.0, None) is None

    def test_both_none(self):
        assert compute_ig_hy_basis(None, None) is None

    def test_zero_ig_returns_hy(self):
        assert compute_ig_hy_basis(0.0, 250.0) == 250.0

    def test_negative_basis_not_clamped(self):
        # Inversion: HY trades inside IG (shouldn't happen, but if it does
        # we MUST surface the negative number, never clamp to zero).
        assert compute_ig_hy_basis(300.0, 250.0) == -50.0


# ---------------------------------------------------------------------------
# Puller end-to-end with mocked FRED
# ---------------------------------------------------------------------------


def _all_groups_response_map() -> dict[str, dict[str, Any]]:
    """Map every FRED id used by PROXY_SERIES to a small canned response.

    Each group emits two dates (2025-01-01 and 2025-01-02) so that the
    IG-HY basis composite has matching legs to materialise.
    """
    canned: dict[str, dict[str, Any]] = {}
    base_values = {
        "BAMLEMHYCRPIOAS": [("2025-01-01", "5.20"), ("2025-01-02", "5.30")],
        "BAMLEMIBHYCRPIEY": [("2025-01-01", "8.10"), ("2025-01-02", "8.15")],
        "BAMLEMCBPIOAS": [("2025-01-01", "1.80"), ("2025-01-02", "1.85")],
        "BAMLHE00EHYIOAS": [("2025-01-01", "4.10"), ("2025-01-02", "4.20")],
        "BAMLHE00EHYIEY": [("2025-01-01", "6.50"), ("2025-01-02", "6.55")],
        "BAMLEMRACRPIEMEAOAS": [("2025-01-01", "1.30"), ("2025-01-02", "1.32")],
        "BAMLC0A4CBBB": [("2025-01-01", "1.20"), ("2025-01-02", "1.25")],
        "BAMLH0A1HYBB": [("2025-01-01", "2.80"), ("2025-01-02", "2.85")],
        "BAMLH0A2HYB": [("2025-01-01", "4.50"), ("2025-01-02", "4.55")],
        "BAMLH0A3HYC": [("2025-01-01", "8.40"), ("2025-01-02", "8.50")],
    }
    for fid, rows in base_values.items():
        canned[fid] = _fred_payload(rows)
    return canned


def _make_fred_get(canned: dict[str, dict[str, Any]]):
    """Return a fake requests.get that dispatches by series_id param."""

    def fake_get(url, params=None, timeout=None):
        sid = (params or {}).get("series_id")
        if sid in canned:
            return _make_response(canned[sid])
        return _make_response({"observations": []})

    return fake_get


class TestRunCreditIndexProxiesPuller:
    def test_happy_path_pulls_all_ten_series(self, stateful_mock_engine):
        engine, sink, _ = stateful_mock_engine
        canned = _all_groups_response_map()

        with patch(
            "ingestion.altdata.credit_index_proxies.requests.get",
            side_effect=_make_fred_get(canned),
        ):
            puller = CreditIndexProxiesPuller(api_key="dummy", db_engine=engine)
            snapshots = puller.pull()
            result = puller.save_to_db(snapshots)

        # 10 series × 2 dates = 20 leg rows + 3 basis composite rows × 2 dates = 6
        assert len(snapshots) == 20
        assert result["inserted"] == 26
        for group in PROXY_SERIES:
            assert result["groups"][group] > 0

    def test_per_group_counts(self, stateful_mock_engine):
        engine, _, _ = stateful_mock_engine
        canned = _all_groups_response_map()

        with patch(
            "ingestion.altdata.credit_index_proxies.requests.get",
            side_effect=_make_fred_get(canned),
        ):
            puller = CreditIndexProxiesPuller(api_key="dummy", db_engine=engine)
            snapshots = puller.pull()
            result = puller.save_to_db(snapshots)

        # cat_7_china_hy: 3 legs * 2 dates + 2 basis = 8
        # cat_13_euro_at1: 3 legs * 2 dates + 2 basis = 8
        # cat_42_cdx_itraxx: 4 legs * 2 dates + 2 basis = 10
        assert result["groups"]["cat_7_china_hy"] == 8
        assert result["groups"]["cat_13_euro_at1"] == 8
        assert result["groups"]["cat_42_cdx_itraxx"] == 10

    def test_missing_api_key(self, stateful_mock_engine):
        engine, sink, _ = stateful_mock_engine

        puller = CreditIndexProxiesPuller(api_key="", db_engine=engine)
        snapshots = puller.pull()
        result = puller.save_to_db(snapshots)

        assert snapshots == []
        assert result["inserted"] == 0
        # No raw_series inserts attempted
        inserts = [s for s in sink if "sid" in s and "src" in s]
        assert inserts == []

    def test_partial_failure_other_groups_still_insert(self, stateful_mock_engine):
        engine, sink, _ = stateful_mock_engine
        canned = _all_groups_response_map()
        # Make every cat_42 leg return HTTP 500
        cat42_ids = set(PROXY_SERIES["cat_42_cdx_itraxx"].values())

        def fake_get(url, params=None, timeout=None):
            sid = (params or {}).get("series_id")
            if sid in cat42_ids:
                return _make_response({}, status=500)
            if sid in canned:
                return _make_response(canned[sid])
            return _make_response({"observations": []})

        with patch(
            "ingestion.altdata.credit_index_proxies.requests.get",
            side_effect=fake_get,
        ):
            puller = CreditIndexProxiesPuller(api_key="dummy", db_engine=engine)
            snapshots = puller.pull()
            result = puller.save_to_db(snapshots)

        # cat_42 should be empty; cat_7 and cat_13 should still insert
        assert result["groups"]["cat_42_cdx_itraxx"] == 0
        assert result["groups"]["cat_7_china_hy"] > 0
        assert result["groups"]["cat_13_euro_at1"] > 0

    def test_fred_dot_sentinel_and_bad_dates(self, stateful_mock_engine):
        engine, _, _ = stateful_mock_engine
        canned = _all_groups_response_map()
        # Inject a '.' and a malformed-date row into BAMLC0A4CBBB
        canned["BAMLC0A4CBBB"] = _fred_payload(
            [
                ("2025-01-01", "1.20"),
                ("not-a-date", "1.30"),
                ("2025-01-02", "."),
                ("2025-01-03", "1.40"),
            ]
        )

        with patch(
            "ingestion.altdata.credit_index_proxies.requests.get",
            side_effect=_make_fred_get(canned),
        ):
            puller = CreditIndexProxiesPuller(api_key="dummy", db_engine=engine)
            snapshots = puller.pull()

        bbb_snaps = [
            s for s in snapshots if s.series_label == "us_bbb_oas"
        ]
        assert len(bbb_snaps) == 2  # 2025-01-01 and 2025-01-03 only
        bbb_dates = {s.date for s in bbb_snaps}
        assert bbb_dates == {date(2025, 1, 1), date(2025, 1, 3)}

    def test_basis_only_when_both_legs_exist(self, stateful_mock_engine):
        engine, sink, _ = stateful_mock_engine
        canned = _all_groups_response_map()
        # Make BAMLEMCBPIOAS (em_ig_oas) only have 2025-01-01, but
        # BAMLEMHYCRPIOAS (em_hy_oas) keeps both dates → basis only on
        # 2025-01-01 for cat_7.
        canned["BAMLEMCBPIOAS"] = _fred_payload([("2025-01-01", "1.80")])

        with patch(
            "ingestion.altdata.credit_index_proxies.requests.get",
            side_effect=_make_fred_get(canned),
        ):
            puller = CreditIndexProxiesPuller(api_key="dummy", db_engine=engine)
            snapshots = puller.pull()
            puller.save_to_db(snapshots)

        basis_inserts = [
            s for s in sink
            if s.get("sid") == "credit_proxy:cat_7_china_hy:ig_hy_basis_bp"
        ]
        assert len(basis_inserts) == 1
        assert basis_inserts[0]["od"] == date(2025, 1, 1)

    def test_idempotent_rerun(self, stateful_mock_engine):
        engine, sink, _ = stateful_mock_engine
        canned = _all_groups_response_map()

        with patch(
            "ingestion.altdata.credit_index_proxies.requests.get",
            side_effect=_make_fred_get(canned),
        ):
            puller = CreditIndexProxiesPuller(api_key="dummy", db_engine=engine)
            first = puller.save_to_db(puller.pull())
            sink.clear()
            second = puller.save_to_db(puller.pull())

        assert first["inserted"] == 26
        assert second["inserted"] == 0
        assert sink == []

    def test_series_id_namespace_contract(self, stateful_mock_engine):
        engine, sink, _ = stateful_mock_engine
        canned = _all_groups_response_map()

        with patch(
            "ingestion.altdata.credit_index_proxies.requests.get",
            side_effect=_make_fred_get(canned),
        ):
            puller = CreditIndexProxiesPuller(api_key="dummy", db_engine=engine)
            puller.save_to_db(puller.pull())

        seen_sids = {s["sid"] for s in sink if "sid" in s}
        # Every non-basis sid follows credit_proxy:<group>:<label>
        for sid in seen_sids:
            assert sid.startswith("credit_proxy:")
            parts = sid.split(":")
            assert len(parts) == 3
            assert parts[1] in PROXY_SERIES
        # Composite basis SIDs are present for every group that has a pair
        assert "credit_proxy:cat_7_china_hy:ig_hy_basis_bp" in seen_sids
        assert "credit_proxy:cat_13_euro_at1:ig_hy_basis_bp" in seen_sids
        assert "credit_proxy:cat_42_cdx_itraxx:ig_hy_basis_bp" in seen_sids

    def test_proxy_targets_set_correctly(self, stateful_mock_engine):
        engine, _, _ = stateful_mock_engine
        canned = _all_groups_response_map()

        with patch(
            "ingestion.altdata.credit_index_proxies.requests.get",
            side_effect=_make_fred_get(canned),
        ):
            puller = CreditIndexProxiesPuller(api_key="dummy", db_engine=engine)
            snapshots = puller.pull()

        targets_by_group = {}
        for s in snapshots:
            assert s.is_proxy is True
            targets_by_group.setdefault(s.group, set()).add(s.proxy_target)
        assert targets_by_group["cat_7_china_hy"] == {"iBoxx USD Asia HY"}
        assert targets_by_group["cat_13_euro_at1"] == {"iBoxx EUR CoCo"}
        assert targets_by_group["cat_42_cdx_itraxx"] == {
            "CDX NA IG / CDX NA HY / iTraxx Main / Xover"
        }

    def test_run_entrypoint_returns_expected_shape(self, stateful_mock_engine):
        engine, _, _ = stateful_mock_engine
        canned = _all_groups_response_map()

        # Force the entrypoint's internal config import to yield our key.
        import sys
        fake_config = MagicMock()
        fake_config.settings.FRED_API_KEY = "dummy"
        with patch.dict(sys.modules, {"config": fake_config}), patch(
            "ingestion.altdata.credit_index_proxies.requests.get",
            side_effect=_make_fred_get(canned),
        ):
            summary = run_credit_index_proxies_puller(engine)

        assert summary["fetched"] == 20
        assert summary["inserted"] == 26
        assert set(summary["groups"].keys()) == set(PROXY_SERIES.keys())

    def test_run_entrypoint_handles_missing_api_key(self, stateful_mock_engine):
        engine, _, _ = stateful_mock_engine

        import sys
        fake_config = MagicMock()
        fake_config.settings.FRED_API_KEY = ""
        with patch.dict(sys.modules, {"config": fake_config}):
            summary = run_credit_index_proxies_puller(engine)

        assert summary["fetched"] == 0
        assert summary["inserted"] == 0
        assert summary["groups"] == {g: 0 for g in PROXY_SERIES}
