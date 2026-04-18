"""Unit tests for ingestion/altdata/taiwan_exports.py (CAT-9)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pytest
import requests

from ingestion.altdata import taiwan_exports as te
from ingestion.altdata.taiwan_exports import (
    FRED_CANDIDATES,
    HISTORICAL_FOUNDRY_UTILIZATION,
    MOEA_API_URL,
    FoundryUtilization,
    TaiwanExportsPuller,
    TaiwanExportSnapshot,
    compute_yoy,
    run_taiwan_exports_puller,
)


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def patched_base_puller():
    """Patch ``BasePuller.__init__`` so we never touch the DB."""
    with patch.object(
        TaiwanExportsPuller,
        "__init__",
        lambda self, db_engine, fred_api_key=None: _mock_init(
            self, db_engine, fred_api_key
        ),
    ):
        yield


def _mock_init(
    self: TaiwanExportsPuller,
    db_engine,
    fred_api_key=None,
) -> None:
    self.engine = db_engine
    self.source_id = 999  # fake — DB never touched
    self.fred_api_key = fred_api_key or ""
    self._last_source = "none"


class _FakeConn:
    """Minimal connection stub that records every INSERT."""

    def __init__(self) -> None:
        self.calls: list[dict] = []

    def execute(self, sql, params=None):
        self.calls.append(dict(params or {}))
        result = MagicMock()
        result.rowcount = 1
        return result


class _FakeEngine:
    """Engine stub whose ``begin()`` yields a shared ``_FakeConn``."""

    def __init__(self) -> None:
        self.conn = _FakeConn()

    def begin(self):
        engine_self = self

        class _Ctx:
            def __enter__(self_inner):
                return engine_self.conn

            def __exit__(self_inner, exc_type, exc, tb):
                return False

        return _Ctx()


class _IdempotentConn(_FakeConn):
    """Second run — pretend every row is a duplicate (rowcount = 0)."""

    def __init__(self, *, first_run: bool = True) -> None:
        super().__init__()
        self.first_run = first_run

    def execute(self, sql, params=None):
        self.calls.append(dict(params or {}))
        result = MagicMock()
        result.rowcount = 1 if self.first_run else 0
        return result


# ---------------------------------------------------------------------------
# Dataclass round-trip
# ---------------------------------------------------------------------------


def test_snapshot_and_foundry_dataclass_roundtrip():
    snap = TaiwanExportSnapshot(
        month_end=date(2026, 1, 31),
        orders_usd_bn=62.5,
        semiconductor_orders_usd_bn=28.0,
        yoy_pct=12.3,
    )
    assert snap.month_end == date(2026, 1, 31)
    assert snap.orders_usd_bn == 62.5
    assert snap.semiconductor_orders_usd_bn == 28.0
    assert snap.yoy_pct == 12.3
    # frozen
    with pytest.raises(Exception):
        snap.orders_usd_bn = 99.9  # type: ignore[misc]

    util = FoundryUtilization(
        quarter_end=date(2026, 3, 31),
        tsmc_pct=92.0,
        umc_pct=80.0,
        blended_pct=86.0,
    )
    assert util.blended_pct == 86.0
    assert util.tsmc_pct == 92.0


# ---------------------------------------------------------------------------
# compute_yoy
# ---------------------------------------------------------------------------


def test_compute_yoy_happy_path():
    assert compute_yoy(110.0, 100.0) == pytest.approx(10.0)
    assert compute_yoy(80.0, 100.0) == pytest.approx(-20.0)


def test_compute_yoy_zero_prior_returns_none():
    assert compute_yoy(50.0, 0.0) is None


def test_compute_yoy_negative_current():
    # Legitimate math: -50 vs prior 100 → -150%
    assert compute_yoy(-50.0, 100.0) == pytest.approx(-150.0)


def test_compute_yoy_non_numeric_returns_none():
    assert compute_yoy("not-a-number", 100.0) is None  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# HISTORICAL_FOUNDRY_UTILIZATION
# ---------------------------------------------------------------------------


def test_historical_foundry_utilization_nonempty_and_sorted():
    assert len(HISTORICAL_FOUNDRY_UTILIZATION) >= 8
    keys = list(HISTORICAL_FOUNDRY_UTILIZATION.keys())
    assert keys == sorted(keys), "Historical utilization dict must be in ascending date order"
    # All values within a plausible 0-100% band
    for pct in HISTORICAL_FOUNDRY_UTILIZATION.values():
        assert 0.0 < pct <= 100.0


# ---------------------------------------------------------------------------
# Happy path — FRED mocked
# ---------------------------------------------------------------------------


def _fake_fred_payload() -> dict:
    return {
        "observations": [
            {"date": "2024-12-01", "value": "55.4"},
            {"date": "2025-01-01", "value": "58.1"},
            {"date": "2025-12-01", "value": "62.0"},  # YoY vs 2024-12
            {"date": "2026-01-01", "value": "64.8"},  # YoY vs 2025-01
        ]
    }


def test_run_puller_happy_path_fred(patched_base_puller):
    engine = _FakeEngine()

    fake_resp = MagicMock()
    fake_resp.raise_for_status = MagicMock()
    fake_resp.json.return_value = _fake_fred_payload()

    with patch.object(
        te.requests, "get", return_value=fake_resp
    ) as mocked_get:
        result = run_taiwan_exports_puller(engine, fred_api_key="FAKEKEY")

    assert mocked_get.called
    assert result["source"] == "fred"
    assert result["fetched"] == 4  # four snapshots
    # Each snapshot writes orders; two of them also have yoy → 4 + 2 = 6 export rows
    # Plus HISTORICAL_FOUNDRY_UTILIZATION rows.
    expected_min = 4 + 2 + len(HISTORICAL_FOUNDRY_UTILIZATION)
    assert result["inserted"] == expected_min

    # sanity: series_ids written cover the four documented keys
    written_sids = {c.get("sid") for c in engine.conn.calls}
    assert "taiwan:export_orders_usd_bn" in written_sids
    assert "taiwan:export_yoy_pct" in written_sids
    assert "taiwan:foundry_blended_util_pct" in written_sids


# ---------------------------------------------------------------------------
# FRED failure -> MOEA used
# ---------------------------------------------------------------------------


def _fake_moea_payload() -> dict:
    return {
        "result": {
            "records": [
                {
                    "month": "2026-01",
                    "export_value": 62.4,
                    "semiconductor_value": 28.1,
                    "yoy_pct": 8.2,
                },
                {
                    "month": "2026-02",
                    "export_value": 64.0,
                    "semiconductor_value": 29.0,
                    "yoy_pct": 9.1,
                },
            ]
        }
    }


def test_run_puller_falls_back_to_moea(patched_base_puller):
    engine = _FakeEngine()

    call_count = {"n": 0}

    def _router(url, *args, **kwargs):
        call_count["n"] += 1
        # FRED candidates: raise; MOEA: return payload
        if "stlouisfed.org" in url:
            raise requests.ConnectionError("fred down")
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json.return_value = _fake_moea_payload()
        return resp

    with patch.object(te.requests, "get", side_effect=_router):
        result = run_taiwan_exports_puller(engine, fred_api_key="FAKEKEY")

    assert result["source"] == "moea"
    assert result["fetched"] == 2
    # 2 months × 3 fields (orders, semi, yoy) = 6 export rows + historical
    expected = 6 + len(HISTORICAL_FOUNDRY_UTILIZATION)
    assert result["inserted"] == expected
    # Must have hit FRED at least once before falling back
    assert call_count["n"] >= 2


# ---------------------------------------------------------------------------
# Both sources fail
# ---------------------------------------------------------------------------


def test_run_puller_both_sources_fail(patched_base_puller):
    engine = _FakeEngine()

    def _always_fail(*args, **kwargs):
        raise requests.ConnectionError("network down")

    with patch.object(te.requests, "get", side_effect=_always_fail):
        result = run_taiwan_exports_puller(engine, fred_api_key="FAKEKEY")

    assert result["source"] == "none"
    assert result["fetched"] == 0
    # No export rows, but historical foundry utilization still written.
    assert result["inserted"] == len(HISTORICAL_FOUNDRY_UTILIZATION)


def test_run_puller_no_api_key_and_moea_dead(patched_base_puller):
    engine = _FakeEngine()

    def _fail(*args, **kwargs):
        raise requests.ConnectionError("no network")

    with patch.object(te.requests, "get", side_effect=_fail):
        result = run_taiwan_exports_puller(engine, fred_api_key=None)

    # FRED path is skipped (no key); MOEA is down → zero fetched, still writes historical.
    assert result["source"] == "none"
    assert result["fetched"] == 0
    assert result["inserted"] == len(HISTORICAL_FOUNDRY_UTILIZATION)


# ---------------------------------------------------------------------------
# Malformed MOEA JSON
# ---------------------------------------------------------------------------


def test_malformed_moea_payload_handled(patched_base_puller):
    engine = _FakeEngine()

    fake_resp = MagicMock()
    fake_resp.raise_for_status = MagicMock()
    # Payload that is *not* the expected shape
    fake_resp.json.return_value = ["this", "is", "not", "a", "dict"]

    def _router(url, *args, **kwargs):
        if "stlouisfed.org" in url:
            raise requests.ConnectionError("fred down")
        return fake_resp

    with patch.object(te.requests, "get", side_effect=_router):
        result = run_taiwan_exports_puller(engine, fred_api_key="FAKEKEY")

    # Should not crash; should end up with zero fetched and source == 'none'.
    assert result["source"] == "none"
    assert result["fetched"] == 0
    assert result["inserted"] == len(HISTORICAL_FOUNDRY_UTILIZATION)


# ---------------------------------------------------------------------------
# Idempotency on re-run
# ---------------------------------------------------------------------------


def test_idempotent_on_rerun(patched_base_puller):
    """Second run against a DB that reports rowcount=0 inserts zero export rows.

    Simulates the ON CONFLICT DO NOTHING path — the puller should report
    inserted=0 and not crash.
    """
    fake_resp = MagicMock()
    fake_resp.raise_for_status = MagicMock()
    fake_resp.json.return_value = _fake_fred_payload()

    class _SecondRunEngine:
        def __init__(self) -> None:
            self.conn = _IdempotentConn(first_run=False)

        def begin(self):
            engine_self = self

            class _Ctx:
                def __enter__(self_inner):
                    return engine_self.conn

                def __exit__(self_inner, exc_type, exc, tb):
                    return False

            return _Ctx()

    engine = _SecondRunEngine()
    with patch.object(te.requests, "get", return_value=fake_resp):
        result = run_taiwan_exports_puller(engine, fred_api_key="FAKEKEY")

    assert result["source"] == "fred"
    assert result["fetched"] == 4
    assert result["inserted"] == 0  # every row was a duplicate


# ---------------------------------------------------------------------------
# Sanity — module constants
# ---------------------------------------------------------------------------


def test_module_constants_sanity():
    assert "TWNEXPORTS" in FRED_CANDIDATES
    assert "TWNEXPORTQTR" in FRED_CANDIDATES
    assert MOEA_API_URL.startswith("https://")
    assert "data.gov.tw" in MOEA_API_URL
