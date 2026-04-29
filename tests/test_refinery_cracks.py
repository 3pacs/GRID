"""CAT-54 — refinery utilization + 3-2-1 crack spread tests."""
from __future__ import annotations

from datetime import date
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from ingestion.altdata.refinery_cracks import (
    GAL_PER_BBL,
    REFINERY_SERIES,
    Crack321,
    RefineryCracksPuller,
    RefineryRow,
    run_refinery_cracks_puller,
)


# ── Helpers ──────────────────────────────────────────────────────────────


def _build_puller(api_key: str = "fake") -> RefineryCracksPuller:
    """Construct a puller without touching the DB (bypasses __init__)."""
    p = RefineryCracksPuller.__new__(RefineryCracksPuller)
    p.engine = MagicMock()
    p.api_key = api_key
    p.source_id = 77
    p._raw_rows = []
    p._crack_rows = []
    return p


def _mock_fred_resp(obs: list[dict[str, Any]]) -> MagicMock:
    r = MagicMock()
    r.raise_for_status.return_value = None
    r.json.return_value = {"observations": obs}
    return r


# ── Constants ────────────────────────────────────────────────────────────


class TestConstants:
    def test_six_series_configured(self) -> None:
        assert len(REFINERY_SERIES) == 6
        labels = set(REFINERY_SERIES.values())
        assert {
            "util_pct",
            "gasoline_stocks",
            "distillate_stocks",
            "wti_spot",
            "gasoline_spot_gulf",
            "diesel_spot_gulf",
        } == labels

    def test_gal_per_bbl_is_standard(self) -> None:
        assert GAL_PER_BBL == 42.0


# ── Crack321 math ────────────────────────────────────────────────────────


class TestCrack321:
    def test_from_spots_formula(self) -> None:
        """(2G + D)/3 − WTI, with G & D converted from $/gal → $/bbl."""
        wti = 80.0
        gas_per_gal = 2.5
        diesel_per_gal = 2.7
        expected_gas_bbl = gas_per_gal * 42.0  # 105.0
        expected_diesel_bbl = diesel_per_gal * 42.0  # 113.4
        expected_crack = (
            2.0 * expected_gas_bbl + expected_diesel_bbl
        ) / 3.0 - wti

        c = Crack321.from_spots(
            obs_date=date(2026, 4, 1),
            wti=wti,
            gasoline_per_gal=gas_per_gal,
            diesel_per_gal=diesel_per_gal,
        )
        assert c.wti == wti
        assert c.gasoline == pytest.approx(expected_gas_bbl)
        assert c.diesel == pytest.approx(expected_diesel_bbl)
        assert c.crack_321 == pytest.approx(expected_crack)
        assert c.obs_date == date(2026, 4, 1)

    def test_crack_positive_in_healthy_margin(self) -> None:
        """Typical Gulf Coast: gas ~$2.50, diesel ~$2.70, WTI ~$80 → positive."""
        c = Crack321.from_spots(
            obs_date=date(2026, 4, 8),
            wti=80.0,
            gasoline_per_gal=2.5,
            diesel_per_gal=2.7,
        )
        assert c.crack_321 > 0

    def test_crack_negative_when_crude_above_products(self) -> None:
        """When crude spikes above product prices, crack inverts negative."""
        c = Crack321.from_spots(
            obs_date=date(2026, 4, 8),
            wti=200.0,
            gasoline_per_gal=1.0,
            diesel_per_gal=1.0,
        )
        assert c.crack_321 < 0


# ── Fetch ────────────────────────────────────────────────────────────────


class TestFetch:
    def test_missing_api_key_returns_empty(self) -> None:
        puller = _build_puller(api_key="")
        assert puller._fetch_series("WCRFPUS2") == []

    def test_happy_path_parses_observations(self) -> None:
        puller = _build_puller()
        obs = [
            {"date": "2026-03-28", "value": "92.5"},
            {"date": "2026-04-04", "value": "93.1"},
        ]
        with patch(
            "ingestion.altdata.refinery_cracks.requests.get",
            return_value=_mock_fred_resp(obs),
        ):
            rows = puller._fetch_series("WCRFPUS2")
        assert len(rows) == 2
        assert rows[0].series_id == "refinery_cracks:util_pct"
        assert rows[0].value == 92.5
        assert rows[0].obs_date == date(2026, 3, 28)

    def test_missing_values_skipped(self) -> None:
        puller = _build_puller()
        obs = [
            {"date": "2026-03-28", "value": "."},
            {"date": "2026-04-04", "value": "93.1"},
            {"date": "2026-04-11", "value": ""},
        ]
        with patch(
            "ingestion.altdata.refinery_cracks.requests.get",
            return_value=_mock_fred_resp(obs),
        ):
            rows = puller._fetch_series("WCRFPUS2")
        assert len(rows) == 1
        assert rows[0].value == 93.1

    def test_http_error_returns_empty(self) -> None:
        puller = _build_puller()
        with patch(
            "ingestion.altdata.refinery_cracks.requests.get",
            side_effect=RuntimeError("fred 503"),
        ):
            assert puller._fetch_series("WCRFPUS2") == []


# ── pull() + materialization ─────────────────────────────────────────────


class TestPull:
    def test_pull_materializes_crack_only_on_shared_dates(self) -> None:
        puller = _build_puller()

        def fake_fetch(fred_code: str, **_: Any) -> list[RefineryRow]:
            label = REFINERY_SERIES[fred_code]
            if label == "wti_spot":
                return [
                    RefineryRow(f"refinery_cracks:{label}", date(2026, 4, 1), 80.0),
                    RefineryRow(f"refinery_cracks:{label}", date(2026, 4, 8), 82.0),
                ]
            if label == "gasoline_spot_gulf":
                return [
                    RefineryRow(f"refinery_cracks:{label}", date(2026, 4, 1), 2.5),
                    RefineryRow(f"refinery_cracks:{label}", date(2026, 4, 8), 2.6),
                ]
            if label == "diesel_spot_gulf":
                return [
                    # Only one shared date with WTI + gas
                    RefineryRow(f"refinery_cracks:{label}", date(2026, 4, 1), 2.7),
                ]
            # Utilization / stock series — not used for crack_321
            return [
                RefineryRow(f"refinery_cracks:{label}", date(2026, 4, 1), 90.0),
            ]

        with patch.object(puller, "_fetch_series", side_effect=fake_fetch):
            summary = puller.pull()

        # Only 2026-04-01 has all three spot components → 1 crack row
        assert summary["crack_321_rows"] == 1
        assert len(puller._crack_rows) == 1
        assert puller._crack_rows[0].series_id == "refinery_cracks:crack_321"
        assert puller._crack_rows[0].obs_date == date(2026, 4, 1)
        # Fetched raw: 3 util/stock series (1 row each) + 2 wti + 2 gas + 1 diesel = 8
        assert summary["fetched_raw"] == 8


# ── save_to_db() ─────────────────────────────────────────────────────────


class TestSaveToDb:
    def test_save_to_db_upserts_raw_and_crack(self) -> None:
        puller = _build_puller()
        puller._raw_rows = [
            RefineryRow("refinery_cracks:util_pct", date(2026, 4, 1), 92.0),
        ]
        puller._crack_rows = [
            RefineryRow("refinery_cracks:crack_321", date(2026, 4, 1), 25.5),
        ]

        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.begin.return_value = conn
        puller._get_existing_dates = MagicMock(return_value=set())

        inserts: list[dict[str, Any]] = []

        def capture(q: Any, p: dict[str, Any] | None = None) -> MagicMock:
            if "INSERT INTO raw_series" in str(q):
                inserts.append(p or {})
            return MagicMock()

        conn.execute = capture
        total = puller.save_to_db()
        assert total == 2
        assert len(inserts) == 2
        series_ids = {row["sid"] for row in inserts}
        assert "refinery_cracks:util_pct" in series_ids
        assert "refinery_cracks:crack_321" in series_ids

    def test_save_to_db_skips_existing_dates(self) -> None:
        puller = _build_puller()
        puller._raw_rows = [
            RefineryRow("refinery_cracks:util_pct", date(2026, 4, 1), 92.0),
            RefineryRow("refinery_cracks:util_pct", date(2026, 4, 8), 93.0),
        ]
        puller._crack_rows = []

        conn = MagicMock()
        conn.__enter__.return_value = conn
        conn.__exit__.return_value = False
        puller.engine.begin.return_value = conn
        puller._get_existing_dates = MagicMock(return_value={date(2026, 4, 1)})

        inserts: list[dict[str, Any]] = []

        def capture(q: Any, p: dict[str, Any] | None = None) -> MagicMock:
            if "INSERT INTO raw_series" in str(q):
                inserts.append(p or {})
            return MagicMock()

        conn.execute = capture
        total = puller.save_to_db()
        assert total == 1
        assert inserts[0]["od"] == date(2026, 4, 8)

    def test_save_to_db_empty_noop(self) -> None:
        puller = _build_puller()
        puller._raw_rows = []
        puller._crack_rows = []
        assert puller.save_to_db() == 0


# ── run_refinery_cracks_puller entrypoint ────────────────────────────────


class TestRunEntrypoint:
    def test_missing_key_returns_zero_result(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from config import settings as _settings

        monkeypatch.setattr(_settings, "FRED_API_KEY", "", raising=False)
        engine = MagicMock()
        result = run_refinery_cracks_puller(engine)
        assert result["fetched"] == 0
        assert result["inserted"] == 0
        assert set(result["series"].keys()) == set(REFINERY_SERIES.values())
        # Engine was never touched (no DB calls).
        engine.begin.assert_not_called()

    def test_happy_path_dispatches_pull_and_save(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from config import settings as _settings

        monkeypatch.setattr(_settings, "FRED_API_KEY", "fake-key", raising=False)

        fake_instance = MagicMock()
        fake_instance.pull.return_value = {
            "fetched_raw": 12,
            "crack_321_rows": 3,
            "per_series": {label: 2 for label in REFINERY_SERIES.values()},
        }
        fake_instance.save_to_db.return_value = 15

        with patch(
            "ingestion.altdata.refinery_cracks.RefineryCracksPuller",
            return_value=fake_instance,
        ) as puller_cls:
            engine = MagicMock()
            result = run_refinery_cracks_puller(engine)

        puller_cls.assert_called_once()
        fake_instance.pull.assert_called_once()
        fake_instance.save_to_db.assert_called_once()
        assert result["fetched"] == 12 + 3
        assert result["inserted"] == 15
        assert result["series"]["crack_321"] == 3
        assert result["series"]["util_pct"] == 2
