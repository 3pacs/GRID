"""Unit tests for ingestion/altdata/sge_premium.py."""

from __future__ import annotations

import sys
import types
from dataclasses import FrozenInstanceError
from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from ingestion.altdata.sge_premium import (
    AKSHARE_FX_FUNCTIONS,
    AKSHARE_LONDON_FUNCTIONS,
    AKSHARE_SGE_FUNCTIONS,
    GRAMS_PER_TROY_OZ,
    GoldSpotSnapshot,
    PREMIUM_DISCOUNT_THRESHOLD_USD,
    PREMIUM_DISTRESS_THRESHOLD_USD,
    SEVERITY_DISCOUNT,
    SEVERITY_DISTRESS,
    SEVERITY_ELEVATED,
    SEVERITY_NEUTRAL,
    SGEPremiumPuller,
    _load_akshare_function,
    classify_premium,
    cny_per_gram_to_usd_per_oz,
    run_sge_premium_puller,
)


# ---------------------------------------------------------------------------
# Mock engine helper
# ---------------------------------------------------------------------------


def _build_mock_engine(source_id: int = 7) -> tuple[MagicMock, MagicMock, list[dict]]:
    """Build a mock SQLAlchemy engine that records every INSERT.

    Returns ``(engine, conn, inserted_rows)`` where ``inserted_rows`` is a
    growing list of every ``params`` dict passed to an INSERT statement.
    """
    engine = MagicMock()
    conn = MagicMock()
    inserted_rows: list[dict] = []

    # In-memory store of (series_id, obs_date) so dedup works.
    existing: set[tuple[str, object]] = set()

    def _execute(stmt, params=None):
        result = MagicMock()
        sql_text = str(stmt)
        if "FROM source_catalog" in sql_text:
            result.fetchone.return_value = (source_id,)
        elif sql_text.startswith("\nSELECT 1 FROM raw_series") or "SELECT 1 FROM raw_series" in sql_text:
            key = (params["sid"], params["od"]) if params else None
            if key in existing:
                result.fetchone.return_value = (1,)
            else:
                result.fetchone.return_value = None
        elif "INSERT INTO raw_series" in sql_text:
            inserted_rows.append(dict(params) if params else {})
            if params:
                existing.add((params["sid"], params["od"]))
            result.fetchone.return_value = None
        elif "SELECT DISTINCT obs_date" in sql_text:
            result.fetchall.return_value = []
        else:
            result.fetchone.return_value = None
            result.fetchall.return_value = []
        return result

    conn.execute.side_effect = _execute

    engine.connect.return_value.__enter__ = MagicMock(return_value=conn)
    engine.connect.return_value.__exit__ = MagicMock(return_value=False)
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)

    return engine, conn, inserted_rows


# ---------------------------------------------------------------------------
# Akshare stub helper
# ---------------------------------------------------------------------------


def _install_fake_akshare(monkeypatch, **funcs) -> types.ModuleType:
    """Install a fake ``akshare`` module exposing only the given functions."""
    fake = types.ModuleType("akshare")
    for name, fn in funcs.items():
        setattr(fake, name, fn)
    monkeypatch.setitem(sys.modules, "akshare", fake)
    return fake


def _sge_df(price: float = 605.0, dt: str = "2026-04-13") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["au9999"],
            "name": ["黄金9999"],
            "price": [price],
            "date": [dt],
        }
    )


def _london_df(price: float = 2350.0, dt: str = "2026-04-12") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["XAUUSD"],
            "name": ["London Gold"],
            "price": [price],
            "date": [dt],
        }
    )


def _fx_df(rate: float = 7.20, dt: str = "2026-04-13") -> pd.DataFrame:
    return pd.DataFrame(
        {
            "symbol": ["USDCNY"],
            "name": ["美元人民币"],
            "price": [rate],
            "date": [dt],
        }
    )


# ===========================================================================
# 1. GoldSpotSnapshot dataclass
# ===========================================================================


class TestGoldSpotSnapshot:
    def test_frozen_roundtrip(self):
        snap = GoldSpotSnapshot(
            date=date(2026, 4, 13),
            sge_cny_per_gram=605.0,
            sge_usd_per_oz=2613.5,
            london_usd_per_oz=2580.0,
            usdcny=7.20,
            premium_usd=33.5,
            premium_severity=SEVERITY_DISTRESS,
        )
        assert snap.date == date(2026, 4, 13)
        assert snap.sge_cny_per_gram == 605.0
        assert snap.premium_severity == SEVERITY_DISTRESS

        # Frozen: any mutation must raise.
        with pytest.raises(FrozenInstanceError):
            snap.premium_usd = 0.0  # type: ignore[misc]


# ===========================================================================
# 2. cny_per_gram_to_usd_per_oz
# ===========================================================================


class TestUnitConversion:
    def test_happy_path(self):
        # 600 CNY/g at 7.2 USDCNY -> ~2592 USD/oz
        result = cny_per_gram_to_usd_per_oz(600.0, 7.2)
        expected = (600.0 / 7.2) * GRAMS_PER_TROY_OZ
        assert result == pytest.approx(expected)
        assert result == pytest.approx(2591.96, abs=0.5)

    def test_zero_usdcny_returns_zero(self):
        # Defensive guard — must NOT raise ZeroDivisionError.
        assert cny_per_gram_to_usd_per_oz(600.0, 0.0) == 0.0

    def test_negative_cny_flows_through(self):
        result = cny_per_gram_to_usd_per_oz(-100.0, 7.0)
        assert result < 0
        assert result == pytest.approx((-100.0 / 7.0) * GRAMS_PER_TROY_OZ)

    def test_formula_matches_constant(self):
        # Explicit formula check: (cny/gram) / usdcny * 31.1035
        cny, fx = 555.55, 6.85
        result = cny_per_gram_to_usd_per_oz(cny, fx)
        assert result == pytest.approx((cny / fx) * 31.1035, rel=1e-9)


# ===========================================================================
# 3. classify_premium
# ===========================================================================


class TestClassifyPremium:
    def test_distress_above_threshold(self):
        assert classify_premium(25.0) == SEVERITY_DISTRESS
        assert classify_premium(PREMIUM_DISTRESS_THRESHOLD_USD + 0.01) == SEVERITY_DISTRESS

    def test_elevated_between_zero_and_distress(self):
        assert classify_premium(10.0) == SEVERITY_ELEVATED
        assert classify_premium(0.5) == SEVERITY_ELEVATED

    def test_neutral_around_zero(self):
        assert classify_premium(0.0) == SEVERITY_NEUTRAL
        assert classify_premium(-5.0) == SEVERITY_NEUTRAL
        assert classify_premium(PREMIUM_DISCOUNT_THRESHOLD_USD) == SEVERITY_NEUTRAL

    def test_discount_below_threshold(self):
        assert classify_premium(-15.0) == SEVERITY_DISCOUNT
        assert classify_premium(PREMIUM_DISCOUNT_THRESHOLD_USD - 0.01) == SEVERITY_DISCOUNT


# ===========================================================================
# 4. _load_akshare_function
# ===========================================================================


class TestLoadAkshareFunction:
    def test_import_error_returns_none(self, monkeypatch):
        # Simulate akshare not installed.
        monkeypatch.setitem(sys.modules, "akshare", None)
        with patch.dict(sys.modules, {"akshare": None}):
            result = _load_akshare_function("anything")
        assert result is None

    def test_missing_attribute_returns_none(self, monkeypatch):
        _install_fake_akshare(monkeypatch)  # empty module, no attrs
        result = _load_akshare_function("does_not_exist")
        assert result is None

    def test_callable_found_returns_callable(self, monkeypatch):
        sentinel_called = {"yes": False}

        def fake_fn():
            sentinel_called["yes"] = True
            return "ok"

        _install_fake_akshare(monkeypatch, spot_price_qh_em=fake_fn)
        result = _load_akshare_function("spot_price_qh_em")
        assert callable(result)
        assert result() == "ok"
        assert sentinel_called["yes"] is True


# ===========================================================================
# 5. run_sge_premium_puller — happy path
# ===========================================================================


class TestRunPullerHappyPath:
    def test_all_legs_present(self, monkeypatch):
        _install_fake_akshare(
            monkeypatch,
            spot_price_qh_em=lambda: _sge_df(price=605.0),
            futures_global_indicator=lambda: _london_df(price=2580.0),
            currency_latest=lambda: _fx_df(rate=7.20),
        )
        engine, _conn, inserted = _build_mock_engine()

        summary = run_sge_premium_puller(engine)

        assert summary["fetched"] == 1
        assert summary["inserted"] == 5  # all five sge:* series written
        assert summary["source"] == "akshare"
        assert summary["latest_premium_usd"] is not None

        # The premium = sge_usd_per_oz - london = (605/7.2)*31.1035 - 2580
        expected_sge_usd = (605.0 / 7.20) * GRAMS_PER_TROY_OZ
        expected_premium = expected_sge_usd - 2580.0
        assert summary["latest_premium_usd"] == pytest.approx(expected_premium, abs=0.01)
        # 605/7.2*31.1035 - 2580 ≈ 33.6 USD/oz, well above the +20 distress line
        assert summary["latest_severity"] == SEVERITY_DISTRESS


# ===========================================================================
# 6. Missing SGE data (empty DataFrame from every probe)
# ===========================================================================


class TestMissingSGE:
    def test_empty_sge_returns_zero_rows(self, monkeypatch):
        # London + FX present, SGE every probe empty.
        _install_fake_akshare(
            monkeypatch,
            spot_price_qh_em=lambda: pd.DataFrame(),
            futures_global_indicator=lambda: _london_df(),
            currency_latest=lambda: _fx_df(),
        )
        engine, _conn, inserted = _build_mock_engine()

        summary = run_sge_premium_puller(engine)

        # Snapshot is still composed (London + FX present, SGE None).
        assert summary["fetched"] == 1
        # SGE leg None -> sge_usd_per_oz None -> premium None.
        assert summary["latest_premium_usd"] is None
        # Two series should still land: london + fx.
        sids = {row["sid"] for row in inserted}
        assert "sge:london_usd_per_oz" in sids
        assert "sge:usdcny_fx" in sids
        assert "sge:cny_per_gram" not in sids
        assert "sge:premium_usd_per_oz" not in sids


# ===========================================================================
# 7. Missing London data
# ===========================================================================


class TestMissingLondon:
    def test_sge_written_premium_null(self, monkeypatch):
        _install_fake_akshare(
            monkeypatch,
            spot_price_qh_em=lambda: _sge_df(price=600.0),
            futures_global_indicator=lambda: pd.DataFrame(),
            currency_latest=lambda: _fx_df(rate=7.0),
        )
        engine, _conn, inserted = _build_mock_engine()

        summary = run_sge_premium_puller(engine)

        assert summary["fetched"] == 1
        assert summary["latest_premium_usd"] is None  # cannot compute basis
        sids = {row["sid"] for row in inserted}
        assert "sge:cny_per_gram" in sids
        assert "sge:usd_per_oz" in sids  # SGE + FX both available -> USD-per-oz computed
        assert "sge:usdcny_fx" in sids
        assert "sge:london_usd_per_oz" not in sids
        assert "sge:premium_usd_per_oz" not in sids


# ===========================================================================
# 8. Missing USDCNY
# ===========================================================================


class TestMissingFX:
    def test_sge_in_cny_only(self, monkeypatch):
        _install_fake_akshare(
            monkeypatch,
            spot_price_qh_em=lambda: _sge_df(price=600.0),
            futures_global_indicator=lambda: _london_df(price=2400.0),
            currency_latest=lambda: pd.DataFrame(),
        )
        engine, _conn, inserted = _build_mock_engine()

        summary = run_sge_premium_puller(engine)

        assert summary["fetched"] == 1
        assert summary["latest_premium_usd"] is None  # no FX -> no USD conversion
        sids = {row["sid"] for row in inserted}
        assert "sge:cny_per_gram" in sids
        assert "sge:london_usd_per_oz" in sids
        assert "sge:usd_per_oz" not in sids  # cannot convert without FX
        assert "sge:usdcny_fx" not in sids
        assert "sge:premium_usd_per_oz" not in sids


# ===========================================================================
# 9. Partial-success: SGE ok + London ok + FX function raises
# ===========================================================================


class TestPartialSuccess:
    def test_fx_function_raises(self, monkeypatch):
        def boom():
            raise RuntimeError("FX upstream 503")

        _install_fake_akshare(
            monkeypatch,
            spot_price_qh_em=lambda: _sge_df(price=600.0),
            futures_global_indicator=lambda: _london_df(price=2400.0),
            currency_latest=boom,
        )
        engine, _conn, inserted = _build_mock_engine()

        summary = run_sge_premium_puller(engine)
        # Did not crash, snapshot still emitted.
        assert summary["fetched"] == 1
        assert summary["latest_premium_usd"] is None
        sids = {row["sid"] for row in inserted}
        assert "sge:cny_per_gram" in sids
        assert "sge:london_usd_per_oz" in sids
        assert "sge:usd_per_oz" not in sids
        assert "sge:premium_usd_per_oz" not in sids


# ===========================================================================
# 10. Idempotent re-run on same date
# ===========================================================================


class TestIdempotent:
    def test_double_run_no_dupes(self, monkeypatch):
        _install_fake_akshare(
            monkeypatch,
            spot_price_qh_em=lambda: _sge_df(price=605.0),
            futures_global_indicator=lambda: _london_df(price=2580.0),
            currency_latest=lambda: _fx_df(rate=7.20),
        )
        engine, _conn, inserted = _build_mock_engine()

        first = run_sge_premium_puller(engine)
        # Second run: dedup table already has every (sid, od) pair; no new
        # INSERTs should be issued.
        before = len(inserted)
        second = run_sge_premium_puller(engine)
        after = len(inserted)

        assert first["inserted"] == 5
        assert second["inserted"] == 0
        assert after == before  # nothing new added


# ===========================================================================
# 11. classify_premium + cny_per_gram_to_usd_per_oz compose end-to-end
# ===========================================================================


class TestCompose:
    def test_end_to_end_compose(self):
        # SGE 660 CNY/g, FX 7.0, London 2400 USD/oz.
        sge_usd = cny_per_gram_to_usd_per_oz(660.0, 7.0)
        premium = sge_usd - 2400.0
        severity = classify_premium(premium)
        # 660/7 = 94.2857, * 31.1035 ≈ 2932.18 -> premium ~532, distress
        assert sge_usd == pytest.approx(2932.18, abs=0.5)
        assert premium > PREMIUM_DISTRESS_THRESHOLD_USD
        assert severity == SEVERITY_DISTRESS

    def test_end_to_end_neutral(self):
        # 540 CNY/g at 7.2 -> ~2333 USD/oz. London at 2335 -> -2 -> neutral.
        sge_usd = cny_per_gram_to_usd_per_oz(540.0, 7.2)
        premium = sge_usd - 2335.0
        assert -10 < premium < 0  # within neutral band
        assert classify_premium(premium) == SEVERITY_NEUTRAL


# ===========================================================================
# 12. Namespace contract: all 5 sge:* series_ids present when all legs ok
# ===========================================================================


class TestNamespaceContract:
    def test_all_five_series_ids(self, monkeypatch):
        _install_fake_akshare(
            monkeypatch,
            spot_price_qh_em=lambda: _sge_df(price=620.0),
            futures_global_indicator=lambda: _london_df(price=2600.0),
            currency_latest=lambda: _fx_df(rate=7.10),
        )
        engine, _conn, inserted = _build_mock_engine()

        run_sge_premium_puller(engine)

        sids = {row["sid"] for row in inserted}
        assert sids == {
            "sge:cny_per_gram",
            "sge:usd_per_oz",
            "sge:london_usd_per_oz",
            "sge:usdcny_fx",
            "sge:premium_usd_per_oz",
        }

    def test_constants_are_correct(self):
        # Sanity: probe ladders are non-empty tuples in the expected order.
        assert isinstance(AKSHARE_SGE_FUNCTIONS, tuple)
        assert isinstance(AKSHARE_LONDON_FUNCTIONS, tuple)
        assert isinstance(AKSHARE_FX_FUNCTIONS, tuple)
        assert AKSHARE_SGE_FUNCTIONS[0] == "spot_price_qh_em"
        assert "futures_global_indicator" in AKSHARE_SGE_FUNCTIONS
        assert "futures_global_indicator" in AKSHARE_LONDON_FUNCTIONS
        assert "currency_latest" in AKSHARE_FX_FUNCTIONS
        assert PREMIUM_DISTRESS_THRESHOLD_USD == 20.0
        assert PREMIUM_DISCOUNT_THRESHOLD_USD == -10.0
        assert GRAMS_PER_TROY_OZ == 31.1035


# ===========================================================================
# 13. Puller class metadata
# ===========================================================================


class TestPullerMetadata:
    def test_source_name(self):
        assert SGEPremiumPuller.SOURCE_NAME == "sge_premium"

    def test_source_config_free(self):
        assert SGEPremiumPuller.SOURCE_CONFIG["cost_tier"] == "FREE"

    def test_inherits_base_puller(self):
        from ingestion.base import BasePuller
        assert issubclass(SGEPremiumPuller, BasePuller)
