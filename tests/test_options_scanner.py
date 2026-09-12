"""Tests for discovery/options_scanner.py — OptionsScanner and MispricingOpportunity.

All tests are pure unit tests with no database dependency.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

import discovery.options_scanner as scanner_module
from discovery.options_scanner import (
    MispricingOpportunity,
    OptionsScanner,
    _nullable_float,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_scanner(mock_engine: MagicMock) -> OptionsScanner:
    """Create an OptionsScanner with a mock engine."""
    return OptionsScanner(db_engine=mock_engine, lookback_days=252)


def _empty_history() -> pd.DataFrame:
    """Return an empty history DataFrame."""
    return pd.DataFrame()


def _make_opportunity(payoff: float = 50.0, **kwargs) -> MispricingOpportunity:
    """Create a MispricingOpportunity with sensible defaults."""
    defaults = dict(
        ticker="TEST",
        scan_date=date(2024, 1, 15),
        score=7.0,
        estimated_payoff_multiple=payoff,
        direction="CALL",
        thesis="Test thesis",
    )
    defaults.update(kwargs)
    return MispricingOpportunity(**defaults)


# ---------------------------------------------------------------------------
# Tests: MispricingOpportunity.is_100x
# ---------------------------------------------------------------------------

class TestMispricingOpportunity:
    """Tests for the MispricingOpportunity dataclass."""

    def test_mispricing_opportunity_is_100x(self) -> None:
        """Payoff >= 100 should flag is_100x as True."""
        opp = _make_opportunity(payoff=100.0)
        assert opp.is_100x is True

        opp2 = _make_opportunity(payoff=500.0)
        assert opp2.is_100x is True

    def test_mispricing_opportunity_not_100x(self) -> None:
        """Payoff < 100 should flag is_100x as False."""
        opp = _make_opportunity(payoff=99.9)
        assert opp.is_100x is False

        opp2 = _make_opportunity(payoff=0.0)
        assert opp2.is_100x is False


# ---------------------------------------------------------------------------
# Tests: _score_pcr
# ---------------------------------------------------------------------------

class TestScorePCR:
    """Tests for OptionsScanner._score_pcr."""

    def test_score_pcr_extreme_high(self, mock_engine: MagicMock) -> None:
        """High put/call ratio (2.0) -> positive score, direction CALL."""
        scanner = _make_scanner(mock_engine)
        current = {"put_call_ratio": 2.0}
        score, direction = scanner._score_pcr(current, _empty_history())
        assert score > 0
        assert direction == "CALL"

    def test_score_pcr_extreme_low(self, mock_engine: MagicMock) -> None:
        """Low put/call ratio (0.3) -> positive score, direction PUT."""
        scanner = _make_scanner(mock_engine)
        current = {"put_call_ratio": 0.3}
        score, direction = scanner._score_pcr(current, _empty_history())
        assert score > 0
        assert direction == "PUT"

    def test_score_pcr_normal(self, mock_engine: MagicMock) -> None:
        """Normal put/call ratio (1.0) -> score 0."""
        scanner = _make_scanner(mock_engine)
        current = {"put_call_ratio": 1.0}
        score, direction = scanner._score_pcr(current, _empty_history())
        assert score == 0


# ---------------------------------------------------------------------------
# Tests: _score_iv_skew
# ---------------------------------------------------------------------------

class TestScoreIVSkew:
    """Tests for OptionsScanner._score_iv_skew."""

    def test_score_iv_skew_extreme(self, mock_engine: MagicMock) -> None:
        """High IV skew (2.0) -> positive score, direction CALL."""
        scanner = _make_scanner(mock_engine)
        current = {"iv_skew": 2.0}
        score, direction = scanner._score_iv_skew(current, _empty_history())
        assert score > 0
        assert direction == "CALL"

    def test_score_iv_skew_collapsed(self, mock_engine: MagicMock) -> None:
        """Collapsed IV skew (0.7) -> positive score, direction PUT."""
        scanner = _make_scanner(mock_engine)
        current = {"iv_skew": 0.7}
        score, direction = scanner._score_iv_skew(current, _empty_history())
        assert score > 0
        assert direction == "PUT"


# ---------------------------------------------------------------------------
# Tests: _score_max_pain_divergence
# ---------------------------------------------------------------------------

class TestScoreMaxPainDivergence:
    """Tests for OptionsScanner._score_max_pain_divergence."""

    def test_score_max_pain_divergence_above(self, mock_engine: MagicMock) -> None:
        """Spot above max pain -> direction PUT (expect pullback)."""
        scanner = _make_scanner(mock_engine)
        current = {"spot_price": 110.0, "max_pain": 100.0}
        score, direction = scanner._score_max_pain_divergence(current)
        assert score > 0
        assert direction == "PUT"

    def test_score_max_pain_divergence_below(self, mock_engine: MagicMock) -> None:
        """Spot below max pain -> direction CALL (expect rally)."""
        scanner = _make_scanner(mock_engine)
        current = {"spot_price": 90.0, "max_pain": 100.0}
        score, direction = scanner._score_max_pain_divergence(current)
        assert score > 0
        assert direction == "CALL"

    def test_score_max_pain_no_divergence(self, mock_engine: MagicMock) -> None:
        """Spot at max pain -> score 0."""
        scanner = _make_scanner(mock_engine)
        current = {"spot_price": 100.0, "max_pain": 100.0}
        score, direction = scanner._score_max_pain_divergence(current)
        assert score == 0


# ---------------------------------------------------------------------------
# Tests: _score_term_structure
# ---------------------------------------------------------------------------

class TestScoreTermStructure:
    """Tests for OptionsScanner._score_term_structure."""

    def test_score_term_structure_inverted(self, mock_engine: MagicMock) -> None:
        """Negative slope (-0.1) -> positive score."""
        scanner = _make_scanner(mock_engine)
        current = {"term_structure_slope": -0.1}
        score, direction = scanner._score_term_structure(current)
        assert score > 0


# ---------------------------------------------------------------------------
# Tests: _score_oi_concentration
# ---------------------------------------------------------------------------

class TestScoreOIConcentration:
    """Tests for OptionsScanner._score_oi_concentration."""

    def test_score_oi_concentration(self, mock_engine: MagicMock) -> None:
        """Concentration of 0.25 (above threshold 0.15) -> positive score."""
        scanner = _make_scanner(mock_engine)
        current = {"oi_concentration": 0.25}
        score = scanner._score_oi_concentration(current)
        assert score > 0


# ---------------------------------------------------------------------------
# Tests: _score_gamma_squeeze
# ---------------------------------------------------------------------------

class TestScoreGammaSqueeze:
    """Tests for OptionsScanner._score_gamma_squeeze."""

    def test_score_gamma_squeeze(self, mock_engine: MagicMock) -> None:
        """High concentration + high divergence + high OI -> positive score."""
        scanner = _make_scanner(mock_engine)
        current = {
            "oi_concentration": 0.20,
            "spot_price": 110.0,
            "max_pain": 100.0,
            "total_oi": 100000,
        }
        score = scanner._score_gamma_squeeze(current)
        assert score > 0


# ---------------------------------------------------------------------------
# Tests: _estimate_payoff_multiple
# ---------------------------------------------------------------------------

class TestEstimatePayoffMultiple:
    """Tests for OptionsScanner._estimate_payoff_multiple."""

    def test_estimate_payoff_multiple(self, mock_engine: MagicMock) -> None:
        """Should return a positive number."""
        scanner = _make_scanner(mock_engine)
        current = {
            "iv_atm": 0.25,
            "spot_price": 100.0,
            "max_pain": 90.0,
        }
        payoff = scanner._estimate_payoff_multiple(current, 7.0, "CALL")
        assert payoff > 0


# ---------------------------------------------------------------------------
# Tests: _build_thesis
# ---------------------------------------------------------------------------

class TestBuildThesis:
    """Tests for OptionsScanner._build_thesis."""

    def test_build_thesis_includes_active_signals(
        self, mock_engine: MagicMock
    ) -> None:
        """Thesis string should be non-empty when active signals exist."""
        scanner = _make_scanner(mock_engine)
        signals = {
            "pcr": {"score": 8, "direction": "CALL", "value": 2.1},
            "iv_skew": {"score": 2, "direction": "", "value": 1.0},
            "max_pain_div": {"score": 0, "direction": "", "value": None},
            "term_structure": {"score": 0, "direction": "", "value": None},
            "oi_concentration": {"score": 0, "value": None},
            "iv_percentile": {"score": 0, "direction": "", "value": None},
            "gamma_squeeze": {"score": 0},
        }
        current = {"spot_price": 100.0, "max_pain": 95.0}
        thesis = scanner._build_thesis("SPY", signals, "CALL", current)
        assert len(thesis) > 0
        assert "SPY" in thesis


# ---------------------------------------------------------------------------
# Tests: format_report
# ---------------------------------------------------------------------------

class TestFormatReport:
    """Tests for OptionsScanner.format_report."""

    def test_format_report_empty(self, mock_engine: MagicMock) -> None:
        """Empty opportunities list produces the 'No mispricing' message."""
        scanner = _make_scanner(mock_engine)
        result = scanner.format_report([])
        assert result == "No mispricing opportunities found."

    def test_format_report_with_opportunities(
        self, mock_engine: MagicMock
    ) -> None:
        """Report should contain the ticker and score for each opportunity."""
        scanner = _make_scanner(mock_engine)
        opp = _make_opportunity(
            payoff=150.0,
            ticker="AAPL",
            score=8.5,
            spot_price=180.0,
            iv_atm=0.30,
        )
        report = scanner.format_report([opp])
        assert "AAPL" in report
        assert "8.5" in report
        assert "150x" in report


# ---------------------------------------------------------------------------
# Tests: NULL options_daily_signals columns (regression for the 2026-09-11
# "Options mispricing scan failed: float() argument must be a string or a
# real number, not 'NoneType'" failure).
#
# #436 unioned the catalyst calendar into ingestion/options.py's ticker list,
# taking options_daily_signals from 41 mega caps to 176 rows. 65 of the new
# rows carry iv_atm = NULL because _compute_atm_iv() finds no contract inside
# the +/-2% ATM band of a thin small-cap chain. _scan_ticker built the
# opportunity with `current.get("iv_atm", 0)` — a dead default, because the
# key is always present with a value of None — and persist_scan then called
# float(None), aborting the whole transaction so the scan persisted nothing.
# ---------------------------------------------------------------------------

def _signal_row(**overrides: object) -> dict:
    """A full _get_current_signals() dict — every key present, as in prod."""
    row: dict = {
        "put_call_ratio": 2.4,       # extreme -> scores
        "max_pain": 80.0,
        "iv_skew": 1.9,              # extreme -> scores
        "total_oi": 250_000,
        "total_volume": 40_000,
        "near_expiry": date(2026, 10, 16),
        "spot_price": 100.0,
        "iv_atm": 0.42,
        "iv_25d_put": 0.51,
        "iv_25d_call": 0.38,
        "term_structure_slope": -0.20,
        "oi_concentration": 0.30,
    }
    row.update(overrides)
    return row


def _stub_external_signals(scanner: OptionsScanner) -> OptionsScanner:
    """Stub the two signals that open their own engines/network paths."""
    scanner._score_vol_surface = MagicMock(return_value=(0.0, "", {}))  # type: ignore[method-assign]
    scanner._score_dealer_gamma_extras = MagicMock(  # type: ignore[method-assign]
        return_value=(0.0, "", 0.0, "", {})
    )
    return scanner


def _scanner_with_row(
    row: dict | None, history: pd.DataFrame | None = None
) -> OptionsScanner:
    """Build a scanner whose signal loaders return ``row`` / ``history``."""
    scanner = _make_scanner(MagicMock())
    scanner._get_current_signals = MagicMock(return_value=row)  # type: ignore[method-assign]
    scanner._get_signal_history = MagicMock(  # type: ignore[method-assign]
        return_value=history if history is not None else pd.DataFrame()
    )
    return _stub_external_signals(scanner)


def _write_engine() -> tuple[MagicMock, MagicMock]:
    """Return (engine, connection) where ``engine.begin()`` yields the conn."""
    conn = MagicMock()
    engine = MagicMock()
    engine.begin.return_value.__enter__ = MagicMock(return_value=conn)
    engine.begin.return_value.__exit__ = MagicMock(return_value=False)
    return engine, conn


def _insert_params(conn: MagicMock) -> list[dict]:
    """Every parameter dict bound to an execute() on the mock connection."""
    return [
        call.args[1]
        for call in conn.execute.call_args_list
        if len(call.args) > 1 and isinstance(call.args[1], dict)
    ]


class TestNullSignalColumns:
    """NULL columns in options_daily_signals must not take the scan down."""

    def test_scan_ticker_survives_null_iv_atm(self) -> None:
        """A thin chain with iv_atm NULL still yields a scored opportunity."""
        scanner = _scanner_with_row(_signal_row(iv_atm=None))
        opps = scanner._scan_ticker("OLMA", date(2026, 9, 11))
        assert len(opps) == 1
        # Preserved as None — NOT coerced to 0.0, which would read downstream
        # as "this name has zero implied volatility".
        assert opps[0].iv_atm is None
        assert opps[0].spot_price == 100.0

    def test_persist_scan_writes_null_for_missing_iv_atm(self) -> None:
        """persist_scan binds SQL NULL instead of raising on float(None).

        This is the exact expression that failed in production.
        """
        engine, conn = _write_engine()
        scanner = OptionsScanner(db_engine=engine)
        opp = _make_opportunity(
            ticker="OLMA",
            spot_price=10.22,
            iv_atm=None,
            strikes=[11.24, 11.75, 12.26],
            expiry="2026-10-16",
        )

        assert scanner.persist_scan([opp]) == 1

        params = _insert_params(conn)
        assert params, "no parameterised INSERT was issued"
        assert params[-1]["iv"] is None
        assert params[-1]["spot"] == 10.22

    def test_persist_scan_writes_every_row_when_one_lacks_iv(self) -> None:
        """One NULL-IV row must not stop the rows around it being written."""
        engine, conn = _write_engine()
        scanner = OptionsScanner(db_engine=engine)
        opps = [
            _make_opportunity(ticker="AAPL", spot_price=180.0, iv_atm=0.30),
            _make_opportunity(ticker="OLMA", spot_price=10.22, iv_atm=None),
            _make_opportunity(ticker="TECX", spot_price=29.05, iv_atm=0.55),
        ]
        assert scanner.persist_scan(opps) == 3
        ivs = [p["iv"] for p in _insert_params(conn)]
        assert ivs == [0.30, None, 0.55]

    def test_persist_scan_nulls_nan_iv_atm(self) -> None:
        """A NaN IV (empty pandas slice upstream) is written as NULL too."""
        engine, conn = _write_engine()
        scanner = OptionsScanner(db_engine=engine)
        opp = _make_opportunity(
            ticker="OLMA", spot_price=10.22, iv_atm=float("nan")
        )
        scanner.persist_scan([opp])
        assert _insert_params(conn)[-1]["iv"] is None

    def test_scan_ticker_skips_null_spot_price(self) -> None:
        """No spot price -> no strikes, no payoff anchor -> skip the ticker."""
        scanner = _scanner_with_row(_signal_row(spot_price=None))
        assert scanner._scan_ticker("ABOS", date(2026, 9, 11)) == []

    def test_scan_ticker_skips_zero_spot_price(self) -> None:
        """A zero spot is as unusable as a NULL one."""
        scanner = _scanner_with_row(_signal_row(spot_price=0.0))
        assert scanner._scan_ticker("ABOS", date(2026, 9, 11)) == []

    def test_scan_ticker_null_near_expiry_is_empty_not_literal_none(self) -> None:
        """str(None) would reach a DATE column as the string 'None'."""
        scanner = _scanner_with_row(_signal_row(near_expiry=None))
        opps = scanner._scan_ticker("OLMA", date(2026, 9, 11))
        assert len(opps) == 1
        assert opps[0].expiry == ""

        engine, conn = _write_engine()
        OptionsScanner(db_engine=engine).persist_scan(opps)
        assert _insert_params(conn)[-1]["expiry"] is None

    def test_scan_ticker_empty_chain_returns_nothing(self) -> None:
        """No options_daily_signals row at all -> no opportunity, no crash."""
        scanner = _scanner_with_row(None)
        assert scanner._scan_ticker("NOSUCH", date(2026, 9, 11)) == []

    def test_scan_all_then_persist_over_null_iv_universe(self) -> None:
        """scan_all + persist_scan across a NULL-IV universe completes.

        Before the fix this raised TypeError inside persist_scan's
        transaction, so every opportunity in the batch was rolled back and the
        scheduler logged "Options mispricing scan failed" with nothing stored.
        """
        engine, conn = _write_engine()
        scanner = OptionsScanner(db_engine=engine)
        scanner._get_available_tickers = MagicMock(  # type: ignore[method-assign]
            return_value=["OLMA", "TECX"]
        )
        scanner._get_current_signals = MagicMock(  # type: ignore[method-assign]
            return_value=_signal_row(iv_atm=None)
        )
        scanner._get_signal_history = MagicMock(  # type: ignore[method-assign]
            return_value=pd.DataFrame()
        )
        _stub_external_signals(scanner)

        opps = scanner.scan_all(scan_date=date(2026, 9, 11), min_score=1.0)
        assert len(opps) == 2
        assert all(o.iv_atm is None for o in opps)
        assert scanner.persist_scan(opps) == 2


class TestFieldCoverageWarning:
    """A per-ticker gap stays quiet; a whole-source gap is surfaced."""

    def _warnings(
        self, scanner: OptionsScanner, coverage: dict
    ) -> list[tuple[str, dict]]:
        """Capture (template, kwargs) — loguru formats lazily, so the raw
        message is still the ``{}`` template when the sink sees it."""
        emitted: list[tuple[str, dict]] = []
        with patch.object(
            scanner_module.log, "warning",
            lambda msg, **kw: emitted.append((msg, kw)),
        ):
            scanner._warn_on_field_coverage(coverage, date(2026, 9, 11))
        return emitted

    def test_systematic_gap_warns(self, mock_engine: MagicMock) -> None:
        """Every scanned ticker missing iv_atm is a data-source problem."""
        scanner = _make_scanner(mock_engine)
        emitted = self._warnings(
            scanner, {"scanned": 176, "no_iv_atm": 176, "no_spot": 0}
        )
        assert len(emitted) == 1
        template, kwargs = emitted[0]
        assert kwargs["f"] == "iv_atm"
        assert (kwargs["n"], kwargs["t"]) == (176, 176)
        assert "data " in template
        # It must render — a loguru template with a missing key logs nothing.
        assert "iv_atm" in template.format(**kwargs)

    def test_systematic_spot_gap_warns(self, mock_engine: MagicMock) -> None:
        """spot_price missing everywhere is surfaced the same way."""
        scanner = _make_scanner(mock_engine)
        emitted = self._warnings(
            scanner, {"scanned": 100, "no_iv_atm": 0, "no_spot": 100}
        )
        assert [kw["f"] for _, kw in emitted] == ["spot_price"]

    def test_thin_chain_minority_does_not_warn(self, mock_engine: MagicMock) -> None:
        """65/176 missing is thin chains, not an outage — info, not warning."""
        scanner = _make_scanner(mock_engine)
        assert self._warnings(
            scanner, {"scanned": 176, "no_iv_atm": 65, "no_spot": 0}
        ) == []

    def test_full_coverage_is_silent(self, mock_engine: MagicMock) -> None:
        """Nothing missing says nothing at all."""
        scanner = _make_scanner(mock_engine)
        assert self._warnings(
            scanner, {"scanned": 41, "no_iv_atm": 0, "no_spot": 0}
        ) == []

    def test_empty_universe_is_silent(self, mock_engine: MagicMock) -> None:
        """Nothing scanned -> no division by zero, no message."""
        scanner = _make_scanner(mock_engine)
        assert self._warnings(
            scanner, {"scanned": 0, "no_iv_atm": 0, "no_spot": 0}
        ) == []

    def test_scan_ticker_fills_the_tally(self, mock_engine: MagicMock) -> None:
        """The tally the coverage check reads is filled by _scan_ticker."""
        scanner = _make_scanner(mock_engine)
        scanner._get_signal_history = MagicMock(  # type: ignore[method-assign]
            return_value=pd.DataFrame()
        )
        _stub_external_signals(scanner)
        rows = {
            "GOOD": _signal_row(),
            "NOIV": _signal_row(iv_atm=None),
            "NOSPOT": _signal_row(spot_price=None),
        }
        scanner._get_current_signals = MagicMock(  # type: ignore[method-assign]
            side_effect=lambda t, d: rows[t]
        )

        coverage: dict[str, int] = {"scanned": 0, "no_spot": 0, "no_iv_atm": 0}
        for ticker in rows:
            scanner._scan_ticker(ticker, date(2026, 9, 11), coverage)

        assert coverage == {"scanned": 3, "no_spot": 1, "no_iv_atm": 1}


class TestNullableFloat:
    """The coercion helper that replaced the bare float() calls."""

    def test_none_stays_none(self) -> None:
        assert _nullable_float(None) is None

    def test_nan_becomes_none(self) -> None:
        assert _nullable_float(float("nan")) is None

    def test_numbers_become_native_floats(self) -> None:
        out = _nullable_float(np.float64(0.42))
        assert out == pytest.approx(0.42)
        assert type(out) is float

    def test_numeric_string_is_accepted(self) -> None:
        assert _nullable_float("10.22") == pytest.approx(10.22)

    def test_garbage_becomes_none(self) -> None:
        assert _nullable_float("N/A") is None
