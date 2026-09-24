from __future__ import annotations

from datetime import date

import pytest

from paper_log.gex_levels.engine_adapter import DealerGammaAdapter, LevelsResult


class _FakeEngine:
    """A LevelsEngine test double — no database, no physics.dealer_gamma."""

    def __init__(self, result: dict) -> None:
        self._result = result
        self.calls: list[tuple[str, date | None]] = []

    def compute_gex_profile(self, ticker: str, snap_date: date | None = None) -> dict:
        self.calls.append((ticker, snap_date))
        return self._result


FULL_RESULT = {
    "ticker": "SPY", "spot": 555.0, "gamma_flip": 550.0, "put_wall": 540.0,
    "call_wall": 560.0, "gex_aggregate": 1_234_000.0, "gex_normalized": 0.6,
    "regime": "LONG_GAMMA",
}


def test_get_levels_passes_ticker_and_snap_date_through() -> None:
    engine = _FakeEngine(FULL_RESULT)
    adapter = DealerGammaAdapter(engine=engine)
    adapter.get_levels("SPY", date(2026, 9, 24))
    assert engine.calls == [("SPY", date(2026, 9, 24))]


def test_available_result_translates_all_fields() -> None:
    adapter = DealerGammaAdapter(engine=_FakeEngine(FULL_RESULT))
    result = adapter.get_levels("SPY", date(2026, 9, 24))

    assert result == LevelsResult(
        available=True, unavailable_reason=None,
        spot=555.0, spot_source="physics.dealer_gamma.DealerGammaEngine",
        gamma_flip=550.0, put_wall=540.0, call_wall=560.0,
        gex_aggregate=1_234_000.0, gex_normalized=0.6, regime="LONG_GAMMA",
        raw=FULL_RESULT,
    )


def test_error_result_is_unavailable() -> None:
    adapter = DealerGammaAdapter(engine=_FakeEngine({"error": "No options data for SPY", "ticker": "SPY"}))
    result = adapter.get_levels("SPY", date(2026, 9, 24))

    assert result.available is False
    assert result.unavailable_reason == "No options data for SPY"
    assert result.spot is None
    assert result.gamma_flip is None
    assert result.put_wall is None
    assert result.call_wall is None
    assert result.regime is None


def test_empty_result_is_unavailable() -> None:
    adapter = DealerGammaAdapter(engine=_FakeEngine({}))
    result = adapter.get_levels("SPY", date(2026, 9, 24))
    assert result.available is False
    assert result.unavailable_reason == "engine returned no result"


@pytest.mark.parametrize("missing_key", ["spot", "gamma_flip", "put_wall", "call_wall"])
def test_missing_any_required_key_is_engine_unavailable(missing_key: str) -> None:
    """"the engine returns no spot, flip or walls" -> engine_unavailable,
    regardless of which one is missing."""
    broken = dict(FULL_RESULT)
    broken[missing_key] = None
    adapter = DealerGammaAdapter(engine=_FakeEngine(broken))
    result = adapter.get_levels("SPY", date(2026, 9, 24))

    assert result.available is False
    assert missing_key in result.unavailable_reason


def test_missing_key_still_surfaces_whatever_was_present() -> None:
    """Partial results (e.g. spot came back but walls didn't) are still
    recorded for audit even though the session will be excluded."""
    broken = dict(FULL_RESULT)
    broken["put_wall"] = None
    broken["call_wall"] = None
    adapter = DealerGammaAdapter(engine=_FakeEngine(broken))
    result = adapter.get_levels("SPY", date(2026, 9, 24))

    assert result.available is False
    assert result.spot == 555.0
    assert result.gamma_flip == 550.0
    assert result.put_wall is None
    assert result.call_wall is None


def test_prefers_engine_provided_spot_source_when_present() -> None:
    """Forward-compatibility seam: if a future engine version (e.g. after
    the dealer-gamma sign/spot fix lands) adds its own spot_source/source
    key, the adapter surfaces that instead of the generic fallback label —
    without any code change here."""
    richer = dict(FULL_RESULT)
    richer["spot_source"] = "options_daily_signals.spot_price"
    adapter = DealerGammaAdapter(engine=_FakeEngine(richer))
    result = adapter.get_levels("SPY", date(2026, 9, 24))

    assert result.available is True
    assert result.spot_source == "options_daily_signals.spot_price"


def test_construction_requires_exactly_one_of_engine_or_db_engine() -> None:
    with pytest.raises(ValueError):
        DealerGammaAdapter()
    with pytest.raises(ValueError):
        DealerGammaAdapter(engine=_FakeEngine(FULL_RESULT), db_engine=object())
