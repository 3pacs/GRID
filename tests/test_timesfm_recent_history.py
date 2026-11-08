"""TimesFM forecasts are anchored on recent history (2026-11-07).

run_timesfm_forecast_cycle read ``ORDER BY obs_date ASC LIMIT 2048`` over
YF:{t}:adj_close and YF:{t}:close together -- the OLDEST rows. CORT's series
ended 2012-05-30 at $4.02, so the oracle predicted CORT $72.77 -> $4.04 in a
week (clamped to -30%). History now comes from store.observations (newest
dates, one source), and a forecast that does not start near today's price
produces no prediction.
"""

from __future__ import annotations

from datetime import date, timedelta
from unittest.mock import MagicMock

import numpy as np

from oracle import forecaster_adapter as fa
from store import observations
from store.observations import Observation

TODAY = date(2026, 11, 7)


def _obs(source: str, last: date, n: int, start_value: float = 50.0) -> list[Observation]:
    """Newest-first, like read_latest_n."""
    days = [last - timedelta(days=i) for i in range(n)]
    return [Observation(series_id="x", obs_date=d, value=start_value + i, source=source)
            for i, d in enumerate(days)]


def test_history_is_the_newest_dates_oldest_first(monkeypatch) -> None:
    calls = []

    def fake(conn, sid, n, *, source=None, start=None, **kw):  # noqa: ANN001
        calls.append((sid, n, source, start))
        return _obs(source, TODAY - timedelta(days=1), 40) if source == "TIINGO" else []

    monkeypatch.setattr(observations, "read_latest_n", fake)
    hist, src = fa.recent_close_history(object(), "CORT", today=TODAY)
    assert src == "TIINGO"
    assert calls[0][0] == "YF:CORT:adj_close" and calls[0][2] == "TIINGO"
    assert calls[0][3] > TODAY - timedelta(days=3 * 365 * 2), "the read is bounded"
    # oldest first, ending at the newest close
    assert hist[0] == 89.0 and hist[-1] == 50.0 and len(hist) == 40
    assert hist.dtype == np.float32


def test_stale_source_falls_back_then_skips(monkeypatch) -> None:
    def fake(conn, sid, n, *, source=None, start=None, **kw):  # noqa: ANN001
        if source == "TIINGO":
            return _obs(source, TODAY - timedelta(days=60), 40)   # stale
        return _obs(source, TODAY - timedelta(days=2), 40)        # yfinance fresh

    monkeypatch.setattr(observations, "read_latest_n", fake)
    hist, src = fa.recent_close_history(object(), "XYZ", today=TODAY)
    assert src == "yfinance" and hist is not None

    monkeypatch.setattr(observations, "read_latest_n",
                        lambda conn, sid, n, **kw: _obs(kw["source"], TODAY - timedelta(days=60), 40))
    hist, reason = fa.recent_close_history(object(), "XYZ", today=TODAY)
    assert hist is None and "60d old" in reason


def test_too_short_history_is_skipped(monkeypatch) -> None:
    monkeypatch.setattr(observations, "read_latest_n",
                        lambda conn, sid, n, **kw: _obs(kw["source"], TODAY, 5))
    hist, reason = fa.recent_close_history(object(), "NEW", today=TODAY)
    assert hist is None and "5 dates" in reason


def _forecast(preds: list[float]) -> MagicMock:
    fr = MagicMock()
    fr.predictions = preds
    fr.forecast_std = [1.0] * len(preds)
    fr.lower_bound = [p - 1 for p in preds]
    fr.upper_bound = [p + 1 for p in preds]
    fr.horizon = len(preds)
    return fr


def test_forecast_not_anchored_on_current_price_makes_no_prediction() -> None:
    # The 2026-11-07 CORT case: history ended at $4.02, price is $72.77.
    stale = _forecast([4.03, 4.04, 4.04, 4.05, 4.05, 4.06, 4.04])
    assert fa.forecast_to_prediction(stale, "CORT", current_price=72.77) is None


def test_anchored_forecast_still_predicts() -> None:
    good = _forecast([72.5, 72.0, 71.4, 70.9, 70.1, 69.8, 69.2])
    pred = fa.forecast_to_prediction(good, "CORT", current_price=72.77)
    assert pred is not None and pred.target_price == 69.2
    assert -6 < pred.expected_move_pct < -4
