"""Regression: scripts/score_oracle_trades.py must pin yf.download's auto_adjust.

fetch_prices() supplies the `actual` price scored against every prediction's
entry_price (score_one_chunk, Step 4). For the vast majority of rows,
entry_price was never touched by this script — it was set at prediction time
by oracle/engine.py's _get_spot_price(), which reads the raw, unadjusted
options_daily_signals.spot_price (the same raw leg documented in
tests/test_yfinance_auto_adjust_explicit.py for intelligence/trust_scorer.py,
fixed in PR #503: https://github.com/3pacs/GRID/pull/503).

yfinance flipped yf.download()'s auto_adjust default to True during the
0.2.x line, which silently back-adjusts Close for dividends/splits. Scoring
a raw entry_price against a back-adjusted `actual` close would manufacture a
spurious return equal to the cumulative adjustment factor between entry and
expiry — the same mixed-basis bug trust_scorer had.
"""

from __future__ import annotations

from unittest.mock import patch

import pandas as pd
import pytest

from scripts.score_oracle_trades import fetch_prices


def _single_ticker_frame(dates, close):
    return pd.DataFrame({"Close": list(close)}, index=pd.to_datetime(dates))


def test_fetch_prices_passes_auto_adjust_false():
    frame = _single_ticker_frame(["2026-03-10", "2026-03-11"], [180.0, 182.0])

    with patch("yfinance.download", return_value=frame) as mock_dl:
        fetch_prices(["AAPL"], "2026-03-01", "2026-03-12")

    assert mock_dl.call_count == 1
    assert mock_dl.call_args.kwargs["auto_adjust"] is False


def test_fetch_prices_returns_raw_not_adjusted_close():
    """A back-adjusted close must never reach the scorer.

    entry_price on the majority of rows is the raw
    options_daily_signals.spot_price, carried over unchanged by
    oracle/engine.py. If fetch_prices() returned a back-adjusted `actual`
    close instead, comparing it to that raw entry_price would fabricate a
    return around any dividend/split event.
    """
    raw = [180.0, 182.0]
    adjusted = [176.4, 178.4]  # ~2% back-adjusted out for a dividend

    def fake_download(*_args, **kwargs):
        # Mirror real yfinance: auto_adjust=True overwrites Close with the
        # adjusted series.
        if kwargs.get("auto_adjust", True):
            return _single_ticker_frame(["2026-03-10", "2026-03-11"], adjusted)
        return _single_ticker_frame(["2026-03-10", "2026-03-11"], raw)

    with patch("yfinance.download", side_effect=fake_download):
        prices = fetch_prices(["AAPL"], "2026-03-01", "2026-03-12")

    values = sorted(prices["AAPL"].values())
    assert values == pytest.approx(raw), (
        "score_oracle_trades must price off the raw close to stay on the "
        "same basis as entry_price (options_daily_signals.spot_price)"
    )
