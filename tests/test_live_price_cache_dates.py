"""Live quote callers cache only observations with an actual bar date."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


def _fake_ticker(*, dated: bool) -> MagicMock:
    ticker = MagicMock()
    if dated:
        ticker.fast_info.last_price = None
        ticker.fast_info.previous_close = None
        ticker.history.return_value = pd.DataFrame(
            {"Close": [180.0, 182.0]},
            index=pd.to_datetime(["2026-03-10", "2026-03-11"]),
        )
    else:
        ticker.fast_info.last_price = 182.0
        ticker.fast_info.previous_close = 180.0
    return ticker


@pytest.mark.parametrize("dated", [True, False])
def test_watchlist_enriched_cache_uses_only_actual_bar_date(dated):
    from api.routers.watchlist_core import list_watchlist_enriched

    conn = MagicMock()
    conn.execute.return_value.fetchall.side_effect = [
        [{"ticker": "AAPL", "display_name": "Apple"}],
        [],
    ]
    conn.execute.return_value.fetchone.return_value = None
    engine = MagicMock()
    engine.connect.return_value.__enter__.return_value = conn

    with patch("api.routers.watchlist_core._init_table"), \
         patch("api.routers.watchlist_core.get_db_engine", return_value=engine), \
         patch("api.routers.watchlist_core._get_cached_prices", return_value=None), \
         patch("api.routers.watchlist_core._cache_price_to_db") as cache, \
         patch("yfinance.Ticker", return_value=_fake_ticker(dated=dated)):
        result = list_watchlist_enriched(limit=1, _token="test")

    assert result["items"][0]["price"] == 182.0
    assert result["items"][0]["price_source"] == "live"
    if dated:
        cache.assert_called_once_with(engine, "AAPL", 182.0, date(2026, 3, 11))
    else:
        cache.assert_not_called()


@pytest.mark.parametrize("dated", [True, False])
def test_price_alert_cache_uses_only_actual_bar_date(dated):
    from api.routers.price_alerts import current_price

    engine = MagicMock()
    with patch("api.routers.price_alerts.get_db_engine", return_value=engine), \
         patch("api.routers.price_alerts._cache_price_to_db") as cache, \
         patch("yfinance.Ticker", return_value=_fake_ticker(dated=dated)):
        price, source = current_price("AAPL", prefer_live=True)

    assert (price, source) == (182.0, "live")
    if dated:
        cache.assert_called_once_with(engine, "AAPL", 182.0, date(2026, 3, 11))
    else:
        cache.assert_not_called()
