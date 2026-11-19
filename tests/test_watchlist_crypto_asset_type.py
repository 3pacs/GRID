"""Watchlist rows added without a type were stored as "stock" (the old schema
default), so BTC/ETH/LINK read as stocks while the price path quoted them as
``-USD`` crypto pairs (seen live 2026-11-07)."""

from __future__ import annotations

from types import SimpleNamespace

import pytest

from api.routers.watchlist_helpers import (
    CRYPTO_TICKERS,
    _guess_asset_type,
    _row_to_dict,
    effective_asset_type,
)
from api.schemas.watchlist import WatchlistItemCreate


@pytest.mark.parametrize("ticker", ["BTC", "eth", "LINK", "BTC-USD", "SOLUSD"])
def test_crypto_tickers_guess_crypto(ticker: str) -> None:
    assert _guess_asset_type(ticker) == "crypto"


def test_stored_default_stock_is_reinferred_but_explicit_types_stand() -> None:
    assert effective_asset_type("BTC", "stock") == "crypto"
    assert effective_asset_type("SPY", "stock") == "etf"
    assert effective_asset_type("AAPL", "stock") == "stock"
    assert effective_asset_type("AAPL", None) == "stock"
    assert effective_asset_type("LINK", "commodity") == "commodity"


def test_row_to_dict_reports_the_effective_type() -> None:
    row = SimpleNamespace(_mapping={"id": 17, "ticker": "BTC", "asset_type": "stock", "added_at": None})
    assert _row_to_dict(row)["asset_type"] == "crypto"


def test_create_without_type_leaves_it_to_the_guess() -> None:
    assert WatchlistItemCreate(ticker="eth").asset_type is None
    assert WatchlistItemCreate(ticker="eth", asset_type="crypto").asset_type == "crypto"
    with pytest.raises(ValueError):
        WatchlistItemCreate(ticker="eth", asset_type="coin")


def test_price_path_and_guess_share_one_set() -> None:
    assert {"BTC", "ETH", "LINK"} <= CRYPTO_TICKERS
