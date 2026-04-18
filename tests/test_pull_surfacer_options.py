import pandas as pd

from scripts.pull_surfacer_options import _spot_price


class _FakeFastInfo(dict):
    pass


def test_spot_price_uses_recent_history_before_slow_info():
    class Stock:
        fast_info = _FakeFastInfo(last_price=None, previous_close=None)

        def history(self, **kwargs):
            return pd.DataFrame({"Close": [101.25, 102.5]})

        @property
        def info(self):
            raise AssertionError("stock.info should not be called when history has a price")

    assert _spot_price(Stock()) == 102.5


def test_spot_price_keeps_info_as_last_resort():
    class Stock:
        fast_info = _FakeFastInfo(last_price=None, previous_close=None)

        def history(self, **kwargs):
            return pd.DataFrame({"Close": []})

        @property
        def info(self):
            return {"regularMarketPrice": 88.0}

    assert _spot_price(Stock()) == 88.0
