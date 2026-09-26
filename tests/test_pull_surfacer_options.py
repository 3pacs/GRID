from types import SimpleNamespace

import pandas as pd

from scripts import pull_options, pull_surfacer_options
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


def test_legacy_cron_delegates_to_batch_aware_writer(monkeypatch):
    import db
    from ingestion import options

    calls = []

    class Puller:
        def __init__(self, db_engine):
            calls.append(db_engine)

        def pull_all(self, **kwargs):
            calls.append(kwargs)
            return [{"ticker": "SPY", "status": "SUCCESS"}]

    engine = object()
    monkeypatch.setattr(db, "get_engine", lambda: engine)
    monkeypatch.setattr(options, "OptionsPuller", Puller)
    assert pull_options.main() == 0
    assert calls == [engine, {
        "tickers": pull_options.EQUITY_TICKERS,
        "include_catalyst_universe": False,
    }]


def test_surfacer_expectation_pass_never_writes_gex_chain(monkeypatch):
    df = pd.DataFrame([{
        "strike": 100.0, "lastPrice": 2.0, "bid": 1.0, "ask": 3.0,
        "volume": 3, "openInterest": 10, "impliedVolatility": 0.2,
        "inTheMoney": False,
    }])
    chain = SimpleNamespace(calls=df, puts=df)

    class Stock:
        def __init__(self):
            self.options = ["2026-10-16"]

        def option_chain(self, _expiry):
            return chain

    class Cursor:
        def __init__(self):
            self.sql = []

        def execute(self, statement, _params):
            self.sql.append(statement)

    monkeypatch.setattr(pull_surfacer_options, "_require_yfinance", lambda: SimpleNamespace(
        Ticker=lambda _ticker: Stock(),
    ))
    monkeypatch.setattr(pull_surfacer_options, "_spot_price", lambda _stock: 100.0)
    cur = Cursor()
    result = pull_surfacer_options._pull_one(cur, "SPY", 1, "2026-09-24")
    assert result["status"] == "done"
    assert result["snapshots"] == 0
    assert result["snapshot_status"] == "not_written_by_surfacer"
    assert any("INSERT INTO options_daily_signals" in sql for sql in cur.sql)
    assert not any("options_snapshots" in sql for sql in cur.sql)
