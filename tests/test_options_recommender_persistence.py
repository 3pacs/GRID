from __future__ import annotations

from types import SimpleNamespace

import numpy as np

from trading.options_recommender import OptionsRecommendation, OptionsRecommender


class _NoRow:
    def fetchone(self):
        return None


class _FakeConn:
    def __init__(self) -> None:
        self.insert_params: list[dict] = []

    def execute(self, statement, params=None):
        sql = str(statement)
        if "INSERT INTO options_recommendations" in sql:
            self.insert_params.append(params or {})
        return _NoRow()


class _FakeBegin:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn

    def __enter__(self) -> _FakeConn:
        return self.conn

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class _FakeDb:
    def __init__(self) -> None:
        self.conn = _FakeConn()

    def begin(self) -> _FakeBegin:
        return _FakeBegin(self.conn)


def test_persist_recommendations_converts_numpy_scalars_to_python_scalars() -> None:
    db = _FakeDb()
    rec = OptionsRecommendation(
        ticker="TSLA",
        direction="PUT",
        strike=np.float64(412.5),
        expiry="2026-06-18",
        entry_price=np.float64(38.1715),
    )
    rec.target_price = np.float64(95.4288)
    rec.stop_loss = np.float64(19.0858)
    rec.expected_return = np.float64(-9.2833)
    rec.kelly_fraction = np.float64(0.0)
    rec.confidence = np.float64(0.214)
    rec.supporting_signals = [{"score": np.float64(10.0)}]
    rec.opposing_signals = []

    inserted = OptionsRecommender._persist_recommendations(
        SimpleNamespace(),
        db,
        [rec],
    )

    params = db.conn.insert_params[0]
    assert inserted == 1
    for key in ("strike", "entry_price", "target_price", "stop_loss", "expected_return", "kelly_fraction", "confidence"):
        assert type(params[key]) is float
