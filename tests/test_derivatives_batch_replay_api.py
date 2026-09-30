"""GEX/walls API: latest complete batch by default, explicit batch replay on request."""

import asyncio
from datetime import date

from api.routers import derivatives


class _Engine:
    def __init__(self) -> None:
        self.calls: list[tuple] = []

    def compute_gex_profile(self, ticker, **kwargs):
        self.calls.append((ticker, kwargs))
        return {"ticker": ticker, "put_wall": 95.0, "call_wall": 105.0,
                "chain_snap_date": "2026-10-01", "chain_batch_id": kwargs.get("capture_batch_id"),
                "chain_capture_ordinal": 7}


def _patched(monkeypatch) -> _Engine:
    engine = _Engine()
    monkeypatch.setattr(derivatives, "_get_gex_engine", lambda: engine)
    return engine


def test_default_reads_latest_batch_with_unchanged_call_shape(monkeypatch) -> None:
    engine = _patched(monkeypatch)
    asyncio.run(derivatives.get_gex("spy", snap_date=None, capture_batch_id=None))
    asyncio.run(derivatives.get_walls("spy", snap_date=None, capture_batch_id=None))
    assert engine.calls == [("SPY", {}), ("SPY", {})]


def test_explicit_batch_replay_passes_day_and_batch(monkeypatch) -> None:
    engine = _patched(monkeypatch)
    day = date(2026, 10, 1)
    walls = asyncio.run(derivatives.get_walls("spy", snap_date=day, capture_batch_id="b-1"))
    assert engine.calls == [("SPY", {"snap_date": day, "capture_batch_id": "b-1"})]
    assert walls["chain_batch_id"] == "b-1" and walls["put_wall"] == 95.0


def test_batch_without_day_is_refused_before_engine(monkeypatch) -> None:
    engine = _patched(monkeypatch)
    result = asyncio.run(derivatives.get_gex("spy", snap_date=None, capture_batch_id="b-1"))
    assert "error" in result and engine.calls == []
