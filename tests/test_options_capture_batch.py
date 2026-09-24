"""A pull is available only after its final provider response is captured."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Self

import pytest

from ingestion import options


class _DB:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    def begin(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, statement: Any, params: dict | None = None) -> None:
        self.calls.append((str(statement), params or {}))


class _Yahoo:
    def __init__(self, expirations: list[int], *, fail_second: bool = False) -> None:
        self.expirations = expirations
        self.fail_second = fail_second
        self.final_response_at: datetime | None = None
        self.calls = 0

    def get_options(self, _ticker: str, _expiry: int | None = None) -> dict | None:
        self.calls += 1
        self.final_response_at = datetime.now(timezone.utc)
        if self.calls == 2 and self.fail_second:
            return None
        opt = {
            "strike": 100.0, "volume": 3, "openInterest": 10,
            "impliedVolatility": 0.2, "lastPrice": 2.0,
            "bid": 1.0, "ask": 3.0, "inTheMoney": False,
        }
        return {
            "quote": {"regularMarketPrice": 100.0},
            "expirations": self.expirations,
            "calls": [opt], "puts": [opt],
        }


def _run_pull(monkeypatch: pytest.MonkeyPatch, *, fail_second: bool = False):
    now = datetime.now(timezone.utc)
    expirations = [int((now + timedelta(days=days)).timestamp()) for days in (10, 20)]
    db = _DB()
    yahoo = _Yahoo(expirations, fail_second=fail_second)
    puller = options.OptionsPuller.__new__(options.OptionsPuller)
    puller.engine = db
    puller._yahoo = yahoo
    monkeypatch.setattr(puller, "_push_to_resolved", lambda *_args: None)
    monkeypatch.setattr(options.time, "sleep", lambda _seconds: None)
    monkeypatch.setattr(options, "compute_max_pain", lambda *_args: 100.0)
    monkeypatch.setattr(options, "compute_iv_skew", lambda *_args: 0.0)
    monkeypatch.setattr(options, "_compute_atm_iv", lambda *_args: 0.2)
    monkeypatch.setattr(options, "_compute_wing_iv", lambda *_args, **_kwargs: 0.2)
    monkeypatch.setattr(options, "_compute_oi_concentration", lambda *_args: 0.5)
    today = datetime.now(timezone.utc).astimezone().date()
    result = puller._pull_ticker("SPY", today.isoformat())
    return result, db, yahoo


def test_completed_batch_time_follows_final_provider_response(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, db, yahoo = _run_pull(monkeypatch)
    assert result["status"] == "SUCCESS"
    assert yahoo.calls == 2
    inserts = [(sql, params) for sql, params in db.calls
               if "INSERT INTO options_snapshots" in sql]
    completions = [(sql, params) for sql, params in db.calls
                   if "UPDATE options_snapshots" in sql]
    assert len(inserts) == 4
    assert len(completions) == 1
    batch_ids = {params["batch_id"] for _, params in inserts}
    assert batch_ids == {completions[0][1]["batch_id"]}
    assert yahoo.final_response_at is not None
    assert completions[0][1]["completed_at"] >= yahoo.final_response_at
    assert db.calls.index(completions[0]) > max(db.calls.index(item) for item in inserts)


def test_failed_expiry_does_not_finalize_partial_batch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    result, db, yahoo = _run_pull(monkeypatch, fail_second=True)
    assert yahoo.calls == 2
    assert result["status"] == "FAILED"
    assert not any("UPDATE options_snapshots" in sql for sql, _ in db.calls)
