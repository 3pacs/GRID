"""Dated chain/close pairing cannot cross a known-at or vintage boundary."""

from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from typing import Any, Self

import pytest

from physics.dealer_gamma import DealerGammaEngine
from store.astrogrid import AstroGridStore


AS_OF = date(2026, 9, 23)
CHAIN_TIME = datetime(2026, 9, 23, 19, tzinfo=timezone.utc)


class _Result:
    def __init__(self, rows: list[tuple]) -> None:
        self.rows = rows

    def fetchall(self) -> list[tuple]:
        return self.rows


class _DB:
    def __init__(self, rows: list[tuple] | None = None) -> None:
        self.rows = rows or []
        self.queries: list[str] = []

    def connect(self) -> Self:
        return self

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def execute(self, sql: Any, _params: dict | None = None) -> _Result:
        self.queries.append(str(sql))
        return _Result(self.rows)


def _option_row(created_at: datetime = CHAIN_TIME) -> tuple:
    return (765.0, "call", 100, 0.2, AS_OF + timedelta(days=7),
            timedelta(days=7), created_at)


def _receipt() -> dict:
    available = datetime(2026, 9, 23, 1, tzinfo=timezone.utc)
    return {
        "price": 760.0, "receipt_id": 11,
        "obs_date": AS_OF - timedelta(days=1),
        "available_at": available,
        "receipt_created_at": available + timedelta(minutes=1),
        "release_date": AS_OF, "vintage_date": AS_OF,
        "conflict_flag": False,
    }


def test_chain_has_no_implicit_future_or_old_date_fallback() -> None:
    db = _DB([])
    chain = DealerGammaEngine(db)._load_chain("SPY", AS_OF)
    assert chain.empty
    assert len(db.queries) == 1
    assert "snap_date = :snap_date" in db.queries[0]
    assert "MAX(snap_date)" not in db.queries[0]


@pytest.mark.parametrize("capture", [
    CHAIN_TIME + timedelta(days=1),
    CHAIN_TIME + timedelta(minutes=5),
])
def test_chain_rejects_late_or_mixed_capture(capture: datetime) -> None:
    db = _DB([_option_row(), _option_row(capture)])
    assert DealerGammaEngine(db)._load_chain("SPY", AS_OF).empty


def test_chain_preserves_actual_capture_and_requested_date() -> None:
    chain = DealerGammaEngine(_DB([_option_row()]))._load_chain("SPY", AS_OF)
    assert not chain.empty
    assert chain.attrs["snap_date"] == AS_OF
    assert chain.attrs["created_at_min"] == CHAIN_TIME


def test_invalid_option_row_from_second_pull_still_invalidates_chain() -> None:
    late_invalid = (765.0, "put", 0, 0.0, AS_OF + timedelta(days=7),
                    timedelta(days=7), CHAIN_TIME + timedelta(minutes=5))
    db = _DB([_option_row(), late_invalid])
    assert DealerGammaEngine(db)._load_chain("SPY", AS_OF).empty


@pytest.mark.parametrize("change", [
    {"obs_date": AS_OF - timedelta(days=5)},
    {"obs_date": AS_OF},
    {"available_at": CHAIN_TIME + timedelta(minutes=1)},
    {"receipt_created_at": CHAIN_TIME + timedelta(minutes=1)},
    {"release_date": AS_OF + timedelta(days=1)},
    {"vintage_date": AS_OF + timedelta(days=1)},
    {"conflict_flag": True},
])
def test_spot_rejects_stale_future_or_revised_receipt(
    monkeypatch: pytest.MonkeyPatch, change: dict,
) -> None:
    candidate = {**_receipt(), **change}
    monkeypatch.setattr(AstroGridStore, "_verified_spy_receipt",
                        lambda *_args, **_kwargs: candidate)
    assert DealerGammaEngine(_DB())._get_spot_receipt("SPY", CHAIN_TIME) is None


def test_spot_accepts_prior_known_receipt(monkeypatch: pytest.MonkeyPatch) -> None:
    candidate = _receipt()
    monkeypatch.setattr(AstroGridStore, "_verified_spy_receipt",
                        lambda *_args, **_kwargs: candidate)
    assert DealerGammaEngine(_DB())._get_spot_receipt("SPY", CHAIN_TIME) == candidate
