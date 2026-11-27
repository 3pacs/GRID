"""A model counts one row of a session-bounded factor per ticker.

The factor is republished every cycle and every copy stays valid; without
this a ticker's vote weight would scale with how often it was republished
(strength is |sum(w*z)| / sqrt(sum(w^2)), so n copies give sqrt(n) times one).
"""
from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

from oracle.model_factory import latest_factor_obs_only

T0 = datetime(2026, 11, 30, 14, 0, tzinfo=UTC)


def _row(source: str, ticker: str | None, obs: str | None, rid: int, minutes: int = 0,
         as_json: bool = False) -> dict:
    meta = {"signal_name": source.split(":")[-1], "rank": 0.9}
    if obs is not None:
        meta["factor_obs_date"] = obs
    return {"id": rid, "source_module": source, "ticker": ticker,
            "valid_from": T0 + timedelta(minutes=minutes),
            "metadata": json.dumps(meta) if as_json else meta}


SRC = "alpha_research:dual_horizon_equity"


def test_one_row_per_ticker_newest_observation_then_newest_publish() -> None:
    rows = [
        _row(SRC, "QQQ", "2026-11-30", 1, minutes=50), _row(SRC, "QQQ", "2026-11-30", 2, minutes=40),
        _row(SRC, "QQQ", "2026-11-27", 3, minutes=55, as_json=True), _row(SRC, "QQQ", "2026-11-27", 4),
        _row(SRC, "SPY", "2026-11-27", 5, minutes=30), _row(SRC, "SPY", "2026-11-27", 6, minutes=35),
    ]

    kept = [r["id"] for r in latest_factor_obs_only(rows)]

    assert kept == [1, 6]


def test_republish_count_does_not_change_a_tickers_weight() -> None:
    """40 SPY copies since its overnight receipt against 3 QQQ copies since the open."""
    rows = [_row(SRC, "SPY", "2026-11-27", i, minutes=-i) for i in range(40)]
    rows += [_row(SRC, "QQQ", "2026-11-30", 100 + i, minutes=-i) for i in range(3)]
    rows += [_row(SRC, "QQQ", "2026-11-27", 200 + i, minutes=-60 - i) for i in range(86)]

    kept = latest_factor_obs_only(rows)

    assert sorted(r["id"] for r in kept) == [0, 100]


def test_rows_without_an_observation_date_are_untouched() -> None:
    rows = [
        _row("alpha_research:vol_price_divergence", "QQQ", None, 1),
        _row("alpha_research:vol_price_divergence", "QQQ", None, 2),
        _row("alpha_research:vix_exposure", None, None, 3),
        _row(SRC, "QQQ", None, 4),  # published before the field existed
        _row(SRC, "QQQ", "2026-11-30", 5),
        {"id": 6, "source_module": "x", "ticker": "QQQ", "metadata": "not json"},
        {"id": 7, "source_module": "x", "ticker": "QQQ", "metadata": None},
    ]

    assert [r["id"] for r in latest_factor_obs_only(rows)] == [1, 2, 3, 4, 5, 6, 7]


def test_sources_are_kept_apart() -> None:
    rows = [
        _row(SRC, "QQQ", "2026-11-30", 1),
        _row("alpha_research:vol_regime_equity", "QQQ", "2026-11-27", 2),
    ]

    assert [r["id"] for r in latest_factor_obs_only(rows)] == [1, 2]
