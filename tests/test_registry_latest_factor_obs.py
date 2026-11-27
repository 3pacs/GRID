"""A model counts one observation of a session-bounded factor per ticker."""
from __future__ import annotations

import json

from oracle.model_factory import latest_factor_obs_only


def _row(source: str, ticker: str | None, obs: str | None, rid: int, as_json: bool = False) -> dict:
    meta = {"signal_name": source.split(":")[-1], "rank": 0.9}
    if obs is not None:
        meta["factor_obs_date"] = obs
    return {"id": rid, "source_module": source, "ticker": ticker,
            "metadata": json.dumps(meta) if as_json else meta}


def test_friday_rows_are_dropped_once_monday_rows_exist() -> None:
    src = "alpha_research:dual_horizon_equity"
    rows = [
        _row(src, "QQQ", "2026-11-30", 1), _row(src, "QQQ", "2026-11-30", 2),
        _row(src, "QQQ", "2026-11-27", 3, as_json=True), _row(src, "QQQ", "2026-11-27", 4),
        _row(src, "SPY", "2026-11-27", 5),  # SPY's newest is Friday: kept
    ]

    kept = [r["id"] for r in latest_factor_obs_only(rows)]

    assert kept == [1, 2, 5]


def test_rows_without_an_observation_date_are_untouched() -> None:
    rows = [
        _row("alpha_research:vol_price_divergence", "QQQ", None, 1),
        _row("alpha_research:vix_exposure", None, None, 2),
        _row("alpha_research:dual_horizon_equity", "QQQ", None, 3),  # published before the field
        _row("alpha_research:dual_horizon_equity", "QQQ", "2026-11-30", 4),
        {"id": 5, "source_module": "x", "ticker": "QQQ", "metadata": "not json"},
        {"id": 6, "source_module": "x", "ticker": "QQQ", "metadata": None},
    ]

    assert [r["id"] for r in latest_factor_obs_only(rows)] == [1, 2, 3, 4, 5, 6]


def test_sources_are_kept_apart() -> None:
    rows = [
        _row("alpha_research:dual_horizon_equity", "QQQ", "2026-11-30", 1),
        _row("alpha_research:vol_regime_equity", "QQQ", "2026-11-27", 2),
    ]

    assert [r["id"] for r in latest_factor_obs_only(rows)] == [1, 2]
