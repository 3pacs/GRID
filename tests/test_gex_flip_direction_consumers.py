"""Consumers must not treat a modeled flip price as a signed flow observation."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

from api.routers import derivatives
from trading.options_recommender import OptionsRecommender


@pytest.mark.parametrize("direction,flip", [("CALL", 99.3), ("PUT", 104.0)])
def test_option_stop_does_not_tighten_from_nearest_flip(
    direction: str, flip: float,
) -> None:
    recommender = OptionsRecommender(MagicMock())
    stop = recommender._compute_stop_loss(
        10.0, 102.0, 100.0, direction,
        {"gamma_flip": flip, "gamma_flip_crossings": 1},
    )
    assert stop == 5.0


def test_timeline_calls_dated_sign_change_a_sign_change_not_a_price_flip(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    earlier = datetime.now(timezone.utc).date() - timedelta(days=2)
    later = datetime.now(timezone.utc).date() - timedelta(days=1)
    db = MagicMock()
    db.connect.return_value.__enter__.return_value.execute.return_value.fetchall.return_value = [
        (earlier, 95.0, None, None, None),
        (later, 102.0, None, None, None),
    ]
    profiles = {
        earlier: {"gex_aggregate": 512492.0, "regime": "LONG_GAMMA", "spot": 95.0},
        later: {"gex_aggregate": -357535.0, "regime": "SHORT_GAMMA", "spot": 102.0},
    }
    gex = MagicMock()
    gex.compute_gex_profile.side_effect = lambda _ticker, snap_date: profiles[snap_date]
    monkeypatch.setattr(derivatives, "get_db_engine", lambda: db)
    monkeypatch.setattr(derivatives, "_get_gex_engine", lambda: gex)
    monkeypatch.setattr(derivatives, "_generate_opex_calendar", lambda *_args: [])
    monkeypatch.setattr(derivatives, "_generate_catalysts", lambda *_args: [])

    result = derivatives.get_flow_timeline("SPY", days=7)

    assert result["gex_sign_changes"] == [{
        "date": str(later), "gex_sign": "negative", "spot": 102.0,
    }]
    assert result["gamma_flip_crossings"] == []
