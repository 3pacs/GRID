"""Reproducible synthetic-only dry run; stdout JSON, no model/network/DB calls."""

from __future__ import annotations
import json
from datetime import datetime, timedelta, timezone
from evaluation.continuous_forecasts import (
    Bar,
    CostScenario,
    Forecast,
    VenueContract,
    evaluate,
)


def sample() -> dict:
    origin = datetime(2026, 3, 7, 12, tzinfo=timezone.utc)
    c = VenueContract(
        "SYNTHETIC-VENUE",
        "BTC/USDT",
        "perpetual",
        "raw_trade_close",
        3600,
        "24/7",
        "synthetic:venue-contract:v1",
        True,
    )

    def bar(i: int) -> Bar:
        t = origin + timedelta(hours=i)
        return Bar(
            t,
            t + timedelta(seconds=5),
            100 + i,
            c.venue,
            c.instrument,
            c.basis,
            f"synthetic:linear-bars:v1:{i}",
        )

    history = tuple(bar(i) for i in range(-24, 0))
    outcome = tuple(bar(i) for i in range(1, 73))
    # Deliberately constructed predictor, not TimesFM inference or alpha evidence.
    point = tuple(b.price + 1 for b in outcome)
    f = Forecast(
        origin,
        origin - timedelta(minutes=1),
        origin - timedelta(minutes=2),
        "synthetic:linear-plus-one:v1",
        "synthetic:history:v1",
        tuple(b.closed_at for b in outcome),
        point,
        tuple(p - 3 for p in point),
        point,
        tuple(p + 3 for p in point),
    )
    end = origin + timedelta(hours=72)
    costs = CostScenario(
        5,
        3,
        12,
        origin,
        end,
        end + timedelta(seconds=5),
        "synthetic:assumed-costs:v1",
        True,
    )
    return evaluate(
        c,
        f,
        history,
        outcome,
        costs,
        as_of=end + timedelta(seconds=5),
        entry_bar=bar(0),
    )


if __name__ == "__main__":
    print(json.dumps(sample(), indent=2, sort_keys=True, allow_nan=False))
