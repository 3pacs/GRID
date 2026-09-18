"""Constructed CFTC COT fixtures — never downloaded, always built here.

Records match the exact shape ``ingestion/altdata/cftc_cot.py``'s
``CFTCCOTPuller`` documents receiving from the CFTC Socrata API
(``report_date_as_yyyy_mm_dd``, ``market_and_exchange_names``, and the five
``_FIELD_MAP`` field names). Used by tests/godview/test_cftc_pillar_pure.py
and the DB-gated materializer tests.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any


def _next_tuesday(d: date) -> date:
    days_ahead = (1 - d.weekday()) % 7  # Tuesday == 1
    return d + timedelta(days=days_ahead)


def build_cot_records(
    n_weeks: int = 170,
    *,
    start_date: date = date(2023, 1, 3),
    market_name: str = "S&P 500 Consolidated - CHICAGO MERCANTILE EXCHANGE",
    base_oi: float = 2_500_000.0,
    base_noncommercial_long: float = 620_000.0,
    base_noncommercial_short: float = 480_000.0,
    drift_per_week: float = 350.0,
) -> list[dict[str, Any]]:
    """Build ``n_weeks`` consecutive, valid, Tuesday-dated COT records.

    The noncommercial net drifts upward week over week so a trailing
    z-score/percentile computed over the series is non-degenerate (not a
    flat line, which would make every z-score 0/undefined).
    """
    start = _next_tuesday(start_date)
    records: list[dict[str, Any]] = []
    for i in range(n_weeks):
        report_date = start + timedelta(weeks=i)
        nc_long = base_noncommercial_long + drift_per_week * i
        nc_short = base_noncommercial_short
        comm_long = base_oi * 0.35 - drift_per_week * i * 0.4
        comm_short = base_oi * 0.45
        records.append(
            {
                "report_date_as_yyyy_mm_dd": report_date.isoformat() + "T00:00:00.000",
                "market_and_exchange_names": market_name,
                "comm_positions_long_all": str(comm_long),
                "comm_positions_short_all": str(comm_short),
                "noncomm_positions_long_all": str(nc_long),
                "noncomm_positions_short_all": str(nc_short),
                "open_interest_all": str(base_oi + drift_per_week * i * 2),
            }
        )
    return records


def build_invalid_records() -> list[dict[str, Any]]:
    """One record for each way a constructed fixture can fail validation."""
    good = build_cot_records(n_weeks=1)[0]

    unparseable_date = dict(good)
    unparseable_date["report_date_as_yyyy_mm_dd"] = "not-a-date"

    missing_metric = dict(good)
    del missing_metric["open_interest_all"]

    negative_value = dict(good)
    negative_value["comm_positions_long_all"] = "-500"

    exceeds_oi = dict(good)
    exceeds_oi["comm_positions_long_all"] = str(float(good["open_interest_all"]) * 10)

    return [unparseable_date, missing_metric, negative_value, exceeds_oi]


def build_non_tuesday_record(report_date: date | None = None) -> dict[str, Any]:
    """A single otherwise-valid record whose report_date is NOT a Tuesday.

    Used to exercise the release_date quarantine rule (contract doc section 8).
    """
    good = build_cot_records(n_weeks=1)[0]
    off_day = report_date or date(2026, 9, 16)  # a Wednesday
    assert off_day.weekday() != 1, "fixture bug: chosen date is a Tuesday"
    good = dict(good)
    good["report_date_as_yyyy_mm_dd"] = off_day.isoformat() + "T00:00:00.000"
    return good
