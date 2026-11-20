"""Read-only, opt-in diagnostic foundation for synchronized options research.

One SQL statement sees registry spot and immutable contracts in one database
statement snapshot. No fallback to a latest-day view or independently read spot.
This is NOT a qualified research panel: the existing tables lack native option
quote timestamps and postcommit visibility receipts. Never infer either from
capture time, provider page time, or registered_at.
"""

from __future__ import annotations

import math
from datetime import date, datetime, timezone
from numbers import Real
from typing import Any

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

_QUERY = text("""
SELECT b.capture_batch_id AS batch_id, b.ticker, b.snap_date,
       b.capture_ordinal AS ordinal, b.row_count,
       b.capture_started_at AS started_at, b.capture_completed_at AS completed_at,
       b.registered_at, b.spot_price, b.spot_provider, b.capture_source, b.backfilled,
       b.provider_regular_market_at AS spot_market_at,
       s.capture_batch_id AS row_batch_id, s.capture_ordinal AS row_ordinal,
       s.ticker AS row_ticker, s.snap_date AS row_snap_date,
       s.capture_started_at AS row_started_at, s.capture_completed_at AS row_completed_at,
       s.provider_regular_market_at AS row_provider_market_at, s.created_at AS row_created_at,
       s.expiry, s.strike, s.opt_type, s.open_interest, s.implied_vol,
       s.last_price, s.bid, s.ask, s.volume
FROM options_capture_batches b
LEFT JOIN options_snapshots_all s ON s.capture_batch_id = b.capture_batch_id
WHERE b.ticker = :ticker AND b.capture_batch_id = :batch_id
ORDER BY s.expiry, s.strike, s.opt_type
""")

_REGISTRY = (
    "batch_id",
    "ticker",
    "snap_date",
    "ordinal",
    "row_count",
    "started_at",
    "completed_at",
    "registered_at",
    "spot_price",
    "spot_provider",
    "capture_source",
    "backfilled",
    "spot_market_at",
)


def _aware(value: Any) -> bool:
    return (
        isinstance(value, datetime)
        and value.tzinfo is not None
        and value.utcoffset() is not None
    )


def _finite(value: Any, *, positive: bool = False) -> bool:
    # Native database numerics only. Strings/bools must not become measurements.
    if isinstance(value, bool) or not isinstance(value, Real):
        return False
    return math.isfinite(value) and (not positive or value > 0)


def load_capture_panel(
    engine: Any, ticker: str, batch_id: str, cutoff: datetime
) -> dict:
    """Inspect exactly one batch, with unavailable/diagnostic status explicit.

    ``cutoff`` must be timezone-aware. It excludes clocks after the requested
    boundary; it does not assert the transaction was committed by that boundary.
    Returned native dates/timestamps preserve source precision for internal use.
    No writes, provider calls, implicit latest selection or scoring occur.
    """
    result = {
        "available": False,
        "research_eligible": False,
        "diagnostic_complete": False,
        "status": "unavailable",
        "ticker": ticker,
        "requested_batch_id": batch_id,
        "cutoff": cutoff,
        "reasons": [],
        "capture": None,
        "contracts": [],
    }

    def reject(reason: str) -> dict:
        result["reasons"] = [reason]
        return result

    if (
        not isinstance(ticker, str)
        or not ticker
        or not isinstance(batch_id, str)
        or not batch_id
    ):
        return reject("invalid_identity")
    if not _aware(cutoff):
        return reject("timezone_aware_cutoff_required")
    try:
        with engine.connect() as conn:
            rows = [
                dict(row)
                for row in conn.execute(
                    _QUERY, {"ticker": ticker, "batch_id": batch_id}
                )
                .mappings()
                .all()
            ]
    except SQLAlchemyError:
        return reject("capture_read_failed")
    if not rows:
        return reject("capture_not_found")
    first = rows[0]
    capture = {key: first[key] for key in _REGISTRY}
    if any(any(row[key] != capture[key] for key in _REGISTRY) for row in rows):
        return reject("mixed_registry_metadata")
    if capture["batch_id"] != batch_id or capture["ticker"] != ticker:
        return reject("capture_identity_mismatch")
    day = capture["snap_date"]
    if type(day) is not date or day > cutoff.astimezone(timezone.utc).date():
        return reject("invalid_capture_day")
    for key in ("ordinal", "row_count"):
        if type(capture[key]) is not int or capture[key] <= 0:
            return reject("invalid_registry_counts")
    if len(rows) != capture["row_count"] or any(
        row["row_batch_id"] is None for row in rows
    ):
        return reject("incomplete_capture")
    start, end, registered, spot_time = (
        capture[key]
        for key in ("started_at", "completed_at", "registered_at", "spot_market_at")
    )
    if not all(_aware(t) for t in (start, end, registered, spot_time)):
        return reject("missing_or_naive_source_clock")
    if not start <= end <= registered <= cutoff or spot_time > end:
        return reject("invalid_or_future_source_clock")
    if any(t.astimezone(timezone.utc).date() != day for t in (start, end)):
        return reject("capture_day_mismatch")
    if not _finite(capture["spot_price"], positive=True):
        return reject("invalid_registry_spot")
    if any(
        not isinstance(capture[k], str) or not capture[k].strip()
        for k in ("spot_provider", "capture_source")
    ):
        return reject("missing_source_identity")
    identities = set()
    contracts = []
    for row in rows:
        if (
            row["row_batch_id"],
            row["row_ordinal"],
            row["row_ticker"],
            row["row_snap_date"],
            row["row_started_at"],
            row["row_completed_at"],
        ) != (batch_id, capture["ordinal"], ticker, day, start, end):
            return reject("mixed_contract_identity")
        created, provider = row["row_created_at"], row["row_provider_market_at"]
        if not _aware(created) or not _aware(provider):
            return reject("missing_or_naive_contract_clock")
        if created > cutoff or provider > end:
            return reject("future_contract_clock")
        if (
            type(row["expiry"]) is not date
            or row["opt_type"] not in ("call", "put")
            or not _finite(row["strike"], positive=True)
        ):
            return reject("invalid_contract_identity")
        identity = (row["expiry"], row["opt_type"], row["strike"])
        if identity in identities:
            return reject("duplicate_contract")
        identities.add(identity)
        contracts.append(
            {key: value for key, value in row.items() if key not in _REGISTRY}
        )
    result.update(
        diagnostic_complete=True,
        capture=capture,
        contracts=contracts,
        reasons=[
            "native_option_quote_clocks_unavailable",
            "postcommit_visibility_unproven",
            "freshness_and_skew_policy_not_applied",
        ],
    )
    return result
