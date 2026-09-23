"""Versioned, conservative SPY close capture policy.

The marker means that a Yahoo 1-day value was fetched after its UTC
observation day ended. It does not claim Yahoo certified the bar as final.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from typing import Any

SPY_CLOSE_CONTRACT = "spy_close_v1"
SPY_CLOSE_SERIES = "YF:SPY:close"
SPY_CLOSE_FEATURE = "spy_full"
SPY_CLOSE_CAPTURE_POLICY = "post_utc_day_end_v1"
SPY_ENTRY_RULE = "last_verified_close_available_at_creation_v1"
SPY_OUTCOME_RULE = "first_verified_close_on_or_after_horizon_within_4d_v1"
SPY_OUTCOME_GRACE_DAYS = 4
SPY_ENTRY_MAX_AGE_DAYS = 4
SPY_CAPTURE_MAX_LOOKBACK_DAYS = 4  # operational catch-up, never a history backfill


def observation_end_utc(obs_date: date) -> datetime:
    return datetime.combine(obs_date + timedelta(days=1), time.min, timezone.utc)


def capture_payload(obs_date: date, captured_at: datetime) -> dict[str, Any] | None:
    """Return a policy marker only for a post-period daily SPY capture."""
    if captured_at.tzinfo is None or captured_at < observation_end_utc(obs_date):
        return None
    return {
        "price_contract_version": SPY_CLOSE_CONTRACT,
        "capture_policy": SPY_CLOSE_CAPTURE_POLICY,
        "price_basis": SPY_CLOSE_SERIES,
        "interval": "1d",
        "obs_date": obs_date.isoformat(),
        "provider_certified_final": False,
    }


def is_policy_capture(payload: Any, obs_date: date, pulled_at: datetime) -> bool:
    return (
        isinstance(payload, dict)
        and payload.get("price_contract_version") == SPY_CLOSE_CONTRACT
        and payload.get("capture_policy") == SPY_CLOSE_CAPTURE_POLICY
        and payload.get("price_basis") == SPY_CLOSE_SERIES
        and payload.get("interval") == "1d"
        and payload.get("obs_date") == obs_date.isoformat()
        and payload.get("provider_certified_final") is False
        and pulled_at.tzinfo is not None
        and pulled_at >= observation_end_utc(obs_date)
    )
