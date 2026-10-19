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


# This is a capture policy, not an exchange/provider certification. Only
# explicitly audited daily label conventions are eligible. No suffix guess
# maps arbitrary caller tickers onto a US calendar.
DAILY_CAPTURE_CONTRACT = "yf_daily_capture_v1"
DAILY_FIELDS = frozenset({"open", "high", "low", "close", "volume", "adj_close"})
US_LISTED_DAILY_TICKERS = frozenset({
    "SPY", "DIA", "QQQ", "IWM", "^GSPC", "^DJI", "^IXIC", "^RUT",
    "XLK", "XLF", "XLE", "XLV", "XLI", "XLY", "XLP", "XLU", "XLRE", "XLB", "XLC",
    "SMH", "KRE", "ICLN", "LIT", "XBI", "ITA", "TSM",
    "TLT", "IEF", "SHY", "LQD", "HYG", "JNK", "EMB", "MUB",
    "GLD", "SLV", "USO", "DBA", "PDBC", "UUP", "FXE", "FXY", "EEM",
    "FXI", "EWJ", "EWZ", "EFA", "TIP", "COPX", "SJNK", "BKLN", "ANGL",
})
UTC_DAILY_TICKERS = frozenset({"BTC-USD", "ETH-USD"})
# Non-NYSE venues whose daily bars end before the next New York local
# midnight (owner decision 2026-10-19). Cboe volatility indexes settle at
# 16:15 ET on the NYSE holiday calendar; ICE DX-Y.NYB follows the New York
# trading day; CME Globex futures (bar labelled by trade date) settle at
# 17:00 ET; Yahoo FX closes at the New York 17:00 ET convention (the UTC
# day end at the latest). All finish their label day before 00:00
# America/New_York of the following date, so the same elapsed NY-label-day
# boundary is conservative for them. NYSE-closed labels stay UNKNOWN even
# when these markets trade: is_market_open is a refusal filter.
CBOE_VOL_INDEX_DAILY_TICKERS = frozenset({"^VIX", "^VIX9D", "^VIX3M", "^VIX6M"})
ICE_DXY_DAILY_TICKERS = frozenset({"DX-Y.NYB"})
CME_GLOBEX_DAILY_TICKERS = frozenset({"HG=F", "GC=F", "SI=F", "CL=F"})
YAHOO_FX_DAILY_TICKERS = frozenset({
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCHF=X", "USDCAD=X", "NZDUSD=X",
})
NY_LABEL_DAY_SESSION_CONVENTIONS: dict[str, str] = {
    **{t: "us_listed_ny_label_day_2026" for t in US_LISTED_DAILY_TICKERS},
    **{t: "cboe_vol_index_ny_label_day_2026" for t in CBOE_VOL_INDEX_DAILY_TICKERS},
    **{t: "ice_dxy_ny_label_day_2026" for t in ICE_DXY_DAILY_TICKERS},
    **{t: "cme_globex_ny_label_day_2026" for t in CME_GLOBEX_DAILY_TICKERS},
    **{t: "yahoo_fx_ny_close_label_day_2026" for t in YAHOO_FX_DAILY_TICKERS},
}
NY_LABEL_DAY_DAILY_TICKERS = frozenset(NY_LABEL_DAY_SESSION_CONVENTIONS)
# Every default ticker now has an explicit convention; kept so callers can
# still partition the universe as supported | unknown.
UNKNOWN_DAILY_TICKERS: frozenset[str] = frozenset()


def daily_capture_boundary(ticker: str, obs_date: date, interval: str = "1d") -> datetime | None:
    """Conservative elapsed-label-day boundary; unsupported sessions are unknown.

    New York label-day mappings are deliberately limited to 2026, the
    audited operational year. Existing standard NYSE holiday checks are a
    refusal filter, not proof of an exhaustive exchange calendar. Waiting
    until *local midnight* avoids assuming a regular/early exchange close
    or a fixed DST offset.
    Crypto's explicit UTC-day convention includes weekends and holidays.
    """
    if interval != "1d":
        return None
    if ticker in UTC_DAILY_TICKERS:
        return observation_end_utc(obs_date)
    if ticker not in NY_LABEL_DAY_SESSION_CONVENTIONS or obs_date.year != 2026:
        return None
    from ingestion.market_calendar import is_market_open
    from zoneinfo import ZoneInfo
    if not is_market_open(obs_date):
        return None
    return datetime.combine(obs_date + timedelta(days=1), time.min, ZoneInfo("America/New_York")).astimezone(timezone.utc)


def daily_capture_payload(ticker: str, field: str, obs_date: date,
                          captured_at: datetime, interval: str = "1d") -> dict[str, Any] | None:
    """Mark an eligible generic capture without claiming a provider-final bar."""
    boundary = daily_capture_boundary(ticker, obs_date, interval)
    if (field not in DAILY_FIELDS or boundary is None or captured_at.tzinfo is None
            or captured_at.utcoffset() is None or captured_at < boundary):
        return None
    return {
        "price_contract_version": DAILY_CAPTURE_CONTRACT,
        "capture_policy": "post_label_day_end_v1",
        "price_basis": f"YF:{ticker}:{field}",
        "interval": interval,
        "obs_date": obs_date.isoformat(),
        "session_convention": ("utc_24h" if ticker in UTC_DAILY_TICKERS
                               else NY_LABEL_DAY_SESSION_CONVENTIONS[ticker]),
        "eligible_after": boundary.isoformat(),
        "provider_certified_final": False,
    }


def is_daily_policy_capture(payload: Any, series_id: str, obs_date: date,
                            pulled_at: datetime | None) -> bool:
    """Validate exact series/date/interval/boundary provenance before selection."""
    if (not isinstance(payload, dict) or pulled_at is None
            or payload.get("provider_certified_final") is not False
            or ("capture_state" in payload and payload["capture_state"] != "POLICY_ELIGIBLE")):
        return False
    # Preserve the existing SPY scoring capture contract exactly.
    if series_id == SPY_CLOSE_SERIES:
        return is_policy_capture(payload, obs_date, pulled_at)
    parts = series_id.split(":")
    if len(parts) != 3 or parts[0] != "YF":
        return False
    expected = daily_capture_payload(parts[1], parts[2], obs_date, pulled_at)
    return expected is not None and all(payload.get(k) == v for k, v in expected.items())


RAW_CAPTURE_CONTRACT = "yf_raw_capture_v2"


def series_id_for_interval(ticker: str, field: str, interval: str) -> str:
    """Daily identifiers stay canonical; future non-daily captures are isolated."""
    canonical = f"YF:{ticker}:{field}"
    return canonical if interval == "1d" else f"{canonical}:{interval}"


def raw_capture_payload(ticker: str, field: str, obs_date: date,
                        captured_at: datetime, interval: str) -> dict[str, Any]:
    """Describe a raw capture independently of whether daily policy qualifies.

    No unsupported ticker, interval or year is silently dropped. A capture
    before a supported boundary is INCOMPLETE; missing session policy is
    UNKNOWN. This is raw provenance, never provider certification.
    """
    if captured_at.tzinfo is None or captured_at.utcoffset() is None:
        raise ValueError("raw capture timestamp must be offset-aware")
    marker = daily_capture_payload(ticker, field, obs_date, captured_at, interval)
    if marker is not None:
        state, reason = "POLICY_ELIGIBLE", "elapsed_supported_label_day"
    else:
        boundary = daily_capture_boundary(ticker, obs_date, interval)
        state = "INCOMPLETE" if boundary is not None and captured_at < boundary else "UNKNOWN"
        reason = "supported_label_day_not_elapsed" if state == "INCOMPLETE" else "daily_session_policy_unknown"
    payload = dict(marker) if marker is not None else {
        "price_basis": series_id_for_interval(ticker, field, interval),
        "interval": interval,
        "obs_date": obs_date.isoformat(),
        "provider_certified_final": False,
    }
    payload.update(raw_capture_contract=RAW_CAPTURE_CONTRACT,
                   capture_state=state, capture_reason=reason)
    return payload
