"""Read filed SEC fundamentals for Dad without provider calls or writes.

These are reported fiscal-period facts, not live/TTM valuation or forecasts.
The legacy ``finviz`` wire key is kept by the router for existing clients.
"""

from __future__ import annotations

import json
import math
import re
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import text

SOURCE_NAME = "SEC_EDGAR_Fundamentals"
SERIES_PREFIX = "sec_filed_fundamentals"
LABELS = {
    "revenue": "Reported revenue",
    "revenue_contracts": "Reported contract revenue",
    "net_income": "Reported net income",
    "total_assets": "Reported assets",
    "eps_basic": "Reported basic EPS",
    "eps_diluted": "Reported diluted EPS",
    "stockholders_equity": "Reported equity",
    "long_term_debt": "Reported long term debt",
}
DURATION_FIELDS = {
    "revenue", "revenue_contracts", "net_income", "eps_basic", "eps_diluted",
}
UNSUPPORTED_FIELDS = (
    "price", "market_cap", "pe_ratio", "forward_pe", "eps_ttm", "eps_next_5y",
    "roe", "debt_equity", "profit_margin", "operating_margin", "beta",
    "short_float", "float", "sector", "industry", "dividend_pct",
)


def read_sec_profile(engine: Any, ticker: str, *, refresh: bool = False) -> dict[str, Any]:
    """Read only provenance-complete facts; unsupported fields stay unavailable.

    Filing dates have day precision. Pull time is capture time and never used
    to make an old filing look current. Historical revisions remain revised
    vintages; this reader does not claim point-in-time availability.
    """
    today = datetime.now(timezone.utc).date()
    rows = []
    error = None
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text(
                    "SELECT rs.series_id, rs.obs_date, rs.pull_timestamp, rs.value, rs.raw_payload "
                    "FROM raw_series rs JOIN source_catalog sc ON sc.id = rs.source_id "
                    "WHERE sc.name = :source AND rs.series_id LIKE :prefix "
                    "AND rs.pull_status = 'SUCCESS' "
                    "ORDER BY rs.obs_date DESC, rs.pull_timestamp DESC"
                ),
                {"source": SOURCE_NAME, "prefix": f"{SERIES_PREFIX}.{ticker}.%"},
            ).fetchall()
    except Exception:
        error = "SEC stored fundamentals unavailable"

    fields = {}
    latest_filed = None
    latest_obs = None
    latest_pull = None
    for sid, obs, pulled, value, payload in rows:
        field = str(sid).rsplit(".", 1)[-1]
        if field not in LABELS or field in fields:
            continue
        try:
            if isinstance(payload, str):
                payload = json.loads(payload)
            filed = date.fromisoformat(payload["filed"])
            end = date.fromisoformat(payload["period_end"])
            start = date.fromisoformat(payload["period_start"]) if field in DURATION_FIELDS else None
            companyfacts = re.fullmatch(
                r"https://data\.sec\.gov/api/xbrl/companyfacts/CIK([0-9]{10})\.json",
                str(payload.get("source_url", "")),
            )
            cik = str(payload.get("cik", ""))
            expected_unit = "USD/shares" if field.startswith("eps_") else "USD"
            number = float(value)
            if (
                isinstance(value, bool) or not math.isfinite(number)
                or filed > today or end > filed or str(obs) != end.isoformat()
                or (start is not None and start > end)
                or payload.get("unit") != expected_unit
                or payload.get("ticker") != ticker
                or payload.get("form") not in {"10-K", "10-K/A", "10-Q", "10-Q/A"}
                or not re.fullmatch(r"\d{10}-\d{2}-\d{6}", str(payload.get("accession", "")))
                or companyfacts is None
                or not re.fullmatch(r"[0-9]{1,10}", cik) or int(cik) <= 0
                or companyfacts.group(1) != cik.zfill(10)
            ):
                continue
        except (TypeError, ValueError, KeyError, AttributeError):
            continue
        item = {
            "field": field, "label": LABELS[field], "group": "reported",
            "raw_value": f"{number:g} {expected_unit}", "parsed": number,
            "numeric_value": number, "obs_date": end.isoformat(),
            "period_start": payload.get("period_start"), "period_end": end.isoformat(),
            "filed": filed.isoformat(), "known_at_precision": "date",
            "accession": payload["accession"], "form": payload["form"],
            "unit": expected_unit, "source_url": payload["source_url"],
            "pull_timestamp": str(pulled),
        }
        fields[field] = item
        latest_filed = max(latest_filed or filed, filed)
        latest_obs = max(latest_obs or end, end)
        latest_pull = max(latest_pull or str(pulled), str(pulled))

    age_days = (today - latest_filed).days if latest_filed else None
    state = "missing" if age_days is None else "stale" if age_days > 180 else "fresh"
    return {
        "status": "unavailable" if not fields else "stale" if state == "stale" else "ready",
        "source": "SEC EDGAR/XBRL", "source_name": SOURCE_NAME,
        "freshness": {"state": state, "age_days": age_days,
                      "label": f"Filed {latest_filed}" if latest_filed else "No filed facts"},
        "latest_pull": latest_pull,
        "latest_obs_date": latest_obs.isoformat() if latest_obs else None,
        "latest_filed_date": latest_filed.isoformat() if latest_filed else None,
        "field_count": len(fields), "rows_inserted": 0,
        "live_refresh_requested": refresh, "refresh_available": False,
        "fields": fields, "stats": [{"id": key, **item} for key, item in fields.items()],
        "unavailable_fields": {key: None for key in UNSUPPORTED_FIELDS},
        "provenance": "Reported fiscal-period facts; filing date has day precision. "
                      "Latest captured revised vintage, not a point-in-time or TTM claim.",
        "error": error,
    }
