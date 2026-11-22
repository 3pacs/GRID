"""Offline adapter for USDA AMS downbound barge rates, in percent of route tariff."""
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
import json
import re

SOURCE = "https://agtransport.usda.gov/Barge/Downbound-Grain-Barge-Rates/deqi-uken"
ORIGINS = frozenset(("Twin Cities", "Mid-Mississippi", "Lower Illinois", "St. Louis",
                     "Cincinnati", "Lower Ohio", "Cairo-Memphis"))


def normalize(payload: bytes, *, first_seen_at: str) -> dict:
    """Do not date-stamp release from value date; never treat missing rates as zero."""
    stamp = datetime.fromisoformat(first_seen_at.replace("Z", "+00:00"))
    if stamp.utcoffset() is None:
        raise ValueError("first_seen_at must have a UTC offset")
    stamp = stamp.astimezone(timezone.utc)
    rows = json.loads(payload)
    if not isinstance(rows, list) or not rows:
        raise ValueError("expected nonempty USDA record array")
    grouped = {}
    quarantine = []
    for i, row in enumerate(rows):
        try:
            stamp_field, origin = row["date"], row["location"]
            if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T00:00:00(?:\.0+)?", stamp_field):
                raise ValueError
            day = date.fromisoformat(stamp_field[:10])
            if origin not in ORIGINS or day > stamp.date():
                raise ValueError
            if row.get("month") != str(day.month) or row.get("year") != str(day.year):
                raise ValueError
            week = int(row["week"])
            if not 1 <= week <= 53:
                raise ValueError
        except (KeyError, TypeError, ValueError):
            quarantine.append({"index": i, "reason": "malformed_date_or_location"})
            continue
        grouped.setdefault((day.isoformat(), origin), []).append((i, week, row.get("rate")))
    records = []
    for (day, origin), values in sorted(grouped.items()):
        i, week, rate = values[0]
        if len(values) != 1:
            quarantine.append({"effective_date": day, "origin": origin, "reason": "duplicate_key"})
            continue
        if rate in (None, ""):
            state, value = "unavailable", None
        elif isinstance(rate, str) and re.fullmatch(r"[0-9]+(?:\.[0-9]+)?", rate) and Decimal(rate) > 0:
            state, value = "reported", str(Decimal(rate))
        else:
            quarantine.append({"index": i, "reason": "invalid_tariff_percentage"})
            continue
        records.append({"effective_date": day, "origin": origin, "reported_week": week,
                        "availability": state, "rate_pct_of_route_tariff": value})
    return {"schema": "grid-usda-barge-rates/1", "lane": "research_only", "source_url": SOURCE,
            "payload_sha256": sha256(payload).hexdigest(), "first_seen_at": stamp.isoformat(),
            "upstream_release_at": None, "records": records, "quarantine": quarantine}
