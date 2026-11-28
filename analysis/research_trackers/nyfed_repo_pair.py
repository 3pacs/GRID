"""Offline, descriptive join of official NY Fed secured reference-rate payloads.

Use for research only. Read source terms before presenting or distributing output.
A blank revision flag or matched timestamp does not establish the first release.
"""
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
from zoneinfo import ZoneInfo
import json

TERMS = "https://www.newyorkfed.org/privacy/termsofuse"


def _clock(value):
    clock = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if clock.utcoffset() is None:
        raise ValueError("capture timestamp lacks UTC offset")
    return clock.astimezone(timezone.utc)


def _records(blob, expected_type):
    payload = json.loads(blob, parse_float=Decimal)
    records = payload.get("refRates") if isinstance(payload, dict) else None
    if not isinstance(records, list) or not records:
        raise ValueError("expected nonempty refRates")
    by_date = {}
    for row in records:
        if not isinstance(row, dict) or row.get("type") != expected_type:
            raise ValueError("wrong rate type")
        day = row.get("effectiveDate")
        if not isinstance(day, str) or date.fromisoformat(day).isoformat() != day or day in by_date:
            raise ValueError("missing, invalid, or duplicate effectiveDate")
        rate = row.get("percentRate")
        if type(rate) not in (Decimal, int) or not Decimal(rate).is_finite():
            raise ValueError("invalid percentRate")
        flag = row.get("revisionIndicator")
        footnote = row.get("footnoteId")
        if flag not in ("", "Y") or (footnote is not None and type(footnote) is not int):
            raise ValueError("unknown revision/footnote encoding")
        by_date[day] = (Decimal(rate), flag, footnote)
    return by_date


def pair_rates(sofr_bytes, tgcr_bytes, *, sofr_seen_at, tgcr_seen_at):
    """Pair by effective date only; no carry-forward or historical availability assumption."""
    sc, tc = _clock(sofr_seen_at), _clock(tgcr_seen_at)
    s, t = _records(sofr_bytes, "SOFR"), _records(tgcr_bytes, "TGCR")
    records, quarantine = [], []
    for day in sorted(s.keys() | t.keys()):
        if date.fromisoformat(day) >= min(c.astimezone(ZoneInfo("America/New_York")).date() for c in (sc, tc)):
            quarantine.append({"effective_date": day, "reason": "capture_before_next_day"})
        elif day not in s or day not in t:
            quarantine.append({"effective_date": day, "reason": "unmatched_effective_date"})
        elif s[day][1:] != ("", None) or t[day][1:] != ("", None):
            quarantine.append({"effective_date": day, "reason": "nonstandard_or_revised_publication"})
        else:
            records.append({
                "effective_date": day,
                "sofr_minus_tgcr_bp": str((s[day][0] - t[day][0]) * 100),
                "first_release_verified": False,
                "available_no_earlier_than": max(sc, tc).isoformat(),
            })
    return {
        "schema": "grid-ny-fed-repo-pair/1", "lane": "descriptive_research_only", "terms": TERMS,
        "captures": {
            "SOFR": {"sha256": sha256(sofr_bytes).hexdigest(), "observed_at": sc.isoformat()},
            "TGCR": {"sha256": sha256(tgcr_bytes).hexdigest(), "observed_at": tc.isoformat()},
        },
        "records": records, "quarantine": quarantine,
    }
