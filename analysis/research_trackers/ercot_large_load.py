"""Read a saved ERCOT monthly report's UTF-8 text, not queue or utilization data."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timezone
import hashlib
import json
from pathlib import Path
import re
from urllib.parse import urlsplit

MONTHS = "January February March April May June July August September October November December".split()
HEADING = re.compile(
    r"(?im)^[ \t]*(?:PUBLIC[ \t]+)?Loads Approved to Energize[ \t]*[-–—][ \t]*Observations[ \t]*$"
)
PAIR = re.compile(
    r"\bOf the ([\d,]+) MW that have received Approval to Energize,\s*"
    r"ERCOT has observed a non[-‐‑– ]simultaneous monthly peak consumption "
    r"of ([\d,]+) MW in ([A-Za-z]+) (\d{4})\b", re.I
)
PATH = re.compile(r"/files/docs/(\d{4})/(\d{2})/(\d{2})/ERCOT-Monthly-Operational-Overview-[A-Za-z0-9-]+\.pdf")
NUMBER = re.compile(r"(?:0|[1-9]\d*|[1-9]\d{0,2}(?:,\d{3})+)\Z", re.ASCII)


def parse_monthly(raw: bytes, *, source_url: str, released_on: str, first_seen_at: str,
                  report_month: str, page: int) -> dict:
    """Parse one observation from local text; provenance is supplied, not fetched or authenticated."""
    u = urlsplit(source_url)
    path = PATH.fullmatch(u.path)
    if u.scheme != "https" or u.netloc != "www.ercot.com" or u.query or u.fragment or not path:
        raise ValueError("expected a direct official ERCOT overview PDF URL")
    released = date.fromisoformat(released_on)
    if released.isoformat() != released_on or "-".join(path.groups()) != released_on:
        raise ValueError("release date does not match stated official URL date")
    first = datetime.fromisoformat(first_seen_at.replace("Z", "+00:00"))
    if first.utcoffset() is None or first.astimezone(timezone.utc).date() < released:
        raise ValueError("first_seen_at must be timezone-aware and not before release date")
    if type(page) is not int or page < 1:
        raise ValueError("expected printed report page, starting at 1")
    pages = [p for p in raw.decode("utf-8-sig").split("\f") if HEADING.search(p)]
    if len(pages) != 1 or len(list(HEADING.finditer(pages[0]))) != 1:
        raise ValueError("missing or ambiguous monthly observations heading")
    footer = pages[0].strip().splitlines()[-1].strip()
    if footer.isascii() and footer.isdecimal() and int(footer) != page:
        raise ValueError("printed report page does not match stated provenance")
    section = HEADING.split(pages[0], maxsplit=1)[1]
    compact = " ".join(section.split()).lstrip("•*- ")
    match = PAIR.match(compact)
    if not match or len(re.findall(r"\bOf the\s+[\d,]+\s+MW\b", compact, re.I)) != 1:
        raise ValueError("missing, malformed or conflicting reported pair")
    a, b, named_month, year = match.groups()
    if not NUMBER.fullmatch(a) or not NUMBER.fullmatch(b):
        raise ValueError("malformed MW")
    approved, observed = (int(n.replace(",", "")) for n in (a, b))
    if approved <= 0 or not 0 <= observed <= approved:
        raise ValueError("inconsistent MW")
    month = date(int(year), MONTHS.index(named_month.capitalize()) + 1, 1)
    if month.isoformat()[:7] != report_month or (released.year, released.month) <= (month.year, month.month):
        raise ValueError("reported month or release chronology mismatch")
    return {
        "schema": "ercot-large-load-monthly-observation/1",
        "lane": "research_only",
        "report_month": report_month,
        "approved_to_energize_mw": approved,
        "observed_sum_of_individual_monthly_maxima_mw": observed,
        "source_url": source_url,
        "source_page": page,
        "released_on": released_on,
        "first_seen_at": first.astimezone(timezone.utc).isoformat(),
        "extracted_text_sha256": hashlib.sha256(raw).hexdigest(),
        "scope_note": "Not queue MW, coincident demand, project conversion, or utilization.",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("text_file", type=Path)
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--released-on", required=True)
    parser.add_argument("--first-seen-at", required=True)
    parser.add_argument("--report-month", required=True)
    parser.add_argument("--page", required=True, type=int)
    args = parser.parse_args()
    try:
        result = parse_monthly(
            args.text_file.read_bytes(), source_url=args.source_url, released_on=args.released_on,
            first_seen_at=args.first_seen_at, report_month=args.report_month, page=args.page,
        )
    except (ValueError, OSError) as exc:
        parser.exit(2, f"source refused: {exc}\n")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
