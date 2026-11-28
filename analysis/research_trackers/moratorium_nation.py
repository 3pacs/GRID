"""Offline adapter for pinned Moratorium Nation CSV. No applicability or delay finding."""
from __future__ import annotations

import argparse
import csv
from datetime import date, datetime, timezone
import hashlib
import hmac
import io
import json
from pathlib import Path
import re

RELEASE_BASE = "https://raw.githubusercontent.com/mjbommar/moratorium-data-2026/"
SCHEMA = "grid-tracker-moratorium-leads/1"
ID = re.compile(r"[a-z0-9][a-z0-9-]{0,159}\Z")
TAG = re.compile(r"v\d{4}\.\d{2}(?:\.\d{2})?(?:\.post\d+)?\Z")
STATES = frozenset("AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY".split())
STATUSES = frozenset({"active", "extended", "pending", "replaced", "expired", "rescinded", "rejected", "unknown"})
FIELDS = (
    "moratorium_id", "state_abbrev", "jurisdiction", "jurisdiction_type",
    "date_enacted_iso", "date_enacted_uncertainty", "enacted_status", "sectors",
)
FALSIFIER = "Obtain the operative local instrument, effective-date evidence, and exact project parcel/permit and exemption scope."


def _day(value: str, name: str) -> str | None:
    if not value:
        return None
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(name)
    return date.fromisoformat(value).isoformat()


def _adoption(value: str) -> tuple[str | None, str | None, str | None]:
    """Keep month/year-only claims without inventing an adoption day."""
    if re.fullmatch(r"[0-9]{4}", value):
        date(int(value), 1, 1)  # Validate, not an inferred date.
        return None, value, "year"
    if re.fullmatch(r"[0-9]{4}-[0-9]{2}", value):
        date.fromisoformat(value + "-01")  # Validate, not an inferred date.
        return None, value, "month"
    exact = _day(value, "reported_adoption_date")
    return exact, None, "day" if exact else None


def _clock(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.utcoffset() is None:
        raise ValueError("first_seen_at must include a UTC offset")
    return parsed.astimezone(timezone.utc)


def adapt(csv_bytes: bytes, *, release_tag: str, expected_sha256: str, first_seen_at: str) -> dict:
    """Verify local source bytes; return deterministic leads and separate research-only rejects."""
    if not TAG.fullmatch(release_tag):
        raise ValueError("use a version tag, not a mutable branch")
    digest = hashlib.sha256(csv_bytes).hexdigest()
    if not re.fullmatch(r"[0-9a-f]{64}", expected_sha256) or not hmac.compare_digest(digest, expected_sha256):
        raise ValueError("source SHA-256 mismatch")
    source = {
        "release_tag": release_tag, "url": f"{RELEASE_BASE}{release_tag}/data/moratorium_inventory.csv",
        "sha256": digest, "first_seen_at": _clock(first_seen_at).isoformat().replace("+00:00", "Z"),
        "attribution": "Moratorium Nation, Michael J. Bommarito; CC BY 4.0",
    }
    result = {"schema": SCHEMA, "scope": "offline_research_only", "source": source, "status": "ok",
              "records": [], "quarantine": []}
    reader = csv.DictReader(io.StringIO(csv_bytes.decode("utf-8-sig"), newline=""), strict=True)
    header = reader.fieldnames or []
    if len(set(header)) != len(header) or not set(FIELDS).issubset(header):
        raise ValueError("missing required or duplicate CSV columns")
    groups: dict[str, list[tuple[int, dict]]] = {}
    for row in reader:
        row_number = reader.line_num
        rid = (row.get("moratorium_id") or "").strip()
        if not ID.fullmatch(rid):
            result["quarantine"].append({"line_end": row_number, "id": None, "defect": "invalid_identifier",
                                         "falsifier": "Reconcile to the unique source record identifier."})
            continue
        groups.setdefault(rid, []).append((row_number, row))
    for rid, entries in sorted(groups.items()):
        if len(entries) != 1:
            result["quarantine"].append({"id": rid, "line_ends": [n for n, _ in entries], "defect": "ambiguous_duplicate",
                                         "falsifier": "Resolve duplicate IDs using record history and original jurisdiction sources."})
            continue
        number, row = entries[0]
        try:
            if None in row or any(v is None for v in row.values()):
                raise ValueError("row_width")
            v = {k: val.strip() for k, val in row.items()}
            if v["state_abbrev"] not in STATES or not v["jurisdiction"] or not v["jurisdiction_type"]:
                raise ValueError("jurisdiction")
            if v["enacted_status"] not in STATUSES:
                raise ValueError("reported_status")
            sectors = json.loads(v["sectors"])
            if not isinstance(sectors, list) or not sectors or any(not isinstance(x, str) or not x.strip() for x in sectors):
                raise ValueError("sectors")
            adoption, adoption_period, adoption_precision = _adoption(v["date_enacted_iso"])
            reported_end = _day(v.get("current_end_date_iso", ""), "reported_end_date")
            if adoption and reported_end and reported_end < adoption:
                raise ValueError("reported_end_before_adoption")
        except ValueError:
            result["quarantine"].append({"id": rid, "line_end": number, "defect": "malformed_record",
                                         "falsifier": "Compare the pinned row with the source schema and local authority."})
            continue
        result["records"].append({
            "id": rid, "evidence_class": "research_lead", "source": source.copy(),
            "jurisdiction": {"state": v["state_abbrev"], "name": v["jurisdiction"], "type": v["jurisdiction_type"]},
            "reported_status": v["enacted_status"], "sectors": sorted(set(x.strip() for x in sectors)),
            "reported_adoption_date": adoption,
            "reported_adoption_period": adoption_period, "reported_adoption_precision": adoption_precision,
            "reported_adoption_date_uncertainty": v["date_enacted_uncertainty"] or None,
            "reported_end_date": reported_end, "operative_start": None,
            "active_now": None, "project_applicability": "unverified", "observed_delay": None,
            "verification_needed": FALSIFIER,
        })
    if result["quarantine"]:
        result["status"] = "warn"
    return result


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("csv_path", type=Path)
    p.add_argument("--tag", required=True)
    p.add_argument("--sha256", required=True)
    p.add_argument("--first-seen-at", required=True, help="actual observer clock, not the enactment date")
    args = p.parse_args()
    try:
        result = adapt(args.csv_path.read_bytes(), release_tag=args.tag, expected_sha256=args.sha256,
                       first_seen_at=args.first_seen_at)
    except (ValueError, csv.Error, OSError) as err:
        p.exit(2, f"source refused: {err}\n")
    print(json.dumps(result, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
