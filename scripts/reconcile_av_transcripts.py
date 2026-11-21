#!/usr/bin/env python3
"""List (and, with --apply, record) pre-#1051 AV transcript rows a verified row supersedes.

Before #1051 the earnings_events puller asked AlphaVantage for calendar-quarter labels
and stored each transcript with a calendar-quarter-end period_end and filing_date. AV
reads the label as the issuer's fiscal quarter, so for the same ticker and
fiscal_quarter a verified #1051 row and a legacy row hold the same call. Their upsert
keys differ (filing_date), so both persist. See ingestion/av_transcript_plan.py.

Default: a read-only report (JSON on stdout) of every legacy row with a verified twin.
Nothing is written.

--apply: insert one row per superseded legacy row into ``earnings_event_supersessions``
(created if absent), append-only, ``ON CONFLICT DO NOTHING``. It never updates or
deletes an earnings_events row and never updates or deletes a supersession record.
Readers do not depend on this table; they drop superseded rows by the same rule
(AV_TRANSCRIPT_NOT_SUPERSEDED_SQL). The table is the audit record of the reconciliation.

Usage:
    python scripts/reconcile_av_transcripts.py [--ticker AAPL] [--apply --run-tag TAG]
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from sqlalchemy import text  # noqa: E402

from ingestion.av_transcript_plan import AV_LEGACY_ROW_SQL, AV_TWIN_JOIN_SQL  # noqa: E402

REPORT_SCHEMA = "av-transcript-supersession-report-v1"
REASON = "verified_fiscal_twin_same_ticker_fiscal_quarter"
_TICKER_RE = re.compile(r"^[A-Z0-9.\-]{1,12}$")
_RUN_TAG_RE = re.compile(r"^[A-Za-z0-9_.:\-]{1,64}$")

# Fixed SQL text; the only values are bound parameters.
CANDIDATES_SQL = (
    "SELECT e.id, e.ticker, e.fiscal_quarter, e.filing_date, e.period_end, "
    "v.id, v.filing_date, v.period_end, v.raw_payload ->> 'fiscal_period_accession', "
    "CASE WHEN e.full_text = v.full_text THEN 1 ELSE 0 END "
    "FROM earnings_events e JOIN earnings_events v ON " + AV_TWIN_JOIN_SQL + " "
    "WHERE " + AV_LEGACY_ROW_SQL + " "
    "AND (CAST(:ticker AS TEXT) IS NULL OR e.ticker = :ticker) "
    "ORDER BY e.ticker, e.fiscal_quarter, e.id, v.id"
)

SUPERSESSION_DDL = (
    "CREATE TABLE IF NOT EXISTS earnings_event_supersessions ("
    "legacy_event_id BIGINT PRIMARY KEY, "
    "superseded_by_event_id BIGINT NOT NULL, "
    "ticker TEXT NOT NULL, "
    "fiscal_quarter TEXT NOT NULL, "
    "reason TEXT NOT NULL, "
    "same_text BOOLEAN, "
    "run_tag TEXT NOT NULL, "
    "marked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP)"
)

INSERT_SQL = (
    "INSERT INTO earnings_event_supersessions "
    "(legacy_event_id, superseded_by_event_id, ticker, fiscal_quarter, reason, same_text, run_tag) "
    "VALUES (:legacy, :twin, :ticker, :quarter, :reason, :same_text, :run_tag) "
    "ON CONFLICT (legacy_event_id) DO NOTHING"
)


def _iso(value: Any) -> str | None:
    return value.isoformat() if hasattr(value, "isoformat") else (None if value is None else str(value))


def find_superseded(conn: Any, ticker: str | None = None) -> list[dict[str, Any]]:
    """One entry per legacy row with a verified twin; the earliest verified id is the twin."""
    rows = conn.execute(text(CANDIDATES_SQL), {"ticker": ticker}).fetchall()
    out: dict[int, dict[str, Any]] = {}
    for (lid, tk, quarter, l_filed, l_end, vid, v_filed, v_end, accession, same) in rows:
        entry = out.get(int(lid))
        if entry is None:
            out[int(lid)] = {
                "legacy_event_id": int(lid), "ticker": tk, "fiscal_quarter": quarter,
                "legacy_filing_date": _iso(l_filed), "legacy_period_end": _iso(l_end),
                "superseded_by_event_id": int(vid), "verified_filing_date": _iso(v_filed),
                "verified_period_end": _iso(v_end), "fiscal_period_accession": accession,
                "same_text": bool(same), "verified_twins": 1,
            }
        else:
            entry["verified_twins"] += 1
    return list(out.values())


def reconcile(engine: Any, *, ticker: str | None = None, apply: bool = False,
              run_tag: str | None = None) -> dict[str, Any]:
    if ticker is not None and not _TICKER_RE.fullmatch(ticker):
        raise ValueError("ticker must be 1-12 of A-Z, 0-9, '.', '-'")
    if apply and (run_tag is None or not _RUN_TAG_RE.fullmatch(run_tag)):
        raise ValueError("--apply needs --run-tag (1-64 of A-Za-z0-9_.:-)")
    report: dict[str, Any] = {"schema": REPORT_SCHEMA, "mode": "apply" if apply else "dry_run",
                              "ticker": ticker, "run_tag": run_tag, "reason": REASON}
    if not apply:
        with engine.connect() as conn:
            if engine.dialect.name == "postgresql":
                conn.execute(text("SET TRANSACTION READ ONLY"))
            found = find_superseded(conn, ticker)
            conn.rollback()
        report.update(superseded=found, count=len(found), written=0)
        return report
    with engine.begin() as conn:
        conn.execute(text(SUPERSESSION_DDL))
        found = find_superseded(conn, ticker)
        written = 0
        for entry in found:
            result = conn.execute(text(INSERT_SQL), {
                "legacy": entry["legacy_event_id"], "twin": entry["superseded_by_event_id"],
                "ticker": entry["ticker"], "quarter": entry["fiscal_quarter"], "reason": REASON,
                "same_text": entry["same_text"], "run_tag": run_tag,
            })
            written += max(int(result.rowcount or 0), 0)
    report.update(superseded=found, count=len(found), written=written,
                  already_recorded=len(found) - written)
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n", 1)[0])
    parser.add_argument("--ticker", help="restrict to one ticker (upper case)")
    parser.add_argument("--apply", action="store_true",
                        help="record supersessions (append-only); never edits earnings_events")
    parser.add_argument("--run-tag", help="required with --apply; stored on each record")
    args = parser.parse_args(argv)
    from db import get_engine
    report = reconcile(get_engine(), ticker=args.ticker, apply=args.apply, run_tag=args.run_tag)
    json.dump(report, sys.stdout, indent=2, sort_keys=True)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
