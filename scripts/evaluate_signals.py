#!/usr/bin/env python3
"""Manual, read-only, provisional signal evaluation. Never scheduled or persisted."""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from evaluation.prices import PITPriceAccessor
from evaluation.signal_outcomes import EVALUATION_VERSION, SignalRecord, evaluate_signal, summarize_outcomes, validate_scoring_parameters

MARKET_TZ = ZoneInfo("America/New_York")
_DIRECTION = {"BUY": "BUY", "CLUSTER_BUY": "BUY", "SELL": "SELL"}


class RefusalError(Exception):
    """Selection cannot establish a safe, bounded read."""


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--db-url", required=True, help="Explicit PostgreSQL connection URL; read-only session enforced")
    p.add_argument("--source-type", required=True)
    p.add_argument("--date-from", required=True, type=date.fromisoformat)
    p.add_argument("--date-to", required=True, type=date.fromisoformat)
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--horizon-days", type=int, default=5)
    p.add_argument("--dead-band-pct", type=_finite_band, default=1.0)
    p.add_argument("--cost-bps", type=_finite_cost, default=0.0)
    return p


def _finite_band(value: str) -> float:
    try:
        number = float(value)
        validate_scoring_parameters(number, 0.0)
        return number
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _finite_cost(value: str) -> float:
    try:
        number = float(value)
        validate_scoring_parameters(0.0, number)
        return number
    except ValueError as exc:
        raise argparse.ArgumentTypeError(str(exc)) from exc


def _build_engine(url: str):
    from sqlalchemy import create_engine

    if not url.startswith(("postgresql://", "postgresql+psycopg://", "postgresql+psycopg2://")):
        raise RefusalError("PostgreSQL URL required for read-only session")
    return create_engine(url, connect_args={"options": "-c default_transaction_read_only=on -c statement_timeout=30000"})


def _market_date(value) -> date:
    """Normalize a native DATE or timezone-aware TIMESTAMPTZ explicitly."""
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise RefusalError("naive signal_date timestamp cannot establish market date")
        return value.astimezone(MARKET_TZ).date()
    if isinstance(value, date):
        return value
    raise RefusalError(f"unsupported signal_date type: {type(value).__name__}")


def _aware_timestamp(value, field: str) -> datetime:
    if not isinstance(value, datetime) or value.tzinfo is None or value.utcoffset() is None:
        raise RefusalError(f"{field} must be a timezone-aware timestamp")
    return value


@dataclasses.dataclass(frozen=True)
class SelectedRow:
    id: int
    source_type: str
    ticker: Optional[str]
    signal_date: date
    signal_type: str
    created_at: datetime
    signal_value: Optional[dict] = None


def _parse_signal_value(value) -> Optional[dict]:
    """Normalize the JSONB signal_value column. Some drivers return a dict
    already; others return the raw JSON text. Anything else (including
    malformed JSON) is treated as absent rather than guessed at."""
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (json.JSONDecodeError, TypeError):
            return None
        return parsed if isinstance(parsed, dict) else None
    return None


def select_signal_sources(engine, *, source_type: str, date_from: date, date_to: date, limit: int) -> list[SelectedRow]:
    """Use the actual column type to choose a DATE or TIMESTAMPTZ predicate.

    PostgreSQL integration is still unproven. In particular, a blanket
    ``::date`` cast on TIMESTAMPTZ depends on the session timezone, so this
    function selects a type-specific predicate and checks native row values.
    """
    from sqlalchemy import text

    if not source_type or limit < 1 or limit > 1000 or date_from > date_to:
        raise RefusalError("source, ordered date bounds, and 1..1000 limit required")
    with engine.connect() as conn:
        data_type = conn.execute(text("""
            SELECT data_type FROM information_schema.columns
             WHERE table_schema = current_schema() AND table_name = 'signal_sources'
               AND column_name = 'signal_date'
        """)).scalar_one_or_none()
        if data_type == "date":
            predicate = "signal_date >= :date_from AND signal_date <= :date_to"
            bounds = {"date_from": date_from, "date_to": date_to}
        elif data_type == "timestamp with time zone":
            # Literal zone in expression; UTC bound parameters avoid a
            # session-timezone-dependent cast of TIMESTAMPTZ to DATE.
            predicate = "signal_date >= :date_from AND signal_date < :date_to_exclusive"
            from datetime import timedelta
            bounds = {
                "date_from": datetime.combine(date_from, time.min, MARKET_TZ).astimezone(timezone.utc),
                "date_to_exclusive": datetime.combine(date_to + timedelta(days=1), time.min, MARKET_TZ).astimezone(timezone.utc),
            }
        else:
            raise RefusalError(f"unverified signal_date database type: {data_type!r}")
        rows = conn.execute(text(f"""
            SELECT id, source_type, ticker, signal_date, signal_type, created_at, signal_value
              FROM signal_sources
             WHERE source_type = :source_type AND {predicate}
             ORDER BY signal_date DESC, id DESC LIMIT :limit
        """), {"source_type": source_type, "limit": limit, **bounds}).fetchall()
    selected = [SelectedRow(r[0], r[1], r[2], _market_date(r[3]), r[4], _aware_timestamp(r[5], "created_at"),
                            _parse_signal_value(r[6])) for r in rows]
    if any(not date_from <= r.signal_date <= date_to for r in selected):
        raise RefusalError("database date predicate returned a row outside market-date bounds")
    return selected


# known_at resolution is source_type-specific and explicit: only congressional
# rows have a proven publication field wired here (disclosure_date, captured
# in signal_value JSONB by ingestion/altdata/congressional.py). Every other
# source_type falls back to created_at inside evaluate_signal() itself -- that
# fallback is the existing, already-tested behaviour; this function's job is
# only to populate known_at where a real publication timestamp is provably
# available, and to label why it did not for everything else, rather than
# leaving the reason implicit.
KNOWN_AT_SOURCE_CONGRESSIONAL_DISCLOSURE = "congressional_disclosure_date"
KNOWN_AT_SOURCE_FALLBACK_MISSING_DISCLOSURE = "created_at_fallback_missing_disclosure_date"
KNOWN_AT_SOURCE_FALLBACK_UNPARSEABLE_DISCLOSURE = "created_at_fallback_unparseable_disclosure_date"
KNOWN_AT_SOURCE_FALLBACK_UNVERIFIED_SOURCE_TYPE = "created_at_fallback_unverified_source_type"


def _resolve_known_at(row: SelectedRow) -> Tuple[Optional[datetime], str]:
    """Return (known_at, known_at_source) for one selected row.

    Congressional trade disclosures publish a `disclosure_date` (the date the
    STOCK Act filing became public), captured verbatim in `signal_value`
    (`ingestion/altdata/congressional.py`). That date has no time component,
    so the conservative, PIT-safe reading is "known no earlier than the end
    of that trading day" -- using `time.max` here reuses evaluate_signal()'s
    own after-16:00-ET rollover rule to push entry to the next session,
    rather than assuming (and possibly leaking) an earlier intraday time.

    Every other source_type has no verified publication field wired here
    yet, so known_at stays None and evaluate_signal() falls back to
    created_at -- explicitly labelled below, not silently assumed safe.
    """
    if "congress" not in row.source_type.lower():
        return None, KNOWN_AT_SOURCE_FALLBACK_UNVERIFIED_SOURCE_TYPE
    raw = (row.signal_value or {}).get("disclosure_date")
    if not isinstance(raw, str):
        return None, KNOWN_AT_SOURCE_FALLBACK_MISSING_DISCLOSURE
    try:
        disclosure_date = date.fromisoformat(raw)
    except ValueError:
        return None, KNOWN_AT_SOURCE_FALLBACK_UNPARSEABLE_DISCLOSURE
    known_at = datetime.combine(disclosure_date, time.max, MARKET_TZ)
    return known_at, KNOWN_AT_SOURCE_CONGRESSIONAL_DISCLOSURE


def to_signal_record(row: SelectedRow, *, horizon_days: int) -> SignalRecord:
    known_at, known_at_source = _resolve_known_at(row)
    return SignalRecord(
        source_type=row.source_type, instrument=row.ticker or "", signal_date=row.signal_date,
        direction=_DIRECTION.get(row.signal_type, "UNKNOWN"), horizon_days=horizon_days,
        signal_source_id=row.id, created_at=row.created_at, known_at=known_at,
        metadata={"known_at_source": known_at_source},
    )


def run(engine, args: argparse.Namespace, *, out=None, today: Optional[date] = None) -> dict:
    if out is None:
        out = sys.stdout
    validate_scoring_parameters(args.dead_band_pct, args.cost_bps)
    rows = select_signal_sources(engine, source_type=args.source_type, date_from=args.date_from,
                                 date_to=args.date_to, limit=args.limit)
    accessor = PITPriceAccessor(engine)
    records = [to_signal_record(r, horizon_days=args.horizon_days) for r in rows if r.ticker]
    # known_at_source is on SignalRecord.metadata, which evaluate_signal()
    # does not carry into its output record -- tally it separately here so
    # the CLI's own claim about how known_at was resolved is actually
    # reflected in what gets printed, not just asserted in prose.
    known_at_source_counts: dict = {}
    for record in records:
        source = record.metadata.get("known_at_source", "unlabelled")
        known_at_source_counts[source] = known_at_source_counts.get(source, 0) + 1
    outcomes = [evaluate_signal(record, accessor, dead_band_pct=args.dead_band_pct,
                                cost_bps=args.cost_bps, today=today)
                for record in records]
    result = {
        "evaluation_version": EVALUATION_VERSION,
        "dry_run": True,
        "assumptions": (
            "provisional: calendar-day horizon; exact-date entry and exit bars; "
            "after-16:00 America/New_York next calendar date; created_at ingestion "
            "proxy fallback, labelled per-row -- see known_at_source_counts below "
            "for how many rows actually used the congressional disclosure_date vs. "
            "fell back to created_at; origin unknown; multi-valued raw-close dates "
            "are refused rather than picked (ambiguous_raw_close_multiple_values), "
            "but that check only runs once a verified raw-close cutover is supplied "
            "to the accessor -- this CLI supplies none, so every row is refused as "
            "price_basis_cutover_unverified before any ambiguity check or price "
            "query can run; crypto/24-7 instruments are refused regardless, before "
            "any query, and are not run through NYSE-session logic"
        ),
        "n_selected": len(rows), "n_skipped_null_ticker": sum(not r.ticker for r in rows),
        "known_at_source_counts": known_at_source_counts,
        "cohort_summary": dataclasses.asdict(summarize_outcomes(outcomes)),
    }
    print(json.dumps(result, indent=2, default=str), file=out)
    return result


def main(argv: Optional[list[str]] = None) -> int:
    args = build_arg_parser().parse_args(argv)
    try:
        engine = _build_engine(args.db_url)
        run(engine, args)
    except RefusalError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
