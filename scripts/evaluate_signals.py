#!/usr/bin/env python3
"""CLI: run the versioned signal evaluator (workstream W3b/W3d) for real.

``evaluation/signal_outcomes.py::evaluate_signal`` and
``evaluation/prices.py`` have existed as tested, importable modules with
no caller — this script is that caller. It is a manual, explicitly-invoked
CLI, never wired into Hermes, ``ingestion/scheduler.py``, or any cron/systemd
unit, and this change does not add it to any of those. **It is not
scheduled and has never been run against production.**

Selection is explicit filters only (``--source-type``, ``--date-from``,
``--date-to``, ``--limit``) against ``signal_sources`` — nothing implicit,
nothing "all rows by default" (``--limit`` has a small, explicit default;
see below). Prices come from ``evaluation.prices.PITPriceAccessor``, PIT-
safe through ``store/pit.py``.

Safety rules, all enforced in code (not just documented):

  * By DEFAULT this is a dry run: it selects, evaluates, and prints the
    cohort summary, and writes NOTHING. Persisting requires BOTH
    ``--persist`` AND ``--i-understand-this-writes-evaluations`` — either
    alone is a refusal.
  * A persist run is INSERT-only into ``signal_evaluations``
    (``evaluation.signal_outcomes.persist_outcomes``, idempotent via
    ``ON CONFLICT ... DO NOTHING``). This script never UPDATEs or DELETEs
    anything, and never writes to ``signal_sources.trust_score``,
    ``signal_sources.outcome``, or any other column on ``signal_sources`` —
    it only ever SELECTs from that table.
    ``intelligence/trust_scorer.py`` is not imported, called, or modified.
  * Refuses to run against a database whose name does not start with
    ``griddb_`` unless ``--allow-any-db`` is passed — a guard against ever
    pointing this at the real production ``grid``/``griddb`` database by
    accident (e.g. an unset ``--db-url`` falling through to
    ``config.py``'s default).

``signal_sources`` has no ``direction`` or ``horizon_days`` column (see
schema.sql), so:
  * ``direction`` is derived from ``signal_sources.signal_type``
    (see ``_DIRECTION_BY_SIGNAL_TYPE`` below) — BUY/CLUSTER_BUY -> BUY,
    SELL -> SELL, anything else (including UNUSUAL_VOLUME, which carries no
    directional call) -> UNKNOWN, which ``evaluate_signal`` always scores
    INELIGIBLE(unknown_direction), never a guess.
  * ``--horizon-days`` is a single CLI-wide value applied to every selected
    signal (default 5 trading-adjacent calendar days).
  * ``--origin-tag`` is a required-by-convention passthrough (default
    "unknown") recorded on every resulting outcome row — this script does
    not infer it from the data.

Rows with a NULL ``ticker`` are excluded from evaluation before they ever
reach the evaluator (an untargeted signal has no instrument to price) and
counted separately in the summary as ``n_skipped_null_ticker``.
"""

from __future__ import annotations

import argparse
import dataclasses
import json
import sys
from datetime import date
from pathlib import Path
from typing import Any, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from evaluation.prices import PITPriceAccessor, PriceSeriesContract
from evaluation.signal_outcomes import (
    EVALUATION_VERSION,
    SignalRecord,
    dedupe_outcomes,
    evaluate_signal,
    persist_outcomes,
    summarize_outcomes,
)

# signal_sources.signal_type -> evaluation direction vocabulary. Anything
# not listed here (including a value the DB doesn't declare in its CHECK
# constraint comment, e.g. free-form future signal_types) maps to UNKNOWN —
# never guessed as BUY/SELL.
_DIRECTION_BY_SIGNAL_TYPE: dict[str, str] = {
    "BUY": "BUY",
    "CLUSTER_BUY": "BUY",
    "SELL": "SELL",
}

DEFAULT_LIMIT = 200
DEFAULT_HORIZON_DAYS = 5
REQUIRED_DB_PREFIX = "griddb_"


class RefusalError(Exception):
    """Raised for any refusal this CLI enforces (never a bare sys.exit call
    buried in business logic, so `run()` stays testable)."""


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="evaluate_signals",
        description=(
            "Evaluate signal_sources rows with the versioned outcome "
            "evaluator (dry run by default; see module docstring)."
        ),
    )
    parser.add_argument("--db-url", default=None, help="Override the DB URL (default: config.py's Settings.DB_URL).")
    parser.add_argument("--source-type", default=None, help="Exact signal_sources.source_type filter.")
    parser.add_argument("--date-from", default=None, help="Inclusive signal_date lower bound (YYYY-MM-DD).")
    parser.add_argument("--date-to", default=None, help="Inclusive signal_date upper bound (YYYY-MM-DD).")
    parser.add_argument(
        "--limit",
        type=int,
        default=DEFAULT_LIMIT,
        help=f"Max signal_sources rows selected (default {DEFAULT_LIMIT}).",
    )
    parser.add_argument(
        "--horizon-days",
        type=int,
        default=DEFAULT_HORIZON_DAYS,
        help=f"Horizon applied to every selected signal (default {DEFAULT_HORIZON_DAYS}).",
    )
    parser.add_argument("--dead-band-pct", type=float, default=1.0, help="Dead-band percent passed to evaluate_signal.")
    parser.add_argument("--cost-bps", type=float, default=0.0, help="Round-trip cost in bps passed to evaluate_signal.")
    parser.add_argument(
        "--origin-tag",
        default="unknown",
        help="Recorded verbatim on every outcome row (default 'unknown'). Not inferred from the data.",
    )
    parser.add_argument(
        "--persist",
        action="store_true",
        help="Write outcomes to signal_evaluations (INSERT-only). Requires --i-understand-this-writes-evaluations.",
    )
    parser.add_argument(
        "--i-understand-this-writes-evaluations",
        action="store_true",
        dest="ack_writes",
        help="Required alongside --persist to actually write anything.",
    )
    parser.add_argument(
        "--allow-any-db",
        action="store_true",
        help=f"Bypass the '{REQUIRED_DB_PREFIX}*' database-name safety check.",
    )
    return parser


def _parse_date(value: Optional[str]) -> Optional[date]:
    if value is None:
        return None
    return date.fromisoformat(value)


# ---------------------------------------------------------------------------
# Engine / DB-name safety
# ---------------------------------------------------------------------------


def _build_engine(db_url: Optional[str]):
    if db_url:
        from sqlalchemy import create_engine

        return create_engine(db_url, pool_pre_ping=True)
    from db import get_engine

    return get_engine()


def _engine_db_name(engine) -> Optional[str]:
    """Best-effort database name for the safety check.

    Works against a real SQLAlchemy Engine (``engine.url.database``) and
    against a fake test engine that carries the same shape
    (``engine.url.database``) — see tests/test_evaluate_signals_cli.py.
    """
    url = getattr(engine, "url", None)
    return getattr(url, "database", None)


def check_db_name(engine, *, allow_any_db: bool) -> None:
    if allow_any_db:
        return
    db_name = _engine_db_name(engine)
    if db_name is None or not db_name.startswith(REQUIRED_DB_PREFIX):
        raise RefusalError(
            f"Refusing to run against database {db_name!r}: name does not "
            f"start with {REQUIRED_DB_PREFIX!r}. Pass --allow-any-db to "
            "override (only for a disposable/scratch database)."
        )


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class _SelectedRow:
    id: int
    source_type: str
    ticker: Optional[str]
    signal_date: date
    signal_type: str


def select_signal_sources(
    engine,
    *,
    source_type: Optional[str],
    date_from: Optional[date],
    date_to: Optional[date],
    limit: int,
) -> list[_SelectedRow]:
    """SELECT-only against signal_sources. Never writes anything here."""
    from sqlalchemy import text

    sql = (
        "SELECT id, source_type, ticker, signal_date, signal_type "
        "FROM signal_sources "
        "WHERE (:source_type IS NULL OR source_type = :source_type) "
        "AND (:date_from IS NULL OR signal_date >= :date_from) "
        "AND (:date_to IS NULL OR signal_date <= :date_to) "
        "ORDER BY signal_date DESC "
        "LIMIT :limit"
    )
    params = {
        "source_type": source_type,
        "date_from": date_from,
        "date_to": date_to,
        "limit": limit,
    }
    with engine.connect() as conn:
        rows = conn.execute(text(sql), params).fetchall()
    return [
        _SelectedRow(
            id=row[0], source_type=row[1], ticker=row[2], signal_date=row[3], signal_type=row[4]
        )
        for row in rows
    ]


def to_signal_record(row: _SelectedRow, *, horizon_days: int, origin_tag: str) -> SignalRecord:
    direction = _DIRECTION_BY_SIGNAL_TYPE.get(row.signal_type, "UNKNOWN")
    return SignalRecord(
        source_type=row.source_type,
        instrument=row.ticker,
        signal_date=row.signal_date,
        direction=direction,
        horizon_days=horizon_days,
        signal_source_id=row.id,
        origin_tag=origin_tag,
    )


# ---------------------------------------------------------------------------
# Run (testable — takes an engine directly, no argparse/sys.exit inside)
# ---------------------------------------------------------------------------


def run(engine, args: argparse.Namespace, *, out=sys.stdout) -> dict[str, Any]:
    """Execute one CLI invocation against ``engine``. Returns the JSON-able
    result dict this also prints, so tests can assert on it directly
    instead of re-parsing stdout."""

    check_db_name(engine, allow_any_db=args.allow_any_db)

    if args.persist and not args.ack_writes:
        raise RefusalError(
            "--persist requires --i-understand-this-writes-evaluations. Refusing to write anything."
        )

    rows = select_signal_sources(
        engine,
        source_type=args.source_type,
        date_from=_parse_date(args.date_from),
        date_to=_parse_date(args.date_to),
        limit=args.limit,
    )
    n_skipped_null_ticker = sum(1 for r in rows if not r.ticker)
    records = [
        to_signal_record(r, horizon_days=args.horizon_days, origin_tag=args.origin_tag)
        for r in rows
        if r.ticker
    ]

    contract = PriceSeriesContract(engine)
    accessor = PITPriceAccessor(engine, contract)

    outcomes = [
        evaluate_signal(
            rec,
            accessor,
            dead_band_pct=args.dead_band_pct,
            cost_bps=args.cost_bps,
        )
        for rec in records
    ]

    summary = summarize_outcomes(outcomes)

    result: dict[str, Any] = {
        "evaluation_version": EVALUATION_VERSION,
        "origin_tag": args.origin_tag,
        "db_name": _engine_db_name(engine),
        "n_selected": len(rows),
        "n_skipped_null_ticker": n_skipped_null_ticker,
        "cohort_summary": dataclasses.asdict(summary),
        "dry_run": not args.persist,
        "persisted_count": None,
    }

    if args.persist:
        deduped, _ = dedupe_outcomes(outcomes)
        persistable = [o for o in deduped if o.signal_source_id is not None]
        inserted = persist_outcomes(engine, persistable)
        result["persisted_count"] = inserted

    print(json.dumps(result, indent=2, default=str), file=out)
    return result


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_arg_parser()
    args = parser.parse_args(argv)
    engine = _build_engine(args.db_url)
    try:
        run(engine, args)
    except RefusalError as exc:
        print(f"REFUSED: {exc}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
