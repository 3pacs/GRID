"""Record a rule-based postmortem for every eligible failure that has none (no LLM, no age limit).

The nightly batch (``intelligence.postmortem.batch_postmortem``) records every failure of the last
30 days and LLM-narrates the newest. Failures older than that which never got a postmortem (on
2026-11-27: 317,899 Oracle misses scored in May 2026, plus 864 recent ones) are recorded only by
this explicit run. It is never scheduled; running ``--apply`` against production is an owner decision.

    python -m scripts.postmortem_backfill                      # dry run: counts, writes nothing
    python -m scripts.postmortem_backfill --apply --limit 1000 --max-seconds 600   # a budgeted slice
    python -m scripts.postmortem_backfill --apply --limit 1000 --max-seconds 600 --scored-before 2026-06-01

Every ``--apply`` run states a budget: ``--limit`` rows and ``--max-seconds`` (no new row is
started after it). A bulk run is staged: a timed first slice, the readers measured, then the
next slice (the PR's run sheet), never one unbounded pass.

A written row is the rule-based classifier's record: failure_category, root_cause, signals wrong and
right, the classifier's "what we missed", ``full_analysis.analysis_method = 'rules'`` and a NULL
analysis confidence. No LLM is called, so no GPU time and no paid provider. A failure older than
``POSTMORTEM_EVENT_WINDOW_DAYS`` emits no PostmortemCompleted (no trust update, no model-weight
decay). Idempotent and resumable: a failure with a postmortem is never selected again, and the store
writes at most one per prediction or trade. Readers window postmortems by the failure's own date
(``miss_on_or_after_sql``), so these rows never read as recent lessons.
"""

from __future__ import annotations

import argparse
import sys
import time
from datetime import date, datetime
from typing import Any, Callable, Iterator

from loguru import logger as log
from sqlalchemy import text

import intelligence.postmortem as pm
from store.options_provenance import VISIBLE_ORACLE_PREDICTIONS

PAGE_SIZE = 5000
PROGRESS_EVERY = 5000

# The same populations the nightly batch selects, without its 30-day window.
_SOURCES: dict[str, dict[str, str]] = {
    "predictions": {
        "from_where": (
            "FROM " + VISIBLE_ORACLE_PREDICTIONS + " src WHERE src.verdict = 'miss'"
            " AND " + pm.FINITE_PNL_PCT_SQL + " AND src.id NOT LIKE 'astrogrid:%'"
            " AND NOT EXISTS (SELECT 1 FROM trade_postmortems tp WHERE tp.prediction_id = src.id)"
        ),
        "date_col": "src.scored_at",
    },
    "trades": {
        "from_where": (
            "FROM options_recommendations src WHERE src.outcome IN ('LOSS', 'EXPIRED')"
            " AND " + pm.FINITE_ACTUAL_RETURN_SQL +
            " AND NOT EXISTS (SELECT 1 FROM trade_postmortems tp WHERE tp.trade_id = src.id)"
        ),
        "date_col": "src.closed_at",
    },
}


def _date_bounds(date_col: str, scored_after: date | None, scored_before: date | None) -> tuple[str, dict[str, Any]]:
    sql, params = "", {}
    if scored_after is not None:
        sql += f" AND {date_col} >= :after"
        params["after"] = scored_after
    if scored_before is not None:
        sql += f" AND {date_col} < :before"
        params["before"] = scored_before
    return sql, params


def count_unrecorded(engine, scored_after: date | None = None, scored_before: date | None = None) -> dict[str, Any]:
    """Unrecorded eligible failures per source, by month of the failure, plus those with no date."""
    out: dict[str, Any] = {}
    with engine.connect() as conn:
        for name, src in _SOURCES.items():
            bounds, params = _date_bounds(src["date_col"], scored_after, scored_before)
            rows = conn.execute(text(
                "SELECT to_char(date_trunc('month', " + src["date_col"] + "), 'YYYY-MM') AS m, COUNT(*) "
                + src["from_where"] + bounds + " GROUP BY 1 ORDER BY 1"
            ), params).fetchall()
            by_month = {str(r[0]) if r[0] is not None else "no date": int(r[1]) for r in rows}
            out[name] = {"by_month": by_month, "total": sum(by_month.values())}
    return out


def iter_unrecorded(
    engine,
    source: str,
    scored_after: date | None = None,
    scored_before: date | None = None,
    page_size: int = PAGE_SIZE,
) -> Iterator[Any]:
    """Ids of unrecorded eligible failures, newest first, in keyset pages.

    The keyset (date, id) always advances, so a failure the writer refuses (and leaves unrecorded)
    is passed once per run, never re-selected in a loop. Failures with no date are not selectable:
    nothing can window them; the dry run reports them as "no date".
    """
    src = _SOURCES[source]
    col = src["date_col"]
    bounds, params = _date_bounds(col, scored_after, scored_before)
    last: tuple[Any, Any] | None = None
    while True:
        page_params = dict(params, page=int(page_size))
        keyset = ""
        if last is not None:
            keyset = f" AND ({col}, src.id) < (:last_at, :last_id)"
            page_params.update(last_at=last[0], last_id=last[1])
        with engine.connect() as conn:
            rows = conn.execute(text(
                f"SELECT src.id, {col} " + src["from_where"] + f" AND {col} IS NOT NULL"
                + bounds + keyset + f" ORDER BY {col} DESC, src.id DESC LIMIT :page"
            ), page_params).fetchall()
        if not rows:
            return
        for row in rows:
            yield row[0]
        last = (rows[-1][1], rows[-1][0])
        if len(rows) < page_size:
            return


def run_backfill(
    engine,
    *,
    limit: int | None = None,
    scored_after: date | None = None,
    scored_before: date | None = None,
    max_errors: int = 100,
    page_size: int = PAGE_SIZE,
    max_seconds: float | None = None,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, int]:
    """Record every selected failure (``narrate=False``) within ``limit`` rows and ``max_seconds``.

    Returns selected / written / skipped / error counts and ``stopped`` (1 when the time budget
    ended the run; the rest stays unrecorded and a later run resumes from it).
    """
    writers: dict[str, Callable[..., Any]] = {
        "predictions": pm.generate_prediction_postmortem,
        "trades": pm.generate_postmortem,
    }
    stats = {"selected": 0, "written": 0, "skipped": 0, "errors": 0, "stopped": 0}
    started = clock()
    for source in ("predictions", "trades"):
        for ident in iter_unrecorded(engine, source, scored_after, scored_before, page_size):
            if limit is not None and stats["selected"] >= limit:
                return stats
            if max_seconds is not None and clock() - started >= max_seconds:
                stats["stopped"] = 1
                log.info("postmortem_backfill: time budget of {s}s spent; stopping", s=max_seconds)
                return stats
            stats["selected"] += 1
            try:
                if writers[source](engine, ident, narrate=False) is not None:
                    stats["written"] += 1
                else:
                    stats["skipped"] += 1   # refused by the writer, or already recorded meanwhile
            except Exception as exc:  # noqa: BLE001 - one bad row must not stop the run
                stats["errors"] += 1
                log.warning("postmortem_backfill: {s} {i} failed: {e}", s=source, i=ident, e=str(exc))
                if stats["errors"] >= max_errors:
                    log.error("postmortem_backfill: stopping after {n} errors", n=stats["errors"])
                    return stats
            if stats["selected"] % PROGRESS_EVERY == 0:
                log.info(
                    "postmortem_backfill: {sel} selected, {w} written, {sk} skipped, {er} errors, {t:.0f}s",
                    sel=stats["selected"], w=stats["written"], sk=stats["skipped"], er=stats["errors"],
                    t=clock() - started,
                )
    return stats


def _parse_date(value: str) -> date:
    return datetime.strptime(value, "%Y-%m-%d").date()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--apply", action="store_true", help="Write the records. Without it: counts only.")
    ap.add_argument("--limit", type=int, default=None, help="Record at most N failures this run.")
    ap.add_argument("--scored-after", type=_parse_date, default=None, help="Only failures on/after YYYY-MM-DD.")
    ap.add_argument("--scored-before", type=_parse_date, default=None, help="Only failures before YYYY-MM-DD.")
    ap.add_argument("--max-errors", type=int, default=100, help="Stop after this many failed rows.")
    ap.add_argument("--max-seconds", type=float, default=None,
                    help="Start no new row after this many seconds (required with --apply).")
    args = ap.parse_args(argv)

    from db import get_engine

    engine = get_engine()
    before = count_unrecorded(engine, args.scored_after, args.scored_before)
    for name, info in before.items():
        print(f"{name}: {info['total']:,} unrecorded")
        for month, n in info["by_month"].items():
            print(f"  {month}: {n:,}")
    if not args.apply:
        print("\nDry run: nothing written. Add --apply --limit N --max-seconds S to record them "
              "(rule-based, no LLM).")
        return 0
    if args.limit is None or args.max_seconds is None:
        print("\n--apply needs an explicit budget: --limit N and --max-seconds S.")
        return 2

    stats = run_backfill(
        engine, limit=args.limit, scored_after=args.scored_after,
        scored_before=args.scored_before, max_errors=args.max_errors, max_seconds=args.max_seconds,
    )
    print(f"\nselected {stats['selected']:,}, written {stats['written']:,}, "
          f"skipped {stats['skipped']:,}, errors {stats['errors']:,}"
          + (" (stopped at the time budget)" if stats["stopped"] else ""))
    after = count_unrecorded(engine, args.scored_after, args.scored_before)
    for name, info in after.items():
        print(f"{name}: {info['total']:,} still unrecorded")
    return 1 if stats["errors"] else 0


if __name__ == "__main__":
    sys.exit(main())
