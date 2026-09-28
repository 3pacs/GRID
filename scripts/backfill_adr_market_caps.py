"""One-shot backfill: divide existing ticker_metrics_daily rows by ADR ratio.

The market_cap_usd / shares_outstanding columns were computed from
SEC XBRL ordinary-share counts x per-ADR yfinance close, which
overstates (or understates, for ratio < 1 tickers like AZN) ADRs whose
1 ADR != 1 ordinary share by the ratio.

This script applies the ratio retroactively to rows already written before
the fix landed in ingestion/altdata/sec_xbrl_shares.py. The sec_xbrl_shares
puller (using the same _ADR_RATIOS table) already divides correctly for
every row it writes or refreshes going forward, so this script only needs
to run once against the pre-existing backlog.

WHY THIS SCRIPT IS DANGEROUS, AND WHY --execute IS REQUIRED
-------------------------------------------------------------
This mutates ticker_metrics_daily in place with no soft-delete or backup.
Running it a second time divides an already-corrected value again, which
is silently wrong (not an error) and produces plausible-looking but
incorrect numbers -- exactly what happened when this script ran twice in
prod because it had no argparse and no --help guard: any invocation,
including `--help`, executed the UPDATE for every ratio.

IDEMPOTENCY GUARD -- run-ledger + --i-know-this-has-not-run
-------------------------------------------------------------
A per-ticker "already corrected" marker column/table would need a schema
migration, which is out of scope for a one-shot safety script, so it was
rejected here.

A numeric "does this already look corrected" magnitude check was also
considered and rejected: ADR ratios in this table range 0.5x-10x, and a
repeated application still produces a share count that looks individually
plausible (e.g. TSM ordinary shares: ~25.9B -> /5 -> ~5.18B (correct) ->
/5 again -> ~1.036B -- still a plausible-looking share count for a large
company in isolation). A heuristic that would not reliably trip on the
exact failure mode this script exists to prevent (accidental re-run) gives
false confidence, which is worse than an explicit, deterministic guard.

Instead, a successful --execute run writes a JSON run-ledger (default:
scripts/.state/backfill_adr_market_caps.ledger.json, override with
--ledger-path) recording when it ran, which ratios were applied, and how
many rows were touched. Any later --execute run refuses immediately if
that ledger already shows a completed run, unless --i-know-this-has-not-run
is also passed. This directly prevents today's incident (accidental
back-to-back invocation) without depending on guessing from the data
itself.

Known limitation: the ledger lives on disk next to this script. If the
scripts/ directory (or its .state/ subdirectory specifically) is wiped or
reset by a deploy between runs, the guard resets too -- point --ledger-path
at a location that survives deploys if that matters for your deployment,
and always check the ledger contents before passing the override flag.

Run on the server (or any host with DB access):
    python3 scripts/backfill_adr_market_caps.py                 # dry-run (default) -- reads only, writes nothing
    python3 scripts/backfill_adr_market_caps.py --execute       # apply once
    python3 scripts/backfill_adr_market_caps.py --execute --i-know-this-has-not-run   # override an existing ledger
    python3 scripts/backfill_adr_market_caps.py --help          # prints help, touches nothing
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from loguru import logger as log
from sqlalchemy import text

from ingestion.altdata.sec_xbrl_shares import _ADR_RATIOS

_DEFAULT_LEDGER_PATH = (
    Path(__file__).resolve().parent / ".state" / "backfill_adr_market_caps.ledger.json"
)

_COUNT_SQL = text(
    """
    SELECT COUNT(*) FROM ticker_metrics_daily
     WHERE ticker = :t AND market_cap_usd IS NOT NULL
    """
)

_UPDATE_SQL = text(
    """
    UPDATE ticker_metrics_daily
       SET market_cap_usd = market_cap_usd / :r,
           shares_outstanding = (
               shares_outstanding / :r
           )::bigint,
           as_of = NOW()
     WHERE ticker = :t
       AND market_cap_usd IS NOT NULL
    """
)


def _get_engine():
    """Return a DB engine using the project config. Lazy import for testability."""
    from db import get_engine

    return get_engine()


def ratios_to_apply() -> dict[str, float]:
    """Return the non-1.0 entries of _ADR_RATIOS as floats.

    1:1 ADRs need no correction; malformed ratio values are skipped.
    """
    out: dict[str, float] = {}
    for ticker, ratio in _ADR_RATIOS.items():
        try:
            ratio_f = float(ratio)
        except (TypeError, ValueError):
            continue
        if ratio_f == 1.0:
            continue
        out[ticker] = ratio_f
    return out


def load_ledger(path: Path) -> Optional[dict[str, Any]]:
    """Return the parsed run-ledger at ``path``, or None if absent/unreadable."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text())
    except (OSError, ValueError) as exc:
        log.warning(
            "backfill_adr_market_caps: unreadable ledger at {p}: {e}",
            p=path, e=str(exc),
        )
        return None


def _write_ledger(
    path: Path,
    ratios_applied: dict[str, float],
    rows_updated: dict[str, int],
    total_rows: int,
    override: bool,
) -> None:
    """Append a completion record to the run-ledger, creating it if needed."""
    entry = {
        "applied_at": datetime.now(timezone.utc).isoformat(),
        "ratios_applied": ratios_applied,
        "rows_updated": rows_updated,
        "total_rows": total_rows,
        "override": override,
    }
    existing = load_ledger(path) or {}
    history = list(existing.get("history", []))
    history.append(entry)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"history": history, "last": entry}, indent=2))


def run(
    *,
    execute: bool,
    ledger_path: Path,
    i_know_this_has_not_run: bool = False,
) -> dict[str, Any]:
    """Preview or apply the ADR ratio correction.

    Dry-run (``execute=False``, the default via the CLI) only issues
    read-only COUNT queries to report how many rows each ratio WOULD touch.
    ``execute=True`` performs the real UPDATEs inside one transaction per
    ticker's ratio, guarded by the run-ledger described in the module
    docstring.

    Returns:
        dict with ``executed`` (bool), ``blocked_reason`` (str | None),
        ``ratios`` (ticker -> ratio considered), ``rows_updated``
        (ticker -> row count touched/would-touch), and ``total_rows``.
    """
    ratios = ratios_to_apply()
    prior = load_ledger(ledger_path)

    if execute and prior is not None and not i_know_this_has_not_run:
        last = prior.get("last", {})
        return {
            "executed": False,
            "blocked_reason": (
                f"run-ledger at {ledger_path} already shows a completed run at "
                f"{last.get('applied_at', '?')} ({last.get('total_rows', '?')} rows updated). "
                "Refusing to run again -- this script is not safe to apply twice. "
                "Pass --i-know-this-has-not-run only after confirming (e.g. by reading "
                "the ledger) that this is not a duplicate."
            ),
            "ratios": ratios,
            "rows_updated": {},
            "total_rows": 0,
        }

    engine = _get_engine()
    rows_updated: dict[str, int] = {}
    total = 0

    if not execute:
        with engine.connect() as conn:
            for ticker, ratio in ratios.items():
                n = conn.execute(_COUNT_SQL.bindparams(t=ticker)).scalar() or 0
                rows_updated[ticker] = n
                total += n
                log.info(
                    "[dry-run] {t} ratio={r} -> would update {n} rows",
                    t=ticker, r=ratio, n=n,
                )
        return {
            "executed": False,
            "blocked_reason": None,
            "ratios": ratios,
            "rows_updated": rows_updated,
            "total_rows": total,
        }

    with engine.begin() as conn:
        for ticker, ratio in ratios.items():
            res = conn.execute(_UPDATE_SQL.bindparams(t=ticker, r=ratio))
            n = res.rowcount or 0
            rows_updated[ticker] = n
            total += n
            log.info(
                "ADR backfill {t} ratio={r} -> {n} rows updated",
                t=ticker, r=ratio, n=n,
            )

    _write_ledger(
        ledger_path, ratios, rows_updated, total,
        override=i_know_this_has_not_run and prior is not None,
    )
    log.info("ADR backfill complete: {n} rows total", n=total)
    return {
        "executed": True,
        "blocked_reason": None,
        "ratios": ratios,
        "rows_updated": rows_updated,
        "total_rows": total,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help=(
            "Actually write the correction. Without this flag the script only "
            "reports what it would change (default: dry-run, no writes)."
        ),
    )
    parser.add_argument(
        "--i-know-this-has-not-run",
        action="store_true",
        help=(
            "Override an existing run-ledger that shows a previous completed "
            "--execute run. Only pass this after verifying (e.g. by reading the "
            "ledger file) that this run is genuinely not a duplicate."
        ),
    )
    parser.add_argument(
        "--ledger-path",
        type=Path,
        default=_DEFAULT_LEDGER_PATH,
        help=f"Path to the run-ledger JSON file (default: {_DEFAULT_LEDGER_PATH}).",
    )
    return parser


def main(argv: Optional[list[str]] = None) -> int:
    args = build_parser().parse_args(argv)

    summary = run(
        execute=args.execute,
        ledger_path=args.ledger_path,
        i_know_this_has_not_run=args.i_know_this_has_not_run,
    )

    if summary["blocked_reason"]:
        log.error("backfill_adr_market_caps: {r}", r=summary["blocked_reason"])
        return 1

    if not args.execute:
        log.info(
            "Dry-run complete -- {n} rows would be updated across {t} tickers. "
            "Re-run with --execute to apply (see --help for the idempotency guard).",
            n=summary["total_rows"], t=len(summary["ratios"]),
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
