"""Run one God View pillar writer (materialization plan slice G7).

Usage
-----
    python3 scripts/run_godview_writers.py --pillar fed  --code-sha-from-git
    python3 scripts/run_godview_writers.py --pillar cftc --code-sha-from-git --dry-run
    python3 scripts/run_godview_writers.py --pillar cftc --code-sha 0123abcd --start 2026-01-01

Pillars
-------
* ``fed``  -> ``godview.fed_liquidity.materialize_fed_liquidity`` (G3)
* ``cftc`` -> ``godview.cftc_positioning.materialize_cftc_positioning`` (G4)

Safety
------
* ``--code-sha`` (explicit) or ``--code-sha-from-git`` (``git rev-parse HEAD``
  of the tree this script lives in) is required; every written row and the
  ``godview_runs`` ledger row carry it. ``--code-sha-from-git`` refuses when a
  tracked file under the writers' import surface is modified, because the sha
  would then not describe the code that ran.
* Every writer transaction runs with ``SET LOCAL lock_timeout = '5s'`` and
  ``statement_timeout = '60s'`` (plan section 3).
* ``--dry-run`` runs the real writer, including the upserts, the database's
  CHECK/FK constraints and the ledger insert, inside transactions that are
  always rolled back. Nothing persists; the printed summary shows what a live
  run would have written.
* This script never imports the untracked incident modules
  (``tests/test_godview_no_incident_imports.py``).

Exit status: 0 success / no-op / partial (keys held by legacy rows, see the
ledger), 1 writer failed, 2 usage error, 3 no upstream inputs at all.

The systemd templates in ``deploy/systemd/grid-godview-*.{service,timer}`` run
this from the release tree under ``flock``. They are not installed by any code
path; enabling them is the owner's activation step (plan A3).
"""

from __future__ import annotations

import argparse
import contextlib
import json
import os
import re
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Iterator
from datetime import date, datetime, timezone
from typing import Any

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

PILLARS: tuple[str, ...] = ("fed", "cftc")

LOCK_TIMEOUT = "5s"
STATEMENT_TIMEOUT = "60s"

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_EMPTY = 3

_SHA_RE = re.compile(r"^[0-9a-f]{7,40}$")

#: Tracked paths whose local modification makes ``git rev-parse HEAD`` a lie
#: about the code that is about to run.
IMPORT_SURFACE: tuple[str, ...] = (
    "godview",
    "ingestion/altdata/cftc_markets.py",
    "ingestion/altdata/fed_liquidity.py",
    "intelligence/cot_extremes.py",
    "store",
    "db.py",
    "config.py",
    "scripts/run_godview_writers.py",
)

_OK_STATUSES = frozenset({"SUCCESS", "SUCCESS_NOOP", "PARTIAL_BLOCKED_BY_LEGACY"})


class CodeShaError(RuntimeError):
    pass


def validate_code_sha(sha: str) -> str:
    sha = (sha or "").strip().lower()
    if not _SHA_RE.match(sha):
        raise CodeShaError(f"code sha must be 7-40 lowercase hex characters, got {sha!r}")
    return sha


def _git(args: list[str]) -> str:
    return subprocess.run(
        ["git", "-C", REPO_ROOT, *args], check=True, capture_output=True, text=True, timeout=30
    ).stdout


def code_sha_from_git(git: Callable[[list[str]], str] = _git) -> str:
    """Full HEAD sha of this tree; refuses if the writers' import surface is modified."""
    try:
        sha = git(["rev-parse", "HEAD"]).strip()
        dirty = git(["status", "--porcelain", "--untracked-files=no", "--", *IMPORT_SURFACE]).strip()
    except (OSError, subprocess.SubprocessError) as exc:
        raise CodeShaError(f"could not read the git sha of {REPO_ROOT}: {exc}") from exc
    if dirty:
        raise CodeShaError(
            "tracked files under the writers' import surface are modified; HEAD does not "
            f"describe the running code:\n{dirty}"
        )
    return validate_code_sha(sha)


class TransactionEngine:
    """Engine stand-in handed to a writer: timeouts on every transaction, and
    commit (live) or unconditional rollback (dry run).

    The writers only ever call ``engine.begin()``.
    """

    def __init__(self, engine: Any, *, commit: bool) -> None:
        self._engine = engine
        self.commit = commit

    @contextlib.contextmanager
    def begin(self) -> Iterator[Any]:
        from sqlalchemy import text

        with self._engine.connect() as conn:
            trans = conn.begin()
            try:
                # Literals, not interpolation (repo SQL rule); a test pins them
                # to LOCK_TIMEOUT / STATEMENT_TIMEOUT.
                conn.execute(text("SET LOCAL lock_timeout = '5s'"))
                conn.execute(text("SET LOCAL statement_timeout = '60s'"))
                yield conn
            except BaseException:
                trans.rollback()
                raise
            if self.commit:
                trans.commit()
            else:
                trans.rollback()


def _aware_ts(value: str) -> datetime:
    ts = datetime.fromisoformat(value)
    return ts if ts.tzinfo is not None else ts.replace(tzinfo=timezone.utc)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=(__doc__ or "").splitlines()[0])
    p.add_argument("--pillar", required=True, choices=PILLARS)
    sha = p.add_mutually_exclusive_group(required=True)
    sha.add_argument("--code-sha", help="release/commit sha recorded on every row")
    sha.add_argument("--code-sha-from-git", action="store_true", help="use git rev-parse HEAD of this tree")
    p.add_argument("--dry-run", action="store_true", help="run the writer and roll everything back")
    p.add_argument("--start", type=date.fromisoformat, default=None, help="earliest obs/report date to write")
    p.add_argument(
        "--as-of-ts",
        type=_aware_ts,
        default=None,
        help="point-in-time replay: ignore pulls after this instant (ISO 8601, UTC if naive)",
    )
    return p.parse_args(argv)


def _writer(pillar: str) -> Callable[..., Any]:
    if pillar == "fed":
        from godview.fed_liquidity import materialize_fed_liquidity

        return materialize_fed_liquidity
    from godview.cftc_positioning import materialize_cftc_positioning

    return materialize_cftc_positioning


def summarize(pillar: str, result: Any, *, dry_run: bool) -> dict[str, Any]:
    rows = getattr(result, "rows", ()) or ()
    reasons = Counter(r.reason for r in rows if getattr(r, "status", None) == "skipped" and r.reason)
    out: dict[str, Any] = {
        "pillar": pillar,
        "dry_run": dry_run,
        "persisted": not dry_run,
        "status": result.status,
        "rows_written": result.rows_written,
        "rows_unchanged": sum(1 for r in rows if getattr(r, "status", None) == "noop"),
        "rows_skipped": result.rows_skipped,
        "skip_reasons": dict(sorted(reasons.items())),
        "run_id": result.run_id,
        "code_sha": result.code_sha,
        "message": result.message,
    }
    no_release = getattr(result, "no_release_rule_dates", None)
    if no_release:
        out["no_release_rule_dates"] = {k: [d.isoformat() for d in v] for k, v in no_release.items()}
    missing = getattr(result, "markets_without_observations", None)
    if missing:
        out["markets_without_observations"] = list(missing)
    return out


def exit_code_for(status: str) -> int:
    if status in _OK_STATUSES:
        return EXIT_OK
    if status == "EMPTY":
        return EXIT_EMPTY
    return EXIT_FAILED


def run(args: argparse.Namespace, engine: Any) -> tuple[int, dict[str, Any]]:
    if args.code_sha_from_git:
        code_sha = code_sha_from_git()
    else:
        code_sha = validate_code_sha(args.code_sha)
    tx_engine = TransactionEngine(engine, commit=not args.dry_run)
    result = _writer(args.pillar)(tx_engine, code_sha=code_sha, start=args.start, as_of_ts=args.as_of_ts)
    summary = summarize(args.pillar, result, dry_run=args.dry_run)
    return exit_code_for(result.status), summary


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    from db import get_engine

    try:
        code, summary = run(args, get_engine())
    except CodeShaError as exc:
        print(f"refusing to run: {exc}", file=sys.stderr)
        return EXIT_USAGE
    print(json.dumps(summary, default=str, sort_keys=True))
    if summary["status"] == "PARTIAL_BLOCKED_BY_LEGACY":
        print(
            "WARNING: some keys are held by legacy (provenance IS NULL) rows and were not written; "
            "they need the plan's A2 archive. See godview_runs.reasons.",
            file=sys.stderr,
        )
    return code


if __name__ == "__main__":
    sys.exit(main())
