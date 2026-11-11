#!/usr/bin/env python3
"""Opt-in one-shot private volume acquisition and snapshot. No scheduler.

Invoke as ``python -m scripts.native_volume_once --fetch --root /private/root
--calendar-manifest /reviewed/calendar.json --calendar-sha256 DIGEST``.
The operator must first provision a trusted same-UID0700 root. --initialize may
create only its dedicated store and receipt directory. Failed/uncertain worker
checkpoints require receipt inspection/replay and reviewed manual recovery; there
is deliberately no blind --recover reset or automatic retry.
"""

import argparse
from contextlib import contextmanager
import fcntl
import os
import stat
import signal
import threading
from datetime import datetime, timezone
import json
from pathlib import Path
from zoneinfo import ZoneInfo

from ingestion.realtime.native_volume_cache import _private
from ingestion.realtime.minute_retention import sqlite_loader
from ingestion.realtime.native_volume_worker import (
    run_once,
    owner_lock,
    request_plan,
    _read_checkpoint,
)
from ingestion.realtime.native_volume_snapshot import build_snapshot, write_snapshot
from ingestion.realtime.research_intervals import _utc


class InvocationDeadline(BaseException):
    """Not swallowed by provider handlers; uncertain work stays held."""


@contextmanager
def invocation_deadline(seconds=45):
    """POSIX wall deadline over configuration, locks, refresh, GET and writes."""
    if threading.current_thread() is not threading.main_thread():
        raise RuntimeError("native acquisition requires main-thread deadline")
    if signal.getitimer(signal.ITIMER_REAL) != (0.0, 0.0):
        raise RuntimeError("existing timer; refuse nested invocation")
    previous = signal.getsignal(signal.SIGALRM)

    def expired(signum, frame):
        raise InvocationDeadline()

    signal.signal(signal.SIGALRM, expired)
    try:
        signal.setitimer(signal.ITIMER_REAL, seconds)
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


@contextmanager
def invocation_lock(root):
    """Serialize the complete fetch-through-snapshot CLI transaction."""
    fd = os.open(
        root / "invocation.lock", os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600
    )
    try:
        st = os.fstat(fd)
        if (
            not stat.S_ISREG(st.st_mode)
            or st.st_uid != os.geteuid()
            or stat.S_IMODE(st.st_mode) & 0o077
            or st.st_nlink != 1
        ):
            raise ValueError("unsafe invocation lock")
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise RuntimeError("invocation already active") from None
        yield
    finally:
        os.close(fd)


def default_calendar(path, digest):
    from ingestion.realtime.native_volume_calendar import ManifestCalendar

    return ManifestCalendar(path, digest)


def default_client():
    # This import may load normal GRID configuration and credentials. It happens
    # only after worker lock, request planning, custody and disk checks passed.
    from ingestion.schwab_market import SchwabMarketClient

    return SchwabMarketClient(
        max_retries=0, timeout=12, token_lock_timeout=3, reauth_on_401=False
    )


class LazyClient:
    _max_retries = 0

    def __init__(self, factory):
        self.factory = factory

    def price_history(self, symbol, **params):
        client = self.factory()
        policy = getattr(client, "_max_retries", None)
        if type(policy) is not int or policy != 0:
            raise ValueError("explicit zero retry policy required")
        if (
            getattr(client, "_reauth_on_401", None) is not False
            or getattr(client, "_token_lock_timeout", None) != 3
        ):
            raise ValueError("bounded no-reauth client required")
        return client.price_history(symbol, **params)


def _main(
    argv=None, *, calendar_factory=None, client_factory=default_client, clock=None
):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--fetch",
        action="store_true",
        help="allow at most one history GET plus optional preflight OAuth refresh",
    )
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--initialize", action="store_true")
    parser.add_argument(
        "--incremental-only",
        action="store_true",
        help="refuse bootstrap/new-session acquisition and initialization",
    )
    parser.add_argument("--calendar-manifest", type=Path)
    parser.add_argument("--calendar-sha256")
    parser.add_argument("--disk-budget-bytes", type=int, default=128_000_000)
    args = parser.parse_args(argv)
    if args.incremental_only and args.initialize:
        print(
            json.dumps(
                {
                    "status": "refused",
                    "reason": "incremental-only mode cannot initialize",
                }
            )
        )
        return 2
    if not args.fetch:
        print(json.dumps({"status": "refused", "reason": "explicit --fetch required"}))
        return 2
    clock = clock or (lambda: datetime.now(timezone.utc))
    try:
        root = args.root
        if not root.is_absolute() or root.resolve() != root:
            raise ValueError("absolute trusted root without symlink ancestry required")
        _private(root, directory=True)
        with invocation_lock(root):
            if calendar_factory is None:
                if args.calendar_manifest is None or args.calendar_sha256 is None:
                    raise ValueError("reviewed calendar manifest and digest required")
                calendar = default_calendar(
                    args.calendar_manifest, args.calendar_sha256
                )
            else:
                calendar = calendar_factory()
            now = _utc(clock())
            key = f"XNYS:{now.astimezone(ZoneInfo('America/New_York')).date().isoformat()}:RTH"
            # A holiday/unsupported calendar raises before client construction.
            calendar(key)
            prior = _read_checkpoint(root / "checkpoint.json")
            if prior and prior.get("symbol") != "SPY":
                raise ValueError("checkpoint identity mismatch")
            if request_plan(calendar, key, now, prior) is None:
                print(json.dumps({"status": "idle", "requested": False}))
                return 0
            outcome = run_once(
                LazyClient(client_factory),
                root=root,
                calendar=calendar,
                session_key=key,
                symbol="SPY",
                initialize_store=args.initialize,
                clock=clock,
                disk_budget_bytes=args.disk_budget_bytes,
                incremental_only=args.incremental_only,
            )
            if outcome.get("requested"):
                # Snapshot creation is also serialized with the same writer lock.
                with owner_lock(root):
                    snapshot = build_snapshot(
                        sqlite_loader(root / "minutes.sqlite"),
                        calendar,
                        session_key=key,
                        symbol="SPY",
                        now=_utc(clock()),
                    )
                    result = write_snapshot(root / "native-volume.json", snapshot)
                print(
                    json.dumps(
                        {
                            "status": "completed",
                            "requested": True,
                            "quality": snapshot["payload"]["status"],
                            "snapshot_sha256": result["sha256"],
                            "receipt_sha256": outcome["receipt_sha256"],
                        }
                    )
                )
            else:
                print(json.dumps({"status": "idle", "requested": False}))
            return 0
    except Exception:
        # No chained errors, provider payloads, secret paths or config values.
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "reason": "preflight, acquisition or snapshot failed; inspect private checkpoint and receipts before manual recovery",
                }
            )
        )
        return 1


def main(argv=None, **kwargs):
    try:
        with invocation_deadline():
            return _main(argv, **kwargs)
    except InvocationDeadline:
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "reason": "45-second invocation deadline; inspect checkpoint and receipts; no automatic retry",
                }
            )
        )
        return 1
    except RuntimeError:
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "reason": "exclusive main-thread deadline unavailable",
                }
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
