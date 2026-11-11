#!/usr/bin/env python3
"""Read-only local readiness report; never sends alerts or starts a producer.

Use --enabled only for an independently authorized cadence, not a staged
one-shot bootstrap. The existing operations runner consumes JSON and exits:
0 healthy/disabled/verified idle, 1 alert, 2 pending/unknown (recheck, not healthy).
"""

import argparse
import fcntl
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import stat
from zoneinfo import ZoneInfo

from ingestion.realtime.native_volume_cache import disk_inventory
from ingestion.realtime.native_volume_calendar import ManifestCalendar
from ingestion.realtime.native_volume_health import evaluate
from ingestion.realtime.research_intervals import _utc


def _private_stat(st, *, directory=False):
    kind = stat.S_ISDIR if directory else stat.S_ISREG
    if (
        not kind(st.st_mode)
        or st.st_uid != os.geteuid()
        or stat.S_IMODE(st.st_mode) & 0o077
        or (not directory and st.st_nlink != 1)
    ):
        raise ValueError("unsafe local custody")


def _unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _json(dfd, name, cap):
    try:
        fd = os.open(name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dfd)
    except FileNotFoundError:
        return None
    with os.fdopen(fd, "rb") as stream:
        _private_stat(os.fstat(stream.fileno()))
        raw = stream.read(cap + 1)
    if len(raw) > cap:
        raise ValueError("bounded file exceeded")
    return json.loads(raw, object_pairs_hook=_unique)


class InventoryBusy(ValueError):
    """A verified writer currently owns the existing inventory lock."""


def _stable_inventory(dfd):
    """Take a nonblocking shared lock on the existing writer file; never create it."""
    try:
        fd = os.open(
            "writer.lock", os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=dfd
        )
    except FileNotFoundError:
        return disk_inventory(dfd)
    try:
        _private_stat(os.fstat(fd))
        try:
            fcntl.flock(fd, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            raise InventoryBusy("inventory writer active") from None
        return disk_inventory(dfd)
    finally:
        os.close(fd)


def inspect(
    *,
    root,
    manifest,
    digest,
    enabled=False,
    now=None,
    last_exit_code=None,
    disk_budget_bytes=128_000_000,
):
    now = _utc(now or datetime.now(timezone.utc))
    args = dict(
        enabled=enabled,
        now=now,
        last_exit_code=last_exit_code,
        disk_budget_bytes=disk_budget_bytes,
    )
    if not enabled:
        return evaluate(
            **args
        )  # Disabled never opens root/calendar or imports credentials.
    try:
        calendar = ManifestCalendar(manifest, digest)
        day = now.astimezone(ZoneInfo("America/New_York")).date()
        if not calendar.start <= day <= calendar.end:
            raise ValueError("calendar range")
        key = f"XNYS:{day.isoformat()}:RTH"
        sessions = {s.key: s for s in calendar.sessions}
        if key not in sessions:
            return evaluate(**args, session=None)
        session = sessions[key]
        args.update(
            session=session,
            calendar_version=calendar.version,
            baseline_sessions=[s.key for s in calendar.prior_sessions(key, 10)],
        )
    except (OSError, ValueError, TypeError, KeyError, AttributeError):
        return evaluate(**args, read_errors=("calendar_unavailable",))
    # Skip filesystem freshness inspection outside the verified acquisition window.
    preliminary = evaluate(**args)
    if preliminary["status"] == "idle":
        return preliminary
    errors = []
    try:
        root = Path(root)
        if not root.is_absolute() or root.resolve() != root:
            raise ValueError("trusted absolute root required")
        dfd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
        try:
            _private_stat(os.fstat(dfd), directory=True)
            try:
                args["checkpoint"] = _json(dfd, "checkpoint.json", 16384)
            except (OSError, ValueError, TypeError):
                errors.append("checkpoint_unreadable")
            try:
                args["snapshot"] = _json(dfd, "native-volume.json", 1_048_576)
            except (OSError, ValueError, TypeError):
                errors.append("snapshot_unreadable")
            try:
                fs = os.fstatvfs(dfd)
                args["free_bytes"] = fs.f_bavail * fs.f_frsize
                args["disk_bytes"], args["free_bytes"] = _stable_inventory(dfd)
            except InventoryBusy:
                errors.append("inventory_busy")
            except (OSError, ValueError):
                errors.append("disk_unreadable")
            try:
                after = _json(dfd, "checkpoint.json", 16384)
                if after != args.get("checkpoint"):
                    errors.append("observation_raced")
            except (OSError, ValueError, TypeError):
                errors.append("checkpoint_unreadable")
        finally:
            os.close(dfd)
    except (OSError, ValueError, TypeError):
        errors.extend(
            ("checkpoint_unreadable", "snapshot_unreadable", "disk_unreadable")
        )
    if "observation_raced" in errors and len(set(errors)) == 1:
        return evaluate(**args, read_errors=("observation_raced",))
    return evaluate(**args, read_errors=errors)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--enabled", action="store_true")
    parser.add_argument("--root", type=Path)
    parser.add_argument("--calendar-manifest", type=Path)
    parser.add_argument("--calendar-sha256")
    parser.add_argument("--last-exit-code", type=int)
    parser.add_argument("--disk-budget-bytes", type=int, default=128_000_000)
    args = parser.parse_args(argv)
    result = inspect(
        root=args.root,
        manifest=args.calendar_manifest,
        digest=args.calendar_sha256,
        enabled=args.enabled,
        last_exit_code=args.last_exit_code,
        disk_budget_bytes=args.disk_budget_bytes,
    )
    print(json.dumps(result, sort_keys=True))
    return (
        1
        if result["alert"]
        else (2 if result["status"] in ("pending", "unknown") else 0)
    )


if __name__ == "__main__":
    raise SystemExit(main())
