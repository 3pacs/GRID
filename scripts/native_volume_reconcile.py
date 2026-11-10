#!/usr/bin/env python3
"""Explicit local receipt recovery / snapshot republish. Never calls a provider.

Use --receipt-sha256 for a known immutable receipt in the trusted root. A failed
request without a saved, matching complete receipt cannot be recovered here.
No blind reset, new backfill, cadence or retry is authorized by this command.
"""

import argparse
from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat

from ingestion.realtime.minute_retention import sqlite_loader
from ingestion.realtime.native_volume_cache import (
    _private,
    _validate,
    _encoded,
    MAX_BYTES,
    retain_receipt,
)
from ingestion.realtime.native_minutes import select_as_of
from ingestion.realtime.native_volume_worker import (
    owner_lock,
    _read_checkpoint,
    _checkpoint,
)
from ingestion.realtime.native_volume_snapshot import build_snapshot, write_snapshot
from ingestion.realtime.research_intervals import _utc, rollup
from scripts.native_volume_once import invocation_lock, default_calendar


def _read_receipt(root, digest):
    if not isinstance(digest, str) or not re.fullmatch(r"[0-9a-f]{64}", digest):
        raise ValueError("explicit immutable receipt SHA256 required")
    _private(root / "receipts", directory=True)
    p = _private(root / "receipts" / (digest + ".json"))
    fd = os.open(p, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as stream:
        info = os.fstat(stream.fileno())
        if (
            not stat.S_ISREG(info.st_mode)
            or info.st_nlink != 1
            or info.st_uid != os.geteuid()
            or stat.S_IMODE(info.st_mode) & 0o077
        ):
            raise ValueError("unsafe receipt custody")
        raw = stream.read(MAX_BYTES + 1)
    if len(raw) > MAX_BYTES or hashlib.sha256(raw).hexdigest() != digest:
        raise ValueError("receipt hash mismatch")
    value = json.loads(raw)
    if _encoded(value) != raw:
        raise ValueError("canonical receipt required")
    parsed = _validate(value)
    return value, parsed


def _match(state, receipt, calendar, now, *, republish_only):
    if state is None:
        raise ValueError("checkpoint required; no inferred reset")
    statuses = (
        ("complete",)
        if republish_only
        else ("failed", "in_progress", "coverage_failed", "complete")
    )
    if state.get("status") not in statuses or state.get("mode") not in (
        "bootstrap",
        "incremental",
    ):
        raise ValueError("checkpoint state not recoverable")
    symbol, key = state.get("symbol"), state.get("session")
    if symbol != "SPY" or receipt["request"]["symbol"] != symbol:
        raise ValueError("identity mismatch")
    session = calendar(key)
    boundary = _utc(datetime.fromisoformat(state["window_end"]))
    started = _utc(datetime.fromisoformat(state["started_at"]))
    requested = _utc(datetime.fromisoformat(receipt["requested_at"]))
    received = _utc(datetime.fromisoformat(receipt["received_at"]))
    op, cl = _utc(session.opens), _utc(session.closes)
    if boundary.second or boundary.microsecond or not op < boundary <= cl:
        raise ValueError("invalid checkpoint boundary")
    if not started <= requested <= received <= now or boundary > started - timedelta(
        seconds=5
    ):
        raise ValueError("receipt/checkpoint clock mismatch")
    prior = tuple(calendar.prior_sessions(key, 10))
    if len(prior) != 10:
        raise ValueError("exact expected prior sessions required")
    start = (
        _utc(prior[0].opens)
        if state["mode"] == "bootstrap"
        else max(op, boundary - timedelta(minutes=3))
    )
    end = boundary - timedelta(milliseconds=1)
    q = receipt["request"]
    if q["startDate"] != int(start.timestamp() * 1000) or q["endDate"] != int(
        end.timestamp() * 1000
    ):
        raise ValueError("receipt is not the checkpoint request")
    return session, prior, boundary, received


def _preserve_checkpoint(root, state):
    """Keep original state before any recovery write, idempotently and privately."""
    raw = json.dumps(
        state, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    digest = hashlib.sha256(raw).hexdigest()
    target = root / f"checkpoint.before-reconcile.{digest}.json"
    try:
        fd = os.open(
            target, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600
        )
    except FileExistsError:
        _private(target)
        fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as stream:
            if stream.read(16385) != raw:
                raise ValueError("checkpoint archive conflict")
    else:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return digest


def _coverage(loader, session, prior, boundary, received):
    """Require all expected baseline + session data, never just HTTP success."""
    for s in (*prior, session):
        start = _utc(s.opens)
        end = boundary if s == session else _utc(s.closes)
        rows = loader("SPY", "schwab_pricehistory", s, received)
        selected = select_as_of(
            rows,
            symbol="SPY",
            source="schwab_pricehistory",
            price_basis="provider_unspecified",
            as_of=received,
            opens=start,
            closes=end,
        )
        expected = {
            start + timedelta(minutes=i)
            for i in range(int((end - start).total_seconds() // 60))
        }
        if {b.ts for b in selected} != expected:
            raise ValueError("retained coverage incomplete; checkpoint remains blocked")
        checked = rollup(selected, s, "1m", as_of=received)
        by_start = {b.start: b for b in checked}
        for t in expected:
            b = by_start.get(t)
            if (
                b is None
                or not b.complete
                or b.volume is None
                or b.last_source_available_at < b.end + timedelta(seconds=5)
            ):
                raise ValueError("invalid or unsettled retained coverage")


def reconcile(*, root, calendar, receipt_sha256, republish_only=False, clock=None):
    """Only local files. Explicit replay may append the already-captured receipt."""
    clock = clock or (lambda: datetime.now(timezone.utc))
    root = Path(root)
    if not root.is_absolute() or root.resolve() != root:
        raise ValueError("trusted absolute root required")
    _private(root, directory=True)
    with invocation_lock(root), owner_lock(root):
        now = _utc(clock())
        state = _read_checkpoint(root / "checkpoint.json")
        receipt, parsed = _read_receipt(root, receipt_sha256)
        session, prior, boundary, received = _match(
            state, receipt, calendar, now, republish_only=republish_only
        )
        if (
            state.get("receipt_sha256") is not None
            and state["receipt_sha256"] != receipt_sha256
        ):
            raise ValueError("checkpoint already binds a different receipt")
        if republish_only and state.get("receipt_sha256") != receipt_sha256:
            raise ValueError("complete checkpoint receipt binding required")
        _private(root / "minutes.sqlite")
        archive = _preserve_checkpoint(root, state)
        inserted = 0
        if not republish_only:
            result = retain_receipt(
                receipt, store=root / "minutes.sqlite", receipts_dir=root / "receipts"
            )
            inserted = result["inserted"]
        loader = sqlite_loader(root / "minutes.sqlite")
        _coverage(loader, session, prior, boundary, received)
        if not republish_only and state["status"] != "complete":
            state = dict(
                state,
                status="complete",
                receipt_sha256=receipt_sha256,
                received_at=received.isoformat(),
                closed_minutes=len(parsed.bars),
                reconciled_at=now.isoformat(),
                recovered_from_checkpoint_sha256=archive,
            )
            _checkpoint(root, state)
        # If projection/write fails, completed checkpoint is recoverable via
        # --republish-only. No second provider request is needed or performed.
        snapshot = build_snapshot(
            loader, calendar, session_key=session.key, symbol="SPY", now=now
        )
        written = write_snapshot(root / "native-volume.json", snapshot)
        return dict(
            status="republished" if republish_only else "reconciled",
            requested=False,
            inserted=inserted,
            quality=snapshot["payload"]["status"],
            receipt_sha256=receipt_sha256,
            snapshot_sha256=written["sha256"],
            preserved_checkpoint_sha256=archive,
        )


def main(argv=None, *, calendar_factory=None, clock=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--receipt-sha256", required=True)
    parser.add_argument("--republish-only", action="store_true")
    parser.add_argument("--calendar-manifest", required=True, type=Path)
    parser.add_argument("--calendar-sha256", required=True)
    args = parser.parse_args(argv)
    try:
        calendar = (
            calendar_factory()
            if calendar_factory
            else default_calendar(args.calendar_manifest, args.calendar_sha256)
        )
        result = reconcile(
            root=args.root,
            calendar=calendar,
            receipt_sha256=args.receipt_sha256,
            republish_only=args.republish_only,
            clock=clock,
        )
        print(json.dumps(result))
        return 0
    except Exception:
        print(
            json.dumps(
                {
                    "status": "blocked",
                    "requested": False,
                    "reason": "matching captured receipt, coverage and private custody required; no reset or fetch performed",
                }
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
