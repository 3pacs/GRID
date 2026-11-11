"""Explicit private native-minute retention; no timer, credentials or live wiring.

Caller initializes a dedicated minute_retention store and supplies a Schwab client
configured with max_retries=0. A receipt is durably saved before bar append. If the
append fails, replay that receipt instead of fetching again. Filesystem owners can
alter files: hashes are integrity checks, not cryptographic producer attestation.
"""

from datetime import datetime, timedelta, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import tempfile

from ingestion.realtime.native_minutes import schwab_minutes
from ingestion.realtime.minute_retention import append
from ingestion.realtime.research_intervals import _utc

MAX_BYTES = 12_000_000
SCHEMA = "grid.native-minute-receipt.v1"
# Keep this much filesystem capacity after the existing two-receipt write allowance.
MIN_FREE_BYTES = 64_000_000
MAX_INVENTORY_FILES = 10_000


class NativeVolumeFailure(ValueError, RuntimeError):
    """Fixed stage only; never carries a provider message or exception type."""

    def __init__(self, stage):
        self.stage = stage if stage in {
            "request", "capture", "validation", "append", "coverage"
        } else "unknown"
        message = "coverage incomplete" if self.stage == "coverage" else f"{self.stage} failed"
        super().__init__(f"native volume {message}; inspect retained receipts; no automatic retry")


def failure_stage(exc, fallback="unknown"):
    return exc.stage if type(exc) is NativeVolumeFailure else fallback


def disk_inventory(dfd):
    """Bounded stat-only accounting under the already-open owner-private root.

    Include checkpoint archives, snapshot and transient SQLite files as well as
    retained minutes and receipts. No deletion or retention compaction occurs.
    """

    def private(st, directory=False):
        kind = stat.S_ISDIR if directory else stat.S_ISREG
        if (
            not kind(st.st_mode)
            or st.st_uid != os.geteuid()
            or stat.S_IMODE(st.st_mode) & 0o077
            or (not directory and st.st_nlink != 1)
        ):
            raise ValueError("unsafe disk inventory custody")

    private(os.fstat(dfd), directory=True)
    used = 0
    count = 0
    seen = set()
    with os.scandir(dfd) as entries:
        for entry in entries:
            count += 1
            if count > MAX_INVENTORY_FILES:
                raise ValueError("native root inventory bound exceeded")
            seen.add(entry.name)
            st = entry.stat(follow_symlinks=False)
            if entry.name == "receipts":
                private(st, directory=True)
                continue
            private(st)
            used += max(st.st_size, getattr(st, "st_blocks", 0) * 512)
    if not {"minutes.sqlite", "receipts"} <= seen:
        raise ValueError("incomplete native store inventory")
    rfd = os.open("receipts", os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=dfd)
    try:
        private(os.fstat(rfd), directory=True)
        with os.scandir(rfd) as entries:
            for number, entry in enumerate(entries, 1):
                if number > MAX_INVENTORY_FILES:
                    raise ValueError("receipt inventory bound exceeded")
                st = entry.stat(follow_symlinks=False)
                private(st)
                if st.st_size > MAX_BYTES:
                    raise ValueError("receipt size bound exceeded")
                used += max(st.st_size, getattr(st, "st_blocks", 0) * 512)
    finally:
        os.close(rfd)
    fs = os.fstatvfs(dfd)
    available = fs.f_bavail * fs.f_frsize
    if available < 0:
        raise ValueError("filesystem available capacity unknown")
    return used, available


def _private(path, *, directory=False):
    p = Path(path)
    s = p.lstat()
    if not stat.S_ISDIR(s.st_mode) if directory else not stat.S_ISREG(s.st_mode):
        raise ValueError("private regular path required")
    if s.st_uid != os.geteuid() or stat.S_IMODE(s.st_mode) & 0o077:
        raise ValueError("owner-only private path required")
    return p


def _encoded(value):
    raw = json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()
    if len(raw) > MAX_BYTES:
        raise ValueError("receipt exceeds size bound")
    return raw


def _validate(receipt):
    if (
        receipt.get("schema") != SCHEMA
        or receipt.get("source") != "schwab_pricehistory"
    ):
        raise ValueError("unsupported receipt")
    q = receipt["request"]
    symbol = q["symbol"]
    if not isinstance(symbol, str) or not re.fullmatch(
        r"[A-Z][A-Z0-9.\-]{0,11}", symbol
    ):
        raise ValueError("invalid symbol")
    if (
        q.get("frequencyType") != "minute"
        or type(q.get("frequency")) is not int
        or q["frequency"] != 1
        or q.get("needExtendedHoursData") != "true"
    ):
        raise ValueError("native minute request required")
    start, end = q["startDate"], q["endDate"]
    if (
        type(start) is not int
        or type(end) is not int
        or start < 0
        or not 0 < end - start <= 31 * 86400000
    ):
        raise ValueError("invalid bounded date range")
    received = _utc(datetime.fromisoformat(receipt["received_at"]))
    requested = _utc(datetime.fromisoformat(receipt["requested_at"]))
    if received < requested or end > int(requested.timestamp() * 1000):
        raise ValueError("invalid receipt clocks")
    parsed = schwab_minutes(
        receipt["payload"],
        requested_symbol=symbol,
        frequency_type="minute",
        frequency=1,
        received_at=received,
    )
    for b in receipt["payload"]["candles"]:
        if not start <= b["datetime"] <= end:
            raise ValueError("provider returned outside requested range")
    return parsed


def retain_receipt(receipt, *, store, receipts_dir):
    """Capture privately, validate, publish receipt, append. Replay inserts zero."""
    root = _private(receipts_dir, directory=True)
    _private(store)
    raw = _encoded(receipt)
    digest = hashlib.sha256(raw).hexdigest()
    capture = root / ("unvalidated-" + digest + ".json")
    target = capture
    # Link a complete fsynced private tempfile atomically, never overwrite a receipt.
    fd, temporary = tempfile.mkstemp(prefix=".native-receipt-", dir=root)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(raw)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.link(temporary, target, follow_symlinks=False)
        except FileExistsError:
            _private(target)
            fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
            with os.fdopen(fd, "rb") as f:
                if f.read(MAX_BYTES + 1) != raw:
                    raise ValueError("receipt hash conflict")
        fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        os.unlink(temporary)
    try:
        parsed = _validate(receipt)
    except Exception:
        raise NativeVolumeFailure("validation") from None
    # Only a validated capture may acquire the accepted receipt filename.
    target = root / (digest + ".json")
    try:
        os.link(capture, target, follow_symlinks=False)
    except FileExistsError:
        _private(target)
        fd = os.open(target, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as f:
            if f.read(MAX_BYTES + 1) != raw:
                raise NativeVolumeFailure("capture") from None
    os.unlink(capture)
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    try:
        inserted = append(store, parsed.bars)
    except Exception:
        raise NativeVolumeFailure("append") from None
    return {
        "receipt_sha256": digest,
        "receipt_file": str(target),
        "payload_sha256": parsed.payload_sha256,
        "inserted": inserted,
        "closed_minutes": len(parsed.bars),
        "excluded_forming": parsed.excluded_forming,
        "received_at": parsed.received_at.isoformat(),
        "live": False,
    }


def fetch_and_retain(client, *, symbol, start, end, store, receipts_dir, clock=None):
    """One bounded client invocation; no automatic retry or implicit initialization."""
    clock = clock or (lambda: datetime.now(timezone.utc))
    before = _utc(clock())
    start, end = _utc(start), _utc(end)
    if not isinstance(symbol, str) or not re.fullmatch(
        r"[A-Z][A-Z0-9.\-]{0,11}", symbol
    ):
        raise ValueError("invalid symbol")
    if start >= end or end > before or end - start > timedelta(days=31):
        raise ValueError("invalid bounded date range")
    _private(store)
    _private(receipts_dir, directory=True)
    retry_policy = getattr(client, "_max_retries", None)
    if type(retry_policy) is not int or retry_policy != 0:
        raise ValueError("client retries must be disabled")
    q = dict(
        symbol=symbol,
        startDate=int(start.timestamp() * 1000),
        endDate=int(end.timestamp() * 1000),
        frequencyType="minute",
        frequency=1,
        needExtendedHoursData="true",
    )
    try:
        payload = client.price_history(
            symbol, **{k: v for k, v in q.items() if k != "symbol"}
        )
    except Exception:
        raise NativeVolumeFailure("request") from None
    receipt = dict(
        schema=SCHEMA,
        source="schwab_pricehistory",
        request=q,
        requested_at=before.isoformat(),
        received_at=_utc(clock()).isoformat(),
        payload=payload,
    )
    try:
        return retain_receipt(receipt, store=store, receipts_dir=receipts_dir)
    except NativeVolumeFailure:
        raise
    except Exception:
        raise NativeVolumeFailure("capture") from None
