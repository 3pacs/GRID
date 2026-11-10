"""Explicit service-owned private snapshot bridge, never a public data export.

No network, credentials, scheduler or HTTP-selected path. The configured writer
and reader must run as the same OS user and use an owner-only parent directory.
Hashes detect accidental corruption, not malicious rewrites by that OS user.
"""

from dataclasses import asdict
from datetime import datetime
import hashlib
import json
import math
import os
from pathlib import Path
import re
import secrets
import stat

from ingestion.realtime.native_volume import retained_volume_view
from ingestion.realtime.research_intervals import _utc

SCHEMA = "grid.native-volume.snapshot.v1"
MAX_BYTES = 1_048_576


def _json_value(value):
    if isinstance(value, datetime):
        return _utc(value).isoformat()
    if isinstance(value, dict):
        if any(type(k) is not str for k in value):
            raise ValueError("string keys required")
        return {k: _json_value(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [_json_value(v) for v in value]
    if value is None or type(value) in (str, bool, int, float):
        return value
    raise ValueError("unsupported snapshot type")


def canonical(value):
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode("utf-8")


def _stamp(value):
    if not isinstance(value, str):
        raise ValueError("ISO timestamp required")
    return _utc(datetime.fromisoformat(value))


def validate_payload(payload, generated_at):
    """Validate this one producer identity, flags, clocks and finite metrics."""
    allowed = {
        "schema",
        "symbol",
        "source",
        "price_basis",
        "as_of",
        "session",
        "calendar_version",
        "end_of_completed_window",
        "latest_receipt_at",
        "receipt_age_seconds",
        "event_age_seconds",
        "completed_minutes",
        "status",
        "reason",
        "cumulative_shares",
        "bar_vwap_estimate",
        "rvol",
        "baseline_sessions",
        "baseline_errors",
        "exact_trade_vwap",
        "signal_confirmation",
        "cross_source_reconciled",
        "live",
        "vwap_basis",
        "rvol_basis",
        "source_revisions",
        "baseline_median_shares",
        "served_at",
        "historical_request",
    }
    if not isinstance(payload, dict) or set(payload) - allowed:
        raise ValueError("unrecognized payload fields")
    generated = _stamp(generated_at)
    if (payload.get("schema"), payload.get("source"), payload.get("price_basis")) != (
        "grid.native-volume.v1",
        "schwab_pricehistory",
        "provider_unspecified",
    ) or not re.fullmatch(r"[A-Z0-9.$^/-]{1,12}", payload.get("symbol", "")):
        raise ValueError("unsupported producer identity")
    if payload.get("status") not in ("ready", "degraded", "unavailable"):
        raise ValueError("invalid quality status")
    if any(
        payload.get(k) is not False
        for k in (
            "exact_trade_vwap",
            "signal_confirmation",
            "cross_source_reconciled",
            "live",
            "historical_request",
        )
    ):
        raise ValueError("unsupported interpretation flags")
    if (
        _stamp(payload.get("as_of")) != generated
        or _stamp(payload.get("served_at")) != generated
    ):
        raise ValueError("snapshot must describe its generation cutoff")
    for key in (
        "cumulative_shares",
        "bar_vwap_estimate",
        "rvol",
        "baseline_median_shares",
    ):
        value = payload.get(key)
        if value is not None and (
            type(value) not in (int, float)
            or not math.isfinite(value)
            or value < 0
            or value > 2**53 - 1
        ):
            raise ValueError("invalid metric")
    shares = payload.get("cumulative_shares")
    if shares is not None and shares != int(shares):
        raise ValueError("integer cumulative shares required")
    count = payload.get("completed_minutes")
    if type(count) is not int or not 0 <= count <= 1440:
        raise ValueError("invalid minute count")
    if payload["status"] != "unavailable":
        for key in ("end_of_completed_window", "latest_receipt_at"):
            age = (generated - _stamp(payload.get(key))).total_seconds()
            if not 0 <= age <= 120:
                raise ValueError("stale or future descriptive data")
        if shares is None:
            raise ValueError("descriptive shares required")
    if payload["status"] == "ready" and any(
        payload.get(k) is None
        for k in ("bar_vwap_estimate", "rvol", "baseline_median_shares")
    ):
        raise ValueError("ready requires all metrics")
    if (
        not payload.get("calendar_version")
        or len(payload.get("baseline_sessions", [])) != 10
    ):
        raise ValueError("calendar provenance required")
    canonical(payload)  # reject nested NaN and nonserializable material too


def build_snapshot(loader, calendar, *, session_key, symbol, now):
    """Project the same retained view used by GRID. No mutation or backfill."""
    now = _utc(now)
    consulted = []

    def tracked_loader(*args):
        rows = tuple(loader(*args))
        consulted.extend(
            hashlib.sha256(canonical(_json_value(asdict(row)))).hexdigest()
            for row in rows
        )
        return rows

    payload = retained_volume_view(
        tracked_loader,
        calendar,
        session_key=session_key,
        symbol=symbol,
        source="schwab_pricehistory",
        price_basis="provider_unspecified",
        as_of=now,
    )
    payload.update(served_at=now, historical_request=False)
    payload = _json_value(payload)
    generated = now.isoformat()
    validate_payload(payload, generated)
    envelope = {
        "schema": SCHEMA,
        "generated_at": generated,
        "payload": payload,
        "payload_sha256": hashlib.sha256(canonical(payload)).hexdigest(),
        "provenance": {
            "producer": "GRID retained native-minute view",
            "retained_rows_consulted": len(consulted),
            "retained_rows_sha256": hashlib.sha256(
                canonical(sorted(consulted))
            ).hexdigest(),
            "integrity_scope": "content integrity only; trust requires verified local producer and owner-only custody",
        },
    }
    if len(canonical(envelope)) > MAX_BYTES:
        raise ValueError("snapshot exceeds size bound")
    return envelope


def write_snapshot(path, envelope):
    """Atomic private file replacement at an operator-configured local path.

    Parent must exist, be same-UID owner-only, and not be a symlink. Open dirfd
    pins it across replacement. Existing targets must be same-UID0600 regular
    files. Never chmod a foreign/insecure file or follow a target symlink.
    """
    if envelope.get("schema") != SCHEMA:
        raise ValueError("snapshot schema")
    payload = envelope.get("payload", {})
    validate_payload(payload, envelope.get("generated_at"))
    if hashlib.sha256(canonical(payload)).hexdigest() != envelope.get("payload_sha256"):
        raise ValueError("payload hash mismatch")
    if set(envelope) != {
        "schema",
        "generated_at",
        "payload",
        "payload_sha256",
        "provenance",
    }:
        raise ValueError("unrecognized envelope fields")
    provenance = envelope.get("provenance", {})
    if set(provenance) != {
        "producer",
        "retained_rows_consulted",
        "retained_rows_sha256",
        "integrity_scope",
    }:
        raise ValueError("unrecognized provenance fields")
    count = provenance.get("retained_rows_consulted")
    if (
        type(count) is not int
        or not 0 <= count <= 1_100_000
        or not re.fullmatch(r"[0-9a-f]{64}", provenance.get("retained_rows_sha256", ""))
    ):
        raise ValueError("invalid retained-row provenance")
    raw = canonical(envelope)
    if len(raw) > MAX_BYTES:
        raise ValueError("snapshot exceeds size bound")
    target = Path(path).absolute()
    if target.name in ("", ".", ".."):
        raise ValueError("invalid snapshot name")
    if target.parent.resolve() != target.parent:
        raise ValueError("symlink ancestry refused")
    flags = os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW
    fd = os.open(target.parent, flags)
    temporary = None
    try:
        parent = os.fstat(fd)
        if parent.st_uid != os.geteuid() or stat.S_IMODE(parent.st_mode) & 0o077:
            raise ValueError("same-user owner-only parent required")

        def target_check():
            try:
                old = os.stat(target.name, dir_fd=fd, follow_symlinks=False)
            except FileNotFoundError:
                return
            if (
                not stat.S_ISREG(old.st_mode)
                or old.st_uid != os.geteuid()
                or stat.S_IMODE(old.st_mode) != 0o600
            ):
                raise ValueError("same-user0600 regular target required")

        target_check()
        temporary = "." + target.name + "." + secrets.token_hex(12)
        out = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
            0o600,
            dir_fd=fd,
        )
        with os.fdopen(out, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        target_check()
        os.replace(temporary, target.name, src_dir_fd=fd, dst_dir_fd=fd)
        temporary = None
        os.fsync(fd)
    finally:
        if temporary is not None:
            os.unlink(temporary, dir_fd=fd)
        os.close(fd)
    return {"bytes": len(raw), "sha256": hashlib.sha256(raw).hexdigest()}
