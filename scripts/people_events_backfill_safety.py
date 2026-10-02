"""Small, fail-closed operational guards for the owner-approved GD3 backfill."""

from __future__ import annotations

import hashlib
import json
import os
import re
from contextlib import contextmanager
from datetime import datetime, time, timedelta, timezone
from pathlib import Path

MAX_TRANSACTION_ROWS = 50
THROTTLE_ROWS = 20_000
GROWTH_RESERVE_BYTES = 32 * 1024 * 1024
MANIFEST_NAME = "gd3_manifest.json"
PROGRESS_NAME = "gd3_progress.jsonl"
STATEMENT_TIMEOUT_MS = 120_000
LOCK_TIMEOUT_MS = 5_000
COMMIT_MARGIN_SECONDS = 5
# GD3 issues one mutation statement per transaction. Include the lock bound
# and commit reserve conservatively even though lock wait is statement time.
ENTRY_MARGIN_SECONDS = (STATEMENT_TIMEOUT_MS + LOCK_TIMEOUT_MS) / 1000 + COMMIT_MARGIN_SECONDS
ZERO_COUNT_KEYS = ("add_sources", "tighten_known_at", "enrich_identity", "supersede", "retract",
                   "actor_conflict", "report_only", "unchanged")


class WriteWindowClosed(RuntimeError):
    pass


def write_window_open(now: datetime | None = None, *, margin_seconds: float = 0) -> bool:
    """Stop an hour before nightly backup, and before both daytime blackouts."""
    now = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
    value = now.time().replace(tzinfo=None)
    if (time(2, 30) <= value < time(10, 30)
            or time(10, 58) <= value < time(11, 12)
            or time(13, 25) <= value < time(14, 20)):
        return False
    for cutoff in (time(2, 30), time(10, 58), time(13, 25)):
        start = datetime.combine(now.date(), cutoff, tzinfo=timezone.utc)
        if start < now:
            start += timedelta(days=1)
        if (start - now).total_seconds() <= margin_seconds:
            return False
    return True


def require_write_entry_window() -> None:
    if not write_window_open(margin_seconds=ENTRY_MARGIN_SECONDS):
        raise WriteWindowClosed("GD3 transaction entry refused by blackout or statement/lock/commit margin")


def require_write_window() -> None:
    if not write_window_open(margin_seconds=COMMIT_MARGIN_SECONDS):
        raise WriteWindowClosed("GD3 short transaction refused by operational blackout")


def atomic_json(path: Path, value: dict) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(value, handle, indent=2, sort_keys=True, default=str)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


@contextmanager
def execution_lock(directory: Path):
    """A stale lock requires controller inspection; never guess that it is safe."""
    path = directory / ".gd3_execute.lock"
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as exc:
        raise RuntimeError("GD3 execution lock exists; controller inspection required") from exc
    try:
        os.write(descriptor, (str(os.getpid()) + "\n").encode("ascii"))
        os.fsync(descriptor)
        yield
    finally:
        os.close(descriptor)
        path.unlink()


def scope_identity(receipt: dict, expected_counts: dict) -> dict:
    return {"form345_sha256": receipt["form345_sha256"],
            "codes": sorted(receipt["codes"].split(",")),
            "events_selected": receipt["stats"]["events_selected"],
            "expected_counts_sha256": hashlib.sha256(
                json.dumps(expected_counts, sort_keys=True).encode("utf-8")).hexdigest(),
            "pipeline_version": receipt["pipeline_version"]}


def load_manifest(directory: Path, identity: dict, *, baseline_bytes: int | None,
                  current_bytes: int, max_gb: float, stamp: str) -> tuple[dict, bool]:
    path = directory / MANIFEST_NAME
    progress = directory / PROGRESS_NAME
    if path.exists():
        manifest = json.loads(path.read_text(encoding="utf-8"))
        required = {"version", "scope", "baseline_bytes", "max_bytes", "run_prefix"}
        if set(manifest) != required or manifest["version"] != 1 or manifest["scope"] != identity:
            raise RuntimeError("GD3 manifest is malformed or source/scope changed")
        if (type(manifest["baseline_bytes"]) is not int or manifest["baseline_bytes"] < 0
                or type(manifest["max_bytes"]) is not int or not 0 < manifest["max_bytes"] <= 8_000_000_000
                or not isinstance(manifest["run_prefix"], str)
                or not re.fullmatch(r"gd3-\d{8}T\d{6}Z", manifest["run_prefix"])):
            raise RuntimeError("GD3 manifest has invalid limits or run identity")
        if not progress.is_file():
            raise RuntimeError("GD3 progress is missing; refusing resume")
        if baseline_bytes is not None and baseline_bytes != manifest["baseline_bytes"]:
            raise RuntimeError("GD3 pre-backfill baseline changed")
        if int(max_gb * 1e9) > manifest["max_bytes"]:
            raise RuntimeError("GD3 resume may not raise the original growth cap")
        resumed = True
    else:
        if progress.exists() or baseline_bytes is None or baseline_bytes < 0:
            raise RuntimeError("first GD3 execute requires --baseline-bytes from the pre-GD3 backup")
        manifest = {"version": 1, "scope": identity, "baseline_bytes": baseline_bytes,
                    "max_bytes": int(max_gb * 1e9), "run_prefix": f"gd3-{stamp}"}
        atomic_json(path, manifest)
        with progress.open("x", encoding="utf-8") as handle:
            handle.flush()
            os.fsync(handle.fileno())
        resumed = False
    if current_bytes < manifest["baseline_bytes"]:
        raise RuntimeError("GD3 aggregate size is below the frozen baseline")
    return manifest, resumed


def read_progress(path: Path, prefix: str) -> tuple[int, int, str]:
    batches = rows = 0
    digest = hashlib.sha256()
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            if not line.endswith("\n"):
                raise RuntimeError("GD3 progress has a torn final record")
            record = json.loads(line)
            batch_rows = record.get("batch_rows")
            if (record.get("run_id") != f"{prefix}-b{batches:05d}"
                    or record.get("status") != "SUCCESS"
                    or type(batch_rows) is not int or not 0 < batch_rows <= MAX_TRANSACTION_ROWS
                    or type(record.get("rows_written")) is not int
                    or record.get("rows_written") != rows + batch_rows):
                raise RuntimeError("GD3 progress sequence/counts are malformed")
            rows += batch_rows
            if batches:
                digest.update(b"\n")
            digest.update(batch_audit_record(record["run_id"], batches,
                                            {"insert": batch_rows, "written": batch_rows,
                                             **dict.fromkeys(ZERO_COUNT_KEYS, 0)}))
            batches += 1
    return batches, rows, digest.hexdigest()


def batch_audit_record(run_id: str, batch: int, counts: dict) -> bytes:
    """Bind the sequential identity and every audited counter to the journal."""
    return json.dumps({"run_id": run_id, "batch": batch, "counts": counts},
                      sort_keys=True, separators=(",", ":")).encode("ascii")


def append_progress(path: Path, record: dict) -> None:
    # Exactly one append per committed batch; no growing JSON list to rewrite.
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def validate_resume(*, batches: int, rows: int, database: dict, stored_rows: int,
                    plan_counts: dict, total_rows: int, progress_digest: str) -> None:
    """Audit DB and progress must agree, including crashes between commit/append."""
    if (int(database["batches"]) != batches or int(database["successful"]) != batches
            or int(database["inserted"]) != rows or int(database["written"]) != rows
            or database["progress_digest"] != progress_digest
            or int(database["invalid"]) != 0 or stored_rows != rows
            or int(plan_counts.get("unchanged", 0)) != rows
            or int(plan_counts.get("insert", 0)) != total_rows - rows):
        raise RuntimeError("GD3 progress, DB audit and canonical plan disagree; controller inspection required")
