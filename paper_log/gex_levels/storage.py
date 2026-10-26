"""Append-only JSONL storage for the GEX-levels paper log.

Pre-registration / task spec (4): "Storage: append-only JSONL in
``--log-dir``; every record has ``prev_sha256`` (SHA-256 of the previous
line's exact bytes), ``code_sha`` ..., ``run_at`` ..., ``session_date``,
``kind``. The first record ever written carries ``prereg_sha256`` ...
Use a lock file so two runs can't interleave. Never rewrite or delete
lines."

Hash-chain convention (pinned here since the pre-registration doesn't spell
out newline handling, and any external verifier needs an exact recipe):
each record is serialized as canonical JSON (``sort_keys=True``, no extra
whitespace) forming one line's content; ``prev_sha256`` is the SHA-256 of
that exact JSON text, UTF-8 encoded, WITHOUT a trailing newline. The file
on disk separates records with a single ``\\n`` (written in binary mode, so
Windows can never silently turn that into ``\\r\\n``); the newline itself is
not part of what gets hashed, on either side of the chain.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import socket
import subprocess
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from loguru import logger as log

from paper_log.gex_levels.config import PREREG_SHA256

LOG_FILENAME = "gex_levels_v1.jsonl"
LOCK_FILENAME = ".gex_levels_v1.lock"

LOCK_TIMEOUT_S = 30.0
LOCK_STALE_AFTER_S = 900.0  # 15 minutes — generous vs. a job that should run in seconds


class LockTimeoutError(RuntimeError):
    """Raised when the append-only log's lock could not be acquired."""


def _json_default(obj: Any) -> str:
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError(f"object of type {type(obj).__name__} is not JSON serializable")


def _canonical_json(record: dict) -> bytes:
    """Deterministic JSON bytes for a record — no trailing newline."""
    return json.dumps(record, sort_keys=True, separators=(",", ":"), default=_json_default).encode("utf-8")


def _iter_raw_lines(path: Path) -> Iterator[bytes]:
    """Yield each non-empty line's bytes, stripped of its trailing newline."""
    if not path.exists():
        return
    with open(path, "rb") as f:
        for raw in f:
            line = raw.rstrip(b"\n").rstrip(b"\r")
            if line:
                yield line


def resolve_code_sha(repo_root: Path, override: str | None = None) -> str:
    """The running code's git commit.

    Production: the installer writes a ``VERSION`` file next to the
    archived code (task spec 8) — that is the only source of truth once
    installed. ``override`` and the ``git rev-parse`` fallback exist purely
    for local/dev runs from a working tree that has no VERSION file; tests
    always pass an explicit value and never hit either fallback path for
    real.
    """
    if override:
        return override

    version_file = Path(repo_root) / "VERSION"
    if version_file.exists():
        sha = version_file.read_text(encoding="utf-8").strip()
        if sha:
            return sha

    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=True,
            timeout=5,
        )
        return result.stdout.strip()
    except Exception as exc:  # noqa: BLE001 — surfaced as a clear, single error
        raise RuntimeError(
            f"could not resolve code_sha: no VERSION file at {version_file} "
            f"and `git rev-parse HEAD` failed ({exc})"
        ) from exc


@dataclass(frozen=True)
class ChainVerification:
    ok: bool
    n_records: int
    first_broken_index: int | None
    detail: str | None


class PaperLogStore:
    """One append-only JSONL file plus its lock, scoped to ``log_dir``."""

    def __init__(self, log_dir: Path) -> None:
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.log_path = self.log_dir / LOG_FILENAME
        self.lock_path = self.log_dir / LOCK_FILENAME

    # ── writing ────────────────────────────────────────────────────────

    def append(self, record: dict[str, Any]) -> dict[str, Any]:
        """Append one record. Adds ``prev_sha256`` (and, only for the very
        first record ever written to this file, ``prereg_sha256``). Never
        rewrites or removes any existing line.

        Returns the record as JSON round-tripped (``json.loads`` of the
        exact bytes just written) rather than the pre-serialization Python
        dict — so a ``date``/``datetime`` field looks the same to the
        caller here as it will to every future ``read_all()`` (which can
        only ever see the JSON form). Without this, a value like
        ``session_date`` would be a real ``date`` object immediately after
        a fresh ``append()`` but an ISO string on every later read of the
        same record — a divergence with no upside.
        """
        record = dict(record)
        with self._locked():
            prev_hash = self._last_line_hash()
            record["prev_sha256"] = prev_hash
            if prev_hash is None:
                record["prereg_sha256"] = PREREG_SHA256

            line = _canonical_json(record)
            with open(self.log_path, "ab") as f:
                f.write(line + b"\n")

        return json.loads(line)

    def _last_line_hash(self) -> str | None:
        last_line: bytes | None = None
        for line in _iter_raw_lines(self.log_path):
            last_line = line
        if last_line is None:
            return None
        return hashlib.sha256(last_line).hexdigest()

    # ── reading ────────────────────────────────────────────────────────

    def read_all(self) -> list[dict[str, Any]]:
        """All records, in file order. Read-only; never used to mutate."""
        return [json.loads(line) for line in _iter_raw_lines(self.log_path)]

    def verify_chain(self) -> ChainVerification:
        """Walk the file and confirm every ``prev_sha256`` matches the hash
        of the line before it, and the first record carries the correct
        ``prereg_sha256``. Detects any tampering (edited, reordered, or
        deleted line) without needing a second, separate copy to diff
        against."""
        lines = list(_iter_raw_lines(self.log_path))
        prev_hash: str | None = None

        for i, line in enumerate(lines):
            record = json.loads(line)
            if record.get("prev_sha256") != prev_hash:
                return ChainVerification(
                    ok=False, n_records=len(lines), first_broken_index=i,
                    detail=(
                        f"record {i}: prev_sha256={record.get('prev_sha256')!r} "
                        f"but the previous line actually hashes to {prev_hash!r}"
                    ),
                )
            if i == 0 and record.get("prereg_sha256") != PREREG_SHA256:
                return ChainVerification(
                    ok=False, n_records=len(lines), first_broken_index=0,
                    detail=(
                        f"first record prereg_sha256={record.get('prereg_sha256')!r}, "
                        f"expected {PREREG_SHA256!r}"
                    ),
                )
            prev_hash = hashlib.sha256(line).hexdigest()

        return ChainVerification(ok=True, n_records=len(lines), first_broken_index=None, detail=None)

    # ── locking ────────────────────────────────────────────────────────

    @contextlib.contextmanager
    def _locked(self):
        deadline = time.monotonic() + LOCK_TIMEOUT_S
        payload = (
            f"pid={os.getpid()} host={socket.gethostname()} "
            f"acquired_at={datetime.now(timezone.utc).isoformat()}"
        ).encode("utf-8")

        while True:
            try:
                fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                try:
                    os.write(fd, payload)
                finally:
                    os.close(fd)
                break
            except FileExistsError:
                age_s = self._lock_age_s()
                if age_s is None:
                    continue  # released between the failed open and stat(); retry now
                if age_s > LOCK_STALE_AFTER_S:
                    log.warning(
                        "paper_log.gex_levels: lock {p} is {age:.0f}s old "
                        "(> {stale:.0f}s) — treating as stale and taking over",
                        p=self.lock_path, age=age_s, stale=LOCK_STALE_AFTER_S,
                    )
                    self.lock_path.unlink(missing_ok=True)
                    continue
                if time.monotonic() >= deadline:
                    holder = self._read_lock_holder()
                    raise LockTimeoutError(
                        f"could not acquire {self.lock_path} within {LOCK_TIMEOUT_S:.0f}s "
                        f"(held by: {holder})"
                    )
                time.sleep(0.2)

        try:
            yield
        finally:
            self.lock_path.unlink(missing_ok=True)

    def _lock_age_s(self) -> float | None:
        try:
            return time.time() - self.lock_path.stat().st_mtime
        except FileNotFoundError:
            return None

    def _read_lock_holder(self) -> str:
        try:
            return self.lock_path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return "<unknown>"
