"""Append-only, hash-chained JSONL: the E2 ledger and the source-log verifiers.

Ledger format (the same convention as the S10 forward log,
``analysis/research_forward_log.py``, re-implemented here so the guarantee
is pinned by the E2 manifest rather than borrowed from a file E2 does not
own): one canonical JSON object per line (``sort_keys``, compact separators,
UTF-8, ``allow_nan=False``), lines separated by a single ``\\n`` written in
binary mode. Every record carries ``prev_sha256`` = sha256 of the previous
line's exact bytes (``None`` for the first). The first record is the
``header`` and pins the manifest sha256 of the code that may extend the
ledger. After every append ``(records, head_sha256)`` is appended to a
chained anchor file; the anchors are what the off-host witness
(``evals/e2/witness.py``) commits, and the ledger is tamper-evident only
relative to a copy of them held off-host.

There is no API here that rewrites, truncates or deletes a line.
"""

from __future__ import annotations

import contextlib
import hashlib
import json
import os
import time
from pathlib import Path
from typing import Iterator

LOCK_TIMEOUT_S = 30.0
LOCK_STALE_AFTER_S = 900.0


class ChainError(RuntimeError):
    """A ledger or a source log fails verification."""


def canonical(record: dict) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def digest(obj) -> str:
    """sha256 of an object's canonical JSON."""
    return sha256_hex(canonical(obj))


def raw_lines(path: Path) -> Iterator[bytes]:
    """Non-empty lines of ``path`` without their line terminator (read-only)."""
    path = Path(path)
    if not path.exists():
        return
    with open(path, "rb") as stream:
        for raw in stream:
            line = raw.rstrip(b"\n").rstrip(b"\r")
            if line:
                yield line


def verify_source_chain(path: Path, *, prereg_sha256: str | None = None,
                        require_canonical: bool = True) -> list[tuple[bytes, dict]]:
    """Verify a stream's own ``prev_sha256`` chain and return ``(line, record)`` pairs.

    ``prev_sha256`` of record ``i`` must equal sha256 of line ``i-1``'s exact
    bytes and be ``None`` for the first. ``prereg_sha256``, when given, must
    be carried by the first record. Raises :class:`ChainError` on any break.
    """
    out: list[tuple[bytes, dict]] = []
    previous = None
    for i, line in enumerate(raw_lines(path)):
        try:
            record = json.loads(line)
        except ValueError as exc:
            raise ChainError(f"{Path(path).name} line {i}: not JSON ({exc})") from None
        if require_canonical and line != json.dumps(record, sort_keys=True, separators=(",", ":")).encode("utf-8"):
            raise ChainError(f"{Path(path).name} line {i}: not canonical JSON")
        if record.get("prev_sha256") != previous:
            raise ChainError(f"{Path(path).name} line {i}: prev_sha256 does not match the previous line")
        if i == 0 and prereg_sha256 is not None and record.get("prereg_sha256") != prereg_sha256:
            raise ChainError(f"{Path(path).name}: first record does not carry the pinned prereg_sha256")
        previous = sha256_hex(line)
        out.append((line, record))
    return out


class Ledger:
    """The E2 ledger for one scoreboard version, in ``board_dir``."""

    def __init__(self, board_dir: Path, version: str) -> None:
        self.board_dir = Path(board_dir)
        self.version = version
        self.path = self.board_dir / f"e2_scoreboard_{version}.jsonl"
        self.anchor_path = self.board_dir / f"e2_scoreboard_{version}.anchors.jsonl"
        self.lock_path = self.board_dir / f".e2_scoreboard_{version}.lock"

    # -- reading (never writes, never creates) ---------------------------------------

    def read_all(self) -> list[dict]:
        return [json.loads(line) for line in raw_lines(self.path)]

    def verify(self, external_anchors: Path | None = None, *, manifest_sha256: str | None = None) -> dict:
        """Walk the chain, then the anchors (local, and an off-host copy if given).

        Returns ``{"ok", "records", "head_sha256", "anchored_records", "detail"}``.
        ``manifest_sha256``, when given, must be the header's.
        """
        lines = list(raw_lines(self.path))
        heads: list[str] = []
        previous = None
        for i, line in enumerate(lines):
            try:
                record = json.loads(line)
            except ValueError:
                return _broken(len(lines), i, "line is not JSON")
            if line != canonical(record):
                return _broken(len(lines), i, "line is not canonical JSON")
            if record.get("prev_sha256") != previous:
                return _broken(len(lines), i, "prev_sha256 does not match the previous line")
            if i == 0:
                if record.get("kind") != "header" or record.get("e2_version") != self.version:
                    return _broken(len(lines), 0, "first record is not this version's header")
                if manifest_sha256 is not None and record.get("manifest_sha256") != manifest_sha256:
                    return _broken(len(lines), 0, "header pins a different E2 manifest (other code): "
                                   "a changed scoreboard is a new version with its own ledger")
            elif record.get("kind") == "header":
                return _broken(len(lines), i, "second header")
            previous = sha256_hex(line)
            heads.append(previous)
        if lines and not self.anchor_path.exists():
            return _broken(len(lines), len(lines), "anchor file missing")
        anchored = 0
        for source in (self.anchor_path, external_anchors):
            if source is None:
                continue
            problem, count = check_anchors(Path(source), heads)
            if problem:
                return _broken(len(lines), count, f"{Path(source).name}: {problem}")
            if source == self.anchor_path:
                anchored = count
        return {"ok": True, "records": len(lines), "head_sha256": previous,
                "anchored_records": anchored, "detail": None}

    # -- writing (append only) ----------------------------------------------------------

    def append_locked(self, records: list[dict], *, manifest_sha256: str) -> list[dict]:
        """Append ``records`` (caller holds :meth:`locked`); refuses a broken chain."""
        if not records:
            return []
        check = self.verify(manifest_sha256=manifest_sha256)
        if not check["ok"]:
            raise ChainError(f"E2 ledger is broken, refusing to append: {check['detail']}")
        if check["records"] == 0 and records[0].get("kind") != "header":
            raise ChainError("the first ledger record must be the header")
        previous = check["head_sha256"]
        out = []
        self.board_dir.mkdir(parents=True, exist_ok=True)
        with open(self.path, "ab") as stream:
            for record in records:
                line = canonical({**record, "prev_sha256": previous})
                stream.write(line + b"\n")
                previous = sha256_hex(line)
                out.append(json.loads(line))
            stream.flush()
            os.fsync(stream.fileno())
        anchors = list(raw_lines(self.anchor_path))
        anchor = {
            "records": check["records"] + len(records),
            "head_sha256": previous,
            "run_at": out[-1].get("run_at"),
            "prev_anchor_sha256": sha256_hex(anchors[-1]) if anchors else None,
        }
        with open(self.anchor_path, "ab") as stream:
            stream.write(canonical(anchor) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        return out

    @contextlib.contextmanager
    def locked(self):
        """Exclusive O_EXCL lock file; a lock older than 15 minutes is stale."""
        self.board_dir.mkdir(parents=True, exist_ok=True)
        deadline = time.monotonic() + LOCK_TIMEOUT_S
        while True:
            try:
                fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"pid={os.getpid()}".encode())
                os.close(fd)
                break
            except FileExistsError:
                try:
                    age = time.time() - self.lock_path.stat().st_mtime
                except FileNotFoundError:
                    continue
                if age > LOCK_STALE_AFTER_S:
                    self.lock_path.unlink(missing_ok=True)
                    continue
                if time.monotonic() >= deadline:
                    raise TimeoutError(f"could not acquire {self.lock_path}") from None
                time.sleep(0.2)
        try:
            yield
        finally:
            self.lock_path.unlink(missing_ok=True)


def _broken(n: int, index: int, detail: str) -> dict:
    return {"ok": False, "records": n, "head_sha256": None, "anchored_records": 0,
            "detail": f"record {index}: {detail}"}


def check_anchors(path: Path, heads: list[str]) -> tuple[str | None, int]:
    """Every anchor must name a prefix the ledger still has, with the same head hash."""
    previous, count = None, 0
    for i, line in enumerate(raw_lines(path)):
        try:
            anchor = json.loads(line)
        except ValueError:
            return f"anchor {i} is not JSON", count
        if line != canonical(anchor) or anchor.get("prev_anchor_sha256") != previous:
            return f"anchor {i} breaks the anchor chain", count
        records = anchor.get("records")
        if not isinstance(records, int) or records < max(count, 1):
            return f"anchor {i} is not monotone", count
        if records > len(heads):
            return f"ledger truncated below anchor {i} ({records} records anchored)", records
        if heads[records - 1] != anchor.get("head_sha256"):
            return f"ledger rewritten below anchor {i}", records
        previous, count = sha256_hex(line), records
    return None, count
