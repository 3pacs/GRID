"""Receipts of what the Research Desk's sources said after each session close.

The Desk brief (3pacs/GRID#1164) reads the factory ledger, Sleuth's leads with
their declared tests, and a thesis log. Sleuth's tables are updated in place and
keep no history, so a brief written from them cannot later show what it read.
This log fixes that from the night it starts running: after each session close
it reads every source once, stores the exact bytes it read, and appends one
hash-chained record naming them.

What a record proves, and what it does not
-------------------------------------------
* It proves the bytes in ``payloads/<sha256>.json`` were what the source
  returned when this process read it, between ``read_started_at`` and
  ``read_finished_at``, provided the anchors are also held off-host
  (``export_anchors``; committing and pushing the vault file is the operator's
  or a sync job's step, as for the E2 witness).
* It does not prove the source was right, and it says nothing about any time
  before the read. A session never recorded stays unknown; it cannot be
  backfilled.
* It scores nothing and promotes nothing.

Rules
-----
* **Session and cutoff.** A receipt belongs to one XNYS session. The evidence
  cutoff is that session's close, computed from the session and never passed
  in. Only the latest closed session can be recorded: not one whose close is
  still ahead, and not an older one after a later session has closed.
* **One per session.** A second run for a recorded session writes nothing and
  returns the first record, whatever the sources say by then.
* **Unreadable is not empty.** A source that raises is recorded as
  ``status: "unknown"`` with the failure's type. A source that reads zero rows
  is ``status: "read"`` with ``rows: 0``. The receipt is written either way,
  so a missing feed is on the record as missing.
* **Append only.** Lines are canonical JSON chained by ``prev_sha256``; each
  append adds a chained anchor line. Nothing here rewrites or deletes a line,
  and payload files are created once.
* **No secrets on the record.** A failed read is recorded by exception type.
  Its message goes to the run log only.

The line, chain and anchor formats are the S10 forward log's and E2's
(``evals/e2/chain.py``). The helpers are restated here rather than imported:
E2 forbids any module outside its package from importing its chain
(``tests/test_e2_manifest_guard.py``), and this log should not change when
another ledger's code does.
"""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import sys
import time as _time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from loguru import logger as log

from ingestion.market_calendar import is_market_open, market_close_time

SCHEMA = "desk-source-receipt-v1"
LOG_ID = "grid-desk-source-receipts"
LOG_NAME = f"{LOG_ID}.jsonl"
ANCHOR_NAME = f"{LOG_ID}.anchors.jsonl"
LOCK_NAME = f".{LOG_ID}.lock"
PAYLOAD_DIR = "payloads"
WITNESS_DIR = "05-GRID/Paper-Log/desk-receipts"

STATUS_READ = "read"
STATUS_UNKNOWN = "unknown"

LEAD_LOOKBACK_DAYS = 60     # leads created this long before the cutoff are in scope
LEAD_CAP = 5000             # a read that reaches the cap is unknown, never truncated
LOCK_TIMEOUT_S = 30.0
LOCK_STALE_AFTER_S = 900.0
_NY = ZoneInfo("America/New_York")
_SESSION_SEARCH_DAYS = 10   # longest run of closed days is well under this

THESIS_LOG_MISSING = (
    "no designated research-disposition log exists (3pacs/GRID#1165); "
    "thesis_snapshots is a market-direction table scored in place and is not it"
)


class ReceiptError(RuntimeError):
    """The receipt log is broken, or a record may not be written."""


class SourceUnavailable(Exception):
    """A source that cannot be read for a reason that is safe to put on the record."""


@dataclass(frozen=True)
class SourceRead:
    """One source's read: the payload it returned and how many items it holds."""

    payload: Any
    rows: int
    scope: Mapping[str, Any]


Source = Callable[[datetime], SourceRead]
Clock = Callable[[], datetime]


def _now() -> datetime:
    return datetime.now(timezone.utc)


# ── canonical lines and anchors ─────────────────────────────────────────────


def canonical(record: Any) -> bytes:
    return json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


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


def check_anchors(path: Path, heads: list[str]) -> tuple[str | None, int]:
    """Every anchor must name a prefix the log still has, with the same head hash."""
    previous, count = None, 0
    for i, line in enumerate(raw_lines(path)):
        try:
            anchor = json.loads(line)
        except ValueError:
            return f"anchor {i} is not JSON", count
        if not isinstance(anchor, dict):
            return f"anchor {i} is not a JSON object", count
        if line != canonical(anchor) or anchor.get("prev_anchor_sha256") != previous:
            return f"anchor {i} breaks the anchor chain", count
        records = anchor.get("records")
        if not isinstance(records, int) or records < max(count, 1):
            return f"anchor {i} is not monotone", count
        if records > len(heads):
            return f"log truncated below anchor {i} ({records} records anchored)", records
        if heads[records - 1] != anchor.get("head_sha256"):
            return f"log rewritten below anchor {i}", records
        previous, count = sha256_hex(line), records
    return None, count


# ── sessions ────────────────────────────────────────────────────────────────


def session_cutoff(session: date) -> datetime:
    """The session's regular close, in UTC. Raises on a closed day."""
    if not is_market_open(session):
        raise ReceiptError(f"{session.isoformat()} is not an XNYS session")
    local = datetime.combine(session, market_close_time(session), tzinfo=_NY)
    return local.astimezone(timezone.utc)


def latest_closed_session(now: datetime) -> date:
    """The most recent session whose close is at or before ``now``."""
    day = now.astimezone(_NY).date()
    for _ in range(_SESSION_SEARCH_DAYS):
        if is_market_open(day) and session_cutoff(day) <= now:
            return day
        day -= timedelta(days=1)
    raise ReceiptError("no closed session found in the search window")


# ── sources ─────────────────────────────────────────────────────────────────


def factory_ledger_source(ledger: Path, anchor: Path) -> Source:
    """The module-factory ledger: verified through its anchor, then summarized."""

    def read(_cutoff: datetime) -> SourceRead:
        from analysis import ledger_steered_exploration as lse

        opened = lse.Ledger(ledger, anchor=anchor)
        opened.verify(full=True)
        summary = lse.summarize(opened)
        payload = {
            "ledger_id": summary["ledger_id"],
            "head_sha256": opened.head,
            "ledger_file_sha256": sha256_hex(Path(ledger).read_bytes()),
            "anchor_file_sha256": sha256_hex(Path(anchor).read_bytes()),
            "summary": summary,
        }
        return SourceRead(payload=payload, rows=int(summary["records"]),
                          scope={"ledger": Path(ledger).name, "anchor": Path(anchor).name})

    return read


def sleuth_leads_source(engine: Any, lookback_days: int = LEAD_LOOKBACK_DAYS) -> Source:
    """Intersection leads created up to the cutoff, each with its stored tests.

    The rows are read as they are at read time. ``created_at <= cutoff`` keeps
    later leads out; it does not undo in-place changes made after the cutoff
    to a lead or test that already existed, which is why the read clocks are
    part of the record.
    """

    def read(cutoff: datetime) -> SourceRead:
        from intelligence.sleuth import Sleuth
        from intelligence.sleuth_intersections import ORIGIN, declared_tests_for_leads

        since = cutoff - timedelta(days=lookback_days)
        leads = Sleuth(engine, ensure_schema=False).get_leads(
            limit=LEAD_CAP, origin=ORIGIN, created_since=since, created_until=cutoff)
        if len(leads) >= LEAD_CAP:
            raise SourceUnavailable("lead scope reached the cap; refusing a truncated read")
        tests = declared_tests_for_leads(engine, leads)
        rows = [
            {**asdict(lead),
             "declared_tests": tests[lead.id]["tests"],
             "test_link_problems": tests[lead.id]["link_problems"]}
            for lead in sorted(leads, key=lambda item: item.id)
        ]
        return SourceRead(
            payload={"leads": rows}, rows=len(rows),
            scope={"origin": ORIGIN, "created_since": since.isoformat(),
                   "created_until": cutoff.isoformat()})

    return read


def thesis_log_source() -> Source:
    """There is no thesis log to read yet; say so on every record until there is."""

    def read(_cutoff: datetime) -> SourceRead:
        raise SourceUnavailable(THESIS_LOG_MISSING)

    return read


# ── the log ─────────────────────────────────────────────────────────────────


class ReceiptLog:
    """The receipt log in ``root``: chained records, chained anchors, payload files."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.path = self.root / LOG_NAME
        self.anchor_path = self.root / ANCHOR_NAME
        self.lock_path = self.root / LOCK_NAME
        self.payload_dir = self.root / PAYLOAD_DIR

    # -- reading (never writes, never creates) -------------------------------

    def records(self) -> list[dict]:
        return [json.loads(line) for line in raw_lines(self.path)]

    def receipt_for(self, session: date) -> dict | None:
        key = session.isoformat()
        for record in self.records():
            if record.get("kind") == "receipt" and record.get("session") == key:
                return record
        return None

    def payload(self, sha256: str) -> bytes:
        """A stored payload's bytes, checked against the hash that names it."""
        raw = (self.payload_dir / f"{sha256}.json").read_bytes()
        if sha256_hex(raw) != sha256:
            raise ReceiptError(f"payload {sha256[:12]} does not match its hash")
        return raw

    def verify(self, external_anchors: Path | None = None, *, full: bool = False) -> dict:
        """Walk the chain, then the anchors (local, and an off-host copy if given).

        ``full`` also re-hashes every payload a record names. Returns
        ``{"ok", "records", "receipts", "head_sha256", "anchored_records",
        "witnessed_records", "detail"}``.
        """
        lines = list(raw_lines(self.path))
        heads: list[str] = []
        sessions: set[str] = set()
        previous = None
        last_cutoff = None
        for i, line in enumerate(lines):
            try:
                record = json.loads(line)
            except ValueError:
                return _broken(len(lines), i, "line is not JSON")
            if not isinstance(record, dict) or line != canonical(record):
                return _broken(len(lines), i, "line is not a canonical JSON object")
            if record.get("prev_sha256") != previous:
                return _broken(len(lines), i, "prev_sha256 does not match the previous line")
            if i == 0:
                if (record.get("kind"), record.get("schema"), record.get("log_id")) != (
                        "header", SCHEMA, LOG_ID):
                    return _broken(len(lines), 0, "first record is not this log's header")
            else:
                problem = _check_receipt(record, sessions, last_cutoff)
                if problem:
                    return _broken(len(lines), i, problem)
                sessions.add(record["session"])
                last_cutoff = record["evidence_cutoff"]
                if full:
                    for name, source in record["sources"].items():
                        if source["status"] != STATUS_READ:
                            continue
                        try:
                            self.payload(source["payload_sha256"])
                        except (OSError, ReceiptError) as exc:
                            return _broken(len(lines), i, f"{name} payload: {type(exc).__name__}")
            previous = sha256_hex(line)
            heads.append(previous)
        if lines and not self.anchor_path.exists():
            return _broken(len(lines), len(lines), "anchor file missing")
        anchored = witnessed = 0
        for source in (self.anchor_path, external_anchors):
            if source is None:
                continue
            problem, count = check_anchors(Path(source), heads)
            if problem:
                return _broken(len(lines), count, f"{Path(source).name}: {problem}")
            if source == self.anchor_path:
                anchored = count
            else:
                witnessed = count
        # A crash between the two appends leaves an unanchored tail. It is
        # reported, not refused: the next append's anchor covers it.
        return {"ok": True, "records": len(lines), "receipts": len(sessions),
                "head_sha256": previous, "anchored_records": anchored,
                "witnessed_records": witnessed, "detail": None}

    # -- writing (append only) ------------------------------------------------

    def record(self, sources: Mapping[str, Source], *, session: date | None = None,
               now: Clock = _now) -> dict:
        """Read every source once and append the session's receipt.

        Returns ``{"written": bool, "record": dict}``. ``written`` is False
        when the session already has a receipt.
        """
        if not sources:
            raise ReceiptError("at least one source is required")
        started = now()
        latest = latest_closed_session(started)
        session = latest if session is None else session
        cutoff = session_cutoff(session)
        if cutoff > started:
            raise ReceiptError(f"session {session.isoformat()} has not closed")
        if session != latest:
            raise ReceiptError(
                f"session {session.isoformat()} can no longer be recorded: "
                f"{latest.isoformat()} has closed since. A missed session stays unknown.")
        with self._locked():
            check = self.verify()
            if not check["ok"]:
                raise ReceiptError(f"receipt log is broken, refusing to append: {check['detail']}")
            existing = self.receipt_for(session)
            if existing is not None:
                return {"written": False, "record": existing}
            entries: dict[str, dict] = {}
            for name in sorted(sources):
                entries[name] = self._read(name, sources[name], cutoff)
            body = {
                "kind": "receipt",
                "session": session.isoformat(),
                "evidence_cutoff": cutoff.isoformat(),
                "read_started_at": started.isoformat(),
                "read_finished_at": now().isoformat(),
                "sources": entries,
                "promotion_allowed": False,
            }
            written = self._append(body, started)
        return {"written": True, "record": written}

    def _read(self, name: str, source: Source, cutoff: datetime) -> dict:
        try:
            read = source(cutoff)
            raw = canonical(read.payload)
        except SourceUnavailable as exc:
            return {"status": STATUS_UNKNOWN, "failure": type(exc).__name__,
                    "reason": str(exc)[:300]}
        except Exception as exc:  # noqa: BLE001 - any failure is "unknown", by design
            # The record gets the type only: database errors can carry hosts
            # and statements. The detail goes to the run log.
            log.warning("desk receipts: source {n} could not be read: {e}", n=name, e=str(exc))
            return {"status": STATUS_UNKNOWN, "failure": type(exc).__name__,
                    "reason": "the read raised; see the run log"}
        digest = sha256_hex(raw)
        self._store_payload(digest, raw)
        return {"status": STATUS_READ, "rows": read.rows, "bytes": len(raw),
                "payload_sha256": digest, "scope": dict(read.scope)}

    def _store_payload(self, digest: str, raw: bytes) -> None:
        self.payload_dir.mkdir(parents=True, exist_ok=True)
        path = self.payload_dir / f"{digest}.json"
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o640)
        except FileExistsError:
            if path.read_bytes() != raw:
                raise ReceiptError(f"payload {digest[:12]} exists with other bytes") from None
            return
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())

    def _append(self, body: dict, at: datetime) -> dict:
        lines = list(raw_lines(self.path))
        new: list[bytes] = []
        previous = sha256_hex(lines[-1]) if lines else None
        if not lines:
            header = canonical({"kind": "header", "schema": SCHEMA, "log_id": LOG_ID,
                                "created_at": at.isoformat(), "prev_sha256": None})
            new.append(header)
            previous = sha256_hex(header)
        line = canonical({**body, "prev_sha256": previous})
        new.append(line)
        self.root.mkdir(parents=True, exist_ok=True)
        with open(self.path, "ab") as stream:
            stream.writelines(item + b"\n" for item in new)
            stream.flush()
            os.fsync(stream.fileno())
        anchors = list(raw_lines(self.anchor_path))
        anchor = canonical({
            "records": len(lines) + len(new),
            "head_sha256": sha256_hex(line),
            "run_at": at.isoformat(),
            "prev_anchor_sha256": sha256_hex(anchors[-1]) if anchors else None,
        })
        with open(self.anchor_path, "ab") as stream:
            stream.write(anchor + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        return json.loads(line)

    @contextlib.contextmanager
    def _locked(self) -> Iterator[None]:
        """Exclusive O_EXCL lock file; a lock older than 15 minutes is stale."""
        self.root.mkdir(parents=True, exist_ok=True)
        deadline = _time.monotonic() + LOCK_TIMEOUT_S
        while True:
            try:
                fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
                os.write(fd, f"pid={os.getpid()}".encode())
                os.close(fd)
                break
            except FileExistsError:
                try:
                    age = _time.time() - self.lock_path.stat().st_mtime
                except FileNotFoundError:
                    continue
                if age > LOCK_STALE_AFTER_S:
                    self.lock_path.unlink(missing_ok=True)
                    continue
                if _time.monotonic() >= deadline:
                    raise TimeoutError(f"could not acquire {self.lock_path}") from None
                _time.sleep(0.2)
        try:
            yield
        finally:
            self.lock_path.unlink(missing_ok=True)

    # -- off-host witness ------------------------------------------------------

    def export_anchors(self, vault_worktree: Path) -> list[str]:
        """Append the anchor lines the vault's witness file lacks.

        Writes the working-tree file only. The witness is worth something once
        that file is committed and pushed, which this does not do.
        """
        check = self.verify()
        if not check["ok"]:
            raise ReceiptError(f"refusing to export anchors of a broken log: {check['detail']}")
        local = list(raw_lines(self.anchor_path))
        path = Path(vault_worktree) / WITNESS_DIR / ANCHOR_NAME
        existing = list(raw_lines(path))
        if existing != local[: len(existing)]:
            raise ReceiptError("the vault's witness file witnesses another log; not appending")
        new = local[len(existing):]
        if new:
            path.parent.mkdir(parents=True, exist_ok=True)
            tail = path.read_bytes() if path.exists() else b""
            with open(path, "ab") as stream:
                if tail and not tail.endswith(b"\n"):
                    stream.write(b"\n")
                stream.writelines(line + b"\n" for line in new)
        return [line.decode("utf-8") for line in new]


def _check_receipt(record: dict, sessions: set[str], last_cutoff: str | None) -> str | None:
    if record.get("kind") != "receipt" or record.get("promotion_allowed") is not False:
        return "not a non-promoting receipt"
    try:
        session = date.fromisoformat(record["session"])
        cutoff = datetime.fromisoformat(record["evidence_cutoff"])
        started = datetime.fromisoformat(record["read_started_at"])
        finished = datetime.fromisoformat(record["read_finished_at"])
        expected = session_cutoff(session)
    except (KeyError, TypeError, ValueError, ReceiptError) as exc:
        return f"session or clocks are invalid ({type(exc).__name__})"
    if cutoff != expected:
        return "evidence_cutoff is not the session's close"
    if not cutoff <= started <= finished:
        return "the read does not follow the cutoff"
    if record["session"] in sessions:
        return "a second receipt for the same session"
    if last_cutoff is not None and record["evidence_cutoff"] <= last_cutoff:
        return "sessions are out of order"
    sources = record.get("sources")
    if not isinstance(sources, dict) or not sources:
        return "no sources"
    for name, source in sources.items():
        status = source.get("status") if isinstance(source, dict) else None
        if status == STATUS_READ:
            digest = source.get("payload_sha256")
            if not (isinstance(digest, str) and len(digest) == 64
                    and isinstance(source.get("rows"), int) and source["rows"] >= 0):
                return f"{name}: a read needs a payload hash and a row count"
        elif status == STATUS_UNKNOWN:
            if "payload_sha256" in source or "rows" in source or not source.get("failure"):
                return f"{name}: an unknown source carries no payload or rows, and names its failure"
        else:
            return f"{name}: status must be read or unknown"
    return None


def _broken(n: int, index: int, detail: str) -> dict:
    return {"ok": False, "records": n, "receipts": 0, "head_sha256": None,
            "anchored_records": 0, "witnessed_records": 0, "detail": f"record {index}: {detail}"}


# ── command line ────────────────────────────────────────────────────────────


def default_sources(engine: Any, factory_ledger: Path, factory_anchor: Path) -> dict[str, Source]:
    return {
        "factory_ledger": factory_ledger_source(factory_ledger, factory_anchor),
        "sleuth_leads": sleuth_leads_source(engine),
        "thesis_log": thesis_log_source(),
    }


def _summary(record: dict) -> dict:
    return {
        "session": record["session"],
        "evidence_cutoff": record["evidence_cutoff"],
        "sources": {
            name: (f"read, {s['rows']} rows, {s['payload_sha256'][:12]}"
                   if s["status"] == STATUS_READ else f"unknown ({s['failure']})")
            for name, s in record["sources"].items()
        },
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m intelligence.desk_source_receipts")
    sub = parser.add_subparsers(dest="command", required=True)
    rec = sub.add_parser("record", help="read the sources and append the latest closed session's receipt")
    rec.add_argument("--dir", type=Path, required=True)
    rec.add_argument("--factory-ledger", type=Path, required=True)
    rec.add_argument("--factory-anchor", type=Path, required=True)
    rec.add_argument("--session", type=date.fromisoformat)
    ver = sub.add_parser("verify", help="check the chain, anchors and payloads")
    ver.add_argument("--dir", type=Path, required=True)
    ver.add_argument("--external-anchors", type=Path)
    exp = sub.add_parser("export-anchors", help="append missing anchor lines to a vault worktree")
    exp.add_argument("--dir", type=Path, required=True)
    exp.add_argument("--vault-worktree", type=Path, required=True)
    args = parser.parse_args(argv)
    log = ReceiptLog(args.dir)
    if args.command == "verify":
        check = log.verify(args.external_anchors, full=True)
        print(json.dumps(check, indent=1))
        return 0 if check["ok"] else 1
    if args.command == "export-anchors":
        print(json.dumps({"exported": len(log.export_anchors(args.vault_worktree))}))
        return 0
    from db import get_engine

    sources = default_sources(get_engine(), args.factory_ledger, args.factory_anchor)
    try:
        result = log.record(sources, session=args.session)
    except ReceiptError as exc:
        print(json.dumps({"ok": False, "refused": str(exc)}))
        return 2
    unknown = sorted(n for n, s in result["record"]["sources"].items()
                     if s["status"] == STATUS_UNKNOWN)
    print(json.dumps({"ok": True, "written": result["written"], "unknown_sources": unknown,
                      **_summary(result["record"])}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
