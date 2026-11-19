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
  cutoff is that session's close, taken from the pinned XNYS manifest
  (``CALENDAR_SHA256``) and never passed in. A date outside the manifest is
  refused. Only the latest closed session can be recorded: not one whose
  close is still ahead, and not an older one after a later session has closed.
* **A post-close read, not close-time state.** The cutoff dates the session.
  The payloads are what the sources held when they were read, and each source
  carries its own read clocks. Sleuth rows are changed in place and the
  factory ledger grows, so a payload can hold state from after the close. A
  reader must use the read clocks, not the cutoff, as the time the state was
  known. When the witness received a head, and when a consumer had it, are
  two further clocks kept by those parties, not by this log.
* **One source, one version.** Each source is read as one consistent version:
  the factory ledger and its anchor from one stable read of both files,
  verified and summarized from those same bytes; Sleuth's leads and tests in
  one database snapshot.
* **One per session.** A second run for a recorded session writes nothing and
  returns the first record, whatever the sources say by then.
* **Unreadable is not empty.** A source that raises is recorded as
  ``status: "unknown"`` with the failure's type. A source that reads zero rows
  is ``status: "read"`` with ``rows: 0``. The receipt is written either way,
  so a missing feed is on the record as missing.
* **Append only.** Lines are canonical JSON chained by ``prev_sha256``; each
  append adds a chained anchor line. Nothing here rewrites or deletes a line,
  and payload files are created once.
* **An interrupted write is finished, not redone.** The receipt line is
  written before its anchor. If the process stops between the two, the next
  run appends the missing anchor (marked ``recovered``) and writes nothing
  else for that session. More than one unanchored record is not something an
  interruption can leave, and is refused.
* **Local anchoring and off-host witnessing are judged separately.** The
  anchor file shows this host's own claim. A head is witnessed only when a
  party other than this recorder holds it. ``witness-delivery/`` holds this
  host's note that it handed a (session, head) to the witness; it lets a
  failed delivery be retried without a second receipt, and it is not the
  witness.
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
import functools
import hashlib
import json
import os
import sqlite3
import sys
import tempfile
import time as _time
from collections.abc import Callable, Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

from loguru import logger as log

from ingestion.market_calendar import ManifestCalendar

SCHEMA = "desk-source-receipt-v1"
LOG_ID = "grid-desk-source-receipts"
LOG_NAME = f"{LOG_ID}.jsonl"
ANCHOR_NAME = f"{LOG_ID}.anchors.jsonl"
LOCK_NAME = f".{LOG_ID}.lock"
PAYLOAD_DIR = "payloads"
WITNESS_DIR = "05-GRID/Paper-Log/desk-receipts"
DELIVERY_DIR = "witness-delivery"   # this host's notes of heads handed to the witness

# The pinned XNYS session manifest (the one ``ingestion.gex_native_volume``
# pins). Sessions and closes come from it alone; a date outside it is refused.
CALENDAR_PATH = (Path(__file__).resolve().parents[1] / "docs/reference/calendar-manifests"
                 / "xnys-rth-20261001-20271231.json")
CALENDAR_SHA256 = "efa8897e2fd94afd4f9f9b29951f142b97aece83812aca7403026df1ec4eb8ad"
STATE_AS_OF_READ = "read"           # payloads are the state at the read clocks, not at the close

DELIVERED = "delivered"
PENDING = "pending"
VIA = ("hub", "operator")
FACTORY_READ_ATTEMPTS = 3

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


PANEL_TRIALS_NOT_NAMED = (
    "no live trial ledger was named for this run (--panel-trials-db); "
    "its head is unknown tonight, not unchanged"
)
PANEL_FACTORY_FAMILY = "panel-factory"


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


@functools.lru_cache(maxsize=1)
def _calendar() -> ManifestCalendar:
    return ManifestCalendar(CALENDAR_PATH, CALENDAR_SHA256)


def session_cutoff(session: date) -> datetime:
    """The session's close from the pinned manifest, in UTC. Raises on a closed
    day and on a date the manifest does not cover."""
    try:
        found = _calendar().for_date(session)
    except (OSError, ValueError) as exc:
        raise ReceiptError(
            f"{session.isoformat()} cannot be checked against the pinned XNYS calendar "
            f"({type(exc).__name__}: {exc})") from exc
    if found is None:
        raise ReceiptError(f"{session.isoformat()} is not an XNYS session")
    return found.closes.astimezone(timezone.utc)


def _is_session(day: date) -> bool:
    try:
        return _calendar().for_date(day) is not None
    except (OSError, ValueError) as exc:
        raise ReceiptError(
            f"{day.isoformat()} cannot be checked against the pinned XNYS calendar "
            f"({type(exc).__name__}: {exc})") from exc


def latest_closed_session(now: datetime) -> date:
    """The most recent session whose close is at or before ``now``."""
    day = now.astimezone(_NY).date()
    for _ in range(_SESSION_SEARCH_DAYS):
        if _is_session(day) and session_cutoff(day) <= now:
            return day
        day -= timedelta(days=1)
    raise ReceiptError("no closed session found in the search window")


# ── sources ─────────────────────────────────────────────────────────────────


def _stable_read(paths: list[Path]) -> list[bytes]:
    """The bytes of every file as they were at one instant.

    Two full passes that agree mean no file changed between its first and
    second read, and those intervals all overlap, so the first pass is the
    files together at one moment. A writer that is mid-append makes the passes
    differ; after a few tries the read is refused, not guessed.
    """
    for _ in range(FACTORY_READ_ATTEMPTS):
        first = [Path(path).read_bytes() for path in paths]
        if first == [Path(path).read_bytes() for path in paths]:
            return first
    raise SourceUnavailable("the factory ledger changed during every read attempt")


def factory_ledger_source(ledger: Path, anchor: Path) -> Source:
    """The module-factory ledger: one stable read of the ledger and its anchor,
    then verified and summarized from those bytes and no others.

    The ledger reader works on paths and re-reads them, so the bytes are
    written to a private folder and opened there. The live files are not read
    again: the summary, the head and both file hashes describe one version.
    The state is the ledger as read, which can be later than the session close.
    """

    def read(_cutoff: datetime) -> SourceRead:
        from analysis import ledger_steered_exploration as lse

        ledger_bytes, anchor_bytes = _stable_read([Path(ledger), Path(anchor)])
        with tempfile.TemporaryDirectory(prefix="desk-receipt-factory-") as scratch:
            ledger_copy = Path(scratch) / Path(ledger).name
            anchor_copy = Path(scratch) / Path(anchor).name
            if ledger_copy == anchor_copy:
                raise SourceUnavailable("the ledger and its anchor have the same file name")
            ledger_copy.write_bytes(ledger_bytes)
            anchor_copy.write_bytes(anchor_bytes)
            opened = lse.Ledger(ledger_copy, anchor=anchor_copy)
            opened.verify(full=True)
            summary = lse.summarize(opened)
            head = opened.head
        if summary.get("head_sha256") != head:
            raise SourceUnavailable("the factory summary names another head than the ledger read")
        payload = {
            "ledger_id": summary["ledger_id"],
            "head_sha256": head,
            "ledger_file_sha256": sha256_hex(ledger_bytes),
            "anchor_file_sha256": sha256_hex(anchor_bytes),
            "summary": summary,
        }
        return SourceRead(payload=payload, rows=int(summary["records"]),
                          scope={"ledger": Path(ledger).name, "anchor": Path(anchor).name,
                                 "snapshot": "one stable read of both files",
                                 "state": "as read; not limited to the session close"})

    return read


def sleuth_leads_source(engine: Any, lookback_days: int = LEAD_LOOKBACK_DAYS) -> Source:
    """Intersection leads created up to the cutoff, each with its stored tests.

    Leads and tests are read in one database snapshot, as they are at read
    time. ``created_at <= cutoff`` keeps later leads out; it does not undo
    in-place changes made after the cutoff to a lead or test that already
    existed, which is why the read clocks are part of the record. Only
    ``declared_tests`` are verified tests; ``unverified_tests`` are rows that
    exist and failed a check.
    """

    def read(cutoff: datetime) -> SourceRead:
        from intelligence.sleuth import Sleuth
        from intelligence.sleuth_intersections import (
            ORIGIN,
            declared_tests_for_leads,
            read_snapshot,
        )

        since = cutoff - timedelta(days=lookback_days)
        with read_snapshot(engine) as conn:
            leads = Sleuth(engine, ensure_schema=False).get_leads(
                limit=LEAD_CAP, origin=ORIGIN, created_since=since, created_until=cutoff,
                conn=conn)
            if len(leads) >= LEAD_CAP:
                raise SourceUnavailable("lead scope reached the cap; refusing a truncated read")
            tests = declared_tests_for_leads(conn, leads)
        rows = [
            {**asdict(lead),
             "declared_tests": tests[lead.id]["tests"],
             "unverified_tests": tests[lead.id]["unverified_tests"],
             "test_link_problems": tests[lead.id]["link_problems"]}
            for lead in sorted(leads, key=lambda item: item.id)
        ]
        return SourceRead(
            payload={"leads": rows}, rows=len(rows),
            scope={"origin": ORIGIN, "created_since": since.isoformat(),
                   "created_until": cutoff.isoformat(),
                   "snapshot": "one database snapshot for leads and tests",
                   "state": "as read; rows are changed in place after the close"})

    return read


def panel_trials_source(db: Path | None) -> Source:
    """The live trial ledger: gex-signals' ``panel_trials`` table (SQLite), where
    every forward test is registered before it records, and ``factory_looks``,
    where a factory slot's one look is stored (3pacs/GRID#1162).

    One read-only transaction. The payload holds the chain's head and row
    count, the trials per family, each ``panel-factory`` slot's registration
    hash, and the sha256 of each stored look. With the nightly hub post this
    puts the head off-host every session, so a registration or a look that is
    later edited or removed no longer matches a head this host cannot rewrite.

    What is checked here: each row's ``prev_hash`` is the row before it. What
    is not: a row's hash is not recomputed from its fields. That recipe lives
    in gex-signals (``panel.verify_trials``) and is not copied into GRID.
    """

    def read(_cutoff: datetime) -> SourceRead:
        if db is None:
            raise SourceUnavailable(PANEL_TRIALS_NOT_NAMED)
        try:
            conn = sqlite3.connect(f"file:{quote(str(Path(db).absolute()))}?mode=ro", uri=True, timeout=5)
        except sqlite3.Error as exc:
            raise SourceUnavailable(f"the live trial ledger could not be opened ({type(exc).__name__})") from None
        try:
            conn.execute("BEGIN")
            trials = conn.execute(
                "SELECT seq, trial_id, family, registered_at, prev_hash, row_hash "
                "FROM panel_trials ORDER BY seq"
            ).fetchall()
            has_looks = conn.execute(
                "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'factory_looks'"
            ).fetchone() is not None
            looks = conn.execute(
                "SELECT trial_id, looked_at, through_day, result FROM factory_looks ORDER BY trial_id"
            ).fetchall() if has_looks else None
        except sqlite3.Error as exc:
            raise SourceUnavailable(f"the live trial ledger could not be read ({type(exc).__name__})") from None
        finally:
            conn.close()
        previous = None
        families: dict[str, int] = {}
        for seq, _trial, family, _registered, prev_hash, row_hash in trials:
            if prev_hash != previous or not (isinstance(row_hash, str) and len(row_hash) == 64):
                raise SourceUnavailable(f"the live trial ledger's hash links are broken at row {seq}")
            previous = row_hash
            families[family or "none"] = families.get(family or "none", 0) + 1
        payload = {
            "store": "panel_trials",
            "rows": len(trials),
            "head": previous,
            "links_checked": True,
            "row_hashes_recomputed": False,
            "families": dict(sorted(families.items())),
            "factory_slots": [
                {"trial_id": t[1], "registered_at": t[3], "row_hash": t[5]}
                for t in trials if t[2] == PANEL_FACTORY_FAMILY
            ],
            "factory_looks": None if looks is None else [
                {"trial_id": row[0], "looked_at": row[1], "through_day": row[2],
                 "result_sha256": sha256_hex(str(row[3]).encode("utf-8"))}
                for row in looks
            ],
        }
        return SourceRead(payload=payload, rows=len(trials),
                          scope={"database": Path(db).name, "snapshot": "one read-only transaction",
                                 "state": "as read; not limited to the session close",
                                 "row_hashes": "linked here; recomputed only by gex-signals"})

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
        self.delivery_dir = self.root / DELIVERY_DIR

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
        "unanchored_records", "witnessed_records", "qualified", "detail"}``.

        ``ok`` says the chain and every anchor given are consistent. It does
        not say the log is evidence. ``qualified`` answers that in two parts
        that do not stand in for each other: ``local_anchor`` (every record is
        under this host's anchor) and ``external_witness`` (every record is
        under the off-host copy; ``None`` when no copy was given, which is
        unknown, not a pass).
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
        anchored = witnessed = 0
        for source in (self.anchor_path, external_anchors):
            if source is None or (source == self.anchor_path and not self.anchor_path.exists()):
                continue
            problem, count = check_anchors(Path(source), heads)
            if problem:
                return _broken(len(lines), count, f"{Path(source).name}: {problem}")
            if source == self.anchor_path:
                anchored = count
            else:
                witnessed = count
        # The receipt line is written before its anchor, so a stopped process
        # can leave one append unanchored: one record, or the header and the
        # first receipt. That is reported and the next run anchors it. Anything
        # more is not what an interruption leaves.
        tail = len(lines) - anchored
        if tail > 1 and not (anchored == 0 and len(lines) == 2):
            return _broken(len(lines), anchored,
                           f"{tail} records beyond the last anchor; an interrupted append leaves one")
        return {"ok": True, "records": len(lines), "receipts": len(sessions),
                "head_sha256": previous, "anchored_records": anchored,
                "unanchored_records": tail, "witnessed_records": witnessed,
                "qualified": {
                    "local_anchor": bool(lines) and tail == 0,
                    "external_witness": (None if external_anchors is None
                                         else bool(lines) and witnessed == len(lines)),
                },
                "detail": None}

    # -- writing (append only) ------------------------------------------------

    def record(self, sources: Mapping[str, Source], *, session: date | None = None,
               now: Clock = _now) -> dict:
        """Read every source once and append the session's receipt.

        Returns ``{"written": bool, "record": dict, "recovered_anchor":
        bool}``. ``written`` is False when the session already has a receipt.
        ``recovered_anchor`` is True when this run appended the anchor an
        interrupted earlier run left out.
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
            recovered = self._anchor_interrupted_tail(check, started)
            existing = self.receipt_for(session)
            if existing is not None:
                return {"written": False, "record": existing, "recovered_anchor": recovered}
            entries: dict[str, dict] = {}
            for name in sorted(sources):
                entries[name] = self._read(name, sources[name], cutoff, now)
            body = {
                "kind": "receipt",
                "session": session.isoformat(),
                "evidence_cutoff": cutoff.isoformat(),
                "calendar_sha256": CALENDAR_SHA256,
                "state_as_of": STATE_AS_OF_READ,
                "read_started_at": started.isoformat(),
                "read_finished_at": now().isoformat(),
                "sources": entries,
                "promotion_allowed": False,
            }
            written = self._append(body, started)
        return {"written": True, "record": written, "recovered_anchor": recovered}

    def _read(self, name: str, source: Source, cutoff: datetime, now: Clock = _now) -> dict:
        began = now()

        def clocks() -> dict[str, str]:
            return {"read_started_at": began.isoformat(), "read_finished_at": now().isoformat()}

        try:
            read = source(cutoff)
            raw = canonical(read.payload)
        except SourceUnavailable as exc:
            return {"status": STATUS_UNKNOWN, "failure": type(exc).__name__,
                    "reason": str(exc)[:300], **clocks()}
        except Exception as exc:  # noqa: BLE001 - any failure is "unknown", by design
            # The record gets the type only: database errors can carry hosts
            # and statements. The detail goes to the run log.
            log.warning("desk receipts: source {n} could not be read: {e}", n=name, e=str(exc))
            return {"status": STATUS_UNKNOWN, "failure": type(exc).__name__,
                    "reason": "the read raised; see the run log", **clocks()}
        finished = clocks()
        digest = sha256_hex(raw)
        self._store_payload(digest, raw)
        return {"status": STATUS_READ, "rows": read.rows, "bytes": len(raw),
                "payload_sha256": digest, "scope": dict(read.scope), **finished}

    def _anchor_interrupted_tail(self, check: dict, at: datetime) -> bool:
        """Append the anchor an interrupted run left out. Call under the lock,
        with ``check`` a passing ``verify()``. Adds one anchor line; rewrites
        nothing. ``recovered`` on the line says it was written by a later run,
        so its ``run_at`` is the recovery, not the original write."""
        if not check["unanchored_records"]:
            return False
        self._append_anchor(check["records"], check["head_sha256"], at, recovered=True)
        log.warning("desk receipts: anchored {n} record(s) an interrupted run left unanchored",
                    n=check["unanchored_records"])
        return True

    def _append_anchor(self, records: int, head: str, at: datetime, *,
                       recovered: bool = False) -> None:
        anchors = list(raw_lines(self.anchor_path))
        body: dict[str, Any] = {
            "records": records,
            "head_sha256": head,
            "run_at": at.isoformat(),
            "prev_anchor_sha256": sha256_hex(anchors[-1]) if anchors else None,
        }
        if recovered:
            body["recovered"] = True
        with open(self.anchor_path, "ab") as stream:
            stream.write(canonical(body) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())

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
        self._append_anchor(len(lines) + len(new), sha256_hex(line), at)
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

    # -- handing heads to the witness ------------------------------------------

    def witness_rows(self) -> list[dict]:
        """Every receipt with the head to witness for it and whether this host
        has noted handing that head over. Read-only.

        A receipt's head is the hash of its own line, and ``records`` is the
        log's length up to it. Both are fixed once written, so a delivery can
        be retried for the same session and the same head however many
        receipts follow. ``delivery`` is this host's note only: ``pending``
        means no note, not that the witness lacks the head, and ``delivered``
        is not proof the witness holds it.
        """
        check = self.verify()
        if not check["ok"]:
            raise ReceiptError(f"receipt log is broken: {check['detail']}")
        rows: list[dict] = []
        for index, line in enumerate(raw_lines(self.path)):
            record = json.loads(line)
            if record.get("kind") != "receipt":
                continue
            head = sha256_hex(line)
            note = self._delivery_note(record["session"], head)
            rows.append({
                "session": record["session"],
                "head_sha256": head,
                "records": index + 1,
                "evidence_cutoff": record["evidence_cutoff"],
                "read_finished_at": record["read_finished_at"],
                "unknown_sources": sorted(name for name, source in record["sources"].items()
                                          if source["status"] == STATUS_UNKNOWN),
                "locally_anchored": index + 1 <= check["anchored_records"],
                "delivery": DELIVERED if note else PENDING,
                "delivered_at": note["delivered_at"] if note else None,
                "via": note["via"] if note else None,
            })
        return rows

    def note_delivery(self, session: date, head_sha256: str, *, via: str = "hub",
                      now: Clock = _now) -> dict:
        """Note that the witness was handed this session's head. Idempotent.

        Refuses a head that is not exactly this session's receipt, and a
        receipt this host has not anchored yet. Returns ``{"written": bool,
        "note": dict}``; a second call for the same pair writes nothing.
        """
        if via not in VIA:
            raise ReceiptError(f"via must be one of {', '.join(VIA)}")
        key = session.isoformat()
        row = next((r for r in self.witness_rows() if r["session"] == key), None)
        if row is None:
            raise ReceiptError(f"session {key} has no receipt")
        if row["head_sha256"] != head_sha256:
            raise ReceiptError(f"session {key} has another head than the one delivered")
        if not row["locally_anchored"]:
            raise ReceiptError(f"session {key} is not anchored on this host yet")
        if row["delivery"] == DELIVERED:
            return {"written": False, "note": self._delivery_note(key, head_sha256)}
        note = {"session": key, "head_sha256": head_sha256, "records": row["records"],
                "delivered_at": now().isoformat(), "via": via}
        self.delivery_dir.mkdir(parents=True, exist_ok=True)
        path = self.delivery_dir / f"{key}.{head_sha256}.json"
        try:
            fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o640)
        except FileExistsError:  # another run noted it between the check and here
            return {"written": False, "note": self._delivery_note(key, head_sha256)}
        with os.fdopen(fd, "wb") as stream:
            stream.write(canonical(note) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        return {"written": True, "note": note}

    def _delivery_note(self, session: str, head_sha256: str) -> dict | None:
        path = self.delivery_dir / f"{session}.{head_sha256}.json"
        try:
            raw = path.read_bytes()
        except FileNotFoundError:
            return None
        try:
            note = json.loads(raw)
        except ValueError:
            note = None
        if (not isinstance(note, dict) or note.get("session") != session
                or note.get("head_sha256") != head_sha256 or note.get("via") not in VIA
                or not isinstance(note.get("delivered_at"), str)):
            raise ReceiptError(f"delivery note {path.name} does not say what its name says")
        return note

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
        if isinstance(source, dict) and ("read_started_at" in source or "read_finished_at" in source):
            try:
                began = datetime.fromisoformat(source["read_started_at"])
                ended = datetime.fromisoformat(source["read_finished_at"])
            except (KeyError, TypeError, ValueError):
                return f"{name}: source read clocks are invalid"
            if not started <= began <= ended <= finished:
                return f"{name}: the source was not read inside the receipt's read window"
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
            "anchored_records": 0, "unanchored_records": None, "witnessed_records": 0,
            "qualified": {"local_anchor": False, "external_witness": False},
            "detail": f"record {index}: {detail}"}


# ── command line ────────────────────────────────────────────────────────────


def default_sources(engine: Any, factory_ledger: Path, factory_anchor: Path,
                    panel_trials_db: Path | None = None) -> dict[str, Source]:
    return {
        "factory_ledger": factory_ledger_source(factory_ledger, factory_anchor),
        "sleuth_leads": sleuth_leads_source(engine),
        "thesis_log": thesis_log_source(),
        "panel_trials": panel_trials_source(panel_trials_db),
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
    rec.add_argument("--panel-trials-db", type=Path,
                     help="gex-signals signals.db; without it the live trial ledger is recorded as unknown")
    rec.add_argument("--session", type=date.fromisoformat)
    ver = sub.add_parser("verify", help="check the chain, anchors and payloads")
    ver.add_argument("--dir", type=Path, required=True)
    ver.add_argument("--external-anchors", type=Path)
    exp = sub.add_parser("export-anchors", help="append missing anchor lines to a vault worktree")
    exp.add_argument("--dir", type=Path, required=True)
    exp.add_argument("--vault-worktree", type=Path, required=True)
    wst = sub.add_parser("witness-status",
                         help="each receipt's head and whether this host noted handing it over")
    wst.add_argument("--dir", type=Path, required=True)
    wst.add_argument("--pending", action="store_true", help="only receipts with no delivery note")
    wst.add_argument("--lines", action="store_true",
                     help="one tab-separated line per receipt: session, head, records, cutoff, "
                          "unknown sources (comma-separated, or -)")
    wmk = sub.add_parser("witness-mark",
                         help="note that the witness was handed one session's exact head")
    wmk.add_argument("--dir", type=Path, required=True)
    wmk.add_argument("--session", type=date.fromisoformat, required=True)
    wmk.add_argument("--head", required=True)
    wmk.add_argument("--via", choices=VIA, default="hub")
    args = parser.parse_args(argv)
    log = ReceiptLog(args.dir)
    if args.command in ("witness-status", "witness-mark"):
        try:
            if args.command == "witness-mark":
                print(json.dumps({"ok": True, **log.note_delivery(
                    args.session, args.head, via=args.via)}))
                return 0
            rows = [r for r in log.witness_rows() if not args.pending or r["delivery"] == PENDING]
        except ReceiptError as exc:
            print(json.dumps({"ok": False, "refused": str(exc)}))
            return 2
        if args.lines:
            for r in rows:
                print("\t".join([r["session"], r["head_sha256"], str(r["records"]),
                                 r["evidence_cutoff"], ",".join(r["unknown_sources"]) or "-"]))
        else:
            print(json.dumps({"ok": True, "receipts": rows}, indent=1))
        return 0
    if args.command == "verify":
        check = log.verify(args.external_anchors, full=True)
        print(json.dumps(check, indent=1))
        return 0 if check["ok"] else 1
    if args.command == "export-anchors":
        print(json.dumps({"exported": len(log.export_anchors(args.vault_worktree))}))
        return 0
    from db import get_engine

    sources = default_sources(get_engine(), args.factory_ledger, args.factory_anchor, args.panel_trials_db)
    try:
        result = log.record(sources, session=args.session)
    except ReceiptError as exc:
        print(json.dumps({"ok": False, "refused": str(exc)}))
        return 2
    unknown = sorted(n for n, s in result["record"]["sources"].items()
                     if s["status"] == STATUS_UNKNOWN)
    print(json.dumps({"ok": True, "written": result["written"],
                      "recovered_anchor": result["recovered_anchor"],
                      "unknown_sources": unknown, **_summary(result["record"])}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
