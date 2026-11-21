"""One session's Desk source receipt, verified before anything is taken from it.

A caller that names a receipt by a head hash and hands over payload bytes has
shown nothing: the hash and the bytes can be anything. This module opens the
receipt log itself (``intelligence.desk_source_receipts``) and checks, in this
order, before it returns a byte:

* the log's own hash chain and its local anchors;
* an anchor copy held somewhere else (``external_anchors``: the vault's
  witness file), which must be a different file from the log's own anchors and
  must cover the receipt;
* that the session's receipt read the named source, and that the payload is
  the bytes its hash names.

What the code cannot check is said in the result, in words, and is never
claimed: that the anchor copy presented really is the one held off-host. A
person checks that, against the vault's pushed file or the hub post of that
session. The result carries the copy's sha256 and the receipt's head so that
check has something exact to compare.

One capture, then no second look. The receipt log, its local anchors and the
anchor copy are each read from disk exactly once. Verification runs on a
private copy written from those captured bytes, and the receipt, its head, its
place in the log and the anchor copy's digest are all taken from the same
captured bytes, with the captured lines tied to what was verified by record
count and head hash. The live files are never reopened after the capture, so a
log replaced after it was verified cannot supply the receipt. The payload file
is read once and used only if those bytes are the ones its hash names.

Read-only. It scores nothing and promotes nothing.
"""

from __future__ import annotations

import hashlib
import json
import tempfile
from datetime import date
from pathlib import Path

BY_CODE = (
    "the receipt log's hash chain and its local anchors, on one capture of the files that is never reopened",
    "the anchor copy presented covers this receipt and is consistent with the log",
    "the receipt is taken from the captured, verified lines; it read the named source; the payload is the bytes its hash names",
)
BY_A_PERSON = ("that the anchor copy presented is the one held off-host "
               "(the vault's pushed witness file, or the hub post of that session)")


class WitnessError(ValueError):
    """The receipt cannot be used as a witness."""


def verified_receipt(receipts: Path, external_anchors: Path, session: str, source: str) -> dict:
    """The verified receipt of ``session`` and its ``source`` payload, or :class:`WitnessError`."""
    from intelligence import desk_source_receipts as dsr

    try:
        date.fromisoformat(session)
    except (TypeError, ValueError):
        raise WitnessError("a receipt is named by its session, an ISO date") from None
    if receipts is None or external_anchors is None:
        raise WitnessError("a witness needs the receipt log and an anchor copy held outside it")
    log = dsr.ReceiptLog(Path(receipts))
    anchors = Path(external_anchors)
    if not log.path.is_file():
        raise WitnessError("there is no receipt log at that path")
    if not anchors.is_file() or anchors.resolve() == log.anchor_path.resolve():
        raise WitnessError("the anchor copy must be a file other than the receipt log's own anchors")
    # The one capture. Everything below is derived from these bytes; none of the three files is opened again.
    try:
        log_bytes = log.path.read_bytes()
        local_anchor_bytes = log.anchor_path.read_bytes() if log.anchor_path.exists() else None
        anchor_bytes = anchors.read_bytes()
    except OSError as exc:
        raise WitnessError(f"the receipt log could not be read ({type(exc).__name__})") from None
    lines = _lines(log_bytes)
    with tempfile.TemporaryDirectory(prefix="desk-receipt-witness-") as scratch:
        private = dsr.ReceiptLog(Path(scratch) / "log")
        private.root.mkdir()
        private.path.write_bytes(log_bytes)
        if local_anchor_bytes is not None:
            private.anchor_path.write_bytes(local_anchor_bytes)
        outside = Path(scratch) / "outside" / dsr.ANCHOR_NAME
        outside.parent.mkdir()
        outside.write_bytes(anchor_bytes)
        try:
            check = private.verify(outside)
        except (OSError, ValueError) as exc:
            raise WitnessError(f"the receipt log could not be verified ({type(exc).__name__})") from None
    if not check["ok"]:
        raise WitnessError(f"the receipt log does not verify: {check['detail']}")
    # The captured lines are the verified ones: same count, same head, and each line links to the one before it.
    if check["records"] != len(lines) or not lines or check["head_sha256"] != dsr.sha256_hex(lines[-1]):
        raise WitnessError("the receipt lines captured are not the ones that were verified")
    previous = None
    for line in lines:
        if json.loads(line).get("prev_sha256") != previous:
            raise WitnessError("the receipt lines captured are not the ones that were verified")
        previous = dsr.sha256_hex(line)
    index = None
    for i, line in enumerate(lines):
        record = json.loads(line)
        if record.get("kind") == "receipt" and record.get("session") == session:
            index = i
            break
    if index is None:
        raise WitnessError(f"the receipt log holds no receipt for session {session}")
    if check["witnessed_records"] < index + 1:
        raise WitnessError("that receipt is not under the anchor copy presented: it has not been witnessed outside the log")
    record = json.loads(lines[index])
    entry = (record.get("sources") or {}).get(source)
    if not isinstance(entry, dict) or entry.get("status") != dsr.STATUS_READ:
        raise WitnessError(f"that receipt did not read {source}")
    try:
        payload = (log.payload_dir / f"{entry['payload_sha256']}.json").read_bytes()    # read once; used only if it is its hash
    except OSError as exc:
        raise WitnessError(f"the {source} payload could not be read ({type(exc).__name__})") from None
    if dsr.sha256_hex(payload) != entry["payload_sha256"]:
        raise WitnessError(f"the {source} payload is not the bytes its hash names")
    return {
        "session": session,
        "head_sha256": dsr.sha256_hex(lines[index]),
        "log_records": index + 1,
        "read_finished_at": entry["read_finished_at"],
        "payload_sha256": entry["payload_sha256"],
        "payload": payload,
        "external_anchor": {"file": anchors.name, "sha256": hashlib.sha256(anchor_bytes).hexdigest(),
                            "witnessed_records": check["witnessed_records"]},
        "checked": {"by_code": list(BY_CODE), "by_a_person": BY_A_PERSON},
    }


def _lines(raw: bytes) -> list[bytes]:
    """The non-empty lines of a captured log, without their terminators: what ``desk_source_receipts.raw_lines`` yields."""
    return [line for line in (part.rstrip(b"\r") for part in raw.split(b"\n")) if line]
