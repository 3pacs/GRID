"""One session's Desk source receipt, verified before anything is taken from it.

A caller that names a receipt by a head hash and hands over payload bytes has
shown nothing: the hash and the bytes can be anything. This module opens the
receipt log itself (``intelligence.desk_source_receipts``) and checks, in this
order, before it returns a byte:

* the log's own hash chain, every payload file against the hash that names it,
  and the log's local anchors;
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

Read-only. It scores nothing and promotes nothing.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

BY_CODE = (
    "the receipt log's hash chain, its local anchors, and every payload file against its hash",
    "the anchor copy presented covers this receipt and is consistent with the log",
    "this receipt read the named source, and the payload is the bytes its hash names",
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
    try:
        check = log.verify(anchors, full=True)
    except (OSError, ValueError) as exc:
        raise WitnessError(f"the receipt log could not be verified ({type(exc).__name__})") from None
    if not check["ok"]:
        raise WitnessError(f"the receipt log does not verify: {check['detail']}")
    lines = list(dsr.raw_lines(log.path))
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
        payload = log.payload(entry["payload_sha256"])
    except (OSError, dsr.ReceiptError) as exc:
        raise WitnessError(f"the {source} payload is not the bytes its hash names ({type(exc).__name__})") from None
    return {
        "session": session,
        "head_sha256": dsr.sha256_hex(lines[index]),
        "log_records": index + 1,
        "read_finished_at": entry["read_finished_at"],
        "payload_sha256": entry["payload_sha256"],
        "payload": payload,
        "external_anchor": {"file": anchors.name, "sha256": hashlib.sha256(anchors.read_bytes()).hexdigest(),
                            "witnessed_records": check["witnessed_records"]},
        "checked": {"by_code": list(BY_CODE), "by_a_person": BY_A_PERSON},
    }
