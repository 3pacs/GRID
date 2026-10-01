"""Stream adapters: one per forward-logged prediction stream.

An adapter reads its stream's own append-only log READ-ONLY, verifies the
log's hash chain, ignores every record written after the run instant, and
exposes:

* ``predictions(view)`` -> normalized prediction dicts (``evals.e2.records``);
* ``resolve(view, prediction, now)`` -> a resolution dict, or ``None`` while
  the outcome is not observable yet;
* ``unit_scores(view, ledger_state, now)`` -> unit-level score records the
  stream's rule defines (``[]`` if none);
* ``activity(view)`` -> counts for the snapshot (logged, excluded by reason ...).

Adapters never write to the stream's directory and never import the stream's
own code: the stream's record format is read as data.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class SourceView:
    """A verified, ``now``-bounded view of one stream log."""

    stream: str
    path: Path
    records: list[dict]                 # records with run_at <= now, in log order
    line_sha256: list[str]              # parallel to records
    prev_sha256: list[str | None]       # parallel to records
    total_records: int                  # records in the file (including any after now)
    head_sha256: str | None             # head of the verified prefix E2 used
    extra: dict = field(default_factory=dict)

    def receipt(self, index: int, *, writer_code_sha: str | None, witness: str) -> dict:
        return {
            "log": self.path.name,
            "line_index": index,
            "line_sha256": self.line_sha256[index],
            "prev_sha256": self.prev_sha256[index],
            "source_records_seen": len(self.records),
            "source_head_sha256_seen": self.head_sha256,
            "writer_code_sha": writer_code_sha,
            "witness": witness,
        }


def build_view(stream: str, path: Path, pairs: list[tuple[bytes, dict]], now, run_at_key: str = "run_at") -> SourceView:
    """Keep the chain prefix whose records were written at or before ``now``.

    The log is append-only in time order, so the prefix stops at the first
    record written after ``now``; nothing after it is visible to this run.
    """
    from evals.e2.chain import sha256_hex
    from evals.e2.records import parse_ts

    kept: list[dict] = []
    shas: list[str] = []
    prevs: list[str | None] = []
    for line, record in pairs:
        stamp = record.get(run_at_key)
        if stamp is None or parse_ts(stamp) > now:
            break
        kept.append(record)
        shas.append(sha256_hex(line))
        prevs.append(record.get("prev_sha256"))
    return SourceView(stream, Path(path), kept, shas, prevs, len(pairs), shas[-1] if shas else None)
