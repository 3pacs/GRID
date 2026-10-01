"""Generic adapter for streams that log directly in the E2 stream format (``e2-stream-v1``).

This is the admission target for new forward streams (VS1/v8 survivors,
GEM-derived calls): the stream writes its own append-only JSONL with the S10
chain convention (canonical JSON lines, ``prev_sha256`` = sha256 of the
previous line), first record a header::

    {"kind": "header", "format": "e2-stream-v1", "stream": "<name>",
     "prereg_sha256": "<sha256 of the stream's pre-registration>", "run_at": ...}

then one record per prediction::

    {"kind": "prediction", "run_at": <when logged>, "prediction_id": "<name>:...",
     "family": ..., "sector": ..., "rule_id": "e2.direction.v1" | "e2.probability.v1" | "e2.rank_ic.v1",
     "target": {"instrument": "AAPL", "instrument_class": "us_equity_large_cap"},
     "horizon": {"label": "5d", "entry_date": "YYYY-MM-DD", "exit_date": "YYYY-MM-DD"},
     "call": {"kind": "direction", "side": 1} | {"kind": "probability", "event": "up", "p": 0.58}
             | {"kind": "rank_score", "score": 1.7}}

The entry is the close of ``entry_date``, which must come AFTER the record
was logged (no hindsight entries); the outcome is the close-to-close return
to ``exit_date``, resolved point-in-time by ``evals.e2.resolve``. A stream is
scored only if it is registered in ``rules.json`` (adding one is a new E2
version).
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

from evals.e2.adapters import DATA_ERRORS, SourceView, build_view, quarantine
from evals.e2.chain import ChainError, verify_source_chain
from evals.e2.records import iso, parse_ts, session_close_utc
from evals.e2.resolve import entry_close_after_issue, resolve_price_call

FORMAT = "e2-stream-v1"
CALL_FOR_RULE = {"e2.direction.v1": "direction", "e2.probability.v1": "probability", "e2.rank_ic.v1": "rank_score"}


class E2StreamAdapter:
    def __init__(self, log_path: Path, stream: str, stream_rules: dict, rules: dict, prices=None) -> None:
        self.path = Path(log_path)
        self.stream = stream
        self.rules = stream_rules
        self.all_rules = rules
        self.prices = prices

    def load(self, now: datetime) -> SourceView:
        pairs = verify_source_chain(self.path, prereg_sha256=self.rules["prereg_sha256"])
        if pairs:
            header = pairs[0][1]
            if header.get("kind") != "header" or header.get("format") != FORMAT or header.get("stream") != self.stream:
                raise ChainError(f"{self.path.name}: first record is not an {FORMAT} header for {self.stream}")
        view = build_view(self.stream, self.path, pairs, now)
        view.extra["late_entry"] = 0
        view.extra["bad_rule"] = 0
        return view

    def predictions(self, view: SourceView) -> list[dict]:
        out, seen = [], set()
        late, bad = 0, 0
        for i, record in enumerate(view.records):
            try:
                if record.get("kind") != "prediction" or record["prediction_id"] in seen:
                    continue
                seen.add(record["prediction_id"])
                if CALL_FOR_RULE.get(record.get("rule_id")) != (record.get("call") or {}).get("kind"):
                    bad += 1
                    continue
                horizon = dict(record["horizon"])
                exit_day = date.fromisoformat(horizon["exit_date"])
                horizon["ends_at"] = iso(session_close_utc(exit_day))
                pred = {
                    "stream": self.stream,
                    "family": record["family"],
                    "sector": record.get("sector"),
                    "prediction_id": record["prediction_id"],
                    "issued_at": iso(parse_ts(record["run_at"])),
                    "log_receipt": view.receipt(i, writer_code_sha=record.get("code_sha"),
                                                witness="stream hash chain + self-reported run_at"),
                    "target": record["target"],
                    "horizon": horizon,
                    "outcome_not_before": iso(session_close_utc(exit_day, early=True)),
                    "call": record["call"],
                    "rule_id": record["rule_id"],
                    "unit": (f"{self.stream}:{record['family']}:{horizon['label']}:{horizon['entry_date']}"
                             if record["rule_id"] == "e2.rank_ic.v1" else None),
                }
                if not entry_close_after_issue(pred):
                    late += 1
                    continue
                out.append(pred)
            except DATA_ERRORS as exc:  # a malformed upstream record is quarantined, not fatal
                quarantine(view, i, exc)
        view.extra["late_entry"], view.extra["bad_rule"] = late, bad
        return out

    def resolve(self, view: SourceView, pred: dict, now: datetime) -> dict | None:
        return resolve_price_call(pred, self.prices, now, self.all_rules)

    def unit_scores(self, view: SourceView, ledger_state: dict, now: datetime) -> list[dict]:
        return []  # rank IC per date is scored by the board for every stream (evals.e2.board)

    def activity(self, view: SourceView) -> dict:
        return {"records": len(view.records), "late_entry_refused": view.extra["late_entry"],
                "rule_call_mismatch_refused": view.extra["bad_rule"],
                "quarantined_records": len(view.extra.get("quarantined", []))}
